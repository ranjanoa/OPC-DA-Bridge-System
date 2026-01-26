using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text.Json;

// Third Party Libraries for OPC and InfluxDB
using TitaniumAS.Opc.Client; 
using TitaniumAS.Opc.Client.Da;
using TitaniumAS.Opc.Client.Common;
using TitaniumAS.Opc.Client.Da.Browsing;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;

var builder = WebApplication.CreateBuilder(args);

// Resolve CS1061: Ensure CORS is configured for the Dashboard UI
builder.Services.AddCors(options => 
    options.AddPolicy("AllowAll", p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();
app.UseCors("AllowAll");

// Global State Management
OpcDaServer? _server = null;
bool _bridgeActive = false;
DateTime _lastCommandProcessed = DateTime.UtcNow;
var _liveMonitor = new ConcurrentDictionary<string, object>();
CancellationTokenSource? _cts = null;

// Resolve CS0103: Initialize OPC DA COM environment
Bootstrap.Initialize();

#region Configuration Logic (JSON Persistence)

// Always points to the folder where the .exe is running
string GetSettingsPath() => Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bridge_settings.json");

BridgeConfig? LoadConfigFromFile() {
    try {
        string path = GetSettingsPath();
        if (System.IO.File.Exists(path)) {
            string json = System.IO.File.ReadAllText(path);
            return JsonSerializer.Deserialize<BridgeConfig>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }
    } catch (Exception ex) { 
        Console.WriteLine($"[CONFIG] Load Error: {ex.Message}"); 
    }
    return null;
}

void SaveConfigToFile(BridgeConfig config) {
    try {
        string path = GetSettingsPath();
        string json = JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true });
        // Use System.IO explicitly to avoid conflicts
        System.IO.File.WriteAllText(path, json);
        Console.WriteLine($"[CONFIG] Persistent settings saved to {path}");
    } catch (Exception ex) { 
        Console.WriteLine($"[CONFIG] Save Error: {ex.Message}"); 
    }
}

#endregion

#region API Endpoints

app.MapGet("/", () => {
    string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "index.html");
    if (System.IO.File.Exists(path)) return Results.Content(System.IO.File.ReadAllText(path), "text/html");
    return Results.Text("Dashboard UI (index.html) not found in the application directory.");
});

app.MapGet("/api/config", () => LoadConfigFromFile() is BridgeConfig c ? Results.Ok(c) : Results.NotFound());

// UI calls this to save state without needing to start the bridge service
app.MapPost("/api/config/save", (BridgeConfig config) => {
    SaveConfigToFile(config);
    return Results.Ok(new { status = "Configuration Persisted" });
});

app.MapGet("/api/live", () => _liveMonitor);

app.MapGet("/api/browse", (string host, string progId, string? nodeId) => {
    try {
        if (_server == null || !_server.IsConnected) {
            _server = new OpcDaServer(UrlBuilder.Build(progId, host));
            _server.Connect();
        }
        var browser = new OpcDaBrowserAuto(_server);
        return Results.Ok(browser.GetElements(nodeId).Select(e => new { 
            id = e.ItemId, 
            name = e.Name, 
            type = e.HasChildren ? "folder" : "tag" 
        }));
    } catch (Exception ex) { return Results.Json(new { error = ex.Message }); }
});

app.MapPost("/api/bridge/start", (BridgeConfig config) => {
    // If bridge is running, cancel previous background tasks gracefully
    if (_bridgeActive) { 
        _cts?.Cancel(); 
        _bridgeActive = false; 
        Thread.Sleep(500); 
    }
    
    // Persist current UI state (Mappings, Influx details, Tags)
    SaveConfigToFile(config); 
    
    _bridgeActive = true;
    _cts = new CancellationTokenSource();
    _lastCommandProcessed = DateTime.UtcNow;
    
    // Launch Bidirectional Loops
    _ = Task.Run(() => RunIngestionLoop(config, _cts.Token));
    _ = Task.Run(() => RunActuationLoop(config, _cts.Token));
    
    Console.WriteLine("[SYSTEM] Bridge Started - Ingesting to kiln1, Watching kiln2");
    return Results.Ok(new { status = "Bridge Started" });
});

app.MapPost("/api/bridge/stop", () => {
    _bridgeActive = false;
    _cts?.Cancel();
    if (_server != null && _server.IsConnected) _server.Disconnect();
    return Results.Ok(new { status = "Stopped" });
});

app.MapDelete("/api/config/tag", (string tagId) => {
    var config = LoadConfigFromFile();
    if (config == null) return Results.NotFound();
    
    // Clean up tag from all mapping lists
    var updated = config with { 
        ReadTags = config.ReadTags.Where(t => t != tagId).ToArray(),
        WriteMapping = config.WriteMapping?.Where(kv => kv.Value != tagId).ToDictionary(kv => kv.Key, kv => kv.Value),
        ReadMapping = config.ReadMapping?.Where(kv => kv.Key != tagId).ToDictionary(kv => kv.Key, kv => kv.Value)
    };
    
    SaveConfigToFile(updated); 
    _liveMonitor.TryRemove(tagId, out _);
    return Results.Ok(updated);
});

#endregion

#region Background Workers

// READ: PLC -> InfluxDB kiln1
async Task RunIngestionLoop(BridgeConfig config, CancellationToken ct) {
    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var writeApi = influx.GetWriteApi();
    var group = _server!.AddGroup("Ingest_" + Guid.NewGuid().ToString().Substring(0, 8));
    group.UpdateRate = TimeSpan.FromMilliseconds(1000);
    group.AddItems(config.ReadTags.Select(t => new OpcDaItemDefinition { ItemId = t, IsActive = true }).ToArray());
    
    while (!ct.IsCancellationRequested && _bridgeActive) {
        try {
            var results = group.Read(group.Items, OpcDaDataSource.Device);
            foreach (var res in results) {
                if (res.Value != null) {
                    _liveMonitor[res.Item.ItemId] = res.Value;
                    
                    // Rename tag to its Alias for InfluxDB field name
                    string influxFieldName = (config.ReadMapping != null && config.ReadMapping.TryGetValue(res.Item.ItemId, out var alias)) 
                        ? alias 
                        : res.Item.ItemId;

                    var point = PointData.Measurement("kiln1")
                        .Field(influxFieldName, Convert.ToDouble(res.Value))
                        .Timestamp(DateTime.UtcNow, WritePrecision.Ns);
                    
                    writeApi.WritePoint(point, config.InfluxBucket, config.InfluxOrg);
                }
            }
        } catch { }
        await Task.Delay(1000, ct);
    }
}

// WRITE: InfluxDB kiln2 -> PLC
async Task RunActuationLoop(BridgeConfig config, CancellationToken ct) {
    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var queryApi = influx.GetQueryApi();
    var writeApi = influx.GetWriteApi();
    var writeGroup = _server!.AddGroup("Act_" + Guid.NewGuid().ToString().Substring(0, 8));
    
    while (!ct.IsCancellationRequested && _bridgeActive) {
        try {
            string flux = $@"from(bucket: ""{config.InfluxBucket}"") 
                          |> range(start: -1m) 
                          |> filter(fn: (r) => r[""_measurement""] == ""kiln2"") 
                          |> last()";
            
            var tables = await queryApi.QueryAsync(flux, config.InfluxOrg);
            foreach (var table in tables) {
                foreach (var record in table.Records) {
                    var ts = record.GetTime()?.ToDateTimeUtc() ?? DateTime.MinValue;
                    
                    if (ts > _lastCommandProcessed) {
                        string virtualAlias = record.GetField();
                        object val = record.GetValue();
                        
                        // Find physical tag based on the incoming alias
                        string targetTag = (config.WriteMapping != null && config.WriteMapping.TryGetValue(virtualAlias, out var t)) 
                            ? t 
                            : virtualAlias;
                        
                        try {
                            var item = writeGroup.Items.FirstOrDefault(i => i.ItemId == targetTag) 
                                       ?? writeGroup.AddItems(new[] { new OpcDaItemDefinition { ItemId = targetTag, IsActive = true } })[0].Item;
                            
                            writeGroup.Write(new[] { item }, new[] { val });
                            _lastCommandProcessed = ts;

                            Console.WriteLine($"[ACTUATION] kiln2 -> {targetTag} = {val}");

                            // Send Feedback to confirm execution
                            var feedback = PointData.Measurement("kiln2_feedback")
                                .Tag("status", "success")
                                .Tag("alias", virtualAlias)
                                .Field("physical_tag", targetTag)
                                .Field("value", Convert.ToDouble(val))
                                .Timestamp(DateTime.UtcNow, WritePrecision.Ns);
                            
                            writeApi.WritePoint(feedback, config.InfluxBucket, config.InfluxOrg);
                        }
                        catch (Exception ex) {
                            Console.WriteLine($"[ERROR][ACT] Failed to write {targetTag}: {ex.Message}");
                        }
                    }
                }
            }
        } catch { }
        await Task.Delay(2000, ct);
    }
}

#endregion

app.Run("http://localhost:5005");

public record BridgeConfig(
    string OpcHost, 
    string OpcProgId, 
    string InfluxUrl, 
    string InfluxToken, 
    string InfluxOrg, 
    string InfluxBucket, 
    string[] ReadTags, 
    Dictionary<string, string>? WriteMapping,
    Dictionary<string, string>? ReadMapping
);