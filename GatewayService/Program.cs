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

// Third Party Libraries
using TitaniumAS.Opc.Client; 
using TitaniumAS.Opc.Client.Da;
using TitaniumAS.Opc.Client.Common;
using TitaniumAS.Opc.Client.Da.Browsing;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;

var builder = WebApplication.CreateBuilder(args);

var jsonOptions = new JsonSerializerOptions 
{ 
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
    WriteIndented = true 
};

builder.Services.AddCors(options => 
    options.AddPolicy("AllowAll", p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();
app.UseCors("AllowAll");

// Global State
OpcDaServer? _server = null;
bool _bridgeActive = false;
DateTime _lastCommandProcessed = DateTime.UtcNow.AddMinutes(-5); // Start with a buffer to catch missed commands
var _liveMonitor = new ConcurrentDictionary<string, object>();
CancellationTokenSource? _cts = null;

Bootstrap.Initialize();

#region Persistent Storage Logic (ProgramData Fallback)

// Resolves a writable path even if Program Files is locked
string GetSettingsPath() {
    string localPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bridge_settings.json");
    
    // Test if we can write to the local folder
    try {
        string testFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "write_test.tmp");
        System.IO.File.WriteAllText(testFile, "test");
        System.IO.File.Delete(testFile);
        return localPath;
    } catch (UnauthorizedAccessException) {
        // Fallback to ProgramData (Standard Windows writable area for all users)
        string programData = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "IndustrialOpcBridge");
        if (!Directory.Exists(programData)) Directory.CreateDirectory(programData);
        string fallbackPath = Path.Combine(programData, "bridge_settings.json");
        
        // If the file doesn't exist in ProgramData yet, try to copy the template from Program Files
        if (!System.IO.File.Exists(fallbackPath) && System.IO.File.Exists(localPath)) {
            try { System.IO.File.Copy(localPath, fallbackPath); } catch { }
        }
        return fallbackPath;
    } catch {
        return localPath;
    }
}

BridgeConfig? LoadConfigFromFile() {
    try {
        string path = GetSettingsPath();
        if (System.IO.File.Exists(path)) {
            string json = System.IO.File.ReadAllText(path);
            var config = JsonSerializer.Deserialize<BridgeConfig>(json, jsonOptions);
            Console.WriteLine($"[CONFIG] Loaded from: {path}");
            return config;
        }
    } catch (Exception ex) { Console.WriteLine($"[CONFIG] Load Error: {ex.Message}"); }
    return null;
}

void SaveConfigToFile(BridgeConfig config) {
    try {
        string path = GetSettingsPath();
        string json = JsonSerializer.Serialize(config, jsonOptions);
        System.IO.File.WriteAllText(path, json);
        Console.WriteLine($"[CONFIG] Successfully saved to: {path}");
    } catch (Exception ex) { 
        Console.WriteLine($"[CONFIG] CRITICAL SAVE ERROR: {ex.Message}");
    }
}

#endregion

#region API Endpoints

app.MapGet("/", () => {
    string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "index.html");
    if (System.IO.File.Exists(path)) return Results.Content(System.IO.File.ReadAllText(path), "text/html");
    return Results.Text("index.html not found.");
});

app.MapGet("/favicon.ico", () => Results.NoContent());

app.MapGet("/api/config", () => LoadConfigFromFile() is BridgeConfig c ? Results.Json(c, jsonOptions) : Results.NotFound());

app.MapPost("/api/config/save", (BridgeConfig config) => {
    SaveConfigToFile(config);
    return Results.Ok(new { status = "Saved" });
});

app.MapGet("/api/live", () => _liveMonitor);

app.MapGet("/api/browse", (string host, string progId, string? nodeId) => {
    try {
        if (_server == null || !_server.IsConnected) {
            _server = new OpcDaServer(UrlBuilder.Build(progId, host));
            _server.Connect();
        }
        var browser = new OpcDaBrowserAuto(_server);
        return Results.Ok(browser.GetElements(nodeId).Select(e => new { id = e.ItemId, name = e.Name, type = e.HasChildren ? "folder" : "tag" }));
    } catch (Exception ex) { 
        return Results.Json(new { error = ex.Message }); 
    }
});

app.MapPost("/api/bridge/start", (BridgeConfig config) => {
    if (_bridgeActive) { _cts?.Cancel(); _bridgeActive = false; Thread.Sleep(500); }
    SaveConfigToFile(config); 
    _bridgeActive = true;
    _cts = new CancellationTokenSource();
    
    // Reset timer with buffer to ensure we don't skip the first command on start
    _lastCommandProcessed = DateTime.UtcNow.AddMinutes(-1); 
    
    _ = Task.Run(() => RunIngestionLoop(config, _cts.Token));
    _ = Task.Run(() => RunActuationLoop(config, _cts.Token));
    
    Console.WriteLine("[SYSTEM] Bridge Service Started.");
    return Results.Ok(new { status = "Active" });
});

app.MapPost("/api/bridge/stop", () => {
    _bridgeActive = false;
    _cts?.Cancel();
    if (_server != null && _server.IsConnected) _server.Disconnect();
    Console.WriteLine("[SYSTEM] Bridge Service Stopped.");
    return Results.Ok(new { status = "Stopped" });
});

app.MapDelete("/api/config/tag", (string tagId) => {
    var config = LoadConfigFromFile();
    if (config == null) return Results.NotFound();
    var updated = config with { 
        ReadTags = config.ReadTags.Where(t => t != tagId).ToArray(),
        ReadMapping = config.ReadMapping?.Where(kv => kv.Key != tagId).ToDictionary(kv => kv.Key, kv => kv.Value),
        WriteMapping = config.WriteMapping?.Where(kv => kv.Value != tagId).ToDictionary(kv => kv.Key, kv => kv.Value)
    };
    SaveConfigToFile(updated);
    _liveMonitor.TryRemove(tagId, out _);
    return Results.Ok(updated);
});

#endregion

#region Background Loops

async Task RunIngestionLoop(BridgeConfig config, CancellationToken ct) {
    if (config.ReadTags == null || config.ReadTags.Length == 0) return;
    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var writeApi = influx.GetWriteApi();
    var group = _server!.AddGroup("Ingest_" + Guid.NewGuid().ToString().Substring(0,8));
    group.UpdateRate = TimeSpan.FromMilliseconds(1000);
    group.AddItems(config.ReadTags.Select(t => new OpcDaItemDefinition { ItemId = t, IsActive = true }).ToArray());
    
    while (!ct.IsCancellationRequested && _bridgeActive) {
        try {
            var results = group.Read(group.Items, OpcDaDataSource.Device);
            foreach (var res in results) {
                if (res.Value != null) {
                    _liveMonitor[res.Item.ItemId] = res.Value;
                    string fieldName = (config.ReadMapping != null && config.ReadMapping.TryGetValue(res.Item.ItemId, out var alias)) ? alias : res.Item.ItemId;
                    var point = PointData.Measurement("kiln1").Field(fieldName, Convert.ToDouble(res.Value)).Timestamp(DateTime.UtcNow, WritePrecision.Ns);
                    writeApi.WritePoint(point, config.InfluxBucket, config.InfluxOrg);
                }
            }
        } catch { }
        await Task.Delay(1000, ct);
    }
}

async Task RunActuationLoop(BridgeConfig config, CancellationToken ct) {
    if (config.WriteMapping == null || config.WriteMapping.Count == 0) {
        Console.WriteLine("[ACTUATION] No write mappings defined. Loop suspended.");
        return;
    }

    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var queryApi = influx.GetQueryApi();
    var writeApi = influx.GetWriteApi();
    var writeGroup = _server!.AddGroup("Act_" + Guid.NewGuid().ToString().Substring(0,8));
    
    Console.WriteLine($"[ACTUATION] Monitoring kiln2 for {config.WriteMapping.Count} mapped fields...");

    while (!ct.IsCancellationRequested && _bridgeActive) {
        try {
            // Query: Check last 1 hour of kiln2 commands
            string flux = $@"from(bucket: ""{config.InfluxBucket}"") 
                          |> range(start: -1h) 
                          |> filter(fn: (r) => r[""_measurement""] == ""kiln2"") 
                          |> last()";
            
            var tables = await queryApi.QueryAsync(flux, config.InfluxOrg);
            int commandsFound = 0;

            foreach (var table in tables) {
                foreach (var record in table.Records) {
                    var ts = record.GetTime()?.ToDateTimeUtc() ?? DateTime.MinValue;
                    
                    // CLOCK DRIFT FIX: We compare record timestamp to our local processing tracker
                    if (ts > _lastCommandProcessed) {
                        commandsFound++;
                        string incomingAlias = record.GetField();
                        object val = record.GetValue();
                        
                        string physicalTag = (config.WriteMapping != null && config.WriteMapping.TryGetValue(incomingAlias, out var t)) ? t : incomingAlias;
                        
                        Console.WriteLine($"[ACTUATION] EXECUTING: {incomingAlias} -> {physicalTag} = {val} (Time: {ts:HH:mm:ss}Z)");
                        
                        var item = writeGroup.Items.FirstOrDefault(i => i.ItemId == physicalTag) 
                                   ?? writeGroup.AddItems(new[] { new OpcDaItemDefinition { ItemId = physicalTag, IsActive = true } })[0].Item;
                        
                        writeGroup.Write(new[] { item }, new[] { val });
                        
                        // Update tracker to this record's time
                        _lastCommandProcessed = ts;

                        var feedback = PointData.Measurement("kiln2_feedback")
                            .Tag("status", "success")
                            .Tag("alias", incomingAlias)
                            .Field("value", Convert.ToDouble(val))
                            .Timestamp(DateTime.UtcNow, WritePrecision.Ns);
                        
                        writeApi.WritePoint(feedback, config.InfluxBucket, config.InfluxOrg);
                    }
                }
            }
        } catch (Exception ex) {
            Console.WriteLine($"[ACTUATION] Error in loop: {ex.Message}");
        }
        await Task.Delay(2000, ct);
    }
}
#endregion

app.Run("http://localhost:5005");

public record BridgeConfig(
    string OpcHost, string OpcProgId, 
    string InfluxUrl, string InfluxToken, string InfluxOrg, string InfluxBucket, 
    string[] ReadTags, 
    Dictionary<string, string>? ReadMapping,
    Dictionary<string, string>? WriteMapping
);