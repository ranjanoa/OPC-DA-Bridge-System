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

// Ensure CamelCase JSON to match JavaScript frontend
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
DateTime _lastCommandProcessed = DateTime.UtcNow;
var _liveMonitor = new ConcurrentDictionary<string, object>();
CancellationTokenSource? _cts = null;
object _lock = new object(); 

Bootstrap.Initialize();

#region Persistent Storage (Permission Fix)

string GetSettingsPath() {
    // Force use of ProgramData to avoid "Access Denied" in Program Files
    string folder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "IndustrialOpcBridge");
    if (!Directory.Exists(folder)) Directory.CreateDirectory(folder);
    return Path.Combine(folder, "bridge_settings.json");
}

BridgeConfig? LoadConfigFromFile() {
    try {
        string path = GetSettingsPath();
        if (System.IO.File.Exists(path)) {
            string json = System.IO.File.ReadAllText(path);
            return JsonSerializer.Deserialize<BridgeConfig>(json, jsonOptions);
        }
    } catch (Exception ex) { Console.WriteLine($"[CONFIG] Load Error: {ex.Message}"); }
    return null;
}

void SaveConfigToFile(BridgeConfig config) {
    try {
        string path = GetSettingsPath();
        string json = JsonSerializer.Serialize(config, jsonOptions);
        System.IO.File.WriteAllText(path, json);
        Console.WriteLine($"[CONFIG] Settings saved to: {path}");
    } catch (Exception ex) { Console.WriteLine($"[CONFIG] FATAL SAVE ERROR: {ex.Message}"); }
}

#endregion

#region Helper: Ensure OPC Connection

bool EnsureConnected(string host, string progId) {
    lock (_lock) {
        try {
            if (_server == null) {
                _server = new OpcDaServer(UrlBuilder.Build(progId, host));
            }
            if (!_server.IsConnected) {
                _server.Connect();
                Console.WriteLine($"[OPC] Connected to {progId} on {host}");
            }
            return true;
        } catch (Exception ex) {
            Console.WriteLine($"[OPC] Connection Failed: {ex.Message}");
            return false;
        }
    }
}

void SafeRemoveGroup(OpcDaGroup? group) {
    if (group != null && _server != null && _server.IsConnected) {
        try { ((ICollection<OpcDaGroup>)_server.Groups).Remove(group); } catch { }
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

app.MapGet("/api/config", () => Results.Json(LoadConfigFromFile() ?? new BridgeConfig("localhost", "", "", "", "", "", new string[0], new Dictionary<string, string>(), new Dictionary<string, string>()), jsonOptions));

app.MapPost("/api/config/save", (BridgeConfig config) => {
    SaveConfigToFile(config);
    return Results.Ok(new { status = "Saved" });
});

app.MapGet("/api/live", () => _liveMonitor);

app.MapGet("/api/browse", (string host, string progId, string? nodeId) => {
    if (!EnsureConnected(host, progId)) return Results.Problem("OPC Connection Failed.");
    try {
        var browser = new OpcDaBrowserAuto(_server!);
        
        // FIX: Reverted to GetElements because OpcDaBrowserAuto doesn't support GetBranches/GetLeaves directly
        // WinCC tags should appear if the user has correct permissions in Windows ("SIMATIC HMI" group)
        var elements = browser.GetElements(nodeId ?? "");
        
        var result = elements.Select(e => new { 
            id = e.ItemId, 
            name = e.Name, 
            type = e.HasChildren ? "folder" : "tag" 
        });

        return Results.Ok(result);
    } catch (Exception ex) { 
        Console.WriteLine($"[BROWSE ERROR] {ex.Message}");
        return Results.Json(new { error = ex.Message }); 
    }
});

app.MapPost("/api/bridge/start", (BridgeConfig config) => {
    try {
        if (_bridgeActive) { _cts?.Cancel(); _bridgeActive = false; Thread.Sleep(500); }
        
        SaveConfigToFile(config); 

        if (!EnsureConnected(config.OpcHost, config.OpcProgId)) {
            return Results.Problem("OPC Connection Failed. Check Host/ProgID.");
        }

        _bridgeActive = true;
        _cts = new CancellationTokenSource();
        _lastCommandProcessed = DateTime.UtcNow.AddSeconds(-5); 
        
        _ = Task.Run(() => RunIngestionLoop(config, _cts.Token));
        _ = Task.Run(() => RunActuationLoop(config, _cts.Token));
        
        return Results.Ok(new { status = "Active" });
    } catch (Exception ex) {
        return Results.Problem(ex.Message);
    }
});

app.MapPost("/api/bridge/stop", () => {
    _bridgeActive = false;
    _cts?.Cancel();
    lock(_lock) {
        if (_server != null && _server.IsConnected) _server.Disconnect();
    }
    return Results.Ok(new { status = "Stopped" });
});

app.MapDelete("/api/config/tag", (string tagId) => {
    var config = LoadConfigFromFile();
    if (config == null) return Results.NotFound();
    
    var rMap = config.ReadMapping ?? new Dictionary<string, string>();
    var wMap = config.WriteMapping ?? new Dictionary<string, string>();
    
    if (rMap.ContainsKey(tagId)) rMap.Remove(tagId);
    if (wMap.ContainsValue(tagId)) {
        var keyToRemove = wMap.FirstOrDefault(x => x.Value == tagId).Key;
        if (keyToRemove != null) wMap.Remove(keyToRemove);
    }

    var updated = config with { 
        ReadTags = config.ReadTags.Where(t => t != tagId).ToArray(),
        ReadMapping = rMap,
        WriteMapping = wMap
    };
    
    SaveConfigToFile(updated);
    
    object? _;
    _liveMonitor.TryRemove(tagId, out _);
    
    return Results.Ok(updated);
});

#endregion

#region Background Workers

async Task RunIngestionLoop(BridgeConfig config, CancellationToken ct) {
    if (config.ReadTags == null || config.ReadTags.Length == 0 || _server == null) return;
    
    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var writeApi = influx.GetWriteApi();
    var group = _server.AddGroup("Ingest_" + Guid.NewGuid().ToString().Substring(0,8));
    group.UpdateRate = TimeSpan.FromMilliseconds(1000);
    group.AddItems(config.ReadTags.Select(t => new OpcDaItemDefinition { ItemId = t, IsActive = true }).ToArray());
    
    while (!ct.IsCancellationRequested && _bridgeActive) {
        try {
            var results = group.Read(group.Items, OpcDaDataSource.Device);
            foreach (var res in results) {
                if (res != null && res.Value != null) {
                    _liveMonitor[res.Item.ItemId] = res.Value;
                    string fieldName = (config.ReadMapping != null && config.ReadMapping.TryGetValue(res.Item.ItemId, out var alias)) ? alias : res.Item.ItemId;
                    var point = PointData.Measurement("kiln1").Field(fieldName, Convert.ToDouble(res.Value)).Timestamp(DateTime.UtcNow, InfluxDB.Client.Api.Domain.WritePrecision.Ns);
                    writeApi.WritePoint(point, config.InfluxBucket, config.InfluxOrg);
                }
            }
        } catch { }
        await Task.Delay(1000, ct);
    }
}

async Task RunActuationLoop(BridgeConfig config, CancellationToken ct) {
    if (_server == null) return;

    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var queryApi = influx.GetQueryApi();
    var writeApi = influx.GetWriteApi();
    var writeGroup = _server.AddGroup("Act_" + Guid.NewGuid().ToString().Substring(0,8));
    
    while (!ct.IsCancellationRequested && _bridgeActive) {
        try {
            string flux = $@"from(bucket: ""{config.InfluxBucket}"") |> range(start: -15m) |> filter(fn: (r) => r[""_measurement""] == ""kiln2"") |> last()";
            var tables = await queryApi.QueryAsync(flux, config.InfluxOrg);
            
            if (tables == null) continue;

            foreach (var table in tables) {
                foreach (var record in table.Records) {
                    var ts = record.GetTime()?.ToDateTimeUtc() ?? DateTime.MinValue;
                    
                    if (ts > _lastCommandProcessed) {
                        string field = record.GetField();
                        object val = record.GetValue();
                        string tag = (config.WriteMapping != null && config.WriteMapping.TryGetValue(field, out var t)) ? t : field;
                        
                        Console.WriteLine($"[ACTUATION] EXECUTING: {field} -> {tag} = {val}");
                        
                        var itemResult = writeGroup.Items.FirstOrDefault(i => i.ItemId == tag);
                        OpcDaItem? item = itemResult;

                        if (item == null) {
                            var added = writeGroup.AddItems(new[] { new OpcDaItemDefinition { ItemId = tag, IsActive = true } });
                            if (added != null && added.Length > 0 && added[0].Error.Succeeded) {
                                item = added[0].Item;
                            }
                        }
                        
                        if (item != null) {
                            writeGroup.Write(new[] { item }, new[] { val });
                            _lastCommandProcessed = ts;
                            
                            var feedback = PointData.Measurement("kiln2_feedback").Tag("status", "success").Tag("alias", field).Field("value", Convert.ToDouble(val)).Timestamp(DateTime.UtcNow, InfluxDB.Client.Api.Domain.WritePrecision.Ns);
                            writeApi.WritePoint(feedback, config.InfluxBucket, config.InfluxOrg);
                        } else {
                            Console.WriteLine($"[ACTUATION] FAILED: Could not add tag {tag} to OPC Group. Check if tag exists in PLC.");
                        }
                    }
                }
            }
        } catch (Exception ex) { Console.WriteLine($"[ACTUATION] Error: {ex.Message}"); }
        await Task.Delay(2000, ct);
    }
}

#endregion

app.Run("http://localhost:5005");

public record BridgeConfig(string OpcHost, string OpcProgId, string InfluxUrl, string InfluxToken, string InfluxOrg, string InfluxBucket, string[] ReadTags, Dictionary<string, string>? ReadMapping, Dictionary<string, string>? WriteMapping);
