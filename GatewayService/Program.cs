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
DateTime _lastCommandProcessed = DateTime.UtcNow;
var _liveMonitor = new ConcurrentDictionary<string, object>();
CancellationTokenSource? _cts = null;
object _lock = new object(); 

Bootstrap.Initialize();

#region Persistence

string GetSettingsPath() {
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
    } catch { }
    return null;
}

void SaveConfigToFile(BridgeConfig config) {
    try {
        string path = GetSettingsPath();
        string json = JsonSerializer.Serialize(config, jsonOptions);
        System.IO.File.WriteAllText(path, json);
    } catch (Exception ex) { Console.WriteLine($"[CONFIG] Save Error: {ex.Message}"); }
}

#endregion

#region Connection Logic

bool EnsureConnected(string host, string progId) {
    lock (_lock) {
        try {
            if (_server == null) _server = new OpcDaServer(UrlBuilder.Build(progId, host));
            if (!_server.IsConnected) {
                _server.Connect();
                Console.WriteLine($"[OPC] Connected to {progId}");
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
        string target = nodeId ?? "";

        // 1. Get Folders (Sorted)
        var branches = browser.GetBranches(target)
            .Select(b => new { id = b.ItemId, name = b.Name, type = "folder" })
            .OrderBy(x => x.name);

        // 2. Get Tags (Sorted)
        var leaves = browser.GetLeaves(target)
            .Select(l => new { id = l.ItemId, name = l.Name, type = "tag" })
            .OrderBy(x => x.name);

        // 3. Combine: Folders first, then Tags
        var combined = branches.Concat(leaves).ToList();

        return Results.Ok(combined);
    } catch (Exception ex) { 
        // Fallback for servers that don't support branch/leaf separation
        try {
             var elements = new OpcDaBrowserAuto(_server!).GetElements(nodeId ?? "")
                .Select(e => new { id = e.ItemId, name = e.Name, type = e.HasChildren ? "folder" : "tag" })
                .OrderByDescending(x => x.type) // Folders first
                .ThenBy(x => x.name);
            return Results.Ok(elements);
        } catch {
             return Results.Json(new { error = ex.Message }); 
        }
    }
});

app.MapPost("/api/bridge/start", (BridgeConfig config) => {
    if (_bridgeActive) { _cts?.Cancel(); _bridgeActive = false; Thread.Sleep(500); }
    SaveConfigToFile(config); 
    if (!EnsureConnected(config.OpcHost, config.OpcProgId)) return Results.Problem("Check OPC Settings.");
    _bridgeActive = true;
    _cts = new CancellationTokenSource();
    _lastCommandProcessed = DateTime.UtcNow.AddSeconds(-10); 
    _ = Task.Run(() => RunIngestionLoop(config, _cts.Token));
    _ = Task.Run(() => RunActuationLoop(config, _cts.Token));
    return Results.Ok(new { status = "Active" });
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
        var key = wMap.FirstOrDefault(x => x.Value == tagId).Key;
        if (key != null) wMap.Remove(key);
    }

    var updated = config with { 
        ReadTags = config.ReadTags.Where(t => t != tagId).ToArray(),
        ReadMapping = rMap,
        WriteMapping = wMap
    };
    
    SaveConfigToFile(updated);
    _liveMonitor.TryRemove(tagId, out object? _);
    return Results.Ok(updated);
});

#endregion

#region Workers

async Task RunIngestionLoop(BridgeConfig config, CancellationToken ct) {
    if (config.ReadTags == null || config.ReadTags.Length == 0) return;
    
    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var writeApi = influx.GetWriteApi();
    
    while (!ct.IsCancellationRequested && _bridgeActive) {
        OpcDaGroup? group = null;
        try {
            if (!EnsureConnected(config.OpcHost, config.OpcProgId)) { await Task.Delay(5000, ct); continue; }

            group = _server!.AddGroup("Ingest_" + Guid.NewGuid().ToString().Substring(0,8));
            group.UpdateRate = TimeSpan.FromMilliseconds(1000);
            
            var tags = config.ReadTags.Select(t => new OpcDaItemDefinition { ItemId = t, IsActive = true }).ToArray();
            if (tags.Length > 0) group.AddItems(tags);
            
            while (!ct.IsCancellationRequested && _bridgeActive && _server.IsConnected) {
                var results = group.Read(group.Items, OpcDaDataSource.Device);
                foreach (var res in results) {
                    if (res?.Value != null) {
                        _liveMonitor[res.Item.ItemId] = res.Value;
                        string field = (config.ReadMapping != null && config.ReadMapping.TryGetValue(res.Item.ItemId, out var a)) ? a : res.Item.ItemId;
                        
                        if (double.TryParse(res.Value.ToString(), out double val)) {
                            var point = PointData.Measurement("kiln1").Field(field, val).Timestamp(DateTime.UtcNow, InfluxDB.Client.Api.Domain.WritePrecision.Ns);
                            writeApi.WritePoint(point, config.InfluxBucket, config.InfluxOrg);
                        }
                    }
                }
                await Task.Delay(1000, ct);
            }
        } catch { await Task.Delay(5000, ct); } finally { SafeRemoveGroup(group); }
    }
}

async Task RunActuationLoop(BridgeConfig config, CancellationToken ct) {
    while (!ct.IsCancellationRequested && _bridgeActive) {
        OpcDaGroup? writeGroup = null;
        try {
            if (!EnsureConnected(config.OpcHost, config.OpcProgId)) { await Task.Delay(5000, ct); continue; }

            using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
            var queryApi = influx.GetQueryApi();
            writeGroup = _server!.AddGroup("Act_" + Guid.NewGuid().ToString().Substring(0,8));
            
            while (!ct.IsCancellationRequested && _bridgeActive && _server.IsConnected) {
                string flux = $@"from(bucket: ""{config.InfluxBucket}"") |> range(start: -15m) |> filter(fn: (r) => r[""_measurement""] == ""kiln2"") |> last()";
                var tables = await queryApi.QueryAsync(flux, config.InfluxOrg);
                if (tables != null) {
                    foreach (var record in tables.SelectMany(t => t.Records)) {
                        var ts = record.GetTime()?.ToDateTimeUtc() ?? DateTime.MinValue;
                        if (ts > _lastCommandProcessed) {
                            string field = record.GetField();
                            object val = record.GetValue();
                            string tag = (config.WriteMapping != null && config.WriteMapping.TryGetValue(field, out var t)) ? t : field;
                            
                            var item = writeGroup.Items.FirstOrDefault(i => i.ItemId == tag);
                            if (item == null) {
                                var added = writeGroup.AddItems(new[] { new OpcDaItemDefinition { ItemId = tag, IsActive = true } });
                                if (added != null && added.Length > 0 && added[0].Error.Succeeded) item = added[0].Item;
                            }

                            if (item != null) {
                                writeGroup.Write(new[] { item }, new[] { val });
                                _lastCommandProcessed = ts;
                                Console.WriteLine($"[ACTUATION] Executed: {tag} = {val}");
                                if (double.TryParse(val.ToString(), out double numericVal)) {
                                    var feedback = PointData.Measurement("kiln2_feedback").Tag("status", "success").Tag("alias", field).Field("value", numericVal).Timestamp(DateTime.UtcNow, InfluxDB.Client.Api.Domain.WritePrecision.Ns);
                                    writeApi.WritePoint(feedback, config.InfluxBucket, config.InfluxOrg);
                                }
                            }
                        }
                    }
                }
                await Task.Delay(2000, ct);
            }
        } catch { await Task.Delay(5000, ct); } finally { SafeRemoveGroup(writeGroup); }
    }
}

#endregion

app.Run("http://localhost:5005");

public record BridgeConfig(string OpcHost, string OpcProgId, string InfluxUrl, string InfluxToken, string InfluxOrg, string InfluxBucket, string[] ReadTags, Dictionary<string, string>? ReadMapping, Dictionary<string, string>? WriteMapping);
