using Microsoft.AspNetCore.Builder;
using TitaniumAS.Opc.Client; 
using TitaniumAS.Opc.Client.Da;
using TitaniumAS.Opc.Client.Common;
using TitaniumAS.Opc.Client.Da.Browsing;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using System.Collections.Concurrent;
using System.Text.Json;
using System.IO;
using System.Threading;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddCors(options => options.AddPolicy("AllowAll", p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));
var app = builder.Build();
app.UseCors("AllowAll");

// Global State
OpcDaServer? _server = null;
bool _bridgeActive = false;
DateTime _lastCommandProcessed = DateTime.UtcNow;
var _liveMonitor = new ConcurrentDictionary<string, object>();
CancellationTokenSource? _cts = null; // Used to restart loops dynamically

Bootstrap.Initialize();

#region Configuration Helpers

string GetSettingsPath() => Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bridge_settings.json");

BridgeConfig? LoadConfigFromFile() {
    try {
        string path = GetSettingsPath();
        if (System.IO.File.Exists(path)) {
            string json = System.IO.File.ReadAllText(path);
            return JsonSerializer.Deserialize<BridgeConfig>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }
    } catch { }
    return null;
}

void SaveConfigToFile(BridgeConfig config) {
    try {
        string path = GetSettingsPath();
        string json = JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true });
        System.IO.File.WriteAllText(path, json);
    } catch { }
}

#endregion

#region API Endpoints

app.MapGet("/", () => {
    string[] paths = {
        Path.Combine(Directory.GetCurrentDirectory(), "index.html"),
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "index.html")
    };
    foreach (var p in paths) if (System.IO.File.Exists(p)) return Results.Content(System.IO.File.ReadAllText(p), "text/html");
    return Results.Text("Dashboard index.html not found.");
});

app.MapGet("/api/config", () => LoadConfigFromFile() is BridgeConfig c ? Results.Ok(c) : Results.NotFound());

app.MapGet("/api/live", () => _liveMonitor);

app.MapGet("/api/browse", (string host, string progId, string? nodeId) => {
    try {
        if (_server == null || !_server.IsConnected) {
            _server = new OpcDaServer(UrlBuilder.Build(progId, host));
            _server.Connect();
        }
        var browser = new OpcDaBrowserAuto(_server);
        return Results.Ok(browser.GetElements(nodeId).Select(e => new { id = e.ItemId, name = e.Name, type = e.HasChildren ? "folder" : "tag" }));
    } catch (Exception ex) { return Results.Json(new { error = ex.Message }); }
});

// START / UPDATE Bridge
app.MapPost("/api/bridge/start", (BridgeConfig config) => {
    // If already running, cancel existing loops to apply new configuration
    if (_bridgeActive) {
        _cts?.Cancel();
        _bridgeActive = false;
        Thread.Sleep(500); // Small grace period for loops to exit
    }

    SaveConfigToFile(config);
    _bridgeActive = true;
    _cts = new CancellationTokenSource();
    _lastCommandProcessed = DateTime.UtcNow;

    _ = Task.Run(() => RunIngestionLoop(config, _cts.Token));
    _ = Task.Run(() => RunActuationLoop(config, _cts.Token));
    
    return Results.Ok(new { status = "Bridge Updated & Running" });
});

app.MapPost("/api/bridge/stop", () => {
    _bridgeActive = false;
    _cts?.Cancel();
    _server?.Disconnect();
    return Results.Ok(new { status = "Stopped" });
});

app.MapDelete("/api/config/tag", (string tagId) => {
    var config = LoadConfigFromFile();
    if (config == null) return Results.NotFound();

    var updated = config with { 
        ReadTags = config.ReadTags.Where(t => t != tagId).ToArray(),
        WriteMapping = config.WriteMapping?.Where(kv => kv.Value != tagId).ToDictionary(kv => kv.Key, kv => kv.Value)
    };
    SaveConfigToFile(updated);
    _liveMonitor.TryRemove(tagId, out _);
    return Results.Ok(updated);
});

#endregion

#region Workers

async Task RunIngestionLoop(BridgeConfig config, CancellationToken ct) {
    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var writeApi = influx.GetWriteApi();
    var group = _server!.AddGroup("IngestGroup_" + Guid.NewGuid().ToString());
    group.UpdateRate = TimeSpan.FromMilliseconds(1000);
    group.AddItems(config.ReadTags.Select(t => new OpcDaItemDefinition { ItemId = t, IsActive = true }).ToArray());

    while (!ct.IsCancellationRequested && _bridgeActive) {
        try {
            var results = group.Read(group.Items, OpcDaDataSource.Device);
            foreach (var res in results) {
                if (res.Value != null) {
                    _liveMonitor[res.Item.ItemId] = res.Value;
                    var point = PointData.Measurement("kiln1").Field(res.Item.ItemId, Convert.ToDouble(res.Value)).Timestamp(DateTime.UtcNow, WritePrecision.Ns);
                    writeApi.WritePoint(point, config.InfluxBucket, config.InfluxOrg);
                }
            }
        } catch { }
        await Task.Delay(1000, ct);
    }
}

async Task RunActuationLoop(BridgeConfig config, CancellationToken ct) {
    using var influx = new InfluxDBClient(config.InfluxUrl, config.InfluxToken);
    var queryApi = influx.GetQueryApi();
    var writeGroup = _server!.AddGroup("ActGroup_" + Guid.NewGuid().ToString());

    while (!ct.IsCancellationRequested && _bridgeActive) {
        try {
            string flux = $@"from(bucket: ""{config.InfluxBucket}"") |> range(start: -1m) |> filter(fn: (r) => r[""_measurement""] == ""kiln2"") |> last()";
            var tables = await queryApi.QueryAsync(flux, config.InfluxOrg);
            foreach (var table in tables) {
                foreach (var record in table.Records) {
                    var ts = record.GetTime()?.ToDateTimeUtc() ?? DateTime.MinValue;
                    if (ts > _lastCommandProcessed) {
                        string alias = record.GetField();
                        object val = record.GetValue();
                        string tag = (config.WriteMapping != null && config.WriteMapping.TryGetValue(alias, out var t)) ? t : alias;
                        var item = writeGroup.Items.FirstOrDefault(i => i.ItemId == tag) ?? writeGroup.AddItems(new[] { new OpcDaItemDefinition { ItemId = tag, IsActive = true } })[0].Item;
                        writeGroup.Write(new[] { item }, new[] { val });
                        _lastCommandProcessed = ts;
                    }
                }
            }
        } catch { }
        await Task.Delay(2000, ct);
    }
}

#endregion

app.Run("http://localhost:5005");

public record BridgeConfig(string OpcHost, string OpcProgId, string InfluxUrl, string InfluxToken, string InfluxOrg, string InfluxBucket, string[] ReadTags, Dictionary<string, string>? WriteMapping);