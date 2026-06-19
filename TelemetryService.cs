using WebSocket_alpha.Models;
using WebSocket_alpha.Services;
using System.Threading;

namespace WebSocket_alpha.Services;

/// <summary>
/// Background service that simulates sensor readings and pushes them
/// to subscribed WebSocket clients every second.
///
/// ARCHITECT PATTERN REUSED:
///   The architect's publisher fires data continuously in a while(!cancelled) loop.
///   Here, IHostedService is the ASP.NET equivalent — it starts with the app,
///   runs continuously, and shuts down cleanly via CancellationToken.
/// </summary>
public class TelemetryService : BackgroundService
{
    private readonly WebConnectionManager _connections;
    private readonly ILogger<TelemetryService> _logger;

    private static readonly string[] Sensors = ["CPU_TEMP", "GPU_LOAD", "NETWORK_MBPS", "DISK_IOPS"];
    private static readonly Random _rng = new();

    // Tracks which clients have subscribed to live telemetry
    private readonly HashSet<string> _subscribers = [];
    private readonly object _lock = new();

    public TelemetryService(WebConnectionManager connections, ILogger<TelemetryService> logger)
    {
        _connections = connections;
        _logger = logger;
    }

    public void Subscribe(string clientId)
    {
        lock (_lock) _subscribers.Add(clientId);
        _logger.LogInformation("[TELEMETRY] Client {Id} subscribed", clientId);
    }

    public void Unsubscribe(string clientId)
    {
        lock (_lock) _subscribers.Remove(clientId);
    }

    /// <summary>
    /// Core push loop — mirrors the architect's continuous read/write while loop,
    /// but server-initiated (push) rather than client-initiated (pull).
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("[TELEMETRY] Background push service started");

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken); // push every 1 second

            string[] currentSubscribers;
            lock (_lock) currentSubscribers = [.. _subscribers];

            if (currentSubscribers.Length == 0) continue;

            // Generate a batch of sensor readings
            var reading = new TelemetryReading(
                Sensor: Sensors[_rng.Next(Sensors.Length)],
                Value: Math.Round(_rng.NextDouble() * 100, 2),
                Unit: "units",
                Timestamp: DateTime.UtcNow
            );

            var message = new WsMessage(
                Type: "telemetry",
                Payload: System.Text.Json.JsonSerializer.Serialize(reading),
                Room: null,
                SenderId: "SERVER"
            );

            // Push to all subscribed clients concurrently
            // Mirrors: Task[] connectionTasks = ... Task.WhenAll(connectionTasks)
            var pushTasks = currentSubscribers
                .Select(id => _connections.Get(id))
                .Where(c => c != null)
                .Select(c => _connections.SendAsync(c!, message, stoppingToken));

            await Task.WhenAll(pushTasks);
        }
    }
}
