namespace WebSocket_alpha.Models;

// ─────────────────────────────────────────────────────────────
// Shared envelope for all JSON WebSocket messages.
// Mirrors the architect's "receivedData" string but typed.
// ─────────────────────────────────────────────────────────────
public record WsMessage(
    string Type,        // echo | broadcast | chat | telemetry | ping | pong | binary-ack | error
    string? Payload,    // UTF-8 text payload (null for binary frames)
    string? Room,       // Used by chat-room use-case
    string? SenderId    // Populated server-side; client can omit
);

// ─────────────────────────────────────────────────────────────
// Represents one connected WebSocket client.
// Mirrors the architect's TcpClient wrapper concept.
// ─────────────────────────────────────────────────────────────
public class ConnectedClient
{
    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];
    public System.Net.WebSockets.WebSocket Socket { get; init; } = null!;
    public string? Room { get; set; }                  // chat-room assignment
    public DateTime ConnectedAt { get; } = DateTime.UtcNow;
    public CancellationTokenSource Cts { get; } = new(); // mirrors architect's cts pattern
}

// ─────────────────────────────────────────────────────────────
// Simulated telemetry data (replaces WeatherForecast)
// ─────────────────────────────────────────────────────────────
public record TelemetryReading(
    string Sensor,
    double Value,
    string Unit,
    DateTime Timestamp
);
