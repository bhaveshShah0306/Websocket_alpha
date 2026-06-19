using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using WebSocket_alpha.Models;

namespace WebSocket_alpha.Services;

/// <summary>
/// Central registry for all active WebSocket connections.
///
/// ARCHITECT PATTERN REUSED:
///   The architect's TcpListener.AcceptTcpClientAsync() loop adds clients
///   to an implicit pool and fires HandleClientAsync in a background Task.
///   Here we make that pool EXPLICIT as a ConcurrentDictionary so every
///   handler can reach all peers (needed for broadcast &amp; chat-room).
/// </summary>
public class WebConnectionManager
{
    // Thread-safe dictionary: clientId → ConnectedClient
    // Mirrors the architect's implicit "50 connection" pool
    private readonly ConcurrentDictionary<string, ConnectedClient> _clients = new();

    // ── Registry operations ──────────────────────────────────────────────────

    public ConnectedClient Register(WebSocket socket)
    {
        var client = new ConnectedClient { Socket = socket };
        _clients[client.Id] = client;
        return client;
    }

    public void Unregister(string clientId)
    {
        _clients.TryRemove(clientId, out _);
    }

    public ConnectedClient? Get(string clientId) =>
        _clients.TryGetValue(clientId, out var c) ? c : null;

    public IEnumerable<ConnectedClient> All() => _clients.Values;

    public IEnumerable<ConnectedClient> InRoom(string room) =>
        _clients.Values.Where(c => c.Room == room);

    public int Count => _clients.Count;

    // ── Send helpers (reuse across all use-cases) ────────────────────────────

    /// <summary>
    /// Send a typed JSON message to a single client.
    /// Mirrors: stream.WriteAsync(responseData, ...) in architect's HandleClientAsync.
    /// </summary>
    public async Task SendAsync(ConnectedClient client, WsMessage message,
        CancellationToken ct = default)
    {
        if (client.Socket.State != WebSocketState.Open) return;

        var json = JsonSerializer.Serialize(message);
        var bytes = Encoding.UTF8.GetBytes(json);
        var segment = new ArraySegment<byte>(bytes);

        // Use the client's own CTS merged with the caller's token
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, client.Cts.Token);
        await client.Socket.SendAsync(segment, WebSocketMessageType.Text, true, linked.Token);
    }

    /// <summary>
    /// Broadcast to ALL connected clients.
    /// Mirrors: architect's ConnectionCount loop firing ConnectAndReadAsync for each peer.
    /// </summary>
    public Task BroadcastAsync(WsMessage message, string? excludeId = null,
        CancellationToken ct = default)
    {
        var targets = All().Where(c => c.Id != excludeId);
        return Task.WhenAll(targets.Select(c => SendJsonSafe(c, message, ct)));
    }

    /// <summary>
    /// Broadcast to all clients in a named room.
    /// </summary>
    public Task BroadcastToRoomAsync(string room, WsMessage message,
        string? excludeId = null, CancellationToken ct = default)
    {
        var targets = InRoom(room).Where(c => c.Id != excludeId);
        return Task.WhenAll(targets.Select(c => SendJsonSafe(c, message, ct)));
    }

    /// <summary>
    /// Send raw binary bytes to a single client.
    /// Mirrors: architect's stream.WriteAsync with raw byte[] buffer.
    /// </summary>
    public async Task SendBinaryAsync(ConnectedClient client, byte[] data,
        CancellationToken ct = default)
    {
        if (client.Socket.State != WebSocketState.Open) return;

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, client.Cts.Token);
        await client.Socket.SendAsync(
            new ArraySegment<byte>(data),
            WebSocketMessageType.Binary,
            endOfMessage: true,
            linked.Token);
    }

    // Fire-and-forget safe send (won't break a broadcast if one client fails)
    private async Task SendJsonSafe(ConnectedClient client, WsMessage message,
        CancellationToken ct)
    {
        try { await SendAsync(client, message, ct); }
        catch { /* client disconnected mid-broadcast — ignore */ }
    }
}
