using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using WebSocket_alpha.Models;
using WebSocket_alpha.Services;
namespace WebSocket_alpha.Middleware;

/// <summary>
/// WebSocket routing middleware.
/// Maps URL paths to use-case handlers — equivalent to the architect's
/// single TcpListener branching on port/data type.
///
/// Routes:
///   /ws/echo          → USE-CASE 1: Echo (request-reply)
///   /ws/broadcast     → USE-CASE 2: Broadcast (one-to-all)
///   /ws/telemetry     → USE-CASE 3: Live server push (subscribe/push)
///   /ws/chat/{room}   → USE-CASE 4: Chat room (grouped broadcast)
///   /ws/binary        → USE-CASE 5: Binary data transfer
///   /ws/heartbeat     → USE-CASE 6: Heartbeat / Ping-Pong keep-alive
/// </summary>
public class WebSocketMiddleware
{
    private readonly RequestDelegate _next;
    private readonly WebConnectionManager _connections;
    private readonly TelemetryService _telemetry;
    private readonly ILogger<WebSocketMiddleware> _logger;

    public WebSocketMiddleware(RequestDelegate next, WebConnectionManager connections,
        TelemetryService telemetry, ILogger<WebSocketMiddleware> logger)
    {
        _next = next;
        _connections = connections;
        _telemetry = telemetry;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            await _next(context);
            return;
        }

        var path = context.Request.Path.Value ?? "";

        // Accept the WebSocket upgrade (equivalent to server.AcceptTcpClientAsync())
        using var socket = await context.WebSockets.AcceptWebSocketAsync();
        var client = _connections.Register(socket);
        _logger.LogInformation("[CONNECTED] Client {Id} joined via {Path}", client.Id, path);

        try
        {
            // Route to the correct use-case handler
            if (path.StartsWith("/ws/echo"))
                await HandleEchoAsync(client);

            else if (path.StartsWith("/ws/broadcast"))
                await HandleBroadcastAsync(client);

            else if (path.StartsWith("/ws/telemetry"))
                await HandleTelemetryAsync(client);

            else if (path.StartsWith("/ws/chat/"))
            {
                var room = path.Split('/').LastOrDefault() ?? "general";
                await HandleChatAsync(client, room);
            }
            else if (path.StartsWith("/ws/binary"))
                await HandleBinaryAsync(client);

            else if (path.StartsWith("/ws/heartbeat"))
                await HandleHeartbeatAsync(client);

            else
            {
                await _connections.SendAsync(client,
                    new WsMessage("error", $"Unknown route: {path}", null, "SERVER"));
            }
        }
        catch (WebSocketException ex) when (
            ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
        {
            // Client closed without proper WS close handshake — same as architect's
            // "Remote host closed the connection" branch
            _logger.LogWarning("[ABRUPT DISCONNECT] Client {Id}", client.Id);
        }
        finally
        {
            _telemetry.Unsubscribe(client.Id);
            _connections.Unregister(client.Id);
            client.Cts.Cancel(); // mirrors cts.Cancel() in architect's shutdown
            _logger.LogInformation("[DISCONNECTED] Client {Id} removed. Total: {Count}",
                client.Id, _connections.Count);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // USE-CASE 1 — ECHO (request → reply)
    //
    // Architect pattern: HandleClientAsync reads data, then immediately writes
    // "Data received successfully." back.  Here we do the same but typed.
    // ════════════════════════════════════════════════════════════════════════
    private async Task HandleEchoAsync(ConnectedClient client)
    {
        await _connections.SendAsync(client,
            new WsMessage("echo", $"Echo channel open. Your ID: {client.Id}", null, "SERVER"));

        await foreach (var msg in ReadMessagesAsync(client))
        {
            _logger.LogInformation("[ECHO] {Id} sent: {Payload}", client.Id, msg.Payload);

            // Mirror back exactly what we received — architect's ack pattern
            await _connections.SendAsync(client,
                new WsMessage("echo", $"ECHO: {msg.Payload}", null, "SERVER"));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // USE-CASE 2 — BROADCAST (one message → all connected clients)
    //
    // Architect pattern: the publisher opens ConnectionCount=50 sockets
    // simultaneously. Here the SERVER fans out to all connected sockets.
    // mirrors Task.WhenAll(connectionTasks) on the server side.
    // ════════════════════════════════════════════════════════════════════════
    private async Task HandleBroadcastAsync(ConnectedClient client)
    {
        await _connections.SendAsync(client,
            new WsMessage("broadcast",
                $"Broadcast channel open. You are {client.Id}. " +
                $"Total clients: {_connections.Count}", null, "SERVER"));

        await foreach (var msg in ReadMessagesAsync(client))
        {
            _logger.LogInformation("[BROADCAST] {Id}: {Payload}", client.Id, msg.Payload);

            // Fan out to every other connected client
            await _connections.BroadcastAsync(
                new WsMessage("broadcast", msg.Payload, null, client.Id),
                excludeId: client.Id);

            // Confirm back to sender how many received it
            int peers = _connections.Count - 1;
            await _connections.SendAsync(client,
                new WsMessage("broadcast",
                    $"Delivered to {peers} peer(s).", null, "SERVER"));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // USE-CASE 3 — LIVE SERVER PUSH / TELEMETRY
    //
    // Architect pattern: publisher continuously sends; this is the server-side
    // equivalent — the server pushes without the client asking.
    // Client sends {"Type":"subscribe"} to opt in.
    // ════════════════════════════════════════════════════════════════════════
    private async Task HandleTelemetryAsync(ConnectedClient client)
    {
        await _connections.SendAsync(client,
            new WsMessage("telemetry",
                "Send {\"Type\":\"subscribe\"} to start receiving sensor data.", null, "SERVER"));

        await foreach (var msg in ReadMessagesAsync(client))
        {
            if (msg.Type == "subscribe")
            {
                _telemetry.Subscribe(client.Id);
                await _connections.SendAsync(client,
                    new WsMessage("telemetry", "Subscribed. Readings will arrive every 1s.",
                        null, "SERVER"));
            }
            else if (msg.Type == "unsubscribe")
            {
                _telemetry.Unsubscribe(client.Id);
                await _connections.SendAsync(client,
                    new WsMessage("telemetry", "Unsubscribed from telemetry feed.",
                        null, "SERVER"));
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // USE-CASE 4 — CHAT ROOM (grouped broadcast by room name)
    //
    // Extension of broadcast: only peers in the same "room" receive messages.
    // Architect pattern: ConnectionCount connections scoped to one target IP/port.
    // Here: a room name segments the connection pool.
    // ════════════════════════════════════════════════════════════════════════
    private async Task HandleChatAsync(ConnectedClient client, string room)
    {
        client.Room = room;

        // Notify everyone in the room that a new peer arrived
        await _connections.BroadcastToRoomAsync(room,
            new WsMessage("chat", $"{client.Id} joined #{room}", room, "SERVER"));

        await foreach (var msg in ReadMessagesAsync(client))
        {
            _logger.LogInformation("[CHAT:{Room}] {Id}: {Payload}", room, client.Id, msg.Payload);

            // Fan out only within the room
            await _connections.BroadcastToRoomAsync(room,
                new WsMessage("chat", msg.Payload, room, client.Id));
        }

        // Notify room on departure
        await _connections.BroadcastToRoomAsync(room,
            new WsMessage("chat", $"{client.Id} left #{room}", room, "SERVER"));
    }

    // ════════════════════════════════════════════════════════════════════════
    // USE-CASE 5 — BINARY DATA TRANSFER
    //
    // Architect pattern: byte[] buffer = new byte[1024]; stream.ReadAsync(buffer…)
    // WebSocket binary frames are the direct equivalent.
    // Client sends raw binary; server echoes with stats, then sends a binary reply.
    // ════════════════════════════════════════════════════════════════════════
    private async Task HandleBinaryAsync(ConnectedClient client)
    {
        await _connections.SendAsync(client,
            new WsMessage("binary-ack",
                "Binary channel open. Send any binary frame to test raw byte transfer.",
                null, "SERVER"));

        var buffer = new byte[64 * 1024]; // 64 KB receive buffer (architect used 1024)

        while (client.Socket.State == WebSocketState.Open)
        {
            var result = await client.Socket.ReceiveAsync(
                new ArraySegment<byte>(buffer), client.Cts.Token);

            if (result.MessageType == WebSocketMessageType.Close)
            {
                await client.Socket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure, "Bye", CancellationToken.None);
                break;
            }

            if (result.MessageType == WebSocketMessageType.Binary)
            {
                int receivedBytes = result.Count;
                _logger.LogInformation("[BINARY] {Id} sent {Bytes} bytes", client.Id, receivedBytes);

                // Acknowledge via text (metadata about what arrived)
                await _connections.SendAsync(client,
                    new WsMessage("binary-ack",
                        $"Received {receivedBytes} bytes. Echoing back as binary.",
                        null, "SERVER"));

                // Echo the raw bytes back — mirrors architect's responseData write
                var echoPayload = buffer[..receivedBytes];
                await _connections.SendBinaryAsync(client, echoPayload);
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // USE-CASE 6 — HEARTBEAT / PING-PONG (keep-alive)
    //
    // Architect pattern: CancellationTokenSource + OperationCanceledException
    // for graceful shutdown. Here: server sends a PING every 5 s; client must
    // reply PONG within 10 s or the server closes the connection.
    // ════════════════════════════════════════════════════════════════════════
    private async Task HandleHeartbeatAsync(ConnectedClient client)
    {
        await _connections.SendAsync(client,
            new WsMessage("ping",
                "Heartbeat channel open. Reply {\"Type\":\"pong\"} to each ping.",
                null, "SERVER"));

        // Ping loop runs concurrently with the read loop
        // mirrors: _ = Task.Run(() => HandleClientAsync(client)) — fire-and-forget background work
        var pingTask = PingLoopAsync(client);
        var readTask = ReadAndHandlePongsAsync(client);

        // Wait for whichever finishes first (disconnect or error)
        await Task.WhenAny(pingTask, readTask);
        client.Cts.Cancel(); // signal the other loop to stop
    }

    private async Task PingLoopAsync(ConnectedClient client)
    {
        int seq = 0;
        try
        {
            while (!client.Cts.IsCancellationRequested
                   && client.Socket.State == WebSocketState.Open)
            {
                await Task.Delay(5000, client.Cts.Token); // send ping every 5s

                await _connections.SendAsync(client,
                    new WsMessage("ping", $"PING seq={++seq}", null, "SERVER"),
                    client.Cts.Token);

                _logger.LogDebug("[HEARTBEAT] Sent PING seq={Seq} to {Id}", seq, client.Id);
            }
        }
        catch (OperationCanceledException)
        {
            // mirrors architect's: catch (OperationCanceledException) { "Disconnecting via cancellation" }
            _logger.LogInformation("[HEARTBEAT] Ping loop cancelled for {Id}", client.Id);
        }
    }

    private async Task ReadAndHandlePongsAsync(ConnectedClient client)
    {
        await foreach (var msg in ReadMessagesAsync(client))
        {
            if (msg.Type == "pong")
            {
                _logger.LogDebug("[HEARTBEAT] PONG received from {Id}", client.Id);
                await _connections.SendAsync(client,
                    new WsMessage("pong", "Heartbeat OK", null, "SERVER"));
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // SHARED: Async message reader (IAsyncEnumerable)
    //
    // This is the WebSocket equivalent of:
    //   while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
    // The architect's inner read loop, lifted into a reusable async stream.
    // ════════════════════════════════════════════════════════════════════════
    private async IAsyncEnumerable<WsMessage> ReadMessagesAsync(ConnectedClient client)
    {
        var buffer = new byte[4096];

        while (client.Socket.State == WebSocketState.Open
               && !client.Cts.IsCancellationRequested)
        {
            WebSocketReceiveResult result;
            try
            {
                result = await client.Socket.ReceiveAsync(
                    new ArraySegment<byte>(buffer), client.Cts.Token);
            }
            catch (OperationCanceledException) { yield break; }
            catch (WebSocketException) { yield break; }

            // Client sent a close frame — mirror architect's "bytesRead == 0" check
            if (result.MessageType == WebSocketMessageType.Close)
            {
                if (client.Socket.State == WebSocketState.CloseReceived)
                    await client.Socket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure, "Goodbye", CancellationToken.None);
                yield break;
            }

            if (result.MessageType != WebSocketMessageType.Text) continue;

            var raw = Encoding.UTF8.GetString(buffer, 0, result.Count);

            WsMessage? msg;
            try
            {
                msg = JsonSerializer.Deserialize<WsMessage>(raw,
                        new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            }
            catch { msg = new WsMessage("text", raw, null, client.Id); } // raw text fallback

            if (msg is not null) yield return msg;
        }
    }
}
