# Websocket.Client.Net

A native WebSocket client library for .NET 8. It uses only the BCL `ClientWebSocket` with no third-party runtime dependencies, and provides built-in support for `ws://`, `wss://`, headers, cookies, event notifications, and automatic reconnection.

## Quick Start

```csharp
using Websocket.Client.Net;

await using var ws = new WebSocketClient("wss://example.com/ws", new WebSocketClientOptions
{
    AutoReconnect = true,
    MaxReconnectAttempts = 10,             // -1 means unlimited retries
    ReconnectDelay = TimeSpan.FromSeconds(1)
});

ws.SetHeader("Authorization", "Bearer token");
ws.SetHeader("User-Agent", "my-service/1.0");
ws.SetCookie("session", "cookie-value");

ws.OnOpen += (sender, e) =>
{
    Console.WriteLine(e.IsReconnect ? "Reconnected" : "Connected");
};

ws.OnMessage += (sender, e) =>
{
    if (e.IsText)
        Console.WriteLine(e.Text);
    else
        Console.WriteLine($"binary: {e.Data.Length} bytes");
};

ws.OnError += (sender, e) =>
{
    Console.WriteLine($"{e.Operation}: {e.Message}; willReconnect={e.WillReconnect}");
};

ws.OnClose += (sender, e) =>
{
    Console.WriteLine($"{e.Code}: {e.Reason}; clean={e.WasClean}");
};

ws.OnReconnecting += (sender, e) =>
{
    Console.WriteLine($"Reconnect attempt {e.Attempt}; waiting {e.Delay}");
};

await ws.ConnectAsync();
await ws.SendAsync("hello");
await ws.DisconnectAsync(reason: "shutdown");
```

Binary sending provides explicit overloads, all of which support `CancellationToken`:

```csharp
await ws.SendAsync(byteArray, cancellationToken: cancellationToken);
await ws.SendAsync(arraySegment, cancellationToken: cancellationToken);
await ws.SendAsync(readOnlySequence, cancellationToken: cancellationToken);
```

The token passed to `ConnectAsync(cancellationToken)` cancels only the current caller's wait; it does not cancel the connection or reconnection flow shared by other callers. If a send token is canceled after a native send has started, the current physical connection is aborted so that later messages cannot follow an incomplete WebSocket message. When automatic reconnection is enabled, a clean new connection is established.

## Throughput-First Zero-Copy Receiving

To keep data valid after an event handler returns, `OnMessage` creates a stable `byte[]` for each complete message. Throughput-sensitive paths can use `MessageReceivedAsync` instead. The supplied memory comes from a pool and may only be used until the callback completes:

```csharp
ws.MessageReceivedAsync = async (sender, message, type, cancellationToken) =>
{
    await processor.ProcessAsync(message, cancellationToken);
    // Do not retain message beyond the callback; call message.ToArray() if needed.
};
```
