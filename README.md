# Websocket.Client.Net

面向 .NET 8 的原生 WebSocket Client 类库。仅使用 BCL 的 `ClientWebSocket`，没有第三方运行时依赖；原生支持 `ws://`、`wss://`、Header、Cookie、事件通知与自动重连。

## 快速使用

```csharp
using Websocket.Client.Net;

await using var ws = new WebSocketClient("wss://example.com/ws", new WebSocketClientOptions
{
    AutoReconnect = true,
    MaxReconnectAttempts = 10,             // -1 表示无限重试
    ReconnectDelay = TimeSpan.FromSeconds(1)
});

ws.SetHeader("Authorization", "Bearer token");
ws.SetHeader("User-Agent", "my-service/1.0");
ws.SetCookie("session", "cookie-value");

ws.OnOpen += (sender, e) =>
{
    Console.WriteLine(e.IsReconnect ? "重连成功" : "连接成功");
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
    Console.WriteLine($"第 {e.Attempt} 次重连，等待 {e.Delay}");
};

await ws.ConnectAsync();
await ws.SendAsync("hello");
await ws.DisconnectAsync(reason: "shutdown");
```

二进制发送提供明确重载，全部支持 `CancellationToken`：

```csharp
await ws.SendAsync(byteArray, cancellationToken: cancellationToken);
await ws.SendAsync(arraySegment, cancellationToken: cancellationToken);
await ws.SendAsync(readOnlySequence, cancellationToken: cancellationToken);
```

`ConnectAsync(cancellationToken)` 的 Token 取消当前调用者的等待，不会取消其他调用者共享的连接或重连流程。发送 Token 若在原生发送开始后取消，当前物理连接会中止，避免后续消息接在不完整的 WebSocket 消息后面；启用自动重连时会建立干净的新连接。

## 吞吐优先的零拷贝接收

`OnMessage` 为了保证事件返回后数据仍然有效，会为每条完整消息创建一份稳定的 `byte[]`。吞吐敏感路径可改用 `MessageReceivedAsync`；传入的内存来自池，只能在回调完成前使用：

```csharp
ws.MessageReceivedAsync = async (sender, message, type, cancellationToken) =>
{
    await processor.ProcessAsync(message, cancellationToken);
    // 不要把 message 保存到回调之外；需要保留时请调用 message.ToArray()。
};
```
