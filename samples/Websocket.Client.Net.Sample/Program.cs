using Websocket.Client.Net;

if (args.Length == 0)
{
    Console.WriteLine("Usage: dotnet run --project samples/Websocket.Client.Net.Sample -- ws://localhost:8080/ws");
    return;
}

await using var ws = new WebSocketClient(args[0], new WebSocketClientOptions
{
    AutoReconnect = true,
    MaxReconnectAttempts = 10,
    ReconnectDelay = TimeSpan.FromSeconds(1)
});

ws.SetHeader("X-Client", "Websocket.Client.Net.Sample");
ws.SetCookie("session", "replace-me");

ws.OnOpen += (_, e) =>
    Console.WriteLine(e.IsReconnect ? "重新连接成功" : "连接成功");

ws.OnMessage += (_, e) =>
    Console.WriteLine(e.IsText ? $"收到文本: {e.Text}" : $"收到二进制: {e.Data.Length} bytes");

ws.OnError += (_, e) =>
    Console.WriteLine($"{e.Operation} 错误: {e.Message}; 将重连: {e.WillReconnect}");

ws.OnClose += (_, e) =>
    Console.WriteLine($"连接关闭: {e.Code}, {e.Reason}; 将重连: {e.WillReconnect}");

ws.OnReconnecting += (_, e) =>
    Console.WriteLine($"{e.Delay.TotalMilliseconds:N0}ms 后执行第 {e.Attempt} 次重连");

using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    shutdown.Cancel();
};

await ws.ConnectAsync(shutdown.Token);
await ws.SendAsync("hello from .NET 8", shutdown.Token);

try
{
    await Task.Delay(Timeout.InfiniteTimeSpan, shutdown.Token);
}
catch (OperationCanceledException)
{
}

await ws.DisconnectAsync(reason: "sample stopped");
