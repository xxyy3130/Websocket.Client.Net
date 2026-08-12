namespace Websocket.Client.Net;

public sealed class WebSocketMessageTooBigException(long maximumBytes)
    : Exception($"The WebSocket message exceeded the configured {maximumBytes} byte limit.")
{
    public long MaximumBytes { get; } = maximumBytes;
}
