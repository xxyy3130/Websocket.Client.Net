namespace Websocket.Client.Net;

/// <summary>Represents the lifecycle state of a <see cref="WebSocketClient"/>.</summary>
public enum WebSocketClientState
{
    Disconnected,
    Connecting,
    Open,
    Reconnecting,
    Closing,
    Disposed
}
