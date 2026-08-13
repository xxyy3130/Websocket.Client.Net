using System.Net.WebSockets;
using System.Text;

namespace Websocket.Client.Net;

public sealed class OpenEventArgs(bool isReconnect) : EventArgs
{
    public bool IsReconnect { get; } = isReconnect;
}

public sealed class MessageEventArgs : EventArgs
{
    private string? _text;

    internal MessageEventArgs(byte[] data, WebSocketMessageType messageType)
    {
        Data = data;
        MessageType = messageType;
    }

    /// <summary>A stable message copy that may safely be retained after the event returns.</summary>
    public ReadOnlyMemory<byte> Data { get; }

    public WebSocketMessageType MessageType { get; }

    public bool IsText => MessageType == WebSocketMessageType.Text;

    public bool IsBinary => MessageType == WebSocketMessageType.Binary;

    /// <summary>UTF-8 text, decoded lazily. Throws when this is not a text message.</summary>
    public string Text => IsText
        ? _text ??= Encoding.UTF8.GetString(Data.Span)
        : throw new InvalidOperationException("The message is not text.");
}

public sealed class ErrorEventArgs(
    Exception exception,
    string operation,
    bool willReconnect) : EventArgs
{
    public Exception Exception { get; } = exception;

    public string Message => Exception.Message;

    public string Operation { get; } = operation;

    public bool WillReconnect { get; } = willReconnect;
}

public sealed class CloseEventArgs(
    WebSocketCloseStatus? code,
    string? reason,
    bool wasClean,
    bool willReconnect) : EventArgs
{
    public WebSocketCloseStatus? Code { get; } = code;

    public string Reason { get; } = reason ?? string.Empty;

    public bool WasClean { get; } = wasClean;

    public bool WillReconnect { get; } = willReconnect;
}

public sealed class ReconnectingEventArgs(
    int attempt,
    int maxAttempts,
    TimeSpan delay,
    Exception? cause) : EventArgs
{
    /// <summary>One-based reconnect attempt number.</summary>
    public int Attempt { get; } = attempt;

    /// <summary>Configured maximum, or -1 for unlimited.</summary>
    public int MaxAttempts { get; } = maxAttempts;

    public TimeSpan Delay { get; } = delay;

    public Exception? Cause { get; } = cause;
}

/// <summary>
/// Concurrent allocation-free receive callback. The message memory is valid only until the returned
/// ValueTask completes. Separate invocations may overlap and complete out of order.
/// </summary>
public delegate ValueTask AsyncMessageHandler(
    WebSocketClient sender,
    ReadOnlyMemory<byte> message,
    WebSocketMessageType messageType,
    CancellationToken cancellationToken);
