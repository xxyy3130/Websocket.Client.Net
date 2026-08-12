using System.Net;
using System.Net.Security;
using System.Net.WebSockets;

namespace Websocket.Client.Net;

/// <summary>Connection, receive, and automatic reconnect settings.</summary>
public sealed class WebSocketClientOptions
{
    /// <summary>Size of the pooled receive buffer. Default: 16 KiB.</summary>
    public int ReceiveBufferSize { get; init; } = 16 * 1024;

    /// <summary>Maximum reassembled message size. Default: 4 MiB.</summary>
    public long MaxMessageSize { get; init; } = 4L * 1024 * 1024;

    /// <summary>Timeout for one connection attempt. Use <see cref="Timeout.InfiniteTimeSpan"/> to disable.</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>Maximum time to wait for a graceful close handshake.</summary>
    public TimeSpan CloseTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>WebSocket keep-alive interval.</summary>
    public TimeSpan KeepAliveInterval { get; init; } = TimeSpan.FromSeconds(20);

    /// <summary>Whether failed initial connections and abnormal disconnects are retried.</summary>
    public bool AutoReconnect { get; init; } = true;

    /// <summary>Number of reconnect attempts. Zero disables retries; -1 means unlimited.</summary>
    public int MaxReconnectAttempts { get; init; } = 5;

    /// <summary>Fixed delay before every reconnect attempt.</summary>
    public TimeSpan ReconnectDelay { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Reconnect after a peer sends NormalClosure. Abnormal closes always follow AutoReconnect.</summary>
    public bool ReconnectOnNormalClosure { get; init; }

    /// <summary>Request headers copied to every new connection.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>Cookie container used by every new connection.</summary>
    public CookieContainer? Cookies { get; init; }

    /// <summary>Subprotocols offered in order.</summary>
    public IReadOnlyList<string>? SubProtocols { get; init; }

    /// <summary>Optional server-certificate validator for wss. Null uses normal platform validation.</summary>
    public RemoteCertificateValidationCallback? ServerCertificateValidationCallback { get; init; }

    /// <summary>Last-mile configuration hook invoked for every newly created native socket.</summary>
    public Action<ClientWebSocketOptions>? ConfigureNativeOptions { get; init; }

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(ReceiveBufferSize, 256);
        if (ReceiveBufferSize > Array.MaxLength)
            throw new ArgumentOutOfRangeException(nameof(ReceiveBufferSize), $"The maximum supported value is {Array.MaxLength}.");
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxMessageSize, 1);
        if (MaxMessageSize > Array.MaxLength)
            throw new ArgumentOutOfRangeException(nameof(MaxMessageSize), $"The maximum supported value is {Array.MaxLength}.");

        ValidateTimeout(ConnectTimeout, nameof(ConnectTimeout), allowInfinite: true);
        ValidateTimeout(CloseTimeout, nameof(CloseTimeout), allowInfinite: false);
        ValidateTimeout(KeepAliveInterval, nameof(KeepAliveInterval), allowInfinite: true, allowZero: true);
        ValidateTimeout(ReconnectDelay, nameof(ReconnectDelay), allowInfinite: false, allowZero: true);

        if (MaxReconnectAttempts < -1)
            throw new ArgumentOutOfRangeException(nameof(MaxReconnectAttempts), "Use -1 for unlimited retries.");
    }

    private static void ValidateTimeout(
        TimeSpan value,
        string name,
        bool allowInfinite,
        bool allowZero = false)
    {
        if (allowInfinite && value == Timeout.InfiniteTimeSpan)
            return;
        if ((allowZero && value == TimeSpan.Zero) || value > TimeSpan.Zero)
            return;

        throw new ArgumentOutOfRangeException(name);
    }
}
