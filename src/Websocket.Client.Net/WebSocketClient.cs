using System.Buffers;
using System.Net;
using System.Net.WebSockets;
using System.Text;

namespace Websocket.Client.Net;

/// <summary>
/// A dependency-free .NET 8 WebSocket client with event-based messages and automatic reconnect.
/// One receive and one send may run concurrently; concurrent sends are serialized internally.
/// </summary>
public sealed class WebSocketClient : IDisposable, IAsyncDisposable
{
    private readonly Uri _uri;
    private readonly WebSocketClientOptions _options;
    private readonly Dictionary<string, string> _headers;
    private readonly Dictionary<string, Cookie> _cookies = new(StringComparer.Ordinal);
    private readonly string[] _subProtocols;
    private readonly object _sync = new();
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private readonly AsyncLocal<bool> _insideUserCallback = new();

    private ClientWebSocket? _socket;
    private CancellationTokenSource? _lifetimeCts;
    private Task? _lifecycleTask;
    private TaskCompletionSource<bool> _openSignal = NewSignal();
    private int _state = (int)WebSocketClientState.Disconnected;
    private volatile bool _manualStop;
    private WebSocketCloseStatus _requestedCloseStatus = WebSocketCloseStatus.NormalClosure;
    private string? _requestedCloseReason;
    private int _disposed;

    public WebSocketClient(string url, WebSocketClientOptions? options = null)
        : this(new Uri(url ?? throw new ArgumentNullException(nameof(url))), options)
    {
    }

    public WebSocketClient(Uri uri, WebSocketClientOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(uri);
        if (!uri.IsAbsoluteUri || (uri.Scheme != Uri.UriSchemeWs && uri.Scheme != Uri.UriSchemeWss))
            throw new ArgumentException("The URI must be absolute and use ws:// or wss://.", nameof(uri));
        if (!string.IsNullOrEmpty(uri.Fragment))
            throw new ArgumentException("WebSocket URIs cannot contain a fragment.", nameof(uri));

        _options = options ?? new WebSocketClientOptions();
        _options.Validate();
        _uri = uri;
        _headers = _options.Headers is null
            ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, string>(_options.Headers, StringComparer.OrdinalIgnoreCase);
        _subProtocols = _options.SubProtocols?.ToArray() ?? [];
    }

    public event EventHandler<OpenEventArgs>? OnOpen;

    public event EventHandler<MessageEventArgs>? OnMessage;

    public event EventHandler<ErrorEventArgs>? OnError;

    public event EventHandler<CloseEventArgs>? OnClose;

    public event EventHandler<ReconnectingEventArgs>? OnReconnecting;

    /// <summary>
    /// Optional zero-copy alternative to OnMessage. The memory is pooled and valid only until
    /// the returned ValueTask completes. If OnMessage is also subscribed, its stable copy is still created.
    /// </summary>
    public AsyncMessageHandler? MessageReceivedAsync { get; set; }

    public Uri Uri => _uri;

    public WebSocketClientState State => (WebSocketClientState)Volatile.Read(ref _state);

    public bool IsAlive
    {
        get
        {
            lock (_sync)
                return State == WebSocketClientState.Open && _socket?.State == WebSocketState.Open;
        }
    }

    /// <summary>Adds or replaces a header for the next connection/reconnection handshake.</summary>
    public void SetHeader(string name, string value)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(value);
        lock (_sync)
            _headers[name] = value;
    }

    public bool RemoveHeader(string name)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        lock (_sync)
            return _headers.Remove(name);
    }

    /// <summary>Adds a cookie scoped to this WebSocket endpoint.</summary>
    public void SetCookie(string name, string value, string path = "/")
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        lock (_sync)
            _cookies[name] = new Cookie(name, value, path);
    }

    public void SetCookie(Cookie cookie)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(cookie);
        lock (_sync)
            _cookies[cookie.Name] = cookie;
    }

    /// <summary>Connects or waits for an in-progress initial/reconnect attempt to succeed.</summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        while (true)
        {
            Task? lifecycleToFinish = null;
            Task? openTask = null;
            lock (_sync)
            {
                ThrowIfDisposed();
                if (State == WebSocketClientState.Open && _socket?.State == WebSocketState.Open)
                    return;

                if (_lifecycleTask is null || _lifecycleTask.IsCompleted)
                {
                    _manualStop = false;
                    _requestedCloseReason = null;
                    _openSignal = NewSignal();
                    _lifetimeCts = new CancellationTokenSource();
                    SetState(WebSocketClientState.Connecting);
                    _lifecycleTask = RunLifecycleAsync(_lifetimeCts.Token);
                }
                else if (_manualStop || State is WebSocketClientState.Disconnected or WebSocketClientState.Closing)
                {
                    // A previous manual/terminal shutdown is still unwinding. Waiting for it avoids
                    // overlapping physical sockets, then this method starts one fresh lifecycle.
                    lifecycleToFinish = _lifecycleTask;
                }
                else if (_openSignal.Task.IsCompleted)
                {
                    // The previous connection may have dropped before the lifecycle has observed it.
                    // Every concurrent caller now waits for the same next successful connection.
                    _openSignal = NewSignal();
                }

                openTask = _openSignal.Task;
            }

            if (lifecycleToFinish is not null)
            {
                await lifecycleToFinish.WaitAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            await openTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }
    }

    /// <summary>Synchronous compatibility wrapper. Prefer ConnectAsync on server paths.</summary>
    public void Connect(CancellationToken cancellationToken = default) =>
        ConnectAsync(cancellationToken).GetAwaiter().GetResult();

    public ValueTask SendAsync(string text, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(text);
        return SendTextCoreAsync(text, cancellationToken);
    }

    public ValueTask SendAsync(
        ReadOnlyMemory<byte> data,
        WebSocketMessageType messageType = WebSocketMessageType.Binary,
        bool endOfMessage = true,
        CancellationToken cancellationToken = default)
    {
        ValidateMessageType(messageType);
        return SendCoreAsync(data, messageType, endOfMessage, cancellationToken);
    }

    /// <summary>Sends an array as one text or binary message.</summary>
    public ValueTask SendAsync(
        byte[] data,
        WebSocketMessageType messageType = WebSocketMessageType.Binary,
        bool endOfMessage = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(data);
        ValidateMessageType(messageType);
        return SendCoreAsync(data, messageType, endOfMessage, cancellationToken);
    }

    /// <summary>Sends an array segment as one text or binary message.</summary>
    public ValueTask SendAsync(
        ArraySegment<byte> data,
        WebSocketMessageType messageType = WebSocketMessageType.Binary,
        bool endOfMessage = true,
        CancellationToken cancellationToken = default)
    {
        ValidateMessageType(messageType);
        var memory = data.Array is null
            ? ReadOnlyMemory<byte>.Empty
            : new ReadOnlyMemory<byte>(data.Array, data.Offset, data.Count);
        return SendCoreAsync(memory, messageType, endOfMessage, cancellationToken);
    }

    /// <summary>
    /// Sends all segments as one message while holding the send gate for the entire sequence,
    /// preventing fragments from being interleaved with concurrent messages.
    /// </summary>
    public ValueTask SendAsync(
        ReadOnlySequence<byte> data,
        WebSocketMessageType messageType = WebSocketMessageType.Binary,
        bool endOfMessage = true,
        CancellationToken cancellationToken = default)
    {
        ValidateMessageType(messageType);
        return data.IsSingleSegment
            ? SendCoreAsync(data.First, messageType, endOfMessage, cancellationToken)
            : SendSequenceCoreAsync(data, messageType, endOfMessage, cancellationToken);
    }

    public async Task DisconnectAsync(
        WebSocketCloseStatus closeStatus = WebSocketCloseStatus.NormalClosure,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ValidateCloseReason(reason);
        await StopCoreAsync(closeStatus, reason, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Alias for DisconnectAsync.</summary>
    public Task CloseAsync(
        WebSocketCloseStatus closeStatus = WebSocketCloseStatus.NormalClosure,
        string? reason = null,
        CancellationToken cancellationToken = default) =>
        DisconnectAsync(closeStatus, reason, cancellationToken);

    /// <summary>Synchronous compatibility wrapper. Prefer DisconnectAsync on server paths.</summary>
    public void Close(WebSocketCloseStatus closeStatus = WebSocketCloseStatus.NormalClosure, string? reason = null) =>
        DisconnectAsync(closeStatus, reason).GetAwaiter().GetResult();

    /// <summary>Immediately terminates the current socket and suppresses automatic reconnect.</summary>
    public void Abort()
    {
        ThrowIfDisposed();
        ClientWebSocket? socket;
        CancellationTokenSource? lifetime;
        lock (_sync)
        {
            _manualStop = true;
            socket = _socket;
            lifetime = _lifetimeCts;
            if (State != WebSocketClientState.Disconnected)
                SetState(WebSocketClientState.Closing);
        }

        socket?.Abort();
        TryCancel(lifetime);
    }

    private async Task RunLifecycleAsync(CancellationToken lifetimeToken)
    {
        // Ensures ConnectAsync stores _lifecycleTask before any finally block can clear it.
        await Task.Yield();

        var reconnectAttempt = 0;
        Exception? reconnectCause = null;
        Exception? terminalError = null;

        try
        {
            while (!lifetimeToken.IsCancellationRequested)
            {
                if (reconnectAttempt > 0)
                {
                    var delay = _options.ReconnectDelay;
                    SetState(WebSocketClientState.Reconnecting);
                    RaiseReconnecting(reconnectAttempt, delay, reconnectCause);
                    await Task.Delay(delay, lifetimeToken).ConfigureAwait(false);
                }

                SetState(WebSocketClientState.Connecting);
                ClientWebSocket? socket = null;
                var opened = false;

                try
                {
                    socket = CreateSocket();
                    lock (_sync)
                    {
                        if (_manualStop || lifetimeToken.IsCancellationRequested)
                            break;
                        _socket = socket;
                    }

                    await ConnectSocketAsync(
                        socket,
                        lifetimeToken).ConfigureAwait(false);

                    opened = true;
                    var wasReconnect = reconnectAttempt > 0;
                    reconnectAttempt = 0;
                    reconnectCause = null;
                    SetState(WebSocketClientState.Open);
                    _openSignal.TrySetResult(true);
                    RaiseOpen(wasReconnect);

                    var closeInfo = await ReceiveLoopAsync(socket, lifetimeToken).ConfigureAwait(false);
                    var willReconnect = ShouldReconnect(closeInfo) && CanReconnect(1);
                    if (willReconnect)
                        PrepareReconnect();
                    else
                        SetState(WebSocketClientState.Disconnected);

                    RaiseClose(closeInfo.Code, closeInfo.Reason, closeInfo.WasClean, willReconnect);
                    if (!willReconnect)
                        break;

                    reconnectAttempt = 1;
                }
                catch (OperationCanceledException) when (lifetimeToken.IsCancellationRequested)
                {
                    if (opened)
                    {
                        SetState(WebSocketClientState.Disconnected);
                        RaiseClose(_requestedCloseStatus, _requestedCloseReason, wasClean: false, willReconnect: false);
                    }
                    break;
                }
                catch (Exception exception)
                {
                    var nextAttempt = opened ? 1 : reconnectAttempt + 1;
                    var willReconnect = !_manualStop && CanReconnect(nextAttempt);
                    RaiseError(exception, opened ? "Receive" : "Connect", willReconnect);

                    if (opened)
                    {
                        if (willReconnect)
                            PrepareReconnect();
                        else
                            SetState(WebSocketClientState.Disconnected);
                        RaiseClose(null, exception.Message, wasClean: false, willReconnect);
                    }

                    if (!willReconnect)
                    {
                        terminalError = exception;
                        break;
                    }

                    reconnectCause = exception;
                    reconnectAttempt = nextAttempt;
                }
                finally
                {
                    if (socket is not null)
                    {
                        lock (_sync)
                        {
                            if (ReferenceEquals(_socket, socket))
                                _socket = null;
                        }
                        socket.Dispose();
                    }
                }
            }
        }
        catch (OperationCanceledException) when (lifetimeToken.IsCancellationRequested)
        {
            // Cancellation is the normal exit path for Abort and a timed-out close handshake.
        }
        finally
        {
            TaskCompletionSource<bool> terminalSignal;
            lock (_sync)
            {
                terminalSignal = _openSignal;
                _socket = null;
                _lifecycleTask = null;
                _lifetimeCts?.Dispose();
                _lifetimeCts = null;
                if (Volatile.Read(ref _disposed) == 0)
                    SetState(WebSocketClientState.Disconnected);
            }

            // Complete the signal owned by this lifecycle. A concurrent ConnectAsync may already
            // have started a new lifecycle with a different signal after the lock was released.
            if (!terminalSignal.Task.IsCompleted)
            {
                if (terminalError is not null)
                    terminalSignal.TrySetException(terminalError);
                else
                    terminalSignal.TrySetCanceled(lifetimeToken);
            }
        }
    }

    private ClientWebSocket CreateSocket()
    {
        var socket = new ClientWebSocket();
        try
        {
            var native = socket.Options;
            native.KeepAliveInterval = _options.KeepAliveInterval;

            lock (_sync)
            {
                if (_options.Cookies is not null)
                    native.Cookies = _options.Cookies;
                foreach (var header in _headers)
                {
                    if (!header.Key.Equals("Cookie", StringComparison.OrdinalIgnoreCase))
                        native.SetRequestHeader(header.Key, header.Value);
                }

                var cookieHeader = BuildCookieHeader();
                if (_headers.TryGetValue("Cookie", out var explicitCookieHeader))
                    cookieHeader = string.IsNullOrEmpty(cookieHeader)
                        ? explicitCookieHeader
                        : $"{explicitCookieHeader}; {cookieHeader}";
                if (!string.IsNullOrEmpty(cookieHeader))
                    native.SetRequestHeader("Cookie", cookieHeader);
            }

            foreach (var protocol in _subProtocols)
                native.AddSubProtocol(protocol);

            if (_options.ServerCertificateValidationCallback is not null)
                native.RemoteCertificateValidationCallback = _options.ServerCertificateValidationCallback;

            _options.ConfigureNativeOptions?.Invoke(native);
            return socket;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private async Task ConnectSocketAsync(
        ClientWebSocket socket,
        CancellationToken lifetimeToken)
    {
        using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(lifetimeToken);
        if (_options.ConnectTimeout != Timeout.InfiniteTimeSpan)
            connectCts.CancelAfter(_options.ConnectTimeout);

        try
        {
            await socket.ConnectAsync(_uri, connectCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (
            !lifetimeToken.IsCancellationRequested &&
            connectCts.IsCancellationRequested)
        {
            throw new TimeoutException($"Connecting to {_uri} timed out after {_options.ConnectTimeout}.");
        }
    }

    private async Task<CloseInfo> ReceiveLoopAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        var receiveBuffer = ArrayPool<byte>.Shared.Rent(_options.ReceiveBufferSize);
        PooledMessageBuffer? messageBuffer = null;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await socket.ReceiveAsync(
                    receiveBuffer.AsMemory(0, _options.ReceiveBufferSize),
                    cancellationToken).ConfigureAwait(false);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    var code = socket.CloseStatus;
                    var reason = socket.CloseStatusDescription;
                    await ReplyToCloseAsync(socket, code, reason, cancellationToken).ConfigureAwait(false);
                    return new CloseInfo(code, reason, WasClean: true);
                }

                if (messageBuffer is null && result.EndOfMessage)
                {
                    if (result.Count > _options.MaxMessageSize)
                    {
                        await TryCloseOutputAsync(
                            socket,
                            WebSocketCloseStatus.MessageTooBig,
                            "Message exceeds configured limit",
                            cancellationToken).ConfigureAwait(false);
                        throw new WebSocketMessageTooBigException(_options.MaxMessageSize);
                    }

                    await DispatchMessageAsync(
                        receiveBuffer.AsMemory(0, result.Count),
                        result.MessageType,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                messageBuffer ??= new PooledMessageBuffer(
                    Math.Min(_options.ReceiveBufferSize, checked((int)_options.MaxMessageSize)),
                    checked((int)_options.MaxMessageSize));

                try
                {
                    messageBuffer.Append(receiveBuffer.AsSpan(0, result.Count));
                }
                catch (WebSocketMessageTooBigException)
                {
                    await TryCloseOutputAsync(
                        socket,
                        WebSocketCloseStatus.MessageTooBig,
                        "Message exceeds configured limit",
                        cancellationToken).ConfigureAwait(false);
                    throw;
                }

                if (!result.EndOfMessage)
                    continue;

                await DispatchMessageAsync(
                    messageBuffer.WrittenMemory,
                    result.MessageType,
                    cancellationToken).ConfigureAwait(false);
                messageBuffer.Dispose();
                messageBuffer = null;
            }

            throw new OperationCanceledException(cancellationToken);
        }
        finally
        {
            messageBuffer?.Dispose();
            ArrayPool<byte>.Shared.Return(receiveBuffer);
        }
    }

    private async ValueTask DispatchMessageAsync(
        ReadOnlyMemory<byte> message,
        WebSocketMessageType messageType,
        CancellationToken cancellationToken)
    {
        var asyncHandler = MessageReceivedAsync;
        if (asyncHandler is not null)
        {
            var previous = _insideUserCallback.Value;
            _insideUserCallback.Value = true;
            try
            {
                await asyncHandler(this, message, messageType, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                RaiseError(exception, "MessageReceivedAsync handler", willReconnect: false);
            }
            finally
            {
                _insideUserCallback.Value = previous;
            }
        }

        var eventHandler = OnMessage;
        if (eventHandler is null)
            return;

        // Event consumers may retain data, so the event surface intentionally owns one stable copy.
        var args = new MessageEventArgs(message.ToArray(), messageType);
        InvokeUserEvent(() => eventHandler(this, args), "OnMessage handler");
    }

    private async ValueTask SendTextCoreAsync(string text, CancellationToken cancellationToken)
    {
        var maximumLength = Encoding.UTF8.GetMaxByteCount(text.Length);
        var buffer = ArrayPool<byte>.Shared.Rent(maximumLength);
        try
        {
            var length = Encoding.UTF8.GetBytes(text.AsSpan(), buffer.AsSpan());
            await SendCoreAsync(
                buffer.AsMemory(0, length),
                WebSocketMessageType.Text,
                endOfMessage: true,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async ValueTask SendCoreAsync(
        ReadOnlyMemory<byte> data,
        WebSocketMessageType messageType,
        bool endOfMessage,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var gateHeld = false;
        ClientWebSocket? socket = null;
        Exception? sendError = null;
        try
        {
            await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            gateHeld = true;
            socket = GetOpenSocket();
            await socket.SendAsync(data, messageType, endOfMessage, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Once a native send has started, cancellation may leave a partial frame/message.
            // Abort the physical socket so the receive loop can reconnect a clean transport.
            socket?.Abort();
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            sendError = exception;
            throw;
        }
        finally
        {
            if (gateHeld)
                _sendGate.Release();
            // Raise after releasing the gate so an error handler may safely Close or Dispose.
            if (sendError is not null)
                RaiseError(sendError, "Send", willReconnect: false);
        }
    }

    private async ValueTask SendSequenceCoreAsync(
        ReadOnlySequence<byte> data,
        WebSocketMessageType messageType,
        bool endOfMessage,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var gateHeld = false;
        ClientWebSocket? socket = null;
        Exception? sendError = null;
        try
        {
            await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            gateHeld = true;
            socket = GetOpenSocket();
            var position = data.Start;
            while (data.TryGet(ref position, out var segment))
            {
                var isLastSegment = !data.TryGet(ref position, out _, advance: false);
                if (segment.IsEmpty && !isLastSegment)
                    continue;

                await socket.SendAsync(
                    segment,
                    messageType,
                    endOfMessage && isLastSegment,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            socket?.Abort();
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            sendError = exception;
            throw;
        }
        finally
        {
            if (gateHeld)
                _sendGate.Release();
            if (sendError is not null)
                RaiseError(sendError, "Send", willReconnect: false);
        }
    }

    private async Task StopCoreAsync(
        WebSocketCloseStatus closeStatus,
        string? reason,
        CancellationToken cancellationToken)
    {
        ClientWebSocket? socket;
        CancellationTokenSource? lifetime;
        Task? lifecycle;

        lock (_sync)
        {
            _manualStop = true;
            _requestedCloseStatus = closeStatus;
            _requestedCloseReason = reason;
            socket = _socket;
            lifetime = _lifetimeCts;
            lifecycle = _lifecycleTask;
            if (lifecycle is not null)
                SetState(WebSocketClientState.Closing);
        }

        if (lifecycle is null)
            return;

        if (socket?.State is WebSocketState.Open or WebSocketState.CloseReceived)
        {
            try
            {
                await TryCloseOutputAsync(socket, closeStatus, reason, cancellationToken).ConfigureAwait(false);
                TryCancelAfter(lifetime, _options.CloseTimeout);
            }
            catch (OperationCanceledException)
            {
                socket.Abort();
                TryCancel(lifetime);
                throw;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                RaiseError(exception, "Close", willReconnect: false);
                socket.Abort();
                TryCancel(lifetime);
            }
        }
        else
        {
            socket?.Abort();
            TryCancel(lifetime);
        }

        // Calling Close from an event/callback must not wait on the lifecycle that is invoking it.
        if (_insideUserCallback.Value)
            return;

        await lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ReplyToCloseAsync(
        ClientWebSocket socket,
        WebSocketCloseStatus? closeStatus,
        string? reason,
        CancellationToken cancellationToken)
    {
        if (socket.State != WebSocketState.CloseReceived)
            return;

        await TryCloseOutputAsync(
            socket,
            closeStatus ?? WebSocketCloseStatus.NormalClosure,
            reason,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task TryCloseOutputAsync(
        ClientWebSocket socket,
        WebSocketCloseStatus closeStatus,
        string? reason,
        CancellationToken cancellationToken)
    {
        await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                await socket.CloseOutputAsync(closeStatus, reason, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _sendGate.Release();
        }
    }

    private bool ShouldReconnect(CloseInfo closeInfo)
    {
        if (_manualStop || !_options.AutoReconnect)
            return false;
        return closeInfo.Code != WebSocketCloseStatus.NormalClosure || _options.ReconnectOnNormalClosure;
    }

    private bool CanReconnect(int attempt) =>
        _options.AutoReconnect &&
        !_manualStop &&
        (_options.MaxReconnectAttempts == -1 || attempt <= _options.MaxReconnectAttempts);

    private void PrepareReconnect()
    {
        lock (_sync)
        {
            if (_openSignal.Task.IsCompleted)
                _openSignal = NewSignal();
            SetState(WebSocketClientState.Reconnecting);
        }
    }

    private void RaiseOpen(bool isReconnect)
    {
        var handler = OnOpen;
        if (handler is not null)
            InvokeUserEvent(() => handler(this, new OpenEventArgs(isReconnect)), "OnOpen handler");
    }

    private void RaiseClose(
        WebSocketCloseStatus? code,
        string? reason,
        bool wasClean,
        bool willReconnect)
    {
        var handler = OnClose;
        if (handler is not null)
            InvokeUserEvent(
                () => handler(this, new CloseEventArgs(code, reason, wasClean, willReconnect)),
                "OnClose handler");
    }

    private void RaiseReconnecting(int attempt, TimeSpan delay, Exception? cause)
    {
        var handler = OnReconnecting;
        if (handler is not null)
            InvokeUserEvent(
                () => handler(this, new ReconnectingEventArgs(
                    attempt,
                    _options.MaxReconnectAttempts,
                    delay,
                    cause)),
                "OnReconnecting handler");
    }

    private void RaiseError(Exception exception, string operation, bool willReconnect)
    {
        var handler = OnError;
        if (handler is null)
            return;

        var previous = _insideUserCallback.Value;
        _insideUserCallback.Value = true;
        try
        {
            handler(this, new ErrorEventArgs(exception, operation, willReconnect));
        }
        catch
        {
            // Error handlers must never tear down the transport or recurse into OnError.
        }
        finally
        {
            _insideUserCallback.Value = previous;
        }
    }

    private void InvokeUserEvent(Action action, string operation)
    {
        var previous = _insideUserCallback.Value;
        _insideUserCallback.Value = true;
        try
        {
            action();
        }
        catch (Exception exception)
        {
            RaiseError(exception, operation, willReconnect: false);
        }
        finally
        {
            _insideUserCallback.Value = previous;
        }
    }

    private string BuildCookieHeader()
    {
        if (_cookies.Count == 0)
            return string.Empty;

        var builder = new StringBuilder();
        foreach (var cookie in _cookies.Values)
        {
            if (cookie.Expired)
                continue;
            if (builder.Length > 0)
                builder.Append("; ");
            builder.Append(cookie.Name).Append('=').Append(cookie.Value);
        }
        return builder.ToString();
    }

    private ClientWebSocket GetOpenSocket()
    {
        ClientWebSocket? socket;
        lock (_sync)
            socket = _socket;

        return socket is not null && socket.State == WebSocketState.Open
            ? socket
            : throw new InvalidOperationException("The WebSocket is not open.");
    }

    private static void ValidateMessageType(WebSocketMessageType messageType)
    {
        if (messageType is not (WebSocketMessageType.Text or WebSocketMessageType.Binary))
            throw new ArgumentOutOfRangeException(nameof(messageType), "Only text and binary messages may be sent.");
    }

    private static void ValidateCloseReason(string? reason)
    {
        if (reason is not null && Encoding.UTF8.GetByteCount(reason) > 123)
            throw new ArgumentException("A WebSocket close reason cannot exceed 123 UTF-8 bytes.", nameof(reason));
    }

    private static void TryCancel(CancellationTokenSource? cancellationTokenSource)
    {
        try
        {
            cancellationTokenSource?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private static void TryCancelAfter(CancellationTokenSource? cancellationTokenSource, TimeSpan delay)
    {
        try
        {
            cancellationTokenSource?.CancelAfter(delay);
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private void SetState(WebSocketClientState state)
    {
        if (state != WebSocketClientState.Disposed && Volatile.Read(ref _disposed) != 0)
            return;
        Volatile.Write(ref _state, (int)state);
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    private static TaskCompletionSource<bool> NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        SetState(WebSocketClientState.Disposed);
        await StopCoreAsync(
            WebSocketCloseStatus.NormalClosure,
            "Client disposed",
            CancellationToken.None).ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private readonly record struct CloseInfo(
        WebSocketCloseStatus? Code,
        string? Reason,
        bool WasClean);

    private sealed class PooledMessageBuffer : IDisposable
    {
        private readonly int _maximumLength;
        private byte[]? _buffer;
        private int _length;

        public PooledMessageBuffer(int initialLength, int maximumLength)
        {
            _maximumLength = maximumLength;
            _buffer = ArrayPool<byte>.Shared.Rent(initialLength);
        }

        public ReadOnlyMemory<byte> WrittenMemory =>
            (_buffer ?? throw new ObjectDisposedException(nameof(PooledMessageBuffer))).AsMemory(0, _length);

        public void Append(ReadOnlySpan<byte> source)
        {
            var requiredLength = checked(_length + source.Length);
            if (requiredLength > _maximumLength)
                throw new WebSocketMessageTooBigException(_maximumLength);

            EnsureCapacity(requiredLength);
            source.CopyTo(_buffer.AsSpan(_length));
            _length = requiredLength;
        }

        private void EnsureCapacity(int requiredLength)
        {
            var current = _buffer ?? throw new ObjectDisposedException(nameof(PooledMessageBuffer));
            if (current.Length >= requiredLength)
                return;

            var doubled = (long)current.Length * 2;
            var newLength = (int)Math.Min(_maximumLength, Math.Max(requiredLength, doubled));
            var replacement = ArrayPool<byte>.Shared.Rent(newLength);
            current.AsSpan(0, _length).CopyTo(replacement);
            _buffer = replacement;
            ArrayPool<byte>.Shared.Return(current);
        }

        public void Dispose()
        {
            var buffer = Interlocked.Exchange(ref _buffer, null);
            if (buffer is not null)
                ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
