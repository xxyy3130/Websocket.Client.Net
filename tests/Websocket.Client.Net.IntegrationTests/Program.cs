using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using Websocket.Client.Net;

Console.WriteLine("Starting the in-process WebSocket test server...");
await using var server = new LocalWebSocketServer();
Console.WriteLine("Test server started.");

await TestEventsHeadersCookiesAndConcurrentSendAsync(server.BaseUri);
await TestSendErrorHandlerCanDisposeAsync(server.BaseUri);
await TestAutomaticReconnectAsync(server);
await TestConnectFromCloseEventAsync(server);
await TestConnectCancellationAsync(server.BaseUri);
await TestConcurrentConnectCancellationIsolationAsync(server);
await TestDisconnectCancellationAsync(server.BaseUri);
Console.WriteLine("All Websocket.Client.Net integration tests passed.");

static async Task TestEventsHeadersCookiesAndConcurrentSendAsync(Uri baseUri)
{
    await using var client = new WebSocketClient(new Uri(baseUri, "/echo"), new WebSocketClientOptions
    {
        AutoReconnect = false,
        ConnectTimeout = TimeSpan.FromSeconds(3)
    });

    client.SetHeader("X-Test", "header-value");
    client.SetCookie("session", "cookie-value");

    var openCount = 0;
    var received = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    var messages = 0;
    var binaryMessages = new List<byte[]>();
    var binariesReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    var closeCount = 0;
    CloseEventArgs? closeArgs = null;
    var errors = new List<Exception>();

    client.OnOpen += (_, _) => Interlocked.Increment(ref openCount);
    client.OnError += (_, e) => errors.Add(e.Exception);
    client.OnClose += (_, e) =>
    {
        closeArgs = e;
        Interlocked.Increment(ref closeCount);
    };
    client.OnMessage += (_, e) =>
    {
        if (e.IsText)
        {
            if (!e.Text.StartsWith("message-", StringComparison.Ordinal))
                received.TrySetException(new InvalidOperationException("Unexpected echoed message."));
            if (Interlocked.Increment(ref messages) == 32)
                received.TrySetResult(true);
            return;
        }

        binaryMessages.Add(e.Data.ToArray());
        if (binaryMessages.Count == 3)
            binariesReceived.TrySetResult(true);
    };

    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await client.ConnectAsync(timeout.Token);

    // ClientWebSocket permits one send at a time. Websocket.Client.Net safely serializes these callers.
    await Task.WhenAll(Enumerable.Range(0, 32)
        .Select(index => client.SendAsync($"message-{index}", timeout.Token).AsTask()));
    await received.Task.WaitAsync(timeout.Token);

    Assert(openCount == 1, "OnOpen must fire exactly once.");
    Assert(messages == 32, "All concurrent sends must be echoed.");

    await client.SendAsync(new byte[] { 1, 2, 3 }, cancellationToken: timeout.Token);
    var segmentSource = new byte[] { 0, 4, 5, 6, 0 };
    await client.SendAsync(new ArraySegment<byte>(segmentSource, 1, 3), cancellationToken: timeout.Token);
    var firstSegment = new BufferSegment(new byte[] { 7, 8 });
    var lastSegment = firstSegment.Append(new byte[] { 9, 10 });
    var sequence = new ReadOnlySequence<byte>(firstSegment, 0, lastSegment, lastSegment.Memory.Length);
    await client.SendAsync(sequence, cancellationToken: timeout.Token);
    await binariesReceived.Task.WaitAsync(timeout.Token);

    Assert(binaryMessages[0].SequenceEqual(new byte[] { 1, 2, 3 }), "byte[] send must preserve data.");
    Assert(binaryMessages[1].SequenceEqual(new byte[] { 4, 5, 6 }), "ArraySegment send must honor offset and count.");
    Assert(binaryMessages[2].SequenceEqual(new byte[] { 7, 8, 9, 10 }), "ReadOnlySequence send must remain one message.");

    using var canceledSend = new CancellationTokenSource();
    canceledSend.Cancel();
    await AssertThrowsAsync<OperationCanceledException>(
        client.SendAsync(new byte[] { 99 }, cancellationToken: canceledSend.Token).AsTask());
    Assert(errors.Count == 0, "The echo test must not raise OnError.");
    await client.DisconnectAsync(reason: "echo test complete", cancellationToken: timeout.Token);
    Assert(closeCount == 1, "OnClose must fire exactly once after a graceful disconnect.");
    Assert(closeArgs is { Code: WebSocketCloseStatus.NormalClosure, WasClean: true, WillReconnect: false },
        "OnClose must report a clean manual close without reconnect.");
}

static async Task TestSendErrorHandlerCanDisposeAsync(Uri baseUri)
{
    var client = new WebSocketClient(new Uri(baseUri, "/echo"), new WebSocketClientOptions
    {
        AutoReconnect = false
    });
    client.OnError += (_, _) => client.Dispose();

    await AssertThrowsAsync<InvalidOperationException>(
        client.SendAsync(new byte[] { 1 }).AsTask().WaitAsync(TimeSpan.FromSeconds(1)));
    Assert(client.State == WebSocketClientState.Disposed,
        "An OnError handler must be able to Dispose without deadlocking on the send gate.");
}

static async Task TestAutomaticReconnectAsync(LocalWebSocketServer server)
{
    await using var client = new WebSocketClient(new Uri(server.BaseUri, "/reconnect"), new WebSocketClientOptions
    {
        AutoReconnect = true,
        MaxReconnectAttempts = 3,
        ReconnectDelay = TimeSpan.FromMilliseconds(20),
        ConnectTimeout = TimeSpan.FromSeconds(3)
    });

    var opens = 0;
    var reconnectEvents = 0;
    var errors = 0;
    Task[]? concurrentConnects = null;
    var message = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    client.OnOpen += (_, _) => Interlocked.Increment(ref opens);
    client.OnReconnecting += (_, _) =>
    {
        Interlocked.Increment(ref reconnectEvents);
        concurrentConnects ??= Enumerable.Range(0, 32)
            .Select(_ => client.ConnectAsync())
            .ToArray();
    };
    client.OnError += (_, _) => Interlocked.Increment(ref errors);
    client.OnMessage += (_, e) => message.TrySetResult(e.Text);

    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await client.ConnectAsync(timeout.Token);
    var text = await message.Task.WaitAsync(timeout.Token);
    Assert(concurrentConnects is not null, "The reconnect event must start concurrent ConnectAsync waiters.");
    await Task.WhenAll(concurrentConnects!).WaitAsync(timeout.Token);

    Assert(text == "reconnected", "The second connection must deliver its message.");
    Assert(opens == 2, "OnOpen must fire again after reconnect.");
    Assert(reconnectEvents >= 1, "OnReconnecting must report the retry.");
    Assert(errors >= 1, "OnError must report the abrupt disconnect.");
    Assert(server.ReconnectConnections == 2,
        "Concurrent ConnectAsync calls must share one reconnect operation and not create duplicate sockets.");
    await client.DisconnectAsync(reason: "reconnect test complete", cancellationToken: timeout.Token);
}

static async Task TestConnectFromCloseEventAsync(LocalWebSocketServer server)
{
    await using var client = new WebSocketClient(new Uri(server.BaseUri, "/restart"), new WebSocketClientOptions
    {
        AutoReconnect = false,
        ConnectTimeout = TimeSpan.FromSeconds(3)
    });

    Task? restartedConnection = null;
    var message = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    EventHandler<CloseEventArgs> reconnectOnClose = (_, _) => restartedConnection ??= client.ConnectAsync();
    client.OnClose += reconnectOnClose;
    client.OnMessage += (_, e) => message.TrySetResult(e.Text);

    await client.ConnectAsync();
    Assert(await message.Task.WaitAsync(TimeSpan.FromSeconds(2)) == "restarted",
        "ConnectAsync started from OnClose must create a fresh connection after shutdown unwinds.");
    Assert(restartedConnection is not null, "OnClose must start the replacement connection.");
    await restartedConnection!.WaitAsync(TimeSpan.FromSeconds(2));
    Assert(server.RestartConnections == 2, "Restarting from OnClose must create exactly one new socket.");
    client.OnClose -= reconnectOnClose;
    await client.DisconnectAsync();
}

static async Task TestConnectCancellationAsync(Uri baseUri)
{
    await using var client = new WebSocketClient(new Uri(baseUri, "/slow-handshake"), new WebSocketClientOptions
    {
        AutoReconnect = false,
        ConnectTimeout = TimeSpan.FromSeconds(3)
    });

    using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
    await AssertThrowsAsync<OperationCanceledException>(client.ConnectAsync(cancellation.Token));
}

static async Task TestConcurrentConnectCancellationIsolationAsync(LocalWebSocketServer server)
{
    await using var client = new WebSocketClient(new Uri(server.BaseUri, "/delayed-handshake"), new WebSocketClientOptions
    {
        AutoReconnect = false,
        ConnectTimeout = TimeSpan.FromSeconds(3)
    });

    using var firstCallerCancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
    var firstCaller = client.ConnectAsync(firstCallerCancellation.Token);
    await Task.Delay(10);
    var secondCaller = client.ConnectAsync();

    await AssertThrowsAsync<OperationCanceledException>(firstCaller);
    await secondCaller.WaitAsync(TimeSpan.FromSeconds(2));
    Assert(client.IsAlive, "Canceling one ConnectAsync waiter must not cancel the shared connection.");
    Assert(server.DelayedHandshakeConnections == 1,
        "Concurrent ConnectAsync callers must share one physical connection attempt.");
    await client.DisconnectAsync();
}

static async Task TestDisconnectCancellationAsync(Uri baseUri)
{
    await using var client = new WebSocketClient(new Uri(baseUri, "/hold"), new WebSocketClientOptions
    {
        AutoReconnect = false,
        ConnectTimeout = TimeSpan.FromSeconds(3)
    });

    await client.ConnectAsync();
    using var cancellation = new CancellationTokenSource();
    cancellation.Cancel();
    await AssertThrowsAsync<OperationCanceledException>(client.DisconnectAsync(cancellationToken: cancellation.Token));

    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(1));
    while (client.State != WebSocketClientState.Disconnected)
        await Task.Delay(10, timeout.Token);
}

static async Task AssertThrowsAsync<TException>(Task task)
    where TException : Exception
{
    try
    {
        await task;
    }
    catch (TException)
    {
        return;
    }

    throw new InvalidOperationException($"Expected {typeof(TException).Name}.");
}

static void Assert(bool condition, string message)
{
    if (!condition)
        throw new InvalidOperationException(message);
}

internal sealed class LocalWebSocketServer : IAsyncDisposable
{
    private static readonly byte[] WebSocketMagic =
        "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"u8.ToArray();

    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task _acceptLoop;
    private int _reconnectConnections;
    private int _delayedHandshakeConnections;
    private int _restartConnections;

    public LocalWebSocketServer()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        var endpoint = (IPEndPoint)_listener.LocalEndpoint;
        BaseUri = new Uri($"ws://127.0.0.1:{endpoint.Port}/");
        _acceptLoop = AcceptLoopAsync();
    }

    public Uri BaseUri { get; }

    public int ReconnectConnections => Volatile.Read(ref _reconnectConnections);

    public int DelayedHandshakeConnections => Volatile.Read(ref _delayedHandshakeConnections);

    public int RestartConnections => Volatile.Read(ref _restartConnections);

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!_shutdown.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_shutdown.Token).ConfigureAwait(false);
                await HandleClientAsync(client, _shutdown.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
        {
        }
        catch (SocketException) when (_shutdown.IsCancellationRequested)
        {
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        using (client)
        {
            var stream = client.GetStream();
            var request = await ReadHttpHeaderAsync(stream, cancellationToken).ConfigureAwait(false);
            var requestLines = request.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
            var path = requestLines[0].Split(' ', StringSplitOptions.RemoveEmptyEntries)[1];
            var headers = requestLines.Skip(1)
                .Select(line => line.Split(':', 2))
                .Where(parts => parts.Length == 2)
                .ToDictionary(parts => parts[0], parts => parts[1].Trim(), StringComparer.OrdinalIgnoreCase);

            if (path == "/slow-handshake")
            {
                await Task.Delay(300, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (path == "/delayed-handshake")
            {
                Interlocked.Increment(ref _delayedHandshakeConnections);
                await Task.Delay(150, cancellationToken).ConfigureAwait(false);
            }

            if (path == "/echo" &&
                (!headers.TryGetValue("X-Test", out var testHeader) || testHeader != "header-value" ||
                 !headers.TryGetValue("Cookie", out var cookie) || !cookie.Contains("session=cookie-value", StringComparison.Ordinal)))
            {
                await stream.WriteAsync("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"u8.ToArray(), cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            if (!headers.TryGetValue("Sec-WebSocket-Key", out var key))
                throw new InvalidOperationException("Missing Sec-WebSocket-Key.");

            var acceptSource = new byte[Encoding.ASCII.GetByteCount(key) + WebSocketMagic.Length];
            var keyLength = Encoding.ASCII.GetBytes(key, acceptSource);
            WebSocketMagic.CopyTo(acceptSource.AsSpan(keyLength));
            var accept = Convert.ToBase64String(SHA1.HashData(acceptSource));
            var response = Encoding.ASCII.GetBytes(
                "HTTP/1.1 101 Switching Protocols\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                $"Sec-WebSocket-Accept: {accept}\r\n\r\n");
            await stream.WriteAsync(response, cancellationToken).ConfigureAwait(false);

            if (path == "/echo")
            {
                await EchoAsync(stream, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (path == "/delayed-handshake")
            {
                var delayedClose = await ReadFrameAsync(stream, cancellationToken).ConfigureAwait(false);
                if (delayedClose.Opcode == 8)
                    await WriteFrameAsync(stream, opcode: 8, delayedClose.Payload, CancellationToken.None).ConfigureAwait(false);
                return;
            }

            if (path == "/hold")
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (path == "/restart")
            {
                if (Interlocked.Increment(ref _restartConnections) == 1)
                {
                    var closePayload = new byte[2];
                    BinaryPrimitives.WriteUInt16BigEndian(closePayload, (ushort)WebSocketCloseStatus.NormalClosure);
                    await WriteFrameAsync(stream, opcode: 8, closePayload, cancellationToken).ConfigureAwait(false);
                    await ReadFrameAsync(stream, cancellationToken).ConfigureAwait(false);
                    return;
                }

                await WriteFrameAsync(stream, opcode: 1, "restarted"u8.ToArray(), cancellationToken).ConfigureAwait(false);
                var restartedClose = await ReadFrameAsync(stream, cancellationToken).ConfigureAwait(false);
                if (restartedClose.Opcode == 8)
                    await WriteFrameAsync(stream, opcode: 8, restartedClose.Payload, CancellationToken.None).ConfigureAwait(false);
                return;
            }

            if (path != "/reconnect")
                return;

            if (Interlocked.Increment(ref _reconnectConnections) == 1)
            {
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                client.Client.LingerState = new LingerOption(enable: true, seconds: 0);
                return;
            }

            await WriteFrameAsync(stream, opcode: 1, "reconnected"u8.ToArray(), cancellationToken).ConfigureAwait(false);
            var closeFrame = await ReadFrameAsync(stream, cancellationToken).ConfigureAwait(false);
            if (closeFrame.Opcode == 8)
                await WriteFrameAsync(stream, opcode: 8, closeFrame.Payload, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static async Task EchoAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        while (true)
        {
            var frame = await ReadFrameAsync(stream, cancellationToken).ConfigureAwait(false);
            await WriteFrameAsync(
                stream,
                frame.Opcode,
                frame.Payload,
                cancellationToken,
                frame.EndOfMessage).ConfigureAwait(false);
            if (frame.Opcode == 8)
                return;
        }
    }

    private static async Task<string> ReadHttpHeaderAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var bytes = new List<byte>(1024);
        var oneByte = new byte[1];
        while (bytes.Count < 16 * 1024)
        {
            if (await stream.ReadAsync(oneByte, cancellationToken).ConfigureAwait(false) == 0)
                throw new EndOfStreamException();
            bytes.Add(oneByte[0]);
            var count = bytes.Count;
            if (count >= 4 && bytes[count - 4] == '\r' && bytes[count - 3] == '\n' &&
                bytes[count - 2] == '\r' && bytes[count - 1] == '\n')
                return Encoding.ASCII.GetString(bytes.ToArray());
        }

        throw new InvalidOperationException("HTTP header is too large.");
    }

    private static async Task<WebSocketFrame> ReadFrameAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var header = new byte[2];
        await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        var endOfMessage = (header[0] & 0x80) != 0;
        var opcode = (byte)(header[0] & 0x0f);
        var isMasked = (header[1] & 0x80) != 0;
        ulong payloadLength = (uint)(header[1] & 0x7f);

        if (payloadLength == 126)
        {
            var extended = new byte[2];
            await stream.ReadExactlyAsync(extended, cancellationToken).ConfigureAwait(false);
            payloadLength = BinaryPrimitives.ReadUInt16BigEndian(extended);
        }
        else if (payloadLength == 127)
        {
            var extended = new byte[8];
            await stream.ReadExactlyAsync(extended, cancellationToken).ConfigureAwait(false);
            payloadLength = BinaryPrimitives.ReadUInt64BigEndian(extended);
        }

        if (payloadLength > 1024 * 1024)
            throw new InvalidOperationException("Test frame is too large.");

        var mask = new byte[4];
        if (isMasked)
            await stream.ReadExactlyAsync(mask, cancellationToken).ConfigureAwait(false);

        var payload = new byte[(int)payloadLength];
        if (payload.Length > 0)
            await stream.ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false);
        if (isMasked)
        {
            for (var index = 0; index < payload.Length; index++)
                payload[index] ^= mask[index & 3];
        }

        return new WebSocketFrame(opcode, payload, endOfMessage);
    }

    private static async Task WriteFrameAsync(
        NetworkStream stream,
        byte opcode,
        byte[] payload,
        CancellationToken cancellationToken,
        bool endOfMessage = true)
    {
        var header = new byte[10];
        header[0] = (byte)((endOfMessage ? 0x80 : 0x00) | opcode);
        var headerLength = 2;
        if (payload.Length <= 125)
        {
            header[1] = (byte)payload.Length;
        }
        else if (payload.Length <= ushort.MaxValue)
        {
            header[1] = 126;
            BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(2), (ushort)payload.Length);
            headerLength = 4;
        }
        else
        {
            header[1] = 127;
            BinaryPrimitives.WriteUInt64BigEndian(header.AsSpan(2), (ulong)payload.Length);
            headerLength = 10;
        }

        await stream.WriteAsync(header.AsMemory(0, headerLength), cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await _shutdown.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        await _acceptLoop.ConfigureAwait(false);
        _shutdown.Dispose();
    }

    private readonly record struct WebSocketFrame(byte Opcode, byte[] Payload, bool EndOfMessage);
}

internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    public BufferSegment(ReadOnlyMemory<byte> memory)
    {
        Memory = memory;
    }

    public BufferSegment(byte[] data) : this(data.AsMemory())
    {
    }

    public BufferSegment Append(ReadOnlyMemory<byte> memory)
    {
        var segment = new BufferSegment(memory)
        {
            RunningIndex = RunningIndex + Memory.Length
        };
        Next = segment;
        return segment;
    }
}
