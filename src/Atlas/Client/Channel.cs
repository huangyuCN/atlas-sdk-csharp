using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using AtlasTimeoutException = Atlas.Errors.TimeoutException;

namespace Atlas.Client;

// Channel 管理一条传输连接的请求-响应匹配；M4 在此基础上增加重连与排队。
public sealed partial class Channel : IAsyncDisposable
{
    private readonly Func<CancellationToken, Task<ITransport>> _dial;
    private readonly ChannelOptions _options;
    private readonly object _gate = new();
    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly Dictionary<InflightKey, Inflight> _inflight = new();
    private readonly CancellationTokenSource _closed = new();
    private ITransport? _transport;
    private Task? _readLoop;
    private uint _epoch;
    private uint _sequence;
    private int _state = (int)ClientState.Disconnected;
    private bool _isClosed;

    // 仅供 Atlas.Tests 构造确定性并发窗口，不对 SDK 使用方公开。
    internal Func<Task>? BeforeInflightCompletion { get; set; }

    // 仅供 Atlas.Tests 验证 Connect/Close 串行化。
    internal Func<Task>? AfterTransportInstalled { get; set; }

    public Channel(Func<CancellationToken, Task<ITransport>> dial, ChannelOptions options)
    {
        _dial = dial ?? throw new ArgumentNullException(nameof(dial));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        ValidateOptions(_options);
    }

    public ClientState State => (ClientState)Volatile.Read(ref _state);

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        await _connectLock.WaitAsync(cancellationToken);
        try
        {
            ThrowIfClosed();
            if (State != ClientState.Disconnected)
            {
                throw new NetworkException($"通道当前状态为 {State}，不能重复连接");
            }

            SetState(ClientState.Connecting);
            var transport = await DialAsync(cancellationToken);
            if (!InstallTransport(transport, out var epoch))
            {
                await CloseTransportAsync(transport);
                ThrowIfClosed();
            }

            if (AfterTransportInstalled != null)
            {
                await AfterTransportInstalled();
            }
            SetState(ClientState.Connected);
            _readLoop = Task.Run(() => ReadLoopAsync(transport, epoch));
        }
        finally
        {
            _connectLock.Release();
        }
    }

    public async Task CloseAsync()
    {
        await _connectLock.WaitAsync();
        (ITransport? transport, Task? readLoop) closing;
        try
        {
            closing = BeginClose();
            _closed.Cancel();
        }
        finally
        {
            _connectLock.Release();
        }

        FailAllInflight(new NetworkException("通道已关闭"));
        if (closing.transport != null)
        {
            await CloseTransportAsync(closing.transport);
        }
        if (closing.readLoop != null)
        {
            await IgnoreReadLoopAsync(closing.readLoop);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync();
        _closed.Dispose();
        _connectLock.Dispose();
        _writeLock.Dispose();
    }

    private async Task<ITransport> DialAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _dial(cancellationToken) ?? throw new NetworkException("拨号返回空传输");
        }
        catch (AtlasException)
        {
            SetState(ClientState.Disconnected);
            throw;
        }
        catch (Exception exception)
        {
            SetState(ClientState.Disconnected);
            throw new NetworkException("拨号失败", exception);
        }
    }

    private bool InstallTransport(ITransport transport, out uint epoch)
    {
        lock (_gate)
        {
            if (_isClosed)
            {
                epoch = 0;
                return false;
            }

            _epoch = NextNonZero(_epoch);
            _transport = transport;
            epoch = _epoch;
            return true;
        }
    }

    private (ITransport? Transport, Task? ReadLoop) BeginClose()
    {
        lock (_gate)
        {
            if (_isClosed)
            {
                return (null, null);
            }

            _isClosed = true;
            var transport = _transport;
            var readLoop = _readLoop;
            _transport = null;
            SetState(ClientState.Disconnected);
            return (transport, readLoop);
        }
    }

    private void ThrowIfClosed()
    {
        lock (_gate)
        {
            if (_isClosed)
            {
                throw new NetworkException("通道已关闭");
            }
        }
    }

    private static void ValidateOptions(ChannelOptions options)
    {
        if (options.Serializer == null)
        {
            throw new ArgumentException("Serializer 不能为空", nameof(options));
        }
        if (options.Serializer.Version != FrameConst.Version && options.Serializer.Version != FrameConst.Version2)
        {
            throw new ArgumentException("Serializer Version 必须是 1 或 2", nameof(options));
        }
        if (options.InvokeTimeoutMs <= 0)
        {
            throw new ArgumentException("InvokeTimeoutMs 必须大于 0", nameof(options));
        }
    }

    private void SetState(ClientState state)
    {
        Volatile.Write(ref _state, (int)state);
    }

    private static uint NextNonZero(uint value)
    {
        var next = value + 1;
        return next == 0 ? 1 : next;
    }

    private static async Task CloseTransportAsync(ITransport transport)
    {
        try
        {
            await transport.CloseAsync();
            await transport.DisposeAsync();
        }
        catch (Exception)
        {
        }
    }

    private static async Task IgnoreReadLoopAsync(Task readLoop)
    {
        try
        {
            await readLoop;
        }
        catch (Exception)
        {
        }
    }
}
