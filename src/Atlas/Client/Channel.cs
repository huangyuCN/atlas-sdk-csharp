using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using AtlasTimeoutException = Atlas.Errors.TimeoutException;

namespace Atlas.Client;

// Channel 管理一条传输连接的请求-响应匹配、心跳与自动重连（M4）。
// 生命周期：ConnectAsync 建立首连并启动读循环/心跳；读循环因网络错误退出后
// （非协议致命、非主动关闭）进入指数退避自动重连；重连期间 Invoke 排队，
// 重连成功后按序重发（FIFO）。协议级致命错误（版本不匹配/帧非法）直接终止，
// 不重连（对标 Go：ProtocolError 不可重试、连接已断）。
public sealed partial class Channel : IAsyncDisposable
{
    private readonly Func<CancellationToken, Task<ITransport>> _dial;
    private readonly ChannelOptions _options;
    private readonly object _gate = new();
    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly Dictionary<InflightKey, Inflight> _inflight = new();
    private readonly object _notifyGate = new();
    private readonly Dictionary<string, List<NotifyEntry>> _notifies = new();
    private readonly CancellationTokenSource _closed = new();
    private readonly Queue<QueuedInvoke> _queue = new();

    // 会话心跳单飞标记（triggerReloginHook 用，对齐 Go sessionHookBusy atomic.Bool）。
    private int _sessionHookBusy;

    // 当前代（epoch）的取消源：换代时 Cancel 旧代以停止该代心跳；新建下一代的
    // 源。读循环不依赖此源（连接读错误自然退出），心跳依赖它以随代退出
    // （M4 评审 P1：旧代心跳不得继续在新连接上运行/误杀新连接）。
    private CancellationTokenSource _generationCts = new();
    private ITransport? _transport;
    private Task? _readLoop;
    private Task? _heartbeatTask;
    private Task? _sessionHeartbeatTask;
    private uint _epoch;
    private uint _sequence;
    private int _state = (int)ClientState.Disconnected;
    private bool _isClosed;
    private bool _reconnecting;

    // 仅供 Atlas.Tests 构造确定性并发窗口，不对 SDK 使用方公开。
    internal Func<Task>? BeforeInflightCompletion { get; set; }

    // 仅供 Atlas.Tests 验证 Connect/Close 串行化。
    internal Func<Task>? AfterTransportInstalled { get; set; }

    // OnRelogin 注册会话重登回调：会话心跳收到业务拒绝（会话过期）时单飞触发
    //（同一时刻至多一次未返回）；由业务方在此决定重登或下线——SDK 不擅自用旧
    // token 自动重登（规范 §5.2：会话失效回调业务方）。对标 Go WithOnReconnected
    // 钩子（会话心跳路径的触发器；重连成功路径的钩子由 M4-4 编排接入）。
    public Func<Task>? OnRelogin { get; set; }

    public Channel(Func<CancellationToken, Task<ITransport>> dial, ChannelOptions options)
    {
        _dial = dial ?? throw new ArgumentNullException(nameof(dial));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        ValidateOptions(_options);
    }

    public ClientState State => (ClientState)Volatile.Read(ref _state);

    // On 订阅本通道的 Notify 帧（按 operation 分发），返回退订句柄。
    // 幂等语义：同一 (op, handler)（delegate 引用相等）重复注册只保留一份，
    // 重复退订安全。handler 在线程池执行、异常隔离，不影响其他分发（对标 Go on）。
    // 订阅挂在 Channel 层（不随连接代际丢失），重连成功后新连接天然继续收到推送。
    public NotifySubscription On(string op, NotifyHandler handler)
    {
        if (handler == null)
        {
            throw new ArgumentNullException(nameof(handler));
        }
        if (string.IsNullOrEmpty(op))
        {
            throw new ArgumentException("operation 不能为空", nameof(op));
        }
        lock (_notifyGate)
        {
            if (!_notifies.TryGetValue(op, out var entries))
            {
                entries = new List<NotifyEntry>();
                _notifies[op] = entries;
            }
            var existing = entries.Find(e => ReferenceEquals(e.Handler, handler));
            if (existing == null)
            {
                entries.Add(new NotifyEntry(handler));
            }
        }
        return new NotifySubscription(() =>
        {
            lock (_notifyGate)
            {
                if (_notifies.TryGetValue(op, out var list))
                {
                    list.RemoveAll(e => ReferenceEquals(e.Handler, handler));
                    if (list.Count == 0)
                    {
                        _notifies.Remove(op);
                    }
                }
            }
        });
    }

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
            // 读循环是该代的生命周期管理者：网络错误退出后自行驱动自动重连
            //（RunReconnectLoopAsync 在其中）；协议致命/主动关闭则直接结束。
            _readLoop = Task.Run(() => ReadLoopAsync(transport, epoch));
            StartGenerationHeartbeat(epoch);
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
            CancelGeneration(); // 停当前代心跳。
        }
        finally
        {
            _connectLock.Release();
        }

        FailAllInflight(new NetworkException("通道已关闭"));
        FailAllQueued();
        if (closing.transport != null)
        {
            await CloseTransportAsync(closing.transport);
        }
        if (closing.readLoop != null)
        {
            await IgnoreReadLoopAsync(closing.readLoop);
        }
        if (_heartbeatTask != null)
        {
            try
            {
                await _heartbeatTask;
            }
            catch (Exception)
            {
                // 心跳退出异常在关闭路径忽略。
            }
        }
        if (_sessionHeartbeatTask != null)
        {
            try
            {
                await _sessionHeartbeatTask;
            }
            catch (Exception)
            {
                // 会话心跳退出异常在关闭路径忽略。
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync();
        _closed.Dispose();
        _generationCts.Dispose();
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

    // CancelGeneration 取消当前代心跳（换代时由重连循环调用；关闭时由 CloseAsync 调用）。
    private void CancelGeneration()
    {
        CancellationTokenSource old;
        lock (_gate)
        {
            old = _generationCts;
            _generationCts = new CancellationTokenSource();
        }
        try
        {
            old.Cancel();
        }
        finally
        {
            old.Dispose();
        }
    }

    // StartGenerationHeartbeat 启动当前代传输心跳与会话心跳。心跳绑定代：换代时
    // CancelGeneration 停旧代心跳（旧心跳不会继续在新连接上发 Ping 或误杀新连接
    // ——M4 评审 P1）。会话心跳（若配置）同代启动、同代退出。
    private void StartGenerationHeartbeat(uint epoch)
    {
        if (_options.HeartbeatIntervalMs <= 0 && _options.SessionHeartbeatIntervalMs <= 0)
        {
            return; // 传输心跳与会话心跳均未配置（非正周期 = 关闭）。
        }
        CancellationTokenSource cts;
        lock (_gate)
        {
            if (_isClosed || _epoch != epoch)
            {
                return; // 已换代或已关闭：不启动过期代心跳。
            }
            cts = _generationCts;
        }
        if (_options.HeartbeatIntervalMs > 0)
        {
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(epoch, cts.Token));
        }
        if (_options.SessionHeartbeatIntervalMs > 0 && _options.SessionHeartbeatOpFactory != null)
        {
            _sessionHeartbeatTask = Task.Run(() => SessionHeartbeatLoopAsync(epoch, cts.Token));
        }
    }

    // 心跳死链后关闭的应是「心跳所属代」的连接。若期间换代（旧心跳延迟退出），
    // 关闭前核对代：只关自己那一代，不误杀新代连接（M4 评审 P1）。
    private async Task CloseGenerationTransportAsync(uint epoch)
    {
        ITransport? transport;
        lock (_gate)
        {
            if (_epoch == epoch)
            {
                transport = _transport;
            }
            else
            {
                transport = null; // 已换代：旧心跳不得关闭新连接。
            }
        }
        if (transport != null)
        {
            await CloseTransportAsync(transport);
        }
    }
}
