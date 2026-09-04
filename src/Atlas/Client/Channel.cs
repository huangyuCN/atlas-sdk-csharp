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

    // 重连钩子同步执行标记（hookBypass，对齐 Go hookBypass atomic.Bool）：置位期间
    // Invoke 直通当前代连接——钩子的重登请求与传输心跳不排队（队列要等钩子成功后才
    // drain；对齐 Go invoke.go hookBypass 文档）。窗口上限 = HookTimeoutMs。
    private int _hookBypass;

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

    // 当前代读循环的退出原因（null = 存活/未记录；仅 _gate 内读写）。
    // settle（CompleteReconnect）前核对：新代读循环可能在本代安装后立即死亡
    //（网络错误——FailGeneration 置 Disconnected；或协议致命）——若此时仍置
    // Connected 则成 zombie（Connected 但无连接/读循环/重连循环，M4-4 评审 P1）。
    // 记录后由 RunReconnectLoopAsync 区分处置：网络类继续退避重连、协议致命终止。
    private Exception? _generationFault;

    // 仅供 Atlas.Tests 构造确定性并发窗口，不对 SDK 使用方公开。
    internal Func<Task>? BeforeInflightCompletion { get; set; }

    // 仅供 Atlas.Tests 验证 Connect/Close 串行化。
    internal Func<Task>? AfterTransportInstalled { get; set; }

    // OnRelogin 注册会话重登回调（= Go onReconnected 重连钩子）：两条路径触发——
    // ① 重连成功后由重连编排器同步执行（settle，M4-4）：期间 hookBypass 直通窗口，
    //    钩子的重登请求直写新连接；失败（错误/超时）弃用连接继续退避重连，成功才
    //    drain 排队队列；
    // ② 会话心跳收到业务拒绝（会话过期）时 CAS 单飞触发（异步，业务方自决重登/下线）——
    //    hookBypass 期间跳过，避免并发重登（规范 §5.2）。SDK 不擅自用旧 token 自动重登。
    // 对标 Go WithOnReconnected 钩子（重连路径 runHookSync + 会话心跳 triggerReloginHook
    // 共用同一回调）。
    public Func<Task>? OnRelogin { get; set; }

    public Channel(Func<CancellationToken, Task<ITransport>> dial, ChannelOptions options)
        : this(ChannelKind.Business, dial, options)
    {
    }

    // Channel 构造：kind 标定通道角色（业务/战斗；dual 编排用，M4-5）。
    public Channel(
        ChannelKind kind,
        Func<CancellationToken, Task<ITransport>> dial,
        ChannelOptions options)
    {
        Kind = kind;
        _dial = dial ?? throw new ArgumentNullException(nameof(dial));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        ValidateOptions(_options);
    }

    // Kind 是本通道角色（dual 形态区分业务/战斗；会话心跳仅业务通道生效的门控依据）。
    public ChannelKind Kind { get; }

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
            _generationFault = null; // 新代起点：清除上一代的退出原因记录。
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

    // SetHookBypass 置位/清除重连钩子直通窗口标记（由 RunReconnectLoopAsync 在
    // 钩子同步执行期间管理；对齐 Go hookBypass.Store）。
    private void SetHookBypass(bool bypass)
    {
        Volatile.Write(ref _hookBypass, bypass ? 1 : 0);
    }

    // IsHookBypass 返回是否处于重连钩子同步执行窗口（Invoke 直通判定用）。
    private bool IsHookBypass()
    {
        return Volatile.Read(ref _hookBypass) != 0;
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
        // 会话心跳仅业务通道生效（规范 §5.2：会话绑定业务通道，战斗通道不续租——
        // 战斗通道无会话概念；对标 Go 实现按通道角色强制）。
        if (Kind == ChannelKind.Business
            && _options.SessionHeartbeatIntervalMs > 0
            && _options.SessionHeartbeatOpFactory != null)
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
