using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;

namespace Atlas.Client;

// QueuedInvoke 是断线重连期间排队的请求（重连成功后按序重发，FIFO）。
// 单一所有权：drain 重发或关闭 failAllQueued 时结算；队列容量由
// ChannelOptions.QueueSize 限制，满则立即失败（NetworkException）。
internal sealed class QueuedInvoke
{
    public QueuedInvoke(
        string operation,
        byte[]? payload,
        TaskCompletionSource<byte[]> completion,
        DateTime deadline)
    {
        Operation = operation;
        Payload = payload;
        Completion = completion;
        Deadline = deadline;
    }

    public string Operation { get; }

    public byte[]? Payload { get; }

    // 完成源：drain 重发成功后置结果；关闭时置 NetworkException。
    public TaskCompletionSource<byte[]> Completion { get; }

    // Deadline 是排队期限：到点未 drain 则由看护认领并超时失败（对齐 Go
    // queueDeadlineWatch——排队阶段计入超时，不无限期悬挂）。
    public DateTime Deadline { get; }

    private int _claimed;

    // TryClaim 原子认领（CAS）：成功表示本路径拥有结算权（drain 重发或看护
    // 超时，二选一，恰好一次）；失败表示已被另一方认领，调用方须放弃。
    public bool TryClaim()
    {
        return Interlocked.CompareExchange(ref _claimed, 1, 0) == 0;
    }
}

public sealed partial class Channel
{
    // RunReconnectLoopAsync 在 ReadLoopAsync 退出（网络错误、非协议致命、非关闭）
    // 后被调用：指数退避拨号直至成功或通道关闭。期间状态 Reconnecting、
    // Invoke 排队；成功后进入 settleGeneration（对齐 Go）：
    //   - 有重连钩子（OnRelogin）：钩子同步执行（hookBypass 窗口，Invoke 直通当前
    //     代连接）——钩子失败（错误/超时）弃用本代连接、保留排队请求、退避重连后
    //     重试；成功后才置 Connected 并 drain（排队请求严格先于新请求）；
    //   - 无钩子：直接置 Connected + drain（同临界区）。
    // 调用方（ReadLoopAsync）已在锁内原子置 _reconnecting=true + Reconnecting
    //（消除 Disconnected 悬置窗口，见 ReadLoopAsync）；本方法直接进入退避循环，
    // 退出时（成功/关闭/放弃）由 finally 清 _reconnecting。
    //
    // 代死亡复核（M4-4 评审 P1 修复）：_reconnecting==true 会吞掉新代读循环自身的
    // shouldReconnect 驱动（见 ReadLoopAsync），故 TrySettleGenerationAsync 在置
    // Connected 前核对 _generationFault（本代读循环退出原因）——网络类死亡则退避
    // 重试不置 Connected（避免 zombie）；协议致命则终止不重拨。
    private async Task RunReconnectLoopAsync()
    {
        var backoff = Math.Max(_options.BackoffBaseMs, 1);
        try
        {
            while (true)
            {
                var transport = await DialOneAttemptAsync(backoff);
                if (transport == null)
                {
                    if (_isClosed)
                    {
                        return;
                    }
                    SetState(ClientState.Reconnecting);
                    backoff = NextBackoff(backoff);
                    continue; // 拨号失败：退避后下一轮。
                }

                // 换代 + settle（钩子）+ 处置。返回 true = 本轮终结（Connected / 已关闭 /
                // 协议致命 terminate）——直接退出；false = 本代在 settle 期间死亡（网络类）
                // ——退避后重试（不置 Connected，避免 zombie，M4-4 评审 P1）。
                if (await TrySettleGenerationAsync(transport))
                {
                    return;
                }
                backoff = NextBackoff(backoff);
            }
        }
        finally
        {
            lock (_gate)
            {
                _reconnecting = false;
                if (_isClosed)
                {
                    SetState(ClientState.Disconnected);
                }
            }
        }
    }

    // TrySettleGenerationAsync 拨号成功后完成换代 + settle（重连钩子同步执行直至
    // 成功）+ 置 Connected/drain，并区分处置结果。返回 true = 本轮重连终结——调用方
    // 退出；false = 本代在 settle 期间/之后死亡（网络类，FailGeneration 已置
    // Disconnected + 记录 _generationFault）——调用方退避后重试，绝不置 Connected
    //（否则 zombie：Connected 但无连接/读循环/重连循环，M4-4 评审 P1）。
    private async Task<bool> TrySettleGenerationAsync(ITransport transport)
    {
        if (!InstallTransport(transport, out var epoch))
        {
            await CloseTransportAsync(transport);
            return true; // 已关闭。
        }
        if (!StartGenerationCore(transport, epoch))
        {
            return true; // 已关闭。
        }

        // settle：钩子同步执行直至成功。Settle 返回 false = 已关闭，或本代在钩子
        // 执行中因协议致命而死（terminate 不再重拨，P1 派生影响）。
        if (!await SettleHookWithRetryAsync(epoch))
        {
            return true;
        }
        if (CompleteReconnect())
        {
            return true; // 置 Connected + drain 成功，重连完成。
        }
        // CompleteReconnect false：已关闭，或本代在 settle 后死亡（读循环退出 +
        // FailGeneration 置 Disconnected）。区分处置：
        if (_isClosed)
        {
            return true;
        }
        if (IsProtocolFatalFault())
        {
            return true; // 本代协议致命：terminate 不重连（状态已 Disconnected）。
        }
        return false; // 网络类：调用方退避后重试。
    }

    // DialOneAttemptAsync 执行一轮退避睡眠 + 拨号。返回 null = 拨号失败或通道已
    // 关闭；调用方区分（_isClosed 决定退出还是退避重试）。
    private async Task<ITransport?> DialOneAttemptAsync(int backoff)
    {
        if (_isClosed)
        {
            return null;
        }
        await SleepInterruptibleAsync(backoff);
        if (_isClosed)
        {
            return null;
        }
        try
        {
            return await DialAsync(_closed.Token);
        }
        catch (AtlasException)
        {
            // DialAsync 失败会置 Disconnected（首连语义）；重连期间保持
            // Reconnecting——拨号失败只是本次退避轮次失败，由调用方重试。
            return null;
        }
    }

    // StartGenerationCore 换代启动新代读循环与心跳（调用方已 InstallTransport）。
    // 返回 false = 通道已关闭（调用方退出，不启动过期代）。
    private bool StartGenerationCore(ITransport transport, uint epoch)
    {
        lock (_gate)
        {
            if (_isClosed)
            {
                return false;
            }
            CancelGenerationLocked();
            _readLoop = Task.Run(() => ReadLoopAsync(transport, epoch));
            StartGenerationHeartbeat(epoch);
            return true;
        }
    }

    // CompleteReconnect 钩子成功后置 Connected + drain 排队队列（同临界区：排队
    // 请求严格先于新请求，对齐 Go settleGeneration 的 genMu 临界区）。
    // 返回 false = 通道已关闭，或本代已死（settle 期间新代读循环退出——FailGeneration
    // 已记录 _generationFault 并置 Disconnected）——此时绝不置 Connected，否则成 zombie
    //（Connected 但无连接/读循环/重连循环，M4-4 评审 P1）。调用方区分处置。
    private bool CompleteReconnect()
    {
        lock (_gate)
        {
            if (_isClosed)
            {
                return false;
            }
            if (_generationFault != null || _transport == null)
            {
                // 本代已死（读循环退出 + FailGeneration 置 Disconnected）：不得置
                // Connected——回到外层重连循环继续退避（或按致命原因终止）。
                return false;
            }
            SetState(ClientState.Connected);
            DrainQueueLocked();
            return true;
        }
    }

    // IsProtocolFatalFault 返回当前代是否因协议致命错误而死（帧非法/版本不匹配等，
    // 不可重试；对齐 Go ProtocolError 不重连语义）。锁内读 _generationFault。
    private bool IsProtocolFatalFault()
    {
        lock (_gate)
        {
            return _generationFault is ProtocolException;
        }
    }

    // SettleHookWithRetryAsync 同步执行重连钩子直至成功（对齐 Go settleGeneration）：
    // 每次钩子执行带 hookTimeout 上限 + 异常保护；失败（钩子返回错误/超时/panic）
    // 弃用本代连接（排队请求保留、未重发）、退避重连后再试——重复直至钩子成功、
    // 通道关闭或重连耗尽。Close 可随时打断（_closed 检查）。
    // 返回 false = 通道已关闭（调用方直接退出）。
    private async Task<bool> SettleHookWithRetryAsync(uint epoch)
    {
        while (true)
        {
            var hook = OnRelogin;
            if (hook == null)
            {
                return true; // 无钩子：直接 drain。
            }

            // 同步执行钩子（hookBypass 窗口）：重登请求直通当前代连接。
            var hookError = await RunHookSyncAsync(hook);
            if (hookError == null)
            {
                return true; // 钩子成功：调用方置 Connected + drain。
            }
            if (_isClosed)
            {
                return false; // Close 打断：退出。
            }
            if (IsProtocolFatalFault())
            {
                // 本代在钩子执行中因协议致命而死（如钩子 invoke 收到版本不匹配）：
                // terminate 不再重拨——否则 Abandon→Redial 会无限重拨（对齐 Go 协议
                // 致命不重连语义；M4-4 评审 P1 派生影响）。
                return false;
            }

            // 钩子失败（业务拒绝/超时/网络）：弃用本代连接（保留排队请求、未重发）、
            // 退避重连后再试。
            if (!await AbandonGenerationAfterHookFailureAsync())
            {
                return false;
            }
            var nextEpoch = await RedialAfterHookFailureAsync();
            if (nextEpoch == null)
            {
                return false; // 已关闭。
            }
            epoch = nextEpoch.Value;
        }
    }

    // AbandonGenerationAfterHookFailureAsync 弃用当前代连接：递增 epoch 使旧读循环/
    // 心跳失效（旧连接关闭触发的 FailGeneration 因 epoch 不匹配不再干扰新状态）、
    // 保留排队请求（未重发、无副作用）。返回 false = 通道已关闭（调用方退出）。
    private async Task<bool> AbandonGenerationAfterHookFailureAsync()
    {
        ITransport? stale;
        lock (_gate)
        {
            if (_isClosed)
            {
                return false;
            }
            stale = _transport;
            _transport = null;
            _epoch = NextNonZero(_epoch); // 换代：旧代读循环 FailGeneration 失效。
            _generationFault = null; // 弃用旧代：清除其退出原因记录（新代即将到来）。
            CancelGenerationLocked();
            _readLoop = null;
            SetState(ClientState.Reconnecting);
        }
        if (stale != null)
        {
            await CloseTransportAsync(stale); // 旧连接关闭：旧读循环因 EOF 自然退出。
        }
        return true;
    }

    // RedialAfterHookFailureAsync 钩子失败后退避重拨新连接并启动新代读循环/心跳。
    // 返回 null = 通道已关闭（调用方退出）；否则返回新代 epoch（调用方继续跑钩子）。
    private async Task<uint?> RedialAfterHookFailureAsync()
    {
        while (true)
        {
            await SleepInterruptibleAsync(Math.Max(_options.BackoffBaseMs, 1));
            if (_isClosed)
            {
                return null;
            }
            try
            {
                var transport = await DialAsync(_closed.Token);
                if (!InstallTransport(transport, out var epoch))
                {
                    await CloseTransportAsync(transport);
                    return null;
                }
                if (!StartGenerationCore(transport, epoch))
                {
                    return null;
                }
                return epoch;
            }
            catch (AtlasException)
            {
                if (_isClosed)
                {
                    return null;
                }
                // 拨号失败：置 Reconnecting 继续退避重试（对齐 Go reconnectFrom）。
                SetState(ClientState.Reconnecting);
            }
        }
    }

    // RunHookSyncAsync 同步执行重连钩子（带 hookTimeout 超时 + panic/异常保护）：
    // hookBypass 置位使钩子执行期间 Invoke 直通当前代连接；超时视为失败由调用方
    // 弃用本代连接；Close 可打断。返回 null = 成功，非 null = 失败原因（对齐 Go
    // runHookSync 的 select done/timer/closeCh 三路）。
    private async Task<Exception?> RunHookSyncAsync(Func<Task> hook)
    {
        SetHookBypass(true);
        try
        {
            using var timeout = new CancellationTokenSource(_options.HookTimeoutMs);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, _closed.Token);
            Task hookTask;
            try
            {
                hookTask = hook(); // hook 可能同步抛（如重登校验失败）——同样视为失败。
            }
            catch (Exception exception)
            {
                return exception is NetworkException
                    ? exception
                    : new NetworkException("重连钩子失败", exception);
            }
            var interrupt = Task.Delay(Timeout.Infinite, linked.Token);
            try
            {
                var completed = await Task.WhenAny(hookTask, interrupt);
                if (completed == interrupt && !hookTask.IsCompleted)
                {
                    // 超时或 Close 打断钩子（对齐 Go runHookSync 的 timer/closeCh 分支）。
                    return new NetworkException("重连钩子中断（超时或通道关闭）");
                }
                await hookTask; // 传播钩子内部异步异常。
                return null;
            }
            catch (Exception exception)
            {
                return exception is NetworkException
                    ? exception
                    : new NetworkException("重连钩子失败", exception);
            }
            finally
            {
                timeout.Cancel(); // 打断挂起的 interrupt Delay（释放资源）。
            }
        }
        finally
        {
            SetHookBypass(false);
        }
    }

    // NextBackoff 计算下一轮退避时长（×2 封顶）。
    private int NextBackoff(int current)
    {
        var next = checked(current * 2);
        if (next <= 0 || next > _options.BackoffMaxMs)
        {
            next = Math.Max(_options.BackoffMaxMs, 1);
        }
        return next;
    }

    // SleepInterruptibleAsync 可被关闭打断的退避睡眠（±20% 抖动，避免
    // 断线风暴下的重连同步；对齐 Go jitter + sleepInterruptible）。
    private readonly Random _jitterRandom = new();

    private async Task SleepInterruptibleAsync(int delayMs)
    {
        if (delayMs <= 0)
        {
            return;
        }
        var delta = Math.Max(delayMs / 5, 1);
        var jittered = delayMs - delta + _jitterRandom.Next(2 * delta + 1);
        if (jittered <= 0)
        {
            jittered = 1;
        }
        try
        {
            await Task.Delay(jittered, _closed.Token);
        }
        catch (OperationCanceledException)
        {
            // 关闭打断睡眠：由调用方检查 _isClosed 退出。
        }
    }

    // CancelGenerationLocked 换代时停旧代心跳（调用方持 _gate）。
    private void CancelGenerationLocked()
    {
        var old = _generationCts;
        _generationCts = new CancellationTokenSource();
        try
        {
            old.Cancel();
        }
        finally
        {
            old.Dispose();
        }
    }

    // DrainQueueLocked 重连成功后按序重发排队请求（FIFO）。调用方持 _gate，
    // 置 Connected 与 drain 同临界区——排队请求严格先于新请求（对标 Go drainQueue）。
    private void DrainQueueLocked()
    {
        while (_queue.Count > 0)
        {
            var queued = _queue.Dequeue();
            _ = ReplayQueuedAsync(queued);
        }
    }

    // ReplayQueuedAsync 重发一条排队请求（fire-and-forget：结果经 Completion 结算）。
    // 开头原子认领：已被看护（排队超时）认领的过期请求跳过重发——避免执行
    // 调用方已放弃的有副作用请求（对齐 Go drainQueue 跳过 q.claimed）。
    private async Task ReplayQueuedAsync(QueuedInvoke queued)
    {
        if (!queued.TryClaim())
        {
            return; // 已被看护认领（排队超时），结果已由看护投递。
        }
        try
        {
            var body = Body.BuildRequestBody(queued.Operation, queued.Payload);
            var request = RegisterInflight();
            try
            {
                await WriteRequestAsync(request, body, CancellationToken.None);
                var reply = await AwaitReplyAsync(request, CancellationToken.None);
                queued.Completion.TrySetResult(ToPayload(reply));
            }
            catch
            {
                RemoveInflight(request.Key, request.Inflight);
                throw;
            }
        }
        catch (AtlasException exception)
        {
            // 重发失败（罕见：新连接又断）——重连循环会再次触发；排队请求失败。
            queued.Completion.TrySetException(exception);
        }
    }

    // FailAllQueued 关闭时取消全部排队请求（对齐 Go failAllQueued）。
    private void FailAllQueued()
    {
        List<QueuedInvoke> all;
        lock (_gate)
        {
            all = new List<QueuedInvoke>(_queue);
            _queue.Clear();
        }
        foreach (var queued in all)
        {
            // 认领后结算：与看护（排队超时）互斥，恰好一次。
            if (queued.TryClaim())
            {
                queued.Completion.TrySetException(new NetworkException("通道已关闭"));
            }
        }
    }
}
