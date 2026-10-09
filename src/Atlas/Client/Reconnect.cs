using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;

namespace Atlas.Client;

// Channel 的重连编排面：退避拨号、换代 settle（重连钩子）、代死亡复核与关闭打断。
// 重连排队（入队/重发/超时/关闭结算）见 Channel.Queue.cs。
public sealed partial class Channel
{
    // _dialFault 是最近一次拨号失败原因（成功即清空；仅 _gate 外单线程重连路径读写）。
    private Exception? _dialFault;

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
                    if (TryAbortReconnect())
                    {
                        return; // 不可重试的拨号失败（接入层拒绝）：终止，不无限重拨。
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

        // settle：钩子同步执行直至成功。Settle 返回 false = 已关闭、协议致命或重试超限
        //（terminate 不再重拨）。
        if (!await SettleHookWithRetryAsync())
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

    // TryAbortReconnect 处理「不可重试的拨号失败」（判定函数由调用方配置，如接入层拒绝
    // hello：票据无效/过期，同一张票重试必然再被拒）：置 Disconnected、回调上层
    //（OnReconnectAborted，异常隔离），返回 true 令调用方终止重连。
    // 判定函数未配置（null）或判定为可重试时恒返回 false——老路径行为不变。
    private bool TryAbortReconnect()
    {
        var fault = _dialFault;
        var classify = _options.DialFailureAbortsReconnect;
        if (fault == null || classify == null || !classify(fault))
        {
            return false;
        }
        SetState(ClientState.Disconnected);
        var callback = _options.OnReconnectAborted;
        if (callback != null)
        {
            try
            {
                callback(fault);
            }
            catch (Exception)
            {
                // 上层回调异常隔离：不影响重连终止与通道状态。
            }
        }
        return true;
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
}
