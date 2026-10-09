using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Transport;

namespace Atlas.Client;

// Channel 的重连钩子面：钩子的同步执行（hookBypass 直通窗口）、失败后弃用本代连接与
// 退避重拨、以及**重试有界**（HookMaxAttempts 超限即终止并通知上层）。
// 重连主循环与 settle 见 Reconnect.cs。
public sealed partial class Channel
{
    // SettleHookWithRetryAsync 同步执行重连钩子直至成功（对齐 Go settleGeneration）：
    // 每次钩子执行带 hookTimeout 上限 + 异常保护；失败（钩子返回错误/超时）弃用本代连接
    //（排队请求保留、未重发）、退避重连后再试。**重试有界**（HookMaxAttempts）：连续失败
    // 到上限即终止重连并通知 OnReconnectAborted——否则钩子持续失败时会无限退避重拨。
    // Close 可随时打断（_closed 检查）。
    // 返回 false = 通道已关闭或重连终止（调用方直接退出）。
    private async Task<bool> SettleHookWithRetryAsync()
    {
        var maxAttempts = Math.Max(_options.HookMaxAttempts, 1);
        var failedAttempts = 0;
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
            if (await ShouldStopHookRetryAsync(hookError, ++failedAttempts, maxAttempts).ConfigureAwait(false))
            {
                return false;
            }

            // 钩子失败（业务拒绝/超时/网络）：弃用本代连接（保留排队请求、未重发）、
            // 退避重连后再试。
            if (!await AbandonGenerationAfterHookFailureAsync())
            {
                return false;
            }
            if (await RedialAfterHookFailureAsync() == null)
            {
                return false; // 已关闭（或不可重试的拨号失败）。
            }
        }
    }

    // ShouldStopHookRetryAsync 判定是否终止钩子重试：通道已关闭 / 本代协议致命 / 重试超限。
    // 超限时顺带收口（弃用本代、置 Disconnected、回调上报），返回 true 令调用方退出。
    private async Task<bool> ShouldStopHookRetryAsync(
        Exception hookError, int failedAttempts, int maxAttempts)
    {
        if (_isClosed)
        {
            return true; // Close 打断：退出。
        }
        if (IsProtocolFatalFault())
        {
            // 本代在钩子执行中因协议致命而死（如钩子 invoke 收到版本不匹配）：
            // terminate 不再重拨——否则 Abandon→Redial 会无限重拨（对齐 Go 协议
            // 致命不重连语义；M4-4 评审 P1 派生影响）。
            return true;
        }
        if (failedAttempts < maxAttempts)
        {
            return false;
        }
        // 重试有界：钩子连续失败到上限即终止重连（不再无限退避重拨），
        // 并以上层回调上报「会话已终止，需重新匹配」。
        await AbortHookRetryAsync(hookError).ConfigureAwait(false);
        return true;
    }

    // AbortHookRetryAsync 钩子重试超限的收口：弃用本代连接（不再重拨）、置 Disconnected，
    // 并通知上层（OnReconnectAborted，异常隔离：回调异常不影响终止与状态）。
    private async Task AbortHookRetryAsync(Exception lastError)
    {
        if (!await AbandonGenerationAfterHookFailureAsync())
        {
            return; // 已关闭：关闭路径自行收口。
        }
        SetState(ClientState.Disconnected);
        var cause = lastError is NetworkException
            ? lastError
            : new NetworkException($"重连钩子重试超限（{_options.HookMaxAttempts} 次）", lastError);
        var callback = _options.OnReconnectAborted;
        if (callback == null)
        {
            return;
        }
        try
        {
            callback(cause);
        }
        catch (Exception)
        {
            // 上层回调异常隔离：不影响重连终止与通道状态。
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
                if (_isClosed || TryAbortReconnect())
                {
                    return null; // 已关闭，或不可重试的拨号失败：终止重连。
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
}
