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
    // Invoke 排队；成功后启动新代读循环/心跳并 drain 排队队列。
    // 调用方（ReadLoopAsync）已在锁内原子置 _reconnecting=true + Reconnecting
    //（消除 Disconnected 悬置窗口，见 ReadLoopAsync）；本方法直接进入退避循环，
    // 退出时（成功/关闭/放弃）由 finally 清 _reconnecting。
    private async Task RunReconnectLoopAsync()
    {
        var backoff = Math.Max(_options.BackoffBaseMs, 1);
        try
        {
            while (true)
            {
                if (_isClosed)
                {
                    return;
                }
                await SleepInterruptibleAsync(backoff);
                if (_isClosed)
                {
                    return;
                }

                ITransport transport;
                try
                {
                    transport = await DialAsync(_closed.Token);
                }
                catch (AtlasException)
                {
                    if (_isClosed)
                    {
                        return;
                    }
                    // DialAsync 失败会置 Disconnected（首连语义）；重连期间保持
                    // Reconnecting——拨号失败只是本次退避轮次失败，继续重试。
                    SetState(ClientState.Reconnecting);
                    backoff = NextBackoff(backoff);
                    continue;
                }

                if (!InstallTransport(transport, out var epoch))
                {
                    await CloseTransportAsync(transport);
                    return;
                }

                // 换代：停旧代心跳，启动新代读循环与心跳。
                lock (_gate)
                {
                    if (_isClosed)
                    {
                        return;
                    }
                    CancelGenerationLocked();
                    _readLoop = Task.Run(() => ReadLoopAsync(transport, epoch));
                    StartGenerationHeartbeat(epoch);
                    SetState(ClientState.Connected);
                }
                DrainQueueLocked();
                return;
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
