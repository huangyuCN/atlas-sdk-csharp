using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

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
        DateTime deadline,
        string? requestID = null)
    {
        Operation = operation;
        Payload = payload;
        Completion = completion;
        Deadline = deadline;
        RequestID = requestID;
    }

    public string Operation { get; }

    public byte[]? Payload { get; }

    // RequestID 是幂等键：drain 重发复用同一 ID（服务端去重窗口内不重复执行）。
    public string? RequestID { get; }

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

// Channel 的重连排队面：入队（见 Channel.Invoke.cs）、成功后按序重发、关闭时结算。
public sealed partial class Channel
{
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
            // 组帧经 BuildRequestFrame：会话槽凭据在重发时点快照（新连接新凭据），
            // 对齐 Go drainQueue → invokeOnce 的重发组帧语义。
            var (header, body) = BuildRequestFrame(queued.Operation, queued.Payload, queued.RequestID);
            var request = RegisterInflight();
            try
            {
                await WriteRequestAsync(request, header, body, CancellationToken.None);
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
