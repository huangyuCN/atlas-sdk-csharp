using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using AtlasTimeoutException = Atlas.Errors.TimeoutException;

namespace Atlas.Client;

// InvokeOptions 是调用级选项（逃生门）：显式幂等键 / 本次不携带。
// 等级日志的调用面在 ClientOptions.Logger（客户端级，对齐 Go WithLog* Option）。
public sealed class InvokeOptions
{
    // IdempotencyKey 显式指定幂等键（覆盖自动生成；按业务实体幂等，如以订单号为键）。
    public string? IdempotencyKey { get; set; }

    // NoIdempotency 使本次调用不携带幂等键（逃生门：高频无副作用调用省去 ID）。
    public bool NoIdempotency { get; set; }
}

// Channel 的调用面：入口重载、幂等键决策、重连排队与单次请求-响应往返。
// 组帧/在途表/读循环分发分别在 Channel.Frame.cs / Channel.Inflight.cs /
// Channel.ReadLoop.cs（同一 partial 类型，按职责分文件）。
public sealed partial class Channel
{
    // InvokeRawAsync 发送原始 payload；payload 为 null 时只发送 operation，用于 Ping 等空请求。
    // 重连期间（State=Reconnecting）默认排队等待重连成功后重发；可用 InvokeRawFailFastAsync
    // 跳过排队立即失败（如心跳的 failFast 直通路径）。
    public Task<byte[]> InvokeRawAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        return InvokeRawCoreAsync(operation, payload, cancellationToken, failFast: false);
    }

    // InvokeRawAsync（带调用级选项）：显式幂等键或本次不携带（逃生门，对齐 Go/TS 的
    // WithIdempotencyKey/WithNoIdempotency）；options 为 null 等价于无选项重载
    //（IAtlasInvoker 重载语义，生成 stub 透传）。
    public Task<byte[]> InvokeRawAsync(
        string operation, byte[]? payload, InvokeOptions? options, CancellationToken cancellationToken)
    {
        return InvokeRawCoreAsync(operation, payload, cancellationToken, failFast: false,
            idempotencyKey: options?.IdempotencyKey, noIdempotency: options?.NoIdempotency ?? false);
    }

    // InvokeRawFailFastAsync 重连期间不排队、立即失败（对齐 Go WithFailFast）。
    internal Task<byte[]> InvokeRawFailFastAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        return InvokeRawCoreAsync(operation, payload, cancellationToken, failFast: true);
    }

    // InvokeRawCoreAsync 是调用入口的唯一实现源：决定幂等键 → 钩子窗口直通 →
    // 重连排队 → 单次往返。
    private async Task<byte[]> InvokeRawCoreAsync(
        string operation,
        byte[]? payload,
        CancellationToken cancellationToken,
        bool failFast,
        string? idempotencyKey = null,
        bool noIdempotency = false)
    {
        // 幂等键在入口一次决定（重发/重试复用同一 ID；对齐 Go invoke 的入口决策）：
        // 显式逃生门 noIdempotency 优先，显式 idempotencyKey 次之，缺省自动生成。
        var requestID = noIdempotency ? null : idempotencyKey ?? NewRequestID();
        // 会话钩子同步执行期间（hookBypass，对齐 Go invoke.go）直通当前代连接：
        // 钩子的重登/重绑请求不排队（队列要等钩子成功后才 drain），传输心跳亦经此
        // 路径保活。已文档化的语义：该窗口内外部并发 Invoke 同样直写当前代连接（连接
        // 可用但可能尚未完成重登，调用方需自行容忍；窗口上限 = HookTimeoutMs）——
        // 无法按调用方区分钩子内外（对齐 Go 公开 API 约束下的既定取舍）。
        if (IsHookBypass())
        {
            return await InvokeOnceAsync(operation, payload, cancellationToken, requestID);
        }
        var queued = EnqueueIfReconnecting(operation, payload, failFast, requestID);
        if (queued != null)
        {
            StartQueueDeadlineWatch(queued);
            return await queued.Completion.Task;
        }
        return await InvokeOnceAsync(operation, payload, cancellationToken, requestID);
    }

    // EnqueueIfReconnecting 在 _gate 临界区原子完成「排队判定 + 入队」：drain 与入队
    // 互斥，不存在「drain 空队列后请求才入队」的永久遗留窗口（对齐 Go B4 修复）。
    // 返回 null = 不排队（非重连态或 failFast），由调用方直接发请求。
    private QueuedInvoke? EnqueueIfReconnecting(
        string operation, byte[]? payload, bool failFast, string? requestID)
    {
        lock (_gate)
        {
            if (_isClosed)
            {
                throw new NetworkException("通道已关闭");
            }
            if (State != ClientState.Reconnecting || failFast)
            {
                return null;
            }
            if (_queue.Count >= _options.QueueSize)
            {
                throw new NetworkException($"重连排队已满（{_options.QueueSize}）");
            }
            // 排队期限 = 单次超时（invokeTimeout）：到点未 drain 则由看护认领
            // 超时失败——排队阶段计入超时（对齐 Go enqueueLocked：不再无限等待重连）。
            var deadline = DateTime.UtcNow.AddMilliseconds(_options.InvokeTimeoutMs);
            var queued = new QueuedInvoke(
                operation,
                payload,
                new TaskCompletionSource<byte[]>(
                    TaskCreationOptions.RunContinuationsAsynchronously),
                deadline,
                requestID);
            _queue.Enqueue(queued);
            return queued;
        }
    }

    // StartQueueDeadlineWatch 启动排队超时看护：到点后原子认领并发送超时结果
    //（恰好一次语义——若已被 drain 认领则跳过；对齐 Go queueDeadlineWatch）。
    private static async void StartQueueDeadlineWatch(QueuedInvoke queued)
    {
        var delay = queued.Deadline - DateTime.UtcNow;
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay);
        }
        // 认领失败 = 已被 drain 重发消费（drain 忽略过期请求）。
        if (!queued.TryClaim())
        {
            return;
        }
        queued.Completion.TrySetException(
            new AtlasTimeoutException($"排队超时（{queued.Operation}）"));
    }

    // InvokeOnceAsync 执行一次请求-响应往返（组帧 → 注册在途 → 写帧 → 等回执 → 还原 payload）。
    private async Task<byte[]> InvokeOnceAsync(
        string operation,
        byte[]? payload,
        CancellationToken cancellationToken,
        string? requestID = null)
    {
        var (header, body) = BuildRequestFrame(operation, payload, requestID);
        _logger.Debugf("send op={0} id={1} req={2}", operation, requestID ?? "", Snippet(payload));
        var request = RegisterInflight();
        try
        {
            await WriteRequestAsync(request, header, body, cancellationToken);
            var reply = await AwaitReplyAsync(request, cancellationToken);
            _logger.Debugf("recv op={0} resp={1}", operation, Snippet(reply.Data));
            return ToPayload(reply);
        }
        catch
        {
            RemoveInflight(request.Key, request.Inflight);
            throw;
        }
    }
}
