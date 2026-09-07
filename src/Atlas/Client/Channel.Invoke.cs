using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Scheduling;
using Atlas.Transport;
using AtlasTimeoutException = Atlas.Errors.TimeoutException;

namespace Atlas.Client;

public sealed partial class Channel
{
    // InvokeRawAsync 发送原始 payload；payload 为 null 时只发送 operation，用于 Ping 等空请求。
    // 重连期间（State=Reconnecting）默认排队等待重连成功后重发；可用 InvokeRawFailFastAsync
    // 跳过排队立即失败（如心跳的 failFast 直通路径）。
    public Task<byte[]> InvokeRawAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        return InvokeRawCoreAsync(operation, payload, cancellationToken, failFast: false);
    }

    // InvokeRawFailFastAsync 重连期间不排队、立即失败（对齐 Go WithFailFast）。
    internal Task<byte[]> InvokeRawFailFastAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        return InvokeRawCoreAsync(operation, payload, cancellationToken, failFast: true);
    }

    private async Task<byte[]> InvokeRawCoreAsync(
        string operation,
        byte[]? payload,
        CancellationToken cancellationToken,
        bool failFast)
    {
        // 会话钩子同步执行期间（hookBypass，对齐 Go invoke.go）直通当前代连接：
        // 钩子的重登/重绑请求不排队（队列要等钩子成功后才 drain），传输心跳亦经此
        // 路径保活。已文档化的语义：该窗口内外部并发 Invoke 同样直写当前代连接（连接
        // 可用但可能尚未完成重登，调用方需自行容忍；窗口上限 = HookTimeoutMs）——
        // 无法按调用方区分钩子内外（对齐 Go 公开 API 约束下的既定取舍）。
        if (IsHookBypass())
        {
            return await InvokeOnceAsync(operation, payload, cancellationToken);
        }
        // 排队判定与入队在 _gate 临界区原子完成：drain 与入队互斥，
        // 不存在「drain 空队列后请求才入队」的永久遗留窗口（对齐 Go B4 修复）。
        QueuedInvoke? queued = null;
        lock (_gate)
        {
            if (_isClosed)
            {
                throw new NetworkException("通道已关闭");
            }
            var reconnecting = State == ClientState.Reconnecting;
            if (reconnecting && !failFast)
            {
                if (_queue.Count >= _options.QueueSize)
                {
                    throw new NetworkException($"重连排队已满（{_options.QueueSize}）");
                }
                // 排队期限 = 单次超时（invokeTimeout）：到点未 drain 则由看护认领
                // 超时失败——排队阶段计入超时（对齐 Go enqueueLocked：不再无限等待重连）。
                var deadline = DateTime.UtcNow.AddMilliseconds(_options.InvokeTimeoutMs);
                queued = new QueuedInvoke(
                    operation,
                    payload,
                    new TaskCompletionSource<byte[]>(
                        TaskCreationOptions.RunContinuationsAsynchronously),
                    deadline);
                _queue.Enqueue(queued);
            }
        }
        if (queued != null)
        {
            StartQueueDeadlineWatch(queued);
            return await queued.Completion.Task;
        }
        return await InvokeOnceAsync(operation, payload, cancellationToken);
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

    private async Task<byte[]> InvokeOnceAsync(
        string operation,
        byte[]? payload,
        CancellationToken cancellationToken)
    {
        var body = Body.BuildRequestBody(operation, payload);
        var request = RegisterInflight();
        try
        {
            await WriteRequestAsync(request, body, cancellationToken);
            var reply = await AwaitReplyAsync(request, cancellationToken);
            return ToPayload(reply);
        }
        catch
        {
            RemoveInflight(request.Key, request.Inflight);
            throw;
        }
    }

    private (InflightKey Key, Inflight Inflight, ITransport Transport) RegisterInflight()
    {
        lock (_gate)
        {
            // Reconnecting 期间不注册 in-flight（写死连接无意义，等完整超时）。
            // 例外：会话钩子执行期间（hookBypass，对齐 Go invokeOnce）放行——
            // 钩子的重登请求正是为建立会话，必须直通当前代连接。
            if (_isClosed || _transport == null)
            {
                throw new NetworkException("通道未连接");
            }
            if (!IsHookBypass() && State != ClientState.Connected)
            {
                // 非钩子窗口：仅 Connected 可注册（Connecting/Reconnecting/Disconnected
                // 均拒绝——Reconnecting 由排队层拦截，failFast 在此失败）。
                throw new NetworkException("通道未连接");
            }

            _sequence = NextNonZero(_sequence);
            var key = new InflightKey(_epoch, _sequence);
            var inflight = new Inflight();
            _inflight.Add(key, inflight);
            return (key, inflight, _transport);
        }
    }

    private async Task WriteRequestAsync(
        (InflightKey Key, Inflight Inflight, ITransport Transport) request,
        byte[] body,
        CancellationToken cancellationToken)
    {
        var acquired = false;
        try
        {
            await _writeLock.WaitAsync(cancellationToken);
            acquired = true;
            var header = new Header
            {
                Type = MsgType.Request,
                Version = (byte)_options.Serializer.Version,
                Seq = request.Key.Sequence,
            };
            await request.Transport.WriteFrameAsync(
                header,
                body,
                _options.MaxBodySize,
                cancellationToken);
        }
        catch (ProtocolException)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw new NetworkException("写帧失败", exception);
        }
        finally
        {
            if (acquired)
            {
                _writeLock.Release();
            }
        }
    }

    private async Task<ReplyData> AwaitReplyAsync(
        (InflightKey Key, Inflight Inflight, ITransport Transport) request,
        CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(_options.InvokeTimeoutMs);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        var interrupted = Task.Delay(Timeout.Infinite, linked.Token);
        var completed = await Task.WhenAny(request.Inflight.Completion.Task, interrupted);
        if (completed == request.Inflight.Completion.Task || request.Inflight.Completion.Task.IsCompleted)
        {
            return await request.Inflight.Completion.Task;
        }

        // 只有成功删除 key 的路径才拥有超时/取消结果；若响应或断连已先认领，
        // 必须等待同一个 Completion，避免「key 已删但 TCS 尚未置位」时误报超时。
        if (!RemoveInflight(request.Key, request.Inflight))
        {
            return await request.Inflight.Completion.Task;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            request.Inflight.Completion.TrySetException(
                new NetworkException("调用已取消", new OperationCanceledException(cancellationToken)));
        }
        else
        {
            request.Inflight.Completion.TrySetException(
                new AtlasTimeoutException($"调用 {request.Key.Sequence} 超时（{_options.InvokeTimeoutMs}ms）"));
        }
        return await request.Inflight.Completion.Task;
    }

    private static byte[] ToPayload(ReplyData reply)
    {
        if (reply.Status == null)
        {
            return reply.Data;
        }
        throw new BusinessException(
            reply.Status.Code,
            reply.Status.Reason,
            reply.Status.Message,
            reply.Status.Metadata);
    }

    private async Task ReadLoopAsync(ITransport transport, uint epoch)
    {
        Exception cause;
        try
        {
            while (!_closed.IsCancellationRequested)
            {
                var (header, body) = await transport.ReadFrameAsync(
                    _options.MaxBodySize,
                    _closed.Token);
                // 快路径：无测试钩子时同步分发（避免每帧 async 状态机分配，评审
                // M2-3 note——帧率敏感）；有钩子（仅测试）才走 async 包装。
                if (BeforeInflightCompletion == null)
                {
                    DispatchFrame(epoch, header, body);
                }
                else
                {
                    await DispatchFrameAsync(epoch, header, body);
                }
            }
            cause = new NetworkException("通道已关闭");
        }
        catch (ProtocolException exception)
        {
            cause = exception;
        }
        catch (Exception exception)
        {
            cause = new NetworkException("读取帧失败", exception);
        }

        FailGeneration(epoch, cause);

        // M4：网络错误退出（非协议致命、非主动关闭）驱动自动重连。
        // 协议级致命错误（ProtocolException：版本不匹配/帧非法）直接终止、不重连
        //（对标 Go：不可重试、连接已断；ChannelTest.ResponseVersionMismatch 依赖此语义）。
        //
        // 置 Reconnecting 与判定同临界区、在 FailGeneration 结算后立即完成（其间无
        // await 间隙）：踢线后 in-flight 被结算（kick Invoke 返回 NetworkException），
        // 若此刻状态仍是 Disconnected 且到置 Reconnecting 前有异步间隙（关闭旧连接等），
        // 紧接发出的 Invoke 会看到 Disconnected 而不排队、直接打向已断连接。提前到锁内
        // 置位消除该窗口——对标 Go supervisor：<-g.done 后立即置 StateReconnecting 再重连。
        var shouldReconnect = false;
        lock (_gate)
        {
            shouldReconnect = !_isClosed
                && _options.AutoReconnect
                && cause is not ProtocolException
                && !_reconnecting;
            if (shouldReconnect)
            {
                _reconnecting = true;
                SetState(ClientState.Reconnecting);
            }
        }
        await CloseTransportAsync(transport);
        if (shouldReconnect)
        {
            await RunReconnectLoopAsync();
        }
    }

    private async Task DispatchFrameAsync(uint epoch, Header header, byte[] body)
    {
        var (inflight, reply) = DispatchFrameCore(epoch, header, body);
        if (inflight == null)
        {
            return;
        }
        // 测试钩子：在「已取 key、未设结果」窗口暂停（制造超时/响应竞态窗口）。
        await BeforeInflightCompletion!();
        inflight.Completion.TrySetResult(reply!);
    }

    // DispatchFrame 同步分发（生产快路径，无测试钩子时零 async 状态机分配，评审
    // M2-3 note——帧率敏感热路径）。
    private void DispatchFrame(uint epoch, Header header, byte[] body)
    {
        var (inflight, reply) = DispatchFrameCore(epoch, header, body);
        if (inflight != null)
        {
            inflight.Completion.TrySetResult(reply!);
        }
    }

    private (Inflight? Inflight, ReplyData? Reply) DispatchFrameCore(
        uint epoch, Header header, byte[] body)
    {
        if (header.Type == MsgType.Notify)
        {
            DispatchNotify(body);
            return (null, null);
        }
        if (header.Type != MsgType.Response)
        {
            throw new ProtocolException($"客户端收到非法帧类型 {(byte)header.Type}");
        }
        if (header.Version != _options.Serializer.Version)
        {
            throw new ProtocolException($"响应帧 version {header.Version} 与载荷编码 {_options.Serializer.Version} 不一致");
        }

        var reply = Reply.DecodeReply(body);
        var key = new InflightKey(epoch, header.Seq);
        return (TakeInflight(key), reply);
    }

    private Inflight? TakeInflight(InflightKey key)
    {
        lock (_gate)
        {
            if (!_inflight.TryGetValue(key, out var inflight))
            {
                return null;
            }
            _inflight.Remove(key);
            return inflight;
        }
    }

    // DispatchNotify 解析 Notify 帧并分发到全部订阅者。帧体解析失败静默丢弃：
    // 推送非请求-响应匹配路径，坏帧不影响连接（对标 Go dispatchNotify）。
    // handler 经 AtlasScheduler 发布（默认线程池；注入调度器后在注入上下文执行）
    // 且异常被隔离，单 handler 崩溃不影响其他分发。
    private void DispatchNotify(byte[] body)
    {
        string op;
        byte[] payload;
        NotifyHandler[] handlers;
        try
        {
            (op, payload) = Body.ParseRequestBody(body);
        }
        catch (ProtocolException)
        {
            return; // 帧体解析失败静默丢弃（推送非匹配路径，坏帧不影响连接）。
        }
        lock (_notifyGate)
        {
            if (!_notifies.TryGetValue(op, out var entries))
            {
                return;
            }
            handlers = new NotifyHandler[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                handlers[i] = entries[i].Handler;
            }
        }
        foreach (var handler in handlers)
        {
            // handler 经 AtlasScheduler 发布（默认线程池；Unity 注入主线程
            // SynchronizationContext 后在主线程执行）——SafeNotify 内捕获异常。
            AtlasScheduler.Post(() => SafeNotify(handler, op, payload));
        }
    }

    // SafeNotify 单 handler 的保护执行：异常被捕获，不影响其他分发或读循环
    //（对标 Go safeNotify 的 recover）。在 AtlasScheduler 发布的回调内同步执行。
    private static void SafeNotify(NotifyHandler handler, string op, byte[] payload)
    {
        try
        {
            handler(op, payload);
        }
        catch (Exception)
        {
            // 单 handler 异常隔离（对标 Go safeNotify 的 recover）。
        }
    }

    private void FailGeneration(uint epoch, Exception cause)
    {
        var failed = new List<Inflight>();
        lock (_gate)
        {
            foreach (var pair in _inflight)
            {
                if (pair.Key.Epoch == epoch)
                {
                    failed.Add(pair.Value);
                }
            }
            foreach (var inflight in failed)
            {
                RemoveInflightLocked(inflight);
            }
            if (_epoch == epoch)
            {
                _transport = null;
                _generationFault = cause; // 本代已死：记录退出原因（settle 前核对用）。
                SetState(ClientState.Disconnected);
            }
        }
        foreach (var inflight in failed)
        {
            inflight.Completion.TrySetException(cause);
        }
    }

    private void FailAllInflight(Exception cause)
    {
        List<Inflight> failed;
        lock (_gate)
        {
            failed = new List<Inflight>(_inflight.Values);
            _inflight.Clear();
        }
        foreach (var inflight in failed)
        {
            inflight.Completion.TrySetException(cause);
        }
    }

    private bool RemoveInflight(InflightKey key, Inflight inflight)
    {
        lock (_gate)
        {
            if (_inflight.TryGetValue(key, out var current) && ReferenceEquals(current, inflight))
            {
                _inflight.Remove(key);
                return true;
            }
            return false;
        }
    }

    private void RemoveInflightLocked(Inflight inflight)
    {
        InflightKey? found = null;
        foreach (var pair in _inflight)
        {
            if (ReferenceEquals(pair.Value, inflight))
            {
                found = pair.Key;
                break;
            }
        }
        if (found.HasValue)
        {
            _inflight.Remove(found.Value);
        }
    }
}
