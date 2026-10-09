using System;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Scheduling;
using Atlas.Transport;

namespace Atlas.Client;

// Channel 的读循环面：读帧 → 分发（响应匹配 / 推送分发）→ 退出结算与重连判定。
// 推送分发必须携带帧头载荷编码版本（ver=1 protojson / ver=2 protobuf wire）——
// 丢版本会让 ver=2 的推送在消费方按 protojson 解码而静默失败。
public sealed partial class Channel
{
    // ReadLoopAsync 是该代连接的生命周期管理者：读循环退出 → 结算在途 → 判定重连。
    private async Task ReadLoopAsync(ITransport transport, uint epoch)
    {
        var cause = await ReadFramesAsync(transport, epoch);

        FailGeneration(epoch, cause);

        // 陈旧代（已换代/已弃用）不驱动重连：本代的连接是被重连编排（钩子失败弃用、
        // 重试超限终止）关掉的，重连另有人在——若让它的读循环退出再起一轮，会让
        // 「重试有界」失效（每轮终止后又被旧连接凭空拉起一轮重连）。
        if (!IsCurrentGeneration(epoch))
        {
            await CloseTransportAsync(transport);
            return;
        }

        // M4：网络错误退出（非协议致命、非主动关闭）驱动自动重连。
        // 协议级致命错误（ProtocolException：版本不匹配/帧非法）直接终止、不重连
        //（对标 Go：不可重试、连接已断；ChannelTest.ResponseVersionMismatch 依赖此语义）。
        var shouldReconnect = EnterReconnecting(cause);
        await CloseTransportAsync(transport);
        if (shouldReconnect)
        {
            await RunReconnectLoopAsync();
        }
    }

    // ReadFramesAsync 是读循环主体：返回退出原因（主动关闭/网络错误/协议致命）。
    private async Task<Exception> ReadFramesAsync(ITransport transport, uint epoch)
    {
        try
        {
            while (!_closed.IsCancellationRequested)
            {
                var (header, body) = await transport.ReadFrameAsync(
                    _options.MaxBodySize,
                    _closed.Token);
                await DispatchByHookAsync(epoch, header, body);
            }
            return new NetworkException("通道已关闭");
        }
        catch (ProtocolException exception)
        {
            return exception;
        }
        catch (Exception exception)
        {
            return new NetworkException("读取帧失败", exception);
        }
    }

    // DispatchByHookAsync 快路径：无测试钩子时同步分发（避免每帧 async 状态机分配，评审
    // M2-3 note——帧率敏感）；有钩子（仅测试）才走 async 包装。
    private async Task DispatchByHookAsync(uint epoch, Header header, byte[] body)
    {
        if (BeforeInflightCompletion == null)
        {
            DispatchFrame(epoch, header, body);
            return;
        }
        await DispatchFrameAsync(epoch, header, body);
    }

    // EnterReconnecting 置 Reconnecting 与判定同临界区、在 FailGeneration 结算后立即完成
    //（其间无 await 间隙）：踢线后 in-flight 被结算（kick Invoke 返回 NetworkException），
    // 若此刻状态仍是 Disconnected 且到置 Reconnecting 前有异步间隙（关闭旧连接等），
    // 紧接发出的 Invoke 会看到 Disconnected 而不排队、直接打向已断连接。提前到锁内
    // 置位消除该窗口——对标 Go supervisor：<-g.done 后立即置 StateReconnecting 再重连。
    // 返回 true = 需要运行重连循环（网络类死亡且开启自动重连）。
    private bool EnterReconnecting(Exception cause)
    {
        lock (_gate)
        {
            var shouldReconnect = !_isClosed
                && _options.AutoReconnect
                && cause is not ProtocolException
                && !_reconnecting;
            if (shouldReconnect)
            {
                _reconnecting = true;
                SetState(ClientState.Reconnecting);
            }
            return shouldReconnect;
        }
    }

    // IsCurrentGeneration 返回该代是否仍是当前代（已关闭恒为 false）：陈旧代的读循环退出
    // 不得驱动重连（重连编排属于新代，或已按重试上限终止）。
    private bool IsCurrentGeneration(uint epoch)
    {
        lock (_gate)
        {
            return !_isClosed && _epoch == epoch;
        }
    }

    // DispatchFrameAsync 分发（测试钩子路径）：在「已取 key、未设结果」窗口暂停。
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

    // DispatchFrameCore 按帧类型分发：推送交订阅表、响应按 (epoch, seq) 匹配在途。
    private (Inflight? Inflight, ReplyData? Reply) DispatchFrameCore(
        uint epoch, Header header, byte[] body)
    {
        if (header.Type == MsgType.Notify)
        {
            DispatchNotify(header, body);
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

    // DispatchNotify 解析 Notify 帧并分发到全部订阅者（按 op 订阅 + 通配订阅）。帧体解析失败静默丢弃：
    // 推送非请求-响应匹配路径，坏帧不影响连接（对标 Go dispatchNotify）。
    // handler 经 AtlasScheduler 发布（默认线程池；注入调度器后在注入上下文执行）
    // 且异常被隔离，单 handler 崩溃不影响其他分发；帧头 version 随回调下发。
    private void DispatchNotify(Header header, byte[] body)
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
            _notifies.TryGetValue(op, out var entries);
            var opCount = entries?.Count ?? 0;
            var anyCount = _notifiesAny.Count;
            if (opCount == 0 && anyCount == 0)
            {
                return;
            }
            // 通配订阅（_notifiesAny：会话状态机的接缝推送识别）排在按 op 订阅之后。
            handlers = new NotifyHandler[opCount + anyCount];
            for (var i = 0; i < opCount; i++)
            {
                handlers[i] = entries![i].Handler;
            }
            for (var i = 0; i < anyCount; i++)
            {
                handlers[opCount + i] = _notifiesAny[i].Handler;
            }
        }
        var version = header.Version;
        foreach (var handler in handlers)
        {
            // handler 经 AtlasScheduler 发布（默认线程池；Unity 注入主线程
            // SynchronizationContext 后在主线程执行）——SafeNotify 内捕获异常。
            AtlasScheduler.Post(() => SafeNotify(handler, op, payload, version));
        }
    }

    // SafeNotify 单 handler 的保护执行：异常被捕获，不影响其他分发或读循环
    //（对标 Go safeNotify 的 recover）。在 AtlasScheduler 发布的回调内同步执行。
    private static void SafeNotify(NotifyHandler handler, string op, byte[] payload, byte version)
    {
        try
        {
            handler(op, payload, version);
        }
        catch (Exception)
        {
            // 单 handler 异常隔离（对标 Go safeNotify 的 recover）。
        }
    }
}
