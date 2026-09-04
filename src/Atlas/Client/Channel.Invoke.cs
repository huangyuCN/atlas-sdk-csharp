using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using AtlasTimeoutException = Atlas.Errors.TimeoutException;

namespace Atlas.Client;

public sealed partial class Channel
{
    // InvokeRawAsync 发送原始 payload；payload 为 null 时只发送 operation，用于 Ping 等空请求。
    public async Task<byte[]> InvokeRawAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
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
            if (_isClosed || State != ClientState.Connected || _transport == null)
            {
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
                await DispatchFrameAsync(epoch, header, body);
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
        await CloseTransportAsync(transport);
    }

    private async Task DispatchFrameAsync(uint epoch, Header header, byte[] body)
    {
        if (header.Type == MsgType.Notify)
        {
            DispatchNotify(body);
            return;
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
        var inflight = TakeInflight(key);
        if (inflight == null)
        {
            return;
        }
        if (BeforeInflightCompletion != null)
        {
            await BeforeInflightCompletion();
        }
        inflight.Completion.TrySetResult(reply);
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
    // handler 在独立 Task 执行且异常被隔离，单 handler 崩溃不影响其他分发。
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
            _ = SafeNotifyAsync(handler, op, payload);
        }
    }

    // SafeNotifyAsync 单 handler 的保护执行：异常被捕获，不影响其他分发或读循环。
    private static async Task SafeNotifyAsync(NotifyHandler handler, string op, byte[] payload)
    {
        try
        {
            await Task.Yield();
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
