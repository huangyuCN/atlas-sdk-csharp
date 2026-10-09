using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using AtlasTimeoutException = Atlas.Errors.TimeoutException;

namespace Atlas.Client;

// Channel 的组帧与在途请求管理：请求帧封装（会话槽/幂等键）、在途表登记、
// 写帧、等待回执与回执还原。读循环分发见 Channel.ReadLoop.cs。
public sealed partial class Channel
{
    // BuildRequestFrame 组请求帧（header + body）：帧级会话槽开启（UDP/KCP）且
    // 凭据提供者就绪时，凭据非空则置位 FrameGen.FlagSession 并以带槽布局封装
    //（[opLen][op][sessionLen][session][payload]）；凭据为空发匿名帧（旧布局、
    // 不置位）；长连接（TCP/WS）恒走旧布局（对齐 Go invokeOnce 组帧）。
    private (Header Header, byte[] Body) BuildRequestFrame(string operation, byte[]? payload, string? requestID)
    {
        var header = new Header
        {
            Type = MsgType.Request,
            Version = (byte)_options.Serializer.Version,
        };
        if (_frameSessionSlot && _options.SessionTokenProvider != null)
        {
            var token = _options.SessionTokenProvider();
            if (!string.IsNullOrEmpty(token))
            {
                header.Flags = FrameGen.FlagSession;
            }
        }
        if (!string.IsNullOrEmpty(requestID))
        {
            header.Flags |= FrameGen.FlagRequestID;
        }
        var slotToken = _frameSessionSlot && _options.SessionTokenProvider != null
            ? _options.SessionTokenProvider()
            : null;
        return (header, Body.BuildRequestBodyFull(operation, slotToken, requestID, payload));
    }

    // Snippet 取 payload 调试摘要（Debug 日志用：完整 JSON 截断 512 字节，防日志爆炸）。
    private static string Snippet(byte[]? data)
    {
        if (data == null || data.Length == 0)
        {
            return "{}";
        }
        var text = System.Text.Encoding.UTF8.GetString(data);
        return text.Length > 512 ? text[..512] + "...(truncated)" : text;
    }

    // NewRequestID 生成请求幂等键（RNGCryptoServiceProvider 12 字节 base64url，无外部依赖）。
    private static string NewRequestID()
    {
        var b = new byte[12];
        using (var rng = System.Security.Cryptography.RandomNumberGenerator.Create())
        {
            rng.GetBytes(b);
        }
        return Convert.ToBase64String(b).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    // RegisterInflight 登记一条在途请求（分配代内序号 + 建完成源），返回 (key, 在途, 传输)。
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

    // WriteRequestAsync 在写锁内复核写线资格、补齐帧头 seq 并写帧：
    //   - WriteGuard（战斗会话的终态双检）在**写锁内、真正写帧之前**调用——终态置位后
    //     不会再有任何新字节上线（已在写锁内的那一笔不回滚，对齐 Go writeRequest 双检）；
    //   - Atlas 异常（协议错误/终态拒绝）原样上抛，其余写失败按网络错误包装。
    private async Task WriteRequestAsync(
        (InflightKey Key, Inflight Inflight, ITransport Transport) request,
        Header header,
        byte[] body,
        CancellationToken cancellationToken)
    {
        var acquired = false;
        try
        {
            await _writeLock.WaitAsync(cancellationToken);
            acquired = true;
            _options.WriteGuard?.Invoke();
            header.Seq = request.Key.Sequence;
            await request.Transport.WriteFrameAsync(
                header,
                body,
                _options.MaxBodySize,
                cancellationToken);
        }
        catch (AtlasException)
        {
            throw; // 已分类的异常（协议/终态/网络）原样上抛，不二次包装。
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

    // AwaitReplyAsync 等待回执或超时/取消；只有成功删除 key 的路径才拥有超时/取消结果。
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

    // ToPayload 还原回执：无 Status 返回业务数据，有 Status 抛业务错误（Reason 为主键）。
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
            reply.Status.Metadata,
            reply.Status.Class);
    }
}
