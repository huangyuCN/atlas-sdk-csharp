using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;

namespace Atlas.Battle;

// BattleSession 的装配与拨号面：面选择、按面直连接入层（WS query / KCP 先 hello 再会话 /
// UDP 首包 hello 换 flow-id）、通道装配（强制帧槽、失败分类、重连钩子）与重连后的重新入局。
public sealed partial class BattleSession
{
    // SelectFace 选面：优先面（配置）必须已下发；否则按 TransportPriority 取第一个已下发面；
    // 都没有即明确报错（不猜端口、不静默换面）。
    private static EdgeTransport SelectFace(DirectPlan plan, BattleSessionOptions options)
    {
        if (options.PreferredTransport.HasValue)
        {
            var preferred = options.PreferredTransport.Value;
            if (!plan.TryEndpoint(preferred, out _))
            {
                throw new ProtocolException(
                    $"{DirectErrors.TransportNotFound}: 优先面 {EdgeTransports.ShortName(preferred)}");
            }
            return preferred;
        }
        foreach (var face in options.TransportPriority)
        {
            if (plan.TryEndpoint(face, out _))
            {
                return face;
            }
        }
        throw new ProtocolException($"{DirectErrors.TransportNotFound}: 计划内的面都不在支持列表");
    }

    // NewChannel 装配一代直连通道：强制帧会话槽（逐帧带票）、重连钩子（重新入局 + 补帧）、
    // 拨号失败分类（接入层拒绝终止重连）与推送订阅（订阅挂在通道上，跨代际保留）。
    private Channel NewChannel()
    {
        var options = new ChannelOptions
        {
            TransportKind = Face switch
            {
                EdgeTransport.Ws => TransportKind.Ws,
                EdgeTransport.Kcp => TransportKind.Kcp,
                _ => TransportKind.Udp,
            },
            // 直连帧面逐帧带票：即使 WS 是长连接，经接入层透传后 battle 帧面仍按帧槽验票。
            ForceFrameSessionSlot = true,
            SessionTokenProvider = () => EdgeWire.TicketSlot(_plan.Ticket),
            Serializer = _options.Serializer,
            InvokeTimeoutMs = _options.InvokeTimeoutMs,
            HeartbeatIntervalMs = _options.HeartbeatIntervalMs,
            AutoReconnect = _options.AutoReconnect,
            BackoffBaseMs = _options.BackoffBaseMs,
            BackoffMaxMs = _options.BackoffMaxMs,
            QueueSize = _options.QueueSize,
            HookTimeoutMs = _options.HookTimeoutMs,
            Logger = _options.Logger,
            LogSilence = _options.LogSilence,
            LogSink = _options.LogSink,
            // 拨号期被接入层拒绝（票无效/过期）→ 终止重连：同一张废票重试必然再被拒。
            DialFailureAbortsReconnect = exception => exception is ProtocolException,
            OnReconnectAborted = OnReconnectAborted,
            // 终态双检：写锁内复核终态（组帧与写线之间）——终态置位后不再有新字节上线。
            WriteGuard = ThrowIfTerminal,
            HookMaxAttempts = _options.HookMaxAttempts,
        };
        var channel = new Channel(ChannelKind.Battle, DialAsync, options);
        // 推送统一入口：通配订阅（含未来新增 op），归口分发见 BattleSession.Push.cs。
        channel.OnAny(OnChannelPush);
        channel.OnRelogin = RejoinAsync;
        return channel;
    }

    // DialOverride 仅供 Atlas.Tests 注入假传输（断言「终态后零写入」「心跳停表」等线级事实），
    // 不改变生产拨号路径（null = 按选定面真实拨号）。
    internal Func<CancellationToken, Task<ITransport>>? DialOverride { get; set; }

    // DialAsync 按选定面直连接入层（每次重连都重新 hello；票仍来自成局通知）。
    // 终态（对局已结束 / 终态失败）拒绝拨号：重连循环据此终止（ProtocolException 走
    // DialFailureAbortsReconnect），于是收尾窗口内既不会重拨、也不会有任何线写入。
    // 每次尝试都计数（Stats.ConnectAttempts / ConnectFailures / HandshakeFailures）。
    private async Task<ITransport> DialAsync(CancellationToken cancellationToken)
    {
        if (HasEnded)
        {
            throw new ProtocolException(
                $"{DirectErrors.SessionEnded}（{EndedReason ?? ""}）：不再重连");
        }
        if (HasFailed)
        {
            throw new ProtocolException(
                $"{DirectErrors.SessionEnded}（{FailureCause?.Message ?? ""}）：会话已终止，不再重连");
        }
        CountDialAttempt();
        try
        {
            return await DialFaceAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            CountDialFailure(exception);
            throw;
        }
    }

    // DialFaceAsync 按选定面真正拨号（测试经 DialOverride 注入假传输，不改变生产路径）。
    private Task<ITransport> DialFaceAsync(CancellationToken cancellationToken)
    {
        if (DialOverride != null)
        {
            return DialOverride(cancellationToken);
        }
        switch (Face)
        {
            case EdgeTransport.Ws:
                var url = _options.TicketInHeader
                    ? EdgeWire.WsUrlPlain(Address, _options.WsPath)
                    : EdgeWire.WsUrl(Address, _plan.Ticket, _options.WsPath);
                var header = _options.TicketInHeader ? EdgeWire.TicketSlot(_plan.Ticket) : null;
                return WsTransport.ConnectDirectAsync(url, header, cancellationToken);
            case EdgeTransport.Kcp:
                var (kcpHost, kcpPort) = SplitAddress();
                return KcpTransport.ConnectDirectAsync(
                    kcpHost, kcpPort, _hello, _options.HelloTimeoutMs, cancellationToken);
            default:
                var (host, port) = SplitAddress();
                return UdpTransport.ConnectDirectAsync(
                    host, port, _hello, _options.HelloTimeoutMs, cancellationToken);
        }
    }

    // SplitAddress 拆分接入层地址 host:port（UDP/KCP 面需分开拨号）；非法即明确报错（不猜端口）。
    private (string Host, int Port) SplitAddress()
    {
        var address = Address;
        var separator = address.LastIndexOf(':');
        if (separator <= 0 || separator == address.Length - 1
            || !int.TryParse(address[(separator + 1)..], out var port)
            || port <= 0 || port > 65535)
        {
            throw new ProtocolException(
                $"{DirectErrors.NotifyNoEndpoint}: {EdgeTransports.ShortName(Face)} 地址非法（{address}）");
        }
        var host = address[..separator];
        // IPv6 字面量在地址里带方括号（[::1]:7100）：剥掉后方可交给 Socket。
        if (host.Length > 1 && host[0] == '[' && host[^1] == ']')
        {
            host = host[1..^1];
        }
        return (host, port);
    }

    // RejoinAsync 是重连钩子：新一代连接就绪后重新入局 + 以 LastSeenFrame 补帧。
    // 业务拒绝一律**不可重试**（同一张票/同一请求重发只会拿到同一拒绝），故：
    //   - BATTLE_ENDED（服务端在懒激活前先补投留档结果再拒绝）→ 终态收口（不上报 Failed）；
    //   - 票类（过期/无效）：会话已废，上层需重新取票 → 终态失败并上报 Failed；
    //   - 终态类（BATTLE_NOT_FOUND/BATTLE_FULL/FRAME_TARGET_MISMATCH）与其余业务拒绝
    //     （含 INVALID_PARAMS）→ 终态失败并上报 Failed——重试风暴与无限重连的根因就在这里。
    // 非业务失败（网络/超时）原样上抛，由 Channel 弃用本代连接、退避后重试（重试有界）。
    private async Task RejoinAsync()
    {
        try
        {
            await JoinBattleAsync().ConfigureAwait(false);
            await SyncFramesAsync(LastSeenFrame).ConfigureAwait(false);
            CountReconnect(); // 自动重连一轮成功（重新入局 + 补帧完成）。
        }
        catch (BusinessException exception) when (DirectErrors.IsBattleEnded(exception))
        {
            // 对局正常结束不是失败：不报 Failed（否则上层会被误导去重新匹配），只做终态收口。
            EnterEnded(DirectErrors.BattleEndedReason);
            await CloseChannelAsync().ConfigureAwait(false);
        }
        catch (BattleEndedException)
        {
            // 终态在钩子执行期间到达（结束信号与重连并发）：不再重新入局（发帧一律拒发），
            // 立即停重连——否则钩子会在终态里空转重试，直到收尾窗口关连接才退出。
            await CloseChannelAsync().ConfigureAwait(false);
        }
        catch (BusinessException exception)
        {
            CountRejoinFailure();
            await FailTerminalAsync(exception, exception.Code, exception.Reason).ConfigureAwait(false);
        }
        catch (Exception)
        {
            CountRejoinFailure();
            throw; // 网络/超时类：交给 Channel 弃用本代连接、退避后重试（重试次数有界）。
        }
    }

    // OnReconnectAborted 重连被接入层拒绝（不可重试）或重试超限而终止时通知上层：
    // 会话已终止，需重新匹配。终态（对局已结束/终态失败）触发的拨号拒绝不算新失败
    //——那是既定收口（不再重连），重复上报会让上层误以为需要重新匹配。
    private void OnReconnectAborted(Exception exception)
    {
        if (HasEnded || HasFailed)
        {
            return;
        }
        RaiseFailed(exception);
    }

    // FailSessionAsync 终止会话：停保活心跳（会话已废，探针不再有意义）→ 关通道并上报。
    private async Task FailSessionAsync(Exception exception)
    {
        await StopHeartbeatAsync().ConfigureAwait(false);
        await CloseAndReportAsync(exception).ConfigureAwait(false);
    }

    // CloseAndReportAsync 关闭通道（打断重连循环、结算剩余请求）并回调 Failed
    //（心跳循环内的终态收口走这里：不停表——循环随即返回即停表，避免自等待死锁）。
    private async Task CloseAndReportAsync(Exception exception)
    {
        await CloseChannelAsync().ConfigureAwait(false);
        RaiseFailed(exception);
    }

    // RaiseFailed 回调上层失败事件：回调异常被隔离（不反向影响重连/关闭路径）。
    private void RaiseFailed(Exception exception)
    {
        var handler = Failed;
        if (handler == null)
        {
            return;
        }
        try
        {
            handler(exception);
        }
        catch (Exception)
        {
            // 上层回调异常隔离。
        }
    }
}
