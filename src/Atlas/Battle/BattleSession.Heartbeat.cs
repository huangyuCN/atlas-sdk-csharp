using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;

namespace Atlas.Battle;

// BattleSession 的直连保活心跳（battle 域探针）：无输入期间按 HeartbeatInterval 周期
// 发送 battle.v1.BattleService/Ping（Tell：服务端不回业务回执），让帧面保持活跃——
// 数据报面（UDP/KCP）的活跃判定靠帧面空闲读超时发现（服务端取 offline_timeout/3），
// 长时间静默会被判掉线，且 NAT 映射失效后下行帧收不到（「连接还在但没数据」）。
//
// 语义要点：
//   - 周期到点才发（不是连接即发）；TimeSpan.Zero = 关闭（不发、不建定时器）；
//   - 失败只记账（HeartbeatStats + 事件），**不终止会话**、不影响正常发帧；
//   - 未连接（重连窗口内）本拍静默跳过：无连接时心跳无事可证，重连编排负责恢复；
//   - 被业务拒绝三分类处置（见 HandleHeartbeatRejectionAsync）：终态类入终态、票类报
//    「需重新取票」、其余只计数继续探测；日志/事件只在状态首次变化时出一条（不刷屏）；
//   - CloseAsync / 会话终态即停表并等循环退出，不留悬挂定时器。
public sealed partial class BattleSession
{
    // _heartbeatGate 保护起停竞态（CloseAsync 与 ConnectAsync 可能并发调用）。
    private readonly object _heartbeatGate = new();
    private CancellationTokenSource? _heartbeatCts;
    private Task? _heartbeatTask;

    // _heartbeatRejectReason 是已记录（日志 + 事件）的最近一次业务拒绝 reason：
    // 同一 reason 只在首次记录，reason 变化（新状态）再记一条——否则每 2s 一拍刷屏。
    private string? _heartbeatRejectReason;

    // HeartbeatFailed 是探针失败回调（网络/超时/业务拒绝）：上层据此观测保活质量；
    // 回调异常被隔离，失败本身不终止会话（掉线判定在服务端，SDK 不擅自断开）。
    // 业务拒绝只在状态首次变化时上报一次（计数见 Stats.Heartbeat.Rejected）。
    public event Action<Exception>? HeartbeatFailed;

    // TicketRejected 是探针被**票据类**业务拒绝的信号（过期/无效）：上层据此回业务链路
    // 重新取票；会话不因此终态（票能换新的，链路是好的）。同一 reason 只上报一次。
    public event Action<Exception>? TicketRejected;

    // PingPayload 组装探针请求载荷（protojson：{"battleId":"..."}）：只带客体寻址字段，
    // 不承载任何对局参数——探针语义就是「我还在」（加参数即越界）。
    public byte[] PingPayload()
    {
        return Utf8("{\"battleId\":" + Quote(_plan.BattleId) + "}");
    }

    // StartHeartbeat 启动保活心跳（连接成功后调用；幂等：已在跑即复用；非正周期 = 关闭）。
    private void StartHeartbeat()
    {
        if (_options.HeartbeatInterval <= TimeSpan.Zero)
        {
            return;
        }
        lock (_heartbeatGate)
        {
            if (_heartbeatCts != null)
            {
                return;
            }
            var cts = new CancellationTokenSource();
            _heartbeatCts = cts;
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(cts.Token));
        }
    }

    // StopHeartbeatAsync 停表并等循环退出（幂等、可重入）：CloseAsync 与终止路径调用。
    // 取消令牌使周期等待即时返回；取消中在途的探针按取消退出，不算失败。
    private async Task StopHeartbeatAsync()
    {
        CancellationTokenSource? cts;
        Task? task;
        lock (_heartbeatGate)
        {
            cts = _heartbeatCts;
            task = _heartbeatTask;
            _heartbeatCts = null;
            _heartbeatTask = null;
        }
        if (cts != null)
        {
            cts.Cancel();
        }
        if (task != null)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // 心跳退出异常不向上传播（关闭路径以关闭成功为准）。
            }
        }
        cts?.Dispose();
    }

    // HeartbeatLoopAsync 是保活周期循环：等待一拍 → 发探针 → 记账 → 再等。
    // 退出条件：令牌取消（关闭）、终态（对局已结束/终态失败）、或探针被终态类拒绝。
    private async Task HeartbeatLoopAsync(CancellationToken token)
    {
        while (await HeartbeatDelayAsync(token).ConfigureAwait(false))
        {
            if (HasEnded || HasFailed)
            {
                return; // 终态：停表退出（不再探活）。
            }
            try
            {
                await SendPingAsync(token).ConfigureAwait(false);
                NoteHeartbeatSent();
                _logger.Debugf("heartbeat ok op={0}", BattleOps.Ping);
            }
            catch (OperationCanceledException)
            {
                return; // 关闭/终止取消：心跳随会话退出，不计失败。
            }
            catch (BattleEndedException)
            {
                return; // 本拍起表前已进入终态：静默退出（拒发不是链路失败）。
            }
            catch (BusinessException exception)
            {
                if (HasEnded || HasFailed)
                {
                    return; // 终态在等待期间到达：本拍未写线（本地拒发），不计拒绝。
                }
                if (await HandleHeartbeatRejectionAsync(exception).ConfigureAwait(false))
                {
                    return; // 终态类：已收口，停表。
                }
            }
            catch (Exception exception)
            {
                NoteHeartbeatFailure();
                RecordHeartbeatFailure(exception);
            }
        }
    }

    // HeartbeatDelayAsync 等待下一拍：令牌取消（关闭/终止）返回 false，调用方退出循环。
    private async Task<bool> HeartbeatDelayAsync(CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return false;
        }
        try
        {
            await Task.Delay(_options.HeartbeatInterval, token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        return !token.IsCancellationRequested;
    }

    // HandleHeartbeatRejectionAsync 处置「探针被业务拒绝」（三分类，与三 SDK 钉死口径一致）：
    //   - 终态类（BATTLE_ENDED / BATTLE_NOT_FOUND / BATTLE_FULL / FRAME_TARGET_MISMATCH）：
    //     入终态（正常结束或终态失败）并停表——返回 true；
    //   - 票类（过期/无效）：**不终态**——等待上层重新取票（票能换新的，链路是好的），
    //     此期间每拍被拒**只计数** + 首见上报一次「需重新取票」信号（日志仅首次），
    //     避免日志/事件风暴；探测继续（不停表、不重连）。
    //   - 其它业务拒绝：只计数 + 经心跳失败出口暴露（日志/事件仅首次状态变化），继续探测
    //    （不重连：探针被拒不是链路故障，重连救不了）。
    private async Task<bool> HandleHeartbeatRejectionAsync(BusinessException exception)
    {
        var ticketRejected = DirectErrors.IsTicketRejected(exception);
        NoteHeartbeatRejected(ticketRejected);
        SetHeartbeatError(exception);
        if (DirectErrors.IsBattleEnded(exception))
        {
            // 服务端判该局已结束：进入终态并停表（终态不是链路失败，不上报失败出口）。
            EnterEnded(DirectErrors.BattleEndedReason);
            return true;
        }
        if (DirectErrors.IsTerminalFailure(exception))
        {
            // 终态失败类（入局被拒/目标失配/参数非法）：会话已无用——终态化并上报 Failed。
            // fromHeartbeat：本方法就在心跳循环里，不能自等心跳任务（循环随即返回即停表）。
            await FailTerminalAsync(exception, exception.Code, exception.Reason, fromHeartbeat: true)
                .ConfigureAwait(false);
            return true;
        }
        ReportHeartbeatRejection(exception, ticketRejected);
        return false;
    }

    // ReportHeartbeatRejection 上报一次被拒的探针（仅状态首次变化）：
    // 票类走「需重新取票」信号（TicketRejected），其余走心跳失败出口（HeartbeatFailed）；
    // 同一 reason 只上报一次 + 只打一条 Warn，reason 变化再各出一条（计数逐拍累加，见 Stats）。
    private void ReportHeartbeatRejection(BusinessException exception, bool ticketRejected)
    {
        // 状态键区分「票类信号」与「其它拒绝」两条出口；同一出口内按 reason 去重。
        var state = (ticketRejected ? "ticket:" : "rejected:") + exception.Reason;
        if (Interlocked.Exchange(ref _heartbeatRejectReason, state) == state)
        {
            return; // 同一状态重复出现：只计数，不重复打日志/事件。
        }
        _logger.Warnf("heartbeat rejected op={0} reason={1} code={2}: {3}",
            BattleOps.Ping, exception.Reason, exception.Code, exception.Message);
        RaiseHandler(ticketRejected ? TicketRejected : HeartbeatFailed, exception);
    }

    // SendPingAsync 发一拍探针：终态（对局已结束/终态失败）拒发；未连接（重连窗口内）跳过；
    // 直通路径不排队（死链/重连期间排队无意义，探针不是业务请求），与正常发帧共用通道写锁。
    private Task<byte[]> SendPingAsync(CancellationToken token)
    {
        ThrowIfClosed();
        ThrowIfTerminal();
        var channel = Volatile.Read(ref _channel);
        if (channel == null || channel.State != ClientState.Connected)
        {
            return Task.FromResult(Array.Empty<byte>());
        }
        return channel.InvokeRawFailFastAsync(BattleOps.Ping, PingPayload(), token);
    }

    // RecordHeartbeatFailure 记一次网络/超时类探针失败：Warn 日志 + 事件（回调异常隔离）。
    private void RecordHeartbeatFailure(Exception exception)
    {
        SetHeartbeatError(exception);
        _logger.Warnf("heartbeat failed op={0}: {1}", BattleOps.Ping, exception.Message);
        RaiseHandler(HeartbeatFailed, exception);
    }

    // RaiseHandler 回调上层事件（异常隔离：不影响心跳循环与发帧）。
    private static void RaiseHandler(Action<Exception>? handler, Exception exception)
    {
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
