using System;
using System.Threading;
using Atlas.Errors;

namespace Atlas.Battle;

// HeartbeatStats 是直连保活心跳的只读计数快照（不引入任何依赖，供上层自采指标）：
//   - Sent：成功送达的探针数（写出且未在写线处被拒）；
//   - Failures：网络/超时类失败数（写失败/回执未达；只计数，不影响会话状态）；
//   - Rejected：被服务端业务拒绝的探针数（每拍都计；日志/事件只在状态首次变化时出一条）；
//   - TicketRejected：其中票据类拒绝数（过期/无效：上层据此重新取票，会话不终态）；
//   - LastError：最近一次失败/拒绝的描述（排障；无失败为 null）。
public readonly struct HeartbeatStats
{
    internal HeartbeatStats(long sent, long failures, long rejected, long ticketRejected, string? lastError)
    {
        Sent = sent;
        Failures = failures;
        Rejected = rejected;
        TicketRejected = ticketRejected;
        LastError = lastError;
    }

    public long Sent { get; }

    public long Failures { get; }

    public long Rejected { get; }

    public long TicketRejected { get; }

    public string? LastError { get; }
}

// BattleSessionStats 是直连战斗会话的只读观测快照（重连/握手/心跳失败计数）：
//   - ConnectAttempts：拨号尝试数（首连 + 每次重拨，含失败）；
//   - ConnectFailures：拨号失败数；
//   - HandshakeFailures：其中握手类失败数（hello/WS 升级被接入层拒绝：ProtocolException）；
//   - Reconnects：成功完成的重连轮次数（自动重连成功 + 显式 ReconnectAsync）；
//   - RejoinFailures：重连钩子（重新入局 + 补帧）失败次数（业务拒绝与网络失败都计）；
//   - Heartbeat：保活探针统计（见 HeartbeatStats）。
public readonly struct BattleSessionStats
{
    internal BattleSessionStats(
        long connectAttempts,
        long connectFailures,
        long handshakeFailures,
        long reconnects,
        long rejoinFailures,
        HeartbeatStats heartbeat)
    {
        ConnectAttempts = connectAttempts;
        ConnectFailures = connectFailures;
        HandshakeFailures = handshakeFailures;
        Reconnects = reconnects;
        RejoinFailures = rejoinFailures;
        Heartbeat = heartbeat;
    }

    public long ConnectAttempts { get; }

    public long ConnectFailures { get; }

    public long HandshakeFailures { get; }

    public long Reconnects { get; }

    public long RejoinFailures { get; }

    public HeartbeatStats Heartbeat { get; }
}

// BattleSession 的观测面：只读计数（Interlocked 累加，无锁读快照）——
// 重连/握手/心跳的失败不再是「只有日志」，上层可直接采成指标（不引入依赖）。
public sealed partial class BattleSession
{
    private long _connectAttempts;
    private long _connectFailures;
    private long _handshakeFailures;
    private long _reconnects;
    private long _rejoinFailures;
    private long _heartbeatSent;
    private long _heartbeatFailures;
    private long _heartbeatRejected;
    private long _heartbeatTicketRejected;
    private string? _lastHeartbeatError;

    // Stats 返回本会话的只读观测快照（每次读取都是一份一致的历史计数）。
    public BattleSessionStats Stats => new(
        Interlocked.Read(ref _connectAttempts),
        Interlocked.Read(ref _connectFailures),
        Interlocked.Read(ref _handshakeFailures),
        Interlocked.Read(ref _reconnects),
        Interlocked.Read(ref _rejoinFailures),
        new HeartbeatStats(
            Interlocked.Read(ref _heartbeatSent),
            Interlocked.Read(ref _heartbeatFailures),
            Interlocked.Read(ref _heartbeatRejected),
            Interlocked.Read(ref _heartbeatTicketRejected),
            Volatile.Read(ref _lastHeartbeatError)));

    // CountDialAttempt 记一次拨号尝试（成功/失败都计：失败率 = ConnectFailures/ConnectAttempts）。
    private void CountDialAttempt()
    {
        Interlocked.Increment(ref _connectAttempts);
    }

    // CountDialFailure 记一次拨号失败（握手类失败另计：ProtocolException = 接入层拒绝 hello/升级）。
    private void CountDialFailure(Exception exception)
    {
        Interlocked.Increment(ref _connectFailures);
        if (exception is ProtocolException)
        {
            Interlocked.Increment(ref _handshakeFailures);
        }
    }

    // CountReconnect 记一次成功完成的重连轮次。
    private void CountReconnect()
    {
        Interlocked.Increment(ref _reconnects);
    }

    // CountRejoinFailure 记一次重连钩子（重新入局 + 补帧）失败。
    private void CountRejoinFailure()
    {
        Interlocked.Increment(ref _rejoinFailures);
    }

    // SetHeartbeatError 记最近一次心跳失败/拒绝的描述（便于排障时看最新一条）。
    private void SetHeartbeatError(Exception exception)
    {
        Volatile.Write(ref _lastHeartbeatError, exception.Message);
    }

    // NoteHeartbeatSent 记一次成功送达的探针。
    private void NoteHeartbeatSent()
    {
        Interlocked.Increment(ref _heartbeatSent);
    }

    // NoteHeartbeatFailure 记一次网络/超时类探针失败。
    private void NoteHeartbeatFailure()
    {
        Interlocked.Increment(ref _heartbeatFailures);
    }

    // NoteHeartbeatRejected 记一次被业务拒绝的探针（票类另计）。
    private void NoteHeartbeatRejected(bool ticketRejected)
    {
        Interlocked.Increment(ref _heartbeatRejected);
        if (ticketRejected)
        {
            Interlocked.Increment(ref _heartbeatTicketRejected);
        }
    }
}
