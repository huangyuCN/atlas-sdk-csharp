using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionHeartbeatRejectTest 覆盖「保活探针被业务拒绝」的三分类口径
//（与 Go/TS 钉死一致）与心跳可观测性：
//   - 终态类（BATTLE_ENDED / BATTLE_NOT_FOUND / BATTLE_FULL / FRAME_TARGET_MISMATCH）：
//     入终态、停表、上报（结束类不报 Failed，其余报 Failed 一次）；
//   - 票类（过期/无效）：**不终态**，上报「需重新取票」信号 + 计数，继续探测；
//   - 其它业务拒绝：只计数 + 心跳失败出口暴露，继续探测（不重连）；
//   - 日志/事件只在状态首次变化时出一条：不再每 2s 一拍刷屏（消除风暴），计数逐拍累加。
public sealed class BattleSessionHeartbeatRejectTest
{
    // Ticket 是测试票据密文（非真实 AEAD 票：接入层/帧面只按字节透传与编码）。
    private static readonly byte[] Ticket = { 0xfb, 0xff, 0x00, 0x01, 0xfe, 0x10, 0x20 };

    private static DirectPlan Plan(int port)
    {
        var encoded = Convert.ToBase64String(Ticket);
        return DirectPlan.FromNotify("{\"match_id\":\"m-1\",\"battle_id\":\"b-1\",\"battle_ticket\":\""
            + encoded + "\",\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_UDP\",\"address\":\"127.0.0.1:"
            + port + "\"}]}");
    }

    private static BattleSessionOptions Options(TimeSpan interval)
    {
        return new BattleSessionOptions
        {
            PreferredTransport = EdgeTransport.Udp,
            HelloTimeoutMs = 3000,
            InvokeTimeoutMs = 3000,
            BackoffBaseMs = 30,
            BackoffMaxMs = 100,
            HeartbeatInterval = interval,
        };
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, int timeoutMs = 3000)
    {
        var watch = Stopwatch.StartNew();
        while (watch.ElapsedMilliseconds < timeoutMs)
        {
            if (condition())
            {
                return true;
            }
            await Task.Delay(10);
        }
        return condition();
    }

    private static int PingCount(BattleEdgeUdpServer server)
    {
        return server.RequestsOf(BattleOps.Ping).Length;
    }

    // 终态类拒绝（入局被拒/目标失配）：探针一拍即入终态并上报 Failed 一次，探针停发。
    [Theory]
    [InlineData(404, DirectErrors.BattleNotFoundReason)]
    [InlineData(409, DirectErrors.BattleFullReason)]
    [InlineData(403, DirectErrors.FrameTargetMismatchReason)]
    public async Task Heartbeat_TerminalClassRejection_TerminalizesAndReportsFailedOnce(int code, string reason)
    {
        await using var server = new BattleEdgeUdpServer();
        server.FailWith(BattleOps.Ping, code, reason, "探针被拒");
        await using var session = BattleSession.Create(
            Plan(server.Port), Options(TimeSpan.FromMilliseconds(80)));
        var failures = 0;
        session.Failed += _ => Interlocked.Increment(ref failures);

        await session.ConnectAsync();
        Assert.True(await WaitUntilAsync(() => session.HasFailed), "终态类拒绝应终态化");
        Assert.Equal(1, Volatile.Read(ref failures)); // 上报一次。
        Assert.Equal(reason, (session.FailureCause as BusinessException)?.Reason);

        await Task.Delay(150); // 排空终态瞬间在途的一拍。
        var pings = PingCount(server);
        await Task.Delay(350); // ≥ 4 个心跳周期。
        Assert.Equal(pings, PingCount(server)); // 停表：不再发探针。
        Assert.True(session.Stats.Heartbeat.Rejected >= 1, "被拒探针应计数");
        Assert.False(session.HasEnded); // 不是「对局已结束」：终态失败另有标记。
    }

    // 票类拒绝：不终态，上报「需重新取票」信号 + 计数（日志仅首次），继续探测。
    [Fact]
    public async Task Heartbeat_TicketRejection_NotTerminal_ReportsOnceAndKeepsProbing()
    {
        await using var server = new BattleEdgeUdpServer();
        server.FailWith(BattleOps.Ping, 401, DirectErrors.TicketExpiredReason, "票据已过期");
        var options = Options(TimeSpan.FromMilliseconds(80));
        var logs = new List<string>();
        options.Logger = SDKLoggerFactory.Of(LogLevel.Warn, line =>
        {
            lock (logs)
            {
                logs.Add(line);
            }
        });
        await using var session = BattleSession.Create(Plan(server.Port), options);
        var tickets = 0;
        var failures = 0;
        session.TicketRejected += _ => Interlocked.Increment(ref tickets);
        session.HeartbeatFailed += _ => Interlocked.Increment(ref failures);

        await session.ConnectAsync();
        Assert.True(await WaitUntilAsync(() => PingCount(server) >= 3), "票类拒绝后应继续探测");
        Assert.Equal(1, Volatile.Read(ref tickets)); // 信号只上报一次（状态未变化）。
        Assert.Equal(0, Volatile.Read(ref failures)); // 票类走专用出口，不混进通用失败。
        Assert.False(session.HasFailed);
        Assert.False(session.HasEnded);
        Assert.Equal(ClientState.Connected, session.State);
        Assert.True(session.Stats.Heartbeat.TicketRejected >= 2, "每拍都被拒：计数逐拍累加");
        Assert.True(session.Stats.Heartbeat.Rejected >= 2);

        // 日志只在状态首次变化时打一条：不随每拍刷屏。
        await Task.Delay(300); // ≥ 3 个周期。
        lock (logs)
        {
            Assert.Equal(1, logs.Count(line => line.Contains("heartbeat rejected", StringComparison.Ordinal)));
        }
    }

    // 其它业务拒绝：只计数 + 心跳失败出口暴露（仅首次），继续探测，不重连、不刷屏。
    [Fact]
    public async Task Heartbeat_OtherBusinessRejection_CountsAndContinuesProbing_WithoutLogStorm()
    {
        await using var server = new BattleEdgeUdpServer();
        server.FailWith(BattleOps.Ping, 500, "INTERNAL", "探针失败");
        var options = Options(TimeSpan.FromMilliseconds(80));
        var logs = new List<string>();
        options.Logger = SDKLoggerFactory.Of(LogLevel.Warn, line =>
        {
            lock (logs)
            {
                logs.Add(line);
            }
        });
        await using var session = BattleSession.Create(Plan(server.Port), options);
        var failures = 0;
        session.HeartbeatFailed += _ => Interlocked.Increment(ref failures);

        await session.ConnectAsync();
        Assert.True(await WaitUntilAsync(() => PingCount(server) >= 3), "其它业务拒绝后应继续探测");
        Assert.Equal(1, Volatile.Read(ref failures)); // 事件只在状态首次变化时一条。
        Assert.True(session.Stats.Heartbeat.Rejected >= 3, "每拍都被拒：计数逐拍累加");
        Assert.Equal("探针失败", session.Stats.Heartbeat.LastError);

        await Task.Delay(300);
        lock (logs)
        {
            Assert.Equal(1, logs.Count(line => line.Contains("heartbeat rejected", StringComparison.Ordinal)));
        }

        // 会话仍可用：探针被拒不等于链路故障（正常发帧不受影响）。
        var reply = await session.SendFrameInputAsync(1, new byte[] { 0x01 });
        Assert.Equal(BattleEdgeDefaults.ReplyJson(BattleOps.SendFrameInput), System.Text.Encoding.UTF8.GetString(reply));
        Assert.Equal(ClientState.Connected, session.State);
        Assert.False(session.HasFailed);
    }

    // 可观测性：重连/握手/心跳失败都有只读计数（不引入依赖，上层可直接采指标）。
    [Fact]
    public async Task Stats_ExposeConnectHandshakeReconnectAndHeartbeatCounters()
    {
        // 握手被拒（接入层不回 flow-id）：拨号尝试/失败/握手失败三类都计。
        await using var rejected = new BattleEdgeUdpServer { RejectHello = true };
        var options = Options(TimeSpan.FromMilliseconds(100));
        options.HelloTimeoutMs = 300;
        var failing = BattleSession.Create(Plan(rejected.Port), options);
        await Assert.ThrowsAsync<ProtocolException>(() => failing.ConnectAsync());
        Assert.Equal(1, failing.Stats.ConnectAttempts);
        Assert.Equal(1, failing.Stats.ConnectFailures);
        Assert.Equal(1, failing.Stats.HandshakeFailures);
        await failing.DisposeAsync();

        // 正常连接 + 显式重连：心跳送达计数与重连计数递增。
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(Plan(server.Port), options);
        await session.ConnectAsync();
        Assert.Equal(1, session.Stats.ConnectAttempts);
        Assert.Equal(0, session.Stats.ConnectFailures);
        Assert.True(await WaitUntilAsync(() => session.Stats.Heartbeat.Sent >= 1), "探针送达应计数");

        await session.ReconnectAsync();
        Assert.Equal(2, session.Stats.ConnectAttempts);
        Assert.Equal(1, session.Stats.Reconnects);
        Assert.Equal(0, session.Stats.RejoinFailures);
    }
}
