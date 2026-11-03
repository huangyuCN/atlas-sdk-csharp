using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
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
            // 保活探针显式配置（SDK 不再内建 PingReq 探针）。
            ProbeOp = TestOps.Ping,
            ProbePayload = TestWire.Probe("b-1"),
        };
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000)
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

    // Describe 汇总会话即时状态与心跳计数（超时诊断）：下次偶发失败能直接看出是「回调还没到」
    // 还是「真的没发生」。
    private static string Describe(BattleSession session, params (string Name, long Value)[] extra)
    {
        var heartbeat = session.Stats.Heartbeat;
        var parts = new List<string>
        {
            $"state={session.State}",
            $"hasFailed={session.HasFailed}",
            $"hasEnded={session.HasEnded}",
            $"failureCause={(session.FailureCause as BusinessException)?.Reason ?? session.FailureCause?.GetType().Name ?? "null"}",
            $"hb(sent={heartbeat.Sent},rejected={heartbeat.Rejected},ticketRejected={heartbeat.TicketRejected},"
                + $"lastError={heartbeat.LastError ?? "null"})",
        };
        foreach (var (name, value) in extra)
        {
            parts.Add($"{name}={value}");
        }
        return string.Join(" ", parts);
    }

    // AwaitStateAsync 等待条件成立（默认 5s 上限，超时即失败并打印诊断快照）：
    // 不赌固定时长；也不接受「先等状态位成立、再立刻断言事件/计数」——两步之间是异步窗口
    // （终态标记先置位，Failed 回调要等通道关闭完毕才分发，CI 上该窗口会被调度拉长）。
    private static async Task AwaitStateAsync(
        string what, Func<bool> condition, Func<string> describe, int timeoutMs = 5000)
    {
        if (await WaitUntilAsync(condition, timeoutMs))
        {
            return;
        }
        throw new Xunit.Sdk.XunitException(
            $"超时 {timeoutMs}ms 仍未满足：{what}；实际：{describe()}");
    }

    private static int PingCount(BattleEdgeUdpServer server)
    {
        return server.RequestsOf(TestOps.Ping).Length;
    }

    // FakeSession 装配一个走假传输的会话（DialOverride 仅供测试注入，不碰网络）。
    private static BattleSession FakeSession(BattleFakeTransport transport, TimeSpan interval)
    {
        var session = BattleSession.Create(Plan(7103), Options(interval));
        session.DialOverride = _ => Task.FromResult<ITransport>(transport);
        return session;
    }

    // Heartbeat_TerminalRejection_FailedCallbackDelayed_StillReportsOnce 复现 CI 偶发（1/345）的根因：
    // 终态标记（HasFailed 背后的 _failed）在 EnterTerminalFailure 里**先行置位**，Failed 回调要等
    // 通道关闭（CloseAndReportAsync → CloseChannelAsync → transport.CloseAsync）**之后**才分发——
    // 两步之间是一段纯异步窗口，CI 上该窗口常被线程池排队/调度拉长（本地快机器上几乎不可见）。
    // 用例用假传输的 BeforeClose 闸门把这个窗口撑到远超轮询间隔，固化「等状态位成立就断言事件计数」
    // 必读不到（红），改后按事件计数等待（绿）。
    [Fact]
    public async Task Heartbeat_TerminalRejection_FailedCallbackDelayed_StillReportsOnce()
    {
        var transport = new BattleFakeTransport();
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.BeforeClose = () => gate.Task;
        await using var session = FakeSession(transport, TimeSpan.FromMilliseconds(50));
        var failures = 0;
        session.Failed += _ => Interlocked.Increment(ref failures);

        await session.ConnectAsync();
        await transport.WaitForWritesAsync(1); // 第一拍探针已写线。
        transport.QueueBusinessError(
            transport.WrittenHeaders[0].Seq, 403, DirectErrors.FrameTargetMismatchReason, "探针被拒");

        // 模拟 CI 的慢关闭：闸门先关着，Failed 回调因此迟到（远超 10ms 轮询间隔）。
        // 旧写法（CI 上偶发红）：等到终态标记成立就立刻读事件计数——状态位先置位、回调后到，必读 0。
        Assert.True(await WaitUntilAsync(() => session.HasFailed), "终态类拒绝应终态化");
        var atFlagTime = Volatile.Read(ref failures); // 旧写法在这一刻断言 == 1。

        gate.TrySetResult(true); // 放行慢关闭（避免关闭在途时 Dispose 卡住）。
        await AwaitStateAsync(
            "Failed 应上报一次（回调迟到也要等到）",
            () => Volatile.Read(ref failures) == 1,
            () => Describe(session, ("failures", Volatile.Read(ref failures))));
        Assert.Equal(0, atFlagTime); // 根因固化：状态位成立瞬间回调尚未到达。
        Assert.Equal(1, Volatile.Read(ref failures)); // 等到回调到达：只上报一次。
        Assert.Equal(DirectErrors.FrameTargetMismatchReason, (session.FailureCause as BusinessException)?.Reason);
    }

    // 终态类拒绝（入局被拒/目标失配）：探针一拍即入终态并上报 Failed 一次，探针停发。
    [Theory]
    [InlineData(404, DirectErrors.BattleNotFoundReason)]
    [InlineData(409, DirectErrors.BattleFullReason)]
    [InlineData(403, DirectErrors.FrameTargetMismatchReason)]
    public async Task Heartbeat_TerminalClassRejection_TerminalizesAndReportsFailedOnce(int code, string reason)
    {
        await using var server = new BattleEdgeUdpServer();
        server.FailWith(TestOps.Ping, code, reason, "探针被拒");
        await using var session = BattleSession.Create(
            Plan(server.Port), Options(TimeSpan.FromMilliseconds(80)));
        var failures = 0;
        session.Failed += _ => Interlocked.Increment(ref failures);

        await session.ConnectAsync();
        // 等「终态化 + Failed 上报一次」同时成立（超时带诊断）：只等 HasFailed 会在 CI 上抢跑。
        await AwaitStateAsync(
            "终态类拒绝应终态化并上报 Failed 一次",
            () => session.HasFailed && Volatile.Read(ref failures) == 1,
            () => Describe(session, ("failures", Volatile.Read(ref failures)), ("pings", PingCount(server))));
        Assert.Equal(1, Volatile.Read(ref failures)); // 上报一次。
        Assert.Equal(reason, (session.FailureCause as BusinessException)?.Reason);

        // 停表是「不再发生」的负向断言：只能给窗口，窗口按心跳周期（80ms）导出，不猜绝对时长。
        await Task.Delay(160); // 2 个周期：排空终态瞬间在途的一拍。
        var pings = PingCount(server);
        await Task.Delay(320); // 再 4 个周期。
        Assert.Equal(pings, PingCount(server)); // 停表：不再发探针。
        Assert.True(session.Stats.Heartbeat.Rejected >= 1, "被拒探针应计数");
        Assert.False(session.HasEnded); // 不是「对局已结束」：终态失败另有标记。
    }

    // 票类拒绝：不终态，上报「需重新取票」信号 + 计数（日志仅首次），继续探测。
    [Fact]
    public async Task Heartbeat_TicketRejection_NotTerminal_ReportsOnceAndKeepsProbing()
    {
        await using var server = new BattleEdgeUdpServer();
        server.FailWith(TestOps.Ping, 401, DirectErrors.TicketExpiredReason, "票据已过期");
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
        // 等「持续探测 + 信号只报一次 + 计数逐拍累加」同时成立（事件与计数都是异步分发）。
        await AwaitStateAsync(
            "票类拒绝后应继续探测（信号一次、计数逐拍累加）",
            () => PingCount(server) >= 3
                && Volatile.Read(ref tickets) == 1
                && session.Stats.Heartbeat.TicketRejected >= 2,
            () => Describe(
                session,
                ("pings", PingCount(server)),
                ("tickets", Volatile.Read(ref tickets)),
                ("failures", Volatile.Read(ref failures))));
        Assert.Equal(1, Volatile.Read(ref tickets)); // 信号只上报一次（状态未变化）。
        Assert.Equal(0, Volatile.Read(ref failures)); // 票类走专用出口，不混进通用失败。
        Assert.False(session.HasFailed);
        Assert.False(session.HasEnded);
        Assert.Equal(ClientState.Connected, session.State);
        Assert.True(session.Stats.Heartbeat.TicketRejected >= 2, "每拍都被拒：计数逐拍累加");
        Assert.True(session.Stats.Heartbeat.Rejected >= 2);

        // 日志只在状态首次变化时打一条：再等 ≥ 3 个探针周期（按计数等待，不赌固定时长）。
        var rejectedBaseline = session.Stats.Heartbeat.Rejected;
        await AwaitStateAsync(
            "再被拒 ≥ 3 拍（日志仍只有一条）",
            () => session.Stats.Heartbeat.Rejected >= rejectedBaseline + 3,
            () => Describe(session, ("rejected", session.Stats.Heartbeat.Rejected)));
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
        server.FailWith(TestOps.Ping, 500, "INTERNAL", "探针失败");
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
        // 等「继续探测 + 失败出口只报一次 + 计数逐拍累加 + 最近错误可读」同时成立。
        await AwaitStateAsync(
            "其它业务拒绝后应继续探测（失败出口一次、计数逐拍累加）",
            () => PingCount(server) >= 3
                && Volatile.Read(ref failures) == 1
                && session.Stats.Heartbeat.Rejected >= 3
                && "探针失败" == session.Stats.Heartbeat.LastError,
            () => Describe(
                session,
                ("pings", PingCount(server)),
                ("failures", Volatile.Read(ref failures))));
        Assert.Equal(1, Volatile.Read(ref failures)); // 事件只在状态首次变化时一条。
        Assert.True(session.Stats.Heartbeat.Rejected >= 3, "每拍都被拒：计数逐拍累加");
        Assert.Equal("探针失败", session.Stats.Heartbeat.LastError);

        // 日志只在状态首次变化时打一条：再等 ≥ 3 拍（按计数等待，不赌固定时长）。
        var rejectedBaseline = session.Stats.Heartbeat.Rejected;
        await AwaitStateAsync(
            "再被拒 ≥ 3 拍（日志仍只有一条）",
            () => session.Stats.Heartbeat.Rejected >= rejectedBaseline + 3,
            () => Describe(session, ("rejected", session.Stats.Heartbeat.Rejected)));
        lock (logs)
        {
            Assert.Equal(1, logs.Count(line => line.Contains("heartbeat rejected", StringComparison.Ordinal)));
        }

        // 会话仍可用：探针被拒不等于链路故障（正常发帧不受影响）。
        var reply = await session.InvokeAsync(TestOps.SendFrameInput, TestWire.Input("b-1", 1, new byte[] { 0x01 }));
        Assert.Equal(BattleEdgeDefaults.ReplyJson(TestOps.SendFrameInput), System.Text.Encoding.UTF8.GetString(reply));
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
