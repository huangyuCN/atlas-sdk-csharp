using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionEndedTest 覆盖对局结束的语义收口（服务端已就绪口径：已结束对局的迟到帧 op
// 被稳定 reason BATTLE_ENDED 拒绝；结算结束通知可能重复投递、载荷逐字一致）：
//   ① BATTLE_ENDED → 终态（HasEnded）+ 停心跳 + 零写入（假传输断言）；
//   ② 同一局结束通知重复到达 → BattleEnd 事件只触发一次（载荷不一致只记 Warn，首个为准）；
//   ③ 终态下发帧/补帧/入局/重连 → BattleEndedException（明确异常，不写线）；
//   ④ 收尾窗口：结束后保留短窗口收尾随推送，CloseAsync 取消窗口并 await 退出（无悬挂定时器）。
public sealed class BattleSessionEndedTest
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

    private static BattleSessionOptions Options()
    {
        return new BattleSessionOptions
        {
            PreferredTransport = EdgeTransport.Udp,
            HelloTimeoutMs = 3000,
            InvokeTimeoutMs = 3000,
            BackoffBaseMs = 50,
            BackoffMaxMs = 200,
        };
    }

    // FakeSession 装配一个走假传输的会话（DialOverride 仅供测试注入，不碰网络）。
    private static BattleSession FakeSession(BattleFakeTransport transport, BattleSessionOptions options)
    {
        var session = BattleSession.Create(Plan(7102), options);
        session.DialOverride = _ => Task.FromResult<ITransport>(transport);
        return session;
    }

    // EndPayload 组装一份结算结束通知载荷（ver=1 protojson 形态）。
    private static byte[] EndPayload(string winner)
    {
        return Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"winnerPlayerId\":\"" + winner + "\"}");
    }

    private static int Count(List<BattlePush> events)
    {
        lock (events)
        {
            return events.Count;
        }
    }

    // WaitUntilAsync 轮询等待条件成立（到点返回 false；终态是异步收口，不接受固定 sleep 赌时序）。
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

    // ① BATTLE_ENDED 业务拒绝 → 终态 + 停心跳 + 零写入。
    [Fact]
    public async Task BattleEnded_Rejection_EntersTerminalState_AndStopsHeartbeatWithoutFurtherWrites()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.HeartbeatInterval = TimeSpan.FromMilliseconds(150);
        options.InvokeTimeoutMs = 2000;
        var session = FakeSession(transport, options);
        await session.ConnectAsync();

        var call = session.SendFrameInputAsync(1, new byte[] { 0x01 });
        await transport.WaitForWritesAsync(1);
        transport.QueueBusinessError(
            transport.WrittenHeaders[0].Seq, 409, DirectErrors.BattleEndedReason, "对局已结束");

        var exception = await Assert.ThrowsAsync<BusinessException>(() => call);

        // 拒绝原样上抛（可判定），同时会话进入终态并记下触发来源。
        Assert.Equal(DirectErrors.BattleEndedReason, exception.Reason);
        Assert.True(session.HasEnded);
        Assert.Equal(DirectErrors.BattleEndedReason, session.EndedReason);

        // 停表：等 ≥ 2 个心跳周期，写线计数不再增长（终态后零写入的线级证据）。
        await Task.Delay(120); // 排空结束瞬间在途的一拍。
        var writesAtEnd = transport.WrittenCount;
        await Task.Delay(400);
        Assert.Equal(writesAtEnd, transport.WrittenCount);

        await session.CloseAsync();
    }

    // ③ 终态下的发帧/补帧/入局/重连：明确异常且不写线。
    [Fact]
    public async Task Ended_SendSyncJoinReconnect_ThrowBattleEndedException_AndWriteNothing()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        await session.ConnectAsync();
        var call = session.JoinBattleAsync();
        await transport.WaitForWritesAsync(1);
        transport.QueueBusinessError(
            transport.WrittenHeaders[0].Seq, 409, DirectErrors.BattleEndedReason, "对局已结束");
        await Assert.ThrowsAsync<BusinessException>(() => call);
        var writes = transport.WrittenCount;

        await Assert.ThrowsAsync<BattleEndedException>(
            () => session.SendFrameInputAsync(2, new byte[] { 0x02 }));
        await Assert.ThrowsAsync<BattleEndedException>(() => session.SyncFramesAsync(1));
        await Assert.ThrowsAsync<BattleEndedException>(() => session.JoinBattleAsync());
        await Assert.ThrowsAsync<BattleEndedException>(() => session.ReconnectAsync());
        await Assert.ThrowsAsync<BattleEndedException>(() => session.ConnectAsync());

        // 明确异常（不是静默丢弃、不是空等超时），且一条都没写到线上。
        Assert.Equal(writes, transport.WrittenCount);
        var thrown = await Assert.ThrowsAsync<BattleEndedException>(
            () => session.SendFrameInputAsync(3, new byte[] { 0x03 }));
        Assert.Equal(DirectErrors.BattleEndedReason, thrown.Reason);
        Assert.Contains(DirectErrors.SessionEnded, thrown.Message, StringComparison.Ordinal);
        await session.CloseAsync();
    }

    // ② 同一局结束通知重复投递（载荷逐字一致）→ 事件只触发一次。
    [Fact]
    public async Task BattleEndNotify_RepeatedDelivery_FiresEventOnce_AndKeepsFirstPayload()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromMilliseconds(300);
        var logs = new List<string>();
        options.Logger = SDKLoggerFactory.Of(LogLevel.Warn, line =>
        {
            lock (logs)
            {
                logs.Add(line);
            }
        });
        var session = FakeSession(transport, options);
        var events = new List<BattlePush>();
        session.BattleEnd += push =>
        {
            lock (events)
            {
                events.Add(push);
            }
        };
        await session.ConnectAsync();

        var payload = EndPayload("p-1");
        transport.QueueNotify(BattleOps.BattleEndNotify, payload);
        Assert.True(await WaitUntilAsync(() => Count(events) == 1), "首个结束通知应触发一次事件");

        // 服务端有界补投（关闭前重投 + 重连补投，最多每人 5 次）：重复到达不再触发事件。
        transport.QueueNotify(BattleOps.BattleEndNotify, payload);
        transport.QueueNotify(BattleOps.BattleEndNotify, payload);
        await Task.Delay(150);

        Assert.Equal(1, Count(events));
        Assert.Equal(payload, events[0].Payload);
        Assert.Equal(BattleOps.BattleEndNotify, events[0].Op);
        Assert.True(session.HasEnded);
        Assert.Equal(DirectErrors.BattleEndNotifyReason, session.EndedReason);
        Assert.DoesNotContain(logs, line => line.Contains("mismatch", StringComparison.Ordinal));
        await session.CloseAsync();
    }

    // ② 载荷不一致（服务端留档与首投漂移）：首个载荷为准 + 记 Warn，事件仍只一次。
    [Fact]
    public async Task BattleEndNotify_PayloadMismatch_KeepsFirstWins_AndLogsWarn()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromMilliseconds(300);
        var logs = new List<string>();
        options.Logger = SDKLoggerFactory.Of(LogLevel.Warn, line =>
        {
            lock (logs)
            {
                logs.Add(line);
            }
        });
        var session = FakeSession(transport, options);
        var events = new List<BattlePush>();
        session.BattleEnd += push =>
        {
            lock (events)
            {
                events.Add(push);
            }
        };
        await session.ConnectAsync();

        var first = EndPayload("p-1");
        transport.QueueNotify(BattleOps.BattleEndNotify, first);
        Assert.True(await WaitUntilAsync(() => Count(events) == 1), "首个结束通知应触发一次事件");
        transport.QueueNotify(BattleOps.BattleEndNotify, EndPayload("p-2"));
        Assert.True(
            await WaitUntilAsync(() => logs.Any(line => line.Contains("mismatch", StringComparison.Ordinal))),
            "载荷不一致应记 Warn（排障可见）");

        Assert.Equal(1, Count(events));
        Assert.Equal(first, events[0].Payload); // 首个载荷为准：不重发事件、不改写已交付结果。
        await session.CloseAsync();
    }

    // ② 迟到 op 路径（服务端先补投留档结果、再以 BATTLE_ENDED 拒绝）：拒绝先到时事件仍触发一次。
    [Fact]
    public async Task BattleEnded_RejectionFirst_ThenNotify_FiresEventOnce_AndKeepsFirstReason()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromMilliseconds(300);
        var session = FakeSession(transport, options);
        var events = new List<BattlePush>();
        session.BattleEnd += push =>
        {
            lock (events)
            {
                events.Add(push);
            }
        };
        await session.ConnectAsync();

        var call = session.SendFrameInputAsync(1, new byte[] { 0x01 });
        await transport.WaitForWritesAsync(1);
        transport.QueueBusinessError(
            transport.WrittenHeaders[0].Seq, 409, DirectErrors.BattleEndedReason, "对局已结束");
        await Assert.ThrowsAsync<BusinessException>(() => call);
        Assert.Equal(DirectErrors.BattleEndedReason, session.EndedReason); // 首个结束信号为准。

        var payload = EndPayload("p-1");
        transport.QueueNotify(BattleOps.BattleEndNotify, payload);
        Assert.True(await WaitUntilAsync(() => Count(events) == 1), "拒绝先到时也应补触发一次 BattleEnd");
        transport.QueueNotify(BattleOps.BattleEndNotify, payload);
        await Task.Delay(150);

        Assert.Equal(1, Count(events));
        Assert.Equal(payload, events[0].Payload);
        Assert.Equal(DirectErrors.BattleEndedReason, session.EndedReason); // 重复通知不改写首个来源。
        await session.CloseAsync();
    }

    // ① 心跳探针被 BATTLE_ENDED 拒绝 → 终态 + 停表，且不算链路失败（不上报 HeartbeatFailed）。
    [Fact]
    public async Task Heartbeat_BattleEndedRejection_EntersTerminalState_AndStopsProbe()
    {
        await using var server = new BattleEdgeUdpServer();
        server.FailWith(BattleOps.Ping, 409, DirectErrors.BattleEndedReason, "对局已结束");
        var options = Options();
        options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
        await using var session = BattleSession.Create(Plan(server.Port), options);
        var failures = 0;
        session.HeartbeatFailed += _ => Interlocked.Increment(ref failures);

        await session.ConnectAsync();
        Assert.True(await WaitUntilAsync(() => session.HasEnded), "探针被 BATTLE_ENDED 拒绝应进入终态");
        Assert.Equal(DirectErrors.BattleEndedReason, session.EndedReason);

        await Task.Delay(200); // 排空结束瞬间在途的一拍。
        var pings = server.RequestsOf(BattleOps.Ping).Length;
        await Task.Delay(400); // ≥ 3 个心跳周期。
        Assert.Equal(pings, server.RequestsOf(BattleOps.Ping).Length); // 停表：不再发探针。
        Assert.Equal(0, Volatile.Read(ref failures)); // 终态不是链路失败，不误报保活质量。

        await Assert.ThrowsAsync<BattleEndedException>(
            () => session.SendFrameInputAsync(1, new byte[] { 0x01 }));
        Assert.Empty(server.RequestsOf(BattleOps.SendFrameInput)); // 零写入（服务端视角）。
    }

    // 重连补投期判定该局已结束（服务端先补投留档结果、再以 BATTLE_ENDED 拒绝重新入局）：
    // 终态收口 + 停重连（不再拨号/入局），且**不上报 Failed**——对局正常结束不是失败。
    [Fact]
    public async Task Reconnect_JoinRejectedWithBattleEnded_EntersTerminalState_WithoutFailedCallback()
    {
        var first = new BattleFakeTransport();
        var second = new BattleFakeTransport();
        var dials = 0;
        var options = Options();
        options.BackoffBaseMs = 30;
        options.EndDrainWindow = TimeSpan.FromMilliseconds(300);
        var session = BattleSession.Create(Plan(7102), options);
        session.DialOverride = _ =>
            Task.FromResult<ITransport>(Interlocked.Increment(ref dials) == 1 ? first : second);
        var failures = 0;
        session.Failed += _ => Interlocked.Increment(ref failures);
        await session.ConnectAsync();

        await first.CloseAsync(); // 断链：读循环退出 → 自动重连（换新一代传输）。
        await second.WaitForWritesAsync(1); // 新一代的 JoinBattle。
        second.QueueBusinessError(
            second.WrittenHeaders[0].Seq, 409, DirectErrors.BattleEndedReason, "对局已结束");

        Assert.True(await WaitUntilAsync(() => session.HasEnded), "重连补投期的 BATTLE_ENDED 应进入终态");
        Assert.Equal(DirectErrors.BattleEndedReason, session.EndedReason);

        await Task.Delay(150); // 让终态瞬间在途的一拍落地。
        var writes = second.WrittenCount;
        await Task.Delay(300); // 留出「若还在重连」的窗口。
        Assert.Equal(writes, second.WrittenCount); // 终态后不再入局/补帧（零写入）。
        Assert.Equal(2, Volatile.Read(ref dials)); // 首连一次 + 本次重连一次；终态后不再拨号。
        Assert.Equal(0, Volatile.Read(ref failures)); // 正常结束不报 Failed。
        await session.CloseAsync();
    }

    // 终态后断链不再重拨（终态不再 hello/入局/补帧）：重连被终态拒绝而终止，且不报 Failed。
    [Fact]
    public async Task Ended_ThenDisconnect_DoesNotRedial_AndDoesNotReportFailed()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.BackoffBaseMs = 30;
        options.EndDrainWindow = TimeSpan.FromSeconds(5); // 窗口够长：重连窗口落在收尾窗口内。
        var session = FakeSession(transport, options);
        var dials = 0;
        session.DialOverride = _ =>
        {
            Interlocked.Increment(ref dials);
            return Task.FromResult<ITransport>(transport);
        };
        var failures = 0;
        session.Failed += _ => Interlocked.Increment(ref failures);
        await session.ConnectAsync();
        Assert.Equal(1, Volatile.Read(ref dials)); // 首连一次。

        transport.QueueNotify(BattleOps.BattleEndNotify, EndPayload("p-1"));
        Assert.True(await WaitUntilAsync(() => session.HasEnded), "结束通知应进入终态");
        await transport.CloseAsync(); // 服务端结算后关连接：读循环退出 → 自动重连被终态拒绝。
        await Task.Delay(400); // ≥ 数个退避周期。

        Assert.Equal(1, Volatile.Read(ref dials)); // 终态后不再拨号（零写入）。
        Assert.Equal(0, Volatile.Read(ref failures)); // 终态拒绝重连不算失败。
        await session.CloseAsync();
    }

    // ④ 收尾窗口：结束后连接仍在，尾随推送照收；窗口到点自动关连接。
    [Fact]
    public async Task EndDrainWindow_KeepsReceivingTrailingPushes_ThenClosesChannel()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromMilliseconds(500);
        var session = FakeSession(transport, options);
        var broadcasts = 0;
        session.FrameBroadcast += _ => Interlocked.Increment(ref broadcasts);
        await session.ConnectAsync();

        transport.QueueNotify(BattleOps.BattleEndNotify, EndPayload("p-1"));
        Assert.True(await WaitUntilAsync(() => session.HasEnded), "结束通知应进入终态");
        Assert.Equal(ClientState.Connected, session.State); // 窗口内不立即断：收尾随推送与在途回执。

        transport.QueueNotify(
            BattleOps.FrameBroadcast, Encoding.UTF8.GetBytes("{\"frame\":{\"frameId\":\"9\"}}"));
        Assert.True(await WaitUntilAsync(() => Volatile.Read(ref broadcasts) == 1), "窗口内尾随推送应仍分发");

        Assert.True(
            await WaitUntilAsync(() => session.State == ClientState.Disconnected),
            "收尾窗口到点应自动关连接（停重连、释放资源）");
        await session.CloseAsync();
    }

    // ④ CloseAsync 取消收尾窗口并 await 退出：不等窗口到点、关闭后无悬挂定时器。
    [Fact]
    public async Task CloseAsync_CancelsDrainWindow_AndLeavesNoDanglingTimer()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromSeconds(10); // 远长于断言窗口：CloseAsync 必须主动取消。
        var session = FakeSession(transport, options);
        await session.ConnectAsync();
        transport.QueueNotify(BattleOps.BattleEndNotify, EndPayload("p-1"));
        Assert.True(await WaitUntilAsync(() => session.HasEnded), "结束通知应进入终态");

        var watch = Stopwatch.StartNew();
        await session.CloseAsync();
        await session.CloseAsync(); // 幂等。

        Assert.True(
            watch.ElapsedMilliseconds < 3000,
            $"CloseAsync 应取消收尾窗口而非等它到点（实测 {watch.ElapsedMilliseconds}ms）");
        Assert.Equal(ClientState.Disconnected, session.State);
        Assert.Equal(DirectErrors.BattleEndNotifyReason, session.EndedReason);

        var writes = transport.WrittenCount;
        await Task.Delay(250); // 留出「若定时器没停」的窗口。
        Assert.Equal(writes, transport.WrittenCount);
        Assert.Equal(ClientState.Disconnected, session.State);
    }

    // 收尾窗口是配置项：负值在装配期即拒绝（不留「连上了才发现配置错」的窗口）。
    [Fact]
    public void Options_NegativeEndDrainWindow_RejectedAtAssembly()
    {
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromMilliseconds(-1);

        Assert.Throws<ArgumentException>(() => BattleSession.Create(Plan(7102), options));
    }
}
