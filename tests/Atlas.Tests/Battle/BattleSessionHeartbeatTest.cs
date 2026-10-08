using System;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionHeartbeatTest 覆盖直连保活心跳（battle.v1.BattleService/Ping）的行为口径：
// 周期到点发探针、CloseAsync 停表、失败只记/事件不终止会话、周期配置生效、可用 0 关闭。
// 探针是 Tell（服务端不回业务回执）：帧层往返成功即可，SDK 不解析回执载荷。
public sealed class BattleSessionHeartbeatTest
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
            HelloTimeoutMs = 3000,
            InvokeTimeoutMs = 3000,
            BackoffBaseMs = 50,
            BackoffMaxMs = 200,
            HeartbeatInterval = interval,
        };
    }

    // WaitUntilAsync 轮询等待条件成立（到点返回 false；心跳是异步周期行为，不接受固定 sleep 赌时序）。
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

    [Fact]
    public void Options_DefaultInterval_IsTwoSeconds_BelowIdleThird()
    {
        var options = new BattleSessionOptions();

        // 缺省 2s：严格小于服务端 offline_timeout 缺省 15s 的 1/3（数据报面空闲读超时 5s）。
        Assert.Equal(TimeSpan.FromSeconds(2), options.HeartbeatInterval);
        Assert.True(options.HeartbeatInterval < TimeSpan.FromSeconds(5),
            "心跳周期必须严格小于 offline_timeout/3（缺省 5s），否则数据报面会被判掉线");
    }

    [Fact]
    public void Options_NegativeInterval_RejectedAtAssembly()
    {
        var options = Options(TimeSpan.FromMilliseconds(-1));

        Assert.Throws<ArgumentException>(() => BattleSession.Create(
            Plan(7102), options));
    }

    [Fact]
    public async Task Heartbeat_SendsPingOnInterval_WithTicketSlotAndBattleId()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port), Options(TimeSpan.FromMilliseconds(100)));

        await session.ConnectAsync();
        // 未到周期不发探针（周期语义：不是连接即发）。
        Assert.Equal(0, PingCount(server));

        Assert.True(await WaitUntilAsync(() => PingCount(server) >= 2), "周期到点应发出 Ping");

        var request = server.RequestsOf(BattleOps.Ping)[0];
        Assert.Equal(Atlas.Frame.FrameGen.FlagSession, request.Flags & Atlas.Frame.FrameGen.FlagSession);
        Assert.Equal(EdgeWire.TicketSlot(Ticket), request.Slot);
        using var document = JsonDocument.Parse(request.Payload);
        Assert.Equal("b-1", document.RootElement.GetProperty("battleId").GetString());
    }

    [Fact]
    public async Task Heartbeat_DoesNotSendPing_AfterCloseAsync()
    {
        await using var server = new BattleEdgeUdpServer();
        var session = BattleSession.Create(Plan(server.Port), Options(TimeSpan.FromMilliseconds(100)));
        await session.ConnectAsync();
        Assert.True(await WaitUntilAsync(() => PingCount(server) >= 2), "周期到点应发出 Ping");

        await session.CloseAsync();
        await Task.Delay(100); // 排空关闭瞬间在途的一拍（网络在途，不计入「关闭后仍发」）。
        var afterClose = PingCount(server);
        await Task.Delay(500); // 留出「若定时器没停」的窗口（≥ 4 个周期）。

        Assert.Equal(afterClose, PingCount(server)); // CloseAsync 停表：关闭后不再发探针。
        await session.DisposeAsync();
    }

    [Fact]
    public async Task Heartbeat_BusinessRejection_ToleratedAndSessionStaysUsable()
    {
        await using var server = new BattleEdgeUdpServer();
        server.FailWith(BattleOps.Ping, 500, "INTERNAL", "探针失败");
        await using var session = BattleSession.Create(
            Plan(server.Port), Options(TimeSpan.FromMilliseconds(100)));
        var failures = 0;
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.HeartbeatFailed += _ =>
        {
            Interlocked.Increment(ref failures);
            signal.TrySetResult(true);
        };

        await session.ConnectAsync();
        Assert.True(await WaitUntilAsync(() => failures > 0), "探针失败应上报事件");
        await signal.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // 失败只记/事件：会话不终止、连接仍可用，正常发帧不受影响。
        Assert.Equal(ClientState.Connected, session.State);
        var reply = await session.SendFrameInputAsync(1, new byte[] { 0x01 });
        Assert.Equal(BattleEdgeDefaults.ReplyJson(BattleOps.SendFrameInput), Encoding.UTF8.GetString(reply));
        Assert.True(await WaitUntilAsync(() => PingCount(server) >= 3), "失败后心跳应继续按周期尝试");
    }

    [Fact]
    public async Task Heartbeat_IntervalConfigurable_FasterIntervalSendsMore()
    {
        await using var fast = new BattleEdgeUdpServer();
        await using var slow = new BattleEdgeUdpServer();
        await using var fastSession = BattleSession.Create(
            Plan(fast.Port), Options(TimeSpan.FromMilliseconds(60)));
        await using var slowSession = BattleSession.Create(
            Plan(slow.Port), Options(TimeSpan.FromMilliseconds(500)));

        await fastSession.ConnectAsync();
        await slowSession.ConnectAsync();
        await Task.Delay(600);

        // 周期配置生效：同一时间窗内快周期发得多、慢周期发得少。
        Assert.True(PingCount(fast) >= 4, $"快周期应发出多条探针，实际 {PingCount(fast)}");
        Assert.True(PingCount(slow) <= 2, $"慢周期不应发出多条探针，实际 {PingCount(slow)}");
        Assert.True(PingCount(fast) > PingCount(slow));
    }

    [Fact]
    public async Task Heartbeat_ZeroInterval_DisablesProbe()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(Plan(server.Port), Options(TimeSpan.Zero));

        await session.ConnectAsync();
        await Task.Delay(300);

        Assert.Equal(0, PingCount(server)); // 0 = 显式关闭（与既有「非正周期 = 关闭」口径一致）。
        Assert.Equal(ClientState.Connected, session.State);
    }

    [Fact]
    public async Task Heartbeat_NotConnected_SkipsSilently()
    {
        await using var server = new BattleEdgeUdpServer();
        var session = BattleSession.Create(Plan(server.Port), Options(TimeSpan.FromMilliseconds(50)));
        var failures = 0;
        session.HeartbeatFailed += _ => Interlocked.Increment(ref failures);
        await session.ConnectAsync();

        // 断开后（未关闭）重连窗口内不探测、也不上报失败：无连接时心跳无事可证（重连编排负责）。
        await session.ReconnectAsync();
        Assert.Equal(ClientState.Connected, session.State);
        Assert.Equal(0, Volatile.Read(ref failures));
        await session.CloseAsync();
        await session.DisposeAsync();
    }
}
