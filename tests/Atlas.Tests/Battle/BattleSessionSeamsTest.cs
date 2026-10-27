using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Frame;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionSeamsTest 覆盖「游戏域消息迁出后」会话保留的三条接缝与两处口径变化
// （对齐 Go SDK f9221a6 的同一裁定）：
//   ① InvokeAsync：业务 op 与裸载荷由调用方给出（op/消息来自游戏协议包），SDK 只编码/写线/配回执；
//   ② Probe：保活探针必须显式配置（ProbeOp + ProbePayload）——不配就一个字节都不发；
//   ③ Replay：重连后的重放步骤必须显式配置——不配就只重新 hello，不重放任何业务 op；
//   ④ 帧号：SDK 不再内建任何载荷解析（ver=1 也不解），补帧基准只由 FrameNumberOf 钩子推进；
//   ⑤ 结束通知：只认非空载荷（空载荷不臆造结束语义），首投载荷经 EndNotifyPayload 取。
public sealed class BattleSessionSeamsTest
{
    // Ticket 是测试票据密文（非真实 AEAD 票：接入层/帧面只按字节透传与编码）。
    private static readonly byte[] Ticket = { 0xfb, 0xff, 0x00, 0x01, 0xfe, 0x10, 0x20 };

    // CustomProbeOp 是一个**非内建**的探针 op：证明心跳确实按配置的 op 发（而非 SDK 里写死的名字）。
    private const string CustomProbeOp = "/test.Custom/Probe";

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
            HeartbeatInterval = TimeSpan.Zero,
        };
    }

    private static BattleSession FakeSession(BattleFakeTransport transport, BattleSessionOptions options)
    {
        var session = BattleSession.Create(Plan(7102), options);
        session.DialOverride = _ => Task.FromResult<ITransport>(transport);
        return session;
    }

    // WaitUntilAsync 轮询等待条件成立（异步分发不接受固定 sleep 赌时序）。
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

    private static System.Text.Json.JsonElement Parse(byte[] payload)
    {
        using var document = System.Text.Json.JsonDocument.Parse(payload);
        return document.RootElement.Clone();
    }

    // ① 裸 op + 裸载荷原样上线，回执原样交回（SDK 不做任何游戏域消息的组装或解码）。
    [Fact]
    public async Task InvokeAsync_ForwardsRawOpAndPayloadVerbatim()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(Plan(server.Port), Options());
        var payload = Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"anything\":[1,2,3]}");

        await session.ConnectAsync();
        var reply = await session.InvokeAsync(TestOps.JoinBattle, payload);

        var request = server.RequestsOf(TestOps.JoinBattle)[0];
        Assert.Equal(payload, request.Payload); // 一个字节都不改写。
        Assert.Equal(BattleEdgeDefaults.JoinBattleReply, Encoding.UTF8.GetString(reply));
        Assert.Empty(server.RequestsOf(TestOps.SendFrameInput)); // 不隐式追加任何 op。
    }

    // ① 空载荷重载：payload = null 表示空体（探针/无体 op 用）。
    [Fact]
    public async Task InvokeAsync_NullPayload_SendsEmptyBody()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(Plan(server.Port), Options());

        await session.ConnectAsync();
        await session.InvokeAsync(TestOps.Ping, null);

        Assert.Empty(server.RequestsOf(TestOps.Ping)[0].Payload);
    }

    // ② 未配置探针：心跳循环不启动，一个字节都不发（不再有内建 PingReq 探针）。
    [Fact]
    public async Task Heartbeat_WithoutProbeConfig_WritesNothing()
    {
        await using var server = new BattleEdgeUdpServer();
        var options = Options();
        options.HeartbeatInterval = TimeSpan.FromMilliseconds(60); // 周期开着，但没配探针。
        await using var session = BattleSession.Create(Plan(server.Port), options);

        await session.ConnectAsync();
        await Task.Delay(400); // ≥ 6 个周期：若会裸探，早就发出了。

        Assert.Empty(server.Requests);
        Assert.Equal(ClientState.Connected, session.State);
        Assert.Equal(0, session.Stats.Heartbeat.Sent);
    }

    // ② 配置了探针：按配置的 op 与载荷周期发（op 不写死、载荷逐字一致）。
    [Fact]
    public async Task Heartbeat_UsesConfiguredProbeOpAndPayload()
    {
        await using var server = new BattleEdgeUdpServer();
        var options = Options();
        options.HeartbeatInterval = TimeSpan.FromMilliseconds(80);
        options.ProbeOp = CustomProbeOp;
        options.ProbePayload = Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"probe\":true}");
        await using var session = BattleSession.Create(Plan(server.Port), options);

        await session.ConnectAsync();
        Assert.True(
            await WaitUntilAsync(() => server.RequestsOf(CustomProbeOp).Length >= 2),
            "配置探针后应按周期发出");

        var request = server.RequestsOf(CustomProbeOp)[0];
        Assert.Equal(options.ProbePayload, request.Payload);
        Assert.Equal(Atlas.Frame.FrameGen.FlagSession, request.Flags & Atlas.Frame.FrameGen.FlagSession);
        Assert.True(session.Stats.Heartbeat.Sent >= 2, "探针送达应计数");
    }

    // ④ ver=1（protojson）帧广播也不再由 SDK 解析：未注入钩子 ⇒ 补帧基准不推进、也不报错。
    [Fact]
    public async Task FrameBroadcast_Ver1_WithoutHook_DoesNotAdvanceBaseline()
    {
        var transport = new BattleFakeTransport();
        await using var session = FakeSession(transport, Options());
        var delivered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = session.OnPush(TestOps.FrameBroadcast, _ => delivered.TrySetResult(true));
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.FrameBroadcast, TestWire.FrameBroadcast(9));
        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Equal(0UL, session.LastSeenFrame); // 不猜载荷编码：不注入钩子就不推进。
        Assert.Equal(ClientState.Connected, session.State);
        await session.CloseAsync();
    }

    // ④ 注入钩子后，ver=1 与 ver=2 的帧广播**都**走钩子推进补帧基准（不再有内建/钩子的双口径）。
    [Theory]
    [InlineData(FrameGen.Version)]
    [InlineData(FrameGen.Version2)]
    public async Task FrameBroadcast_WithHook_AdvancesBaseline_ForAnyVersion(byte version)
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        var hookCalls = 0;
        options.FrameNumberOf = payload =>
        {
            Interlocked.Increment(ref hookCalls);
            return 42UL;
        };
        await using var session = FakeSession(transport, options);
        var delivered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = session.OnPush(TestOps.FrameBroadcast, _ => delivered.TrySetResult(true));
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.FrameBroadcast, new byte[] { 0x08, 0x2A }, version);
        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Equal(42UL, session.LastSeenFrame);
        Assert.Equal(1, Volatile.Read(ref hookCalls));
        await session.CloseAsync();
    }

    // ② 重连只重新 hello：未配置 Replay ⇒ 不重放任何业务 op（内建 JoinBattle+SyncFrames 已删）。
    [Fact]
    public async Task Reconnect_WithoutReplay_ReplaysNothing()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(Plan(server.Port), Options());

        await session.ConnectAsync();
        await session.InvokeAsync(TestOps.JoinBattle, TestWire.Join("b-1"));
        await session.ReconnectAsync();

        Assert.Equal(2, server.HelloCount); // 重新 hello（同一张票）。
        Assert.Single(server.RequestsOf(TestOps.JoinBattle)); // 只有调用方自己发的那一次。
        Assert.Empty(server.RequestsOf(TestOps.SyncFrames));
    }

    // ③ 配置 Replay 后，重连按顺序重放，载荷按当前补帧基准生成（断点续上）。
    [Fact]
    public async Task Reconnect_ReplaysConfiguredStepsInOrder_WithLastSeenFrame()
    {
        await using var server = new BattleEdgeUdpServer();
        var options = Options();
        options.Replay = new[]
        {
            new ReplayStep(TestOps.JoinBattle, _ => TestWire.Join("b-1")),
            new ReplayStep(TestOps.SyncFrames, lastSeen => TestWire.Sync("b-1", lastSeen)),
        };
        await using var session = BattleSession.Create(Plan(server.Port), options);
        await session.ConnectAsync();

        session.LastSeenFrame = 7;
        await session.ReconnectAsync();

        var requests = server.Requests;
        Assert.Equal(2, requests.Length);
        Assert.Equal(TestOps.JoinBattle, requests[0].Op);
        Assert.Equal(TestOps.SyncFrames, requests[1].Op);
        Assert.Equal("7", Parse(requests[1].Payload).GetProperty("lastSeenFrame").GetString());
        Assert.Equal(1, session.Stats.Reconnects);
    }

    // ③ 重放步骤必须可寻址：空 op 在装配期即拒绝（不留「重连时才空指针/发错对象」的窗口）。
    [Fact]
    public void ReplayStep_EmptyOp_Rejected()
    {
        Assert.Throws<ArgumentException>(() => new ReplayStep(""));
    }

    // ③ 重放表定了重连行为：null 表或含 null 步骤在装配期（Create）即拒绝。
    [Fact]
    public void Options_NullReplay_RejectedAtAssembly()
    {
        var options = Options();
        options.Replay = null!;

        Assert.Throws<ArgumentException>(() => BattleSession.Create(Plan(7102), options));
    }

    // ③ 同上：含 null 步骤同样在装配期拒绝。
    [Fact]
    public void Options_NullReplayStep_RejectedAtAssembly()
    {
        var options = Options();
        options.Replay = new ReplayStep[] { null! };

        Assert.Throws<ArgumentException>(() => BattleSession.Create(Plan(7102), options));
    }

    // ⑤ 空载荷结束通知：不臆造结束语义——不进终态、不回调、EndNotifyPayload 为空。
    [Fact]
    public async Task BattleEndNotify_EmptyPayload_DoesNotEnterTerminal()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromMilliseconds(200);
        await using var session = FakeSession(transport, options);
        var pushes = 0;
        session.Push += _ => Interlocked.Increment(ref pushes);
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.BattleEndNotify, Array.Empty<byte>());
        await Task.Delay(250);

        Assert.False(session.HasEnded);
        Assert.Null(session.EndNotifyPayload);
        Assert.Equal(0, Volatile.Read(ref pushes)); // 空载荷既不进终态也不分发。
        Assert.Equal(ClientState.Connected, session.State);
        await session.CloseAsync();
    }

    // ⑤ 首投载荷留档：EndNotifyPayload 返回副本（重复投递不改写首个载荷、不重放订阅回调）。
    [Fact]
    public async Task EndNotifyPayload_KeepsFirstPayload_AndIsACopy()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromMilliseconds(300);
        await using var session = FakeSession(transport, options);
        await session.ConnectAsync();

        var first = TestWire.EndPayload("p-1");
        transport.QueueNotify(TestOps.BattleEndNotify, first);
        Assert.True(await WaitUntilAsync(() => session.HasEnded), "结束通知应进入终态");

        var stored = session.EndNotifyPayload;
        Assert.Equal(first, stored);
        stored![0] = 0xFF; // 改副本不影响内部留档。
        Assert.Equal(first, session.EndNotifyPayload);

        transport.QueueNotify(TestOps.BattleEndNotify, TestWire.EndPayload("p-2"));
        await Task.Delay(150);
        Assert.Equal(first, session.EndNotifyPayload); // 首个载荷为准。
        await session.CloseAsync();
    }
}
