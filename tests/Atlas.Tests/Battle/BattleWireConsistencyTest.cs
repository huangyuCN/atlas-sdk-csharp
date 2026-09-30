using System;
using System.Text;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Errors;
using Atlas.Frame;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleWireConsistencyTest 是接缝级 wire 一致性测试：测试内起最小本地服务端，
// 对 SDK 真实发出的字节逐字节断言——
//   TCP（WS 面）：升级请求行里的 query 票、X-Atlas-Ticket 头、帧槽与帧头标志位；
//   UDP 面：首包 hello 段逐字节 + 后续数据报的 flow-id 前缀；
//   KCP 面：首个数据报必须是 hello 段（先 hello 后会话），其后才是带前缀的 KCP 报文。
public sealed class BattleWireConsistencyTest
{
    private static readonly byte[] Ticket = { 0xfb, 0xff, 0x00, 0x01, 0xfe, 0x10, 0x20 };

    private static string Slot => EdgeWire.TicketSlot(Ticket);

    private static DirectPlan Plan(int port, string face)
    {
        return DirectPlan.FromNotify("{\"battle_id\":\"b-1\",\"battle_ticket\":\""
            + Convert.ToBase64String(Ticket) + "\",\"endpoints\":[{\"transport\":\"" + face
            + "\",\"address\":\"127.0.0.1:" + port + "\"}]}");
    }

    private static BattleSessionOptions Options()
    {
        return new BattleSessionOptions
        {
            HelloTimeoutMs = 3000,
            InvokeTimeoutMs = 3000,
            BackoffBaseMs = 50,
            BackoffMaxMs = 200,
        };
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }
            await Task.Delay(20);
        }
        Assert.Fail($"等待条件超时（{timeout.TotalMilliseconds}ms）");
    }

    [Fact]
    public async Task Ws_UpgradeQueryCarriesTicket_AndFramesCarrySlot()
    {
        await using var server = new BattleEdgeWsServer();
        await using var session = BattleSession.Create(Plan(server.Port, "EDGE_TRANSPORT_WS"), Options());

        await session.ConnectAsync();
        await session.JoinBattleAsync();

        // 升级请求行：路径 "/" + query `ticket=<base64url(票密文，无填充)>`。
        Assert.Equal("/?ticket=" + Slot, server.Targets[0]);
        var request = server.RequestsOf(BattleOps.JoinBattle)[0];
        Assert.Equal((byte)(FrameGen.FlagSession | FrameGen.FlagRequestID),
            (byte)(request.Flags & (FrameGen.FlagSession | FrameGen.FlagRequestID)));
        Assert.Equal(Slot, request.Slot);
        Assert.Equal(FrameGen.Version, request.Version);
        Assert.Contains("b-1", Encoding.UTF8.GetString(request.Payload), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Ws_TicketInHeader_WhenConfigured()
    {
        await using var server = new BattleEdgeWsServer();
        var options = Options();
        options.TicketInHeader = true;
        await using var session = BattleSession.Create(Plan(server.Port, "EDGE_TRANSPORT_WS"), options);

        await session.ConnectAsync();
        await session.JoinBattleAsync();

        // 头形态：query 不带票，票在 X-Atlas-Ticket（接入层两种都接受）。
        Assert.Equal("/", server.Targets[0]);
        Assert.Equal(Slot, server.TicketHeaders[0]);
    }

    [Fact]
    public async Task Udp_HelloByteForByte_ThenFlowIdPrefixedFrames()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(Plan(server.Port, "EDGE_TRANSPORT_UDP"), Options());

        await session.ConnectAsync();
        await session.JoinBattleAsync();

        // 首包 = hello 段：magic(4)="ATLH" || version(1)=1 || 票长(2,大端) || 票密文。
        var want = new byte[] { (byte)'A', (byte)'T', (byte)'L', (byte)'H', 0x01, 0x00, 0x07 };
        var hello = new byte[want.Length + Ticket.Length];
        Array.Copy(want, hello, want.Length);
        Array.Copy(Ticket, 0, hello, want.Length, Ticket.Length);
        Assert.Equal(hello, server.HelloDatagram);

        // 帧数据报 = flow-id(8 字节大端) || 帧；剥离后即可解出 JoinBattle 帧。
        var raw = server.LastFrameDatagram;
        Assert.True(raw.Length > EdgeWire.FlowIdLen);
        Assert.Equal(server.FlowId, EdgeWire.DecodeFlowId(raw));
        var frame = EdgeWire.StripFlowId(raw, server.FlowId);
        var (header, body) = FrameIO.DecodeMessage(frame, FrameGen.MaxBodySize);
        Assert.Equal(FrameGen.Magic, header.Magic);
        Assert.Equal(MsgType.Request, header.Type);
        var (op, slot, _, _) = Body.ParseRequestBodyFull(body, header.Flags);
        Assert.Equal(BattleOps.JoinBattle, op);
        Assert.Equal(Slot, slot);
    }

    [Fact]
    public async Task Kcp_HelloBeforeSession_OnSameSocket()
    {
        await using var server = new BattleEdgeKcpServer();
        await using var session = BattleSession.Create(Plan(server.Port, "EDGE_TRANSPORT_KCP"), Options());

        await session.ConnectAsync();
        await session.JoinBattleAsync();
        await session.SendFrameInputAsync(new byte[] { 0x01 });
        await session.SyncFramesAsync(0);

        // 顺序断言：服务端收到的**第一个数据报**就是 hello 段（先 hello，再起 KCP 会话）。
        Assert.Equal(EdgeWire.EncodeHello(Ticket), server.FirstDatagram);
        Assert.Equal(1, server.HelloCount);
        Assert.True(server.KcpDatagramCount > 0, "hello 之后应有 KCP 报文");

        // KCP 会话报文带 flow-id 前缀（服务端前缀失配即丢弃：能收到帧即证明前缀正确）。
        var requests = server.Requests;
        Assert.Equal(3, requests.Length);
        Assert.Equal(BattleOps.JoinBattle, requests[0].Op);
        Assert.Equal(Slot, requests[0].Slot);
        Assert.Equal(BattleOps.SendFrameInput, requests[1].Op);
        Assert.Equal(BattleOps.SyncFrames, requests[2].Op);
    }

    [Fact]
    public async Task Kcp_Connect_RejectedHello_NoFlowId_ThrowsProtocol()
    {
        // 接入层拒绝：hello 后不回 flow-id（仅断开 + 计数）→ SDK 按「拒绝」分类，不重试。
        await using var server = new BattleEdgeUdpServer { RejectHello = true };
        var options = Options();
        options.HelloTimeoutMs = 300;
        await using var session = BattleSession.Create(Plan(server.Port, "EDGE_TRANSPORT_KCP"), options);

        var exception = await Assert.ThrowsAsync<ProtocolException>(() => session.ConnectAsync());

        Assert.StartsWith(DirectErrors.HelloRejected, exception.Message);
    }

    [Fact]
    public async Task Ws_NetworkDrop_AutoReconnect_ReHellosAndRejoins()
    {
        // 网络断开（服务端主动断开连接）：自动重连 + 重新 hello + 重新入局（可重试）。
        await using var server = new BattleEdgeWsServer { CloseAfterRequests = 1 };
        await using var session = BattleSession.Create(Plan(server.Port, "EDGE_TRANSPORT_WS"), Options());

        await session.ConnectAsync();
        await session.JoinBattleAsync();

        await WaitUntilAsync(
            () => server.AcceptedCount >= 2 && server.RequestsOf(BattleOps.JoinBattle).Length >= 2,
            TimeSpan.FromSeconds(5));
        Assert.Equal("/?ticket=" + Slot, server.Targets[1]);
    }

    [Fact]
    public async Task Ws_TicketExpiredDuringRejoin_FiresFailed_AndDoesNotRetry()
    {
        // 票过期可回退：断线重连后的重新入局被帧面拒（BATTLE_TICKET_EXPIRED）→ 终止会话并
        // 以可判定异常通知上层（重新匹配取新票），不再拿废票重试。
        await using var server = new BattleEdgeWsServer { CloseAfterRequests = 1 };
        server.FailAfter(BattleOps.JoinBattle, 1, 401, DirectErrors.TicketExpiredReason, "票据已过期");
        await using var session = BattleSession.Create(Plan(server.Port, "EDGE_TRANSPORT_WS"), Options());
        Exception? failure = null;
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.Failed += exception =>
        {
            failure = exception;
            signal.TrySetResult(true);
        };

        await session.ConnectAsync();
        await session.JoinBattleAsync(); // 第 1 次入局：成功

        await signal.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(failure);
        Assert.True(AtlasException.IsBusinessError(failure!, DirectErrors.TicketExpiredReason));
        var attempts = server.AcceptedCount;
        await Task.Delay(400); // 留出「若会重试」的窗口。
        Assert.Equal(attempts, server.AcceptedCount);
    }

    [Fact]
    public async Task Ws_ReHelloRejected_FiresFailed_AndDoesNotRetry()
    {
        // 接入层拒绝（连接被断）与网络断开区分：拒绝即终止（不重试），并以上层可判定的
        // ProtocolException 通知（票无效/过期需重新匹配）。
        await using var server = new BattleEdgeWsServer { CloseAfterRequests = 1, RejectAfterFirst = true };
        await using var session = BattleSession.Create(Plan(server.Port, "EDGE_TRANSPORT_WS"), Options());
        Exception? failure = null;
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.Failed += exception =>
        {
            failure = exception;
            signal.TrySetResult(true);
        };

        await session.ConnectAsync();
        await session.JoinBattleAsync();

        await signal.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsType<ProtocolException>(failure);
        Assert.StartsWith(DirectErrors.WsRejected, failure!.Message);
        var attempts = server.AcceptedCount;
        await Task.Delay(400); // 留出「若会重试」的窗口。
        Assert.Equal(attempts, server.AcceptedCount);
    }
}
