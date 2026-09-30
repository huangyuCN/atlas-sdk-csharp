using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionTest 覆盖直连战斗会话的行为口径：面选择、入局/输入/补帧、
// 推送回调、重连重新 hello + 补帧、票过期的可判定异常、接入层拒绝不重试。
public sealed class BattleSessionTest
{
    // Ticket 是测试票据密文（非真实 AEAD 票：接入层/帧面只按字节透传与编码）。
    private static readonly byte[] Ticket = { 0xfb, 0xff, 0x00, 0x01, 0xfe, 0x10, 0x20 };

    private static string Slot => EdgeWire.TicketSlot(Ticket);

    private static DirectPlan Plan(int port, string face, byte[]? ticket = null)
    {
        var encoded = Convert.ToBase64String(ticket ?? Ticket);
        return DirectPlan.FromNotify("{\"match_id\":\"m-1\",\"battle_id\":\"b-1\",\"battle_ticket\":\""
            + encoded + "\",\"endpoints\":[{\"transport\":\"" + face + "\",\"address\":\"127.0.0.1:" + port + "\"}]}");
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

    private static JsonElement Parse(byte[] payload)
    {
        using var document = JsonDocument.Parse(payload);
        return document.RootElement.Clone();
    }

    [Fact]
    public void Create_NullPlan_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => BattleSession.Create(null!));
    }

    [Fact]
    public void Create_PreferredFaceNotInPlan_ThrowsTransportNotFound()
    {
        var plan = Plan(7100, EdgeTransports.WireName(EdgeTransport.Ws));
        var options = Options();
        options.PreferredTransport = EdgeTransport.Udp;

        var exception = Assert.Throws<ProtocolException>(() => BattleSession.Create(plan, options));

        Assert.StartsWith(DirectErrors.TransportNotFound, exception.Message);
    }

    [Fact]
    public void Create_NoCommonFace_ThrowsTransportNotFound()
    {
        // 只下发了 WS，而调用方只支持 KCP/UDP：不得静默换面、不得猜端口。
        var plan = Plan(7100, EdgeTransports.WireName(EdgeTransport.Ws));
        var options = Options();
        options.TransportPriority = new[] { EdgeTransport.Udp, EdgeTransport.Kcp };

        var exception = Assert.Throws<ProtocolException>(() => BattleSession.Create(plan, options));

        Assert.StartsWith(DirectErrors.TransportNotFound, exception.Message);
    }

    [Fact]
    public void Create_PicksFirstAvailableFaceByPriority()
    {
        var plan = DirectPlan.FromNotify("{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_KCP\",\"address\":\"127.0.0.1:7101\"},"
            + "{\"transport\":\"EDGE_TRANSPORT_UDP\",\"address\":\"127.0.0.1:7102\"}]}");

        var byDefault = BattleSession.Create(plan, Options());
        Assert.Equal(EdgeTransport.Kcp, byDefault.Face);

        var options = Options();
        options.TransportPriority = new[] { EdgeTransport.Udp, EdgeTransport.Kcp };
        var byOption = BattleSession.Create(plan, options);
        Assert.Equal(EdgeTransport.Udp, byOption.Face);
    }

    [Fact]
    public void Create_PreferredFaceWins_WhenPresent()
    {
        var plan = DirectPlan.FromNotify("{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"127.0.0.1:7100\"},"
            + "{\"transport\":\"EDGE_TRANSPORT_UDP\",\"address\":\"127.0.0.1:7102\"}]}");

        var options = Options();
        options.PreferredTransport = EdgeTransport.Udp;

        Assert.Equal(EdgeTransport.Udp, BattleSession.Create(plan, options).Face);
    }

    [Fact]
    public async Task Connect_SendsHello_ThenJoinBattleWithTicketSlot()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());

        await session.ConnectAsync();
        var reply = await session.JoinBattleAsync();

        // hello 段逐字节：magic(4)="ATLH" || version(1)=1 || 票长(2,大端) || 票密文。
        Assert.Equal(EdgeWire.EncodeHello(Ticket), server.HelloDatagram);
        var requests = server.RequestsOf(BattleOps.JoinBattle);
        Assert.Single(requests);
        Assert.Equal(Slot, requests[0].Slot);
        Assert.Equal("b-1", Parse(requests[0].Payload).GetProperty("battleId").GetString());
        Assert.Equal(BattleEdgeDefaults.JoinBattleReply, Encoding.UTF8.GetString(reply));
    }

    [Fact]
    public async Task Connect_RequiresTicketSlotOnEveryRequest()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());

        await session.ConnectAsync();
        await session.JoinBattleAsync();
        await session.SendFrameInputAsync(new byte[] { 0x01 });
        await session.SyncFramesAsync(0);

        var requests = server.Requests;
        Assert.Equal(3, requests.Length);
        foreach (var request in requests)
        {
            // 直连帧面逐帧带票（含 WS 长连接）：帧头 FlagSession 置位 + 会话槽为 base64url 无填充票。
            Assert.Equal(Atlas.Frame.FrameGen.FlagSession, request.Flags & Atlas.Frame.FrameGen.FlagSession);
            Assert.Equal(Slot, request.Slot);
        }
    }

    [Fact]
    public async Task SendFrameInput_ConvenienceOverload_BuildsProtojsonPayload()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());

        await session.ConnectAsync();
        await session.SendFrameInputAsync(3, new byte[] { 0xaa, 0xbb });

        var request = server.RequestsOf(BattleOps.SendFrameInput)[0];
        var payload = Parse(request.Payload);
        Assert.Equal("b-1", payload.GetProperty("battleId").GetString());
        // protojson 的 64 位整数为字符串形态，bytes 为标准 base64。
        Assert.Equal("3", payload.GetProperty("input").GetProperty("frameId").GetString());
        Assert.Equal(Convert.ToBase64String(new byte[] { 0xaa, 0xbb }),
            payload.GetProperty("input").GetProperty("payload").GetString());
    }

    [Fact]
    public async Task SendFrameInput_RawPayload_ForwardedVerbatim()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());
        var raw = Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"input\":{\"frameId\":\"9\"}}");

        await session.ConnectAsync();
        await session.SendFrameInputAsync(raw);

        Assert.Equal(raw, server.RequestsOf(BattleOps.SendFrameInput)[0].Payload);
    }

    [Fact]
    public async Task SyncFrames_SendsLastSeenFrame()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());

        await session.ConnectAsync();
        var reply = await session.SyncFramesAsync(11);

        var request = server.RequestsOf(BattleOps.SyncFrames)[0];
        var payload = Parse(request.Payload);
        Assert.Equal("11", payload.GetProperty("lastSeenFrame").GetString());
        Assert.Equal(BattleEdgeDefaults.SyncFramesReply, Encoding.UTF8.GetString(reply));
    }

    [Fact]
    public async Task FrameBroadcast_Push_InvokesCallback_AndTracksLastSeenFrame()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());
        BattlePush? received = null;
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.FrameBroadcast += push =>
        {
            received = push;
            signal.TrySetResult(true);
        };

        await session.ConnectAsync();
        await session.JoinBattleAsync();
        var body = Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"frame\":{\"frameId\":\"9\",\"inputs\":[]}}");
        await server.PushNotifyAsync(BattleOps.FrameBroadcast, body);

        await signal.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.NotNull(received);
        Assert.Equal(BattleOps.FrameBroadcast, received!.Op);
        Assert.Equal(body, received.Payload);
        Assert.Equal(Atlas.Frame.FrameGen.Version, received.Version);
        Assert.Equal(9UL, session.LastSeenFrame);
    }

    [Fact]
    public async Task BattleEnd_Push_InvokesCallback()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());
        BattlePush? ended = null;
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.BattleEnd += push =>
        {
            ended = push;
            signal.TrySetResult(true);
        };

        await session.ConnectAsync();
        await server.PushNotifyAsync(BattleOps.BattleEndNotify,
            Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"winnerPlayerId\":\"p-1\"}"));

        await signal.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.NotNull(ended);
        Assert.Equal(BattleOps.BattleEndNotify, ended!.Op);
        Assert.Contains("p-1", Encoding.UTF8.GetString(ended.Payload), StringComparison.Ordinal);
    }

    [Fact]
    public async Task JoinBattle_TicketExpired_SurfacesBusinessReason()
    {
        await using var server = new BattleEdgeUdpServer();
        server.FailWith(BattleOps.JoinBattle, 401, DirectErrors.TicketExpiredReason, "票据已过期");
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());

        await session.ConnectAsync();
        var exception = await Assert.ThrowsAsync<BusinessException>(() => session.JoinBattleAsync());

        // 可判定异常：上层据此重新匹配（对齐 Go errors.Is(err, ErrTicketExpired) 口径）。
        Assert.True(AtlasException.IsBusinessError(exception, DirectErrors.TicketExpiredReason));
        Assert.Equal(401, exception.Code);
    }

    [Fact]
    public async Task Connect_EdgeRejectsHello_ThrowsProtocol_AndDoesNotRetry()
    {
        await using var server = new BattleEdgeUdpServer { RejectHello = true };
        var options = Options();
        options.HelloTimeoutMs = 300;
        options.AutoReconnect = true;
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), options);

        var exception = await Assert.ThrowsAsync<ProtocolException>(() => session.ConnectAsync());

        Assert.StartsWith(DirectErrors.HelloRejected, exception.Message);
        await Task.Delay(400); // 留出「若会重试」的窗口。
        Assert.Equal(1, server.HelloCount); // 接入层拒绝：不重试（与网络断开区分）。
    }

    [Fact]
    public async Task ReconnectAsync_ReHellosAndResyncsFromLastSeenFrame()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());

        await session.ConnectAsync();
        await session.JoinBattleAsync();
        session.LastSeenFrame = 7;

        await session.ReconnectAsync();

        // 重新 hello（同一张票）+ JoinBattle + SyncFrames(last_seen_frame) 补帧。
        Assert.Equal(2, server.HelloCount);
        Assert.Equal(2, server.RequestsOf(BattleOps.JoinBattle).Length);
        var syncs = server.RequestsOf(BattleOps.SyncFrames);
        Assert.Single(syncs);
        Assert.Equal("7", Parse(syncs[0].Payload).GetProperty("lastSeenFrame").GetString());
    }

    [Fact]
    public async Task CloseAsync_ThenJoin_ThrowsNetworkException()
    {
        await using var server = new BattleEdgeUdpServer();
        var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());
        await session.ConnectAsync();

        await session.CloseAsync();
        await session.CloseAsync(); // 幂等。

        await Assert.ThrowsAsync<NetworkException>(() => session.JoinBattleAsync());
        await session.DisposeAsync();
    }

    [Fact]
    public async Task Connect_Twice_Throws()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());
        await session.ConnectAsync();

        await Assert.ThrowsAsync<NetworkException>(() => session.ConnectAsync());
    }

    [Fact]
    public async Task State_ConnectedThenDisconnected()
    {
        await using var server = new BattleEdgeUdpServer();
        var session = BattleSession.Create(
            Plan(server.Port, EdgeTransports.WireName(EdgeTransport.Udp)), Options());

        Assert.Equal(ClientState.Disconnected, session.State);
        await session.ConnectAsync();
        Assert.Equal(ClientState.Connected, session.State);
        await session.CloseAsync();
        Assert.Equal(ClientState.Disconnected, session.State);
        await session.DisposeAsync();
    }
}
