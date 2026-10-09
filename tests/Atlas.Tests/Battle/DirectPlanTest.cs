using System;
using System.Collections.Generic;
using Atlas.Battle;
using Atlas.Errors;
using Xunit;

namespace Atlas.Tests.Battle;

// DirectPlanTest 覆盖成局推送的解析口径：proto 字段名与 protojson 默认键名两种形态、
// 推送信封形态、票 base64 容错，以及缺票/空票/缺面/坏载荷的明确报错（既有异常体系内）。
public sealed class DirectPlanTest
{
    // notifySnake 是服务端 UseProtoNames 口径的成局推送载荷（字段名同 proto，票为带填充标准 base64）。
    private const string NotifySnake = "{\"match_id\":\"m-1\",\"battle_id\":\"b-1\","
        + "\"player_ids\":[\"p-1\",\"p-2\"],\"battle_ticket\":\"AAECAwQ=\","
        + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"10.0.0.1:7100\"},"
        + "{\"transport\":\"EDGE_TRANSPORT_KCP\",\"address\":\"10.0.0.1:7101\"},"
        + "{\"transport\":\"EDGE_TRANSPORT_UDP\",\"address\":\"10.0.0.1:7102\"}]}";

    // notifyCamel 是 protojson 默认口径（lowerCamelCase）的同一份通知。
    private const string NotifyCamel = "{\"matchId\":\"m-1\",\"battleId\":\"b-1\","
        + "\"playerIds\":[\"p-1\",\"p-2\"],\"battleTicket\":\"AAECAwQ=\","
        + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"10.0.0.1:7100\"},"
        + "{\"transport\":\"EDGE_TRANSPORT_KCP\",\"address\":\"10.0.0.1:7101\"},"
        + "{\"transport\":\"EDGE_TRANSPORT_UDP\",\"address\":\"10.0.0.1:7102\"}]}";

    [Fact]
    public void FromNotify_ParsesSnakeCasePayload_AllFaces()
    {
        var plan = DirectPlan.FromNotify(NotifySnake);

        Assert.Equal("m-1", plan.MatchId);
        Assert.Equal("b-1", plan.BattleId);
        Assert.Equal(new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04 }, plan.Ticket);
        Assert.Equal("10.0.0.1:7100", plan.Endpoint(EdgeTransport.Ws));
        Assert.Equal("10.0.0.1:7101", plan.Endpoint(EdgeTransport.Kcp));
        Assert.Equal("10.0.0.1:7102", plan.Endpoint(EdgeTransport.Udp));
        Assert.Equal(3, plan.Endpoints.Count);
    }

    [Fact]
    public void FromNotify_ParsesCamelCasePayload()
    {
        var plan = DirectPlan.FromNotify(NotifyCamel);

        Assert.Equal("b-1", plan.BattleId);
        Assert.Equal(new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04 }, plan.Ticket);
        Assert.Equal("10.0.0.1:7102", plan.Endpoint(EdgeTransport.Udp));
    }

    [Fact]
    public void FromNotify_AcceptsPushEnvelope_DefensiveCompatibility()
    {
        // 规范形态是帧 Notify（op = 消息完整名，payload = protojson 本体）；信封只存在于
        // NATS 事件总线侧，此处按防御性兼容接受（type 用消息完整名，不是服务/方法名）。
        var envelope = "{\"type\":\"" + BattleOps.MatchStartedNotify + "\",\"payload\":" + NotifyCamel + "}";

        var plan = DirectPlan.FromNotify(envelope);

        Assert.Equal("m-1", plan.MatchId);
        Assert.Equal("b-1", plan.BattleId);
    }

    [Fact]
    public void PushOps_AreMessageFullNames()
    {
        // 推送 op 是**消息完整名**（atlas.route.v1.push 声明在消息上），不是服务/方法名：
        // 与生成物（src/Atlas/Battle/Gen/*.g.cs）逐字一致（SDK 侧不手写 op 字面量）。
        Assert.Equal("/game.v1.MatchStartedNotify", BattleOps.MatchStartedNotify);
        Assert.Equal("/battle.v1.FrameBroadcast", BattleOps.FrameBroadcast);
        Assert.Equal("/battle.v1.BattleEndNotify", BattleOps.BattleEndNotify);
        Assert.Equal("/battle.v1.PlayerOutNotify", BattleOps.PlayerOutNotify);
        // rpc op 才是服务/方法全名（客户端 op 寻址）。
        Assert.Equal("/battle.v1.BattleService/JoinBattle", BattleOps.JoinBattle);
    }

    [Theory]
    [InlineData("AAECAwQ=")]   // 带填充（protojson bytes 标准口径）
    [InlineData("AAECAwQ")]    // 无填充（容错）
    public void FromNotify_TicketBase64Variants(string encoded)
    {
        var payload = "{\"battle_id\":\"b-1\",\"battle_ticket\":\"" + encoded + "\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"h:1\"}]}";

        var plan = DirectPlan.FromNotify(payload);

        Assert.Equal(new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04 }, plan.Ticket);
    }

    [Fact]
    public void FromNotify_MissingTicketField_ThrowsNoTicket()
    {
        var payload = "{\"battle_id\":\"b-1\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"h:1\"}]}";

        var exception = Assert.Throws<ProtocolException>(() => DirectPlan.FromNotify(payload));

        Assert.StartsWith(DirectErrors.NotifyNoTicket, exception.Message);
    }

    [Fact]
    public void FromNotify_EmptyTicket_ThrowsNoTicket()
    {
        // 空票连不上接入层：不得把「服务端没出票」伪装成「客户端连不上」。
        var payload = "{\"battle_id\":\"b-1\",\"battle_ticket\":\"\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"h:1\"}]}";

        var exception = Assert.Throws<ProtocolException>(() => DirectPlan.FromNotify(payload));

        Assert.StartsWith(DirectErrors.NotifyNoTicket, exception.Message);
    }

    [Theory]
    [InlineData("{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\"}")]                                    // 无 endpoints
    [InlineData("{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\",\"endpoints\":[]}")]                    // 空列表
    [InlineData("{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\",\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_QUIC\",\"address\":\"h:1\"}]}")]
    [InlineData("{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\",\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_UNSPECIFIED\",\"address\":\"h:1\"}]}")]
    public void FromNotify_NoUsableEndpoint_ThrowsNoEndpoint(string payload)
    {
        // 缺该面即明确报错：不得回退猜端口、不得静默换面。
        var exception = Assert.Throws<ProtocolException>(() => DirectPlan.FromNotify(payload));

        Assert.StartsWith(DirectErrors.NotifyNoEndpoint, exception.Message);
    }

    [Fact]
    public void FromNotify_EmptyAddress_ThrowsNoEndpoint()
    {
        var payload = "{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"\"}]}";

        var exception = Assert.Throws<ProtocolException>(() => DirectPlan.FromNotify(payload));

        Assert.StartsWith(DirectErrors.NotifyNoEndpoint, exception.Message);
    }

    [Theory]
    [InlineData("not-json")]
    [InlineData("[1,2]")]
    [InlineData("{\"battle_id\":\"b-1\",\"battle_ticket\":\"!!!not-base64!!!\",\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"h:1\"}]}")]
    public void FromNotify_MalformedPayload_ThrowsMalformed(string payload)
    {
        var exception = Assert.Throws<ProtocolException>(() => DirectPlan.FromNotify(payload));

        Assert.StartsWith(DirectErrors.NotifyMalformed, exception.Message);
    }

    [Fact]
    public void FromNotify_UnknownFieldIgnored_ForwardCompatible()
    {
        // 服务端加字段不破坏旧客户端（对齐 protojson 的忽略未知字段语义）。
        var payload = "{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\",\"new_field\":123,"
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"h:1\",\"extra\":true}]}";

        var plan = DirectPlan.FromNotify(payload);

        Assert.Equal("b-1", plan.BattleId);
    }

    [Fact]
    public void Endpoint_MissingFace_ThrowsTransportNotFound()
    {
        var plan = DirectPlan.FromNotify("{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"h:1\"}]}");

        var exception = Assert.Throws<ProtocolException>(() => plan.Endpoint(EdgeTransport.Kcp));

        Assert.StartsWith(DirectErrors.TransportNotFound, exception.Message);
        Assert.False(plan.TryEndpoint(EdgeTransport.Kcp, out _));
        Assert.True(plan.TryEndpoint(EdgeTransport.Ws, out var address));
        Assert.Equal("h:1", address);
    }

    [Theory]
    [InlineData("EDGE_TRANSPORT_WS", EdgeTransport.Ws)]
    [InlineData("EDGE_TRANSPORT_KCP", EdgeTransport.Kcp)]
    [InlineData("EDGE_TRANSPORT_UDP", EdgeTransport.Udp)]
    [InlineData("edge_transport_ws", EdgeTransport.Ws)]
    [InlineData("ws", EdgeTransport.Ws)]
    [InlineData("kcp", EdgeTransport.Kcp)]
    [InlineData("udp", EdgeTransport.Udp)]
    public void ParseTransport_AcceptsEnumNameAndShortName(string text, EdgeTransport want)
    {
        Assert.True(EdgeTransports.TryParse(text, out var face));
        Assert.Equal(want, face);
        Assert.Equal(want, EdgeTransports.Parse(text));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("EDGE_TRANSPORT_UNSPECIFIED")]
    [InlineData("EDGE_TRANSPORT_QUIC")]
    public void ParseTransport_Unknown_Throws(string text)
    {
        Assert.False(EdgeTransports.TryParse(text, out _));
        var exception = Assert.Throws<ProtocolException>(() => EdgeTransports.Parse(text));
        Assert.StartsWith(DirectErrors.TransportUnknown, exception.Message);
    }

    [Theory]
    [InlineData(EdgeTransport.Ws, "ws", "EDGE_TRANSPORT_WS")]
    [InlineData(EdgeTransport.Kcp, "kcp", "EDGE_TRANSPORT_KCP")]
    [InlineData(EdgeTransport.Udp, "udp", "EDGE_TRANSPORT_UDP")]
    public void EdgeTransport_TextForms(EdgeTransport face, string shortName, string wireName)
    {
        Assert.Equal(shortName, EdgeTransports.ShortName(face));
        Assert.Equal(wireName, EdgeTransports.WireName(face));
    }

    [Theory]
    [InlineData(EdgeTransport.Ws, 1)]
    [InlineData(EdgeTransport.Kcp, 2)]
    [InlineData(EdgeTransport.Udp, 3)]
    public void EdgeTransport_WireNumbersMatchProto(EdgeTransport face, int number)
    {
        // 数值对齐 battle.v1.EdgeTransport 枚举（协议唯一来源，不得漂移）。
        Assert.Equal(number, (int)face);
    }

    [Fact]
    public void FromNotify_ByteArrayOverload_SameAsString()
    {
        var plan = DirectPlan.FromNotify(System.Text.Encoding.UTF8.GetBytes(NotifySnake));

        Assert.Equal("b-1", plan.BattleId);
    }

    [Fact]
    public void FromNotify_TicketOnlyEndpointSkipped_RemainingFaceUsable()
    {
        // 某面地址为空不阻断其他可用面：可用的那面照常选出。
        var payload = "{\"battle_id\":\"b-1\",\"battle_ticket\":\"AAECAwQ=\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"\"},"
            + "{\"transport\":\"EDGE_TRANSPORT_KCP\",\"address\":\"h:7101\"}]}";

        var plan = DirectPlan.FromNotify(payload);

        Assert.False(plan.TryEndpoint(EdgeTransport.Ws, out _));
        Assert.Equal("h:7101", plan.Endpoint(EdgeTransport.Kcp));
    }

    [Fact]
    public void Endpoints_IsReadOnlySnapshot()
    {
        var plan = DirectPlan.FromNotify(NotifySnake);
        var snapshot = new Dictionary<EdgeTransport, string>(plan.Endpoints);

        Assert.Equal(3, snapshot.Count);
        Assert.Throws<NotSupportedException>(() =>
            ((IDictionary<EdgeTransport, string>)plan.Endpoints).Add(EdgeTransport.Ws, "x:1"));
    }
}
