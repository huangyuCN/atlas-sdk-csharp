using System;
using Atlas.Serialization;
using Gateway.V1;
using PocoHeartbeatReply = Atlas.Gateway.V1.HeartbeatReply;
using ProtoJoinBattleReq = global::Battle.V1.JoinBattleReq;
using PocoLoginReply = Atlas.Gateway.V1.LoginReply;
using Xunit;

namespace Atlas.Tests.Client;

// ProtobufSerializer（ver=2 二进制，只收 IMessage）与 JsonSerializer（ver=1 protojson，
// 兼收 IMessage 与生成 stub 的 POCO）的编解码语义验证——字段与权威协议锚点对齐
// （LoginReply: player_id=1 / token=2 / player=3）。
public sealed class SerializerTest
{
    [Fact]
    public void Json_IgnoreUnknownFields_ToleratesServerExtraFields()
    {
        // 服务端 LoginReply 额外返回 player（common.PlayerSummary）——旧客户端 DTO
        // 无该字段时解析必须忽略而非报错（对齐 Go DiscardUnknown：服务端加字段不破坏旧客户端）。
        var json = "{\"playerId\":\"p1\",\"token\":\"t1\",\"player\":{\"playerId\":\"p1\"}}";
        var serializer = new JsonSerializer();

        var parsed = (LoginReply)serializer.Deserialize(System.Text.Encoding.UTF8.GetBytes(json), typeof(LoginReply));

        Assert.Equal("p1", parsed.PlayerId);
        Assert.Equal("t1", parsed.Token);
    }

    [Fact]
    public void Protobuf_Version_IsTwo()
    {
        Assert.Equal(2, new ProtobufSerializer().Version);
    }

    [Fact]
    public void Protobuf_BinaryRoundtrip_PreservesFields()
    {
        var reply = new LoginReply { PlayerId = "p1", Token = "tok-1" };
        var serializer = new ProtobufSerializer();

        var bytes = serializer.Serialize(reply);
        var parsed = (LoginReply)serializer.Deserialize(bytes, typeof(LoginReply));

        Assert.Equal("p1", parsed.PlayerId);
        Assert.Equal("tok-1", parsed.Token);
    }

    [Fact]
    public void Protobuf_Wire_IsCompactBinary()
    {
        // LoginReply{player_id="p1", token="t"}: field1(len2)=p1, field2(len1)=t
        // 期望紧凑 wire（非 JSON）：0x0A 0x02 'p' '1' 0x12 0x01 't'
        var reply = new LoginReply { PlayerId = "p1", Token = "t" };
        var bytes = new ProtobufSerializer().Serialize(reply);

        Assert.Equal(new byte[] { 0x0A, 0x02, (byte)'p', (byte)'1', 0x12, 0x01, (byte)'t' }, bytes);
    }

    [Fact]
    public void Json_ProtoJsonSemantics_Int64AsString()
    {
        // protojson 线上形态：int64 字段序列化为字符串（规范 §6.2）。
        var request = new HeartbeatRequest { Ts = 1234567890123L };
        var serializer = new JsonSerializer();

        var json = System.Text.Encoding.UTF8.GetString(serializer.Serialize(request));

        Assert.Contains("\"ts\": \"1234567890123\"", json);
    }

    [Fact]
    public void Protobuf_RejectsNonMessageType()
    {
        var serializer = new ProtobufSerializer();
        Assert.Throws<ArgumentException>(() => serializer.Deserialize(new byte[] { 0x08, 0x00 }, typeof(string)));
    }

    [Fact]
    public void Protobuf_RejectsPocoObject()
    {
        // R12 边界：ver=2（protobuf 二进制）只收 IMessage；生成 stub 的 POCO 走 ver=1。
        var serializer = new ProtobufSerializer();
        Assert.Throws<ArgumentException>(() => serializer.Serialize(new PocoLoginReply { PlayerId = "p1" }));
        Assert.Throws<ArgumentException>(() => serializer.Deserialize(new byte[] { 0x0A, 0x02 }, typeof(PocoLoginReply)));
    }

    [Fact]
    public void NullInput_ThrowsArgumentNull()
    {
        var serializer = new ProtobufSerializer();
        Assert.Throws<ArgumentNullException>(() => serializer.Serialize((Google.Protobuf.IMessage)null!));
        Assert.Throws<ArgumentNullException>(() => serializer.Serialize((object)null!));
        Assert.Throws<ArgumentNullException>(() => serializer.Deserialize(null!, typeof(LoginReply)));
    }

    // Json_SerializeObject_PocoRoundtrip 验证对象重载（R12 方案 A）：POCO DTO
    // （生成 stub 的 DTO）经 System.Text.Json 分支往返，字段名取 [JsonPropertyName]。
    [Fact]
    public void Json_SerializeObject_PocoRoundtrip()
    {
        var serializer = new JsonSerializer();
        var reply = new PocoLoginReply { PlayerId = "p1", Token = "t1" };

        var json = System.Text.Encoding.UTF8.GetString(serializer.Serialize((object)reply));
        Assert.Contains("\"playerId\":\"p1\"", json);

        var parsed = (PocoLoginReply)serializer.Deserialize(
            System.Text.Encoding.UTF8.GetBytes(json), typeof(PocoLoginReply));
        Assert.Equal("p1", parsed.PlayerId);
        Assert.Equal("t1", parsed.Token);
    }

    // Json_Poco_ReadsInt64FromString 验证 POCO 分支尊重 [JsonNumberHandling]：
    // protojson 下发的 int64 字符串（规范 §6.2）可读入 long/ulong 字段。
    [Fact]
    public void Json_Poco_ReadsInt64FromString()
    {
        var serializer = new JsonSerializer();

        var parsed = (PocoHeartbeatReply)serializer.Deserialize(
            System.Text.Encoding.UTF8.GetBytes("{\"serverTimeUnixMs\":\"1700000000000\"}"),
            typeof(PocoHeartbeatReply));

        Assert.Equal(1700000000000UL, parsed.ServerTimeUnixMs);
    }

    // Protobuf_SmokeRequestDtos_Roundtrip 验证冒烟 ver=2 路径的请求 DTO（生成物
    // IMessage 族）经 ProtobufSerializer 往返：登录（含 client_version 上报）与
    // 战斗域 JoinBattle（权威 proto：battle_id 客体寻址，消息体零身份字段）。
    [Fact]
    public void Protobuf_SmokeRequestDtos_Roundtrip()
    {
        var serializer = new ProtobufSerializer();

        var login = new LoginRequest
        {
            PlayerId = "p1",
            Password = "pw",
            ClientVersion = AtlasVersion.Value,
        };
        var parsedLogin = (LoginRequest)serializer.Deserialize(
            serializer.Serialize(login), typeof(LoginRequest));
        Assert.Equal("p1", parsedLogin.PlayerId);
        Assert.Equal(AtlasVersion.Value, parsedLogin.ClientVersion);

        var join = new ProtoJoinBattleReq { BattleId = "b1" };
        var parsedJoin = (ProtoJoinBattleReq)serializer.Deserialize(
            serializer.Serialize(join), typeof(ProtoJoinBattleReq));
        Assert.Equal("b1", parsedJoin.BattleId);
    }

    // Json_Poco_OmitsUnsetFields 验证 POCO 分支的零值省略语义（对齐 protojson 默认
    // 不下发零值/未设置字段）。
    [Fact]
    public void Json_Poco_OmitsUnsetFields()
    {
        var serializer = new JsonSerializer();

        var json = System.Text.Encoding.UTF8.GetString(
            serializer.Serialize((object)new PocoLoginReply { Token = "t1" }));

        Assert.Contains("\"token\":\"t1\"", json);
        Assert.DoesNotContain("playerId", json);
    }
}
