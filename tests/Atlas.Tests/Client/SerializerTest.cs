using System;
using Atlas.Serialization;
using Gateway.V1;
using Xunit;

namespace Atlas.Tests.Client;

// ProtobufSerializer（ver=2 二进制）与 JsonSerializer（ver=1 protojson）的
// DTO 编解码语义验证——字段与协议锚点对齐（LoginReply: player_id=1/token=2）。
public sealed class SerializerTest
{
    [Fact]
    public void Json_IgnoreUnknownFields_ToleratesServerExtraFields()
    {
        // 服务端 LoginReply 额外返回 player（common.PlayerSummary）——旧客户端 DTO
        // 无该字段，解析必须忽略而非报错（对齐 Go DiscardUnknown：服务端加字段不破坏旧客户端）。
        var json = "{\"playerId\":\"p1\",\"token\":\"t1\",\"player\":{\"id\":\"p1\"}}";
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
        var reply = new LoginReply { PlayerId = "p1", Token = "tok-1", ServerTimeUnixMs = 1234567890123L };
        var serializer = new ProtobufSerializer();

        var bytes = serializer.Serialize(reply);
        var parsed = (LoginReply)serializer.Deserialize(bytes, typeof(LoginReply));

        Assert.Equal("p1", parsed.PlayerId);
        Assert.Equal("tok-1", parsed.Token);
        Assert.Equal(1234567890123L, parsed.ServerTimeUnixMs);
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
        var reply = new LoginReply { PlayerId = "p1", ServerTimeUnixMs = 1234567890123L };
        var serializer = new JsonSerializer();

        var json = System.Text.Encoding.UTF8.GetString(serializer.Serialize(reply));

        Assert.Contains("\"serverTimeUnixMs\": \"1234567890123\"", json);
    }

    [Fact]
    public void Protobuf_RejectsNonMessageType()
    {
        var serializer = new ProtobufSerializer();
        Assert.Throws<ArgumentException>(() => serializer.Deserialize(new byte[] { 0x08, 0x00 }, typeof(string)));
    }

    [Fact]
    public void NullInput_ThrowsArgumentNull()
    {
        var serializer = new ProtobufSerializer();
        Assert.Throws<ArgumentNullException>(() => serializer.Serialize(null!));
        Assert.Throws<ArgumentNullException>(() => serializer.Deserialize(null!, typeof(LoginReply)));
    }
}
