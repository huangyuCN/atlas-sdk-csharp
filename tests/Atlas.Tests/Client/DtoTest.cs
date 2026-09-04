using System;
using System.Text;
using Atlas.Serialization;
using Gateway.V1;
using Xunit;

namespace Atlas.Tests.Client;

public sealed class DtoTest
{
    [Fact]
    public void ProtoJson_LoginReply_Int64AsString()
    {
        // protojson 线上形态：int64 字段序列化为字符串。
        var reply = new LoginReply
        {
            Token = "t1",
            PlayerId = "p1",
            ServerTimeUnixMs = 1234567890123L,
        };

        var json = new JsonSerializer().Serialize(reply);
        var text = Encoding.UTF8.GetString(json);

        Assert.Contains("\"serverTimeUnixMs\": \"1234567890123\"", text);
    }

    [Fact]
    public void ProtoJson_ParseBack_Roundtrip()
    {
        var reply = new LoginReply
        {
            Token = "t1",
            PlayerId = "p1",
            ServerTimeUnixMs = 1234567890123L,
        };
        var serializer = new JsonSerializer();

        var parsed = (LoginReply)serializer.Deserialize(serializer.Serialize(reply), typeof(LoginReply));

        Assert.Equal(reply.Token, parsed.Token);
        Assert.Equal(reply.PlayerId, parsed.PlayerId);
        Assert.Equal(reply.ServerTimeUnixMs, parsed.ServerTimeUnixMs);
    }
}
