using System;
using Atlas.Errors;
using Atlas.Serialization;
using Google.Protobuf.WellKnownTypes;
using Xunit;

namespace Atlas.Tests.Errors;

public sealed class ErrorsTest
{
    [Fact]
    public void IsBusinessError_MatchesReasonOnly()
    {
        var exception = new BusinessException(5, "PLAYER_NOT_FOUND", "玩家不存在", null);

        Assert.True(AtlasException.IsBusinessError(exception, "PLAYER_NOT_FOUND"));
        Assert.False(AtlasException.IsBusinessError(exception, "OTHER"));
        Assert.False(AtlasException.IsBusinessError(new NetworkException("断线"), "PLAYER_NOT_FOUND"));
    }

    [Fact]
    public void ProtocolException_InheritsAtlasExceptionAndPreservesInnerException()
    {
        var cause = new InvalidOperationException("底层错误");
        var exception = new ProtocolException("协议错误", cause);

        Assert.IsAssignableFrom<AtlasException>(exception);
        Assert.Equal("协议错误", exception.Message);
        Assert.Same(cause, exception.InnerException);
    }

    [Fact]
    public void JsonSerializer_ProtoJson_RoundtripsWellKnownMessages()
    {
        ISerializer serializer = new JsonSerializer();
        var value = new StringValue { Value = "atlas" };
        var encoded = serializer.Serialize(value);
        var decoded = Assert.IsType<StringValue>(serializer.Deserialize(encoded, typeof(StringValue)));

        Assert.Equal("atlas", decoded.Value);
    }

    [Fact]
    public void JsonSerializer_ProtoJson_FormatsInt64AsString()
    {
        ISerializer serializer = new JsonSerializer();
        var value = new Int64Value { Value = 1234567890123L };

        var json = System.Text.Encoding.UTF8.GetString(serializer.Serialize(value));

        Assert.Equal("\"1234567890123\"", json);
    }
}
