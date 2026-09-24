using System;
using Atlas.Errors;
using Atlas.Frame;
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

    // ErrorClassCodec_ParsesEnumNameAndNumber 验证错误分类同时接受枚举名（protojson
    // 下发形态）与数值（帧 Status.class 的 wire 形态），未知取值回落未分类（前向兼容）。
    [Fact]
    public void ErrorClassCodec_ParsesEnumNameAndNumber()
    {
        // 枚举名（errors.proto 的 ErrorClass 枚举名）。
        Assert.Equal(ErrorClass.Unspecified, ErrorClassCodec.Parse("ERROR_CLASS_UNSPECIFIED"));
        Assert.Equal(ErrorClass.Business, ErrorClassCodec.Parse("ERROR_CLASS_BUSINESS"));
        Assert.Equal(ErrorClass.Runtime, ErrorClassCodec.Parse("ERROR_CLASS_RUNTIME"));
        Assert.Equal(ErrorClass.Canceled, ErrorClassCodec.Parse("ERROR_CLASS_CANCELED"));
        // 数值（字符串与数字两形态）。
        Assert.Equal(ErrorClass.Runtime, ErrorClassCodec.Parse("2"));
        Assert.Equal(ErrorClass.Canceled, ErrorClassCodec.Of(3));
        // 稳定标签（gRPC metadata 形态）。
        Assert.Equal(ErrorClass.Business, ErrorClassCodec.Parse("business"));
        Assert.Equal(ErrorClass.Runtime, ErrorClassCodec.Parse("Runtime"));
        // 空/未知：回落未分类，不抛错（新增分类不炸旧客户端）。
        Assert.Equal(ErrorClass.Unspecified, ErrorClassCodec.Parse(null));
        Assert.Equal(ErrorClass.Unspecified, ErrorClassCodec.Parse(""));
        Assert.Equal(ErrorClass.Unspecified, ErrorClassCodec.Parse("ERROR_CLASS_FUTURE"));
        Assert.Equal(ErrorClass.Unspecified, ErrorClassCodec.Parse("不是分类"));
    }

    // Status_ClassSupportsWireNumberAndJsonName 验证 Status 的分类解析两条形态：
    // wire（proto enum 数值）与文本（protojson 枚举名/数值）。
    [Fact]
    public void Status_ClassSupportsWireNumberAndJsonName()
    {
        // wire 形态：field 5 varint = 2（runtime）。
        var status = StatusWire.Decode(new byte[] { 0x28, 0x02 });
        Assert.Equal(ErrorClass.Runtime, status.Class);

        // 文本形态（protojson 下发枚举名；数值同样接受）。
        Assert.Equal(ErrorClass.Runtime, Status.ParseClass("ERROR_CLASS_RUNTIME"));
        Assert.Equal(ErrorClass.Runtime, Status.ParseClass("2"));
    }

    // BusinessException_ProjectsErrorClass 验证业务异常携带分类枚举（含标签/枚举名转换）。
    [Fact]
    public void BusinessException_ProjectsErrorClass()
    {
        var exception = new BusinessException(404, "PLAYER_NOT_FOUND", "玩家不存在", null, ErrorClass.Business);

        Assert.Equal(ErrorClass.Business, exception.Class);
        Assert.Equal("business", ErrorClassCodec.Label(exception.Class));
        Assert.Equal("ERROR_CLASS_BUSINESS", ErrorClassCodec.ProtoName(exception.Class));
        Assert.Equal(ErrorClass.Unspecified, new BusinessException(5, "R", "m", null).Class);
    }

    [Fact]
    public void IsBusinessError_FindsWrappedBusinessException()
    {
        Exception exception = new InvalidOperationException(
            "包装错误",
            new BusinessException(5, "PLAYER_NOT_FOUND", "玩家不存在", null));

        Assert.True(AtlasException.IsBusinessError(exception, "PLAYER_NOT_FOUND"));
        Assert.False(AtlasException.IsBusinessError(exception, "OTHER"));
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
