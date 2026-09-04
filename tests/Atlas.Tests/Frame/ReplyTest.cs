using System;
using System.Collections.Generic;
using System.Text;
using Atlas.Errors;
using Atlas.Frame;
using Xunit;

namespace Atlas.Tests.Frame;

public sealed class ReplyTest
{
    [Fact]
    public void BuildBody_Roundtrip_PreservesOperationAndPayload()
    {
        var payload = Encoding.UTF8.GetBytes("{\"x\":1}");

        var body = Body.BuildRequestBody("/gateway.v1.GatewayAuth/Login", payload);
        var (operation, actualPayload) = Body.ParseRequestBody(body);

        Assert.Equal("/gateway.v1.GatewayAuth/Login", operation);
        Assert.Equal(payload, actualPayload);
    }

    [Fact]
    public void BuildBody_UsesBigEndianOperationLength()
    {
        var actual = Body.BuildRequestBody("/a", new byte[] { 0x01, 0x02 });

        Assert.Equal(new byte[] { 0x00, 0x02, 0x2F, 0x61, 0x01, 0x02 }, actual);
    }

    [Fact]
    public void BuildBody_NullPayload_UsesEmptyPayload()
    {
        var actual = Body.BuildRequestBody("/a", null!);

        Assert.Equal(new byte[] { 0x00, 0x02, 0x2F, 0x61 }, actual);
    }

    [Fact]
    public void BuildBody_OperationLengthBoundary()
    {
        var maximum = new string('a', FrameConst.MaxOperationLen);

        var actual = Body.BuildRequestBody(maximum, Array.Empty<byte>());

        Assert.Equal(FrameConst.MaxOperationLen + 2, actual.Length);
        Assert.Throws<ProtocolException>(() => Body.BuildRequestBody(maximum + "a", Array.Empty<byte>()));
    }

    [Fact]
    public void BuildBody_EmptyOperation_ThrowsArgument()
    {
        Assert.Throws<ArgumentException>(() => Body.BuildRequestBody("", Array.Empty<byte>()));
    }

    [Fact]
    public void ParseBody_TruncatedOperation_ThrowsProtocol()
    {
        Assert.Throws<ProtocolException>(() => Body.ParseRequestBody(new byte[] { 0, 2, (byte)'a' }));
    }

    [Fact]
    public void DecodeReply_Success_ReturnsData()
    {
        var data = Encoding.UTF8.GetBytes("{\"playerId\":\"p1\"}");
        var body = ReplySuccess(data);

        var reply = Reply.DecodeReply(body);

        Assert.Null(reply.Status);
        Assert.Equal(data, reply.Data);
    }

    [Theory]
    [MemberData(nameof(TruncatedReplyBodies))]
    public void DecodeReply_TruncatedBody_ThrowsProtocol(byte[] body)
    {
        Assert.Throws<ProtocolException>(() => Reply.DecodeReply(body));
    }

    [Fact]
    public void DecodeReply_LengthAboveInt32Max_ThrowsProtocol()
    {
        var body = new byte[] { 0, 0x80, 0x00, 0x00, 0x00 };

        Assert.Throws<ProtocolException>(() => Reply.DecodeReply(body));
    }

    [Fact]
    public void DecodeReply_Error_ReturnsStatusAndData()
    {
        var status = Concat(
            new byte[] { 0x12, 0x08 }, Encoding.UTF8.GetBytes("NO_TOKEN"),
            new byte[] { 0x1A, 0x09 }, Encoding.UTF8.GetBytes("缺 token"));
        var data = Encoding.UTF8.GetBytes("details");

        var reply = Reply.DecodeReply(ReplyError(status, data));

        Assert.NotNull(reply.Status);
        Assert.Equal("NO_TOKEN", reply.Status!.Reason);
        Assert.Equal("缺 token", reply.Status.Message);
        Assert.Equal(data, reply.Data);
    }

    [Fact]
    public void DecodeReply_EmptyStatus_UsesZeroValueStatus()
    {
        var reply = Reply.DecodeReply(ReplyError(Array.Empty<byte>(), Array.Empty<byte>()));

        Assert.NotNull(reply.Status);
        Assert.Equal(0, reply.Status!.Code);
        Assert.Empty(reply.Status.Metadata);
    }

    [Fact]
    public void DecodeStatus_FullWire_FieldsPopulated()
    {
        var status = Concat(
            new byte[] { 0x08, 0x05 },
            new byte[] { 0x12, 0x01, (byte)'x' },
            new byte[] { 0x1A, 0x01, (byte)'m' },
            new byte[] { 0x22, 0x06, 0x0A, 0x01, (byte)'k', 0x12, 0x01, (byte)'v' });

        var actual = StatusWire.Decode(status);

        Assert.Equal(5, actual.Code);
        Assert.Equal("x", actual.Reason);
        Assert.Equal("m", actual.Message);
        Assert.Equal("v", actual.Metadata["k"]);
    }

    [Fact]
    public void DecodeStatus_NegativeCode_UsesLowInt32Bits()
    {
        var negativeOne = Concat(new byte[] { 0x08 }, Repeat(0xFF, 9), new byte[] { 0x01 });

        var actual = StatusWire.Decode(negativeOne);

        Assert.Equal(-1, actual.Code);
    }

    [Theory]
    [InlineData((byte)0x09)]
    [InlineData((byte)0x0D)]
    public void DecodeStatus_UnsupportedWireType_ThrowsProtocol(byte tag)
    {
        var field = tag == 0x09
            ? Concat(new byte[] { tag }, new byte[8])
            : Concat(new byte[] { tag }, new byte[4]);

        Assert.Throws<ProtocolException>(() => StatusWire.Decode(field));
    }

    [Fact]
    public void DecodeStatus_UnknownLegalField_IsIgnored()
    {
        var status = new byte[] { 0x28, 0x01, 0x12, 0x01, (byte)'x' };

        var actual = StatusWire.Decode(status);

        Assert.Equal("x", actual.Reason);
    }

    [Fact]
    public void DecodeStatus_UnknownBytesField_IsIgnored()
    {
        var status = new byte[] { 0x32, 0x01, 0x7F, 0x12, 0x01, (byte)'x' };

        var actual = StatusWire.Decode(status);

        Assert.Equal("x", actual.Reason);
    }

    [Theory]
    [MemberData(nameof(InvalidStatusWires))]
    public void DecodeStatus_TruncatedWire_ThrowsProtocol(byte[] wire)
    {
        Assert.Throws<ProtocolException>(() => StatusWire.Decode(wire));
    }

    public static IEnumerable<object[]> TruncatedReplyBodies()
    {
        yield return new object[] { new byte[] { 0, 0, 0, 0, 1 } };
        yield return new object[] { new byte[] { 1, 0, 0, 0, 1 } };
        yield return new object[] { new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 } };
        yield return new object[] { new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 1 } };
    }

    public static IEnumerable<object[]> InvalidStatusWires()
    {
        yield return new object[] { new byte[] { 0x12, 0x02, (byte)'x' } };
        yield return new object[] { Concat(new byte[] { 0x08 }, Repeat(0x80, 10)) };
    }

    private static byte[] ReplySuccess(byte[] data)
    {
        var body = new byte[5 + data.Length];
        body[0] = 0;
        WriteUInt32(body, 1, data.Length);
        Array.Copy(data, 0, body, 5, data.Length);
        return body;
    }

    private static byte[] ReplyError(byte[] status, byte[] data)
    {
        var body = new byte[9 + status.Length + data.Length];
        body[0] = 1;
        WriteUInt32(body, 1, status.Length);
        Array.Copy(status, 0, body, 5, status.Length);
        WriteUInt32(body, 5 + status.Length, data.Length);
        Array.Copy(data, 0, body, 9 + status.Length, data.Length);
        return body;
    }

    private static void WriteUInt32(byte[] bytes, int offset, int value)
    {
        bytes[offset] = (byte)(value >> 24);
        bytes[offset + 1] = (byte)(value >> 16);
        bytes[offset + 2] = (byte)(value >> 8);
        bytes[offset + 3] = (byte)value;
    }

    private static byte[] Concat(params byte[][] parts)
    {
        var length = 0;
        foreach (var part in parts)
        {
            length += part.Length;
        }

        var result = new byte[length];
        var offset = 0;
        foreach (var part in parts)
        {
            Array.Copy(part, 0, result, offset, part.Length);
            offset += part.Length;
        }

        return result;
    }

    private static byte[] Repeat(byte value, int count)
    {
        var values = new byte[count];
        for (var index = 0; index < count; index++)
        {
            values[index] = value;
        }

        return values;
    }
}
