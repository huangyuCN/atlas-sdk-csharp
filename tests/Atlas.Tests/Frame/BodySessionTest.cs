using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Xunit;

namespace Atlas.Tests.Frame;

// BodySessionTest 验证帧 body 会话槽与帧头 flags 位图语义（对齐 Go
// frame/body_session_test.go）：带槽布局 [opLen][op][sessionLen][session][payload]、
// 未置位旧布局 [opLen][op][payload]、槽截断报协议错误、flags 未知位拒绝。
public sealed class BodySessionTest
{
    private const string Operation = "/game.v1.PlayerService/EnterMatchQueue";

    // TestSessionSlotRoundTrip 验证会话槽 body 编解码往返：
    // [opLen][op][sessionLen][session][payload] 与帧头 FlagSession 配对解析。
    [Fact]
    public void SessionSlot_RoundTrip_ParsesOperationSessionPayload()
    {
        var body = Body.BuildRequestBodyWithSession(Operation, "tok-abc", Encoding.UTF8.GetBytes(@"{""ruleset"":""rank""}"));

        var (operation, session, payload) = Body.ParseRequestBodyWithSession(body, FrameConst.FlagSession);

        Assert.Equal(Operation, operation);
        Assert.Equal("tok-abc", session);
        Assert.Equal(@"{""ruleset"":""rank""}", Encoding.UTF8.GetString(payload));
    }

    // SessionSlot_Absent 验证 flags 未置位时解析走旧布局（会话槽缺席不影响）。
    [Fact]
    public void SessionSlot_Absent_ParsesLegacyLayout()
    {
        var body = Body.BuildRequestBody("/game.v1.PlayerService/GetPlayer", Encoding.UTF8.GetBytes("{}"));

        var (operation, session, payload) = Body.ParseRequestBodyWithSession(body, 0);

        Assert.Equal("", session);
        Assert.Equal("/game.v1.PlayerService/GetPlayer", operation);
        Assert.Equal("{}", Encoding.UTF8.GetString(payload));
        // 旧封装接口解析结果与带 flags 参数接口一致。
        var legacy = Body.ParseRequestBody(body);
        Assert.Equal(operation, legacy.Operation);
        Assert.Equal(payload, legacy.Payload);
    }

    // SessionSlot_Truncated 验证置位但槽截断时报协议错误。
    [Fact]
    public void SessionSlot_Truncated_ThrowsProtocol()
    {
        var body = Body.BuildRequestBodyWithSession("/op", "abc", null);
        var truncated = new byte[body.Length - 1];
        Array.Copy(body, truncated, truncated.Length);

        Assert.Throws<ProtocolException>(() => Body.ParseRequestBodyWithSession(truncated, FrameConst.FlagSession));
    }

    // SessionSlot_MissingLength 验证置位但槽长度字节缺席时报协议错误。
    [Fact]
    public void SessionSlot_MissingLength_ThrowsProtocol()
    {
        // 无槽布局 body（op + 1B payload），按 FlagSession 置位解析 → 缺槽长度。
        var body = Body.BuildRequestBody("/op", new byte[] { 1 });

        var exception = Assert.Throws<ProtocolException>(() =>
            Body.ParseRequestBodyWithSession(body, FrameConst.FlagSession));
        Assert.Contains("缺少", exception.Message);
    }

    // SessionSlot_OverLimit 验证会话槽超过 256B 上限时封装报协议错误。
    [Fact]
    public void SessionSlot_OverLimit_ThrowsProtocol()
    {
        var oversized = new string('t', FrameConst.MaxSessionLen + 1);

        Assert.Throws<ProtocolException>(() =>
            Body.BuildRequestBodyWithSession("/op", oversized, null));
    }

    // SessionSlot_AtLimit 验证会话槽恰为 256B 上限时可正常往返。
    [Fact]
    public void SessionSlot_AtLimit_RoundTrips()
    {
        var token = new string('t', FrameConst.MaxSessionLen);
        var body = Body.BuildRequestBodyWithSession("/op", token, null);

        var (_, session, _) = Body.ParseRequestBodyWithSession(body, FrameConst.FlagSession);

        Assert.Equal(token, session);
    }

    // Header_FlagsRoundTrip 验证 flags 经流式帧与消息边界帧的完整往返。
    [Fact]
    public async Task Header_FlagsRoundTrip_StreamAndMessage()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Flags = FrameConst.FlagSession,
            Seq = 7,
        };
        var body = Body.BuildRequestBodyWithSession("/op", "tok", null);

        // 流式：WriteFrameAsync/ReadFrameAsync 往返。
        await using var stream = new System.IO.MemoryStream();
        await FrameIO.WriteFrameAsync(stream, header, body, FrameConst.MaxBodySize, System.Threading.CancellationToken.None);
        stream.Position = 0;
        var (streamHeader, streamBody) = await FrameIO.ReadFrameAsync(
            stream, FrameConst.MaxBodySize, System.Threading.CancellationToken.None);
        Assert.Equal(FrameConst.FlagSession, streamHeader.Flags);
        Assert.Equal(7u, streamHeader.Seq);
        Assert.Equal(body, streamBody);

        // 消息边界：EncodeMessage/DecodeMessage 往返。
        var message = FrameIO.EncodeMessage(header, body, FrameConst.MaxBodySize);
        var (messageHeader, messageBody) = FrameIO.DecodeMessage(message, FrameConst.MaxBodySize);
        Assert.Equal(FrameConst.FlagSession, messageHeader.Flags);
        Assert.Equal(body, messageBody);
    }

    // Header_FlagsByteLayout 验证 flags 写入帧头第 7 字节（buf[6]），帧头仍 16B。
    [Fact]
    public void Header_FlagsByte_WrittenAtOffset6()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Flags = FrameConst.FlagSession,
            Seq = 7,
        };

        var bytes = header.Encode();

        Assert.Equal(FrameConst.HeaderSize, bytes.Length);
        Assert.Equal(FrameConst.FlagSession, bytes[6]);
        Assert.Equal(0, bytes[7]);
    }

    // Header_RejectsUnknownFlags 验证未知 flags 位被拒绝（前向保留位白名单）。
    [Theory]
    [InlineData((byte)0x04)]
    [InlineData((byte)0xFE)]
    [InlineData((byte)0xFF)]
    public void Header_UnknownFlags_ThrowsProtocol(byte flags)
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Flags = flags,
            Seq = 1,
        };

        var exception = Assert.Throws<ProtocolException>(() => Header.Decode(header.Encode()));

        Assert.Contains("flags", exception.Message);
    }
}
