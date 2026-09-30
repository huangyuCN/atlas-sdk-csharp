using System;
using System.Text;
using Atlas.Battle;
using Atlas.Errors;
using Xunit;

namespace Atlas.Tests.Battle;

// EdgeWireTest 逐字节覆盖接入层线格式：hello 段（magic/版本/票长大端）、
// 帧会话槽（base64url 无填充）、WS 升级 query 拼装、数据报 flow-id 封装与剥离。
public sealed class EdgeWireTest
{
    [Fact]
    public void EncodeHello_ByteForByte()
    {
        var ticket = new byte[] { 0xde, 0xad, 0xbe, 0xef };

        var hello = EdgeWire.EncodeHello(ticket);

        var want = new byte[] { (byte)'A', (byte)'T', (byte)'L', (byte)'H', 0x01, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef };
        Assert.Equal(want, hello);
        Assert.Equal(EdgeWire.HelloHeaderLen + ticket.Length, hello.Length);
        Assert.True(EdgeWire.IsHello(hello));
    }

    [Fact]
    public void EncodeHello_EmptyTicket_HeaderOnly()
    {
        var hello = EdgeWire.EncodeHello(Array.Empty<byte>());

        Assert.Equal(new byte[] { (byte)'A', (byte)'T', (byte)'L', (byte)'H', 0x01, 0x00, 0x00 }, hello);
        Assert.Equal(7, EdgeWire.HelloHeaderLen);
        Assert.True(EdgeWire.IsHello(hello));
    }

    [Fact]
    public void EncodeHello_TicketLenBigEndian()
    {
        var ticket = new byte[300];
        for (var i = 0; i < ticket.Length; i++)
        {
            ticket[i] = 0x5a;
        }

        var hello = EdgeWire.EncodeHello(ticket);

        Assert.Equal(EdgeWire.HelloVersion, hello[4]);
        Assert.Equal(0x01, hello[5]);
        Assert.Equal(0x2c, hello[6]);
        Assert.Equal(300, (hello[5] << 8) | hello[6]);
        Assert.Equal(EdgeWire.HelloHeaderLen + ticket.Length, hello.Length);
    }

    [Fact]
    public void IsHello_RejectsForeignAndShortDatagrams()
    {
        Assert.False(EdgeWire.IsHello(new byte[] { (byte)'A', (byte)'T', (byte)'L', (byte)'X', 1, 0, 0 }));
        // 版本不符（服务端 IsHello 同样按版本判定）。
        Assert.False(EdgeWire.IsHello(new byte[] { (byte)'A', (byte)'T', (byte)'L', (byte)'H', 2, 0, 0 }));
        Assert.False(EdgeWire.IsHello(new byte[] { 1, 2, 3 }));
        Assert.False(EdgeWire.IsHello(Array.Empty<byte>()));
    }

    [Fact]
    public void TicketSlot_IsBase64UrlRawWithoutPadding()
    {
        // 0xfb 0xff 0x00 0x01 0xfe 是「标准 base64 会产出 +/ 与填充」的字节，用来逼出编码口径。
        var ticket = new byte[] { 0xfb, 0xff, 0x00, 0x01, 0xfe };

        var slot = EdgeWire.TicketSlot(ticket);

        Assert.Equal("-_8AAf4", slot);
        Assert.DoesNotContain("+", slot, StringComparison.Ordinal);
        Assert.DoesNotContain("/", slot, StringComparison.Ordinal);
        Assert.DoesNotContain("=", slot, StringComparison.Ordinal);
        // 服务端按 base64.RawURLEncoding 反解（帧槽取值约定，逐字节一致）。
        Assert.Equal(ticket, EdgeWire.DecodeTicketSlot(slot));
    }

    [Fact]
    public void WsUrl_CarriesTicketQuery_WithPath()
    {
        var ticket = new byte[] { 0x01, 0x02, 0x03 };

        var url = EdgeWire.WsUrl("10.0.0.1:7100", ticket, "/ws");

        var uri = new Uri(url);
        Assert.Equal("ws", uri.Scheme);
        Assert.Equal("10.0.0.1:7100", uri.Host + ":" + uri.Port);
        Assert.Equal("/ws", uri.AbsolutePath);
        Assert.Contains("ticket=" + EdgeWire.TicketSlot(ticket), url, StringComparison.Ordinal);
    }

    [Fact]
    public void WsUrl_DefaultPathIsRoot()
    {
        // 接入层按升级请求取票，路径由 battle 帧面放行：缺省 "/"（与 Go SDK 口径一致）。
        var url = EdgeWire.WsUrl("h:1", new byte[] { 0x01 }, null);

        var uri = new Uri(url);
        Assert.Equal("/", uri.AbsolutePath);
        Assert.Equal("ws://h:1/?ticket=AQ", url);
    }

    [Fact]
    public void WrapFlowId_ByteForByte()
    {
        var payload = new byte[] { 0xaa, 0xbb };

        var wrapped = EdgeWire.WrapFlowId(0x0102030405060708, payload);

        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 0xaa, 0xbb }, wrapped);
        Assert.Equal(8, EdgeWire.FlowIdLen);
    }

    [Fact]
    public void StripFlowId_ReturnsCopy_NotAlias()
    {
        // 读缓冲逐包复用：剥离必须拷贝出载荷，返回切片不得指向入参。
        var wrapped = EdgeWire.WrapFlowId(7, new byte[] { 0xaa, 0xbb });

        var payload = EdgeWire.StripFlowId(wrapped, 7);

        Assert.Equal(new byte[] { 0xaa, 0xbb }, payload);
        payload[0] = 0x00;
        Assert.Equal(0xaa, wrapped[EdgeWire.FlowIdLen]);
    }

    [Fact]
    public void StripFlowId_MismatchOrShort_Throws()
    {
        var good = EdgeWire.WrapFlowId(7, new byte[] { 0x01 });
        var mismatch = Assert.Throws<ProtocolException>(() => EdgeWire.StripFlowId(good, 8));
        Assert.StartsWith(DirectErrors.FlowIdMismatch, mismatch.Message);

        var tooShort = Assert.Throws<ProtocolException>(() => EdgeWire.StripFlowId(new byte[] { 1, 2, 3 }, 7));
        Assert.StartsWith(DirectErrors.FlowIdMismatch, tooShort.Message);
    }

    [Fact]
    public void StripFlowId_EmptyPayload_Ok()
    {
        var payload = EdgeWire.StripFlowId(EdgeWire.WrapFlowId(0xffffffffffffffff, Array.Empty<byte>()), 0xffffffffffffffff);

        Assert.Empty(payload);
    }

    [Fact]
    public void DecodeFlowId_ReadsEightBytesBigEndian()
    {
        var datagram = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 0x99 };

        Assert.Equal(0x0102030405060708UL, EdgeWire.DecodeFlowId(datagram));
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, EdgeWire.EncodeFlowId(0x0102030405060708));
    }

    [Fact]
    public void DecodeFlowId_ShortDatagram_Throws()
    {
        var exception = Assert.Throws<ProtocolException>(() => EdgeWire.DecodeFlowId(new byte[] { 1, 2, 3 }));

        Assert.StartsWith(DirectErrors.FlowIdMismatch, exception.Message);
    }

    [Fact]
    public void WireKeys_MatchServerContract()
    {
        // 接入层与帧引擎的键名唯一来源（contrib/edge、transport/frame）：手写副本必须逐字一致。
        Assert.Equal("ticket", EdgeWire.TicketQueryKey);
        Assert.Equal("X-Atlas-Ticket", EdgeWire.TicketHeaderKey);
        Assert.Equal("Atlas-Frame-Session", EdgeWire.FrameSessionHeaderKey);
        Assert.Equal("ATLH", EdgeWire.HelloMagicText);
        Assert.Equal(Encoding.ASCII.GetBytes("ATLH"), EdgeWire.HelloMagicBytes());
    }
}
