using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Xunit;

namespace Atlas.Tests.Frame;

public sealed class FrameIOTest
{
    [Fact]
    public void Header_EncodeRoundtrip_PreservesFields()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 0x01020304,
            Length = 0x0A,
        };

        var bytes = header.Encode();
        var decoded = Header.Decode(bytes);

        Assert.Equal(FrameConst.HeaderSize, bytes.Length);
        Assert.Equal(header.Magic, decoded.Magic);
        Assert.Equal(header.Version, decoded.Version);
        Assert.Equal(header.Type, decoded.Type);
        Assert.Equal(header.Seq, decoded.Seq);
        Assert.Equal(header.Length, decoded.Length);
    }

    [Fact]
    public void Header_Encode_UsesFixedBigEndianLayout()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 0x01020304,
            Length = 0x0A0B0C0D,
        };

        Assert.Equal(new byte[]
        {
            0x41, 0x54, 0x4C, 0x53, 0x01, 0x01, 0x00, 0x00,
            0x01, 0x02, 0x03, 0x04, 0x0A, 0x0B, 0x0C, 0x0D,
        }, header.Encode());
    }

    [Fact]
    public void Decode_InvalidType_ThrowsProtocol()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = (MsgType)4,
            Seq = 1,
            Length = 0,
        };

        Assert.Throws<ProtocolException>(() => Header.Decode(header.Encode()));
    }

    [Fact]
    public void Decode_InvalidType_PrecedesInvalidVersion()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = 3,
            Type = (MsgType)4,
            Seq = 1,
            Length = 0,
        };

        var exception = Assert.Throws<ProtocolException>(() => Header.Decode(header.Encode()));

        Assert.Contains("type", exception.Message);
    }

    [Fact]
    public void Decode_DefaultMaxBodySize_RejectsOversizeLength()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
            Length = (uint)FrameConst.MaxBodySize + 1,
        };

        Assert.Throws<ProtocolException>(() => Header.Decode(header.Encode()));
    }

    [Fact]
    public async Task ReadFrame_SegmentedWrite_ReadsFullFrame()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
            Length = 2,
        };
        await using var wire = new MemoryStream();
        await FrameIO.WriteFrameAsync(wire, header, new byte[] { 7, 8 }, FrameConst.MaxBodySize, CancellationToken.None);
        var bytes = wire.ToArray();

        await using var stream = new SegmentedReadStream(bytes);

        var actual = await FrameIO.ReadFrameAsync(stream, FrameConst.MaxBodySize, CancellationToken.None);

        Assert.Equal((uint)1, actual.Header.Seq);
        Assert.Equal(new byte[] { 7, 8 }, actual.Body);
    }

    [Fact]
    public void Decode_InvalidMagic_ThrowsProtocol()
    {
        var bytes = new byte[FrameConst.HeaderSize];

        Assert.Throws<ProtocolException>(() => Header.Decode(bytes));
    }

    [Theory]
    [InlineData((byte)0)]
    [InlineData((byte)3)]
    public void Decode_InvalidVersion_ThrowsProtocol(byte version)
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = version,
            Type = MsgType.Request,
            Seq = 1,
            Length = 0,
        };

        Assert.Throws<ProtocolException>(() => Header.Decode(header.Encode()));
    }

    [Fact]
    public void Decode_ZeroSequence_ThrowsProtocol()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Notify,
            Seq = 0,
            Length = 0,
        };

        Assert.Throws<ProtocolException>(() => Header.Decode(header.Encode()));
    }

    [Fact]
    public async Task WriteReadFrame_StreamRoundtrip_SplitsAndReassembles()
    {
        var first = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
            Length = 3,
        };
        var second = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version2,
            Type = MsgType.Response,
            Seq = 2,
            Length = 2,
        };

        await using var stream = new MemoryStream();
        await FrameIO.WriteFrameAsync(stream, first, new byte[] { 1, 2, 3 }, FrameConst.MaxBodySize, CancellationToken.None);
        await FrameIO.WriteFrameAsync(stream, second, new byte[] { 4, 5 }, FrameConst.MaxBodySize, CancellationToken.None);
        stream.Position = 0;

        var actualFirst = await FrameIO.ReadFrameAsync(stream, FrameConst.MaxBodySize, CancellationToken.None);
        var actualSecond = await FrameIO.ReadFrameAsync(stream, FrameConst.MaxBodySize, CancellationToken.None);

        Assert.Equal(first.Seq, actualFirst.Item1.Seq);
        Assert.Equal(new byte[] { 1, 2, 3 }, actualFirst.Item2);
        Assert.Equal(second.Seq, actualSecond.Item1.Seq);
        Assert.Equal(new byte[] { 4, 5 }, actualSecond.Item2);
    }

    [Fact]
    public async Task WriteFrame_UsesActualBodyLength()
    {
        var header = new Header
        {
            Type = MsgType.Request,
            Seq = 1,
            Length = 99,
        };
        await using var stream = new MemoryStream();
        await FrameIO.WriteFrameAsync(stream, header, new byte[] { 1, 2 }, FrameConst.MaxBodySize, CancellationToken.None);
        stream.Position = 0;

        var actual = await FrameIO.ReadFrameAsync(stream, FrameConst.MaxBodySize, CancellationToken.None);

        Assert.Equal(FrameConst.Magic, actual.Header.Magic);
        Assert.Equal(FrameConst.Version, actual.Header.Version);
        Assert.Equal((uint)2, actual.Header.Length);
        Assert.Equal(new byte[] { 1, 2 }, actual.Body);
    }

    [Fact]
    public async Task ReadFrame_CustomMaxBodySize_RejectsLargerBody()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
            Length = 3,
        };
        await using var stream = new MemoryStream();
        await FrameIO.WriteFrameAsync(stream, header, new byte[] { 1, 2, 3 }, FrameConst.MaxBodySize, CancellationToken.None);
        stream.Position = 0;

        await Assert.ThrowsAsync<ProtocolException>(async () =>
            await FrameIO.ReadFrameAsync(stream, 2, CancellationToken.None));
    }

    // 分段读取流：首个 ReadAsync 只交付 1 字节，延迟后才交付余下数据，模拟对端分段写入。
    private sealed class SegmentedReadStream : MemoryStream
    {
        private bool firstRead = true;

        public SegmentedReadStream(byte[] bytes)
            : base(bytes)
        {
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (firstRead)
            {
                firstRead = false;
                return await base.ReadAsync(buffer.Slice(0, 1), cancellationToken);
            }

            await Task.Delay(10, cancellationToken);
            return await base.ReadAsync(buffer, cancellationToken);
        }
    }

    // ---- EncodeMessage / DecodeMessage 负向单测（评审补强：此前仅经 WS 间接覆盖） ----

    [Fact]
    public void EncodeMessage_BodyOverLimit_ThrowsProtocol()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
        };
        var oversized = new byte[3];
        // maxBodySize=2 时 body 超限应抛 ProtocolException。
        Assert.Throws<ProtocolException>(() => FrameIO.EncodeMessage(header, oversized, 2));
    }

    [Fact]
    public void EncodeMessage_NullBody_ThrowsArgumentNull()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
        };
        Assert.Throws<ArgumentNullException>(() => FrameIO.EncodeMessage(header, null!, FrameConst.MaxBodySize));
    }

    [Fact]
    public void DecodeMessage_ShorterThanHeader_ThrowsProtocol()
    {
        Assert.Throws<ProtocolException>(() => FrameIO.DecodeMessage(new byte[] { 1, 2, 3 }, FrameConst.MaxBodySize));
    }

    [Fact]
    public void DecodeMessage_LengthMismatch_ThrowsProtocol()
    {
        // 头 bodyLen=3 但消息只有 2B body——长度不一致即协议非法（消息边界下失步）。
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
            Length = 3,
        };
        var message = new byte[FrameConst.HeaderSize + 2];
        var headerBytes = header.Encode();
        Array.Copy(headerBytes, message, FrameConst.HeaderSize);
        Assert.Throws<ProtocolException>(() => FrameIO.DecodeMessage(message, FrameConst.MaxBodySize));
    }

    [Fact]
    public void DecodeMessage_BadMagic_ThrowsProtocol()
    {
        // ≥16B 但 magic 非法（全零）——头校验失败。
        var message = new byte[FrameConst.HeaderSize + 2];
        Assert.Throws<ProtocolException>(() => FrameIO.DecodeMessage(message, FrameConst.MaxBodySize));
    }

    [Fact]
    public void DecodeMessage_ValidMessage_Roundtrip()
    {
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 9,
        };
        var message = FrameIO.EncodeMessage(header, new byte[] { 7, 8 }, FrameConst.MaxBodySize);
        var (decodedHeader, body) = FrameIO.DecodeMessage(message, FrameConst.MaxBodySize);
        Assert.Equal(9u, decodedHeader.Seq);
        Assert.Equal(new byte[] { 7, 8 }, body);
    }
}
