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
}
