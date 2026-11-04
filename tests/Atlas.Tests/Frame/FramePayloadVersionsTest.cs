using System;
using System.Collections.Generic;
using Atlas.Frame;
using Xunit;

namespace Atlas.Tests.Frame;

// 帧头载荷版本白名单的验收用例（与框架仓 Go 用例、TS SDK test/frameVersions.test.ts 三端对称）：
//   ① ver=3（下行增量广播）必须能解出帧头与 body——旧白名单只认 ver=1/2 时这里是真红；
//   ② 未定义的 ver=4 仍必须被拒（扩白名单 ≠ 放开一切）；
//   ③ 「能声明的版本集合 == 解码器接受的版本集合」逐版本对账（防两处独立维护再漂移）。
public sealed class FramePayloadVersionsTest
{
    // LiteralFrame 用线格式字面量拼一帧（不用 Encode 自拼自解，否则是恒等式证明）。
    private static byte[] LiteralFrame(byte version, byte[] body)
    {
        var frame = new byte[FrameGen.HeaderSize + body.Length];
        frame[0] = 0x41; // "ATLS"
        frame[1] = 0x54;
        frame[2] = 0x4c;
        frame[3] = 0x53;
        frame[4] = version;
        frame[5] = FrameGen.MsgTypeRequest;
        frame[6] = 0; // flags
        frame[7] = 0; // 保留位
        frame[8] = 0; // seq = 1（大端）
        frame[9] = 0;
        frame[10] = 0;
        frame[11] = 1;
        var len = body.Length;
        frame[12] = (byte)(len >> 24);
        frame[13] = (byte)(len >> 16);
        frame[14] = (byte)(len >> 8);
        frame[15] = (byte)len;
        Array.Copy(body, 0, frame, FrameGen.HeaderSize, body.Length);
        return frame;
    }

    [Fact]
    public void Decode_Version3_ExtractsHeaderAndBody()
    {
        var body = new byte[] { (byte)'d', (byte)'e', (byte)'l', (byte)'t', (byte)'a' };
        var (header, decoded) = FrameCodec.Decode(LiteralFrame(3, body), 0);
        Assert.Equal(3, header.Version);
        Assert.Equal(body, decoded);
    }

    [Fact]
    public void Decode_UndefinedVersion4_IsRejected()
    {
        var ex = Assert.Throws<FrameCodecException>(
            () => FrameCodec.Decode(LiteralFrame(4, Array.Empty<byte>()), 0));
        Assert.Equal(FrameErrorKind.Protocol, ex.Kind);
        Assert.Contains("invalid version", ex.Message);
    }

    [Fact]
    public void DeclaredVersions_MatchDecoderAcceptedVersions()
    {
        var declared = FrameGen.PayloadVersions;
        Assert.Contains((byte)3, declared);
        var accepted = new List<byte>();
        for (var v = 0; v < 256; v++)
        {
            var header = new FrameHeader
            {
                Magic = FrameGen.Magic,
                Version = (byte)v,
                Type = FrameGen.MsgTypeRequest,
                Seq = 1,
            };
            try
            {
                FrameCodec.CheckHeader(header, 0);
                accepted.Add((byte)v);
            }
            catch (FrameCodecException)
            {
                // 被拒：不属于接受集合
            }
        }
        Assert.Equal(new List<byte>(declared), accepted);
    }
}
