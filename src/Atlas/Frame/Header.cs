using Atlas.Errors;

namespace Atlas.Frame;

// MsgType 是帧类型（三值取自生成物 FrameGen：与框架 transport/frame 同源，勿手写数值）。
public enum MsgType : byte
{
    Request = FrameGen.MsgTypeRequest,
    Response = FrameGen.MsgTypeResponse,
    Notify = FrameGen.MsgTypeNotify,
}

// 帧头（16B 大端）：magic(4) ver(1) type(1) flags(1) rsv(1) seq(4) bodyLen(4)。
// flags 位图（原 rsv 首字节）：bit0 = FrameGen.FlagSession，bit1 = FrameGen.FlagRequestID，
// 未知位非零即协议非法。协议常量与编解码（含头校验）唯一来源为生成物
// FrameGen/FrameCodec（本仓 scripts/gen-dto.sh 从框架 transport/frame/gen/csharp 复制，
// 勿手写副本）；本结构只保留 SDK 的类型化形态与「线格式 ↔ 类型」转换。
public struct Header
{
    public uint Magic;
    public byte Version;
    public MsgType Type;
    public byte Flags;
    public byte Rsv;
    public uint Seq;
    public uint Length;

    public byte[] Encode()
    {
        return FrameCodec.EncodeHeader(ToWire());
    }

    public static Header Decode(byte[] bytes)
    {
        if (bytes == null || bytes.Length != FrameGen.HeaderSize)
        {
            throw new ProtocolException($"帧头长度 {bytes?.Length ?? 0} != {FrameGen.HeaderSize}");
        }

        return DecodeChecked(bytes, 0);
    }

    // DecodeChecked 解析帧头并把生成物的编解码异常收敛为 SDK 的 ProtocolException
    // （Header.Decode 与 FrameIO.ReadFrameAsync 共用；maxBodySize ≤0 回退绝对上限）。
    internal static Header DecodeChecked(byte[] bytes, int maxBodySize)
    {
        try
        {
            return FromWire(FrameCodec.DecodeHeader(bytes, maxBodySize));
        }
        catch (FrameCodecException exception)
        {
            throw new ProtocolException(exception.Message);
        }
    }

    // Check 按生成物口径校验 magic、seq、类型、版本白名单、flags 未知位和长度；
    // seq=0 对所有帧类型均非法，与 golden frame-bad-seq-zero 保持一致。
    internal static void Check(Header header, int maxBodySize)
    {
        try
        {
            FrameCodec.CheckHeader(header.ToWire(), maxBodySize);
        }
        catch (FrameCodecException exception)
        {
            throw new ProtocolException(exception.Message);
        }
    }

    // NormalizeMaxBodySize 返回生效的 body 上限（≤0 回退绝对上限），转发生成物口径。
    internal static int NormalizeMaxBodySize(int maxBodySize)
    {
        return FrameCodec.NormalizeMaxBodySize(maxBodySize);
    }

    // ToWire 转成生成物的线格式帧头（Rsv 不参与线格式：第 7 字节恒 0）。
    internal FrameHeader ToWire()
    {
        return new FrameHeader
        {
            Magic = Magic,
            Version = Version,
            Type = (byte)Type,
            Flags = Flags,
            Seq = Seq,
            Length = Length,
        };
    }

    // FromWire 从线格式帧头还原 SDK 结构（保留位协议要求为 0，故 Rsv 恒 0）。
    internal static Header FromWire(FrameHeader header)
    {
        return new Header
        {
            Magic = header.Magic,
            Version = header.Version,
            Type = (MsgType)header.Type,
            Flags = header.Flags,
            Rsv = 0,
            Seq = header.Seq,
            Length = header.Length,
        };
    }
}
