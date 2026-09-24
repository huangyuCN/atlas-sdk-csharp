using System;
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
// 未知位非零即协议非法。协议常量（magic/版本/长度上限/标志位）唯一来源为生成物
// FrameGen（本仓 scripts/gen-dto.sh 从框架 transport/frame/gen/csharp 复制，勿手写副本）。
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
        var bytes = new byte[FrameGen.HeaderSize];
        WriteUInt32(bytes, 0, Magic);
        bytes[4] = Version;
        bytes[5] = (byte)Type;
        bytes[6] = Flags;
        WriteUInt32(bytes, 8, Seq);
        WriteUInt32(bytes, 12, Length);
        return bytes;
    }

    public static Header Decode(byte[] bytes)
    {
        if (bytes == null || bytes.Length != FrameGen.HeaderSize)
        {
            throw new ProtocolException($"帧头长度 {bytes?.Length ?? 0} != {FrameGen.HeaderSize}");
        }

        var header = new Header
        {
            Magic = ReadUInt32(bytes, 0),
            Version = bytes[4],
            Type = (MsgType)bytes[5],
            Flags = bytes[6],
            Rsv = bytes[7],
            Seq = ReadUInt32(bytes, 8),
            Length = ReadUInt32(bytes, 12),
        };
        Check(header, FrameGen.MaxBodySize);
        return header;
    }

    // FlagReservedMask 是未定义的保留位掩码（bit0/bit1 已定义，bit2..bit7 即 0xFC；
    // 未知位即协议非法，前向保留位白名单；对齐 Go frame 的未导出常量 flagReserved）。
    private const byte FlagReservedMask = 0xFC;

    // Check 按 Go Header.Check 的顺序校验 magic、seq、类型、版本白名单、flags
    // 未知位和长度；seq=0 对所有帧类型均非法，与 golden frame-bad-seq-zero 保持一致。
    internal static void Check(Header header, int maxBodySize)
    {
        var limit = NormalizeMaxBodySize(maxBodySize);
        if (header.Magic != FrameGen.Magic)
        {
            throw new ProtocolException($"非法 magic: 0x{header.Magic:X}");
        }
        if (header.Seq == 0)
        {
            throw new ProtocolException("非法 seq: 0");
        }
        if (header.Type != MsgType.Request && header.Type != MsgType.Response && header.Type != MsgType.Notify)
        {
            throw new ProtocolException($"非法 type: {(byte)header.Type}");
        }
        if (header.Version != FrameGen.Version && header.Version != FrameGen.Version2)
        {
            throw new ProtocolException($"非法 version: {header.Version}（白名单 {{1,2}}）");
        }
        if ((header.Flags & FlagReservedMask) != 0)
        {
            throw new ProtocolException(
                $"非法 flags: 0x{header.Flags:X}（仅 bit0 = FlagSession / bit1 = FlagRequestID 合法，未知位必须为 0）");
        }
        if (header.Length > (uint)limit)
        {
            throw new ProtocolException($"body 过长: {header.Length} > {limit}");
        }
    }

    internal static int NormalizeMaxBodySize(int maxBodySize)
    {
        return maxBodySize > 0 ? maxBodySize : FrameGen.MaxBodySize;
    }

    private static uint ReadUInt32(byte[] bytes, int offset)
    {
        return ((uint)bytes[offset] << 24)
            | ((uint)bytes[offset + 1] << 16)
            | ((uint)bytes[offset + 2] << 8)
            | bytes[offset + 3];
    }

    private static void WriteUInt32(byte[] bytes, int offset, uint value)
    {
        bytes[offset] = (byte)(value >> 24);
        bytes[offset + 1] = (byte)(value >> 16);
        bytes[offset + 2] = (byte)(value >> 8);
        bytes[offset + 3] = (byte)value;
    }
}
