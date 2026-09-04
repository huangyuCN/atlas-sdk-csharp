using System;
using Atlas.Errors;

namespace Atlas.Frame;

// 帧协议常量（与服务端 transport/frame 及 Go SDK 字节级一致）。
public static class FrameConst
{
    public const uint Magic = 0x41544C53;
    public const byte Version = 1;
    public const byte Version2 = 2;
    public const int HeaderSize = 16;
    public const int MaxBodySize = 2 << 20;
    public const int MaxOperationLen = 4096;
}

public enum MsgType : byte
{
    Request = 1,
    Response = 2,
    Notify = 3,
}

// 帧头（16B 大端）：magic(4) ver(1) type(1) rsv(2) seq(4) bodyLen(4)。
public struct Header
{
    public uint Magic;
    public byte Version;
    public MsgType Type;
    public byte Rsv1;
    public byte Rsv2;
    public uint Seq;
    public uint Length;

    public byte[] Encode()
    {
        var bytes = new byte[FrameConst.HeaderSize];
        WriteUInt32(bytes, 0, Magic);
        bytes[4] = Version;
        bytes[5] = (byte)Type;
        WriteUInt32(bytes, 8, Seq);
        WriteUInt32(bytes, 12, Length);
        return bytes;
    }

    public static Header Decode(byte[] bytes)
    {
        if (bytes == null || bytes.Length != FrameConst.HeaderSize)
        {
            throw new ProtocolException($"帧头长度 {bytes?.Length ?? 0} != {FrameConst.HeaderSize}");
        }

        var header = new Header
        {
            Magic = ReadUInt32(bytes, 0),
            Version = bytes[4],
            Type = (MsgType)bytes[5],
            Rsv1 = bytes[6],
            Rsv2 = bytes[7],
            Seq = ReadUInt32(bytes, 8),
            Length = ReadUInt32(bytes, 12),
        };
        Check(header, FrameConst.MaxBodySize);
        return header;
    }

    // Check 按 Go Header.Check 的顺序校验 magic、seq、类型、版本白名单和长度；
    // seq=0 对所有帧类型均非法，与 golden frame-bad-seq-zero 保持一致。
    internal static void Check(Header header, int maxBodySize)
    {
        var limit = NormalizeMaxBodySize(maxBodySize);
        if (header.Magic != FrameConst.Magic)
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
        if (header.Version != FrameConst.Version && header.Version != FrameConst.Version2)
        {
            throw new ProtocolException($"非法 version: {header.Version}（白名单 {{1,2}}）");
        }
        if (header.Length > (uint)limit)
        {
            throw new ProtocolException($"body 过长: {header.Length} > {limit}");
        }
    }

    internal static int NormalizeMaxBodySize(int maxBodySize)
    {
        return maxBodySize > 0 ? maxBodySize : FrameConst.MaxBodySize;
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
