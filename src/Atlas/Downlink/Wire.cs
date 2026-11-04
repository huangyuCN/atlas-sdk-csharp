// 下行增量（ver=3）的最小 protobuf wire 读写器。
//
// 为什么 SDK 里要有一份：合并必须在**字节层**完成——SDK 不持有游戏域 DTO，而 ver=3 的恢复
// 语义是「把增量并进基线、仍按 FrameBroadcast 全量字节交给上层」。这里只认本协议用到的
// 字段号与线型，不引游戏协议包、也不依赖生成的消息类。
//
// 编码口径与 Go 的 proto.Marshal 逐字节对齐（同一批 golden 向量是唯一裁判）：
//   - 字段按字段号升序；proto3 默认值（0 / 空串 / 空 bytes）省略；
//   - 长度分隔型：tag + varint(len) + 原始字节；varint 用 64 位补码。
using System;
using System.Collections.Generic;

namespace Atlas.Downlink;

/// <summary>WireField 一条已解码的 wire 字段（Wire=0 用 Varint，其余用 Bytes）。</summary>
internal readonly struct WireField
{
    public WireField(int no, int wire, ulong varint, byte[] bytes)
    {
        No = no;
        Wire = wire;
        Varint = varint;
        Bytes = bytes;
    }

    /// <summary>No 字段号。</summary>
    public int No { get; }

    /// <summary>Wire 线型（0 varint / 1 fixed64 / 2 长度分隔 / 5 fixed32）。</summary>
    public int Wire { get; }

    /// <summary>Varint wire=0 时的值。</summary>
    public ulong Varint { get; }

    /// <summary>Bytes 非 varint 字段的原始字节。</summary>
    public byte[] Bytes { get; }
}

/// <summary>DownlinkException 增量/全量载荷非法（与「失步丢弃」区分：这是载荷本身解不开）。</summary>
public sealed class DownlinkException : Exception
{
    /// <summary>以消息构造。</summary>
    public DownlinkException(string message)
        : base(message)
    {
    }
}

/// <summary>Wire 是 protobuf 最小读写器（只服务下行帧广播的合并）。</summary>
internal static class Wire
{
    /// <summary>Parse 把消息解析为字段列表（保留出现顺序，容忍未知字段号与线型）。</summary>
    public static List<WireField> Parse(byte[] buf)
    {
        var fields = new List<WireField>();
        var off = 0;
        while (off < buf.Length)
        {
            var tag = ReadVarint(buf, ref off);
            var no = (int)(tag >> 3);
            var wire = (int)(tag & 7);
            if (no == 0)
            {
                throw new DownlinkException("protobuf: 字段号为 0");
            }
            switch (wire)
            {
                case 0:
                    fields.Add(new WireField(no, wire, ReadVarint(buf, ref off), Array.Empty<byte>()));
                    break;
                case 1:
                    fields.Add(new WireField(no, wire, 0, Slice(buf, ref off, 8)));
                    break;
                case 2:
                    var len = ReadVarint(buf, ref off);
                    if (len > int.MaxValue)
                    {
                        throw new DownlinkException("protobuf: 长度超出 int 范围");
                    }
                    fields.Add(new WireField(no, wire, 0, Slice(buf, ref off, (int)len)));
                    break;
                case 5:
                    fields.Add(new WireField(no, wire, 0, Slice(buf, ref off, 4)));
                    break;
                default:
                    throw new DownlinkException($"protobuf: 不支持的线型 {wire}");
            }
        }
        return fields;
    }

    /// <summary>ReadVarint 从 off 读一个 varint（最多 10 字节），并前移 off。</summary>
    public static ulong ReadVarint(byte[] buf, ref int off)
    {
        ulong value = 0;
        var shift = 0;
        for (var n = 0; n < 10; n++)
        {
            if (off >= buf.Length)
            {
                throw new DownlinkException("protobuf: varint 截断");
            }
            var b = buf[off++];
            value |= (ulong)(b & 0x7f) << shift;
            if ((b & 0x80) == 0)
            {
                return value;
            }
            shift += 7;
        }
        throw new DownlinkException("protobuf: varint 超长");
    }

    /// <summary>Varint 编码一个 64 位值。</summary>
    public static byte[] Varint(ulong value)
    {
        var outBytes = new List<byte>(10);
        var n = value;
        while (n >= 0x80)
        {
            outBytes.Add((byte)((n & 0x7f) | 0x80));
            n >>= 7;
        }
        outBytes.Add((byte)n);
        return outBytes.ToArray();
    }

    /// <summary>Tag 编码字段号与线型。</summary>
    public static byte[] Tag(int no, int wire)
    {
        return Varint(((ulong)no << 3) | (uint)wire);
    }

    /// <summary>Concat 拼接若干字节块（跳过空块）。</summary>
    public static byte[] Concat(params byte[][] chunks)
    {
        var len = 0;
        foreach (var c in chunks)
        {
            len += c.Length;
        }
        var outBytes = new byte[len];
        var off = 0;
        foreach (var c in chunks)
        {
            Buffer.BlockCopy(c, 0, outBytes, off, c.Length);
            off += c.Length;
        }
        return outBytes;
    }

    /// <summary>LengthDelimited 编码长度分隔字段；空 bytes 按 proto3 语义省略。</summary>
    public static byte[] LengthDelimited(int no, byte[] bytes)
    {
        if (bytes.Length == 0)
        {
            return Array.Empty<byte>();
        }
        return Concat(Tag(no, 2), Varint((ulong)bytes.Length), bytes);
    }

    /// <summary>VarintField 编码 varint 字段；0 按 proto3 语义省略。</summary>
    public static byte[] VarintField(int no, ulong value)
    {
        return value == 0 ? Array.Empty<byte>() : Concat(Tag(no, 0), Varint(value));
    }

    /// <summary>MessageField 编码消息字段（**空消息也带 tag**：presence 是协议语义）。</summary>
    public static byte[] MessageField(int no, byte[] bytes)
    {
        return Concat(Tag(no, 2), Varint((ulong)bytes.Length), bytes);
    }

    /// <summary>EncodeField 按已解码字段重新编码（改写 frame_id 后保留其余字段）。</summary>
    public static byte[] EncodeField(WireField f)
    {
        if (f.Wire == 0)
        {
            return Concat(Tag(f.No, 0), Varint(f.Varint));
        }
        if (f.Wire == 2)
        {
            return Concat(Tag(f.No, 2), Varint((ulong)f.Bytes.Length), f.Bytes);
        }
        return Concat(Tag(f.No, f.Wire), f.Bytes);
    }

    /// <summary>FirstBytes 取指定字段号的第一条长度分隔值；不存在返回 null。</summary>
    public static byte[]? FirstBytes(List<WireField> fields, int no)
    {
        foreach (var f in fields)
        {
            if (f.No == no && f.Wire == 2)
            {
                return f.Bytes;
            }
        }
        return null;
    }

    /// <summary>AllBytes 取指定字段号的全部长度分隔值（保持出现顺序）。</summary>
    public static List<byte[]> AllBytes(List<WireField> fields, int no)
    {
        var outBytes = new List<byte[]>();
        foreach (var f in fields)
        {
            if (f.No == no && f.Wire == 2)
            {
                outBytes.Add(f.Bytes);
            }
        }
        return outBytes;
    }

    /// <summary>FirstVarint 取指定字段号的第一条 varint；不存在返回 0。</summary>
    public static ulong FirstVarint(List<WireField> fields, int no)
    {
        foreach (var f in fields)
        {
            if (f.No == no && f.Wire == 0)
            {
                return f.Varint;
            }
        }
        return 0;
    }

    /// <summary>Slice 从 off 切 len 字节并前移 off（越界即抛）。</summary>
    private static byte[] Slice(byte[] buf, ref int off, int len)
    {
        if (len < 0 || off + len > buf.Length)
        {
            throw new DownlinkException("protobuf: 长度分隔截断");
        }
        var outBytes = new byte[len];
        Buffer.BlockCopy(buf, off, outBytes, 0, len);
        off += len;
        return outBytes;
    }
}
