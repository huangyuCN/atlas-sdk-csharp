using System;
using System.Collections.Generic;
using System.Text;
using Atlas.Errors;

namespace Atlas.Frame;

// Status 是 atlas errors.Status 的客户端侧还原。字段号：code=1、reason=2、
// message=3、metadata=4。手写 protobuf wire 解码，不依赖 protobuf 运行时。
public sealed class Status
{
    public int Code { get; internal set; }

    public string Reason { get; internal set; } = "";

    public string Message { get; internal set; } = "";

    public Dictionary<string, string> Metadata { get; } = new Dictionary<string, string>();
}

public static class StatusWire
{
    private const int WireVarint = 0;
    private const int WireBytes = 2;

    public static Status Decode(byte[] bytes)
    {
        if (bytes == null)
        {
            return new Status();
        }

        var status = new Status();
        var position = 0;
        while (position < bytes.Length)
        {
            var tag = ReadVarint(bytes, ref position);
            var field = (int)(tag >> 3);
            var wire = (int)(tag & 0x7);
            DecodeField(status, bytes, ref position, field, wire);
        }

        return status;
    }

    private static void DecodeField(Status status, byte[] bytes, ref int position, int field, int wire)
    {
        switch (wire)
        {
            case WireVarint:
                var numeric = ReadVarint(bytes, ref position);
                if (field == 1)
                {
                    status.Code = unchecked((int)numeric);
                }

                return;
            case WireBytes:
                var value = ReadBytes(bytes, ref position);
                DecodeBytesField(status, field, value);
                return;
            default:
                throw new ProtocolException($"field {field} 不支持的 wire type {wire}");
        }
    }

    private static void DecodeBytesField(Status status, int field, byte[] value)
    {
        switch (field)
        {
            case 2:
                status.Reason = Encoding.UTF8.GetString(value);
                break;
            case 3:
                status.Message = Encoding.UTF8.GetString(value);
                break;
            case 4:
                var (key, mapValue) = DecodeMapEntry(value);
                status.Metadata[key] = mapValue;
                break;
        }
    }

    private static (string Key, string Value) DecodeMapEntry(byte[] bytes)
    {
        var key = "";
        var value = "";
        var position = 0;
        while (position < bytes.Length)
        {
            var tag = ReadVarint(bytes, ref position);
            var field = (int)(tag >> 3);
            var wire = (int)(tag & 0x7);
            if (wire == WireVarint)
            {
                _ = ReadVarint(bytes, ref position);
                continue;
            }

            if (wire != WireBytes)
            {
                throw new ProtocolException($"field {field} 不支持的 wire type {wire}");
            }

            var item = Encoding.UTF8.GetString(ReadBytes(bytes, ref position));
            if (field == 1)
            {
                key = item;
            }
            else if (field == 2)
            {
                value = item;
            }
        }

        return (key, value);
    }

    private static byte[] ReadBytes(byte[] bytes, ref int position)
    {
        var length = ReadVarint(bytes, ref position);
        var remaining = bytes.Length - position;
        if (length > (ulong)remaining)
        {
            throw new ProtocolException("Status bytes 长度非法");
        }

        var result = new byte[(int)length];
        Array.Copy(bytes, position, result, 0, result.Length);
        position += result.Length;
        return result;
    }

    private static ulong ReadVarint(byte[] bytes, ref int position)
    {
        ulong value = 0;
        for (var index = 0; index < 10; index++)
        {
            if (position >= bytes.Length)
            {
                throw new ProtocolException("Status varint 非法");
            }

            var current = bytes[position++];
            if (index == 9 && current > 1)
            {
                throw new ProtocolException("Status varint 非法");
            }

            if (current < 0x80)
            {
                return value | ((ulong)current << (index * 7));
            }

            value |= (ulong)(current & 0x7F) << (index * 7);
        }

        throw new ProtocolException("Status varint 非法");
    }
}
