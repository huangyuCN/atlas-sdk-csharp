using System;
using Atlas.Errors;

namespace Atlas.Frame;

public sealed class ReplyData
{
    public ReplyData(byte[] data, Status? status)
    {
        Data = data;
        Status = status;
    }

    public byte[] Data { get; }

    public Status? Status { get; }
}

public static class Reply
{
    // 成功 [hasError=0][dataLen:u32][data...]
    // 失败 [hasError=1][statusLen:u32][Status][dataLen:u32][data...]
    public static ReplyData DecodeReply(byte[] body)
    {
        if (body == null || body.Length < 5)
        {
            throw new ProtocolException("reply 过短");
        }

        return body[0] == 0
            ? DecodeSuccess(body)
            : DecodeError(body);
    }

    private static ReplyData DecodeSuccess(byte[] body)
    {
        var dataLength = ReadLength(body, 1);
        var dataOffset = 5;
        var data = CopySegment(body, dataOffset, dataLength, "reply data 截断");
        return new ReplyData(data, null);
    }

    private static ReplyData DecodeError(byte[] body)
    {
        var statusLength = ReadLength(body, 1);
        var statusOffset = 5;
        var statusBytes = CopySegment(body, statusOffset, statusLength, "reply status 截断");
        var status = statusLength == 0 ? new Status() : StatusWire.Decode(statusBytes);
        var dataLengthOffset = statusOffset + statusLength;
        var dataLength = ReadLength(body, dataLengthOffset);
        var data = CopySegment(body, dataLengthOffset + 4, dataLength, "reply data 截断");
        return new ReplyData(data, status);
    }

    private static int ReadLength(byte[] body, int offset)
    {
        if (offset < 0 || offset > body.Length - 4)
        {
            throw new ProtocolException("reply data 截断");
        }

        var length = ((uint)body[offset] << 24)
            | ((uint)body[offset + 1] << 16)
            | ((uint)body[offset + 2] << 8)
            | body[offset + 3];
        if (length > int.MaxValue)
        {
            throw new ProtocolException("reply 长度超过本地上限");
        }

        return (int)length;
    }

    private static byte[] CopySegment(byte[] body, int offset, int length, string error)
    {
        if (offset < 0 || length < 0 || offset > body.Length - length)
        {
            throw new ProtocolException(error);
        }

        var result = new byte[length];
        Array.Copy(body, offset, result, 0, length);
        return result;
    }
}
