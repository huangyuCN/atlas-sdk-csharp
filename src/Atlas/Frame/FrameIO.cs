using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Frame;

public static class FrameIO
{
    // ReadFull 读满指定字节数，处理 TCP 的半包读取。
    private static async ValueTask ReadFullAsync(Stream stream, byte[] buffer, int count, CancellationToken cancellationToken)
    {
        var offset = 0;
        while (offset < count)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), cancellationToken);
            if (read == 0)
            {
                throw new EndOfStreamException("连接关闭：帧截断");
            }
            offset += read;
        }
    }

    public static async ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(
        Stream stream,
        int maxBodySize,
        CancellationToken cancellationToken)
    {
        if (stream == null)
        {
            throw new ArgumentNullException(nameof(stream));
        }

        var headerBytes = new byte[FrameConst.HeaderSize];
        await ReadFullAsync(stream, headerBytes, headerBytes.Length, cancellationToken);
        var header = Header.Decode(headerBytes);
        Header.Check(header, maxBodySize);

        var body = new byte[(int)header.Length];
        if (body.Length > 0)
        {
            await ReadFullAsync(stream, body, body.Length, cancellationToken);
        }
        return (header, body);
    }

    public static async ValueTask WriteFrameAsync(
        Stream stream,
        Header header,
        byte[] body,
        int maxBodySize,
        CancellationToken cancellationToken)
    {
        if (stream == null)
        {
            throw new ArgumentNullException(nameof(stream));
        }
        if (body == null)
        {
            throw new ArgumentNullException(nameof(body));
        }

        header = PrepareHeader(header, body.Length);
        Header.Check(header, maxBodySize);

        var headerBytes = header.Encode();
        var output = new byte[headerBytes.Length + body.Length];
        Buffer.BlockCopy(headerBytes, 0, output, 0, headerBytes.Length);
        Buffer.BlockCopy(body, 0, output, headerBytes.Length, body.Length);
        await stream.WriteAsync(output.AsMemory(), cancellationToken);
    }

    // EncodeMessage 将 header 与 body 编码为完整帧字节（消息边界传输体，如 WebSocket：
    // 一条消息 = 一个完整帧）。与 Go frame.Encode 语义一致：长度超限返回协议错误，
    // Magic/Version 零值按协议默认补齐，Length 以实际 body 长度为准。
    public static byte[] EncodeMessage(Header header, byte[] body, int maxBodySize)
    {
        if (body == null)
        {
            throw new ArgumentNullException(nameof(body));
        }

        header = PrepareHeader(header, body.Length);
        Header.Check(header, maxBodySize);

        var headerBytes = header.Encode();
        var output = new byte[headerBytes.Length + body.Length];
        Buffer.BlockCopy(headerBytes, 0, output, 0, headerBytes.Length);
        Buffer.BlockCopy(body, 0, output, headerBytes.Length, body.Length);
        return output;
    }

    // DecodeMessage 从一条完整消息解析帧（与 EncodeMessage 对应；WS 读侧）。
    // 消息长度与帧头 bodyLen 不一致即协议非法——消息边界传输下已失步，由上层
    // 按协议错误终止连接（对齐 Go frame.Decode）。
    public static (Header Header, byte[] Body) DecodeMessage(byte[] message, int maxBodySize)
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }
        if (message.Length < FrameConst.HeaderSize)
        {
            throw new ProtocolException($"消息短于帧头: {message.Length} < {FrameConst.HeaderSize}");
        }

        var headerBytes = new byte[FrameConst.HeaderSize];
        Array.Copy(message, headerBytes, headerBytes.Length);
        var header = Header.Decode(headerBytes);
        Header.Check(header, maxBodySize);
        if ((ulong)(message.Length - FrameConst.HeaderSize) != header.Length)
        {
            throw new ProtocolException($"消息长度与 bodyLen 不一致: {message.Length - FrameConst.HeaderSize} != {header.Length}");
        }

        var body = new byte[message.Length - FrameConst.HeaderSize];
        Buffer.BlockCopy(message, FrameConst.HeaderSize, body, 0, body.Length);
        return (header, body);
    }

    // 写帧以实际 body 长度为准，并补齐协议默认 magic/version，与 Go Frame.Write 一致。
    private static Header PrepareHeader(Header header, int bodyLength)
    {
        if (header.Magic == 0)
        {
            header.Magic = FrameConst.Magic;
        }
        if (header.Version == 0)
        {
            header.Version = FrameConst.Version;
        }
        header.Length = (uint)bodyLength;
        return header;
    }
}
