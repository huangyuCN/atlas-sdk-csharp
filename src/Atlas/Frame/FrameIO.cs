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
