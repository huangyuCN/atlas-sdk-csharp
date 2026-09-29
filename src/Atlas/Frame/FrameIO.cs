using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Frame;

// 帧 I/O 入口：字节层编解码唯一实现是生成物（src/Atlas/Frame/Gen/FrameCodec.cs，
// 由框架仓 gen-frame 产出、scripts/gen-dto.sh 快照），本文件只保留流式读写、
// 出站拦截（EncodeMessage 前的 Header.Check）与错误类型收敛（ProtocolException）。
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

        var headerBytes = new byte[FrameGen.HeaderSize];
        await ReadFullAsync(stream, headerBytes, headerBytes.Length, cancellationToken);
        // 头校验（含长度上限）先于 body 读取：超限头/坏头立即按协议错误拒绝（golden 口径）。
        var header = Header.DecodeChecked(headerBytes, maxBodySize);

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

        var output = EncodeMessage(header, body, maxBodySize);
        await stream.WriteAsync(output.AsMemory(), cancellationToken);
    }

    // EncodeMessage 将 header 与 body 编码为完整帧字节（消息边界传输体，如 WebSocket：
    // 一条消息 = 一个完整帧）。出站先按生成物口径校验（非法头/超限 body 抛 ProtocolException），
    // Magic/Version 零值按协议默认补齐，Length 以实际 body 长度为准。
    public static byte[] EncodeMessage(Header header, byte[] body, int maxBodySize)
    {
        if (body == null)
        {
            throw new ArgumentNullException(nameof(body));
        }

        var prepared = PrepareHeader(header, body.Length);
        Header.Check(prepared, maxBodySize);
        return FrameCodec.Encode(prepared.ToWire(), body, maxBodySize);
    }

    // DecodeMessage 从一条完整消息解析帧（与 EncodeMessage 对应；WS 读侧）。
    // 消息长度与帧头 bodyLen 不一致即协议非法——消息边界传输下已失步，由上层
    // 按协议错误终止连接（转发生成物 DecodeMessage，口径与 Go frame.DecodeMessage 一致）。
    public static (Header Header, byte[] Body) DecodeMessage(byte[] message, int maxBodySize)
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        try
        {
            var (header, body) = FrameCodec.DecodeMessage(message, maxBodySize);
            return (Header.FromWire(header), body);
        }
        catch (FrameCodecException exception)
        {
            throw new ProtocolException(exception.Message);
        }
    }

    // 写帧以实际 body 长度为准，并补齐协议默认 magic/version，与 Go Frame.Write 一致。
    private static Header PrepareHeader(Header header, int bodyLength)
    {
        if (header.Magic == 0)
        {
            header.Magic = FrameGen.Magic;
        }
        if (header.Version == 0)
        {
            header.Version = FrameGen.Version;
        }
        header.Length = (uint)bodyLength;
        return header;
    }
}
