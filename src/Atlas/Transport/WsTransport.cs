using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;

namespace Atlas.Transport;

// WsTransport 基于 ClientWebSocket 的消息边界传输（WS 通道）：
// 一条 WS 消息 = 一个完整帧（消息载荷 = 帧头 16B + body）。读侧收整条消息后
// 用 DecodeMessage 解析；写侧把整帧编码为单条二进制消息发送。WS 无粘包，
// 与 Go 的 wsTransport 语义一致。消息长度超限（读侧）为协议违规——不可重试。
public sealed class WsTransport : ITransport
{
    // defaultHandshakeTimeout 是 WS 握手超时（ClientWebSocket 默认无超时，显式兜底防永久挂起）。
    private static readonly TimeSpan DefaultHandshakeTimeout = TimeSpan.FromSeconds(10);

    private readonly ClientWebSocket _socket;

    private WsTransport(ClientWebSocket socket)
    {
        _socket = socket;
    }

    // NormalizeUrl 规整 WS 拨号地址：host:port + path（空则 "/ws"，对齐模板网关约定）；
    // 完整 ws:// wss:// URL 原样返回（path 忽略）。
    public static string NormalizeUrl(string address, string path)
    {
        if (address.StartsWith("ws://", StringComparison.Ordinal)
            || address.StartsWith("wss://", StringComparison.Ordinal))
        {
            return address;
        }
        if (string.IsNullOrEmpty(path))
        {
            path = "/ws";
        }
        else if (!path.StartsWith("/", StringComparison.Ordinal))
        {
            path = "/" + path;
        }
        return "ws://" + address + path;
    }

    // ConnectAsync 建立 WebSocket 连接。url 支持 host:port（自动补 ws:// + path）或完整 URL。
    public static async Task<ITransport> ConnectAsync(string address, string path, CancellationToken cancellationToken)
    {
        var url = NormalizeUrl(address, path);
        return await ConnectUrlAsync(url, cancellationToken);
    }

    // ConnectAsync 以完整 URL 建立 WebSocket 连接（path 已含于 URL）。
    public static async Task<ITransport> ConnectAsync(string url, CancellationToken cancellationToken)
    {
        return await ConnectUrlAsync(url, cancellationToken);
    }

    private static async Task<ITransport> ConnectUrlAsync(string url, CancellationToken cancellationToken)
    {
        var socket = new ClientWebSocket();
        try
        {
            using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            handshakeCts.CancelAfter(DefaultHandshakeTimeout);
            await socket.ConnectAsync(new Uri(url), handshakeCts.Token);
            return new WsTransport(socket);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            socket.Dispose();
            throw new NetworkException($"WebSocket 握手超时: {url}");
        }
        catch (Exception exception) when (!(exception is OperationCanceledException))
        {
            socket.Dispose();
            throw new NetworkException($"WebSocket 连接失败: {url}", exception);
        }
    }

    public async ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(int maxBodySize, CancellationToken cancellationToken)
    {
        // 读侧消息上限：帧头 + body 上限（对齐 Go conn.SetReadLimit(HeaderSize+maxBodySize)）。
        var maxMessageSize = FrameConst.HeaderSize + maxBodySize;
        var buffer = new byte[4096];
        var received = new System.Collections.Generic.List<byte>(FrameConst.HeaderSize + 4096);
        while (true)
        {
            var result = await _socket.ReceiveAsync(buffer, cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                throw new EndOfStreamException("WebSocket 已关闭");
            }
            if (received.Count + result.Count > maxMessageSize)
            {
                throw new ProtocolException($"WebSocket 消息超限: {received.Count + result.Count} > {maxMessageSize}");
            }
            var segment = new byte[result.Count];
            Array.Copy(buffer, segment, result.Count);
            received.AddRange(segment);
            if (result.EndOfMessage)
            {
                break;
            }
        }
        return FrameIO.DecodeMessage(received.ToArray(), maxBodySize);
    }

    public Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken cancellationToken)
    {
        byte[] message;
        try
        {
            message = FrameIO.EncodeMessage(header, body, maxBodySize);
        }
        catch (ProtocolException)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw new ProtocolException($"WebSocket 帧编码失败: {exception.Message}");
        }
        return _socket.SendAsync(message, WebSocketMessageType.Binary, true, cancellationToken);
    }

    public async Task CloseAsync()
    {
        try
        {
            if (_socket.State == WebSocketState.Open)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "close", cts.Token);
            }
        }
        catch (Exception)
        {
            // 关闭失败忽略：底层 Abort 兜底。
        }
        finally
        {
            _socket.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        return default;
    }
}
