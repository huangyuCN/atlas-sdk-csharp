using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using Atlas.Tests.Client;
using Xunit;

namespace Atlas.Tests.Transport;

// TCP / WS 传输测试：TCP 走真实 TcpTransport 拨号 FakeServer（端到端帧路径）；
// WS 语义（一条消息 = 一个完整帧）由本地自旋的 WS echo 服务端覆盖——测试宿主
// net8.0 的 HttpListener + WebSocket 可作服务端，验证消息边界与编解码正确性。
public sealed class TransportTest
{
    [Fact]
    public async Task TcpTransport_Roundtrip_OverRealChannel()
    {
        await using var server = new FakeServer();
        await using var channel = new Channel(
            token => TcpTransport.ConnectAsync(IPAddress.Loopback.ToString(), server.Port, token),
            new ChannelOptions
            {
                InvokeTimeoutMs = 500,
                MaxBodySize = FrameConst.MaxBodySize,
            });
        await channel.ConnectAsync(CancellationToken.None);

        var response = await channel.InvokeRawAsync("echo", new byte[] { 1, 2, 3 }, CancellationToken.None);

        Assert.Equal(new byte[] { 1, 2, 3 }, response);
        Assert.Equal(ClientState.Connected, channel.State);
    }

    [Fact]
    public async Task TcpTransport_ConnectFailure_ThrowsNetworkException()
    {
        // 找一个空闲端口（先监听再关闭，快速取得端口号后立即拨号会失败）。
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();

        await using var channel = new Channel(
            token => TcpTransport.ConnectAsync(IPAddress.Loopback.ToString(), port, token),
            new ChannelOptions { InvokeTimeoutMs = 500 });
        await Assert.ThrowsAsync<NetworkException>(
            () => channel.ConnectAsync(CancellationToken.None));
    }

    [Fact]
    public async Task WsTransport_Roundtrip_OneMessageIsOneFrame()
    {
        await using var server = new WsEchoServer();
        var url = server.Url;

        var transport = await WsTransport.ConnectAsync(url, CancellationToken.None);
        try
        {
            var body = Body.BuildRequestBody("echo", new byte[] { 9, 9 });
            var header = new Header { Type = MsgType.Request, Version = FrameConst.Version, Seq = 1 };
            await transport.WriteFrameAsync(header, body, FrameConst.MaxBodySize, CancellationToken.None);

            var (replyHeader, replyBody) = await transport.ReadFrameAsync(FrameConst.MaxBodySize, CancellationToken.None);
            Assert.Equal(MsgType.Response, replyHeader.Type);
            Assert.Equal(FrameConst.Version, replyHeader.Version);
            Assert.Equal((uint)1, replyHeader.Seq);
            var (operation, payload) = Body.ParseRequestBody(replyBody);
            Assert.Equal("echo", operation);
            Assert.Equal(new byte[] { 9, 9 }, payload);
        }
        finally
        {
            await transport.CloseAsync();
        }
    }

    [Fact]
    public void WsTransport_NormalizeUrl_AddsWsPrefixAndDefaultPath()
    {
        Assert.Equal("ws://127.0.0.1:9002/ws", WsTransport.NormalizeUrl("127.0.0.1:9002", ""));
        Assert.Equal("ws://127.0.0.1:9002/custom", WsTransport.NormalizeUrl("127.0.0.1:9002", "custom"));
        Assert.Equal("wss://host/game", WsTransport.NormalizeUrl("wss://host/game", "ignored"));
        Assert.Equal("ws://host:9002", WsTransport.NormalizeUrl("ws://host:9002", "ignored"));
    }

    [Fact]
    public async Task WsTransport_ConcurrentWrites_DoNotThrow()
    {
        // 写互斥验证（评审补强）：ClientWebSocket.SendAsync 要求单写者，多线程并发
        // 写会抛 InvalidOperationException——内建写锁应串行化，全部写成功且帧完整。
        await using var server = new WsEchoServer();
        var transport = await WsTransport.ConnectAsync(server.Url, CancellationToken.None);
        try
        {
            var tasks = new List<Task>(8);
            for (var i = 0; i < 8; i++)
            {
                var seq = (uint)(i + 1);
                var header = new Header { Type = MsgType.Request, Version = FrameConst.Version, Seq = seq };
                var body = Body.BuildRequestBody("echo", new[] { (byte)i });
                tasks.Add(transport.WriteFrameAsync(header, body, FrameConst.MaxBodySize, CancellationToken.None));
            }
            await Task.WhenAll(tasks); // 任一写抛 InvalidOperationException 即失败

            // 读回 8 个响应验证无交错损坏（每条消息 = 完整帧）。
            var seen = new HashSet<uint>();
            for (var i = 0; i < 8; i++)
            {
                var (replyHeader, replyBody) =
                    await transport.ReadFrameAsync(FrameConst.MaxBodySize, CancellationToken.None);
                Assert.Equal(MsgType.Response, replyHeader.Type);
                Assert.True(seen.Add(replyHeader.Seq), $"重复 seq {replyHeader.Seq}");
                var (operation, payload) = Body.ParseRequestBody(replyBody);
                Assert.Equal("echo", operation);
                Assert.Equal((uint)(replyHeader.Seq - 1), payload[0]);
            }
        }
        finally
        {
            await transport.CloseAsync();
        }
    }
}

// WsEchoServer：HttpListener 承载的本地 WS echo 服务端——收到消息回同样字节
// 的 Response 帧（测试宿主专用，验证「一条消息 = 一个完整帧」的 WS 边界语义）。
internal sealed class WsEchoServer : IAsyncDisposable
{
    private readonly HttpListener _listener = new();
    private readonly Task _serveTask;
    private readonly string _url;

    public WsEchoServer()
    {
        _listener.Prefixes.Add("http://127.0.0.1:18080/");
        _listener.Start();
        _url = "ws://127.0.0.1:18080/";
        _serveTask = Task.Run(ServeAsync);
    }

    public string Url => _url;

    private async Task ServeAsync()
    {
        try
        {
            while (_listener.IsListening)
            {
                var context = await _listener.GetContextAsync();
                if (!context.Request.IsWebSocketRequest)
                {
                    context.Response.StatusCode = 400;
                    context.Response.Close();
                    continue;
                }

                var wsContext = await context.AcceptWebSocketAsync(null);
                var socket = wsContext.WebSocket;
                try
                {
                    var buffer = new byte[64 * 1024];
                    while (socket.State == System.Net.WebSockets.WebSocketState.Open)
                    {
                        var receiveResult = await socket.ReceiveAsync(buffer, CancellationToken.None);
                        if (receiveResult.MessageType == System.Net.WebSockets.WebSocketMessageType.Close)
                        {
                            await socket.CloseAsync(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, "close", CancellationToken.None);
                            return;
                        }

                        var message = new byte[receiveResult.Count];
                        Array.Copy(buffer, message, receiveResult.Count);

                        // 收到的是完整帧（16B 头 + body）：帧头独立解码，body 切分后按
                        // Response 类型回写同帧（body 内仍为 request body，语义为 echo）。
                        var headerBytes = new byte[FrameConst.HeaderSize];
                        Array.Copy(message, headerBytes, headerBytes.Length);
                        var header = Header.Decode(headerBytes);
                        var body = new byte[message.Length - FrameConst.HeaderSize];
                        Array.Copy(message, FrameConst.HeaderSize, body, 0, body.Length);
                        var responseHeader = new Header
                        {
                            Magic = FrameConst.Magic,
                            Version = header.Version,
                            Type = MsgType.Response,
                            Seq = header.Seq,
                        };
                        var encoded = FrameIO.EncodeMessage(responseHeader, body, FrameConst.MaxBodySize);
                        await socket.SendAsync(encoded, System.Net.WebSockets.WebSocketMessageType.Binary, true, CancellationToken.None);
                    }
                }
                catch (System.Net.WebSockets.WebSocketException)
                {
                    // 客户端断开：忽略，继续接受下一连接。
                }
            }
        }
        catch (ObjectDisposedException)
        {
        }
        catch (HttpListenerException)
        {
        }
    }

    public ValueTask DisposeAsync()
    {
        _listener.Stop();
        _listener.Close();
        return ValueTask.CompletedTask;
    }
}
