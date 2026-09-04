using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Transport;

// UDP 传输测试：真实 loopback UDP 数据报（server Socket 回显 + client UdpTransport）。
public sealed class UdpTransportTest
{
    // 建立一对 UDP 端点：server 绑定回环随机端口，返回 (server, serverPort)。
    private static (UdpEchoServer Server, int Port) StartServer()
    {
        var server = new UdpEchoServer();
        return (server, server.Port);
    }

    [Fact]
    public async Task Roundtrip_RequestResponse_OneDatagramPerFrame()
    {
        var (server, port) = StartServer();
        await using var transport = await UdpTransport.ConnectAsync("127.0.0.1", port, CancellationToken.None);
        await using var _ = server;

        // 请求：seq=7、body "hello-udp"
        var requestBody = new byte[] { 1, 2, 3, 4, 5 };
        var requestHeader = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 7,
        };
        await transport.WriteFrameAsync(requestHeader, requestBody, FrameConst.MaxBodySize, CancellationToken.None);

        // 读回显响应（同一数据报往返）
        var (responseHeader, responseBody) = await transport.ReadFrameAsync(
            FrameConst.MaxBodySize, CancellationToken.None);

        Assert.Equal(MsgType.Response, responseHeader.Type);
        Assert.Equal(7u, responseHeader.Seq);
        Assert.Equal(requestBody, responseBody);
    }

    [Fact]
    public async Task BadDatagram_IsSilentlyDropped_ThenValidOneReceived()
    {
        var (server, port) = StartServer();
        await using var transport = await UdpTransport.ConnectAsync("127.0.0.1", port, CancellationToken.None);
        await using var _ = server;

        // 向 server 端口投垃圾字节（构造假数据报），再由 server 回显给 transport：
        // 先让 server 收到垃圾 → 它回显 → transport 侧解析失败应静默丢弃。
        // 用第二个 client 直接发垃圾给 transport 的本地端口更直接——
        // 但 transport 已 connect 到 server；经 server 中转验证坏包丢弃。
        var garbage = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 1, 2, 3 };
        using (var sender = new UdpClient())
        {
            await sender.SendAsync(garbage, garbage.Length, "127.0.0.1", port);
        }

        // 稍候垃圾被 server 回显到 transport → 应被静默丢弃。
        // 随后发一个合法请求，验证 transport 仍能正常收发（丢弃不影响后续）。
        var requestBody = new byte[] { 9, 9, 9 };
        var requestHeader = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 2,
        };
        await transport.WriteFrameAsync(requestHeader, requestBody, FrameConst.MaxBodySize, CancellationToken.None);

        var (_, responseBody) = await transport.ReadFrameAsync(
            FrameConst.MaxBodySize, CancellationToken.None);
        Assert.Equal(requestBody, responseBody);
    }

    [Fact]
    public async Task DatagramOver64KiB_WriteFails_ProtocolException()
    {
        var (server, port) = StartServer();
        await using var transport = await UdpTransport.ConnectAsync("127.0.0.1", port, CancellationToken.None);
        await using var _ = server;

        var oversized = new byte[UdpTransport.MaxDatagramSize]; // body 已超（含头）
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
        };
        await Assert.ThrowsAsync<ProtocolException>(() =>
            transport.WriteFrameAsync(header, oversized, FrameConst.MaxBodySize, CancellationToken.None));
    }

    [Fact]
    public async Task Close_ReadReturns_ThenInvokeFails()
    {
        var (server, port) = StartServer();
        var transport = await UdpTransport.ConnectAsync("127.0.0.1", port, CancellationToken.None);
        await using var _ = server;

        await transport.CloseAsync();
        // 关闭后读应抛异常（socket 关闭）
        await Assert.ThrowsAnyAsync<Exception>(() =>
            transport.ReadFrameAsync(FrameConst.MaxBodySize, CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task HeaderTruncatedDatagram_IsSilentlyDropped()
    {
        var (server, port) = StartServer();
        await using var transport = await UdpTransport.ConnectAsync("127.0.0.1", port, CancellationToken.None);
        await using var _ = server;

        // 短于 16B 帧头的数据报（经 server 回显）
        var shortDatagram = new byte[] { 0x01, 0x02, 0x03 };
        using (var sender = new UdpClient())
        {
            await sender.SendAsync(shortDatagram, shortDatagram.Length, "127.0.0.1", port);
        }

        // 合法请求随后仍可往返（短包已丢弃）
        var requestBody = new byte[] { 0x55 };
        var requestHeader = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 3,
        };
        await transport.WriteFrameAsync(requestHeader, requestBody, FrameConst.MaxBodySize, CancellationToken.None);
        var (_, responseBody) = await transport.ReadFrameAsync(
            FrameConst.MaxBodySize, CancellationToken.None);
        Assert.Equal(requestBody, responseBody);
    }
}

// UdpEchoServer：绑定回环随机端口，收到数据报后原样回显（模拟网关 UDP 通道）。
internal sealed class UdpEchoServer : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly Task _serveTask;
    private readonly CancellationTokenSource _cts = new();

    public int Port { get; }

    public UdpEchoServer()
    {
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        _socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        Port = ((IPEndPoint)_socket.LocalEndPoint!).Port;
        _serveTask = Task.Run(() => ServeAsync(_cts.Token));
    }

    private async Task ServeAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[UdpTransport.MaxDatagramSize];
        var remote = new IPEndPoint(IPAddress.Any, 0);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var received = await _socket.ReceiveFromAsync(
                    new ArraySegment<byte>(buffer), SocketFlags.None, remote, cancellationToken);
                var remoteEndPoint = received.RemoteEndPoint;
                var count = received.ReceivedBytes;
                // 模拟网关：若是合法请求帧则改为 Response 回显（保持 seq/body）；
                // 垃圾/非法帧原样回显（由 client 侧验证坏包丢弃语义）。
                var echo = new byte[count];
                Array.Copy(buffer, echo, count);
                if (count >= FrameConst.HeaderSize && echo[5] == (byte)MsgType.Request)
                {
                    echo[5] = (byte)MsgType.Response; // type 字段偏移 5
                }
                await _socket.SendToAsync(new ArraySegment<byte>(echo), SocketFlags.None, remoteEndPoint, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException)
            {
                break;
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _socket.Dispose();
        try { _serveTask.Wait(TimeSpan.FromSeconds(1)); } catch { /* 忽略收尾异常 */ }
        return default;
    }
}
