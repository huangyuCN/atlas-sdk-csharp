using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Frame;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Transport;

// KCP 传输测试：KcpSharp 客户端(KcpTransport) ↔ KcpSharp 服务端(KcpEchoServer)，
// 帧级往返验证消息模式互通（头消息 + body 消息）。
public sealed class KcpTransportTest
{
    [Fact]
    public async Task Roundtrip_RequestResponse_FrameEcho()
    {
        var server = new KcpEchoServer();
        await using var _ = server;
        await using var transport = await KcpTransport.ConnectAsync("127.0.0.1", server.Port, CancellationToken.None);

        // 请求帧：seq=7、body "hello-kcp"
        var requestBody = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        var requestHeader = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 7,
        };
        await transport.WriteFrameAsync(requestHeader, requestBody, FrameConst.MaxBodySize, CancellationToken.None);

        // 读回显响应
        using var cts = new CancellationTokenSource(5000);
        var (responseHeader, responseBody) = await transport.ReadFrameAsync(FrameConst.MaxBodySize, cts.Token);

        Assert.Equal(MsgType.Response, responseHeader.Type);
        Assert.Equal(7u, responseHeader.Seq);
        Assert.Equal(requestBody, responseBody);
    }

    [Fact]
    public async Task EmptyBody_RequestResponse_HeaderOnly()
    {
        var server = new KcpEchoServer();
        await using var _ = server;
        await using var transport = await KcpTransport.ConnectAsync("127.0.0.1", server.Port, CancellationToken.None);

        // 空 body 请求（如 Ping）
        var requestHeader = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 3,
        };
        await transport.WriteFrameAsync(requestHeader, Array.Empty<byte>(), FrameConst.MaxBodySize, CancellationToken.None);

        using var cts = new CancellationTokenSource(5000);
        var (responseHeader, responseBody) = await transport.ReadFrameAsync(FrameConst.MaxBodySize, cts.Token);

        Assert.Equal(MsgType.Response, responseHeader.Type);
        Assert.Equal(3u, responseHeader.Seq);
        Assert.Empty(responseBody);
    }

    [Fact]
    public async Task ConnectFailure_Throws()
    {
        // 未监听端口 → ConnectAsync 不应抛（KCP 无连接，拨号即成功）；
        // 实际失败在读写。此处验证 ConnectAsync 对坏参数抛错。
        await Assert.ThrowsAsync<ArgumentException>(
            () => KcpTransport.ConnectAsync("", 1, CancellationToken.None));
    }
}
