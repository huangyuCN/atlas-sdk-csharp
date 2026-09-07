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

    [Fact]
    public async Task Write_InvalidHeaderType_ThrowsProtocol_OutboundIntercept()
    {
        var server = new KcpEchoServer();
        await using var _ = server;
        await using var transport = await KcpTransport.ConnectAsync("127.0.0.1", server.Port, CancellationToken.None);

        // 出站非法头（type=4、seq=0、非法 version）应在写侧即拦截（Header.Check），
        // 而非发出后由对端丢弃——对齐 FrameIO.WriteFrameAsync 语义。
        var badType = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = (MsgType)4,
            Seq = 1,
        };
        await Assert.ThrowsAsync<ProtocolException>(() =>
            transport.WriteFrameAsync(badType, Array.Empty<byte>(), FrameConst.MaxBodySize, CancellationToken.None));

        var zeroSeq = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 0,
        };
        await Assert.ThrowsAsync<ProtocolException>(() =>
            transport.WriteFrameAsync(zeroSeq, Array.Empty<byte>(), FrameConst.MaxBodySize, CancellationToken.None));

        var badVersion = new Header
        {
            Magic = FrameConst.Magic,
            Version = 3,
            Type = MsgType.Request,
            Seq = 1,
        };
        await Assert.ThrowsAsync<ProtocolException>(() =>
            transport.WriteFrameAsync(badVersion, Array.Empty<byte>(), FrameConst.MaxBodySize, CancellationToken.None));
    }

    [Fact]
    public async Task Write_NullBody_ThrowsArgumentNull()
    {
        var server = new KcpEchoServer();
        await using var _ = server;
        await using var transport = await KcpTransport.ConnectAsync("127.0.0.1", server.Port, CancellationToken.None);

        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 1,
        };
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            transport.WriteFrameAsync(header, null!, FrameConst.MaxBodySize, CancellationToken.None));
    }

    [Fact]
    public async Task Write_MaxBodySizeZero_DefaultsToAbsoluteLimit()
    {
        // maxBodySize<=0 应归一化为绝对上限（对齐 Header.NormalizeMaxBodySize）：
        // 小 body 在 maxBodySize=0 下不被误判超限（评审 P3 修复）。
        var server = new KcpEchoServer();
        await using var _ = server;
        await using var transport = await KcpTransport.ConnectAsync("127.0.0.1", server.Port, CancellationToken.None);

        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Request,
            Seq = 11,
        };
        var body = new byte[] { 1, 2, 3 };
        await transport.WriteFrameAsync(header, body, 0, CancellationToken.None);

        using var cts = new CancellationTokenSource(5000);
        var (responseHeader, responseBody) = await transport.ReadFrameAsync(0, cts.Token);
        Assert.Equal(MsgType.Response, responseHeader.Type);
        Assert.Equal(11u, responseHeader.Seq);
        Assert.Equal(body, responseBody);
    }

    // 死链写超时兜底（评审 P2-1 修复验证）：对端（黑洞 UDP 端点）存在但不回
    // 任何 ack，持续写填满发送窗口后 SendAsync 阻塞——KcpWriteTimeoutMs 兜底
    // 超时返回 NetworkException 而非无限挂起（对齐 Go kcpWriteTimeout=10s）。
    [Fact]
    public async Task Write_DeadLinkWindowFull_TimesOutInsteadOfHanging()
    {
        // 黑洞端点：绑定 UDP 端口但永不回包（不回 ack，模拟死链对端）。
        var blackhole = new Socket(SocketType.Dgram, ProtocolType.Udp);
        blackhole.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)blackhole.LocalEndPoint!).Port;

        await using var transport = await KcpTransport.ConnectAsync("127.0.0.1", port, CancellationToken.None);

        // 注入短超时验证超时路径（默认 10000ms 对齐 Go；测试用 800ms 缩短等待）。
        var original = KcpTransport.KcpWriteTimeoutMs;
        KcpTransport.KcpWriteTimeoutMs = 800;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            // 窗口 128 × MTU 1400 ≈ 179KB 未 ack 才满。写 ~200KB（多帧 8KB body）
            // 触发窗口满 → 后续写阻塞 → 800ms 超时抛 NetworkException。
            var header = new Header
            {
                Magic = FrameConst.Magic,
                Version = FrameConst.Version,
                Type = MsgType.Request,
                Seq = 1,
            };
            var payload = new byte[8192]; // 单帧 8KB body
            var deadLinkCaught = false;
            var writes = 0;
            for (int i = 0; i < 40; i++) // 40 × 8KB = 320KB > 179KB 窗口容量
            {
                try
                {
                    await transport.WriteFrameAsync(header, payload, FrameConst.MaxBodySize, CancellationToken.None);
                    header.Seq = (uint)(i + 2);
                    writes++;
                }
                catch (NetworkException ne) when (ne.Message.Contains("写帧超时"))
                {
                    deadLinkCaught = true;
                    break;
                }
            }
            Assert.True(deadLinkCaught, "死链窗口满后写应超时抛 NetworkException，而非无限挂起");
            Assert.True(writes < 40, "应未完成全部写即超时");
            // 验证走的是 CancelAfter 超时路径（耗时 ≈ 注入超时 800ms），
            // 而非立即失败——死链超时兜底的核心证据。
            Assert.True(sw.ElapsedMilliseconds >= 600,
                $"写超时应约 {KcpTransport.KcpWriteTimeoutMs}ms 触发，实际 {sw.ElapsedMilliseconds}ms");
        }
        finally
        {
            KcpTransport.KcpWriteTimeoutMs = original;
            blackhole.Dispose();
        }
    }
}
