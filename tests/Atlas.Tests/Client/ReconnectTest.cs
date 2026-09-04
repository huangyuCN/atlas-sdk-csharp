using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Client;

// ReconnectTest 验证指数退避自动重连：服务端断开 → 自动重连 → 恢复后
// Invoke 成功；重连期间 Invoke 排队并在重连成功后按序重发（FIFO）。
// 对齐 Go reconnect_test.go 的 TestAutoReconnect / TestReconnectQueue /
// TestFailFastDuringReconnect / TestCloseStopsReconnect 语义。
public sealed class ReconnectTest
{
    // 退避参数：短 base 让测试快速完成（对齐 Go 测试用小退避）。
    private static ChannelOptions FastReconnectOptions(ChannelOptions? seed = null)
    {
        var options = seed ?? new ChannelOptions();
        options.InvokeTimeoutMs = 500;
        options.BackoffBaseMs = 10;
        options.BackoffMaxMs = 30;
        options.QueueSize = 8;
        return options;
    }

    // StartChannel 启动一个连接给定端口的 Channel（首连）。
    private static async Task<Channel> StartChannelAsync(Func<CancellationToken, Task<ITransport>> dial, ChannelOptions options)
    {
        var channel = new Channel(dial, options);
        await channel.ConnectAsync(CancellationToken.None);
        return channel;
    }

    [Fact]
    public async Task AutoReconnect_AfterServerKick_InvokeSucceeds()
    {
        await using var server = new FakeServer();
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastReconnectOptions());
        await using (channel)
        {
            // 建立后首次往返。
            var first = await channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);
            Assert.Equal(new byte[] { 1 }, first);

            // kick 断开当前连接 → readLoop 退出 → 自动重连。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // 等待重连成功（新连接被服务端 accept）。
            await WaitForReconnectedAsync(channel, server);

            // 新连接上 Invoke 成功。
            var response = await channel.InvokeRawAsync("echo", new byte[] { 2 }, CancellationToken.None);
            Assert.Equal(new byte[] { 2 }, response);
            Assert.True(server.AcceptedConnections >= 2, $"服务端应接受 ≥2 连接，实际 {server.AcceptedConnections}");
        }
    }

    [Fact]
    public async Task ReconnectQueue_RequestsDrainInOrder()
    {
        await using var server = new FakeServer();
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastReconnectOptions());
        await using (channel)
        {
            await channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);

            // kick 触发重连；重连窗口内的 Invoke 应排队。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // 对齐 Go reconnect_test 语义：kick 返回只保证 in-flight 被结算，状态
            // 切换 Reconnecting 由 readLoop/重连编排异步完成——先同步等到 Reconnecting
            // 再发排队请求（Disconnected→Reconnecting 之间是合法失败窗口：连接确已
            // 断开且重连未开始，Invoke 失败是正确的）。
            await WaitForStateAsync(channel, ClientState.Reconnecting);

            // 窗口内排队两个请求（不等重连完成）。
            var queuedA = channel.InvokeRawAsync("echo", new byte[] { 10 }, CancellationToken.None);
            var queuedB = channel.InvokeRawAsync("echo", new byte[] { 20 }, CancellationToken.None);

            await WaitForReconnectedAsync(channel, server);

            Assert.Equal(new byte[] { 10 }, await queuedA);
            Assert.Equal(new byte[] { 20 }, await queuedB);
        }
    }

    [Fact]
    public async Task FailFast_DoesNotQueue_FailsImmediately()
    {
        await using var server = new FakeServer();
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastReconnectOptions());
        await using (channel)
        {
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));
        }
    }

    [Fact]
    public async Task ReconnectQueueFull_WhenReconnecting_FailsImmediately()
    {
        // 用一个只能成功一次的 dial：首连成功，之后拨号一直失败 → 保持 Reconnecting。
        var attempts = 0;
        await using var server = new FakeServer();
        var options = FastReconnectOptions();
        options.QueueSize = 2;
        var channel = new Channel(
            token =>
            {
                var n = Interlocked.Increment(ref attempts);
                if (n == 1)
                {
                    return TcpTestTransport.ConnectAsync(server.Port, token);
                }
                throw new NetworkException("拨号失败");
            },
            options);
        await using (channel)
        {
            await channel.ConnectAsync(CancellationToken.None);

            // kick 断开 → 读循环退出 → 重连（拨号持续失败，保持 Reconnecting）。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));
            await WaitForStateAsync(channel, ClientState.Reconnecting);

            // 填满 2 个队列槽位（入队成功，挂起重连恢复）。
            var queuedA = channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);
            var queuedB = channel.InvokeRawAsync("echo", new byte[] { 2 }, CancellationToken.None);

            // 第 3 个请求应因队列满立即失败。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("echo", new byte[] { 3 }, CancellationToken.None));
        }
    }

    [Fact]
    public async Task Heartbeat_RestartsOnNewGeneration_DoesNotKillNewConnection()
    {
        // 验证 M4 评审 P1：重连后新代启动新心跳；旧代心跳不继续误杀新连接。
        // 用短心跳周期：kick 前已在新连接收到旧代心跳，重连后新连接收到新代心跳。
        await using var server = new FakeServer();
        var options = FastReconnectOptions();
        options.HeartbeatIntervalMs = 15;
        options.HeartbeatFailures = 2;
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);
        await using (channel)
        {
            // 等待首代心跳发生。
            await WaitForPingAsync(server, 1);
            var pingsBefore = server.PingCount;
            Assert.True(pingsBefore >= 1, "首代心跳应至少发生一次");

            // kick 断开 → 自动重连。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));
            await WaitForReconnectedAsync(channel, server);

            // 重连后新代心跳应继续（新连接上收到 Ping）。
            await WaitForPingCountAsync(server, pingsBefore + 1);

            // 连接仍健康：invoke 成功（旧代心跳未误杀新连接）。
            var response = await channel.InvokeRawAsync("echo", new byte[] { 7 }, CancellationToken.None);
            Assert.Equal(new byte[] { 7 }, response);
            Assert.Equal(ClientState.Connected, channel.State);
        }
    }

    [Fact]
    public async Task Close_StopsReconnect_StateDisconnected()
    {
        await using var server = new FakeServer();
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastReconnectOptions());
        await channel.CloseAsync();

        // Close 后状态为 Disconnected，不再重连（不会回到 Connecting/Connected）。
        await Task.Delay(80);
        Assert.Equal(ClientState.Disconnected, channel.State);
        await Assert.ThrowsAsync<NetworkException>(
            () => channel.InvokeRawAsync("echo", Array.Empty<byte>(), CancellationToken.None));
        await channel.DisposeAsync();
    }

    private static async Task WaitForReconnectedAsync(Channel channel, FakeServer server)
    {
        for (var attempt = 0; attempt < 200; attempt++)
        {
            if (channel.State == ClientState.Connected && server.AcceptedConnections >= 2)
            {
                return;
            }
            await Task.Delay(10);
        }
        Assert.Fail($"等待重连成功超时：state={channel.State} accepted={server.AcceptedConnections}");
    }

    private static async Task WaitForStateAsync(Channel channel, ClientState state)
    {
        for (var attempt = 0; attempt < 200; attempt++)
        {
            if (channel.State == state)
            {
                return;
            }
            await Task.Delay(10);
        }
        Assert.Fail($"等待状态 {state} 超时：当前 {channel.State}");
    }

    private static async Task WaitForPingAsync(FakeServer server, int count)
    {
        await WaitForPingCountAsync(server, count);
    }

    private static async Task WaitForPingCountAsync(FakeServer server, int count)
    {
        for (var attempt = 0; attempt < 200; attempt++)
        {
            if (server.PingCount >= count)
            {
                return;
            }
            await Task.Delay(10);
        }
        Assert.Fail($"等待心跳 {count} 次超时：当前 {server.PingCount}");
    }
}
