using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;
using AtlasTimeoutException = Atlas.Errors.TimeoutException;

namespace Atlas.Tests.Client;

// HeartbeatDeadLinkTest 验证传输心跳死链语义（规范 §5.2 + M4-2）：
//   1. 业务拒绝不计死链——往返完成即链路存活，失败计数归零，不触发重连；
//   2. 仅网络类失败（超时/写失败，无往返）计死链；连续 HeartbeatFailures 次触发重连；
//   3. 死链按代精确匹配——换代后旧代心跳不误杀新连接（补充 M4-1 覆盖的换代语义）；
//   4. M4-1 P1 落实：重连排队期限计入超时（看护认领超时失败），drain 跳过过期请求。
public sealed class HeartbeatDeadLinkTest
{
    // 快速参数：短心跳周期/超时/退避让测试快速收敛。
    private static ChannelOptions FastOptions(Action<ChannelOptions>? tune = null)
    {
        var options = new ChannelOptions
        {
            InvokeTimeoutMs = 120,
            BackoffBaseMs = 10,
            BackoffMaxMs = 40,
            QueueSize = 8,
        };
        tune?.Invoke(options);
        return options;
    }

    private static async Task<Channel> StartAsync(
        Func<CancellationToken, Task<ITransport>> dial,
        ChannelOptions options)
    {
        var channel = new Channel(dial, options);
        await channel.ConnectAsync(CancellationToken.None);
        return channel;
    }

    [Fact]
    public async Task Heartbeat_BusinessError_DoesNotCountDeadLink()
    {
        // 服务端对 Ping 持续回业务拒绝（模拟网关未注册内置 Ping）：往返完成即链路
        // 存活——远超失败阈值时长后仍 Connected、连接数保持 1（不重连）。
        await using var server = new FakeServer { PingBusinessError = true };
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o =>
            {
                o.HeartbeatIntervalMs = 15;
                o.HeartbeatFailures = 2;
            }));
        await using (channel)
        {
            // 等待多次心跳往返（远超 2 次失败阈值窗口）。
            await WaitUntilAsync(() => server.PingCount >= 5, TimeSpan.FromSeconds(2));

            Assert.Equal(ClientState.Connected, channel.State);
            Assert.Equal(1, server.AcceptedConnections); // 业务拒绝未触发重拨。
        }
    }

    [Fact]
    public async Task Heartbeat_NetworkTimeouts_TriggerReconnect()
    {
        // 服务端静默丢弃 Ping → 心跳 invoke 超时（无往返）计死链 → 连续
        // HeartbeatFailures 次后关本代连接并触发自动重连（AcceptedConnections 增长）。
        await using var server = new FakeServer();
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o =>
            {
                o.HeartbeatIntervalMs = 15;
                o.HeartbeatFailures = 2;
            }));
        await using (channel)
        {
            // 首连健康：业务请求正常往返。
            var resp = await channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);
            Assert.Equal(new byte[] { 1 }, resp);

            // 开始丢弃 Ping：心跳超时计死链，连续 2 次后关连接并重连。
            server.DropPings = true;
            await WaitUntilAsync(
                () => server.AcceptedConnections >= 2,
                TimeSpan.FromSeconds(3));

            // 恢复 Ping 响应后新连接健康（若重连循环已建新连接）。
            server.DropPings = false;
            Assert.True(server.AcceptedConnections >= 2, "死链应触发至少一次重连");
        }
    }

    [Fact]
    public async Task QueuedInvoke_ExpiresAfterDeadline_WhenReconnectNeverSucceeds()
    {
        // 重连持续失败（服务端已停、拨号被拒）→ 排队请求到点由看护认领超时失败，
        // 不无限期悬挂（M4-1 P1：排队期限计入超时，对齐 Go queueDeadlineWatch）。
        var server = new FakeServer();
        var options = FastOptions(o => o.HeartbeatIntervalMs = 0); // 关心跳，纯测排队。
        var channel = new Channel(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);
        await channel.ConnectAsync(CancellationToken.None);

        // 踢线断连；停掉服务端使重连拨号永久失败（退避重试永不成功）。
        _ = channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None);
        await server.DisposeAsync();

        // 等进入重连窗口后排队请求；invokeTimeout=120ms，看护到点认领超时失败。
        await WaitUntilAsync(() => channel.State == ClientState.Reconnecting, TimeSpan.FromSeconds(2));
        var started = DateTime.UtcNow;
        await Assert.ThrowsAsync<AtlasTimeoutException>(
            () => channel.InvokeRawAsync("echo", new byte[] { 7 }, CancellationToken.None));
        var elapsed = DateTime.UtcNow - started;
        Assert.True(elapsed >= TimeSpan.FromMilliseconds(80), $"应等待超时而非立即失败: {elapsed}");

        await channel.CloseAsync();
        await channel.DisposeAsync();
    }

    [Fact]
    public async Task QueueDrained_BeforeDeadline_CompletesSuccessfully()
    {
        // drain 先于 deadline 认领 → 请求成功返回（非超时），恰好一次结算。
        await using var server = new FakeServer();
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o =>
            {
                o.HeartbeatIntervalMs = 0; // 关心跳，纯测排队 drain。
                o.InvokeTimeoutMs = 2000;  // 排队期限长，保证 drain 先发生。
            }));
        await using (channel)
        {
            // 踢线进入重连（拨号到同一 server，会快速重连成功）。
            _ = channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None);

            // 尽快排队（可能已 Reconnecting）；若仍在 Connected 则直接成功。
            try
            {
                var resp = await channel.InvokeRawAsync("echo", new byte[] { 42 }, CancellationToken.None);
                Assert.Equal(new byte[] { 42 }, resp);
            }
            catch (NetworkException)
            {
                // 若踢线结算先到（in-flight 失败），重试一次排队。
                var resp = await channel.InvokeRawAsync("echo", new byte[] { 42 }, CancellationToken.None);
                Assert.Equal(new byte[] { 42 }, resp);
            }
        }
    }

    [Fact]
    public async Task Heartbeat_RecoveredConnection_RestartsCounting()
    {
        // 重连成功（新代）后心跳在新连接上继续；失败计数已随换代归零，
        // 恢复的健康连接不因旧代计数被误杀（M4 换代绑定 + 按代精确匹配）。
        await using var server = new FakeServer();
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o =>
            {
                o.HeartbeatIntervalMs = 15;
                o.HeartbeatFailures = 3;
            }));
        await using (channel)
        {
            await WaitUntilAsync(() => server.PingCount >= 1, TimeSpan.FromSeconds(2));

            // kick 断开 → 自动重连 → 新连接收到新代心跳且健康。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));
            await WaitUntilAsync(
                () => channel.State == ClientState.Connected && server.AcceptedConnections >= 2,
                TimeSpan.FromSeconds(3));

            await WaitUntilAsync(() => server.PingCount >= 2, TimeSpan.FromSeconds(2));
            var resp = await channel.InvokeRawAsync("echo", new byte[] { 9 }, CancellationToken.None);
            Assert.Equal(new byte[] { 9 }, resp);
            Assert.Equal(ClientState.Connected, channel.State);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }
            await Task.Delay(10);
        }
        throw new System.TimeoutException($"等待条件超时（{timeout.TotalSeconds}s）");
    }
}
