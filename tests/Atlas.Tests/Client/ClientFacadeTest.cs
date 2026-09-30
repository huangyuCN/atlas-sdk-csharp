using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Client;

// ClientFacadeTest 验证 AtlasClient 门面（对齐 Go client.go Client/ChannelView）：
//   - 单通道形态：Invoke/On 默认走业务通道；Close 优雅关闭全部通道；
//   - 多通道（dual 雏形）：状态聚合向下降级（任一通道取最劣）、Channel(kind)
//     视图独立 Invoke/On/State；
//   - 双通道独立重连（对齐 Go DialDual 的 per-channel 独立）雏形。
public sealed class ClientFacadeTest
{
    private static ChannelOptions FastOptions(ChannelOptions? seed = null)
    {
        var options = seed ?? new ChannelOptions();
        options.InvokeTimeoutMs = 500;
        options.BackoffBaseMs = 10;
        options.BackoffMaxMs = 30;
        options.QueueSize = 8;
        return options;
    }

    // SingleClient 启动单业务通道 Client（连接本地 FakeServer）。
    private static async Task<AtlasClient> StartSingleClientAsync(FakeServer server)
    {
        var client = new AtlasClient(
            new ChannelConfig(
                ChannelKind.Business,
                token => TcpTestTransport.ConnectAsync(server.Port, token))
            {
                Options = FastOptions(),
            });
        await client.ConnectAsync(CancellationToken.None);
        return client;
    }

    [Fact]
    public async Task SingleChannel_InvokeAndClose_Roundtrip()
    {
        await using var server = new FakeServer();
        await using var client = await StartSingleClientAsync(server);

        Assert.Equal(ClientState.Connected, client.State);
        var reply = await client.InvokeRawAsync("echo", new byte[] { 9 }, CancellationToken.None);
        Assert.Equal(new byte[] { 9 }, reply);

        await client.CloseAsync();
        Assert.Equal(ClientState.Disconnected, client.State);
        await Assert.ThrowsAsync<NetworkException>(
            () => client.InvokeRawAsync("echo", Array.Empty<byte>(), CancellationToken.None));
    }

    [Fact]
    public async Task SingleChannel_OnNotify_DeliveredToSubscriber()
    {
        await using var server = new FakeServer();
        await using var client = await StartSingleClientAsync(server);

        var received = new TaskCompletionSource<byte[]>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var off = client.On("notify-op", (op, payload, _) => received.TrySetResult(payload));
        using (off)
        {
            await server.PushNotifyAsync("notify-op", new byte[] { 1, 2, 3 });
            Assert.Equal(new byte[] { 1, 2, 3 }, await received.Task.WaitAsync(TimeSpan.FromSeconds(2)));
        }
    }

    // 双通道 Client：状态聚合向下降级——业务 Connected、战斗 Reconnecting 时
    // Client.State == Reconnecting（取最劣）。
    // 稳定性（终验抖动修复，2026-09-30）：被踢后的 Reconnecting 是**瞬时窗口**——快退避
    // （BackoffBaseMs=10）+ 服务端仍在，一次退避即重连成功，实测窗口仅 8~25ms。旧写法
    // 「踢线后每 10ms 轮询状态」要靠轮询撞上该窗口：invoke 失败经线程池续体恢复
    //（RunContinuationsAsynchronously），续体若在窗口结束后才被调度，轮询永远看不到
    // Reconnecting，2s 后断言得 Expected: Reconnecting / Actual: Connected（全量并发下必红；
    // 单跑因续体及时而通过）。经 1ms 采样核验：窗口内战斗通道恒为 Reconnecting 且
    // Client.State 同步降级为 Reconnecting（25/25 轮），聚合逻辑无缺陷——属测试等待式
    // 断言抖动，故改为**事件驱动**：被踢后的重连拨号挂起并发出「重连已开始」信号，
    // 状态稳定停在 Reconnecting 再断言；断言本身未放宽。
    [Fact]
    public async Task DualClient_StateAggregation_TakesWorst()
    {
        await using var businessServer = new FakeServer();
        await using var battleServer = new FakeServer();
        var reconnectStarted = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var kicked = false;
        var client = new AtlasClient(
            new ChannelConfig(ChannelKind.Business, token => TcpTestTransport.ConnectAsync(businessServer.Port, token))
            {
                Options = FastOptions(),
            },
            new ChannelConfig(ChannelKind.Battle, async token =>
            {
                if (kicked)
                {
                    // 踢线后的重连拨号：发出信号并挂起到通道关闭（CloseAsync 取消令牌）——
                    // 此刻状态已置 Reconnecting 且不会再前进，断言不依赖时序窗口。
                    reconnectStarted.TrySetResult(true);
                    await Task.Delay(Timeout.Infinite, token);
                    throw new NetworkException("战斗通道重连拨号被测试挂起");
                }
                return await TcpTestTransport.ConnectAsync(battleServer.Port, token);
            })
            {
                Options = FastOptions(),
            });
        await using (client)
        {
            await client.ConnectAsync(CancellationToken.None);
            Assert.Equal(ClientState.Connected, client.State);

            // 踢掉战斗通道：战斗进入重连（Reconnecting）→ 聚合降级为 Reconnecting。
            var battleView = client.Channel(ChannelKind.Battle);
            Assert.NotNull(battleView);
            kicked = true; // 只影响后续拨号（当前连接不受影响）。
            await Assert.ThrowsAsync<NetworkException>(
                () => battleView.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // 事件驱动等待：重连拨号已发起（上限 5s 兜底；不靠轮询撞瞬时窗口）。
            await reconnectStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(ClientState.Reconnecting, battleView.State);
            Assert.Equal(ClientState.Reconnecting, client.State); // 聚合取最劣（战斗 Reconnecting）。
            Assert.Equal(ClientState.Connected, client.Channel(ChannelKind.Business).State); // 业务仍 Connected。
        }
    }

    // 双通道：业务通道 Invoke 默认走业务 server；ChannelView 独立到战斗通道。
    [Fact]
    public async Task DualClient_ChannelView_RoutesToOwnChannel()
    {
        await using var businessServer = new FakeServer();
        await using var battleServer = new FakeServer();
        var client = new AtlasClient(
            new ChannelConfig(ChannelKind.Business, token => TcpTestTransport.ConnectAsync(businessServer.Port, token))
            {
                Options = FastOptions(),
            },
            new ChannelConfig(ChannelKind.Battle, token => TcpTestTransport.ConnectAsync(battleServer.Port, token))
            {
                Options = FastOptions(),
            });
        await using (client)
        {
            await client.ConnectAsync(CancellationToken.None);

            // 业务 server 收到业务通道请求（含跨通道 echo 到不同 payload）。
            var businessReply = await client.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);
            Assert.Equal(new byte[] { 1 }, businessReply);

            // 战斗视图独立发请求 → 战斗 server 响应。
            var battleReply = await client.Channel(ChannelKind.Battle)
                .InvokeRawAsync("echo", new byte[] { 2 }, CancellationToken.None);
            Assert.Equal(new byte[] { 2 }, battleReply);
            Assert.Equal(ChannelKind.Battle, client.Channel(ChannelKind.Battle).Kind);
            Assert.Equal(ChannelKind.Business, client.Channel(ChannelKind.Business).Kind);
        }
    }

    // 未知 kind 视图 nil 安全（方法返回失败/Disconnected/空退订句柄）。
    [Fact]
    public async Task UnknownChannelView_IsSafe()
    {
        await using var server = new FakeServer();
        await using var client = await StartSingleClientAsync(server);

        var unknown = client.Channel((ChannelKind)99);
        Assert.NotNull(unknown);
        Assert.Equal(ClientState.Disconnected, unknown.State);
        Assert.Equal(ChannelKind.Business, unknown.Kind); // 未知视图返回默认角色。
        await Assert.ThrowsAsync<NetworkException>(
            () => unknown.InvokeRawAsync("echo", Array.Empty<byte>(), CancellationToken.None));
        using var off = unknown.On("op", (_, _, _) => { }); // 空退订句柄可 Dispose。
    }
}
