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
        var off = client.On("notify-op", (op, payload) => received.TrySetResult(payload));
        using (off)
        {
            await server.PushNotifyAsync("notify-op", new byte[] { 1, 2, 3 });
            Assert.Equal(new byte[] { 1, 2, 3 }, await received.Task.WaitAsync(TimeSpan.FromSeconds(2)));
        }
    }

    // 双通道 Client：状态聚合向下降级——业务 Connected、战斗 Reconnecting 时
    // Client.State == Reconnecting（取最劣）。
    [Fact]
    public async Task DualClient_StateAggregation_TakesWorst()
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
            Assert.Equal(ClientState.Connected, client.State);

            // 踢掉战斗通道：战斗进入重连（Reconnecting）→ 聚合降级为 Reconnecting。
            var battleView = client.Channel(ChannelKind.Battle);
            Assert.NotNull(battleView);
            await Assert.ThrowsAsync<NetworkException>(
                () => battleView.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // 等待战斗通道进入重连。
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (battleView.State != ClientState.Reconnecting && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
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
        using var off = unknown.On("op", (_, _) => { }); // 空退订句柄可 Dispose。
    }
}
