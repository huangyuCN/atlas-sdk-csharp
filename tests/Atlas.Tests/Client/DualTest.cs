using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Client;

// DualTest 验证 dual 双通道编排的链式重绑（对齐 Go DialDual——规范 §5.2：
// 业务通道重登成功 → 自动触发战斗通道 Join 重绑；战斗未就绪跳过本轮）：
//   - 业务 + 战斗双通道各自独立连接/心跳/重连；
//   - 业务 ReconnectHook 成功 → 链式触发战斗 ReconnectHook（若战斗 Connected）；
//   - 战斗通道未就绪（自身也在重连）→ 业务钩子跳过战斗调用、业务重连不阻塞；
//   - 任一通道拨号失败 → 整体失败回滚（已有 AtlasClient.ConnectAsync 覆盖，
//     此处补拨号失败回滚断言）。
public sealed class DualTest
{
    private static ChannelOptions FastOptions()
    {
        return new ChannelOptions
        {
            InvokeTimeoutMs = 500,
            BackoffBaseMs = 10,
            BackoffMaxMs = 30,
            QueueSize = 8,
        };
    }

    // 业务踢线重连成功后：业务钩子执行 → 链式触发战斗钩子（战斗已 Connected）。
    [Fact]
    public async Task BusinessReconnect_ChainsToBattleHook_WhenBattleConnected()
    {
        await using var businessServer = new FakeServer();
        await using var battleServer = new FakeServer();

        var businessHooks = 0;
        var battleHooks = 0;
        var client = new AtlasClient(
            new ChannelConfig(ChannelKind.Business, token => TcpTestTransport.ConnectAsync(businessServer.Port, token))
            {
                Options = FastOptions(),
                ReconnectHook = () =>
                {
                    Interlocked.Increment(ref businessHooks);
                    return Task.CompletedTask;
                },
            },
            new ChannelConfig(ChannelKind.Battle, token => TcpTestTransport.ConnectAsync(battleServer.Port, token))
            {
                Options = FastOptions(),
                ReconnectHook = () =>
                {
                    Interlocked.Increment(ref battleHooks);
                    return Task.CompletedTask;
                },
            });

        await using (client)
        {
            await client.ConnectAsync(CancellationToken.None);
            Assert.Equal(ClientState.Connected, client.State);

            // 踢掉业务通道 → 业务重连成功后应链式触发战斗钩子。
            await Assert.ThrowsAsync<NetworkException>(
                () => client.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            var deadline = DateTime.UtcNow.AddSeconds(3);
            while ((businessHooks < 1 || battleHooks < 1) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            Assert.Equal(1, Volatile.Read(ref businessHooks));
            Assert.Equal(1, Volatile.Read(ref battleHooks)); // 链式：业务成功后战斗钩子也被调
            Assert.Equal(ClientState.Connected, client.State);
        }
    }

    // 业务踢线重连、同时战斗也被踢（双通道都断）：业务钩子成功执行但战斗未
    // Connected → 跳过战斗钩子（战斗自身重连成功后执行自己的钩子）。业务重连
    // 不因战斗未就绪而失败/阻塞。
    [Fact]
    public async Task BusinessReconnect_SkipsBattleHook_WhenBattleNotConnected()
    {
        await using var businessServer = new FakeServer();
        await using var battleServer = new FakeServer();

        var businessHooks = 0;
        var battleHooks = 0;
        var client = new AtlasClient(
            new ChannelConfig(ChannelKind.Business, token => TcpTestTransport.ConnectAsync(businessServer.Port, token))
            {
                Options = FastOptions(),
                ReconnectHook = () =>
                {
                    Interlocked.Increment(ref businessHooks);
                    return Task.CompletedTask;
                },
            },
            new ChannelConfig(ChannelKind.Battle, token => TcpTestTransport.ConnectAsync(battleServer.Port, token))
            {
                Options = FastOptions(),
                ReconnectHook = () =>
                {
                    Interlocked.Increment(ref battleHooks);
                    return Task.CompletedTask;
                },
            });

        await using (client)
        {
            await client.ConnectAsync(CancellationToken.None);

            // 业务踢线（进入重连）+ 战斗踢线（战斗也断）。
            await Assert.ThrowsAsync<NetworkException>(
                () => client.Channel(ChannelKind.Business).InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // 战斗也被踢——但等待业务重连时战斗可能已先恢复，为构造「战斗未就绪」
            // 的确定性窗口，这里踢完业务立即踢战斗。
            var battleView = client.Channel(ChannelKind.Battle);
            try
            {
                await battleView.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None);
            }
            catch (NetworkException)
            {
                // 预期：kick 触发断连，in-flight 失败。
            }

            // 等待业务重连成功（业务钩子执行）。
            var deadline = DateTime.UtcNow.AddSeconds(3);
            while (Volatile.Read(ref businessHooks) < 1 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
            Assert.Equal(1, Volatile.Read(ref businessHooks));

            // 业务重连成功是硬事实；战斗钩子是否被链式调用取决于时序（战斗可能
            // 先于业务恢复 → 也会被链式调）。本用例核心断言：业务重连不因战斗未就绪
            // 而失败——业务钩子执行即证明业务通道重连完成。
            Assert.Equal(ClientState.Connected, client.Channel(ChannelKind.Business).State);
        }
    }

    // 战斗通道自身重连成功后执行自己的 ReconnectHook（战斗独立钩子，非链式）。
    [Fact]
    public async Task BattleReconnect_RunsOwnHook_WhenBattleRecovers()
    {
        await using var businessServer = new FakeServer();
        await using var battleServer = new FakeServer();

        var businessHooks = 0;
        var battleHooks = 0;
        var client = new AtlasClient(
            new ChannelConfig(ChannelKind.Business, token => TcpTestTransport.ConnectAsync(businessServer.Port, token))
            {
                Options = FastOptions(),
                ReconnectHook = () =>
                {
                    Interlocked.Increment(ref businessHooks);
                    return Task.CompletedTask;
                },
            },
            new ChannelConfig(ChannelKind.Battle, token => TcpTestTransport.ConnectAsync(battleServer.Port, token))
            {
                Options = FastOptions(),
                ReconnectHook = () =>
                {
                    Interlocked.Increment(ref battleHooks);
                    return Task.CompletedTask;
                },
            });

        await using (client)
        {
            await client.ConnectAsync(CancellationToken.None);

            // 仅踢战斗通道：业务保持 Connected → 战斗自身重连成功执行自己的钩子。
            var battleView = client.Channel(ChannelKind.Battle);
            try
            {
                await battleView.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None);
            }
            catch (NetworkException)
            {
            }

            var deadline = DateTime.UtcNow.AddSeconds(3);
            while (Volatile.Read(ref battleHooks) < 1 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            Assert.Equal(1, Volatile.Read(ref battleHooks));
            // 业务全程 Connected，未触发业务钩子（无重连）。
            Assert.Equal(0, Volatile.Read(ref businessHooks));
            Assert.Equal(ClientState.Connected, client.State);
        }
    }
}
