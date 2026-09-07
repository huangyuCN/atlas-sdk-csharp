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

        // 战斗拨号：第 1 次成功（初始 Connect），之后持续失败——kick 后战斗保持
        // Reconnecting（未 Connected），确定性构造「战斗未就绪」窗口供 skip 断言。
        var battleAttempts = 0;
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
            new ChannelConfig(ChannelKind.Battle, token =>
            {
                var n = Interlocked.Increment(ref battleAttempts);
                if (n == 1)
                {
                    return TcpTestTransport.ConnectAsync(battleServer.Port, token);
                }
                throw new NetworkException("战斗拨号失败");
            })
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

            // 先踢战斗：战斗重连拨号持续失败 → 保持 Reconnecting（确定性未就绪）。
            var battleView = client.Channel(ChannelKind.Battle);
            try
            {
                await battleView.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None);
            }
            catch (NetworkException)
            {
                // 预期：kick 触发断连，in-flight 失败。
            }
            var battleDeadline = DateTime.UtcNow.AddSeconds(2);
            while (client.Channel(ChannelKind.Battle).State == ClientState.Connected
                && DateTime.UtcNow < battleDeadline)
            {
                await Task.Delay(10); // 等战斗进入 Reconnecting。
            }

            // 再踢业务：业务重连成功 → settle 查战斗 State（Reconnecting≠Connected）
            // → 链式跳过战斗钩子（确定性：战斗拨号持续失败，业务 settle 时战斗必未恢复）。
            await Assert.ThrowsAsync<NetworkException>(
                () => client.Channel(ChannelKind.Business).InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            var deadline = DateTime.UtcNow.AddSeconds(3);
            while (Volatile.Read(ref businessHooks) < 1 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
            Assert.Equal(1, Volatile.Read(ref businessHooks));
            // skip 确定性断言：战斗未 Connected → 业务钩子不链式调战斗钩子。
            Assert.Equal(0, Volatile.Read(ref battleHooks));
            Assert.Equal(ClientState.Connected, client.Channel(ChannelKind.Business).State);
            Assert.Equal(ClientState.Reconnecting, client.Channel(ChannelKind.Battle).State);
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

    // 拨号失败回滚：战斗通道拨号失败 → ConnectAsync 整体失败，已连业务通道被回滚
    // 关闭（对齐 Go TestDialDualRollbackOnFailure；评审 M4-5 P2——此前无 C# 覆盖）。
    [Fact]
    public async Task ConnectAsync_BattleDialFails_RollsBackBusinessChannel()
    {
        await using var businessServer = new FakeServer();

        var client = new AtlasClient(
            new ChannelConfig(ChannelKind.Business, token => TcpTestTransport.ConnectAsync(businessServer.Port, token))
            {
                Options = FastOptions(),
            },
            new ChannelConfig(ChannelKind.Battle, _ => throw new NetworkException("战斗拨号失败"))
            {
                Options = FastOptions(),
            });

        await using (client)
        {
            await Assert.ThrowsAsync<NetworkException>(() => client.ConnectAsync(CancellationToken.None));

            // 回滚后：业务通道已关闭（Disconnected），Invoke 立即失败。
            Assert.Equal(ClientState.Disconnected, client.Channel(ChannelKind.Business).State);
            await Assert.ThrowsAsync<NetworkException>(
                () => client.Channel(ChannelKind.Business).InvokeRawAsync(
                    "echo", Array.Empty<byte>(), CancellationToken.None));
        }
    }
}
