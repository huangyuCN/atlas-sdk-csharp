using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Client;

// ReconnectZombieTest 复现 M4-4 评审 P1（重连丢失竞态/zombie 僵死）：
// 刚安装的新代读循环在 settle（CompleteReconnect）前死亡时，ReadLoopAsync 的
// shouldReconnect 因 _reconnecting==true 被吞掉（仅 finally 清除）；若 FailGeneration
// 先置 Disconnected + _transport=null，而 CompleteReconnect 只查 _isClosed 仍
// SetState(Connected)，则通道进入「Connected 但无连接/读循环/重连循环」的永久僵死。
//
// 修复方向：当前代读循环退出时记录 _generationFault；settle 成功后、置 Connected 前
// 核对代存活——代已死（网络类）则继续退避重连、不置 Connected；协议致命则终止
// 不再重拨（对齐 Go supervisor 的 g.done 复查 + 协议致命 terminate 语义）。
public sealed class ReconnectZombieTest
{
    private static ChannelOptions FastOptions(ChannelOptions? seed = null)
    {
        var options = seed ?? new ChannelOptions();
        options.InvokeTimeoutMs = 500;
        options.BackoffBaseMs = 10;
        options.BackoffMaxMs = 30;
        options.QueueSize = 8;
        options.HookTimeoutMs = 500;
        return options;
    }

    private static async Task<Channel> StartChannelAsync(
        Func<CancellationToken, Task<ITransport>> dial,
        ChannelOptions options)
    {
        var channel = new Channel(dial, options);
        await channel.ConnectAsync(CancellationToken.None);
        return channel;
    }

    private static async Task WaitForStateAsync(Channel channel, ClientState state)
    {
        for (var attempt = 0; attempt < 300; attempt++)
        {
            if (channel.State == state)
            {
                return;
            }
            await Task.Delay(10);
        }
        Assert.Fail($"等待状态 {state} 超时：当前 {channel.State}");
    }

    private static async Task WaitForConnectionsAsync(FakeServer server, int count)
    {
        for (var attempt = 0; attempt < 300; attempt++)
        {
            if (server.AcceptedConnections >= count)
            {
                return;
            }
            await Task.Delay(10);
        }
        Assert.Fail($"等待服务端连接数 {count} 超时：当前 {server.AcceptedConnections}");
    }

    // P1 主场景：无钩子重连时，新代读循环在 CompleteReconnect 前死亡（网络错误）。
    // 修复前：CompleteReconnect 仍置 Connected → zombie（Connected 但 Invoke 失败）。
    // 修复后：置 Connected 前核对代存活 → 代已死则继续退避重连 → 最终真 Connected。
    [Fact]
    public async Task Reconnect_NewGenerationDiesBeforeComplete_EventuallyConnects()
    {
        await using var server = new FakeServer();
        var options = FastOptions();
        options.HeartbeatIntervalMs = 0; // 禁用传输心跳：聚焦重连编排，避免干扰。
        var dialCount = 0;
        var channel = await StartChannelAsync(async token =>
        {
            var n = Interlocked.Increment(ref dialCount);
            if (n == 2)
            {
                // 第二次拨号（首次重连）返回连接后立即 EOF 的 transport：
                // 新代读循环启动即死，FailGeneration 置 Disconnected。
                return new ImmediateEofTransport();
            }
            return await TcpTestTransport.ConnectAsync(server.Port, token);
        }, options);
        await using (channel)
        {
            // 首连往返建立。
            var first = await channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);
            Assert.Equal(new byte[] { 1 }, first);

            // kick 触发重连；重连拨号（第 2 次）的新代立即死。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // 修复后应继续退避重连：第 3 次拨号连上 FakeServer → 真 Connected。
            await WaitForConnectionsAsync(server, 2);
            Assert.True(dialCount >= 3, $"应重拨 ≥3 次（首连 + 死代 + 恢复），实际 {dialCount}");

            // 非 zombie：Connected 且新连接上 Invoke 成功。
            await WaitForStateAsync(channel, ClientState.Connected);
            var response = await channel.InvokeRawAsync("echo", new byte[] { 2 }, CancellationToken.None);
            Assert.Equal(new byte[] { 2 }, response);
        }
    }

    // P1 派生影响：settle 钩子窗口内新代读循环遇协议致命（ProtocolException）——
    // 修复前 RunReconnectLoopAsync 会经 Abandon→Redial 无限重拨（Go 语义是协议致命
    // terminate 不重连）。修复后：钩子失败时若当前代因协议致命而死 → 终止不重拨。
    [Fact]
    public async Task Reconnect_ProtocolFatalDuringHookSettle_DoesNotRedialForever()
    {
        await using var server = new FakeServer();
        var options = FastOptions();
        options.HeartbeatIntervalMs = 0;
        var dialCount = 0;
        var channel = await StartChannelAsync(
            token => { Interlocked.Increment(ref dialCount); return TcpTestTransport.ConnectAsync(server.Port, token); },
            options);
        await using (channel)
        {
            var hookRan = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            // 钩子内 invoke bad-version：FakeServer 对该 op 回 Version2 响应 →
            // 客户端读循环判版本不匹配抛 ProtocolException（协议致命）。
            channel.OnRelogin = async () =>
            {
                hookRan.TrySetResult(true);
                await channel.InvokeRawAsync("bad-version", Array.Empty<byte>(), CancellationToken.None);
            };

            await channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // settle 钩子遇协议致命 → 终止（Disconnected），不再重拨。
            await hookRan.Task.WaitAsync(TimeSpan.FromSeconds(2));
            await WaitForStateAsync(channel, ClientState.Disconnected);

            // 拨号次数应稳定（首连 1 + 重连 1 = 2），不再无限增长。
            var dialsAtDeath = dialCount;
            await Task.Delay(120); // 给足时间暴露无限重拨。
            Assert.Equal(dialsAtDeath, dialCount);
        }
    }

    // ImmediateEofTransport 模拟「连接建立后立即断开」：读帧即抛 EOF（网络错误路径）。
    private sealed class ImmediateEofTransport : ITransport
    {
        public ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(int maxBodySize, CancellationToken token)
        {
            throw new EndOfStreamException("模拟连接立即断开");
        }

        public Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public Task CloseAsync()
        {
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
