using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Client;

// HookBypassTest 验证重连成功后的会话钩子同步执行语义（对齐 Go
// settleGeneration/runHookSync/triggerReloginHook 的 hookBypass 直通窗口）：
//   - 钩子执行期间本通道保持 Reconnecting，钩子的重登 Invoke 直通当前代连接
//     （不排队、不因 Reconnecting 失败）；
//   - 钩子失败（返回错误/超时）弃用本代连接、保留排队请求、退避重连后重试；
//   - 钩子成功后才置 Connected 并 drain 排队队列（FIFO）。
public sealed class HookBypassTest
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

    // 重连钩子执行期间（hookBypass），钩子内 Invoke（重登）直通当前代连接成功：
    // 重连成功后 OnRelogin 同步执行，其重登请求不排队、不因 Reconnecting 失败。
    [Fact]
    public async Task Hook_InvokeDuringHookBypass_DirectToCurrentConnection()
    {
        await using var server = new FakeServer();
        var options = FastOptions();
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);
        await using (channel)
        {
            var hookRan = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var hookResult = new TaskCompletionSource<byte[]>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            channel.OnRelogin = async () =>
            {
                hookRan.TrySetResult(true);
                // 钩子内重登 Invoke：hookBypass 窗口应直通当前代连接成功。
                var reply = await channel.InvokeRawAsync("echo", new byte[] { 42 }, CancellationToken.None);
                hookResult.TrySetResult(reply);
            };

            // 踢线触发重连；重连成功进入钩子。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            Assert.True(await hookRan.Task.WaitAsync(TimeSpan.FromSeconds(2)),
                "重连成功后应执行 OnRelogin 钩子");
            Assert.Equal(new byte[] { 42 }, await hookResult.Task.WaitAsync(TimeSpan.FromSeconds(2)));
            Assert.Equal(ClientState.Connected, channel.State);
        }
    }

    // 钩子失败（重登业务被拒）→ 弃用本代连接、继续退避重连、重试钩子直至成功；
    // 排队请求在钩子成功后 drain。
    [Fact]
    public async Task Hook_Failure_ReconnectsAndRetriesUntilSuccess()
    {
        await using var server = new FakeServer();
        var options = FastOptions();
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);
        await using (channel)
        {
            var attempts = 0;
            channel.OnRelogin = () =>
            {
                attempts++;
                if (attempts < 2)
                {
                    // 前两次失败：弃用连接、退避重连后重试。
                    throw new BusinessException(401, "TOKEN_EXPIRED", "重登失败", null);
                }
                return Task.CompletedTask;
            };

            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // 排队一个请求：钩子最终成功后被 drain。
            var queued = channel.InvokeRawAsync("echo", new byte[] { 7 }, CancellationToken.None);

            // 等待重连 + 钩子重试完成（最多 ~2s）。
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (attempts < 2 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }
            Assert.True(attempts >= 2, $"钩子应重试至少 2 次（实际 {attempts}）");
            Assert.Equal(ClientState.Connected, channel.State);
            Assert.Equal(new byte[] { 7 }, await queued.WaitAsync(TimeSpan.FromSeconds(1)));
        }
    }

    // 钩子超时（hookTimeout 上限）视为失败：弃用连接、请求保留排队、退避重连重试。
    [Fact]
    public async Task Hook_Timeout_AbandonsConnectionAndRetries()
    {
        await using var server = new FakeServer();
        var options = FastOptions();
        options.HookTimeoutMs = 80;
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);
        await using (channel)
        {
            var attempts = 0;
            var block = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            channel.OnRelogin = () =>
            {
                attempts++;
                if (attempts == 1)
                {
                    return block.Task; // 第一次挂起直至超时。
                }
                return Task.CompletedTask;
            };

            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));

            // 第一次钩子超时后应弃用连接并重试（attempts>=2）。
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (attempts < 2 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }
            Assert.True(attempts >= 2, $"钩子超时后应重试（实际 {attempts} 次）");
            Assert.Equal(ClientState.Connected, channel.State);
        }
    }

    // Close 可打断挂起的重连钩子（对齐 Go runHookSync 的 closeCh 分支）：钩子永久
    // 挂起时 CloseAsync 应立即返回，不等待 hookTimeout 期满。
    [Fact]
    public async Task Close_InterruptsHangingHook_ReturnsPromptly()
    {
        await using var server = new FakeServer();
        var options = FastOptions();
        options.HookTimeoutMs = 60_000; // 长超时：若 Close 不打断将等满 60s。
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);

        var hookEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var neverCompletes = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        channel.OnRelogin = () =>
        {
            hookEntered.TrySetResult(true);
            return neverCompletes.Task; // 永久挂起钩子。
        };

        // 踢线触发重连；重连成功后进入挂起钩子。
        await Assert.ThrowsAsync<NetworkException>(
            () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));
        await hookEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // Close 应立即返回（不被 60s 钩子超时阻塞）。
        var closeTask = channel.CloseAsync();
        var completed = await Task.WhenAny(closeTask, Task.Delay(TimeSpan.FromSeconds(2)));
        Assert.Same(closeTask, completed);
        await closeTask;
        Assert.Equal(ClientState.Disconnected, channel.State);
    }

    // 会话心跳业务错误触发重登钩子后，钩子未返回期间再次业务错误不重复触发
    //（CAS 单飞：Interlocked.CompareExchange on _sessionHookBusy——上一轮触发未完成
    // 则跳过）。本用例挂起的是会话心跳路径的异步重登钩子（RunReloginHookAsync），
    // 非重连 settle 的同步钩子窗口（hookBypass）——见下一用例的区分。
    [Fact]
    public async Task SessionHeartbeat_BusinessError_WhileReloginInFlight_NoDuplicate()
    {
        await using var server = new FakeServer();
        server.BusinessErrorOps.Add("session-heartbeat");
        var options = FastOptions();
        options.SessionHeartbeatIntervalMs = 10;
        options.SessionHeartbeatOpFactory = () => new SessionHeartbeatRequest("session-heartbeat", new byte[] { 1 });
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);
        await using (channel)
        {
            var reloginCalls = 0;
            var reloginStarted = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            channel.OnRelogin = () =>
            {
                reloginCalls++;
                reloginStarted.TrySetResult(true);
                return release.Task; // 挂起重登钩子：CAS 单飞窗口。
            };

            // 触发首次会话心跳重登（业务错误路径）。FakeServer 对 session-heartbeat 回业务拒绝。
            await reloginStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
            var callsAfterFirst = reloginCalls;

            // 挂起期间（重登钩子未返回）再触发几轮会话心跳——重登应被 CAS 单飞跳过。
            await Task.Delay(60);
            Assert.Equal(callsAfterFirst, reloginCalls); // 挂起期间不重复触发。

            release.TrySetResult(true);
        }
    }

    // 会话心跳业务错误在「重连 settle 的同步钩子窗口」（hookBypass=true）期间跳过
    // 重登触发（对齐 Go triggerReloginHook 的 hookBypass 检查）——避免与重连钩子
    // 并发重登造成 Login 竞态。真实构造：kick 断连 → 重连 settle 进入 OnRelogin
    // 挂起（hookBypass 置位）→ 新代会话心跳收到业务拒绝 → 应跳过（不重复调用
    // OnRelogin）。区别于上一用例：上一例挂起的是会话心跳触发的异步钩子，本例
    // 挂起的是重连 settle 同步钩子（hookBypass 窗口）。
    [Fact]
    public async Task SessionHeartbeat_BusinessError_DuringReconnectHookBypass_SkipsTrigger()
    {
        await using var server = new FakeServer();
        var options = FastOptions();
        options.SessionHeartbeatIntervalMs = 10;
        options.SessionHeartbeatOpFactory = () => new SessionHeartbeatRequest("session-heartbeat", new byte[] { 1 });
        var channel = await StartChannelAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);
        await using (channel)
        {
            var reloginCalls = 0;
            var reloginStarted = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            channel.OnRelogin = () =>
            {
                reloginCalls++;
                if (reloginCalls == 1)
                {
                    reloginStarted.TrySetResult(true);
                    return release.Task; // 首次 = 重连 settle 钩子：挂起以持住 hookBypass 窗口。
                }
                return Task.CompletedTask;
            };

            // 首连往返正常（会话心跳回显不触发钩子）。
            await channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);

            // kick 断连触发重连；断连后才让服务端对 session-heartbeat 回业务拒绝
            //（旧代已随断连取消，只有重连后新代会话心跳受影响）。
            await Assert.ThrowsAsync<NetworkException>(
                () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));
            server.BusinessErrorOps.Add("session-heartbeat");

            // 重连 settle 进入 OnRelogin（hookBypass 窗口）并挂起。
            await reloginStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

            // 等新代会话心跳在窗口内至少跑一轮（业务拒绝已生效）——若 TriggerReloginHook
            // 的 hookBypass 跳过失效，reloginCalls 将变 2（RunReloginHookAsync 再触发）。
            var heartbeatSeen = 0;
            for (var attempt = 0; attempt < 200; attempt++)
            {
                foreach (var op in server.OperationNames)
                {
                    if (op == "session-heartbeat")
                    {
                        heartbeatSeen++;
                    }
                }
                if (heartbeatSeen >= 1)
                {
                    break;
                }
                await Task.Delay(10);
            }
            Assert.True(heartbeatSeen >= 1, "新代会话心跳未在 hookBypass 窗口内运行");

            // 钩子挂起期间会话心跳业务拒绝被 hookBypass 跳过：仅 1 次（settle 那次）。
            await Task.Delay(50);
            Assert.Equal(1, reloginCalls);

            release.TrySetResult(true);
            // 释放后重连完成：Connected。
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (channel.State != ClientState.Connected && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
            Assert.Equal(ClientState.Connected, channel.State);
        }
    }
}
