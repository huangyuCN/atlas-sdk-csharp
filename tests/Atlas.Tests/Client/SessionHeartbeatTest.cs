using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;
using AtlasTimeoutException = Atlas.Errors.TimeoutException;

namespace Atlas.Tests.Client;

// SessionHeartbeatTest 验证会话心跳（业务续租）语义（规范 §5.2 业务层心跳 +
// 对标 Go session_heartbeat_test.go）：
//   1. 按周期在通道发起业务 Heartbeat（op/payload 由 opFactory 提供）；
//   2. 业务错误（如会话过期）触发重登钩子（CAS 单飞：并发触发只调一次）；
//   3. 网络类失败静默（不触发重登——连接死亡由重连机制与传输心跳负责）；
//   4. opFactory 未就绪（空 operation）跳过本轮；
//   5. 会话心跳 interval≤0 即关闭（不调度）。
// 注意：dual 形态的「会话心跳仅业务通道」门控在 Client/通道角色层
//（对标 Go 的 KindBusiness 判定），C# 的 dual 编排在 M4-5 交付——届时补门控用例。
public sealed class SessionHeartbeatTest
{
    // 会话心跳业务 op（服务端命中 BusinessErrorOps 时回业务拒绝）。
    private const string SessionOp = "/gateway.v1.GatewayAuth/Heartbeat";
    private const string SessionOpBoom = "/gateway.v1.GatewayAuth/Heartbeat-boom";

    // 快速参数：关闭传输心跳隔离观测，短会话心跳周期/超时让测试快速收敛。
    private static ChannelOptions FastOptions(Action<ChannelOptions>? tune = null)
    {
        var options = new ChannelOptions
        {
            HeartbeatIntervalMs = 0, // 关闭传输心跳，隔离会话心跳观测。
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
    public async Task SessionHeartbeat_InvokesBusinessOp_Periodically()
    {
        // 会话心跳按周期发送业务 Heartbeat（payload 由工厂提供）。
        await using var server = new FakeServer();
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o => ConfigureSession(o, SessionOp, new byte[] { 1, 2, 3 })));
        await using (channel)
        {
            await WaitUntilAsync(
                () => CountOperation(server, SessionOp) >= 2,
                TimeSpan.FromSeconds(2));
        }
    }

    [Fact]
    public async Task SessionHeartbeat_BusinessError_TriggersReloginHook()
    {
        // 会话心跳收到业务拒绝（模拟会话过期）→ 触发重登钩子。
        await using var server = new FakeServer();
        var reloginCount = 0;
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o =>
            {
                o.InvokeTimeoutMs = 500;
                ConfigureSession(o, SessionOpBoom, new byte[] { 1 });
            }));
        channel.OnRelogin = () =>
        {
            Interlocked.Increment(ref reloginCount);
            return Task.CompletedTask;
        };
        server.BusinessErrorOps.Add(SessionOpBoom);
        await using (channel)
        {
            await WaitUntilAsync(() => Volatile.Read(ref reloginCount) >= 1, TimeSpan.FromSeconds(3));
        }
    }

    [Fact]
    public async Task SessionHeartbeat_ConcurrentBusinessErrors_ReloginSingleFlight()
    {
        // CAS 单飞：业务错误连续触发（多轮会话心跳）时，上一轮钩子未返回则跳过
        //（对齐 Go triggerReloginHook 的 sessionHookBusy CAS）——并发触发只调一次。
        await using var server = new FakeServer();
        var reloginCount = 0;
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o =>
            {
                o.InvokeTimeoutMs = 500;
                ConfigureSession(o, SessionOpBoom, new byte[] { 1 });
            }));
        // 钩子未返回（永久等待 release）——期间多次业务错误只应触发一次。
        var release = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        channel.OnRelogin = async () =>
        {
            Interlocked.Increment(ref reloginCount);
            Interlocked.Increment(ref entered);
            await release.Task; // 模拟重登进行中（未返回）。
        };
        server.BusinessErrorOps.Add(SessionOpBoom);
        await using (channel)
        {
            await WaitUntilAsync(() => Volatile.Read(ref entered) >= 1, TimeSpan.FromSeconds(3));
            // 等数轮业务错误（钩子未返回——单飞应跳过后续触发）。
            await Task.Delay(300);
            Assert.Equal(1, Volatile.Read(ref reloginCount));
            release.TrySetResult(true);
        }
    }

    [Fact]
    public async Task SessionHeartbeat_NetworkError_Silent_NoRelogin()
    {
        // 网络类失败（超时——服务端挂起不响应）静默：不触发重登钩子
        //（连接死亡由重连机制与传输心跳负责；对齐 Go NetworkErrorSilent）。
        await using var server = new FakeServer();
        var reloginCount = 0;
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o =>
            {
                o.InvokeTimeoutMs = 30;
                ConfigureSession(o, "hold", new byte[] { 1 }); // hold：服务端挂起不响应。
            }));
        channel.OnRelogin = () =>
        {
            Interlocked.Increment(ref reloginCount);
            return Task.CompletedTask;
        };
        await using (channel)
        {
            // hold op：服务端等待放行信号不回包 → 会话心跳 invoke 超时（网络类失败）。
            await Task.Delay(400);
            Assert.Equal(0, Volatile.Read(ref reloginCount));
        }
    }

    [Fact]
    public async Task SessionHeartbeat_SkipsEmptyOp_WhenFactoryNotReady()
    {
        // opFactory 返回空 operation（业务侧未登录无 token）：跳过本轮不发请求。
        await using var server = new FakeServer();
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions(o =>
            {
                ConfigureSession(o, "", new byte[] { 1 }); // 空 op = 未就绪。
            }));
        await using (channel)
        {
            await Task.Delay(200);
            Assert.Empty(server.OperationNames); // 未发任何请求。
        }
    }

    [Fact]
    public async Task SessionHeartbeat_DisabledWhenZeroInterval()
    {
        // interval≤0 = 关闭会话心跳（默认 ChannelOptions 即关闭）。
        await using var server = new FakeServer();
        var channel = await StartAsync(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            FastOptions()); // 不配置会话心跳。
        await using (channel)
        {
            await Task.Delay(200);
            Assert.Empty(server.OperationNames);
        }
    }

    // 配置会话心跳：interval=20ms + opFactory（返回固定 op 与 payload）。
    private static void ConfigureSession(ChannelOptions options, string op, byte[] payload)
    {
        options.SessionHeartbeatIntervalMs = 20;
        options.SessionHeartbeatOpFactory = () => new SessionHeartbeatRequest(op, payload);
    }

    private static int CountOperation(FakeServer server, string op)
    {
        var count = 0;
        foreach (var name in server.OperationNames)
        {
            if (name == op)
            {
                count++;
            }
        }
        return count;
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
