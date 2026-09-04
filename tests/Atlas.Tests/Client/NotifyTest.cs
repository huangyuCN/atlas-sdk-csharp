using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Frame;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Client;

// NotifyTest 覆盖 Notify 订阅分发与传输心跳 Ping（对标 Go notify_test/heartbeat 语义）。
public sealed class NotifyTest
{
    [Fact]
    public async Task On_ReceivesPushedNotify_WithOpAndPayload()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);
        var received = NewSignal();

        channel.On("match.found", (op, payload) =>
        {
            Assert.Equal("match.found", op);
            Assert.Equal("room-1", Encoding.UTF8.GetString(payload));
            received.TrySetResult(true);
        });

        await server.PushNotifyAsync("match.found", Encoding.UTF8.GetBytes("room-1"));

        await received.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task On_OtherOperation_NotDelivered()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);
        var otherReceived = NewSignal();
        var ownReceived = NewSignal();

        channel.On("match.found", (_, _) => ownReceived.TrySetResult(true));
        channel.On("other.op", (_, _) => otherReceived.TrySetResult(true));

        // 推其他 op：订阅 match.found 的 handler 不应触发。
        await server.PushNotifyAsync("other.op", Encoding.UTF8.GetBytes("x"));
        await Task.Delay(50);
        Assert.False(ownReceived.Task.IsCompleted);
        Assert.True(otherReceived.Task.IsCompleted);

        // 再推本 op：确认 match.found 订阅仍能收到（按 op 分发，非时序泄漏）。
        ownReceived = NewSignal();
        await server.PushNotifyAsync("match.found", Encoding.UTF8.GetBytes("room-1"));
        await ownReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task DisposeSubscription_StopsDelivery()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);
        var received = NewSignal();
        var count = 0;

        var subscription = channel.On("match.found", (_, _) =>
        {
            Interlocked.Increment(ref count);
            received.TrySetResult(true);
        });

        await server.PushNotifyAsync("match.found", Encoding.UTF8.GetBytes("first"));
        await received.Task.WaitAsync(TimeSpan.FromSeconds(2));

        subscription.Dispose();
        received = NewSignal();
        await server.PushNotifyAsync("match.found", Encoding.UTF8.GetBytes("second"));
        await Task.Delay(100);

        Assert.Equal(1, count);
        Assert.False(received.Task.IsCompleted);
    }

    [Fact]
    public async Task On_SameHandlerRegisteredTwice_DeliversOnce()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);
        var count = 0;

        NotifyHandler handler = (_, _) => Interlocked.Increment(ref count);
        var first = channel.On("match.found", handler);
        var second = channel.On("match.found", handler);

        await server.PushNotifyAsync("match.found", Array.Empty<byte>());
        await Task.Delay(200);

        Assert.Equal(1, count);
        // 退订一个不影响的剩余订阅（同 handler 幂等只占一份）。
        first.Dispose();
        second.Dispose();
    }

    [Fact]
    public async Task HandlerThrows_DoesNotAffectOtherHandlers()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);
        var goodReceived = NewSignal();

        channel.On("match.found", (_, _) => throw new InvalidOperationException("handler 崩溃"));
        channel.On("match.found", (_, _) => goodReceived.TrySetResult(true));

        await server.PushNotifyAsync("match.found", Array.Empty<byte>());

        await goodReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task NotifyFrame_DoesNotBreakRequestResponseMatching()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);
        var received = NewSignal();

        channel.On("match.found", (_, _) => received.TrySetResult(true));

        // 推 Notify 后普通 Invoke 仍正常（Notify 不参与请求匹配）。
        var echo = channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);
        await server.PushNotifyAsync("match.found", Array.Empty<byte>());

        Assert.Equal(new byte[] { 1 }, await echo);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task Heartbeat_SendsPingsPeriodically_WhenConnected()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500, heartbeatIntervalMs: 30);

        // 等若干心跳周期到达：30ms 周期 × 4 = 120ms 内应至少收到 2 次 Ping。
        await WaitUntilAsync(() => server.PingCount >= 2, TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task Heartbeat_DisabledWhenIntervalNonPositive()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500, heartbeatIntervalMs: 0);

        await Task.Delay(150);
        Assert.Equal(0, server.PingCount);
    }

    private static async Task<Channel> ConnectAsync(
        FakeServer server,
        int timeoutMs,
        int? heartbeatIntervalMs = null)
    {
        var options = new ChannelOptions
        {
            InvokeTimeoutMs = timeoutMs,
            HeartbeatIntervalMs = heartbeatIntervalMs ?? 0, // 默认关心跳，测试按需开
        };
        var channel = new Channel(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            options);
        await channel.ConnectAsync(CancellationToken.None);
        return channel;
    }

    private static TaskCompletionSource<bool> NewSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
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
        Assert.True(condition(), $"等待条件超时（{timeout}）");
    }
}
