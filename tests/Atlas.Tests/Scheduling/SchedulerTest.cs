using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Scheduling;
using Atlas.Tests.Client;
using Atlas.Unity;
using Xunit;

namespace Atlas.Tests.Scheduling;

// 捕获被 Post 调度的回调，模拟 Unity 主线程同步上下文（真实 Unity 上下文把
// 回调排队到主线程循环执行；测试用同步执行模拟"发布到指定上下文"语义）。
internal sealed class CapturingContext : SynchronizationContext
{
    private readonly ConcurrentQueue<Action> _posted = new();

    public override void Post(SendOrPostCallback d, object? state)
    {
        _posted.Enqueue(() => d(state));
    }

    // Drain 同步执行全部已发布回调（模拟 Unity 主线程逐帧处理队列）。
    public int Drain()
    {
        var count = 0;
        while (_posted.TryDequeue(out var action))
        {
            action();
            count++;
        }
        return count;
    }

    public int PendingCount => _posted.Count;
}

// AtlasScheduler 与 AtlasUnity 桥的调度注入验证（对齐设计文档 §6.2：
// handler 默认线程池、注入 SynchronizationContext 后在注入上下文执行）。
public sealed class SchedulerTest
{
    // 默认调度走线程池：回调不在调用线程同步执行。
    [Fact]
    public void Post_WithoutScheduler_RunsOnThreadPool()
    {
        try
        {
            AtlasScheduler.SetScheduler(null);
            var called = new ManualResetEventSlim(false);
            Thread? callbackThread = null;
            var callerThread = Thread.CurrentThread;
            AtlasScheduler.Post(() =>
            {
                callbackThread = Thread.CurrentThread;
                called.Set();
            });
            Assert.True(called.Wait(TimeSpan.FromSeconds(2)), "默认调度应在线程池执行回调");
            Assert.NotNull(callbackThread);
            Assert.NotSame(callerThread, callbackThread);
        }
        finally
        {
            AtlasScheduler.SetScheduler(null);
        }
    }

    // 注入 SynchronizationContext 后，回调经 Post 发布到注入上下文（不在调用线程同步跑）。
    [Fact]
    public void Post_WithInjectedContext_PostsToContext()
    {
        try
        {
            var context = new CapturingContext();
            AtlasScheduler.SetScheduler(context);
            var called = false;
            AtlasScheduler.Post(() => called = true);
            // 注入后回调不应立即同步执行，而是排队到注入上下文。
            Assert.False(called, "注入上下文后回调应排队而非同步执行");
            Assert.Equal(1, context.Drain());
            Assert.True(called);
        }
        finally
        {
            AtlasScheduler.SetScheduler(null);
        }
    }

    // 注入 null 恢复默认线程池（多次切换安全）。
    [Fact]
    public void SetScheduler_Null_RestoresDefault()
    {
        try
        {
            AtlasScheduler.SetScheduler(new CapturingContext());
            AtlasScheduler.SetScheduler(null);
            var called = new ManualResetEventSlim(false);
            AtlasScheduler.Post(() => called.Set());
            Assert.True(called.Wait(TimeSpan.FromSeconds(2)), "恢复默认后应在线程池执行");
        }
        finally
        {
            AtlasScheduler.SetScheduler(null);
        }
    }

    // AtlasUnity 桥转发到 AtlasScheduler：经桥注入后 Post 走注入上下文。
    [Fact]
    public void AtlasUnity_SetMainThreadScheduler_ForwardsToAtlasScheduler()
    {
        try
        {
            var context = new CapturingContext();
            AtlasUnity.SetMainThreadScheduler(context);
            var called = false;
            AtlasScheduler.Post(() => called = true);
            Assert.False(called);
            Assert.Equal(1, context.Drain());
            Assert.True(called);
        }
        finally
        {
            AtlasUnity.SetMainThreadScheduler(null);
        }
    }

    // 集成：Notify handler 在注入调度器后经注入上下文发布（readLoop 收 Notify →
    // handler 排队到注入上下文而非线程池直接跑）。
    [Fact]
    public async Task NotifyHandler_WithInjectedContext_DispatchesThroughContext()
    {
        try
        {
            var context = new CapturingContext();
            AtlasScheduler.SetScheduler(context);
            await using var server = new FakeServer();
            await using var channel = new Channel(
                token => TcpTestTransport.ConnectAsync(server.Port, token),
                new ChannelOptions());
            await channel.ConnectAsync(CancellationToken.None);
            string? receivedOp = null;
            using var sub = channel.On("notify-op", (op, _) => receivedOp = op);

            // 服务端推一条 Notify 帧。
            await server.PushNotifyAsync("notify-op", Array.Empty<byte>());

            // 等 Notify 到达 handler 执行点（经注入上下文排队——不直接同步跑）。
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (context.PendingCount == 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
            Assert.True(context.PendingCount > 0, "Notify handler 应排队到注入上下文");
            Assert.Null(receivedOp); // 尚未 Drain——handler 未执行。
            Assert.True(context.Drain() > 0);
            Assert.Equal("notify-op", receivedOp);
        }
        finally
        {
            AtlasScheduler.SetScheduler(null);
        }
    }
}
