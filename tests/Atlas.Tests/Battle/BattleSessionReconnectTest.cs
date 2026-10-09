using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionReconnectTest 覆盖显式重连的两条口径：
//   ① 并发 ReconnectAsync **单飞**：只重连一轮（不各拨一代、不互相关掉对方装上的通道），
//      完成后会话照常可用；
//   ② 重连钩子的重试**有界**：连续失败到 HookMaxAttempts 即终止重连并上报 Failed
//      （不再无限退避重拨）。
public sealed class BattleSessionReconnectTest
{
    // Ticket 是测试票据密文（非真实 AEAD 票：接入层/帧面只按字节透传与编码）。
    private static readonly byte[] Ticket = { 0xfb, 0xff, 0x00, 0x01, 0xfe, 0x10, 0x20 };

    private static DirectPlan Plan(int port)
    {
        var encoded = Convert.ToBase64String(Ticket);
        return DirectPlan.FromNotify("{\"match_id\":\"m-1\",\"battle_id\":\"b-1\",\"battle_ticket\":\""
            + encoded + "\",\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_UDP\",\"address\":\"127.0.0.1:"
            + port + "\"}]}");
    }

    private static BattleSessionOptions Options()
    {
        return new BattleSessionOptions
        {
            PreferredTransport = EdgeTransport.Udp,
            HelloTimeoutMs = 3000,
            InvokeTimeoutMs = 3000,
            BackoffBaseMs = 30,
            BackoffMaxMs = 100,
            HeartbeatInterval = TimeSpan.Zero,
        };
    }

    // WaitUntilAsync 轮询等待条件成立（到点返回 false；重连是异步编排，不接受固定 sleep 赌时序）。
    private static async Task<bool> WaitUntilAsync(Func<bool> condition, int timeoutMs = 3000)
    {
        var watch = Stopwatch.StartNew();
        while (watch.ElapsedMilliseconds < timeoutMs)
        {
            if (condition())
            {
                return true;
            }
            await Task.Delay(10);
        }
        return condition();
    }

    // ① 两次并发 ReconnectAsync → 单飞成一轮：共 2 次拨号（首连 + 本轮），会话可用。
    [Fact]
    public async Task ConcurrentReconnect_CoalescesIntoOneRound_AndSessionStaysUsable()
    {
        await using var server = new BattleEdgeUdpServer();
        await using var session = BattleSession.Create(Plan(server.Port), Options());
        await session.ConnectAsync();
        await session.JoinBattleAsync();
        Assert.Equal(1, server.HelloCount);

        session.LastSeenFrame = 5;
        var first = session.ReconnectAsync();
        var second = session.ReconnectAsync();
        await Task.WhenAll(first, second);

        // 单飞：只重连一轮（两次调用共享同一轮），不是各拨一代。
        Assert.Equal(2, server.HelloCount);
        Assert.Equal(2, server.RequestsOf(BattleOps.JoinBattle).Length);
        Assert.Single(server.RequestsOf(BattleOps.SyncFrames));
        Assert.Equal(ClientState.Connected, session.State);

        // 会话仍可用：重连后的发帧走新一轮的连接（不是被并发调用关掉的旧代）。
        var reply = await session.SendFrameInputAsync(1, new byte[] { 0x01 });
        Assert.Equal(BattleEdgeDefaults.ReplyJson(BattleOps.SendFrameInput), Encoding.UTF8.GetString(reply));
        Assert.Single(server.RequestsOf(BattleOps.SendFrameInput));
    }

    // ② 重连钩子连续失败（新一代入局无回执，超时）→ 到 HookMaxAttempts 即终止并上报 Failed。
    [Fact]
    public async Task Reconnect_HookRetryExhausted_ReportsFailed_AndStopsDialing()
    {
        var first = new BattleFakeTransport();
        var dials = 0;
        var options = Options();
        options.HookMaxAttempts = 3;
        options.InvokeTimeoutMs = 150; // 每轮入局很快超时：3 轮内走完上限。
        options.BackoffBaseMs = 20;
        var session = BattleSession.Create(Plan(7102), options);
        // 后续每一代都不回执（入局超时）：钩子失败 → 弃用本代 → 退避重拨 → 再失败。
        session.DialOverride = _ =>
        {
            var next = Interlocked.Increment(ref dials);
            return Task.FromResult<ITransport>(next == 1 ? first : new BattleFakeTransport());
        };
        var failures = 0;
        session.Failed += _ => Interlocked.Increment(ref failures);
        await session.ConnectAsync();

        await first.CloseAsync(); // 断链 → 自动重连（钩子每轮入局超时）。

        Assert.True(await WaitUntilAsync(() => Volatile.Read(ref failures) == 1), "钩子重试超限应上报 Failed");
        await Task.Delay(150); // 留出「若还在重试」的窗口。
        var attemptsAtFail = Volatile.Read(ref dials);
        await Task.Delay(200);
        Assert.Equal(attemptsAtFail, Volatile.Read(ref dials)); // 终止后不再拨号。
        Assert.True(attemptsAtFail <= 1 + options.HookMaxAttempts,
            $"重试必须有界（HooKMaxAttempts={options.HookMaxAttempts}，实际拨号 {attemptsAtFail}）");
        // 可观测：拨号尝试与钩子失败都有计数（Stats 只读快照）。
        Assert.True(session.Stats.ConnectAttempts >= 2, "拨号尝试应计数");
        Assert.True(session.Stats.RejoinFailures >= 1, "重连钩子失败应计数");
        await session.CloseAsync();
    }
}
