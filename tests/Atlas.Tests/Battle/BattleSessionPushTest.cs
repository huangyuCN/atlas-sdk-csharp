using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionPushTest 覆盖直连战斗会话的**统一推送出口**：帧广播 / 战斗结束 / 玩家出局
// 三类推送都从同一出口（Push 事件 + OnPush 按 op 订阅）可见，载荷与帧头版本原样透传。
//
// 专用事件（FrameBroadcast / BattleEnd / PlayerOut）已随游戏域消息迁出 SDK 一并删除：
// 它们的语义（帧广播 / 结算 / 出局）属游戏协议包，调用方用协议包的 op 常量订阅即可——
// SDK 不再为每条游戏消息造一个公开事件，只保留「op → (payload, version)」这一个出口。
public sealed class BattleSessionPushTest
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
            BackoffBaseMs = 50,
            BackoffMaxMs = 200,
            HeartbeatInterval = TimeSpan.Zero,
        };
    }

    private static BattleSession FakeSession(BattleFakeTransport transport, BattleSessionOptions options)
    {
        var session = BattleSession.Create(Plan(7102), options);
        session.DialOverride = _ => Task.FromResult<ITransport>(transport);
        return session;
    }

    // WaitUntilAsync 轮询等待条件成立（推送分发是异步的，不接受固定 sleep 赌时序）。
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

    // 玩家出局推送：统一出口（Push 事件 + OnPush 订阅）都能收到，载荷原样透传。
    [Fact]
    public async Task PlayerOutNotify_Push_ReachesUnifiedExit()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        var unified = new List<BattlePush>();
        var subscribed = new List<BattlePush>();
        session.Push += push => unified.Add(push);
        using var subscription = session.OnPush(TestOps.PlayerOutNotify, push =>
        {
            lock (subscribed)
            {
                subscribed.Add(push);
            }
        });
        await session.ConnectAsync();

        var payload = Encoding.UTF8.GetBytes(
            "{\"battleId\":\"b-1\",\"playerId\":\"p-2\",\"reason\":\"PLAYER_OUT_REASON_OFFLINE_TIMEOUT\"}");
        transport.QueueNotify(TestOps.PlayerOutNotify, payload);
        Assert.True(await WaitUntilAsync(() => Count(unified) == 1), "玩家出局应经统一出口分发");

        // 出局通知不是终态信号：多人局其余玩家继续（不误停发）；SDK 也不解其载荷。
        Assert.False(session.HasEnded);
        Assert.Equal(TestOps.PlayerOutNotify, unified[0].Op);
        Assert.Equal(payload, unified[0].Payload);
        Assert.True(await WaitUntilAsync(() => Count(subscribed) == 1));
        Assert.Equal(payload, subscribed[0].Payload);
        await session.CloseAsync();
    }

    // 帧广播与战斗结束同样经统一出口（同一出口、同一载荷口径）。
    [Fact]
    public async Task FrameBroadcastAndBattleEnd_ReachUnifiedExit()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.EndDrainWindow = TimeSpan.FromMilliseconds(200);
        var session = FakeSession(transport, options);
        var ops = new List<string>();
        session.Push += push =>
        {
            lock (ops)
            {
                ops.Add(push.Op);
            }
        };
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.FrameBroadcast,
            Encoding.UTF8.GetBytes("{\"frame\":{\"frameId\":\"9\"}}"));
        transport.QueueNotify(TestOps.BattleEndNotify,
            Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"winnerPlayerId\":\"p-1\"}"));

        Assert.True(await WaitUntilAsync(() =>
        {
            lock (ops)
            {
                return ops.Count == 2;
            }
        }), "两类推送都应经统一出口");
        lock (ops)
        {
            // 分发顺序不保证（推送经调度器发布），只断言两类推送都在统一出口出现过。
            Assert.Contains(TestOps.FrameBroadcast, ops);
            Assert.Contains(TestOps.BattleEndNotify, ops);
        }
        await session.CloseAsync();
    }

    // 未预设 op 的推送（未来新增）也必须从统一出口可见——push 出口不写死 op 名单。
    [Fact]
    public async Task UnknownPushOp_ReachesUnifiedExit()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        BattlePush? received = null;
        session.Push += push => Volatile.Write(ref received, push);
        await session.ConnectAsync();

        transport.QueueNotify("/battle.v1.FutureNotify", Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\"}"));
        Assert.True(await WaitUntilAsync(() => Volatile.Read(ref received) != null), "未知 op 也应经统一出口");
        Assert.Equal("/battle.v1.FutureNotify", received!.Op);
        await session.CloseAsync();
    }

    // 订阅按 op 过滤、退订幂等；退订后不再收到推送。
    [Fact]
    public async Task OnPush_FiltersByOp_AndDisposeUnsubscribes()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        var frameBroadcasts = 0;
        var playerOuts = 0;
        var subscription = session.OnPush(TestOps.PlayerOutNotify, _ => Interlocked.Increment(ref playerOuts));
        session.OnPush(TestOps.FrameBroadcast, _ => Interlocked.Increment(ref frameBroadcasts));
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.PlayerOutNotify, Encoding.UTF8.GetBytes("{\"playerId\":\"p-2\"}"));
        Assert.True(await WaitUntilAsync(() => Volatile.Read(ref playerOuts) == 1), "订阅应收到本 op 的推送");
        Assert.Equal(0, Volatile.Read(ref frameBroadcasts)); // 别的 op 不串台。

        subscription.Dispose();
        subscription.Dispose(); // 退订幂等。
        transport.QueueNotify(TestOps.PlayerOutNotify, Encoding.UTF8.GetBytes("{\"playerId\":\"p-3\"}"));
        await Task.Delay(150);
        Assert.Equal(1, Volatile.Read(ref playerOuts)); // 退订后不再分发。
        await session.CloseAsync();
    }

    // 业务回调异常被隔离：一个订阅抛错不影响其余订阅与统一出口（推送路径不允许被打断）。
    [Fact]
    public async Task PushCallback_ExceptionIsIsolated()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        var delivered = 0;
        session.Push += _ => throw new InvalidOperationException("统一出口回调炸了");
        session.OnPush(TestOps.PlayerOutNotify, _ => Interlocked.Increment(ref delivered));
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.PlayerOutNotify, Encoding.UTF8.GetBytes("{\"playerId\":\"p-2\"}"));
        Assert.True(await WaitUntilAsync(() => Volatile.Read(ref delivered) == 1), "异常回调不应打断其余分发");
        await session.CloseAsync();
    }

    private static int Count(List<BattlePush> pushes)
    {
        lock (pushes)
        {
            return pushes.Count;
        }
    }
}
