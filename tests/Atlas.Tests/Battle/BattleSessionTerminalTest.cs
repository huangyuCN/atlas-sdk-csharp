using System;
using System.Collections.Generic;
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

// BattleSessionTerminalTest 覆盖终态的两条线级不变量：
//   ① 终态后**零写线**：终态检查点位于写锁内（组帧与写线之间复核）——在途那一笔写
//      （已持写锁、终态置位前组帧）不回滚，之后任何写一律拒发；
//   ② 终态时**在途请求立即以终态 Status 结算**（不等回执/超时，不报成网络错误）。
public sealed class BattleSessionTerminalTest
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
            InvokeTimeoutMs = 5000,
            BackoffBaseMs = 50,
            BackoffMaxMs = 200,
            HeartbeatInterval = TimeSpan.Zero,
            EndDrainWindow = TimeSpan.FromMilliseconds(300),
        };
    }

    private static BattleSession FakeSession(BattleFakeTransport transport, BattleSessionOptions options)
    {
        var session = BattleSession.Create(Plan(7102), options);
        session.DialOverride = _ => Task.FromResult<ITransport>(transport);
        return session;
    }

    private static byte[] EndPayload(string winner)
    {
        return Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"winnerPlayerId\":\"" + winner + "\"}");
    }

    // WaitUntilAsync 轮询等待条件成立（到点返回 false；终态是异步收口，不接受固定 sleep 赌时序）。
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

    // ① 竞态窗口：第一笔写在写锁内挂起（已组帧、未上线）→ 置终态 → 放行第一笔 →
    //    第二笔（已组帧、在等写锁）出锁时复核终态，不再写线。
    [Fact]
    public async Task Terminal_ReachedBetweenFramingAndWrite_SecondWriteNeverHitsWire()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        var enteredWrite = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseWrite = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.BeforeWrite = () =>
        {
            enteredWrite.TrySetResult(true);
            return releaseWrite.Task;
        };
        await session.ConnectAsync();

        var first = session.SendFrameInputAsync(1, new byte[] { 0x01 });
        Assert.True(await enteredWrite.Task.WaitAsync(TimeSpan.FromSeconds(3)), "第一笔写应进入写锁");
        var second = session.SendFrameInputAsync(2, new byte[] { 0x02 });
        await Task.Delay(50); // 第二笔已组帧并排在写锁之外（竞态窗口就位）。

        // 终态在「第二笔已组帧、尚未上线」的窗口内到达（结束通知走推送路径）。
        transport.QueueNotify(BattleOps.BattleEndNotify, EndPayload("p-1"));
        Assert.True(await WaitUntilAsync(() => session.HasEnded), "结束通知应进入终态");

        releaseWrite.TrySetResult(true);
        // 第一笔在终态置位前已进写锁：不回滚（上线），但其在途请求按终态 Status 立即结算。
        await Assert.ThrowsAsync<BusinessException>(() => first);
        // 第二笔出锁时复核终态：明确异常且零写线。
        var exception = await Assert.ThrowsAsync<BattleEndedException>(() => second);

        // 终态后零写线：只有第一笔落地，第二笔在锁内被拒（明确异常，不是静默丢弃）。
        Assert.Equal(1, transport.WrittenCount);
        Assert.Contains(DirectErrors.SessionEnded, exception.Message, StringComparison.Ordinal);
        await session.CloseAsync();
    }

    // ② 终态时在途请求立即以终态 Status 结算：reason/code/class/metadata 四要素齐备，
    //    既不等 InvokeTimeoutMs，也不被收尾窗口关连接后的网络错误顶替。
    [Fact]
    public async Task Terminal_SettlesInflightRequestImmediately_WithTerminalStatus()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        await session.ConnectAsync();

        // 入局请求已写出、服务端不回执（在途）。
        var call = session.JoinBattleAsync();
        await transport.WaitForWritesAsync(1);

        transport.QueueNotify(BattleOps.BattleEndNotify, EndPayload("p-1"));
        var exception = await Assert.ThrowsAsync<BusinessException>(
            () => call.WaitAsync(TimeSpan.FromSeconds(2)));

        Assert.Equal(DirectErrors.BattleEndedReason, exception.Reason);
        Assert.Equal(DirectErrors.BattleEndedCode, exception.Code);
        Assert.Equal(ErrorClass.Business, exception.Class);
        Assert.True(exception.Metadata.TryGetValue(DirectErrors.LocalSettlementKey, out var local));
        Assert.Equal("true", local);
        Assert.True(AtlasException.IsBusinessError(exception, DirectErrors.BattleEndedReason));
        await session.CloseAsync();
    }

    // ② 派生：终态后新发起的调用本地拒绝（不写线），不占用在途表、不等超时。
    [Fact]
    public async Task Terminal_AfterSettlement_NewCallsRejectedLocally()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        await session.ConnectAsync();

        transport.QueueNotify(BattleOps.BattleEndNotify, EndPayload("p-1"));
        Assert.True(await WaitUntilAsync(() => session.HasEnded), "结束通知应进入终态");
        var writes = transport.WrittenCount;

        await Assert.ThrowsAsync<BattleEndedException>(() => session.JoinBattleAsync());
        await Assert.ThrowsAsync<BattleEndedException>(
            () => session.SendFrameInputAsync(1, new byte[] { 0x01 }));
        Assert.Equal(writes, transport.WrittenCount);
        await session.CloseAsync();
    }
}
