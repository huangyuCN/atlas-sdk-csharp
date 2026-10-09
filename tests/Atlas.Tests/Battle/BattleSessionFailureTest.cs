using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionFailureTest 覆盖「不可重试业务拒绝 → 终态化 + 上报 Failed」的钉死口径：
//   - BATTLE_NOT_FOUND(3001/404)、BATTLE_FULL(3002/409)、FRAME_TARGET_MISMATCH(403)、
//     INVALID_PARAMS、票类（过期/无效）在**重连重新入局**路径上一律终态化并上报一次；
//   - 同样的拒绝出现在前台帧调用上同样终态化（不是只处理重连路径）；
//   - 终态化即在途请求以**该拒绝的** Status 结算（reason/code 与拒绝同形，非一律 BATTLE_ENDED）。
public sealed class BattleSessionFailureTest
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

    private static BattleSession FakeSession(BattleFakeTransport transport, BattleSessionOptions options)
    {
        var session = BattleSession.Create(Plan(7102), options);
        session.DialOverride = _ => Task.FromResult<ITransport>(transport);
        return session;
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

    // 重连重新入局被业务拒绝 → 终态化 + 上报 Failed 一次 + 不再拨号/写线。
    [Theory]
    [InlineData(404, DirectErrors.BattleNotFoundReason)]
    [InlineData(409, DirectErrors.BattleFullReason)]
    [InlineData(403, DirectErrors.FrameTargetMismatchReason)]
    [InlineData(400, DirectErrors.InvalidParamsReason)]
    [InlineData(401, DirectErrors.TicketExpiredReason)]
    [InlineData(401, DirectErrors.TicketInvalidReason)]
    public async Task Rejoin_Rejected_TerminalizesAndReportsFailedOnce(int code, string reason)
    {
        var first = new BattleFakeTransport();
        var second = new BattleFakeTransport();
        var dials = 0;
        var session = BattleSession.Create(Plan(7102), Options());
        session.DialOverride = _ =>
            Task.FromResult<ITransport>(Interlocked.Increment(ref dials) == 1 ? first : second);
        var failures = new List<Exception>();
        session.Failed += exception =>
        {
            lock (failures)
            {
                failures.Add(exception);
            }
        };
        await session.ConnectAsync();

        await first.CloseAsync(); // 断链 → 自动重连（新一代重新入局）。
        await second.WaitForWritesAsync(1);
        second.QueueBusinessError(second.WrittenHeaders[0].Seq, code, reason, "服务端拒绝");

        Assert.True(await WaitUntilAsync(() => session.HasFailed), "不可重试业务拒绝应终态化");
        // 语义分工：无结算可展示的终态拒绝走 HasFailed，**不得置 HasEnded**（否则上层会去取结算）。
        Assert.False(session.HasEnded);
        var cause = Assert.IsType<BusinessException>(session.FailureCause);
        Assert.Equal(reason, cause.Reason);
        Assert.Equal(code, cause.Code);
        Assert.Single(failures); // Failed 恰好一次。
        Assert.Equal(2, Volatile.Read(ref dials)); // 终态后不再拨号。

        // 终态零写线 + 后续调用本地拒绝（不占用在途表、不等超时）。
        var writes = second.WrittenCount;
        await Task.Delay(150);
        Assert.Equal(writes, second.WrittenCount);
        var rejected = await Assert.ThrowsAsync<BusinessException>(
            () => session.SendFrameInputAsync(1, new byte[] { 0x01 }));
        Assert.Equal(reason, rejected.Reason);
        Assert.Equal(writes, second.WrittenCount);
        await session.CloseAsync();
    }

    // 前台帧调用被终态类拒绝同样终态化（不只在重连路径处置）。
    [Fact]
    public async Task ForegroundCall_Rejected_TerminalizesAndRejectsFurtherSends()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        var failures = 0;
        session.Failed += _ => Interlocked.Increment(ref failures);
        await session.ConnectAsync();

        var call = session.JoinBattleAsync();
        await transport.WaitForWritesAsync(1);
        transport.QueueBusinessError(
            transport.WrittenHeaders[0].Seq, DirectErrors.BattleNotFoundCode,
            DirectErrors.BattleNotFoundReason, "对局不存在");

        var exception = await Assert.ThrowsAsync<BusinessException>(() => call);
        Assert.Equal(DirectErrors.BattleNotFoundReason, exception.Reason);
        Assert.True(await WaitUntilAsync(() => session.HasFailed), "前台拒绝应终态化");
        Assert.Equal(1, Volatile.Read(ref failures));

        var writes = transport.WrittenCount;
        await Assert.ThrowsAsync<BusinessException>(() => session.JoinBattleAsync());
        Assert.Equal(writes, transport.WrittenCount);
        await session.CloseAsync();
    }

    // 终态化结算用的是**该拒绝的** reason/code（不是一律 BATTLE_ENDED），metadata 标本地结算。
    [Fact]
    public async Task TerminalFailure_SettlesOtherInflight_WithTheRejectionStatus()
    {
        var transport = new BattleFakeTransport();
        var session = FakeSession(transport, Options());
        await session.ConnectAsync();

        var rejected = session.JoinBattleAsync();
        await transport.WaitForWritesAsync(1);
        var inflight = session.SendFrameInputAsync(1, new byte[] { 0x01 });
        await transport.WaitForWritesAsync(2);

        transport.QueueBusinessError(
            transport.WrittenHeaders[0].Seq, DirectErrors.BattleFullCode,
            DirectErrors.BattleFullReason, "战斗已满");

        var first = await Assert.ThrowsAsync<BusinessException>(() => rejected);
        Assert.Equal(DirectErrors.BattleFullReason, first.Reason);
        // 另一条在途请求立即以同一终态 Status 结算（reason/code/class/metadata 四要素齐备）。
        var settled = await Assert.ThrowsAsync<BusinessException>(
            () => inflight.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal(DirectErrors.BattleFullReason, settled.Reason);
        Assert.Equal(DirectErrors.BattleFullCode, settled.Code);
        Assert.Equal(ErrorClass.Business, settled.Class);
        Assert.True(settled.Metadata.TryGetValue(DirectErrors.LocalSettlementKey, out var local));
        Assert.Equal("true", local);
        await session.CloseAsync();
    }

    // 分类判定入口（isBattleEnded 同族）：三个终态类 reason + 票类 + 非重试判定都由它给出。
    [Fact]
    public void TerminalClassification_FollowsPinnedContract()
    {
        Assert.True(DirectErrors.IsBattleNotFound(
            new BusinessException(DirectErrors.BattleNotFoundCode, DirectErrors.BattleNotFoundReason, "", null)));
        Assert.True(DirectErrors.IsBattleFull(
            new BusinessException(DirectErrors.BattleFullCode, DirectErrors.BattleFullReason, "", null)));
        Assert.True(DirectErrors.IsFrameTargetMismatch(
            new BusinessException(DirectErrors.FrameTargetMismatchCode, DirectErrors.FrameTargetMismatchReason, "", null)));
        Assert.True(DirectErrors.IsBattleEnded(
            new BusinessException(DirectErrors.BattleEndedCode, DirectErrors.BattleEndedReason, "", null)));
        Assert.True(DirectErrors.IsTerminalFailure(
            new BusinessException(DirectErrors.BattleNotFoundCode, DirectErrors.BattleNotFoundReason, "", null)));
        Assert.True(DirectErrors.IsTerminalFailure(
            new BusinessException(DirectErrors.FrameTargetMismatchCode, DirectErrors.FrameTargetMismatchReason, "", null)));
        Assert.False(DirectErrors.IsTerminalFailure(
            new BusinessException(DirectErrors.BattleEndedCode, DirectErrors.BattleEndedReason, "", null)));
        Assert.True(DirectErrors.IsTicketRejected(
            new BusinessException(401, DirectErrors.TicketExpiredReason, "", null)));
        Assert.True(DirectErrors.IsBusinessRejected(
            new NetworkException("x", new BusinessException(500, "INTERNAL", "", null))));
        Assert.False(DirectErrors.IsBusinessRejected(new NetworkException("x")));

        // 协议事实逐字锁定：帧目标失配与框架 frameops.ReasonTargetMismatch 同值（403）；
        // 本地结算 metadata 键为三 SDK 统一键名（勿各写一套）。
        Assert.Equal("FRAME_TARGET_MISMATCH", DirectErrors.FrameTargetMismatchReason);
        Assert.Equal(403, DirectErrors.FrameTargetMismatchCode);
        Assert.Equal("x-atlas-sdk-local-settled", DirectErrors.LocalSettlementKey);
    }
}
