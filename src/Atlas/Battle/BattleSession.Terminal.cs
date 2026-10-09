using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Battle;

// BattleSession 的终态面：会话有两种终态，共同不变量是「零写线 + 在途立即结算 + 不再重连」。
// **语义分工（三 SDK 钉死）**：
//   1. **对局正常结束**（HasEnded；BATTLE_ENDED 业务拒绝或结算结束通知，见 BattleSession.Ended.cs）
//      ⇒ **有结算可展示**（BattleEnd 事件/推送载荷）；收尾窗口内连接仍可读，**不上报 Failed**
//      （否则上层会被误导去重新匹配）；
//   2. **无结算可展示的终态拒绝**（HasFailed；BATTLE_NOT_FOUND/BATTLE_FULL/FRAME_TARGET_MISMATCH/
//      INVALID_PARAMS、票类）⇒ 会话已无用——停心跳、关通道（打断重连）并以 Failed 上报一次。
// 两者**互不置位**：终态类拒绝绝不置 HasEnded（否则上层会去取不存在的结算），
// 正常结束也不置 HasFailed。
public sealed partial class BattleSession
{
    // _failed 是终态失败标记（0/1，首个失败生效且不复位：一个会话只对应一次成局）。
    private int _failed;

    // _failedCause 是触发终态失败的原始错误（可判定：BusinessException/ProtocolException 等）。
    private Exception? _failedCause;

    // HasFailed 报告会话是否已进入终态失败（**无结算可展示**的不可重试拒绝：入局被拒/目标失配/
    // 票类等）。与 HasEnded 互不置位：正常结束（有结算）看 HasEnded，终态拒绝看 HasFailed。
    public bool HasFailed => Volatile.Read(ref _failed) != 0;

    // FailureCause 返回终态失败的原因（未失败为 null）；与 Failed 事件同源，便于迟到订阅者取因。
    public Exception? FailureCause => Volatile.Read(ref _failedCause);

    // ThrowIfTerminal 终态守卫（发帧/补帧/入局/重连/心跳的统一入口，检查在任何组帧/写线之前）：
    // 对局已结束 → BattleEndedException；终态失败 → 原失败异常（可判定）；两者都保证不写线。
    private void ThrowIfTerminal()
    {
        ThrowIfEnded();
        var cause = Volatile.Read(ref _failedCause);
        if (cause != null)
        {
            throw cause;
        }
    }

    // EnterTerminalFailure 进入终态失败（幂等：首个失败生效）：置位 → 记原因 → 在途与排队请求
    // 立即以终态 Status 结算。返回 true = 本次是首个失败（调用方负责关通道与上报 Failed）。
    private bool EnterTerminalFailure(Exception cause, int code, string reason)
    {
        if (Interlocked.CompareExchange(ref _failed, 1, 0) != 0)
        {
            return false;
        }
        Volatile.Write(ref _failedCause, cause);
        SettleInflightTerminal(code, reason);
        return true;
    }

    // FailTerminalAsync 终态失败收口：置位（首个生效，重复调用幂等）→ 停心跳、关通道
    //（打断重连循环、结算剩余请求）→ 上报 Failed 一次；此后一切上发都以失败异常拒发。
    // fromHeartbeat = true 表示收口发生在保活心跳循环内（调用方就是那个循环任务）：
    // 跳过一次停表等待（否则 await 自己所在的循环任务会自等待死锁），循环随即返回即停表。
    private async Task FailTerminalAsync(Exception cause, int code, string reason, bool fromHeartbeat = false)
    {
        if (!EnterTerminalFailure(cause, code, reason))
        {
            return;
        }
        if (fromHeartbeat)
        {
            await CloseAndReportAsync(cause).ConfigureAwait(false);
            return;
        }
        await FailSessionAsync(cause).ConfigureAwait(false);
    }

    // SettleInflightTerminal 立即以终态 Status 结算在途与排队请求（不等回执/超时，
    // 不报成网络错误——收尾窗口内连接仍可读，但这些请求不会再有结果）。
    private void SettleInflightTerminal(int code, string reason)
    {
        Volatile.Read(ref _channel)?.SettlePending(TerminalStatus(code, reason));
    }

    // TerminalStatus 构造本地终态结算的 Status 异常：reason/code/class 与服务端回执同形，
    // metadata 标「本地结算」（排障区分来源；业务分支只看 reason）。
    private static BusinessException TerminalStatus(int code, string reason)
    {
        var metadata = new Dictionary<string, string>
        {
            [DirectErrors.LocalSettlementKey] = "true",
        };
        return new BusinessException(code, reason,
            $"{DirectErrors.SessionEnded}（{reason}）：在途请求按终态结算", metadata, ErrorClass.Business);
    }

    // GuardRejectionAsync 观察一次帧调用的结果并收口终态类拒绝：
    //   - BATTLE_ENDED（对局已结束）：进终态、停发，拒绝原样上抛（正常收尾，不报 Failed）；
    //   - 终态失败类（BATTLE_NOT_FOUND/BATTLE_FULL/FRAME_TARGET_MISMATCH/INVALID_PARAMS）：
    //     终态化并上报 Failed，拒绝原样上抛——调用方看到的是可判定业务拒绝，而不是重试到超时。
    private async Task<byte[]> GuardRejectionAsync(Task<byte[]> call)
    {
        try
        {
            return await call.ConfigureAwait(false);
        }
        catch (BusinessException exception) when (DirectErrors.IsBattleEnded(exception))
        {
            EnterEnded(DirectErrors.BattleEndedReason);
            throw;
        }
        catch (BusinessException exception) when (DirectErrors.IsTerminalFailure(exception))
        {
            await FailTerminalAsync(exception, exception.Code, exception.Reason).ConfigureAwait(false);
            throw;
        }
    }
}
