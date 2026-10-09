using System;
using Atlas.Errors;

namespace Atlas.Battle;

// DirectErrors 的判定函数族（与上层/测试共用的可判定入口，等同 Go SDK 的 errors.Is 口径）：
// 直连战斗会话的错误分支只按这些函数判定，不在各处散落 reason 字面量比较。
public static partial class DirectErrors
{
    // IsBattleEnded 判定「对局已结束」的业务拒绝（服务端原样拒绝与本地终态拒绝同形：
    // 调用方一处判定即可，不必区分是谁先发现的）。
    public static bool IsBattleEnded(Exception? exception)
    {
        return IsBusinessError(exception, BattleEndedReason);
    }

    // IsBattleNotFound 判定「对局不存在」的业务拒绝（不可重试：会话终态化并上报 Failed）。
    public static bool IsBattleNotFound(Exception? exception)
    {
        return IsBusinessError(exception, BattleNotFoundReason);
    }

    // IsBattleFull 判定「战斗已满」的业务拒绝（不可重试：会话终态化并上报 Failed）。
    public static bool IsBattleFull(Exception? exception)
    {
        return IsBusinessError(exception, BattleFullReason);
    }

    // IsInvalidParams 判定「请求参数非法」的业务拒绝（调用方用法错误，重试必然再失败）。
    public static bool IsInvalidParams(Exception? exception)
    {
        return IsBusinessError(exception, InvalidParamsReason);
    }

    // IsFrameTargetMismatch 判定「帧目标失配」的业务拒绝（票面对局 ≠ 正文目标，403）：
    // 正常 SDK 不会产生，出现即客户端/协议侧错误——与终态类同族处置（不重试、终态化、上报）。
    public static bool IsFrameTargetMismatch(Exception? exception)
    {
        return IsBusinessError(exception, FrameTargetMismatchReason);
    }

    // IsTicketRejected 判定票据类业务拒绝（过期或无效）：上层据此重新取票，而不是终态化。
    public static bool IsTicketRejected(Exception? exception)
    {
        return IsBusinessError(exception, TicketExpiredReason)
            || IsBusinessError(exception, TicketInvalidReason);
    }

    // IsBusinessRejected 判定「服务端业务拒绝」（任一 reason）：业务拒绝一律不可重试——
    // 同一请求原样重发只会拿到同一拒绝，处置由调用方按 reason 细分。
    public static bool IsBusinessRejected(Exception? exception)
    {
        for (Exception? current = exception; current != null; current = current.InnerException)
        {
            if (current is BusinessException)
            {
                return true;
            }
        }
        return false;
    }

    // IsTerminalClass 判定「终态类」业务拒绝（对局已结束 / 不存在 / 已满 / 帧目标失配）：
    // 四者都表示会话已无用——一律入终态、停心跳、停止重连与写线，不再原样重试。
    public static bool IsTerminalClass(Exception? exception)
    {
        return IsBattleEnded(exception)
            || IsBattleNotFound(exception)
            || IsBattleFull(exception)
            || IsFrameTargetMismatch(exception);
    }

    // IsTerminalFailure 判定「需终态化并上报 Failed」的业务拒绝：终态类中除「对局已结束」
    //（正常结束，不误报 Failed）以外的全部，外加参数非法（调用方用法错误，重试必然再失败）。
    public static bool IsTerminalFailure(Exception? exception)
    {
        return IsBattleNotFound(exception)
            || IsBattleFull(exception)
            || IsFrameTargetMismatch(exception)
            || IsInvalidParams(exception);
    }

    // IsBusinessError 沿 InnerException 链查找业务拒绝并比对 reason（与
    // AtlasException.IsBusinessError 同一口径，这里补 null 安全判定）。
    private static bool IsBusinessError(Exception? exception, string reason)
    {
        return exception != null && AtlasException.IsBusinessError(exception, reason);
    }
}
