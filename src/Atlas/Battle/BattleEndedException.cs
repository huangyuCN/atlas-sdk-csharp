using Atlas.Errors;

namespace Atlas.Battle;

// BattleEndedException 表示对局已结束（服务端业务拒绝 BATTLE_ENDED，或收到结算结束通知）
// 之后仍发起帧输入/补帧/入局/重连：SDK 已进入终态并**拒绝写线**——不静默丢弃、不空等超时。
//
// 为什么用专用异常而不是 InvalidOperationException：InvalidOperationException 的语义是
// 「调用时机/对象状态非法」这类编程错误，与「对局正常结束」这一可预期的业务终态混在一起，
// 上层无法只按类型区分；专用异常让 `catch (BattleEndedException)` 精确命中终态，
// 且与既有 AtlasException 体系一致（服务端拒绝仍走 BusinessException，票废仍走 Failed）。
public sealed class BattleEndedException : AtlasException
{
    // BattleEndedException 构造：reason 是触发终态的结束信号来源（见 Reason）。
    public BattleEndedException(string reason, string message)
        : base(message)
    {
        Reason = reason ?? "";
    }

    // Reason 是触发终态的结束信号来源：DirectErrors.BattleEndedReason（服务端业务拒绝）
    // 或 DirectErrors.BattleEndNotifyReason（结算结束通知先到）。
    public string Reason { get; }
}
