using Atlas.Battle.V1;
using Atlas.Game.V1;

namespace Atlas.Battle;

// BattleOps 是战斗域与成局推送的 op 名（服务端消息完整名），取值**全部来自生成物**
//（src/Atlas/Battle/Gen/*.g.cs：由 scripts/gen-dto.sh 从 protoc-gen-atlas-client 产物抽取，
// 与模板仓 proto 同源；本文件不手写任何 op 字面量）：
//   - 请求 op = "/<proto 包>.<服务>/<方法>"（BattleServiceProtocolOps）；
//   - 推送 op = "/<proto 包>.<消息>"（BattleServicePushOps / PlayerServicePushOps）。
public static class BattleOps
{
    // JoinBattle 玩家加入战斗（请求-响应；回执含会话元信息、当前帧与快照）。
    public const string JoinBattle = BattleServiceProtocolOps.JoinBattle;

    // SendFrameInput 帧输入上行（returns Empty；身份由帧会话槽票据注入）。
    public const string SendFrameInput = BattleServiceProtocolOps.SendFrameInput;

    // SyncFrames 断线重连补帧（携带 last_seen_frame 按帧区间拉取缺失帧）。
    public const string SyncFrames = BattleServiceProtocolOps.SyncFrames;

    // Ping 直连保活探针（returns Empty 即 Tell：无业务回执、不改对局状态）：
    // 无输入期间周期发送，让帧面保持活跃（数据报面靠帧面空闲读超时判活跃）。
    public const string Ping = BattleServiceProtocolOps.Ping;

    // FrameBroadcast 服务端推送：帧广播下行。
    public const string FrameBroadcast = BattleServicePushOps.FrameBroadcast;

    // BattleEndNotify 服务端推送：战斗结束。
    public const string BattleEndNotify = BattleServicePushOps.BattleEndNotify;

    // PlayerOutNotify 服务端推送：玩家出局（掉线被判负并移出参战名单；多人局其余玩家继续）。
    public const string PlayerOutNotify = BattleServicePushOps.PlayerOutNotify;

    // MatchStartedNotify 服务端推送：匹配成功开局通知（DirectPlan.FromNotify 的载荷来源）。
    public const string MatchStartedNotify = PlayerServicePushOps.MatchStartedNotify;

    // MatchFailedNotify 服务端推送：匹配失败通知（上层按 op 分发，SDK 不解读载荷）。
    public const string MatchFailedNotify = PlayerServicePushOps.MatchFailedNotify;

    // PartyRosterNotify 服务端推送：队伍名单变更（上层按 op 分发，SDK 不解读载荷）。
    public const string PartyRosterNotify = PlayerServicePushOps.PartyRosterNotify;
}
