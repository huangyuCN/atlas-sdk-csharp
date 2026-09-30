namespace Atlas.Battle;

// BattleOps 是战斗域与成局推送的 op 名（服务端消息完整名，与 protoc 生成的 opclient 口径一致：
// 请求 op = "/<proto 包>.<服务>/<方法>"，推送 op = "/<proto 包>.<消息>"）。
//
// 待回归：模板仓的 C# opclient 产物（protoc-gen-atlas-client 的 _client.g.cs）尚未覆盖
// battle/game 域，故此处手写快照；生成物落地后改为引用生成物（同 FrameGen 的做法），
// 数值/字面量以生成物为准。
public static class BattleOps
{
    // JoinBattle 玩家加入战斗（请求-响应；回执含会话元信息、当前帧与快照）。
    public const string JoinBattle = "/battle.v1.BattleService/JoinBattle";

    // SendFrameInput 帧输入上行（returns Empty；身份由帧会话槽票据注入）。
    public const string SendFrameInput = "/battle.v1.BattleService/SendFrameInput";

    // SyncFrames 断线重连补帧（携带 last_seen_frame 按帧区间拉取缺失帧）。
    public const string SyncFrames = "/battle.v1.BattleService/SyncFrames";

    // FrameBroadcast 服务端推送：帧广播下行。
    public const string FrameBroadcast = "/battle.v1.FrameBroadcast";

    // BattleEndNotify 服务端推送：战斗结束。
    public const string BattleEndNotify = "/battle.v1.BattleEndNotify";

    // MatchStartedNotify 服务端推送：匹配成功开局通知（DirectPlan.FromNotify 的载荷来源）。
    // 消息完整名（非服务/方法名）：与 Go 生成物 PlayerServicePushOps.MatchStartedNotify 同值。
    public const string MatchStartedNotify = "/game.v1.MatchStartedNotify";
}
