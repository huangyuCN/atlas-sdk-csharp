namespace Atlas.Battle;

// WireOps 是会话自身要认的两条线上寻址键（op）：帧广播（推进补帧基准）与战斗结束通知
//（进入终态）。op 的**取值本身即协议事实**，与「SDK 是否持有游戏域消息类型」无关——
// 同 Go SDK 的 direct.Op* 口径。
//
// 游戏域消息（battle / game / matcher 的 DTO、枚举与 op 表）已迁到游戏协议包
//（NuGet HuangyuCN.AtlasGame.Protocol，源自模板仓 protocol/）：调用方发业务 op、订阅推送时
// 用协议包生成的常量；本类只服务会话内部这两处语义，不对外导出（SDK 不再为一整套游戏 op 造公开表）。
internal static class WireOps
{
    // FrameBroadcast 是服务端帧广播推送的 op（会话据此推进补帧基准）。
    internal const string FrameBroadcast = "/battle.v1.FrameBroadcast";

    // BattleEndNotify 是战斗结束通知推送的 op（会话据此进入终态、留档首投载荷）。
    internal const string BattleEndNotify = "/battle.v1.BattleEndNotify";
}
