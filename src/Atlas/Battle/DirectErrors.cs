using Atlas.Transport;

namespace Atlas.Battle;

// DirectErrors 是直连战斗会话的稳定报错口径：全部经**既有异常体系**抛出
//（ProtocolException = 协议/线格式非法且不可重试；BusinessException = 帧面业务拒绝），
// 不新增异常类型族；常量供调用方与测试做可判定的断言（等同 Go SDK 的哨兵错误）。
// 判定函数（isBattleEnded 同族）见 DirectErrorClassify.cs。
public static partial class DirectErrors
{
    // NotifyMalformed 成局通知载荷非法（非 JSON / 非对象 / 票不是合法 base64）。
    public const string NotifyMalformed = "成局通知载荷非法";

    // NotifyNoTicket 成局通知缺 battle_ticket 或票为空（空票连不上接入层）。
    public const string NotifyNoTicket = "成局通知缺战斗票据（battle_ticket）";

    // NotifyNoEndpoint 成局通知没有可用的接入面地址（缺字段 / 空列表 / 面名未知 / 地址为空）。
    public const string NotifyNoEndpoint = "成局通知缺可用的接入层地址（endpoints）";

    // TransportUnknown 接入层传输面未知（既非枚举名也非短名）。
    public const string TransportUnknown = "接入层传输面未知";

    // TransportNotFound 本局未下发所请求的传输面（不得猜端口、不得静默换面）。
    public const string TransportNotFound = "本局未下发该接入层传输面";

    // FlowIdMismatch 数据报 flow-id 前缀失配或长度不足（失配即丢弃并要求重新 hello）。
    public const string FlowIdMismatch = EdgeFlow.MismatchMessage;

    // HelloRejected 接入层拒绝 hello：握手期内未回应 flow-id（L4 无应用层回执，仅断开）。
    public const string HelloRejected = EdgeHandshake.RejectedMessage;

    // WsRejected 接入层拒绝 WS 升级（TCP 已连通但升级未完成）。
    public const string WsRejected = WsTransport.RejectedMessage;

    // SessionClosed 会话已关闭（关闭后不得再发请求）。
    public const string SessionClosed = "战斗会话已关闭";

    // SessionAlreadyConnected 会话已连接（ConnectAsync 不得重复调用）。
    public const string SessionAlreadyConnected = "战斗会话已连接";

    // TicketExpiredReason 票过期的帧面业务拒绝 reason：上层据此重新匹配（可判定）。
    public const string TicketExpiredReason = "BATTLE_TICKET_EXPIRED";

    // TicketInvalidReason 票非法/被篡改的帧面业务拒绝 reason（与过期分开，处置不同）。
    public const string TicketInvalidReason = "BATTLE_TICKET_INVALID";

    // BattleEndedReason 已结束对局的帧面业务拒绝 reason（服务端稳定 reason，与票类 reason 互斥）：
    // SDK 据此进入终态并停发（帧输入/补帧/心跳），而不是重试到超时——票废才重取票。
    public const string BattleEndedReason = "BATTLE_ENDED";

    // BattleEndedCode 「对局已结束」的状态码口径（服务端 ErrBattleEnded → 409）。
    // 本地终态结算沿用同一 code：上层不必区分「服务端回的」与「SDK 本地结算的」。
    public const int BattleEndedCode = 409;

    // BattleNotFoundReason 对局不存在的业务拒绝 reason（错误码 3001，HTTP 语义 404）：
    // 不可重试——对局不存在时重试/重连毫无意义，会话直接终态化并上报 Failed。
    public const string BattleNotFoundReason = "BATTLE_NOT_FOUND";

    // BattleNotFoundCode 「对局不存在」的状态码口径（errors.proto: BattleNotFound = 3001 → 404）。
    public const int BattleNotFoundCode = 404;

    // BattleFullReason 战斗已满/入局被拒的业务拒绝 reason（错误码 3002，HTTP 语义 409）：
    // 不可重试——名额已满，重试只会再被拒；同样终态化并上报 Failed。
    public const string BattleFullReason = "BATTLE_FULL";

    // BattleFullCode 「战斗已满」的状态码口径（errors.proto: BattleFull = 3002 → 409）。
    public const int BattleFullCode = 409;

    // InvalidParamsReason 请求参数非法的业务拒绝 reason（错误码 3003 一族）：
    // 帧请求由 SDK 组装，参数非法说明调用方用法错误，重试必然再失败。
    public const string InvalidParamsReason = "INVALID_PARAMS";

    // FrameTargetMismatchReason 帧面发现「票面对局」与「请求正文目标」不一致时的业务拒绝
    // reason（HTTP 语义 403）：正常情况下 SDK 不会产生（票与正文同源），一旦出现即
    // 客户端/协议侧错误——不可重试、终态化并上报，绝不进重试循环（框架侧为失败关闭）。
    // 字面量与框架 contrib/actor/frameops/target.go 的 ReasonTargetMismatch 对齐；
    // 框架暂无 C# errors 生成物，故此处手写（同 ErrorClass 现状），生成物落地后改引用。
    public const string FrameTargetMismatchReason = "FRAME_TARGET_MISMATCH";

    // FrameTargetMismatchCode 「帧目标失配」的状态码口径（框架侧失败关闭 → 403）。
    public const int FrameTargetMismatchCode = 403;

    // BattleEndNotifyReason 终态由**结算结束通知**触发（非业务拒绝）时的来源标记：
    // 与 BattleEndedReason 分开，便于上层与排障区分「服务端拒绝先到 vs 结算推送先到」。
    public const string BattleEndNotifyReason = "BATTLE_END_NOTIFY";

    // SessionEnded 会话已进入终态（对局已结束）：终态下的发帧/补帧/入局/重连一律拒发（不写线）。
    public const string SessionEnded = "战斗已结束";

    // LocalSettlementKey 是本地终态结算的 metadata 标记键（ended/failed 两族通用）：
    // 排障时据此区分「服务端回执」与「SDK 因终态本地结算」（业务分支只看 reason，不依赖本键）。
    // 三 SDK 统一键名（Go/TS/C# 同值），勿各写一套——本轮审计发现三仓曾各不相同。
    public const string LocalSettlementKey = "x-atlas-sdk-local-settled";
}
