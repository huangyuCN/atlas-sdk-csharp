using System;

namespace Atlas.Client;

// SessionOps 是会话生命周期 op 名集合（5 个：Register/Login/Resume/Logout/Heartbeat）。
// 取值由接缝实现提供（生成物 SessionProtocolOps 为其素材）；状态机不写任何 op 字面量，
// 故默认空串（缺失即该接口不可用，服务端 op 约定由项目侧声明）。
public sealed class SessionOps
{
    public string Register { get; set; } = "";

    public string Login { get; set; } = "";

    public string Resume { get; set; } = "";

    public string Logout { get; set; } = "";

    public string Heartbeat { get; set; } = "";
}

// SessionMessage 是交给接缝的消息视图（op + 原始 payload 字节）：状态机不引用任何会话
// 消息类型，解码归接缝实现（用生成物的 DTO + 注入的 ISerializer）。C# 无类型擦除
//（对齐 TS 接缝的 token(msg: unknown)），故回执以「op + 字节」交给实现自行解码。
public sealed class SessionMessage
{
    public SessionMessage(string operation, byte[] payload)
    {
        Operation = operation ?? "";
        Payload = payload ?? Array.Empty<byte>();
    }

    // Operation 是消息的 op（请求 op 或推送 op）。
    public string Operation { get; }

    // Payload 是消息原始载荷字节（空回执为空数组）。
    public byte[] Payload { get; }
}

// PushEnvelope 是交给接缝的推送信封（S0.5 修订 1）：op + 帧头载荷编码版本 + 未解码的
// 原始载荷字节。状态机分发推送时**必须带上帧头 version**——ver=1 为 protojson 字节、
// ver=2 为 protobuf wire 字节，接缝据此选解码器；丢掉 version 会让 ver=2 的推送原因
// 静默丢失（按 protojson 解 protobuf 字节必然失败）。形状已冻结，三语言不得各自加字段。
public sealed class PushEnvelope
{
    // Op 是推送 op（与订阅键同源，取自帧 body 的 operation 段）。
    public string Op { get; init; } = "";

    // Version 是帧头载荷编码版本（1 = JSON/protojson，2 = protobuf wire；未知版本不得抛错）。
    public byte Version { get; init; }

    // Body 是推送原始载荷字节（SDK 不解码；空载荷为空数组）。
    public byte[] Body { get; init; } = Array.Empty<byte>();
}

// SessionCredentials 是接缝从会话消息里提取的凭据视图（非协议 DTO：不参与编解码，
// 只是 token/playerId/过期时间三元组的载体）。
public readonly struct SessionCredentials
{
    public SessionCredentials(string token, string playerId, long expiresAt)
    {
        Token = token ?? "";
        PlayerId = playerId ?? "";
        ExpiresAt = expiresAt;
    }

    // Token 是会话凭据（接缝未提取到为空串）。
    public string Token { get; }

    // PlayerId 是会话玩家 ID（接缝未提取到为空串）。
    public string PlayerId { get; }

    // ExpiresAt 是会话过期时间（秒/毫秒由协议定；可选钩子：无该字段恒 0 = 未知）。
    public long ExpiresAt { get; }
}

// ISessionProtocol 是会话协议接缝（S0.5 冻结形状 + 修订 1，三语言同名同职责，形状冻结后
// 不得各自加字段）：
//   1. Ops——5 个会话 op 名（唯一来源，状态机不写字面量）；
//   2. Token / PlayerId——从会话请求/回执取凭据（无该字段返回空串，可选钩子语义）；
//   3. ExpiresAt——取过期时间（可选钩子：模板无 expiry 字段时恒 0，本轮不启用续期）；
//   4. Kicked——推送识别 + 原因提取：收推送 op 与**推送信封**（PushEnvelope：op +
//      帧头 version + 未解码原始字节）。op 命中「被挤下线」推送即返回 ok=true，
//      原因标识为生成物的枚举名（如 KICKED_REASON_LOGGED_IN_ELSEWHERE）；
//      **命中但取不到原因（空载荷/解码失败/未知 version）时 reason 为空串、ok 仍为
//      true，且不得抛错**——状态机按 ok 清凭据，空原因不得让上层保留已失效的凭据。
//      非本会话推送返回 ok=false。
// 不进接缝的成员：续期/刷新、多端标识、序列化器选择（序列化归 ISerializer 插槽）。
//
// 实现约束（项目侧）：实现必须用**生成的会话 DTO**（模板仓按项目生成的 opclient stub）
// 解码回执与推送——会话回执解析失败或关键凭据为空时，Session 会显式抛 ProtocolException，
// 绝不「凭据为空仍报成功」。因此项目二进制必须链接生成的会话 DTO 包（未链接时登录/恢复
// 会以协议错误失败，而不是静默返回空凭据）。
public interface ISessionProtocol
{
    SessionOps Ops { get; }

    string Token(object? msg);

    string PlayerId(object? msg);

    long ExpiresAt(object? msg);

    (string Reason, bool Ok) Kicked(string operation, PushEnvelope payload);
}
