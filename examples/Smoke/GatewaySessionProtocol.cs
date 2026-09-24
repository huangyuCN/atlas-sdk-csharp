using System;
using Atlas.Client;
using Atlas.Frame;
using Atlas.Gateway.V1;
using Atlas.Serialization;
using Google.Protobuf.Reflection;
using ProtoKickedNotify = global::Gateway.V1.KickedNotify;
using ProtoKickedReason = global::Gateway.V1.KickedReason;
using ProtoSessionReflection = global::Gateway.V1.SessionReflection;

namespace Atlas.Smoke;

// GatewaySessionProtocol 是项目侧的会话协议接缝实现（R13）：5 个 op 名、三个凭据
// 提取器与推送识别全部取自**生成物**（api/gateway/v1/opclient/session_client.g.cs，
// 由 scripts/gen-dto.sh 从模板仓 descriptor set 生成），SDK 会话状态机只依赖
// ISessionProtocol——本类即「生成物 → 接缝」的适配层。
//
// 接入示例（一行）：
//     var session = new Session(new SessionOptions { Protocol = GatewaySessionProtocol.Instance });
public sealed class GatewaySessionProtocol : ISessionProtocol
{
    // Instance 是无状态单例（回执解码用 ver=1 protojson 序列化器；ver=2 二进制回执
    // 由 IMessage 族 DTO 另行解码——POCO 路径仅 ver=1，见 R12 边界）。推送按帧头
    // version 选解码器（ver=1 → POCO、ver=2 → IMessage），两条路都取到同一枚举名。
    public static GatewaySessionProtocol Instance { get; } = new();

    private readonly ISerializer _serializer;
    private readonly ISerializer _protobufSerializer;

    public GatewaySessionProtocol(ISerializer? serializer = null, ISerializer? protobufSerializer = null)
    {
        _serializer = serializer ?? new JsonSerializer();
        _protobufSerializer = protobufSerializer ?? new ProtobufSerializer();
    }

    // Ops 返回生成物的 5 个会话 op（接缝是 op 名的唯一来源，SDK 不写字面量）。
    public SessionOps Ops { get; } = new SessionOps
    {
        Register = SessionProtocolOps.Register,
        Login = SessionProtocolOps.Login,
        Resume = SessionProtocolOps.Resume,
        Logout = SessionProtocolOps.Logout,
        Heartbeat = SessionProtocolOps.Heartbeat,
    };

    // Token 从会话回执/请求取凭据（生成物提取器 SessionProtocolDescriptor.SessionToken）。
    public string Token(object? msg)
    {
        return SessionProtocolDescriptor.SessionToken(Decode(msg));
    }

    // PlayerId 从会话回执/请求取玩家 ID（生成物提取器）。
    public string PlayerId(object? msg)
    {
        return SessionProtocolDescriptor.SessionPlayerID(Decode(msg));
    }

    // ExpiresAt 从会话回执取过期时间（模板 session.proto 无 expiry 字段：恒 0 = 未知，
    // 本轮不启用续期，R13）。
    public long ExpiresAt(object? msg)
    {
        return SessionProtocolDescriptor.SessionExpiresAt(Decode(msg));
    }

    // Kicked 判定推送 op 是否为「被挤下线」并提取原因（S0.5 修订 1：第二参是推送信封，
    // 含帧头 version）。**op 命中即 ok=true**——原因取不到时 reason 为空串，状态机照常
    // 清凭据；空原因绝不能让上层保留已失效的凭据。
    public (string Reason, bool Ok) Kicked(string operation, PushEnvelope payload)
    {
        if (operation != SessionPushOps.KickedNotify)
        {
            return ("", false);
        }
        return (ReasonOf(payload), true);
    }

    // ReasonOf 按帧头 version 选解码器（1 = JSON/POCO、2 = protobuf/IMessage）：未知
    // version、空载荷或解码失败一律返回空串，**不得抛错**（推送解码失败不影响连接，
    // 原因缺失由状态机按「已命中」处理）。
    private string ReasonOf(PushEnvelope payload)
    {
        if (payload.Body.Length == 0)
        {
            return "";
        }
        try
        {
            if (payload.Version == FrameGen.Version)
            {
                var poco = (KickedNotify?)_serializer.Deserialize(payload.Body, typeof(KickedNotify));
                return poco?.Reason ?? "";
            }
            if (payload.Version == FrameGen.Version2)
            {
                var proto = (ProtoKickedNotify)_protobufSerializer.Deserialize(
                    payload.Body, typeof(ProtoKickedNotify));
                return ProtoEnumName(proto.Reason);
            }
        }
        catch (Exception)
        {
            return ""; // 载荷与 version 不符/解码失败：原因取不到，ok 仍为 true。
        }
        return ""; // 未知 version：不得抛错。
    }

    // ProtoEnumName 取 proto 枚举的原始名（如 KICKED_REASON_LOGGED_IN_ELSEWHERE）：
    // protoc 生成的 C# 枚举名去掉了下划线，而 protojson 下发的是原始枚举名——用生成
    // 描述符取原始名，保证 ver=1（JSON 字符串）与 ver=2（protobuf 数值）原因同口径。
    private static string ProtoEnumName(ProtoKickedReason reason)
    {
        var descriptor = ProtoSessionReflection.Descriptor.FindTypeByName<EnumDescriptor>("KickedReason");
        return descriptor?.FindValueByNumber((int)reason)?.Name ?? "";
    }

    // Decode 把接缝的消息视图（op + 原始字节）解成生成物的会话 DTO：按 op 选回执类型
    // （生成物是消息形状的唯一来源，适配层只做 op → 类型映射）。
    private object? Decode(object? msg)
    {
        if (msg is not SessionMessage message || message.Payload.Length == 0)
        {
            return null;
        }
        var type = ReplyTypeOf(message.Operation);
        return type == null ? null : _serializer.Deserialize(message.Payload, type);
    }

    // ReplyTypeOf 是 op → 会话回执 DTO 的映射（未注册的 op 返回 null：接缝钩子按
    // 「无该字段」返回零值，与可选钩子语义一致）。
    private static Type? ReplyTypeOf(string operation)
    {
        if (operation == SessionProtocolOps.Register)
        {
            return typeof(RegisterReply);
        }
        if (operation == SessionProtocolOps.Login)
        {
            return typeof(LoginReply);
        }
        if (operation == SessionProtocolOps.Resume)
        {
            return typeof(ResumeReply);
        }
        if (operation == SessionProtocolOps.Logout)
        {
            return typeof(LogoutReply);
        }
        if (operation == SessionProtocolOps.Heartbeat)
        {
            return typeof(HeartbeatReply);
        }
        if (operation == SessionPushOps.KickedNotify)
        {
            return typeof(KickedNotify);
        }
        return null;
    }
}
