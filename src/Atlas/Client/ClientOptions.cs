using System;
using Atlas.Frame;
using Atlas.Serialization;

namespace Atlas.Client;

// SessionHeartbeatRequest 是一次会话心跳的请求描述（业务 Heartbeat 的 op + payload）。
// payload 为 null 表示仅发 operation（空请求）；业务方在闭包内用同一 Serializer
// 序列化请求并携带最新 token。返回空 Operation 表示本轮跳过（业务侧未就绪）。
public readonly struct SessionHeartbeatRequest
{
    public SessionHeartbeatRequest(string operation, byte[]? payload)
    {
        Operation = operation;
        Payload = payload;
    }

    public string Operation { get; }

    public byte[]? Payload { get; }
}

// ChannelOptions 是单通道配置；M4 将使用重连、心跳与队列字段。
public sealed class ChannelOptions
{
    public int HeartbeatIntervalMs { get; set; } = 30_000;

    public int InvokeTimeoutMs { get; set; } = 10_000;

    public int MaxBodySize { get; set; } = FrameConst.MaxBodySize;

    public ISerializer Serializer { get; set; } = new JsonSerializer();

    public bool AutoReconnect { get; set; } = true;

    public int BackoffBaseMs { get; set; } = 500;

    public int BackoffMaxMs { get; set; } = 30_000;

    public int QueueSize { get; set; } = 64;

    public int HookTimeoutMs { get; set; } = 10_000;

    public int HeartbeatFailures { get; set; } = 3;

    // SessionHeartbeatIntervalMs 是会话心跳（业务续租）周期（毫秒）；≤0 表示关闭
    //（默认）。须小于服务端会话租期（建议租期/2，对标 Go WithSessionHeartbeat 注释）；
    // 仅业务通道生效（dual 形态门控在 Client/通道角色层，M4-5）。
    public int SessionHeartbeatIntervalMs { get; set; }

    // SessionHeartbeatOpFactory 构造每次会话心跳的 (op, payload)：业务方闭包携带
    // 最新 token 并自行序列化请求（用与通道一致的 Serializer）。返回空 Operation
    // 表示本轮跳过（尚未登录无 token）。非空时与 SessionHeartbeatIntervalMs>0 共同
    // 启用会话心跳（对齐 Go WithSessionHeartbeat 的 interval>0 && opFactory!=nil）。
    public Func<SessionHeartbeatRequest>? SessionHeartbeatOpFactory { get; set; }
}
