using System;
using System.Threading.Tasks;
using Atlas.Frame;
using Atlas.Serialization;

namespace Atlas.Client;

// TransportKind 是通道传输类型（对标 Go client.Transport；零值 Tcp 对齐
// Go 零值 TransportTCP）。帧会话槽仅在无连接传输（Udp/Kcp）的请求帧上启用。
public enum TransportKind
{
    Tcp = 0,
    Ws = 1,
    Kcp = 2,
    Udp = 3,
}

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

    public int MaxBodySize { get; set; } = FrameGen.MaxBodySize;

    public ISerializer Serializer { get; set; } = new JsonSerializer();

    public bool AutoReconnect { get; set; } = true;

    public int BackoffBaseMs { get; set; } = 500;

    public int BackoffMaxMs { get; set; } = 30_000;

    public int QueueSize { get; set; } = 64;

    public int HookTimeoutMs { get; set; } = 10_000;

    // Logger 是调试日志实现（WithLog* 构造的 SDKLogger；null + LogSilence=false
    // = 默认 Error 级 Console.Error；对齐 Go WithLog* Option）。
    public SDKLogger? Logger { get; set; }

    // LogSilence 完全静默（显式关闭默认 Error 输出；对齐 Go WithLogSilence）。
    public bool LogSilence { get; set; }

    // LogOut 输出目标委托（可空；WithLog 输出器内部经它可写自定义目标）。
    public Action<string>? LogSink { get; set; }

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

    // TransportKind 声明本通道传输类型（拨号工厂不可自描述，由调用方声明）：
    // 帧会话槽（SessionTokenProvider）仅对 Udp/Kcp（无连接）传输生效；Tcp/Ws
    // 长连接按连接绑定身份、不携带会话槽（对齐 Go frameSessionSlot 推导）。
    public TransportKind TransportKind { get; set; }

    // ForceFrameSessionSlot 强制请求帧携带帧会话槽（直连战斗帧面必须逐帧带票：即使 WS 是
    // 长连接，经接入层透传后 battle 帧面仍按帧槽验票）。默认 false = 按传输类型推导
    //（UDP/KCP 带槽、TCP/WS 不带），老路径行为不变。
    public bool ForceFrameSessionSlot { get; set; }

    // DialFailureAbortsReconnect 判定「拨号失败是否终止重连」：接入层拒绝 hello（票据无效/
    // 过期）时，用同一张票重试必然再被拒——重连循环应立即终止而不是无限退避重拨。
    // null（默认）= 全部按可重试处理，老路径行为不变。
    public Func<Exception, bool>? DialFailureAbortsReconnect { get; set; }

    // OnReconnectAborted 是重连因不可重试的拨号失败而终止时的回调（参数为终止原因）：
    // 上层据此重新匹配 / 报错。回调异常被隔离（不影响重连终止）。
    public Action<Exception>? OnReconnectAborted { get; set; }

    // SessionTokenProvider 是会话凭据提供者（Session 对象装配；业务层亦可自给）：
    // 无连接传输（UDP/KCP）的请求帧据此自动携带会话槽（FrameGen.FlagSession），
    // 服务端按凭据验证身份；长连接（TCP/WS）不携带。闭包返回空串表示当前无会话
    //（匿名帧，如登录前的 Login 请求）（对齐 Go WithSessionTokenProvider）。
    public Func<string>? SessionTokenProvider { get; set; }

    // OnReconnected 是本通道重连成功后的会话钩子（对标 Go WithOnReconnected）：
    // Session 对象经此装配断线自动恢复钩子；语义同 ChannelConfig.ReconnectHook
    //（重连编排同步执行、hookBypass 直通窗口）。AtlasClient 装配层同时配置
    // ChannelConfig.ReconnectHook 时以 ReconnectHook 为准（通道级覆盖）。
    public Func<Task>? OnReconnected { get; set; }

    // WriteGuard 是写线前的复核钩子（在**写锁内**、真正写帧之前调用；null = 不复核）：
    // 战斗会话据此在「组帧与写线之间」复核终态，保证终态置位后零写线
    //（已在写锁内的那一笔不回滚——对齐 Go writeRequest 的终态双检）。
    // 钩子抛出的异常原样上抛（不包装成网络错误）：终态拒绝必须是可判定的业务/终态异常。
    public Action? WriteGuard { get; set; }

    // HookMaxAttempts 是重连钩子（会话重登/重新入局）的最大尝试次数（>0；缺省 10）：
    // 超过即终止本次重连并通知 OnReconnectAborted——重试必须有界，否则钩子持续失败时
    // 会无限退避重拨（服务端持续拒绝/链路长期不可用等场景永不退出）。单次尝试仍受
    // HookTimeoutMs 约束。
    // 业务通道与战斗通道同样生效（本选项属通道级）；≤0 按 1 处理（至少执行一次钩子，
    // 仍不会无界重试）——战斗会话装配层 BattleSessionOptions 对同项取更严格口径：
    // ≤0 在装配期直接报错（不留「配了却不生效」的窗口）。
    public int HookMaxAttempts { get; set; } = 10;
}
