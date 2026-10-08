using System;
using Atlas.Client;
using Atlas.Serialization;

namespace Atlas.Battle;

// BattleSessionOptions 是直连战斗会话的配置：面选择、握手/调用超时、重连与请求排队。
// 未显式配置的字段取与业务通道一致的缺省值（重连退避、排队上限、调用超时）。
public sealed class BattleSessionOptions
{
    // PreferredTransport 是优先使用的传输面（可选）：配置后必须已下发，否则明确报错；
    // 未配置时按 TransportPriority 顺序取第一个已下发面。
    public EdgeTransport? PreferredTransport { get; set; }

    // TransportPriority 是面选择顺序（缺省 ws → kcp → udp）：只影响「用哪个面」，
    // 不影响地址本身（地址只来自成局通知）。
    public EdgeTransport[] TransportPriority { get; set; } =
    {
        EdgeTransport.Ws,
        EdgeTransport.Kcp,
        EdgeTransport.Udp,
    };

    // WsPath 是 WS 升级请求路径（缺省 "/"：接入层按升级请求取票，路径由 battle 帧面放行）。
    public string WsPath { get; set; } = "/";

    // TicketInHeader 为 true 时票据放 X-Atlas-Ticket 头、URL 不带 query（缺省走 query）。
    public bool TicketInHeader { get; set; }

    // HelloTimeoutMs 是接入层握手超时（UDP/KCP 等 flow-id；WS 为升级握手超时）。
    // 到点无回应 = 接入层拒绝（协议错误，不重试）。
    public int HelloTimeoutMs { get; set; } = 5000;

    // InvokeTimeoutMs 是单次帧请求的等待回执超时。
    public int InvokeTimeoutMs { get; set; } = 10_000;

    // HeartbeatIntervalMs 是**传输心跳**周期（缺省 0 = 关闭）：通道级判活探针
    //（/atlas.internal.Heartbeat/Ping），失败累计即判死链并触发重连。
    public int HeartbeatIntervalMs { get; set; }

    // HeartbeatInterval 是**直连保活心跳**周期（缺省 2s；TimeSpan.Zero = 关闭）：
    // 无输入期间周期发送 battle 域探针（battle.v1.BattleService/Ping，Tell 无回执），
    // 让帧面保持活跃。服务端掉线窗口 offline_timeout 缺省 15s，数据报面（UDP/KCP）
    // 的空闲读超时取其 1/3（缺省 5s）——**周期必须严格小于该值**，否则两拍之间
    // 就会被判掉线，且 NAT 映射失效后下行帧收不到（表现为「连接还在但没数据」）。
    // 与 HeartbeatIntervalMs 的分工：后者证明本代链路存活（失败即重连），
    // 本项维持服务端帧面活跃（失败只记账，不终止会话）。
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(2);

    // AutoReconnect 为 true 时断线后自动重新 hello 并重新入局（缺省开启）。
    public bool AutoReconnect { get; set; } = true;

    // BackoffBaseMs 是重连退避起始时长（毫秒）。
    public int BackoffBaseMs { get; set; } = 500;

    // BackoffMaxMs 是重连退避上限（毫秒）。
    public int BackoffMaxMs { get; set; } = 30_000;

    // QueueSize 是重连期间的请求排队上限（帧输入在重连窗口内不丢）。
    public int QueueSize { get; set; } = 64;

    // HookTimeoutMs 是重连钩子（重新入局 + 补帧）的单次执行上限。
    public int HookTimeoutMs { get; set; } = 10_000;

    // Serializer 是载荷编码插槽（缺省 protojson）。
    public ISerializer Serializer { get; set; } = new JsonSerializer();

    // Logger 是调试日志实现（null + LogSilence=false = 默认 Error 级 Console.Error）。
    public SDKLogger? Logger { get; set; }

    // LogSilence 完全静默（显式关闭默认 Error 输出）。
    public bool LogSilence { get; set; }

    // LogSink 是日志输出目标（可空）。
    public Action<string>? LogSink { get; set; }

    // Validate 校验配置（装配期快速失败，不留「连上了才发现配置错」的窗口）。
    internal void Validate()
    {
        if (HelloTimeoutMs <= 0)
        {
            throw new ArgumentException("HelloTimeoutMs 必须大于 0", nameof(HelloTimeoutMs));
        }
        if (InvokeTimeoutMs <= 0)
        {
            throw new ArgumentException("InvokeTimeoutMs 必须大于 0", nameof(InvokeTimeoutMs));
        }
        if (HeartbeatInterval < TimeSpan.Zero)
        {
            throw new ArgumentException(
                "HeartbeatInterval 不得为负（TimeSpan.Zero = 关闭保活心跳）", nameof(HeartbeatInterval));
        }
        if (QueueSize <= 0)
        {
            throw new ArgumentException("QueueSize 必须大于 0", nameof(QueueSize));
        }
        if (TransportPriority == null || TransportPriority.Length == 0)
        {
            throw new ArgumentException("TransportPriority 不能为空", nameof(TransportPriority));
        }
        if (Serializer == null)
        {
            throw new ArgumentException("Serializer 不能为空", nameof(Serializer));
        }
    }
}
