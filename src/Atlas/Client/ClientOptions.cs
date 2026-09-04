using Atlas.Frame;
using Atlas.Serialization;

namespace Atlas.Client;

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
}
