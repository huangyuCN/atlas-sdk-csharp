using System;
using System.Threading;

namespace Atlas.Client;

// NotifyHandler 是 Notify 帧回调：收到 operation 与原始 payload（SDK 不做 DTO
// 解码，业务侧自行反序列化）。handler 默认在线程池执行（经 AtlasScheduler 发布；
// 注入调度器后在注入上下文执行），异常被隔离，不影响其他分发。
public delegate void NotifyHandler(string op, byte[] payload);

// NotifySubscription 是退订句柄：Dispose 后该订阅不再收到 Notify 帧；重复 Dispose 安全。
public sealed class NotifySubscription : IDisposable
{
    private Action? _off;

    internal NotifySubscription(Action off)
    {
        _off = off;
    }

    public void Dispose()
    {
        var off = _off;
        if (off != null && Interlocked.CompareExchange(ref _off, null, off) == off)
        {
            off();
        }
    }
}

// NotifyEntry 是订阅表中的一条（handler 去重键 + 回调）。
internal sealed class NotifyEntry
{
    public NotifyEntry(NotifyHandler handler)
    {
        Handler = handler;
    }

    public NotifyHandler Handler { get; }
}
