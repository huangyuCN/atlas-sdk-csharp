using System;
using System.Collections.Generic;
using System.Threading;

namespace Atlas.Client;

// NotifyHandler 是 Notify 帧回调：收到 operation、原始 payload 与**帧头载荷编码版本**
//（1 = JSON/protojson、2 = protobuf wire；SDK 不做 DTO 解码，业务侧按版本自行反序列化）。
// 版本必须随回调下发：丢版本会让 ver=2 的推送按 protojson 解码而静默失败。
// handler 默认在线程池执行（经 AtlasScheduler 发布；注入调度器后在注入上下文执行），
// 异常被隔离，不影响其他分发。
public delegate void NotifyHandler(string op, byte[] payload, byte version);

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

public sealed partial class Channel
{
    // _notifiesAny 是所有推送 op 的通配订阅表（会话状态机把每个推送 op 交给接缝识别，
    // 见 Session.Bind——接缝的 Kicked(op, PushEnvelope) 语义要求状态机拿到 op 与版本）。
    private readonly List<NotifyEntry> _notifiesAny = new();

    // OnAny 订阅全部推送 op（通配分发；语义同 On：handler 去重、退订幂等、异常隔离、
    // 订阅跨连接代际保留）。会话状态机经它把每个推送交给接缝识别（内核不硬编码推送
    // op）；业务方也可用它观察不预设 op 的推送（对齐 TS 仓 onAny）。
    public NotifySubscription OnAny(NotifyHandler handler)
    {
        if (handler == null)
        {
            throw new ArgumentNullException(nameof(handler));
        }
        lock (_notifyGate)
        {
            if (!_notifiesAny.Exists(entry => ReferenceEquals(entry.Handler, handler)))
            {
                _notifiesAny.Add(new NotifyEntry(handler));
            }
        }
        return new NotifySubscription(() =>
        {
            lock (_notifyGate)
            {
                _notifiesAny.RemoveAll(entry => ReferenceEquals(entry.Handler, handler));
            }
        });
    }

    // ClearSubscriptions 清空全部订阅（CloseAsync 调用）：关闭即释放订阅表——关闭后
    // 不再有任何推送分发（订阅随通道生命周期结束，不跨关闭保留）。
    private void ClearSubscriptions()
    {
        lock (_notifyGate)
        {
            _notifies.Clear();
            _notifiesAny.Clear();
        }
    }
}
