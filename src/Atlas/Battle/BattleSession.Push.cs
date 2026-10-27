using System;
using System.Collections.Generic;

namespace Atlas.Battle;

// BattleSession 的推送出口面：**统一出口**（Push 事件 + OnPush 按 op 订阅）覆盖全部下行推送
//——帧广播、战斗结束、玩家出局，以及未来新增/未预设的 op（出口不写死 op 名单）。
// 载荷与帧头版本原样透传：SDK 不解任何游戏域消息（ver=1 protojson / ver=2 protobuf 二进制
// 都由调用方用游戏协议包按版本解码）。
//
// 会话自身只为两处语义认 op（WireOps，纯寻址键）：帧广播推进补帧基准、战斗结束通知进入终态
// 并留档首投载荷（见 BattleSession.Ended.cs）。其余 op 一律只分发、不解释。
//
// 专用事件（FrameBroadcast / BattleEnd / PlayerOut）已随游戏域消息迁出删除：一条游戏消息
// 一个公开事件等于把游戏域的语义搬回 SDK，调用方用协议包的 op 常量 OnPush 订阅即可。
public sealed partial class BattleSession
{
    // _pushGate 保护按 op 的订阅表（OnPush/退订与分发可能并发）。
    private readonly object _pushGate = new();
    private readonly Dictionary<string, List<Action<BattlePush>>> _pushSubscriptions = new();

    // Push 是统一推送出口：每个推送帧在此分发一次（含未预设 op），上层只挂一个回调即可观测
    // 全部下行推送（事件形式）。
    public event Action<BattlePush>? Push;

    // OnPush 按 op 订阅推送（与 Push 事件同源、同一分发顺序），返回退订句柄；
    // 同一 (op, handler) 重复注册只保留一份，重复退订安全。
    // 回调拿到 (op, 原始载荷, 帧头版本)：op 取自游戏协议包，ver=2 二进制载荷须由调用方按
    // version 选自己的协议包解码（SDK 不预解游戏消息）。
    public IDisposable OnPush(string op, Action<BattlePush> handler)
    {
        if (handler == null)
        {
            throw new ArgumentNullException(nameof(handler));
        }
        if (string.IsNullOrEmpty(op))
        {
            throw new ArgumentException("operation 不能为空", nameof(op));
        }
        lock (_pushGate)
        {
            if (!_pushSubscriptions.TryGetValue(op, out var handlers))
            {
                handlers = new List<Action<BattlePush>>();
                _pushSubscriptions[op] = handlers;
            }
            if (!handlers.Contains(handler))
            {
                handlers.Add(handler);
            }
        }
        return new PushSubscription(() =>
        {
            lock (_pushGate)
            {
                if (_pushSubscriptions.TryGetValue(op, out var handlers))
                {
                    handlers.Remove(handler);
                    if (handlers.Count == 0)
                    {
                        _pushSubscriptions.Remove(op);
                    }
                }
            }
        });
    }

    // OnChannelPush 是通道通配订阅的入口（所有推送 op 都进这里）：会话自身要认的两条语义
    //（帧广播 → 推进补帧基准；战斗结束 → 终态 + 留档）先处理，其余一律只走统一出口。
    private void OnChannelPush(string op, byte[] payload, byte version)
    {
        if (op == WireOps.FrameBroadcast)
        {
            TrackLastSeenFrame(payload);
        }
        else if (op == WireOps.BattleEndNotify)
        {
            OnBattleEnd(op, payload, version);
            return; // 首投才分发/进终态（补投与空载荷由 OnBattleEnd 收口）。
        }
        DispatchPush(new BattlePush(op, payload, version));
    }

    // DispatchPush 把一条推送交给统一出口（Push 事件 + 按 op 订阅）：回调逐个隔离，
    // 任一回调抛异常不影响其余出口与读循环（对齐通道层的 SafeNotify 口径）。
    private void DispatchPush(BattlePush push)
    {
        InvokePushHandler(Push, push);
        Action<BattlePush>[] handlers;
        lock (_pushGate)
        {
            if (!_pushSubscriptions.TryGetValue(push.Op, out var list) || list.Count == 0)
            {
                return;
            }
            handlers = list.ToArray();
        }
        foreach (var handler in handlers)
        {
            try
            {
                handler(push);
            }
            catch (Exception)
            {
                // 单订阅异常隔离。
            }
        }
    }

    // InvokePushHandler 调用一个推送回调（异常隔离：回调抛错不影响其余分发）。
    private static void InvokePushHandler(Action<BattlePush>? handler, BattlePush push)
    {
        if (handler == null)
        {
            return;
        }
        try
        {
            handler(push);
        }
        catch (Exception)
        {
            // 上层回调异常隔离（不影响读循环与其余出口）。
        }
    }

    // TrackLastSeenFrame 推进补帧基准：**只**走调用方注入的帧号钩子。
    // SDK 不持游戏域消息、也不猜载荷编码（ver=1 的 protojson 同样不解析）：未注入 ⇒ 不推进、
    // 也不报错，LastSeenFrame 保持调用方给的值（补帧退化为全量）。
    private void TrackLastSeenFrame(byte[] payload)
    {
        var hook = _options.FrameNumberOf;
        if (hook == null)
        {
            return;
        }
        ulong? frameId;
        try
        {
            frameId = hook(new ReadOnlyMemory<byte>(payload));
        }
        catch (Exception)
        {
            return; // 上层解码异常不得中断推送分发（与推送回调的异常隔离同一口径）。
        }
        if (frameId.HasValue)
        {
            LastSeenFrame = frameId.Value;
        }
    }
}

// PushSubscription 是按 op 订阅的退订句柄：Dispose 后该订阅不再收到推送；重复 Dispose 安全。
public sealed class PushSubscription : IDisposable
{
    private Action? _off;

    internal PushSubscription(Action off)
    {
        _off = off;
    }

    public void Dispose()
    {
        var off = _off;
        if (off != null && ReferenceEquals(System.Threading.Interlocked.CompareExchange(ref _off, null, off), off))
        {
            off();
        }
    }
}
