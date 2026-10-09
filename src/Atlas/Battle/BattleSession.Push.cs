using System;
using System.Collections.Generic;
using System.Text.Json;
using Atlas.Frame;

namespace Atlas.Battle;

// BattleSession 的推送出口面：**统一出口**（Push 事件 + OnPush 按 op 订阅）覆盖战斗域
// 下行推送——帧广播（FrameBroadcast）、战斗结束（BattleEndNotify）、玩家出局
//（PlayerOutNotify），以及未来新增/未预设的 op（统一出口不写死名单）。
//
// 分发顺序（同一推送帧内）：状态推进（帧号/终态）→ Push 事件 → OnPush 订阅 → 专用事件
//（FrameBroadcast / BattleEnd / PlayerOut）。业务回调异常逐个隔离：一个回调抛错不影响
// 其余出口（推送路径不允许被业务回调打断）。
//
// op 常量取自生成物（BattleOps → BattleServicePushOps/PlayerServicePushOps），
// 不在本文件写任何 op 字面量。
public sealed partial class BattleSession
{
    // _pushGate 保护按 op 的订阅表（OnPush/退订与分发可能并发）。
    private readonly object _pushGate = new();
    private readonly Dictionary<string, List<Action<BattlePush>>> _pushSubscriptions = new();

    // FrameBroadcast 是帧广播推送回调（原始载荷 + 帧头载荷编码版本，SDK 不解码 DTO）。
    public event Action<BattlePush>? FrameBroadcast;

    // BattleEnd 是战斗结束推送回调（原始载荷 + 版本；同一局只触发一次，重复投递不重放）。
    public event Action<BattlePush>? BattleEnd;

    // PlayerOut 是玩家出局推送回调（原始载荷 + 版本；多人局可能多次触发，每次出局一人）。
    public event Action<BattlePush>? PlayerOut;

    // Push 是统一推送出口：每个推送帧在此分发一次（含未预设 op 与重复投递的结束通知），
    // 上层只挂一个回调即可观测全部下行推送（事件形式）。
    public event Action<BattlePush>? Push;

    // OnPush 按 op 订阅推送（与 Push 事件同源、同一分发顺序），返回退订句柄；
    // 同一 (op, handler) 重复注册只保留一份，重复退订安全。
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

    // OnChannelPush 是通道通配订阅的入口（所有推送 op 都进这里）：按 op 归口到对应处理，
    // 未预设 op 只走统一出口——不猜语义、不静默丢弃（接口未来新增推送时上层即可见）。
    private void OnChannelPush(string op, byte[] payload, byte version)
    {
        if (op == BattleOps.FrameBroadcast)
        {
            OnFrameBroadcast(op, payload, version);
            return;
        }
        if (op == BattleOps.BattleEndNotify)
        {
            OnBattleEnd(op, payload, version);
            return;
        }
        if (op == BattleOps.PlayerOutNotify)
        {
            OnPlayerOut(op, payload, version);
            return;
        }
        DispatchPush(new BattlePush(op, payload, version));
    }

    // OnFrameBroadcast 分发帧广播：推进 LastSeenFrame 后交给统一出口与专用事件。
    private void OnFrameBroadcast(string op, byte[] payload, byte version)
    {
        TrackLastSeenFrame(payload, version);
        var push = new BattlePush(op, payload, version);
        DispatchPush(push);
        InvokePushHandler(FrameBroadcast, push);
    }

    // OnPlayerOut 分发玩家出局：只走出口（不改会话状态——多人局其余玩家继续对局）。
    private void OnPlayerOut(string op, byte[] payload, byte version)
    {
        var push = new BattlePush(op, payload, version);
        DispatchPush(push);
        InvokePushHandler(PlayerOut, push);
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

    // TrackLastSeenFrame 从帧广播载荷尽力取帧号（ver=1 protojson：{"frame":{"frameId":"9"}}）
    // 用于断线补帧；取不到（ver=2/字段缺失/非 JSON）就保持原值——不猜、不抛错。
    private void TrackLastSeenFrame(byte[] payload, byte version)
    {
        if (version != FrameGen.Version)
        {
            return;
        }
        try
        {
            using var document = JsonDocument.Parse(payload);
            if (!document.RootElement.TryGetProperty("frame", out var frame)
                || frame.ValueKind != JsonValueKind.Object)
            {
                return;
            }
            if (!frame.TryGetProperty("frameId", out var id) && !frame.TryGetProperty("frame_id", out id))
            {
                return;
            }
            if (id.ValueKind == JsonValueKind.String && ulong.TryParse(id.GetString(), out var fromText))
            {
                LastSeenFrame = fromText;
            }
            else if (id.ValueKind == JsonValueKind.Number && id.TryGetUInt64(out var fromNumber))
            {
                LastSeenFrame = fromNumber;
            }
        }
        catch (JsonException)
        {
            // 推送载荷不是 protojson：保持原值（LastSeenFrame 由调用方维护）。
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
