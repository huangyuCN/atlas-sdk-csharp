using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Battle;

// BattleSession 的终态面：对局结束（服务端业务拒绝 BATTLE_ENDED，或收到结算结束通知）后的
// 语义收口——
//   1. 终态：首个结束信号置 HasEnded（可判定），此后发帧/补帧/入局/重连一律
//      BattleEndedException（明确异常，检查在最前、不写线）；
//   2. 停表：停直连保活心跳（终态后不再发 Ping——服务端也会以 BATTLE_ENDED 拒绝迟到探针）；
//   3. 补投幂等：同一局结束通知重复到达（服务端有界补投：每玩家 ≤5 次 + 重连补投）只触发
//      一次 BattleEnd 事件，首个载荷为准，载荷不一致只记 Warn（不重发事件、不改写已交付结果）；
//   4. 收尾窗口：结束后保留 EndDrainWindow 收尾随推送与在途回执，到点自动关连接（停重连）；
//      CloseAsync 取消窗口并 await 收尾任务退出，不留悬挂定时器。
public sealed partial class BattleSession
{
    // _drainGate 保护收尾窗口的起停（CloseAsync 与结束路径可能并发调用）。
    private readonly object _drainGate = new();

    // _ended 是终态标记（0/1，首个结束信号生效且不再复位：一个会话只对应一局）。
    private int _ended;

    // _endedReason 是触发终态的结束信号来源（见 EndedReason）。
    private string? _endedReason;

    // _endNotified 是结束通知事件的一次性闩（0/1）：重复投递不再触发事件。
    private int _endNotified;

    // _endPayload 是首个结束通知载荷（重复投递据此比对，不被改写）。
    private byte[]? _endPayload;

    private CancellationTokenSource? _drainCts;
    private Task? _drainTask;

    // HasEnded 是对局是否已结束（终态判定）：服务端以 BATTLE_ENDED 拒绝任一帧调用，
    // 或收到结算结束通知，即置位；终态下 SDK 不再写线（发帧/补帧/心跳/重连一律拒发）。
    public bool HasEnded => Volatile.Read(ref _ended) != 0;

    // EndedReason 是触发终态的结束信号来源（未结束为 null）：
    // DirectErrors.BattleEndedReason（服务端业务拒绝）或 BattleEndNotifyReason（结束通知先到）。
    public string? EndedReason => Volatile.Read(ref _endedReason);

    // EnterEnded 进入终态（幂等：首个结束信号生效，重复信号不改写来源、不重启窗口）：
    // 置位 → 启动收尾窗口（停心跳 → 等窗口 → 关连接）。返回 true = 本次是首个结束信号。
    private bool EnterEnded(string reason)
    {
        if (Interlocked.CompareExchange(ref _ended, 1, 0) != 0)
        {
            return false;
        }
        Volatile.Write(ref _endedReason, reason);
        StartEndDrain();
        return true;
    }

    // ThrowIfEnded 终态守卫：结束后的发帧/补帧/入局/重连一律明确异常且**不写线**
    //（检查在任何组帧/写线之前，与写线之间无 await 间隙）。
    private void ThrowIfEnded()
    {
        if (Volatile.Read(ref _ended) == 0)
        {
            return;
        }
        var reason = EndedReason ?? "";
        throw new BattleEndedException(
            reason, $"{DirectErrors.SessionEnded}（{reason}）：不再发送帧输入/补帧/心跳");
    }

    // OnBattleEnd 分发战斗结束推送（幂等）：首个结束通知进入终态并触发一次 BattleEnd 事件；
    // 重复投递（服务端关闭前重投 + 重连补投）只比对载荷——不一致记 Warn，事件不重发、
    // 首个载荷不被改写（同一局只有一个结算结果，重复投递不得让上层看到两份）。
    private void OnBattleEnd(string op, byte[] payload, byte version)
    {
        EnterEnded(DirectErrors.BattleEndNotifyReason);
        if (Interlocked.CompareExchange(ref _endNotified, 1, 0) != 0)
        {
            NoteEndReplay(payload);
            return;
        }
        Volatile.Write(ref _endPayload, payload);
        BattleEnd?.Invoke(new BattlePush(op, payload, version));
    }

    // NoteEndReplay 记一次重复结束通知：载荷逐字一致（服务端口径）即静默忽略；不一致记 Warn
    //（首个载荷仍为准）——不一致说明服务端留档与首投漂移，排障需要，但不改客户端已交付结果。
    private void NoteEndReplay(byte[] payload)
    {
        var first = Volatile.Read(ref _endPayload);
        if (SameBytes(first, payload))
        {
            return;
        }
        _logger.Warnf("battle end notify payload mismatch op={0} first={1}B replay={2}B",
            BattleOps.BattleEndNotify, first?.Length ?? 0, payload.Length);
    }

    // StartEndDrain 启动收尾窗口（首个结束信号触发一次，重复信号不延长——否则补投风暴
    // 会把会话无限拖住）：停表 → 等窗口 → 关连接。
    private void StartEndDrain()
    {
        lock (_drainGate)
        {
            if (_drainTask != null)
            {
                return;
            }
            var cts = new CancellationTokenSource();
            _drainCts = cts;
            _drainTask = Task.Run(() => EndDrainAsync(cts.Token));
        }
    }

    // EndDrainAsync 是收尾窗口本体：先停保活心跳（终态不再探活），再等窗口到点（或被
    // CloseAsync 取消），最后关连接（同时终止自动重连——终态不再 hello/入局）。
    private async Task EndDrainAsync(CancellationToken token)
    {
        try
        {
            await StopHeartbeatAsync().ConfigureAwait(false);
            await Task.Delay(_options.EndDrainWindow, token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // CloseAsync 取消窗口：不等窗口到点，直接进入关连接。
        }
        await CloseChannelAsync().ConfigureAwait(false);
    }

    // CancelEndDrain 取消收尾窗口的等待（CloseAsync 调用：关闭不等窗口到点）。
    private void CancelEndDrain()
    {
        lock (_drainGate)
        {
            _drainCts?.Cancel();
        }
    }

    // AwaitEndDrainAsync 等收尾任务退出并释放资源（幂等）：CloseAsync 调用，保证关闭返回后
    // 没有悬挂的定时器/后台任务（收尾任务自己也会关连接，与 CloseAsync 的关连接互为幂等）。
    private async Task AwaitEndDrainAsync()
    {
        CancellationTokenSource? cts;
        Task? task;
        lock (_drainGate)
        {
            cts = _drainCts;
            task = _drainTask;
            _drainCts = null;
            _drainTask = null;
        }
        if (task != null)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // 收尾退出异常不向上传播（关闭路径以关闭成功为准）。
            }
        }
        cts?.Dispose();
    }

    // SameBytes 逐字节比对两次结束通知载荷（长度不同即不一致，不做前缀比较）。
    private static bool SameBytes(byte[]? left, byte[]? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }
        if (left == null || right == null || left.Length != right.Length)
        {
            return false;
        }
        for (var i = 0; i < left.Length; i++)
        {
            if (left[i] != right[i])
            {
                return false;
            }
        }
        return true;
    }
}
