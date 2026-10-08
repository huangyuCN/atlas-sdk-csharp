using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;

namespace Atlas.E2E;

// EndSemanticsCheck 是对局结束语义的跨机验收：跑到对局**自然结束**（单局帧数上限
// max_frames=60 × tick 100ms ≈ 6s 的结算，不是人为触发迟到 op），再断言结束后的口径：
//   ① 结算结束通知到达（BattleEnd 事件）且会话进入终态（HasEnded / EndedReason）；
//   ② 结束后 SDK 不再写线：心跳停表、发帧/补帧被 SDK 拒发（BattleEndedException），
//      证据是会话 Debug 打点里「结束时刻之后再无 send op=...」（含心跳探针，线级口径）；
//   ③ 服务端有界补投（关闭连接前重投 EndRetries=2 次 + 迟到 op 补投）到达时事件仍只触发一次；
//   ④ 收尾窗口后自动关连接（State=Disconnected，重连被终态拒绝且不上报 Failed）。
internal sealed class EndSemanticsCheck
{
    // DrainMs 是收尾窗口（与 SDK 缺省一致；独立写出便于证据行自解释）。
    private const int DrainMs = 2000;

    // SettleTimeout 是等自然结算的上限（帧数上限约 6s，留足冷路径余量）。
    private static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(20);

    private readonly E2EConfig _config;

    public EndSemanticsCheck(E2EConfig config)
    {
        _config = config;
    }

    // RunAsync 跑一次结束语义验收；返回 true = 全部断言通过。
    public async Task<bool> RunAsync(EdgeTransport face, DirectPlan plan, CancellationToken ct)
    {
        var name = EdgeTransports.ShortName(face);
        Console.WriteLine($"[结束] 面={name} 地址={plan.Endpoint(face)} "
            + $"心跳={_config.HeartbeatInterval.TotalMilliseconds:F0}ms/拍 收尾窗口={DrainMs}ms");
        var recorder = new EndRecorder();
        await using var session = BattleSession.Create(plan, Options(face, recorder));
        recorder.Attach(session); // 订阅先于连接：不漏首条推送。
        await session.ConnectAsync(ct).ConfigureAwait(false);
        await session.JoinBattleAsync(ct).ConfigureAwait(false);
        await session.SendFrameInputAsync(1, new byte[] { 0x00 }, ct).ConfigureAwait(false);
        Console.WriteLine("[结束] 已入局并发首帧输入（payload=0 不推进）：等对局自然结算"
            + $"（帧数上限 max_frames=60 × tick 100ms ≈ 6s，上限 {SettleTimeout.TotalSeconds:F0}s）");

        if (!await recorder.WaitEndAsync(SettleTimeout, ct).ConfigureAwait(false))
        {
            Console.WriteLine("[结束] 未在时限内收到结算结束通知：本项未通过");
            return false;
        }
        return await VerifyAsync(session, recorder, name, ct).ConfigureAwait(false);
    }

    // VerifyAsync 在结束信号到达后逐条验收：终态拒发 → 停发 → 幂等 → 收尾窗口 → 关闭。
    private async Task<bool> VerifyAsync(
        BattleSession session, EndRecorder recorder, string face, CancellationToken ct)
    {
        Console.WriteLine($"[结束] 收到结算结束通知：HasEnded={session.HasEnded} 来源={session.EndedReason} "
            + $"载荷={Snippet(recorder.Payload)} 结束前发送打点={recorder.SendsBeforeEnd} 条");
        var input = await ProbeAsync(() => session.SendFrameInputAsync(2, new byte[] { 0x00 }, ct))
            .ConfigureAwait(false);
        var sync = await ProbeAsync(() => session.SyncFramesAsync(ct)).ConfigureAwait(false);
        Console.WriteLine($"[结束] 终态拒发：SendFrameInput={input} SyncFrames={sync}");

        await Task.Delay(300, ct).ConfigureAwait(false); // 让结束瞬间在途的一拍落地，再取证据基线。
        var markMs = recorder.NowMs;
        await Task.Delay(DrainMs + 1500, ct).ConfigureAwait(false); // 覆盖服务端重投 + 收尾窗口。

        var closed = await WaitStateAsync(session, ClientState.Disconnected, TimeSpan.FromSeconds(3))
            .ConfigureAwait(false);
        var watch = Stopwatch.StartNew();
        await session.CloseAsync().ConfigureAwait(false);
        await session.CloseAsync().ConfigureAwait(false); // 幂等。
        var afterEnd = recorder.SendsAfter(markMs);
        Console.WriteLine($"[结束] 事件触发 {recorder.Ends} 次（服务端补投被幂等吸收）、结束后发送打点 "
            + $"{afterEnd.Length} 条、帧广播 {recorder.Broadcasts} 条、"
            + $"State={(closed ? "Disconnected" : session.State.ToString())}、"
            + $"CloseAsync={watch.ElapsedMilliseconds}ms（幂等）、Failed={recorder.Failed} "
            + $"心跳失败={recorder.HeartbeatFailures}");
        return Verdict(recorder, input, sync, closed, watch.ElapsedMilliseconds, afterEnd, face);
    }

    // Options 组装本次会话选项：Debug 打点进 recorder（「结束后零发送」的证据来源）。
    private BattleSessionOptions Options(EdgeTransport face, EndRecorder recorder)
    {
        return new BattleSessionOptions
        {
            PreferredTransport = face,
            HeartbeatInterval = _config.HeartbeatInterval,
            EndDrainWindow = TimeSpan.FromMilliseconds(DrainMs),
            HelloTimeoutMs = 5000,
            InvokeTimeoutMs = 3000,
            BackoffBaseMs = 200,
            BackoffMaxMs = 1000,
            Logger = SDKLoggerFactory.Of(LogLevel.Debug, recorder.AddLogLine),
        };
    }

    // ProbeAsync 记录一次终态调用的结果：期望被 SDK 拒发（BattleEndedException）且不写线。
    private static async Task<string> ProbeAsync(Func<Task<byte[]>> call)
    {
        try
        {
            await call().ConfigureAwait(false);
            return "未被拒发（异常）";
        }
        catch (BattleEndedException exception)
        {
            return $"{exception.GetType().Name}({exception.Reason})";
        }
        catch (Exception exception)
        {
            return $"{exception.GetType().Name}: {exception.Message}";
        }
    }

    // WaitStateAsync 轮询等待会话状态到点（收尾窗口后自动关连接是异步收口）。
    private static async Task<bool> WaitStateAsync(BattleSession session, ClientState state, TimeSpan timeout)
    {
        var watch = Stopwatch.StartNew();
        while (watch.Elapsed < timeout)
        {
            if (session.State == state)
            {
                return true;
            }
            await Task.Delay(25).ConfigureAwait(false);
        }
        return session.State == state;
    }

    // Verdict 汇总判定：任一条不满足即本项未通过（结论行与证据行同源）。
    private static bool Verdict(EndRecorder recorder, string input, string sync, bool closed,
        long closeMs, string[] afterEnd, string face)
    {
        var refused = input.StartsWith("BattleEndedException", StringComparison.Ordinal)
            && sync.StartsWith("BattleEndedException", StringComparison.Ordinal);
        var once = recorder.Ends == 1;
        var stopped = afterEnd.Length == 0;
        var closeFast = closeMs < 2000;
        var ok = refused && once && stopped && closed && !recorder.Failed && closeFast;
        Console.WriteLine($"结束结论（C#/{face}）：终态拒发={refused} 事件仅一次={once} 结束后零发送={stopped} "
            + $"收尾窗口后关连接={closed} 无失败回调={!recorder.Failed} CloseAsync<2s={closeFast} → "
            + (ok ? "通过" : "未达成"));
        foreach (var line in afterEnd)
        {
            Console.WriteLine($"[结束] 结束后仍发送：{line}");
        }
        return ok;
    }

    private static string Snippet(byte[] payload)
    {
        var text = Encoding.UTF8.GetString(payload);
        return text.Length > 200 ? text[..200] + "..." : text;
    }
}

// EndRecorder 收集本次验收的全部客户端可见证据：结束事件次数/时刻/载荷、Debug 发送打点
// （含心跳探针）、帧广播与失败回调——单一 Stopwatch 保证「结束时刻」与「发送时刻」可比。
internal sealed class EndRecorder
{
    private readonly object _gate = new();
    private readonly Stopwatch _watch = Stopwatch.StartNew();
    private readonly List<long> _sends = new();
    private readonly TaskCompletionSource<bool> _endSignal =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Ends { get; private set; }

    public long EndedAtMs { get; private set; } = -1;

    public byte[] Payload { get; private set; } = Array.Empty<byte>();

    public int Broadcasts { get; private set; }

    public bool Failed { get; private set; }

    public int HeartbeatFailures { get; private set; }

    // NowMs 是本次验收的单调时刻（发送打点与结束时刻同一时钟）。
    public long NowMs => _watch.ElapsedMilliseconds;

    // SendsBeforeEnd 是结束时刻之前的发送打点数（基线）。
    public int SendsBeforeEnd
    {
        get
        {
            lock (_gate)
            {
                return CountBeforeLocked(EndedAtMs);
            }
        }
    }

    // Attach 订阅会话事件（在 ConnectAsync 之前调用，避免漏掉首条推送）。
    public void Attach(BattleSession session)
    {
        session.BattleEnd += push =>
        {
            lock (_gate)
            {
                Ends++;
                EndedAtMs = _watch.ElapsedMilliseconds;
                Payload = push.Payload;
            }
            _endSignal.TrySetResult(true);
        };
        session.FrameBroadcast += _ => Broadcasts++;
        session.Failed += _ => Failed = true;
        session.HeartbeatFailed += _ => HeartbeatFailures++;
    }

    // AddLogLine 是会话 Debug 打点的落点：只留「send op=...」发送行（心跳探针同走这条）。
    public void AddLogLine(string line)
    {
        if (!line.Contains("send op=", StringComparison.Ordinal))
        {
            return;
        }
        lock (_gate)
        {
            _sends.Add(_watch.ElapsedMilliseconds);
        }
    }

    // SendsAfter 返回指定时刻之后（含）的发送打点（期望为空：终态零发送）。
    public string[] SendsAfter(long atMs)
    {
        lock (_gate)
        {
            var result = new List<string>();
            foreach (var at in _sends)
            {
                if (at >= atMs)
                {
                    result.Add($"{at}ms（结束于 {EndedAtMs}ms）");
                }
            }
            return result.ToArray();
        }
    }

    // WaitEndAsync 等首个结束通知（超时返回 false，不抛：由调用方给结论）。
    public async Task<bool> WaitEndAsync(TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            await _endSignal.Task.WaitAsync(timeout, ct).ConfigureAwait(false);
            return true;
        }
        catch (System.TimeoutException)
        {
            return false;
        }
    }

    private int CountBeforeLocked(long atMs)
    {
        var count = 0;
        foreach (var at in _sends)
        {
            if (at < atMs)
            {
                count++;
            }
        }
        return count;
    }
}
