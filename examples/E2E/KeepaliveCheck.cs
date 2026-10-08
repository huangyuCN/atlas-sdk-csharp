using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;

namespace Atlas.E2E;

// KeepaliveCheck 是保活验收：在**局中**安排「只发心跳、不发输入」的静默窗口，
// 断言未被判出局、窗口后仍能继续发帧/补帧；同参数关掉保活心跳再跑一次作对照。
//
// 服务端口径（battle.v1 部署事实，驱动据此设计观测点）：
//   - offline_timeout 缺省 15s，数据报面（KCP/UDP）空闲读超时取其 1/3 = 5s：
//     静默超过 5s 即拆连接/逐出对端，此后下行帧收不到（「连接还在但没数据」）；
//   - 单局帧数上限 max_frames=60 × tick 100ms ≈ 6s：窗口会被局的自然结算截断
//     （结算不是掉线，但观测窗口随之封顶，结论行照实报）。
//
// 客户端可见的观测口径（不猜服务端内部状态）：
//   - 帧广播到达时刻：下行仍通 = 直连仍在场（掉线即推送寻址失效，帧停收）；
//   - 静默窗口中途的探针往返：仍被受理 = 未被移出帧面；超时 = 已被拆流。
internal sealed class KeepaliveCheck
{
    // IdleThresholdMs 是数据报面空闲读超时缺省值（offline_timeout 15s / 3）。
    public const int IdleThresholdMs = 5000;

    private readonly E2EConfig _config;

    public KeepaliveCheck(E2EConfig config)
    {
        _config = config;
    }

    // RunAsync 跑一次保活验收（heartbeatEnabled=false 即对照）；返回 true = 结论符合预期。
    public async Task<bool> RunAsync(EdgeTransport face, DirectPlan plan, bool heartbeatEnabled, CancellationToken ct)
    {
        var name = EdgeTransports.ShortName(face);
        var interval = heartbeatEnabled ? _config.HeartbeatInterval : TimeSpan.Zero;
        var tag = heartbeatEnabled ? "保活" : "对照";
        Console.WriteLine($"[{tag}] 面={name} 地址={plan.Endpoint(face)} "
            + $"心跳={(heartbeatEnabled ? $"{interval.TotalMilliseconds:F0}ms/拍" : "关闭")} "
            + $"静默目标={_config.QuietMs}ms 中途探针={_config.ProbeMs}ms");

        await using var session = BattleSession.Create(plan, Options(face, interval));
        var recorder = new BroadcastRecorder(session, heartbeatEnabled);
        var failed = false;
        session.Failed += _ => failed = true;

        await session.ConnectAsync(ct).ConfigureAwait(false);
        var join = await session.JoinBattleAsync(ct).ConfigureAwait(false);
        Console.WriteLine($"[{tag}] 入局完成：JoinBattle 回执 {Snippet(join)}");
        // 输入载荷取 0：示例竞速模拟器按 payload[0] 步进，非 0 会把玩家推到终点提前结算，
        // 掩盖后续保活证据（结算不是掉线，但会把观测窗口截断）。
        await session.SendFrameInputAsync(1, new byte[] { 0x00 }, ct).ConfigureAwait(false);
        Console.WriteLine($"[{tag}] 已发输入 frame=1（payload=0，不推进）；开始静默（只发心跳、不发输入）");

        var silence = await recorder.WaitSilenceAsync(_config.ProbeMs, _config.QuietMs, ct).ConfigureAwait(false);
        var probe = await ProbeAsync(session, ct).ConfigureAwait(false);
        Console.WriteLine($"[{tag}] 静默窗口实测 {silence.ElapsedMs}ms（目标 {_config.QuietMs}ms；"
            + (silence.TruncatedBySettle
                ? "局已自然结算，窗口止于结算"
                : $"单局帧上限 max_frames=60×tick 100ms≈6s，窗口止于中途探针点 {_config.ProbeMs}ms（8s 不可达，见报告）")
            + "）");
        Console.WriteLine($"[{tag}] 窗口内收帧广播 {recorder.Count} 条：最大间隔 {recorder.MaxGapMs}ms、"
            + $"末条距窗口结束 {recorder.SinceLastMs}ms、心跳失败 {recorder.HeartbeatFailures} 次");
        Console.WriteLine($"[{tag}] 中途探针：发帧={(probe.InputOk ? "ok" : probe.InputError)} "
            + $"补帧={(probe.SyncOk ? "ok " + probe.SyncReply : probe.SyncError)} 会话状态={probe.State}");
        return Verdict(heartbeatEnabled, recorder, probe, failed, name);
    }

    // Options 组装会话选项：探针失败要快点暴露，故调用超时取 2.5s（缺省 10s 会拖过对局）。
    private static BattleSessionOptions Options(EdgeTransport face, TimeSpan interval)
    {
        return new BattleSessionOptions
        {
            PreferredTransport = face,
            HeartbeatInterval = interval,
            HelloTimeoutMs = 5000,
            InvokeTimeoutMs = 2500,
            BackoffBaseMs = 200,
            BackoffMaxMs = 1000,
        };
    }

    // ProbeAsync 中途探针：静默窗口跑到探针点后「还能不能发帧/补帧」。
    private static async Task<ProbeResult> ProbeAsync(BattleSession session, CancellationToken ct)
    {
        var result = new ProbeResult { State = session.State, LastSeenFrame = session.LastSeenFrame };
        try
        {
            await session.SendFrameInputAsync(2, new byte[] { 0x00 }, ct).ConfigureAwait(false);
            result.InputOk = true;
        }
        catch (Exception exception)
        {
            result.InputError = exception.GetType().Name + ": " + exception.Message;
        }
        try
        {
            var sync = await session.SyncFramesAsync(ct).ConfigureAwait(false);
            result.SyncOk = true;
            result.SyncReply = Snippet(sync);
        }
        catch (Exception exception)
        {
            result.SyncError = exception.GetType().Name + ": " + exception.Message;
        }
        return result;
    }

    // Verdict 打印结论并给出本项是否通过（结论行与证据行同源）。
    private bool Verdict(bool heartbeatEnabled, BroadcastRecorder recorder, ProbeResult probe, bool failed, string face)
    {
        var tag = heartbeatEnabled ? "保活" : "对照";
        var downlinkMs = recorder.MaxSurvivedMs; // 静默开始后末条下行帧的时刻 = 下行存活时长。
        Console.WriteLine($"[{tag}] 下行存活 {downlinkMs}ms（空闲阈值 {IdleThresholdMs}ms）、"
            + $"战斗结束推送={recorder.BattleEnded}、会话终止回调={failed}");
        if (heartbeatEnabled)
        {
            var crossed = downlinkMs >= IdleThresholdMs;
            var served = probe.InputOk && probe.SyncOk;
            Console.WriteLine($"保活结论（C#/{face}）：静默 {downlinkMs}ms 下行未断（跨过 {IdleThresholdMs}ms 空闲阈值={crossed}）、"
                + $"探针{(served ? "仍可发帧/补帧" : "失败")} → {(crossed && served ? "未被判出局" : "未达成")}");
            return crossed && served;
        }
        var broken = !probe.InputOk || !probe.SyncOk;
        var control = broken || downlinkMs <= IdleThresholdMs + 1000;
        Console.WriteLine($"对照结论（C#/{face}）：同参数关心跳 → 下行存活 {downlinkMs}ms（≈空闲阈值 {IdleThresholdMs}ms 处停收）、"
            + $"探针{(broken ? "被拒/超时" : "仍成功")} → {(control ? "观察到被判掉线" : "未观察到掉线")}");
        return control;
    }

    private static string Snippet(byte[] payload)
    {
        var text = Encoding.UTF8.GetString(payload);
        return text.Length > 200 ? text[..200] + "..." : text;
    }
}

// SilenceWindow 是静默窗口的实测结果。
internal sealed class SilenceWindow
{
    // ElapsedMs 是静默窗口实测时长。
    public long ElapsedMs { get; set; }

    // TruncatedBySettle 为真表示局在窗口跑满前被服务端自然结算（帧数上限）。
    public bool TruncatedBySettle { get; set; }
}

// ProbeResult 是静默窗口探针的结果。
internal sealed class ProbeResult
{
    public bool InputOk { get; set; }

    public bool SyncOk { get; set; }

    public string InputError { get; set; } = "";

    public string SyncError { get; set; } = "";

    public string SyncReply { get; set; } = "";

    public ClientState State { get; set; } = ClientState.Disconnected;

    public ulong LastSeenFrame { get; set; }
}

// BroadcastRecorder 记录静默窗口内的下行帧广播与结束推送（客户端可见的「还在场」证据）。
internal sealed class BroadcastRecorder
{
    private readonly Stopwatch _watch = Stopwatch.StartNew();
    private readonly object _gate = new();
    private readonly List<long> _broadcasts = new();
    private long _silenceStart = -1;
    private long _silenceEnd = -1;

    public BroadcastRecorder(BattleSession session, bool heartbeatEnabled)
    {
        session.FrameBroadcast += _ => Track(_watch.ElapsedMilliseconds);
        session.BattleEnd += _ => BattleEndedAt = _watch.ElapsedMilliseconds;
        if (heartbeatEnabled)
        {
            session.HeartbeatFailed += _ => HeartbeatFailures++;
        }
    }

    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _broadcasts.Count;
            }
        }
    }

    public int HeartbeatFailures { get; private set; }

    public bool BattleEnded => BattleEndedAt > 0;

    public long BattleEndedAt { get; private set; } = -1;

    // MaxGapMs 是相邻帧广播的最大间隔（下行被掐断即出现大缺口）。
    public long MaxGapMs
    {
        get
        {
            lock (_gate)
            {
                long max = 0;
                for (var i = 1; i < _broadcasts.Count; i++)
                {
                    max = Math.Max(max, _broadcasts[i] - _broadcasts[i - 1]);
                }
                return max;
            }
        }
    }

    // MaxSurvivedMs 是静默开始后「最后一次收到下行帧」的时长（下行存活时长）。
    public long MaxSurvivedMs
    {
        get
        {
            lock (_gate)
            {
                return _broadcasts.Count == 0 || _silenceStart < 0 ? 0 : _broadcasts[^1] - _silenceStart;
            }
        }
    }

    // SinceLastMs 是末条帧广播距静默窗口结束的间隔。
    public long SinceLastMs
    {
        get
        {
            lock (_gate)
            {
                return _broadcasts.Count == 0 || _silenceEnd < 0 ? -1 : _silenceEnd - _broadcasts[^1];
            }
        }
    }

    // WaitSilenceAsync 跑静默窗口：先等到探针点（probeMs），局被结算即提前结束。
    public async Task<SilenceWindow> WaitSilenceAsync(int probeMs, int quietMs, CancellationToken ct)
    {
        lock (_gate)
        {
            _silenceStart = _watch.ElapsedMilliseconds;
        }
        var truncated = await WaitAsync(probeMs, ct).ConfigureAwait(false);
        lock (_gate)
        {
            _silenceEnd = _watch.ElapsedMilliseconds;
            return new SilenceWindow
            {
                ElapsedMs = _silenceEnd - _silenceStart,
                TruncatedBySettle = truncated || quietMs <= probeMs,
            };
        }
    }

    // WaitAsync 等到指定时长或局结束（返回 true = 局先结束）。
    private async Task<bool> WaitAsync(int milliseconds, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(milliseconds);
        while (DateTime.UtcNow < deadline && !BattleEnded)
        {
            await Task.Delay(25, ct).ConfigureAwait(false);
        }
        return BattleEnded;
    }

    private void Track(long atMs)
    {
        lock (_gate)
        {
            _broadcasts.Add(atMs);
        }
    }
}
