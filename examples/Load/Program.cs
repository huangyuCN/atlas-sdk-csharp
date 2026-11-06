// C# 客户端压测：多连接 → 注册登录 → 入队匹配 → 战斗直连（ws/udp/kcp）→ 按 pps 发帧 + SyncFrames 探针。
//
// 与 Go 侧 lt-driver、TS 侧 battle-load.mjs 同口径，用于验证 C# SDK 在真实游戏负载下的表现。
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Atlas.Battle;
using Atlas.Client;
using Atlas.E2E;
using Atlas.Serialization;
using Atlas.Transport;

namespace Atlas.Load;

// BusinessOps 是业务域 op 的示例局部字面量（与 E2E 示例同源；SDK 不持游戏域 op 表）。
internal static class BusinessOps
{
    public const string Register = "/gateway.v1.Session/Register";
    public const string Login = "/gateway.v1.Session/Login";
    public const string Heartbeat = "/gateway.v1.Session/Heartbeat";
    public const string EnterMatchQueue = "/game.v1.PlayerService/EnterMatchQueue";
    public const string MatchStartedNotify = "/game.v1.MatchStartedNotify";
}

// Player 是一条业务链路（网关 TCP）+ 一名压测玩家。
internal sealed class Player : IAsyncDisposable
{
    private const string Password = "pw";

    private readonly AtlasClient _client;
    private readonly object _gate = new();
    private TaskCompletionSource<DirectPlan>? _pending;
    private readonly CancellationTokenSource _beat = new();
    private Task? _beatTask;

    private Player(AtlasClient client, string tag)
    {
        _client = client;
        Tag = tag;
    }

    // Tag 是本玩家的短标识。
    public string Tag { get; }

    // PlayerId 是注册回执里的玩家 ID。
    public string PlayerId { get; private set; } = "";

    // Plan 是成局后的直连计划（票据 + 接入层各面地址）。
    public DirectPlan? Plan { get; private set; }

    // ConnectAsync 建业务通道、订阅成局通知、注册并登录。
    public static async Task<Player> CreateAsync(string gateway, string tag, CancellationToken ct)
    {
        var (host, port) = Split(gateway);
        var options = new ChannelOptions
        {
            Serializer = new Atlas.Serialization.JsonSerializer(),
            InvokeTimeoutMs = 8000,
            HeartbeatIntervalMs = 10_000,
        };
        var client = new AtlasClient(new ChannelConfig(
            ChannelKind.Business,
            token => TcpTransport.ConnectAsync(host, port, token)) { Options = options });
        await client.ConnectAsync(ct).ConfigureAwait(false);

        var p = new Player(client, tag);
        client.On(BusinessOps.MatchStartedNotify, p.OnMatchStarted);
        await p.RegisterAndLoginAsync(ct).ConfigureAwait(false);
        p._beatTask = p.BeatAsync();
        return p;
    }

    // RegisterAndLoginAsync 注册（账号唯一）并登录。
    private async Task RegisterAndLoginAsync(CancellationToken ct)
    {
        var account = "csload-" + Tag + "-" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var reg = Utf8("{\"account\":" + Quote(account) + ",\"password\":" + Quote(Password)
            + ",\"nickname\":" + Quote(account) + "}");
        var reply = await _client.InvokeRawAsync(BusinessOps.Register, reg, ct).ConfigureAwait(false);
        PlayerId = Field(reply, "playerId");
        if (PlayerId.Length == 0)
        {
            throw new InvalidOperationException("注册回执缺 playerId");
        }
        var login = Utf8("{\"playerId\":" + Quote(PlayerId) + ",\"password\":" + Quote(Password)
            + ",\"clientVersion\":" + Quote(AtlasVersion.Value) + ",\"clientEnd\":" + Quote("cs-load") + "}");
        await _client.InvokeRawAsync(BusinessOps.Login, login, ct).ConfigureAwait(false);
    }

    // ArmMatch 上膛一个成局等待槽，返回等待任务。
    public Task<DirectPlan> ArmMatch()
    {
        var pending = new TaskCompletionSource<DirectPlan>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            _pending = pending;
        }
        return pending.Task;
    }

    // EnqueueAsync 入队匹配。
    public Task EnqueueAsync(CancellationToken ct)
        => _client.InvokeRawAsync(BusinessOps.EnterMatchQueue, Utf8("{\"ruleset\":\"casual\"}"), ct);

    // BeatAsync 后台业务心跳（网关会话 ttl 较短，长阶段不断租）。
    private async Task BeatAsync()
    {
        while (!_beat.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(8000, _beat.Token).ConfigureAwait(false);
                await _client.InvokeRawAsync(BusinessOps.Heartbeat, Utf8("{}"), _beat.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception)
            {
                // 心跳失败只记不抛：真实断言由主链路负责。
            }
        }
    }

    // OnMatchStarted 收到成局通知：解析直连计划并交给等待槽。
    private void OnMatchStarted(string op, byte[] payload, byte version)
    {
        DirectPlan plan;
        try
        {
            plan = DirectPlan.FromNotify(payload);
        }
        catch (Exception)
        {
            return;
        }
        lock (_gate)
        {
            Plan = plan;
            _pending?.TrySetResult(plan);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _beat.Cancel();
        if (_beatTask is not null)
        {
            try
            {
                await _beatTask.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // 收尾异常忽略。
            }
        }
        _beat.Dispose();
        await _client.DisposeAsync().ConfigureAwait(false);
    }

    private static (string Host, int Port) Split(string addr)
    {
        var i = addr.LastIndexOf(':');
        return (addr[..i], int.Parse(addr[(i + 1)..]));
    }

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);
    private static string Quote(string s) => "\"" + s.Replace("\"", "\\\"") + "\"";

    // Field 从 protojson 回执里取一个顶层字符串字段。
    private static string Field(byte[] json, string name)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString() ?? ""
            : "";
    }
}

// Session 是一条战斗直连会话 + 它的统计。
internal sealed class BattleConn : IAsyncDisposable
{
    private BattleConn(BattleSession session, DirectPlan plan)
    {
        Session = session;
        Plan = plan;
    }

    // Session 是 SDK 的战斗会话。
    public BattleSession Session { get; }

    // Plan 是该玩家的直连计划。
    public DirectPlan Plan { get; }

    // _frames 是收到的帧广播条数（只经原子自增写）。
    private long _frames;

    // Frames 返回收到的帧广播条数。
    public long Frames => Interlocked.Read(ref _frames);

    // CreateAsync 建会话并连接入局；接入层拒连按退避重试。
    public static async Task<BattleConn> CreateAsync(EdgeTransport face, DirectPlan plan, CancellationToken ct)
    {
        Exception? last = null;
        for (var attempt = 1; attempt <= 10; attempt++)
        {
            var session = BattleSession.Create(plan, new BattleSessionOptions
            {
                PreferredTransport = face,
                HeartbeatInterval = TimeSpan.FromSeconds(3),
                ProbeOp = GameOps.Ping,
                ProbePayload = GameWire.Probe(plan.BattleId),
                Replay = GameWire.Replay(plan.BattleId),
                FrameNumberOf = GameWire.FrameNumberOf,
                HelloTimeoutMs = 5000,
                InvokeTimeoutMs = 5000,
            });
            var conn = new BattleConn(session, plan);
            try
            {
                session.OnPush(GameOps.FrameBroadcast, _ => Interlocked.Increment(ref conn._frames));
                await session.ConnectAsync(ct).ConfigureAwait(false);
                await session.InvokeAsync(GameOps.JoinBattle, GameWire.Join(plan.BattleId), ct)
                    .ConfigureAwait(false);
                return conn;
            }
            catch (Exception err)
            {
                last = err;
                await session.CloseAsync().ConfigureAwait(false);
                await Task.Delay(250, ct).ConfigureAwait(false);
            }
        }
        throw last ?? new InvalidOperationException("直连失败");
    }

    public async ValueTask DisposeAsync() => await Session.CloseAsync().ConfigureAwait(false);
}

internal static class Program
{
    private static string _gateway = "10.10.1.23:9001";
    private static int _players = 100;
    private static int _pps = 15;
    private static int _probeMs = 1000;
    private static int _durationMs = 60_000;
    private static int _conc = 32;
    private static EdgeTransport _face = EdgeTransport.Ws;

    private static async Task<int> Main(string[] argv)
    {
        Parse(argv);
        var t0 = DateTime.UtcNow;
        Console.Error.WriteLine($"=== C# 压测 face={EdgeTransports.ShortName(_face)} players={_players} pps={_pps} duration={_durationMs}ms ===");

        var players = new List<Player>();
        var regFail = 0;
        await ParallelFor(_players, _conc, async i =>
        {
            try
            {
                var p = await Player.CreateAsync(_gateway, "p" + i, CancellationToken.None).ConfigureAwait(false);
                lock (players)
                {
                    players.Add(p);
                }
            }
            catch (Exception err)
            {
                if (Interlocked.Increment(ref regFail) <= 3)
                {
                    Console.Error.WriteLine($"[注册失败] p{i}: {err.Message}");
                }
            }
        });
        Console.Error.WriteLine($"[阶段] 注册登录 {players.Count}/{_players}（失败 {regFail}）+{(DateTime.UtcNow - t0).TotalSeconds:F1}s");

        var waited = players.Select(p => p.ArmMatch()).ToArray();
        foreach (var p in players)
        {
            try
            {
                await p.EnqueueAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception err)
            {
                Console.Error.WriteLine($"[入队失败] {p.Tag}: {err.Message}");
            }
        }
        var matched = 0;
        foreach (var w in waited)
        {
            try
            {
                await w.WaitAsync(TimeSpan.FromSeconds(90)).ConfigureAwait(false);
                matched++;
            }
            catch (Exception)
            {
                // 未成局：计入 matched 差额。
            }
        }
        var plans = players.Where(p => p.Plan is not null).ToArray();
        Console.Error.WriteLine($"[阶段] 成局 {plans.Length}（通知 {matched}）+{(DateTime.UtcNow - t0).TotalSeconds:F1}s");

        var conns = new List<BattleConn>();
        var connFail = 0;
        await ParallelFor(plans.Length, _conc, async i =>
        {
            try
            {
                var c = await BattleConn.CreateAsync(_face, plans[i].Plan!, CancellationToken.None).ConfigureAwait(false);
                lock (conns)
                {
                    conns.Add(c);
                }
            }
            catch (Exception err)
            {
                if (Interlocked.Increment(ref connFail) <= 3)
                {
                    Console.Error.WriteLine($"[直连失败] {plans[i].Tag}: {err.Message}");
                }
            }
        });
        Console.Error.WriteLine($"[阶段] 直连入局 {conns.Count}/{plans.Length}（失败 {connFail}）+{(DateTime.UtcNow - t0).TotalSeconds:F1}s");

        var rtts = new List<double>();
        var rttGate = new object();
        long inputs = 0, inputErr = 0, probeErr = 0;
        var inputEvery = Math.Max(1, 1000 / _pps);
        var start = Stopwatch.StartNew();
        var endsAt = _durationMs;

        await Task.WhenAll(conns.Select(async c =>
        {
            ulong frame = 1;
            long lastProbe = -_probeMs;
            while (start.ElapsedMilliseconds < endsAt)
            {
                if (start.ElapsedMilliseconds - lastProbe >= _probeMs)
                {
                    lastProbe = start.ElapsedMilliseconds;
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        await c.Session.InvokeAsync(
                            GameOps.SyncFrames, GameWire.Sync(c.Plan.BattleId, Math.Max(1, frame - 1)),
                            CancellationToken.None).ConfigureAwait(false);
                        var ms = sw.Elapsed.TotalMilliseconds;
                        lock (rttGate)
                        {
                            rtts.Add(ms);
                        }
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref probeErr);
                    }
                }
                try
                {
                    await c.Session.InvokeAsync(
                        GameOps.SendFrameInput, GameWire.Input(c.Plan.BattleId, frame++, new byte[] { 0 }),
                        CancellationToken.None).ConfigureAwait(false);
                    Interlocked.Increment(ref inputs);
                }
                catch (Exception)
                {
                    Interlocked.Increment(ref inputErr);
                }
                await Task.Delay(inputEvery).ConfigureAwait(false);
            }
        }));

        rtts.Sort();
        var elapsed = start.Elapsed.TotalSeconds;
        double P(double p) => rtts.Count == 0 ? 0 : rtts[Math.Min(rtts.Count - 1, (int)(rtts.Count * p))];
        var summary = new
        {
            client = "csharp",
            transport = EdgeTransports.ShortName(_face).ToLowerInvariant(),
            players_requested = _players,
            players_registered = players.Count,
            players_matched = plans.Length,
            connections_established = conns.Count,
            register_failed = regFail,
            connect_failed = connFail,
            duration_s = Math.Round(elapsed, 1),
            input_pps = Math.Round(inputs / elapsed, 1),
            inputs_sent = inputs,
            input_errors = inputErr,
            probe_errors = probeErr,
            probe_ok = rtts.Count,
            probe_rtt_ms = new
            {
                count = rtts.Count,
                p50_ms = Math.Round(P(0.5), 2),
                p95_ms = Math.Round(P(0.95), 2),
                p99_ms = Math.Round(P(0.99), 2),
                max_ms = rtts.Count == 0 ? 0 : Math.Round(rtts[^1], 2),
            },
            frames_received = conns.Sum(c => c.Frames),
            process_rss_mb = Environment.WorkingSet / 1048576,
            dotnet = Environment.Version.ToString(),
        };
        Console.WriteLine("SUMMARY " + System.Text.Json.JsonSerializer.Serialize(summary));

        await ParallelFor(conns.Count, 32, async i => await conns[i].DisposeAsync().ConfigureAwait(false));
        await ParallelFor(players.Count, 32, async i => await players[i].DisposeAsync().ConfigureAwait(false));
        return 0;
    }

    // Parse 解析 `--k v` 参数。
    private static void Parse(string[] argv)
    {
        for (var i = 0; i + 1 < argv.Length; i += 2)
        {
            var k = argv[i].TrimStart('-');
            var v = argv[i + 1];
            switch (k)
            {
                case "players": _players = int.Parse(v); break;
                case "pps": _pps = int.Parse(v); break;
                case "probe": _probeMs = int.Parse(v); break;
                case "duration": _durationMs = int.Parse(v); break;
                case "conc": _conc = int.Parse(v); break;
                case "gateway": _gateway = v; break;
                case "face":
                    if (!EdgeTransports.TryParse(v, out _face))
                    {
                        throw new ArgumentException($"未知面 {v}（ws|udp|kcp）");
                    }
                    break;
            }
        }
    }

    // ParallelFor 以固定并发度跑 n 个任务。
    private static Task ParallelFor(int n, int conc, Func<int, Task> fn)
    {
        var next = -1;
        var workers = Enumerable.Range(0, Math.Min(conc, Math.Max(1, n))).Select(async _ =>
        {
            for (;;)
            {
                var i = Interlocked.Increment(ref next);
                if (i >= n)
                {
                    return;
                }
                await fn(i).ConfigureAwait(false);
            }
        });
        return Task.WhenAll(workers);
    }
}
