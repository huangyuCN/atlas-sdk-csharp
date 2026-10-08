using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;

namespace Atlas.E2E;

// MatchPair 是一次成局的两名玩家（A 为「主视角」玩家，B 只为凑齐两人开局）。
//
// 每一步开新局都用**全新账号 + 全新连接**：模板的匹配映射在成局后不清理
//（redis 键 TTL = 成局票据 TTL），同一玩家立刻再次入队会被「已在匹配中」拒绝——
// 复用玩家会让第二面之后的闭环假失败，与 SDK 无关。
internal sealed class MatchPair : IAsyncDisposable
{
    private readonly BusinessPlayer _playerA;
    private readonly BusinessPlayer _playerB;
    private readonly CancellationTokenSource _beat = new();
    private readonly Task _beatTask;

    private MatchPair(BusinessPlayer playerA, BusinessPlayer playerB, DirectPlan plan)
    {
        _playerA = playerA;
        _playerB = playerB;
        Plan = plan;
        _beatTask = BeatAsync();
    }

    // Plan 是主视角玩家 A 的直连计划（票据 + 接入层「面 → 地址」）。
    public DirectPlan Plan { get; }

    // CreateAsync 建两名新玩家（连接 + 注册 + 登录）→ 双人入队 → 等成局通知。
    public static async Task<MatchPair> CreateAsync(E2EConfig config, string phase, CancellationToken ct)
    {
        var suffix = Environment.TickCount64;
        var playerA = await BusinessPlayer.ConnectAsync(config, "A", ct).ConfigureAwait(false);
        var playerB = await BusinessPlayer.ConnectAsync(config, "B", ct).ConfigureAwait(false);
        try
        {
            await playerA.RegisterAndLoginAsync($"e2e-{suffix}-{phase}-a", ct).ConfigureAwait(false);
            await playerB.RegisterAndLoginAsync($"e2e-{suffix}-{phase}-b", ct).ConfigureAwait(false);
            var plan = await QueueAndWaitAsync(playerA, playerB, ct).ConfigureAwait(false);
            return new MatchPair(playerA, playerB, plan);
        }
        catch
        {
            await playerA.DisposeAsync().ConfigureAwait(false);
            await playerB.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    // QueueAndWaitAsync 双人入队并等成局通知（先上膛再入队，避免推送早于等待槽）。
    private static async Task<DirectPlan> QueueAndWaitAsync(
        BusinessPlayer playerA, BusinessPlayer playerB, CancellationToken ct)
    {
        var waitA = playerA.ArmMatch();
        var waitB = playerB.ArmMatch();
        await playerA.EnterMatchQueueAsync(ct).ConfigureAwait(false);
        await playerB.EnterMatchQueueAsync(ct).ConfigureAwait(false);
        var timeout = TimeSpan.FromSeconds(30);
        var planA = await waitA.WaitAsync(timeout, ct).ConfigureAwait(false);
        var planB = await waitB.WaitAsync(timeout, ct).ConfigureAwait(false);
        if (planA.BattleId != planB.BattleId)
        {
            throw new InvalidOperationException(
                $"两名玩家收到不同对局：A={planA.BattleId} B={planB.BattleId}");
        }
        Console.WriteLine($"[业务] 成局通知：match_id={planA.MatchId} battle_id={planA.BattleId} "
            + $"票据={planA.Ticket.Length}B 接入层面={Endpoints(planA)}");
        return planA;
    }

    // BeatAsync 后台业务心跳（网关会话 ttl=30s；长阶段不断租）。
    private async Task BeatAsync()
    {
        while (!_beat.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(8000, _beat.Token).ConfigureAwait(false);
                await _playerA.HeartbeatAsync(_beat.Token).ConfigureAwait(false);
                await _playerB.HeartbeatAsync(_beat.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception)
            {
                // 心跳失败只记：阶段内的真实断言自会暴露会话问题。
            }
        }
    }

    // Endpoints 打印成局通知下发的「面 → 地址」列表。
    private static string Endpoints(DirectPlan plan)
    {
        var parts = new System.Collections.Generic.List<string>();
        foreach (var pair in plan.Endpoints)
        {
            parts.Add(EdgeTransports.ShortName(pair.Key) + "→" + pair.Value);
        }
        return string.Join(",", parts);
    }

    public async ValueTask DisposeAsync()
    {
        _beat.Cancel();
        try
        {
            await _beatTask.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // 心跳收尾异常忽略。
        }
        _beat.Dispose();
        await _playerA.DisposeAsync().ConfigureAwait(false);
        await _playerB.DisposeAsync().ConfigureAwait(false);
    }
}
