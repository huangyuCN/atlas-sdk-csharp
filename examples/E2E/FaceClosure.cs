using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;

namespace Atlas.E2E;

// FaceClosure 跑一次「某面经接入层直连战斗帧面」的闭环：
//   hello/flow-id（数据报面）或 WS 升级带票 → JoinBattle → SendFrameInput → SyncFrames
//   → 收到帧广播推送；每一步都把关键回执打在标准输出上（跨机闭环的证据行）。
internal sealed class FaceClosure
{
    private readonly E2EConfig _config;

    public FaceClosure(E2EConfig config)
    {
        _config = config;
    }

    // RunAsync 跑指定面的一次闭环；任一环节失败即抛出（退出码非 0）。
    public async Task RunAsync(EdgeTransport face, DirectPlan plan, CancellationToken ct)
    {
        var name = EdgeTransports.ShortName(face);
        var address = plan.Endpoint(face);
        var expected = E2EConfig.EdgePort(face);
        var portMatched = address.EndsWith(":" + expected, StringComparison.Ordinal);
        Console.WriteLine($"[闭环] 面={name} 下发地址={address}（期望端口 {expected} → "
            + (portMatched ? "一致" : "不一致（以服务端下发为准）") + "）");

        await using var session = BattleSession.Create(plan, Options(face, plan));
        Console.WriteLine($"[闭环] {name} 会话装配完成：battle_id={plan.BattleId} 票长={plan.Ticket.Length}B "
            + $"握手={Handshake(face)}");
        await session.ConnectAsync(ct).ConfigureAwait(false);
        Console.WriteLine($"[闭环] {name} 经接入层直连就绪（{address} → battle 帧面）");

        // 业务 op 与载荷由示例本地给出（真实项目取游戏协议包的 op 常量与 DTO）：
        // SDK 只负责发出、匹配回执与错误归一。
        var join = await session.InvokeAsync(
            GameOps.JoinBattle, GameWire.Join(plan.BattleId), ct).ConfigureAwait(false);
        Console.WriteLine($"[闭环] {name} JoinBattle 回执 {Snippet(join)}");

        // 输入载荷取 0：示例竞速模拟器按 payload[0] 步进，非 0 会推到终点提前结算。
        var input = await session.InvokeAsync(
            GameOps.SendFrameInput, GameWire.Input(plan.BattleId, 1, new byte[] { 0x00 }), ct).ConfigureAwait(false);
        Console.WriteLine($"[闭环] {name} SendFrameInput(frame=1) 回执 {Snippet(input)}（Tell：服务端不回业务体）");

        var sync = await session.InvokeAsync(
            GameOps.SyncFrames, GameWire.Sync(plan.BattleId, 0), ct).ConfigureAwait(false);
        Console.WriteLine($"[闭环] {name} SyncFrames(last_seen=0) 回执 {Snippet(sync)}");

        var frameId = await WaitBroadcastAsync(session, TimeSpan.FromSeconds(3), ct).ConfigureAwait(false);
        Console.WriteLine($"[闭环] {name} 收到帧广播 op={GameOps.FrameBroadcast} frameId={frameId} "
            + $"（LastSeenFrame={session.LastSeenFrame}）");
        Console.WriteLine($"闭环通过（C#/{name} 经接入层）");
    }

    // Options 组装本次会话选项：优先面锁定本面（缺面即报错，不静默换面）。
    private BattleSessionOptions Options(EdgeTransport face, DirectPlan plan)
    {
        return new BattleSessionOptions
        {
            PreferredTransport = face,
            HeartbeatInterval = _config.HeartbeatInterval,
            // 保活探针与重连重放都显式配置（SDK 不持游戏域消息，不再内建 Ping/Join+Sync）。
            ProbeOp = GameOps.Ping,
            ProbePayload = GameWire.Probe(plan.BattleId),
            Replay = GameWire.Replay(plan.BattleId),
            FrameNumberOf = GameWire.FrameNumberOf,
            HelloTimeoutMs = 5000,
            InvokeTimeoutMs = 5000,
            BackoffBaseMs = 200,
            BackoffMaxMs = 1000,
            Logger = _config.Debug ? Atlas.Client.SDKLoggerFactory.Of(Atlas.Client.LogLevel.Debug) : null,
        };
    }

    // Handshake 返回本面的握手口径（证据行用；与 EdgeWire 的线格式一一对应）。
    private static string Handshake(EdgeTransport face)
    {
        return face == EdgeTransport.Ws
            ? "WS 升级请求带票（?ticket=base64url）"
            : "首包 hello 段（ATLH/票密文）换 flow-id，此后双向带 flow-id 前缀 + 逐帧票槽";
    }

    // WaitBroadcastAsync 等一帧帧广播推送（帧引擎直发；超时即失败）。
    // 按 op 订阅（游戏协议包的推送 op 常量；示例用本地字面量）：载荷由 GameWire 解码取帧号。
    private static async Task<ulong> WaitBroadcastAsync(BattleSession session, TimeSpan timeout, CancellationToken ct)
    {
        var signal = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = session.OnPush(GameOps.FrameBroadcast, push =>
        {
            if (GameWire.FrameNumberOf(push.Payload) is { } frameId)
            {
                signal.TrySetResult(frameId);
            }
        });
        return await signal.Task.WaitAsync(timeout, ct).ConfigureAwait(false);
    }

    private static string Snippet(byte[] payload)
    {
        var text = Encoding.UTF8.GetString(payload);
        return text.Length > 220 ? text[..220] + "..." : text;
    }
}
