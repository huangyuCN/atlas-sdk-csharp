using System;
using System.Text;
using System.Text.Json;
using Atlas.Battle;

namespace Atlas.E2E;

// GameOps 是战斗域 op 的**示例局部字面量**：游戏域消息与 op 表已迁出 SDK，真实项目请用游戏
// 协议包（NuGet HuangyuCN.AtlasGame.Protocol，源自模板仓 protocol/）生成的 op 常量。
// 示例保持零游戏依赖，故只在这里写线上寻址键。
internal static class GameOps
{
    public const string JoinBattle = "/battle.v1.BattleService/JoinBattle";

    public const string SendFrameInput = "/battle.v1.BattleService/SendFrameInput";

    public const string SyncFrames = "/battle.v1.BattleService/SyncFrames";

    public const string Ping = "/battle.v1.BattleService/Ping";

    public const string FrameBroadcast = "/battle.v1.FrameBroadcast";

    public const string BattleEndNotify = "/battle.v1.BattleEndNotify";
}

// GameWire 是示例侧的最小线上载荷构造与解析（protojson 形态，按协议包 DTO 的字段口径手写）：
// SDK 只负责把这些字节发出去、把回执原样交回来，**不解游戏消息**。
internal static class GameWire
{
    // Join 构造入局请求载荷（对局标识是唯一的客体寻址字段）。
    public static byte[] Join(string battleId)
    {
        return Utf8("{\"battleId\":" + Quote(battleId) + "}");
    }

    // Probe 构造保活探针载荷（与入局同形：只带客体寻址字段）。
    public static byte[] Probe(string battleId)
    {
        return Join(battleId);
    }

    // Input 构造帧输入请求载荷（64 位整数按 protojson 字符串形态，bytes 为 base64）。
    public static byte[] Input(string battleId, ulong frameId, byte[] payload)
    {
        return Utf8("{\"battleId\":" + Quote(battleId) + ",\"input\":{\"frameId\":\"" + frameId
            + "\",\"payload\":\"" + Convert.ToBase64String(payload) + "\"}}");
    }

    // Sync 构造补帧请求载荷（基准 = 已见帧号）。
    public static byte[] Sync(string battleId, ulong lastSeenFrame)
    {
        return Utf8("{\"battleId\":" + Quote(battleId) + ",\"lastSeenFrame\":\"" + lastSeenFrame + "\"}");
    }

    // Replay 返回重连重放步骤（入局 + 按 LastSeenFrame 补帧）：SDK 不再内建这两步，
    // 「是否已入局」由本配置表达。
    public static ReplayStep[] Replay(string battleId)
    {
        return new[]
        {
            new ReplayStep(GameOps.JoinBattle, _ => Join(battleId)),
            new ReplayStep(GameOps.SyncFrames, lastSeen => Sync(battleId, lastSeen)),
        };
    }

    // FrameNumberOf 是帧号提取钩子：从帧广播载荷取帧号（真实项目用协议包解 FrameBroadcast
    // 后取 frame.frame_id；SDK 不再内建任何载荷解析）。
    public static ulong? FrameNumberOf(ReadOnlyMemory<byte> payload)
    {
        try
        {
            using var document = JsonDocument.Parse(payload);
            if (!document.RootElement.TryGetProperty("frame", out var frame)
                || frame.ValueKind != JsonValueKind.Object
                || !frame.TryGetProperty("frameId", out var id))
            {
                return null;
            }
            if (id.ValueKind == JsonValueKind.String && ulong.TryParse(id.GetString(), out var text))
            {
                return text;
            }
            return id.ValueKind == JsonValueKind.Number && id.TryGetUInt64(out var number)
                ? number
                : (ulong?)null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string Quote(string value)
    {
        return JsonSerializer.Serialize(value);
    }

    private static byte[] Utf8(string json)
    {
        return Encoding.UTF8.GetBytes(json);
    }
}
