using System;
using System.Text;
using System.Text.Json;

namespace Atlas.Tests.Battle;

// TestOps 是测试侧独立声明的线上寻址键（op 名）：**不读实现常量**。
// 游戏域消息（battle/game/matcher 的 DTO 与 op 表）已迁出 SDK，由游戏协议包提供；
// 测试按裸字节 + 字面量 op 驱动会话，与真实调用方「用自己协议包的常量」同一口径。
internal static class TestOps
{
    public const string JoinBattle = "/battle.v1.BattleService/JoinBattle";

    public const string SendFrameInput = "/battle.v1.BattleService/SendFrameInput";

    public const string SyncFrames = "/battle.v1.BattleService/SyncFrames";

    public const string Ping = "/battle.v1.BattleService/Ping";

    public const string FrameBroadcast = "/battle.v1.FrameBroadcast";

    public const string BattleEndNotify = "/battle.v1.BattleEndNotify";

    public const string PlayerOutNotify = "/battle.v1.PlayerOutNotify";

    public const string MatchStartedNotify = "/game.v1.MatchStartedNotify";
}

// TestWire 是测试侧的最小线上载荷构造与解析（protojson 形态，按游戏侧协议包的 DTO 形态手写）：
// 会话只负责把这些字节发出去、把回执原样交回来，不解游戏消息。
internal static class TestWire
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

    // FrameBroadcast 构造一帧帧广播载荷（ver=1 protojson 形态）。
    public static byte[] FrameBroadcast(ulong frameId)
    {
        return Utf8("{\"battleId\":\"b-1\",\"frame\":{\"frameId\":\"" + frameId + "\",\"inputs\":[]}}");
    }

    // EndPayload 构造结算结束通知载荷。
    public static byte[] EndPayload(string winner)
    {
        return Utf8("{\"battleId\":\"b-1\",\"winnerPlayerId\":" + Quote(winner) + "}");
    }

    // FrameNumberOf 是示例/测试侧的帧号提取钩子（真实项目用协议包解 FrameBroadcast 后取帧号）：
    // 会话不再内建任何载荷解析，补帧基准只由调用方注入的钩子推进。
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
            if (id.ValueKind == JsonValueKind.String && ulong.TryParse(id.GetString(), out var fromText))
            {
                return fromText;
            }
            return id.ValueKind == JsonValueKind.Number && id.TryGetUInt64(out var fromNumber)
                ? fromNumber
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
