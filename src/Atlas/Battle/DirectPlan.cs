using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Text.Json;
using Atlas.Errors;

namespace Atlas.Battle;

// DirectPlan 是成局推送（MatchStartedNotify）解析出的直连作战计划：对局标识 + 本人票据 +
// 「传输面 → 接入层地址」列表。票与地址都只来自服务端下发——缺票/空票/缺面一律明确报错，
// 绝不猜端口、不静默换面（票为空连不上接入层，只会把服务端问题伪装成网络问题）。
//
// 载荷口径（规范形态唯一）：
//   - 客户端收到的是帧 Notify：operation = 推送 op（消息完整名 /game.v1.MatchStartedNotify），
//     payload = **protojson 通知本体**——帧 operation 由调用方按 BattleOps.MatchStartedNotify
//     订阅时已剥离，本入口拿到的就是本体字节；
//   - {type, payload} 信封只存在于 NATS 事件总线侧（网关自己解包后转发），SDK 侧看不到；
//     本入口仍**防御性**兼容该形态（探测到 payload 是对象即先解包），但不作为规范形态。
// 兼容接受：proto 字段名 snake_case 与 protojson 默认 lowerCamelCase 两套键名；
// UTF-8 字节或字符串两种入口。
public sealed class DirectPlan
{
    private DirectPlan(string matchId, string battleId, byte[] ticket,
        IReadOnlyDictionary<EdgeTransport, string> endpoints)
    {
        MatchId = matchId;
        BattleId = battleId;
        Ticket = ticket;
        Endpoints = endpoints;
    }

    // MatchId 是对局 ID（protojson 字符串字段，缺省空串）。
    public string MatchId { get; }

    // BattleId 是战斗实例 ID（JoinBattle/SyncFrames 的客体外键）。
    public string BattleId { get; }

    // Ticket 是本人入场票据密文（接入层 hello 与帧会话槽都用它）。
    public byte[] Ticket { get; }

    // Endpoints 是「传输面 → host:port」只读快照。
    public IReadOnlyDictionary<EdgeTransport, string> Endpoints { get; }

    // FromNotify 解析推送载荷文本（protojson 或推送信封 JSON）。
    public static DirectPlan FromNotify(string payload)
    {
        if (payload == null)
        {
            throw new ArgumentNullException(nameof(payload));
        }
        return FromNotify(Encoding.UTF8.GetBytes(payload));
    }

    // FromNotify 解析推送载荷字节（帧通道下发的原始 payload）。
    public static DirectPlan FromNotify(byte[] payload)
    {
        if (payload == null)
        {
            throw new ArgumentNullException(nameof(payload));
        }
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(payload);
        }
        catch (JsonException exception)
        {
            throw new ProtocolException($"{DirectErrors.NotifyMalformed}: 不是合法 JSON: {exception.Message}", exception);
        }
        using (document)
        {
            return ParseObject(Unwrap(document.RootElement));
        }
    }

    // Endpoint 返回指定面的接入层地址；未下发该面即报错（不猜端口、不静默换面）。
    public string Endpoint(EdgeTransport face)
    {
        if (Endpoints.TryGetValue(face, out var address))
        {
            return address;
        }
        throw new ProtocolException(
            $"{DirectErrors.TransportNotFound}: {EdgeTransports.ShortName(face)}（已下发: {FaceList()}）");
    }

    // TryEndpoint 取指定面的接入层地址（未下发返回 false，不抛错）。
    public bool TryEndpoint(EdgeTransport face, out string address)
    {
        return Endpoints.TryGetValue(face, out address!);
    }

    // Unwrap 防御性兼容 NATS 信封形态 {type, payload}（规范形态是 protojson 本体，见类注释）：
    // payload 为对象时下钻一层，否则按本体解析。
    private static JsonElement Unwrap(JsonElement root)
    {
        if (root.ValueKind == JsonValueKind.Object
            && TryGetProperty(root, "payload", out var inner)
            && inner.ValueKind == JsonValueKind.Object)
        {
            return inner;
        }
        return root;
    }

    private static DirectPlan ParseObject(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new ProtocolException($"{DirectErrors.NotifyMalformed}: 载荷不是 JSON 对象");
        }
        var matchId = ReadString(root, "match_id", "matchId");
        var battleId = ReadString(root, "battle_id", "battleId");
        return new DirectPlan(matchId, battleId, ReadTicket(root), ReadEndpoints(root));
    }

    // ReadTicket 读取 battle_ticket：缺失或空串即报错；base64 带填充/无填充都接受。
    private static byte[] ReadTicket(JsonElement root)
    {
        if (!TryGetProperty(root, "battle_ticket", "battleTicket", out var node)
            || node.ValueKind != JsonValueKind.String
            || string.IsNullOrEmpty(node.GetString()))
        {
            throw new ProtocolException($"{DirectErrors.NotifyNoTicket}: 字段缺失或为空");
        }
        var text = node.GetString()!;
        // protojson 的 bytes 为标准 base64（带填充）；无填充与 url-safe 变体一并容忍。
        var normalized = text.Replace('-', '+').Replace('_', '/');
        switch (normalized.Length % 4)
        {
            case 2:
                normalized += "==";
                break;
            case 3:
                normalized += "=";
                break;
        }
        try
        {
            return Convert.FromBase64String(normalized);
        }
        catch (FormatException exception)
        {
            throw new ProtocolException($"{DirectErrors.NotifyMalformed}: battle_ticket 不是合法 base64", exception);
        }
    }

    // ReadEndpoints 读取 endpoints 列表：未知面/空地址条目跳过，一个可用面都没有即报错。
    private static IReadOnlyDictionary<EdgeTransport, string> ReadEndpoints(JsonElement root)
    {
        var endpoints = new Dictionary<EdgeTransport, string>();
        if (TryGetProperty(root, "endpoints", out var list) && list.ValueKind == JsonValueKind.Array)
        {
            foreach (var entry in list.EnumerateArray())
            {
                var face = ReadFace(entry);
                if (face == null)
                {
                    continue;
                }
                var address = ReadAddress(entry);
                if (address.Length > 0)
                {
                    endpoints[face.Value] = address;
                }
            }
        }
        if (endpoints.Count == 0)
        {
            throw new ProtocolException($"{DirectErrors.NotifyNoEndpoint}: 未下发任何可用面地址");
        }
        return new ReadOnlyDictionary<EdgeTransport, string>(endpoints);
    }

    // ReadFace 读取一个 endpoint 条目的传输面（未知面名返回 null = 跳过该条目）。
    private static EdgeTransport? ReadFace(JsonElement entry)
    {
        if (entry.ValueKind != JsonValueKind.Object
            || !TryGetProperty(entry, "transport", out var node)
            || node.ValueKind != JsonValueKind.String
            || !EdgeTransports.TryParse(node.GetString(), out var face))
        {
            return null;
        }
        return face;
    }

    // ReadAddress 读取一个 endpoint 条目的地址（缺失/非字符串/空白返回空串 = 跳过）。
    private static string ReadAddress(JsonElement entry)
    {
        if (!TryGetProperty(entry, "address", out var node) || node.ValueKind != JsonValueKind.String)
        {
            return "";
        }
        return node.GetString()?.Trim() ?? "";
    }

    // ReadString 读取字符串字段（两种键名都试；缺失/非字符串返回空串）。
    private static string ReadString(JsonElement root, string snake, string camel)
    {
        if (!TryGetProperty(root, snake, camel, out var node) || node.ValueKind != JsonValueKind.String)
        {
            return "";
        }
        return node.GetString() ?? "";
    }

    // TryGetProperty 按 proto 字段名与 protojson 默认键名两种形态取属性（兼容两套命名）。
    private static bool TryGetProperty(JsonElement root, string snake, string camel, out JsonElement value)
    {
        return root.TryGetProperty(snake, out value) || root.TryGetProperty(camel, out value);
    }

    // TryGetProperty 取单一键名（信封形态的 payload 键）。
    private static bool TryGetProperty(JsonElement root, string name, out JsonElement value)
    {
        return root.TryGetProperty(name, out value);
    }

    // FaceList 返回已下发面的短名列表（错误信息里点名可选项，便于排查下发配置）。
    private string FaceList()
    {
        var names = new List<string>();
        foreach (var face in Endpoints.Keys)
        {
            names.Add(EdgeTransports.ShortName(face));
        }
        names.Sort(StringComparer.Ordinal);
        return names.Count == 0 ? "无" : string.Join(",", names);
    }
}
