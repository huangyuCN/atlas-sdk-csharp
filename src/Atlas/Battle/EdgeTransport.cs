using System;
using Atlas.Errors;

namespace Atlas.Battle;

// EdgeTransport 是接入层的传输面（对齐 battle.v1.EdgeTransport 枚举，数值即 proto 数值）。
// 成局通知按面下发地址：客户端按自身支持的传输面取地址，缺该面即明确报错。
public enum EdgeTransport
{
    // Ws WebSocket（TCP 承载）：升级请求带票（query 或 X-Atlas-Ticket 头）。
    Ws = 1,

    // Kcp KCP（可靠 UDP）：同一 socket 上先 hello 段换 flow-id，再起 KCP 会话。
    Kcp = 2,

    // Udp 裸 UDP：首包 hello 段换 flow-id，其后双向都带 flow-id 前缀。
    Udp = 3,
}

// EdgeTransports 是传输面的文本口径唯一解析点：
//   - protojson 枚举名（EDGE_TRANSPORT_WS / _KCP / _UDP，服务端下发形态）；
//   - 短名（ws / kcp / udp，Option 与日志形态）。
// 未指定（EDGE_TRANSPORT_UNSPECIFIED）与未知取值一律报错，不静默回落。
public static class EdgeTransports
{
    // ProtoEnumPrefix 是 protojson 枚举名的前缀（battle.v1.EdgeTransport）。
    public const string ProtoEnumPrefix = "EDGE_TRANSPORT_";

    // TryParse 按枚举名或短名解析传输面（大小写不敏感）；未知/空返回 false。
    public static bool TryParse(string? text, out EdgeTransport face)
    {
        face = default;
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }
        var name = text.Trim();
        if (name.StartsWith(ProtoEnumPrefix, StringComparison.OrdinalIgnoreCase))
        {
            name = name[ProtoEnumPrefix.Length..];
        }
        switch (name.ToLowerInvariant())
        {
            case "ws":
                face = EdgeTransport.Ws;
                return true;
            case "kcp":
                face = EdgeTransport.Kcp;
                return true;
            case "udp":
                face = EdgeTransport.Udp;
                return true;
            default:
                return false;
        }
    }

    // Parse 解析传输面；未知取值抛 ProtocolException（不可重试：地址来源非法）。
    public static EdgeTransport Parse(string? text)
    {
        if (!TryParse(text, out var face))
        {
            throw new ProtocolException($"{DirectErrors.TransportUnknown}: {text ?? "<null>"}");
        }
        return face;
    }

    // ShortName 返回短名（ws / kcp / udp；Option 与日志口径）。
    public static string ShortName(EdgeTransport face)
    {
        return face switch
        {
            EdgeTransport.Ws => "ws",
            EdgeTransport.Kcp => "kcp",
            EdgeTransport.Udp => "udp",
            _ => throw new ProtocolException($"{DirectErrors.TransportUnknown}: {(int)face}"),
        };
    }

    // WireName 返回 protojson 枚举名（EDGE_TRANSPORT_*；服务端下发形态）。
    public static string WireName(EdgeTransport face)
    {
        return ProtoEnumPrefix + ShortName(face).ToUpperInvariant();
    }
}
