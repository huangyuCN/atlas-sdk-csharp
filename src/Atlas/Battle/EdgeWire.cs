using System;
using System.Text;
using Atlas.Errors;
using Atlas.Transport;

namespace Atlas.Battle;

// EdgeWire 是接入层线格式的唯一实现点（与框架 contrib/edge、transport/frame 逐字节对齐）：
//   - hello 段：magic(4)="ATLH" || version(1)=1 || 票长(2,大端) || 票密文；
//   - 帧会话槽：base64url RawURLEncoding（无填充）的票密文；
//   - WS 升级地址：ws://host:port/path?ticket=<base64url(票)>；
//   - 数据报 flow-id：8 字节大端，双向「flow-id || 载荷」。
// 服务端为唯一裁判：本类只做编码/解码，不猜端口、不做静默回落。
public static class EdgeWire
{
    // HelloHeaderLen 是 hello 段固定头长度（magic 4 + 版本 1 + 票长 2）。
    public const int HelloHeaderLen = 7;

    // HelloVersion 是 hello 段格式版本（本轮 = 1）。
    public const byte HelloVersion = 1;

    // HelloMagicText 是 hello 段魔数（4 字节 ASCII）。
    public const string HelloMagicText = "ATLH";

    // FlowIdLen 是 flow-id 的线格式长度（8 字节大端；唯一实现在 EdgeFlow）。
    public const int FlowIdLen = EdgeFlow.FlowIdLen;

    // TicketQueryKey 是 WS 升级请求 query 里承载票据的参数名（唯一实现：EdgeHandshake）。
    public const string TicketQueryKey = EdgeHandshake.TicketQueryKey;

    // TicketHeaderKey 是 WS 升级请求里承载票据的头名（唯一实现：EdgeHandshake）。
    public const string TicketHeaderKey = EdgeHandshake.TicketHeaderKey;

    // FrameSessionHeaderKey 是帧会话槽在服务端请求头里的键名
    //（transport/frame.RequestHeaderKeySession）：SDK 侧只作为取值口径的说明。
    public const string FrameSessionHeaderKey = "Atlas-Frame-Session";

    // HelloMagicBytes 返回 hello 段魔数字节（调用方不得改写返回值）。
    public static byte[] HelloMagicBytes()
    {
        return Encoding.ASCII.GetBytes(HelloMagicText);
    }

    // EncodeHello 把票据密文编码为 hello 段（客户端只编不解，服务端只解不编）。
    public static byte[] EncodeHello(byte[]? ticket)
    {
        var payload = ticket ?? Array.Empty<byte>();
        var hello = new byte[HelloHeaderLen + payload.Length];
        hello[0] = (byte)'A';
        hello[1] = (byte)'T';
        hello[2] = (byte)'L';
        hello[3] = (byte)'H';
        hello[4] = HelloVersion;
        hello[5] = (byte)(payload.Length >> 8);
        hello[6] = (byte)payload.Length;
        if (payload.Length > 0)
        {
            Buffer.BlockCopy(payload, 0, hello, HelloHeaderLen, payload.Length);
        }
        return hello;
    }

    // IsHello 报告数据报是否为 hello 段（magic + 版本；数据报面据此区分首包与后续包）。
    public static bool IsHello(byte[]? datagram)
    {
        if (datagram == null || datagram.Length < HelloHeaderLen)
        {
            return false;
        }
        return datagram[0] == (byte)'A' && datagram[1] == (byte)'T'
            && datagram[2] == (byte)'L' && datagram[3] == (byte)'H'
            && datagram[4] == HelloVersion;
    }

    // TicketSlot 返回帧会话槽取值：base64url 无填充（服务端按 RawURLEncoding 反解）。
    public static string TicketSlot(byte[]? ticket)
    {
        return ToBase64Url(ticket ?? Array.Empty<byte>());
    }

    // DecodeTicketSlot 按服务端口径反解帧会话槽（测试与自检用；非法即协议错误）。
    public static byte[] DecodeTicketSlot(string? slot)
    {
        if (string.IsNullOrEmpty(slot))
        {
            return Array.Empty<byte>();
        }
        var padded = slot.Replace('-', '+').Replace('_', '/');
        switch (padded.Length % 4)
        {
            case 2:
                padded += "==";
                break;
            case 3:
                padded += "=";
                break;
        }
        try
        {
            return Convert.FromBase64String(padded);
        }
        catch (FormatException exception)
        {
            throw new ProtocolException($"{DirectErrors.NotifyMalformed}: 帧会话槽不是合法 base64url", exception);
        }
    }

    // WsUrl 拼装接入层 WS 面地址：ws://host:port/path?ticket=<base64url(票)>；path 为空取 "/"。
    public static string WsUrl(string address, byte[]? ticket, string? path)
    {
        return WsUrlPlain(address, path) + "?" + TicketQueryKey + "=" + Uri.EscapeDataString(TicketSlot(ticket));
    }

    // WsUrlPlain 拼装不带票的 WS 面地址（票据走 X-Atlas-Ticket 头时用）。
    public static string WsUrlPlain(string address, string? path)
    {
        if (string.IsNullOrEmpty(address))
        {
            throw new ProtocolException($"{DirectErrors.NotifyNoEndpoint}: WS 地址为空");
        }
        var target = string.IsNullOrEmpty(path)
            ? "/"
            : (path.StartsWith("/", StringComparison.Ordinal) ? path : "/" + path);
        return "ws://" + address + target;
    }

    // EncodeFlowId 把 flow-id 编码为 8 字节大端（唯一实现：EdgeFlow，数据报面双向都带该前缀）。
    public static byte[] EncodeFlowId(ulong flowId)
    {
        return EdgeFlow.EncodeFlowId(flowId);
    }

    // DecodeFlowId 读数据报前 8 字节为 flow-id；不足 8 字节即协议错误。
    public static ulong DecodeFlowId(byte[]? datagram)
    {
        return EdgeFlow.DecodeFlowId(datagram);
    }

    // WrapFlowId 给载荷加 flow-id 前缀（数据报面发出去的方向）。
    public static byte[] WrapFlowId(ulong flowId, byte[]? payload)
    {
        return EdgeFlow.WrapFlowId(flowId, payload);
    }

    // StripFlowId 剥掉 flow-id 前缀并**拷贝**出载荷（读缓冲逐包复用，返回值不得指向入参）；
    // 前缀不符或不足 8 字节即协议错误（调用方按「失配丢弃」处置）。
    public static byte[] StripFlowId(byte[]? datagram, ulong flowId)
    {
        return EdgeFlow.StripFlowId(datagram, flowId);
    }

    // ToBase64Url 用 base64url 无填充编码（与 Go base64.RawURLEncoding 一致）。
    private static string ToBase64Url(byte[] bytes)
    {
        return Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }
}
