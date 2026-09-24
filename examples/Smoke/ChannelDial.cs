using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Serialization;
using Atlas.Transport;

namespace Atlas.Smoke;

// ChannelDial 封装形态差异的拨号入口：按 transport 类型构造 AtlasClient（单业务通道）
// 的拨号工厂。
public static class ChannelDial
{
    // ParseEndpoint 解析 host:port（IPv4 字面量/域名 + 端口；冒烟目标为局域网/本机）。
    public static (string host, int port) ParseEndpoint(string address)
    {
        var idx = address.LastIndexOf(':');
        if (idx <= 0 || idx == address.Length - 1)
        {
            throw new ArgumentException($"非法地址（需 host:port）: {address}");
        }
        var host = address.Substring(0, idx);
        var port = int.Parse(address.Substring(idx + 1));
        return (host, port);
    }

    // DialAsync 建立单通道 AtlasClient（关闭自动传输心跳——冒烟用显式 Ping 探针验证
    // 往返，避免后台心跳与业务断言交织；重连编排属 M4 范围，此处单连接）。
    public static async Task<AtlasClient> DialAsync(
        string transport, string addr, string wsPath, SmokeMode mode, CancellationToken ct)
    {
        var (host, port) = ParseEndpoint(addr);
        var options = new ChannelOptions
        {
            Serializer = mode == SmokeMode.Protobuf
                ? (ISerializer)new ProtobufSerializer()
                : new JsonSerializer(),
            HeartbeatIntervalMs = 0, // 冒烟用显式 Ping；后台心跳关 M4 编排
            InvokeTimeoutMs = 5_000,
            // 冒烟定位即调试工具：Debug 级收发打点（请求/响应 JSON + 幂等键），
            // 与服务端日志的 request_id 一一对应（三语言 SDK 行为对齐验证）。
            Logger = SDKLoggerFactory.Of(LogLevel.Debug),
        };

        Func<CancellationToken, Task<ITransport>> dial;
        switch (transport)
        {
            case "tcp":
                dial = token => TcpTransport.ConnectAsync(host, port, token);
                break;
            case "ws":
                {
                    var url = WsTransport.NormalizeUrl(host + ":" + port, wsPath);
                    dial = token => WsTransport.ConnectAsync(url, token);
                    break;
                }
            case "kcp":
                dial = token => KcpTransport.ConnectAsync(host, port, token);
                break;
            case "udp":
                dial = token => UdpTransport.ConnectAsync(host, port, token);
                break;
            default:
                throw new ArgumentException($"未知传输 {transport}（tcp|ws|kcp|udp）");
        }

        var client = new AtlasClient(new ChannelConfig(ChannelKind.Business, dial) { Options = options });
        await client.ConnectAsync(ct);
        return client;
    }
}
