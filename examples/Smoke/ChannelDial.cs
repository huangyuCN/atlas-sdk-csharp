using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Serialization;
using Atlas.Transport;

namespace Atlas.Smoke;

// ChannelDial 封装形态差异的拨号入口：按 transport 类型构造 Channel 拨号工厂。
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

    // Dial 建立单通道 Channel（关闭自动传输心跳——冒烟用显式 Ping 探针验证往返，
    // 避免后台心跳与业务断言交织；重连编排属 M4 范围，此处单连接）。
    public static async Task<Channel> DialAsync(string transport, string addr, string wsPath, SmokeMode mode, CancellationToken ct)
    {
        var (host, port) = ParseEndpoint(addr);
        var options = new ChannelOptions
        {
            Serializer = mode == SmokeMode.Protobuf
                ? (ISerializer)new ProtobufSerializer()
                : new JsonSerializer(),
            HeartbeatIntervalMs = 0, // 冒烟用显式 Ping；后台心跳关 M4 编排
            InvokeTimeoutMs = 5_000,
        };

        Channel channel;
        switch (transport)
        {
            case "tcp":
                channel = new Channel(
                    token => TcpTransport.ConnectAsync(host, port, token),
                    options);
                break;
            case "ws":
                {
                    var url = WsTransport.NormalizeUrl(host + ":" + port, wsPath);
                    channel = new Channel(
                        token => WsTransport.ConnectAsync(url, token),
                        options);
                    break;
                }
            case "kcp":
                channel = new Channel(
                    token => KcpTransport.ConnectAsync(host, port, token),
                    options);
                break;
            case "udp":
                channel = new Channel(
                    token => UdpTransport.ConnectAsync(host, port, token),
                    options);
                break;
            default:
                throw new ArgumentException($"未知传输 {transport}（tcp|ws|kcp|udp）");
        }

        await channel.ConnectAsync(ct);
        return channel;
    }
}
