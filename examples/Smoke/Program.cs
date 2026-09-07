// smoke 是 atlas-sdk-csharp 的真机冒烟程序：连接真实 gateway 服务端，按通道
// 用途验证。形态（TCP / WS / KCP / UDP 单通道）：
//
//   dotnet run --project examples/Smoke -- -transport tcp -addr 127.0.0.1:9001
//   dotnet run --project examples/Smoke -- -transport ws  -addr 127.0.0.1:9002
//   dotnet run --project examples/Smoke -- -transport kcp -addr 127.0.0.1:9003
//   dotnet run --project examples/Smoke -- -transport udp -addr 127.0.0.1:9004
//   dotnet run --project examples/Smoke -- -transport tcp -serializer protobuf
//
// 退出码 0 = 冒烟通过；非 0 = 失败。通过时输出「冒烟通过」结尾行。
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;

namespace Atlas.Smoke;

public static class Program
{
    // ParseArgs 手解 -key value 参数（对标 Go flag 子集；无需第三方 CLI 库）。
    static (string transport, string addr, string wsPath, SmokeMode mode) ParseArgs(string[] args)
    {
        var transport = "tcp";
        var addr = "127.0.0.1:9001";
        var wsPath = "/ws";
        var mode = SmokeMode.Json;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-transport" when i + 1 < args.Length: transport = args[++i]; break;
                case "-addr" when i + 1 < args.Length: addr = args[++i]; break;
                case "-ws-path" when i + 1 < args.Length: wsPath = args[++i]; break;
                case "-serializer" when i + 1 < args.Length:
                    mode = ParseMode(args[++i]);
                    break;
                case "-h" or "--help":
                    Console.WriteLine(
                        "用法: smoke -transport tcp|ws|kcp|udp -addr host:port " +
                        "[-ws-path /ws] [-serializer json|protojson|protobuf]");
                    Environment.Exit(0);
                    break;
            }
        }

        return (transport, addr, wsPath, mode);
    }

    static SmokeMode ParseMode(string value)
    {
        return value switch
        {
            "json" => SmokeMode.Json,
            "protojson" => SmokeMode.ProtoJson,
            "protobuf" => SmokeMode.Protobuf,
            _ => throw new ArgumentException($"未知 -serializer {value}（json|protojson|protobuf）"),
        };
    }

    public static async Task<int> Main(string[] args)
    {
        try
        {
            var (transport, addr, wsPath, mode) = ParseArgs(args);
            var ops = new SmokeOps(mode);
            Console.WriteLine($"[冒烟] 通道: {transport} {addr}, 载荷编码: {ops.ModeName(mode)}");

            // 网关按通道用途绑定协议（模板 D6）：TCP/WS 注册认证业务 op；
            // KCP/UDP 仅注册战斗协议，走通道验证形态（Ping/JoinBattle 业务拒绝）。
            await using var channel = await ChannelDial.DialAsync(transport, addr, wsPath, mode, CancellationToken.None);
            if (transport is "kcp" or "udp")
            {
                await RunBattleChannelAsync(ops, channel, transport);
            }
            else
            {
                await RunBusinessAsync(ops, channel, transport);
            }

            Console.WriteLine("冒烟通过（真机链路验证完成）");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[冒烟] 失败: {ex.Message}");
            for (var inner = ex.InnerException; inner != null; inner = inner.InnerException)
            {
                Console.Error.WriteLine($"  └─ {inner.GetType().Name}: {inner.Message}");
            }
            return 1;
        }
    }

    // RunBusinessAsync 业务通道（TCP/WS）冒烟：注册 → 登录 → 业务心跳 3 次 → Ping 往返。
    static async Task RunBusinessAsync(SmokeOps ops, Channel channel, string transport)
    {
        var ct = CancellationToken.None;
        var account = $"smoke-{Environment.TickCount64}";
        var player = await ops.RegisterAsync(channel, account, ct);
        Console.WriteLine($"[冒烟] 注册成功 playerId={player}");

        var (loginPlayer, token) = await ops.LoginAsync(channel, player, ct);
        Console.WriteLine($"[冒烟] 登录成功 playerId={loginPlayer} token 已存");

        for (var i = 0; i < 3; i++)
        {
            await ops.HeartbeatAsync(channel, loginPlayer, token, ct);
        }
        Console.WriteLine("[冒烟] 业务心跳 3 次往返 OK");

        if (!await ops.PingAsync(channel, ct))
        {
            throw new Exception(transport + " 传输心跳 Ping 往返失败（链路未恢复）");
        }
        Console.WriteLine("[冒烟] 传输心跳 Ping 往返 OK（" + transport + "）");
    }

    // RunBattleChannelAsync 战斗协议通道（KCP/UDP）冒烟：Ping 往返 + JoinBattle
    // 业务拒绝验证 payload 编解码（对标 Go runBattleChannelSmoke）。
    static async Task RunBattleChannelAsync(SmokeOps ops, Channel channel, string transport)
    {
        var ct = CancellationToken.None;
        if (!await ops.PingAsync(channel, ct))
        {
            throw new Exception(transport + " 通道往返探针失败（链路未恢复）");
        }
        Console.WriteLine($"[冒烟] {transport} 通道往返探针 OK");

        var rejected = await ops.TryJoinBattleAsync(channel, ct);
        if (!rejected)
        {
            throw new Exception($"{transport} JoinBattle 应被拒绝（伪造 token），却成功");
        }
        Console.WriteLine($"[冒烟] {transport} JoinBattle 业务拒绝（payload 编解码正确）");
    }
}
