using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Errors;

namespace Atlas.E2E;

// e2e 是 atlas-sdk-csharp 的**跨机闭环驱动**：本机 C# 客户端 → 远端真服务，
// 把两条链路都跑通并把结论打在标准输出上：
//   1. 业务链路：网关（9001/tcp）注册 → 登录（连接即会话）→ 入队匹配 → 收成局通知
//      （battle_ticket + 接入层「面 → 地址」列表）；
//   2. 战斗链路：按面直连接入层（ws 7100 / kcp 7101 / udp 7102），跑
//      JoinBattle → SendFrameInput → SyncFrames → 收到帧广播，三面各跑一次；
//   3. 保活验收：局中「只发心跳、不发输入」的静默窗口，断言未被判出局且之后仍能发帧/补帧；
//      同参数关掉保活心跳再跑一次作对照。
//
// 用法（环境变量门控；未设置 ATLAS_E2E_SERVER 即不运行，绝无默认外网目标）：
//   ATLAS_E2E_SERVER=10.10.9.36 dotnet run --project examples/E2E
//   ATLAS_E2E_SERVER=10.10.9.36 dotnet run --project examples/E2E -- -faces kcp,udp,ws -quiet-ms 8000
// 命令行：-server / -faces / -quiet-ms / -heartbeat-ms / -closure-only / -keepalive-only /
//         -skip-control / -debug
// 退出码：0 = 三面闭环与保活验收全部通过；1 = 存在未通过项；2 = 未指定目标服务器。
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var config = E2EConfig.Parse(args, Environment.GetEnvironmentVariable("ATLAS_E2E_SERVER"));
        if (config == null)
        {
            Usage();
            return 2;
        }
        Console.WriteLine($"[e2e] 目标={config.Server} 网关={config.GatewayAddress} 面=[{FaceList(config)}] "
            + $"静默窗口={config.QuietMs}ms 心跳={config.HeartbeatInterval.TotalMilliseconds:F0}ms");
        var failures = new List<string>();
        try
        {
            await RunAsync(config, failures).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"[e2e] 失败：{exception.GetType().Name}: {exception.Message}");
            for (var inner = exception.InnerException; inner != null; inner = inner.InnerException)
            {
                Console.Error.WriteLine($"  └─ {inner.GetType().Name}: {inner.Message}");
            }
            return 1;
        }
        if (failures.Count > 0)
        {
            Console.WriteLine($"[e2e] 存在未通过项：{string.Join("；", failures)}");
            return 1;
        }
        Console.WriteLine($"跨机闭环全部通过（C# SDK → {config.Server}：业务链路 + 三面经接入层直连 + 保活验收）");
        return 0;
    }

    // RunAsync 串起全部阶段：三面闭环 → 保活验收 → 对照；每一步都是新的一局。
    private static async Task RunAsync(E2EConfig config, List<string> failures)
    {
        var ct = CancellationToken.None;
        if (config.RunClosure)
        {
            await RunClosureAsync(config, ct).ConfigureAwait(false);
        }
        var check = new KeepaliveCheck(config);
        if (config.RunKeepalive)
        {
            await using var pair = await MatchPair.CreateAsync(config, "keepalive", ct).ConfigureAwait(false);
            if (!await check.RunAsync(EdgeTransport.Kcp, pair.Plan, true, ct).ConfigureAwait(false))
            {
                failures.Add("保活验收未通过");
            }
        }
        if (config.RunControl)
        {
            await using var pair = await MatchPair.CreateAsync(config, "control", ct).ConfigureAwait(false);
            if (!await check.RunAsync(EdgeTransport.Kcp, pair.Plan, false, ct).ConfigureAwait(false))
            {
                failures.Add("对照未观察到掉线");
            }
        }
    }

    // RunClosureAsync 逐面跑闭环：每面一局（同局多面并发会让一面的帧广播淹没另一面的证据）。
    // 冷路径/网络抖动导致的一次失败换新局重试一次（同一张票在已结算的局里重试无意义）。
    private static async Task RunClosureAsync(E2EConfig config, CancellationToken ct)
    {
        var closure = new FaceClosure(config);
        foreach (var face in config.Faces)
        {
            var name = EdgeTransports.ShortName(face);
            await using var pair = await MatchPair.CreateAsync(config, "closure-" + name, ct).ConfigureAwait(false);
            try
            {
                await closure.RunAsync(face, pair.Plan, ct).ConfigureAwait(false);
            }
            catch (Exception exception) when (IsTransient(exception))
            {
                Console.WriteLine($"[闭环] {name} 首次失败（{exception.GetType().Name}: {exception.Message}），换新局重试一次");
                await using var retry = await MatchPair.CreateAsync(config, "closure-" + name + "-retry", ct)
                    .ConfigureAwait(false);
                await closure.RunAsync(face, retry.Plan, ct).ConfigureAwait(false);
            }
        }
    }

    // IsTransient 判定可换局重试的失败（网络/超时类；票废/接入层拒绝等不可重试）。
    private static bool IsTransient(Exception exception)
    {
        return exception is NetworkException || exception is Atlas.Errors.TimeoutException;
    }

    private static string FaceList(E2EConfig config)
    {
        var parts = new List<string>();
        foreach (var face in config.Faces)
        {
            parts.Add(EdgeTransports.ShortName(face));
        }
        return string.Join(",", parts);
    }

    private static void Usage()
    {
        Console.WriteLine("用法：ATLAS_E2E_SERVER=<host> dotnet run --project examples/E2E -- [选项]");
        Console.WriteLine("  环境变量 ATLAS_E2E_SERVER 是唯一门控（未设置即不运行，无默认目标）");
        Console.WriteLine("  -server <host>      目标服务器（覆盖环境变量）");
        Console.WriteLine("  -faces kcp,udp,ws   三面闭环的面清单与顺序（缺省全跑）");
        Console.WriteLine("  -quiet-ms 8000      保活静默窗口（只发心跳、不发输入的时长）");
        Console.WriteLine("  -heartbeat-ms 2000  保活心跳周期（缺省 2s，须 < offline_timeout/3 = 5s）");
        Console.WriteLine("  -closure-only       只跑三面闭环；-keepalive-only 只跑保活；-skip-control 跳过对照");
        Console.WriteLine("  -debug              打开战斗会话 Debug 级收发打点（排障）");
    }
}
