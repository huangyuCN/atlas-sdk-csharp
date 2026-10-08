using System;
using System.Collections.Generic;
using Atlas.Battle;

namespace Atlas.E2E;

// E2EConfig 是跨机闭环驱动的运行参数：目标服务器由环境变量 ATLAS_E2E_SERVER 门控
//（不设置即不运行——真服务驱动绝不默认打外网），命令行可覆盖面清单与静默窗口。
internal sealed class E2EConfig
{
    // 端口是模板部署的缺省口径（网关 tcp 9001 / 接入层 ws 7100、kcp 7101、udp 7102）。
    public const int GatewayTcpPort = 9001;
    public const int EdgeWsPort = 7100;
    public const int EdgeKcpPort = 7101;
    public const int EdgeUdpPort = 7102;

    private E2EConfig(string server, EdgeTransport[] faces, int quietMs, TimeSpan heartbeat,
        bool closure, bool keepalive, bool control, bool debug, int probeMs, bool end)
    {
        Debug = debug;
        ProbeMs = probeMs;
        Server = server;
        Faces = faces;
        QuietMs = quietMs;
        HeartbeatInterval = heartbeat;
        RunClosure = closure;
        RunKeepalive = keepalive;
        RunControl = control;
        RunEnd = end;
    }

    // Server 是目标服务器主机（成局通知里的接入层地址由服务端下发，本项只用于网关地址）。
    public string Server { get; }

    // GatewayAddress 是网关业务通道地址（host:port）。
    public string GatewayAddress => Server + ":" + GatewayTcpPort;

    // Faces 是三面闭环要跑的面清单（顺序即执行顺序）。
    public EdgeTransport[] Faces { get; }

    // QuietMs 是保活验收的静默窗口（只发心跳、不发输入的时长）。
    public int QuietMs { get; }

    // HeartbeatInterval 是保活心跳周期（探针一拍的长度）。
    public TimeSpan HeartbeatInterval { get; }

    // RunClosure 为真时跑三面闭环。
    public bool RunClosure { get; }

    // RunKeepalive 为真时跑「心跳开」的保活验收。
    public bool RunKeepalive { get; }

    // RunControl 为真时跑「心跳关」的同参数对照。
    public bool RunControl { get; }

    // RunEnd 为真时跑对局结束语义验收（跑到自然结算，断言终态停发与事件幂等）。
    public bool RunEnd { get; }

    // ProbeMs 是静默窗口中途的探针点（到点补发一帧 + 补帧，验证仍被受理）。
    public int ProbeMs { get; }

    // Debug 为真时打开战斗会话的 Debug 级收发打点（排障用：帧发送/回执/心跳逐条可见）。
    public bool Debug { get; }

    // Parse 解析运行参数：环境变量缺省 + 命令行覆盖；server 缺失返回 null（打印用法）。
    public static E2EConfig? Parse(string[] args, string? envServer)
    {
        var server = envServer;
        var faces = new List<EdgeTransport> { EdgeTransport.Kcp, EdgeTransport.Udp, EdgeTransport.Ws };
        var quietMs = 8000;
        var heartbeatMs = 2000;
        var probeMs = 5200;
        var closure = true;
        var keepalive = true;
        var control = true;
        var debug = false;
        var end = true;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-server" when i + 1 < args.Length: server = args[++i]; break;
                case "-faces" when i + 1 < args.Length: faces = ParseFaces(args[++i]); break;
                case "-quiet-ms" when i + 1 < args.Length: quietMs = int.Parse(args[++i]); break;
                case "-heartbeat-ms" when i + 1 < args.Length: heartbeatMs = int.Parse(args[++i]); break;
                case "-probe-ms" when i + 1 < args.Length: probeMs = int.Parse(args[++i]); break;
                case "-closure-only": keepalive = false; control = false; end = false; break;
                case "-keepalive-only": closure = false; control = false; end = false; break;
                case "-end-only": closure = false; keepalive = false; control = false; break;
                case "-skip-control": control = false; break;
                case "-skip-end": end = false; break;
                case "-debug": debug = true; break;
                case "-h" or "--help": return null;
            }
        }
        if (string.IsNullOrWhiteSpace(server))
        {
            return null;
        }
        return new E2EConfig(server!.Trim(), faces.ToArray(), quietMs,
            TimeSpan.FromMilliseconds(heartbeatMs), closure, keepalive, control, debug, probeMs, end);
    }

    // ParseFaces 解析面清单（kcp,udp,ws 短名；未知面即报错，不静默跳过）。
    private static List<EdgeTransport> ParseFaces(string text)
    {
        var faces = new List<EdgeTransport>();
        foreach (var item in text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (!EdgeTransports.TryParse(item, out var face))
            {
                throw new ArgumentException($"未知传输面: {item}（kcp|udp|ws）");
            }
            faces.Add(face);
        }
        if (faces.Count == 0)
        {
            throw new ArgumentException("-faces 不得为空（kcp|udp|ws 逗号分隔）");
        }
        return faces;
    }

    // EdgePort 返回某面在模板部署里的缺省端口（用于打印「服务端下发地址是否与约定一致」）。
    public static int EdgePort(EdgeTransport face)
    {
        return face switch
        {
            EdgeTransport.Ws => EdgeWsPort,
            EdgeTransport.Kcp => EdgeKcpPort,
            _ => EdgeUdpPort,
        };
    }
}
