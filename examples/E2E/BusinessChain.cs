using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Serialization;
using Atlas.Transport;

namespace Atlas.E2E;

// BusinessOps 是业务链路的 op 名（模板 proto 的客户端面）：
//   - 网关会话协议（/gateway.v1.Session/*）：网关自留接口，登录/心跳建立并续租连接绑定；
//   - 玩家域撮合 op 与成局推送 op：本仓快照（与 BattleOps 同口径——protoc 生成物覆盖
//     到 game 域后改为引用生成物；此处字面量以模板 proto 为准）。
internal static class BusinessOps
{
    public const string Register = "/gateway.v1.Session/Register";

    public const string Login = "/gateway.v1.Session/Login";

    public const string Heartbeat = "/gateway.v1.Session/Heartbeat";

    public const string EnterMatchQueue = "/game.v1.PlayerService/EnterMatchQueue";

    public const string MatchStartedNotify = "/game.v1.MatchStartedNotify";
}

// BusinessPlayer 是一名玩家的业务链路（一条到网关的 tcp 通道）：
// 注册 → 登录（连接即会话）→ 入队匹配 → 等成局通知（票据 + 接入层面地址）。
// 成局通知是推送：订阅挂在通道上（跨连接代际保留），每次入队前重新「上膛」一个等待槽。
internal sealed class BusinessPlayer : IAsyncDisposable
{
    private const string Password = "pw-123456";
    private readonly AtlasClient _client;
    private readonly object _gate = new();
    private TaskCompletionSource<DirectPlan>? _pending;

    private BusinessPlayer(AtlasClient client, string tag)
    {
        _client = client;
        Tag = tag;
    }

    // Tag 是本玩家在本次运行里的短标识（日志与账号后缀）。
    public string Tag { get; }

    // PlayerId 是注册回执的玩家 ID（登录请求的主体）。
    public string PlayerId { get; private set; } = "";

    // Token 是登录回执的会话凭据（只在 Login 出现一次）。
    public string Token { get; private set; } = "";

    // ConnectAsync 建立到网关的业务通道并订阅成局通知。
    public static async Task<BusinessPlayer> ConnectAsync(E2EConfig config, string tag, CancellationToken ct)
    {
        var (host, port) = SplitAddress(config.GatewayAddress);
        var options = new ChannelOptions
        {
            Serializer = new Atlas.Serialization.JsonSerializer(),
            InvokeTimeoutMs = 5000,
            // 传输心跳保活业务通道（网关内置 Ping handler 即时空回执）。
            HeartbeatIntervalMs = 10_000,
        };
        var client = new AtlasClient(new ChannelConfig(
            ChannelKind.Business,
            token => TcpTransport.ConnectAsync(host, port, token)) { Options = options });
        await client.ConnectAsync(ct).ConfigureAwait(false);
        var player = new BusinessPlayer(client, tag);
        client.On(BusinessOps.MatchStartedNotify, player.OnMatchStarted);
        return player;
    }

    // RegisterAndLoginAsync 注册（账号唯一）→ 登录（建立会话绑定）。
    public async Task RegisterAndLoginAsync(string account, CancellationToken ct)
    {
        var register = Utf8("{\"account\":" + Quote(account) + ",\"password\":" + Quote(Password)
            + ",\"nickname\":" + Quote("e2e-" + Tag) + "}");
        var reply = await _client.InvokeRawAsync(BusinessOps.Register, register, ct).ConfigureAwait(false);
        PlayerId = ReadString(reply, "playerId");
        if (PlayerId.Length == 0)
        {
            throw new InvalidOperationException($"[{Tag}] 注册回执缺 playerId: {Snippet(reply)}");
        }

        var login = Utf8("{\"playerId\":" + Quote(PlayerId) + ",\"password\":" + Quote(Password)
            + ",\"clientVersion\":" + Quote(AtlasVersion.Value) + "}");
        var loginReply = await _client.InvokeRawAsync(BusinessOps.Login, login, ct).ConfigureAwait(false);
        Token = ReadString(loginReply, "token");
        if (Token.Length == 0)
        {
            throw new InvalidOperationException($"[{Tag}] 登录回执缺 token: {Snippet(loginReply)}");
        }
    }

    // ArmMatch 入队前「上膛」一个成局等待槽（返回等待任务），避免与上一局的推送串台。
    public Task<DirectPlan> ArmMatch()
    {
        var pending = new TaskCompletionSource<DirectPlan>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            _pending = pending;
        }
        return pending.Task;
    }

    // EnterMatchQueueAsync 入队匹配（规则集 casual = 模板缺省匹配器）。
    public async Task EnterMatchQueueAsync(CancellationToken ct)
    {
        var payload = Utf8("{\"ruleset\":\"casual\"}");
        await _client.InvokeRawAsync(BusinessOps.EnterMatchQueue, payload, ct).ConfigureAwait(false);
    }

    // HeartbeatAsync 业务心跳（会话续租；长时间只跑战斗链路时防止业务会话过期）。
    public async Task HeartbeatAsync(CancellationToken ct)
    {
        var payload = Utf8("{\"ts\":\"" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + "\"}");
        await _client.InvokeRawAsync(BusinessOps.Heartbeat, payload, ct).ConfigureAwait(false);
    }

    // OnMatchStarted 是成局通知回调：解析直连计划并交给等待槽（未上膛即忽略）。
    private void OnMatchStarted(string op, byte[] payload, byte version)
    {
        DirectPlan plan;
        try
        {
            plan = DirectPlan.FromNotify(payload);
        }
        catch (Exception)
        {
            return; // 载荷非法：由等待槽超时暴露（不在推送线程里抛）。
        }
        lock (_gate)
        {
            _pending?.TrySetResult(plan);
        }
    }

    // SplitAddress 拆分 host:port（IPv6 字面量带方括号时剥掉）。
    private static (string Host, int Port) SplitAddress(string address)
    {
        var separator = address.LastIndexOf(':');
        var host = address[..separator];
        if (host.Length > 1 && host[0] == '[' && host[^1] == ']')
        {
            host = host[1..^1];
        }
        return (host, int.Parse(address[(separator + 1)..]));
    }

    // ReadString 读取 protojson 回执里的字符串字段（缺失/非字符串返回空串）。
    private static string ReadString(byte[] payload, string name)
    {
        using var document = JsonDocument.Parse(payload);
        return document.RootElement.TryGetProperty(name, out var node) && node.ValueKind == JsonValueKind.String
            ? node.GetString() ?? ""
            : "";
    }

    private static string Snippet(byte[] payload)
    {
        var text = Encoding.UTF8.GetString(payload);
        return text.Length > 200 ? text[..200] + "..." : text;
    }

    private static string Quote(string value)
    {
        return System.Text.Json.JsonSerializer.Serialize(value);
    }

    private static byte[] Utf8(string json)
    {
        return Encoding.UTF8.GetBytes(json);
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync().ConfigureAwait(false);
    }
}
