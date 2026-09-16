using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Atlas.Client;

// SessionReply 是会话生命周期接口的统一回执形状（对齐 Go client.SessionReply：
// gateway.v1 会话消息，protojson 语义下字段名为 lowerCamel）。经 Struct JSON
// 解析，未知字段忽略（protojson DiscardUnknown 对齐；业务亦可用自身 DTO 解析）。
public sealed class SessionReply
{
    public string PlayerId { get; set; } = "";

    public string Token { get; set; } = "";
}

// SessionOps 是会话生命周期 op 名集合（默认见 Session *Operation 常量；可整体
// 覆盖，服务端 op 约定不一致时使用；对齐 Go client.SessionOps）。
public sealed class SessionOps
{
    public string Login { get; set; } = Session.LoginOperation;

    public string Resume { get; set; } = Session.ResumeOperation;

    public string Logout { get; set; } = Session.LogoutOperation;

    public string Heartbeat { get; set; } = Session.HeartbeatOperation;
}

// SessionOptions 配置 Session（对齐 Go SessionOption 函数式选项的属性形态）。
public sealed class SessionOptions
{
    // Ops 是会话生命周期 op 名集合（默认 gateway.v1.Session/{Login,Resume,Logout,Heartbeat}）。
    public SessionOps Ops { get; set; } = new();

    // HeartbeatIntervalMs 是内置会话心跳周期（毫秒；默认 30s；≤0 关闭）。心跳请求
    // 不携带 payload：服务端按连接（长连接）或帧会话槽（无连接）定位会话续租；
    // 须小于服务端会话租期（建议租期/2）。
    public int HeartbeatIntervalMs { get; set; } = 30_000;

    // AutoResume 设置断线重连后自动恢复会话（默认开启）：业务通道重连成功后以
    // 保存的凭据调 Resume；失败时 SDK 继续退避重连。
    public bool AutoResume { get; set; } = true;

    // ResumeHook 是自动恢复成功后的附加钩子（对齐 Go WithResumeHook，如 dual 形态
    // 的 Join 重绑定）；无凭据（未登录即断连）时钩子直接返回、不执行本钩子。
    public Func<Task>? ResumeHook { get; set; }
}

// Session 是会话生命周期管理器（对齐 Go client.Session）：凭据保管、断线自动恢复、
// 内置会话心跳与帧会话槽装配。业务请求经 Client.InvokeRawAsync 发送（消息体不含
// 身份字段），无连接传输（UDP/KCP）按帧会话槽携带凭据、长连接按连接绑定。
public sealed class Session
{
    // 会话生命周期接口的默认 operation（gateway.v1 收敛后的 Gateway 自留接口；
    // 服务端 proto 重写后以此为准，可经 SessionOptions.Ops 覆盖；对齐 Go OpSession*）。

    // LoginOperation 是会话登录 op：凭账号信息建立会话，回执下发会话凭据。
    public const string LoginOperation = "/gateway.v1.Session/Login";

    // RegisterOperation 是注册 op（建立会话前的账号创建；回执含 playerId）。
    public const string RegisterOperation = "/gateway.v1.Session/Register";

    // ResumeOperation 是断线重连免密恢复会话 op（凭据从帧会话槽或请求体携带）。
    public const string ResumeOperation = "/gateway.v1.Session/Resume";

    // LogoutOperation 是登出 op（服务端清理会话）。
    public const string LogoutOperation = "/gateway.v1.Session/Logout";

    // HeartbeatOperation 是会话心跳 op（无 payload；服务端按连接/会话槽续租）。
    public const string HeartbeatOperation = "/gateway.v1.Session/Heartbeat";

    // KickedNotifyOperation 是被挤下线推送的 operation（服务端 Notify 帧按消息名寻址）。
    public const string KickedNotifyOperation = "/gateway.v1.KickedNotify";

    // 会话回执 JSON 解析器：忽略未知字段（protojson DiscardUnknown 语义，服务端
    // 加字段不破坏旧客户端）。
    private static readonly JsonParser ReplyParser = new JsonParser(
        JsonParser.Settings.Default.WithIgnoreUnknownFields(true));

    private readonly SessionOptions _options;
    private readonly object _gate = new();
    private AtlasClient? _client;
    private NotifySubscription? _kickedSubscription;
    private string _token = "";
    private string _playerId = "";

    // Session 构造会话管理器（经 Bind 绑定 AtlasClient 后使用）。
    public Session(SessionOptions? options = null)
    {
        _options = options ?? new SessionOptions();
    }

    // Token 返回当前会话凭据（未登录为空串）。
    public string Token
    {
        get
        {
            lock (_gate)
            {
                return _token;
            }
        }
    }

    // PlayerId 返回当前会话的玩家 ID（未登录为空串）。
    public string PlayerId
    {
        get
        {
            lock (_gate)
            {
                return _playerId;
            }
        }
    }

    // Bind 绑定 Client（必须先于 LoginAsync/ResumeAsync/LogoutAsync 调用），并自动
    // 订阅被挤下线推送：收到即清空本地凭据（会话已失效）（对齐 Go Session.Bind）。
    public void Bind(AtlasClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _kickedSubscription?.Dispose();
        _kickedSubscription = client.On(KickedNotifyOperation, (_, _) => ClearCredentials());
    }

    // ChannelOptions 返回装配到业务通道的选项：会话凭据提供者（帧会话槽）、内置
    // 会话心跳与自动恢复钩子（对齐 Go Session.ChannelOptions）。在业务通道的
    // ChannelConfig.Options 传入；断线自动恢复经 Options.OnReconnected 生效
    //（ChannelConfig.ReconnectHook 显式配置时覆盖之）。同时记得为通道声明
    // TransportKind（UDP/KCP 传输才按帧携带会话槽）。
    public ChannelOptions ChannelOptions()
    {
        var options = new ChannelOptions
        {
            SessionTokenProvider = () => Token,
            OnReconnected = _options.AutoResume ? ResumeHookAsync : null,
        };
        if (_options.HeartbeatIntervalMs > 0)
        {
            options.SessionHeartbeatIntervalMs = _options.HeartbeatIntervalMs;
            options.SessionHeartbeatOpFactory = () => Token.Length == 0
                ? default // 未登录：跳过本轮（空 operation，对标 Go 返回 ("", nil)）。
                : new SessionHeartbeatRequest(_options.Ops.Heartbeat, null); // 心跳无 payload。
        }
        return options;
    }

    // LoginAsync 调用会话登录接口并保管回执凭据。payload 为业务登录请求的序列化
    // 字节（如账号密码；null 不携带 payload）。
    public Task<SessionReply> LoginAsync(CancellationToken cancellationToken, byte[]? payload = null)
    {
        return CallAsync(_options.Ops.Login, payload, cancellationToken);
    }

    // RegisterAsync 调用注册接口（回执含 playerId；不建立会话、不保管凭据）。
    public Task<SessionReply> RegisterAsync(CancellationToken cancellationToken, byte[]? payload = null)
    {
        return InvokeSessionAsync(RegisterOperation, payload, cancellationToken);
    }

    // ResumeAsync 用保管中的凭据免密恢复会话（断线重连场景）；无凭据抛
    // InvalidOperationException（不发网络请求，对齐 Go Resume 报错语义）。
    public async Task<SessionReply> ResumeAsync(CancellationToken cancellationToken)
    {
        var token = Token;
        if (token.Length == 0)
        {
            throw new InvalidOperationException("session: 无会话凭据（未登录）");
        }
        return await CallAsync(
            _options.Ops.Resume,
            RestorePayload(token, PlayerId),
            cancellationToken).ConfigureAwait(false);
    }

    // HeartbeatAsync 手动触发一次会话心跳并返回对时回执（无载荷：服务端按连接/
    // 帧槽定位会话续租）；与内置定时心跳语义一致，未登录抛 InvalidOperationException。
    public async Task<SessionHeartbeatReply> HeartbeatAsync(CancellationToken cancellationToken)
    {
        if (Token.Length == 0)
        {
            throw new InvalidOperationException("session: 无会话凭据（未登录）");
        }
        var data = await InvokeRawSessionAsync(_options.Ops.Heartbeat, null, cancellationToken).ConfigureAwait(false);
        return SessionHeartbeatReply.Parse(data);
    }

    // RestoreAsync 用外部凭据恢复会话（成功后凭据由 Session 保管）：凭据来自
    // 上一代连接（如断线前快照），区别于 ResumeAsync（用保管中的凭据）。
    public async Task<SessionReply> RestoreAsync(string token, string playerId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(token) || string.IsNullOrEmpty(playerId))
        {
            throw new InvalidOperationException("session: 恢复凭据与玩家 ID 不能为空");
        }
        return await CallAsync(
            _options.Ops.Resume,
            RestorePayload(token, playerId),
            cancellationToken).ConfigureAwait(false);
    }

    // LogoutAsync 登出并清空本地凭据（无论请求成败都清空，对齐 Go Logout）。
    public async Task LogoutAsync(CancellationToken cancellationToken)
    {
        try
        {
            await InvokeSessionAsync(_options.Ops.Logout, TokenPayload(Token), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ClearCredentials();
        }
    }

    // InvokeRawAsync 发送业务请求（会话凭据已按传输形态自动携带，消息体不含身份
    // 字段；对齐 Go Session.Invoke 委托业务通道）。
    public Task<byte[]> InvokeRawAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        var client = _client ?? throw new InvalidOperationException("session: 未绑定 Client");
        return client.InvokeRawAsync(operation, payload, cancellationToken);
    }

    // ResumeHookAsync 是断线重连后的自动恢复钩子（对齐 Go resumeHook）：无凭据
    //（未登录即断连）不算失败，登录由业务层重新发起；有凭据则调 Resume，失败上抛
    //（SDK 弃用本代连接继续退避重连后再试）；成功后链式执行附加钩子。
    private async Task ResumeHookAsync()
    {
        if (Token.Length == 0)
        {
            return;
        }
        await ResumeAsync(CancellationToken.None).ConfigureAwait(false);
        var extra = _options.ResumeHook;
        if (extra != null)
        {
            await extra().ConfigureAwait(false);
        }
    }

    // CallAsync 调用会话接口并在成功后保管回执凭据（Login/Resume 用）。
    private async Task<SessionReply> CallAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        var reply = await InvokeSessionAsync(operation, payload, cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            _token = reply.Token;
            _playerId = reply.PlayerId;
        }
        return reply;
    }

    // InvokeSessionAsync 委托业务通道 InvokeRawAsync 并解析会话回执。
    private async Task<SessionReply> InvokeSessionAsync(
        string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        var data = await InvokeRawSessionAsync(operation, payload, cancellationToken).ConfigureAwait(false);
        return ParseReply(data);
    }

    // InvokeRawSessionAsync 委托业务通道 InvokeRawAsync 返回原始回执字节
    //（心跳等非 SessionReply 形状回执使用）。
    private Task<byte[]> InvokeRawSessionAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        var client = _client ?? throw new InvalidOperationException("session: 未绑定 Client");
        return client.InvokeRawAsync(operation, payload, cancellationToken);
    }

    // RestorePayload 构造 Resume 请求体（protojson 形态 {"token":...,"playerId":...}）。
    private static byte[] RestorePayload(string token, string playerId)
    {
        var request = new Struct();
        request.Fields.Add("token", Value.ForString(token));
        if (playerId.Length > 0)
        {
            request.Fields.Add("playerId", Value.ForString(playerId));
        }
        return Encoding.UTF8.GetBytes(JsonFormatter.Default.Format(request));
    }

    // TokenPayload 构造 Logout 请求体（protojson 形态 {"token":...}）。
    private static byte[] TokenPayload(string token)
    {
        var request = new Struct();
        request.Fields.Add("token", Value.ForString(token));
        return Encoding.UTF8.GetBytes(JsonFormatter.Default.Format(request));
    }

    // ParseReply 解析会话回执 JSON（Struct：任意键对象 + 值；空回执按零值凭据）。
    private static SessionReply ParseReply(byte[] data)
    {
        var reply = new SessionReply();
        if (data.Length == 0)
        {
            return reply;
        }
        try
        {
            var payload = (Struct)ReplyParser.Parse(Encoding.UTF8.GetString(data), Struct.Descriptor);
            if (payload.Fields.TryGetValue("playerId", out var playerId)
                && playerId.KindCase == Value.KindOneofCase.StringValue)
            {
                reply.PlayerId = playerId.StringValue;
            }
            if (payload.Fields.TryGetValue("token", out var token)
                && token.KindCase == Value.KindOneofCase.StringValue)
            {
                reply.Token = token.StringValue;
            }
        }
        catch (Exception exception)
        {
            throw new ProtocolException("session: 解析会话回执失败", exception);
        }
        return reply;
    }

    // ClearCredentials 清空凭据（登出或会话失效被挤下线）。
    private void ClearCredentials()
    {
        lock (_gate)
        {
            _token = "";
            _playerId = "";
        }
    }
}

// SessionHeartbeatReply 是会话心跳回执（客户端对时用；int64 经 protojson 为
// 字符串，对齐 Go client.SessionHeartbeatReply；命名避开业务 DTO 同名歧义）。
public sealed class SessionHeartbeatReply
{
    public string? ServerTimeUnixMs { get; private set; }

    // Parse 从回执 JSON 构造（Struct：任意键对象 + 值；空回执按零值）。
    public static SessionHeartbeatReply Parse(byte[] data)
    {
        var reply = new SessionHeartbeatReply();
        if (data.Length > 0)
        {
            var parsed = JsonParser.Default.Parse(Encoding.UTF8.GetString(data), Struct.Descriptor);
            var fields = (Struct)parsed;
            if (fields.Fields.TryGetValue("serverTimeUnixMs", out var v) && v.KindCase == Value.KindOneofCase.StringValue)
            {
                reply.ServerTimeUnixMs = v.StringValue;
            }
        }
        return reply;
    }
}
