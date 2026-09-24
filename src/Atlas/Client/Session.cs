using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Atlas.Client;

// SessionOptions 配置 Session（对齐 Go SessionOption 函数式选项的属性形态）。
public sealed class SessionOptions
{
    // Protocol 是会话协议接缝（必填）：op 名、凭据提取与推送识别全部经它；项目侧用
    // 生成物提供的实现接入（示例：Protocol = GatewaySessionProtocol.Instance）。
    public ISessionProtocol? Protocol { get; set; }

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

    // OnKicked 是被挤下线回调：原因标识来自接缝 Kicked 的原因提取（如枚举名
    // KICKED_REASON_LOGGED_IN_ELSEWHERE）；**取不到原因时为空串**（推送载荷为空/
    // 解码失败/未知帧版本）——空原因不影响清凭据，业务侧不得据此认为「没被踢」。
    // 凭据清空在回调前完成（对齐 Go onKicked）。回调在执行推送分发的线程上调用
    //（通道调度器；Unity 注入主线程上下文后即在主线程）。
    public Action<string>? OnKicked { get; set; }

    // ClientVersion 是登录/恢复请求上报的客户端版本（默认取单一来源 AtlasVersion.Value，
    // 业务可用它改报自身版本；M1：服务端按 min_client_version 门槛裁决）。
    public string ClientVersion { get; set; } = AtlasVersion.Value;
}

// Session 是会话生命周期管理器（对齐 Go client.Session）：凭据保管、断线自动恢复、
// 内置会话心跳与帧会话槽装配。会话协议事实（op 名/凭据字段/推送）全部来自
// ISessionProtocol 接缝——本状态机不引用任何会话消息类型、不写 op 字面量。
// 业务请求经 Client.InvokeRawAsync 发送（消息体不含身份字段），无连接传输（UDP/KCP）
// 按帧会话槽携带凭据、长连接按连接绑定。
//
// 回执契约：会话 op 的回执必须能按**生成的会话 DTO**解码出关键凭据——解析失败或
// 关键凭据为空时本类抛 ProtocolException（绝不「凭据为空仍报成功」）。因此项目二进制
// 必须链接生成的会话 DTO 包（模板仓 opclient 产物），并把它接到接缝实现上。
public sealed class Session : IDisposable
{
    // ReplyParser 解析请求/回执的 protojson 对象载荷（忽略未知字段：服务端加字段
    // 不破坏旧客户端）。
    private static readonly JsonParser ObjectParser = new JsonParser(
        JsonParser.Settings.Default.WithIgnoreUnknownFields(true));

    private readonly SessionOptions _options;
    private readonly ISessionProtocol _protocol;
    private readonly object _gate = new();

    // _pushHandler 是稳定的推送回调实例：OnAny 按 delegate 引用去重，方法组每次转换
    // 都产生新 delegate——缓存一份才能让「重复 Bind」按稳定键去重（不累积重复订阅）。
    private readonly NotifyHandler _pushHandler;
    private AtlasClient? _client;
    private NotifySubscription? _pushSubscription;
    private string _token = "";
    private string _playerId = "";
    private long _expiresAt;
    private string _kickedReason = "";

    // Session 构造会话管理器（经 Bind 绑定 AtlasClient 后使用）；Protocol 接缝必填。
    public Session(SessionOptions? options = null)
    {
        _options = options ?? new SessionOptions();
        _protocol = _options.Protocol
            ?? throw new ArgumentException("SessionOptions.Protocol 必填（会话协议接缝）", nameof(options));
        _pushHandler = HandlePush;
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

    // ExpiresAt 返回接缝最近提取的会话过期时间（0 = 未知；本轮无续期消费方）。
    public long ExpiresAt
    {
        get
        {
            lock (_gate)
            {
                return _expiresAt;
            }
        }
    }

    // KickedReason 返回最近一次「被挤下线」推送的原因标识（接缝提取；未发生为空串）。
    // 凭据在收到推送时即被清空，本值供业务提示玩家（如「账号已在别处登录」）；
    // 推送载荷取不到原因时本值也是空串（凭据同样已清空）。
    public string KickedReason
    {
        get
        {
            lock (_gate)
            {
                return _kickedReason;
            }
        }
    }

    // Bind 绑定 Client（必须先于 LoginAsync/ResumeAsync/LogoutAsync 调用），并自动订阅
    // 全部推送：op 交接缝识别，命中「被挤下线」即清空本地凭据（会话已失效）并回调原因
    //（对齐 Go Session.Bind；原因提取为本轮新增能力）。重复 Bind 先退订旧订阅（不累积
    // 重复订阅：同一推送只处理一次），Dispose 时退订。
    public void Bind(AtlasClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _pushSubscription?.Dispose();
        _pushSubscription = client.OnAny(_pushHandler);
    }

    // Dispose 解除推送订阅并解除 Client 绑定（幂等）：Dispose 后不再处理任何推送。
    // 会话凭据不清空（业务可继续读取/落库）；通道生命周期归 Client，本方法不关闭通道。
    public void Dispose()
    {
        _pushSubscription?.Dispose();
        _pushSubscription = null;
        _client = null;
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
            var heartbeatOperation = _protocol.Ops.Heartbeat;
            options.SessionHeartbeatIntervalMs = _options.HeartbeatIntervalMs;
            options.SessionHeartbeatOpFactory = () => Token.Length == 0
                ? default // 未登录：跳过本轮（空 operation，对标 Go 返回 ("", nil)）。
                : new SessionHeartbeatRequest(heartbeatOperation, null); // 心跳无 payload。
        }
        return options;
    }

    // LoginAsync 调用会话登录接口并保管回执凭据。payload 为业务登录请求的序列化
    // 字节（如账号密码；null 不携带 payload）；客户端版本由本方法盖上请求体（M1）。
    // 回执必须解出非空 token，否则抛 ProtocolException（不得空凭据报成功）。
    public Task<SessionCredentials> LoginAsync(CancellationToken cancellationToken, byte[]? payload = null)
    {
        return CallAsync(_protocol.Ops.Login, StampClientVersion(payload), cancellationToken);
    }

    // RegisterAsync 调用注册接口（回执含 playerId；不建立会话、不保管凭据）。
    // 回执必须解出非空 playerId，否则抛 ProtocolException（同「凭据为空即失败」口径）。
    public async Task<SessionCredentials> RegisterAsync(CancellationToken cancellationToken, byte[]? payload = null)
    {
        var credentials = await InvokeSessionAsync(_protocol.Ops.Register, payload, cancellationToken, requireReply: true)
            .ConfigureAwait(false);
        if (credentials.PlayerId.Length == 0)
        {
            throw new ProtocolException(
                $"注册回执缺少 playerId（{_protocol.Ops.Register}）：回执未按生成的 RegisterReply 解码");
        }
        return credentials;
    }

    // ResumeAsync 用保管中的凭据免密恢复会话（断线重连场景）；无凭据抛
    // InvalidOperationException（不发网络请求，对齐 Go Resume 报错语义）。
    public Task<SessionCredentials> ResumeAsync(CancellationToken cancellationToken)
    {
        var token = Token;
        if (token.Length == 0)
        {
            throw new InvalidOperationException("session: 无会话凭据（未登录）");
        }
        return CallAsync(_protocol.Ops.Resume, ResumePayload(token, PlayerId), cancellationToken);
    }

    // HeartbeatAsync 手动触发一次会话心跳并返回原始回执字节（无载荷：服务端按连接/
    // 帧槽定位会话续租）；与内置定时心跳语义一致，未登录抛 InvalidOperationException。
    // 回执 DTO（对时字段）由业务侧按自身协议解码——SDK 不做会话 DTO 解码。
    public async Task<byte[]> HeartbeatAsync(CancellationToken cancellationToken)
    {
        if (Token.Length == 0)
        {
            throw new InvalidOperationException("session: 无会话凭据（未登录）");
        }
        return await InvokeRawSessionAsync(_protocol.Ops.Heartbeat, null, cancellationToken).ConfigureAwait(false);
    }

    // RestoreAsync 用外部凭据恢复会话（成功后凭据由 Session 保管）：凭据来自
    // 上一代连接（如断线前快照），区别于 ResumeAsync（用保管中的凭据）。
    public Task<SessionCredentials> RestoreAsync(string token, string playerId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(token) || string.IsNullOrEmpty(playerId))
        {
            throw new InvalidOperationException("session: 恢复凭据与玩家 ID 不能为空");
        }
        return CallAsync(_protocol.Ops.Resume, ResumePayload(token, playerId), cancellationToken);
    }

    // LogoutAsync 登出并清空本地凭据（无论请求成败都清空，对齐 Go Logout）。
    // LogoutRequest 是空消息（模板 session.proto：身份由连接/帧会话槽承载），故不携带
    // 任何手写字段——旧实现发 {"token":...} 属契约外的自造字段。
    public async Task LogoutAsync(CancellationToken cancellationToken)
    {
        try
        {
            await InvokeSessionAsync(_protocol.Ops.Logout, null, cancellationToken).ConfigureAwait(false);
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

    // HandlePush 把每个推送交给接缝识别（接缝是「被挤下线」的唯一识别点）：帧头版本随
    // 推送信封下发（ver=1 protojson / ver=2 protobuf wire），命中即清凭据并回调原因。
    private void HandlePush(string operation, byte[] payload, byte version)
    {
        var (reason, ok) = _protocol.Kicked(operation, new PushEnvelope
        {
            Op = operation,
            Version = version,
            Body = payload,
        });
        if (!ok)
        {
            return; // 非「被挤下线」推送：会话状态不变。
        }
        // 命中即清凭据——**与 reason 是否为空无关**：取不到原因（空载荷/解码失败/
        // 未知版本）时 reason 为空串，但凭据已失效，绝不能保留（S0.5 #5）。
        ClearCredentials();
        lock (_gate)
        {
            _kickedReason = reason;
        }
        _options.OnKicked?.Invoke(reason);
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

    // CallAsync 调用会话接口并在成功后保管回执凭据（Login/Resume 用）；返回**合并后的
    // 有效凭据**：ResumeReply 不下发 token（服务端沿用原凭据）时返回保管中的 token。
    // 回执必须非空且解出关键凭据，否则抛 ProtocolException（见 InvokeSessionAsync /
    // StoreCredentials）。
    private async Task<SessionCredentials> CallAsync(
        string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        var credentials = await InvokeSessionAsync(operation, payload, cancellationToken, requireReply: true)
            .ConfigureAwait(false);
        return StoreCredentials(operation, credentials);
    }

    // InvokeSessionAsync 委托业务通道 InvokeRawAsync 并经接缝提取凭据。requireReply=true
    //（Login/Register/Resume：回执必须解出生成 DTO）时空回执即报错——空回执不是「无该
    // 字段」，而是回执未按生成 DTO 解码（S0.5 修订 1 #1：解析失败不得静默成功）。
    private async Task<SessionCredentials> InvokeSessionAsync(
        string operation, byte[]? payload, CancellationToken cancellationToken, bool requireReply = false)
    {
        var data = await InvokeRawSessionAsync(operation, payload, cancellationToken).ConfigureAwait(false);
        if (requireReply && data.Length == 0)
        {
            throw new ProtocolException($"会话回执为空（{operation}）：未按生成的会话 DTO 解码");
        }
        return CredentialsOf(operation, data);
    }

    // InvokeRawSessionAsync 委托业务通道 InvokeRawAsync 返回原始回执字节。
    private Task<byte[]> InvokeRawSessionAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        var client = _client ?? throw new InvalidOperationException("session: 未绑定 Client");
        return client.InvokeRawAsync(operation, payload, cancellationToken);
    }

    // CredentialsOf 把回执交给接缝提取凭据（token/playerId/过期时间）——状态机不引用
    // 任何会话消息类型。接缝解码失败（回执非生成 DTO 形态）即抛 ProtocolException：
    // 会话回执解析失败不得静默成功（S0.5 修订 1 #1）。
    private SessionCredentials CredentialsOf(string operation, byte[] data)
    {
        var message = new SessionMessage(operation, data);
        try
        {
            return new SessionCredentials(
                _protocol.Token(message),
                _protocol.PlayerId(message),
                _protocol.ExpiresAt(message));
        }
        catch (Exception exception)
        {
            throw new ProtocolException(
                $"会话回执解析失败（{operation}）：项目二进制必须链接生成的会话 DTO 并接入接缝", exception);
        }
    }

    // StoreCredentials 合并回执凭据：空值沿用（ResumeReply 不下发 token——服务端沿用
    // 原凭据，playerId 同理），过期时间非 0 才覆盖（可选钩子语义）；返回合并后的有效凭据。
    // 会话已建立（Login/Resume 成功返回）却仍无 token ⇒ 回执未解出关键凭据 ⇒ 抛
    // ProtocolException——「凭据为空却报成功」会让上层带着空身份继续跑（S0.5 修订 1 #1）。
    private SessionCredentials StoreCredentials(string operation, SessionCredentials credentials)
    {
        lock (_gate)
        {
            if (credentials.Token.Length > 0)
            {
                _token = credentials.Token;
            }
            if (credentials.PlayerId.Length > 0)
            {
                _playerId = credentials.PlayerId;
            }
            if (credentials.ExpiresAt != 0)
            {
                _expiresAt = credentials.ExpiresAt;
            }
            if (_token.Length == 0)
            {
                throw new ProtocolException(
                    $"会话回执缺少凭据（{operation}）：token 为空——回执未按生成的会话 DTO 解码");
            }
            return new SessionCredentials(_token, _playerId, _expiresAt);
        }
    }

    // ResumePayload 构造 Resume 请求体（protojson 形态；带客户端版本上报，M1）。
    // 字段名（token/playerId/clientVersion）是接缝共约的请求字段名：SDK 内核不引用
    // 生成的会话 DTO（接缝形状冻结、状态机零消息类型），故只能按约定名组装 protojson；
    // 若模板协议改字段名，此处与生成物会漂移——由接缝契约测试（生成物提取器）兜底。
    private byte[] ResumePayload(string token, string playerId)
    {
        var request = new Struct();
        request.Fields.Add("token", Value.ForString(token));
        if (playerId.Length > 0)
        {
            request.Fields.Add("playerId", Value.ForString(playerId));
        }
        request.Fields.Add("clientVersion", Value.ForString(_options.ClientVersion));
        return Encoding.UTF8.GetBytes(JsonFormatter.Default.Format(request));
    }

    // StampClientVersion 把客户端版本盖上登录请求体（M1 版本上报）。载荷须为
    // protojson 对象；非 JSON 对象载荷（ver=2 二进制）原样透传——ver=2 由调用方在
    // DTO 上携带 ClientVersion（SDK 不解析二进制载荷）。
    private byte[] StampClientVersion(byte[]? payload)
    {
        var fields = TryParseObject(payload);
        if (fields == null)
        {
            return payload ?? Array.Empty<byte>();
        }
        fields.Fields["clientVersion"] = Value.ForString(_options.ClientVersion);
        return Encoding.UTF8.GetBytes(JsonFormatter.Default.Format(fields));
    }

    // TryParseObject 解析 protojson 对象载荷：空载荷按空对象；非 JSON 对象返回 null。
    private static Struct? TryParseObject(byte[]? payload)
    {
        if (payload == null || payload.Length == 0)
        {
            return new Struct();
        }
        try
        {
            return (Struct)ObjectParser.Parse(Encoding.UTF8.GetString(payload), Struct.Descriptor);
        }
        catch (Exception)
        {
            return null; // 非 protojson 对象（如 protobuf 二进制载荷）：原样透传。
        }
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
