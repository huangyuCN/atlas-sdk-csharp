using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Gateway.V1;
using Atlas.Smoke;
using Atlas.Transport;
using Session = Atlas.Client.Session;
using Xunit;

namespace Atlas.Tests.Client;

// SessionTest 验证 Session 对象语义（对齐 Go client/session_test.go）：
//   1. 凭据保管：登录后 token/playerId 被保管，Logout 后清空；
//   2. 无凭据 Resume 报错且不发网络请求；
//   3. 无连接传输（UDP/KCP）请求帧携带会话槽：登录后凭据随帧槽送达服务端
//      （登录前为匿名帧、不置位）；TCP/WS 长连接不带槽；
//   4. 内置会话心跳：凭据为空跳过本轮，登录后按周期携带会话槽续租；
//   5. 被挤下线推送自动清凭据（原因经接缝提取并回调）；
//   6. ChannelOptions 装配：凭据提供者追踪最新凭据、自动恢复钩子
//      （有凭据调 Resume、无凭据不算失败）；
//   7. 客户端版本上报：登录/恢复请求体带 clientVersion（单一来源 AtlasVersion.Value）。
public sealed class SessionTest
{
    private const string BusinessOp = "/game.v1.PlayerService/GetPlayer";
    private const string AnonOp = "/game.v1.PlayerService/Anon";

    // FastSessionOptions 快速参数：关闭内置会话心跳隔离观测（用例按需单独开启），
    // 会话协议接缝由项目侧生成物实现（GatewaySessionProtocol）注入。
    private static SessionOptions FastSessionOptions()
    {
        return new SessionOptions
        {
            HeartbeatIntervalMs = 0,
            Protocol = GatewaySessionProtocol.Instance,
        };
    }

    // BusinessChannelOptions 业务通道选项：装配 Session 凭据提供者并声明传输类型。
    private static ChannelOptions BusinessChannelOptions(Session session, TransportKind kind)
    {
        var options = session.ChannelOptions();
        options.TransportKind = kind;
        options.HeartbeatIntervalMs = 0; // 关闭传输心跳，隔离观测。
        options.InvokeTimeoutMs = 2_000;
        options.BackoffBaseMs = 10;
        options.BackoffMaxMs = 40;
        return options;
    }

    // StartUdpClientAsync 启动单业务通道 UDP Client 并绑定 Session。
    private static async Task<AtlasClient> StartUdpClientAsync(
        SessionUdpServer server, Session session, ChannelOptions options)
    {
        var client = new AtlasClient(
            new ChannelConfig(
                ChannelKind.Business,
                token => UdpTransport.ConnectAsync("127.0.0.1", server.Port, token))
            {
                Options = options,
            });
        await client.ConnectAsync(CancellationToken.None);
        session.Bind(client);
        return client;
    }

    // JsonPayload 构造 protojson 形态请求体（Struct：{"key":"value",...}）。
    private static byte[] JsonPayload(params (string Key, string Value)[] fields)
    {
        var request = new Google.Protobuf.WellKnownTypes.Struct();
        foreach (var (key, value) in fields)
        {
            request.Fields.Add(key, Google.Protobuf.WellKnownTypes.Value.ForString(value));
        }
        return Encoding.UTF8.GetBytes(Google.Protobuf.JsonFormatter.Default.Format(request));
    }

    // Login_StoresCredentials_LogoutClears 验证登录后凭据被保管、Logout 后清空；
    // 登录帧凭据为空走匿名槽（无会话槽），登出帧携带保管中的凭据。
    [Fact]
    public async Task Login_StoresCredentials_Logout_Clears()
    {
        await using var server = new SessionUdpServer();
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        var credentials = await session.LoginAsync(
            CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));
        Assert.Equal("tok-42", credentials.Token);
        Assert.Equal("tok-42", session.Token);
        Assert.Equal("42", session.PlayerId);
        Assert.True(server.Saw(SessionProtocolOps.Login, "")); // 登录前无凭据：匿名帧。
        Assert.False(server.Saw(SessionProtocolOps.Login, "tok-42"));

        await session.LogoutAsync(CancellationToken.None);
        Assert.Equal("", session.Token);
        Assert.Equal("", session.PlayerId);
        Assert.True(server.Saw(SessionProtocolOps.Logout, "tok-42")); // 登出帧携带凭据。
        // LogoutRequest 是空消息（身份由连接/帧会话槽承载）：不得自造 token 字段。
        Assert.Empty(server.PayloadFor(SessionProtocolOps.Logout));
    }

    // Login_EmptyTokenReply_Throws 验证「凭据为空不得报成功」：回执解不出 token 即抛
    // ProtocolException（S0.5 修订 1 #1），而不是带着空凭据返回成功。
    [Fact]
    public async Task Login_EmptyTokenReply_Throws()
    {
        await using var server = new SessionUdpServer();
        server.SetReply(SessionProtocolOps.Login, "{\"playerId\":\"42\"}"); // 缺 token。
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        var error = await Assert.ThrowsAsync<ProtocolException>(
            () => session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x"))));

        Assert.Contains("token", error.Message);
        Assert.Equal("", session.Token); // 失败即无凭据，绝不半成功。
    }

    // Login_UndecodableReply_Throws 验证回执解析失败显式报错（可判定异常，不静默降级）。
    [Fact]
    public async Task Login_UndecodableReply_Throws()
    {
        await using var server = new SessionUdpServer();
        server.SetReply(SessionProtocolOps.Login, "{不是 JSON");
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        var error = await Assert.ThrowsAsync<ProtocolException>(
            () => session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x"))));

        Assert.Contains("解析失败", error.Message);
        Assert.Equal("", session.Token);
    }

    // Register_EmptyPlayerIdReply_Throws 验证注册回执缺 playerId 同样显式报错
    //（「关键凭据为空即失败」口径覆盖注册：不得返回空 playerId 的成功结果）。
    [Fact]
    public async Task Register_EmptyPlayerIdReply_Throws()
    {
        await using var server = new SessionUdpServer();
        server.SetReply(SessionProtocolOps.Register, "{}");
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        var error = await Assert.ThrowsAsync<ProtocolException>(
            () => session.RegisterAsync(CancellationToken.None, JsonPayload(("account", "a"))));

        Assert.Contains("playerId", error.Message);
    }

    // Login_EmptyReplyPayload_Throws 验证空回执同样报错：会话 op 的回执必须解出生成 DTO，
    // 空载荷不是「无该字段」而是解析失败（不得静默成功）。
    [Fact]
    public async Task Login_EmptyReplyPayload_Throws()
    {
        await using var server = new SessionUdpServer();
        server.SetReply(SessionProtocolOps.Login, "");
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        var error = await Assert.ThrowsAsync<ProtocolException>(
            () => session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x"))));

        Assert.Contains("回执为空", error.Message);
        Assert.Equal("", session.Token);
    }

    // Resume_WithoutToken_Throws 验证无凭据时 Resume 报错且不发网络请求。
    [Fact]
    public async Task Resume_WithoutToken_Throws_NoNetworkRequest()
    {
        await using var server = new SessionUdpServer();
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => session.ResumeAsync(CancellationToken.None));
        Assert.Empty(server.Seen); // 无凭据 Resume 不发网络请求。
    }

    // Resume_AfterLogin_KeepsCredentials 验证 Resume 用保管凭据恢复并刷新回执；
    // ResumeReply 不下发 token（服务端沿用原凭据）——合并语义保留原 token。
    [Fact]
    public async Task Resume_AfterLogin_KeepsCredentials()
    {
        await using var server = new SessionUdpServer();
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);
        await session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));

        var credentials = await session.ResumeAsync(CancellationToken.None);

        Assert.Equal("tok-42", credentials.Token);
        Assert.Equal("42", session.PlayerId);
        Assert.True(server.Saw(SessionProtocolOps.Resume, "tok-42")); // Resume 帧携带凭据。
    }

    // UdpInvoke_CarriesSessionSlot 验证无连接传输的请求帧携带会话槽：
    // 登录前匿名帧不带槽，登录后凭据随帧槽送达服务端。
    [Fact]
    public async Task UdpInvoke_AnonymousThenCarriesSlot_AfterLogin()
    {
        await using var server = new SessionUdpServer();
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        // 登录前：匿名帧（不置位、无槽）。
        await client.InvokeRawAsync(AnonOp, null, CancellationToken.None);
        await WaitUntilAsync(() => server.Saw(AnonOp, ""), TimeSpan.FromSeconds(2));

        // 登录后：业务请求（消息体无身份字段）经帧槽携带凭据。
        await session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));
        await client.InvokeRawAsync(BusinessOp, null, CancellationToken.None);
        await WaitUntilAsync(
            () => server.Saw(BusinessOp, "tok-42"), TimeSpan.FromSeconds(2));
    }

    // KcpInvoke_CarriesSessionSlot 验证 KCP（无连接可靠传输）请求帧同样携带会话槽。
    [Fact]
    public async Task KcpInvoke_CarriesSlot_AfterLogin()
    {
        await using var server = new SessionKcpServer();
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Kcp);
        options.InvokeTimeoutMs = 5_000; // KCP 往返较慢（常规模式 interval=40ms）。
        var client = new AtlasClient(
            new ChannelConfig(
                ChannelKind.Business,
                token => KcpTransport.ConnectAsync("127.0.0.1", server.Port, token))
            {
                Options = options,
            });
        await using (client)
        {
            await client.ConnectAsync(CancellationToken.None);
            session.Bind(client);

            await session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));
            await client.InvokeRawAsync(BusinessOp, null, CancellationToken.None);
            await WaitUntilAsync(
                () => server.Saw(BusinessOp, "tok-42"), TimeSpan.FromSeconds(5));
        }
    }

    // BuiltInSessionHeartbeat_SkipWhenLoggedOut_CarriesSlotWhenLoggedIn 验证内置会话心跳：
    // 未登录跳过本轮不发请求；登录后按周期续租且帧槽携带凭据。
    [Fact]
    public async Task BuiltInSessionHeartbeat_SkipsLoggedOut_CarriesSlotAfterLogin()
    {
        await using var server = new SessionUdpServer();
        var session = new Session(new SessionOptions
        {
            HeartbeatIntervalMs = 40,
            Protocol = GatewaySessionProtocol.Instance,
        });
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        // 未登录：心跳工厂返回空 operation，跳过本轮（不发任何请求）。
        await Task.Delay(150);
        Assert.DoesNotContain(
            server.Seen, record => record.StartsWith(SessionProtocolOps.Heartbeat, StringComparison.Ordinal));

        // 登录后：心跳按周期发起且帧槽携带凭据（op 取自生成物）。
        await session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));
        await WaitUntilAsync(
            () => server.Saw(SessionProtocolOps.Heartbeat, "tok-42"), TimeSpan.FromSeconds(2));
    }

    // KickedNotify_ClearsCredentials_AndReportsReason 验证被挤下线推送清空本地凭据，
    // 并把接缝提取的原因标识回调给业务（本轮新增能力）。
    [Fact]
    public async Task KickedNotify_ClearsCredentials_AndReportsReason()
    {
        await using var server = new SessionUdpServer();
        var reason = "";
        var options = FastSessionOptions();
        options.OnKicked = value => reason = value;
        var session = new Session(options);
        var channelOptions = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, channelOptions);
        await session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));
        Assert.Equal("tok-42", session.Token);

        await server.PushKickedAsync();
        await WaitUntilAsync(() => session.Token.Length == 0, TimeSpan.FromSeconds(2));
        Assert.Equal("", session.PlayerId);
        await WaitUntilAsync(() => reason.Length > 0, TimeSpan.FromSeconds(2));
        Assert.Equal(KickedReasonConst.KICKEDREASONLOGGEDINELSEWHERE, reason);
    }

    // LoginAndResume_ReportClientVersion 验证客户端版本上报：登录与恢复请求体
    // 均带 clientVersion，取值来自单一来源 AtlasVersion.Value（不两处手写）。
    [Fact]
    public async Task LoginAndResume_ReportClientVersion()
    {
        // 单一来源：取自 Atlas.csproj 的 <Version>（已剥离 SDK 追加的 +<commit> 元数据）。
        Assert.Matches(@"^\d+\.\d+\.\d+$", AtlasVersion.Value);
        await using var server = new SessionUdpServer();
        var session = new Session(FastSessionOptions());
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, options);

        await session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));
        Assert.Equal(AtlasVersion.Value, ClientVersionOf(server.PayloadFor(SessionProtocolOps.Login)));

        await session.ResumeAsync(CancellationToken.None);
        var resume = Encoding.UTF8.GetString(server.PayloadFor(SessionProtocolOps.Resume));
        using var document = JsonDocument.Parse(resume);
        Assert.Equal(AtlasVersion.Value, document.RootElement.GetProperty("clientVersion").GetString());
        Assert.Equal("tok-42", document.RootElement.GetProperty("token").GetString());
        Assert.Equal("42", document.RootElement.GetProperty("playerId").GetString());
    }

    // ChannelOptions_TokenProviderTracksCredentials 验证装配选项的凭据提供者
    // 追踪最新凭据（登录前空串 = 匿名帧）。
    [Fact]
    public async Task ChannelOptions_TokenProvider_TracksCredentials()
    {
        var session = new Session(FastSessionOptions());
        var options = session.ChannelOptions();
        Assert.Equal("", options.SessionTokenProvider!());

        await using var server = new SessionUdpServer();
        var channelOptions = BusinessChannelOptions(session, TransportKind.Udp);
        await using var client = await StartUdpClientAsync(server, session, channelOptions);
        await session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));

        Assert.Equal("tok-42", options.SessionTokenProvider!());
    }

    // ChannelOptions_AutoResumeHook 验证自动恢复钩子：有凭据调 Resume 并链式执行
    // 附加钩子；无凭据不算失败、不发请求。
    [Fact]
    public async Task ChannelOptions_AutoResumeHook_ResumesAndRunsExtraHook()
    {
        await using var server = new SessionUdpServer();
        var extraRan = 0;
        var sessionOptions = FastSessionOptions();
        sessionOptions.ResumeHook = () =>
        {
            Interlocked.Increment(ref extraRan);
            return Task.CompletedTask;
        };
        var session = new Session(sessionOptions);
        var options = BusinessChannelOptions(session, TransportKind.Udp);
        Assert.NotNull(options.OnReconnected); // AutoResume 默认开启：装配了恢复钩子。

        // 无凭据（未登录即断连）：钩子直接返回，不算失败、不发请求。
        await options.OnReconnected!();
        Assert.Equal(0, Volatile.Read(ref extraRan));
        Assert.Empty(server.Seen);

        // 有凭据：调 Resume 并链式执行附加钩子。
        await using var client = await StartUdpClientAsync(server, session, options);
        await session.LoginAsync(CancellationToken.None, JsonPayload(("playerId", "42"), ("password", "x")));
        await options.OnReconnected!();
        Assert.True(server.Saw(SessionProtocolOps.Resume, "tok-42"));
        Assert.Equal(1, Volatile.Read(ref extraRan));
    }

    // ChannelOptions_AutoResumeDisabled 验证 AutoResume=false 时不装配恢复钩子。
    [Fact]
    public void ChannelOptions_AutoResumeDisabled_NoHook()
    {
        var session = new Session(new SessionOptions
        {
            AutoResume = false,
            HeartbeatIntervalMs = 0,
            Protocol = GatewaySessionProtocol.Instance,
        });
        var options = session.ChannelOptions();
        Assert.Null(options.OnReconnected);
        Assert.Null(options.SessionHeartbeatOpFactory); // 心跳周期 0：不装配心跳。
        Assert.NotNull(options.SessionTokenProvider); // 凭据提供者恒装配。
    }

    // ChannelOptions_HeartbeatFactory_SkipsLoggedOut 验证心跳工厂未登录返回空 operation
    //（Session 对象凭据为空时返回 default = 空 operation，调度器跳过本轮）。
    [Fact]
    public void ChannelOptions_HeartbeatFactory_EmptyOpWhenLoggedOut()
    {
        var session = new Session(new SessionOptions
        {
            HeartbeatIntervalMs = 500,
            Protocol = GatewaySessionProtocol.Instance,
        });
        var options = session.ChannelOptions();
        Assert.Equal(500, options.SessionHeartbeatIntervalMs);
        Assert.Null(options.SessionHeartbeatOpFactory!().Operation); // 未登录：跳过本轮。
    }

    // ClientVersionOf 从 protojson 请求体取 clientVersion（缺失返回空串）。
    private static string ClientVersionOf(byte[] payload)
    {
        if (payload.Length == 0)
        {
            return "";
        }
        using var document = JsonDocument.Parse(Encoding.UTF8.GetString(payload));
        return document.RootElement.TryGetProperty("clientVersion", out var value)
            ? value.GetString() ?? ""
            : "";
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }
            await Task.Delay(10);
        }
        throw new System.TimeoutException($"等待条件超时（{timeout.TotalSeconds}s）");
    }
}
