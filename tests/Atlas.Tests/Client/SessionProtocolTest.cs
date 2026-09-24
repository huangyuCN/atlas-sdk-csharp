using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Frame;
using Atlas.Gateway.V1;
using Atlas.Smoke;
using Atlas.Transport;
using Google.Protobuf;
using ProtoKickedNotify = global::Gateway.V1.KickedNotify;
using ProtoKickedReason = global::Gateway.V1.KickedReason;
using Session = Atlas.Client.Session;
using Xunit;

namespace Atlas.Tests.Client;

// SessionProtocolTest 验证会话协议接缝（S0.5 冻结形状 + 修订 1：5 op + 3 解码钩子 +
// 1 推送识别，推送第二参为带帧头 version 的信封）：
//   1. 状态机只依赖接缝——注入 fake 接缝即可驱动登录/心跳/被踢，无需任何真实会话 DTO；
//   2. 接缝是 op 名的唯一来源——自定义 op 串原样发到线上（状态机不写字面量）；
//   3. 生成物素材可直接实现接缝（GatewaySessionProtocol）：凭据提取 + 挤下线原因提取，
//      ver=1（protojson）与 ver=2（protobuf wire）两条载荷都取到原因；
//   4. op 命中即清凭据——原因取不到（空 reason）时同样清（空原因不得保留失效凭据）；
//   5. 重复 Bind 不累积重复订阅（同一推送只处理一次），Dispose 后退订；
//   6. Session/SessionOptions 的成员签名不引用会话消息类型（生成物命名空间）。
public sealed class SessionProtocolTest
{
    // FakeProtocol 是纯 fake 接缝：op/凭据/原因全部固定，不引用任何会话消息类型
    //（证明状态机与真实会话 DTO 解耦；对齐 Go 侧 fake SessionProtocol 用法）。
    private sealed class FakeProtocol : ISessionProtocol
    {
        public FakeProtocol(SessionOps ops) => Ops = ops;

        public SessionOps Ops { get; }

        public string TokenValue { get; set; } = "tok-42";

        public string PlayerIdValue { get; set; } = "42";

        public long ExpiresAtValue { get; set; }

        public string KickedReason { get; set; } = "LOGGED_IN_ELSEWHERE";

        // KickedOpHit 控制 op 是否命中「被挤下线」（命中即 ok=true，原因允许为空串）。
        public bool KickedOpHit { get; set; } = true;

        public int KickedCalls { get; private set; }

        public byte LastVersion { get; private set; }

        public string LastOperation { get; private set; } = "";

        public string Token(object? msg) => TokenValue;

        public string PlayerId(object? msg) => PlayerIdValue;

        public long ExpiresAt(object? msg) => ExpiresAtValue;

        // Kicked 收推送信封（op + 帧头 version + 未解码字节）：fake 不解析载荷，
        // 只记录版本并按 KickedOpHit 回命中与否。
        public (string Reason, bool Ok) Kicked(string operation, PushEnvelope payload)
        {
            KickedCalls++;
            LastVersion = payload.Version;
            LastOperation = operation;
            return KickedOpHit ? (KickedReason, true) : ("", false);
        }
    }

    // GeneratedOps 取自生成物（protoc-gen-atlas-client 的 SessionProtocolOps）。
    private static SessionOps GeneratedOps()
    {
        return new SessionOps
        {
            Register = SessionProtocolOps.Register,
            Login = SessionProtocolOps.Login,
            Resume = SessionProtocolOps.Resume,
            Logout = SessionProtocolOps.Logout,
            Heartbeat = SessionProtocolOps.Heartbeat,
        };
    }

    // Envelope 构造推送信封（op 固定为被挤下线推送；version 为帧头载荷编码版本）。
    private static PushEnvelope Envelope(byte version, byte[] body)
    {
        return new PushEnvelope
        {
            Op = SessionPushOps.KickedNotify,
            Version = version,
            Body = body,
        };
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

    // UdpOptions 会话 UDP 形态选项（关传输心跳，隔离观测）。
    private static ChannelOptions UdpOptions(Session session)
    {
        var options = session.ChannelOptions();
        options.TransportKind = TransportKind.Udp;
        options.HeartbeatIntervalMs = 0;
        options.InvokeTimeoutMs = 2_000;
        return options;
    }

    // CustomOps_AreUsedOnWire 验证接缝是 op 名的唯一来源：自定义 op 串原样发出。
    [Fact]
    public async Task CustomOps_AreUsedOnWire_WithoutRealSessionDto()
    {
        await using var server = new FakeServer();
        var fake = new FakeProtocol(new SessionOps
        {
            Register = "/custom.Session/Register",
            Login = "/custom.Session/Login",
            Resume = "/custom.Session/Resume",
            Logout = "/custom.Session/Logout",
            Heartbeat = "/custom.Session/Heartbeat",
        });
        var session = new Session(new SessionOptions { Protocol = fake, HeartbeatIntervalMs = 0 });
        var options = session.ChannelOptions();
        options.HeartbeatIntervalMs = 0;
        options.InvokeTimeoutMs = 2_000;
        await using var client = new AtlasClient(
            new ChannelConfig(
                ChannelKind.Business,
                ct => TcpTransport.ConnectAsync("127.0.0.1", server.Port, ct))
            {
                Options = options,
            });
        await client.ConnectAsync(CancellationToken.None);
        session.Bind(client);

        var credentials = await session.LoginAsync(CancellationToken.None, new byte[] { 0x7B, 0x7D });

        Assert.Contains("/custom.Session/Login", server.OperationNames);
        Assert.Equal("tok-42", credentials.Token);
        Assert.Equal("42", credentials.PlayerId);
        await session.LogoutAsync(CancellationToken.None);
        Assert.Contains("/custom.Session/Logout", server.OperationNames);
    }

    // FakeProtocol_DrivesLoginHeartbeatAndKicked 验证注入 fake 接缝即可驱动
    // 登录/心跳/被踢（状态机不依赖真实会话 DTO），且推送信封带上帧头 version。
    [Fact]
    public async Task FakeProtocol_DrivesLoginHeartbeatAndKicked()
    {
        await using var server = new SessionUdpServer();
        var fake = new FakeProtocol(GeneratedOps())
        {
            ExpiresAtValue = 1_700_000_000_000L,
        };
        var reason = "";
        var session = new Session(new SessionOptions
        {
            Protocol = fake,
            HeartbeatIntervalMs = 40,
            OnKicked = value => reason = value,
        });
        await using var client = await StartUdpClientAsync(server, session, UdpOptions(session));

        var credentials = await session.LoginAsync(
            CancellationToken.None, Encoding.UTF8.GetBytes("{}"));
        Assert.Equal("tok-42", credentials.Token);
        Assert.Equal(1_700_000_000_000L, credentials.ExpiresAt);
        Assert.Equal(1_700_000_000_000L, session.ExpiresAt);

        // 内置心跳：op 来自接缝，凭据非空即按周期发起。
        await WaitUntilAsync(
            () => Array.Exists(server.Seen, record => record.StartsWith(SessionProtocolOps.Heartbeat, StringComparison.Ordinal)),
            TimeSpan.FromSeconds(2));

        // 被挤下线推送：接缝识别 + 原因提取，状态机清空凭据并回调原因。
        // 先等回调（回调在清凭据之后执行）：只等 token 清空会与回调置值竞态。
        await server.PushKickedAsync();
        await WaitUntilAsync(() => reason.Length > 0, TimeSpan.FromSeconds(2));
        Assert.True(fake.KickedCalls > 0);
        Assert.Equal("LOGGED_IN_ELSEWHERE", reason);
        Assert.Equal("", session.Token);
        Assert.Equal("", session.PlayerId);
        // 帧头 version 随信封交给接缝（服务器按 ver=1 推送）。
        Assert.Equal(FrameGen.Version, fake.LastVersion);
        Assert.Equal(SessionPushOps.KickedNotify, fake.LastOperation);
    }

    // GeneratedProtocol_ExtractsCredentialsAndKickReason 验证生成物素材可直接实现接缝。
    [Fact]
    public async Task GeneratedProtocol_ExtractsCredentialsAndKickReason()
    {
        await using var server = new SessionUdpServer();
        var session = new Session(new SessionOptions
        {
            Protocol = GatewaySessionProtocol.Instance,
            HeartbeatIntervalMs = 0,
        });
        await using var client = await StartUdpClientAsync(server, session, UdpOptions(session));

        var credentials = await session.LoginAsync(
            CancellationToken.None, Encoding.UTF8.GetBytes("{\"playerId\":\"42\",\"password\":\"x\"}"));
        Assert.Equal("tok-42", credentials.Token);
        Assert.Equal("42", credentials.PlayerId);
        // 模板 session.proto 无 expiry 字段：过期时间钩子恒 0（可选钩子语义）。
        Assert.Equal(0L, credentials.ExpiresAt);

        // Resume 回执不含 token（服务端沿用原凭据）：合并语义保留原 token。
        var resumed = await session.ResumeAsync(CancellationToken.None);
        Assert.Equal("tok-42", resumed.Token);
        Assert.Equal("42", resumed.PlayerId);

        var protocol = GatewaySessionProtocol.Instance;
        var notify = Encoding.UTF8.GetBytes(
            "{\"reason\":\"" + KickedReasonConst.KICKEDREASONLOGGEDINELSEWHERE + "\"}");
        var (reason, ok) = protocol.Kicked(SessionPushOps.KickedNotify, Envelope(FrameGen.Version, notify));
        Assert.True(ok);
        Assert.Equal("KICKED_REASON_LOGGED_IN_ELSEWHERE", reason);

        // 非推送 op：识别为「不是被挤下线」（ok=false，原因空）。
        var (unknownReason, unknownOk) = protocol.Kicked(SessionProtocolOps.Login, Envelope(FrameGen.Version, notify));
        Assert.False(unknownOk);
        Assert.Equal("", unknownReason);
    }

    // GeneratedProtocol_DecodesVer1AndVer2KickReason 验证接缝按帧头 version 选解码器：
    // ver=1（protojson/POCO）与 ver=2（protobuf/IMessage）都取到同一枚举名原因；
    // 未知 version / 空载荷 / 坏载荷不得抛错（op 命中即 ok=true，原因空串）。
    [Fact]
    public void GeneratedProtocol_DecodesVer1AndVer2KickReason()
    {
        var protocol = GatewaySessionProtocol.Instance;
        var json = Encoding.UTF8.GetBytes(
            "{\"reason\":\"" + KickedReasonConst.KICKEDREASONLOGGEDINELSEWHERE + "\"}");
        var (ver1Reason, ver1Ok) = protocol.Kicked(
            SessionPushOps.KickedNotify, Envelope(FrameGen.Version, json));
        Assert.True(ver1Ok);
        Assert.Equal("KICKED_REASON_LOGGED_IN_ELSEWHERE", ver1Reason);

        var proto = new ProtoKickedNotify { Reason = ProtoKickedReason.LoggedInElsewhere };
        var (ver2Reason, ver2Ok) = protocol.Kicked(
            SessionPushOps.KickedNotify, Envelope(FrameGen.Version2, proto.ToByteArray()));
        Assert.True(ver2Ok);
        // 两版本原因同口径（IMessage 的 C# 枚举名去下划线，故经生成描述符取 proto 原名）。
        Assert.Equal("KICKED_REASON_LOGGED_IN_ELSEWHERE", ver2Reason);

        foreach (var envelope in new[]
        {
            Envelope(9, new byte[] { 1, 2 }),                     // 未知 version
            Envelope(FrameGen.Version, Array.Empty<byte>()),      // 空载荷
            Envelope(FrameGen.Version2, new byte[] { 0xFF }),     // 坏 protobuf 载荷
        })
        {
            var (reason, ok) = protocol.Kicked(SessionPushOps.KickedNotify, envelope);
            Assert.True(ok); // op 命中即 ok=true（原因取不到不影响「已命中」判定）
            Assert.Equal("", reason);
        }
    }

    // GeneratedProtocol_Ver2Push_ClearsCredentialsWithReason 验证真链路：服务端以 ver=2
    // （protobuf wire）推送被挤下线，状态机带上帧头 version 交给接缝，原因不静默丢失。
    [Fact]
    public async Task GeneratedProtocol_Ver2Push_ClearsCredentialsWithReason()
    {
        await using var server = new SessionUdpServer();
        var reason = "";
        var session = new Session(new SessionOptions
        {
            Protocol = GatewaySessionProtocol.Instance,
            HeartbeatIntervalMs = 0,
            OnKicked = value => reason = value,
        });
        await using var client = await StartUdpClientAsync(server, session, UdpOptions(session));
        await session.LoginAsync(
            CancellationToken.None, Encoding.UTF8.GetBytes("{\"playerId\":\"42\",\"password\":\"x\"}"));
        Assert.Equal("tok-42", session.Token);

        await server.PushKickedAsync(FrameGen.Version2);
        await WaitUntilAsync(() => session.Token.Length == 0, TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => reason.Length > 0, TimeSpan.FromSeconds(2));
        Assert.Equal("KICKED_REASON_LOGGED_IN_ELSEWHERE", reason);
        Assert.Equal("KICKED_REASON_LOGGED_IN_ELSEWHERE", session.KickedReason);
    }

    // Kicked_EmptyReason_StillClearsCredentials 验证「op 命中但原因取不到」：reason 为空串
    // 时 ok 仍为 true，状态机照常清凭据（空原因绝不能让上层保留已失效的凭据）。
    [Fact]
    public async Task Kicked_EmptyReason_StillClearsCredentials()
    {
        await using var server = new SessionUdpServer();
        var fake = new FakeProtocol(GeneratedOps()) { KickedReason = "" };
        var callbackCount = 0;
        var session = new Session(new SessionOptions
        {
            Protocol = fake,
            HeartbeatIntervalMs = 0,
            OnKicked = _ => Interlocked.Increment(ref callbackCount),
        });
        await using var client = await StartUdpClientAsync(server, session, UdpOptions(session));
        await session.LoginAsync(CancellationToken.None, Encoding.UTF8.GetBytes("{}"));
        Assert.Equal("tok-42", session.Token);

        await server.PushKickedAsync();
        await WaitUntilAsync(() => session.Token.Length == 0, TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => Volatile.Read(ref callbackCount) == 1, TimeSpan.FromSeconds(2));
        Assert.Equal("", session.PlayerId);
        Assert.Equal("", session.KickedReason); // 原因取不到：空串（但凭据已清）
    }

    // NonKickedPush_KeepsCredentials 验证非命中 op 的推送不影响会话（ok=false 不清凭据）。
    [Fact]
    public async Task NonKickedPush_KeepsCredentials()
    {
        await using var server = new SessionUdpServer();
        var fake = new FakeProtocol(GeneratedOps()) { KickedOpHit = false };
        var session = new Session(new SessionOptions
        {
            Protocol = fake,
            HeartbeatIntervalMs = 0,
        });
        await using var client = await StartUdpClientAsync(server, session, UdpOptions(session));
        await session.LoginAsync(CancellationToken.None, Encoding.UTF8.GetBytes("{}"));

        await server.PushKickedAsync();
        await WaitUntilAsync(() => fake.KickedCalls > 0, TimeSpan.FromSeconds(2));
        await Task.Delay(150);

        Assert.Equal("tok-42", session.Token); // 非命中：凭据保留
        Assert.Equal("42", session.PlayerId);
    }

    // Bind_Twice_DeliversOnce_DisposeUnsubscribes 验证订阅生命周期：重复 Bind 不累积
    // 重复订阅（同一推送只处理一次），Dispose 后退订（后续推送不再处理）。
    [Fact]
    public async Task Bind_Twice_DeliversOnce_DisposeUnsubscribes()
    {
        await using var server = new SessionUdpServer();
        var fake = new FakeProtocol(GeneratedOps());
        var session = new Session(new SessionOptions
        {
            Protocol = fake,
            HeartbeatIntervalMs = 0,
        });
        // StartUdpClientAsync 内已 Bind 一次；此处再 Bind 一次（重绑先退订旧订阅）。
        await using var client = await StartUdpClientAsync(server, session, UdpOptions(session));
        await session.LoginAsync(CancellationToken.None, Encoding.UTF8.GetBytes("{}"));
        session.Bind(client);

        await server.PushKickedAsync();
        await WaitUntilAsync(() => fake.KickedCalls >= 1, TimeSpan.FromSeconds(2));
        await Task.Delay(200); // 给重复订阅的第二次分发留出时间窗。
        Assert.Equal(1, fake.KickedCalls); // 重复 Bind 不累积重复订阅。

        session.Dispose();
        var before = fake.KickedCalls;
        await server.PushKickedAsync();
        await Task.Delay(200);
        Assert.Equal(before, fake.KickedCalls); // Dispose 已退订：不再处理推送。
    }

    // Rebind_UnsubscribesPreviousClient 验证重绑到另一个 Client 时旧订阅被退订：
    // 旧通道的推送不得再交给接缝（否则订阅跨 Client 累积）。
    [Fact]
    public async Task Rebind_UnsubscribesPreviousClient()
    {
        await using var first = new SessionUdpServer();
        await using var second = new SessionUdpServer();
        var fake = new FakeProtocol(GeneratedOps());
        var session = new Session(new SessionOptions
        {
            Protocol = fake,
            HeartbeatIntervalMs = 0,
        });
        await using var firstClient = await StartUdpClientAsync(first, session, UdpOptions(session));
        await session.LoginAsync(CancellationToken.None, Encoding.UTF8.GetBytes("{}"));
        // 重绑到第二个 Client（StartUdpClientAsync 内 Bind）：旧 Client 的订阅应被退订。
        await using var secondClient = await StartUdpClientAsync(second, session, UdpOptions(session));
        await session.LoginAsync(CancellationToken.None, Encoding.UTF8.GetBytes("{}")); // 打到新 Client。

        var before = fake.KickedCalls;
        await first.PushKickedAsync(); // 旧 Client 的推送：不再交给接缝。
        await Task.Delay(200);
        Assert.Equal(before, fake.KickedCalls);

        await second.PushKickedAsync(); // 新 Client 的推送：仍然处理。
        await WaitUntilAsync(() => fake.KickedCalls > before, TimeSpan.FromSeconds(2));
    }

    // Ops_AreProvidedByProtocol 验证接缝的 op 名即生成物 op 串（无手写副本）。
    [Fact]
    public void GeneratedProtocolOps_MatchGeneratedConstants()
    {
        var ops = GatewaySessionProtocol.Instance.Ops;
        Assert.Equal(SessionProtocolOps.Register, ops.Register);
        Assert.Equal(SessionProtocolOps.Login, ops.Login);
        Assert.Equal(SessionProtocolOps.Resume, ops.Resume);
        Assert.Equal(SessionProtocolOps.Logout, ops.Logout);
        Assert.Equal(SessionProtocolOps.Heartbeat, ops.Heartbeat);
    }

    // StateMachine_DoesNotReferenceSessionMessageTypes 用反射钉住「状态机不引用会话
    // 消息类型」：Session/SessionOptions/SessionCredentials 的成员类型不得来自生成物
    // 的会话消息命名空间。
    [Fact]
    public void SessionMembers_DoNotReferenceSessionMessageTypes()
    {
        foreach (var type in new[] { typeof(Session), typeof(SessionOptions), typeof(SessionCredentials) })
        {
            foreach (var member in MemberTypes(type))
            {
                Assert.False(
                    IsSessionMessageType(member),
                    $"{type.Name} 的成员引用了会话消息类型 {member}");
            }
        }
    }

    // MemberTypes 收集类型的字段/属性/方法签名（含构造参数）引用的全部类型。
    private static IEnumerable<Type> MemberTypes(Type type)
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic
            | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;
        foreach (var field in type.GetFields(flags))
        {
            yield return field.FieldType;
        }
        foreach (var property in type.GetProperties(flags))
        {
            yield return property.PropertyType;
        }
        foreach (var method in type.GetMethods(flags))
        {
            yield return method.ReturnType;
            foreach (var parameter in method.GetParameters())
            {
                yield return parameter.ParameterType;
            }
        }
        foreach (var constructor in type.GetConstructors(flags))
        {
            foreach (var parameter in constructor.GetParameters())
            {
                yield return parameter.ParameterType;
            }
        }
    }

    // IsSessionMessageType 判定类型是否来自生成物的会话消息命名空间（并含泛型实参）。
    private static bool IsSessionMessageType(Type type)
    {
        if (type.IsGenericType)
        {
            foreach (var argument in type.GetGenericArguments())
            {
                if (IsSessionMessageType(argument))
                {
                    return true;
                }
            }
        }
        var ns = type.Namespace ?? "";
        return ns == "Atlas.Gateway.V1" || ns == "Atlas.Common.V1";
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
        throw new TimeoutException($"等待条件超时（{timeout.TotalSeconds}s）");
    }
}
