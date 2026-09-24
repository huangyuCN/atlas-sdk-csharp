using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Serialization;
using Atlas.Gateway.V1;
using Google.Protobuf;
using SessionStub = Atlas.Gateway.V1.Session;
using Xunit;

namespace Atlas.Tests.Client;

// DtoTest 验证生成 stub（protoc-gen-atlas-client lang=csharp）的接入语义：
//   1. DTO 为 protojson JSON 语义（camelCase 字段名、整数字段读字符串、枚举名字符串）；
//   2. stub 经构造注入的 ISerializer 编解码——不写死 System.Text.Json（R12 方案 A）；
//   3. stub 透传 InvokeOptions（含幂等键）到 IAtlasInvoker 的选项重载（C.5）。
public sealed class DtoTest
{
    // FakeInvoker 记录 stub 下发的 op/payload/选项并回预置载荷。
    private sealed class FakeInvoker : IAtlasInvoker
    {
        public string? LastOperation { get; private set; }

        public byte[]? LastPayload { get; private set; }

        public InvokeOptions? LastOptions { get; private set; }

        public byte[] Reply { get; set; } = Array.Empty<byte>();

        public Task<byte[]> InvokeRawAsync(
            string operation, byte[]? payload, CancellationToken cancellationToken)
        {
            return InvokeRawAsync(operation, payload, null, cancellationToken);
        }

        public Task<byte[]> InvokeRawAsync(
            string operation, byte[]? payload, InvokeOptions? options, CancellationToken cancellationToken)
        {
            LastOperation = operation;
            LastPayload = payload;
            LastOptions = options;
            return Task.FromResult(Reply);
        }
    }

    // FakeSerializer 是零协议依赖的假序列化器：stub 的 DTO ↔ payload 全部经它编解码。
    private sealed class FakeSerializer : ISerializer
    {
        public int Version => 1;

        public object? LastSerialized { get; private set; }

        public Type? LastDeserializeType { get; private set; }

        public byte[] Payload { get; set; } = Encoding.UTF8.GetBytes("fake-payload");

        public object? Reply { get; set; }

        public byte[] Serialize(IMessage message) => Payload;

        public byte[] Serialize(object message)
        {
            LastSerialized = message;
            return Payload;
        }

        public object Deserialize(byte[] data, Type type)
        {
            LastDeserializeType = type;
            return Reply!;
        }
    }

    // Stub_InjectedSerializer_DrivesCodec 验证注入 fake ISerializer 即可驱动 stub
    // 编解码（stub 内不再有 System.Text.Json 直调）。
    [Fact]
    public async Task Stub_InjectedSerializer_DrivesCodec()
    {
        var invoker = new FakeInvoker { Reply = new byte[] { 0x7B, 0x7D } };
        var serializer = new FakeSerializer
        {
            Reply = new LoginReply { PlayerId = "p1", Token = "t1" },
        };
        var stub = new SessionStub(invoker, serializer);

        var reply = await stub.LoginAsync(new LoginRequest { PlayerId = "p1", Password = "x" });

        Assert.Equal(SessionProtocolOps.Login, invoker.LastOperation);
        Assert.Equal(serializer.Payload, invoker.LastPayload);
        Assert.IsType<LoginRequest>(serializer.LastSerialized);
        Assert.Equal(typeof(LoginReply), serializer.LastDeserializeType);
        Assert.Equal("p1", reply!.PlayerId);
        Assert.Equal("t1", reply.Token);
    }

    // Stub_NullRequest_SendsZeroValueDto 验证 req 为 null 时 stub 序列化零值请求
    //（protojson 零值省略：服务端按默认值处理）。
    [Fact]
    public async Task Stub_NullRequest_SendsZeroValueDto()
    {
        var invoker = new FakeInvoker();
        var serializer = new FakeSerializer { Reply = new LogoutReply() };
        var stub = new SessionStub(invoker, serializer);

        await stub.LogoutAsync(null);

        Assert.Equal(SessionProtocolOps.Logout, invoker.LastOperation);
        Assert.IsType<LogoutRequest>(serializer.LastSerialized);
    }

    // Stub_PassesInvokeOptions 验证调用级选项（含幂等键）透传到 IAtlasInvoker 新重载。
    [Fact]
    public async Task Stub_PassesInvokeOptions_WithIdempotencyKey()
    {
        var invoker = new FakeInvoker { Reply = Array.Empty<byte>() };
        var stub = new SessionStub(invoker, new FakeSerializer());
        var options = new InvokeOptions { IdempotencyKey = "order-42" };

        await stub.HeartbeatAsync(new HeartbeatRequest { Ts = 1L }, options, CancellationToken.None);

        Assert.Same(options, invoker.LastOptions);
        Assert.Equal("order-42", invoker.LastOptions!.IdempotencyKey);
        Assert.Equal(SessionProtocolOps.Heartbeat, invoker.LastOperation);
    }

    // Stub_EmptyReply_ReturnsNull 验证空回执不解析（data.Length == 0 → null）。
    [Fact]
    public async Task Stub_EmptyReply_ReturnsNull()
    {
        var invoker = new FakeInvoker { Reply = Array.Empty<byte>() };
        var stub = new SessionStub(invoker, new FakeSerializer());

        Assert.Null(await stub.LoginAsync(new LoginRequest()));
    }

    // PocoDto_ProtoJsonFieldNames 验证 POCO 的 protojson 语义：camelCase 字段名
    // （对齐权威 proto 的 JSON 名）与 64 位整数读字符串。
    [Fact]
    public void PocoDto_ProtoJsonFieldNames()
    {
        var serializer = new JsonSerializer();

        var json = Encoding.UTF8.GetString(serializer.Serialize(
            (object)new ResumeRequest { Token = "t1", PlayerId = "p1", ClientVersion = "0.1.0" }));
        Assert.Contains("\"token\":\"t1\"", json);
        Assert.Contains("\"playerId\":\"p1\"", json);
        Assert.Contains("\"clientVersion\":\"0.1.0\"", json);

        // uint64 读端兼容 protojson 的字符串下发；写端为 number（服务端 protojson 解析接受）。
        var reply = (HeartbeatReply)serializer.Deserialize(
            Encoding.UTF8.GetBytes("{\"serverTimeUnixMs\":\"1700000000000\"}"), typeof(HeartbeatReply));
        Assert.Equal(1700000000000UL, reply.ServerTimeUnixMs);
    }

    // PocoDto_KickedNotify_ReasonIsEnumName 验证推送 DTO 的原因字段是枚举名（protojson
    // 枚举下发形态；未知枚举名前向兼容）。
    [Fact]
    public void PocoDto_KickedNotify_ReasonIsEnumName()
    {
        var serializer = new JsonSerializer();

        var notify = (KickedNotify)serializer.Deserialize(
            Encoding.UTF8.GetBytes("{\"reason\":\"KICKED_REASON_SESSION_EXPIRED\"}"), typeof(KickedNotify));

        Assert.Equal(KickedReasonConst.KICKEDREASONSESSIONEXPIRED, notify.Reason);
    }
}
