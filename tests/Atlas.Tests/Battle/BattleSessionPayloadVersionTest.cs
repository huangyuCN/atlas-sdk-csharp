using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Serialization;
using Atlas.Transport;
using Google.Protobuf;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleSessionPayloadVersionTest 覆盖载荷编码版本（规范 §3.1 载荷编码协商）在直连战斗会话上
// 的三条口径：
//   ① 透传：推送把帧头 version 与原始载荷原样交给调用方（ver=2 二进制由游戏协议包自行解码，
//      SDK 不预解、也不猜编码）；
//   ② 补帧基准：**任何版本**都只走调用方注入的帧号钩子（SDK 不再内建任何载荷解析），
//      未注入时不推进基准、也不报错（载荷非自描述，猜编码会静默错帧）；
//   ③ 请求/响应：白名单 {1,2} + 与本次会话的载荷编码一致，未知版本/失配一律明确抛协议错误；
//      出站版本可配（缺省 ver=1），非法版本在装配期（Create）即拒绝。
public sealed class BattleSessionPayloadVersionTest
{
    // Ticket 是测试票据密文（非真实 AEAD 票：接入层/帧面只按字节透传与编码）。
    private static readonly byte[] Ticket = { 0xfb, 0xff, 0x00, 0x01, 0xfe, 0x10, 0x20 };

    // Ver2Frame 是「看起来像 protobuf 二进制」的帧广播载荷：首字节 0x08 = field 1 varint，
    // 次字节 0x2A = 42。不是合法 JSON，任何按 JSON 解码的尝试都会失败——用它证明 SDK 没有预解。
    private static readonly byte[] Ver2Frame = { 0x08, 0x2A };

    private static DirectPlan Plan(int port)
    {
        var encoded = Convert.ToBase64String(Ticket);
        return DirectPlan.FromNotify("{\"match_id\":\"m-1\",\"battle_id\":\"b-1\",\"battle_ticket\":\""
            + encoded + "\",\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_UDP\",\"address\":\"127.0.0.1:"
            + port + "\"}]}");
    }

    private static BattleSessionOptions Options()
    {
        return new BattleSessionOptions
        {
            PreferredTransport = EdgeTransport.Udp,
            HelloTimeoutMs = 3000,
            InvokeTimeoutMs = 3000,
            BackoffBaseMs = 50,
            BackoffMaxMs = 200,
            HeartbeatInterval = TimeSpan.Zero,
        };
    }

    private static BattleSession FakeSession(BattleFakeTransport transport, BattleSessionOptions options)
    {
        var session = BattleSession.Create(Plan(7102), options);
        session.DialOverride = _ => Task.FromResult<ITransport>(transport);
        return session;
    }

    // WaitUntilAsync 轮询等待条件成立（推送分发是异步的，不接受固定 sleep 赌时序）。
    private static async Task<bool> WaitUntilAsync(Func<bool> condition, int timeoutMs = 3000)
    {
        var watch = Stopwatch.StartNew();
        while (watch.ElapsedMilliseconds < timeoutMs)
        {
            if (condition())
            {
                return true;
            }
            await Task.Delay(10);
        }
        return condition();
    }

    // ② ver=2 推送：统一出口（Push 事件 + 按 op 订阅）都拿到 version=2 与逐字相同的原始字节
    //——SDK 只透传，不解码。游戏消息的专用事件已随游戏域消息迁出（调用方按 op 订阅）。
    [Fact]
    public async Task FrameBroadcast_Ver2_DeliversRawPayloadAndVersionToUnifiedExits()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        await using var session = FakeSession(transport, options);
        BattlePush? unifiedPush = null;
        BattlePush? subscribedPush = null;
        session.Push += push => Volatile.Write(ref unifiedPush, push);
        using var subscription = session.OnPush(
            TestOps.FrameBroadcast, push => Volatile.Write(ref subscribedPush, push));
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.FrameBroadcast, Ver2Frame, FrameGen.Version2);

        Assert.True(await WaitUntilAsync(
            () => Volatile.Read(ref unifiedPush) != null && Volatile.Read(ref subscribedPush) != null),
            "ver=2 帧广播应分发到统一出口与订阅");
        foreach (var push in new[] { Volatile.Read(ref unifiedPush), Volatile.Read(ref subscribedPush) })
        {
            Assert.NotNull(push);
            Assert.Equal(TestOps.FrameBroadcast, push!.Op);
            Assert.Equal(FrameGen.Version2, push.Version); // 帧头版本原样透传。
            Assert.Equal(Ver2Frame, push.Payload); // 原始字节逐字一致（未经任何解码）。
        }

        await session.CloseAsync();
    }

    // ver=2 且未注入帧号钩子：补帧基准**不推进**，也不报错、不断链（与 TS/C# 既定口径一致）。
    [Fact]
    public async Task FrameBroadcast_Ver2_WithoutHook_KeepsBaselineWithoutError()
    {
        var transport = new BattleFakeTransport();
        await using var session = FakeSession(transport, Options());
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.OnPush(TestOps.FrameBroadcast, _ => signal.TrySetResult(true));
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.FrameBroadcast, Ver2Frame, FrameGen.Version2);
        await signal.Task.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Equal(0UL, session.LastSeenFrame); // 不推进（二进制载荷无从猜帧号）。
        Assert.Equal(ClientState.Connected, session.State); // 也不报错、不断链。
        Assert.False(session.HasEnded);

        await session.CloseAsync();
    }

    // ③ 注入帧号钩子后，ver=2 帧广播的补帧基准正确推进（旧实现见 ver≠1 直接 return ⇒ 本用例真红）。
    [Fact]
    public async Task FrameBroadcast_Ver2_WithHook_AdvancesBaseline()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        // 钩子直接从「生成包解码结果」里取帧号：首个 varint 字段即 frameId（示例用最简字节）。
        options.FrameNumberOf = payload =>
            payload.Length == 2 && payload.Span[0] == 0x08 ? payload.Span[1] : (ulong?)null;
        await using var session = FakeSession(transport, options);
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.OnPush(TestOps.FrameBroadcast, _ => signal.TrySetResult(true));
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.FrameBroadcast, Ver2Frame, FrameGen.Version2);
        await signal.Task.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Equal(42UL, session.LastSeenFrame);

        await session.CloseAsync();
    }

    // 钩子返回 null（本帧取不到帧号，如载荷里没有 frameId）：补帧基准保持原值，推送照常分发。
    [Fact]
    public async Task FrameBroadcast_Ver2_HookReturningNull_KeepsBaseline()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.FrameNumberOf = _ => null;
        await using var session = FakeSession(transport, options);
        BattlePush? received = null;
        session.OnPush(TestOps.FrameBroadcast, push => Volatile.Write(ref received, push));
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.FrameBroadcast, Ver2Frame, FrameGen.Version2);

        Assert.True(await WaitUntilAsync(() => Volatile.Read(ref received) != null), "取不到帧号也必须分发推送");
        Assert.Equal(0UL, session.LastSeenFrame);
        Assert.Equal(FrameGen.Version2, Volatile.Read(ref received)!.Version);

        await session.CloseAsync();
    }

    // ver=1 也走注入的帧号钩子：SDK 不再内建 protojson 解析（游戏域消息已迁出，载荷编码由
    // 调用方用协议包解码后再交给钩子）——同一份钩子对 ver=1/ver=2 一视同仁。
    [Fact]
    public async Task FrameBroadcast_Ver1_UsesInjectedHook_NoBuiltInParsing()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        var hookCalls = 0;
        options.FrameNumberOf = _ =>
        {
            Interlocked.Increment(ref hookCalls);
            return 99UL;
        };
        await using var session = FakeSession(transport, options);
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.OnPush(TestOps.FrameBroadcast, _ => signal.TrySetResult(true));
        await session.ConnectAsync();

        var json = Encoding.UTF8.GetBytes("{\"battleId\":\"b-1\",\"frame\":{\"frameId\":\"9\"}}");
        transport.QueueNotify(TestOps.FrameBroadcast, json, FrameGen.Version);
        await signal.Task.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Equal(99UL, session.LastSeenFrame); // 钩子返回值（不是载荷里的 9：SDK 不解载荷）。
        Assert.Equal(1, Volatile.Read(ref hookCalls)); // ver=1 同样调用钩子。

        await session.CloseAsync();
    }

    // ④ 响应帧版本不在白名单 {1,2}：明确抛协议错误（错误消息须指明白名单）。
    [Fact]
    public async Task Response_UnknownVersion_ThrowsProtocolErrorWithWhitelistMessage()
    {
        var transport = new BattleFakeTransport();
        await using var session = FakeSession(transport, Options());
        await session.ConnectAsync();
        var invoke = session.InvokeAsync(TestOps.JoinBattle, TestWire.Join("b-1"));
        await transport.WaitForWritesAsync(1);

        transport.QueueSuccessVersion(transport.WrittenHeaders[^1].Seq, Array.Empty<byte>(), 3);

        var exception = await Assert.ThrowsAsync<ProtocolException>(() => invoke);
        Assert.Contains("白名单", exception.Message, StringComparison.Ordinal);
        Assert.False(session.HasEnded);
    }

    // 出站版本可配：ver=2 会话按帧头 ver=2 发请求，并接受 ver=2 回执（白名单放行 ver=2）。
    [Fact]
    public async Task Session_Ver2_WritesVer2AndAcceptsVer2Reply()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.Serializer = new ProtobufSerializer();
        await using var session = FakeSession(transport, options);
        await session.ConnectAsync();

        var request = new byte[] { 0x0A, 0x03, (byte)'b', (byte)'-', (byte)'1' }; // protobuf 二进制
        var invoke = session.InvokeAsync(TestOps.JoinBattle, request);
        await transport.WaitForWritesAsync(1);
        Assert.Equal(FrameGen.Version2, transport.WrittenHeaders[^1].Version);

        var reply = new byte[] { 0x08, 0x01 };
        transport.QueueSuccessVersion(transport.WrittenHeaders[^1].Seq, reply, FrameGen.Version2);

        Assert.Equal(reply, await invoke);
        await session.CloseAsync();
    }

    // 响应版本与会话载荷编码失配（都在白名单内）：仍是不可重试的协议级失败（fail fast）。
    [Fact]
    public async Task Response_VersionMismatch_ThrowsProtocolError()
    {
        var transport = new BattleFakeTransport();
        var options = Options();
        options.Serializer = new ProtobufSerializer();
        await using var session = FakeSession(transport, options);
        await session.ConnectAsync();
        var invoke = session.InvokeAsync(TestOps.JoinBattle, Array.Empty<byte>());
        await transport.WaitForWritesAsync(1);

        transport.QueueSuccessVersion(transport.WrittenHeaders[^1].Seq, Array.Empty<byte>(), FrameGen.Version);

        var exception = await Assert.ThrowsAsync<ProtocolException>(() => invoke);
        Assert.Contains("不一致", exception.Message, StringComparison.Ordinal);
    }

    // ④ 未知载荷编码版本在装配期（Create，即拨号前）即拒绝：不留「连上了才发现版本非法」的窗口。
    [Fact]
    public void Create_UnknownSerializerVersion_RejectedAtAssembly()
    {
        var options = Options();
        options.Serializer = new FixedVersionSerializer(3);

        Assert.Throws<ArgumentException>(() => BattleSession.Create(Plan(7102), options));
    }

    // ⑤ 默认配置（不注入序列化器与钩子）：仍是 ver=1 线格式；补帧基准不推进（没有内建解析），
    // 但推送照常分发——「不解载荷」是 SDK 的边界，不是故障。
    [Fact]
    public async Task DefaultSession_KeepsVer1Wire_AndLeavesBaselineToHook()
    {
        var transport = new BattleFakeTransport();
        await using var session = FakeSession(transport, Options());
        var signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        session.OnPush(TestOps.FrameBroadcast, _ => signal.TrySetResult(true));
        await session.ConnectAsync();

        var invoke = session.InvokeAsync(TestOps.SyncFrames, TestWire.Sync("b-1", 11));
        await transport.WaitForWritesAsync(1);
        Assert.Equal(FrameGen.Version, transport.WrittenHeaders[^1].Version);

        transport.QueueSuccessVersion(
            transport.WrittenHeaders[^1].Seq, Encoding.UTF8.GetBytes("{}"), FrameGen.Version);
        await invoke;

        transport.QueueNotify(
            TestOps.FrameBroadcast,
            Encoding.UTF8.GetBytes("{\"frame\":{\"frameId\":\"5\"}}"),
            FrameGen.Version);
        await signal.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(0UL, session.LastSeenFrame); // 未注入钩子：不猜载荷编码、不推进基准。

        await session.CloseAsync();
    }

    // FixedVersionSerializer 是只声明载荷编码版本的假序列化器：本文件只验证版本协商，
    // 不触发编解码（会话的 raw 调用面不经过序列化器）。
    private sealed class FixedVersionSerializer : ISerializer
    {
        public FixedVersionSerializer(int version)
        {
            Version = version;
        }

        public int Version { get; }

        public byte[] Serialize(IMessage message) => Array.Empty<byte>();

        public byte[] Serialize(object message) => Array.Empty<byte>();

        public object Deserialize(byte[] data, Type type) => throw new NotSupportedException();
    }
}
