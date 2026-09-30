using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Errors;
using Atlas.Frame;
using KcpSharp;

namespace Atlas.Tests.Battle;

// EdgeRequest 记录服务端收到的一次帧请求（op / 帧会话槽 / 载荷），供「线格式逐字节断言」用。
internal sealed class EdgeRequest
{
    public string Op { get; set; } = "";

    public string Slot { get; set; } = "";

    public byte[] Payload { get; set; } = Array.Empty<byte>();

    public byte Flags { get; set; }

    public byte Version { get; set; }
}

// EdgeReply 构造帧回执载荷（对齐 src/Atlas/Frame/Reply.cs 的包络口径）：
// 成功 [hasError=0][dataLen:u32][data]；业务错误 [hasError=1][statusLen:u32][Status][dataLen:u32][data]。
internal static class EdgeReply
{
    public static byte[] Success(byte[] payload)
    {
        var envelope = new byte[5 + payload.Length];
        WriteUInt32(envelope, 1, (uint)payload.Length);
        Array.Copy(payload, 0, envelope, 5, payload.Length);
        return envelope;
    }

    // BusinessError 构造带 Status 的业务拒绝回执（code/reason/message + class=BUSINESS）。
    public static byte[] BusinessError(int code, string reason, string message)
    {
        var status = new List<byte>();
        status.Add(0x08); // field 1: code（varint）
        WriteVarint(status, (ulong)code);
        WriteStringField(status, 2, reason);
        WriteStringField(status, 3, message);
        status.Add(0x28); // field 5: class（varint，1 = ERROR_CLASS_BUSINESS）
        WriteVarint(status, 1);

        var envelope = new byte[5 + status.Count + 4];
        envelope[0] = 1;
        WriteUInt32(envelope, 1, (uint)status.Count);
        status.CopyTo(envelope, 5);
        WriteUInt32(envelope, 5 + status.Count, 0);
        return envelope;
    }

    private static void WriteStringField(List<byte> buffer, int field, string value)
    {
        buffer.Add((byte)((field << 3) | 2));
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(buffer, (ulong)bytes.Length);
        buffer.AddRange(bytes);
    }

    private static void WriteVarint(List<byte> buffer, ulong value)
    {
        while (value >= 0x80)
        {
            buffer.Add((byte)(value | 0x80));
            value >>= 7;
        }
        buffer.Add((byte)value);
    }

    private static void WriteUInt32(byte[] bytes, int offset, uint value)
    {
        bytes[offset] = (byte)(value >> 24);
        bytes[offset + 1] = (byte)(value >> 16);
        bytes[offset + 2] = (byte)(value >> 8);
        bytes[offset + 3] = (byte)value;
    }
}

// EdgeFrameCodec 是测试服务端的帧请求解析：解帧 → 解析 body 各段（与 SDK 同一实现，避免自造口径）。
internal static class EdgeFrameCodec
{
    public static bool TryParse(byte[] body, Header header, out EdgeRequest request)
    {
        request = new EdgeRequest();
        try
        {
            var (op, slot, _, payload) = Body.ParseRequestBodyFull(body, header.Flags);
            request.Op = op;
            request.Slot = slot;
            request.Payload = payload;
            request.Flags = header.Flags;
            request.Version = header.Version;
            return true;
        }
        catch (ProtocolException)
        {
            return false;
        }
    }
}

// BattleEdgeUdpServer 模拟「接入层 + battle 帧面」的 UDP 面：
// 首包 hello（ATLH 段）→ 回 8 字节 flow-id → 后续数据报必须带 flow-id 前缀，
// 剥前缀解帧后按 op 回预置回执；可主动推送 Notify 帧（帧广播 / 战斗结束）。
internal sealed class BattleEdgeUdpServer : IAsyncDisposable
{
    private readonly Socket _socket = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _serveTask;
    private readonly List<EdgeRequest> _requests = new();
    private readonly object _gate = new();
    private readonly Dictionary<string, string> _replies = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (int Code, string Reason, string Message)> _failures = new(StringComparer.Ordinal);
    private readonly ulong _flowId;
    private IPEndPoint? _remote;

    public BattleEdgeUdpServer()
    {
        _socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        Port = ((IPEndPoint)_socket.LocalEndPoint!).Port;
        _flowId = 0x0102030405060708;
        _serveTask = Task.Run(() => ServeAsync(_cts.Token));
    }

    public int Port { get; }

    // RejectHello 为 true 时对 hello 不回 flow-id（模拟接入层拒绝：断开 + 无回执）。
    public bool RejectHello { get; set; }

    // HelloCount 是收到的 hello 首包数（「拒绝不重试」断言用）。
    public int HelloCount { get; private set; }

    // HelloDatagram 是最近一次收到的 hello 原始数据报（逐字节断言用）。
    public byte[] HelloDatagram { get; private set; } = Array.Empty<byte>();

    // FlowId 是本次接入层流号（数据报前缀的期望值）。
    public ulong FlowId => _flowId;

    // LastFrameDatagram 是最近一次收到的帧数据报（含 flow-id 前缀，前缀断言用）。
    public byte[] LastFrameDatagram { get; private set; } = Array.Empty<byte>();

    public EdgeRequest[] Requests
    {
        get
        {
            lock (_gate)
            {
                return _requests.ToArray();
            }
        }
    }

    public EdgeRequest[] RequestsOf(string op)
    {
        var all = Requests;
        var result = new List<EdgeRequest>();
        foreach (var request in all)
        {
            if (request.Op == op)
            {
                result.Add(request);
            }
        }
        return result.ToArray();
    }

    // SetReply 覆盖指定 op 的成功回执 JSON。
    public void SetReply(string op, string json)
    {
        lock (_gate)
        {
            _replies[op] = json;
        }
    }

    // FailWith 使指定 op 回业务拒绝（票过期等可判定错误）。
    public void FailWith(string op, int code, string reason, string message)
    {
        lock (_gate)
        {
            _failures[op] = (code, reason, message);
        }
    }

    // PushNotifyAsync 推送 Notify 帧（version 为帧头载荷编码版本；带 flow-id 前缀）。
    public async Task PushNotifyAsync(string op, byte[] payload, byte version = FrameGen.Version)
    {
        var remote = _remote ?? throw new InvalidOperationException("尚无客户端 hello");
        var body = Body.BuildRequestBody(op, payload);
        var header = new Header
        {
            Magic = FrameGen.Magic,
            Version = version,
            Type = MsgType.Notify,
            Seq = 1,
        };
        var frame = FrameIO.EncodeMessage(header, body, FrameGen.MaxBodySize);
        await SendFramedAsync(frame, remote, _cts.Token);
    }

    private async Task ServeAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[UdpTransportProbe.MaxDatagramSize];
        var any = new IPEndPoint(IPAddress.Any, 0);
        while (!cancellationToken.IsCancellationRequested)
        {
            SocketReceiveFromResult received;
            try
            {
                received = await _socket.ReceiveFromAsync(
                    new ArraySegment<byte>(buffer), SocketFlags.None, any, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (SocketException)
            {
                break;
            }

            var datagram = new byte[received.ReceivedBytes];
            Array.Copy(buffer, datagram, datagram.Length);
            var remote = new IPEndPoint(
                ((IPEndPoint)received.RemoteEndPoint).Address,
                ((IPEndPoint)received.RemoteEndPoint).Port);
            await OnDatagramAsync(datagram, remote, cancellationToken);
        }
    }

    // OnDatagramAsync 处理一个数据报：hello 首包换 flow-id，其余校验前缀后解帧回执。
    private async Task OnDatagramAsync(byte[] datagram, IPEndPoint remote, CancellationToken cancellationToken)
    {
        if (EdgeWire.IsHello(datagram))
        {
            HelloCount++;
            HelloDatagram = datagram;
            if (RejectHello)
            {
                return; // 拒绝：不回 flow-id。
            }
            _remote = remote;
            await _socket.SendToAsync(
                new ArraySegment<byte>(EdgeWire.EncodeFlowId(_flowId)), SocketFlags.None, remote, cancellationToken);
            return;
        }

        LastFrameDatagram = datagram;
        if (!TryStripFlowId(datagram, out var payload))
        {
            return; // 前缀不符/过短：丢弃（服务端语义）。
        }
        await HandleFrameAsync(payload, remote, cancellationToken);
    }

    private bool TryStripFlowId(byte[] datagram, out byte[] payload)
    {
        payload = Array.Empty<byte>();
        try
        {
            payload = EdgeWire.StripFlowId(datagram, _flowId);
            return true;
        }
        catch (ProtocolException)
        {
            return false;
        }
    }

    private async Task HandleFrameAsync(byte[] payload, IPEndPoint remote, CancellationToken cancellationToken)
    {
        Header header;
        byte[] body;
        try
        {
            (header, body) = FrameIO.DecodeMessage(payload, FrameGen.MaxBodySize);
        }
        catch (ProtocolException)
        {
            return;
        }
        if (!EdgeFrameCodec.TryParse(body, header, out var request))
        {
            return;
        }
        lock (_gate)
        {
            _requests.Add(request);
        }
        var responseBody = BuildReply(request.Op);
        var responseHeader = new Header
        {
            Magic = FrameGen.Magic,
            Version = FrameGen.Version,
            Type = MsgType.Response,
            Seq = header.Seq,
        };
        var frame = FrameIO.EncodeMessage(responseHeader, responseBody, FrameGen.MaxBodySize);
        await SendFramedAsync(frame, remote, cancellationToken);
    }

    private byte[] BuildReply(string op)
    {
        lock (_gate)
        {
            if (_failures.TryGetValue(op, out var failure))
            {
                return EdgeReply.BusinessError(failure.Code, failure.Reason, failure.Message);
            }
            if (_replies.TryGetValue(op, out var json))
            {
                return EdgeReply.Success(Encoding.UTF8.GetBytes(json));
            }
        }
        return EdgeReply.Success(Encoding.UTF8.GetBytes(BattleEdgeDefaults.ReplyJson(op)));
    }

    private Task SendFramedAsync(byte[] frame, IPEndPoint remote, CancellationToken cancellationToken)
    {
        var datagram = EdgeWire.WrapFlowId(_flowId, frame);
        return _socket.SendToAsync(new ArraySegment<byte>(datagram), SocketFlags.None, remote, cancellationToken).AsTask();
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _socket.Dispose();
        try
        {
            await _serveTask.WaitAsync(TimeSpan.FromSeconds(1));
        }
        catch
        {
            // 忽略收尾异常（取消/关闭路径）。
        }
        _cts.Dispose();
    }
}

// BattleEdgeDefaults 是 battle 帧面的缺省回执（protojson 形态，字段名用 protojson 默认口径）。
internal static class BattleEdgeDefaults
{
    public const string JoinBattleReply = "{\"meta\":{\"sessionId\":\"b-1\"},\"currentFrame\":\"0\"}";

    public const string SyncFramesReply = "{\"currentFrame\":\"5\",\"missed\":[]}";

    public static string ReplyJson(string op)
    {
        switch (op)
        {
            case BattleOps.JoinBattle:
                return JoinBattleReply;
            case BattleOps.SyncFrames:
                return SyncFramesReply;
            default:
                return "{}";
        }
    }
}

// UdpTransportProbe 只提供数据报缓冲上限常量（避免测试直接依赖承载体内部实现）。
internal static class UdpTransportProbe
{
    public const int MaxDatagramSize = 64 * 1024;
}

// BattleEdgeKcpServer 模拟「接入层 + battle 帧面」的 KCP 面：
// **首个数据报必须是 hello 段**（顺序断言）→ 回 flow-id → 其后的数据报剥 flow-id 前缀
// 喂 KCP 会话；会话发回的数据报加回前缀。帧组装为「头消息(16B) + body 消息」。
