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

internal sealed class BattleEdgeKcpServer : IAsyncDisposable
{
    private readonly Socket _socket = new(SocketType.Dgram, ProtocolType.Udp);
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _serveTask;
    private readonly Dictionary<EndPoint, KcpEdgeSession> _sessions = new();
    private readonly object _gate = new();
    private readonly ulong _flowId = 0x0a0b0c0d0e0f0102;
    private readonly KcpConversationOptions _options;

    public BattleEdgeKcpServer()
    {
        _options = new KcpConversationOptions
        {
            NoDelay = false,
            UpdateInterval = 40,
            FastResend = 0,
            DisableCongestionControl = false,
            SendWindow = 128,
            ReceiveWindow = 128,
            RemoteReceiveWindow = 128,
            Mtu = 1400,
            StreamMode = false,
        };
        _socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        Port = ((IPEndPoint)_socket.LocalEndPoint!).Port;
        _serveTask = Task.Run(() => ServeAsync(_cts.Token));
    }

    public int Port { get; }

    // FirstDatagram 是服务端收到的第一个数据报（必须是 hello 段：先 hello 后会话的顺序断言）。
    public byte[] FirstDatagram { get; private set; } = Array.Empty<byte>();

    // DatagramCount 是收到的数据报总数（含 hello）。
    public int DatagramCount { get; private set; }

    // HelloCount 是识别为 hello 的首包数。
    public int HelloCount { get; private set; }

    // KcpDatagramCount 是 hello 之后（KCP 会话）收到的数据报数。
    public int KcpDatagramCount { get; private set; }

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

    private readonly List<EdgeRequest> _requests = new();

    private async Task ServeAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[65536];
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
            await OnDatagramAsync(datagram, received.RemoteEndPoint, cancellationToken);
        }
    }

    // OnDatagramAsync 处理一个数据报：首个数据报必须是 hello（顺序断言），其后剥前缀喂 KCP 会话。
    private async Task OnDatagramAsync(byte[] datagram, EndPoint remote, CancellationToken cancellationToken)
    {
        DatagramCount++;
        if (DatagramCount == 1)
        {
            FirstDatagram = datagram;
        }
        if (EdgeWire.IsHello(datagram))
        {
            HelloCount++;
            await _socket.SendToAsync(
                new ArraySegment<byte>(EdgeWire.EncodeFlowId(_flowId)), SocketFlags.None, remote, cancellationToken);
            return;
        }

        byte[] payload;
        try
        {
            payload = EdgeWire.StripFlowId(datagram, _flowId);
        }
        catch (ProtocolException)
        {
            return; // 前缀不符/过短：丢弃。
        }
        KcpDatagramCount++;
        var session = GetOrCreateSession(remote, payload);
        if (session != null)
        {
            _ = session.InputAsync(payload);
        }
    }

    private KcpEdgeSession? GetOrCreateSession(EndPoint remote, byte[] firstPacket)
    {
        lock (_gate)
        {
            if (_sessions.TryGetValue(remote, out var existing))
            {
                return existing;
            }
            if (firstPacket.Length < 4)
            {
                return null;
            }
            var conv = firstPacket[0] | (firstPacket[1] << 8) | (firstPacket[2] << 16) | (firstPacket[3] << 24);
            var session = new KcpEdgeSession(this, _socket, remote, conv, _options, _flowId);
            _sessions[remote] = session;
            return session;
        }
    }

    private void OnClosed(EndPoint remote)
    {
        lock (_gate)
        {
            _sessions.Remove(remote);
        }
    }

    private void Record(EdgeRequest request)
    {
        lock (_gate)
        {
            _requests.Add(request);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _socket.Dispose();
        lock (_gate)
        {
            foreach (var session in _sessions.Values)
            {
                session.Stop();
            }
            _sessions.Clear();
        }
        try
        {
            await _serveTask.WaitAsync(TimeSpan.FromSeconds(1));
        }
        catch
        {
            // 忽略收尾异常。
        }
        _cts.Dispose();
    }

    // KcpEdgeSession 是单个客户端会话：KCP 承载（发包加 flow-id 前缀）+ 帧级应答。
    private sealed class KcpEdgeSession : IKcpTransport
    {
        private readonly BattleEdgeKcpServer _server;
        private readonly Socket _socket;
        private readonly EndPoint _remote;
        private readonly ulong _flowId;
        private readonly KcpConversation _conversation;
        private readonly CancellationTokenSource _cts = new();

        public KcpEdgeSession(
            BattleEdgeKcpServer server,
            Socket socket,
            EndPoint remote,
            int conversationId,
            KcpConversationOptions options,
            ulong flowId)
        {
            _server = server;
            _socket = socket;
            _remote = remote;
            _flowId = flowId;
            _conversation = new KcpConversation(this, conversationId, options);
            _ = Task.Run(ReceiveLoopAsync);
        }

        public void Stop()
        {
            _cts.Cancel();
            _conversation.Dispose();
        }

        public ValueTask SendPacketAsync(Memory<byte> packet, CancellationToken cancellationToken)
        {
            var datagram = EdgeWire.WrapFlowId(_flowId, packet.ToArray());
            return new ValueTask(
                _socket.SendToAsync(new ArraySegment<byte>(datagram), SocketFlags.None, _remote, cancellationToken).AsTask());
        }

        public Task InputAsync(byte[] packet)
        {
            return _conversation.InputPakcetAsync(packet, CancellationToken.None).AsTask();
        }

        private async Task ReceiveLoopAsync()
        {
            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    var result = await _conversation.WaitToReceiveAsync(_cts.Token);
                    if (result.TransportClosed || result.BytesReceived == 0)
                    {
                        if (result.TransportClosed)
                        {
                            break;
                        }
                        continue;
                    }
                    var message = new byte[result.BytesReceived];
                    if (!_conversation.TryReceive(message, out result))
                    {
                        break;
                    }
                    await HandleMessageAsync(message);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception)
            {
                // 会话级异常：结束该会话。
            }
            finally
            {
                _server.OnClosed(_remote);
            }
        }

        // HandleMessageAsync 组装「头消息 + body 消息」为一帧，记录后按 op 回执。
        private async Task HandleMessageAsync(byte[] first)
        {
            if (first.Length != FrameGen.HeaderSize)
            {
                return; // 非帧首消息：丢弃。
            }
            Header header;
            try
            {
                header = Header.Decode(first);
            }
            catch (ProtocolException)
            {
                return;
            }
            var body = Array.Empty<byte>();
            if (header.Length > 0)
            {
                body = await ReceiveBodyAsync(header.Length);
            }
            if (!EdgeFrameCodec.TryParse(body, header, out var request))
            {
                return;
            }
            _server.Record(request);

            var responseBody = EdgeReply.Success(
                Encoding.UTF8.GetBytes(BattleEdgeDefaults.ReplyJson(request.Op)));
            var responseHeader = new Header
            {
                Magic = FrameGen.Magic,
                Version = FrameGen.Version,
                Type = MsgType.Response,
                Seq = header.Seq,
                // 头/body 分两条消息发送：bodyLen 必须显式写入（Encode 只按字段值编码）。
                Length = (uint)responseBody.Length,
            };
            await _conversation.SendAsync(responseHeader.Encode(), _cts.Token);
            if (responseBody.Length > 0)
            {
                await _conversation.SendAsync(responseBody, _cts.Token);
            }
        }

        private async Task<byte[]> ReceiveBodyAsync(uint length)
        {
            var body = new byte[length];
            var filled = 0;
            while (filled < body.Length)
            {
                var result = await _conversation.WaitToReceiveAsync(_cts.Token);
                if (result.TransportClosed || result.BytesReceived == 0)
                {
                    break;
                }
                var chunk = new byte[result.BytesReceived];
                if (!_conversation.TryReceive(chunk, out result))
                {
                    break;
                }
                var take = Math.Min(chunk.Length, body.Length - filled);
                Array.Copy(chunk, 0, body, filled, take);
                filled += take;
            }
            return body;
        }
    }
}
