using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using KcpSharp;

namespace Atlas.Tests.Client;

// SessionUdpServer 是会话测试服务端（UDP）：按 op 回预置 protojson 回执，并记录
// 每次请求的 "op|session"（会话槽与凭据流转断言用；对齐 Go sessionTestServer）。
internal sealed class SessionUdpServer : IAsyncDisposable
{
    private readonly Socket _socket = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
    private readonly ConcurrentQueue<string> _seen = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _serveTask;
    private IPEndPoint? _lastRemote;

    public SessionUdpServer()
    {
        _socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        Port = ((IPEndPoint)_socket.LocalEndPoint!).Port;
        _serveTask = Task.Run(() => ServeAsync(_cts.Token));
    }

    public int Port { get; }

    // Seen 返回服务端收到的全部 "op|session" 记录（按到达顺序）。
    public string[] Seen => _seen.ToArray();

    // Saw 判断服务端是否收到过指定 (op, session) 的请求。
    public bool Saw(string op, string session)
    {
        return _seen.Contains(op + "|" + session);
    }

    // PushKickedAsync 向最近一次请求的客户端推送被挤下线 Notify 帧。
    public async Task PushKickedAsync()
    {
        var remote = _lastRemote ?? throw new InvalidOperationException("尚无客户端请求");
        var body = Body.BuildRequestBody(Session.KickedNotifyOperation, null);
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Notify,
            Seq = 1,
        };
        var datagram = FrameIO.EncodeMessage(header, body, FrameConst.MaxBodySize);
        await _socket.SendToAsync(
            new ArraySegment<byte>(datagram), SocketFlags.None, remote, _cts.Token);
    }

    private async Task ServeAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[UdpTransport.MaxDatagramSize];
        var remote = new IPEndPoint(IPAddress.Any, 0);
        while (!cancellationToken.IsCancellationRequested)
        {
            SocketReceiveFromResult received;
            try
            {
                received = await _socket.ReceiveFromAsync(
                    new ArraySegment<byte>(buffer), SocketFlags.None, remote, cancellationToken);
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
            var requestRemote = new IPEndPoint(
                ((IPEndPoint)received.RemoteEndPoint).Address,
                ((IPEndPoint)received.RemoteEndPoint).Port);

            Header header;
            byte[] body;
            try
            {
                (header, body) = FrameIO.DecodeMessage(datagram, FrameConst.MaxBodySize);
            }
            catch (ProtocolException)
            {
                continue; // 坏数据报静默丢弃。
            }
            string op;
            string session;
            try
            {
                (op, session, _) = Body.ParseRequestBodyWithSession(body, header.Flags);
            }
            catch (ProtocolException)
            {
                continue;
            }

            _seen.Enqueue(op + "|" + session);
            _lastRemote = requestRemote;
            await WriteReplyAsync(requestRemote, header.Seq, op, cancellationToken);
        }
    }

    // WriteReplyAsync 按 op 回预置 protojson 回执：Login/Resume 下发凭据，其余空回执。
    private async Task WriteReplyAsync(IPEndPoint remote, uint sequence, string op, CancellationToken cancellationToken)
    {
        var json = op == Session.LoginOperation || op == Session.ResumeOperation
            ? "{\"playerId\":\"42\",\"token\":\"tok-42\"}"
            : "{}";
        var data = System.Text.Encoding.UTF8.GetBytes(json);
        var envelope = new byte[5 + data.Length];
        WriteUInt32(envelope, 1, (uint)data.Length);
        Array.Copy(data, 0, envelope, 5, data.Length);
        var header = new Header
        {
            Magic = FrameConst.Magic,
            Version = FrameConst.Version,
            Type = MsgType.Response,
            Seq = sequence,
        };
        var datagram = FrameIO.EncodeMessage(header, envelope, FrameConst.MaxBodySize);
        await _socket.SendToAsync(
            new ArraySegment<byte>(datagram), SocketFlags.None, remote, cancellationToken);
    }

    private static void WriteUInt32(byte[] bytes, int offset, uint value)
    {
        bytes[offset] = (byte)(value >> 24);
        bytes[offset + 1] = (byte)(value >> 16);
        bytes[offset + 2] = (byte)(value >> 8);
        bytes[offset + 3] = (byte)value;
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

// SessionKcpServer 是会话测试服务端（KCP）：会话激活模式同 KcpEchoServer（首包按
// conv 建会话），帧组装为「头消息(16B) + body 消息」；按 op 回预置回执并记录
// "op|session"（KCP 无连接通道的帧会话槽断言用）。
internal sealed class SessionKcpServer : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly Dictionary<EndPoint, SessionStub> _sessions = new();
    private readonly object _gate = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loopTask;
    private readonly ConcurrentQueue<string> _seen = new();

    public SessionKcpServer()
    {
        var options = new KcpConversationOptions
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
        _socket = new Socket(SocketType.Dgram, ProtocolType.Udp);
        _socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        Port = ((IPEndPoint)_socket.LocalEndPoint!).Port;
        _loopTask = Task.Run(() => ReceiveLoopAsync(options, _cts.Token));
    }

    public int Port { get; }

    // Seen 返回服务端收到的全部 "op|session" 记录（按到达顺序）。
    public string[] Seen => _seen.ToArray();

    // Saw 判断服务端是否收到过指定 (op, session) 的请求。
    public bool Saw(string op, string session)
    {
        return _seen.Contains(op + "|" + session);
    }

    private async Task ReceiveLoopAsync(KcpConversationOptions options, CancellationToken cancellationToken)
    {
        var buffer = new byte[65536];
        var any = new IPEndPoint(IPAddress.Any, 0);
        while (!cancellationToken.IsCancellationRequested)
        {
            SocketReceiveFromResult received;
            try
            {
                received = await _socket.ReceiveFromAsync(buffer, SocketFlags.None, any, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            var packet = new byte[received.ReceivedBytes];
            Array.Copy(buffer, packet, received.ReceivedBytes);
            var remote = received.RemoteEndPoint;
            var session = GetOrCreateSession(remote, packet, options);
            if (session != null)
            {
                _ = Task.Run(() => session.InputAsync(packet));
            }
        }
    }

    private SessionStub? GetOrCreateSession(EndPoint remote, byte[] firstPacket, KcpConversationOptions options)
    {
        lock (_gate)
        {
            if (_sessions.TryGetValue(remote, out var existing))
            {
                return existing;
            }
            int conversationId = 1;
            if (firstPacket.Length >= 4)
            {
                conversationId = firstPacket[0] | (firstPacket[1] << 8) | (firstPacket[2] << 16) | (firstPacket[3] << 24);
            }
            var session = new SessionStub(this, _socket, remote, conversationId, options);
            _sessions[remote] = session;
            return session;
        }
    }

    private void OnSessionClosed(EndPoint remote)
    {
        lock (_gate)
        {
            _sessions.Remove(remote);
        }
    }

    // SessionStub 封装单个客户端的 KCP 会话：帧组装 + 回执（对齐 KcpEchoServer.EchoSession）。
    private sealed class SessionStub : IKcpTransport
    {
        private readonly SessionKcpServer _server;
        private readonly Socket _socket;
        private readonly EndPoint _remote;
        private readonly KcpConversation _conversation;
        private readonly CancellationTokenSource _cts = new();

        public SessionStub(
            SessionKcpServer server, Socket socket, EndPoint remote, int conversationId, KcpConversationOptions options)
        {
            _server = server;
            _socket = socket;
            _remote = remote;
            _conversation = new KcpConversation(this, conversationId, options);
            _ = Task.Run(ReceiveLoopAsync);
        }

        public ValueTask SendPacketAsync(Memory<byte> packet, CancellationToken cancellationToken)
        {
            return new ValueTask(_socket.SendToAsync(packet, SocketFlags.None, _remote, cancellationToken).AsTask());
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
                    if (result.TransportClosed)
                    {
                        break;
                    }
                    if (result.BytesReceived == 0)
                    {
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
            }
            finally
            {
                _conversation.Dispose();
                _cts.Dispose();
                _server.OnSessionClosed(_remote);
            }
        }

        // HandleMessageAsync 组装完整帧（头消息 + body 消息），记录 (op, session) 并按 op 回执。
        private async Task HandleMessageAsync(byte[] first)
        {
            if (first.Length != FrameConst.HeaderSize)
            {
                return; // 非帧首消息：坏消息丢弃。
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

            byte[] body = Array.Empty<byte>();
            if (header.Length > 0)
            {
                var filled = 0;
                var combined = new List<byte>();
                while (filled < (int)header.Length)
                {
                    var result = await _conversation.WaitToReceiveAsync(_cts.Token);
                    if (result.TransportClosed)
                    {
                        return;
                    }
                    if (result.BytesReceived == 0)
                    {
                        continue;
                    }
                    var chunk = new byte[result.BytesReceived];
                    if (!_conversation.TryReceive(chunk, out result))
                    {
                        return;
                    }
                    combined.AddRange(chunk);
                    filled += chunk.Length;
                }
                body = combined.ToArray();
            }

            string op;
            string session;
            try
            {
                (op, session, _) = Body.ParseRequestBodyWithSession(body, header.Flags);
            }
            catch (ProtocolException)
            {
                return;
            }
            _server._seen.Enqueue(op + "|" + session);
            await WriteReplyAsync(header, op);
        }

        // WriteReplyAsync 按 op 回预置 protojson 回执（头消息 + body 消息两次发送）。
        private async Task WriteReplyAsync(Header requestHeader, string op)
        {
            var json = op == Session.LoginOperation || op == Session.ResumeOperation
                ? "{\"playerId\":\"42\",\"token\":\"tok-42\"}"
                : "{}";
            var data = System.Text.Encoding.UTF8.GetBytes(json);
            var envelope = new byte[5 + data.Length];
            envelope[0] = 0;
            WriteUInt32(envelope, 1, (uint)data.Length);
            Array.Copy(data, 0, envelope, 5, data.Length);
            var replyHeader = new Header
            {
                Magic = requestHeader.Magic,
                Version = FrameConst.Version,
                Type = MsgType.Response,
                Seq = requestHeader.Seq,
                Length = (uint)envelope.Length,
            };
            await _conversation.SendAsync(replyHeader.Encode(), _cts.Token);
            await _conversation.SendAsync(envelope, _cts.Token);
        }

        private static void WriteUInt32(byte[] bytes, int offset, uint value)
        {
            bytes[offset] = (byte)(value >> 24);
            bytes[offset + 1] = (byte)(value >> 16);
            bytes[offset + 2] = (byte)(value >> 8);
            bytes[offset + 3] = (byte)value;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _socket.Dispose();
        lock (_gate)
        {
            _sessions.Clear();
        }
        try
        {
            await _loopTask.WaitAsync(TimeSpan.FromSeconds(1));
        }
        catch
        {
            // 忽略收尾异常。
        }
        _cts.Dispose();
    }
}
