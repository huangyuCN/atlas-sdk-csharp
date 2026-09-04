using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Transport;
using KcpSharp;

namespace Atlas.Tests.Transport;

// KcpEchoServer 是 KcpSharp 帧级 echo 服务端（测试对端）。
// 采用 KcpSharp 标准服务端模式（同 samples/KcpEcho）：socket 收包按来源
// endpoint 激活会话 → conversation.InputPakcetAsync；会话收到完整帧（头消息
// + body 消息）后原样回显。帧组装与 KcpTransport 相同：先收 16B 头消息解析
// bodyLen，再收 body 消息（bodyLen=0 只有头）。
public sealed class KcpEchoServer : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly Dictionary<EndPoint, EchoSession> _sessions = new();
    private readonly object _gate = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loopTask;
    private int _messageCount;

    public int Port { get; }
    public int MessageCount { get { lock (_gate) return _messageCount; } }

    public KcpEchoServer()
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
        _loopTask = Task.Run(() => ReceiveLoopAsync(options));
    }

    private async Task ReceiveLoopAsync(KcpConversationOptions options)
    {
        var buffer = new byte[65536];
        var any = new IPEndPoint(IPAddress.Any, 0);
        while (!_cts.IsCancellationRequested)
        {
            SocketReceiveFromResult recv;
            try
            {
                recv = await _socket.ReceiveFromAsync(buffer, SocketFlags.None, any, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            var packet = new byte[recv.ReceivedBytes];
            Array.Copy(buffer, packet, recv.ReceivedBytes);
            var remote = recv.RemoteEndPoint;
            var session = GetOrCreateSession(remote, packet, options);
            if (session != null)
            {
                _ = Task.Run(() => session.InputAsync(packet));
            }
        }
    }

    private EchoSession? GetOrCreateSession(EndPoint remote, byte[] firstPacket, KcpConversationOptions options)
    {
        lock (_gate)
        {
            if (_sessions.TryGetValue(remote, out var existing))
            {
                return existing;
            }
            // 从首包解析 conv（KCP 段头前 4 字节，KcpSharp 为小端——
            // BinaryPrimitives.ReadUInt32LittleEndian，见 KcpConversation.InputPakcetAsync）。
            // 服务端接受客户端任意 conv 建会话（对齐 kcp-go Accept 语义）。
            int conv = 1;
            if (firstPacket.Length >= 4)
            {
                conv = firstPacket[0] | (firstPacket[1] << 8) | (firstPacket[2] << 16) | (firstPacket[3] << 24);
            }
            var session = new EchoSession(this, _socket, remote, conv, options);
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

    // EchoSession 封装单个客户端的 conversation：输入包喂 conversation，帧级回显。
    private sealed class EchoSession : IKcpTransport
    {
        private readonly KcpEchoServer _server;
        private readonly Socket _socket;
        private readonly EndPoint _remote;
        private readonly KcpConversation _conversation;
        private readonly CancellationTokenSource _cts = new();

        public EchoSession(KcpEchoServer server, Socket socket, EndPoint remote, int conversationId, KcpConversationOptions options)
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
                    var msg = new byte[result.BytesReceived];
                    if (!_conversation.TryReceive(msg, out result))
                    {
                        break;
                    }
                    await HandleMessageAsync(msg);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception)
            {
                // 会话级异常：关闭该客户端会话（不拖垮服务端）。
            }
            finally
            {
                _conversation.Dispose();
                _cts.Dispose();
                _server.OnSessionClosed(_remote);
            }
        }

        // 帧组装：先头消息（16B）再 body 消息；回显同帧（头+body 两次发送）。
        private async Task HandleMessageAsync(byte[] first)
        {
            if (first.Length != FrameConst.HeaderSize)
            {
                // 非帧首消息：按坏消息丢弃（保持会话存活）。
                return;
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
                var result = await _conversation.WaitToReceiveAsync(_cts.Token);
                if (result.TransportClosed)
                {
                    return;
                }
                if (result.BytesReceived > 0)
                {
                    body = new byte[result.BytesReceived];
                    if (!_conversation.TryReceive(body, out result))
                    {
                        return;
                    }
                    if (result.BytesReceived < header.Length)
                    {
                        // 分块 body：继续收满。
                        var rest = await _conversation.WaitToReceiveAsync(_cts.Token);
                        if (rest.TransportClosed)
                        {
                            return;
                        }
                        var more = new byte[rest.BytesReceived];
                        if (_conversation.TryReceive(more, out rest))
                        {
                            var combined = new byte[body.Length + more.Length];
                            Array.Copy(body, combined, body.Length);
                            Array.Copy(more, 0, combined, body.Length, more.Length);
                            body = combined;
                        }
                    }
                }
            }

            lock (_server._gate)
            {
                _server._messageCount++;
            }

            // 回显：响应头（type 保持原样便于断言）+ body。
            var replyHeader = new Header
            {
                Magic = header.Magic,
                Version = header.Version,
                Type = MsgType.Response,
                Seq = header.Seq,
                Length = (uint)body.Length,
            };
            await _conversation.SendAsync(replyHeader.Encode(), _cts.Token);
            if (body.Length > 0)
            {
                await _conversation.SendAsync(body, _cts.Token);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _socket.Dispose();
        lock (_gate)
        {
            _sessions.Clear();
        }
        return default;
    }
}
