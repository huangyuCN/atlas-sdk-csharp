using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;

namespace Atlas.Transport;

// UdpTransport 基于面向连接 UDP socket（Socket 连接式）的数据报帧传输：
// 一报一帧——发送 = 单数据报（16B 帧头 + body），接收 = 单数据报解析为帧。
// 坏数据报（垃圾字节/短于帧头/非法头/长度不一致）静默丢弃并继续读，不视为连接
// 故障（对齐 Go transport_udp.go：服务端 ErrBadFrame 软跳过语义，防垃圾/放大攻击）。
// UDP 无连接关闭通知：死链由传输心跳连续失败判定后驱动重拨（M4）；本类 Read 在
// socket 关闭/取消时返回异常供 readLoop 收尾。
//
// 直连接入层形态（ConnectDirectAsync）：首包 hello 换 flow-id，其后**双向**数据报都带
// 「flow-id(8 大端) || 载荷」前缀（发出的加、收到的剥；前缀失配的数据报丢弃）。
public sealed class UdpTransport : ITransport
{
    // MaxDatagramSize 是 UDP 单数据报上限（字节，含 16B 帧头与 flow-id 前缀）：与 Go
    // udpMaxDatagramSize 及服务端读缓冲对齐——超限数据报在接收侧被截断后丢弃，写侧提前拦截。
    public const int MaxDatagramSize = 64 * 1024;

    private readonly Socket _socket;
    // 读缓冲复用（对齐 Go transport_udp.go readBuf）：readLoop 单线程读，每次
    // ReadFrameAsync 复用同一 64KiB 缓冲——避免高频帧下每帧一次分配（评审 P3）。
    private readonly byte[] _readBuffer = new byte[MaxDatagramSize];
    // flow-id：仅直连接入层形态非空（0 是合法流号，故用可空类型表示「无前缀」）。
    private readonly ulong? _flowId;

    private UdpTransport(Socket socket, ulong? flowId)
    {
        _socket = socket;
        _flowId = flowId;
    }

    // ConnectAsync 建立面向连接的 UDP 传输（Socket.Connect 到远端；netstandard2.1
    // 同步 Connect 后即可收发——UDP Connect 不产生网络往返）。host 解析失败或
    // 端口非法抛 SocketException/ArgumentException。
    public static async Task<ITransport> ConnectAsync(string host, int port, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(host))
        {
            throw new ArgumentException("host 不能为空", nameof(host));
        }

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        try
        {
            await Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                socket.Connect(host, port);
            }, cancellationToken);
            return new UdpTransport(socket, null);
        }
        catch (OperationCanceledException)
        {
            socket.Dispose();
            throw;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    // ConnectDirectAsync 建立数据报面直连会话：同一 socket 上先发 hello 段（首包），
    // 等接入层回 8 字节 flow-id，其后双向数据报都带 flow-id 前缀。
    // 握手无回应 = 接入层拒绝（ProtocolException，不重试）；连接被拒/不可达 = 网络错误（可重试）。
    public static async Task<ITransport> ConnectDirectAsync(
        string host,
        int port,
        byte[] hello,
        int helloTimeoutMs,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(host))
        {
            throw new ArgumentException("host 不能为空", nameof(host));
        }

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        try
        {
            await Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                socket.Connect(host, port);
            }, cancellationToken);
            var flowId = await EdgeHandshake.ExchangeFlowIdAsync(
                socket, hello, helloTimeoutMs, cancellationToken).ConfigureAwait(false);
            return new UdpTransport(socket, flowId);
        }
        catch (OperationCanceledException)
        {
            socket.Dispose();
            throw;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    // ReadFrameAsync 读一个数据报并解析为帧；坏数据报静默丢弃并继续读。
    // 仅 socket 关闭/取消（连接收尾）上抛异常。直连形态先剥 flow-id 前缀：前缀失配
    //（旧流残留/串流数据报）同样静默丢弃——接入层也会丢弃失配包。
    public async ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(
        int maxBodySize, CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int received;
            try
            {
                received = await _socket.ReceiveAsync(
                    new ArraySegment<byte>(_readBuffer), SocketFlags.None, cancellationToken);
            }
            catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }

            var offset = 0;
            if (_flowId.HasValue)
            {
                if (received < EdgeFlow.FlowIdLen + FrameGen.HeaderSize
                    || EdgeFlow.DecodeFlowId(_readBuffer) != _flowId.Value)
                {
                    continue; // 前缀失配/过短：坏数据报，静默丢弃。
                }
                offset = EdgeFlow.FlowIdLen;
            }

            if (received - offset < FrameGen.HeaderSize)
            {
                continue; // 短于帧头：坏数据报，静默丢弃
            }

            var datagram = new byte[received - offset];
            Array.Copy(_readBuffer, offset, datagram, 0, datagram.Length);
            try
            {
                // 数据报应恰为完整帧（16B 头 + body）；任何解码失败 = 坏包丢弃。
                var (header, body) = FrameIO.DecodeMessage(datagram, maxBodySize);
                return (header, body);
            }
            catch (ProtocolException)
            {
                continue; // 坏数据报：静默丢弃
            }
        }
    }

    // WriteFrameAsync 编码整帧并以单个数据报发送（一报一帧；直连形态加 flow-id 前缀）。
    // 超出 64KiB（含帧头与前缀）写侧即报错——超限帧在服务端读缓冲被截断后丢弃，
    // 提前拦截让调用方立刻感知配置问题（对齐 Go WriteFrame 语义）。
    public Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken cancellationToken)
    {
        if (body == null)
        {
            throw new ArgumentNullException(nameof(body));
        }
        var prefix = _flowId.HasValue ? EdgeFlow.FlowIdLen : 0;
        if (body.Length + FrameGen.HeaderSize + prefix > MaxDatagramSize)
        {
            throw new ProtocolException(
                $"udp: 数据报过大: {body.Length} body + {FrameGen.HeaderSize} 头 + {prefix} 前缀 > {MaxDatagramSize}");
        }

        var datagram = FrameIO.EncodeMessage(header, body, maxBodySize);
        if (_flowId.HasValue)
        {
            datagram = EdgeFlow.WrapFlowId(_flowId.Value, datagram);
        }
        // netstandard2.1 的 Socket.SendAsync(ArraySegment, SocketFlags) 返回 Task<int>。
        return _socket.SendAsync(new ArraySegment<byte>(datagram), SocketFlags.None);
    }

    // CloseAsync 关闭底层 UDP socket（阻塞中的 Receive 随即返回异常，触发收尾）。
    public Task CloseAsync()
    {
        _socket.Dispose();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        return default;
    }
}
