using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Frame;
using KcpSharp;

namespace Atlas.Transport;

// KcpTransport 基于 KcpSharp 的 KCP 帧传输（KCP 战斗通道）。
// 互通基线（与服务端 atlas transport/kcp 默认会话配置及 Go SDK 对齐）：
//   明文（无 block）、无 FEC、NoDelay=false(常规)、interval=40ms、FastResend=0、
//   DisableCongestionControl=false、窗口 128、MTU 1400——见
//   atlas 主仓 transport/kcp/session_config.go defaultKcpSessionConfig。
// 消息语义（真机互通实证，见 task-M3-3 探针）：kcp-go 默认消息模式——一次
// SendAsync = 对端一次 Read 完整收回。帧按「头消息(16B) + body 消息」两次发送
// （对齐 Go SDK frame.Write 对 kcp 的两次 Write）；读侧先收头消息解析 bodyLen，
// 再收 body 消息（bodyLen=0 时服务端只发头消息不发 body 消息）。切勿开
// KcpSharp StreamMode——kcp-go 侧为消息模式，开流模式破坏互通。
public sealed class KcpTransport : ITransport
{
    // 写超时兜底（对齐 Go client/transport_kcp.go kcpWriteTimeout）：KCP 无连接
    // 关闭通知，死链（对端消失）后发送窗口堆满 SendAsync 会永久等待并持有上层
    // 写锁——心跳死链检测被阻塞绕过，重连卡死。每次写设 10s 兜底超时（对齐 Go）。
    // internal 可变以便测试注入短值验证超时路径（默认 10000 对齐 Go）。
    internal static int KcpWriteTimeoutMs = 10000;

    private readonly KcpConversation _conversation;
    private readonly Socket _socket;
    private readonly IKcpTransport<KcpConversation>? _transport;

    private KcpTransport(Socket socket, IKcpTransport<KcpConversation> transport, KcpConversation conversation)
    {
        _socket = socket;
        _transport = transport;
        _conversation = conversation;
    }

    // ConnectAsync 建立 KCP 会话并返回帧传输。host/port 为服务端 UDP 地址。
    public static async Task<ITransport> ConnectAsync(string host, int port, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(host))
        {
            throw new ArgumentException("host 不能为空", nameof(host));
        }

        var address = await ResolveAddressAsync(host).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        var socket = CreateBoundSocket();
        try
        {
            var (transport, conversation) = CreateConversation(socket, address, port);
            return new KcpTransport(socket, transport, conversation);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    // ResolveAddressAsync 解析服务端地址（DNS 或字面量；KcpSharp 需 EndPoint）。
    private static async Task<IPAddress> ResolveAddressAsync(string host)
    {
        if (IPAddress.TryParse(host, out var literal))
        {
            return literal;
        }
        var resolved = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
        if (resolved.Length == 0)
        {
            throw new ArgumentException($"无法解析主机 {host}", nameof(host));
        }
        return resolved[0];
    }

    // CreateBoundSocket 创建并绑定本地 UDP socket（不指定则系统分配；服务端为 IPv4）。
    private static Socket CreateBoundSocket()
    {
        var socket = new Socket(SocketType.Dgram, ProtocolType.Udp);
        socket.Bind(new IPEndPoint(IPAddress.Any, 0));
        return socket;
    }

    // CreateConversation 构造 KCP 会话（参数对齐服务端 kcp-go 默认基线）。
    private static (IKcpTransport<KcpConversation> Transport, KcpConversation Conversation)
        CreateConversation(Socket socket, IPAddress address, int port)
    {
        var options = new KcpConversationOptions
        {
            NoDelay = false,              // kcp-go nodelay=0
            UpdateInterval = 40,          // kcp-go interval=40（毫秒）
            FastResend = 0,               // kcp-go resend=0
            DisableCongestionControl = false, // kcp-go nc=0
            SendWindow = 128,             // kcp-go sndWnd
            ReceiveWindow = 128,          // kcp-go rcvWnd
            RemoteReceiveWindow = 128,    // 对端窗口（kcp-go 默认 128）
            Mtu = 1400,                   // kcp-go mtu
            StreamMode = false,           // 消息模式（kcp-go 默认，互通关键）
        };

        // 随机 conv（0x10000000..0x7FFFFFFF），与服务端按 conv 匹配对话
        //（kcp-go 客户端 DialWithOptions 亦用随机 conv）。
        int conv = unchecked((int)(uint)new Random().Next(0x10000000, 0x7FFFFFFF));
        var transport = KcpSocketTransport.CreateConversation(
            socket, new IPEndPoint(address, port), conv, options);
        // Connection 在 Start 后才可用（KcpSocketTransport 状态机要求）。
        transport.Start();
        return (transport, transport.Connection);
    }

    public async ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(int maxBodySize, CancellationToken cancellationToken)
    {
        maxBodySize = Header.NormalizeMaxBodySize(maxBodySize);
        // 收头消息（16B）。
        var headerMessage = await ReceiveMessageAsync(FrameConst.HeaderSize, cancellationToken).ConfigureAwait(false);
        if (headerMessage.Length != FrameConst.HeaderSize)
        {
            throw new ProtocolException($"KCP 帧头消息长度 {headerMessage.Length} != 16");
        }
        var header = Header.Decode(headerMessage); // 校验失败抛 ProtocolException
        if (header.Length > (uint)maxBodySize)
        {
            throw new ProtocolException($"body 过长: {header.Length} > {maxBodySize}");
        }

        // bodyLen>0：收 body 消息（可能分多块，循环拼接至读满 bodyLen）。
        if (header.Length == 0)
        {
            return (header, Array.Empty<byte>());
        }
        var body = new byte[header.Length];
        int filled = 0;
        while (filled < body.Length)
        {
            var chunk = await ReceiveMessageAsync(body.Length - filled, cancellationToken).ConfigureAwait(false);
            if (chunk.Length == 0)
            {
                throw new EndOfStreamException("KCP 连接关闭：body 截断");
            }
            Array.Copy(chunk, 0, body, filled, chunk.Length);
            filled += chunk.Length;
        }
        return (header, body);
    }

    // ReceiveMessageAsync 收一条 KCP 消息（消息模式；返回实际字节）。
    private async ValueTask<byte[]> ReceiveMessageAsync(int maxSize, CancellationToken cancellationToken)
    {
        KcpConversationReceiveResult result;
        try
        {
            result = await _conversation.WaitToReceiveAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        if (result.TransportClosed)
        {
            throw new EndOfStreamException("KCP 传输已关闭");
        }
        if (result.BytesReceived > maxSize)
        {
            // 单条消息超预期：按协议违规处理（读侧丢弃会失步，报错由上层断连）。
            throw new ProtocolException($"KCP 消息过大: {result.BytesReceived} > {maxSize}");
        }
        if (result.BytesReceived == 0)
        {
            return Array.Empty<byte>();
        }
        var buf = new byte[result.BytesReceived];
        if (!_conversation.TryReceive(buf, out result))
        {
            throw new ProtocolException("KCP TryReceive 失败");
        }
        if (result.BytesReceived != buf.Length)
        {
            // TryReceive 实际填充少于 WaitToReceive 声称（罕见）；按实收截断。
            var actual = new byte[result.BytesReceived];
            Array.Copy(buf, actual, result.BytesReceived);
            return actual;
        }
        return buf;
    }

    // WriteFrameAsync 写帧：头消息 + body 消息两次发送。每次写设 10s 兜底超时
    //（KcpWriteTimeoutMs，对齐 Go kcpWriteTimeout）——死链窗口满时 SendAsync
    // 无限等待会持有上层写锁，必须超时返回让上层判死链重连。
    public async Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken cancellationToken)
    {
        if (body == null)
        {
            throw new ArgumentNullException(nameof(body));
        }
        maxBodySize = Header.NormalizeMaxBodySize(maxBodySize);

        // 帧头强制填充默认值并校验（对齐 Go frame.Write：magic/version 零值按默认
        // 补齐、type/seq/version/长度经 Header.Check——非法头在出站即拦截）。
        var h = header;
        if (h.Magic == 0)
        {
            h.Magic = FrameConst.Magic;
        }
        if (h.Version == 0)
        {
            h.Version = FrameConst.Version;
        }
        h.Length = (uint)body.Length;
        Header.Check(h, maxBodySize);

        await SendHeaderAndBodyAsync(h, body, cancellationToken).ConfigureAwait(false);
    }

    // SendHeaderAndBodyAsync 头消息 + body 消息两次发送（消息模式互通关键），每次
    // 写设 10s 兜底超时——死链窗口满时 SendAsync 无限等待会持有上层写锁，必须超时
    // 返回让上层判死链重连（对齐 Go kcpWriteTimeout）。
    private async Task SendHeaderAndBodyAsync(Header header, byte[] body, CancellationToken cancellationToken)
    {
        using var writeTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        writeTimeout.CancelAfter(KcpWriteTimeoutMs);
        var token = writeTimeout.Token;
        try
        {
            // SendAsync 返回 true = 成功入队；false = 传输已关闭（KcpSharp 语义）。
            bool ok = await _conversation.SendAsync(header.Encode(), token).ConfigureAwait(false);
            if (!ok)
            {
                throw new EndOfStreamException("KCP 传输已关闭：发帧头失败");
            }
            if (body.Length > 0)
            {
                ok = await _conversation.SendAsync(body, token).ConfigureAwait(false);
                if (!ok)
                {
                    throw new EndOfStreamException("KCP 传输已关闭：发 body 失败");
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw; // 调用方主动取消：原样透传。
        }
        catch (OperationCanceledException oce)
        {
            // 写超时兜底（死链）：转网络错误，上层按死链处理（心跳失败计数）。
            throw new NetworkException($"KCP 写帧超时（{KcpWriteTimeoutMs}ms），判定死链", oce);
        }
    }

    public Task CloseAsync()
    {
        try
        {
            _conversation.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // 已释放则忽略。
        }
        // 释放接收泵（KcpSocketTransport），避免其计时器/队列残留到进程退出
        //（评审修复：此前仅 Dispose conversation 与 socket，接收泵未收尾）。
        try
        {
            _transport?.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // 已释放则忽略。
        }
        _socket.Dispose();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        CloseAsync();
        return default;
    }
}
