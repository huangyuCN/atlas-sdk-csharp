using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Frame;

namespace Atlas.Tests.Battle;

// BattleEdgeWsServer 模拟「接入层 + battle 帧面」的 WS 面（最小 RFC6455 实现）：
// 读 HTTP 升级请求头 → 记录请求行 target 与 X-Atlas-Ticket（**逐字节**校验 query 票据）→
// 完成握手 → 一条 WS 消息 = 一个完整 ATLS 帧 → 按 op 回执；可主动推送 Notify。
// 拒绝模式下直接关闭 TCP（接入层「断开 + 无应用层回执」的最小语义）。
internal sealed class BattleEdgeWsServer : IAsyncDisposable
{
    private const string WebSocketGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _serveTask;
    private readonly List<EdgeRequest> _requests = new();
    private readonly List<string> _targets = new();
    private readonly List<string> _ticketHeaders = new();
    private readonly Dictionary<string, int> _opCounts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (int SuccessCount, int Code, string Reason, string Message)> _failures =
        new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private TcpClient? _client;
    private int _accepted;

    public BattleEdgeWsServer()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint!).Port;
        _serveTask = Task.Run(() => ServeAsync(_cts.Token));
    }

    public int Port { get; }

    // RejectUpgrade 为 true 时任何连接都被立即关闭（模拟接入层拒绝：票据无效/过期）。
    public bool RejectUpgrade { get; set; }

    // RejectAfterFirst 为 true 时首个连接正常服务、其后连接一律拒绝（重连被拒断言用）。
    public bool RejectAfterFirst { get; set; }

    // CloseAfterRequests > 0 时，服务端回完第 N 个请求后主动断开（模拟网络断开）。
    public int CloseAfterRequests { get; set; } = -1;

    public int AcceptedCount => Volatile.Read(ref _accepted);

    // FailAfter 使指定 op 在第 successCount 次成功之后回业务拒绝（票过期/无效等重连路径用例）。
    public void FailAfter(string op, int successCount, int code, string reason, string message)
    {
        lock (_gate)
        {
            _failures[op] = (successCount, code, reason, message);
        }
    }

    public string[] Targets
    {
        get
        {
            lock (_gate)
            {
                return _targets.ToArray();
            }
        }
    }

    public string[] TicketHeaders
    {
        get
        {
            lock (_gate)
            {
                return _ticketHeaders.ToArray();
            }
        }
    }

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

    // PushNotifyAsync 以一条 WS 二进制消息推送 Notify 帧。
    public async Task PushNotifyAsync(string op, byte[] payload, byte version = FrameGen.Version)
    {
        TcpClient? client;
        lock (_gate)
        {
            client = _client;
        }
        if (client == null)
        {
            throw new InvalidOperationException("尚无已升级的连接");
        }
        var body = Body.BuildRequestBody(op, payload);
        var header = new Header
        {
            Magic = FrameGen.Magic,
            Version = version,
            Type = MsgType.Notify,
            Seq = 1,
        };
        var frame = FrameIO.EncodeMessage(header, body, FrameGen.MaxBodySize);
        await client.GetStream().WriteAsync(EncodeWsFrame(frame), _cts.Token);
    }

    private async Task ServeAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(cancellationToken);
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
            var index = Interlocked.Increment(ref _accepted);
            _ = Task.Run(() => HandleAsync(client, index, cancellationToken), CancellationToken.None);
        }
    }

    private async Task HandleAsync(TcpClient client, int index, CancellationToken cancellationToken)
    {
        using (client)
        {
            var stream = client.GetStream();
            if (RejectUpgrade || (RejectAfterFirst && index > 1))
            {
                return; // 接入层拒绝：不发任何响应即断开。
            }
            var head = await ReadHeadAsync(stream, cancellationToken);
            if (head == null)
            {
                return;
            }
            var target = ParseRequestTarget(head);
            var headers = ParseHeaders(head);
            lock (_gate)
            {
                _targets.Add(target);
                _ticketHeaders.Add(headers.TryGetValue("x-atlas-ticket", out var ticket) ? ticket : "");
                _client = client;
            }
            if (!headers.TryGetValue("sec-websocket-key", out var key))
            {
                return;
            }
            await stream.WriteAsync(BuildUpgradeResponse(key), cancellationToken);
            await stream.FlushAsync(cancellationToken);
            await ServeFramesAsync(stream, cancellationToken);
        }
    }

    private async Task ServeFramesAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var served = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            var message = await ReadWsMessageAsync(stream, cancellationToken);
            if (message == null)
            {
                return; // 客户端关闭或连接断开。
            }
            Header header;
            byte[] body;
            try
            {
                (header, body) = FrameIO.DecodeMessage(message, FrameGen.MaxBodySize);
            }
            catch (Atlas.Errors.ProtocolException)
            {
                continue;
            }
            if (!EdgeFrameCodec.TryParse(body, header, out var request))
            {
                continue;
            }
            lock (_gate)
            {
                _requests.Add(request);
            }
            var responseBody = BuildReplyBody(request.Op);
            var responseHeader = new Header
            {
                Magic = FrameGen.Magic,
                Version = FrameGen.Version,
                Type = MsgType.Response,
                Seq = header.Seq,
            };
            var frame = FrameIO.EncodeMessage(responseHeader, responseBody, FrameGen.MaxBodySize);
            await stream.WriteAsync(EncodeWsFrame(frame), cancellationToken);
            served++;
            if (CloseAfterRequests > 0 && served >= CloseAfterRequests)
            {
                return; // 主动断开：模拟网络断开（客户端读循环退出 → 自动重连）。
            }
        }
    }

    // BuildReplyBody 按 op 计数构造回执：命中 FailAfter 且已超过成功次数即回业务拒绝。
    private byte[] BuildReplyBody(string op)
    {
        lock (_gate)
        {
            _opCounts.TryGetValue(op, out var count);
            count++;
            _opCounts[op] = count;
            if (_failures.TryGetValue(op, out var failure) && count > failure.SuccessCount)
            {
                return EdgeReply.BusinessError(failure.Code, failure.Reason, failure.Message);
            }
        }
        return EdgeReply.Success(Encoding.UTF8.GetBytes(BattleEdgeDefaults.ReplyJson(op)));
    }

    private static async Task<string?> ReadHeadAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];
        var head = new StringBuilder();
        while (head.Length < 16 * 1024)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(), cancellationToken);
            if (read == 0)
            {
                return null;
            }
            head.Append(Encoding.ASCII.GetString(buffer, 0, read));
            if (head.ToString().Contains("\r\n\r\n", StringComparison.Ordinal))
            {
                return head.ToString();
            }
        }
        return null;
    }

    private static string ParseRequestTarget(string head)
    {
        var lineEnd = head.IndexOf("\r\n", StringComparison.Ordinal);
        var line = lineEnd < 0 ? head : head[..lineEnd];
        var parts = line.Split(' ');
        return parts.Length >= 2 ? parts[1] : "";
    }

    private static Dictionary<string, string> ParseHeaders(string head)
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var lines = head.Split("\r\n");
        for (var i = 1; i < lines.Length; i++)
        {
            var separator = lines[i].IndexOf(':');
            if (separator <= 0)
            {
                continue;
            }
            headers[lines[i][..separator].Trim()] = lines[i][(separator + 1)..].Trim();
        }
        return headers;
    }

    private static byte[] BuildUpgradeResponse(string key)
    {
        using var sha1 = SHA1.Create();
        var accept = Convert.ToBase64String(sha1.ComputeHash(Encoding.ASCII.GetBytes(key + WebSocketGuid)));
        var response = "HTTP/1.1 101 Switching Protocols\r\n"
            + "Upgrade: websocket\r\n"
            + "Connection: Upgrade\r\n"
            + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
        return Encoding.ASCII.GetBytes(response);
    }

    // ReadWsMessageAsync 读一条完整 WS 二进制消息（客户端帧必带掩码）；关闭帧/断连返回 null。
    private static async Task<byte[]?> ReadWsMessageAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var payload = new List<byte>();
        while (true)
        {
            var header = await ReadExactAsync(stream, 2, cancellationToken);
            if (header == null)
            {
                return null;
            }
            var opcode = header[0] & 0x0f;
            var fin = (header[0] & 0x80) != 0;
            var masked = (header[1] & 0x80) != 0;
            if (opcode == 0x8)
            {
                return null;
            }
            var length = await ReadFrameLengthAsync(stream, header[1] & 0x7f, cancellationToken);
            if (length < 0)
            {
                return null;
            }
            var mask = masked ? await ReadExactAsync(stream, 4, cancellationToken) : null;
            if (masked && mask == null)
            {
                return null;
            }
            var data = await ReadExactAsync(stream, (int)length, cancellationToken);
            if (data == null)
            {
                return null;
            }
            AppendUnmasked(payload, data, mask);
            if (fin)
            {
                return payload.ToArray();
            }
        }
    }

    // ReadFrameLengthAsync 解析 WS 帧长度（7 位内联 / 16 位 / 64 位扩展）；失败返回 -1。
    private static async Task<long> ReadFrameLengthAsync(NetworkStream stream, int inline, CancellationToken cancellationToken)
    {
        if (inline == 126)
        {
            var extended = await ReadExactAsync(stream, 2, cancellationToken);
            return extended == null ? -1 : (extended[0] << 8) | extended[1];
        }
        if (inline == 127)
        {
            var extended = await ReadExactAsync(stream, 8, cancellationToken);
            if (extended == null)
            {
                return -1;
            }
            long length = 0;
            for (var i = 0; i < 8; i++)
            {
                length = (length << 8) | extended[i];
            }
            return length;
        }
        return inline;
    }

    // AppendUnmasked 把数据按掩码（客户端帧必带）解掩码后追加到消息缓冲。
    private static void AppendUnmasked(List<byte> payload, byte[] data, byte[]? mask)
    {
        for (var i = 0; i < data.Length; i++)
        {
            payload.Add(mask == null ? data[i] : (byte)(data[i] ^ mask[i % 4]));
        }
    }

    private static async Task<byte[]?> ReadExactAsync(NetworkStream stream, int count, CancellationToken cancellationToken)
    {
        if (count == 0)
        {
            return Array.Empty<byte>();
        }
        var buffer = new byte[count];
        var offset = 0;
        while (offset < count)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), cancellationToken);
            if (read == 0)
            {
                return null;
            }
            offset += read;
        }
        return buffer;
    }

    // EncodeWsFrame 编码服务端→客户端的二进制帧（服务端帧不加掩码）。
    private static byte[] EncodeWsFrame(byte[] payload)
    {
        var headerLength = payload.Length < 126 ? 2 : payload.Length <= 0xffff ? 4 : 10;
        var frame = new byte[headerLength + payload.Length];
        frame[0] = 0x82; // FIN + binary
        if (payload.Length < 126)
        {
            frame[1] = (byte)payload.Length;
        }
        else if (payload.Length <= 0xffff)
        {
            frame[1] = 126;
            frame[2] = (byte)(payload.Length >> 8);
            frame[3] = (byte)payload.Length;
        }
        else
        {
            frame[1] = 127;
            for (var i = 0; i < 8; i++)
            {
                frame[2 + i] = (byte)((long)payload.Length >> (8 * (7 - i)));
            }
        }
        Array.Copy(payload, 0, frame, headerLength, payload.Length);
        return frame;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        lock (_gate)
        {
            _client?.Close();
            _client = null;
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
}
