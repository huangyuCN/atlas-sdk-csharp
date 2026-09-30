using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Transport;

// EdgeHandshake 是接入层数据报面（UDP/KCP）的公共握手：同一 socket 上**先发 hello 段**，
// 等接入层回 8 字节 flow-id。两类失败必须分开（否则「票无效」会被当成「网络抖动」无限重试）：
//   - 握手期无回应（超时）= 接入层拒绝（L4 无应用层回执，只有断开 + 指标）→ ProtocolException；
//   - 连接被拒/不可达（SocketException：ConnectionRefused 等）→ NetworkException。
// KCP 面的顺序约束同样由本类保证：hello 段先于任何 KCP 报文发出。
public static class EdgeHandshake
{
    // RejectedMessage 是接入层拒绝 hello 的稳定报错前缀（Atlas.Battle.DirectErrors 引用同一字面量）。
    public const string RejectedMessage = "接入层拒绝 hello（未回应 flow-id）";

    // TicketQueryKey 是 WS 升级请求 query 里承载票据的参数名（contrib/edge.TicketQueryKey）。
    public const string TicketQueryKey = "ticket";

    // TicketHeaderKey 是 WS 升级请求里承载票据的头名（contrib/edge.TicketHeaderKey）。
    public const string TicketHeaderKey = "X-Atlas-Ticket";

    // ExchangeFlowIdAsync 发送 hello 段并等待 flow-id；返回的流号是后续数据报的前缀。
    // socket 必须已连到接入层（connected UDP）：收发都只针对该对端。
    public static async Task<ulong> ExchangeFlowIdAsync(
        Socket socket,
        byte[] hello,
        int timeoutMs,
        CancellationToken cancellationToken)
    {
        if (socket == null)
        {
            throw new ArgumentNullException(nameof(socket));
        }
        if (hello == null)
        {
            throw new ArgumentNullException(nameof(hello));
        }
        // 首包 = hello 段：接入层按首个数据报（或流式面首个定长段）解析票据。
        await socket.SendAsync(new ArraySegment<byte>(hello), SocketFlags.None).ConfigureAwait(false);
        return await ReceiveFlowIdAsync(socket, timeoutMs, cancellationToken).ConfigureAwait(false);
    }

    // ReceiveFlowIdAsync 等 8 字节 flow-id；其它长度的数据报按噪声丢弃后继续等。
    private static async Task<ulong> ReceiveFlowIdAsync(Socket socket, int timeoutMs, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(timeoutMs);
        var buffer = new byte[EdgeFlow.FlowIdLen];
        while (true)
        {
            int received;
            try
            {
                received = await socket.ReceiveAsync(
                    new ArraySegment<byte>(buffer), SocketFlags.None, timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new ProtocolException($"{RejectedMessage}（{timeoutMs}ms 无回应）");
            }
            catch (SocketException exception)
            {
                throw new NetworkException("接入层不可达", exception);
            }
            if (received == EdgeFlow.FlowIdLen)
            {
                return EdgeFlow.DecodeFlowId(buffer);
            }
        }
    }
}
