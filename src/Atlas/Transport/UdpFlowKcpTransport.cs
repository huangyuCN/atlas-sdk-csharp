using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KcpSharp;

namespace Atlas.Transport;

// UdpFlowKcpTransport 是直连接入层 KCP 面的会话承载：KcpSharp 自带的 socket 承载直接收发
// 裸 KCP 报文，而接入层在 hello 换号后要求**每个数据报**都带「flow-id(8) || 载荷」前缀，
// 故自实现承载：
//   发：KCP 报文加前缀后按单数据报发出；
//   收：剥离前缀（失配/过短丢弃，与接入层同口径）后喂给 KCP 会话。
// 一个实例对应一条流（socket + 远端 + 流号），由 KcpTransport.ConnectDirectAsync 装配。
internal sealed class UdpFlowKcpTransport : IKcpTransport, IDisposable
{
    // MaxDatagramSize 是单数据报上限（含 flow-id 前缀；与 UDP 面读缓冲一致）。
    private const int MaxDatagramSize = 64 * 1024;

    private readonly Socket _socket;
    private readonly IPEndPoint _remote;
    private readonly ulong _flowId;
    // 读缓冲复用：收包泵单线程，每个数据报复用同一缓冲（避免高频 KCP 报文下每包一次分配）。
    private readonly byte[] _receiveBuffer = new byte[MaxDatagramSize];
    private readonly CancellationTokenSource _stop = new();
    private KcpConversation? _conversation;
    private Task? _pump;

    public UdpFlowKcpTransport(Socket socket, IPEndPoint remote, ulong flowId)
    {
        _socket = socket;
        _remote = remote;
        _flowId = flowId;
    }

    // Start 构造 KCP 会话并启动收包泵。
    // 注：InputPakcetAsync 是 KcpSharp 0.8.8 的公开方法名（上游拼写如此），非本仓笔误。
    public KcpConversation Start(int conversationId, KcpConversationOptions options)
    {
        _conversation = new KcpConversation(this, conversationId, options);
        _pump = Task.Run(PumpAsync);
        return _conversation;
    }

    // SendPacketAsync 把 KCP 报文封装为「flow-id(8) || 报文」的数据报发出。
    public ValueTask SendPacketAsync(Memory<byte> packet, CancellationToken cancellationToken)
    {
        var datagram = EdgeFlow.WrapFlowId(_flowId, packet.ToArray());
        // connected socket：Send 即发往 _remote（netstandard2.1 无取消令牌重载）。
        return new ValueTask(_socket.SendAsync(new ArraySegment<byte>(datagram), SocketFlags.None));
    }

    // PumpAsync 收包泵：读数据报 → 校验并剥离前缀 → 喂给 KCP 会话；失配包丢弃（不中断）。
    private async Task PumpAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            int received;
            try
            {
                // connected socket：Receive 只收对端数据报；关闭时由 socket 释放中断
                //（netstandard2.1 的 ReceiveAsync 无取消令牌重载）。
                received = await _socket.ReceiveAsync(
                    new ArraySegment<byte>(_receiveBuffer), SocketFlags.None).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (SocketException)
            {
                return;
            }

            var conversation = _conversation;
            if (conversation == null || received < EdgeFlow.FlowIdLen)
            {
                continue;
            }
            if (EdgeFlow.DecodeFlowId(_receiveBuffer) != _flowId)
            {
                continue; // 前缀失配：丢弃（接入层同样丢弃失配包）。
            }
            var payload = new byte[received - EdgeFlow.FlowIdLen];
            if (payload.Length > 0)
            {
                Buffer.BlockCopy(_receiveBuffer, EdgeFlow.FlowIdLen, payload, 0, payload.Length);
            }
            try
            {
                await conversation.InputPakcetAsync(payload, _stop.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                return; // 会话已释放/输入失败：结束收包泵（关闭路径）。
            }
        }
    }

    // Dispose 停止收包泵并释放会话（幂等：KcpTransport.CloseAsync 也会释放同一会话）。
    public void Dispose()
    {
        try
        {
            _stop.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // 已释放：忽略。
        }
        try
        {
            _conversation?.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // 已释放：忽略。
        }
        try
        {
            _stop.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // 已释放：忽略。
        }
    }
}
