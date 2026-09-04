using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Client;

// ChannelView 是通道视图（对标 Go client.ChannelView）：从 Client.Channel(kind) 取得，
// 提供独立 Invoke/On/State/Kind 观测与请求——dual 形态下区分业务/战斗通道的细粒度
// 访问（每通道独立心跳/重连/排队）。生命周期归 Client：视图不单独 Connect/Close。
// 未知 kind 的视图方法 nil 安全（返回失败/空句柄/Disconnected）。
public sealed class ChannelView
{
    private readonly Channel? _channel;

    internal ChannelView(Channel? channel)
    {
        _channel = channel;
    }

    // InvokeRawAsync 在本通道上发送原始 payload 并等待响应（语义同 Client.InvokeRawAsync）。
    public Task<byte[]> InvokeRawAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        if (_channel == null)
        {
            throw new NetworkException("未知通道");
        }
        return _channel.InvokeRawAsync(operation, payload, cancellationToken);
    }

    // On 订阅本通道的 Notify 分发（重连后订阅保留自动接收；语义同 Client.On）。
    public NotifySubscription On(string op, NotifyHandler handler)
    {
        if (_channel == null)
        {
            return new NotifySubscription(() => { });
        }
        return _channel.On(op, handler);
    }

    // State 返回本通道独立状态（dual 双通道独立重连的细粒度观测，规范 §5.2）。
    public ClientState State => _channel?.State ?? ClientState.Disconnected;

    // Kind 返回本通道角色。
    public ChannelKind Kind => _channel?.Kind ?? ChannelKind.Business;
}
