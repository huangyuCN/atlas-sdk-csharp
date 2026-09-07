using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Transport;

namespace Atlas.Client;

// ChannelConfig 是 Client 编排的单个通道描述：角色 + 拨号工厂 + 配置
//（对标 Go client.ChannelConfig）。单通道 Client 用业务通道；dual 形态
//（M4-5）用两个 ChannelConfig（业务 + 战斗）。
public sealed class ChannelConfig
{
    // Kind 是通道角色（零值 Business——单通道形态即业务通道）。
    public ChannelKind Kind { get; set; } = ChannelKind.Business;

    // Dial 是本通道的拨号工厂（TCP/WS/KCP/UDP 传输的 ConnectAsync）。
    public Func<CancellationToken, Task<ITransport>> Dial { get; set; } = null!;

    // Options 是本通道配置（心跳/重连/排队/serializer 等）。
    public ChannelOptions Options { get; set; } = new();

    // ReconnectHook 是本通道重连成功后的会话钩子（对标 Go WithOnReconnected）：
    // 业务通道在此重登（更新 token 并回调业务）；战斗通道在此执行重绑（Join 语义
    // 替身，如战斗通道传输心跳往返）。业务通道钩子成功后，若本 Client 含战斗通道
    // 且其已 Connected，SDK 自动链式触发战斗钩子（对齐 Go DialDual 链式重绑——
    // 规范 §5.2）；战斗未就绪则跳过本轮（战斗自身重连成功后执行自己的钩子）。
    public Func<Task>? ReconnectHook { get; set; }

    public ChannelConfig()
    {
    }

    public ChannelConfig(ChannelKind kind, Func<CancellationToken, Task<ITransport>> dial)
    {
        Kind = kind;
        Dial = dial;
    }
}

// AtlasClient 是多通道编排器（门面，对标 Go client.Client）：管理一条或多条
// Channel，Invoke/On 默认走业务通道；dual 形态用 Channel(kind) 取通道视图做
// 独立请求/订阅/状态观测。通道生命周期归 Client：Close 优雅关闭全部通道
//（停心跳/重连、断开连接、取消全部 in-flight 与排队请求——幂等、无死锁）。
// 聚合状态向下降级（任一通道取最劣 Connected < Reconnecting < Disconnected，
// 规范 §5.1）；细粒度用 Channel(kind).State()。
public sealed class AtlasClient : IAsyncDisposable
{
    private readonly Dictionary<ChannelKind, Channel> _channels = new();
    private readonly Channel _business;

    // Client 构造：接收一个或多个通道配置。业务通道必须存在（单通道形态即业务）。
    // 传入配置各自拨号需调用方先 ConnectAsync——门面不自动拨号（各通道连接时机
    // 由调用方编排，对标 Go dialChannels 建好后统一返回已连接 Client）。
    public AtlasClient(params ChannelConfig[] configs)
    {
        if (configs == null || configs.Length == 0)
        {
            throw new ArgumentException("至少需要一个通道配置", nameof(configs));
        }
        foreach (var config in configs)
        {
            if (config.Dial == null)
            {
                throw new ArgumentException("通道配置缺少拨号工厂", nameof(configs));
            }
            if (_channels.ContainsKey(config.Kind))
            {
                throw new ArgumentException($"重复的通道角色 {config.Kind}", nameof(configs));
            }
            var channel = new Channel(config.Kind, config.Dial, config.Options)
            {
                OnRelogin = config.ReconnectHook,
            };
            _channels[config.Kind] = channel;
        }
        if (!_channels.TryGetValue(ChannelKind.Business, out _business!))
        {
            throw new ArgumentException("业务通道必须存在（单通道形态即业务）", nameof(configs));
        }
        // 链式重绑（对齐 Go DialDual）：业务通道钩子成功后自动触发战斗通道钩子。
        // 仅当业务与战斗通道都配置了 ReconnectHook 时链式——业务钩子是触发点
        //（战斗自身重连成功后也会执行自己的钩子，二者不互斥）。
        // 已知竞态窗口（与 Go 同源）：双通道同时断线、战斗先恢复 Connected 时，
        // 业务链式触发的战斗钩子与战斗自身重连触发的钩子可能在小窗口双跑——
        // Join 重绑幂等性依赖服务端容忍重复 Join（对齐 Go v0.4 同语义，非新增风险）。
        LinkBattleRebindHook();
    }

    // LinkBattleRebindHook 做 dual 链式包装：若本 Client 含业务通道钩子与战斗通道
    // 钩子，把战斗钩子链入业务钩子之后（业务重登成功 → 战斗重绑；战斗未 Connected
    // 则跳过本轮——对齐 Go DialDual 实现，业务通道重连不因战斗未就绪而失败）。
    private void LinkBattleRebindHook()
    {
        if (!_channels.TryGetValue(ChannelKind.Battle, out var battle))
        {
            return;
        }
        if (_business.OnRelogin == null || battle.OnRelogin == null)
        {
            return;
        }
        var businessHook = _business.OnRelogin;
        var battleHook = battle.OnRelogin;
        _business.OnRelogin = async () =>
        {
            await businessHook();
            // 战斗通道可能还在自身重连中（双通道同时断线、战斗恢复较慢）：
            // 跳过本次重绑——战斗自身重连成功后会执行自己的钩子，避免业务
            // 钩子因「战斗未就绪」失败而拖累业务通道反复重连（评审 v0.4 语义）。
            if (battle.State != ClientState.Connected)
            {
                return;
            }
            await battleHook();
        };
    }

    // ConnectAsync 连接全部通道（顺序连接；任一失败即整体失败并回滚已建通道——
    // 对齐 Go DialDual「任一通道拨号失败即整体失败，已建通道被回滚关闭」）。
    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        var connected = new List<Channel>();
        try
        {
            foreach (var channel in _channels.Values)
            {
                await channel.ConnectAsync(cancellationToken);
                connected.Add(channel);
            }
        }
        catch
        {
            foreach (var channel in connected)
            {
                await channel.CloseAsync();
            }
            throw;
        }
    }

    // InvokeRawAsync 发送原始 payload 并等待响应（默认业务通道）。
    public Task<byte[]> InvokeRawAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        return _business.InvokeRawAsync(operation, payload, cancellationToken);
    }

    // On 订阅 Notify 帧（默认业务通道），返回退订句柄。
    public NotifySubscription On(string op, NotifyHandler handler)
    {
        return _business.On(op, handler);
    }

    // State 聚合全部通道状态：任一通道非 Connected 即向下降级（规范 §5.1）；
    // 细粒度状态用 Channel(kind).State()。
    public ClientState State
    {
        get
        {
            var worst = ClientState.Connected;
            foreach (var channel in _channels.Values)
            {
                var state = channel.State;
                if (StateSeverity(state) > StateSeverity(worst))
                {
                    worst = state;
                }
            }
            return worst;
        }
    }

    // Channel 返回通道视图（独立 Invoke/On/State；生命周期归 Client，不单独关闭）。
    // 未知 kind 返回空视图（方法 nil 安全）。
    public ChannelView Channel(ChannelKind kind)
    {
        _channels.TryGetValue(kind, out var channel);
        return new ChannelView(channel);
    }

    // CloseAsync 优雅关闭全部通道（停心跳/重连、断开连接、取消全部 in-flight 与
    // 排队请求）。幂等；任何时刻调用都不会死锁（对齐 Go Client.Close）。
    public async Task CloseAsync()
    {
        foreach (var channel in _channels.Values)
        {
            await channel.CloseAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var channel in _channels.Values)
        {
            await channel.DisposeAsync();
        }
    }

    // StateSeverity 状态劣化度：Connected(0) < Reconnecting(1) < Disconnected(2)
    //（对齐 Go stateSeverity——聚合取最劣）。
    private static int StateSeverity(ClientState state)
    {
        switch (state)
        {
            case ClientState.Connected:
                return 0;
            case ClientState.Reconnecting:
                return 1;
            default:
                return 2;
        }
    }
}
