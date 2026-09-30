using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Frame;

namespace Atlas.Battle;

// BattleSession 是一次直连战斗会话：从成局通知取票据与接入层地址 → 直连接入层（hello 换票）
// → 跑战斗帧（JoinBattle / SendFrameInput / SyncFrames）→ 收帧广播与战斗结束推送。
//
// 复用业务通道的请求-响应匹配、推送分发、重连排队与退避（Channel），只额外做三件事：
//   1. 逐帧携带帧会话槽（ForceFrameSessionSlot + base64url 票）：经接入层透传后 battle 帧面
//      仍按帧槽验票，WS 长连接也不例外；
//   2. 重连钩子 = 重新入局 + 按 LastSeenFrame 补帧（SyncFrames(last_seen_frame)）；
//   3. 失败分类：接入层拒绝（ProtocolException）终止重连；票过期/无效（BusinessException
//      的 BATTLE_TICKET_EXPIRED / BATTLE_TICKET_INVALID）终止会话并回调 Failed，供上层重新匹配。
//
// 生命周期：Create（装配期选面，缺面即报错）→ ConnectAsync（hello）→ JoinBattleAsync →
// SyncFramesAsync → SendFrameInputAsync；断线自动重连（AutoReconnect）或显式 ReconnectAsync；
// 关闭后（CloseAsync/DisposeAsync）不可再连接。
public sealed partial class BattleSession : IAsyncDisposable
{
    private readonly DirectPlan _plan;
    private readonly BattleSessionOptions _options;
    private readonly EdgeTransport _face;
    // hello 段在装配期编码一次（重连时原样重发同一张票）。
    private readonly byte[] _hello;
    private Channel? _channel;
    private int _closed;

    private BattleSession(DirectPlan plan, BattleSessionOptions options, EdgeTransport face, string address)
    {
        _plan = plan;
        _options = options;
        _face = face;
        Address = address;
        _hello = EdgeWire.EncodeHello(plan.Ticket);
    }

    // Create 装配直连战斗会话：校验选项 + 按 endpoints 选面（可配置优先面）；
    // 本局未下发该面即抛出明确异常（不猜端口、不静默换面）。
    public static BattleSession Create(DirectPlan plan, BattleSessionOptions? options = null)
    {
        if (plan == null)
        {
            throw new ArgumentNullException(nameof(plan));
        }
        var effective = options ?? new BattleSessionOptions();
        effective.Validate();
        var face = SelectFace(plan, effective);
        return new BattleSession(plan, effective, face, plan.Endpoint(face));
    }

    // Plan 是本次会话的直连计划（票据与面地址来源）。
    public DirectPlan Plan => _plan;

    // Face 是本次会话选定的接入层传输面。
    public EdgeTransport Face => _face;

    // Address 是选定面的接入层地址（host:port）。
    public string Address { get; }

    // LastSeenFrame 是本地已见的最大帧号：重连补帧（SyncFrames）以此为起点。
    // 帧广播推送（ver=1 protojson）会自动推进该值；调用方也可显式赋值（自定义解码时）。
    public ulong LastSeenFrame { get; set; }

    // State 是底层通道状态（未连接/连接中/已连接/重连中/已断开）。
    public ClientState State => Volatile.Read(ref _channel)?.State ?? ClientState.Disconnected;

    // FrameBroadcast 是帧广播推送回调（原始载荷 + 帧头载荷编码版本，SDK 不解码 DTO）。
    public event Action<BattlePush>? FrameBroadcast;

    // BattleEnd 是战斗结束推送回调（原始载荷 + 版本）。
    public event Action<BattlePush>? BattleEnd;

    // Failed 是会话终止回调：接入层拒绝（ProtocolException）或票过期/无效（BusinessException）
    // 等**不可重试**失败——上层据此重新匹配或提示下线。
    public event Action<Exception>? Failed;

    // ConnectAsync 建立直连（hello 握手 + 接入层验票 + 转发到 battle 帧面）并订阅推送。
    // 可选地随后自行调用 JoinBattleAsync / SyncFramesAsync 入局。
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();
        if (Volatile.Read(ref _channel) != null)
        {
            throw new NetworkException(DirectErrors.SessionAlreadyConnected);
        }
        var channel = NewChannel();
        // 先登记再拨号：拨号失败/立刻断线时，重连钩子（重新入局）也能找到本代通道。
        _channel = channel;
        try
        {
            await channel.ConnectAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await CloseChannelAsync().ConfigureAwait(false);
            throw;
        }
    }

    // ReconnectAsync 显式重连：关闭旧通道 → 重新 hello（同一张票）→ JoinBattle →
    // SyncFrames(LastSeenFrame) 补帧。任一环节失败都不留半开会话（关闭后抛出原异常）。
    public async Task ReconnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();
        await CloseChannelAsync().ConfigureAwait(false);
        var channel = NewChannel();
        _channel = channel;
        try
        {
            await channel.ConnectAsync(cancellationToken).ConfigureAwait(false);
            await JoinBattleAsync(cancellationToken).ConfigureAwait(false);
            await SyncFramesAsync(LastSeenFrame, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await CloseChannelAsync().ConfigureAwait(false);
            throw;
        }
    }

    // JoinBattleAsync 入局：用计划里的 battle_id 自动组装 protojson 请求。
    public Task<byte[]> JoinBattleAsync(CancellationToken cancellationToken = default)
    {
        return InvokeAsync(BattleOps.JoinBattle, JoinBattlePayload(), cancellationToken);
    }

    // JoinBattleAsync 入局（原始载荷重载：调用方自备 DTO 编码）。
    public Task<byte[]> JoinBattleAsync(byte[] payload, CancellationToken cancellationToken = default)
    {
        return InvokeAsync(BattleOps.JoinBattle, payload, cancellationToken);
    }

    // SendFrameInputAsync 发送帧输入（原始载荷重载：调用方自备 LockstepInput 编码）。
    public Task<byte[]> SendFrameInputAsync(byte[] payload, CancellationToken cancellationToken = default)
    {
        return InvokeAsync(BattleOps.SendFrameInput, payload, cancellationToken);
    }

    // SendFrameInputAsync 发送帧输入（便捷重载：按 protojson 组装 FrameInputReq）。
    public Task<byte[]> SendFrameInputAsync(ulong frameId, byte[] input, CancellationToken cancellationToken = default)
    {
        return SendFrameInputAsync(FrameInputPayload(frameId, input), cancellationToken);
    }

    // SyncFramesAsync 补帧：以 LastSeenFrame 为起点。
    public Task<byte[]> SyncFramesAsync(CancellationToken cancellationToken = default)
    {
        return SyncFramesAsync(LastSeenFrame, cancellationToken);
    }

    // SyncFramesAsync 补帧：以显式 last_seen_frame 为起点（服务端回缺失帧区间）。
    public Task<byte[]> SyncFramesAsync(ulong lastSeenFrame, CancellationToken cancellationToken = default)
    {
        return InvokeAsync(BattleOps.SyncFrames, SyncFramesPayload(lastSeenFrame), cancellationToken);
    }

    // JoinBattlePayload 组装 JoinBattle 请求载荷（protojson：{"battleId":"..."}）。
    public byte[] JoinBattlePayload()
    {
        return Utf8("{\"battleId\":" + Quote(_plan.BattleId) + "}");
    }

    // SyncFramesPayload 组装补帧请求载荷（64 位整数按 protojson 字符串形态）。
    public byte[] SyncFramesPayload(ulong lastSeenFrame)
    {
        return Utf8("{\"battleId\":" + Quote(_plan.BattleId)
            + ",\"lastSeenFrame\":\"" + lastSeenFrame + "\"}");
    }

    // FrameInputPayload 组装帧输入请求载荷（bytes 按 protojson 的 base64 形态）。
    public byte[] FrameInputPayload(ulong frameId, byte[]? input)
    {
        return Utf8("{\"battleId\":" + Quote(_plan.BattleId) + ",\"input\":{\"frameId\":\"" + frameId
            + "\",\"payload\":\"" + Convert.ToBase64String(input ?? Array.Empty<byte>()) + "\"}}");
    }

    // CloseAsync 关闭会话（幂等）：停止重连、结算在途请求、关闭连接。关闭后不可再连接。
    public async Task CloseAsync()
    {
        Interlocked.Exchange(ref _closed, 1);
        await CloseChannelAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
    }

    // InvokeAsync 发一次帧请求（未连接/已关闭即失败，不静默丢弃）。
    private Task<byte[]> InvokeAsync(string operation, byte[]? payload, CancellationToken cancellationToken)
    {
        ThrowIfClosed();
        var channel = Volatile.Read(ref _channel)
            ?? throw new NetworkException($"{DirectErrors.SessionClosed}: 尚未连接（先 ConnectAsync）");
        return channel.InvokeRawAsync(operation, payload, cancellationToken);
    }

    // CloseChannelAsync 关闭并摘下当前通道（幂等：无通道时直接返回）。
    private async Task CloseChannelAsync()
    {
        var channel = Interlocked.Exchange(ref _channel, null);
        if (channel != null)
        {
            await channel.CloseAsync().ConfigureAwait(false);
        }
    }

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new NetworkException(DirectErrors.SessionClosed);
        }
    }

    // OnFrameBroadcast 分发帧广播：推进 LastSeenFrame 后交给回调（SDK 不解码 DTO）。
    private void OnFrameBroadcast(string op, byte[] payload, byte version)
    {
        TrackLastSeenFrame(payload, version);
        FrameBroadcast?.Invoke(new BattlePush(op, payload, version));
    }

    // OnBattleEnd 分发战斗结束推送。
    private void OnBattleEnd(string op, byte[] payload, byte version)
    {
        BattleEnd?.Invoke(new BattlePush(op, payload, version));
    }

    // TrackLastSeenFrame 从帧广播载荷尽力取帧号（ver=1 protojson：{"frame":{"frameId":"9"}}）
    // 用于断线补帧；取不到（ver=2/字段缺失/非 JSON）就保持原值——不猜、不抛错。
    private void TrackLastSeenFrame(byte[] payload, byte version)
    {
        if (version != FrameGen.Version)
        {
            return;
        }
        try
        {
            using var document = JsonDocument.Parse(payload);
            if (!document.RootElement.TryGetProperty("frame", out var frame)
                || frame.ValueKind != JsonValueKind.Object)
            {
                return;
            }
            if (!frame.TryGetProperty("frameId", out var id) && !frame.TryGetProperty("frame_id", out id))
            {
                return;
            }
            if (id.ValueKind == JsonValueKind.String && ulong.TryParse(id.GetString(), out var fromText))
            {
                LastSeenFrame = fromText;
            }
            else if (id.ValueKind == JsonValueKind.Number && id.TryGetUInt64(out var fromNumber))
            {
                LastSeenFrame = fromNumber;
            }
        }
        catch (JsonException)
        {
            // 推送载荷不是 protojson：保持原值（LastSeenFrame 由调用方维护）。
        }
    }

    // Quote 把字符串编码为 JSON 字面量（battle_id 可能含引号等字符）。
    private static string Quote(string value)
    {
        return JsonSerializer.Serialize(value);
    }

    private static byte[] Utf8(string json)
    {
        return Encoding.UTF8.GetBytes(json);
    }
}
