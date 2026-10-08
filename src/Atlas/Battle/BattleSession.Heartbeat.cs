using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;

namespace Atlas.Battle;

// BattleSession 的直连保活心跳（battle 域探针）：无输入期间按 HeartbeatInterval 周期
// 发送 battle.v1.BattleService/Ping（Tell：服务端不回业务回执），让帧面保持活跃——
// 数据报面（UDP/KCP）的活跃判定靠帧面空闲读超时发现（服务端取 offline_timeout/3），
// 长时间静默会被判掉线，且 NAT 映射失效后下行帧收不到（「连接还在但没数据」）。
//
// 语义要点：
//   - 周期到点才发（不是连接即发）；TimeSpan.Zero = 关闭（不发、不建定时器）；
//   - 失败只记/事件（HeartbeatFailed + Warn 日志），**不终止会话**、不影响正常发帧；
//   - 未连接（重连窗口内）本拍静默跳过：无连接时心跳无事可证，重连编排负责恢复；
//   - CloseAsync / 会话终止（票废）即停表并等循环退出，不留悬挂定时器。
public sealed partial class BattleSession
{
    // _heartbeatGate 保护起停竞态（CloseAsync 与 ConnectAsync 可能并发调用）。
    private readonly object _heartbeatGate = new();
    private CancellationTokenSource? _heartbeatCts;
    private Task? _heartbeatTask;

    // HeartbeatFailed 是探针失败回调（超时/写失败/业务拒绝）：上层据此观测保活质量；
    // 回调异常被隔离，失败本身不终止会话（掉线判定在服务端，SDK 不擅自断开）。
    public event Action<Exception>? HeartbeatFailed;

    // PingPayload 组装探针请求载荷（protojson：{"battleId":"..."}）：只带客体寻址字段，
    // 不承载任何对局参数——探针语义就是「我还在」（加参数即越界）。
    public byte[] PingPayload()
    {
        return Utf8("{\"battleId\":" + Quote(_plan.BattleId) + "}");
    }

    // StartHeartbeat 启动保活心跳（连接成功后调用；幂等：已在跑即复用；非正周期 = 关闭）。
    private void StartHeartbeat()
    {
        if (_options.HeartbeatInterval <= TimeSpan.Zero)
        {
            return;
        }
        lock (_heartbeatGate)
        {
            if (_heartbeatCts != null)
            {
                return;
            }
            var cts = new CancellationTokenSource();
            _heartbeatCts = cts;
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(cts.Token));
        }
    }

    // StopHeartbeatAsync 停表并等循环退出（幂等、可重入）：CloseAsync 与终止路径调用。
    // 取消令牌使周期等待即时返回；取消中在途的探针按取消退出，不算失败。
    private async Task StopHeartbeatAsync()
    {
        CancellationTokenSource? cts;
        Task? task;
        lock (_heartbeatGate)
        {
            cts = _heartbeatCts;
            task = _heartbeatTask;
            _heartbeatCts = null;
            _heartbeatTask = null;
        }
        if (cts != null)
        {
            cts.Cancel();
        }
        if (task != null)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // 心跳退出异常不向上传播（关闭路径以关闭成功为准）。
            }
        }
        cts?.Dispose();
    }

    // HeartbeatLoopAsync 是保活周期循环：等待一拍 → 发探针 → 记失败 → 再等。
    // 退出条件只有「令牌取消」（关闭/终止）；探针失败绝不终止循环。
    private async Task HeartbeatLoopAsync(CancellationToken token)
    {
        while (await HeartbeatDelayAsync(token).ConfigureAwait(false))
        {
            try
            {
                await SendPingAsync(token).ConfigureAwait(false);
                _logger.Debugf("heartbeat ok op={0}", BattleOps.Ping);
            }
            catch (OperationCanceledException)
            {
                return; // 关闭/终止取消：心跳随会话退出，不计失败。
            }
            catch (Exception exception)
            {
                RecordHeartbeatFailure(exception);
            }
        }
    }

    // HeartbeatDelayAsync 等待下一拍：令牌取消（关闭/终止）返回 false，调用方退出循环。
    private async Task<bool> HeartbeatDelayAsync(CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return false;
        }
        try
        {
            await Task.Delay(_options.HeartbeatInterval, token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        return !token.IsCancellationRequested;
    }

    // SendPingAsync 发一拍探针：未连接（重连窗口内）跳过；直通路径不排队
    //（死链/重连期间排队无意义，探针不是业务请求），与正常发帧共用通道写锁、互不干扰。
    private Task<byte[]> SendPingAsync(CancellationToken token)
    {
        ThrowIfClosed();
        var channel = Volatile.Read(ref _channel);
        if (channel == null || channel.State != ClientState.Connected)
        {
            return Task.FromResult(Array.Empty<byte>());
        }
        return channel.InvokeRawFailFastAsync(BattleOps.Ping, PingPayload(), token);
    }

    // RecordHeartbeatFailure 记一次探针失败：Warn 日志 + 事件（回调异常隔离）。
    private void RecordHeartbeatFailure(Exception exception)
    {
        _logger.Warnf("heartbeat failed op={0}: {1}", BattleOps.Ping, exception.Message);
        var handler = HeartbeatFailed;
        if (handler == null)
        {
            return;
        }
        try
        {
            handler(exception);
        }
        catch (Exception)
        {
            // 上层回调异常隔离（不影响心跳循环与发帧）。
        }
    }
}
