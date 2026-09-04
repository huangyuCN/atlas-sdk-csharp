using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Client;

public sealed partial class Channel
{
    // HeartbeatOperation 是传输保活心跳 operation（服务端 StreamEngine 内置空响应
    // handler；对标 Go client.HeartbeatOperation）。传输心跳不续租业务会话。
    public const string HeartbeatOperation = "/atlas.internal.Heartbeat/Ping";

    // 心跳周期循环：按 HeartbeatIntervalMs 周期发 Ping（failFast 直通路径）。
    // 绑定代（epoch）：代 token 被取消（换代/关闭）即退出——旧代心跳不会继续在
    // 新连接上运行（M4 评审 P1：旧代心跳不得误杀新代连接）。
    // 连续失败 HeartbeatFailures 次判定死链：关闭本代连接触发读循环退出与重连。
    // 业务拒绝不计死链——往返完成即链路存活（对标 Go heartbeatLoop：
    // BusinessError 失败计数归零，仅网络类失败计死链）。
    private async Task HeartbeatLoopAsync(uint epoch, CancellationToken token)
    {
        var failures = 0;
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.HeartbeatIntervalMs, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            if (token.IsCancellationRequested)
            {
                return;
            }
            try
            {
                await InvokeRawAsync(HeartbeatOperation, null, token);
                failures = 0;
            }
            catch (BusinessException)
            {
                failures = 0; // 业务拒绝（往返完成）：链路存活，不计死链。
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (AtlasException)
            {
                failures++;
                if (failures >= _options.HeartbeatFailures)
                {
                    // 死链：关闭本代连接（换代已发生则跳过，不误杀新连接）。
                    // readLoop 的 ReadFrameAsync 随之退出，驱动自动重连。
                    await CloseGenerationTransportAsync(epoch);
                    return;
                }
            }
        }
    }
}
