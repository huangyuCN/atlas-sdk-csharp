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

    // HeartbeatLoopAsync 是心跳周期循环：按 HeartbeatIntervalMs 周期发 Ping（failFast
    // 直通路径）。绑定代（epoch）：代 token 被取消（换代/关闭）即退出——旧代心跳不会
    // 继续在新连接上运行（M4 评审 P1：旧代心跳不得误杀新代连接）。连续失败
    // HeartbeatFailures 次判定死链：关闭本代连接触发读循环退出与重连。
    private async Task HeartbeatLoopAsync(uint epoch, CancellationToken token)
    {
        var failures = 0;
        while (await HeartbeatDelayAsync(token))
        {
            try
            {
                // failFast：死链期间进入重连排队无意义，心跳自身的失败计数就是
                // 重连触发器（对齐 Go heartbeatLoop 的 WithFailFast 语义）。
                await InvokeRawFailFastAsync(HeartbeatOperation, null, token);
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
                // 评审 M4-2 note：ProtocolException（协议致命）也经此分支计入失败。真实
                // 场景中协议致命已先使 readLoop 因 ProtocolException 退出（terminate 不
                // 重连，连接被关）——心跳随后在此分支计失败并按代核对关闭连接，连接已关
                // 故无害；无双路径触发重连（重连仅由 readLoop 的 shouldReconnect 驱动，
                // 协议致命时不为 true）。此处计数保留作网络类（超时/写失败）死链判定用。
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

    // HeartbeatDelayAsync 等待下一个心跳周期：代取消（换代/关闭）返回 false，调用方退出。
    private async Task<bool> HeartbeatDelayAsync(CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return false;
        }
        try
        {
            await Task.Delay(_options.HeartbeatIntervalMs, token);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        return !token.IsCancellationRequested;
    }
}
