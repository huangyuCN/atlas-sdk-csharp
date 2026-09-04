using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;
using Atlas.Transport;

namespace Atlas.Client;

public sealed partial class Channel
{
    // HeartbeatOperation 是传输保活心跳 operation（服务端 StreamEngine 内置空响应
    // handler；对标 Go client.HeartbeatOperation）。传输心跳不续租业务会话。
    public const string HeartbeatOperation = "/atlas.internal.Heartbeat/Ping";

    // 心跳周期循环：按 HeartbeatIntervalMs 周期发 Ping（failFast 直通路径）。
    // 连续失败 HeartbeatFailures 次判定死链：关闭当前代连接触发 readLoop 退出
    //（M4 接重连；M2-4 关闭连接即由 readLoop 收尾结算 in-flight 与状态）。
    // 业务拒绝不计死链——往返完成即链路存活（对标 Go heartbeatLoop：
    // BusinessError 失败计数归零，仅网络类失败计死链）。
    private async Task HeartbeatLoopAsync(CancellationToken token)
    {
        if (_options.HeartbeatIntervalMs <= 0)
        {
            return; // 非正周期 = 关闭传输心跳。
        }
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
                    // 死链：关闭当前代连接。readLoop 的 ReadFrameAsync 随之抛出并
                    // 退出，由 FailGeneration 结算 in-flight 与状态（对标 Go g.tr.Close()）。
                    await CloseCurrentTransportAsync();
                    return;
                }
            }
        }
    }

    // CloseCurrentTransportAsync 关闭当前代连接（死链触发 readLoop 收尾）。
    private async Task CloseCurrentTransportAsync()
    {
        ITransport? transport;
        lock (_gate)
        {
            transport = _transport;
        }
        if (transport != null)
        {
            await CloseTransportAsync(transport);
        }
    }
}
