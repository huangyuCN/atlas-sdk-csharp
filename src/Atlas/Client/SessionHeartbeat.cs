using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Client;

public sealed partial class Channel
{
    // 会话心跳（业务续租）调度：对标 Go sessionHeartbeatLoop（规范 §5.2 业务层）。
    // 周期调用 opFactory 构造业务 Heartbeat 并 Invoke（续租 Redis 会话 TTL）；
    // 业务错误（*BusinessException，如会话过期）→ 单飞触发重登钩子（OnRelogin）；
    // 网络错误/超时静默——连接死亡由传输心跳与重连机制接管（会话心跳自身不触发
    // 重连）。绑定代（epoch）：代 token 取消（换代/关闭）即退出——旧代会话心跳
    // 不会在新连接上续租（对标 Go 随 g.done 退出）。
    private async Task SessionHeartbeatLoopAsync(uint epoch, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.SessionHeartbeatIntervalMs, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            if (token.IsCancellationRequested)
            {
                return;
            }
            if (_options.SessionHeartbeatOpFactory == null)
            {
                return; // 工厂被清空（配置变更防御）：停止调度。
            }
            var request = _options.SessionHeartbeatOpFactory();
            if (string.IsNullOrEmpty(request.Operation))
            {
                continue; // 工厂未就绪（如尚未登录无 token）：跳过本轮（对标 Go）。
            }
            try
            {
                // 会话心跳走直发路径（failFast，不排队）：断线重连期间心跳失败静默
                //（新代连接建立后随代重启续租）——排队等待重连补发对续租无意义，
                // 且重连期间旧 token 续租可能顶掉新会话（对齐 Go invokeOnce 直发）。
                await InvokeRawFailFastAsync(request.Operation, request.Payload, CancellationToken.None);
            }
            catch (BusinessException)
            {
                // 业务拒绝（会话过期/无效）：触发重登钩子（单飞）。
                TriggerReloginHook();
            }
            catch (AtlasException)
            {
                // NetworkException/TimeoutException 静默：重连机制处理（对标 Go）。
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    // TriggerReloginHook 单飞触发会话重登钩子（会话心跳业务错误路径，对标 Go
    // triggerReloginHook）：重连钩子同步执行期间（hookBypass，对齐 Go）或上一轮
    // 触发未完成时跳过——避免并发重登造成 Login 竞态与 token 抖动；失败静默，
    // 下一轮会话心跳再触发。钩子异步执行，SDK 不等待其完成（业务方决定重登/下线）。
    private void TriggerReloginHook()
    {
        if (IsHookBypass())
        {
            return; // 重连钩子同步执行中（hookBypass 窗口）：本轮跳过（对齐 Go）。
        }
        if (Interlocked.CompareExchange(ref _sessionHookBusy, 1, 0) != 0)
        {
            return; // 上一轮触发的钩子未返回：本轮跳过（CAS 单飞）。
        }
        _ = RunReloginHookAsync();
    }

    private async Task RunReloginHookAsync()
    {
        try
        {
            var hook = OnRelogin;
            if (hook != null)
            {
                await hook();
            }
        }
        catch (Exception)
        {
            // 重登钩子异常静默（业务方自决重登失败的下线策略；对齐 Go 的 _ = fn()）。
        }
        finally
        {
            Volatile.Write(ref _sessionHookBusy, 0);
        }
    }
}
