using System;
using System.Threading;

namespace Atlas.Scheduling;

// AtlasScheduler 是核心库的用户回调调度器（对齐设计文档 §6.2：handler 默认线程池、
// 宿主可注入主线程 SynchronizationContext 让 handler 在主线程执行）。
//
// 背景：库不感知 Unity 主线程——Notify handler 等用户回调默认在线程池执行；
// Unity 宿主经 Atlas.Unity 的 SetMainThreadScheduler 注入主线程同步上下文后，
// 全部用户回调经 SynchronizationContext.Post 发布到主线程（游戏对象/UI 安全）。
// 非 Unity 宿主（服务器工具/压测）保持默认线程池，注入 null 即恢复默认。
public static class AtlasScheduler
{
    private static SynchronizationContext? _context;

    private static readonly object Gate = new();

    // SetScheduler 注入回调调度上下文；null 恢复默认（线程池执行）。
    // 线程安全：原子替换，后续 Post 立即走新调度器。
    public static void SetScheduler(SynchronizationContext? context)
    {
        lock (Gate)
        {
            _context = context;
        }
    }

    // Post 将回调投递到当前调度器（默认线程池；注入后经 SynchronizationContext.Post）。
    // 不阻塞调用线程；回调异常由调用方（如 SafeNotify）负责隔离。
    // Post 自身对注入上下文的 Post 调用做异常防护——宿主上下文若实现异常
    //（已终结/非线程安全等）不会沿读循环冒泡误杀连接，回退线程池执行回调。
    public static void Post(Action callback)
    {
        if (callback == null)
        {
            throw new ArgumentNullException(nameof(callback));
        }
        SynchronizationContext? context;
        lock (Gate)
        {
            context = _context;
        }
        if (context != null)
        {
            try
            {
                context.Post(_ => callback(), null);
                return;
            }
            catch (Exception)
            {
                // 注入上下文异常（如宿主已终结）：回退线程池，保证回调仍执行且
                // 异常不冒泡到读循环（避免误判为网络失败触发 FailGeneration）。
            }
        }
        ThreadPool.QueueUserWorkItem(_ => callback());
    }
}
