using System.Threading;
using Atlas.Scheduling;

namespace Atlas.Unity;

// AtlasUnity 是 Unity 宿主的适配桥（对齐设计文档 §6.2：handler 默认线程池、
// Unity 侧可选注入主线程 SynchronizationContext 让 Notify 等用户回调在主线程
// 执行——游戏对象/UI 操作安全）。
//
// 典型用法（Unity 侧脚本，非本程序集）：
//   Atlas.Unity.AtlasUnity.SetMainThreadScheduler(SynchronizationContext.Current);
// 在 Unity 主线程（MonoBehaviour.Start/Awake）调用一次；之后全部 Notify handler
// 经 UnitySynchronizationContext.Post 发布到主线程。传 null 恢复默认（线程池）。
public static class AtlasUnity
{
    // SetMainThreadScheduler 注入 Unity 主线程调度器；null 恢复默认线程池执行。
    // 线程安全：转发到 AtlasScheduler.SetScheduler（原子替换）。
    public static void SetMainThreadScheduler(SynchronizationContext? context)
    {
        AtlasScheduler.SetScheduler(context);
    }
}
