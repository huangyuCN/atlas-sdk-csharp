using System;

namespace Atlas.Unity;

// WebSocketPad 是 WebGL 平台的 WebSocket 传输垫片占位（设计文档 §5 通道矩阵：
// Unity WebGL 仅 WebSocket 可用——浏览器原生 WS；引擎宿主经 JSB 绑定的
// WebSocket 对象收发二进制消息，等价 single 形态的单通道业务+战斗）。
//
// 占位说明（M5-3 Unity 真机验证时实现/接线）：
//   - WebGL 构建不能直接用 System.Net.WebSockets.ClientWebSocket（Unity WebGL
//     无托管 socket 实现），需经引擎的 WS 绑定（NativeWebSocket 或
//     UnityEngine.Networking 的 WebSocket）。
//   - Atlas 核心传输抽象 ITransport 已就绪（ReadFrameAsync/WriteFrameAsync），
//     WebGL 适配实现把「一条消息 = 一个完整帧」映射到引擎 WS 的二进制消息事件，
//     语义与 WsTransport 完全一致（见 src/Atlas/Transport/WsTransport.cs）。
//   - 本文件仅声明接入点与约束，不引入 UnityEngine 编译依赖（引擎类型由
//     Unity 工程侧组装，运行时经 AtlasScheduler 注入主线程上下文）。
//
// WebGL 通道拨号入口约定：AtlasClient 单通道形态（ChannelKind.Business +
// TransportWS 语义），经引擎 WS 拨号函数替换 Channel 的 dial 工厂即可复用全部
// 内核（匹配/Notify/心跳/重连）——无需新内核代码。
public static class WebSocketPad
{
    // WebGLReady 标记当前宿主是否处于 WebGL 平台（M5-3 验证时由 Unity 工程
    // 注入；非 WebGL 恒 false，保持默认 socket 通道）。
    public static bool WebGLReady { get; set; }
}
