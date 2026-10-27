using System;

namespace Atlas.Battle;

// ReplayStep 是重连后要重放的一步（通常是「入局」与「按 LastSeenFrame 补帧」）。
//
// 为什么由调用方给出：游戏域消息（入局/补帧等请求消息）已迁出 SDK，SDK 不再持有
// 这些类型、也不知道它们的字段——重放什么、载荷怎么编，只有拿协议包的游戏侧知道。SDK 只负责
// 「拨号成功后按顺序发出、把业务拒绝按终态口径收口」。
public sealed class ReplayStep
{
    // ReplayStep 构造一步重放：op 是协议寻址键（游戏协议包生成的常量），
    // payload 按当前补帧基准生成请求体（null = 空载荷；未设置时发空载荷）。
    public ReplayStep(string op, Func<ulong, byte[]?>? payload = null)
    {
        if (string.IsNullOrEmpty(op))
        {
            throw new ArgumentException("op 不能为空（重放步骤必须可寻址）", nameof(op));
        }
        Op = op;
        Payload = payload;
    }

    // Op 是重放请求的 op（协议寻址键，取值来自游戏协议包）。
    public string Op { get; }

    // Payload 按当前补帧基准（LastSeenFrame）生成请求载荷；null 表示空载荷。
    public Func<ulong, byte[]?>? Payload { get; }
}
