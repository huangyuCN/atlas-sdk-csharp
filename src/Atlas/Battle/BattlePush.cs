using System;

namespace Atlas.Battle;

// BattlePush 是一次服务端推送的信封视图（**不含任何游戏域消息类型**）：op + 帧头载荷编码版本
// + 原始载荷。SDK 不做 DTO 解码：调用方用它自己协议包的 op 常量订阅，按 version 选解码器
//（ver=1 protojson / ver=2 protobuf wire）——两种都由游戏侧协议包解码，SDK 只交字节与版本；
// 与 Channel.On 的回调口径一致。
public sealed class BattlePush
{
    public BattlePush(string op, byte[] payload, byte version)
    {
        Op = op ?? "";
        Payload = payload ?? Array.Empty<byte>();
        Version = version;
    }

    // Op 是推送 op（游戏协议包生成的常量；如 /battle.v1.FrameBroadcast）。
    public string Op { get; }

    // Payload 是推送原始载荷字节（未解码）。
    public byte[] Payload { get; }

    // Version 是帧头载荷编码版本（1 = protojson，2 = protobuf wire）。
    public byte Version { get; }
}
