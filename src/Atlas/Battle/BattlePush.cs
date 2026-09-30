using System;

namespace Atlas.Battle;

// BattlePush 是一次服务端推送（帧广播 / 战斗结束）的视图：op + 帧头载荷编码版本 + 原始载荷。
// SDK 不做 DTO 解码（零 protobuf 运行时依赖）：调用方按 version 选解码器
//（ver=1 protojson / ver=2 protobuf wire），与 Channel.On 的回调口径一致。
public sealed class BattlePush
{
    public BattlePush(string op, byte[] payload, byte version)
    {
        Op = op ?? "";
        Payload = payload ?? Array.Empty<byte>();
        Version = version;
    }

    // Op 是推送 op（BattleOps.FrameBroadcast / BattleOps.BattleEndNotify）。
    public string Op { get; }

    // Payload 是推送原始载荷字节（未解码）。
    public byte[] Payload { get; }

    // Version 是帧头载荷编码版本（1 = protojson，2 = protobuf wire）。
    public byte Version { get; }
}
