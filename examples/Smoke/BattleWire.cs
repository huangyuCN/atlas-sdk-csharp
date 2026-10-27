using System;
using System.Text;
using System.Text.Json;

namespace Atlas.Smoke;

// BattleWire 是冒烟侧的战斗绑定探针载荷（**示例局部构造**）：
// 游戏域消息（入局/发帧/补帧等请求消息）已迁出 SDK，真实项目请用游戏协议包
//（NuGet HuangyuCN.AtlasGame.Protocol，源自模板仓 protocol/）生成的 DTO 与 op 常量；
// 冒烟保持零游戏依赖，故只在这里写线上字节。
internal static class BattleWire
{
    // JoinJson 构造入局请求的 protojson 载荷（ver=1 路径）。
    public static byte[] JoinJson(string battleId)
    {
        return Encoding.UTF8.GetBytes("{\"battleId\":" + JsonSerializer.Serialize(battleId) + "}");
    }

    // JoinProtobuf 构造入局请求的 protobuf 二进制载荷（ver=2 路径，字段 1 = battle_id）：
    // 手写这几字节是示例的取舍——真实项目由协议包生成 DTO 后序列化。
    public static byte[] JoinProtobuf(string battleId)
    {
        var text = Encoding.UTF8.GetBytes(battleId);
        var payload = new byte[2 + text.Length];
        payload[0] = 0x0A; // field 1, wire type 2（length-delimited，string）
        payload[1] = (byte)text.Length;
        Array.Copy(text, 0, payload, 2, text.Length);
        return payload;
    }
}
