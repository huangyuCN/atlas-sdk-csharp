// 关键帧请求（RequestFrameKeyframe）的载荷编码：op 是游戏协议寻址键，载荷字段属游戏协议，
// 但 SDK 在解码失步时必须能**自己**发一次请求（否则「丢弃」就变成永久花屏）。
//
// 字段号与 battle.v1.RequestFrameKeyframeReq 一致：
//   string battle_id = 1; uint64 last_seq = 2;
// ver=1（protojson）发 `{"battleId":"...","lastSeq":"<uint64 字符串>"}`；
// ver=2（protobuf 二进制）按最小 wire 编码。只说「我实际拥有哪一帧」，不携带任何对局参数。
using System;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Atlas.Frame;

namespace Atlas.Downlink;

/// <summary>KeyframeRequest 构造关键帧请求载荷（按会话载荷编码版本）。</summary>
public static class KeyframeRequest
{
    /// <summary>Encode 按 version 生成 RequestFrameKeyframeReq 载荷字节。</summary>
    public static byte[] Encode(string battleId, ulong lastSeq, int version)
    {
        if (version == FrameGen.Version)
        {
            var json = JsonSerializer.Serialize(new KeyframeJson
            {
                BattleId = battleId,
                LastSeq = lastSeq.ToString(CultureInfo.InvariantCulture),
            });
            return Encoding.UTF8.GetBytes(json);
        }
        return Wire.Concat(
            Wire.LengthDelimited(1, Encoding.UTF8.GetBytes(battleId)),
            Wire.VarintField(2, lastSeq));
    }

    // KeyframeJson 是 protojson 形态的请求体（字段名 lowerCamelCase）。
    private sealed class KeyframeJson
    {
        [System.Text.Json.Serialization.JsonPropertyName("battleId")]
        public string BattleId { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("lastSeq")]
        public string LastSeq { get; set; } = string.Empty;
    }
}
