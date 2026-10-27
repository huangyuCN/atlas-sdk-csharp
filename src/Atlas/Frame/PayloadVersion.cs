namespace Atlas.Frame;

// PayloadVersion 是载荷编码版本（规范 §3.1 载荷编码协商）的唯一判定处：帧头 version 字段
// 声明本帧载荷的编码方式——ver=1 为 protojson（可读，保留为**调试开关**）、ver=2 为
// protobuf 二进制（生产主路径，游戏侧生成包解码）。帧级自描述 ⇒ SDK 只看帧头即可判定编码，
// 不需要（也不应该）猜载荷编码。
internal static class PayloadVersion
{
    // SupportedList 是白名单的文本形态（错误消息用）：散落的 "{1,2}" 字面量容易漂移。
    public const string SupportedList = "{1,2}";

    // IsSupported 报告版本是否在载荷编码白名单内（ver=1 protojson / ver=2 protobuf 二进制）。
    public static bool IsSupported(int version)
    {
        return version == FrameGen.Version || version == FrameGen.Version2;
    }
}
