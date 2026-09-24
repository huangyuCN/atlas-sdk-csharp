using System;
using System.Collections.Generic;

namespace Atlas.Errors;

// ErrorClass 是错误分类（业务 / 运行时 / 取消）：日志定级、故障率口径与客户端处置
// 决策用。取值对齐框架 errors.proto 的 ErrorClass 枚举（0 未分类 / 1 业务 / 2 运行时 /
// 3 取消）——帧通道下发数值（proto enum 的 wire 形态），protojson 通道下发枚举名
// （ERROR_CLASS_BUSINESS 等），两种形态都由 ErrorClassCodec 解析。
//
// 待回归：框架侧尚未产出该枚举的 C# 快照（errors 只有 Go 产物）；待框架生成 C# 常量后
// 本枚举改为引用生成物（同 FrameGen 的做法），数值/枚举名以生成物为准。
public enum ErrorClass
{
    // Unspecified 未分类：历史错误或未显式标注；定级时按运行时处理。
    Unspecified = 0,

    // Business 业务错误：玩家可理解的业务拒绝（道具不足、不在队伍中等）。
    Business = 1,

    // Runtime 运行时错误：基础设施或编程缺陷（DB 不可达、序列化失败等）。
    Runtime = 2,

    // Canceled 取消：客户端主动断开或超时取消（不算故障）。
    Canceled = 3,
}

// ErrorClassCodec 是错误分类的**唯一解析点**：同时接受枚举名与数值。
//   - 枚举名（protojson 下发形态）：ERROR_CLASS_BUSINESS / ERROR_CLASS_RUNTIME ...
//   - 数值（帧 Status.class 的 wire 形态）：0 / 1 / 2 / 3（字符串或数字）
//   - 稳定标签（gRPC metadata 的 atlas.error_class 形态）：business / runtime / canceled
// 未知取值一律回落 Unspecified（前向兼容：新增分类不炸旧客户端）。
public static class ErrorClassCodec
{
    // ProtoEnumPrefix 是 protojson 枚举名的前缀（errors.proto 的 ErrorClass 枚举）。
    public const string ProtoEnumPrefix = "ERROR_CLASS_";

    private static readonly Dictionary<string, ErrorClass> Labels = new(StringComparer.OrdinalIgnoreCase)
    {
        ["unspecified"] = ErrorClass.Unspecified,
        ["business"] = ErrorClass.Business,
        ["runtime"] = ErrorClass.Runtime,
        ["canceled"] = ErrorClass.Canceled,
    };

    // Of 按数值取分类（wire varint 形态；未知数值原样保留，前向兼容）。
    public static ErrorClass Of(int value)
    {
        return (ErrorClass)value;
    }

    // Parse 解析枚举名或数值（空/未知回落 Unspecified，不抛错）。
    public static ErrorClass Parse(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return ErrorClass.Unspecified;
        }
        var trimmed = text.Trim();
        if (int.TryParse(trimmed, out var numeric))
        {
            return Of(numeric);
        }
        var name = trimmed;
        if (name.StartsWith(ProtoEnumPrefix, StringComparison.OrdinalIgnoreCase))
        {
            name = name[ProtoEnumPrefix.Length..];
        }
        return Labels.TryGetValue(name, out var value) ? value : ErrorClass.Unspecified;
    }

    // Label 返回稳定标签（小写；与框架 errors.Class.String() 同源，gRPC metadata 用）。
    public static string Label(ErrorClass value)
    {
        return value switch
        {
            ErrorClass.Business => "business",
            ErrorClass.Runtime => "runtime",
            ErrorClass.Canceled => "canceled",
            _ => "unspecified",
        };
    }

    // ProtoName 返回 protojson 枚举名（ERROR_CLASS_*；越界取值回落未分类）。
    public static string ProtoName(ErrorClass value)
    {
        return ProtoEnumPrefix + Label(value).ToUpperInvariant();
    }
}
