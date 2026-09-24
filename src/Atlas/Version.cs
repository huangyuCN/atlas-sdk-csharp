using System;
using System.Reflection;

namespace Atlas;

// AtlasVersion 是 SDK 客户端版本单一来源：取自 Atlas.csproj 的 <Version>
//（构建时生成为 AssemblyInformationalVersionAttribute），仓内无第二处手写版本字面量。
// 登录/握手请求据此上报 client_version（服务端按 min_client_version 门槛裁决）。
public static class AtlasVersion
{
    // Value 返回语义化版本串（去掉 SDK 追加的构建元数据后缀 "+<commit>"）。
    public static string Value { get; } = Resolve();

    private static string Resolve()
    {
        var assembly = typeof(AtlasVersion).Assembly;
        var informational = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrEmpty(informational))
        {
            var plus = informational!.IndexOf('+');
            return plus < 0 ? informational : informational.Substring(0, plus);
        }
        return assembly.GetName().Version?.ToString(3) ?? "0.0.0";
    }
}
