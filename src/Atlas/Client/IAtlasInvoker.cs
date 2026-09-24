using System.Threading;
using System.Threading.Tasks;

namespace Atlas.Client;

// IAtlasInvoker 是强类型 stub 生成物（protoc-gen-atlas-client 的 C# 输出）的最小依赖：
// 会话内核 AtlasClient 天然满足，业务测试可注入 fake。stub 经构造注入的 ISerializer
// 编解码 DTO ↔ payload 字节（R12：stub 内不写死 JSON 库），本接口只承担「按 op 发原始
// 载荷并等待响应」，并把调用级选项（含幂等键）透传给服务端。
public interface IAtlasInvoker
{
    // InvokeRawAsync 发送原始 payload 并等待响应（语义同 AtlasClient.InvokeRawAsync）。
    Task<byte[]> InvokeRawAsync(string operation, byte[]? payload, CancellationToken cancellationToken);

    // InvokeRawAsync 带调用级选项（显式幂等键 / 本次不携带；语义同
    // AtlasClient.InvokeRawAsync 的选项重载）。
    Task<byte[]> InvokeRawAsync(
        string operation, byte[]? payload, InvokeOptions? options, CancellationToken cancellationToken);
}
