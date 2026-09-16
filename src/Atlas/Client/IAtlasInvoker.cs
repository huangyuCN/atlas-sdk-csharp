using System.Threading;
using System.Threading.Tasks;

namespace Atlas.Client;

// IAtlasInvoker 是强类型 stub 生成物（protoc-gen-atlas-client 的 C# 输出）的最小依赖：
// 会话内核 AtlasClient 天然满足，业务测试可注入 fake。stub 自带 System.Text.Json
// 序列化（DTO ↔ payload 字节），本接口只承担「按 op 发原始载荷并等待响应」。
public interface IAtlasInvoker
{
    // InvokeRawAsync 发送原始 payload 并等待响应（语义同 AtlasClient.InvokeRawAsync）。
    Task<byte[]> InvokeRawAsync(string operation, byte[]? payload, CancellationToken cancellationToken);
}
