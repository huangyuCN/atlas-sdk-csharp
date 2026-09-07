# Atlas SDK（Unity UPM 包）

Atlas 帧协议 C# 客户端 SDK 的 Unity 分发包。核心库（Atlas）与 Unity 适配薄层
（Atlas.Unity）以预编译 dll 形式置于 `Plugins/`，随包附带第三方运行时依赖
（Google.Protobuf / KcpSharp）——**无需额外安装 NuGet 包**。

## 安装

Package Manager → Add package from git URL（或本地 path）：

```
https://github.com/huangyuCN/atlas-sdk-csharp.git?path=/packages/com.huangyucn.atlas
```

## 快速开始

```csharp
using Atlas.Client;
using Atlas.Serialization;

// 业务通道（TCP）连 Atlas 网关，注册→登录后发请求/收推送。
var client = new AtlasClient(
    new ChannelConfig(ChannelKind.Business, addr => TcpTransport.ConnectAsync(host, port, token))
    {
        Options = new ChannelOptions { Serializer = new JsonSerializer() },
    });
await client.ConnectAsync(CancellationToken.None);
var reply = await client.InvokeRawAsync("/gateway.v1.GatewayAuth/Login", payload, CancellationToken.None);
```

### Unity 主线程调度

Notify handler 默认在线程池执行；若需发布到 Unity 主线程，注入主线程
`SynchronizationContext`：

```csharp
AtlasUnity.SetMainThreadScheduler(UnitySynchronizationContext.Current);
```

## 重新打包

`scripts/package-upm.sh` 从源码重新组装本包（改动核心库后执行）。

## 通道与编码

| 通道 | 说明 |
|------|------|
| TCP / WebSocket / KCP / UDP | 四传输，`AtlasClient` 或单 `Channel` 拨号 |
| dual | 业务 TCP + 战斗 KCP/UDP，链式重绑 |

编码：`JsonSerializer`（ver=1，protojson 语义）/ `ProtobufSerializer`（ver=2 二进制）。
DTO 由 `protoc-gen-csharp` 从游戏项目 proto 生成（.pb.cs）。
