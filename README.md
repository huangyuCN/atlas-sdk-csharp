# atlas-sdk-csharp

[![CI](https://github.com/huangyuCN/atlas-sdk-csharp/actions/workflows/ci.yml/badge.svg)](https://github.com/huangyuCN/atlas-sdk-csharp/actions/workflows/ci.yml)
[![Unity](https://img.shields.io/badge/Unity-6000.0%2B-000000?logo=unity&logoColor=white)](https://unity.com)
[![.NET](https://img.shields.io/badge/.NET-Standard%202.1-512BD4?logo=.net&logoColor=white)](https://dotnet.microsoft.com)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Atlas 帧协议的 C# 客户端 SDK（**Unity 优先**）。用于游戏客户端、机器人、压测脚本连接
[Atlas](https://github.com/huangyuCN/atlas) 游戏服务端，提供开箱即用的长连接能力：
**请求-响应匹配、服务端推送订阅、双层心跳、断线自动重连、dual 双通道编排**。

核心库为纯托管 .NET Standard 2.1（零 UnityEngine 依赖），可被 Unity
（Mono/IL2CPP）引用，也可被 .NET 服务器工具与压测脚本复用。

> **当前状态**：协议层、运行时内核（请求匹配、推送订阅、双层心跳、断线重连、
> dual 双通道编排）与四通道传输全部可用；真机四通道 × 三编码冒烟通过（详见
> [docs/unity-verify.md](docs/unity-verify.md)）。Unity 编辑器真机挂载验证待验
> （本机 Unity 许可证失效，见 [docs/unity-verify.md](docs/unity-verify.md) §4）。

支持四种传输通道，与 Atlas 网关的通道形态一一对应：

| 通道 | 典型用途 | 拨号 |
|------|---------|------|
| TCP | 业务通道（登录 / 会话 / 匹配） | `TcpTransport.ConnectAsync` |
| WebSocket | 浏览器形态的单通道业务+战斗 | `WsTransport.ConnectAsync` |
| KCP | 战斗通道（可靠 UDP，低延迟） | `KcpTransport.ConnectAsync` |
| UDP | 战斗通道（低延迟，尽力而为） | `UdpTransport.ConnectAsync` |
| TCP/WS + KCP/UDP 组合 | dual 形态：业务 + 战斗双通道 | `AtlasClient` + 双 `ChannelConfig` |

## 特性

- **四通道矩阵**：TCP（流式分帧）、WebSocket（一条消息 = 一个完整帧）、KCP
  （KcpSharp，服务端 kcp-go 会话参数对齐：明文无 FEC、NoDelay=false、
  interval=40ms、窗口 128、MTU 1400）、UDP（一报一帧，单数据报上限 64KiB 含帧头，
  坏数据报静默丢弃）。KCP/UDP 无连接关闭通知，死链由传输心跳发现；KCP 写路径带
  死链超时兜底（10s，对齐 Go `kcpWriteTimeout`）。
- **dual 双通道编排**：业务 + 战斗通道各自独立连接、心跳、重连与请求排队；
  业务重登成功后 SDK 自动链式触发战斗通道重新绑定（Join 语义）；任一通道拨号
  失败整体回滚。
- **请求-响应匹配**：`seq` 单调递增 + 按连接代次（epoch）隔离匹配；超时、迟到
  响应静默丢弃、断连统一失败——全部路径恰好一次投递（含排队看护、drain 重发
  的 CAS 认领互斥）。
- **服务端推送（Notify）**：按 operation 分发到订阅者，handler 默认线程池执行
  （可注入 `SynchronizationContext` 发布到 Unity 主线程）、单 handler 异常隔离、
  支持幂等注册与退订。
- **双层心跳**：传输保活（周期 Ping，只保活连接、不续租业务会话）+ 可选的
  **会话心跳**（仅业务通道）：按业务协议周期续租会话，会话过期触发重登钩子。
  传输心跳业务拒绝不计死链（往返完成 = 链路存活）；仅网络类失败计死链（默认
  连续 3 次触发重连），按代精确匹配不误杀新连接。
- **断线自动重连**：指数退避（500ms 起 ×2 封顶 30s，带抖动）；重连期间请求排队
  （默认 64 条，排队期限计入超时，到点超时失败）；重连成功后执行会话重登钩子
  （hookBypass 直通窗口：钩子内重登/重绑请求直通当前代连接，窗口上限 10s）。
- **结构化错误**：业务拒绝还原为 `BusinessException`（`Code/Reason/Message/
  Metadata`）；网络、超时、协议错误各自成类；`IsBusinessError(ex, reason)`
  沿 `InnerException` 链查找（对齐 Go `errors.As`）。
- **协议一致性**：字节级 golden vectors（22 用例，含截断、非法头、64 位整数等
  边界）逐用例校验；向量源在 atlas 主仓 `testdata/golden/`（规范与向量同仓），
  各语言 SDK 消费同一份向量，行为跨语言一致。
- **载荷编码协商**：帧头 `version` 为协商位——ver=1（protojson JSON）/
  ver=2（protobuf 二进制 wire）。DTO 一律 `protoc-gen-csharp` 产出，同一套
  消息类型经 `JsonSerializer` 或 `ProtobufSerializer` 双编码互通（详见
  [docs/encoding.md](docs/encoding.md)）。

## 安装

### NuGet（.NET 项目 / 压测 / 服务器工具）

```bash
dotnet add package Atlas.Sdk
```

要求 .NET Standard 2.1 兼容宿主（.NET 6+ / Unity 6000.0+）。包携带
Google.Protobuf、KcpSharp 等依赖。

### UPM（Unity 工程）

`Packages/manifest.json` 加 git URL 依赖（仓库公开后可用）：

```json
{
  "dependencies": {
    "com.huangyucn.atlas": "https://github.com/huangyuCN/atlas-sdk-csharp.git?path=packages/com.huangyucn.atlas"
  }
}
```

包内 `Plugins/` 已预编译全部依赖（Atlas.dll + Google.Protobuf + KcpSharp 等
7 个 dll），安装即用、无需本地编译。本地安装也可在 Package Manager 里
`Add package from disk` 指向 `packages/com.huangyucn.atlas/`。

## 快速开始

### DTO 生成（protoc-gen-csharp）

DTO 为 protoc 官方 C# 产物（`IMessage`），一个 `.proto` 对应一个 `.pb.cs`：

```bash
protoc --csharp_out=./Gen api/gateway/v1/auth.proto
```

> ver=1 的线上 JSON 由 `JsonSerializer`（Google.Protobuf protojson）直接序列化
> 消息对象——字段名 camelCase、64 位整数为字符串等规则由官方实现保证，无需
> 手写 DTO 或 json tag。

### 连接、请求与推送（TCP）

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Serialization;
using Atlas.Transport;
using Gateway.V1;

// 1) 拨号：构造 Channel（dial 工厂 + 选项），Connect 后即可收发。
var channel = new Channel(
    ct => TcpTransport.ConnectAsync("127.0.0.1", 9001, ct),
    new ChannelOptions
    {
        HeartbeatIntervalMs = 30_000,   // 传输保活 Ping
        InvokeTimeoutMs = 10_000,
        Serializer = new JsonSerializer(), // ver=1 protojson（默认）
    });
await channel.ConnectAsync(CancellationToken.None);

// 2) 订阅服务端推送（handler 默认线程池执行；退订句柄 Dispose）。
var sub = channel.On("/gateway.v1.GatewayAuth/Notify", (op, payload) =>
    Console.WriteLine($"收到推送: {op} {payload.Length} bytes"));
sub.Dispose(); // 退订

// 3) 请求-响应：DTO 为 protoc 生成的 IMessage，经 Serializer 自动编解码。
var serializer = new JsonSerializer();
var request = new LoginRequest { PlayerId = "p1", Password = "***" };
var respBytes = await channel.InvokeRawAsync(
    "/gateway.v1.GatewayAuth/Login",
    serializer.Serialize(request),
    CancellationToken.None);
var reply = (LoginReply)serializer.Deserialize(respBytes, typeof(LoginReply));
Console.WriteLine($"登录成功: {reply.PlayerId}");

// 4) 优雅关闭（取消 in-flight、停心跳与读循环；幂等）。
await channel.CloseAsync();
```

错误处理：`InvokeRawAsync` 抛四类异常——`BusinessException`（业务拒绝，按
`Reason` 分支，用 `IsBusinessError(ex, "REASON")` 判定）/ `NetworkException` /
`TimeoutException` / `ProtocolException`。

### dual 双通道（业务 TCP + 战斗 KCP）

业务通道承载登录/会话，战斗通道承载高频帧输入，两通道独立心跳与重连。
`AtlasClient` 自动链式编排：**业务重登成功后自动触发战斗重绑**；战斗通道自身
断线重连时仅重绑。

```csharp
var client = new AtlasClient(
    new ChannelConfig(ChannelKind.Business, ct =>
        TcpTransport.ConnectAsync("127.0.0.1", 9001, ct))
    {
        Options = new ChannelOptions { /* 业务心跳、重登钩子等 */ },
        ReconnectHook = async () => await ReloginAsync(),   // 重连后重登
    },
    new ChannelConfig(ChannelKind.Battle, ct =>
        KcpTransport.ConnectAsync("127.0.0.1", 9003, ct))
    {
        Options = new ChannelOptions { InvokeTimeoutMs = 1_000 }, // 战斗短超时
        ReconnectHook = async () => await JoinBattleAsync(),      // 战斗重绑
    });
await client.ConnectAsync(CancellationToken.None);

var business = client.Channel(ChannelKind.Business); // 业务通道视图
var battle = client.Channel(ChannelKind.Battle);     // 战斗通道视图
// 业务/战斗独立请求与订阅；State 聚合向下降级（任一通道非 Connected 即降级）。
```

## 编码语义（json = protojson）

C# SDK 全 Google.Protobuf 官方栈：DTO 为 `protoc-gen-csharp` 产出的 `IMessage`，
**一套消息类型同时服务 ver=1（protojson JSON）与 ver=2（protobuf 二进制）**。
`JsonSerializer` 即严格 protojson（`JsonFormatter`/`JsonParser`）——与 Go/TS SDK
的编码差异见 [docs/encoding.md](docs/encoding.md)。

## 配置项（ChannelOptions）

| 属性 | 默认 | 说明 |
|------|------|------|
| `HeartbeatIntervalMs` | 30_000 | 传输心跳周期；连续 3 次失败判定死链；`≤0` 关闭 |
| `InvokeTimeoutMs` | 10_000 | 请求默认超时（排队期限亦计入） |
| `MaxBodySize` | 2MiB | 单帧 body 上限（需与服务端对齐） |
| `Serializer` | `JsonSerializer` | 序列化插槽（ver=1 protojson / ver=2 protobuf） |
| `AutoReconnect` | true | 断线自动重连开关 |
| `BackoffBaseMs`/`BackoffMaxMs` | 500/30_000 | 重连退避参数（×2 封顶 + 抖动） |
| `QueueSize` | 64 | 重连期间请求排队上限（满后立即失败） |
| `HookTimeoutMs` | 10_000 | 重连钩子（重登/重绑）窗口上限 |
| `HeartbeatFailures` | 3 | 心跳死链判定阈值 |

`ChannelConfig`：`Kind`（`Business`/`Battle`）、`Dial`（拨号工厂）、`Options`
（本通道配置）、`ReconnectHook`（本通道重连后钩子，dual 下链式编排）。

## 兼容性

当前代码与 atlas 服务端 `feat/actor` 分支（golden manifest 锁定）的帧协议对齐，
由 22 个字节级 golden 用例校验（向量源在
[atlas](https://github.com/huangyuCN/atlas) 主仓 `testdata/golden/`，协议单点）。
服务端协议变更时向量随之更新，保证行为变更可查。

## 测试与开发

```bash
dotnet build Atlas.sln              # 构建
dotnet test Atlas.sln               # 全量测试（含 golden vectors）
bash scripts/gen-dto.sh             # 重新生成冒烟 DTO（需 protoc 26.1）
bash scripts/package-upm.sh         # 重新组装 UPM 包（Plugins dll）
dotnet pack src/Atlas/Atlas.csproj  # 产出 NuGet 包
```

> golden vectors 在 atlas 主仓 `testdata/golden/`。本地测试默认读取与本仓同级的
> `../atlas/testdata/golden`，或用环境变量 `ATLAS_GOLDEN_DIR` 指定（CI 检出
> 主仓后指向该目录）。

## 文档

- [编码语义说明（json=protojson，与 Go/TS 差异）](docs/encoding.md)
- [Unity 挂载验证报告（Mono/IL2CPP/WebGL 待验项）](docs/unity-verify.md)
