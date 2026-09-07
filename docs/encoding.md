# 载荷编码语义说明（C# json = protojson）

> 本文档说明 atlas-sdk-csharp 的载荷编码语义、与 Go/TS SDK 的差异，以及
> 三语言统一路线（D7）的状态。设计决策来源：
> [2026-09-04-client-sdk-csharp-design.md](https://github.com/huangyuCN/atlas/blob/feat/actor/docs/superpowers/specs/2026-09-04-client-sdk-csharp-design.md)
> （D4/D7/D9）。

## 1. C# 编码栈（全 Google.Protobuf 官方实现）

C# SDK 不引入自研序列化器或手写 DTO 生成器，DTO 与编解码全部走 Google 官方
protobuf C# 实现：

| 载荷编码 | 帧头 version | 序列化器 | 实现 |
|---------|-------------|---------|------|
| json（=protojson） | 1 | `Atlas.Serialization.JsonSerializer` | `Google.Protobuf.JsonFormatter` / `JsonParser` |
| protobuf 二进制 | 2 | `Atlas.Serialization.ProtobufSerializer` | `IMessage.ToByteArray` / 静态 `Parser.ParseFrom` |

- **DTO 单一来源**：`protoc-gen-csharp` 产出的 `.pb.cs`（`IMessage`）一套类型，
  同一对象可经 `JsonSerializer`（ver=1）或 `ProtobufSerializer`（ver=2）双编码
  互通——不维护两份 DTO。
- **帧头 version 随序列化器声明**：构造期白名单校验 `{1,2}`，出站帧头 version =
  序列化器声明版本；响应帧版本必须回显一致，否则 `ProtocolException`。
- **nil 请求特判**：`req == null`（如传输心跳 Ping）不序列化、不携带 payload。

## 2. json = protojson 语义（C# 特有）

**C# 的 `JsonSerializer` 就是严格 protojson**（Google 官方实现），不存在「纯
JSON」形态。线上字节即 proto3 JSON 映射：

- 字段名 camelCase（protojson 规则）
- 64 位整数（`int64`/`uint64`）序列化为字符串（避免 JS 侧精度丢失）
- 枚举为字符串名；未知枚举值以数字出现
- `google.protobuf.Timestamp` → RFC3339、`Duration` → `"3.000s"` 形态
- **零值字段省略**（`JsonFormatter.Default` 不输出默认值）
- **反序列化忽略未知字段**（`JsonParser` + `IgnoreUnknownFields`）——对齐 Go
  `DiscardUnknown` 语义：服务端加字段不破坏旧客户端（真机曾因服务端
  `LoginReply` 含跨包 `PlayerSummary` 而验证此路径）

> **protojson 语义无「模式」参数**：C# 的 json 与 protojson 是同一形态
> （冒烟 `SmokeMode.ProtoJson` 与 `Json` 等价，均为 ver=1）。

## 3. 与 Go / TS SDK 的历史差异

三语言沿不同路径演进（Go v0.5 与 TS 先交付、C# 后交付采用官方栈），编码差异
如下——**线上互通无风险**（服务端 protojson 解码器对「零值省略 vs 零值下发」
等价处理，缺失字段与显式零值在 proto3 中同义），但语义细节需知悉：

| 维度 | atlas-sdk-go v0.5 | atlas-sdk-ts | atlas-sdk-csharp |
|------|-------------------|--------------|------------------|
| DTO 来源 | 两套：`atlas sdk gen` plain struct（ver=1）+ `protoc-gen-go` pb.go（ver=2） | 两套：`atlas sdk gen` interface（json）+ 手写动态 schema（protobuf，因 protoc-gen-es v2 bug） | **一套**：`protoc-gen-csharp` pb.cs（双编码共用） |
| ver=1 编码 | 两实现：默认 JSON（Go encoding/json on sdkgen struct）+ `contrib/protojson`（protojson on pb.go） | 手写 JSON（protojson 风格 plain object） | **官方 protojson**（`JsonFormatter`） |
| ver=2 编码 | `contrib/protobuf`（protobuf-go 运行时） | @bufbuild 运行时 + 动态 schema | `Google.Protobuf` 官方运行时 |
| 零值字段 | **下发**（服务端 `EmitUnpopulated` 对齐；sdkgen struct 字段全输出） | 手写对象按属性有无输出 | **省略**（`JsonFormatter.Default`） |
| 未知字段 | 忽略（protojson DiscardUnknown / json 天然忽略） | 手写忽略 | 忽略（`IgnoreUnknownFields`） |
| 64 位整数 | string | string | string（官方保证） |
| 依赖面 | 核心零 protobuf；protojson/protobuf 可选 | 零宿主依赖手写 | 必引 Google.Protobuf（Unity 战斗通道本就需要） |

### 3.1 差异的实质影响

- **零值省略 vs 下发**：C# 客户端发出的 json 请求省略零值字段；Go/TS 可能下发。
  服务端 protojson 解码两者等价（proto3 语义），业务侧**判断「字段是否存在」的
  逻辑不可依赖编码形态**——应以字段值（零值即默认）为准。这与跨语言 SDK 的
  既有约定一致（协议层对「message 字段未设置为 null」的约束不受影响）。
- **DTO 单套 vs 双套**：Go/TS 的 ver=1 DTO 与 ver=2 DTO 是不同类型（业务按编码
  选型引用）；C# 一套 `IMessage` 通吃，业务侧无需按编码维护两套类型。
- **C# 强制 protojson**：Go 默认 JSONSerializer 可序列化任意 plain struct
  （非 proto 类型）；C# 的 `JsonSerializer.Serialize` 入参必须为 `IMessage`。
  游戏项目 proto 驱动下无差异；纯手写 JSON 对象直发在 C# 侧不支持（需先定义
  .proto → 生成 DTO）。

## 4. 统一路线状态（D7：B 分步）

设计决策 D7 确立「三语言统一到官方栈」的分步路线，当前状态：

```
atlas-sdk-csharp  ← 已完成：全 Google.Protobuf 官方栈（DTO 一套、json=protojson）
atlas-sdk-ts      ← 待跟进：手写 json → @bufbuild 官方 toJson；动态 schema → protoc-gen-es
                    （前置：protoc-gen-es v2 在 node 24/26 的 bug 解决）
atlas-sdk-go      ← 最后评估：ver=1 plain struct + sdkgen 是否随官方栈收敛（破坏性变更，后定）
```

- **不强制现有 Go/TS 用户迁移**：已交付形态冻结为历史版本，协议层与线上互通
  不受影响（编码差异见 §3，服务端兼容）。
- **`atlas sdk gen` 维持现状**：Go/TS 后端保留供存量项目；C# 不引入生成器后端
  （DTO 走官方 protoc-gen-csharp），主仓零改动。

## 5. 验证记录

- golden vectors（22 用例，字节级协议一致性）——三语言共享同一向量源
- 真机冒烟（10.10.9.36，TCP 9001 / WS 9002 / KCP 9003 / UDP 9004）：
  业务闭环（注册→登录→心跳）与战斗通道（Ping + JoinBattle 业务拒绝）
  json/protojson/protobuf 全编码验证通过
- `SmokeMode.ProtoJson` 与 `Json` 等价（C# 无独立 protojson 形态）——冒烟矩阵
  中两者同为 ver=1 验证
