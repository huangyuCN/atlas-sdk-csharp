# Unity 挂载验证报告

> 日期：2026-09-04
> 任务：M5-3（Unity 工程挂载验证——UPM 包 `com.huangyucn.atlas` 可用性）
> 结论：**降级验证**——UPM 包 dll 编译面 + 运行时全链路验证通过；Unity 编辑器/IL2CPP 真机验证因本机许可证失效为**已知待验项**。

## 1. Unity 编辑器验证尝试（受阻）

按设计规格 §6/§8.3，M5-3 原计划在 Unity 编辑器模式（Mono）跑通 TCP mock 闭环，验证
UPM 包 dll 在 Unity 宿主可用。尝试步骤：

1. 建最小 Unity 工程骨架（`Unity -batchmode -quit -createProject`）；
2. 结果：**Unity 启动失败——许可证失效**。

```
[Licensing::Client] Error: Code 500 while processing request (status: Unable to update licenses. Errors: No ULF license found.,Token not found in cache)
[Licensing::Client] Error: Code 404 while processing request (status: Found 0 entitlement groups and 0 free entitlements ...)
No valid Unity Editor license found. Please activate your license.
```

根因：本机 Unity 6000.0.32f1 安装的授权为 2026-03-25 签发的 `UnityEntitlementLicense.xml`
（含 `com.unity.editor.headless` entitlement），但许可证服务端会话 token 失效、无有效 ULF，
batch 模式无法激活。GUI 登录激活不在本任务可执行范围（需交互式 Unity Hub 登录）。

> 裁决：本环境 Unity batch 不可行属**硬环境限制**（许可证），非代码问题。按任务约定
> 「Unity 不可跑则降级记录」执行。

## 2. 降级验证：UPM 包 dll 消费验证（等价 Unity 引用方式）

Unity 引用 UPM 包 = 引用 `Plugins/` 下的预编译 dll（`com.huangyucn.atlas` 包结构）。
降级验证用纯 .NET 工程**直接引用 UPM 包 Plugins dll**（与 Unity 加载方式一致），
验证两层：

### 2.1 dll 编译面 + 类型加载（Unity 脚本编译期等价）

- `AtlasClient` / `JsonSerializer` / `ChannelConfig` 类型可加载；
- Google.Protobuf 依赖（`Plugins/Google.Protobuf.dll`）解析 OK；
- 无 `FileNotFoundException` / `TypeLoadException`（M5-2 P1 修复的
  `Microsoft.Bcl.HashCode.dll` 缺失问题已不存在——7 dll 齐备）。

### 2.2 运行时全链路 mock 闭环（Unity 脚本运行期等价）

用 UPM 包 dll 起真实 TCP mock 网关（回显服务端），客户端经 Atlas SDK 跑：

1. `TcpTransport.ConnectAsync` 建立连接 ✅
2. `InvokeRawAsync(Register, json)` 请求-响应往返（帧编解码 + seq 匹配）✅
3. `/atlas.internal.Heartbeat/Ping` 空响应往返 ✅
4. 网关确认收到全部 op（按序）✅

```
[UPM 验证] TCP 连接建立 OK
[UPM 验证] Invoke 往返 OK，echo 一致=True
[UPM 验证] Ping 往返 OK，空响应 len=0
[UPM 验证] 网关收到 ops: /gateway.v1.GatewayAuth/Register,/atlas.internal.Heartbeat/Ping
[UPM 验证] mock 闭环通过（TCP 连接 + Invoke + Ping 全链路）
```

这证明 UPM 包产物（`Atlas.dll` + 7 个 vendored 依赖 dll）**编译面与运行面均可用**，
Unity 编辑器脚本引用后将执行同一套 API 路径。

## 3. IL2CPP / WebGL 状态

| 项 | 状态 | 说明 |
|----|------|------|
| Unity 编辑器（Mono）mock 闭环 | ⏸ 待验 | 许可证失效无法跑 batch；UPM dll 消费验证已覆盖等价 API 面 |
| IL2CPP 构建冒烟 | ⏸ 待验 | 需可用 Unity 许可证；静态生成消息（protoc-gen-csharp 产物）+ 反射 Parser 路径需真机确认 |
| WebGL WS 垫片 | ⏸ 待验 | `Atlas.Unity.WebSocketPad` 为运行期标记占位，接线待 Unity 可用后验证 |
| JsonSerializer 反射 Parser | ⚠️ 已知风险 | IL2CPP AOT 裁剪需 `link.xml` 保留或类型注册表（设计规格 §9 记录，M5 验证项） |

## 4. 后续（Unity 许可证可用时）

在激活 Unity 的环境中执行（本机或 CI 独立 job）：

```bash
# 建工程 + UPM 本地 path 引用
Unity -batchmode -quit -createProject <tmp-unity-proj>
# manifest.json dependencies 加 "com.huangyucn.atlas": "file:../atlas-sdk-csharp/packages/com.huangyucn.atlas"
# 编辑器脚本连 mock 网关跑闭环
Unity -batchmode -quit -projectPath <tmp-unity-proj> -executeMethod AtlasVerify.Run
```

预期验证：Mono 编辑器 mock 闭环 → IL2CPP Android/macOS 构建冒烟 → WebGL WS single 形态。

## 5. 验证证据留存

降级验证脚本在系统临时目录（不入库）：`/tmp/atlas-upm-consumer/Consumer/`——
纯 .NET 工程直接引用 `packages/com.huangyucn.atlas/Plugins/*.dll`，跑通 mock 闭环。
仓库内无新增 Unity 工程（避免 Library/ 等构建产物污染）。
