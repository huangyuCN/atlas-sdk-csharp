# 更新日志

本文件记录 Atlas C# SDK 的版本变更。该 SDK 有两条分发线，**版本号同号**：

- NuGet 包 `HuangyuCN.Atlas.Sdk`（.NET 项目 / 压测 / 服务器工具）
- UPM 包 `com.huangyucn.atlas`（Unity 工程）

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [0.7.0] - 2026-10-09

版本线统一：NuGet（`HuangyuCN.Atlas.Sdk`）与 UPM（`com.huangyucn.atlas`）同号为 **0.7.0**。
包名由 `Atlas.Sdk` 改为 `HuangyuCN.Atlas.Sdk`——前者已被第三方占用（nuget.org 作者
`Atlas`，认证平台 SDK，push 必 403）；新名与 TS / Go / UPM 三条线同一 `huangyuCN` 品牌域。

### 破坏性变更（BREAKING）

- **对局结束语义收口**（`2f50d3e`）：终态下 `JoinBattle` / `SendFrameInput` /
  `SyncFrames` / `Connect` / `Reconnect` 改抛专用 `BattleEndedException`，
  **不再抛 `InvalidOperationException`**——后者语义是编程错误，无法与「对局正常结束」
  区分。检查在任何组帧/写线之前；终态停心跳并拒绝拨号。
- **`BattleSessionOptions` 新增配置项**（`f94e6fa` / `2f50d3e`）：
  `HeartbeatInterval`（直连保活心跳，缺省 2s，`Zero` = 关闭，负值装配期拒绝；
  与传输心跳 `HeartbeatIntervalMs` 明确区分）与 `EndDrainWindow`
  （收尾窗口，缺省 2s，`0` = 立即关闭）。
- **推送出口统一**（`ea02df2`）：统一入口 `OnAny` + `Push` 事件 + `OnPush(op)` 订阅 +
  `PlayerOut`；op 常量全部改引生成物，仓内零手写 op 字面量。按旧出口订阅的调用方需迁移。
- **生成物跟随模板 proto 重新生成**（`4659537`）：DTO/stub 按传输面地址调整、
  新增 `PlayerOutNotify`、`migration` 相关变更；消费生成 DTO 的调用方需同步重生成。

### 新增

- **直连保活心跳**（`f94e6fa`）：连接就绪起表，每拍发 `BattleService/Ping`
  （Tell + 逐帧票槽），与业务发帧共用写锁互不干扰；失败只 Warn + `HeartbeatFailed`
  事件，**绝不终止会话**；重连窗口静默跳过；`CloseAsync` / 票废终止先停表并 await
  退出（幂等、无悬挂定时器）。
- **对局结束语义**（`2f50d3e`）：`HasEnded` / `EndedReason`
  （`BATTLE_ENDED` = 服务端拒绝、`BATTLE_END_NOTIFY` = 推送先到）；四处触发——
  帧回包 reason、探针被拒、结束通知推送、重连钩子被拒；结束事件幂等（事件闩与终态闩
  分离，重复投递仅比对载荷，不一致记 Warn、首个为准）。
- **只读观测**（`ea02df2`）：`Stats` 改为只读快照；新增 `IsBattleEnded` /
  `IsNotFound` / `IsFull` / `IsFrameTargetMismatch` / `IsTerminalFailure` 判定族。
- **跨机三面 E2E 驱动**（`f94e6fa`）：`examples/E2E` 跨机三面闭环 + 保活验收驱动
  （`ATLAS_E2E_SERVER` 门控，零新增包依赖）。

### 修复

- **并发 `ReconnectAsync`**（`ea02df2`）：`_connectGate` 串行 + `_reconnectRound`
  单飞（后到者复用本轮）+ 只关「本次持有」的通道（`DetachChannel` CAS）。
- **重试有界**（`ea02df2`）：重连钩子加 `HookMaxAttempts`（缺省 10）；
  非票类业务拒绝终态化（重连钩子与前台调用两处），`Failed` 恰一次。
- **终态零写线**（`ea02df2`）：`ChannelOptions.WriteGuard` 在写锁内（组帧与写线之间）
  复核终态，杜绝终态后仍有字节上线。
- **心跳三分类**（`ea02df2`）：终态类入终态 / 票类 `TicketRejected` 信号不终态、
  日志仅首次 / 其余计数 + `HeartbeatFailed`；终态在途请求立即以终态 Status 结算。
- **陈旧代读循环**（`ea02df2`）：`Channel.ReadLoop` 增 `IsCurrentGeneration`
  ——陈旧代读循环会凭空拉起新一轮重连，使「重试有界」失效。
- metadata 键统一为 `x-atlas-sdk-local-settled`。

### 测试与门禁

- 用例数 270 → 278（`f94e6fa`）→ 289（`2f50d3e`）→ **315**（`ea02df2`，评审修复新增用例）。
- `dotnet build` 0 警告；`dotnet format --verify-no-changes` 0。

## 0.7.0 之前

0.7.0 之前为仓内开发阶段，**未发布**到 NuGet / UPM（无对应 git tag）。
