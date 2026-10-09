#!/usr/bin/env bash
# gen-dto：从上游生成物/描述符刷新本仓的全部「协议事实」，单一来源、零手写副本。
#
# 输入（只读消费，不在本仓生成协议）：
#   - 框架仓帧协议生成物：$ATLAS_DIR/transport/frame/gen/csharp/{FrameGen,FrameCodec}.cs
#   - 模板仓会话协议：$ATLAS_LAYOUT_DIR/api/gateway/v1/session.proto
#     （session.proto 导入框架仓的 atlas route 注解，故导出 descriptor set 必须
#      同时给两个 include 根）
#   - 框架仓生成器：$ATLAS_DIR/bin/protoc-gen-atlas-client（缺失时用 go build 构建）
# 输出（全部入库，CI 有「重生成无 diff」门禁）：
#   1. src/Atlas/Frame/Gen/FrameGen.cs + FrameCodec.cs        帧协议常量与编解码快照（逐字节复制）
#   2. examples/Smoke/Proto/gen/imessage/*.cs  protoc --csharp_out 的 IMessage DTO
#      （Message 路径：-serializer protobuf 用；含会话协议 + 战斗域契约）
#   3. examples/Smoke/Proto/gen/api/**/opclient/*_client.g.cs 参数化插件的 POCO DTO +
#      会话 stub + 协议描述符（lang=csharp，命名空间注入 Atlas.Client/Atlas.Serialization，
#      根前缀 Atlas；产物自包含——不依赖消费方 ImplicitUsings，也无需生成后改写）
#   4. src/Atlas/Battle/Gen/*.g.cs  战斗/成局推送的 op 常量快照（从插件产物中抽取
#      ProtocolOps/PushOps 类，见 scripts/extract-ops.awk）：op 字面量的唯一来源是
#      生成物，SDK 侧不手写；DTO/stub 不入快照（与 examples 的 IMessage 族同命名空间，
#      同集会撞类型）
#
# 环境变量（默认同级相对路径；CI 用 workspace 绝对路径显式指定）：
#   ATLAS_DIR         框架仓根（默认同级 ../atlas）
#   ATLAS_LAYOUT_DIR  模板仓根（默认同级 ../atlas-game-layout）
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_ROOT="$(pwd)"
ATLAS_DIR="${ATLAS_DIR:-$(cd .. && pwd)/atlas}"
ATLAS_LAYOUT_DIR="${ATLAS_LAYOUT_DIR:-$(cd .. && pwd)/atlas-game-layout}"
for dir in "$ATLAS_DIR" "$ATLAS_LAYOUT_DIR"; do
  if [ ! -d "$dir" ]; then
    echo "gen-dto: 上游仓不存在：$dir（用 ATLAS_DIR / ATLAS_LAYOUT_DIR 指定）" >&2
    exit 1
  fi
done

OUT=examples/Smoke/Proto/gen

# extract_ops 从插件产物抽取 op 常量类，写成带生成头的快照文件。
# 参数：产物文件、期望类名（空格分隔）、生成头（含 namespace）、目标文件。
# 抽取前逐个核对类存在：生成器版式变化即显式失败，不静默产出空快照。
extract_ops() {
  local fragment="$1" wants="$2" header="$3" target="$4"
  local name
  for name in $wants; do
    if ! grep -q "^public static class ${name}\$" "$fragment"; then
      echo "gen-dto: 生成物里找不到 ${name}（${fragment}）：生成器版式变了？" >&2
      exit 1
    fi
  done
  {
    printf '%s\n' "$header"
    awk -v wants="$wants" -f "$REPO_ROOT/scripts/extract-ops.awk" "$fragment"
  } > "$target"
  echo "op 常量快照 → $target"
}

# 1) 帧协议常量与编解码：框架生成物逐字节复制到仓内固定路径（SDK 侧只引用生成物，无手写副本）。
mkdir -p src/Atlas/Frame/Gen
cp "$ATLAS_DIR/transport/frame/gen/csharp/FrameGen.cs" src/Atlas/Frame/Gen/FrameGen.cs
cp "$ATLAS_DIR/transport/frame/gen/csharp/FrameCodec.cs" src/Atlas/Frame/Gen/FrameCodec.cs
echo "帧协议常量/编解码 → src/Atlas/Frame/Gen/{FrameGen,FrameCodec}.cs"

# 2) 会话协议 descriptor set：模板仓导出（两个 include 根：模板仓 + 框架仓注解）。
DESC="$(mktemp -t atlas-gateway-desc.XXXXXX)"
trap 'rm -f "$DESC"' EXIT
protoc --descriptor_set_out="$DESC" --include_imports \
  -I "$ATLAS_LAYOUT_DIR" -I "$ATLAS_DIR" \
  "$ATLAS_LAYOUT_DIR/api/gateway/v1/session.proto" \
  "$ATLAS_LAYOUT_DIR/api/battle/v1/battle_service.proto" \
  "$ATLAS_LAYOUT_DIR/api/game/v1/player_service.proto"
echo "descriptor set → ${DESC}（模板仓导出）"

# 3) 生成器插件：优先用框架仓已构建产物，缺失时现场构建（CI 装了 Go 工具链）。
PLUGIN="${PROTOC_GEN_ATLAS_CLIENT:-$ATLAS_DIR/bin/protoc-gen-atlas-client}"
if [ ! -x "$PLUGIN" ]; then
  (cd "$ATLAS_DIR" && go build -o bin/protoc-gen-atlas-client ./cmd/protoc-gen-atlas-client)
fi

# 4) 生成（固定文件清单 = 会话协议 + common；route/descriptor 属 import 闭包噪声，随后清理）。
rm -rf "$OUT"
mkdir -p "$OUT/imessage"
# IMessage DTO（Message 路径）：会话协议 + common + 战斗域契约（examples 的
# kcp/udp 通道 JoinBattle 探针用）+ route 注解（session/battle 的 service option
# 在生成代码里引用它的 C# 类型）；google/protobuf/empty.proto 由运行时提供，不生成。
protoc --descriptor_set_in="$DESC" --csharp_out="$OUT/imessage" \
  api/gateway/v1/session.proto \
  api/common/v1/common.proto \
  api/battle/v1/battle_service.proto \
  api/battle/v1/battle.proto \
  api/lockstep/lockstep.proto \
  api/atlas/v1/route.proto
protoc --descriptor_set_in="$DESC" \
  --plugin=protoc-gen-atlas-client="$PLUGIN" \
  --atlas-client_out="$OUT" \
  --atlas-client_opt=lang=csharp,csharp_client_namespace=Atlas.Client,csharp_serialization_namespace=Atlas.Serialization,csharp_namespace_prefix=Atlas,paths=source_relative \
  api/gateway/v1/session.proto \
  api/common/v1/common.proto
# 插件按依赖闭包出文件（route 注解与 google descriptor 只为解析依赖而入闭包），
# 本仓不消费它们的 DTO：生成后删除，避免死代码入库。
rm -rf "$OUT/api/atlas" "$OUT/google"
echo "会话 DTO/stub → $OUT"

# 5) 战斗/成局 op 常量快照：战斗域（JoinBattle/SendFrameInput/SyncFrames/Ping +
#    FrameBroadcast/BattleEndNotify/PlayerOutNotify）与成局推送（MatchStartedNotify）。
#    产物出到临时目录，只抽取 op 常量类入库（DTO/stub 与 examples 的会话生成物无关，
#    也不进 SDK：同命名空间的 IMessage 族只在 examples/tests 编译）。
OPS_DIR="$(mktemp -d)"
trap 'rm -f "$DESC"; rm -rf "$OPS_DIR"' EXIT
protoc --descriptor_set_in="$DESC" \
  --plugin=protoc-gen-atlas-client="$PLUGIN" \
  --atlas-client_out="$OPS_DIR" \
  --atlas-client_opt=lang=csharp,csharp_client_namespace=Atlas.Client,csharp_serialization_namespace=Atlas.Serialization,csharp_namespace_prefix=Atlas,paths=source_relative \
  api/battle/v1/battle_service.proto \
  api/game/v1/player_service.proto
mkdir -p src/Atlas/Battle/Gen
extract_ops "$OPS_DIR/api/battle/v1/opclient/battle_service_client.g.cs" \
  "BattleServiceProtocolOps BattleServicePushOps" \
  "// <auto-generated>
// 由 scripts/gen-dto.sh 从 protoc-gen-atlas-client 产物抽取（DO NOT EDIT）。
// source: api/battle/v1/battle_service.proto
// 只保留 op 常量类：战斗域请求 op 与推送 op 的字面量唯一来源，SDK 侧不手写。

namespace Atlas.Battle.V1;
" \
  src/Atlas/Battle/Gen/BattleServiceOps.g.cs
extract_ops "$OPS_DIR/api/game/v1/opclient/player_service_client.g.cs" \
  "PlayerServicePushOps" \
  "// <auto-generated>
// 由 scripts/gen-dto.sh 从 protoc-gen-atlas-client 产物抽取（DO NOT EDIT）。
// source: api/game/v1/player_service.proto
// 只保留推送 op 常量类：成局通知（MatchStartedNotify）的下发 op 唯一来源。

namespace Atlas.Game.V1;
" \
  src/Atlas/Battle/Gen/PlayerServiceOps.g.cs

# 改动计数排除 CI 检出的上游目录（它们不是本仓产物，见 workflow 门禁同款 pathspec）。
echo "生成完成（重跑本脚本应无 diff；当前工作区改动：$(git -C "$REPO_ROOT" status --porcelain -- . ':!atlas' ':!atlas-game-layout' | wc -l | tr -d ' ') 处）"
