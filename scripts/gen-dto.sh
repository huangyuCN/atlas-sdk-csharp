#!/usr/bin/env bash
# gen-dto：从上游生成物/描述符刷新本仓的全部「协议事实」，单一来源、零手写副本。
#
# **契约消息边界**：本脚本的产出清单 = SDK 持有的协议面。游戏域消息（battle / game / matcher
# 的 DTO、枚举与 op 表）**不在本仓生成**——它们已迁到游戏协议包
# （模板仓 protocol/，NuGet HuangyuCN.AtlasGame.Protocol），SDK 不依赖、不生成、不引用。
# 谁再往这里加游戏域生成项，「无 diff」门禁就会把它们生成回 SDK（见
# tests/Atlas.Tests/Battle/BattleGameDomainBoundaryTest.cs 的边界守卫）。
#
# 输入（只读消费，不在本仓生成协议）：
#   - 框架仓帧协议生成物：$ATLAS_DIR/transport/frame/gen/csharp/{FrameGen,FrameCodec}.cs
#   - 模板仓会话协议：$ATLAS_LAYOUT_DIR/api/gateway/v1/session.proto
#     （session.proto 导入框架仓的 atlas route 注解，故导出 descriptor set 必须
#      同时给两个 include 根）
#   - 框架仓契约消息 lockstep：$ATLAS_DIR/api/lockstep/lockstep.proto（不再被会话消息
#     import，必须显式列进 descriptor set 与 csharp_out 才会继续产出 Lockstep.cs）
#   - 框架仓生成器：$ATLAS_DIR/bin/protoc-gen-atlas-client（缺失时用 go build 构建）
# 输出（全部入库，CI 有「重生成无 diff」门禁）：
#   1. src/Atlas/Frame/Gen/FrameGen.cs + FrameCodec.cs        帧协议常量与编解码快照（逐字节复制）
#   2. examples/Smoke/Proto/gen/imessage/*.cs  protoc --csharp_out 的 IMessage DTO
#      （Message 路径：-serializer protobuf 用；只含**契约消息**：会话 + common + lockstep + route）
#   3. examples/Smoke/Proto/gen/api/**/opclient/*_client.g.cs 参数化插件的 POCO DTO +
#      会话 stub + 协议描述符（lang=csharp，命名空间注入 Atlas.Client/Atlas.Serialization，
#      根前缀 Atlas；产物自包含——不依赖消费方 ImplicitUsings，也无需生成后改写）
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

# 1) 帧协议常量与编解码：框架生成物逐字节复制到仓内固定路径（SDK 侧只引用生成物，无手写副本）。
mkdir -p src/Atlas/Frame/Gen
cp "$ATLAS_DIR/transport/frame/gen/csharp/FrameGen.cs" src/Atlas/Frame/Gen/FrameGen.cs
cp "$ATLAS_DIR/transport/frame/gen/csharp/FrameCodec.cs" src/Atlas/Frame/Gen/FrameCodec.cs
echo "帧协议常量/编解码 → src/Atlas/Frame/Gen/{FrameGen,FrameCodec}.cs"

# 2) 契约消息 descriptor set：模板仓会话协议 + 框架仓 lockstep（两个 include 根）。
DESC="$(mktemp -t atlas-gateway-desc.XXXXXX)"
trap 'rm -f "$DESC"' EXIT
protoc --descriptor_set_out="$DESC" --include_imports \
  -I "$ATLAS_LAYOUT_DIR" -I "$ATLAS_DIR" \
  "$ATLAS_LAYOUT_DIR/api/gateway/v1/session.proto" \
  "$ATLAS_DIR/api/lockstep/lockstep.proto"
echo "descriptor set → ${DESC}（模板仓会话 + 框架仓 lockstep）"

# 3) 生成器插件：优先用框架仓已构建产物，缺失时现场构建（CI 装了 Go 工具链）。
PLUGIN="${PROTOC_GEN_ATLAS_CLIENT:-$ATLAS_DIR/bin/protoc-gen-atlas-client}"
if [ ! -x "$PLUGIN" ]; then
  (cd "$ATLAS_DIR" && go build -o bin/protoc-gen-atlas-client ./cmd/protoc-gen-atlas-client)
fi

# 4) 生成（固定文件清单 = 会话协议 + common + 契约消息 lockstep；route/descriptor 属 import
#    闭包噪声，随后清理）。
rm -rf "$OUT"
mkdir -p "$OUT/imessage"
# IMessage DTO（Message 路径）：会话协议 + common + lockstep 契约消息 + route 注解
#（session 的 service option 在生成代码里引用它的 C# 类型）；google/protobuf/*.proto 由运行时提供，
# 不生成。
protoc --descriptor_set_in="$DESC" --csharp_out="$OUT/imessage" \
  api/gateway/v1/session.proto \
  api/common/v1/common.proto \
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

# 改动计数排除 CI 检出的上游目录（它们不是本仓产物，见 workflow 门禁同款 pathspec）。
echo "生成完成（重跑本脚本应无 diff；当前工作区改动：$(git -C "$REPO_ROOT" status --porcelain -- . ':!atlas' ':!atlas-game-layout' | wc -l | tr -d ' ') 处）"
