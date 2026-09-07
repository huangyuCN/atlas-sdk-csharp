#!/usr/bin/env bash
# 组装 UPM 包（com.huangyucn.atlas）：dotnet publish 产 self-contained dll，
# 过滤出 Atlas/Atlas.Unity 与第三方运行时依赖（Google.Protobuf/KcpSharp 及
# netstandard2.1 补充库），拷入 packages/com.huangyucn.atlas/Plugins/。
# 纯 BCL System.*（Unity 引擎内置 .NET Standard 2.1 API）不随包——避免与 Unity
# 内置实现冲突。
set -euo pipefail
cd "$(dirname "$0")/.."

PKG=packages/com.huangyucn.atlas
PLUGINS="$PKG/Plugins"
mkdir -p "$PLUGINS"

# Release publish 两库（Atlas.Unity 引用 Atlas，一次 publish 带出依赖）。
dotnet publish src/Atlas.Unity/Atlas.Unity.csproj -c Release -o /tmp/atlas-upm-pub --no-restore >/dev/null

# 保留清单：自研两库 + 第三方运行时依赖（Unity 6 内置其余 System.*）。
# Microsoft.Bcl.HashCode 是 KcpSharp(netstandard2.0) 的独立依赖程序集，非
# Unity 内置 System.HashCode 类型，必须随包（评审 M5-2-P1：遗漏则 Unity 加载
# KcpSharp 时 FileNotFoundException）。
KEEP='^Atlas\.dll$|^Atlas\.Unity\.dll$|^Google\.Protobuf\.dll$|^KcpSharp\.dll$|^Microsoft\.Bcl\.AsyncInterfaces\.dll$|^Microsoft\.Bcl\.HashCode\.dll$|^System\.Threading\.Tasks\.Extensions\.dll$'

copied=0
for f in /tmp/atlas-upm-pub/*.dll; do
  name=$(basename "$f")
  if echo "$name" | grep -qE "$KEEP"; then
    cp "$f" "$PLUGINS/"
    copied=$((copied + 1))
  fi
done
echo "UPM Plugins 就绪：$copied 个 dll → $PLUGINS"
ls "$PLUGINS"
