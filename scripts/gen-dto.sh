#!/usr/bin/env bash
# protoc-gen-csharp 生成 pb.cs（对标 Go examples/smoke/gatewayv1 的 protoc --go_out）。
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=examples/Smoke/Proto/gen
mkdir -p "$OUT"
protoc --csharp_out="$OUT" --proto_path=examples/Smoke/Proto \
  examples/Smoke/Proto/gatewayv1/auth.proto
echo "生成完成 → $OUT"
