#!/usr/bin/env bash
# golden 目录定位：ATLAS_GOLDEN_DIR 优先，否则猜相邻 atlas 主仓。
set -euo pipefail

if [ -n "${ATLAS_GOLDEN_DIR:-}" ]; then
  echo "$ATLAS_GOLDEN_DIR"
  exit 0
fi

for directory in ../atlas/testdata/golden ../../atlas/testdata/golden; do
  if [ -d "$directory" ]; then
    cd "$directory"
    pwd
    exit 0
  fi
done

echo "找不到 golden 目录（设 ATLAS_GOLDEN_DIR）" >&2
exit 1
