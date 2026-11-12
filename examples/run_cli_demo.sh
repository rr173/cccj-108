#!/usr/bin/env bash
# CLI 端到端可复现演示（隔离模式 python -I）。
# 用法: bash examples/run_cli_demo.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VAR="$ROOT/var/cli_demo"
REG="$VAR/registry"
DB="$VAR/landing.db"
CLI=(python3 -I "$ROOT/cli_iso.py")

rm -rf "$VAR"
mkdir -p "$VAR"

echo "### 0. 生成真实 Avro 二进制批文件"
python3 -I "$ROOT/examples/make_cli_batch.py"

echo; echo "### 1. 初始化目录与数据库"
"${CLI[@]}" init --registry "$REG" --db "$DB"

echo; echo "### 2. 注册不可变 writer schema"
"${CLI[@]}" register-writer --registry "$REG" --id w-event-v1 --file "$ROOT/contracts/writer_v1.json"
"${CLI[@]}" register-writer --registry "$REG" --id w-event-v2 --file "$ROOT/contracts/writer_v2.json"

echo; echo "### 3. 发布 reader v1 前的兼容预检（w-event-v2 long->int 应被门禁拦截）"
"${CLI[@]}" publish-reader --registry "$REG" --file "$ROOT/contracts/reader_v1.json" || true
echo "--- 加 --force 仅落盘不激活（演示门禁拦截后仍可人工决策）"
"${CLI[@]}" publish-reader --registry "$REG" --file "$ROOT/contracts/reader_v1.json" --force

echo; echo "### 4. 显式契约检查：w-event-v2 -> reader v1 必须失败"
"${CLI[@]}" check-compat --registry "$REG" --writer w-event-v2 --reader 1 || true

echo; echo "### 5. 发布并激活 reader v2（预检全绿）"
"${CLI[@]}" publish-reader --registry "$REG" --file "$ROOT/contracts/reader_v2.json"
"${CLI[@]}" activate-reader --registry "$REG" --version 2

echo; echo "### 6. 批接收（含缺口 offset=3、未知 ID、坏字节）"
"${CLI[@]}" ingest --registry "$REG" --db "$DB" --batch "$VAR/batch.json"

echo; echo "### 7. 查看隔离原因（原始字节以 hex 保留）"
"${CLI[@]}" quarantine --db "$DB"

echo; echo "### 8. 未知 ID 后补注册，再按范围再处理 offset 5..6"
"${CLI[@]}" register-writer --registry "$REG" --id w-ghost --file "$ROOT/contracts/writer_v1.json"
"${CLI[@]}" reprocess --registry "$REG" --db "$DB" --partition orders --from-offset 5 --to-offset 6
"${CLI[@]}" attempts --db "$DB" --partition orders --offset 5

echo; echo "### 9. 业务表与最终确认位置"
"${CLI[@]}" events --db "$DB" --partition orders

echo; echo "### 10. 崩溃注入：模拟进程在业务写入与确认之间退出（独立新分区）"
"${CLI[@]}" ingest --registry "$REG" --db "$DB" --batch "$VAR/batch_crash.json" \
    --crash-before-commit || true
echo "--- 崩溃后查询：crash-demo 分区不应有任何部分写入"
"${CLI[@]}" events --db "$DB" --partition crash-demo
"${CLI[@]}" quarantine --db "$DB" --partition crash-demo

echo; echo "### 11. 进程重启后重传相同字节：幂等重做，接收结果与确认位置共同恢复"
"${CLI[@]}" ingest --registry "$REG" --db "$DB" --batch "$VAR/batch_crash.json"
"${CLI[@]}" events --db "$DB" --partition crash-demo

echo; echo "### 12. 冲突表（本演示无冲突，留作查询命令示例）"
"${CLI[@]}" conflicts --db "$DB"
