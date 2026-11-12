#!/usr/bin/env bash
# 设备联动异常识别器：纯 CLI 可复现演示（python -I 隔离模式）。
# 用法: bash examples/run_linkage_cli_demo.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VAR="$ROOT/var/linkage_cli"
REG="$VAR/registry"
DB="$VAR/landing.db"
CLI=(python3 -I "$ROOT/cli_iso.py")
CONFIG="factory-a,factory-b"

rm -rf "$VAR"
mkdir -p "$VAR"

echo "### 0. 生成真实 Avro 二进制批文件（低压先到、业务时间在后）"
python3 -I "$ROOT/examples/make_linkage_batch.py"

echo; echo "### 1. 初始化 + 注册信号 writer + 发布/激活信号 reader"
"${CLI[@]}" init --registry "$REG" --db "$DB"
"${CLI[@]}" register-writer --registry "$REG" --id w-signal-v1 \
    --file "$ROOT/contracts/writer_signal_v1.json"
"${CLI[@]}" register-writer --registry "$REG" --id w-event-v1 \
    --file "$ROOT/contracts/writer_v1.json"
"${CLI[@]}" publish-reader --registry "$REG" \
    --file "$ROOT/contracts/reader_signal_v1.json"
"${CLI[@]}" activate-reader --registry "$REG" --version 1

echo; echo "### 2. 批接收（全部走 Avro 接入链路；普通消息与坏值也先 LANDED）"
"${CLI[@]}" ingest --registry "$REG" --db "$DB" --batch "$VAR/batch.json"

echo; echo "### 3. 推进：只声明 factory-a 的 W——factory-b 未声明，阻止结算"
"${CLI[@]}" linkage-advance --db "$DB" --config "$CONFIG" \
    --watermark factory-a=610000
"${CLI[@]}" linkage-waiting --db "$DB" --config "$CONFIG"
"${CLI[@]}" linkage-sources --db "$DB" --config "$CONFIG" --state SKIPPED
"${CLI[@]}" linkage-sources --db "$DB" --config "$CONFIG" --state REJECTED
"${CLI[@]}" linkage-decisions --db "$DB" --config "$CONFIG"

echo; echo "### 4. factory-b 也声明 W=610000：全局进度等于右端，仍不结算"
"${CLI[@]}" linkage-advance --db "$DB" --config "$CONFIG" \
    --watermark factory-b=610000
"${CLI[@]}" linkage-waiting --db "$DB" --config "$CONFIG"

echo; echo "### 5. 严格大于右端 (610001) 才结算 —— dev-backfill 与 dev-cross 都是 HIT"
"${CLI[@]}" linkage-advance --db "$DB" --config "$CONFIG" \
    --watermark factory-a=610001 --watermark factory-b=610001
"${CLI[@]}" linkage-decisions --db "$DB" --config "$CONFIG" \
    --partition factory-a --offset 1
"${CLI[@]}" linkage-decisions --db "$DB" --config "$CONFIG" \
    --partition factory-b --offset 0

echo; echo "### 6. 水位倒退必须报错（退出码 2），旧水位保持不变"
"${CLI[@]}" linkage-advance --db "$DB" --config "$CONFIG" \
    --watermark factory-a=610000 || echo "（已按预期拒绝）"
"${CLI[@]}" linkage-watermarks --db "$DB" --config "$CONFIG"

echo; echo "### 7. 迟到信号旁路：业务时间早于所属分区旧水位"
# 用现有批文件的同一 device，业务时间 500 早于 factory-a 的 W=610001
python3 - "$ROOT" "$VAR" <<'PY'
import base64, io, json, sys
root, var = sys.argv[1], sys.argv[2]
sys.path.insert(0, root)
import site, os
_us = site.getusersitepackages()
if os.path.isdir(_us): sys.path.insert(0, _us)
import fastavro
w = json.load(open(os.path.join(root, "contracts", "writer_signal_v1.json")))
rec = {"event_id": "late-reset", "device": "dev-backfill", "event_ms": 500,
       "signal_kind": "RESET", "value": None,
       "source": {"channel": "plant-bus", "ref": None}}
bio = io.BytesIO()
fastavro.schemaless_writer(bio, fastavro.parse_schema(w), rec)
doc = {"packets": [{"partition": "factory-a", "offset": 4,
                    "writer_schema_id": "w-signal-v1", "encoding": "base64",
                    "datum": base64.b64encode(bio.getvalue()).decode()}]}
json.dump(doc, open(os.path.join(var, "batch_late.json"), "w"))
PY
"${CLI[@]}" ingest --registry "$REG" --db "$DB" --batch "$VAR/batch_late.json"
"${CLI[@]}" linkage-advance --db "$DB" --config "$CONFIG"
"${CLI[@]}" linkage-sources --db "$DB" --config "$CONFIG" --state LATE_BYPASS

echo; echo "### 8. 独立全量窗口枚举核对结论与来源依据（不匹配退出码 1）"
"${CLI[@]}" linkage-audit --db "$DB" --config "$CONFIG"

echo; echo "### 9. 崩溃注入：写入部分最终结论后、COMMIT 前退出（退出码 3，事务回滚）"
"${CLI[@]}" linkage-advance --db "$DB" --config "$CONFIG" \
    --watermark factory-a=2000001 --watermark factory-b=2000001 \
    --crash-after-decision || echo "（已按预期模拟退出）"
"${CLI[@]}" linkage-watermarks --db "$DB" --config "$CONFIG"
"${CLI[@]}" linkage-decisions --db "$DB" --config "$CONFIG"
echo "--- 重启后重新推进同一批：幂等，不重复不漏报"
"${CLI[@]}" linkage-advance --db "$DB" --config "$CONFIG" \
    --watermark factory-a=2000001 --watermark factory-b=2000001
"${CLI[@]}" linkage-audit --db "$DB" --config "$CONFIG"
