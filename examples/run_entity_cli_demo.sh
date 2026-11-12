#!/usr/bin/env bash
# 离线实体匹配：纯 CLI 可复现演示（python -I 隔离模式）。
# 设备名先编码成真实 Avro datum，经 ingest 落进 events；匹配只读 events。
# 用法: bash examples/run_entity_cli_demo.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VAR="$ROOT/var/entity_cli"
REG="$VAR/registry"
DB="$VAR/landing.db"
INV1="$ROOT/examples/entity_inventory/assets_v1.json"
INV2="$ROOT/examples/entity_inventory/assets_v2.json"
CLI=(python3 -I "$ROOT/cli_iso.py")
CONFIRM="送风机->AST-004"
BAN="冷却泵－ＰＡ－１００->AST-001"

rm -rf "$VAR"
mkdir -p "$VAR"

echo "### 0. 生成真实 Avro 二进制批文件（设备名不是手填 events）"
python3 -I "$ROOT/examples/make_entity_batch.py" "$VAR"

echo; echo "### 1. 契约与接入：信号 writer + reader，然后批接收"
"${CLI[@]}" init --registry "$REG" --db "$DB"
"${CLI[@]}" register-writer --registry "$REG" --id w-signal-v1 \
    --file "$ROOT/contracts/writer_signal_v1.json"
"${CLI[@]}" publish-reader --registry "$REG" \
    --file "$ROOT/contracts/reader_signal_v1.json"
"${CLI[@]}" activate-reader --registry "$REG" --version 1
"${CLI[@]}" ingest --registry "$REG" --db "$DB" --batch "$VAR/batch.json"
"${CLI[@]}" events --db "$DB" --partition factory-a | head -40

echo; echo "### 2. 台账校验：共享别名显式暴露；严格模式以退出码 2 拒绝歧义"
"${CLI[@]}" entity-inventory-check --inventory "$INV1"
"${CLI[@]}" entity-inventory-check --inventory "$INV1" --strict-ambiguity \
    || echo "（已按预期以歧义拒绝：共享别名需要人工或全局分数定夺）"

echo; echo "### 3. 主导盘点：min_score=0 margin=10（全角/大小写/无候选/共享别名）"
"${CLI[@]}" entity-match-run --db "$DB" --inventory "$INV1" \
    --min-score 0 --margin 10 --scope factory-a

echo; echo "### 4. 按运行编号解释整份运行（参数、台账摘要、来源、候选、分配）"
"${CLI[@]}" entity-runs --db "$DB"
"${CLI[@]}" entity-run-explain --db "$DB" --run-id 1

echo; echo "### 5. 解释单个设备：候选依据 + 替代解 + 来源坐标"
"${CLI[@]}" entity-explain --db "$DB" --run-id 1 --device "送风机"
"${CLI[@]}" entity-explain --db "$DB" --run-id 1 --device "···"

echo; echo "### 6. margin 端点：min_score 与 margin 的边界写法"
echo "--- margin=9：gap=10 的配对严格大于 margin -> AUTO_CONFIRMED"
"${CLI[@]}" entity-match-run --db "$DB" --inventory "$INV1" \
    --min-score 0 --margin 9 --scope factory-a > "$VAR/run_margin9.json"
echo "--- margin=10：同一 gap 不再严格大于 -> AUTO_PENDING"
"${CLI[@]}" entity-match-run --db "$DB" --inventory "$INV1" \
    --min-score 0 --margin 10 --scope factory-a > "$VAR/run_margin10.json"
python3 - "$VAR" <<'PY'
import json, sys, os
var = sys.argv[1]
rows = {}
for name in ("run_margin9.json", "run_margin10.json"):
    doc = json.load(open(os.path.join(var, name), encoding="utf-8"))
    rows[name] = {d["device"]: d["status"] for d in doc["devices"]}
for device in sorted(rows["run_margin9.json"]):
    a = rows["run_margin9.json"][device]
    b = rows["run_margin10.json"][device]
    if a != b:
        print(f"  {device!r}: margin=9 -> {a}  margin=10 -> {b}")
PY

echo; echo "### 7. 人工确认占用资产 + 人工禁止边（可被 --confirm/--ban 多次给出）"
"${CLI[@]}" entity-match-run --db "$DB" --inventory "$INV1" \
    --min-score 0 --margin 10 --scope factory-a --confirm "$CONFIRM"
"${CLI[@]}" entity-match-run --db "$DB" --inventory "$INV1" \
    --min-score 0 --margin 10 --scope factory-a --ban "$BAN"

echo; echo "### 8. 冲突整份拒绝：退出码 2，且不新增运行记录"
set +e
"${CLI[@]}" entity-match-run --db "$DB" --inventory "$INV1" \
    --min-score 0 --margin 10 --scope factory-a \
    --confirm "$CONFIRM" --confirm "循环风机 ＦＡＮ－１->AST-004" \
    --confirm "空压机三号->AST-006" --ban "空压机三号->AST-006" \
    --confirm "幽灵设备->AST-001"
echo "退出码=$?"
set -e
"${CLI[@]}" entity-runs --db "$DB" | python3 -c \
    "import json,sys;print('运行记录数:', json.load(sys.stdin)['count'])"

echo; echo "### 9. 台账更正后再盘点：新结果 + 旧运行解释仍可查询"
"${CLI[@]}" entity-inventory-check --inventory "$INV2"
"${CLI[@]}" entity-match-run --db "$DB" --inventory "$INV2" \
    --min-score 0 --margin 10 --scope factory-a
"${CLI[@]}" entity-explain --db "$DB" --run-id 1 --device "冷却泵 PA-101X"
"${CLI[@]}" entity-explain --db "$DB" --run-id 2 --device "冷却泵 PA-101X"

echo; echo "### 10. 穷举核对每个运行（分数、替代值、唯一性、约束遵守）"
for run_id in 1 2 3 4 5 6; do
    "${CLI[@]}" entity-run-audit --db "$DB" --run-id "$run_id" | \
        python3 -c "import json,sys;d=json.load(sys.stdin);print('run', $run_id, 'matched=', d['matched'], d.get('problems'))"
done
