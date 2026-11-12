#!/usr/bin/env python3
"""离线实体匹配演示：真实 Avro 接入链路 + 全局分配 + 唯一性复核。

所有设备名都先以真实 Avro 二进制（``writer_signal_v1``）走接入链路落进
``events``，匹配阶段只从 ``events`` 的 device 唯一值读取；演示不手填 events。

覆盖人工容易判断错的场景：

1. 贪心选择比全局分配差（同一份清单内，逐个设备挑最像会少拿分）
2. 两套最优方案同分：相关设备被留给人工，不冒充唯一解
3. 别名在不同资产间重名：显式暴露歧义，按全局分数与人工确认落位
4. 人工确认占用资产后，剩余设备重新分配
5. 禁止一条边后最优解改变
6. 无候选（含清理后空串）与 margin 端点（gap == margin 不是严格大于）
7. 台账更正后新一次盘点结果变化，旧运行的解释仍可查询
8. 小规模样例用穷举实现核对生产求解器的分数、唯一性与约束遵守

用法：
    python3 -I run_entity_demo.py
演示目录：./var/entity_demo（每次重跑会清空）。
"""

from __future__ import annotations

import base64
import io
import json
import os
import shutil
import site
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
_user_site = site.getusersitepackages()
if os.path.isdir(_user_site) and _user_site not in sys.path:
    sys.path.insert(0, _user_site)

import fastavro  # noqa: E402

from avro_landing.entity_audit import audit_run  # noqa: E402
from avro_landing.entity_store import MatchRunStore  # noqa: E402
from avro_landing.store import EventStore  # noqa: E402

DEMO = os.path.join(ROOT, "var", "entity_demo")
REG = os.path.join(DEMO, "registry")
DB = os.path.join(DEMO, "landing.db")
SAMPLES = os.path.join(ROOT, "examples", "entity_inventory")
CLI = [sys.executable, "-I", os.path.join(ROOT, "cli_iso.py")]

W_SIGNAL = os.path.join(ROOT, "contracts", "writer_signal_v1.json")
READER = os.path.join(ROOT, "contracts", "reader_signal_v1.json")


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def load(name: str) -> dict:
    with open(name, encoding="utf-8") as fh:
        return json.load(fh)


def encode(schema: dict, record: dict) -> str:
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, fastavro.parse_schema(schema), record)
    return base64.b64encode(bio.getvalue()).decode("ascii")


def cli(*argv: str) -> int:
    print("$", " ".join(str(a) for a in argv))
    code = subprocess.run(CLI + [str(a) for a in argv], cwd=ROOT).returncode
    if code != 0:
        print(f"[退出码 {code}]")
    return code


def write_batch(name: str, packets: list[dict]) -> str:
    path = os.path.join(DEMO, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"packets": packets}, fh, ensure_ascii=False, indent=2)
    return path


def signal_packet(partition: str, offset: int, w_sig: dict, *,
                  device: str, tag: str, event_ms: int,
                  kind: str = "TEMPERATURE", value: float = 90.0) -> dict:
    return {
        "partition": partition, "offset": offset, "writer_schema_id": "w-signal-v1",
        "encoding": "base64",
        "datum": encode(w_sig, {
            "event_id": tag, "device": device, "event_ms": event_ms,
            "signal_kind": kind, "value": value,
            "source": {"channel": "plant-bus", "ref": None},
        }),
    }


def land_devices(partition: str, devices: list[str], start_offset: int,
                 w_sig: dict) -> int:
    """把设备名作为真实信号走接入链路落地；返回下一个可用 offset。"""
    packets = []
    offset = start_offset
    for device in devices:
        packets.append(signal_packet(
            partition, offset, w_sig, device=device,
            tag=f"{partition}-{offset}", event_ms=offset * 1000 + 1000,
        ))
        offset += 1
    cli("ingest", "--registry", REG, "--db", DB,
        "--batch", write_batch(f"batch-{partition}-{start_offset}.json", packets))
    return offset


def run_match(inventory: str, *, min_score: int, margin: int,
              scope: str, confirm: list[str] | None = None,
              ban: list[str] | None = None) -> str:
    """运行一次盘点，从 CLI 输出里取 run_id（保持 CLI 是唯一入口）。"""
    argv = ["entity-match-run", "--db", DB, "--inventory", inventory,
            "--min-score", str(min_score), "--margin", str(margin),
            "--scope", scope]
    for item in confirm or []:
        argv += ["--confirm", item]
    for item in ban or []:
        argv += ["--ban", item]
    out = subprocess.run(CLI + argv, cwd=ROOT, capture_output=True, text=True)
    print("$", " ".join(argv))
    if out.returncode != 0:
        print(out.stdout)
        raise SystemExit(out.returncode)
    doc = json.loads(out.stdout)
    print(f"  运行编号={doc['run_id']}  台账版本={doc['inventory']['revision']}"
          f"  min_score={doc['params']['min_score']}  margin={doc['params']['margin']}")
    print(f"  设备数={doc['device_count']}  全局总分={doc['optimal_total']}  "
          f"同分方案={doc['equivalent_optima']}")
    for item in doc["devices"]:
        state = item["status"]
        if item["matched_asset_id"]:
            detail = f"-> {item['matched_asset_id']} 分={item['score']}"
            if item["gap"] is not None:
                alt = item["alternative_asset_ids"]
                detail += f"  替代={alt or '无'}  S_alt={item['alternative_total']}  gap={item['gap']}"
            print(f"  {item['device']!r:26} {state:17} {detail}")
        else:
            print(f"  {item['device']!r:26} {state:17} 原因={item['reason']}")
    return str(doc["run_id"])


def audit(run_id: str) -> None:
    code = cli("entity-run-audit", "--db", DB, "--run-id", run_id)
    print("穷举核对退出码:", code, "（0 = 分数/唯一性/约束全部一致）")


def main() -> int:
    if os.path.isdir(DEMO):
        shutil.rmtree(DEMO)
    os.makedirs(DEMO)
    w_sig = load(W_SIGNAL)

    banner("步骤 0：契约与接入准备（设备名只经 Avro 接入链路进入 events）")
    cli("init", "--registry", REG, "--db", DB)
    cli("register-writer", "--registry", REG, "--id", "w-signal-v1",
        "--file", W_SIGNAL)
    cli("publish-reader", "--registry", REG, "--file", READER)
    cli("activate-reader", "--registry", REG, "--version", "1")
    print("台账校验（共享别名应被显式暴露，不按文件顺序归属）：")
    cli("entity-inventory-check", "--inventory",
        os.path.join(SAMPLES, "assets_v1.json"))

    # ------------------------------------------------------------------ #
    banner("步骤 1：主导盘点（v1 台账）—— 全角/大小写/分隔符、共享别名、无候选")
    print("送风机同时是 AST-004 与 AST-005 的别名；匹配只看全局总分，")
    print("同分时按（设备名，资产编号）稳定展示，并以等价方案数暴露不唯一。")
    devices = [
        "冷却泵－ＰＡ－１００",   # 全角 + 全角连字符：清理后与 AST-001 正式名完全一致
        "冷却泵 PA-101X",       # 与 AST-002 差 1
        "冷却泵 PA-100B",       # 与 AST-001 差 1，可能被更优设备挤掉
        "冷却泵 PA-900",        # 与 AST-007 差 1
        "冷却泵 PA-900X",       # 只有 AST-007 可配，但比 PA-900 差
        "循环风机 ＦＡＮ－１",   # 全角：AST-004 正式名一致
        "送风机",               # AST-004 / AST-005 共享别名
        "循环泵",               # AST-003 别名
        "空压机 XZ-77",         # AST-008 正式名一致
        "空压机三号",           # AST-006 别名
        "···",                  # 清理后空串：无效名称
        "冷却泵 XZ-77",         # 无任何距离 <= 2 的候选
    ]
    land_devices("factory-a", devices, 0, w_sig)
    v1_run = run_match(os.path.join(SAMPLES, "assets_v1.json"),
                       min_score=0, margin=10, scope="factory-a")
    audit(v1_run)

    # ------------------------------------------------------------------ #
    banner("步骤 2：贪心比全局差（专门的对照样本）")
    print("两台采集标识都落在同一个资产附近：『冷却泵 PA-100』与 AG-001 完全一致（100 分），")
    print("『冷却泵 PA-100X』与 AG-001 差 1（90 分），且它配不到 AG-002。")
    print("贪心先满足单边分数 100 的那台，另一台落空、总分只有 100；")
    print("全局分配会把 100 分那台换到 AG-002（80 分），换来 90 分那台配上 AG-001，总分 170。")
    greedy_devices = ["冷却泵 PA-100", "冷却泵 PA-100X"]
    land_devices("greedy-line", greedy_devices, 0, w_sig)
    greedy_run = run_match(os.path.join(SAMPLES, "greedy_v1.json"),
                           min_score=0, margin=0, scope="greedy-line")
    store = EventStore(DB)
    match_store = MatchRunStore(store)
    run = match_store.get_run(int(greedy_run))
    rows = match_store.run_devices(int(greedy_run))
    scores = {(c["device"], c["asset_id"]): c["score"]
              for c in match_store.run_candidates(int(greedy_run))}
    used_assets: set[str] = set()
    greedy_pairs: dict[str, str] = {}
    greedy_total = 0
    ordered = sorted(
        ((score, device, asset)
         for (device, asset), score in scores.items()),
        key=lambda item: (-item[0], item[1], item[2]),
    )
    for score, device, asset in ordered:
        if device in greedy_pairs or asset in used_assets:
            continue
        greedy_pairs[device] = asset
        used_assets.add(asset)
        greedy_total += score
    print(f"贪心（按单边分数从高到低）总分 = {greedy_total}，配对 = "
          f"{ {d: a for d, a in sorted(greedy_pairs.items())} }")
    print(f"全局最优总分 = {run['optimal_total']}，配对 = "
          f"{run['assignment']['pairs']}")
    assert greedy_total < run["optimal_total"], "对照样本应当让贪心更差"
    audit(greedy_run)

    # ------------------------------------------------------------------ #
    banner("步骤 3：两套最优方案同分 —— 相关设备留给人工")
    print("『送风机』与『送风机 』清理后完全相同（尾随空格被去掉），都是两个")
    print("资产 95 分的别名；两套最优方案总分同为 190，同分方案数 = 2。")
    print("按 margin=10，gap=0 不严格大于 margin，相关设备写成 AUTO_PENDING，")
    print("并展示原选择、替代选择与 S/S_alt；排序只用于展示，不冒充唯一解。")
    tie_devices = ["送风机", "送风机 "]
    land_devices("tie-line", tie_devices, 0, w_sig)
    tie_run = run_match(os.path.join(SAMPLES, "assets_v1.json"),
                         min_score=0, margin=10, scope="tie-line")
    audit(tie_run)

    # ------------------------------------------------------------------ #
    banner("步骤 4：人工确认占用资产后，剩余设备重新分配")
    print("人工确认 送风机 -> AST-004（越过 min_score 门槛也允许，但必须有候选依据），")
    print("AST-004 随即被占用，同样可配 AST-004 的设备改配 AST-005。")
    confirm_run = run_match(
        os.path.join(SAMPLES, "assets_v1.json"),
        min_score=0, margin=10, scope="factory-a",
        confirm=["送风机->AST-004"],
    )
    audit(confirm_run)

    # ------------------------------------------------------------------ #
    banner("步骤 5：禁止一条边后最优解改变（所有求解都遵守，含替代解）")
    print("禁止 冷却泵－ＰＡ－１００ -> AST-001 后，它改配次优资产，")
    print("原本被挤掉的设备重新进入分配。")
    ban_run = run_match(
        os.path.join(SAMPLES, "assets_v1.json"),
        min_score=0, margin=10, scope="factory-a",
        ban=["冷却泵－ＰＡ－１００->AST-001"],
    )
    audit(ban_run)

    # ------------------------------------------------------------------ #
    banner("步骤 6：margin 端点 —— gap 必须严格大于 margin")
    print("同一份 v1 盘点分别用 margin=9 与 margin=10：gap=10 的配对在 9 时已确认，")
    print("在 10 时变成待确认；gap=0 的配对两个端点都待确认。")
    m9 = run_match(os.path.join(SAMPLES, "assets_v1.json"),
                   min_score=0, margin=9, scope="factory-a")
    m10 = run_match(os.path.join(SAMPLES, "assets_v1.json"),
                    min_score=0, margin=10, scope="factory-a")
    store = EventStore(DB)
    rows9 = {r["device"]: r for r in MatchRunStore(store).run_devices(int(m9))}
    store.close()
    store = EventStore(DB)
    rows10 = {r["device"]: r for r in MatchRunStore(store).run_devices(int(m10))}
    store.close()
    for device, row in sorted(rows9.items()):
        if row["gap"] is None:
            continue
        other = rows10[device]
        if row["status"] != other["status"]:
            print(f"  {device!r}: margin=9 -> {row['status']} (gap={row['gap']})，"
                  f"margin=10 -> {other['status']}")

    # ------------------------------------------------------------------ #
    banner("步骤 7：台账更正后新一次盘点 —— 旧运行解释仍可查询")
    print("v2 台账把 AST-002 更正为『冷却泵 PX-900』、AST-006 增加别名 AC-1、")
    print("AST-007 改为『冷却泵 PB-200』并删除其备用名，新增 AST-009。")
    print("新运行按新快照重算；旧运行的解释读的是它自己的台账快照。")
    cli("entity-inventory-check", "--inventory",
        os.path.join(SAMPLES, "assets_v2.json"))
    # 台账更正后新增两台设备（同样走 Avro 接入链路），再跑新一次盘点
    correction_devices = ["冷却泵 PA-100E", "冷却泵 PA-101E"]
    land_devices("factory-a", correction_devices, 20, w_sig)
    v2_run = run_match(os.path.join(SAMPLES, "assets_v2.json"),
                       min_score=0, margin=10, scope="factory-a")
    audit(v2_run)
    print("\n台账更正带来的变化（只摘录相关设备）：")
    store = EventStore(DB)
    match_store = MatchRunStore(store)
    old_rows = {r["device"]: r for r in match_store.run_devices(int(v1_run))}
    new_rows = {r["device"]: r for r in match_store.run_devices(int(v2_run))}
    store.close()
    for device in ("冷却泵 PA-100E", "冷却泵 PA-101X", "冷却泵 PA-101E"):
        before = old_rows.get(device)
        after = new_rows.get(device)
        if after is None:
            continue
        print(f"  {device!r}: 旧台账="
              f"{(before or {}).get('matched_asset_id') or (before or {}).get('reason') or '未参与'}"
              f"  新台账={after['matched_asset_id'] or after['reason']}")
    print(f"\n旧运行 #{v1_run} 中『冷却泵 PA-101X』的解释（台账仍是 v1 快照，")
    print("所以看不到 v2 才加入的 AST-001 别名 PA-100E）：")
    cli("entity-explain", "--db", DB, "--run-id", v1_run,
        "--device", "冷却泵 PA-101X")
    print(f"\n新运行 #{v2_run} 中同一设备的解释：")
    cli("entity-explain", "--db", DB, "--run-id", v2_run,
        "--device", "冷却泵 PA-101X")
    print("\n按设备查看全部历史运行（v2 在前）：")
    cli("entity-explain", "--db", DB, "--device", "冷却泵 PA-100E")

    # ------------------------------------------------------------------ #
    banner("步骤 8：错误盘点整份拒绝 —— 不产生半份结果")
    print("两条确认争用同一资产 + 同一配对又确认又禁止 + 引用不存在的设备：")
    before = len(MatchRunStore(EventStore(DB)).list_runs())
    code = cli("entity-match-run", "--db", DB,
               "--inventory", os.path.join(SAMPLES, "assets_v1.json"),
               "--min-score", "0", "--margin", "10", "--scope", "factory-a",
               "--confirm", "送风机->AST-004",
               "--confirm", "循环风机 ＦＡＮ－１->AST-004",
               "--confirm", "空压机三号->AST-006",
               "--ban", "空压机三号->AST-006",
               "--confirm", "幽灵设备->AST-001")
    after = len(MatchRunStore(EventStore(DB)).list_runs())
    print(f"退出码={code}；运行记录数 {before} -> {after}（应保持不变）")
    print("\n非法台账（重复 asset_id + 清理后空别名）同样整份拒绝：")
    bad_inventory = os.path.join(DEMO, "bad_inventory.json")
    with open(bad_inventory, "w", encoding="utf-8") as fh:
        json.dump({"assets": [
            {"asset_id": "X-1", "name": "冷却泵 PA-100", "aliases": ["···"]},
            {"asset_id": "X-1", "name": "重复编号"},
        ]}, fh, ensure_ascii=False)
    cli("entity-inventory-check", "--inventory", bad_inventory)

    banner("完成")
    print("演示数据库:", DB)
    print("命令：entity-inventory-check / entity-match-run / entity-runs /")
    print("      entity-explain / entity-run-explain / entity-run-audit")
    print("所有运行均可用 entity-run-explain --run-id N 复查原始参数、")
    print("台账摘要、来源坐标、候选依据、人工限制、全局分配与替代解。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
