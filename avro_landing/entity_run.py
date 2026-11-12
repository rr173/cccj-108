"""运行编排：一致读视图 -> 构造计划 -> 单事务保存 -> 查询解释。

一致读视图
----------
``events`` 的 device 唯一值、运行编号与台账快照必须来自同一个读快照。
实现方式是在**同一个 SQLite 连接**上显式开启 ``BEGIN`` 事务（非
``BEGIN IMMEDIATE``，不升级写锁），读取完成后 COMMIT；随后的写入在另一个
``BEGIN IMMEDIATE`` 事务中完成。读事务存在期间，同一连接看不到其他连接
之后提交的数据，也不会看到本进程中途写入的数据。

盘点输入有任何错误时抛出 PlanRejected，事务回滚，不产生半份运行记录。
"""

from __future__ import annotations

import sqlite3
from typing import Iterable, Optional

from .entity_match import (
    HumanConstraints,
    MatchParams,
    MatchPlan,
    PlanRejected,
    plan_matching,
    read_device_sources,
)
from .entity_names import Inventory, MatchInputError, load_inventory
from .entity_store import MatchRunStore
from .store import EventStore, utcnow


def _parse_override(raw: str) -> tuple[str, str]:
    if "->" not in raw:
        raise PlanRejected([{
            "code": "INVALID_OVERRIDE_FORMAT",
            "detail": f"人工限制格式应为 DEVICE->ASSET_ID，收到 {raw!r}",
        }])
    device, _, asset_id = raw.partition("->")
    return device.strip(), asset_id.strip()


def parse_human(
    confirm: Iterable[str] = (), ban: Iterable[str] = (),
) -> HumanConstraints:
    confirmations = [_parse_override(item) for item in confirm]
    bans = [_parse_override(item) for item in ban]
    return HumanConstraints(confirmations=confirmations, bans=bans)


def run_matching(
    event_store: EventStore,
    *,
    inventory: Inventory,
    params: MatchParams,
    human: HumanConstraints,
    scope: Optional[str] = None,
    partitions: Optional[Iterable[str]] = None,
) -> dict:
    """执行一次盘点并保存运行记录，返回运行摘要。"""
    params.validate()

    # --- 1. 一致读视图：device 唯一值 + 已运行编号 ---
    conn = event_store.connect()
    match_store = MatchRunStore(event_store)
    conn.execute("BEGIN")
    try:
        devices = read_device_sources(conn, scope=scope, partitions=partitions)
        previous_runs = match_store.existing_run_ids(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    # --- 2. 校验并求解（内存中完成；任何错误整份拒绝） ---
    try:
        plan = plan_matching(
            device_inputs=devices,
            assets=inventory.assets,
            params=params,
            human=human,
            scope=scope,
        )
    except PlanRejected:
        raise

    # --- 3. 单事务保存：运行、来源、候选、人工限制、分配、替代解 ---
    with event_store.transaction() as conn2:
        run_id = match_store.save_run(conn2, plan, inventory)
    return {
        "ok": True,
        "run_id": run_id,
        "previous_run_ids": previous_runs,
        "plan": plan,
        "inventory": inventory,
    }


def run_matching_from_paths(
    db_path: str,
    inventory_path: str,
    *,
    min_score: int,
    margin: int,
    confirm: Iterable[str] = (),
    ban: Iterable[str] = (),
    scope: Optional[str] = None,
    partitions: Optional[Iterable[str]] = None,
) -> dict:
    inventory = load_inventory(inventory_path)
    human = parse_human(confirm, ban)
    params = MatchParams(min_score=min_score, margin=margin)
    event_store = EventStore(db_path)
    try:
        result = run_matching(
            event_store, inventory=inventory, params=params,
            human=human, scope=scope, partitions=partitions,
        )
    finally:
        event_store.close()
    return result


def _require_run(match_store: MatchRunStore, run_id: int) -> dict:
    run = match_store.get_run(run_id)
    if run is None:
        raise KeyError(run_id)
    return run


def explain_run(
    match_store: MatchRunStore, run_id: int, device: Optional[str] = None,
) -> dict:
    run = _require_run(match_store, run_id)
    out: dict = {
        "ok": True,
        "run": {
            "run_id": run["run_id"],
            "created_at": run["created_at"],
            "scope": run["scope"],
            "params": {"min_score": run["min_score"], "margin": run["margin"]},
            "inventory": {
                "path": run["inventory_path"],
                "sha256": run["inventory_sha256"],
                "revision": run["inventory_revision"],
                "asset_count": run["asset_count"],
                "snapshot": run["inventory_snapshot"],
            },
            "device_count": run["device_count"],
            "optimal_total": run["optimal_total"],
            "equivalent_optima": run["equivalent_optima"],
        },
        "human_constraints": match_store.run_constraints(run_id),
        "assignment": {
            "total": run["optimal_total"],
            "equivalent_optima": run["equivalent_optima"],
            "pairs": run["assignment"].get("pairs", {}),
            "unmatched_devices": run["assignment"].get("unmatched_devices", []),
            "unmatched_assets": run["assignment"].get("unmatched_assets", []),
            "uniqueness_note": run["assignment"].get("uniqueness_note"),
        },
        "decisions": {
            "confirmed": sum(1 for d in match_store.run_devices(run_id)
                             if d["decision"] == "CONFIRMED"),
            "pending": sum(1 for d in match_store.run_devices(run_id)
                           if d["decision"] == "PENDING"),
            "unmatched": sum(1 for d in match_store.run_devices(run_id)
                             if d["decision"] == "UNMATCHED"),
        },
    }
    if device is None:
        out["devices"] = match_store.run_devices(run_id)
        return out

    item = match_store.run_device(run_id, device)
    if item is None:
        out["ok"] = False
        out["error"] = f"运行 {run_id} 中没有设备 {device!r}"
        return out
    out["device"] = item
    out["sources"] = match_store.run_sources(run_id, device)
    out["candidates"] = match_store.run_candidates(run_id, device)
    out["alternatives"] = match_store.run_alternatives(run_id, device)
    return out


def explain_device(match_store: MatchRunStore, device: str) -> dict:
    """不指定运行编号时，解释该设备在所有运行中的记录（新的在前）。"""
    runs = match_store.list_runs()
    entries = []
    for run in reversed(runs):
        item = match_store.run_device(run["run_id"], device)
        if item is None:
            continue
        entries.append({
            "run_id": run["run_id"],
            "created_at": run["created_at"],
            "inventory_revision": run["inventory_revision"],
            "params": {"min_score": run["min_score"], "margin": run["margin"]},
            "device": item,
            "candidates": match_store.run_candidates(run["run_id"], device),
            "alternatives": match_store.run_alternatives(run["run_id"], device),
        })
    return {
        "ok": bool(entries),
        "device": device,
        "run_count": len(entries),
        "runs": entries,
        "note": (
            "每次运行保存自己的台账快照；旧运行的解释不随台账更正改变"
            if entries else "该设备在已保存的运行中没有记录"
        ),
    }
