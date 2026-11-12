"""交叉核对：用小规模穷举另一套实现，核对生产求解器。

本模块刻意不复用 :mod:`avro_landing.entity_solver` 的增广算法：

- 小规模（设备 <= 9、资产 <= 12）用**递归枚举全部合法一对一配对**（含不配对）
  直接求出最优总分；
- 替代值用同一套穷举，分别按"禁掉当前选择 / 强制必须配上"重算；
- 唯一性用穷举出的同分方案计数判断；
- 约束遵守情况逐条复核（一对一、人工确认占用、人工禁止、分数门槛）。

生产运行的解释查询后也可以调用 ``audit_run``，把运行记录里的候选与人工
限制重新读出来做同一套核对。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from .entity_match import (
    DECISION_CONFIRMED,
    DECISION_PENDING,
    DECISION_UNMATCHED,
    STATUS_AUTO_CONFIRMED,
    STATUS_AUTO_PENDING,
    STATUS_HUMAN_CONFIRMED,
    STATUS_UNMATCHED,
)


MAX_DEVICES = 12
MAX_ASSETS = 12
# 穷举节点预算：超出后标记"未穷举核对"，避免在规模过大时卡死
NODE_BUDGET = 2_000_000


class ExhaustiveBudgetExceeded(Exception):
    pass


def _candidates_from_rows(rows: list[dict]) -> dict[tuple[str, str], int]:
    return {(row["device"], row["asset_id"]): row["score"] for row in rows}


def _score_edges(
    candidates: dict[tuple[str, str], int],
    min_score: int,
    confirmations: dict[str, str],
    bans: set[tuple[str, str]],
) -> dict[tuple[str, str], int]:
    edges: dict[tuple[str, str], int] = {}
    for (device, asset_id), score in candidates.items():
        if (device, asset_id) in bans:
            continue
        if confirmations.get(device) == asset_id:
            edges[(device, asset_id)] = score
            continue
        if score >= min_score:
            edges[(device, asset_id)] = score
    return edges


def enumerate_best(
    devices: Iterable[str],
    assets: Iterable[str],
    edges: dict[tuple[str, str], int],
    *,
    pinned: Optional[dict[str, str]] = None,
    forbidden: Optional[Iterable[tuple[str, str]]] = None,
    required: Iterable[str] = (),
    count_equivalents: bool = False,
) -> tuple[Optional[int], Optional[dict[str, str]], int]:
    """递归枚举全部合法配对。

    返回 (最优总分或 None 表示不可行, 一个最优方案, 同分方案数)。
    """
    devices = sorted(devices)
    assets = sorted(assets)
    pinned = dict(pinned or {})
    forbidden_set = set(forbidden or ())
    required_set = {d for d in required if d not in pinned}

    usable = {
        (d, a): s for (d, a), s in edges.items()
        if s > 0 and (d, a) not in forbidden_set
    }
    fixed_total = sum(usable.get((d, a), 0) for d, a in pinned.items())
    remaining_devices = [d for d in devices if d not in pinned]
    occupied = set(pinned.values())
    remaining_assets = [a for a in assets if a not in occupied]
    remaining_edges = {
        (d, a): s for (d, a), s in usable.items()
        if d in remaining_devices and a in remaining_assets
    }

    best_total: Optional[int] = None
    best_pairs: Optional[dict[str, str]] = None
    best_count = 0
    nodes = 0

    def walk(index: int, used: frozenset[str], total: int, pairs: dict[str, str],
             matched_required: set[str]) -> None:
        nonlocal best_total, best_pairs, best_count, nodes
        nodes += 1
        if nodes > NODE_BUDGET:
            raise ExhaustiveBudgetExceeded(
                f"穷举节点超过预算 {NODE_BUDGET}"
            )
        if index == len(remaining_devices):
            if required_set - matched_required:
                return
            if best_total is None or total > best_total:
                best_total = total
                best_pairs = dict(pairs)
                best_count = 1
            elif total == best_total:
                best_count += 1
            return
        device = remaining_devices[index]
        walk(index + 1, used, total, pairs, matched_required)   # 不配对
        for asset in remaining_assets:
            if asset in used:
                continue
            score = remaining_edges.get((device, asset))
            if score is None:
                continue
            pairs[device] = asset
            walk(index + 1, used | {asset}, total + score, pairs,
                 matched_required | {device})
            del pairs[device]

    walk(0, frozenset(), 0, {}, set())
    if best_total is None:
        return None, None, 0
    merged = dict(pinned)
    merged.update(best_pairs or {})
    total = fixed_total + best_total
    return total, merged, best_count


def audit_run(run: dict, devices: list[dict], candidates: list[dict],
              constraints: list[dict]) -> dict:
    """用穷举核对一次已保存的运行。

    ``run`` 为 :meth:`entity_store.MatchRunStore.get_run` 的返回值，
    ``devices`` 为 ``run_devices``，``candidates`` 为 ``run_candidates``，
    ``constraints`` 为 ``run_constraints``。
    """
    problems: list[str] = []
    notes: list[str] = []
    params = run["params"] if "params" in run else {
        "min_score": run["min_score"], "margin": run["margin"],
    }
    min_score = params["min_score"]
    margin = params["margin"]

    device_ids = sorted({row["device"] for row in devices})
    asset_ids = sorted({asset["asset_id"]
                        for asset in run["inventory_snapshot"]["assets"]})
    candidate_scores = _candidates_from_rows(candidates)

    confirmations = {
        row["device"]: row["asset_id"]
        for row in constraints if row["kind"] == "CONFIRM"
    }
    bans = {(row["device"], row["asset_id"])
            for row in constraints if row["kind"] == "BAN"}

    edges = _score_edges(candidate_scores, min_score, confirmations, bans)
    if len(device_ids) > MAX_DEVICES or len(asset_ids) > MAX_ASSETS:
        return {
            "matched": True,
            "exhaustive": False,
            "skipped_reason": (
                f"规模超出穷举核对范围（设备 {len(device_ids)} / {MAX_DEVICES}，"
                f"资产 {len(asset_ids)} / {MAX_ASSETS}），未做逐方案枚举"
            ),
            "problems": problems,
            "notes": notes,
        }
    try:
        total, pairs, optima = enumerate_best(
            device_ids, asset_ids, edges,
            pinned=confirmations, forbidden=bans, count_equivalents=True,
        )
    except ExhaustiveBudgetExceeded as exc:
        return {
            "matched": True,
            "exhaustive": False,
            "skipped_reason": str(exc),
            "problems": problems,
            "notes": notes,
        }
    if total is None:
        problems.append("穷举没有可行解，但运行已保存")
        return {"matched": False, "problems": problems, "notes": notes}

    if total != run["optimal_total"]:
        problems.append(
            f"最优总分不一致: 运行={run['optimal_total']} 穷举={total}"
        )
    # 求解器最多报告 2（2 表示"至少两个"）；穷举在小规模上可能数更多。
    reported = run["equivalent_optima"]
    if reported < 2 and optima != reported:
        problems.append(
            f"唯一性计数不一致: 运行={reported} 穷举={optima}"
        )

    # --- 逐设备核对 -----------------------------------------------------
    run_pairs = {row["device"]: row["matched_asset_id"]
                 for row in devices if row["matched_asset_id"] is not None}
    run_total = sum(row["score"] for row in devices)
    if run_total != total:
        problems.append(f"运行配对分数合计 {run_total} 与穷举最优 {total} 不一致")

    for row in devices:
        device = row["device"]
        if device in confirmations:
            if row["matched_asset_id"] != confirmations[device]:
                problems.append(
                    f"人工确认 {device!r} 未按其确认资产落地"
                )
            if row["decision"] != DECISION_CONFIRMED:
                problems.append(f"人工确认 {device!r} 不是 CONFIRMED 决策")
            continue

        chosen = row["matched_asset_id"]
        if chosen is not None:
            if run_pairs.get(device) != chosen:
                problems.append(f"{device!r} 运行记录中的配对不一致")
            if (device, chosen) in bans:
                problems.append(f"{device!r} 配到了被人工禁止的资产")
            if candidate_scores.get((device, chosen), 0) < min_score:
                problems.append(
                    f"{device!r} 自动配对分数低于 min_score 却未走人工确认"
                )
            # 替代解：禁掉当前选择后穷举
            alt_edges = dict(edges)
            alt_edges.pop((device, chosen), None)
            alt_total, alt_pairs, _ = enumerate_best(
                device_ids, asset_ids, alt_edges,
                pinned=confirmations, forbidden=bans,
            )
            if alt_total is None:
                expected_unique = True
                expected_gap = None
                expected_alt = None
            else:
                expected_gap = total - alt_total
                expected_unique = expected_gap > margin
                expected_alt = (alt_pairs or {}).get(device)
            if row["alternative_total"] != alt_total:
                problems.append(
                    f"{device!r} 替代总分不一致: 运行={row['alternative_total']} "
                    f"穷举={alt_total}"
                )
            if row["gap"] != expected_gap:
                problems.append(
                    f"{device!r} gap 不一致: 运行={row['gap']} 穷举={expected_gap}"
                )
            expected_decision = (
                DECISION_CONFIRMED if expected_unique else DECISION_PENDING
            )
            if row["decision"] != expected_decision:
                problems.append(
                    f"{device!r} 决策不一致: 运行={row['decision']} "
                    f"穷举应为 {expected_decision}"
                )
            # 替代选择可能不唯一（同分方案）；只要求运行给出的替代资产
            # 确实能让替代解保持 S_alt。
            run_alt_ids = row["alternative_asset_ids"]
            if run_alt_ids:
                alt_asset = run_alt_ids[0]
                pinned_alt = dict(confirmations)
                pinned_alt[device] = alt_asset
                verify_alt_total, _, _ = enumerate_best(
                    device_ids, asset_ids, alt_edges,
                    pinned=pinned_alt, forbidden=bans,
                )
                if verify_alt_total != alt_total:
                    problems.append(
                        f"{device!r} 替代选择 {alt_asset!r} 无法保持 S_alt={alt_total}"
                    )
                if len(run_alt_ids) != 1:
                    problems.append(f"{device!r} 替代选择数量异常")
            elif expected_alt is not None:
                problems.append(
                    f"{device!r} 运行未给出替代选择，但穷举存在替代方案"
                )
            if row["status"] not in (STATUS_AUTO_CONFIRMED, STATUS_AUTO_PENDING):
                problems.append(f"{device!r} 自动设备状态异常: {row['status']}")
        else:
            if row["decision"] != DECISION_UNMATCHED:
                problems.append(f"{device!r} 未配对但决策不是 UNMATCHED")
            alt_total, alt_pairs, _ = enumerate_best(
                device_ids, asset_ids, edges,
                pinned=confirmations, forbidden=bans, required=[device],
            )
            if alt_total is None:
                if row["alternative_total"] is not None:
                    problems.append(
                        f"{device!r} 运行声称有替代方案，穷举不可行"
                    )
            else:
                expected_gap = total - alt_total
                if row["alternative_total"] != alt_total:
                    problems.append(
                        f"{device!r} 强制配对替代总分不一致: "
                        f"运行={row['alternative_total']} 穷举={alt_total}"
                    )
                if row["gap"] != expected_gap:
                    problems.append(
                        f"{device!r} 强制配对 gap 不一致: "
                        f"运行={row['gap']} 穷举={expected_gap}"
                    )

    return {
        "matched": not problems,
        "problems": problems,
        "notes": notes,
        "optimal_total": total,
        "equivalent_optima": optima,
        "verified_devices": len(devices),
    }


def audit_small(
    *, devices: list[dict], assets: list[dict], candidates: list[dict],
    constraints: list[dict], min_score: int, margin: int,
) -> dict:
    """直接传入小规模样例（无运行记录）的核对入口。

    每行设备记录至少要有 device / matched_asset_id / decision / status /
    score / alternative_total / gap / alternative_asset_ids。
    """
    run = {
        "params": {"min_score": min_score, "margin": margin},
        "optimal_total": 0,
        "equivalent_optima": 0,
        "inventory_snapshot": {"assets": assets},
    }
    device_ids = sorted({row["device"] for row in devices})
    asset_ids = sorted({row["asset_id"] for row in assets})
    confirmations = {row["device"]: row["asset_id"]
                     for row in constraints if row["kind"] == "CONFIRM"}
    bans = {(row["device"], row["asset_id"])
            for row in constraints if row["kind"] == "BAN"}
    scores = _candidates_from_rows(candidates)
    edges = _score_edges(scores, min_score, confirmations, bans)
    total, pairs, optima = enumerate_best(
        device_ids, asset_ids, edges, pinned=confirmations, forbidden=bans,
        count_equivalents=True,
    )
    if total is None:
        return {"matched": False, "problems": ["穷举没有可行解"]}
    run["optimal_total"] = total
    run["equivalent_optima"] = optima
    return audit_run(run, devices, candidates, constraints)
