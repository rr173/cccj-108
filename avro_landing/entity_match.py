"""实体匹配核心：从一致读视图取采集标识、构造候选、人工限制校验与全局求解。

一次运行的输入
--------------
- ``events`` 业务表中的 device 唯一值（只读，不另建分析输入表）；
- 本地资产清单（调用方已校验，并以规范化快照随运行保存）；
- 命令参数 ``min_score``（0..100）与 ``margin``（非负整数）以及可选的
  盘点范围 ``scope``；
- 人工确认与人工禁止的配对。

计算口径
--------
- 候选只来自距离 <= 2 的（设备，资产名称）组合，得分按正式名/别名整数公式；
- 人工确认先占用两端资源，并从剩余问题中扣除；确认可越过 ``min_score``
  门槛，但仍需存在可复核的候选得分（否则没有候选依据，按输入错误拒绝）；
- 人工禁止在所有求解（含替代解）中都有效；
- 剩余问题求全局一对一最大权和（不做逐个设备贪心）；
- 唯一性：对每个未被人工确认的设备，禁掉它当前的选择（若当前未配对，
  则改为要求它必须配到某个合格资产）再求最优 S_alt。存在可行替代方案时
  gap = S - S_alt；不存在时标记"无替代"。
- 自动匹配：无替代或 gap 严格大于 margin -> 已确认；否则待确认，
  展示原选择、替代选择与两个总分。人工确认不做自动置信判断。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Iterable, Optional

from .entity_names import (
    ALIAS_BASE_SCORE,
    KIND_ALIAS,
    KIND_FORMAL,
    FORMAL_BASE_SCORE,
    MAX_EDIT_DISTANCE,
    SCORE_PER_EDIT,
    Asset,
    clean_name,
    levenshtein,
)
from .entity_solver import Assignment, solve_optimum


# --- 结论状态 -------------------------------------------------------------
STATUS_HUMAN_CONFIRMED = "HUMAN_CONFIRMED"
STATUS_AUTO_CONFIRMED = "AUTO_CONFIRMED"
STATUS_AUTO_PENDING = "AUTO_PENDING"
STATUS_UNMATCHED = "UNMATCHED"

DECISION_CONFIRMED = "CONFIRMED"
DECISION_PENDING = "PENDING"
DECISION_UNMATCHED = "UNMATCHED"

MATCHED_BY_AUTO = "AUTO"
MATCHED_BY_HUMAN = "HUMAN"

# 未匹配原因码
REASON_EMPTY_DEVICE = "EMPTY_DEVICE_NAME"
REASON_NO_CANDIDATE = "NO_QUALIFYING_CANDIDATE"
REASON_ALL_BELOW_MIN_SCORE = "ALL_CANDIDATES_BELOW_MIN_SCORE"
REASON_UNMATCHED_BY_GLOBAL_OPTIMUM = "UNMATCHED_BY_GLOBAL_OPTIMUM"

# 输入错误码
ERR_UNKNOWN_DEVICE = "UNKNOWN_DEVICE"
ERR_UNKNOWN_ASSET = "UNKNOWN_ASSET"
ERR_DEVICE_RESERVED = "DEVICE_RESERVED_BY_TWO_CONFIRMATIONS"
ERR_ASSET_RESERVED = "ASSET_RESERVED_BY_TWO_CONFIRMATIONS"
ERR_CONFIRM_AND_BAN = "CONFIRMATION_AND_BAN_CONFLICT"
ERR_MIN_SCORE = "INVALID_MIN_SCORE"
ERR_MARGIN = "INVALID_MARGIN"
ERR_DUPLICATE_CONFIRMATION = "DUPLICATE_CONFIRMATION"

# 候选不可用原因
AVAIL_OK = "AVAILABLE"
AVAIL_BELOW_MIN_SCORE = "BELOW_MIN_SCORE"
AVAIL_ASSET_RESERVED = "ASSET_RESERVED_BY_HUMAN"
AVAIL_DEVICE_RESERVED = "DEVICE_RESERVED_BY_HUMAN"
AVAIL_BANNED = "BANNED_BY_HUMAN"


class PlanRejected(Exception):
    """整份盘点请求被拒绝；不会写入任何结果。"""

    def __init__(self, errors: Iterable[dict], message: str = ""):
        self.errors = list(errors)
        super().__init__(message or "; ".join(
            f"{e.get('code')}: {e.get('detail', '')}" for e in self.errors
        ))

    def as_dict(self) -> dict:
        return {"ok": False, "errors": self.errors}


# --- 运算参数 -------------------------------------------------------------
@dataclass(frozen=True)
class MatchParams:
    min_score: int
    margin: int

    def validate(self) -> None:
        if isinstance(self.min_score, bool) or not isinstance(self.min_score, int):
            raise PlanRejected([{
                "code": ERR_MIN_SCORE,
                "detail": f"min_score 必须是 0..100 的整数，收到 {self.min_score!r}",
            }])
        if not 0 <= self.min_score <= 100:
            raise PlanRejected([{
                "code": ERR_MIN_SCORE,
                "detail": f"min_score 必须在 0..100 之间，收到 {self.min_score}",
            }])
        if isinstance(self.margin, bool) or not isinstance(self.margin, int):
            raise PlanRejected([{
                "code": ERR_MARGIN,
                "detail": f"margin 必须是非负整数，收到 {self.margin!r}",
            }])
        if self.margin < 0:
            raise PlanRejected([{
                "code": ERR_MARGIN,
                "detail": f"margin 必须是非负整数，收到 {self.margin}",
            }])

    def as_dict(self) -> dict:
        return {"min_score": self.min_score, "margin": self.margin}


# --- 人工限制 -------------------------------------------------------------
@dataclass
class HumanConstraints:
    confirmations: list[tuple[str, str]] = field(default_factory=list)
    bans: list[tuple[str, str]] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "confirmations": [
                {"device": d, "asset_id": a} for d, a in self.confirmations
            ],
            "bans": [{"device": d, "asset_id": a} for d, a in self.bans],
        }


# --- 设备来源 -------------------------------------------------------------
@dataclass(frozen=True)
class DeviceOccurrence:
    device: str
    partition: str
    offset: int
    writer_id: str
    reader_version: int


@dataclass
class DeviceInput:
    """采集标识及其在 events 中的来源（全部出现位置）。"""

    device: str
    clean: str
    occurrences: list[DeviceOccurrence]

    @property
    def primary(self) -> DeviceOccurrence:
        return self.occurrences[0]

    def as_dict(self) -> dict:
            return {
                "device": self.device,
                "clean_name": self.clean,
                "occurrence_count": len(self.occurrences),
                "occurrences": [
                    {
                        "partition": o.partition,
                        "offset": o.offset,
                        "writer_id": o.writer_id,
                        "reader_version": o.reader_version,
                    }
                    for o in self.occurrences
                ],
            }


def read_device_sources(
    conn: sqlite3.Connection,
    *,
    scope: Optional[str] = None,
    partitions: Optional[Iterable[str]] = None,
) -> list[DeviceInput]:
    """从单一连接读取 device 唯一值及其来源坐标。

    调用方必须已开启一致读视图（``BEGIN`` 事务或快照），本函数只执行一条
    ``SELECT``，不会自行提交或另起连接。
    """
    sql = (
        "SELECT partition, offset, writer_id, reader_version, payload_json "
        "FROM events WHERE 1=1"
    )
    params: list = []
    if partitions is not None:
        wanted = list(partitions)
        if not wanted:
            return []
        placeholders = ",".join("?" for _ in wanted)
        sql += f" AND partition IN ({placeholders})"
        params.extend(wanted)
    if scope is not None:
        # 盘点范围：精确匹配一个采集来源分区
        sql += " AND partition = ?"
        params.append(scope)
    sql += " ORDER BY partition, offset"

    grouped: dict[str, list[DeviceOccurrence]] = {}
    for row in conn.execute(sql, params).fetchall():
        payload = json.loads(row["payload_json"])
        device = payload.get("device") if isinstance(payload, dict) else None
        if not isinstance(device, str) or not device:
            continue
        grouped.setdefault(device, []).append(DeviceOccurrence(
            device=device,
            partition=row["partition"],
            offset=row["offset"],
            writer_id=row["writer_id"],
            reader_version=row["reader_version"],
        ))
    out = []
    for device in sorted(grouped):
        out.append(DeviceInput(
            device=device,
            clean=clean_name(device),
            occurrences=grouped[device],
        ))
    return out


# --- 候选 -----------------------------------------------------------------
@dataclass(frozen=True)
class Candidate:
    device: str
    asset_id: str
    kind: str
    name_raw: str
    name_clean: str
    distance: int
    score: int
    shared_alias: bool

    def as_dict(self) -> dict:
        return {
            "device": self.device,
            "asset_id": self.asset_id,
            "kind": self.kind,
            "name_raw": self.name_raw,
            "name_clean": self.name_clean,
            "distance": self.distance,
            "score": self.score,
            "shared_alias": self.shared_alias,
        }


def evaluating_candidates(
    devices: Iterable[str], assets: Iterable[Asset],
) -> list[Candidate]:
    """逐设备逐资产逐名称计算距离；同一资产取最高分。"""
    device_list = sorted(devices)
    assets = list(assets)
    # 共享别名标记：同一清理后别名出现在多个资产
    alias_owners: dict[str, set[str]] = {}
    for asset in assets:
        for alias in asset.aliases:
            alias_owners.setdefault(alias.clean, set()).add(asset.asset_id)
    shared = {clean for clean, owners in alias_owners.items() if len(owners) > 1}

    out: list[Candidate] = []
    for device in device_list:
        device_clean = clean_name(device)
        if not device_clean:
            continue
        for asset in assets:
            best: Optional[Candidate] = None
            for kind, name_raw, name_clean in asset.names():
                distance = levenshtein(device_clean, name_clean)
                if distance > MAX_EDIT_DISTANCE:
                    continue
                base = FORMAL_BASE_SCORE if kind == KIND_FORMAL else ALIAS_BASE_SCORE
                score = base - SCORE_PER_EDIT * distance
                candidate = Candidate(
                    device=device, asset_id=asset.asset_id, kind=kind,
                    name_raw=name_raw, name_clean=name_clean,
                    distance=distance, score=score,
                    shared_alias=(kind == KIND_ALIAS and name_clean in shared),
                )
                if best is None or score > best.score:
                    best = candidate
            if best is not None:
                out.append(best)
    out.sort(key=lambda c: (c.device, c.asset_id))
    return out


# --- 计划 -----------------------------------------------------------------
@dataclass
class DevicePlan:
    device: str
    clean: str
    status: str
    decision: str
    asset_id: Optional[str]
    matched_by: Optional[str]
    score: int
    candidate: Optional[Candidate]
    occurrence: DeviceOccurrence
    alternatives: list[Candidate] = field(default_factory=list)
    alternative_asset_ids: list[str] = field(default_factory=list)
    alternative_total: Optional[int] = None
    alternative_exists: bool = True
    alternative_pairs: Optional[dict[str, str]] = None
    gap: Optional[int] = None
    unique: bool = False
    # 该设备的替代解是否把别的设备挤出了全局分配（用于暴露"抢资源"）
    displaced_devices: list[str] = field(default_factory=list)
    margin: Optional[int] = None
    reason: Optional[str] = None
    reason_detail: Optional[str] = None

    @property
    def gap_rule(self) -> Optional[str]:
        if self.status == STATUS_HUMAN_CONFIRMED:
            return None
        if self.matched_by is None:
            return "无替代方案（强制配对也不可行）" if not self.alternative_exists else None
        if not self.alternative_exists:
            return "无替代方案"
        if self.gap is None:
            return None
        margin = self.margin if self.margin is not None else 0
        return (
            f"gap={self.gap} {'>' if self.gap > margin else '<='} margin={margin}"
        )

    def as_dict(self) -> dict:
        return {
            "device": self.device,
            "clean_name": self.clean,
            "status": self.status,
            "decision": self.decision,
            "matched_asset_id": self.asset_id,
            "matched_by": self.matched_by,
            "score": self.score,
            "candidate": self.candidate.as_dict() if self.candidate else None,
            "source": {
                "partition": self.occurrence.partition,
                "offset": self.occurrence.offset,
                "writer_id": self.occurrence.writer_id,
                "reader_version": self.occurrence.reader_version,
            },
            "unique": self.unique,
            "gap": self.gap,
            "gap_rule": self.gap_rule,
            "alternative_total": self.alternative_total,
            "alternative_asset_ids": list(self.alternative_asset_ids),
            "alternative_exists": self.alternative_exists,
            "alternative_pairs": (
                dict(sorted(self.alternative_pairs.items()))
                if self.alternative_pairs is not None else None
            ),
            "alternatives": [c.as_dict() for c in self.alternatives],
            "displaced_devices": list(self.displaced_devices),
            "reason": self.reason,
            "reason_detail": self.reason_detail,
        }


@dataclass
class MatchPlan:
    params: MatchParams
    devices: list[DeviceInput]
    candidates: list[Candidate]
    human: HumanConstraints
    assignment: Assignment
    plans: list[DevicePlan]
    availability: dict[str, dict[str, str]]
    scope: Optional[str]

    def as_dict(self) -> dict:
        return {
            "params": self.params.as_dict(),
            "scope": self.scope,
            "device_count": len(self.devices),
            "optimal_total": self.assignment.total,
            "equivalent_optima": self.assignment.equivalent_optima,
            "assignment": self.assignment.as_dict(),
            "devices": [p.as_dict() for p in self.plans],
            "human": self.human.as_dict(),
        }


def _availability(
    devices: list[str],
    candidates_by_pair: dict[tuple[str, str], Candidate],
    params: MatchParams,
    human: HumanConstraints,
) -> dict[str, dict[str, str]]:
    """逐设备逐候选资产给出"是否可用及原因"。人工限制优先于分数门槛。"""
    confirmations = {d: a for d, a in human.confirmations}
    confirmed_assets = set(confirmations.values())
    bans = set(human.bans)
    table: dict[str, dict[str, str]] = {device: {} for device in devices}
    for (device, asset_id), candidate in candidates_by_pair.items():
        if confirmations.get(device) == asset_id:
            table[device][asset_id] = AVAIL_OK
        elif (device, asset_id) in bans:
            table[device][asset_id] = AVAIL_BANNED
        elif device in confirmations:
            table[device][asset_id] = AVAIL_DEVICE_RESERVED
        elif asset_id in confirmed_assets:
            table[device][asset_id] = AVAIL_ASSET_RESERVED
        elif candidate.score < params.min_score:
            table[device][asset_id] = AVAIL_BELOW_MIN_SCORE
        else:
            table[device][asset_id] = AVAIL_OK
    return table


def _auto_edges(
    candidates_by_pair: dict[tuple[str, str], Candidate],
    params: MatchParams,
    human: HumanConstraints,
) -> dict[tuple[str, str], int]:
    """自动求解边：分数达到 min_score、未被禁止；确认边单独加入。

    确认可以越过 min_score 门槛，因此确认边总是加入（其候选依据已校验）；
    两端资源由求解器的 pinned 逻辑占用。
    """
    confirmations = {d: a for d, a in human.confirmations}
    bans = set(human.bans)
    edges: dict[tuple[str, str], int] = {}
    for (device, asset_id), candidate in candidates_by_pair.items():
        if (device, asset_id) in bans:
            continue
        if confirmations.get(device) == asset_id:
            edges[(device, asset_id)] = candidate.score
            continue
        if candidate.score >= params.min_score:
            edges[(device, asset_id)] = candidate.score
    return edges


def _validate_human(
    devices: dict[str, DeviceInput],
    asset_ids: set[str],
    human: HumanConstraints,
    candidates_by_pair: dict[tuple[str, str], Candidate],
) -> None:
    errors: list[dict] = []
    seen: dict[tuple[str, str], int] = {}
    for index, (device, asset_id) in enumerate(human.confirmations):
        if device not in devices:
            errors.append({
                "code": ERR_UNKNOWN_DEVICE,
                "detail": f"人工确认引用不存在的设备 {device!r}（不在 events 的唯一 device 值中）",
                "device": device, "asset_id": asset_id,
            })
        if asset_id not in asset_ids:
            errors.append({
                "code": ERR_UNKNOWN_ASSET,
                "detail": f"人工确认引用不存在的资产 {asset_id!r}",
                "device": device, "asset_id": asset_id,
            })
        key = (device, asset_id)
        if key in seen:
            errors.append({
                "code": ERR_DUPLICATE_CONFIRMATION,
                "detail": f"同一设备-资产重复确认 {device!r} -> {asset_id!r}",
                "device": device, "asset_id": asset_id,
            })
        seen[key] = index
        if key not in candidates_by_pair:
            errors.append({
                "code": "CONFIRMATION_WITHOUT_CANDIDATE",
                "detail": (
                    f"人工确认 {device!r} -> {asset_id!r} 没有任何距离 <= "
                    f"{MAX_EDIT_DISTANCE} 的候选名称依据，无法记录候选得分"
                ),
                "device": device, "asset_id": asset_id,
            })
    for index, (device, asset_id) in enumerate(human.bans):
        if device not in devices:
            errors.append({
                "code": ERR_UNKNOWN_DEVICE,
                "detail": f"人工禁止引用不存在的设备 {device!r}",
                "device": device, "asset_id": asset_id,
            })
        if asset_id not in asset_ids:
            errors.append({
                "code": ERR_UNKNOWN_ASSET,
                "detail": f"人工禁止引用不存在的资产 {asset_id!r}",
                "device": device, "asset_id": asset_id,
            })

    confirm_devices: dict[str, str] = {}
    confirm_assets: dict[str, str] = {}
    for device, asset_id in human.confirmations:
        if device in confirm_devices:
            errors.append({
                "code": ERR_DEVICE_RESERVED,
                "detail": (
                    f"设备 {device!r} 被两条确认同时占用："
                    f"{confirm_devices[device]!r} 与 {asset_id!r}"
                ),
                "device": device,
            })
        else:
            confirm_devices[device] = asset_id
        if asset_id in confirm_assets:
            errors.append({
                "code": ERR_ASSET_RESERVED,
                "detail": (
                    f"资产 {asset_id!r} 被两条确认同时占用："
                    f"{confirm_assets[asset_id]!r} 与 {device!r}"
                ),
                "asset_id": asset_id,
            })
        else:
            confirm_assets[asset_id] = device

    for device, asset_id in human.confirmations:
        if (device, asset_id) in set(human.bans):
            errors.append({
                "code": ERR_CONFIRM_AND_BAN,
                "detail": f"同一配对既被确认又被禁止：{device!r} -> {asset_id!r}",
                "device": device, "asset_id": asset_id,
            })

    # 人工确认但仍被禁止覆盖到的边无需重复报；上面已报同一配对。
    if errors:
        raise PlanRejected(errors)


def _displaced(
    current_pairs: dict[str, str], alternative_pairs: dict[str, str],
    *, exclude: str,
) -> list[str]:
    """替代解相对当前全局分配"失去或改变配对"的其他设备。"""
    out = []
    for device, asset_id in sorted(current_pairs.items()):
        if device == exclude:
            continue
        if alternative_pairs.get(device) != asset_id:
            out.append(device)
    return out


def plan_matching(
    *,
    device_inputs: list[DeviceInput],
    assets: dict[str, Asset],
    params: MatchParams,
    human: HumanConstraints,
    scope: Optional[str] = None,
) -> MatchPlan:
    """整份构造并校验；任何冲突都抛出 PlanRejected（不产生半份结果）。"""
    params.validate()
    devices = {item.device: item for item in device_inputs}
    candidates = evaluating_candidates(
        (item.device for item in device_inputs), assets.values()
    )
    candidates_by_pair = {(c.device, c.asset_id): c for c in candidates}

    _validate_human(devices, set(assets), human, candidates_by_pair)

    availability = _availability(list(devices), candidates_by_pair, params, human)
    auto_edges = _auto_edges(candidates_by_pair, params, human)
    confirmations = {d: a for d, a in human.confirmations}
    bans = set(human.bans)

    assignment = solve_optimum(
        list(devices), list(assets), auto_edges,
        pinned=confirmations, forbidden=bans,
    )
    if assignment is None:
        raise PlanRejected([{
            "code": "INFEASIBLE_REQUIRED_DEVICE",
            "detail": "人工确认后剩余问题的全局最优不存在",
        }])

    plans = _build_plans(
        devices=devices, assets=assets, candidates_by_pair=candidates_by_pair,
        availability=availability, auto_edges=auto_edges,
        params=params, human=human, assignment=assignment,
    )
    return MatchPlan(
        params=params,
        devices=device_inputs,
        candidates=candidates,
        human=human,
        assignment=assignment,
        plans=plans,
        availability=availability,
        scope=scope,
    )


def _alternative_edges(
    auto_edges: dict[tuple[str, str], int],
    device: str,
    chosen: Optional[str],
) -> dict[tuple[str, str], int]:
    """构造替代解输入：禁掉当前选择；未配对时改为必须配上某个合格资产。"""
    edges = dict(auto_edges)
    if chosen is not None:
        edges.pop((device, chosen), None)
    return edges


def _build_plans(
    *,
    devices: dict[str, DeviceInput],
    assets: dict[str, Asset],
    candidates_by_pair: dict[tuple[str, str], Candidate],
    availability: dict[str, dict[str, str]],
    auto_edges: dict[tuple[str, str], int],
    params: MatchParams,
    human: HumanConstraints,
    assignment: Assignment,
) -> list[DevicePlan]:
    confirmations = {d: a for d, a in human.confirmations}
    bans = set(human.bans)
    device_ids = sorted(devices)
    asset_ids = sorted(assets)
    plans: list[DevicePlan] = []

    for device in device_ids:
        item = devices[device]
        occurrence = item.primary
        chosen = assignment.pairs.get(device)
        own_candidates = sorted(
            (c for (d, _a), c in candidates_by_pair.items() if d == device),
            key=lambda c: (c.asset_id, c.kind),
        )

        if not item.clean:
            plans.append(DevicePlan(
                device=device, clean=item.clean, status=STATUS_UNMATCHED,
                decision=DECISION_UNMATCHED, asset_id=None, matched_by=None,
                score=0, candidate=None, occurrence=occurrence,
                reason=REASON_EMPTY_DEVICE,
                reason_detail="名称清理后为空串，按口径无效，不参与匹配",
            ))
            continue

        if device in confirmations:
            asset_id = confirmations[device]
            candidate = candidates_by_pair[(device, asset_id)]
            plans.append(DevicePlan(
                device=device, clean=item.clean,
                status=STATUS_HUMAN_CONFIRMED, decision=DECISION_CONFIRMED,
                asset_id=asset_id, matched_by=MATCHED_BY_HUMAN,
                score=candidate.score, candidate=candidate,
                occurrence=occurrence,
                alternatives=[], unique=True, gap=None, alternative_total=None,
            ))
            continue

        if chosen is not None:
            candidate = candidates_by_pair[(device, chosen)]
            # 替代解：禁掉当前选择后重求全局最优。
            # 未配对得 0 始终合法，因此对已配对设备该子问题总有可行解；
            # "替代选择"可能是另一个资产，也可能是"不配对"（同分方案）。
            alt_edges = _alternative_edges(auto_edges, device, chosen)
            alt = solve_optimum(
                device_ids, asset_ids, alt_edges,
                pinned=confirmations, forbidden=bans,
            )
            if alt is None:
                alternative_total = None
                alternative_asset_id = None
                alternative_pairs = None
                alternative_exists = False
                gap = None
                status = STATUS_AUTO_CONFIRMED
                unique = True
            else:
                alternative_total = alt.total
                alternative_asset_id = alt.pairs.get(device)
                alternative_pairs = dict(alt.pairs)
                alternative_exists = True
                gap = assignment.total - alt.total
                unique = gap > params.margin
                status = (STATUS_AUTO_CONFIRMED if unique
                          else STATUS_AUTO_PENDING)
            alternatives = [
                c for c in own_candidates if c.asset_id == alternative_asset_id
            ]
            displaced = _displaced(
                assignment.pairs, alternative_pairs or {}, exclude=device,
            )
            plans.append(DevicePlan(
                device=device, clean=item.clean, status=status,
                decision=(DECISION_CONFIRMED if status == STATUS_AUTO_CONFIRMED
                          else DECISION_PENDING),
                asset_id=chosen, matched_by=MATCHED_BY_AUTO,
                score=auto_edges[(device, chosen)], candidate=candidate,
                occurrence=occurrence,
                alternatives=alternatives,
                alternative_asset_ids=(
                    [alternative_asset_id] if alternative_asset_id else []
                ),
                alternative_total=alternative_total,
                alternative_exists=alternative_exists,
                alternative_pairs=alternative_pairs,
                gap=gap,
                unique=unique,
                displaced_devices=displaced,
                margin=params.margin,
            ))
            continue

        # 全局最优下未配对：给出可查询的原因
        reasons = [availability[device].get(c.asset_id) for c in own_candidates]
        if not own_candidates:
            reason = REASON_NO_CANDIDATE
            detail = (
                f"与全部资产名称的编辑距离都超过 {MAX_EDIT_DISTANCE}，没有合格候选"
            )
        elif all(state != AVAIL_OK for state in reasons):
            reason = REASON_ALL_BELOW_MIN_SCORE
            best = max(c.score for c in own_candidates)
            detail = (
                f"全部候选都不可用（最高分 {best}，min_score={params.min_score}；"
                "人工限制也可能参与），没有可分配资产"
            )
        else:
            reason = REASON_UNMATCHED_BY_GLOBAL_OPTIMUM
            detail = (
                "存在合格候选，但把它配上去会挤掉分数更高的设备或降低全局总分；"
                "未配对得 0 仍为全局最优"
            )
        # 当前未配对：改为要求它必须配到某个合格资产，再求一次最优
        alt = solve_optimum(
            device_ids, asset_ids, auto_edges,
            pinned=confirmations, forbidden=bans, required=[device],
        )
        alternative_asset_id = alt.pairs.get(device) if alt is not None else None
        alternative_total = alt.total if alt is not None else None
        alternative_pairs = dict(alt.pairs) if alt is not None else None
        alternatives = [
            c for c in own_candidates if c.asset_id == alternative_asset_id
        ]
        displaced = _displaced(
            assignment.pairs, alternative_pairs or {}, exclude=device,
        )
        plans.append(DevicePlan(
            device=device, clean=item.clean, status=STATUS_UNMATCHED,
            decision=DECISION_UNMATCHED, asset_id=None, matched_by=None,
            score=0, candidate=None, occurrence=occurrence,
            alternatives=alternatives,
            alternative_asset_ids=(
                [alternative_asset_id] if alternative_asset_id else []
            ),
            alternative_total=alternative_total,
            alternative_exists=alternative_total is not None,
            alternative_pairs=alternative_pairs,
            gap=(None if alternative_total is None
                 else assignment.total - alternative_total),
            unique=alternative_total is None,
            displaced_devices=displaced,
            margin=params.margin,
            reason=reason, reason_detail=detail,
        ))

    return plans
