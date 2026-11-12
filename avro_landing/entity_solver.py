"""全局一对一分配求解器（整数最大权匹配，允许不配对）。

问题形态
--------
给定设备集合 D、资产集合 A、合格边 E（设备-资产-整数得分），求一组配对：
每个设备至多一个资产、每个资产至多一个设备、未配对得 0，使总得分最大。

实现
----
在 (|D| + |A|) 的方阵上求**最大权完美匹配**（匈牙利算法 / 带势函数的最短
增广路，全程整数运算）：

- 真实设备行与真实资产列之间只放合格边（得分 >= min_score 且未被人工禁止）；
- 每个设备另有一个专属"不配对"虚拟列（权 0），每个资产另有一个专属虚拟行
  （权 0）；这样最优解允许任意设备/资产不配对，且总分为非负；
- 不合格边权为 ``-INFEASIBLE``，永远不会被最优解选中。

同分方案：展示用的方案按（设备名，资产编号）排序后确定；求解器另给出
``equivalent_optima`` 数量，排序只用于稳定展示，不作为唯一性结论。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional


INFEASIBLE = -10**9
DUMMY_SCORE = 0


@dataclass
class Assignment:
    total: int
    pairs: dict[str, str]              # device -> asset_id（仅真实配对）
    unmatched_devices: list[str]
    unmatched_assets: list[str]
    equivalent_optima: int             # 同分最优方案数量

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "pairs": dict(sorted(self.pairs.items())),
            "unmatched_devices": list(self.unmatched_devices),
            "unmatched_assets": list(self.unmatched_assets),
            "equivalent_optima": self.equivalent_optima,
            "uniqueness_note": (
                "展示顺序按设备名与资产编号固定；同分方案数量由 "
                "equivalent_optima 暴露，排序不作为唯一性结论"
            ),
        }


def _max_weight_assignment(weights: list[list[int]]) -> list[int]:
    """方阵最大权完美匹配（匈牙利算法，整数）。

    返回 ``column -> row``（均为 0 起下标）。实现按最小代价形式运行，
    代价矩阵取 ``-weights``（全部为整数，无浮点）。
    """
    n = len(weights)
    if n == 0:
        return []
    cost = [[-w for w in row] for row in weights]
    u = [0] * (n + 1)
    v = [0] * (n + 1)
    p = [0] * (n + 1)
    way = [0] * (n + 1)
    for i in range(1, n + 1):
        p[0] = i
        j0 = 0
        minv = [10**18] * (n + 1)
        used = [False] * (n + 1)
        while True:
            used[j0] = True
            i0 = p[j0]
            delta = 10**18
            j1 = 0
            for j in range(1, n + 1):
                if used[j]:
                    continue
                cur = cost[i0 - 1][j - 1] - u[i0] - v[j]
                if cur < minv[j]:
                    minv[j] = cur
                    way[j] = j0
                if minv[j] < delta:
                    delta = minv[j]
                    j1 = j
            for j in range(0, n + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break
        while True:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
            if j0 == 0:
                break
    result = [-1] * n
    for j in range(1, n + 1):
        if p[j] != 0:
            result[j - 1] = p[j] - 1
    return result


def _solve_raw(
    devices: list[str],
    assets: list[str],
    edges: dict[tuple[str, str], int],
    *,
    mandatory: frozenset[str] = frozenset(),
) -> tuple[dict[str, str], int, bool]:
    """核心求解，返回 (pairs, total, all_mandatory_matched)。

    ``mandatory`` 中的设备不允许落到"不配对"虚拟列（权改为 INFEASIBLE）；
    若最终仍落回虚拟列，说明该子问题不可行。
    """
    nd, na = len(devices), len(assets)
    n = nd + na
    # 分块矩阵（行 = 设备行 + 资产虚拟行；列 = 资产列 + 设备虚拟列）：
    #   [设备->资产]  [设备->自己的虚拟列]
    #   [资产虚拟行]  [资产虚拟行->设备虚拟列]
    # 除下列可行项外全部 INFEASIBLE：
    weights = [[INFEASIBLE] * n for _ in range(n)]
    for i in range(nd):
        # 设备 i 的专属"不配对"虚拟列；required 设备的虚拟列不可用
        pen = INFEASIBLE if devices[i] in mandatory else DUMMY_SCORE
        weights[i][na + i] = pen
    for k in range(na):
        # 资产 k 未被设备占用时，由它的虚拟行收回自己的列（不占用真实设备）
        weights[nd + k][k] = DUMMY_SCORE
        # 其余资产虚拟行落到未被使用的设备虚拟列上（同样不占用真实设备）
        for i in range(nd):
            weights[nd + k][na + i] = DUMMY_SCORE
    d_index = {d: i for i, d in enumerate(devices)}
    a_index = {a: j for j, a in enumerate(assets)}
    for (device, asset_id), score in edges.items():
        if score <= 0:
            continue
        weights[d_index[device]][a_index[asset_id]] = score

    assignment = _max_weight_assignment(weights)   # column -> row
    pairs: dict[str, str] = {}
    total = 0
    for col, row in enumerate(assignment):
        if col < na and row < nd:
            asset_id = assets[col]
            device = devices[row]
            if weights[row][col] != INFEASIBLE:
                pairs[device] = asset_id
                total += weights[row][col]
    mandatory_ok = all(d in pairs for d in mandatory)
    return pairs, total, mandatory_ok


def count_optima(
    devices: Iterable[str],
    assets: Iterable[str],
    edges: dict[tuple[str, str], int],
    optimal_total: int,
    *,
    max_devices: int = 10,
    max_assets: int = 12,
    limit: int = 2,
) -> int:
    """统计达到最优总分的不同方案数（上限 ``limit``）。"""
    devices = sorted(devices)
    assets = sorted(assets)
    if len(devices) <= max_devices and len(assets) <= max_assets:
        count = 0
        scores = {(d, a): s for (d, a), s in edges.items() if s > 0}

        def walk(index: int, used: frozenset[str], total: int,
                 chosen: dict[str, str]) -> None:
            nonlocal count
            if count >= limit:
                return
            if index == len(devices):
                if total == optimal_total:
                    count += 1
                return
            device = devices[index]
            walk(index + 1, used, total, chosen)          # 不配对
            for asset in assets:
                if asset in used:
                    continue
                score = scores.get((device, asset))
                if score is None:
                    continue
                chosen[device] = asset
                walk(index + 1, used | {asset}, total + score, chosen)
                chosen.pop(device, None)

        walk(0, frozenset(), 0, {})
        return count

    # 规模较大：只判断是否存在"第二个同分方案"。
    first, first_total, _ = _solve_raw(devices, assets, edges)
    for device, asset in sorted(first.items()):
        alt_edges = dict(edges)
        alt_edges.pop((device, asset), None)
        alt, alt_total, _ = _solve_raw(devices, assets, alt_edges)
        if alt_total == optimal_total and alt != first:
            return 2
    return 1


def solve_optimum(
    devices: Iterable[str],
    assets: Iterable[str],
    edges: dict[tuple[str, str], int],
    *,
    pinned: Optional[dict[str, str]] = None,
    forbidden: Optional[Iterable[tuple[str, str]]] = None,
    required: Optional[Iterable[str]] = None,
) -> Optional[Assignment]:
    """求解全局最优。``required`` 中设备无法配到合格资产时返回 None。"""
    devices = sorted(devices)
    assets = sorted(assets)
    pinned = dict(pinned or {})
    forbidden_set = set(forbidden or ())

    usable = {
        (d, a): s for (d, a), s in edges.items()
        if s > 0 and (d, a) not in forbidden_set
    }
    fixed_total = 0
    for device, asset in pinned.items():
        if (device, asset) in forbidden_set:
            # 调用方应先做冲突校验；这里保持防御
            return None
        score = usable.get((device, asset), 0)
        fixed_total += score

    remaining_devices = [d for d in devices if d not in pinned]
    remaining_assets = [a for a in assets if a not in set(pinned.values())]
    remaining_edges = {
        (d, a): s for (d, a), s in usable.items()
        if d in remaining_devices and a in remaining_assets
    }
    req = frozenset(d for d in (required or []) if d in remaining_devices)
    pairs, total, mandatory_ok = _solve_raw(
        remaining_devices, remaining_assets, remaining_edges, mandatory=req,
    )
    if not mandatory_ok:
        return None

    merged = dict(pinned)
    merged.update(pairs)
    occupied_assets = set(merged.values())
    equivalent = count_optima(remaining_devices, remaining_assets,
                              remaining_edges, total)
    return Assignment(
        total=fixed_total + total,
        pairs=merged,
        unmatched_devices=[d for d in devices if d not in merged],
        unmatched_assets=[a for a in assets if a not in occupied_assets],
        equivalent_optima=max(equivalent, 1),
    )
