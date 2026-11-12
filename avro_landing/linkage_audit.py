"""独立核对器：只针对"已接纳信号"做全量窗口枚举。

刻意不复用 :mod:`avro_landing.linkage` 的在线匹配逻辑（不打增量状态、不读窗口表），
而是把来源总账里被识别器判定为 APPLIED 的信号全部读出来，逐个温度信号枚举
整个观察区间，用另一套代码得出应有结论与来源依据，再与线上结论逐条比对：

- 已到期（窗口右端 < 全局水位）的 A：必须有结论，且状态、区间、来源依据一致；
- 未到期的 A：不得有任何结论，只计入 pending（"仍在等待完整数据"）；
- 线上多出的结论：报错。

字段有效性复用 :func:`rules.extract_signal`（同一份输入口径，不是匹配逻辑）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .linkage_store import LinkageStore
from .rules import (
    CANCELLED_BY_RESET,
    HIT,
    NO_PRESSURE,
    OBSERVATION_WINDOW_MS,
    PRESSURE,
    PRESSURE_CANDIDATE_MAX,
    RESET,
    TEMPERATURE,
    TEMPERATURE_OPEN_THRESHOLD,
)
from .store import EventStore


@dataclass
class ExpectedDecision:
    partition: str
    offset: int
    device: str
    a_ms: int
    right_ms: int
    due: bool
    state: str
    evidence: list[dict] = field(default_factory=list)


def enumerate_expected(event_store: EventStore) -> list[ExpectedDecision]:
    state = LinkageStore(event_store)
    signals = [
        {
            "partition": row["partition"],
            "offset": row["offset"],
            "device": row["device"],
            "event_ms": row["event_ms"],
            "signal_kind": row["signal_kind"],
            "value": row["value"],
        }
        for row in state.list_sources(state="APPLIED")
    ]

    progress, _missing = state.global_watermark()

    expected: list[ExpectedDecision] = []
    for a in signals:
        if a["signal_kind"] != TEMPERATURE or a["value"] < TEMPERATURE_OPEN_THRESHOLD:
            continue
        right_ms = a["event_ms"] + OBSERVATION_WINDOW_MS
        due = progress is not None and right_ms < progress

        resets = sorted(
            (
                s for s in signals
                if s["device"] == a["device"] and s["signal_kind"] == RESET
                and a["event_ms"] <= s["event_ms"] <= right_ms
            ),
            key=lambda s: (s["event_ms"], s["partition"], s["offset"]),
        )
        if resets:
            chosen = resets[0]
            expected.append(ExpectedDecision(
                partition=a["partition"], offset=a["offset"],
                device=a["device"], a_ms=a["event_ms"], right_ms=right_ms,
                due=due, state=CANCELLED_BY_RESET,
                evidence=[{
                    "role": RESET,
                    "partition": chosen["partition"],
                    "offset": chosen["offset"],
                    "event_ms": chosen["event_ms"],
                    "value": chosen["value"],
                }],
            ))
            continue

        candidates = sorted(
            (
                s for s in signals
                if s["device"] == a["device"] and s["signal_kind"] == PRESSURE
                and s["value"] is not None and s["value"] <= PRESSURE_CANDIDATE_MAX
                and a["event_ms"] < s["event_ms"] <= right_ms
            ),
            key=lambda s: (s["event_ms"], s["partition"], s["offset"]),
        )
        evidence = [{
            "role": TEMPERATURE,
            "partition": a["partition"],
            "offset": a["offset"],
            "event_ms": a["event_ms"],
            "value": a["value"],
        }]
        if candidates:
            chosen = candidates[0]
            state_name = HIT
            evidence.append({
                "role": PRESSURE,
                "partition": chosen["partition"],
                "offset": chosen["offset"],
                "event_ms": chosen["event_ms"],
                "value": chosen["value"],
            })
        else:
            state_name = NO_PRESSURE
        expected.append(ExpectedDecision(
            partition=a["partition"], offset=a["offset"],
            device=a["device"], a_ms=a["event_ms"], right_ms=right_ms,
            due=due, state=state_name, evidence=evidence,
        ))
    expected.sort(key=lambda d: (d.partition, d.offset))
    return expected


def compare(event_store: EventStore) -> dict:
    """逐条比对线上结论与全量枚举结论，包括来源依据。"""
    expected = enumerate_expected(event_store)
    state = LinkageStore(event_store)

    actual = {}
    for d in state.list_decisions():
        actual[(d["partition"], d["offset"])] = {
            "state": d["state"],
            "a_ms": d["a_ms"],
            "right_ms": d["right_ms"],
            "evidence": [
                {
                    "role": r["role"],
                    "partition": r["partition"],
                    "offset": r["offset"],
                    "event_ms": r["event_ms"],
                    "value": r["value"],
                }
                for r in state.decision_evidence(d["id"])
            ],
        }

    problems: list[str] = []
    pending: list[list] = []
    for want in expected:
        key = (want.partition, want.offset)
        got = actual.get(key)
        if not want.due:
            pending.append([want.partition, want.offset])
            if got is not None:
                problems.append(f"{key} 尚未到期却已有结论")
            continue
        if got is None:
            problems.append(f"缺少到期结论 {key}")
            continue
        if got["state"] != want.state:
            problems.append(f"{key} 状态不一致: 线上={got['state']} 枚举={want.state}")
        if (got["a_ms"], got["right_ms"]) != (want.a_ms, want.right_ms):
            problems.append(f"{key} 窗口区间不一致")
        if got["evidence"] != want.evidence:
            problems.append(
                f"{key} 来源依据不一致: 线上={got['evidence']} 枚举={want.evidence}"
            )

    expected_keys = {(w.partition, w.offset) for w in expected}
    for key in sorted(set(actual) - expected_keys):
        problems.append(f"多出线上结论 {key}（全量枚举没有对应温度信号）")

    return {
        "matched": not problems,
        "expected_count": len(expected),
        "due_expected_count": sum(1 for w in expected if w.due),
        "actual_count": len(actual),
        "pending": pending,
        "expected": [d.__dict__ for d in expected],
        "problems": problems,
    }
