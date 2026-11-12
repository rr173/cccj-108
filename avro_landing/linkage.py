"""设备联动异常识别器（推进 / 结算 / 查询）。

推进语义（单次 ``advance`` 的全部动作在一个 SQLite 事务内）
-----------------------------------------------------------
1. 校验本次水位声明：分区必须在启动时固定的集合内，且不能倒退。
2. 按**本次推进开始时的旧水位**处理所有首次见到的业务行：
   - 非 TEMPERATURE/PRESSURE/RESET 的普通消息 -> SKIPPED（可查询原因）
   - 字段非法（非有限数值、event_ms 非整数等）-> REJECTED（可查询原因）
   - ``event_ms < 所属分区旧水位`` -> LATE_BYPASS（迟到旁路，不参与规则；
     恰好等于旧水位仍可进入）
   - 分区不在启动配置集合 -> SKIPPED（UNCONFIGURED_PARTITION）
   - 其余 -> APPLIED，参与规则并立即更新窗口状态
3. 应用本次水位声明（全局进度 = 所有配置分区 W 的最小值；未声明则阻止结算）。
4. 只有全局进度**严格大于**窗口右端时，才把到期窗口写成最终结论。

等价性说明
----------
规则只看业务时间与 > 阈值比较，因此"信号先到/后到"不改变结果：
- 新压力信号会追加到所有已存在且业务时间覆盖它的窗口；
- 新开窗口会回填此前已接纳的、业务时间落在窗口内的压力信号；
- 复位同理，会取消所有业务时间覆盖它的窗口。
因为已结算窗口右端 < 已发布的全局水位，而水位只增不减，之后被接纳的
信号业务时间必然 > 该窗口右端，所以已结算结论不会被后到的数据改写。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional

from .linkage_store import LinkageStore, LinkageWatermarkError
from .rules import (
    CANCELLED_BY_RESET,
    FINAL_STATES,
    HIT,
    NO_PRESSURE,
    OBSERVATION_WINDOW_MS,
    OUTCOME_ACCEPTED,
    OUTCOME_LATE,
    OUTCOME_REJECTED,
    OUTCOME_SKIPPED,
    PRESSURE,
    PRESSURE_CANDIDATE_MAX,
    REASON_LATE_BEFORE_WATERMARK,
    REASON_NOT_A_SIGNAL,
    REASON_UNCONFIGURED_PARTITION,
    RESET,
    TEMPERATURE,
    TEMPERATURE_OPEN_THRESHOLD,
    SignalPayloadError,
    extract_signal,
)
from .store import EventStore


class SimulatedCrash(Exception):
    """演示用：在事务提交前强制退出进程（事务整体回滚）。"""


@dataclass
class SourceRecord:
    partition: str
    offset: int
    state: str
    reason: Optional[str] = None
    detail: Optional[str] = None
    device: Optional[str] = None
    event_ms: Optional[int] = None
    signal_kind: Optional[str] = None
    value: Optional[float] = None
    declared_watermark: Optional[int] = None

    def as_dict(self) -> dict:
        return {
            "partition": self.partition,
            "offset": self.offset,
            "state": self.state,
            "reason": self.reason,
            "detail": self.detail,
            "device": self.device,
            "event_ms": self.event_ms,
            "signal_kind": self.signal_kind,
            "value": self.value,
            "declared_watermark": self.declared_watermark,
        }


@dataclass
class SettledDecision:
    partition: str
    offset: int
    device: str
    a_ms: int
    right_ms: int
    state: str
    evidence: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "partition": self.partition,
            "offset": self.offset,
            "device": self.device,
            "a_ms": self.a_ms,
            "right_ms": self.right_ms,
            "state": self.state,
            "evidence": self.evidence,
        }


CrashHook = Callable[[str, str, int], None]


class LinkageRecognizer:
    def __init__(
        self,
        event_store: EventStore,
        partitions: Iterable[str],
    ):
        self.event_store = event_store
        self.state = LinkageStore(event_store)
        self.state.init_configuration(partitions)
        self.partitions = self.state.configured_partitions()

    # ------------------------------------------------------------------ #
    # 推进
    # ------------------------------------------------------------------ #
    def advance(
        self,
        declared: Optional[dict[str, int]] = None,
        crash_hook: Optional[CrashHook] = None,
    ) -> dict:
        """吸收新落地业务行，应用水位声明，结算到期窗口。"""
        declared = dict(declared or {})

        with self.event_store.transaction() as conn:
            old_marks = self.state.watermarks(conn)

            # --- 1. 先校验（不合法则整体不产生副作用） ---
            self._validate_declared(conn, declared)
            # --- 2. 旧水位处理首次见到的消息 ---
            sources = self._absorb_unseen(conn, old_marks)
            # --- 3. 应用新水位 ---
            declarations = []
            for partition in sorted(declared):
                previous = self.state.declare_watermark(conn, partition, declared[partition])
                declarations.append({
                    "partition": partition,
                    "previous_w": previous if previous >= 0 else None,
                    "new_w": declared[partition],
                })
            # --- 4. 按新水位结算 ---
            settled, progress, missing = self._settle(conn, crash_hook)

        return {
            "declared": declarations,
            "sources": sources,
            "settled": [d.as_dict() for d in settled],
            "progress": {
                "global_watermark": progress,
                "missing_declarations": list(missing),
            },
            "waiting_windows": len(self.state.waiting_windows()),
        }

    def _validate_declared(self, conn, declared: dict[str, int]) -> None:
        configured = set(self.state.configured_partitions(conn))
        marks = self.state.watermarks(conn)
        for partition, new_w in declared.items():
            if partition not in configured:
                raise LinkageWatermarkError(
                    f"分区 {partition!r} 不在启动时固定的采集分区集合 {sorted(configured)}"
                )
            if isinstance(new_w, bool) or not isinstance(new_w, int):
                raise LinkageWatermarkError(
                    f"分区 {partition!r} 的水位必须是整数毫秒，收到 {new_w!r}"
                )
            if partition in marks and new_w < marks[partition]:
                raise LinkageWatermarkError(
                    f"水位倒退：分区 {partition!r} 旧 W={marks[partition]}，新 W={new_w}"
                )

    def _absorb_unseen(self, conn, old_marks: dict[str, int]) -> dict:
        rows = self.state.unseen_events(conn)
        counts = {
            OUTCOME_ACCEPTED: 0, OUTCOME_SKIPPED: 0,
            OUTCOME_REJECTED: 0, OUTCOME_LATE: 0,
        }
        records: list[SourceRecord] = []
        for row in rows:
            record = self._classify(row, old_marks)
            self.state.record_source(
                conn,
                partition=record.partition, offset=record.offset,
                writer_id=row["writer_id"], state=record.state,
                reason=record.reason, detail=record.detail,
                device=record.device, event_ms=record.event_ms,
                signal_kind=record.signal_kind, value=record.value,
                declared_watermark=record.declared_watermark,
            )
            counts[record.state] += 1
            records.append(record)
            if record.state == OUTCOME_ACCEPTED:
                self._apply_signal(
                    conn, partition=record.partition, offset=record.offset,
                    device=record.device, event_ms=record.event_ms,
                    kind=record.signal_kind, value=record.value,
                )
        return {"counts": counts, "records": [r.as_dict() for r in records]}

    def _classify(self, row, old_marks: dict[str, int]) -> SourceRecord:
        import json

        partition, offset = row["partition"], row["offset"]
        base = SourceRecord(partition=partition, offset=offset, state=OUTCOME_SKIPPED)
        if partition not in self.state.configured_partitions():
            base.reason = REASON_UNCONFIGURED_PARTITION
            base.detail = (
                f"分区 {partition!r} 不在启动时固定的采集分区集合 "
                f"{list(self.partitions)}，不参与规则"
            )
            return base
        try:
            signal = extract_signal(json.loads(row["payload_json"]))
        except SignalPayloadError as exc:
            if exc.reason == REASON_NOT_A_SIGNAL:
                # 普通消息（非 TEMPERATURE/PRESSURE/RESET）不参与规则，跳过
                base.state = OUTCOME_SKIPPED
            else:
                base.state = OUTCOME_REJECTED
            base.reason = exc.reason
            base.detail = exc.detail
            return base

        base.device = signal.device
        base.event_ms = signal.event_ms
        base.signal_kind = signal.kind
        base.value = signal.value

        old_w = old_marks.get(partition)
        if old_w is not None:
            base.declared_watermark = old_w
            if signal.event_ms < old_w:
                base.state = OUTCOME_LATE
                base.reason = REASON_LATE_BEFORE_WATERMARK
                base.detail = (
                    f"event_ms={signal.event_ms} 早于所属分区旧水位 W={old_w}；"
                    "保存为迟到旁路，不参与规则、不改既有结论"
                )
                return base

        base.state = OUTCOME_ACCEPTED
        return base

    def _apply_signal(
        self, conn, *, partition: str, offset: int,
        device: str, event_ms: int, kind: str, value: Optional[float],
    ) -> None:
        if kind == TEMPERATURE and value >= TEMPERATURE_OPEN_THRESHOLD:
            right_ms = event_ms + OBSERVATION_WINDOW_MS
            window_id = self.state.open_window(
                conn, partition=partition, offset=offset, device=device,
                a_ms=event_ms, right_ms=right_ms, value=value,
            )
            self.state.add_existing_candidates_for_window(
                conn, window_id, device=device, a_ms=event_ms, right_ms=right_ms,
            )
            reset = self.state.first_existing_reset_for_window(
                conn, device=device, a_ms=event_ms, right_ms=right_ms,
            )
            if reset is not None:
                self.state.cancel_window(
                    conn, window_id,
                    reset_partition=reset["partition"],
                    reset_offset=reset["offset"],
                    reset_ms=reset["event_ms"],
                )
        elif kind == PRESSURE and value <= PRESSURE_CANDIDATE_MAX:
            for window in self.state.windows_for_pressure(
                conn, device=device, event_ms=event_ms,
            ):
                self.state.add_candidate(
                    conn, window["id"], partition=partition, offset=offset,
                    event_ms=event_ms, value=value,
                )
        elif kind == RESET:
            for window in self.state.windows_for_reset(
                conn, device=device, event_ms=event_ms,
            ):
                if window["status"] == "WAITING":
                    self.state.cancel_window(
                        conn, window["id"],
                        reset_partition=partition, reset_offset=offset,
                        reset_ms=event_ms,
                    )
                else:
                    self.state.note_cancelled_window_reset(
                        conn, window["id"],
                        reset_partition=partition, reset_offset=offset,
                        reset_ms=event_ms,
                    )

    def _settle(self, conn, crash_hook: Optional[CrashHook]):
        progress, missing = self.state.global_watermark(conn)
        if progress is None:
            return [], None, missing

        settled: list[SettledDecision] = []
        for window in self.state.due_windows(conn, progress):
            seat = self._build_seat(conn, window)
            decision_id = self.state.insert_decision(
                conn,
                partition=window["partition"], offset=window["offset"],
                device=window["device"], a_ms=window["a_ms"],
                right_ms=window["right_ms"], state=seat.state,
            )
            self.state.mark_window_final(conn, window["id"], seat.state)
            if decision_id is None:
                # 结论已存在（上次推进已发出）：只做窗口状态的幂等收敛，
                # 不重复发出，也不漏掉窗口状态。
                continue
            for evidence in seat.evidence:
                self.state.insert_evidence(
                    conn, decision_id, evidence["role"],
                    evidence["partition"], evidence["offset"],
                    evidence["event_ms"], evidence.get("value"),
                )
            settled.append(seat)
            if crash_hook is not None:
                # 演示/测试：已写入部分最终结论，但尚未 COMMIT
                crash_hook(
                    "after_decision", window["partition"], window["offset"],
                )
        return settled, progress, missing

    def _build_seat(self, conn, window) -> SettledDecision:
        """按窗口的持久状态（复位依据 / 候选）构造最终结论与来源依据。"""
        if window["status"] == "CANCELLED":
            seat = SettledDecision(
                partition=window["partition"], offset=window["offset"],
                device=window["device"], a_ms=window["a_ms"],
                right_ms=window["right_ms"], state=CANCELLED_BY_RESET,
                evidence=[{
                    "role": RESET,
                    "partition": window["cancel_partition"],
                    "offset": window["cancel_offset"],
                    "event_ms": window["cancel_ms"],
                    "value": None,
                }],
            )
            assert seat.state in FINAL_STATES
            return seat

        candidate = self.state.chosen_candidate(conn, window["id"])
        evidence = [{
            "role": TEMPERATURE,
            "partition": window["partition"],
            "offset": window["offset"],
            "event_ms": window["a_ms"],
            "value": window["value"],
        }]
        if candidate is not None:
            state = HIT
            evidence.append({
                "role": PRESSURE,
                "partition": candidate["partition"],
                "offset": candidate["offset"],
                "event_ms": candidate["event_ms"],
                "value": candidate["value"],
            })
        else:
            state = NO_PRESSURE
        seat = SettledDecision(
            partition=window["partition"], offset=window["offset"],
            device=window["device"], a_ms=window["a_ms"],
            right_ms=window["right_ms"], state=state, evidence=evidence,
        )
        assert seat.state in FINAL_STATES
        return seat

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def query(self, partition: str, offset: int) -> dict:
        decision = self.state.get_decision(partition, offset)
        if decision is None:
            return {"found": False, "partition": partition, "offset": offset}

        evidence = []
        for row in self.state.decision_evidence(decision["id"]):
            item = {
                "role": row["role"],
                "partition": row["partition"],
                "offset": row["offset"],
                "event_ms": row["event_ms"],
                "value": row["value"],
            }
            landed = self.state.event_payload(row["partition"], row["offset"])
            item["source_payload"] = landed["payload"] if landed else None
            evidence.append(item)

        out = {
            "found": True,
            "partition": decision["partition"],
            "offset": decision["offset"],
            "device": decision["device"],
            "a_ms": decision["a_ms"],
            "right_ms": decision["right_ms"],
            "observation_range": [decision["a_ms"], decision["right_ms"]],
            "state": decision["state"],
            "decided_at": decision["decided_at"],
            "evidence": evidence,
        }
        if decision["state"] == NO_PRESSURE:
            out["note"] = (
                f"观察区间 ({decision['a_ms']}, {decision['right_ms']}] 内没有符合条件的低压"
            )
        return out

    def waiting(self) -> list[dict]:
        out = []
        for w in self.state.waiting_windows():
            out.append({
                "partition": w["partition"], "offset": w["offset"],
                "device": w["device"], "a_ms": w["a_ms"], "right_ms": w["right_ms"],
                "status": w["status"],
                "state": ("PENDING" if w["status"] == "WAITING"
                          else "PENDING_CANCELLED"),
                "cancelled_by": (
                    None if w["status"] != "CANCELLED" else {
                        "partition": w["cancel_partition"],
                        "offset": w["cancel_offset"],
                        "event_ms": w["cancel_ms"],
                    }
                ),
                "observation_range": [w["a_ms"], w["right_ms"]],
            })
        return out

    def sources(self, state: Optional[str] = None) -> list[dict]:
        return [dict(r) for r in self.state.list_sources(state)]

    def decisions(self) -> list[dict]:
        return [dict(r) for r in self.state.list_decisions()]

    def watermarks(self) -> dict:
        progress, missing = self.state.global_watermark()
        return {
            "configured_partitions": list(self.partitions),
            "watermarks": self.state.watermarks(),
            "global_watermark": progress,
            "missing_declarations": list(missing),
        }
