"""设备联动异常识别器测试：真实 Avro 二进制 + SQLite + 独立核对器。

覆盖工厂口径的每条边界：
- 温度 >= 80 开窗；窗口右端 = A + 600000
- 候选低压：(A, A+600000]，值 <= 20；多个候选取业务时间最早，同毫秒按
  (partition, offset) 排序
- RESET 闭区间 [A, A+600000] 取消窗口；窗口到期后三种最终结论
- 显式水位：未声明分区阻止结算；全局进度为配置分区 W 的最小值；
  严格大于右端才结算；水位倒退报错；不能因长时间无消息排除分区
- 迟到旁路：event_ms 严格小于所属分区旧 W；等于旧 W 仍可进入
- 跨分区同设备合并判断
- 旧位置（更小 offset）的隔离消息修复后首次被分析
- 同一次推进：先按旧水位处理首次见到的消息，再应用新水位
- 结论逻辑身份 = A 的来源，重复推进不重复发出
- 崩溃恢复：写入部分结论后强制退出，重开不重复不漏报
"""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

import fastavro

from avro_landing.adapter import LandingAdapter, Packet, ST_LANDED, ST_QUARANTINED
from avro_landing.linkage import (
    LinkageRecognizer,
    SimulatedCrash as LinkageCrash,
)
from avro_landing.linkage_audit import compare
from avro_landing.linkage_store import LinkageWatermarkError, LinkageConfigurationError
from avro_landing.registry import SchemaRegistry
from avro_landing.rules import (
    CANCELLED_BY_RESET,
    HIT,
    NO_PRESSURE,
    PRESSURE,
    RESET,
    TEMPERATURE,
)
from avro_landing.store import EventStore


CONTRACTS = Path(__file__).resolve().parent.parent / "contracts"
W = 600_000
ALL_PARTITIONS = ("factory-a", "factory-b")


def load(name: str) -> dict:
    return json.loads((CONTRACTS / name).read_text(encoding="utf-8"))


def avro_encode(schema: dict, record: dict) -> bytes:
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, fastavro.parse_schema(schema), record)
    return bio.getvalue()


class LinkageTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.registry = SchemaRegistry(self.tmp / "registry")
        self.registry.init()
        self.store = EventStore(self.tmp / "landing.db")
        self.store.connect()

        self.signals = load("writer_signal_v1.json")
        self.legacy = load("writer_v1.json")
        self.reader = load("reader_signal_v1.json")
        self.registry.register_writer("w-signal", self.signals)
        self.registry.register_writer("w-legacy", self.legacy)
        self.registry.publish_reader(self.reader)
        self.registry.activate_reader(1)

        self.adapter = LandingAdapter(self.registry, self.store)
        self.recognizer = LinkageRecognizer(self.store, ALL_PARTITIONS)
        self._offset = {p: 0 for p in ("factory-a", "factory-b", "factory-c")}
        self._id = 0

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    # --- 编码与接入 ---------------------------------------------------
    def _sig(self, device: str, event_ms: int, kind: str, value=None) -> bytes:
        self._id += 1
        return avro_encode(self.signals, {
            "event_id": f"s-{self._id}",
            "device": device,
            "event_ms": event_ms,
            "signal_kind": kind,
            "value": value,
            "source": {"channel": "bus", "ref": None},
        })

    def land(self, partition: str, device: str, event_ms: int, kind: str,
             value=None, writer_id="w-signal") -> int:
        offset = self._offset[partition]
        self._offset[partition] += 1
        result = self.adapter.ingest_one(
            Packet(partition, offset, writer_id, self._sig(device, event_ms, kind, value))
        )
        self.assertEqual(result.status, ST_LANDED)
        return offset

    def land_legacy(self, partition: str, event_id: str) -> int:
        offset = self._offset[partition]
        self._offset[partition] += 1
        raw = avro_encode(self.legacy, {
            "event_id": event_id, "amount": 3, "note": None,
            "source": {"channel": "web", "ref": None},
        })
        result = self.adapter.ingest_one(Packet(partition, offset, "w-legacy", raw))
        self.assertEqual(result.status, ST_LANDED)
        return offset

    # --- 推进与查询 ---------------------------------------------------
    def advance(self, marks: dict[str, int] | None = None) -> dict:
        return self.recognizer.advance(marks or {})

    def decision(self, partition: str, offset: int) -> dict:
        return self.recognizer.query(partition, offset)

    def final(self, partition: str, offset: int) -> str:
        found = self.decision(partition, offset)
        self.assertTrue(found["found"], f"{partition}/{offset} 尚无最终结论")
        return found["state"]

    def assert_audit_matches(self) -> None:
        report = compare(self.store)
        self.assertTrue(report["matched"], report["problems"])


class TestWindowRules(LinkageTestBase):
    def test_temperature_80_is_inclusive_and_window_opens(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 80.0)
        self.advance({"factory-a": 1_000 + W})
        waiting = self.recognizer.waiting()
        self.assertEqual(len(waiting), 1)
        self.assertEqual(waiting[0]["right_ms"], 1_000 + W)
        self.assertEqual(waiting[0]["state"], "PENDING")
        self.assertEqual(waiting[0]["status"], "WAITING")
        # W 等于右端：不得结算
        self.assertFalse(self.decision("factory-a", a_off)["found"])

    def test_temperature_below_80_does_not_open(self):
        off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 79.999)
        self.advance({"factory-a": 1_000 + W + 1})
        self.assertFalse(self.decision("factory-a", off)["found"])
        self.assertEqual(self.recognizer.sources("APPLIED")[0]["signal_kind"], TEMPERATURE)
        self.assertEqual(self.recognizer.waiting(), [])

    def test_pressure_20_is_candidate_and_21_is_not(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.land("factory-b", "dev-1", 1_001, PRESSURE, 20.0)
        self.land("factory-b", "dev-1", 2_000, PRESSURE, 21.0)
        self.advance({"factory-a": 1_001 + W, "factory-b": 1_001 + W})
        found = self.decision("factory-a", a_off)
        self.assertEqual(found["state"], HIT)
        pressure = [e for e in found["evidence"] if e["role"] == PRESSURE]
        self.assertEqual(len(pressure), 1)
        self.assertEqual(pressure[0]["value"], 20.0)

    def test_candidate_window_is_open_at_left_and_closed_at_right(self):
        a_ms = 1_000
        a_off = self.land("factory-a", "dev-1", a_ms, TEMPERATURE, 90.0)
        # 恰在 A 本身：必须落在 (A, A+W]，不能算
        self.land("factory-a", "dev-1", a_ms, PRESSURE, 5.0)
        # 恰在右端：闭区间右端，算
        self.land("factory-a", "dev-1", a_ms + W, PRESSURE, 6.0)
        self.advance({"factory-a": a_ms + W + 1, "factory-b": a_ms + W + 1})
        found = self.decision("factory-a", a_off)
        self.assertEqual(found["state"], HIT)
        pressure = [e for e in found["evidence"] if e["role"] == PRESSURE]
        self.assertEqual(pressure[0]["event_ms"], a_ms + W)

    def test_earliest_candidate_wins_and_same_ms_tie_breaks_by_source(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        # 业务时间更晚、但先到（分区顺序靠前）
        self.land("factory-a", "dev-1", 600_000, PRESSURE, 10.0)
        # 业务时间更早、后到（另一个分区）
        self.land("factory-b", "dev-1", 500_000, PRESSURE, 11.0)
        # 同毫秒并列：factory-a/offset 更小者优先
        self.land("factory-a", "dev-1", 500_000, PRESSURE, 12.0)
        self.advance({"factory-a": 1_000 + W + 1, "factory-b": 1_000 + W + 1})
        found = self.decision("factory-a", a_off)
        pressure = [e for e in found["evidence"] if e["role"] == PRESSURE][0]
        self.assertEqual(pressure["event_ms"], 500_000)
        self.assertEqual(pressure["partition"], "factory-a")
        self.assertEqual(pressure["value"], 12.0)
        self.assert_audit_matches()

    def test_reset_cancels_only_its_own_windows(self):
        a1 = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        a2 = self.land("factory-a", "dev-1", 2_000, TEMPERATURE, 85.0)
        other = self.land("factory-a", "dev-2", 1_000, TEMPERATURE, 90.0)
        # 2500 同时落在 (1000,601000] 与 (2000,602000] 内
        self.land("factory-b", "dev-1", 2_500, RESET, None)
        self.advance({"factory-a": 2_000 + W + 1, "factory-b": 2_000 + W + 1})
        self.assertEqual(self.final("factory-a", a1), CANCELLED_BY_RESET)
        self.assertEqual(self.final("factory-a", a2), CANCELLED_BY_RESET)
        self.assertEqual(self.final("factory-a", other), NO_PRESSURE)
        # 取消依据可查（最早的那条 RESET）
        found = self.decision("factory-a", a1)
        self.assertEqual(found["evidence"][0]["role"], RESET)
        self.assertEqual(found["evidence"][0]["event_ms"], 2_500)

    def test_reset_outside_its_window_does_not_cancel(self):
        a1 = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        a2 = self.land("factory-a", "dev-2", 1_000 + W + 1, TEMPERATURE, 90.0)
        # dev-2 的 RESET 落在 a2 窗口内（其左端是 A2），不该取消 dev-1 的 a1
        self.land("factory-b", "dev-2", 1_000 + W + 2, RESET, None)
        self.advance({"factory-a": 2_000 + 2 * W, "factory-b": 2_000 + 2 * W})
        self.assertEqual(self.final("factory-a", a1), NO_PRESSURE)
        self.assertEqual(self.final("factory-a", a2), CANCELLED_BY_RESET)

    def test_multiple_temperature_windows_can_share_one_pressure(self):
        a1 = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        a2 = self.land("factory-a", "dev-1", 2_000, TEMPERATURE, 90.0)
        self.land("factory-b", "dev-1", 3_000, PRESSURE, 5.0)
        self.advance({"factory-a": 2_000 + W + 1, "factory-b": 2_000 + W + 1})
        d1 = self.decision("factory-a", a1)
        d2 = self.decision("factory-a", a2)
        self.assertEqual(d1["state"], HIT)
        self.assertEqual(d2["state"], HIT)
        ev1 = [e for e in d1["evidence"] if e["role"] == PRESSURE][0]
        ev2 = [e for e in d2["evidence"] if e["role"] == PRESSURE][0]
        self.assertEqual((ev1["partition"], ev1["offset"]), (ev2["partition"], ev2["offset"]))

    def test_pressure_before_temperature_arrival_is_backfilled(self):
        # 压力先落地，业务时间在窗口内
        self.land("factory-a", "dev-1", 500_000, PRESSURE, 10.0)
        a_off = self.land("factory-a", "dev-1", 10_000, TEMPERATURE, 90.0)
        self.advance({"factory-a": 10_000 + W + 1, "factory-b": 10_000 + W + 1})
        found = self.decision("factory-a", a_off)
        self.assertEqual(found["state"], HIT)
        self.assertEqual(found["evidence"][1]["partition"], "factory-a")
        self.assertEqual(found["evidence"][1]["offset"], 0)

    def test_cross_partition_same_device_are_combined(self):
        a_off = self.land("factory-b", "dev-shared", 20_000, TEMPERATURE, 88.0)
        self.land("factory-a", "dev-shared", 20_500, PRESSURE, 7.0)
        self.advance({"factory-a": 20_501 + W, "factory-b": 20_501 + W})
        found = self.decision("factory-b", a_off)
        self.assertEqual(found["state"], HIT)
        self.assertEqual(found["evidence"][1]["partition"], "factory-a")

    def test_no_pressure_reports_observation_range(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.land("factory-b", "dev-1", 2_000, PRESSURE, 25.0)  # 超阈值，不算候选
        self.advance({"factory-a": 1_000 + W + 1, "factory-b": 1_000 + W + 1})
        found = self.decision("factory-a", a_off)
        self.assertEqual(found["state"], NO_PRESSURE)
        self.assertEqual(found["observation_range"], [1_000, 1_000 + W])
        self.assertIn("没有符合条件的低压", found["note"])
        self.assert_audit_matches()


class TestWatermarks(LinkageTestBase):
    def test_undeclared_partition_blocks_settlement(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        report = self.advance({"factory-a": 1_000 + W + 1})
        self.assertIsNone(report["progress"]["global_watermark"])
        self.assertEqual(report["progress"]["missing_declarations"], ["factory-b"])
        self.assertEqual(report["settled"], [])
        self.assertFalse(self.decision("factory-a", a_off)["found"])

    def test_global_progress_is_minimum_and_never_drops_on_silence(self):
        self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.advance({"factory-a": 10_000, "factory-b": 5_000})
        self.assertEqual(self.recognizer.watermarks()["global_watermark"], 5_000)
        # factory-b 长时间没有新消息：不能把它排除
        report = self.advance({"factory-a": 2_000_000})
        self.assertEqual(report["progress"]["global_watermark"], 5_000)
        self.assertEqual(report["progress"]["missing_declarations"], [])
        self.assertEqual(self.recognizer.watermarks()["watermarks"]["factory-b"], 5_000)

    def test_global_progress_strictly_greater_than_right_end(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        right = 1_000 + W
        self.advance({"factory-a": right, "factory-b": right})
        self.assertFalse(self.decision("factory-a", a_off)["found"])
        self.assertEqual(len(self.recognizer.waiting()), 1)
        self.advance({"factory-a": right + 1, "factory-b": right + 1})
        self.assertEqual(self.final("factory-a", a_off), NO_PRESSURE)

    def test_watermark_regression_is_an_error_without_side_effects(self):
        self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.advance({"factory-a": 100_000, "factory-b": 100_000})
        with self.assertRaises(LinkageWatermarkError):
            self.advance({"factory-a": 99_999})
        self.assertEqual(self.recognizer.watermarks()["watermarks"]["factory-a"], 100_000)

    def test_same_advance_uses_old_watermark_then_applies_new(self):
        """同次推进：先按旧水位分类首次见到的消息，再应用新水位。"""
        self.advance({"factory-a": 10_000, "factory-b": 10_000})
        off = self.land("factory-a", "dev-1", 5_000, PRESSURE, 10.0)
        report = self.advance({"factory-a": 50_000})
        record = [r for r in report["sources"]["records"] if r["offset"] == off][0]
        self.assertEqual(record["state"], "LATE_BYPASS")   # 早于旧水位 10000
        self.assertEqual(record["declared_watermark"], 10_000)
        self.assertEqual(record["reason"], "LATE_BEFORE_WATERMARK")
        self.assertEqual(self.recognizer.watermarks()["watermarks"]["factory-a"], 50_000)
        self.assert_audit_matches()

    def test_event_ms_equal_to_old_watermark_is_still_accepted(self):
        self.advance({"factory-a": 10_000, "factory-b": 10_000})
        a_off = self.land("factory-a", "dev-1", 10_000, TEMPERATURE, 90.0)
        report = self.advance()
        record = [r for r in report["sources"]["records"] if r["offset"] == a_off][0]
        # 恰好等于旧水位：仍可进入，只是记录了判定所用的旧水位
        self.assertEqual(record["state"], "APPLIED")
        self.assertEqual(record["declared_watermark"], 10_000)
        self.assertIsNone(record["reason"])

    def test_late_reset_is_bypassed_and_does_not_change_conclusions(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.advance({"factory-a": 1_000 + W + 1, "factory-b": 1_000 + W + 1})
        self.assertEqual(self.final("factory-a", a_off), NO_PRESSURE)
        late = self.land("factory-b", "dev-1", 500, RESET, None)
        report = self.advance()
        record = [r for r in report["sources"]["records"] if r["offset"] == late][0]
        self.assertEqual(record["state"], "LATE_BYPASS")
        self.assertEqual(record["signal_kind"], RESET)
        self.assertIn("旧水位 W=601001", record["detail"])
        self.assertEqual(self.final("factory-a", a_off), NO_PRESSURE)

    def test_partition_set_is_fixed_across_restarts(self):
        with self.assertRaises(LinkageConfigurationError):
            LinkageRecognizer(self.store, ("factory-a",))
        with self.assertRaises(LinkageConfigurationError):
            LinkageRecognizer(self.store, ("factory-a", "factory-b", "factory-c"))
        # 相同集合可重开
        same = LinkageRecognizer(self.store, ("factory-a", "factory-b"))
        self.assertEqual(same.partitions, ALL_PARTITIONS)

    def test_unknown_partition_declaration_rejected(self):
        self.land("factory-a", "dev-1", 1_000, RESET, None)
        with self.assertRaises(LinkageWatermarkError):
            self.advance({"factory-c": 10})


class TestInputHandling(LinkageTestBase):
    def test_legacy_message_is_skipped_with_reason(self):
        off = self.land_legacy("factory-a", "order-1")
        report = self.advance({"factory-a": 1, "factory-b": 1})
        record = [r for r in report["sources"]["records"] if r["offset"] == off][0]
        self.assertEqual(record["state"], "SKIPPED")
        # 业务行在 events 中，但识别器记录了跳过原因
        stored = [s for s in self.recognizer.sources("SKIPPED")
                  if s["partition"] == "factory-a" and s["offset"] == off][0]
        self.assertEqual(stored["reason"], "NOT_A_SIGNAL_KIND")

    def test_invalid_value_is_rejected_with_reason(self):
        off = self.land("factory-a", "dev-1", 1_000, PRESSURE, None)
        report = self.advance({"factory-a": 1, "factory-b": 1})
        record = [r for r in report["sources"]["records"] if r["offset"] == off][0]
        self.assertEqual(record["state"], "REJECTED")
        self.assertEqual(record["reason"], "INVALID_SIGNAL_VALUE")

    def test_non_finite_value_is_rejected(self):
        from avro_landing.rules import extract_signal, SignalPayloadError
        for bad in (float("inf"), float("nan")):
            with self.assertRaises(SignalPayloadError):
                extract_signal({"device": "d", "event_ms": 1,
                                "signal_kind": "PRESSURE", "value": bad})

    def test_non_signal_kind_is_skipped(self):
        self.land("factory-a", "dev-1", 1_000, "HUMIDITY", 55.0)
        report = self.advance()
        record = [r for r in report["sources"]["records"] if r["offset"] == 0][0]
        self.assertEqual(record["state"], "SKIPPED")
        self.assertEqual(record["reason"], "NOT_A_SIGNAL_KIND")

    def test_boolean_value_is_rejected(self):
        from avro_landing.rules import extract_signal, SignalPayloadError
        with self.assertRaises(SignalPayloadError):
            extract_signal({"device": "d", "event_ms": 1,
                            "signal_kind": "PRESSURE", "value": True})

    def test_sources_are_read_from_events_not_a_side_table(self):
        """识别器没有独立输入表：来源坐标全部能在 events 里找到。"""
        off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.advance({"factory-a": 10, "factory-b": 10})
        rows = self.store.list_events("factory-a")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["offset"], off)
        payload = json.loads(rows[0]["payload_json"])
        self.assertEqual(payload["signal_kind"], TEMPERATURE)


class TestQuarantineReentry(LinkageTestBase):
    def test_repaired_quarantine_at_lower_offset_is_analyzed(self):
        # 未知 writer 先落地 -> events 没有它
        off_q = self._offset["factory-a"]
        self._offset["factory-a"] += 1
        result = self.adapter.ingest_one(
            Packet("factory-a", off_q, "w-not-yet",
                   self._sig("dev-1", 10_000, TEMPERATURE, 90.0))
        )
        self.assertEqual(result.status, ST_QUARANTINED)
        # 更晚的 offset 正常落地；先不声明水位，只让识别器看到现有业务行
        self.land("factory-a", "dev-1", 100_000, PRESSURE, 9.0)
        self.advance()
        self.assertEqual(
            [s for s in self.recognizer.sources() if s["offset"] == off_q], []
        )

        # 修复 writer 后按范围再处理：业务行以更小的 offset 出现
        self.registry.register_writer("w-not-yet", self.signals)
        outcomes = self.adapter.reprocess(partition="factory-a",
                                          offset_from=off_q, offset_to=off_q)
        self.assertEqual(outcomes[0].status, ST_LANDED)
        self.assertEqual(len(self.store.list_events("factory-a")), 2)

        # 下一次推进必须看到它（差集扫描，不是从最大 offset 往后扫）
        self.advance()
        applied = [s for s in self.recognizer.sources("APPLIED")
                   if s["offset"] == off_q]
        self.assertEqual(len(applied), 1)
        self.advance({"factory-a": 10_000 + W + 1, "factory-b": 10_000 + W + 1})
        found = self.decision("factory-a", off_q)
        self.assertEqual(found["state"], HIT)
        self.assertEqual(found["evidence"][1]["event_ms"], 100_000)
        self.assert_audit_matches()

    def test_reprocessing_does_not_duplicate_source_consumption(self):
        off_q = self._offset["factory-a"]
        self._offset["factory-a"] += 1
        packet = Packet("factory-a", off_q, "w-not-yet",
                        self._sig("dev-1", 10_000, TEMPERATURE, 90.0))
        self.assertEqual(self.adapter.ingest_one(packet).status, ST_QUARANTINED)
        self.registry.register_writer("w-not-yet", self.signals)
        self.land("factory-a", "dev-1", 100, RESET, None)
        self.advance({"factory-a": 1, "factory-b": 1})
        self.assertEqual(
            self.adapter.reprocess(partition="factory-a")[0].status, ST_LANDED
        )
        self.advance()
        self.assertEqual(len([s for s in self.recognizer.sources("APPLIED")
                              if s["offset"] == off_q]), 1)


class TestIdempotenceAndCrash(LinkageTestBase):
    def test_repeated_advance_does_not_emit_duplicate_decisions(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.land("factory-b", "dev-1", 2_000, PRESSURE, 9.0)
        self.advance({"factory-a": 1_000 + W + 1, "factory-b": 1_000 + W + 1})
        self.assertEqual(len(self.recognizer.decisions()), 1)
        # 再推进（新水位上涨、无一行新数据）
        self.advance({"factory-a": 2_000_000, "factory-b": 2_000_000})
        self.assertEqual(len(self.recognizer.decisions()), 1)
        self.assertEqual(self.final("factory-a", a_off), HIT)
        self.assert_audit_matches()

    def test_crash_after_partial_decisions_is_all_or_none(self):
        self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.land("factory-b", "dev-2", 1_000, TEMPERATURE, 90.0)
        self.land("factory-b", "dev-1", 2_000, PRESSURE, 9.0)
        self.advance({"factory-a": 10, "factory-b": 10})

        calls = {"n": 0}

        def crash(stage, partition, offset):
            calls["n"] += 1
            raise LinkageCrash(f"crash after decision {partition}/{offset}")

        with self.assertRaises(LinkageCrash):
            self.recognizer.advance(
                {"factory-a": 1_000 + W + 1, "factory-b": 1_000 + W + 1},
                crash_hook=crash,
            )
        self.assertEqual(calls["n"], 1)

        # 重启后的世界：水位与结论都没有部分写入
        fresh = EventStore(self.store.db_path)
        fresh_recognizer = LinkageRecognizer(fresh, ALL_PARTITIONS)
        progress, missing = fresh_recognizer.state.global_watermark()
        self.assertEqual(progress, 10)
        self.assertEqual(missing, ())
        self.assertEqual(len(fresh_recognizer.decisions()), 0)
        self.assertEqual(fresh_recognizer.watermarks()["watermarks"],
                         {"factory-a": 10, "factory-b": 10})
        fresh.close()

        # 重新推进：两条结论都写出，来源只消费一次（无重复）
        self.advance({"factory-a": 1_000 + W + 1, "factory-b": 1_000 + W + 1})
        self.assertEqual(len(self.recognizer.decisions()), 2)
        self.assertEqual(len(self.recognizer.sources()), 3)
        self.assertEqual(self.final("factory-a", 0), HIT)
        self.assertEqual(self.final("factory-b", 0), NO_PRESSURE)
        self.assert_audit_matches()

    def test_settled_decision_not_rewritten_by_later_signals(self):
        a_off = self.land("factory-a", "dev-1", 1_000, TEMPERATURE, 90.0)
        self.advance({"factory-a": 1_000 + W + 1, "factory-b": 1_000 + W + 1})
        self.assertEqual(self.final("factory-a", a_off), NO_PRESSURE)
        # 之后才到的压力业务时间必须晚于水位，因此不在旧窗口内
        late = self.land("factory-b", "dev-1", 600_500, PRESSURE, 5.0)
        self.advance()
        record = [s for s in self.recognizer.sources()
                  if s["partition"] == "factory-b" and s["offset"] == late][0]
        self.assertEqual(record["state"], "LATE_BYPASS")
        self.assertEqual(self.final("factory-a", a_off), NO_PRESSURE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
