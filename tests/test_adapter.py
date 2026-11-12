"""端到端二进制测试：所有事件均以 fastavro 真实编码的 Avro 二进制构造。

覆盖场景（对应需求清单）：
1. 字段默认值（顶层 + 嵌套 record）
2. 数字提升的单向性（int->long 接受；long->int 拒绝）
3. nullable union 的两个分支（null / string）
4. 字段 alias（note -> remark）
5. 嵌套 record 缺字段（source.priority 用 default）
6. 未知 writer ID 隔离 -> 后补注册 -> 再处理成功
7. offset 缺口：确认位置停在缺口前，补齐后推进
8. 相同字节重传幂等；字节/schema 改变判冲突
9. 重复再处理只补一次业务结果，旧诊断与处理版本保留
10. 模拟进程在业务写入与确认之间退出：接收结果与确认位置共同恢复
11. 坏字节 DECODE_ERROR、不支持的 Avro 结构被显式拒绝
12. 发布新版 reader 前的兼容预检门禁
"""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

import fastavro

from avro_landing.adapter import (
    LandingAdapter,
    Packet,
    SimulatedCrash,
    ST_CONFLICT,
    ST_LANDED,
    ST_QUARANTINED,
    ST_RETRANSMIT,
    REASON_DECODE_ERROR,
    REASON_READER_INCOMPATIBLE,
    REASON_UNKNOWN_WRITER,
)
from avro_landing.compat import check_compatibility
from avro_landing.registry import SchemaRegistry
from avro_landing.store import EventStore


CONTRACTS = Path(__file__).resolve().parent.parent / "contracts"


def load(name: str) -> dict:
    return json.loads((CONTRACTS / name).read_text(encoding="utf-8"))


def avro_encode(schema: dict, record: dict) -> bytes:
    """用 fastavro 进行真正的 Avro 二进制编码（无容器头）。"""
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, fastavro.parse_schema(schema), record)
    return bio.getvalue()


class AdapterTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.registry = SchemaRegistry(self.tmp / "registry")
        self.registry.init()
        self.store = EventStore(self.tmp / "landing.db")
        self.store.connect()
        # 契约
        self.writer_v1 = load("writer_v1.json")
        self.writer_v2 = load("writer_v2.json")
        self.reader_v1 = load("reader_v1.json")
        self.reader_v2 = load("reader_v2.json")
        self.reader_v3 = load("reader_v3.json")
        self.registry.register_writer("w-event-v1", self.writer_v1)
        self.registry.register_writer("w-event-v2", self.writer_v2)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def adapter(self, crash_hook=None) -> LandingAdapter:
        return LandingAdapter(self.registry, self.store, crash_hook=crash_hook)

    # 常用编码助手
    def event_v1(self, *, note="hello", ref=None, amount=42) -> bytes:
        return avro_encode(self.writer_v1, {
            "event_id": "e-1",
            "amount": amount,
            "note": note,
            "source": {"channel": "web", "ref": ref},
        })

    def event_v2(self) -> bytes:
        return avro_encode(self.writer_v2, {
            "event_id": "e-2",
            "amount": 5_000_000_000,  # 超出 int 范围，只能用 long writer
            "note": None,
            "source": {"channel": "mobile", "ref": "r-9"},
        })


class TestAvroEvolution(AdapterTestBase):
    def test_01_field_defaults_top_level_and_nested(self):
        """新增带 default 的顶层字段 region 与嵌套字段 priority 由 reader 填充。"""
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)
        raw = self.event_v1()

        results = self.adapter().ingest_batch([Packet("p1", 0, "w-event-v1", raw)])
        self.assertEqual(results[0].status, ST_LANDED)

        events = self.store.list_events("p1")
        payload = json.loads(events[0]["payload_json"])
        self.assertEqual(payload["region"], "unknown")       # 顶层新增字段默认值
        self.assertEqual(payload["source"]["priority"], 0)   # 嵌套 record 缺字段默认值
        self.assertEqual(payload["note"], "hello")

    def test_02_numeric_promotion_is_directional(self):
        """int->long 合法；long->int 非法（方向性，不能反向降级）。"""
        ok = check_compatibility(self.writer_v1, self.reader_v2)  # int -> long
        self.assertTrue(ok.compatible, ok.reason)

        bad = check_compatibility(self.writer_v2, self.reader_v1)  # long -> int
        self.assertFalse(bad.compatible)
        self.assertIn("类型不兼容", bad.reason)

        # 真实落库：v2(long) 在只有 reader v1(int) 时必须被隔离
        self.registry.publish_reader(self.reader_v1)
        self.registry.activate_reader(1)
        results = self.adapter().ingest_batch([Packet("p1", 0, "w-event-v2", self.event_v2())])
        self.assertEqual(results[0].status, ST_QUARANTINED)
        self.assertEqual(results[0].reason_code, REASON_READER_INCOMPATIBLE)
        # 原始字节完整保留
        q = self.store.list_quarantine(partition="p1")[0]
        self.assertEqual(q["raw_bytes"], self.event_v2())

    def test_03_nullable_union_both_branches(self):
        """可空 union 的 null 与 string 两个分支都能正确解析。"""
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)

        raw_null = self.event_v1(note=None)
        raw_str = self.event_v1(note="bonjour")
        raw_ref_null = self.event_v1(ref=None)
        raw_ref_str = self.event_v1(ref="ref-77")

        adapter = self.adapter()
        r = adapter.ingest_batch([
            Packet("p1", 0, "w-event-v1", raw_null),
            Packet("p1", 1, "w-event-v1", raw_str),
            Packet("p1", 2, "w-event-v1", raw_ref_null),
            Packet("p1", 3, "w-event-v1", raw_ref_str),
        ])
        self.assertTrue(all(x.status == ST_LANDED for x in r))
        payloads = [json.loads(e["payload_json"]) for e in self.store.list_events("p1")]
        self.assertIsNone(payloads[0]["note"])
        self.assertEqual(payloads[1]["note"], "bonjour")
        self.assertIsNone(payloads[2]["source"]["ref"])
        self.assertEqual(payloads[3]["source"]["ref"], "ref-77")

    def test_04_field_alias(self):
        """reader 字段 remark 通过 alias 匹配 writer 的 note 字段。"""
        ok = check_compatibility(self.writer_v1, self.reader_v3)
        self.assertTrue(ok.compatible, ok.reason)
        ver = self.registry.publish_reader(self.reader_v3)
        self.registry.activate_reader(ver)

        self.adapter().ingest_batch([Packet("p1", 0, "w-event-v1", self.event_v1(note="alias-me"))])
        payload = json.loads(self.store.list_events("p1")[0]["payload_json"])
        self.assertEqual(payload["remark"], "alias-me")
        self.assertNotIn("note", payload)

    def test_05_nested_record_missing_field(self):
        """嵌套 record 新增字段缺 default 必须拒绝；有 default 则通过。"""
        bad_nested = {
            "type": "record", "name": "landing.Event", "namespace": "landing",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "amount", "type": "int"},
                {"name": "note", "type": ["null", "string"], "default": None},
                {"name": "source", "type": {
                    "type": "record", "name": "Source",
                    "fields": [
                        {"name": "channel", "type": "string"},
                        {"name": "mandatory_new", "type": "int"},  # 无 default
                    ],
                }},
            ],
        }
        result = check_compatibility(self.writer_v1, bad_nested)
        self.assertFalse(result.compatible)
        self.assertIn("reader 新增字段缺少 default", result.reason)

        # reader_v2 的嵌套 priority 带 default，同包可正常落地
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)
        r = self.adapter().ingest_batch([Packet("p1", 0, "w-event-v1", self.event_v1())])
        self.assertEqual(r[0].status, ST_LANDED)


class TestFailureAndRecovery(AdapterTestBase):
    def test_06_unknown_writer_then_register_and_reprocess(self):
        """未知 schema ID 先隔离；后补注册后用新 reader 再处理成功。"""
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)
        raw = self.event_v1()

        r = self.adapter().ingest_batch([Packet("p1", 0, "w-not-yet-known", raw)])
        self.assertEqual(r[0].status, ST_QUARANTINED)
        self.assertEqual(r[0].reason_code, REASON_UNKNOWN_WRITER)
        self.assertEqual(self.store.list_events("p1"), [])

        # 后补注册同一个不可变 schema
        self.registry.register_writer("w-not-yet-known", self.writer_v1)
        # 再处理：沿用原始 writer（隔离行里的 w-not-yet-known），使用当前 reader
        outcomes = self.adapter().reprocess(partition="p1")
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0].status, ST_LANDED)

        # 业务结果只补一次；隔离行保留并标记 RESOLVED，旧诊断仍在
        self.assertEqual(len(self.store.list_events("p1")), 1)
        q = self.store.list_quarantine(partition="p1")[0]
        self.assertEqual(q["status"], "RESOLVED")
        self.assertEqual(q["writer_id"], "w-not-yet-known")
        self.assertIn("未知 writer schema id", q["diagnosis"])
        attempts = self.store.list_attempts("p1", 0)
        self.assertEqual(attempts[0]["outcome"], "RESOLVED")
        self.assertEqual(attempts[0]["reader_version"], ver)

        # 再跑一次 reprocess：RESOLVED 项不再被选中，不会重复补业务结果；
        # 坏字节等仍 OPEN 的项会被重试，但 event_exists 保证业务结果只补一次
        self.assertEqual(self.adapter().reprocess(partition="p1", offset_from=0, offset_to=0), [])
        second = self.adapter().reprocess(partition="p1")
        self.assertTrue(all(o.status == ST_QUARANTINED for o in second))
        self.assertEqual(len(self.store.list_events("p1")), 1)

    def test_07_offset_gap_blocks_checkpoint_until_filled(self):
        """缺口之后的事件不能推动确认位置；缺口补齐后连续推进。"""
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)
        adapter = self.adapter()

        # 先到 0、1、3（缺 2）
        r = adapter.ingest_batch([
            Packet("p1", 0, "w-event-v1", self.event_v1(note="a")),
            Packet("p1", 1, "w-event-v1", self.event_v1(note="b")),
            Packet("p1", 3, "w-event-v1", self.event_v1(note="d")),
        ])
        self.assertEqual([x.status for x in r], [ST_LANDED] * 3)
        self.assertEqual(self.store.get_checkpoint("p1"), 1)  # 停在缺口前

        # 另一个分区独立确认
        r2 = adapter.ingest_batch([Packet("p2", 0, "w-event-v1", self.event_v1(note="z"))])
        self.assertEqual(r2[0].confirmed_offset, 0)
        self.assertEqual(self.store.get_checkpoint("p2"), 0)

        # 补齐缺口 offset 2
        r3 = adapter.ingest_batch([Packet("p1", 2, "w-event-v1", self.event_v1(note="c"))])
        self.assertEqual(r3[0].confirmed_offset, 3)  # 一次性推进到 3
        self.assertEqual(self.store.get_checkpoint("p1"), 3)

    def test_08_retransmit_identical_is_idempotent_changed_is_conflict(self):
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)
        adapter = self.adapter()
        raw = self.event_v1()

        first = adapter.ingest_batch([Packet("p1", 0, "w-event-v1", raw)])
        self.assertEqual(first[0].status, ST_LANDED)

        # 相同 partition/offset + 相同字节 + 相同 schema ID => 重传
        again = adapter.ingest_batch([Packet("p1", 0, "w-event-v1", raw)])
        self.assertEqual(again[0].status, ST_RETRANSMIT)
        self.assertEqual(len(self.store.list_events("p1")), 1)  # 无重复业务行

        # 字节改变 => 冲突
        changed = self.event_v1(note="different-bytes")
        conflict = adapter.ingest_batch([Packet("p1", 0, "w-event-v1", changed)])
        self.assertEqual(conflict[0].status, ST_CONFLICT)
        self.assertEqual(len(self.store.list_conflicts()), 1)
        # 既有业务结果不被覆盖
        self.assertEqual(
            json.loads(self.store.list_events("p1")[0]["payload_json"])["note"], "hello"
        )

        # schema ID 改变（字节保持原样）=> 同样判冲突
        conflict2 = adapter.ingest_batch([Packet("p1", 0, "w-event-v2", raw)])
        self.assertEqual(conflict2[0].status, ST_CONFLICT)
        self.assertEqual(len(self.store.list_conflicts()), 2)

    def test_09_crash_between_write_and_commit_recovers_together(self):
        """模拟进程在业务写入与确认之间退出：事务整体回滚，共同恢复。"""
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)
        raw = self.event_v1()

        crash_calls = {"n": 0}

        def crash_hook(stage, partition, offset):
            crash_calls["n"] += 1
            raise SimulatedCrash("boom")

        # 第一次接收：崩溃发生在 COMMIT 前
        with self.assertRaises(SimulatedCrash):
            self.adapter(crash_hook).ingest_batch([Packet("p1", 0, "w-event-v1", raw)])
        self.assertEqual(crash_calls["n"], 1)

        # 进程重启后的世界：业务表、ingress、checkpoint 全部没有部分写入
        fresh_store = EventStore(self.store.db_path)
        self.assertEqual(fresh_store.list_events("p1"), [])
        self.assertEqual(fresh_store.get_checkpoint("p1"), -1)
        self.assertEqual(fresh_store.list_quarantine(), [])

        # 采集端重传相同字节：作为全新事件幂等重做并成功
        fresh_adapter = LandingAdapter(self.registry, fresh_store)
        r = fresh_adapter.ingest_batch([Packet("p1", 0, "w-event-v1", raw)])
        self.assertEqual(r[0].status, ST_LANDED)
        self.assertEqual(r[0].confirmed_offset, 0)
        self.assertEqual(len(fresh_store.list_events("p1")), 1)
        fresh_store.close()

    def test_10_decode_error_quarantines_raw_bytes(self):
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)
        garbage = b"\x0c\x99\xff\x00not-valid-avro"
        r = self.adapter().ingest_batch([Packet("p1", 0, "w-event-v1", garbage)])
        self.assertEqual(r[0].status, ST_QUARANTINED)
        self.assertEqual(r[0].reason_code, REASON_DECODE_ERROR)
        q = self.store.list_quarantine()[0]
        self.assertEqual(q["raw_bytes"], garbage)

    def test_11_unsupported_avro_structures_are_rejected(self):
        """enum/array/非可空多分支 union/具名引用等被支持边界明确拒绝。"""
        enum_schema = {
            "type": "record", "name": "landing.Event", "namespace": "landing",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "amount", "type": "int"},
                {"name": "note", "type": ["null", "string"], "default": None},
                {"name": "source", "type": {
                    "type": "record", "name": "Source",
                    "fields": [{"name": "channel", "type": "string"}],
                }},
                {"name": "color", "type": {
                    "type": "enum", "name": "Color",
                    "symbols": ["RED", "GREEN"],
                }},
            ],
        }
        result = check_compatibility(self.writer_v1, enum_schema)
        self.assertFalse(result.compatible)
        self.assertTrue(result.unsupported)
        self.assertIn("不支持 Avro 类型 'enum'", result.reason)

        multi_union = {
            "type": "record", "name": "landing.Event", "namespace": "landing",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "amount", "type": "int"},
                {"name": "note", "type": ["null", "string", "int"], "default": None},
                {"name": "source", "type": {
                    "type": "record", "name": "Source",
                    "fields": [{"name": "channel", "type": "string"}],
                }},
            ],
        }
        result2 = check_compatibility(self.writer_v1, multi_union)
        self.assertFalse(result2.compatible)
        self.assertIn("仅支持可空 union", result2.reason)

        # 直接注册含不支持结构的 writer 也被拒绝
        with self.assertRaises(Exception):
            self.registry.register_writer("bad", enum_schema)

    def test_12_publish_precheck_gate(self):
        """发布新 reader 前对全部已注册 writer 预检；存在不兼容则拒绝发布。"""
        # reader_v1 无法读 w-event-v2(long->int)，预检应失败并阻止发布
        precheck = self.registry.precheck(self.reader_v1)
        self.assertTrue(precheck["w-event-v1"].compatible)
        self.assertFalse(precheck["w-event-v2"].compatible)

        with self.assertRaises(SystemExit):
            # 模拟 CLI 门禁逻辑
            blocked = {w: r.reason for w, r in precheck.items() if not r.compatible}
            if blocked:
                raise SystemExit(2)

        # reader_v2 对两个 writer 都兼容，可发布
        precheck2 = self.registry.precheck(self.reader_v2)
        self.assertTrue(all(r.compatible for r in precheck2.values()))
        ver = self.registry.publish_reader(self.reader_v2)
        self.registry.activate_reader(ver)
        self.assertEqual(self.registry.active_reader_version(), ver)

    def test_13_writer_schema_immutability(self):
        """同 ID 不同内容的 writer 注册被拒绝；相同内容幂等。"""
        self.registry.register_writer("w-event-v1", self.writer_v1)  # 幂等
        mutated = json.loads(json.dumps(self.writer_v1))
        mutated["fields"][1]["type"] = "long"
        with self.assertRaises(Exception):
            self.registry.register_writer("w-event-v1", mutated)


if __name__ == "__main__":
    unittest.main(verbosity=2)
