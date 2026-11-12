"""接收适配器：批接收、隔离、checkpoint 推进与隔离项再处理。

处理流水线（单条事件，全部在一个 SQLite 事务内完成）
----------------------------------------------------
1. 重传/冲突判定（ingress 去重日志）
2. 按 writer_schema_id 取不可变 writer schema；未知 -> 隔离 UNKNOWN_WRITER_SCHEMA
3. writer -> 当前激活 reader 的兼容性检查（compat 模块，Avro 方向）；
   不兼容/越界结构 -> 隔离 READER_INCOMPATIBLE / UNSUPPORTED_AVRO_STRUCTURE
4. 用 fastavro 按 writer schema 解码、reader schema 做 schema resolution；
   解码失败或尾部有多余字节 -> 隔离 DECODE_ERROR
5. 成功 -> events 业务表；失败 -> quarantine（保留原始字节与诊断）
6. 写 ingress 并按连续 offset 推进 checkpoints
7. COMMIT。``crash_hook`` 在 6 之后、COMMIT 之前触发，模拟进程退出：
   事务整体回滚，接收结果与确认位置共同恢复。

注意：本模块从不使用"JSON 字段是否存在"代替 Avro 解析——
所有结构判断走 compat 规则，所有字节解码走 fastavro 二进制 reader。
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Callable, Optional

import fastavro

from .compat import check_compatibility
from .registry import SchemaRegistry
from .store import EventStore, sha256_hex


# 隔离原因码
REASON_UNKNOWN_WRITER = "UNKNOWN_WRITER_SCHEMA"
REASON_UNSUPPORTED = "UNSUPPORTED_AVRO_STRUCTURE"
REASON_DECODE_ERROR = "DECODE_ERROR"
REASON_READER_INCOMPATIBLE = "READER_INCOMPATIBLE"

# 单条处理结果状态
ST_LANDED = "LANDED"
ST_QUARANTINED = "QUARANTINED"
ST_RETRANSMIT = "RETRANSMIT"
ST_CONFLICT = "CONFLICT"


class SimulatedCrash(Exception):
    """测试用：模拟进程在事务提交前退出（事务随即回滚）。"""


@dataclass
class Packet:
    """采集端输入包。"""

    partition: str
    offset: int
    writer_schema_id: str
    datum: bytes


@dataclass
class IngestResult:
    partition: str
    offset: int
    status: str
    reason_code: Optional[str] = None
    diagnosis: Optional[str] = None
    confirmed_offset: Optional[int] = None


CrashHook = Callable[[str, str, int], None]


class LandingAdapter:
    def __init__(
        self,
        registry: SchemaRegistry,
        store: EventStore,
        crash_hook: Optional[CrashHook] = None,
    ):
        self.registry = registry
        self.store = store
        self.crash_hook = crash_hook

    # ------------------------------------------------------------------ #
    # 批接收
    # ------------------------------------------------------------------ #
    def ingest_batch(self, packets: list[Packet]) -> list[IngestResult]:
        results: list[IngestResult] = []
        for packet in packets:
            results.append(self.ingest_one(packet))
        return results

    def ingest_one(self, packet: Packet) -> IngestResult:
        store = self.store
        partition, offset = packet.partition, packet.offset

        # --- 1. 重传 / 冲突判定（独立事务） ---
        with store.transaction() as conn:
            stored = store.get_ingress(conn, partition, offset)
            if stored is not None:
                incoming_sha = sha256_hex(packet.datum)
                same_bytes = stored["byte_sha256"] == incoming_sha
                same_schema = stored["writer_id"] == packet.writer_schema_id
                if same_bytes and same_schema:
                    # 相同 partition/offset + 相同字节 + 相同 schema ID => 重传，幂等
                    confirmed = store.advance_checkpoint(conn, partition)
                    return IngestResult(
                        partition, offset, ST_RETRANSMIT,
                        confirmed_offset=confirmed,
                    )
                # 任一改变 => 冲突，记录但不覆盖既有接收结果
                store.record_conflict(
                    conn, partition, offset, stored,
                    packet.writer_schema_id, incoming_sha,
                )
                return IngestResult(
                    partition, offset, ST_CONFLICT,
                    diagnosis=(
                        f"同 partition/offset 重传内容不一致："
                        f"stored(writer={stored['writer_id']}, sha={stored['byte_sha256'][:12]}) "
                        f"vs incoming(writer={packet.writer_schema_id}, sha={incoming_sha[:12]})"
                    ),
                    confirmed_offset=store.get_checkpoint(partition),
                )

        # --- 2~7. 新事件：接收落库（单事务原子提交） ---
        with store.transaction() as conn:
            outcome = self._persist_packet(conn, packet)
            # 崩溃注入：业务/隔离已写入、checkpoint 已更新，但 COMMIT 尚未发生
            if self.crash_hook is not None:
                self.crash_hook("before_commit", partition, offset)
            confirmed = store.advance_checkpoint(conn, partition)
        outcome.confirmed_offset = confirmed
        return outcome

    def _persist_packet(self, conn, packet: Packet) -> IngestResult:
        partition, offset = packet.partition, packet.offset
        writer_id, raw = packet.writer_schema_id, packet.datum

        # --- 2. 未知 writer schema -> 隔离（原始字节原样保留） ---
        try:
            writer_schema = self.registry.get_writer(writer_id)
        except Exception as exc:
            self.store.insert_quarantine(
                conn, partition, offset, writer_id, raw,
                REASON_UNKNOWN_WRITER, str(exc), None,
            )
            self.store.record_ingress(conn, partition, offset, writer_id, raw)
            return IngestResult(partition, offset, ST_QUARANTINED,
                                REASON_UNKNOWN_WRITER, str(exc))

        # --- 3. 取激活 reader，做 writer -> reader 兼容判断 ---
        try:
            reader_version, reader_schema = self.registry.get_active_reader()
        except Exception as exc:
            self.store.insert_quarantine(
                conn, partition, offset, writer_id, raw,
                REASON_READER_INCOMPATIBLE, f"无可用 reader: {exc}", None,
            )
            self.store.record_ingress(conn, partition, offset, writer_id, raw)
            return IngestResult(partition, offset, ST_QUARANTINED,
                                REASON_READER_INCOMPATIBLE, str(exc))

        compat = check_compatibility(writer_schema, reader_schema)
        if not compat.compatible:
            code = REASON_UNSUPPORTED if compat.unsupported else REASON_READER_INCOMPATIBLE
            diagnosis = compat.reason
            self.store.insert_quarantine(
                conn, partition, offset, writer_id, raw,
                code, diagnosis, reader_version,
            )
            self.store.record_ingress(conn, partition, offset, writer_id, raw)
            return IngestResult(partition, offset, ST_QUARANTINED, code, diagnosis)

        # --- 4. 真正的 Avro 二进制解码（writer 编码 + reader resolution） ---
        try:
            parsed_writer = fastavro.parse_schema(writer_schema)
            parsed_reader = fastavro.parse_schema(reader_schema)
            bio = io.BytesIO(raw)
            decoded = fastavro.schemaless_reader(
                bio, parsed_writer, reader_schema=parsed_reader
            )
            if bio.tell() != len(raw):
                raise ValueError(
                    f"Avro datum 解码后仍有 {len(raw) - bio.tell()} 个尾部字节未消费"
                )
        except Exception as exc:
            diagnosis = f"{type(exc).__name__}: {exc}"
            self.store.insert_quarantine(
                conn, partition, offset, writer_id, raw,
                REASON_DECODE_ERROR, diagnosis, reader_version,
            )
            self.store.record_ingress(conn, partition, offset, writer_id, raw)
            return IngestResult(partition, offset, ST_QUARANTINED,
                                REASON_DECODE_ERROR, diagnosis)

        # --- 5. 业务表落库 ---
        payload_json = json.dumps(decoded, ensure_ascii=False, sort_keys=True)
        self.store.insert_event(
            conn, partition, offset, writer_id, reader_version, payload_json
        )
        self.store.record_ingress(conn, partition, offset, writer_id, raw)
        return IngestResult(partition, offset, ST_LANDED)

    # ------------------------------------------------------------------ #
    # 隔离项再处理（新 reader + 原始 writer；只补一次业务结果）
    # ------------------------------------------------------------------ #
    def reprocess(
        self,
        partition: Optional[str] = None,
        offset_from: Optional[int] = None,
        offset_to: Optional[int] = None,
    ) -> list[IngestResult]:
        rows = self.store.list_quarantine(
            partition=partition, offset_from=offset_from, offset_to=offset_to,
            status="OPEN",
        )
        outcomes: list[IngestResult] = []
        for row in rows:
            outcomes.append(self._reprocess_one(row))
        return outcomes

    def _reprocess_one(self, row) -> IngestResult:
        partition, offset = row["partition"], row["offset"]
        writer_id, raw = row["writer_id"], row["raw_bytes"]

        with self.store.transaction() as conn:
            try:
                reader_version, reader_schema = self.registry.get_active_reader()
            except Exception as exc:
                self.store.add_attempt(
                    conn, partition, offset, -1, "STILL_QUARANTINED", str(exc)
                )
                return IngestResult(partition, offset, ST_QUARANTINED,
                                    row["reason_code"], str(exc))

            # 沿用原始 writer；未知 ID 若已补注册则可继续
            try:
                writer_schema = self.registry.get_writer(writer_id)
            except Exception as exc:
                diagnosis = f"{REASON_UNKNOWN_WRITER}: {exc}"
                self.store.add_attempt(
                    conn, partition, offset, reader_version,
                    "STILL_QUARANTINED", diagnosis,
                )
                return IngestResult(partition, offset, ST_QUARANTINED,
                                    REASON_UNKNOWN_WRITER, diagnosis)

            compat = check_compatibility(writer_schema, reader_schema)
            if not compat.compatible:
                code = REASON_UNSUPPORTED if compat.unsupported else REASON_READER_INCOMPATIBLE
                # 旧诊断保留在 quarantine 行；本次诊断进 attempts
                self.store.add_attempt(
                    conn, partition, offset, reader_version,
                    "STILL_QUARANTINED", f"{code}: {compat.reason}",
                )
                return IngestResult(partition, offset, ST_QUARANTINED,
                                    code, compat.reason)

            try:
                bio = io.BytesIO(raw)
                decoded = fastavro.schemaless_reader(
                    bio,
                    fastavro.parse_schema(writer_schema),
                    reader_schema=fastavro.parse_schema(reader_schema),
                )
                if bio.tell() != len(raw):
                    raise ValueError(
                        f"Avro datum 解码后仍有 {len(raw) - bio.tell()} 个尾部字节未消费"
                    )
            except Exception as exc:
                diagnosis = f"{REASON_DECODE_ERROR}: {type(exc).__name__}: {exc}"
                self.store.add_attempt(
                    conn, partition, offset, reader_version,
                    "STILL_QUARANTINED", diagnosis,
                )
                return IngestResult(partition, offset, ST_QUARANTINED,
                                    REASON_DECODE_ERROR, str(exc))

            # 成功：业务结果只补一次（防重复再处理）
            if not self.store.event_exists(conn, partition, offset):
                self.store.insert_event(
                    conn, partition, offset, writer_id, reader_version,
                    json.dumps(decoded, ensure_ascii=False, sort_keys=True),
                )
            # 隔离行保留（含旧诊断），仅更新状态与处理版本
            self.store.mark_quarantine_resolved(conn, partition, offset, reader_version)
            self.store.add_attempt(
                conn, partition, offset, reader_version, "RESOLVED",
                f"使用 reader v{reader_version} 再处理成功，原始诊断保留: {row['diagnosis']}",
            )
            return IngestResult(partition, offset, ST_LANDED)
