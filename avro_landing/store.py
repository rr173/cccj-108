"""SQLite 持久层。

表结构
------
checkpoints         每个 partition 的连续确认 offset（语义：<= offset 已持久接收）
ingress             已持久接收事件去重日志（partition/offset -> 字节指纹/schema ID）
events              业务表：成功解析的事件
quarantine          隔离表：原始字节 + 诊断（未知 schema / 解码失败 / reader 不兼容）
quarantine_attempts 隔离项再处理尝试（保留旧诊断与处理版本，成功也只补一次业务结果）
conflicts           同 partition/offset 重传但字节或 schema ID 改变的冲突记录

关键一致性约束
--------------
ingress 写入、业务/隔离落库、checkpoint 推进在**同一个事务**内提交，
因此"业务写入成功但确认位置未推进"的中间态不可能跨进程存活——
进程在 COMMIT 前任意点退出都会整体回滚，重传后幂等重做。
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS checkpoints (
    partition        TEXT PRIMARY KEY,
    confirmed_offset INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ingress (
    partition   TEXT NOT NULL,
    offset      INTEGER NOT NULL,
    writer_id   TEXT NOT NULL,
    byte_sha256 TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (partition, offset)
);

CREATE TABLE IF NOT EXISTS events (
    partition    TEXT NOT NULL,
    offset       INTEGER NOT NULL,
    writer_id    TEXT NOT NULL,
    reader_version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    landed_at    TEXT NOT NULL,
    PRIMARY KEY (partition, offset)
);

CREATE TABLE IF NOT EXISTS quarantine (
    partition   TEXT NOT NULL,
    offset      INTEGER NOT NULL,
    writer_id   TEXT NOT NULL,
    raw_bytes   BLOB NOT NULL,
    reason_code TEXT NOT NULL,
    diagnosis   TEXT NOT NULL,
    reader_version INTEGER,
    status      TEXT NOT NULL DEFAULT 'OPEN',   -- OPEN / RESOLVED
    first_seen  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (partition, offset)
);

CREATE TABLE IF NOT EXISTS quarantine_attempts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    partition      TEXT NOT NULL,
    offset         INTEGER NOT NULL,
    reader_version INTEGER NOT NULL,
    outcome        TEXT NOT NULL,               -- RESOLVED / STILL_QUARANTINED
    diagnosis      TEXT NOT NULL,               -- 本次诊断（旧诊断保留在 quarantine 行历史中）
    attempted_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conflicts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    partition    TEXT NOT NULL,
    offset       INTEGER NOT NULL,
    stored_writer_id   TEXT NOT NULL,
    incoming_writer_id TEXT NOT NULL,
    stored_sha256      TEXT NOT NULL,
    incoming_sha256    TEXT NOT NULL,
    detected_at  TEXT NOT NULL
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class EventStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    # --- 连接管理 ---------------------------------------------------------
    def connect(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.executescript(SCHEMA_SQL)
            self._conn = conn
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # --- 接收去重 ---------------------------------------------------------
    def get_ingress(self, conn: sqlite3.Connection, partition: str, offset: int) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM ingress WHERE partition = ? AND offset = ?",
            (partition, offset),
        ).fetchone()

    def record_ingress(
        self, conn: sqlite3.Connection, partition: str, offset: int,
        writer_id: str, raw: bytes,
    ) -> None:
        conn.execute(
            "INSERT INTO ingress(partition, offset, writer_id, byte_sha256, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (partition, offset, writer_id, sha256_hex(raw), utcnow()),
        )

    def record_conflict(
        self, conn: sqlite3.Connection, partition: str, offset: int,
        stored: sqlite3.Row, incoming_writer_id: str, incoming_sha: str,
    ) -> None:
        conn.execute(
            "INSERT INTO conflicts(partition, offset, stored_writer_id, incoming_writer_id, "
            "stored_sha256, incoming_sha256, detected_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (partition, offset, stored["writer_id"], incoming_writer_id,
             stored["byte_sha256"], incoming_sha, utcnow()),
        )

    # --- 业务 / 隔离落库 --------------------------------------------------
    def insert_event(
        self, conn: sqlite3.Connection, partition: str, offset: int,
        writer_id: str, reader_version: int, payload_json: str,
    ) -> None:
        conn.execute(
            "INSERT INTO events(partition, offset, writer_id, reader_version, "
            "payload_json, landed_at) VALUES (?, ?, ?, ?, ?, ?)",
            (partition, offset, writer_id, reader_version, payload_json, utcnow()),
        )

    def event_exists(self, conn: sqlite3.Connection, partition: str, offset: int) -> bool:
        return conn.execute(
            "SELECT 1 FROM events WHERE partition = ? AND offset = ?",
            (partition, offset),
        ).fetchone() is not None

    def insert_quarantine(
        self, conn: sqlite3.Connection, partition: str, offset: int,
        writer_id: str, raw: bytes, reason_code: str, diagnosis: str,
        reader_version: Optional[int],
    ) -> None:
        now = utcnow()
        conn.execute(
            "INSERT INTO quarantine(partition, offset, writer_id, raw_bytes, reason_code, "
            "diagnosis, reader_version, status, first_seen, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)",
            (partition, offset, writer_id, raw, reason_code, diagnosis,
             reader_version, now, now),
        )

    def get_quarantine(self, conn: sqlite3.Connection, partition: str, offset: int) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM quarantine WHERE partition = ? AND offset = ?",
            (partition, offset),
        ).fetchone()

    def list_quarantine(
        self, partition: Optional[str] = None,
        offset_from: Optional[int] = None, offset_to: Optional[int] = None,
        status: Optional[str] = None,
    ) -> list[sqlite3.Row]:
        sql = "SELECT * FROM quarantine WHERE 1=1"
        params: list[Any] = []
        if partition is not None:
            sql += " AND partition = ?"
            params.append(partition)
        if offset_from is not None:
            sql += " AND offset >= ?"
            params.append(offset_from)
        if offset_to is not None:
            sql += " AND offset <= ?"
            params.append(offset_to)
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY partition, offset"
        return self.connect().execute(sql, params).fetchall()

    def mark_quarantine_resolved(
        self, conn: sqlite3.Connection, partition: str, offset: int,
        reader_version: int,
    ) -> None:
        """再处理成功：隔离行保留（旧诊断不覆盖），仅置 RESOLVED 与处理版本。"""
        conn.execute(
            "UPDATE quarantine SET status = 'RESOLVED', "
            "reader_version = ?, updated_at = ? WHERE partition = ? AND offset = ?",
            (reader_version, utcnow(), partition, offset),
        )

    def add_attempt(
        self, conn: sqlite3.Connection, partition: str, offset: int,
        reader_version: int, outcome: str, diagnosis: str,
    ) -> None:
        conn.execute(
            "INSERT INTO quarantine_attempts(partition, offset, reader_version, outcome, "
            "diagnosis, attempted_at) VALUES (?, ?, ?, ?, ?, ?)",
            (partition, offset, reader_version, outcome, diagnosis, utcnow()),
        )

    def list_attempts(self, partition: str, offset: int) -> list[sqlite3.Row]:
        return self.connect().execute(
            "SELECT * FROM quarantine_attempts WHERE partition = ? AND offset = ? "
            "ORDER BY id",
            (partition, offset),
        ).fetchall()

    def list_conflicts(self) -> list[sqlite3.Row]:
        return self.connect().execute(
            "SELECT * FROM conflicts ORDER BY id"
        ).fetchall()

    # --- checkpoint -------------------------------------------------------
    def get_checkpoint(self, partition: str) -> int:
        """返回已确认 offset；无记录时返回 -1（表示尚无任何确认）。"""
        row = self.connect().execute(
            "SELECT confirmed_offset FROM checkpoints WHERE partition = ?",
            (partition,),
        ).fetchone()
        return row["confirmed_offset"] if row else -1

    def advance_checkpoint(self, conn: sqlite3.Connection, partition: str) -> int:
        """基于 ingress 中连续 offset 推进确认位置，返回推进后的 offset。

        只认 ingress（已持久接收：业务落库或完成隔离）。遇到第一个缺口即停，
        缺口之后的事件不能推动确认位置。
        """
        current = self.get_checkpoint(partition)
        rows = conn.execute(
            "SELECT offset FROM ingress WHERE partition = ? AND offset > ? ORDER BY offset",
            (partition, current),
        ).fetchall()
        expected = current + 1
        for row in rows:
            if row["offset"] == expected:
                current = expected
                expected += 1
            elif row["offset"] > expected:
                break  # 缺口
        conn.execute(
            "INSERT INTO checkpoints(partition, confirmed_offset) VALUES (?, ?) "
            "ON CONFLICT(partition) DO UPDATE SET confirmed_offset = excluded.confirmed_offset",
            (partition, current),
        )
        return current

    # --- 查询辅助 ---------------------------------------------------------
    def list_events(self, partition: Optional[str] = None) -> list[sqlite3.Row]:
        if partition is None:
            return self.connect().execute(
                "SELECT * FROM events ORDER BY partition, offset"
            ).fetchall()
        return self.connect().execute(
            "SELECT * FROM events WHERE partition = ? ORDER BY offset",
            (partition,),
        ).fetchall()

    def list_checkpoints(self) -> list[sqlite3.Row]:
        return self.connect().execute(
            "SELECT * FROM checkpoints ORDER BY partition"
        ).fetchall()
