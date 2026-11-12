"""设备联动异常识别器的 SQLite 状态层。

与接入层共用同一个数据库文件与连接（``EventStore``）：识别器只读取
``events`` 业务表，不另建"分析输入表"，来源坐标（partition, offset）原样沿用。

放在这里的都是"状态"：已见来源、水位、窗口、候选低压、复位依据、最终结论。
规则判断本身在 :mod:`avro_landing.linkage`，本模块不含阈值比较逻辑。
"""

from __future__ import annotations

import sqlite3
from typing import Iterable, Optional

from .store import EventStore, utcnow


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS linkage_partitions (
    partition     TEXT PRIMARY KEY,
    configured_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS linkage_watermarks (
    partition  TEXT PRIMARY KEY,
    w_ms       INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS linkage_watermark_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    partition   TEXT NOT NULL,
    previous_w  INTEGER,          -- NULL 表示首次声明
    new_w       INTEGER NOT NULL,
    declared_at TEXT NOT NULL
);

-- 已见来源总账：每个 events(partition, offset) 只处理一次，PK 保证不回读。
CREATE TABLE IF NOT EXISTS linkage_sources (
    partition          TEXT NOT NULL,
    offset             INTEGER NOT NULL,
    writer_id          TEXT NOT NULL,
    state              TEXT NOT NULL,   -- APPLIED / SKIPPED / REJECTED / LATE_BYPASS
    reason             TEXT,
    detail             TEXT,
    device             TEXT,
    event_ms           INTEGER,
    signal_kind        TEXT,
    value              REAL,
    declared_watermark INTEGER,         -- 迟到判定所用的旧水位
    seen_at            TEXT NOT NULL,
    PRIMARY KEY (partition, offset)
);
CREATE INDEX IF NOT EXISTS linkage_sources_kind
    ON linkage_sources(signal_kind, state, device, event_ms);

CREATE TABLE IF NOT EXISTS linkage_windows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    partition       TEXT NOT NULL,      -- A 的来源
    offset          INTEGER NOT NULL,
    device          TEXT NOT NULL,
    a_ms            INTEGER NOT NULL,
    right_ms        INTEGER NOT NULL,
    value           REAL NOT NULL,
    status          TEXT NOT NULL,      -- WAITING / CANCELLED / FINALIZED
    final_state     TEXT,               -- HIT / NO_PRESSURE / CANCELLED_BY_RESET
    cancel_partition TEXT,
    cancel_offset   INTEGER,
    cancel_ms       INTEGER,
    opened_at       TEXT NOT NULL,
    finalized_at    TEXT,
    UNIQUE(partition, offset)
);
CREATE INDEX IF NOT EXISTS linkage_windows_due
    ON linkage_windows(status, right_ms);
CREATE INDEX IF NOT EXISTS linkage_windows_device
    ON linkage_windows(device, status);

CREATE TABLE IF NOT EXISTS linkage_candidates (
    window_id  INTEGER NOT NULL,
    partition  TEXT NOT NULL,
    offset     INTEGER NOT NULL,
    event_ms   INTEGER NOT NULL,
    value      REAL NOT NULL,
    PRIMARY KEY (window_id, partition, offset)
);
CREATE INDEX IF NOT EXISTS linkage_candidates_order
    ON linkage_candidates(window_id, event_ms, partition, offset);

CREATE TABLE IF NOT EXISTS linkage_decisions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    partition   TEXT NOT NULL,          -- A 的来源 = 结论的逻辑身份
    offset      INTEGER NOT NULL,
    device      TEXT NOT NULL,
    a_ms        INTEGER NOT NULL,
    right_ms    INTEGER NOT NULL,
    state       TEXT NOT NULL,
    decided_at  TEXT NOT NULL,
    UNIQUE(partition, offset)
);

CREATE TABLE IF NOT EXISTS linkage_decision_evidence (
    decision_id INTEGER NOT NULL,
    role        TEXT NOT NULL,          -- TEMPERATURE / PRESSURE / RESET
    partition   TEXT NOT NULL,
    offset      INTEGER NOT NULL,
    event_ms    INTEGER NOT NULL,
    value       REAL,
    PRIMARY KEY (decision_id, role)
);
"""


class LinkageConfigurationError(Exception):
    """配置分区集合固定，不允许在运行中改变。"""


class LinkageWatermarkError(Exception):
    """水位声明非法（未知分区 / 倒退）。"""


class LinkageStore:
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
        self.ensure_schema()

    @property
    def conn(self) -> sqlite3.Connection:
        return self.event_store.connect()

    def ensure_schema(self) -> None:
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    # --- 配置分区 ---------------------------------------------------------
    def configured_partitions(self, conn: Optional[sqlite3.Connection] = None) -> tuple[str, ...]:
        c = conn or self.conn
        rows = c.execute(
            "SELECT partition FROM linkage_partitions ORDER BY partition"
        ).fetchall()
        return tuple(r["partition"] for r in rows)

    def init_configuration(self, partitions: Iterable[str], conn: Optional[sqlite3.Connection] = None) -> None:
        """首次启动时固定采集分区集合；之后每次启动必须完全一致。"""
        wanted = tuple(sorted(set(partitions)))
        if not wanted:
            raise LinkageConfigurationError("必须至少配置一个采集分区")
        c = conn or self.conn
        existing = self.configured_partitions(c)
        if existing and existing != wanted:
            raise LinkageConfigurationError(
                f"配置分区集合固定为 {list(existing)}，本次启动传入 {list(wanted)}"
            )
        if not existing:
            now = utcnow()
            c.executemany(
                "INSERT INTO linkage_partitions(partition, configured_at) VALUES (?, ?)",
                [(p, now) for p in wanted],
            )
        if conn is None:
            c.commit()

    # --- 水位 -------------------------------------------------------------
    def watermarks(self, conn: Optional[sqlite3.Connection] = None) -> dict[str, int]:
        c = conn or self.conn
        return {
            r["partition"]: r["w_ms"]
            for r in c.execute("SELECT partition, w_ms FROM linkage_watermarks").fetchall()
        }

    def global_watermark(
        self, conn: Optional[sqlite3.Connection] = None,
    ) -> tuple[Optional[int], tuple[str, ...]]:
        """所有配置分区都已声明时返回最小 W；否则返回 (None, 未声明分区)。"""
        c = conn or self.conn
        configured = self.configured_partitions(c)
        marks = self.watermarks(c)
        missing = tuple(p for p in configured if p not in marks)
        if missing:
            return None, missing
        return min(marks[p] for p in configured), ()

    def declare_watermark(
        self, conn: sqlite3.Connection, partition: str, new_w: int,
    ) -> int:
        """声明分区水位；倒退是错误。返回旧水位（首次声明为 -1）。"""
        configured = self.configured_partitions(conn)
        if partition not in configured:
            raise LinkageWatermarkError(
                f"分区 {partition!r} 不在启动时固定的采集分区集合 {list(configured)}"
            )
        if isinstance(new_w, bool) or not isinstance(new_w, int):
            raise LinkageWatermarkError(f"水位必须是整数毫秒，收到 {new_w!r}")
        row = conn.execute(
            "SELECT w_ms FROM linkage_watermarks WHERE partition = ?", (partition,),
        ).fetchone()
        previous = row["w_ms"] if row else -1
        if row is not None and new_w < previous:
            raise LinkageWatermarkError(
                f"水位倒退：分区 {partition!r} 旧 W={previous}，新 W={new_w}"
            )
        conn.execute(
            "INSERT INTO linkage_watermarks(partition, w_ms, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(partition) DO UPDATE SET w_ms = excluded.w_ms, updated_at = excluded.updated_at",
            (partition, new_w, utcnow()),
        )
        conn.execute(
            "INSERT INTO linkage_watermark_events(partition, previous_w, new_w, declared_at) "
            "VALUES (?, ?, ?, ?)",
            (partition, None if previous < 0 else previous, new_w, utcnow()),
        )
        return previous

    # --- 来源 -------------------------------------------------------------
    def unseen_events(self, conn: sqlite3.Connection) -> list[sqlite3.Row]:
        """读取尚未被识别器看过的业务行。

        刻意按 (partition, offset) 做差集，而不是"从最大 offset 之后扫描"：
        隔离项修复后可能以更小的 offset 出现，也允许任意到达顺序。
        """
        return conn.execute(
            "SELECT e.partition, e.offset, e.writer_id, e.reader_version, e.payload_json "
            "FROM events e "
            "LEFT JOIN linkage_sources s "
            "  ON s.partition = e.partition AND s.offset = e.offset "
            "WHERE s.partition IS NULL "
            "ORDER BY e.partition, e.offset"
        ).fetchall()

    def source_seen(self, conn: sqlite3.Connection, partition: str, offset: int) -> bool:
        return conn.execute(
            "SELECT 1 FROM linkage_sources WHERE partition = ? AND offset = ?",
            (partition, offset),
        ).fetchone() is not None

    def record_source(
        self, conn: sqlite3.Connection, *, partition: str, offset: int,
        writer_id: str, state: str, reason: Optional[str] = None,
        detail: Optional[str] = None, device: Optional[str] = None,
        event_ms: Optional[int] = None, signal_kind: Optional[str] = None,
        value: Optional[float] = None, declared_watermark: Optional[int] = None,
    ) -> None:
        conn.execute(
            "INSERT INTO linkage_sources(partition, offset, writer_id, state, reason, detail, "
            "device, event_ms, signal_kind, value, declared_watermark, seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (partition, offset, writer_id, state, reason, detail, device, event_ms,
             signal_kind, value, declared_watermark, utcnow()),
        )

    def applied_signals(
        self, conn: sqlite3.Connection, *, device: str, kind: str,
    ) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT partition, offset, event_ms, value FROM linkage_sources "
            "WHERE state = 'APPLIED' AND device = ? AND signal_kind = ? "
            "ORDER BY event_ms, partition, offset",
            (device, kind),
        ).fetchall()

    def list_sources(self, state: Optional[str] = None) -> list[sqlite3.Row]:
        if state is None:
            return self.conn.execute(
                "SELECT * FROM linkage_sources ORDER BY partition, offset"
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM linkage_sources WHERE state = ? ORDER BY partition, offset",
            (state,),
        ).fetchall()

    # --- 窗口 -------------------------------------------------------------
    def open_window(
        self, conn: sqlite3.Connection, *, partition: str, offset: int,
        device: str, a_ms: int, right_ms: int, value: float,
    ) -> int:
        row = conn.execute(
            "SELECT id FROM linkage_windows WHERE partition = ? AND offset = ?",
            (partition, offset),
        ).fetchone()
        if row is not None:
            return row["id"]
        cur = conn.execute(
            "INSERT INTO linkage_windows(partition, offset, device, a_ms, right_ms, value, "
            "status, opened_at) VALUES (?, ?, ?, ?, ?, ?, 'WAITING', ?)",
            (partition, offset, device, a_ms, right_ms, value, utcnow()),
        )
        return cur.lastrowid

    def add_existing_candidates_for_window(
        self, conn: sqlite3.Connection, window_id: int, *, device: str,
        a_ms: int, right_ms: int,
    ) -> None:
        """窗口创建时回填此前已接纳的压力信号（业务时间可能早于到达时间）。"""
        conn.execute(
            "INSERT OR IGNORE INTO linkage_candidates(window_id, partition, offset, event_ms, value) "
            "SELECT ?, partition, offset, event_ms, value FROM linkage_sources "
            "WHERE state = 'APPLIED' AND signal_kind = 'PRESSURE' AND device = ? "
            "  AND event_ms > ? AND event_ms <= ?",
            (window_id, device, a_ms, right_ms),
        )

    def first_existing_reset_for_window(
        self, conn: sqlite3.Connection, *, device: str, a_ms: int, right_ms: int,
    ) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT partition, offset, event_ms, value FROM linkage_sources "
            "WHERE state = 'APPLIED' AND signal_kind = 'RESET' AND device = ? "
            "  AND event_ms >= ? AND event_ms <= ? "
            "ORDER BY event_ms, partition, offset LIMIT 1",
            (device, a_ms, right_ms),
        ).fetchone()

    def cancel_window(
        self, conn: sqlite3.Connection, window_id: int, *,
        reset_partition: str, reset_offset: int, reset_ms: int,
    ) -> None:
        conn.execute(
            "UPDATE linkage_windows SET status = 'CANCELLED', cancel_partition = ?, "
            "cancel_offset = ?, cancel_ms = ? WHERE id = ? AND status = 'WAITING'",
            (reset_partition, reset_offset, reset_ms, window_id),
        )

    def note_cancelled_window_reset(
        self, conn: sqlite3.Connection, window_id: int, *,
        reset_partition: str, reset_offset: int, reset_ms: int,
    ) -> None:
        """已取消窗口后来又见到更早的业务时间 RESET 时，更新依据为最早者。"""
        conn.execute(
            "UPDATE linkage_windows SET cancel_partition = ?, cancel_offset = ?, cancel_ms = ? "
            "WHERE id = ? AND status = 'CANCELLED' "
            "  AND (cancel_ms > ? OR (cancel_ms = ? AND "
            "       (cancel_partition > ? OR (cancel_partition = ? AND cancel_offset > ?))))",
            (reset_partition, reset_offset, reset_ms, window_id,
             reset_ms, reset_ms, reset_partition, reset_partition, reset_offset),
        )

    def windows_for_pressure(
        self, conn: sqlite3.Connection, *, device: str, event_ms: int,
    ) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT id FROM linkage_windows WHERE device = ? AND status = 'WAITING' "
            "  AND a_ms < ? AND right_ms >= ?",
            (device, event_ms, event_ms),
        ).fetchall()

    def add_candidate(
        self, conn: sqlite3.Connection, window_id: int, *,
        partition: str, offset: int, event_ms: int, value: float,
    ) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO linkage_candidates(window_id, partition, offset, event_ms, value) "
            "VALUES (?, ?, ?, ?, ?)",
            (window_id, partition, offset, event_ms, value),
        )

    def windows_for_reset(
        self, conn: sqlite3.Connection, *, device: str, event_ms: int,
    ) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM linkage_windows WHERE device = ? "
            "  AND status IN ('WAITING', 'CANCELLED') "
            "  AND a_ms <= ? AND right_ms >= ?",
            (device, event_ms, event_ms),
        ).fetchall()

    def chosen_candidate(
        self, conn: sqlite3.Connection, window_id: int,
    ) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT partition, offset, event_ms, value FROM linkage_candidates "
            "WHERE window_id = ? ORDER BY event_ms, partition, offset LIMIT 1",
            (window_id,),
        ).fetchone()

    def candidate_count(self, conn: sqlite3.Connection, window_id: int) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM linkage_candidates WHERE window_id = ?",
            (window_id,),
        ).fetchone()
        return int(row["n"])

    def due_windows(self, conn: sqlite3.Connection, global_w: int) -> list[sqlite3.Row]:
        """右端严格小于全局水位的窗口才可结算。"""
        return conn.execute(
            "SELECT * FROM linkage_windows WHERE status IN ('WAITING', 'CANCELLED') "
            "  AND right_ms < ? ORDER BY right_ms, partition, offset",
            (global_w,),
        ).fetchall()

    def waiting_windows(self, conn: Optional[sqlite3.Connection] = None) -> list[sqlite3.Row]:
        c = conn or self.conn
        return c.execute(
            "SELECT * FROM linkage_windows WHERE status IN ('WAITING', 'CANCELLED') "
            "ORDER BY right_ms, partition, offset"
        ).fetchall()

    def mark_window_final(self, conn: sqlite3.Connection, window_id: int, state: str) -> None:
        conn.execute(
            "UPDATE linkage_windows SET status = 'FINALIZED', final_state = ?, finalized_at = ? "
            "WHERE id = ?",
            (state, utcnow(), window_id),
        )

    # --- 结论 -------------------------------------------------------------
    def insert_decision(
        self, conn: sqlite3.Connection, *, partition: str, offset: int, device: str,
        a_ms: int, right_ms: int, state: str,
    ) -> Optional[int]:
        """结论的逻辑身份是 A 的来源；重复写入返回 None（不重复发出）。"""
        cur = conn.execute(
            "INSERT INTO linkage_decisions(partition, offset, device, a_ms, right_ms, state, decided_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(partition, offset) DO NOTHING",
            (partition, offset, device, a_ms, right_ms, state, utcnow()),
        )
        if cur.rowcount == 0:
            return None
        return cur.lastrowid

    def insert_evidence(
        self, conn: sqlite3.Connection, decision_id: int, role: str,
        partition: str, offset: int, event_ms: int, value: Optional[float],
    ) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO linkage_decision_evidence(decision_id, role, partition, offset, "
            "event_ms, value) VALUES (?, ?, ?, ?, ?, ?)",
            (decision_id, role, partition, offset, event_ms, value),
        )

    def list_decisions(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM linkage_decisions ORDER BY partition, offset"
        ).fetchall()

    def get_decision(self, partition: str, offset: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM linkage_decisions WHERE partition = ? AND offset = ?",
            (partition, offset),
        ).fetchone()

    def decision_evidence(self, decision_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM linkage_decision_evidence WHERE decision_id = ? "
            "ORDER BY CASE role WHEN 'TEMPERATURE' THEN 0 WHEN 'PRESSURE' THEN 1 ELSE 2 END",
            (decision_id,),
        ).fetchall()

    def event_payload(self, partition: str, offset: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT writer_id, payload_json FROM events WHERE partition = ? AND offset = ?",
            (partition, offset),
        ).fetchone()
        if row is None:
            return None
        import json
        return {"writer_id": row["writer_id"], "payload": json.loads(row["payload_json"])}

    def list_windows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM linkage_windows ORDER BY partition, offset"
        ).fetchall()
