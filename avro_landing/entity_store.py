"""离线实体匹配的持久层：运行记录、候选、人工限制、分配与替代解。

所有表与接入层共用同一个 SQLite 文件（``EventStore`` 的同一连接），
匹配运行本身不改写 ``events``。

一次运行保存的内容
------------------
- 原始参数（``min_score``、``margin``、盘点范围 scope、执行时刻）；
- 台账内容摘要（原始文件 SHA-256、规范化快照、revision、资产数）；
- 采集标识与它在 events 中的来源（全部出现坐标，而不只是一个 offset）；
- 每个设备的候选依据（原名称、清理后名称、正式名/别名身份、编辑距离、得分）；
- 人工限制（确认与禁止，逐条保存）；
- 全局分配（总分、同分方案计数、每对配对）；
- 每个自动配对的替代解（禁掉当前选择后的 S_alt、替代选择、gap）。

台账快照随运行保存，因此台账更正后的新运行不会改写旧运行的候选与分配，
旧运行仍可按运行编号查询。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Iterable, Optional

from .entity_match import (
    Candidate,
    DevicePlan,
    MatchParams,
    MatchPlan,
    PlanRejected,
)
from .entity_names import Inventory
from .store import EventStore, utcnow


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS match_runs (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    scope           TEXT,
    min_score       INTEGER NOT NULL,
    margin          INTEGER NOT NULL,
    inventory_path  TEXT NOT NULL,
    inventory_sha256 TEXT NOT NULL,
    inventory_revision TEXT,
    inventory_json  TEXT NOT NULL,      -- 规范化快照（台账更正不影响旧运行）
    asset_count     INTEGER NOT NULL,
    device_count    INTEGER NOT NULL,
    optimal_total   INTEGER NOT NULL,
    equivalent_optima INTEGER NOT NULL,
    assignment_json TEXT NOT NULL,      -- 全局分配明细
    human_json      TEXT NOT NULL,      -- 人工限制（原样）
    notes           TEXT,
    UNIQUE(run_id)
);

CREATE TABLE IF NOT EXISTS match_devices (
    run_id      INTEGER NOT NULL,
    device      TEXT NOT NULL,
    clean_name  TEXT NOT NULL,
    status      TEXT NOT NULL,
    decision    TEXT NOT NULL,
    asset_id    TEXT,
    matched_by  TEXT,
    score       INTEGER NOT NULL,
    candidate_json  TEXT,               -- 候选依据（含身份/距离/分数）
    alternative_json TEXT,              -- 替代选择候选列表
    alternative_asset_ids TEXT,
    alternative_total INTEGER,          -- NULL = 不存在替代方案
    alternative_pairs_json TEXT,
    gap         INTEGER,
    unique_flag INTEGER NOT NULL,
    displaced_json TEXT,
    gap_rule    TEXT,
    reason      TEXT,
    reason_detail TEXT,
    PRIMARY KEY (run_id, device)
);
CREATE INDEX IF NOT EXISTS match_devices_asset ON match_devices(run_id, asset_id);

CREATE TABLE IF NOT EXISTS match_sources (
    run_id      INTEGER NOT NULL,
    device      TEXT NOT NULL,
    partition   TEXT NOT NULL,
    offset      INTEGER NOT NULL,
    writer_id   TEXT NOT NULL,
    reader_version INTEGER NOT NULL,
    PRIMARY KEY (run_id, partition, offset)
);

CREATE TABLE IF NOT EXISTS match_candidates (
    run_id      INTEGER NOT NULL,
    device      TEXT NOT NULL,
    asset_id    TEXT NOT NULL,
    kind        TEXT NOT NULL,          -- FORMAL / ALIAS
    name_raw    TEXT NOT NULL,
    name_clean  TEXT NOT NULL,
    distance    INTEGER NOT NULL,
    score       INTEGER NOT NULL,
    shared_alias INTEGER NOT NULL,
    available   TEXT NOT NULL,          -- 该候选在本次运行中是否可用及原因
    used        INTEGER NOT NULL,       -- 是否进入全局分配
    PRIMARY KEY (run_id, device, asset_id)
);

CREATE TABLE IF NOT EXISTS match_human_constraints (
    run_id    INTEGER NOT NULL,
    kind      TEXT NOT NULL,            -- CONFIRM / BAN
    device    TEXT NOT NULL,
    asset_id  TEXT NOT NULL,
    note      TEXT,
    PRIMARY KEY (run_id, kind, device, asset_id)
);

CREATE TABLE IF NOT EXISTS match_alternatives (
    run_id      INTEGER NOT NULL,
    device      TEXT NOT NULL,
    rank        INTEGER NOT NULL,
    asset_id    TEXT NOT NULL,
    score       INTEGER NOT NULL,
    candidate_json TEXT NOT NULL,       -- 保持与主候选同样的依据字段
    PRIMARY KEY (run_id, device, rank)
);
"""


class MatchRunStore:
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
        self.ensure_schema()

    @property
    def conn(self) -> sqlite3.Connection:
        return self.event_store.connect()

    def ensure_schema(self) -> None:
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    # --- 运行前读取（一致读视图内调用） ---------------------------------
    def existing_run_ids(self, conn: sqlite3.Connection) -> list[int]:
        rows = conn.execute("SELECT run_id FROM match_runs ORDER BY run_id").fetchall()
        return [row["run_id"] for row in rows]

    # --- 保存 -------------------------------------------------------------
    def save_run(
        self,
        conn: sqlite3.Connection,
        plan: MatchPlan,
        inventory: Inventory,
    ) -> int:
        """在调用方事务内写入整份运行；任何错误随事务回滚。"""
        created_at = utcnow()
        availability = plan.availability
        device_asset_pairs = {
            (p.device, p.asset_id) for p in plan.plans if p.asset_id is not None
        }
        cur = conn.execute(
            "INSERT INTO match_runs(created_at, scope, min_score, margin, "
            "inventory_path, inventory_sha256, inventory_revision, inventory_json, "
            "asset_count, device_count, optimal_total, equivalent_optima, "
            "assignment_json, human_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                created_at,
                plan.scope,
                plan.params.min_score,
                plan.params.margin,
                inventory.source_path,
                inventory.sha256,
                inventory.revision,
                json.dumps(inventory.canonical, ensure_ascii=False, sort_keys=True),
                len(inventory.assets),
                len(plan.devices),
                plan.assignment.total,
                plan.assignment.equivalent_optima,
                json.dumps(plan.assignment.as_dict(), ensure_ascii=False, sort_keys=True),
                json.dumps(plan.human.as_dict(), ensure_ascii=False, sort_keys=True),
            ),
        )
        run_id = int(cur.lastrowid)

        for item in plan.devices:
            for occurrence in item.occurrences:
                conn.execute(
                    "INSERT OR IGNORE INTO match_sources(run_id, device, partition, "
                    "offset, writer_id, reader_version) VALUES (?, ?, ?, ?, ?, ?)",
                    (run_id, item.device, occurrence.partition, occurrence.offset,
                     occurrence.writer_id, occurrence.reader_version),
                )

        for candidate in plan.candidates:
            pair = (candidate.device, candidate.asset_id)
            avail = availability.get(candidate.device, {}).get(
                candidate.asset_id, "NOT_CONSIDERED"
            )
            conn.execute(
                "INSERT INTO match_candidates(run_id, device, asset_id, kind, name_raw, "
                "name_clean, distance, score, shared_alias, available, used) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, candidate.device, candidate.asset_id, candidate.kind,
                 candidate.name_raw, candidate.name_clean, candidate.distance,
                 candidate.score, int(candidate.shared_alias), avail,
                 int(pair in device_asset_pairs)),
            )

        for row in plan.plans:
            candidate_json = (
                json.dumps(row.candidate.as_dict(), ensure_ascii=False, sort_keys=True)
                if row.candidate else None
            )
            alternative_json = (
                json.dumps([c.as_dict() for c in row.alternatives],
                           ensure_ascii=False, sort_keys=True)
                if row.alternatives else None
            )
            alternative_pairs_json = (
                json.dumps(dict(sorted(row.alternative_pairs.items())),
                           ensure_ascii=False, sort_keys=True)
                if row.alternative_pairs else None
            )
            conn.execute(
                "INSERT INTO match_devices(run_id, device, clean_name, status, decision, "
                "asset_id, matched_by, score, candidate_json, alternative_json, "
                "alternative_asset_ids, alternative_total, alternative_pairs_json, "
                "gap, unique_flag, displaced_json, gap_rule, reason, reason_detail) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, row.device, row.clean, row.status, row.decision,
                 row.asset_id, row.matched_by, row.score, candidate_json,
                 alternative_json, json.dumps(row.alternative_asset_ids,
                                              ensure_ascii=False),
                 row.alternative_total, alternative_pairs_json,
                 row.gap, int(row.unique),
                 json.dumps(row.displaced_devices, ensure_ascii=False),
                 row.gap_rule, row.reason, row.reason_detail),
            )
            for rank, candidate in enumerate(row.alternatives):
                conn.execute(
                    "INSERT INTO match_alternatives(run_id, device, rank, asset_id, "
                    "score, candidate_json) VALUES (?, ?, ?, ?, ?, ?)",
                    (run_id, row.device, rank, candidate.asset_id, candidate.score,
                     json.dumps(candidate.as_dict(), ensure_ascii=False, sort_keys=True)),
                )

        for device, asset_id in plan.human.confirmations:
            conn.execute(
                "INSERT OR IGNORE INTO match_human_constraints(run_id, kind, device, "
                "asset_id, note) VALUES (?, 'CONFIRM', ?, ?, ?)",
                (run_id, device, asset_id, "人工确认（先占用两端资源）"),
            )
        for device, asset_id in plan.human.bans:
            conn.execute(
                "INSERT OR IGNORE INTO match_human_constraints(run_id, kind, device, "
                "asset_id, note) VALUES (?, 'BAN', ?, ?, ?)",
                (run_id, device, asset_id, "人工禁止（所有求解均有效）"),
            )

        return run_id

    # --- 查询 -------------------------------------------------------------
    def list_runs(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM match_runs ORDER BY run_id"
        ).fetchall()
        return [self._run_row(row) for row in rows]

    def get_run(self, run_id: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM match_runs WHERE run_id = ?", (run_id,),
        ).fetchone()
        if row is None:
            return None
        return self._run_row(row)

    def _run_row(self, row) -> dict:
        return {
            "run_id": row["run_id"],
            "created_at": row["created_at"],
            "scope": row["scope"],
            "min_score": row["min_score"],
            "margin": row["margin"],
            "inventory_path": row["inventory_path"],
            "inventory_sha256": row["inventory_sha256"],
            "inventory_revision": row["inventory_revision"],
            "asset_count": row["asset_count"],
            "device_count": row["device_count"],
            "optimal_total": row["optimal_total"],
            "equivalent_optima": row["equivalent_optima"],
            "assignment": json.loads(row["assignment_json"]),
            "human": json.loads(row["human_json"]),
            "inventory_snapshot": json.loads(row["inventory_json"]),
        }

    def run_devices(self, run_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM match_devices WHERE run_id = ? ORDER BY device",
            (run_id,),
        ).fetchall()
        out = []
        for row in rows:
            out.append({
                "device": row["device"],
                "clean_name": row["clean_name"],
                "status": row["status"],
                "decision": row["decision"],
                "matched_asset_id": row["asset_id"],
                "matched_by": row["matched_by"],
                "score": row["score"],
                "candidate": (json.loads(row["candidate_json"])
                              if row["candidate_json"] else None),
                "alternatives": (json.loads(row["alternative_json"])
                                 if row["alternative_json"] else []),
                "alternative_asset_ids": json.loads(row["alternative_asset_ids"]),
                "alternative_total": row["alternative_total"],
                "alternative_pairs": (json.loads(row["alternative_pairs_json"])
                                      if row["alternative_pairs_json"] else None),
                "gap": row["gap"],
                "unique": bool(row["unique_flag"]),
                "displaced_devices": json.loads(row["displaced_json"] or "[]"),
                "gap_rule": row["gap_rule"],
                "reason": row["reason"],
                "reason_detail": row["reason_detail"],
            })
        return out

    def run_device(self, run_id: int, device: str) -> Optional[dict]:
        for item in self.run_devices(run_id):
            if item["device"] == device:
                return item
        return None

    def run_sources(self, run_id: int, device: Optional[str] = None) -> list[dict]:
        sql = "SELECT * FROM match_sources WHERE run_id = ?"
        params: list = [run_id]
        if device is not None:
            sql += " AND device = ?"
            params.append(device)
        sql += " ORDER BY partition, offset"
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def run_candidates(self, run_id: int, device: Optional[str] = None) -> list[dict]:
        sql = "SELECT * FROM match_candidates WHERE run_id = ?"
        params: list = [run_id]
        if device is not None:
            sql += " AND device = ?"
            params.append(device)
        sql += " ORDER BY device, asset_id"
        out = []
        for row in self.conn.execute(sql, params).fetchall():
            item = dict(row)
            item["shared_alias"] = bool(item["shared_alias"])
            item["used"] = bool(item["used"])
            out.append(item)
        return out

    def run_constraints(self, run_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM match_human_constraints WHERE run_id = ? "
            "ORDER BY kind, device, asset_id",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def run_alternatives(self, run_id: int, device: Optional[str] = None) -> list[dict]:
        sql = "SELECT * FROM match_alternatives WHERE run_id = ?"
        params: list = [run_id]
        if device is not None:
            sql += " AND device = ?"
            params.append(device)
        sql += " ORDER BY device, rank"
        out = []
        for row in self.conn.execute(sql, params).fetchall():
            item = dict(row)
            item["candidate"] = json.loads(item.pop("candidate_json"))
            out.append(item)
        return out
