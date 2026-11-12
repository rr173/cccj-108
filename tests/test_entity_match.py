"""离线实体匹配的回归测试：名称口径、全局求解、人工限制、唯一性、持久化。

覆盖讲解要点：
- NFKC/casefold/去非字母数字、中文保留、空串无效
- 整数 Levenshtein（插入/删除/替换各 1）与 100-10d / 95-10d
- 候选依据保留原名称、清理后名称、正式名/别名身份与距离
- 全局一对一最大权匹配（贪心反例、同分方案计数、禁止边、确认占用）
- 替代解：禁掉当前选择 / 未配对改为强制配对；gap 与 margin 严格大于
- 人工确认冲突整份拒绝（两条确认争用同一端、引用不存在、同时确认又禁止）
- 输入错误不产生半份运行；一致读视图；运行保存与旧解释可查
- 设备名必须经 Avro 接入链路进入 events（演示与测试都不手填 events）
"""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

import fastavro

from avro_landing.adapter import LandingAdapter, Packet, ST_LANDED
from avro_landing.entity_audit import audit_run
from avro_landing.entity_match import (
    AVAIL_ASSET_RESERVED,
    AVAIL_BANNED,
    AVAIL_BELOW_MIN_SCORE,
    DECISION_CONFIRMED,
    DECISION_PENDING,
    DECISION_UNMATCHED,
    DeviceInput,
    DeviceOccurrence,
    HumanConstraints,
    MatchParams,
    PlanRejected,
    evaluating_candidates,
    plan_matching,
)
from avro_landing.entity_names import (
    KIND_ALIAS,
    KIND_FORMAL,
    MatchInputError,
    clean_name,
    levenshtein,
    load_inventory,
    shared_alias_report,
    validate_inventory_document,
)
from avro_landing.entity_run import explain_device, explain_run, parse_human, run_matching
from avro_landing.entity_solver import solve_optimum
from avro_landing.entity_store import MatchRunStore
from avro_landing.registry import SchemaRegistry
from avro_landing.store import EventStore


ROOT = Path(__file__).resolve().parent.parent
CONTRACTS = ROOT / "contracts"
SAMPLES = ROOT / "examples" / "entity_inventory"
W_SIGNAL = json.loads((CONTRACTS / "writer_signal_v1.json").read_text(encoding="utf-8"))
READER_SIGNAL = json.loads((CONTRACTS / "reader_signal_v1.json").read_text(encoding="utf-8"))


def avro_encode(schema: dict, record: dict) -> bytes:
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, fastavro.parse_schema(schema), record)
    return bio.getvalue()


def device_inputs(devices: list[str], *, partition: str = "factory-a",
                  start: int = 0) -> list[DeviceInput]:
    return [
        DeviceInput(
            device=device,
            clean=clean_name(device),
            occurrences=[DeviceOccurrence(
                device=device, partition=partition, offset=start + i,
                writer_id="w-signal-v1", reader_version=1,
            )],
        )
        for i, device in enumerate(devices)
    ]


class NameRulesTest(unittest.TestCase):
    def test_nfkc_casefold_and_character_filter(self):
        self.assertEqual(clean_name("冷却泵－ＰＡ－１００"), "冷却泵pa100")
        self.assertEqual(clean_name("冷却泵 PA-100"), "冷却泵pa100")
        self.assertEqual(clean_name("  Fan_1 "), "fan1")
        # 中文属 Lo，保留
        self.assertEqual(clean_name("设备：甲／乙"), "设备甲乙")
        # 单独的数字也算字母数字类别
        self.assertEqual(clean_name("①"), "1")
        # 只有标点/符号 -> 空串
        self.assertEqual(clean_name("···"), "")
        self.assertEqual(clean_name("％"), "")

    def test_levenshtein_all_operations_cost_one(self):
        self.assertEqual(levenshtein("", ""), 0)
        self.assertEqual(levenshtein("", "abc"), 3)
        self.assertEqual(levenshtein("fan1", "fan2"), 1)      # 替换
        self.assertEqual(levenshtein("fan1", "fan11"), 1)     # 插入
        self.assertEqual(levenshtein("fan11", "fan1"), 1)     # 删除
        self.assertEqual(levenshtein("kitten", "sitting"), 3)

    def test_scores_use_formal_and_alias_base(self):
        candidates = evaluating_candidates(
            ["冷却泵 PA-100", "送风机"],
            load_inventory(SAMPLES / "assets_v1.json").assets.values(),
        )
        formal = [c for c in candidates
                  if c.device == "冷却泵 PA-100" and c.asset_id == "AST-001"][0]
        self.assertEqual(formal.kind, KIND_FORMAL)
        self.assertEqual(formal.score, 100)
        alias = [c for c in candidates if c.device == "送风机"]
        self.assertTrue(alias)
        for item in alias:
            self.assertEqual(item.kind, KIND_ALIAS)
            self.assertEqual(item.score, 95 - 10 * item.distance)


class InventoryValidationTest(unittest.TestCase):
    def test_duplicate_asset_id_rejected(self):
        assets, revision, errors = validate_inventory_document({"assets": [
            {"asset_id": "A", "name": "泵"},
            {"asset_id": "A", "name": "风机"},
        ]})
        self.assertEqual([e["code"] for e in errors], ["DUPLICATE_ASSET_ID"])

    def test_empty_clean_name_and_bad_alias_rejected(self):
        assets, revision, errors = validate_inventory_document({"assets": [
            {"asset_id": "A", "name": "···"},
            {"asset_id": "B", "name": "泵", "aliases": ["---"]},
        ]})
        codes = [e["code"] for e in errors]
        self.assertIn("INVALID_NAME", codes)
        self.assertIn("INVALID_ALIAS", codes)

    def test_shared_alias_is_reported_not_hidden(self):
        assets, revision, errors = validate_inventory_document({"assets": [
            {"asset_id": "A", "name": "泵一", "aliases": ["送风机"]},
            {"asset_id": "B", "name": "泵二", "aliases": ["送风机"]},
        ]})
        self.assertEqual(errors, [])
        report = shared_alias_report(assets)
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]["asset_ids"], ["A", "B"])
        self.assertEqual(report[0]["clean"], "送风机")

    def test_missing_inventory_file_is_input_error(self):
        with self.assertRaises(MatchInputError):
            load_inventory("/nonexistent/inventory.json")


class SolverTest(unittest.TestCase):
    def test_greedy_is_worse_than_global_optimum(self):
        assets = ["AG-001", "AG-002"]
        edges = {
            ("冷却泵 PA-100", "AG-001"): 100,
            ("冷却泵 PA-100", "AG-002"): 80,
            ("冷却泵 PA-1000", "AG-001"): 90,
            ("冷却泵 PA-101X", "AG-001"): 90,
        }
        greedy_total = 100  # 贪心先满足单边最高的 PA-100 -> AG-001，其余落空
        result = solve_optimum(
            ["冷却泵 PA-100", "冷却泵 PA-1000", "冷却泵 PA-101X"], assets, edges
        )
        self.assertEqual(result.total, 170)
        self.assertGreater(result.total, greedy_total)
        self.assertEqual(result.pairs["冷却泵 PA-100"], "AG-002")
        # AG-001 由 PA-1000 / PA-101X 二选一（同分），因此计数为 2
        self.assertEqual(result.equivalent_optima, 2)

    def test_tie_reports_multiple_optima(self):
        edges = {
            ("d1", "A1"): 95, ("d1", "A2"): 95,
            ("d2", "A1"): 95, ("d2", "A2"): 95,
        }
        result = solve_optimum(["d1", "d2"], ["A1", "A2"], edges)
        self.assertEqual(result.total, 190)
        self.assertEqual(result.equivalent_optima, 2)

    def test_forbidden_edge_and_pinned_reservation(self):
        edges = {
            ("d1", "A1"): 100, ("d1", "A2"): 80,
            ("d2", "A1"): 90, ("d2", "A2"): 95,
        }
        self.assertEqual(solve_optimum(["d1", "d2"], ["A1", "A2"], edges).total, 195)
        banned = solve_optimum(
            ["d1", "d2"], ["A1", "A2"], edges, forbidden=[("d2", "A1")],
        )
        self.assertEqual(banned.total, 195)
        self.assertEqual(banned.pairs, {"d1": "A1", "d2": "A2"})
        pinned = solve_optimum(
            ["d1", "d2"], ["A1", "A2"], edges, pinned={"d2": "A1"},
        )
        self.assertEqual(pinned.total, 170)
        self.assertEqual(pinned.pairs, {"d1": "A2", "d2": "A1"})

    def test_required_device_can_be_infeasible(self):
        self.assertIsNone(solve_optimum(
            ["d1", "d2"], ["A1"], {("d1", "A1"): 100}, required=["d2"],
        ))


class PlanningTest(unittest.TestCase):
    def setUp(self):
        self.inv = load_inventory(SAMPLES / "assets_v1.json")

    def plan(self, devices, *, min_score=0, margin=10, confirm=(), ban=()):
        human = HumanConstraints(confirmations=list(confirm), bans=list(ban))
        return plan_matching(
            device_inputs=device_inputs(devices),
            assets=self.inv.assets,
            params=MatchParams(min_score=min_score, margin=margin),
            human=human,
        )

    def test_candidates_keep_auditable_basis(self):
        plan = self.plan(["冷却泵－ＰＡ－１００"])
        row = [p for p in plan.plans if p.device == "冷却泵－ＰＡ－１００"][0]
        basis = row.candidate.as_dict()
        self.assertEqual(basis["device"], "冷却泵－ＰＡ－１００")
        self.assertEqual(basis["name_raw"], "冷却泵 PA-100")
        self.assertEqual(basis["name_clean"], "冷却泵pa100")
        self.assertEqual(basis["kind"], KIND_FORMAL)
        self.assertEqual(basis["distance"], 0)
        self.assertEqual(basis["score"], 100)

    def test_shared_alias_is_flagged_and_not_resolved_by_file_order(self):
        plan = self.plan(["送风机"])
        row = [p for p in plan.plans if p.device == "送风机"][0]
        self.assertTrue(row.candidate.shared_alias)
        self.assertEqual(row.candidate.asset_id, "AST-005")  # 稳定展示顺序
        self.assertEqual(row.alternative_asset_ids, ["AST-004"])
        # 两套同分方案：gap=0 <= margin，因此待确认而不冒充已确认
        self.assertEqual(row.status, "AUTO_PENDING")
        self.assertEqual(row.decision, DECISION_PENDING)
        self.assertEqual(row.gap, 0)
        self.assertFalse(row.unique)

    def test_margin_is_strictly_greater(self):
        tight = self.plan(["冷却泵 PA-101X"], margin=9)
        loose = self.plan(["冷却泵 PA-101X"], margin=10)
        row_tight = tight.plans[0]
        row_loose = loose.plans[0]
        self.assertEqual(row_tight.gap, 10)
        self.assertEqual(row_tight.status, "AUTO_CONFIRMED")
        self.assertEqual(row_loose.status, "AUTO_PENDING")
        self.assertEqual(row_loose.decision, DECISION_PENDING)

    def test_confirmation_reserves_both_ends_and_reruns_rest(self):
        plan = self.plan(["循环风机 ＦＡＮ－１", "送风机"],
                         confirm=[("送风机", "AST-004")])
        rows = {p.device: p for p in plan.plans}
        self.assertEqual(rows["送风机"].status, "HUMAN_CONFIRMED")
        self.assertEqual(rows["送风机"].decision, DECISION_CONFIRMED)
        self.assertEqual(rows["循环风机 ＦＡＮ－１"].asset_id, "AST-005")
        self.assertEqual(rows["循环风机 ＦＡＮ－１"].score, 90)
        self.assertEqual(
            plan.availability["循环风机 ＦＡＮ－１"]["AST-004"],
            AVAIL_ASSET_RESERVED,
        )

    def test_confirmation_may_override_min_score(self):
        # AST-002 与设备差 1，得分 90；把门槛提高到 95 仍允许人工确认
        plan = self.plan(["冷却泵 PA-101X"], min_score=95,
                         confirm=[("冷却泵 PA-101X", "AST-002")])
        self.assertEqual(plan.plans[0].status, "HUMAN_CONFIRMED")
        self.assertEqual(plan.plans[0].score, 90)

    def test_ban_changes_optimal_assignment_and_stays_in_alternatives(self):
        plan = self.plan(["冷却泵－ＰＡ－１００"], ban=[("冷却泵－ＰＡ－１００", "AST-001")])
        row = plan.plans[0]
        self.assertNotEqual(row.asset_id, "AST-001")
        self.assertEqual(plan.availability[row.device]["AST-001"], AVAIL_BANNED)

    def test_below_min_score_candidate_is_explained(self):
        plan = self.plan(["冷却循环"], min_score=95)
        row = plan.plans[0]
        self.assertEqual(row.status, "UNMATCHED")
        self.assertEqual(row.decision, DECISION_UNMATCHED)
        self.assertEqual(row.reason, "ALL_CANDIDATES_BELOW_MIN_SCORE")
        self.assertEqual(plan.availability["冷却循环"]["AST-003"], AVAIL_BELOW_MIN_SCORE)

    def test_empty_and_uncandidate_devices_get_reasons(self):
        plan = self.plan(["···", "冷却泵 XZ-77"])
        rows = {p.device: p for p in plan.plans}
        self.assertEqual(rows["···"].reason, "EMPTY_DEVICE_NAME")
        self.assertEqual(rows["冷却泵 XZ-77"].reason, "NO_QUALIFYING_CANDIDATE")

    def test_unmatched_device_reports_forced_alternative(self):
        plan = self.plan([
            "冷却泵 PA-100", "冷却泵 PA-1000", "冷却泵 PA-100B", "冷却泵 PA-101X",
        ])
        rows = {p.device: p for p in plan.plans}
        row = rows["冷却泵 PA-1000"]
        self.assertIsNone(row.asset_id)
        self.assertEqual(row.reason, "UNMATCHED_BY_GLOBAL_OPTIMUM")
        # 强制它配上时必须挤掉别人，替代总分下降
        self.assertEqual(row.alternative_asset_ids, ["AST-001"])
        self.assertEqual(row.alternative_total, plan.assignment.total)
        self.assertEqual(row.gap, 0)

    def test_conflict_cases_reject_whole_request(self):
        cases = {
            "两条确认争用同一设备": [("送风机", "AST-004"), ("送风机", "AST-005")],
            "两条确认争用同一资产": [("送风机", "AST-004"),
                                     ("循环风机 ＦＡＮ－１", "AST-004")],
            "不存在的设备": [("幽灵设备", "AST-001")],
            "不存在的资产": [("送风机", "AST-999")],
        }
        for label, confirm in cases.items():
            with self.assertRaises(PlanRejected, msg=label) as ctx:
                self.plan(["送风机", "循环风机 ＦＡＮ－１"], confirm=confirm)
            self.assertTrue(ctx.exception.errors)
        with self.assertRaises(PlanRejected) as ctx:
            self.plan(["空压机三号"],
                      confirm=[("空压机三号", "AST-006")],
                      ban=[("空压机三号", "AST-006")])
        self.assertEqual(ctx.exception.errors[0]["code"],
                         "CONFIRMATION_AND_BAN_CONFLICT")

    def test_plan_matches_bruteforce_reference(self):
        devices = ["冷却泵 PA-100", "冷却泵 PA-1000", "冷却泵 PX-900"]
        plan = self.plan(devices, margin=0)
        report = audit_run(
            {
                "params": {"min_score": 0, "margin": 0},
                "optimal_total": plan.assignment.total,
                "equivalent_optima": plan.assignment.equivalent_optima,
                "inventory_snapshot": {"assets": [
                    asset.as_dict() for asset in self.inv.assets.values()
                ]},
            },
            [p.as_dict() for p in plan.plans],
            [c.as_dict() for c in plan.candidates],
            [{"kind": "CONFIRM", "device": d, "asset_id": a}
             for d, a in plan.human.confirmations]
            + [{"kind": "BAN", "device": d, "asset_id": a}
               for d, a in plan.human.bans],
        )
        self.assertTrue(report["matched"], report["problems"])


class RunStoreTest(unittest.TestCase):
    """真实 Avro 接入 -> 一致读视图 -> 持久化运行 -> 解释与穷举核对。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.registry = SchemaRegistry(self.tmp / "registry")
        self.registry.init()
        self.store = EventStore(self.tmp / "landing.db")
        self.store.connect()
        self.registry.register_writer("w-signal-v1", W_SIGNAL)
        self.registry.publish_reader(READER_SIGNAL)
        self.registry.activate_reader(1)
        self.adapter = LandingAdapter(self.registry, self.store)
        self.match_store = MatchRunStore(self.store)
        self._offset = 0
        self._id = 0

    def tearDown(self):
        self.store.close()
        self._tmp.cleanup()

    def land(self, device: str, partition: str = "factory-a") -> None:
        self._id += 1
        raw = avro_encode(W_SIGNAL, {
            "event_id": f"e-{self._id}",
            "device": device,
            "event_ms": 1_000 + self._offset,
            "signal_kind": "TEMPERATURE",
            "value": 90.0,
            "source": {"channel": "bus", "ref": None},
        })
        result = self.adapter.ingest_one(
            Packet(partition, self._offset, "w-signal-v1", raw)
        )
        self.assertEqual(result.status, ST_LANDED)
        self._offset += 1

    def run_for(self, inventory_path, **kwargs):
        inventory = load_inventory(inventory_path)
        human = parse_human(kwargs.pop("confirm", []), kwargs.pop("ban", []))
        params = MatchParams(**{k: kwargs.pop(k) for k in ("min_score", "margin")})
        return run_matching(
            self.store, inventory=inventory, params=params, human=human,
            **kwargs,
        )

    def test_run_saves_full_evidence_and_old_runs_stay_queryable(self):
        for device in ["冷却泵 PA-100", "冷却泵 PA-1000", "送风机", "···"]:
            self.land(device)
        result = self.run_for(SAMPLES / "assets_v1.json", min_score=0, margin=10)
        run_id = result["run_id"]
        run = self.match_store.get_run(run_id)
        self.assertEqual(run["scope"], None)
        self.assertEqual(run["min_score"], 0)
        self.assertEqual(run["margin"], 10)
        self.assertEqual(run["inventory_revision"], "2026-11-12-inventory-v1")

        device_rows = self.match_store.run_devices(run_id)
        self.assertEqual(len(device_rows), 4)
        sources = self.match_store.run_sources(run_id, "送风机")
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0]["partition"], "factory-a")
        candidate_rows = self.match_store.run_candidates(run_id, "冷却泵 PA-1000")
        self.assertTrue(candidate_rows)
        # 候选依据：原名称、清理后名称、身份、距离、分数
        for row in candidate_rows:
            self.assertEqual(
                set(row) & {"kind", "name_raw", "name_clean", "distance", "score"},
                {"kind", "name_raw", "name_clean", "distance", "score"},
            )
        constraints = self.match_store.run_constraints(run_id)
        self.assertEqual(constraints, [])
        explained = explain_run(self.match_store, run_id)
        self.assertEqual(explained["assignment"]["total"], run["optimal_total"])

        # 台账更正后再跑一次；旧运行解释仍读自己的快照
        for device in ["冷却泵 PA-101X", "冷却泵 PA-100E"]:
            self.land(device)
        result2 = self.run_for(SAMPLES / "assets_v2.json", min_score=0, margin=10)
        run2 = self.match_store.get_run(result2["run_id"])
        self.assertEqual(run2["inventory_revision"], "2026-11-12-inventory-v2")
        explained_old = explain_run(self.match_store, run_id, "冷却泵 PA-1000")
        self.assertEqual(explained_old["run"]["inventory"]["revision"],
                         "2026-11-12-inventory-v1")
        self.assertIsNotNone(explained_old["device"]["matched_asset_id"])
        history = explain_device(self.match_store, "冷却泵 PA-100E")
        self.assertEqual(history["run_count"], 1)
        self.assertEqual(history["runs"][0]["run_id"], result2["run_id"])

    def test_rejected_plan_writes_no_run(self):
        for device in ["送风机", "循环风机 ＦＡＮ－１"]:
            self.land(device)
        before = len(self.match_store.list_runs())
        with self.assertRaises(PlanRejected):
            self.run_for(
                SAMPLES / "assets_v1.json", min_score=0, margin=10,
                confirm=["送风机->AST-004", "循环风机 ＦＡＮ－１->AST-004"],
            )
        self.assertEqual(len(self.match_store.list_runs()), before)
        # 台账输入错误同样不产生运行
        bad = self.tmp / "bad.json"
        bad.write_text(json.dumps({"assets": [{"asset_id": "A", "name": "×"}]}),
                       encoding="utf-8")
        with self.assertRaises(MatchInputError):
            self.run_for(bad, min_score=0, margin=10)
        self.assertEqual(len(self.match_store.list_runs()), before)

    def test_devices_come_from_events_not_manual_input(self):
        self.land("送风机")
        result = self.run_for(SAMPLES / "assets_v1.json", min_score=0, margin=10)
        devices = {row["device"] for row in
                   self.match_store.run_devices(result["run_id"])}
        self.assertEqual(devices, {"送风机"})
        # 同一设备多次出现：来源坐标全部保存
        self.land("送风机", partition="factory-b")
        result2 = self.run_for(SAMPLES / "assets_v1.json", min_score=0, margin=10)
        self.assertEqual(len(self.match_store.run_sources(result2["run_id"], "送风机")), 2)

    def test_scope_limits_events_partition(self):
        self.land("送风机", partition="factory-a")
        self.land("循环泵", partition="factory-b")
        result = self.run_for(SAMPLES / "assets_v1.json", min_score=0,
                              margin=10, scope="factory-b")
        devices = {row["device"] for row in
                   self.match_store.run_devices(result["run_id"])}
        self.assertEqual(devices, {"循环泵"})

    def test_saved_run_is_confirmed_by_bruteforce_audit(self):
        for device in ["冷却泵－ＰＡ－１００", "冷却泵 PA-100B", "送风机",
                       "循环泵", "冷却泵 XZ-77"]:
            self.land(device)
        result = self.run_for(SAMPLES / "assets_v1.json", min_score=0, margin=10)
        run_id = result["run_id"]
        report = audit_run(
            self.match_store.get_run(run_id),
            self.match_store.run_devices(run_id),
            self.match_store.run_candidates(run_id),
            self.match_store.run_constraints(run_id),
        )
        self.assertTrue(report["matched"], report["problems"])

    def test_persistence_is_atomic_on_crash_before_commit(self):
        self.land("送风机")
        before = len(self.match_store.list_runs())
        inventory = load_inventory(SAMPLES / "assets_v1.json")
        plan = plan_matching(
            device_inputs=device_inputs(["送风机"]),
            assets=inventory.assets,
            params=MatchParams(0, 10),
            human=HumanConstraints(),
        )
        with self.assertRaises(RuntimeError):
            with self.store.transaction() as conn:
                MatchRunStore(self.store).save_run(conn, plan, inventory)
                raise RuntimeError("simulated crash before commit")
        # 事务回滚：运行、候选、来源一条都不留
        self.assertEqual(len(self.match_store.list_runs()), before)
        probe = MatchRunStore(EventStore(self.tmp / "landing.db"))
        self.assertEqual(probe.conn.execute(
            "SELECT COUNT(*) AS n FROM match_devices").fetchone()["n"], 0)
        probe.conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
