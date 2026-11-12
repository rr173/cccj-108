"""命令行入口。

用法示例
--------
初始化与契约管理：
  python -m avro_landing.cli init --registry ./var/registry --db ./var/landing.db
  python -m avro_landing.cli register-writer --registry ./var/registry \
      --id w-event-v1 --file ./contracts/writer_v1.json
  python -m avro_landing.cli publish-reader --registry ./var/registry \
      --file ./contracts/reader_v1.json        # 预检通过才发布，返回版本号
  python -m avro_landing.cli activate-reader --registry ./var/registry --version 1
  python -m avro_landing.cli check-compat --registry ./var/registry \
      --writer w-event-v1 --reader 1

运行：
  python -m avro_landing.cli ingest --registry ./var/registry --db ./var/landing.db \
      --batch ./var/batch.json
  python -m avro_landing.cli quarantine --db ./var/landing.db [--partition p] ...
  python -m avro_landing.cli reprocess --db ./var/landing.db --registry ./var/registry \
      --partition p --from-offset 5 --to-offset 9
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path

from .adapter import LandingAdapter, Packet, SimulatedCrash
from .compat import UnsupportedAvroStructure
from .linkage import LinkageRecognizer, SimulatedCrash as LinkageSimulatedCrash
from .linkage_audit import compare as compare_linkage
from .entity_audit import audit_run as audit_entity_run
from .entity_match import MatchParams, PlanRejected
from .entity_names import MatchInputError, inventory_report, load_inventory
from .entity_run import explain_device, explain_run, parse_human, run_matching
from .entity_store import MatchRunStore
from .linkage_store import (
    LinkageConfigurationError,
    LinkageStore,
    LinkageWatermarkError,
)
from .registry import RegistryError, SchemaRegistry
from .store import EventStore


def _load_json(path: str | Path) -> dict:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _decode_datum(encoding: str, data: str) -> bytes:
    if encoding == "base64":
        return base64.b64decode(data)
    if encoding == "hex":
        return bytes.fromhex(data)
    raise SystemExit(f"不支持的 datum 编码: {encoding}（支持 base64/hex）")


# ---------------------------------------------------------------------- #
# 子命令
# ---------------------------------------------------------------------- #
def cmd_init(args) -> None:
    registry = SchemaRegistry(args.registry)
    registry.init()
    store = EventStore(args.db)
    store.connect()
    store.close()
    _print({"ok": True, "registry": args.registry, "db": args.db})


def cmd_register_writer(args) -> None:
    registry = SchemaRegistry(args.registry)
    schema = _load_json(args.file)
    try:
        registry.register_writer(args.id, schema)
    except (RegistryError, UnsupportedAvroStructure) as exc:
        reason = getattr(exc, "reason", str(exc))
        _print({"ok": False, "error": reason})
        raise SystemExit(2)
    _print({"ok": True, "registered_writer": args.id})


def cmd_publish_reader(args) -> None:
    registry = SchemaRegistry(args.registry)
    schema = _load_json(args.file)
    try:
        precheck = registry.precheck(schema)
    except UnsupportedAvroStructure as exc:
        _print({"ok": False, "stage": "shape", "error": exc.reason})
        raise SystemExit(2)

    blocked = {wid: r.reason for wid, r in precheck.items() if not r.compatible}
    if blocked and not args.force:
        _print({
            "ok": False,
            "stage": "precheck",
            "error": "存在不兼容的已注册 writer，拒绝发布（--force 可跳过）",
            "incompatible": blocked,
        })
        raise SystemExit(2)

    version = registry.publish_reader(schema)
    _print({"ok": True, "published_reader_version": version,
            "precheck": {wid: {"compatible": r.compatible, "reason": r.reason}
                         for wid, r in precheck.items()}})


def cmd_activate_reader(args) -> None:
    registry = SchemaRegistry(args.registry)
    try:
        registry.activate_reader(args.version)
    except RegistryError as exc:
        _print({"ok": False, "error": str(exc)})
        raise SystemExit(2)
    _print({"ok": True, "active_reader": args.version})


def cmd_check_compat(args) -> None:
    registry = SchemaRegistry(args.registry)
    result = registry.check_pair(args.writer, args.reader)
    _print({
        "writer_id": args.writer,
        "reader_version": args.reader,
        "direction": "writer -> reader",
        "compatible": result.compatible,
        "unsupported_structure": result.unsupported,
        "reasons": result.reasons,
    })
    if not result.compatible:
        raise SystemExit(1)


def _load_packets(batch_path: str) -> list[Packet]:
    doc = _load_json(batch_path)
    packets: list[Packet] = []
    for item in doc["packets"]:
        packets.append(Packet(
            partition=item["partition"],
            offset=int(item["offset"]),
            writer_schema_id=item["writer_schema_id"],
            datum=_decode_datum(item.get("encoding", "base64"), item["datum"]),
        ))
    return packets


def cmd_ingest(args) -> None:
    registry = SchemaRegistry(args.registry)
    store = EventStore(args.db)
    crash_hook = None
    if args.crash_before_commit:
        def crash_hook(stage: str, partition: str, offset: int) -> None:
            if stage == "before_commit":
                raise SimulatedCrash(
                    f"模拟进程退出：partition={partition} offset={offset} "
                    f"已写入但未 COMMIT，事务回滚"
                )
    adapter = LandingAdapter(registry, store, crash_hook=crash_hook)
    packets = _load_packets(args.batch)
    try:
        results = adapter.ingest_batch(packets)
    except SimulatedCrash as exc:
        store.close()
        _print({"ok": False, "simulated_crash": str(exc),
                "note": "进程已退出，事务回滚；重传该包将幂等重做"})
        raise SystemExit(3)
    out = [{
        "partition": r.partition, "offset": r.offset, "status": r.status,
        "reason_code": r.reason_code, "diagnosis": r.diagnosis,
        "confirmed_offset": r.confirmed_offset,
    } for r in results]
    store.close()
    _print({"ok": True, "results": out,
            "checkpoints": [dict(r) for r in EventStore(args.db).list_checkpoints()]})


def cmd_quarantine(args) -> None:
    store = EventStore(args.db)
    rows = store.list_quarantine(
        partition=args.partition, offset_from=args.from_offset,
        offset_to=args.to_offset, status=args.status,
    )
    out = []
    for r in rows:
        out.append({
            "partition": r["partition"], "offset": r["offset"],
            "writer_id": r["writer_id"], "reason_code": r["reason_code"],
            "diagnosis": r["diagnosis"], "reader_version": r["reader_version"],
            "status": r["status"], "first_seen": r["first_seen"],
            "updated_at": r["updated_at"],
            "raw_bytes_hex": r["raw_bytes"].hex(),
            "raw_bytes_len": len(r["raw_bytes"]),
        })
    store.close()
    _print({"count": len(out), "quarantine": out})


def cmd_reprocess(args) -> None:
    registry = SchemaRegistry(args.registry)
    store = EventStore(args.db)
    adapter = LandingAdapter(registry, store)
    results = adapter.reprocess(
        partition=args.partition, offset_from=args.from_offset,
        offset_to=args.to_offset,
    )
    out = [{
        "partition": r.partition, "offset": r.offset, "status": r.status,
        "reason_code": r.reason_code, "diagnosis": r.diagnosis,
    } for r in results]
    store.close()
    _print({"ok": True, "reprocessed": len(out), "results": out})


def cmd_events(args) -> None:
    store = EventStore(args.db)
    rows = store.list_events(partition=args.partition)
    out = []
    for r in rows:
        out.append({
            "partition": r["partition"], "offset": r["offset"],
            "writer_id": r["writer_id"], "reader_version": r["reader_version"],
            "payload": json.loads(r["payload_json"]), "landed_at": r["landed_at"],
        })
    store.close()
    _print({"count": len(out), "events": out})


def cmd_attempts(args) -> None:
    store = EventStore(args.db)
    rows = store.list_attempts(args.partition, args.offset)
    store.close()
    _print({"attempts": [dict(r) for r in rows]})


def cmd_conflicts(args) -> None:
    store = EventStore(args.db)
    rows = store.list_conflicts()
    store.close()
    _print({"conflicts": [dict(r) for r in rows]})


# ---------------------------------------------------------------------- #
# 离线实体匹配
# ---------------------------------------------------------------------- #
def _open_match(args) -> tuple[EventStore, MatchRunStore]:
    store = EventStore(args.db)
    store.connect()
    return store, MatchRunStore(store)


def _print_entity_plan(result) -> None:
    plan = result["plan"]
    body = plan.as_dict()
    body["run_id"] = result["run_id"]
    body["previous_run_ids"] = result["previous_run_ids"]
    body["inventory"] = {
        "path": result["inventory"].source_path,
        "sha256": result["inventory"].sha256,
        "revision": result["inventory"].revision,
        "asset_count": len(result["inventory"].assets),
    }
    _print({"ok": True, **body})


def cmd_entity_inventory_check(args) -> None:
    try:
        inventory = load_inventory(args.inventory)
    except MatchInputError as exc:
        _print({"ok": False, "inventory": args.inventory, **exc.as_dict()})
        raise SystemExit(2)
    report = inventory_report(inventory)
    _print(report)
    if args.strict_ambiguity and (
        report["shared_aliases"] or report["duplicate_formal_names"]
    ):
        _print({
            "ok": False,
            "error": "台账存在共享别名或重复正式名（--strict-ambiguity），"
                     "按歧义拒绝",
            "shared_aliases": report["shared_aliases"],
            "duplicate_formal_names": report["duplicate_formal_names"],
        })
        raise SystemExit(2)


def cmd_entity_match_run(args) -> None:
    if args.min_score < 0 or args.min_score > 100:
        _print({"ok": False, "errors": [{
            "code": "INVALID_MIN_SCORE",
            "detail": f"min_score 必须在 0..100 之间，收到 {args.min_score}",
        }]})
        raise SystemExit(2)
    if args.margin < 0:
        _print({"ok": False, "errors": [{
            "code": "INVALID_MARGIN",
            "detail": f"margin 必须是非负整数，收到 {args.margin}",
        }]})
        raise SystemExit(2)
    try:
        inventory = load_inventory(args.inventory)
        human = parse_human(args.confirm, args.ban)
        params = MatchParams(min_score=args.min_score, margin=args.margin)
    except (MatchInputError, PlanRejected) as exc:
        errors = exc.errors if hasattr(exc, "errors") else [{
            "code": "INVALID_INPUT", "detail": str(exc),
        }]
        _print({"ok": False, "errors": errors})
        raise SystemExit(2)

    store = EventStore(args.db)
    store.connect()
    try:
        result = run_matching(
            store, inventory=inventory, params=params, human=human,
            scope=args.scope,
        )
    except PlanRejected as exc:
        store.close()
        _print({"ok": False, **exc.as_dict()})
        raise SystemExit(2)
    store.close()
    _print_entity_plan(result)


def cmd_entity_run_explain(args) -> None:
    store, match_store = _open_match(args)
    try:
        report = explain_run(match_store, args.run_id, device=args.device)
    finally:
        store.close()
    _print(report)
    if not report.get("ok"):
        raise SystemExit(1)


def cmd_entity_runs(args) -> None:
    store, match_store = _open_match(args)
    runs = match_store.list_runs()
    store.close()
    _print({
        "ok": True,
        "count": len(runs),
        "runs": [
            {
                "run_id": run["run_id"],
                "created_at": run["created_at"],
                "scope": run["scope"],
                "params": {"min_score": run["min_score"], "margin": run["margin"]},
                "inventory_revision": run["inventory_revision"],
                "inventory_sha256": run["inventory_sha256"],
                "device_count": run["device_count"],
                "optimal_total": run["optimal_total"],
                "equivalent_optima": run["equivalent_optima"],
            }
            for run in runs
        ],
    })


def cmd_entity_explain(args) -> None:
    store, match_store = _open_match(args)
    if args.run_id is not None:
        report = explain_run(match_store, args.run_id, device=args.device)
        if not report.get("ok"):
            store.close()
            _print(report)
            raise SystemExit(1)
    else:
        if args.device is None:
            store.close()
            raise SystemExit("--device 或 --run-id 至少给一个")
        report = explain_device(match_store, args.device)
    store.close()
    _print(report)


def cmd_entity_reset(args) -> None:
    """清空离线匹配状态；接入层数据（events/ingress/checkpoints）保留。"""
    store = EventStore(args.db)
    store.connect()
    MatchRunStore(store).ensure_schema()
    names = [
        "match_alternatives", "match_candidates", "match_sources",
        "match_devices", "match_human_constraints", "match_runs",
    ]
    with store.transaction() as conn:
        for name in names:
            conn.execute(f"DELETE FROM {name}")
    store.close()
    _print({"ok": True, "cleared": names})


def cmd_entity_run_audit(args) -> None:
    store, match_store = _open_match(args)
    run = match_store.get_run(args.run_id)
    if run is None:
        store.close()
        _print({"ok": False, "error": f"运行 {args.run_id} 不存在"})
        raise SystemExit(1)
    report = audit_entity_run(
        run,
        match_store.run_devices(args.run_id),
        match_store.run_candidates(args.run_id),
        match_store.run_constraints(args.run_id),
    )
    store.close()
    _print(report)
    if not report["matched"]:
        raise SystemExit(1)


# ---------------------------------------------------------------------- #
# 设备联动异常识别器
# ---------------------------------------------------------------------- #
def _parse_partitions(raw: str) -> list[str]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        raise SystemExit("--config 至少需要一个采集分区，逗号分隔")
    return parts


def _open_recognizer(args) -> tuple[EventStore, LinkageRecognizer]:
    store = EventStore(args.db)
    store.connect()
    try:
        recognizer = LinkageRecognizer(store, _parse_partitions(args.config))
    except LinkageConfigurationError as exc:
        store.close()
        _print({"ok": False, "error": str(exc)})
        raise SystemExit(2)
    return store, recognizer


def cmd_linkage_advance(args) -> None:
    declarations: dict[str, int] = {}
    for item in args.watermark:
        if "=" not in item:
            raise SystemExit(f"水位声明格式应为 分区=毫秒，收到 {item!r}")
        partition, _, raw = item.partition("=")
        try:
            declarations[partition.strip()] = int(raw)
        except ValueError:
            raise SystemExit(f"水位必须是整数毫秒，收到 {raw!r}")

    store, recognizer = _open_recognizer(args)
    crash_hook = None
    if args.crash_after_decision:
        def crash_hook(stage: str, partition: str, offset: int) -> None:
            raise LinkageSimulatedCrash(
                f"模拟进程退出：{stage} 已写结论 partition={partition} offset={offset}，"
                "事务未 COMMIT"
            )
    try:
        report = recognizer.advance(declarations, crash_hook=crash_hook)
    except LinkageWatermarkError as exc:
        store.close()
        _print({"ok": False, "error": str(exc)})
        raise SystemExit(2)
    except LinkageSimulatedCrash as exc:
        store.close()
        _print({"ok": False, "simulated_crash": str(exc),
                "note": "事务整体回滚：水位与结论都不会留下部分状态"})
        raise SystemExit(3)
    store.close()
    _print({"ok": True, **report})


def _print_decision(recognizer: LinkageRecognizer, partition: str, offset: int) -> None:
    _print(recognizer.query(partition, offset))


def cmd_linkage_decisions(args) -> None:
    store, recognizer = _open_recognizer(args)
    if args.partition is not None and args.offset is not None:
        _print_decision(recognizer, args.partition, args.offset)
    else:
        _print({"count": len(recognizer.decisions()), "decisions": recognizer.decisions()})
    store.close()


def cmd_linkage_waiting(args) -> None:
    store, recognizer = _open_recognizer(args)
    _print({"waiting": recognizer.waiting()})
    store.close()


def cmd_linkage_sources(args) -> None:
    store, recognizer = _open_recognizer(args)
    _print({"sources": recognizer.sources(args.state)})
    store.close()


def cmd_linkage_watermarks(args) -> None:
    store, recognizer = _open_recognizer(args)
    _print({"ok": True, **recognizer.watermarks()})
    store.close()


def cmd_linkage_audit(args) -> None:
    store, recognizer = _open_recognizer(args)
    report = compare_linkage(store)
    store.close()
    _print(report)
    if not report["matched"]:
        raise SystemExit(1)


def cmd_linkage_reset(args) -> None:
    """清空识别器状态；接入层数据（events/ingress/checkpoints/契约目录）保留。"""
    store = EventStore(args.db)
    store.connect()
    LinkageStore(store).ensure_schema()
    names = [
        "linkage_decision_evidence", "linkage_decisions", "linkage_candidates",
        "linkage_windows", "linkage_sources", "linkage_watermark_events",
        "linkage_watermarks", "linkage_partitions",
    ]
    with store.transaction() as conn:
        for name in names:
            conn.execute(f"DELETE FROM {name}")
    LinkageStore(store).ensure_schema()
    store.close()
    _print({"ok": True, "cleared": names})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="avro_landing",
                                     description="Avro 事件落地适配器")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="初始化契约目录与数据库")
    p.add_argument("--registry", required=True)
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("register-writer", help="注册不可变 writer schema")
    p.add_argument("--registry", required=True)
    p.add_argument("--id", required=True)
    p.add_argument("--file", required=True)
    p.set_defaults(func=cmd_register_writer)

    p = sub.add_parser("publish-reader", help="兼容预检并发布新 reader 版本")
    p.add_argument("--registry", required=True)
    p.add_argument("--file", required=True)
    p.add_argument("--force", action="store_true", help="忽略预检失败强制发布")
    p.set_defaults(func=cmd_publish_reader)

    p = sub.add_parser("activate-reader", help="激活指定 reader 版本")
    p.add_argument("--registry", required=True)
    p.add_argument("--version", type=int, required=True)
    p.set_defaults(func=cmd_activate_reader)

    p = sub.add_parser("check-compat", help="显式 writer->reader 兼容检查")
    p.add_argument("--registry", required=True)
    p.add_argument("--writer", required=True)
    p.add_argument("--reader", type=int, required=True)
    p.set_defaults(func=cmd_check_compat)

    p = sub.add_parser("ingest", help="批量接收事件包")
    p.add_argument("--registry", required=True)
    p.add_argument("--db", required=True)
    p.add_argument("--batch", required=True)
    p.add_argument("--crash-before-commit", action="store_true",
                   help="模拟进程在业务写入与确认之间退出")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("quarantine", help="查看隔离原因与原始字节")
    p.add_argument("--db", required=True)
    p.add_argument("--partition")
    p.add_argument("--from-offset", type=int, dest="from_offset")
    p.add_argument("--to-offset", type=int, dest="to_offset")
    p.add_argument("--status", choices=["OPEN", "RESOLVED"])
    p.set_defaults(func=cmd_quarantine)

    p = sub.add_parser("reprocess", help="按范围用新 reader 再处理隔离项")
    p.add_argument("--registry", required=True)
    p.add_argument("--db", required=True)
    p.add_argument("--partition")
    p.add_argument("--from-offset", type=int, dest="from_offset")
    p.add_argument("--to-offset", type=int, dest="to_offset")
    p.set_defaults(func=cmd_reprocess)

    p = sub.add_parser("events", help="查看业务表事件")
    p.add_argument("--db", required=True)
    p.add_argument("--partition")
    p.set_defaults(func=cmd_events)

    p = sub.add_parser("attempts", help="查看隔离项再处理尝试历史")
    p.add_argument("--db", required=True)
    p.add_argument("--partition", required=True)
    p.add_argument("--offset", type=int, required=True)
    p.set_defaults(func=cmd_attempts)

    p = sub.add_parser("conflicts", help="查看重传冲突")
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_conflicts)

    # --- 离线实体匹配 -------------------------------------------------
    p = sub.add_parser("entity-inventory-check",
                       help="校验资产清单并暴露共享别名/重复正式名")
    p.add_argument("--inventory", required=True)
    p.add_argument("--strict-ambiguity", action="store_true",
                   help="存在共享别名或重复正式名时以退出码 2 拒绝")
    p.set_defaults(func=cmd_entity_inventory_check)

    p = sub.add_parser("entity-match-run",
                       help="从 events 的 device 唯一值与本地台账运行一次全局匹配")
    p.add_argument("--db", required=True)
    p.add_argument("--inventory", required=True)
    p.add_argument("--min-score", type=int, required=True, dest="min_score")
    p.add_argument("--margin", type=int, required=True)
    p.add_argument("--scope", help="盘点范围（按 events.partition 精确匹配）")
    p.add_argument("--confirm", action="append", default=[], metavar="DEVICE->ASSET_ID",
                   help="人工确认的配对，可重复")
    p.add_argument("--ban", action="append", default=[], metavar="DEVICE->ASSET_ID",
                   help="人工禁止的配对，可重复")
    p.set_defaults(func=cmd_entity_match_run)

    p = sub.add_parser("entity-runs", help="列出已保存的匹配运行")
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_entity_runs)

    p = sub.add_parser("entity-run-explain",
                       help="按运行编号查看整份运行或某设备的解释")
    p.add_argument("--db", required=True)
    p.add_argument("--run-id", type=int, required=True, dest="run_id")
    p.add_argument("--device")
    p.set_defaults(func=cmd_entity_run_explain)

    p = sub.add_parser("entity-explain",
                       help="按设备查看历史运行中的解释（不给 --run-id 时全部运行）")
    p.add_argument("--db", required=True)
    p.add_argument("--device")
    p.add_argument("--run-id", type=int, dest="run_id")
    p.set_defaults(func=cmd_entity_explain)

    p = sub.add_parser("entity-reset",
                       help="清空离线匹配运行记录，便于重跑演示（接入数据保留）")
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_entity_reset)

    p = sub.add_parser("entity-run-audit",
                       help="用穷举实现核对已保存运行的分数、唯一性与约束")
    p.add_argument("--db", required=True)
    p.add_argument("--run-id", type=int, required=True, dest="run_id")
    p.set_defaults(func=cmd_entity_run_audit)

    # --- 设备联动异常识别器 -------------------------------------------
    p = sub.add_parser("linkage-advance",
                       help="吸收新落地业务行 + 声明水位 + 结算到期窗口（单事务）")
    p.add_argument("--db", required=True)
    p.add_argument("--config", required=True,
                   help="启动时固定的采集分区集合，逗号分隔（每次必须一致）")
    p.add_argument("--watermark", action="append", default=[], metavar="PARTITION=MS",
                   help="本次水位声明，可重复：--watermark factory-a=610000")
    p.add_argument("--crash-after-decision", action="store_true",
                   help="演示用：写入部分最终结论后、COMMIT 前强制退出（事务回滚）")
    p.set_defaults(func=cmd_linkage_advance)

    p = sub.add_parser("linkage-decisions",
                       help="查询最终结论；给 --partition/--offset 时连来源依据一起查")
    p.add_argument("--db", required=True)
    p.add_argument("--config", required=True)
    p.add_argument("--partition")
    p.add_argument("--offset", type=int)
    p.set_defaults(func=cmd_linkage_decisions)

    p = sub.add_parser("linkage-waiting", help="查询尚未结算的窗口")
    p.add_argument("--db", required=True)
    p.add_argument("--config", required=True)
    p.set_defaults(func=cmd_linkage_waiting)

    p = sub.add_parser("linkage-sources", help="查询来源总账（含迟到旁路与跳过/拒绝原因）")
    p.add_argument("--db", required=True)
    p.add_argument("--config", required=True)
    p.add_argument("--state", choices=["APPLIED", "SKIPPED", "REJECTED", "LATE_BYPASS"])
    p.set_defaults(func=cmd_linkage_sources)

    p = sub.add_parser("linkage-watermarks", help="查询分区水位与全局进度")
    p.add_argument("--db", required=True)
    p.add_argument("--config", required=True)
    p.set_defaults(func=cmd_linkage_watermarks)

    p = sub.add_parser("linkage-audit",
                       help="用独立全量窗口枚举核对结论与来源依据（不匹配退出码 1）")
    p.add_argument("--db", required=True)
    p.add_argument("--config", required=True)
    p.set_defaults(func=cmd_linkage_audit)

    p = sub.add_parser("linkage-reset", help="清空识别器状态，便于重跑演示")
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_linkage_reset)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
