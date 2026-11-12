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
