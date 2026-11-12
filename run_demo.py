#!/usr/bin/env python3
"""端到端可复现演示：用真实 Avro 二进制走完整接收/隔离/再处理流程。

在隔离模式（python -I）下运行：
    python3 -I run_demo.py

演示目录：./var/demo（registry + sqlite + 批文件），可随时删除重跑。
"""
import io
import json
import os
import shutil
import site
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
_user_site = site.getusersitepackages()
if os.path.isdir(_user_site):
    sys.path.insert(0, _user_site)

import fastavro  # noqa: E402

from avro_landing.adapter import (  # noqa: E402
    LandingAdapter, Packet, SimulatedCrash,
)
from avro_landing.registry import SchemaRegistry  # noqa: E402
from avro_landing.store import EventStore  # noqa: E402


DEMO = os.path.join(ROOT, "var", "demo")


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def encode(schema: dict, record: dict) -> bytes:
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, fastavro.parse_schema(schema), record)
    return bio.getvalue()


def dump_checkpoints(store: EventStore) -> None:
    print("checkpoints:", {r["partition"]: r["confirmed_offset"]
                           for r in store.list_checkpoints()})


def main() -> None:
    if os.path.isdir(DEMO):
        shutil.rmtree(DEMO)
    os.makedirs(DEMO)

    registry = SchemaRegistry(os.path.join(DEMO, "registry"))
    registry.init()
    db_path = os.path.join(DEMO, "landing.db")
    store = EventStore(db_path)
    store.connect()

    w1 = json.load(open(os.path.join(ROOT, "contracts", "writer_v1.json"), encoding="utf-8"))
    w2 = json.load(open(os.path.join(ROOT, "contracts", "writer_v2.json"), encoding="utf-8"))
    r1 = json.load(open(os.path.join(ROOT, "contracts", "reader_v1.json"), encoding="utf-8"))
    r2 = json.load(open(os.path.join(ROOT, "contracts", "reader_v2.json"), encoding="utf-8"))
    r3 = json.load(open(os.path.join(ROOT, "contracts", "reader_v3.json"), encoding="utf-8"))

    # --- 契约准备 -------------------------------------------------------
    banner("步骤 0：注册不可变 writer，发布并激活 reader v1")
    registry.register_writer("w-event-v1", w1)
    registry.register_writer("w-event-v2", w2)
    pre = registry.precheck(r1)
    print("reader v1 预检:", {k: v.compatible for k, v in pre.items()},
          "（w-event-v2 因 long->int 被拒，故 v1 不激活给生产，演示用）")
    registry.publish_reader(r1)
    registry.activate_reader(1)

    good_v1 = encode(w1, {
        "event_id": "e-100", "amount": 7, "note": "note-null-branch",
        "source": {"channel": "web", "ref": None},
    })
    good_v1_str = encode(w1, {
        "event_id": "e-101", "amount": 9, "note": "note-string-branch",
        "source": {"channel": "web", "ref": "ref-1"},
    })
    long_event = encode(w2, {
        "event_id": "e-102", "amount": 6_000_000_000, "note": None,
        "source": {"channel": "mobile", "ref": None},
    })
    garbage = b"\x0c\xff\x99\x00garbage"

    adapter = LandingAdapter(registry, store)

    # --- 第一批：正常 + 不兼容 + 坏字节 + 未知 ID ------------------------
    banner("步骤 1：接收 p1 offsets 0..3（含 long->int 不兼容、坏字节、未知 ID）")
    results = adapter.ingest_batch([
        Packet("p1", 0, "w-event-v1", good_v1),
        Packet("p1", 1, "w-event-v2", long_event),       # reader v1 不兼容
        Packet("p1", 2, "w-event-v1", garbage),          # 解码失败
        Packet("p1", 3, "w-unknown-x", good_v1_str),     # 未知 writer
    ])
    for r in results:
        print(f"  offset={r.offset} status={r.status} reason={r.reason_code} diag={r.diagnosis}")
    dump_checkpoints(store)
    print("业务表事件数:", len(store.list_events("p1")),
          " 隔离表事件数:", len(store.list_quarantine(partition="p1")))

    # --- offset 缺口 -----------------------------------------------------
    banner("步骤 2：offset 缺口——p1 先到 5（缺 4），确认位置不得越过缺口")
    r = adapter.ingest_batch([Packet("p1", 5, "w-event-v1", good_v1_str)])
    print("  offset=5 status:", r[0].status)
    dump_checkpoints(store)  # 仍停在 0（1..3 是隔离，也算已持久接收，连续到 3；缺 4）

    # --- 发布 reader v2（先预检）并激活 ----------------------------------
    banner("步骤 3：发布 reader v2——先对全部 writer 预检，通过后激活")
    pre2 = registry.precheck(r2)
    print("reader v2 预检:", {k: {"ok": v.compatible, "reasons": v.reasons}
                              for k, v in pre2.items()})
    assert all(v.compatible for v in pre2.values())
    v2 = registry.publish_reader(r2)
    registry.activate_reader(v2)
    print("已激活 reader v", v2)

    # --- 再处理隔离项 -----------------------------------------------------
    banner("步骤 4：再处理 p1 offset 1..3（新 reader，沿用原始 writer）")
    # 未知 ID 先补注册
    registry.register_writer("w-unknown-x", w1)
    outcomes = adapter.reprocess(partition="p1", offset_from=1, offset_to=3)
    for o in outcomes:
        print(f"  offset={o.offset} status={o.status} reason={o.reason_code}")
    qrows = store.list_quarantine(partition="p1")
    for q in qrows:
        print(f"  隔离行保留: offset={q['offset']} status={q['status']} "
              f"reader_version={q['reader_version']} 旧诊断={q['diagnosis'][:48]}...")
    print("再处理尝试历史:")
    for off in (1, 2, 3):
        for a in store.list_attempts("p1", off):
            print(f"  offset={off} v{a['reader_version']} {a['outcome']}: {a['diagnosis'][:60]}")

    # 重复再处理：只补一次业务结果
    again = adapter.reprocess(partition="p1", offset_from=1, offset_to=3)
    print("重复再处理返回（OPEN 已清空）:", again)
    print("业务表事件数（应仍为 4：offset 0 + 1 + 2 + 3）:", len(store.list_events("p1")))

    # --- 缺口补齐 ---------------------------------------------------------
    banner("步骤 5：补齐缺口 offset 4，确认位置应连续推进过 5")
    r = adapter.ingest_batch([Packet("p1", 4, "w-event-v1", good_v1)])
    print("  offset=4 status:", r[0].status, "confirmed:", r[0].confirmed_offset)
    dump_checkpoints(store)

    # --- 重传与冲突 -------------------------------------------------------
    banner("步骤 6：重传判定——相同字节幂等；字节或 schema ID 改变判冲突")
    r = adapter.ingest_batch([Packet("p1", 0, "w-event-v1", good_v1)])
    print("  相同重传:", r[0].status)
    r = adapter.ingest_batch([Packet("p1", 0, "w-event-v1", good_v1_str)])
    print("  字节改变:", r[0].status, "-", r[0].diagnosis)
    r = adapter.ingest_batch([Packet("p1", 0, "w-event-v2", good_v1)])
    print("  schema 改变:", r[0].status)
    print("冲突表条数:", len(store.list_conflicts()))

    # --- reader v3 alias --------------------------------------------------
    banner("步骤 7：发布 reader v3（note -> remark alias），新事件验证 alias")
    v3 = registry.publish_reader(r3)
    registry.activate_reader(v3)
    r = adapter.ingest_batch([Packet("p1", 6, "w-event-v1", good_v1_str)])
    payload = json.loads(store.list_events("p1")[-1]["payload_json"])
    print("  offset=6 status:", r[0].status, " payload.remark =", payload.get("remark"),
          " note 键存在？", "note" in payload)

    # --- 崩溃恢复 ---------------------------------------------------------
    banner("步骤 8：模拟进程在业务写入与 COMMIT 之间退出")

    def crash(stage, partition, offset):
        raise SimulatedCrash(f"crash at {stage} {partition}/{offset}")

    crash_store = EventStore(db_path)
    crash_adapter = LandingAdapter(registry, crash_store, crash_hook=crash)
    try:
        crash_adapter.ingest_batch([Packet("p3", 0, "w-event-v1", good_v1)])
    except SimulatedCrash as exc:
        print("  进程退出:", exc)
    print("  崩溃后（同一连接视角之外的持久状态）:")
    probe = EventStore(db_path)
    print("    p3 业务事件数:", len(probe.list_events("p3")),
          " p3 checkpoint:", probe.get_checkpoint("p3"),
          " p3 隔离数:", len(probe.list_quarantine(partition="p3")))
    probe.close()
    crash_store.close()

    banner("步骤 9：进程重启，采集端重传 p3/0——幂等重做，共同恢复")
    restarted = EventStore(db_path)
    ok_adapter = LandingAdapter(registry, restarted)
    r = ok_adapter.ingest_batch([Packet("p3", 0, "w-event-v1", good_v1)])
    print("  重传后 status:", r[0].status, "confirmed:", r[0].confirmed_offset)
    print("  p3 业务事件数:", len(restarted.list_events("p3")))
    restarted.close()

    banner("最终状态")
    final = EventStore(db_path)
    dump_checkpoints(final)
    print("业务事件:", [(e["partition"], e["offset"], "v"+str(e["reader_version"]))
                       for e in final.list_events()])
    print("隔离（含 RESOLVED 历史）:",
          [(q["partition"], q["offset"], q["status"], q["reason_code"])
           for q in final.list_quarantine()])
    final.close()
    print("\n演示数据保留在:", DEMO)


if __name__ == "__main__":
    main()
