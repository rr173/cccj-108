#!/usr/bin/env python3
"""生成 CLI 演示用的 Avro 二进制批文件（base64 编码的真实 datum）。"""
import base64
import io
import json
import os
import site
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
_user_site = site.getusersitepackages()
if os.path.isdir(_user_site):
    sys.path.insert(0, _user_site)

import fastavro


def encode(schema, record):
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, fastavro.parse_schema(schema), record)
    return base64.b64encode(bio.getvalue()).decode("ascii")


def main():
    out_dir = os.path.join(ROOT, "var", "cli_demo")
    os.makedirs(out_dir, exist_ok=True)
    w1 = json.load(open(os.path.join(ROOT, "contracts", "writer_v1.json"), encoding="utf-8"))
    w2 = json.load(open(os.path.join(ROOT, "contracts", "writer_v2.json"), encoding="utf-8"))

    packets = [
        {"partition": "orders", "offset": 0, "writer_schema_id": "w-event-v1",
         "encoding": "base64",
         "datum": encode(w1, {"event_id": "o-0", "amount": 10, "note": None,
                              "source": {"channel": "web", "ref": None}})},
        {"partition": "orders", "offset": 1, "writer_schema_id": "w-event-v1",
         "encoding": "base64",
         "datum": encode(w1, {"event_id": "o-1", "amount": 20, "note": "vip",
                              "source": {"channel": "web", "ref": "r-2"}})},
        {"partition": "orders", "offset": 2, "writer_schema_id": "w-event-v2",
         "encoding": "base64",
         "datum": encode(w2, {"event_id": "o-2", "amount": 9_000_000_001, "note": None,
                              "source": {"channel": "pos", "ref": None}})},
        {"partition": "orders", "offset": 4, "writer_schema_id": "w-event-v1",
         "encoding": "base64",
         "datum": encode(w1, {"event_id": "o-4", "amount": 40, "note": "gap-after",
                              "source": {"channel": "web", "ref": None}})},
        {"partition": "orders", "offset": 5, "writer_schema_id": "w-ghost",
         "encoding": "base64",
         "datum": encode(w1, {"event_id": "o-5", "amount": 50, "note": "unknown-id",
                              "source": {"channel": "web", "ref": None}})},
        {"partition": "orders", "offset": 6, "writer_schema_id": "w-event-v1",
         "encoding": "hex", "datum": "deadbeef"},  # 坏字节
    ]
    path = os.path.join(out_dir, "batch.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"packets": packets}, fh, ensure_ascii=False, indent=2)

    # 独立新分区批文件：专门用于崩溃注入演示（确保走的是新事件事务路径）
    crash_packets = [{
        "partition": "crash-demo", "offset": 0, "writer_schema_id": "w-event-v1",
        "encoding": "base64",
        "datum": encode(w1, {"event_id": "c-0", "amount": 77, "note": "crash-me",
                             "source": {"channel": "web", "ref": None}}),
    }]
    crash_path = os.path.join(out_dir, "batch_crash.json")
    with open(crash_path, "w", encoding="utf-8") as fh:
        json.dump({"packets": crash_packets}, fh, ensure_ascii=False, indent=2)
    print(path)
    print(crash_path)


if __name__ == "__main__":
    main()
