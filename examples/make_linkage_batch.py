#!/usr/bin/env python3
"""生成识别器演示用的批文件（真实 Avro 二进制，base64）。

与 run_linkage_demo.py 的步骤 2 相同的第一批数据，方便脱离 Python 编排、
用纯 CLI 手工走一遍。用法：
    python3 -I examples/make_linkage_batch.py
"""

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


def signal(schema, device, event_ms, kind, value, tag="s"):
    return encode(schema, {
        "event_id": tag,
        "device": device,
        "event_ms": event_ms,
        "signal_kind": kind,
        "value": value,
        "source": {"channel": "plant-bus", "ref": None},
    })


def main():
    out_dir = os.path.join(ROOT, "var", "linkage_cli")
    os.makedirs(out_dir, exist_ok=True)
    w_sig = json.load(open(os.path.join(ROOT, "contracts", "writer_signal_v1.json"),
                          encoding="utf-8"))
    w1 = json.load(open(os.path.join(ROOT, "contracts", "writer_v1.json"),
                       encoding="utf-8"))

    batch = {"packets": [
        {"partition": "factory-a", "offset": 0, "writer_schema_id": "w-signal-v1",
         "encoding": "base64",
         "datum": signal(w_sig, "dev-backfill", 500_000, "PRESSURE", 12.0, "early")},
        {"partition": "factory-a", "offset": 1, "writer_schema_id": "w-signal-v1",
         "encoding": "base64",
         "datum": signal(w_sig, "dev-backfill", 10_000, "TEMPERATURE", 95.0, "late")},
        {"partition": "factory-a", "offset": 2, "writer_schema_id": "w-event-v1",
         "encoding": "base64",
         "datum": encode(w1, {"event_id": "legacy-order", "amount": 5, "note": None,
                              "source": {"channel": "web", "ref": None}})},
        {"partition": "factory-a", "offset": 3, "writer_schema_id": "w-signal-v1",
         "encoding": "base64",
         "datum": signal(w_sig, "dev-backfill", 1_500, "PRESSURE", None, "null")},
        {"partition": "factory-b", "offset": 0, "writer_schema_id": "w-signal-v1",
         "encoding": "base64",
         "datum": signal(w_sig, "dev-cross", 20_000, "TEMPERATURE", 88.0, "cross")},
        {"partition": "factory-b", "offset": 1, "writer_schema_id": "w-signal-v1",
         "encoding": "base64",
         "datum": signal(w_sig, "dev-cross", 615_000, "PRESSURE", 18.0, "cross")},
    ]}
    path = os.path.join(out_dir, "batch.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(batch, fh, ensure_ascii=False, indent=2)
    print(path)


if __name__ == "__main__":
    main()
