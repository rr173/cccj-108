#!/usr/bin/env python3
"""生成离线实体匹配演示批文件（真实 Avro 二进制，base64）。

设备名不是手填进 events 的：这里把每个设备名编码成
``writer_signal_v1`` 的真实 datum，再由 ``ingest`` 走接入链路落库。
用法：
    python3 -I examples/make_entity_batch.py [输出目录]
默认输出到 ./var/entity_cli。
"""

from __future__ import annotations

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


def encode(schema: dict, record: dict) -> str:
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, fastavro.parse_schema(schema), record)
    return base64.b64encode(bio.getvalue()).decode("ascii")


def device_packet(partition: str, offset: int, schema: dict,
                  device: str, event_ms: int) -> dict:
    return {
        "partition": partition,
        "offset": offset,
        "writer_schema_id": "w-signal-v1",
        "encoding": "base64",
        "datum": encode(schema, {
            "event_id": f"{partition}-{offset}",
            "device": device,
            "event_ms": event_ms,
            "signal_kind": "TEMPERATURE",
            "value": 90.0,
            "source": {"channel": "plant-bus", "ref": None},
        }),
    }


def main() -> None:
    out_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        ROOT, "var", "entity_cli"
    )
    os.makedirs(out_dir, exist_ok=True)
    schema = json.load(open(
        os.path.join(ROOT, "contracts", "writer_signal_v1.json"), encoding="utf-8"
    ))
    devices = [
        "冷却泵－ＰＡ－１００",   # 全角 + 全角连字符
        "冷却泵 PA-101X",
        "冷却泵 PA-100B",
        "冷却泵 PA-900",
        "循环风机 ＦＡＮ－１",   # 全角
        "送风机",               # 两个资产共享的别名
        "循环泵",
        "空压机 XZ-77",
        "空压机三号",
        "···",                  # 清理后空串
        "冷却泵 XZ-77",         # 无候选
    ]
    packets = [
        device_packet("factory-a", i, schema, device, 1000 + i)
        for i, device in enumerate(devices)
    ]
    path = os.path.join(out_dir, "batch.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"packets": packets}, fh, ensure_ascii=False, indent=2)
    print(path)


if __name__ == "__main__":
    main()
