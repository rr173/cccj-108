"""设备联动异常识别规则常量与信号载荷解析。

规则口径（工厂给定，不在别处另立口径）
------------------------------------
- 温度读数 >= 80 开启观察窗口 A，窗口右端 = A.event_ms + 600000。
- 候选低压 B：同设备、业务时间落在 (A, A+600000]、压力值 <= 20。
- RESET：同设备、业务时间落在闭区间 [A, A+600000] 即取消整个窗口。
- 多个 A 独立判断，可以引用同一个 B。
- 窗口右端 < 全局水位时才允许结算（严格大于，见 linkage 模块）。

本模块只放"口径常量"与"载荷字段校验"，不含任何在线匹配逻辑。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


TEMPERATURE = "TEMPERATURE"
PRESSURE = "PRESSURE"
RESET = "RESET"
SIGNAL_KINDS = frozenset({TEMPERATURE, PRESSURE, RESET})

TEMPERATURE_OPEN_THRESHOLD = 80.0     # 达到 80 即开窗（含 80）
PRESSURE_CANDIDATE_MAX = 20.0         # 不超过 20 即候选（含 20）
OBSERVATION_WINDOW_MS = 600_000

# 来源处理结果
OUTCOME_ACCEPTED = "APPLIED"
OUTCOME_SKIPPED = "SKIPPED"
OUTCOME_REJECTED = "REJECTED"
OUTCOME_LATE = "LATE_BYPASS"

# 跳过 / 拒绝原因码
REASON_NOT_A_SIGNAL = "NOT_A_SIGNAL_KIND"
REASON_INVALID_DEVICE = "INVALID_DEVICE"
REASON_INVALID_EVENT_MS = "INVALID_EVENT_MS"
REASON_INVALID_VALUE = "INVALID_SIGNAL_VALUE"
REASON_UNCONFIGURED_PARTITION = "UNCONFIGURED_PARTITION"
REASON_LATE_BEFORE_WATERMARK = "LATE_BEFORE_WATERMARK"

# 最终结论状态
HIT = "HIT"
NO_PRESSURE = "NO_PRESSURE"
CANCELLED_BY_RESET = "CANCELLED_BY_RESET"
FINAL_STATES = frozenset({HIT, NO_PRESSURE, CANCELLED_BY_RESET})


class SignalPayloadError(ValueError):
    """业务行不是可参与规则的设备信号；reason 为可查询原因码。"""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class DeviceSignal:
    kind: str
    device: str
    event_ms: int
    value: float | None
    raw_value: object = None

def extract_signal(payload: Any) -> DeviceSignal:
    """从 events.payload（reader 解析结果）读出 device/event_ms/signal_kind/value。

    仅接受三类 signal_kind；TEMPERATURE/PRESSURE 的 value 必须是有限数值。
    RESET 的 value 不参与判定，可为 null，也可为任意值。
    """
    if not isinstance(payload, dict):
        raise SignalPayloadError(REASON_NOT_A_SIGNAL, "payload 不是对象")

    kind = payload.get("signal_kind")
    if not isinstance(kind, str) or kind not in SIGNAL_KINDS:
        raise SignalPayloadError(
            REASON_NOT_A_SIGNAL,
            f"signal_kind={kind!r} 不是 {sorted(SIGNAL_KINDS)} 之一",
        )

    device = payload.get("device")
    if not isinstance(device, str) or not device:
        raise SignalPayloadError(REASON_INVALID_DEVICE, f"device={device!r}")

    event_ms = payload.get("event_ms")
    if isinstance(event_ms, bool) or not isinstance(event_ms, int):
        raise SignalPayloadError(REASON_INVALID_EVENT_MS, f"event_ms={event_ms!r}")

    raw_value = payload.get("value")
    if kind in (TEMPERATURE, PRESSURE):
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            raise SignalPayloadError(REASON_INVALID_VALUE, f"value={raw_value!r}")
        value = float(raw_value)
        if not math.isfinite(value):
            raise SignalPayloadError(REASON_INVALID_VALUE, f"value={raw_value!r} 非有限数值")
    else:
        value = None
        if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
            candidate = float(raw_value)
            if math.isfinite(candidate):
                value = candidate

    return DeviceSignal(kind=kind, device=device, event_ms=event_ms,
                        value=value, raw_value=raw_value)
