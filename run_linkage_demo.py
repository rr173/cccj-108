"""设备联动异常识别器的可复现演示（离线、真实 Avro 二进制）。

用现有接入链路落地三类信号，再用命令行推进识别器并查询结论。覆盖：

1. 低压先于高温到达但业务时间在后（窗口创建时回填已接纳候选）
2. 普通旧样例消息被跳过（可查原因）；非有限数值被拒绝（可查原因）
3. 另一个分区未声明水位时阻止结算；声明后按钮式到期（W 严格大于右端）
4. 窗口结束前补入 RESET：窗口先记 CANCELLED，到期后结论为 CANCELLED_BY_RESET
5. 右端点低压（闭区间候选）与端点水位（严格大于才结算）的差别
6. 迟到 RESET 被旁路：保留来源、声明水位与原因，不改既有结论
7. 旧位置隔离消息修好后首次被分析（差集扫描，不看最大 offset）
8. 事务内写入部分最终结论后强杀进程：重启后不重复、不漏报
9. 独立全量窗口枚举核对全部结论与来源依据

用法：
    python3 -I run_linkage_demo.py
演示目录：./var/linkage_demo（每次重跑会清空）。
"""

from __future__ import annotations

import base64
import io
import json
import os
import shutil
import site
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
_user_site = site.getusersitepackages()
if os.path.isdir(_user_site) and _user_site not in sys.path:
    sys.path.insert(0, _user_site)

import fastavro  # noqa: E402

DEMO = os.path.join(ROOT, "var", "linkage_demo")
REG = os.path.join(DEMO, "registry")
DB = os.path.join(DEMO, "landing.db")
CONFIG = "factory-a,factory-b"
CLI = [sys.executable, "-I", os.path.join(ROOT, "cli_iso.py")]


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def load(name: str) -> dict:
    with open(os.path.join(ROOT, "contracts", name), encoding="utf-8") as fh:
        return json.load(fh)


def encode(schema: dict, record: dict) -> str:
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, fastavro.parse_schema(schema), record)
    return base64.b64encode(bio.getvalue()).decode("ascii")


def cli(*argv: str) -> int:
    """执行 CLI，输出原样透传；返回退出码。"""
    print("$", " ".join(str(a) for a in argv))
    code = subprocess.run(CLI + [str(a) for a in argv], cwd=ROOT).returncode
    if code not in (0,):
        print(f"[退出码 {code}]")
    return code


def write_batch(name: str, packets: list[dict]) -> str:
    path = os.path.join(DEMO, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"packets": packets}, fh, ensure_ascii=False, indent=2)
    return path


def signal(w_sig: dict, *, device: str, event_ms: int,
           kind: str, value: float | None, tag: str) -> str:
    return encode(w_sig, {
        "event_id": tag,
        "device": device,
        "event_ms": event_ms,
        "signal_kind": kind,
        "value": value,
        "source": {"channel": "plant-bus", "ref": None},
    })


def packet(partition: str, offset: int, datum: str,
           writer_id: str = "w-signal-v1") -> dict:
    return {
        "partition": partition, "offset": offset,
        "writer_schema_id": writer_id, "encoding": "base64", "datum": datum,
    }


def ingest(batch: str) -> None:
    cli("ingest", "--registry", REG, "--db", DB, "--batch", batch)


def advance(*marks: str, crash: bool = False) -> int:
    argv = ["linkage-advance", "--db", DB, "--config", CONFIG]
    argv += [f"--watermark={m}" for m in marks]
    if crash:
        argv.append("--crash-after-decision")
    return cli(*argv)


def decision(partition: str, offset: int, note: str = "") -> None:
    if note:
        print(f"\n{note}")
    cli("linkage-decisions", "--db", DB, "--config", CONFIG,
        "--partition", partition, "--offset", offset)


def main() -> int:
    if os.path.isdir(DEMO):
        shutil.rmtree(DEMO)
    os.makedirs(DEMO)

    w_sig = load("writer_signal_v1.json")
    w1 = load("writer_v1.json")

    banner("步骤 1：契约 —— 信号 writer + 兼容旧样例的 reader")
    cli("init", "--registry", REG, "--db", DB)
    cli("register-writer", "--registry", REG, "--id", "w-signal-v1",
        "--file", os.path.join(ROOT, "contracts", "writer_signal_v1.json"))
    cli("register-writer", "--registry", REG, "--id", "w-event-v1",
        "--file", os.path.join(ROOT, "contracts", "writer_v1.json"))
    cli("publish-reader", "--registry", REG,
        "--file", os.path.join(ROOT, "contracts", "reader_signal_v1.json"))
    cli("activate-reader", "--registry", REG, "--version", "1")
    print("\n识别器启动时固定采集分区集合:", CONFIG)
    print("（重复启动时该集合必须完全一致，运行中不允许改配置）")

    # ------------------------------------------------------------------ #
    banner("步骤 2：低压先到、业务时间在后；普通消息跳过；坏数值拒绝")
    print("设备 dev-backfill：A=10000 -> 右端 610000。压力 500000 到得更早，")
    print("但业务时间在窗口内；窗口创建时回填它作为候选。")
    batch = write_batch("batch-1.json", [
        packet("factory-a", 0, signal(w_sig, device="dev-backfill", event_ms=500_000,
                                      kind="PRESSURE", value=12.0, tag="pressure-early")),
        packet("factory-a", 1, signal(w_sig, device="dev-backfill", event_ms=10_000,
                                      kind="TEMPERATURE", value=95.0, tag="temp-late")),
        packet("factory-a", 2, encode(w1, {
            "event_id": "legacy-order", "amount": 5, "note": None,
            "source": {"channel": "web", "ref": None},
        }), writer_id="w-event-v1"),
        packet("factory-a", 3, signal(w_sig, device="dev-backfill", event_ms=1_500,
                                      kind="PRESSURE", value=None, tag="null-value")),
    ])
    ingest(batch)
    print("\n接入层全部 LANDED；识别器按规则分类：")
    print("  offset=2 普通旧样例 -> SKIPPED（NOT_A_SIGNAL_KIND）")
    print("  offset=3 压力 value=null -> REJECTED（INVALID_SIGNAL_VALUE）")

    print("\n推进 1：只声明 factory-a 的 W=610000；factory-b 未声明 -> 阻止结算")
    advance("factory-a=610000")
    print("\n等待中的窗口：")
    cli("linkage-waiting", "--db", DB, "--config", CONFIG)
    print("\n跳过与拒绝的原因：")
    cli("linkage-sources", "--db", DB, "--config", CONFIG, "--state", "SKIPPED")
    cli("linkage-sources", "--db", DB, "--config", CONFIG, "--state", "REJECTED")
    print("\n此时不得有任何最终结论：")
    cli("linkage-decisions", "--db", DB, "--config", CONFIG)

    # ------------------------------------------------------------------ #
    banner("步骤 3：跨分区设备 + 严格大于——W=610000 等于右端，仍不结算")
    print("dev-cross：A 在 factory-b/0 (20000)，压力在 factory-b/1 (615000)，")
    print("两者跨分区、同设备，必须合起来判断。")
    print("同时：dev-backfill 右端=610000，全局 W=610000 不大于右端，继续等待。")
    batch = write_batch("batch-2.json", [
        packet("factory-b", 0, signal(w_sig, device="dev-cross", event_ms=20_000,
                                      kind="TEMPERATURE", value=88.0, tag="temp-cross")),
        packet("factory-b", 1, signal(w_sig, device="dev-cross", event_ms=615_000,
                                      kind="PRESSURE", value=18.0, tag="pressure-cross")),
    ])
    ingest(batch)
    advance("factory-b=610000")
    cli("linkage-waiting", "--db", DB, "--config", CONFIG)
    print("\n把两个分区都推进到 610001（严格大于 dev-backfill 右端）：")
    advance("factory-a=610001", "factory-b=610001")
    decision("factory-a", 1, "dev-backfill 结论（候选 offset=0 到得比温度早）")

    # ------------------------------------------------------------------ #
    banner("步骤 4：窗口结束前补入 RESET —— 先 CANCELLED，到期后取消结论")
    print("dev-reset：A=620000 -> 右端 1220000；压力 620500；RESET 在 1219000。")
    batch = write_batch("batch-3.json", [
        packet("factory-b", 2, signal(w_sig, device="dev-reset", event_ms=620_000,
                                      kind="TEMPERATURE", value=90.0, tag="temp-cancel")),
        packet("factory-b", 3, signal(w_sig, device="dev-reset", event_ms=620_500,
                                      kind="PRESSURE", value=5.0, tag="pressure-cancel")),
        packet("factory-b", 4, signal(w_sig, device="dev-reset", event_ms=1_219_000,
                                      kind="RESET", value=None, tag="reset-in-window")),
    ])
    ingest(batch)
    print("\n推进到 W=1219999：仍不大于右端 1220000，窗口已记 CANCELLED 但不出结论。")
    advance("factory-a=1_219_999", "factory-b=1_219_999")
    cli("linkage-waiting", "--db", DB, "--config", CONFIG)
    print("\n推进到 W=1220001：结算。dev-cross 也在这一步一并到期。")
    advance("factory-a=1_220_001", "factory-b=1_220_001")
    decision("factory-b", 2, "dev-reset 结论（取消依据是 RESET，不是压力）")
    decision("factory-b", 0, "dev-cross 结论（跨分区压力来源）")

    # ------------------------------------------------------------------ #
    banner("步骤 5：右端点低压（闭区间）与端点水位（严格大于）")
    print("dev-endpoint：A=1300000，右端=1900000。")
    print("压力恰在 1900000 且值=20（不超过 20，落在 (A,右端]）-> 是候选。")
    batch = write_batch("batch-4.json", [
        packet("factory-a", 4, signal(w_sig, device="dev-endpoint", event_ms=1_300_000,
                                      kind="TEMPERATURE", value=81.0, tag="temp-edge")),
        packet("factory-b", 5, signal(w_sig, device="dev-endpoint", event_ms=1_900_000,
                                      kind="PRESSURE", value=20.0, tag="pressure-right-edge")),
    ])
    ingest(batch)
    print("\n先声明 W=1900000（等于右端）：不得结算。")
    advance("factory-a=1_900_000", "factory-b=1_900_000")
    cli("linkage-waiting", "--db", DB, "--config", CONFIG)
    print("\n再推进到 1900001：结算 HIT，低压来源是右端点那条。")
    advance("factory-a=1_900_001", "factory-b=1_900_001")
    decision("factory-a", 4, "右端点低压被采纳")

    # ------------------------------------------------------------------ #
    banner("步骤 6：迟到 RESET 被旁路 —— 不回改既有结论，也不悄悄丢失")
    print("dev-reset 的窗口已取消并发出结论；补一条业务时间 1150000 的 RESET，")
    print("它落在该窗口内，但早于 factory-b 已声明的旧水位 1900001 -> 迟到旁路。")
    batch = write_batch("batch-5.json", [
        packet("factory-b", 6, signal(w_sig, device="dev-reset", event_ms=1_150_000,
                                      kind="RESET", value=None, tag="reset-late")),
    ])
    ingest(batch)
    advance()
    cli("linkage-sources", "--db", DB, "--config", CONFIG, "--state", "LATE_BYPASS")
    decision("factory-b", 2, "既有结论不变（取消依据仍是 1219000 的 RESET）")
    print("\n对照：时间恰好等于旧水位仍可进入（下一步的隔离修复信号 event_ms=1950000，")
    print("届时 factory-a 旧水位=1900001，不影响；等于旧 W 的用例见自动化测试）。")

    # ------------------------------------------------------------------ #
    banner("步骤 7：旧位置隔离消息修好后首次被分析")
    print("先落地 factory-a/8（未知 writer，进隔离表、不在 events），")
    print("再落地 factory-a/9（普通已落地行）；推进时看不到 offset=8。")
    print("修复 w-held-back 并再处理后，events 里新增的是更小的 offset=8 ——")
    print("识别器按 (partition, offset) 差集扫描，因此不会漏掉它。")
    held = signal(w_sig, device="dev-quarantine", event_ms=1_950_000,
                  kind="TEMPERATURE", value=84.0, tag="temp-quarantined")
    batch = write_batch("batch-6.json", [
        packet("factory-a", 8, held, writer_id="w-held-back"),
        packet("factory-a", 9, signal(w_sig, device="dev-other", event_ms=2_100_000,
                                      kind="PRESSURE", value=30.0, tag="other")),
        packet("factory-b", 7, signal(w_sig, device="dev-quarantine", event_ms=2_000_000,
                                      kind="PRESSURE", value=9.0, tag="pressure-quarantine")),
    ])
    ingest(batch)
    advance()
    cli("linkage-sources", "--db", DB, "--config", CONFIG, "--state", "APPLIED")
    print("（offset=8 尚不在 sources 中：它还没有业务行）")
    print("\n修复 writer 后再处理 offset=8：")
    cli("register-writer", "--registry", REG, "--id", "w-held-back",
        "--file", os.path.join(ROOT, "contracts", "writer_signal_v1.json"))
    cli("reprocess", "--registry", REG, "--db", DB,
        "--partition", "factory-a", "--from-offset", "8", "--to-offset", "8")
    print("\n再推进：首次看到 offset=8，开窗并回填已接纳的压力 factory-b/7。")
    advance()
    cli("linkage-sources", "--db", DB, "--config", CONFIG, "--state", "APPLIED")
    cli("linkage-waiting", "--db", DB, "--config", CONFIG)
    print("\n推进到右端之后（W=2550001）：")
    advance("factory-a=2_550_001", "factory-b=2_550_001")
    decision("factory-a", 8, "隔离修复后的温度窗口结论")

    # ------------------------------------------------------------------ #
    banner("步骤 8：事务内写入部分结论后强杀进程 —— 重启不重复、不漏报")
    batch = write_batch("batch-7.json", [
        packet("factory-b", 8, signal(w_sig, device="dev-crash", event_ms=2_600_000,
                                      kind="TEMPERATURE", value=90.0, tag="temp-crash")),
        packet("factory-b", 9, signal(w_sig, device="dev-crash", event_ms=2_700_000,
                                      kind="PRESSURE", value=11.0, tag="pressure-crash")),
        packet("factory-a", 10, signal(w_sig, device="dev-crash-2", event_ms=2_600_000,
                                       kind="TEMPERATURE", value=90.0, tag="temp-crash-2")),
    ])
    ingest(batch)
    print("\n推进到 W=3200001；写完第一条最终结论后、COMMIT 前强制退出：")
    code = advance("factory-a=3_200_001", "factory-b=3_200_001", crash=True)
    print("退出码:", code)
    print("\n重启后的持久状态：水位应停在 2550001，结论也不应出现 2600000 的两条。")
    cli("linkage-watermarks", "--db", DB, "--config", CONFIG)
    cli("linkage-decisions", "--db", DB, "--config", CONFIG)
    print("\n重新推进同一批（显式水位可重发；被回滚的来源会重新消费一次）：")
    advance("factory-a=3_200_001", "factory-b=3_200_001")
    cli("linkage-waiting", "--db", DB, "--config", CONFIG)
    decision("factory-b", 8, "crash 后重做：HIT")
    decision("factory-a", 10, "crash 后重做：NO_PRESSURE（无候选时给出观察区间）")
    print("\n再推进一次（无新水位）：已发结论不会重复发出。")
    advance()
    cli("linkage-decisions", "--db", DB, "--config", CONFIG)

    # ------------------------------------------------------------------ #
    banner("步骤 9：独立全量窗口枚举核对结论与来源依据")
    code = cli("linkage-audit", "--db", DB, "--config", CONFIG)
    print("核对退出码:", code, "（0 = 状态、窗口区间与来源依据全部一致）")

    banner("完成")
    print("演示数据库:", DB)
    print("命令：linkage-advance / linkage-decisions / linkage-waiting /")
    print("      linkage-sources / linkage-watermarks / linkage-audit")
    return code


if __name__ == "__main__":
    sys.exit(main())
