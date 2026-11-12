# Avro 事件落地适配器 + 设备联动异常识别器

采集端 Avro 事件的落地适配器：在读者契约（reader schema）演进后，仍能处理
以前被隔离的事件。技术栈为 **Python 3.11 + fastavro + SQLite**，契约目录是
仓库内 JSON 文件，**不依赖任何远程注册中心**。

在此之上附带一个**设备联动异常识别器**（`avro_landing/linkage.py`）：从已落地的
`events` 业务表中识别"设备高温后十分钟内低压且期间未复位"的组合，用**显式水位**
决定何时可以给出最终结论。识别器不另建分析输入表，来源坐标
`(partition, offset)` 原样沿用接入链路。

## 1. 目录结构

```
avro_landing/
  compat.py    # writer -> reader 兼容检查 + Avro 支持边界（明确拒绝越界结构）
  registry.py  # 仓库内 JSON 契约目录：不可变 writer ID、reader 版本与激活
  store.py     # SQLite：业务表/隔离表/ingress 去重/checkpoint/冲突表/尝试历史
  adapter.py   # 批接收、隔离判定、崩溃注入点、隔离项再处理
  cli.py       # 命令行：契约检查、批接收、隔离查询、范围再处理、识别器推进/查询
  rules.py     # 识别口径常量与设备信号载荷校验（无匹配逻辑）
  linkage_store.py  # 识别器持久状态：来源总账/水位/窗口/候选/结论/依据
  linkage.py        # 推进：吸收来源 -> 应用水位 -> 结算到期窗口（单事务）
  linkage_audit.py  # 独立核对器：对已接纳信号做全量窗口枚举
contracts/     # 示例契约 JSON（writer v1/v2、信号 writer、reader v1..v3、信号 reader）
tests/         # 真实二进制 Avro 端到端测试 + 识别器规则测试
examples/      # CLI 可复现演示脚本与批文件生成器
run_tests.py   # python -I 隔离模式测试引导
run_demo.py    # 接入链路的端到端演示（含崩溃恢复）
run_linkage_demo.py  # 识别器端到端演示（规则边界 + 崩溃 + 独立核对）
cli_iso.py     # python -I 隔离模式 CLI 包装
```

## 2. 支持的 Avro 结构与边界

兼容判断严格按 **writer schema → reader schema** 方向实现 Avro Schema
Resolution 规则（`compat.py`），实际字节由 `fastavro.schemaless_reader`
以 reader schema 做二进制解析。**从不以"JSON 字段是否存在"代替 Avro 解析。**

支持：

- 固定顶层 record（`landing.Event`）及任意层级嵌套 record（禁止递归）
- 原语 `null/boolean/int/long/float/double/string/bytes`
- 可空 union（恰好两个分支且含 null），支持裸数组与 `{"type": [...]}` 两种写法
- reader 新增**带 default** 的字段（含嵌套 record 内字段）
- 数字提升，且方向敏感：`int→long/float/double`、`long→float/double`、
  `float→double`；**`long→int` 明确拒绝**
- 字段 alias（reader 字段 `aliases` 匹配 writer 字段名；record 名 alias 同样支持）
- writer 多余字段在 reader 侧跳过（字段删除合法，仅记录 INFO 诊断）

明确拒绝（`UNSUPPORTED_AVRO_STRUCTURE`，注册时和接收时双重拦截）：

- `enum` / `fixed` / `array` / `map`
- 非可空或多于两个分支的 union
- 具名类型引用（要求 schema 完全内联）
- 递归/自引用 record、`logicalType`

## 3. 持久化模型与一致性

| 表 | 作用 |
|---|---|
| `ingress` | 已持久接收去重日志：`(partition, offset)` 唯一，存 writer ID 与字节 SHA-256 |
| `events` | 业务表：解析成功的事件 JSON、落地所用 reader 版本 |
| `quarantine` | 隔离表：**原始字节**、原因码、诊断、处理版本、状态（OPEN/RESOLVED） |
| `quarantine_attempts` | 每次再处理尝试；旧诊断保留，处理版本留痕 |
| `checkpoints` | 每分区连续确认 offset（语义：`<= confirmed_offset` 全部已持久接收） |
| `conflicts` | 同 `(partition, offset)` 重传但字节或 schema ID 改变 |

隔离原因码：`UNKNOWN_WRITER_SCHEMA` / `UNSUPPORTED_AVRO_STRUCTURE` /
`DECODE_ERROR` / `READER_INCOMPATIBLE`。

**原子性**：单条事件的 ingress 写入、业务/隔离落库、checkpoint 推进在同一个
SQLite 事务（`BEGIN IMMEDIATE`）内提交。进程在 COMMIT 前任意点退出都会整体
回滚——不存在"业务已写但确认位置未推进"的持久中间态。

**确认规则**：只按 ingress 中的连续 offset 推进，遇到第一个缺口即停；
业务落库与完成隔离都算"已持久接收"。缺口之后的事件不能推动确认位置，
缺口补齐后一次性连续推进。

**重传/冲突**：相同 `(partition, offset)` + 相同字节 + 相同 writer ID 是
重传（幂等，返回 `RETRANSMIT`）；字节或 schema ID 任一改变记 `conflicts`
并返回 `CONFLICT`，既有接收结果不被覆盖。

**再处理**：使用当前激活的新 reader，但沿用隔离行中的**原始 writer ID**
（未知 ID 可先补注册）。成功后业务结果只补一次（`event_exists` 防护 +
主键约束），隔离行保留为 RESOLVED 且旧诊断不覆盖，处理版本写入
`quarantine_attempts`。再处理不改变 checkpoint。

## 4. 快速开始（可复现）

环境中 pip 需自行引导（容器内使用 `--break-system-packages` 或等价方式）：

```bash
python3 -m pip install -r requirements.txt   # 仅依赖 fastavro
```

### 4.1 自动化测试（44 个真实二进制 Avro + 识别器用例）

```bash
python3 -I run_tests.py        # -I 隔离模式；引导脚本自动补依赖路径
```

覆盖：字段默认值、数字提升单向性、nullable 双分支、alias、嵌套缺字段、
未知 ID 后补注册、offset 缺口补齐、重复再处理幂等、坏字节、越界结构拒绝、
预检门禁、writer 不可变、崩溃共同恢复；以及识别器的开窗阈值/闭开区间、
候选排序、RESET 取消、三种最终结论、水位严格大于、未声明阻止结算、
水位倒退、迟到旁路、跨分区合并、旧位置隔离修复、幂等与崩溃恢复。

### 4.2 端到端演示（Python 编排，输出每一步状态）

```bash
python3 -I run_demo.py
```

### 4.3 CLI 全流程演示（契约检查 / 批接收 / 隔离查询 / 范围再处理 / 崩溃）

```bash
bash examples/run_cli_demo.sh
```

### 4.4 识别器演示

```bash
python3 -I run_linkage_demo.py       # Python 编排：规则边界 + 崩溃 + 独立核对
bash examples/run_linkage_cli_demo.sh  # 纯 CLI：推进/查询/核对/崩溃注入
```

`run_linkage_demo.py` 覆盖：低压先到但业务时间在后、另一个分区旧水位卡住结算、
窗口结束前补入 RESET、右端点低压与端点水位的严格差别、迟到 RESET 被旁路、
旧位置隔离消息修好后首次被分析、事务内写入部分结论后强杀进程、独立全量枚举核对。

### 4.5 手工 CLI 命令

```bash
CLI=(python3 -I cli_iso.py)
$CLI init --registry ./var/r --db ./var/landing.db
$CLI register-writer --registry ./var/r --id w-event-v1 -f contracts/writer_v1.json
$CLI publish-reader  --registry ./var/r -f contracts/reader_v2.json   # 发布前自动预检
$CLI activate-reader --registry ./var/r --version 2
$CLI check-compat    --registry ./var/r --writer w-event-v2 --reader 1   # 退出码 1=不兼容
$CLI ingest          --registry ./var/r --db ./var/landing.db --batch batch.json
$CLI quarantine      --db ./var/landing.db --partition orders --status OPEN
$CLI reprocess       --registry ./var/r --db ./var/landing.db \
                     --partition orders --from-offset 5 --to-offset 6
$CLI events --db ./var/landing.db
$CLI attempts --db ./var/landing.db --partition orders --offset 5
$CLI conflicts --db ./var/landing.db
```

识别器命令（`--config` 每次启动必须完全一致，即启动时固定的采集分区集合）：

```bash
$CLI linkage-advance   --db ./var/landing.db --config factory-a,factory-b \
                       --watermark factory-a=610000 --watermark factory-b=610000
$CLI linkage-decisions --db ./var/landing.db --config factory-a,factory-b \
                       --partition factory-a --offset 1     # 结论 + 来源依据
$CLI linkage-decisions --db ./var/landing.db --config factory-a,factory-b
$CLI linkage-waiting   --db ./var/landing.db --config factory-a,factory-b
$CLI linkage-sources   --db ./var/landing.db --config factory-a,factory-b --state LATE_BYPASS
$CLI linkage-watermarks --db ./var/landing.db --config factory-a,factory-b
$CLI linkage-audit     --db ./var/landing.db --config factory-a,factory-b  # 不一致退出码 1
$CLI linkage-reset     --db ./var/landing.db   # 清空识别器状态（接入数据保留）
```

`linkage-advance` 在**一个事务**内完成：先按推进开始时的旧水位分类首次见到的
业务行，再应用本次水位声明，最后对全局水位严格大于右端的窗口结算。加
`--crash-after-decision` 可模拟"写入部分最终结论后、COMMIT 前退出"（回滚，退出码 3）。

批文件格式（datum 为真实 Avro 二进制，支持 base64/hex）：

```json
{
  "packets": [
    {"partition": "orders", "offset": 0, "writer_schema_id": "w-event-v1",
     "encoding": "base64", "datum": "<base64 avro binary>"}
  ]
}
```

崩溃注入（在事务 COMMIT 前抛出，事务回滚，退出码 3）：

```bash
$CLI ingest --registry ./var/r --db ./var/landing.db --batch batch_crash.json \
            --crash-before-commit
```

## 5. 契约演进示例

| 版本 | 关键变化 |
|---|---|
| writer v1 | `amount: int`、`note: [null,string]`、嵌套 `Source{channel, ref}` |
| writer v2 | `amount` 变为 `long`（值可超 int 范围） |
| reader v1 | 嵌套 `Source` 去掉 `ref`；`amount` 仍为 int → 无法读 v2（long→int 拒绝） |
| reader v2 | `amount→long`、新增 `region` 默认 `unknown`、嵌套新增 `priority` 默认 0 |
| reader v3 | `note` 改名为 `remark` 并声明 `"aliases": ["note"]` |

发布 reader v2 前 `precheck` 对目录内全部 writer 做 writer→reader 检查，
任一不兼容即阻止发布（CLI 返回退出码 2，`--force` 仅用于人工兜底）。

## 6. 设备联动异常识别器

### 6.1 判定口径

| 项 | 口径 |
|---|---|
| 开窗 A | `TEMPERATURE` 且 `value >= 80`，窗口 `(A.event_ms, A.event_ms+600000]` |
| 候选 B | 同设备 `PRESSURE` 且 `value <= 20`，业务时间落在 `(A, A+600000]` |
| 候选排序 | 业务时间最早；同一毫秒按 `(partition, offset)` 升序（即采集分区名、位置编号） |
| 复位 | 同设备 `RESET`，业务时间落在闭区间 `[A, A+600000]`，整个窗口作废 |
| 多窗口 | 每个 A 独立判断，可引用同一个 B |
| 最终结论 | `HIT`（有候选）/ `NO_PRESSURE`（无候选）/ `CANCELLED_BY_RESET`（被复位取消） |
| 结论身份 | A 的来源 `(partition, offset)`；重复推进不会重复发出 |

信号载荷从已落地 `events.payload_json` 读取 `device`、`event_ms`、`signal_kind`、
`value`。`signal_kind` 只接受 `TEMPERATURE/PRESSURE/RESET`；前两类的 `value`
必须是有限数值。普通旧样例消息记为 `SKIPPED`（`NOT_A_SIGNAL_KIND`），非法值记为
`REJECTED`（`INVALID_SIGNAL_VALUE` / `INVALID_EVENT_MS` / `INVALID_DEVICE`），
两类原因都可在 `linkage-sources` 查询。

### 6.2 水位与到期

- 启动时固定一组采集分区，之后每次启动传入的集合必须完全一致。
- 操作人员逐分区声明 `W`：该分区后续正常信号不会带小于 `W` 的 `event_ms`。
- **全局进度 = 所有配置分区 W 的最小值**；任一分区尚未声明则全局进度为未知，
  阻止一切结算。长时间没有新消息的分区仍保留其旧 W，不会被排除。
- 仅当**全局进度严格大于窗口右端**时才结算（`W == right` 仍等待）。
- 水位倒退报错（退出码 2），且本次推进整体不产生副作用。

### 6.3 迟到旁路

首次见到的信号若 `event_ms` 严格小于**所属分区旧水位**，保存为 `LATE_BYPASS`
记录，公开来源坐标、当时旧 W 与原因，不参与规则、不回改既有结论。
`event_ms` 恰好等于旧 W 时仍可进入（记为 `APPLIED`，同时保留判定所用的旧 W）。

### 6.4 同一推进内的顺序与一致读

`linkage-advance` 在一个 `BEGIN IMMEDIATE` 事务里：
1. 校验水位声明；
2. 按**旧水位**分类全部首次见到的业务行（来源总账按 `(partition, offset)` 做差集，
   而不是"从最大 offset 往后扫"，因此隔离修复后更小 offset 的新行也会被看到）；
3. 应用新水位；
4. 按新水位结算到期窗口并写结论与来源依据；
5. COMMIT。进程在 COMMIT 前退出则整体回滚——不会出现"水位已推进但结论缺失"
   或"结论已发但来源未消费"的持久中间态。

### 6.5 识别器持久表

| 表 | 作用 |
|---|---|
| `linkage_partitions` | 启动时固定的采集分区集合 |
| `linkage_watermarks` / `linkage_watermark_events` | 分区当前 W / 每次声明历史 |
| `linkage_sources` | 已见来源总账：APPLIED / SKIPPED / REJECTED / LATE_BYPASS + 原因 |
| `linkage_windows` | 观察窗口：WAITING / CANCELLED / FINALIZED + 复位依据 |
| `linkage_candidates` | 每个窗口已接纳的候选低压 |
| `linkage_decisions` | 最终结论，`(partition, offset)` 唯一 = A 的来源 |
| `linkage_decision_evidence` | 结论来源依据：TEMPERATURE / PRESSURE / RESET 的坐标与值 |

### 6.6 独立核对器

`linkage-audit` 不使用 `linkage.py` 的在线匹配逻辑：它读取 `linkage_sources`
里被判定为 `APPLIED` 的信号，对每个温度信号做全量窗口枚举，重算状态与来源依据，
再逐条比对线上结论：

- 到期窗口必须有结论，且状态、区间、来源依据一致；
- 未到期窗口不得有结论（列入 `pending`）；
- 线上结论不得多出。
