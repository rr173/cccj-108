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
  entity_names.py   # 离线实体匹配口径：名称清理、整数编辑距离、台账校验
  entity_solver.py  # 全局一对一最大权匹配（整数匈牙利算法）+ 同分方案计数
  entity_match.py   # 候选构造、人工限制校验、替代解与"唯一可靠"判定
  entity_store.py   # 匹配运行持久化：参数/台账快照/来源/候选/限制/分配/替代
  entity_run.py     # 一致读视图 + 单事务保存 + 按运行编号/设备解释
  entity_audit.py   # 独立穷举核对：小规模样例的最优值/替代值/唯一性/约束
contracts/     # 示例契约 JSON（writer v1/v2、信号 writer、reader v1..v3、信号 reader）
tests/         # 真实二进制 Avro 端到端测试 + 识别器规则测试 + 实体匹配测试
examples/      # CLI 可复现演示脚本、批文件生成器与离线台账样本
run_tests.py   # python -I 隔离模式测试引导
run_demo.py    # 接入链路的端到端演示（含崩溃恢复）
run_linkage_demo.py  # 识别器端到端演示（规则边界 + 崩溃 + 独立核对）
run_entity_demo.py   # 实体匹配端到端演示（贪心反例/同分/共享别名/人工限制/台账更正）
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

### 4.1 自动化测试（72 个真实二进制 Avro + 识别器 + 实体匹配用例）

```bash
python3 -I run_tests.py        # -I 隔离模式；引导脚本自动补依赖路径
```

覆盖：字段默认值、数字提升单向性、nullable 双分支、alias、嵌套缺字段、
未知 ID 后补注册、offset 缺口补齐、重复再处理幂等、坏字节、越界结构拒绝、
预检门禁、writer 不可变、崩溃共同恢复；识别器的开窗阈值/闭开区间、
候选排序、RESET 取消、三种最终结论、水位严格大于、未声明阻止结算、
水位倒退、迟到旁路、跨分区合并、旧位置隔离修复、幂等与崩溃恢复；
实体匹配的名称口径（NFKC/casefold/去非字母数字/中文保留/空串无效）、
整数编辑距离与分数、共享别名暴露、全局最优（贪心反例）、同分方案计数、
禁止边/人工确认占用、替代解与 gap、margin 严格大于、冲突整份拒绝、
一致读视图、运行保存与旧解释可查、穷举核对。

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

### 4.5 离线实体匹配演示

```bash
python3 -I run_entity_demo.py            # Python 编排：8 个场景 + 逐运行穷举核对
bash examples/run_entity_cli_demo.sh     # 纯 CLI：台账校验/运行/解释/冲突/更正
```

`run_entity_demo.py` 覆盖：全角/大小写/分隔符与共享别名、贪心选择比全局分配差、
两套同分最优方案留给人工、人工确认占用资产后的重新分配、禁止一条边后最优解改变、
无候选与清理后空串、margin 端点（gap 严格大于）、台账更正后的新盘点与旧运行解释、
输入错误整份拒绝。所有设备名都先编码成真实 Avro datum 经 `ingest` 落进 `events`。

### 4.6 手工 CLI 命令

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

离线实体匹配命令（设备值只读 `events`，不读手填输入）：

```bash
$CLI entity-inventory-check --inventory examples/entity_inventory/assets_v1.json
$CLI entity-match-run --db ./var/landing.db \
      --inventory examples/entity_inventory/assets_v1.json \
      --min-score 0 --margin 10 --scope factory-a \
      [--confirm '送风机->AST-004'] [--ban '冷却泵－ＰＡ－１００->AST-001']
$CLI entity-runs        --db ./var/landing.db
$CLI entity-run-explain --db ./var/landing.db --run-id 1 [--device '送风机']
$CLI entity-explain     --db ./var/landing.db --device '送风机' [--run-id 1]
$CLI entity-run-audit   --db ./var/landing.db --run-id 1   # 不一致退出码 1
$CLI entity-reset       --db ./var/landing.db             # 清空匹配运行（接入数据保留）
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

## 7. 离线实体匹配（采集标识 -> 资产编号）

把已落库 `events` 里的 device 唯一值统一到资产编号，供后续关联设备档案。
算法全程整数、可逐项复核；不连真实设备、不需要网页。

### 7.1 名称清理与候选口径

| 项 | 口径 |
|---|---|
| 清理 | Unicode NFKC -> casefold -> 去掉所有非字母/数字类别（L*/N*）字符；中文属 Lo，保留 |
| 无效名称 | 清理后为空串（如 `···`、`％`）不参与匹配，原因 `EMPTY_DEVICE_NAME` |
| 编辑距离 | Levenshtein，插入/删除/替换各算 1 |
| 候选资格 | 距离 `d <= 2`；采集名分别与正式名、每个别名比较 |
| 分数 | 正式名 `100-10d`；别名 `95-10d`；同一资产取最高分 |
| 门槛 | 只有分数 `>= min_score`（0..100）的边参与自动分配；未配对得 0 |
| 候选依据 | 每条候选都保存原名称、清理后名称、正式名/别名身份、距离、分数 |

同一清理后别名可以出现在不同资产：`shared_aliases` 显式列出，候选上的
`shared_alias` 标记为真。归属由全局总分与人工限制决定，**绝不按文件顺序**。

### 7.2 全局分配（不是逐个设备贪心）

一份台账 = 一次盘点范围。求解在整个范围上做**最大权一对一匹配**
（整数匈牙利算法 / 带势函数的最短增广路，允许不配对）：
一个资产最多一个采集标识，一个标识最多一个资产。逐个设备挑最像的局部贪心
可能抢掉另一台设备唯一合适的资产，因此不使用。

同分方案：展示用的方案按（设备名，资产编号）稳定排序，同时报告
`equivalent_optima`（同分方案数量）与每个自动配对的替代解；这种排序只用于
稳定展示，不作为"唯一可靠"的结论。

### 7.3 人工确认与禁止

- 确认 `DEVICE->ASSET_ID` **先占用两端资源**，再从剩余问题中扣除；
  它可越过自动分数门槛，但必须存在可复核的候选（距离 <= 2），
  否则没有候选依据、整份请求拒绝。
- 禁止 `DEVICE->ASSET_ID` 在**所有求解**（含替代解）中有效。
- 以下冲突整份拒绝、退出码 2、不写入任何运行：
  两条确认争用同一设备 / 同一资产、引用不存在的设备或资产、
  同一配对同时确认又禁止。参数非法（`min_score` 越界、`margin < 0`）同样拒绝。

### 7.4 唯一可靠与 margin

设最优总分 `S`。对最优方案中每个未经人工确认的设备：

- 已配对：禁掉它当前的选择再求最优 `S_alt`；`gap = S - S_alt`。
- 未配对：改为要求它必须配到某个合格资产再求最优 `S_alt`，
  该子问题不可行即"无替代"，否则记录 `gap`。

存在替代方案时 `gap >= 0`；不存在时标记**无替代**。自动配对只有
**无替代**或 **`gap > margin`（严格大于）** 才写为 `CONFIRMED`；
其余写为 `PENDING`，并在解释里给出原选择、替代选择、`S` 与 `S_alt`。
人工确认不做自动置信判断（直接 `HUMAN_CONFIRMED` / `CONFIRMED`）。

未匹配原因：`EMPTY_DEVICE_NAME`、`NO_QUALIFYING_CANDIDATE`、
`ALL_CANDIDATES_BELOW_MIN_SCORE`（含被人工限制占满）、`UNMATCHED_BY_GLOBAL_OPTIMUM`
（有合格候选但配上去会降低全局总分）。

### 7.5 一致读与运行留痕

`entity-match-run` 在同一个 SQLite 连接上先显式 `BEGIN` 读取一致快照
（device 唯一值、已有运行编号），再在 `BEGIN IMMEDIATE` 事务里整体写入：
运行参数、台账 SHA-256 与规范化快照、每个设备的全部来源坐标、候选依据、
人工限制、全局分配与替代解。盘点输入有错时事务回滚，不留半份结果。

台账快照随运行保存，因此**台账更正后的新运行不改变旧运行的解释**；
`entity-run-explain --run-id N` 随时可复查原始参数、台账内容摘要、
来源、候选、人工限制、全局分配与替代解。

| 表 | 作用 |
|---|---|
| `match_runs` | 每次运行：参数、台账摘要与快照、总分、同分方案计数、人工限制 |
| `match_devices` | 每设备结论：状态/决策/分数/候选/替代解/gap/未匹配原因 |
| `match_sources` | 设备在 `events` 中的全部来源坐标 |
| `match_candidates` | 候选依据（原名称/清理名/身份/距离/得分/共享别名/可用性） |
| `match_human_constraints` | 确认与禁止，逐条留痕 |
| `match_alternatives` | 每个自动配对的替代选择 |

### 7.6 独立穷举核对

`entity-run-audit` 不复用生产求解器：小规模样例（设备 <= 12、资产 <= 12，
并设节点预算）用**递归枚举全部合法配对**重算最优总分、同分方案数、
每个设备的替代值与 `gap`，再逐条比对运行记录：

- 分数合计与穷举最优一致；
- 自动配对分数不低于 `min_score`，人工确认覆盖门槛时按确认口径；
- 人工禁止的边从未进入任何解（含替代解）；
- 唯一性判定（无替代 / `gap > margin`）与穷举一致；
- 替代选择确实能让替代解保持 `S_alt`（同分替代的展示顺序不作唯一性）。
