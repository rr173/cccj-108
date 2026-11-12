# Avro 事件落地适配器

采集端 Avro 事件的落地适配器：在读者契约（reader schema）演进后，仍能处理
以前被隔离的事件。技术栈为 **Python 3.11 + fastavro + SQLite**，契约目录是
仓库内 JSON 文件，**不依赖任何远程注册中心**。

## 1. 目录结构

```
avro_landing/
  compat.py    # writer -> reader 兼容检查 + Avro 支持边界（明确拒绝越界结构）
  registry.py  # 仓库内 JSON 契约目录：不可变 writer ID、reader 版本与激活
  store.py     # SQLite：业务表/隔离表/ingress 去重/checkpoint/冲突表/尝试历史
  adapter.py   # 批接收、隔离判定、崩溃注入点、隔离项再处理
  cli.py       # 命令行：契约检查、批接收、隔离查询、范围再处理
contracts/     # 示例契约 JSON（writer v1/v2，reader v1/v2/v3）
tests/         # 13 个真实二进制 Avro 端到端测试
examples/      # CLI 可复现演示脚本与批文件生成器
run_tests.py   # python -I 隔离模式测试引导
run_demo.py    # python -I 隔离模式端到端演示（含崩溃恢复）
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

### 4.1 自动化测试（13 个真实二进制 Avro 用例）

```bash
python3 -I run_tests.py        # -I 隔离模式；引导脚本自动补依赖路径
```

覆盖：字段默认值、数字提升单向性、nullable 双分支、alias、嵌套缺字段、
未知 ID 后补注册、offset 缺口补齐、重复再处理幂等、坏字节、越界结构拒绝、
预检门禁、writer 不可变、崩溃共同恢复。

### 4.2 端到端演示（Python 编排，输出每一步状态）

```bash
python3 -I run_demo.py
```

### 4.3 CLI 全流程演示（契约检查 / 批接收 / 隔离查询 / 范围再处理 / 崩溃）

```bash
bash examples/run_cli_demo.sh
```

### 4.4 手工 CLI 命令

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
