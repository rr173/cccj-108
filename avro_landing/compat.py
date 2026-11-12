"""Avro schema 兼容性检查（严格按 writer -> reader 方向）。

设计要点
--------
1. 兼容性判断基于 Avro Schema Resolution 规范（spec #12），**不**依赖
   "JSON 字段是否存在" 之类的旁路判断；实际二进制解码由 fastavro 按
   reader schema 完成（见 adapter.py），本模块只负责回答
   "这个 writer 能不能被这个 reader 读" 并给出诊断。
2. 本适配器对允许的 Avro 结构有明确边界，越界结构由 :class:`SchemaShape`
   直接拒绝，错误码为 ``UNSUPPORTED_AVRO_STRUCTURE``。
3. 支持的结构：
   - 固定顶层 record（含嵌套 record，嵌套层级任意但不允许递归/自引用）
   - 原语 null/boolean/int/long/float/double/string/bytes
   - 可空 union（``["null", T]`` 与 ``["T", "null"]``），union 内也只允许
     上述原语或嵌套 record
   - record 新增带 default 的字段、字段删除（reader 缺字段）
   - 字段 alias（reader 字段通过 aliases 匹配 writer 字段名）
   - int -> long 数字提升（及 long->float/double、float->double）
4. 明确拒绝：enum/fixed/array/map、非可空多分支 union、具名类型引用
   （要求 schema 全内联）、递归 record、logicalType、未知原语。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


# --- 支持的原语集合 -------------------------------------------------------
PRIMITIVES = {
    "null",
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "string",
    "bytes",
}

# Avro 规范允许的安全数字/类型提升（writer -> reader 方向）。
# 注意方向性：int -> long 合法，long -> int 非法。
PROMOTIONS: dict[str, set[str]] = {
    "int": {"long", "float", "double"},
    "long": {"float", "double"},
    "float": {"double"},
    "string": {"bytes"},
    "bytes": {"string"},
}


@dataclass
class CompatResult:
    """兼容性检查结果。"""

    compatible: bool
    reasons: list[str] = field(default_factory=list)
    unsupported: bool = False  # 命中"明确不支持的 Avro 结构"

    @property
    def reason(self) -> str:
        return "; ".join(self.reasons) if self.reasons else ""

    def fail(self, reason: str) -> "CompatResult":
        self.compatible = False
        self.reasons.append(reason)
        return self


class UnsupportedAvroStructure(Exception):
    """schema 使用了本适配器明确不支持的 Avro 结构。"""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


# --- 支持边界（shape）校验 ------------------------------------------------
def validate_shape(schema: Any, path: str = "$", seen: Optional[set[int]] = None) -> None:
    """递归校验 schema 是否落在适配器支持的结构边界内。

    越界即抛 :class:`UnsupportedAvroStructure`。
    ``seen`` 用于检测递归（自引用）record。
    """
    if seen is None:
        seen = set()

    if isinstance(schema, str):
        # 具名引用（如 {"name":"Foo"} 之后写 "Foo"）一律不支持：
        # 目录要求 schema 完全内联，避免解析顺序依赖。
        if schema not in PRIMITIVES:
            raise UnsupportedAvroStructure(
                f"{path}: 不支持具名类型引用 '{schema}'，schema 必须完全内联"
            )
        return

    if not isinstance(schema, dict):
        # Avro union 可直接写成裸 JSON 数组："type": ["null", "string"]
        if isinstance(schema, list):
            _validate_union({"type": schema}, path, seen)
            return
        raise UnsupportedAvroStructure(f"{path}: schema 节点类型非法 {type(schema).__name__}")

    # logicalType 不在支持范围内，保持落地语义简单可预测。
    if "logicalType" in schema:
        raise UnsupportedAvroStructure(
            f"{path}: 不支持 logicalType='{schema['logicalType']}'"
        )

    stype = schema.get("type")

    if isinstance(stype, str):
        if stype in PRIMITIVES:
            return
        if stype == "record":
            _validate_record(schema, path, seen)
            return
        if stype in ("enum", "fixed", "array", "map"):
            raise UnsupportedAvroStructure(
                f"{path}: 本适配器不支持 Avro 类型 '{stype}'"
            )
        raise UnsupportedAvroStructure(f"{path}: 未知或不支持的类型 '{stype}'")

    if isinstance(stype, list):
        _validate_union(schema, path, seen)
        return

    raise UnsupportedAvroStructure(f"{path}: 非法 type 节点 {stype!r}")


def _validate_record(schema: dict, path: str, seen: set[int]) -> None:
    if "name" not in schema or not isinstance(schema["name"], str):
        raise UnsupportedAvroStructure(f"{path}: record 缺少字符串 name")
    if id(schema) in seen:
        raise UnsupportedAvroStructure(
            f"{path}: 不支持递归/自引用 record '{schema['name']}'"
        )
    seen = seen | {id(schema)}
    fields = schema.get("fields")
    if not isinstance(fields, list) or not fields:
        raise UnsupportedAvroStructure(f"{path}: record '{schema['name']}' 缺少 fields")
    for i, f in enumerate(fields):
        if not isinstance(f, dict) or "name" not in f or "type" not in f:
            raise UnsupportedAvroStructure(
                f"{path}.fields[{i}]: 字段必须是含 name/type 的对象"
            )
        validate_shape(f["type"], f"{path}.{f['name']}", seen)
        # default 可以不存在；存在时类型不做静态深查，交给 fastavro。
        if "aliases" in f and not (
            isinstance(f["aliases"], list) and all(isinstance(a, str) for a in f["aliases"])
        ):
            raise UnsupportedAvroStructure(
                f"{path}.{f['name']}: aliases 必须是字符串数组"
            )


def _validate_union(schema: dict, path: str, seen: set[int]) -> None:
    branches = schema["type"]
    if len(branches) != 2 or "null" not in branches:
        raise UnsupportedAvroStructure(
            f"{path}: 仅支持可空 union（恰好两个分支且其中一个为 null），"
            f"实际分支={branches}"
        )
    for branch in branches:
        validate_shape(branch, path + ".<union>", seen)


# --- 名称处理 -------------------------------------------------------------
def _fullname(schema: dict, default_ns: Optional[str]) -> str:
    name = schema["name"]
    if "." in name:
        return name
    ns = schema.get("namespace", default_ns)
    return f"{ns}.{name}" if ns else name


# --- 兼容性主入口 ---------------------------------------------------------
def check_compatibility(writer: Any, reader: Any) -> CompatResult:
    """按 writer -> reader 方向检查 schema 解析兼容性。

    先做双方支持边界校验，再执行 Avro resolution 规则。
    """
    result = CompatResult(compatible=True)
    try:
        validate_shape(writer, "writer")
        validate_shape(reader, "reader")
    except UnsupportedAvroStructure as exc:
        result.compatible = False
        result.unsupported = True
        result.reasons.append(exc.reason)
        return result

    _resolve(writer, reader, "$", result, None, None)
    return result


def _is_record(node: Any) -> bool:
    return isinstance(node, dict) and node.get("type") == "record"


def _is_union(node: Any) -> bool:
    # 兼容两种 union 写法：{"type": [...]} 与裸数组 [...]
    if isinstance(node, list):
        return True
    return isinstance(node, dict) and isinstance(node.get("type"), list)


def _union_branches(node: Any) -> list[Any]:
    return node if isinstance(node, list) else node["type"]


def _type_name(node: Any) -> str:
    if isinstance(node, list):
        return "union"
    if isinstance(node, str):
        return node
    t = node["type"]
    return t if isinstance(t, str) else "union"


def _resolve(
    writer: Any,
    reader: Any,
    path: str,
    result: CompatResult,
    writer_ns: Optional[str],
    reader_ns: Optional[str],
) -> None:
    if not result.compatible:
        return

    # 双方都是 union：writer union 的每个分支都必须能匹配 reader union 的某分支
    if _is_union(writer) and _is_union(reader):
        for w_branch in _union_branches(writer):
            if not _union_accepts(w_branch, _union_branches(reader), path,
                                  result, writer_ns, reader_ns):
                result.fail(
                    f"{path}: writer union 分支 {_type_name(w_branch)} 无法匹配 "
                    f"reader union 分支 {[ _type_name(b) for b in _union_branches(reader) ]}"
                )
                return
        return

    # reader 是 union：writer 的类型必须能解析到 union 的某个分支
    if _is_union(reader):
        if not _union_accepts(writer, _union_branches(reader), path, result, writer_ns, reader_ns):
            result.fail(
                f"{path}: writer 类型 {_type_name(writer)} 无法匹配 reader union 分支 "
                f"{[ _type_name(b) for b in _union_branches(reader) ]}"
            )
        return

    # writer 是 union（reader 不是）：每个可能分支都必须能解析到 reader
    if _is_union(writer):
        for branch in _union_branches(writer):
            _resolve(branch, reader, path, result, writer_ns, reader_ns)
        return

    # record -> record
    if _is_record(writer) or _is_record(reader):
        if not (_is_record(writer) and _is_record(reader)):
            result.fail(
                f"{path}: record 与非 record 不兼容 "
                f"(writer={_type_name(writer)}, reader={_type_name(reader)})"
            )
            return
        _resolve_record(writer, reader, path, result, writer_ns, reader_ns)
        return

    # 原语 -> 原语（含数字提升，方向敏感）
    w_name = writer if isinstance(writer, str) else writer["type"]
    r_name = reader if isinstance(reader, str) else reader["type"]
    if w_name == r_name:
        return
    if r_name in PROMOTIONS.get(w_name, set()):
        return
    result.fail(
        f"{path}: 类型不兼容且无合法提升 writer={w_name} -> reader={r_name}"
    )


def _union_accepts(
    writer: Any,
    reader_branches: list[Any],
    path: str,
    result: CompatResult,
    writer_ns: Optional[str],
    reader_ns: Optional[str],
) -> bool:
    for branch in reader_branches:
        trial = CompatResult(compatible=True)
        _resolve(writer, branch, path, trial, writer_ns, reader_ns)
        if trial.compatible:
            return True
    return False


def _resolve_record(
    writer: dict,
    reader: dict,
    path: str,
    result: CompatResult,
    writer_ns: Optional[str],
    reader_ns: Optional[str],
) -> None:
    w_full = _fullname(writer, writer_ns)
    r_full = _fullname(reader, reader_ns)
    w_ns = writer.get("namespace", writer_ns)
    r_ns = reader.get("namespace", reader_ns)

    # 名称需要匹配；reader 的 aliases 也可以接受 writer 的名称
    reader_aliases = set(reader.get("aliases", []))
    if w_full != r_full and w_full not in reader_aliases:
        result.fail(f"{path}: record 名称不匹配 writer={w_full} reader={r_full}")
        return

    w_fields = {f["name"]: f for f in writer["fields"]}
    consumed_writer_fields: set[str] = set()

    for rf in reader["fields"]:
        rname = rf["name"]
        wf = None
        # 1) 同名匹配；2) reader 字段 alias 匹配 writer 字段名
        candidates = [rname, *rf.get("aliases", [])]
        for cand in candidates:
            if cand in w_fields:
                wf = w_fields[cand]
                consumed_writer_fields.add(cand)
                break

        fpath = f"{path}.{rname}"
        if wf is None:
            # reader 新增字段：writer 没有，必须带 default 才能填充
            if "default" not in rf:
                result.fail(f"{fpath}: reader 新增字段缺少 default，writer 中无此字段")
            continue

        _resolve(wf["type"], rf["type"], fpath, result, w_ns, r_ns)

    # writer 多余字段在 Avro resolution 中直接跳过（record 字段删除合法），
    # 仅记录便于审计，不构成失败。
    extra = [name for name in w_fields if name not in consumed_writer_fields]
    if extra:
        result.reasons.append(f"INFO {path}: writer 多余字段将被跳过 {extra}")
