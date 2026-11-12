"""台账与名称口径：名称清理、整数 Levenshtein 距离、资产清单校验。

口径（全部可复核）
------------------
- 名称清理：Unicode NFKC -> casefold -> 去掉所有非 Unicode 字母/数字类别
  （类别首字母不是 L 或 N）的字符。中文属 Lo，保留；清理成空串即无效。
- 编辑距离：Levenshtein，插入、删除、替换各算 1。
- 距离 d <= 2 才有资格；正式名得分 100-10d，别名得分 95-10d；
  同一资产只取最高分。

本模块不读写数据库，也不做分配求解。
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional


# --- 口径常量 -------------------------------------------------------------
MAX_EDIT_DISTANCE = 2
FORMAL_BASE_SCORE = 100
ALIAS_BASE_SCORE = 95
SCORE_PER_EDIT = 10

KIND_FORMAL = "FORMAL"
KIND_ALIAS = "ALIAS"

# 资产清单校验错误码
ERR_INVALID_JSON = "INVALID_JSON"
ERR_ROOT = "INVALID_INVENTORY_ROOT"
ERR_UNKNOWN_ROOT_FIELD = "UNKNOWN_ROOT_FIELD"
ERR_ASSETS_NOT_LIST = "ASSETS_NOT_LIST"
ERR_ASSET_NOT_OBJECT = "ASSET_NOT_OBJECT"
ERR_UNKNOWN_ASSET_FIELD = "UNKNOWN_ASSET_FIELD"
ERR_MISSING_ASSET_ID = "MISSING_ASSET_ID"
ERR_INVALID_ASSET_ID = "INVALID_ASSET_ID"
ERR_DUPLICATE_ASSET_ID = "DUPLICATE_ASSET_ID"
ERR_MISSING_NAME = "MISSING_NAME"
ERR_INVALID_NAME = "INVALID_NAME"
ERR_ALIASES_NOT_LIST = "ALIASES_NOT_LIST"
ERR_INVALID_ALIAS = "INVALID_ALIAS"
ERR_INVALID_REVISION = "INVALID_REVISION"


class MatchInputError(Exception):
    """盘点输入无效；调用方必须整份拒绝，不得写入半份结果。"""

    def __init__(self, errors: Iterable[dict], message: str = ""):
        self.errors = list(errors)
        detail = message or "; ".join(
            f"{e.get('code')}: {e.get('detail', '')}" for e in self.errors
        ) or "invalid input"
        super().__init__(detail)

    def as_dict(self) -> dict:
        return {"ok": False, "errors": self.errors}


def clean_name(raw: str) -> str:
    """NFKC -> casefold -> 仅保留 Unicode 字母/数字类别。"""
    if not isinstance(raw, str):
        raise TypeError(f"名称必须是字符串，收到 {type(raw).__name__}")
    folded = unicodedata.normalize("NFKC", raw).casefold()
    return "".join(
        ch for ch in folded if unicodedata.category(ch)[0] in ("L", "N")
    )


def name_is_valid(raw: Any) -> bool:
    return isinstance(raw, str) and bool(clean_name(raw))


def levenshtein(left: str, right: str) -> int:
    """整数 Levenshtein 距离：插入/删除/替换各 1。"""
    if left == right:
        return 0
    if len(left) < len(right):
        left, right = right, left
    if not right:
        return len(left)
    previous = list(range(len(right) + 1))
    for i, ch_left in enumerate(left, 1):
        current = [i] + [0] * len(right)
        for j, ch_right in enumerate(right, 1):
            current[j] = min(
                previous[j] + 1,
                current[j - 1] + 1,
                previous[j - 1] + (0 if ch_left == ch_right else 1),
            )
        previous = current
    return previous[-1]


def candidate_score(kind: str, distance: int) -> int:
    base = FORMAL_BASE_SCORE if kind == KIND_FORMAL else ALIAS_BASE_SCORE
    return base - SCORE_PER_EDIT * distance


# --- 资产模型 -------------------------------------------------------------
@dataclass(frozen=True)
class AliasName:
    raw: str
    clean: str


@dataclass(frozen=True)
class Asset:
    asset_id: str
    name: str
    name_clean: str
    aliases: tuple[AliasName, ...] = ()

    def names(self) -> list[tuple[str, str, str]]:
        """返回 [(kind, raw, clean)]，含正式名与全部别名。"""
        out = [(KIND_FORMAL, self.name, self.name_clean)]
        out.extend((KIND_ALIAS, a.raw, a.clean) for a in self.aliases)
        return out

    def as_dict(self) -> dict:
        return {
            "asset_id": self.asset_id,
            "name": self.name,
            "name_clean": self.name_clean,
            "aliases": [{"raw": a.raw, "clean": a.clean} for a in self.aliases],
        }


@dataclass
class Inventory:
    assets: dict[str, Asset]
    revision: Optional[str]
    source_path: str
    raw_bytes: bytes
    sha256: str
    canonical: dict

    @property
    def asset_ids(self) -> list[str]:
        return sorted(self.assets)

    def as_snapshot(self) -> dict:
        """持久化快照：之后的台账更正不影响旧运行的解释。"""
        return dict(self.canonical)


def _asset_sort_key(item: Asset) -> tuple:
    return (item.asset_id,)


def _add_error(errors: list[dict], code: str, detail: str, **loc: Any) -> None:
    item = {"code": code, "detail": detail}
    item.update(loc)
    errors.append(item)


def validate_inventory_document(
    doc: Any, *, source: str = "<memory>",
) -> tuple[list[Asset], Optional[str], list[dict]]:
    """校验已解析的 JSON 文档；返回 (assets, revision, errors)。"""
    errors: list[dict] = []
    revision: Optional[str] = None

    raw_assets: Any
    if isinstance(doc, list):
        raw_assets = doc
    elif isinstance(doc, dict):
        allowed = {"assets", "revision", "note"}
        unknown = sorted(set(doc) - allowed)
        for key in unknown:
            _add_error(errors, ERR_UNKNOWN_ROOT_FIELD, f"未知顶层字段 {key!r}", field=key)
        raw_assets = doc.get("assets")
        if not isinstance(raw_assets, list):
            _add_error(
                errors, ERR_ASSETS_NOT_LIST,
                f"{source} 的 assets 必须是数组，收到 {type(raw_assets).__name__}",
            )
            raw_assets = []
        if "revision" in doc:
            if isinstance(doc["revision"], str):
                revision = doc["revision"]
            else:
                _add_error(errors, ERR_INVALID_REVISION,
                           f"revision 必须是字符串，收到 {type(doc['revision']).__name__}")
        if "note" in doc and not isinstance(doc["note"], str):
            _add_error(errors, ERR_INVALID_REVISION,
                       "note 必须是字符串")
    else:
        _add_error(errors, ERR_ROOT,
                   f"{source} 根节点必须是资产数组或含 assets 的对象")
        raw_assets = []

    assets: list[Asset] = []
    seen_ids: dict[str, int] = {}
    for index, item in enumerate(raw_assets):
        if not isinstance(item, dict):
            _add_error(errors, ERR_ASSET_NOT_OBJECT,
                       f"第 {index} 项资产必须是对象，收到 {type(item).__name__}",
                       index=index)
            continue
        unknown = sorted(set(item) - {"asset_id", "name", "aliases"})
        for key in unknown:
            _add_error(errors, ERR_UNKNOWN_ASSET_FIELD,
                       f"第 {index} 项资产含未知字段 {key!r}", index=index, field=key)
        asset_id = item.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id.strip():
            _add_error(errors, ERR_MISSING_ASSET_ID if asset_id is None else ERR_INVALID_ASSET_ID,
                       f"第 {index} 项资产缺少非空字符串 asset_id", index=index)
            continue
        if asset_id in seen_ids:
            _add_error(errors, ERR_DUPLICATE_ASSET_ID,
                       f"asset_id {asset_id!r} 重复（第 {seen_ids[asset_id]} 与第 {index} 项）",
                       index=index, asset_id=asset_id)
            continue
        seen_ids[asset_id] = index

        name = item.get("name")
        if not isinstance(name, str) or not name:
            _add_error(errors, ERR_MISSING_NAME if name is None else ERR_INVALID_NAME,
                       f"资产 {asset_id!r} 缺少非空字符串 name", index=index,
                       asset_id=asset_id)
            continue
        name_clean = clean_name(name)
        if not name_clean:
            _add_error(errors, ERR_INVALID_NAME,
                       f"资产 {asset_id!r} 的正式名 {name!r} 清理后为空",
                       index=index, asset_id=asset_id)
            continue

        aliases_doc = item.get("aliases", [])
        if not isinstance(aliases_doc, list):
            _add_error(errors, ERR_ALIASES_NOT_LIST,
                       f"资产 {asset_id!r} 的 aliases 必须是数组",
                       index=index, asset_id=asset_id)
            aliases_doc = []
        aliases: list[AliasName] = []
        seen_alias: set[str] = set()
        for alias_index, alias in enumerate(aliases_doc):
            if not isinstance(alias, str) or not alias:
                _add_error(errors, ERR_INVALID_ALIAS,
                           f"资产 {asset_id!r} 第 {alias_index} 个别名必须是非空字符串",
                           index=index, asset_id=asset_id, alias_index=alias_index)
                continue
            alias_clean = clean_name(alias)
            if not alias_clean:
                _add_error(errors, ERR_INVALID_ALIAS,
                           f"资产 {asset_id!r} 的别名 {alias!r} 清理后为空",
                           index=index, asset_id=asset_id, alias_index=alias_index)
                continue
            if alias in seen_alias:
                continue
            seen_alias.add(alias)
            aliases.append(AliasName(raw=alias, clean=alias_clean))

        assets.append(Asset(
            asset_id=asset_id,
            name=name,
            name_clean=name_clean,
            aliases=tuple(aliases),
        ))

    return assets, revision, errors


def shared_alias_report(assets: Iterable[Asset]) -> list[dict]:
    """按清理后的别名分组；同一别名出现在不同资产即显式暴露歧义。"""
    grouped: dict[str, dict] = {}
    for asset in assets:
        for alias in asset.aliases:
            entry = grouped.setdefault(
                alias.clean, {"clean": alias.clean, "raw": [], "asset_ids": []}
            )
            if alias.raw not in entry["raw"]:
                entry["raw"].append(alias.raw)
            if asset.asset_id not in entry["asset_ids"]:
                entry["asset_ids"].append(asset.asset_id)
    report = []
    for entry in grouped.values():
        if len(entry["asset_ids"]) < 2:
            continue
        report.append({
            "clean": entry["clean"],
            "raw": sorted(entry["raw"]),
            "asset_ids": sorted(entry["asset_ids"]),
        })
    report.sort(key=lambda item: (item["clean"], item["asset_ids"]))
    return report


def duplicate_formal_report(assets: Iterable[Asset]) -> list[dict]:
    """清理后正式名相同的资产（不因此擅自决定归属，只暴露）。"""
    grouped: dict[str, dict] = {}
    for asset in assets:
        entry = grouped.setdefault(
            asset.name_clean, {"clean": asset.name_clean, "raw": [], "asset_ids": []}
        )
        if asset.name not in entry["raw"]:
            entry["raw"].append(asset.name)
        entry["asset_ids"].append(asset.asset_id)
    report = []
    for entry in grouped.values():
        if len(entry["asset_ids"]) < 2:
            continue
        report.append({
            "clean": entry["clean"],
            "raw": sorted(entry["raw"]),
            "asset_ids": sorted(entry["asset_ids"]),
        })
    report.sort(key=lambda item: (item["clean"], item["asset_ids"]))
    return report


def canonical_inventory(assets: Iterable[Asset], revision: Optional[str]) -> dict:
    return {
        "revision": revision,
        "assets": [
            {
                "asset_id": asset.asset_id,
                "name": asset.name,
                "aliases": [a.raw for a in asset.aliases],
            }
            for asset in sorted(assets, key=_asset_sort_key)
        ],
    }


def inventory_digest(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()


def load_inventory(path: str | Path, *, strict: bool = True) -> Inventory:
    """读取并校验资产清单。任何错误都以 MatchInputError 整份拒绝。"""
    file_path = Path(path)
    try:
        raw = file_path.read_bytes()
    except OSError as exc:
        raise MatchInputError([{
            "code": "INVENTORY_READ_ERROR",
            "detail": f"无法读取资产清单 {file_path}: {exc}",
        }])
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MatchInputError([{
            "code": ERR_INVALID_JSON,
            "detail": f"资产清单不是 UTF-8 文本: {exc}",
        }])
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise MatchInputError([{
            "code": ERR_INVALID_JSON,
            "detail": f"资产清单 JSON 解析失败: {exc}",
        }])
    assets, revision, errors = validate_inventory_document(doc, source=str(file_path))
    if errors:
        if strict:
            raise MatchInputError(errors)
    return Inventory(
        assets={asset.asset_id: asset for asset in assets},
        revision=revision,
        source_path=str(file_path),
        raw_bytes=raw,
        sha256=inventory_digest(raw),
        canonical=canonical_inventory(assets, revision),
    )


def inventory_report(inventory: Inventory) -> dict:
    assets = [inventory.assets[i] for i in inventory.asset_ids]
    return {
        "ok": True,
        "inventory_path": inventory.source_path,
        "inventory_sha256": inventory.sha256,
        "revision": inventory.revision,
        "asset_count": len(assets),
        "alias_count": sum(len(a.aliases) for a in assets),
        "assets": [asset.as_dict() for asset in assets],
        "shared_aliases": shared_alias_report(assets),
        "duplicate_formal_names": duplicate_formal_report(assets),
    }
