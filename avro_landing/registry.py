"""仓库内 JSON 文件契约目录（无远程注册中心）。

目录布局::

    registry_dir/
      registry.json              # 索引：writer 映射、reader 版本、当前激活版本
      writers/<writer_id>.json   # 不可变 writer schema，按 ID 存盘
      readers/v<version>.json    # 不可变 reader schema，按版本存盘

writer schema 一经注册不可变（重复注册相同内容幂等，内容不同则报错）。
reader schema 按固定记录类型维护版本，发布新版本前可先跑 compat 预检，
再显式 activate。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from .compat import CompatResult, UnsupportedAvroStructure, check_compatibility, validate_shape


INDEX_FILENAME = "registry.json"


class RegistryError(Exception):
    """目录操作失败（schema 不存在、不可变冲突等）。"""


class SchemaRegistry:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.writers_dir = self.root / "writers"
        self.readers_dir = self.root / "readers"
        self.index_path = self.root / INDEX_FILENAME
        self._index: Optional[dict] = None

    # --- 初始化与加载 -----------------------------------------------------
    def init(self) -> None:
        self.writers_dir.mkdir(parents=True, exist_ok=True)
        self.readers_dir.mkdir(parents=True, exist_ok=True)
        if not self.index_path.exists():
            self._index = {
                "record_type": "landing.Event",
                "writers": {},          # writer_id -> {"file": "writers/x.json"}
                "readers": {},          # "1" -> {"file": "readers/v1.json"}
                "active_reader": None,  # 激活 reader 版本（int 字符串）
            }
            self._flush()

    def _load(self) -> dict:
        if not self.index_path.exists():
            raise RegistryError(f"契约目录尚未初始化: {self.root}")
        if self._index is None:
            with self.index_path.open("r", encoding="utf-8") as fh:
                self._index = json.load(fh)
        return self._index

    def _flush(self) -> None:
        tmp = self.index_path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self._index, fh, ensure_ascii=False, indent=2)
        tmp.replace(self.index_path)

    @property
    def record_type(self) -> str:
        return self._load()["record_type"]

    # --- writer 注册 ------------------------------------------------------
    def register_writer(self, writer_id: str, schema: dict) -> None:
        """注册不可变 writer schema。

        相同 ID 重复注册且定义一致 -> 幂等成功；定义不一致 -> 报错。
        注册时即做支持边界校验。
        """
        self._validate_id(writer_id)
        validate_shape(schema, f"writer:{writer_id}")
        index = self._load()
        rel = f"writers/{writer_id}.json"
        path = self.root / rel
        if writer_id in index["writers"]:
            existing = self._load_schema_file(path)
            if existing != schema:
                raise RegistryError(
                    f"writer schema '{writer_id}' 已存在且内容不同，writer schema 不可变"
                )
            return
        self._write_schema_file(path, schema)
        index["writers"][writer_id] = {"file": rel}
        self._flush()

    def get_writer(self, writer_id: str) -> dict:
        index = self._load()
        entry = index["writers"].get(writer_id)
        if entry is None:
            raise RegistryError(f"未知 writer schema id: {writer_id}")
        return self._load_schema_file(self.root / entry["file"])

    def list_writers(self) -> list[str]:
        return sorted(self._load()["writers"].keys())

    # --- reader 版本管理 --------------------------------------------------
    def publish_reader(self, schema: dict) -> int:
        """发布一个新 reader 版本，返回新版本号（不自动激活）。"""
        validate_shape(schema, "reader")
        index = self._load()
        version = max((int(v) for v in index["readers"]), default=0) + 1
        rel = f"readers/v{version}.json"
        self._write_schema_file(self.root / rel, schema)
        index["readers"][str(version)] = {"file": rel}
        self._flush()
        return version

    def activate_reader(self, version: int) -> None:
        index = self._load()
        if str(version) not in index["readers"]:
            raise RegistryError(f"reader 版本不存在: v{version}")
        index["active_reader"] = str(version)
        self._flush()

    def active_reader_version(self) -> Optional[int]:
        value = self._load()["active_reader"]
        return int(value) if value is not None else None

    def get_reader(self, version: int) -> dict:
        index = self._load()
        entry = index["readers"].get(str(version))
        if entry is None:
            raise RegistryError(f"未知 reader 版本: v{version}")
        return self._load_schema_file(self.root / entry["file"])

    def get_active_reader(self) -> tuple[int, dict]:
        version = self.active_reader_version()
        if version is None:
            raise RegistryError("尚未激活任何 reader schema 版本")
        return version, self.get_reader(version)

    def list_readers(self) -> list[int]:
        return sorted(int(v) for v in self._load()["readers"])

    # --- 发布前兼容预检 ---------------------------------------------------
    def precheck(self, reader_schema: dict) -> dict[str, CompatResult]:
        """对目录内**所有**已注册 writer 做 writer -> 新 reader 的兼容预检。"""
        # 先单独做新 reader 的支持边界校验，给出清晰错误
        validate_shape(reader_schema, "reader")
        results: dict[str, CompatResult] = {}
        for writer_id in self.list_writers():
            writer = self.get_writer(writer_id)
            results[writer_id] = check_compatibility(writer, reader_schema)
        return results

    def check_pair(self, writer_id: str, reader_version: int) -> CompatResult:
        return check_compatibility(self.get_writer(writer_id), self.get_reader(reader_version))

    # --- 工具 -------------------------------------------------------------
    @staticmethod
    def _validate_id(writer_id: str) -> None:
        if not writer_id or not all(c.isalnum() or c in "._-" for c in writer_id):
            raise RegistryError(f"非法 writer id: {writer_id!r}")

    @staticmethod
    def _write_schema_file(path: Path, schema: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(schema, fh, ensure_ascii=False, indent=2)

    @staticmethod
    def _load_schema_file(path: Path) -> dict:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
