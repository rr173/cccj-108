"""Avro 事件落地适配器。

模块组成：
- compat:   Avro writer -> reader schema 兼容性检查（含支持边界校验）
- registry: 基于仓库内 JSON 文件的不可变 schema 目录
- store:    SQLite 持久层（业务表 / 隔离表 / 确认位置 / 冲突表）
- adapter:  批接收、隔离、再处理编排
- cli:      命令行入口
- rules:    设备联动规则口径与信号载荷校验
- linkage_store / linkage: 识别器持久状态与推进/结算/查询
- linkage_audit: 独立全量窗口枚举核对器
"""

__version__ = "1.1.0"
