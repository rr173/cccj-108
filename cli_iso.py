#!/usr/bin/env python3
"""隔离模式（python -I）下的 CLI 包装器。

-I 忽略 PYTHONPATH/user-site，这里显式补齐仓库根与依赖目录后转发到 cli.main。
用法：
    python3 -I cli_iso.py <avro_landing.cli 的参数...>
"""
import os
import site
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
_user_site = site.getusersitepackages()
if os.path.isdir(_user_site):
    sys.path.insert(0, _user_site)

from avro_landing.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
