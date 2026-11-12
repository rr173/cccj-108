#!/usr/bin/env python3
"""隔离模式（python -I）测试引导。

-I 会忽略 user site 与 PYTHONPATH，因此在脚本内显式插入：
1) 仓库根目录（保证 import avro_landing 来自仓库而非 cwd 注入）
2) 依赖安装目录（fastavro 为纯 Python 包）

用法：
    python3 -I run_tests.py
"""
import os
import site
import sys
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

user_site = site.getusersitepackages()
if os.path.isdir(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(ROOT, "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
