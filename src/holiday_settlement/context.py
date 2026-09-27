"""读取并校验一份领域上下文。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REQUIRED = {"domain", "version", "sample_id", "actors", "facts", "constraints"}

def validate_context(value: object) -> dict:
    """返回结构合法的上下文，便于后续服务复用。"""
    if not isinstance(value, dict):
        raise ValueError("领域资料必须是对象")
    missing = REQUIRED - set(value)
    if missing:
        raise ValueError("领域资料缺少字段：" + ",".join(sorted(missing)))
    if not isinstance(value["version"], int) or value["version"] < 1:
        raise ValueError("资料版本必须为正整数")
    for name in ("actors", "facts", "constraints"):
        items = value[name]
        if not isinstance(items, list) or len(items) < 2 or any(not isinstance(x, str) or not x.strip() for x in items):
            raise ValueError(f"{name} 只能包含至少两项非空文本")
    return value

def load_context(path: Path) -> dict:
    return validate_context(json.loads(path.read_text(encoding="utf-8")))

def summarize(value: dict) -> str:
    return f"{value['domain']} v{value['version']}：{len(value['actors'])} 个参与方，{len(value['constraints'])} 项约束"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("用法：python3 -m 模块路径 <资料文件>")
    print(summarize(load_context(Path(sys.argv[1]))))
