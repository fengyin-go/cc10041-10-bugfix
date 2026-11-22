"""节点执行失败注入。

真实执行器里失败来自计算引擎；为了能演示“失败后重试并关联此前阻塞范围”，
这里允许调用方显式声明本轮要失败的节点。每个进程内同一节点只失败一次，
重试时自动恢复，方便演示失败→重试的闭环。
"""
from __future__ import annotations

from typing import Iterable

_FAILED_ONCE: set[str] = set()


def reset_simulation() -> None:
    _FAILED_ONCE.clear()


def should_fail(node: str, fail_nodes: Iterable[str] | None) -> str | None:
    """命中失败注入名单时返回失败原因；否则返回 None。"""
    if not fail_nodes:
        return None
    candidates = {str(item).strip() for item in fail_nodes if str(item).strip()}
    if node not in candidates:
        return None
    if node in _FAILED_ONCE:
        return None
    _FAILED_ONCE.add(node)
    return f"注入执行失败：{node} 计算过程异常"
