"""DAG 依赖解析与分区编排。

这个模块只做纯图计算，不读写任何业务数据：
  * 解析节点/依赖字符串；
  * 用 Tarjan 找出真正的环（而不是把环下游也误判成环）；
  * 把“成环节点 + 其所有下游分支”标记为阻塞；
  * 只对剩下的可执行节点做拓扑排序，并给出可执行分区清单。

引擎层拿到这里的结果后再决定哪些分支可以真正写数。
"""
from __future__ import annotations

import hashlib
from typing import Any, Iterable

# 节点/依赖字段可能是逗号、中文逗号、分号或换行分隔。
_NODE_SEPARATORS = (",", "，", ";", "；", "\n", "\r")
_ARROW_SEPARATORS = ("->", "→", "=>", "⇒")


def _as_items(raw: Any) -> list[str]:
    """把字段值归一化成一组去空白后的文本片段。"""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        items: list[str] = []
        for value in raw:
            items.extend(_as_items(value))
        return items
    text = str(raw)
    for separator in _NODE_SEPARATORS:
        text = text.replace(separator, "\n")
    return [part.strip() for part in text.split("\n") if part.strip()]


def parse_nodes(raw: Any) -> list[str]:
    """解析节点列表，去重且保留声明顺序。"""
    nodes: list[str] = []
    seen: set[str] = set()
    for item in _as_items(raw):
        # 允许节点列表里直接出现 "A->B" 时也能识别出两端。
        if any(arrow in item for arrow in _ARROW_SEPARATORS):
            for src, dst in parse_edges(item):
                for node in (src, dst):
                    if node not in seen:
                        seen.add(node)
                        nodes.append(node)
            continue
        if item not in seen:
            seen.add(item)
            nodes.append(item)
    return nodes


def parse_edges(raw: Any) -> list[tuple[str, str]]:
    """解析依赖关系。方向约定：``A->B`` 表示 B 依赖 A。"""
    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in _as_items(raw):
        parts: list[str] | None = None
        for arrow in _ARROW_SEPARATORS:
            if arrow in item:
                bits = item.split(arrow)
                if len(bits) == 2:
                    parts = [bits[0].strip(), bits[1].strip()]
                break
        if not parts or not parts[0] or not parts[1]:
            continue
        edge = (parts[0], parts[1])
        if edge not in seen:
            seen.add(edge)
            edges.append(edge)
    return edges


def _strongly_connected(nodes: list[str], adjacency: dict[str, list[str]]) -> list[list[str]]:
    """Tarjan SCC：用于区分“真的在环里”的节点和“被环拖住”的下游节点。"""
    index = 0
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    components: list[list[str]] = []

    def visit(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlink[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)

        for neighbour in adjacency.get(node, []):
            if neighbour not in indices:
                visit(neighbour)
                lowlink[node] = min(lowlink[node], lowlink[neighbour])
            elif neighbour in on_stack:
                lowlink[node] = min(lowlink[node], indices[neighbour])

        if lowlink[node] == indices[node]:
            component: list[str] = []
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.append(member)
                if member == node:
                    break
            components.append(component)

    for node in nodes:
        if node not in indices:
            visit(node)
    return components


def _topological_order(
    nodes: list[str],
    executable: set[str],
    dependencies: dict[str, list[str]],
    adjacency: dict[str, list[str]],
) -> list[str]:
    """仅对可执行节点做 Kahn 拓扑排序，保持节点声明顺序的稳定性。"""
    node_index = {node: idx for idx, node in enumerate(nodes)}
    in_degree = {node: 0 for node in nodes if node in executable}
    for node in in_degree:
        for upstream in dependencies.get(node, []):
            if upstream in executable:
                in_degree[node] += 1

    ready = sorted(
        (node for node, degree in in_degree.items() if degree == 0),
        key=lambda node: node_index[node],
    )
    order: list[str] = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for neighbour in adjacency.get(node, []):
            if neighbour not in in_degree:
                continue
            in_degree[neighbour] -= 1
            if in_degree[neighbour] == 0:
                ready.append(neighbour)
                ready.sort(key=lambda item: node_index[item])
    return order


def _connected_components(nodes: list[str], edges: list[tuple[str, str]]) -> list[list[str]]:
    """弱连通分量：一组相互关联的管道视为一个编排分区。"""
    node_set = set(nodes)
    neighbours: dict[str, set[str]] = {node: set() for node in nodes}
    for src, dst in edges:
        if src in node_set and dst in node_set:
            neighbours[src].add(dst)
            neighbours[dst].add(src)

    components: list[list[str]] = []
    visited: set[str] = set()
    for node in nodes:
        if node in visited:
            continue
        stack = [node]
        visited.add(node)
        members: set[str] = set()
        while stack:
            current = stack.pop()
            members.add(current)
            for neighbour in neighbours.get(current, set()):
                if neighbour not in visited:
                    visited.add(neighbour)
                    stack.append(neighbour)
        components.append([item for item in nodes if item in members])
    return components


def analyze_dag(dag: dict[str, Any]) -> dict[str, Any]:
    """解析一个 DAG 配置，返回环、阻塞范围、可执行节点和分区清单。"""
    nodes = parse_nodes(dag.get("节点列表", ""))
    edges = parse_edges(dag.get("依赖关系", ""))
    node_set = set(nodes)

    dependencies: dict[str, list[str]] = {node: [] for node in nodes}
    adjacency: dict[str, list[str]] = {node: [] for node in nodes}
    invalid_edges: list[dict[str, str]] = []
    unknown_upstream: dict[str, list[str]] = {}

    for src, dst in edges:
        if src in node_set and dst in node_set:
            dependencies[dst].append(src)
            adjacency[src].append(dst)
        else:
            reason = "依赖两端节点未在节点列表中声明"
            if src not in node_set and dst in node_set:
                reason = f"上游节点 {src} 未在节点列表中声明"
                unknown_upstream.setdefault(dst, []).append(src)
            elif dst not in node_set:
                reason = f"下游节点 {dst} 未在节点列表中声明"
            invalid_edges.append({"from": src, "to": dst, "reason": reason})

    components = _strongly_connected(nodes, adjacency)
    cycles: list[list[str]] = []
    cycle_nodes: set[str] = set()
    for component in components:
        is_self_loop = any(
            node in adjacency.get(node, []) for node in component
        )
        if len(component) > 1 or is_self_loop:
            members = [node for node in nodes if node in set(component)]
            cycles.append(members)
            cycle_nodes.update(members)
    cycles.sort(key=lambda members: min(nodes.index(node) for node in members))

    # 成环节点本身不可执行；所有依赖其产物的下游分支同样不可执行。
    blocked_nodes: set[str] = set(cycle_nodes)
    blocked_by: dict[str, list[str]] = {}
    blocked_reason: dict[str, str] = {}
    for node in nodes:
        if node in cycle_nodes:
            upstream_cycle = [up for up in dependencies.get(node, []) if up in cycle_nodes]
            blocked_by[node] = upstream_cycle or [node]
            blocked_reason[node] = "节点处于依赖环"

    queue: list[str] = [node for node in nodes if node in cycle_nodes]
    for node, upstreams in unknown_upstream.items():
        if node not in blocked_nodes:
            blocked_nodes.add(node)
            blocked_by[node] = list(upstreams)
            blocked_reason[node] = "依赖节点未定义，无法确定上游产物"
            queue.append(node)

    while queue:
        current = queue.pop(0)
        for downstream in adjacency.get(current, []):
            if downstream in blocked_nodes:
                continue
            blocked_nodes.add(downstream)
            blocked_by[downstream] = [current]
            blocked_reason[downstream] = "上游节点阻塞，无法取得输入产物"
            queue.append(downstream)

    executable_nodes = [node for node in nodes if node not in blocked_nodes]
    executable_set = set(executable_nodes)
    execution_order = _topological_order(nodes, executable_set, dependencies, adjacency)

    # 理论上可执行子图没有环，Kahn 一定能全部排出；兜底防止残缺配置。
    for node in list(execution_order):
        if node not in executable_set:
            execution_order.remove(node)

    blocked_order = [node for node in nodes if node in blocked_nodes]

    graph_partitions: list[dict[str, Any]] = []
    for position, component in enumerate(_connected_components(nodes, edges), start=1):
        component_set = set(component)
        component_cycle = [node for node in component if node in cycle_nodes]
        component_executable = [node for node in component if node in executable_set]
        component_blocked = [node for node in component if node in blocked_nodes]
        if component_blocked and not component_executable:
            status = "阻塞"
        elif component_blocked:
            status = "部分可执行"
        else:
            status = "可执行"
        graph_partitions.append({
            "partition": f"分区{position}",
            "name": f"分区{position}",
            "nodes": component,
            "executable": not component_blocked and bool(component_executable),
            "runnable": bool(component_executable),
            "status": status,
            "executable_nodes": component_executable,
            "blocked_nodes": component_blocked,
            "cycle_nodes": component_cycle,
            "execution_order": [node for node in execution_order if node in component_set],
            "reason": (
                "依赖关系成环，整组分支无法出数"
                if component_blocked and not component_executable
                else "上游阻塞，仅部分节点可执行"
                if component_blocked
                else "依赖关系无环，可完整执行"
            ),
        })

    executable_partitions = [item for item in graph_partitions if item["executable"]]
    blocked_partitions = [item for item in graph_partitions if item["blocked_nodes"] and not item["executable_nodes"]]
    partially_executable_partitions = [
        item for item in graph_partitions
        if item["executable_nodes"] and item["blocked_nodes"]
    ]

    fingerprint_source = "|".join(nodes) + "#" + "|".join(f"{a}->{b}" for a, b in edges)
    fingerprint = hashlib.sha1(fingerprint_source.encode("utf-8")).hexdigest()[:16]

    return {
        "nodes": nodes,
        "edges": [{"from": src, "to": dst} for src, dst in edges],
        "dependencies": dependencies,
        "adjacency": adjacency,
        "invalid_edges": invalid_edges,
        "dependency_errors": invalid_edges,
        "has_cycle": bool(cycles),
        "is_acyclic": not cycles,
        "valid": not cycles and not invalid_edges,
        "cycles": cycles,
        "cycle_nodes": [node for node in nodes if node in cycle_nodes],
        "blocked_nodes": blocked_order,
        "blocked_by": blocked_by,
        "blocked_reasons": blocked_reason,
        "executable_nodes": executable_nodes,
        "runnable_nodes": executable_nodes,
        "execution_order": execution_order,
        "blocked_order": blocked_order,
        "partitions": graph_partitions,
        "components": graph_partitions,
        "executable_partitions": executable_partitions,
        "blocked_partitions": blocked_partitions,
        "partially_executable_partitions": partially_executable_partitions,
        "partition_count": len(graph_partitions),
        "graph_fingerprint": fingerprint,
    }
