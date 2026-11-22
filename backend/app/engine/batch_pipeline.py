"""批量管道引擎：DAG编排、任务调度、回填续做、输出审计。

真实数据流：
  batch_pipeline → pipeline_dag → task_scheduler(backfill) → result_writer
  → output_audit → checkpoint_mgr

依赖解析在 :mod:`app.engine.dag_graph` 中完成：先算出真正的环、
被环阻断的下游分支和可执行分区，再只调度可执行子图。
只有真正写入结果写入器的节点才会生成“校验通过”的审计记录，
成环节点及其下游分支只登记未产出原因，不写任何数据。
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from app.engine.dag_graph import analyze_dag
from app.engine.execution_policy import should_fail
from app.store import store

# 同一次运行的幂等注册表：run_key 决定 run_id，并发触发只会执行一次。
_RUN_REGISTRY_LOCK = threading.RLock()
_RUN_LOCKS: dict[str, threading.Lock] = {}
_RUN_CACHE: dict[str, dict[str, Any]] = {}
# 所有落库动作串行化，避免不同运行并发抢 id / 重复写分区。
_STORE_WRITE_LOCK = threading.RLock()


def reset_runtime_state() -> None:
    """清空进程内的运行注册表，主要供测试使用。"""
    with _RUN_REGISTRY_LOCK:
        _RUN_LOCKS.clear()
        _RUN_CACHE.clear()


def _run_lock(key: str) -> threading.Lock:
    with _RUN_REGISTRY_LOCK:
        lock = _RUN_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _RUN_LOCKS[key] = lock
        return lock


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


@dataclass
class BatchPipelineEngine:
    """批量管道编排与调度执行器。"""

    pipeline_key: str = "batch_pipeline"
    dag_key: str = "pipeline_dag"
    backfill_key: str = "backfill_handler"
    output_audit_key: str = "output_audit"
    checkpoint_key: str = "checkpoint_mgr"
    result_writer_key: str = "result_writer"
    lineage_key: str = "lineage_tracker"
    partition_key: str = "data_partition"
    run_table_key: str = "pipeline_run"
    run_partition_key: str = "pipeline_run_partition"
    fail_nodes: tuple[str, ...] = ()

    run_id: str = ""
    run_key: str = ""
    tasks_completed: int = 0
    tasks_failed: int = 0
    execution_log: list[dict[str, Any]] = field(default_factory=list)

    # ------------------------------------------------------------------
    # 规划：启动前拿到真实可执行分区清单
    # ------------------------------------------------------------------

    def plan_dag(self, *, dag_name: str | None = None) -> dict[str, Any]:
        """只解析依赖，不执行任何管道，返回可执行/阻塞分区清单。"""
        dag = self._resolve_dag(dag_name)
        if not dag:
            return {
                "dag_name": dag_name or "",
                "found": False,
                "reason": "未找到可发布的DAG",
                "partitions": [],
                "executable_partitions": [],
                "blocked_partitions": [],
            }
        analysis = analyze_dag(dag)
        analysis["dag_name"] = dag.get("图名称", "")
        self._decorate_partitions(analysis)
        return {
            "dag_name": dag.get("图名称", ""),
            "found": True,
            "has_cycle": analysis["has_cycle"],
            "is_acyclic": analysis["is_acyclic"],
            "valid": analysis["valid"],
            "cycles": analysis["cycles"],
            "cycle_nodes": analysis["cycle_nodes"],
            "executable_nodes": analysis["executable_nodes"],
            "blocked_nodes": analysis["blocked_nodes"],
            "execution_order": analysis["execution_order"],
            "dependency_errors": analysis["dependency_errors"],
            "partitions": analysis["partitions"],
            "components": analysis["partitions"],
            "executable_partitions": [
                item["name"] for item in analysis["executable_partitions"]
            ],
            "blocked_partitions": [
                item["name"] for item in analysis["blocked_partitions"]
            ],
            "partition_count": analysis["partition_count"],
            "graph_fingerprint": analysis["graph_fingerprint"],
        }

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------

    def run_dag(
        self,
        *,
        dag_name: str | None = None,
        run_key: str | None = None,
        retry_of: str | None = None,
        fail_nodes: list[str] | None = None,
    ) -> dict[str, Any]:
        """解析DAG依赖关系并按拓扑顺序执行可执行分区。

        ``run_key`` 是幂等键：并发用同一个 ``run_key`` 触发时，只有第一次会
        真正写数据，其余请求返回同一份运行与分区结果。
        ``retry_of`` 指向此前失败的运行，新运行会带上那次运行的阻塞范围。
        """
        if fail_nodes is not None:
            self.fail_nodes = tuple(
                str(item).strip() for item in fail_nodes if str(item).strip()
            )
        dag = self._resolve_dag(dag_name)
        if not dag:
            return self._empty("未找到可发布的DAG")

        name = dag.get("图名称", "")
        idempotency_key = f"{name}::{run_key or uuid.uuid4().hex}"
        lock = _run_lock(idempotency_key)
        with lock:
            with _RUN_REGISTRY_LOCK:
                cached = _RUN_CACHE.get(idempotency_key)
            if cached:
                reused = copy.deepcopy(cached)
                reused["reused"] = True
                reused["idempotent_hit"] = True
                return reused

            with _STORE_WRITE_LOCK:
                result = self._perform_run(
                    dag=dag, run_key=run_key, retry_of=retry_of,
                    idempotency_key=idempotency_key,
                )
            with _RUN_REGISTRY_LOCK:
                _RUN_CACHE[idempotency_key] = copy.deepcopy(result)
            return result

    def _perform_run(
        self,
        *,
        dag: dict[str, Any],
        run_key: str | None,
        retry_of: str | None,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.run_id = f"batch-{hashlib.sha1(idempotency_key.encode('utf-8')).hexdigest()[:12]}"
        self.run_key = run_key or ""
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.execution_log = []

        analysis = analyze_dag(dag)
        analysis["dag_name"] = dag.get("图名称", "")
        self._decorate_partitions(analysis)
        pipelines = self._pipeline_map()
        previous_run = self._find_run(retry_of) if retry_of else None
        previous_scope = (
            previous_run.get("blocked_scope", {}) if previous_run else {}
        )

        node_results: dict[str, dict[str, Any]] = {}
        blocked_upstream: dict[str, list[str]] = {}

        # 1) 成环节点与依赖未定义节点：只登记阻塞，不写数据。
        for node in analysis["blocked_order"]:
            reason = analysis["blocked_reasons"].get(node, "依赖阻塞")
            pipeline = pipelines.get(node)
            node_results[node] = {
                "node": node,
                "pipeline": node,
                "status": "blocked",
                "reason": reason,
                "blocked_by": analysis["blocked_by"].get(node, []),
                "output_target": pipeline.get("输出目标", "") if pipeline else "",
                "executed_at": None,
                "wrote_rows": 0,
                "partition": self._partition_name_for(analysis, node),
            }
            blocked_upstream[node] = analysis["blocked_by"].get(node, [])
            self.execution_log.append(node_results[node])

        # 2) 可执行节点按拓扑顺序执行；运行期失败的下游立即转为阻塞。
        for node in analysis["execution_order"]:
            failed_upstream = [
                upstream
                for upstream in analysis["dependencies"].get(node, [])
                if node_results.get(upstream, {}).get("status") != "completed"
            ]
            if failed_upstream:
                pipeline = pipelines.get(node)
                node_results[node] = {
                    "node": node,
                    "pipeline": node,
                    "status": "blocked",
                    "reason": "上游运行失败，无法取得输入产物",
                    "blocked_by": failed_upstream,
                    "output_target": pipeline.get("输出目标", "") if pipeline else "",
                    "executed_at": None,
                    "wrote_rows": 0,
                    "partition": self._partition_name_for(analysis, node),
                }
                blocked_upstream[node] = failed_upstream
                self.execution_log.append(node_results[node])
                continue

            result = self._execute_node(node)
            node_results[node] = result
            if result.get("status") == "completed":
                self.tasks_completed += 1
            else:
                self.tasks_failed += 1
            self.execution_log.append(result)

        # 3) 落审计 / 运行分区 / 血缘 / 运行记录，全部以实际写入结果为准。
        receipts = self._persist_run_partitions(analysis, node_results, retry_of)
        audits = self._write_audit(analysis, node_results, receipts)
        run_partitions = [self._partition_view(row) for row in receipts]
        self._track_lineage(analysis, node_results)
        self._commit_checkpoint(analysis, node_results)

        blocked_nodes = [
            node for node in analysis["nodes"]
            if node_results.get(node, {}).get("status") != "completed"
        ]
        unproduced_targets = _dedupe([
            node_results[node].get("output_target", "")
            for node in blocked_nodes
            if node_results[node].get("output_target")
        ])
        blocked_scope = {
            "run_id": self.run_id,
            "nodes": blocked_nodes,
            "targets": unproduced_targets,
            "partitions": [
                item["name"] for item in run_partitions if not item["confirmed"]
            ],
            "cycle_nodes": analysis["cycle_nodes"],
            "cycles": analysis["cycles"],
        }

        if not analysis["executable_nodes"]:
            status = "阻塞"
        elif self.tasks_completed == len(analysis["nodes"]):
            status = "成功"
        elif self.tasks_completed > 0:
            status = "部分成功"
        else:
            status = "失败"

        run_record = {
            "id": _next_id(store.rows(self.run_table_key)),
            "run_id": self.run_id,
            "run_key": self.run_key,
            "dag_name": dag.get("图名称", ""),
            "status": status,
            "has_cycle": analysis["has_cycle"],
            "is_acyclic": analysis["is_acyclic"],
            "cycles": analysis["cycles"],
            "cycle_nodes": analysis["cycle_nodes"],
            "executable_nodes": analysis["executable_nodes"],
            "blocked_nodes": blocked_nodes,
            "execution_order": analysis["execution_order"],
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "blocked_tasks": len(blocked_nodes),
            "total_tasks": len(analysis["nodes"]),
            "partitions": run_partitions,
            "blocked_scope": blocked_scope,
            "previous_blocked_scope": previous_scope,
            "retry_of": retry_of or "",
            "graph_fingerprint": analysis["graph_fingerprint"],
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "audits_written": len(audits),
            "reused": False,
        }
        store.rows(self.run_table_key).append(run_record)

        return {
            "run_id": self.run_id,
            "run_key": self.run_key,
            "dag_name": dag.get("图名称", ""),
            "status": status,
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "blocked_tasks": len(blocked_nodes),
            "total_tasks": len(analysis["nodes"]),
            "has_cycle": analysis["has_cycle"],
            "is_acyclic": analysis["is_acyclic"],
            "cycles": analysis["cycles"],
            "cycle_nodes": analysis["cycle_nodes"],
            "executable_nodes": analysis["executable_nodes"],
            "blocked_nodes": blocked_nodes,
            "execution_order": analysis["execution_order"],
            "partitions": run_partitions,
            "executable_partitions": [
                item["name"] for item in run_partitions if item["confirmed"]
            ],
            "blocked_partitions": [
                item["name"] for item in run_partitions if not item["confirmed"]
            ],
            "blocked_scope": blocked_scope,
            "previous_blocked_scope": previous_scope,
            "retry_of": retry_of or "",
            "audits_written": len(audits),
            "execution_log": self.execution_log,
            "reused": False,
        }

    def retry_run(
        self,
        *,
        previous_run_id: str,
        dag_name: str | None = None,
        run_key: str | None = None,
    ) -> dict[str, Any]:
        """失败后重试：新运行关联上一次运行的阻塞范围。"""
        previous = self._find_run(previous_run_id)
        if previous is None:
            return self._empty(f"未找到此前的运行：{previous_run_id}")
        return self.run_dag(
            dag_name=dag_name or previous.get("dag_name"),
            run_key=run_key,
            retry_of=previous_run_id,
        )

    def _execute_node(self, node_name: str) -> dict[str, Any]:
        """执行单个可执行节点；只有完成后才写结果写入器。"""
        pipelines = self._pipeline_map()
        pipeline = pipelines.get(node_name)
        if not pipeline:
            return {
                "node": node_name,
                "pipeline": node_name,
                "status": "failed",
                "reason": "未找到对应管道配置",
                "blocked_by": [],
                "output_target": "",
                "executed_at": int(time.time() * 1000),
                "wrote_rows": 0,
                "partition": "",
            }

        time.sleep(0.001)
        output_target = pipeline.get("输出目标", "")
        failure_reason = should_fail(node_name, self.fail_nodes)
        if failure_reason:
            pipeline["status"] = "运行失败"
            pipeline["调度状态"] = f"已被{self.run_id}执行，执行失败"
            return {
                "node": node_name,
                "pipeline": pipeline.get("管道名称", ""),
                "status": "failed",
                "reason": failure_reason,
                "engine": pipeline.get("计算引擎", ""),
                "input_source": pipeline.get("输入源", ""),
                "output_target": output_target,
                "executed_at": int(time.time() * 1000),
                "wrote_rows": 0,
                "partition": "",
            }

        row_count = self._write_result(pipeline)

        pipeline["status"] = "已完成"
        pipeline["调度状态"] = f"已被{self.run_id}执行"
        return {
            "node": node_name,
            "pipeline": pipeline.get("管道名称", ""),
            "status": "completed",
            "engine": pipeline.get("计算引擎", ""),
            "input_source": pipeline.get("输入源", ""),
            "output_target": output_target,
            "executed_at": int(time.time() * 1000),
            "wrote_rows": row_count,
            "partition": "",
        }

    def _write_result(self, pipeline: dict[str, Any]) -> int:
        """把节点产物写入结果写入器，返回实际落地的行数。"""
        rows = store.rows(self.result_writer_key)
        row_count = 1
        rows.append({
            "id": _next_id(rows),
            "配置名称": f"{pipeline.get('管道名称', '')}-{self.run_id}",
            "写入目标类型": "表",
            "输出格式": "parquet",
            "分区策略": "按日分区",
            "写入模式": "追加",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "run_id": self.run_id,
            "node": pipeline.get("管道名称", ""),
            "管道名称": pipeline.get("管道名称", ""),
            "输入源": pipeline.get("输入源", ""),
            "输出目标": pipeline.get("输出目标", ""),
            "输出行数": row_count,
            "written_at": int(time.time() * 1000),
        })
        return row_count

    # ------------------------------------------------------------------
    # 运行产物与审计
    # ------------------------------------------------------------------

    def _persist_run_partitions(
        self,
        analysis: dict[str, Any],
        node_results: dict[str, dict[str, Any]],
        retry_of: str | None,
    ) -> list[dict[str, Any]]:
        """每个图分区只落一条运行分区记录，已确认状态以实际写入为准。"""
        rows = store.rows(self.run_partition_key)
        partitions: list[dict[str, Any]] = []
        for partition in analysis["partitions"]:
            nodes = partition["nodes"]
            completed = [
                node for node in nodes
                if node_results.get(node, {}).get("status") == "completed"
            ]
            unproduced = [node for node in nodes if node not in completed]
            actual_targets = _dedupe([
                node_results[node].get("output_target", "") for node in completed
            ])
            missing_targets = _dedupe([
                node_results.get(node, {}).get("output_target", "") for node in unproduced
            ])
            row_count = sum(
                int(node_results[node].get("wrote_rows", 0)) for node in completed
            )
            if completed and not unproduced:
                status = "已产出"
                confirmed = True
                reason = "全部分区节点已实际写入并通过审计"
            elif completed:
                status = "部分产出"
                confirmed = False
                reason = "部分节点因依赖环或上游阻塞未产出"
            else:
                status = "未产出"
                confirmed = False
                if partition["cycle_nodes"]:
                    reason = "依赖关系成环，整组分支无法出数"
                else:
                    reason = "没有可执行节点或上游运行失败"

            record = {
                "id": _next_id(rows),
                "run_id": self.run_id,
                "run_key": self.run_key,
                "dag_name": analysis.get("dag_name", ""),
                "partition_id": partition.get("partition_id", ""),
                "name": partition["name"],
                "分区名称": partition["name"],
                "nodes": nodes,
                "executable_nodes": partition["executable_nodes"],
                "blocked_nodes": unproduced,
                "cycle_nodes": partition["cycle_nodes"],
                "status": status,
                "状态": status,
                "confirmed": confirmed,
                "output_targets": actual_targets,
                "实际写入目标": actual_targets,
                "unproduced_targets": missing_targets,
                "未产出目标": missing_targets,
                "reason": reason,
                "阻塞原因": reason,
                "row_count": row_count,
                "输出行数": row_count,
                "produced_at": time.strftime("%Y-%m-%d %H:%M:%S") if confirmed else "",
                "retry_of": retry_of or "",
            }
            rows.append(record)

            # 回填到分析结果，方便调用方看到执行后的真实状态。
            partition["runtime_status"] = status
            partition["confirmed"] = confirmed
            partition["wrote_nodes"] = completed
            partition["unproduced_nodes"] = unproduced
            partition["actual_output_targets"] = actual_targets
            partition["unproduced_targets"] = missing_targets
            partition["row_count"] = row_count
            partition["reason"] = reason
            partitions.append(record)

        # 重新计算运行级可执行/阻塞分区
        analysis["executable_partitions"] = [
            item for item in analysis["partitions"] if item.get("confirmed")
        ]
        analysis["blocked_partitions"] = [
            item for item in analysis["partitions"] if not item.get("confirmed")
        ]
        return partitions

    def _write_audit(
        self,
        analysis: dict[str, Any],
        node_results: dict[str, dict[str, Any]],
        partition_receipts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """审计消费分支执行回执：只把确实落地的数据标为校验通过。

        ``partition_receipts`` 是本次运行每个分支真实的执行回执；审计按回执
        里的实际写入目标与行数判定，而不是按节点“执行过”就判定通过。
        """
        audit_rows = store.rows(self.output_audit_key)
        node_partition = {
            node: self._partition_name_for(analysis, node)
            for node in analysis["nodes"]
        }
        receipt_by_node: dict[str, dict[str, Any]] = {}
        for receipt in partition_receipts:
            for node in receipt.get("nodes", []):
                receipt_by_node[node] = receipt
        written: list[dict[str, Any]] = []
        for node in analysis["nodes"]:
            result = node_results.get(node, {})
            receipt = receipt_by_node.get(node, {})
            completed = result.get("status") == "completed"
            landed_rows = int(result.get("wrote_rows", 0)) if completed else 0
            if completed and landed_rows > 0 and receipt.get("confirmed"):
                verification = "校验通过"
                status = "已记录"
                reason = ""
                landed = True
            else:
                verification = (
                    "未产出（依赖环）"
                    if node in analysis["cycle_nodes"]
                    else "未产出（上游阻塞）"
                    if result.get("status") == "blocked"
                    else "未产出（执行失败）"
                )
                status = "校验失败"
                reason = result.get("reason", "没有实际写入结果")
                landed = False

            audit = {
                "id": _next_id(audit_rows),
                "管道名称": node,
                "输出目标": result.get("output_target", ""),
                "输出时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "输出行数": str(landed_rows),
                "校验结果": verification,
                "status": status,
                "pending": not landed,
                "abnormal": not landed,
                "run_id": self.run_id,
                "node": node,
                "partition": node_partition.get(node, ""),
                "partition_id": receipt.get("partition_id", ""),
                "partition_status": receipt.get("status", ""),
                "confirmed": bool(receipt.get("confirmed")),
                "blocked_by": result.get("blocked_by", []),
                "阻塞原因": reason,
                "已落地": landed,
                "实际写入行数": landed_rows,
                "校验依据": "结果写入器实际写入记录" if landed else "无实际写入记录",
            }
            audit_rows.append(audit)
            written.append(audit)
        return written

    # ------------------------------------------------------------------
    # 查询：审计、分区历史
    # ------------------------------------------------------------------

    def audit_report(self, *, run_id: str | None = None) -> dict[str, Any]:
        """对照一次运行实际写入的目标，返回审计明细和未产出目标。"""
        runs = store.rows(self.run_table_key)
        run = self._find_run(run_id) if run_id else (runs[-1] if runs else None)
        if run is None:
            return {"found": False, "reason": "未找到运行记录", "run_id": run_id or ""}
        actual_run_id = run.get("run_id", "")
        audits = [
            row for row in store.rows(self.output_audit_key)
            if row.get("run_id") == actual_run_id
        ]
        writes = [
            row for row in store.rows(self.result_writer_key)
            if row.get("run_id") == actual_run_id
        ]
        partition_rows = [
            row for row in store.rows(self.run_partition_key)
            if row.get("run_id") == actual_run_id
        ]
        partitions = [self._partition_view(row) for row in partition_rows]

        # 审计以运行实际写入的目标为准：没有对应写入记录的审计结果自动降级。
        landed_targets = {
            str(row.get("输出目标", "")) for row in writes if row.get("输出目标")
        }
        effective_audits: list[dict[str, Any]] = []
        for row in audits:
            audit = dict(row)
            declared = row.get("校验结果")
            target = str(row.get("输出目标", ""))
            really_landed = bool(row.get("已落地")) and target in landed_targets
            if declared == "校验通过" and not really_landed:
                audit["校验结果"] = "校验失败"
                audit["status"] = "校验失败"
                audit["已落地"] = False
                audit["校验依据"] = "本次运行没有对应目标的实际写入记录，校验结论降级"
                audit["abnormal"] = True
                audit["pending"] = True
            effective_audits.append(audit)
        audits = effective_audits
        passed = [row for row in audits if row.get("校验结果") == "校验通过"]
        failed = [row for row in audits if row.get("校验结果") != "校验通过"]
        return {
            "found": True,
            "run_id": actual_run_id,
            "dag_name": run.get("dag_name", ""),
            "status": run.get("status", ""),
            "has_cycle": run.get("has_cycle", False),
            "cycles": run.get("cycles", []),
            "cycle_nodes": run.get("cycle_nodes", []),
            "partitions": partitions,
            "partition_receipts": partition_rows,
            "audits": audits,
            "writes": writes,
            "summary": {
                "audit_count": len(audits),
                "passed": len(passed),
                "failed": len(failed),
                "written_targets": _dedupe([
                    str(row.get("输出目标", "")) for row in writes
                ]),
                "passed_targets": _dedupe([
                    str(row.get("输出目标", "")) for row in passed
                ]),
                "blocked_targets": _dedupe([
                    str(row.get("输出目标", "")) for row in failed
                    if row.get("输出目标")
                ]),
                "cycle_targets": _dedupe([
                    str(row.get("输出目标", "")) for row in failed
                    if row.get("校验结果") == "未产出（依赖环）"
                ]),
                "confirmed_partitions": [
                    item["name"] for item in partitions if item.get("confirmed")
                ],
                "unproduced_partitions": [
                    item["name"] for item in partitions if not item.get("confirmed")
                ],
            },
            "blocked_scope": run.get("blocked_scope", {}),
            "previous_blocked_scope": run.get("previous_blocked_scope", {}),
            "retry_of": run.get("retry_of", ""),
        }

    def partition_history(
        self,
        *,
        target: str | None = None,
        run_id: str | None = None,
        partition_id: str | None = None,
    ) -> dict[str, Any]:
        """跨运行查询分区产出情况，修正环后仍能看到此前未产出的分区。"""
        rows = store.rows(self.run_partition_key)
        filtered = []
        for row in rows:
            if run_id and row.get("run_id") != run_id:
                continue
            if partition_id and row.get("partition_id") != partition_id:
                continue
            if target:
                targets = [
                    *row.get("output_targets", []),
                    *row.get("unproduced_targets", []),
                ]
                if target not in targets:
                    continue
            filtered.append(row)
        return {
            "target": target or "",
            "run_id": run_id or "",
            "partition_id": partition_id or "",
            "total": len(filtered),
            "partitions": [self._partition_view(row) for row in filtered],
            "unproduced": [
                self._partition_view(row)
                for row in filtered
                if not row.get("confirmed")
            ],
            "produced": [
                self._partition_view(row)
                for row in filtered
                if row.get("confirmed")
            ],
        }

    def list_runs(self, *, limit: int = 50) -> dict[str, Any]:
        """列出运行记录，包含前次阻塞范围与重试关联。"""
        runs = store.rows(self.run_table_key)[-limit:]
        return {
            "total": len(store.rows(self.run_table_key)),
            "runs": [self._run_view(run) for run in runs],
        }

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        run = self._find_run(run_id)
        return self._run_view(run) if run else None

    # ------------------------------------------------------------------
    # 回填
    # ------------------------------------------------------------------

    def run_backfill(
        self,
        *,
        time_range: str | None = None,
        run_id: str | None = None,
        previous_run_id: str | None = None,
    ) -> dict[str, Any]:
        """执行历史回填任务，支持断点续做并关联上一次的阻塞范围。"""
        tasks = store.rows(self.backfill_key)
        active = [t for t in tasks if t.get("status") in ("待调度", "回填失败")]
        if not active:
            return self._empty("无待回填任务")
        task = active[0]

        previous = self._find_run(previous_run_id or run_id)
        blocked_scope = previous.get("blocked_scope", {}) if previous else {}
        target_pipeline = task.get("目标管道", "")
        if blocked_scope.get("nodes") and target_pipeline not in blocked_scope["nodes"]:
            # 优先回填此前确实被阻塞的管道，避免重试范围漂移。
            for node in blocked_scope["nodes"]:
                for candidate in active:
                    if candidate.get("目标管道") == node:
                        task = candidate
                        target_pipeline = node
                        break

        task["status"] = "回填中"
        task["执行状态"] = f"在{time.strftime('%H:%M:%S')}恢复回填"
        task["回填时间范围"] = time_range or task.get("回填时间范围", "")
        task["处理进度"] = f"已完成{self.tasks_completed + 1}个批次"
        task["关联运行"] = previous.get("run_id", "") if previous else (run_id or "")
        task["关联阻塞范围"] = blocked_scope
        task["前次运行"] = previous_run_id or run_id or ""
        return {
            "backfill": True,
            "task_name": task.get("任务名称", ""),
            "target_pipeline": target_pipeline,
            "time_range": task.get("回填时间范围", ""),
            "progress": task.get("处理进度", ""),
            "resumed_from": task.get("执行状态", ""),
            "linked_run": previous.get("run_id", "") if previous else (run_id or ""),
            "previous_blocked_scope": blocked_scope,
        }

    # ------------------------------------------------------------------
    # DAG 定义（修正环后用同一入口再执行）
    # ------------------------------------------------------------------

    def update_dag_definition(
        self,
        *,
        dag_name: str,
        nodes: str | None = None,
        dependencies: str | None = None,
    ) -> dict[str, Any]:
        """更新DAG节点/依赖定义。修正环后可以原样再次执行。"""
        dag = self._resolve_dag(dag_name)
        if not dag:
            return {"ok": False, "reason": f"未找到DAG：{dag_name}"}
        if nodes is not None:
            dag["节点列表"] = nodes
        if dependencies is not None:
            dag["依赖关系"] = dependencies
        plan = self.plan_dag(dag_name=dag_name)
        return {
            "ok": True,
            "dag_name": dag_name,
            "has_cycle": plan.get("has_cycle", False),
            "cycles": plan.get("cycles", []),
            "executable_partitions": plan.get("executable_partitions", []),
            "blocked_partitions": plan.get("blocked_partitions", []),
        }

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _resolve_dag(self, dag_name: str | None) -> dict[str, Any] | None:
        dags = store.rows(self.dag_key)
        published = [d for d in dags if d.get("status") == "已发布"]
        if not published:
            return None
        if dag_name:
            for d in published:
                if d.get("图名称") == dag_name:
                    return d
            return None
        return published[0]

    def _pipeline_map(self) -> dict[str, dict[str, Any]]:
        return {
            str(row.get("管道名称", "")): row
            for row in store.rows(self.pipeline_key)
            if row.get("管道名称")
        }

    def _decorate_partitions(self, analysis: dict[str, Any]) -> None:
        pipelines = self._pipeline_map()
        for position, partition in enumerate(analysis["partitions"], start=1):
            partition["name"] = f"分区{position}"
            partition["partition_id"] = hashlib.sha1(
                "|".join(sorted(partition["nodes"])).encode("utf-8")
            ).hexdigest()[:10]
            partition["intended_output_targets"] = _dedupe([
                pipelines.get(node, {}).get("输出目标", "")
                for node in partition["nodes"]
            ])
            partition["output_targets"] = partition["intended_output_targets"]
            partition["writable"] = bool(partition["executable_nodes"])
            if partition["cycle_nodes"]:
                partition["blocked_reason"] = "依赖关系成环，分支内节点及其下游均不可执行"
            elif partition["blocked_nodes"]:
                partition["blocked_reason"] = "上游节点未定义，无法确定输入产物"
            else:
                partition["blocked_reason"] = ""

    def _partition_name_for(self, analysis: dict[str, Any], node: str) -> str:
        for position, partition in enumerate(analysis["partitions"], start=1):
            if node in partition["nodes"]:
                return f"分区{position}"
        return ""

    def _find_run(self, run_id: str | None) -> dict[str, Any] | None:
        if not run_id:
            return None
        for run in store.rows(self.run_table_key):
            if run.get("run_id") == run_id:
                return run
        return None

    def _commit_checkpoint(
        self,
        analysis: dict[str, Any],
        node_results: dict[str, dict[str, Any]],
    ) -> None:
        """为本次运行提交检查点，记录已完成与阻塞节点。"""
        cps = store.rows(self.checkpoint_key)
        cps.append({
            "id": _next_id(cps),
            "管道名称": f"batch-{self.run_id}",
            "检查点序列": str(len(cps) + 1),
            "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "数据偏移量": str(self.tasks_completed),
            "恢复状态": "可恢复",
            "status": "活跃",
            "pending": False,
            "abnormal": bool(analysis["blocked_nodes"]),
            "run_id": self.run_id,
            "completed_nodes": [
                node for node, result in node_results.items()
                if result.get("status") == "completed"
            ],
            "blocked_nodes": analysis["blocked_nodes"],
        })

    def _track_lineage(
        self,
        analysis: dict[str, Any],
        node_results: dict[str, dict[str, Any]],
    ) -> None:
        """记录实际执行节点的血缘，阻塞节点标记为链路断裂。"""
        lineage_rows = store.rows(self.lineage_key)
        pipelines = self._pipeline_map()
        for node in analysis["nodes"]:
            result = node_results.get(node, {})
            upstream = analysis["dependencies"].get(node, [])
            lineage_rows.append({
                "id": _next_id(lineage_rows),
                "输入表": "、".join(upstream) if upstream else "原始源",
                "转换管道": node,
                "输出表": result.get("output_target")
                or pipelines.get(node, {}).get("输出目标", ""),
                "血缘类型": "DAG编排",
                "记录时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "已建立" if result.get("status") == "completed" else "链路断裂",
                "pending": False,
                "abnormal": result.get("status") != "completed",
                "run_id": self.run_id,
            })

    def _partition_view(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "run_id": row.get("run_id", ""),
            "run_key": row.get("run_key", ""),
            "dag_name": row.get("dag_name", ""),
            "partition_id": row.get("partition_id", ""),
            "name": row.get("name", ""),
            "nodes": list(row.get("nodes", [])),
            "executable_nodes": list(row.get("executable_nodes", [])),
            "blocked_nodes": list(row.get("blocked_nodes", [])),
            "cycle_nodes": list(row.get("cycle_nodes", [])),
            "status": row.get("status", ""),
            "confirmed": bool(row.get("confirmed")),
            "output_targets": list(row.get("output_targets", [])),
            "unproduced_targets": list(row.get("unproduced_targets", [])),
            "reason": row.get("reason", ""),
            "row_count": int(row.get("row_count", 0)),
            "produced_at": row.get("produced_at", ""),
            "retry_of": row.get("retry_of", ""),
        }

    def _run_view(self, run: dict[str, Any]) -> dict[str, Any]:
        return {
            "run_id": run.get("run_id", ""),
            "run_key": run.get("run_key", ""),
            "dag_name": run.get("dag_name", ""),
            "status": run.get("status", ""),
            "has_cycle": run.get("has_cycle", False),
            "cycles": run.get("cycles", []),
            "cycle_nodes": run.get("cycle_nodes", []),
            "executable_nodes": run.get("executable_nodes", []),
            "blocked_nodes": run.get("blocked_nodes", []),
            "tasks_completed": run.get("tasks_completed", 0),
            "tasks_failed": run.get("tasks_failed", 0),
            "blocked_tasks": run.get("blocked_tasks", 0),
            "total_tasks": run.get("total_tasks", 0),
            "partitions": run.get("partitions", []),
            "blocked_scope": run.get("blocked_scope", {}),
            "previous_blocked_scope": run.get("previous_blocked_scope", {}),
            "retry_of": run.get("retry_of", ""),
            "started_at": run.get("started_at", ""),
            "finished_at": run.get("finished_at", ""),
        }

    def _empty(self, reason: str) -> dict[str, Any]:
        return {
            "run_id": self.run_id or "none",
            "tasks_completed": 0,
            "tasks_failed": 0,
            "blocked_tasks": 0,
            "total_tasks": 0,
            "reason": reason,
        }
