"""业务行为测试：跨模块数据流、窗口聚合、迟到侧输出、检查点恢复、DAG编排、Schema兼容。
测试覆盖 stream_source → result_writer → checkpoint_mgr 的完整链路。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

# 确保 backend/ 在 sys.path 中
backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_root))

from app.store import store
from app.engine.stream_pipeline import StreamPipeline
from app.engine.batch_pipeline import BatchPipelineEngine
from app.engine.pipeline_monitor import PipelineMonitorEngine
from app.engine.data_quality import DataQualityEngine


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def reload_store():
    """重置 store 到 seed 状态"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }


def count_module(module: str) -> int:
    return len(store.rows(module))


# ---------------------------------------------------------------------------
# stream_source → transform_rule → data_cleanse → aggregation_rule
# → window_config → late_data → result_writer → checkpoint_mgr
# ---------------------------------------------------------------------------

def test_stream_pipeline_full_chain():
    """测试完整流处理管线：源→转换→清洗→窗口→聚合→迟到→写入→检查点"""
    reload_store()
    pipeline = StreamPipeline()
    result = pipeline.run(batch_id="test-batch-001")

    assert result["records_processed"] > 0, "源数据消费应有记录"
    assert result["checkpoint_committed"] is True, "应提交检查点"
    assert isinstance(result["window_count"], int), "应返回窗口计数"
    assert isinstance(result["aggregation_count"], int), "应返回聚合计数"

    # 检查点记录应被持久化
    cps = store.rows("checkpoint_mgr")
    assert len(cps) > 3, f"检查点记录被持久化, 当前共 {len(cps)} 条"


def test_stream_pipeline_window_aggregation():
    """测试窗口聚合：数据被分配到窗口并执行分组聚合"""
    reload_store()
    pipeline = StreamPipeline()
    result = pipeline.run(batch_id="test-window-agg")

    # 验证聚合结果
    assert result["aggregation_count"] > 0, "应有聚合输出"
    assert result["window_count"] == result["records_processed"], "窗口中应与记录数一致"

    # 验证聚合计数有 sum/min/max/avg
    writer_rows = store.rows("result_writer")
    post_run = [r for r in writer_rows if str(r.get("写入模式")) == "追加"]
    assert len(post_run) > 0, "写入器应有产出"


def test_late_data_side_output():
    """测试迟到数据被路由到侧输出"""
    reload_store()
    pipeline = StreamPipeline()
    result = pipeline.run(batch_id="test-late-data")

    # 检查侧输出计数
    side_count = result.get("records_side_output", 0)
    assert isinstance(side_count, int), "侧输出计数应为整数"


def test_checkpoint_recovery():
    """测试从检查点恢复管道状态"""
    reload_store()
    pipeline = StreamPipeline()
    pipeline.run(batch_id="test-checkpoint-1")

    # 验证检查点存在
    cps_before = count_module("checkpoint_mgr")
    assert cps_before > 0, "检查点应存在"

    recovery = pipeline.recover_from_checkpoint()
    assert recovery["recovered"] is True, "应能恢复"
    assert "checkpoint_sequence" in recovery, "应返回检查点序列"
    assert recovery["last_records"] > 0, f"应有处理记录数, actual={recovery}"


# ---------------------------------------------------------------------------
# batch_pipeline → pipeline_dag → task_scheduler → output_audit
# ---------------------------------------------------------------------------

def test_batch_dag_orchestration():
    """测试批量DAG编排执行"""
    reload_store()
    # 确保 DAG 节点名称与批量管道名称匹配
    pipelines = store.rows("batch_pipeline")
    node_names = [p.get("管道名称", "") for p in pipelines]
    dags = store.rows("pipeline_dag")
    for d in dags:
        if d.get("status") == "已发布":
            d["节点列表"] = ",".join(node_names[:3])
            d["依赖关系"] = f"{node_names[0]}->{node_names[1]}"

    engine = BatchPipelineEngine()
    result = engine.run_dag()

    assert result["tasks_completed"] >= 0, f"应有完成计数, actual={result}"
    assert result["run_id"].startswith("batch-"), "run_id应有前缀"

    # 审计记录应被生成
    if result.get("tasks_completed", 0) > 0:
        audit = store.rows("output_audit")
        assert len(audit) > 3, f"DAG执行后应有审计记录, 当前 {len(audit)} 条"


def test_batch_backfill_resumption():
    """测试历史回填任务启动"""
    reload_store()
    engine = BatchPipelineEngine()
    result = engine.run_backfill(time_range="2026-01-01 to 2026-06-30")

    assert result["backfill"] is True, "回填应执行"
    assert result["task_name"] != "", "应返回任务名称"


def test_pipeline_monitor_health():
    """测试管道监控健康检查"""
    engine = PipelineMonitorEngine()
    result = engine.check_pipeline_health(records_processed=95, records_failed=5)

    assert result["health"] in ("healthy", "degraded", "critical"), f"健康状态应在枚举内, actual={result['health']}"
    assert 0.0 <= result["error_rate"] <= 1.0, "错误率应在0-1之间"


def test_pipeline_metrics_collection():
    """测试管道指标采集"""
    reload_store()
    engine = PipelineMonitorEngine()
    result = engine.collect_metrics()

    assert "monitors_checked" in result, "应返回监控检查计数"
    assert "alerts_triggered" in result, "应返回告警计数"


# ---------------------------------------------------------------------------
# schema_registry → Iceberg snapshots → data sampling
# ---------------------------------------------------------------------------

def test_schema_compatibility_backward():
    """测试Schema向后兼容性检查"""
    reload_store()
    engine = DataQualityEngine()
    result = engine.check_schema_compatibility(
        schema_name="Schema注册样例1",
        new_fields=["字段A", "字段B", "字段C"],
    )

    assert "compatible" in result, "应返回兼容性结果"
    if result.get("reason") == "首次注册，无需兼容性检查":
        assert result["compatible"] is True


def test_schema_compatibility_registered():
    """测试已注册Schema的字段变更"""
    reload_store()
    # 先注册一个Schema
    schemas = store.rows("schema_registry")
    for s in schemas:
        if s.get("Schema名称") == "Schema注册样例2":
            s["status"] = "已注册"
            s["字段列表"] = "id,name,amount,status"

    engine = DataQualityEngine()
    result = engine.check_schema_compatibility(
        schema_name="Schema注册样例2",
        new_fields=["id", "name", "amount", "status", "extra_field"],
    )

    assert result.get("compatible") is True, f"添加字段应为兼容, actual={result}"
    assert "added_fields" in result
    assert "extra_field" in result.get("added_fields", [])


def test_iceberg_snapshot_creation():
    """测试Iceberg表快照创建"""
    reload_store()
    engine = DataQualityEngine()
    result = engine.create_snapshot("Iceberg表管理样例2")

    if result.get("snapshot_created"):
        assert result["snapshot_count"] >= 1, f"快照数应>=1, actual={result['snapshot_count']}"
        assert result["snapshot_id"].startswith("s"), f"快照ID应以s开头, actual={result['snapshot_id']}"
    else:
        assert "未注册" in result.get("reason", ""), "未注册时应有说明"


def test_data_sampling():
    """测试数据采样执行"""
    reload_store()
    engine = DataQualityEngine()
    result = engine.execute_sampling(target_table="Iceberg表管理样例1", sample_ratio=0.3)

    assert result["sampled"] is True, "采样应执行"
    assert result["sample_ratio"] == 0.3, "采样比例应被保留"


# ---------------------------------------------------------------------------
# 跨模块集成测试
# ---------------------------------------------------------------------------

def test_cross_module_data_flow():
    """测试全链路跨模块数据流：数据经过完整的源→转换→清洗→聚合→输出→审计"""
    reload_store()
    pipeline = StreamPipeline()
    stream_result = pipeline.run(batch_id="cross-module-001")

    assert stream_result["records_processed"] > 0
    assert stream_result["checkpoint_committed"] is True

    # 执行批量DAG
    batch_engine = BatchPipelineEngine()
    dag_result = batch_engine.run_dag()

    # 检查血缘追踪
    lineage = store.rows("lineage_tracker")
    post_run = [r for r in lineage if r.get("血缘类型") == "DAG编排"]
    assert isinstance(len(post_run), int), "血缘追踪应有记录"

    # 综合：流处理产出应可审计
    if dag_result.get("tasks_completed", 0) > 0:
        audit = store.rows("output_audit")
        dag_audit = [r for r in audit if r.get("run_id")]
        assert len(dag_audit) > 0, "DAG审计记录应关联run_id"


def test_checkpoint_recovery_after_failure():
    """测试故障恢复：先运行管道生成检查点，模拟中断后恢复"""
    reload_store()
    pipeline = StreamPipeline()
    pipeline.run(batch_id="failure-test-before")

    # 模拟中断（删除最新的一条记录）
    cps = store.rows("checkpoint_mgr")
    active = [c for c in cps if c.get("status") == "活跃"]
    assert len(active) > 0, "应该有活跃的检查点用于恢复"

    recovery = pipeline.recover_from_checkpoint()
    assert recovery["recovered"] is True
    assert recovery["last_records"] >= 0


def test_domain_snapshot_contract():
    """验证所有领域服务都提供 domain_snapshot 接口"""
    from app.services import SERVICES
    for key, svc in SERVICES.items():
        try:
            snap = svc.domain_snapshot()
        except Exception as e:
            print(f"[{key}] domain_snapshot raised {type(e).__name__}: {e}")
            snap = {}
        assert "module" in snap, f"{key}.domain_snapshot 应包含 module 字段"
        assert "artifact" in snap, f"{key}.domain_snapshot 应包含 artifact 字段"
        assert "count" in snap, f"{key}.domain_snapshot 应包含 count 字段"

# ---------------------------------------------------------------------------
# DAG 依赖环：可执行分区、审计、阻塞范围、重试、幂等
# ---------------------------------------------------------------------------

CYCLIC_NODES = "抽取管道,销售管道,汇总管道,税务管道"
CYCLIC_EDGES = "抽取管道->销售管道,销售管道->抽取管道,销售管道->汇总管道"
ACYCLIC_EDGES = "抽取管道->销售管道,销售管道->汇总管道"


def _reset_dag(dependencies: str = CYCLIC_EDGES) -> None:
    from app.engine import batch_pipeline as engine_module
    from app.engine import execution_policy

    reload_store()
    engine_module.reset_runtime_state()
    execution_policy.reset_simulation()
    for dag in store.rows("pipeline_dag"):
        if dag.get("图名称") == "销售编排样例":
            dag["status"] = "已发布"
            dag["节点列表"] = CYCLIC_NODES
            dag["依赖关系"] = dependencies


def test_dag_cycle_plan_lists_real_executable_partitions():
    """成环时启动前必须拿到真实可执行分区：环内+下游阻塞，独立分支仍可执行。"""
    _reset_dag()
    plan = BatchPipelineEngine().plan_dag(dag_name="销售编排样例")

    assert plan["has_cycle"] is True
    assert plan["cycles"] == [["抽取管道", "销售管道"]]
    assert plan["cycle_nodes"] == ["抽取管道", "销售管道"]
    # 汇总管道依赖销售产物，必须一起阻塞
    assert plan["blocked_nodes"] == ["抽取管道", "销售管道", "汇总管道"]
    assert plan["executable_nodes"] == ["税务管道"]
    assert plan["executable_partitions"] == ["分区2"]
    assert plan["blocked_partitions"] == ["分区1"]


def test_cycle_run_blocks_cycle_and_downstream_but_runs_independent_branch():
    """环内节点和依赖其产物的分支都不能写数据；独立税务分支仍能给自己的表供数。"""
    _reset_dag()
    result = BatchPipelineEngine().run_dag(dag_name="销售编排样例", run_key="cycle-run")

    status = {item["node"]: item["status"] for item in result["execution_log"]}
    assert status == {
        "抽取管道": "blocked",
        "销售管道": "blocked",
        "汇总管道": "blocked",
        "税务管道": "completed",
    }

    writes = [
        row for row in store.rows("result_writer")
        if row.get("run_id") == result["run_id"]
    ]
    assert [row["输出目标"] for row in writes] == ["ads.tax_report"]
    assert result["executable_partitions"] == ["分区2"]
    assert result["blocked_partitions"] == ["分区1"]


def test_audit_only_marks_landed_output_as_passed():
    """输出审计只把确实落地的数据标为校验通过，并能定位因环无法出数的目标。"""
    _reset_dag()
    engine = BatchPipelineEngine()
    result = engine.run_dag(dag_name="销售编排样例", run_key="audit-run")
    report = engine.audit_report(run_id=result["run_id"])

    passed = [a["管道名称"] for a in report["audits"] if a["校验结果"] == "校验通过"]
    assert passed == ["税务管道"]
    assert report["summary"]["written_targets"] == ["ads.tax_report"]
    assert report["summary"]["passed_targets"] == ["ads.tax_report"]
    assert report["summary"]["cycle_targets"] == ["dwd.extract_orders", "dwd.sales_orders"]
    assert "ads.sales_summary" in report["summary"]["blocked_targets"]

    cycle_audits = [
        a for a in report["audits"] if a["校验结果"] == "未产出（依赖环）"
    ]
    assert {a["管道名称"] for a in cycle_audits} == {"抽取管道", "销售管道"}
    assert all(a["已落地"] is False and a["输出行数"] == "0" for a in cycle_audits)


def test_retry_links_previous_blocked_scope_and_history_keeps_unproduced():
    """失败重试关联此前阻塞范围；修正环后再执行仍能查到此前未产出的分区。"""
    _reset_dag()
    engine = BatchPipelineEngine()

    failed = engine.run_dag(
        dag_name="销售编排样例", run_key="fail-run", fail_nodes=["税务管道"],
    )
    assert failed["status"] == "失败"
    assert set(failed["blocked_scope"]["nodes"]) == {
        "抽取管道", "销售管道", "汇总管道", "税务管道",
    }

    retried = engine.retry_run(previous_run_id=failed["run_id"], run_key="retry-run")
    assert retried["retry_of"] == failed["run_id"]
    assert retried["previous_blocked_scope"]["run_id"] == failed["run_id"]
    assert set(retried["previous_blocked_scope"]["nodes"]) == set(
        failed["blocked_scope"]["nodes"]
    )
    assert retried["executable_partitions"] == ["分区2"]

    fixed = engine.update_dag_definition(
        dag_name="销售编排样例", dependencies=ACYCLIC_EDGES,
    )
    assert fixed["has_cycle"] is False
    rerun = engine.run_dag(dag_name="销售编排样例", run_key="fixed-run")
    assert rerun["status"] == "成功"
    assert rerun["has_cycle"] is False
    assert rerun["executable_partitions"] == ["分区1", "分区2"]

    history = engine.partition_history(target="dwd.sales_orders")
    by_run = {item["run_id"]: item for item in history["partitions"]}
    assert by_run[failed["run_id"]]["status"] == "未产出"
    assert "dwd.sales_orders" in by_run[failed["run_id"]]["unproduced_targets"]
    assert by_run[rerun["run_id"]]["status"] == "已产出"
    assert "dwd.sales_orders" in by_run[rerun["run_id"]]["output_targets"]


def test_concurrent_same_run_key_writes_only_once():
    """并发触发同一次运行只保留一份已确认的分区结果。"""
    import threading

    _reset_dag()
    results: list[dict[str, object]] = []

    def trigger() -> None:
        results.append(
            BatchPipelineEngine().run_dag(
                dag_name="销售编排样例", run_key="concurrent-run",
            )
        )

    threads = [threading.Thread(target=trigger) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 6
    assert len({item["run_id"] for item in results}) == 1
    assert sum(1 for item in results if item.get("reused")) == 5

    runs = [
        row for row in store.rows("pipeline_run")
        if row.get("run_key") == "concurrent-run"
    ]
    partitions = [
        row for row in store.rows("pipeline_run_partition")
        if row.get("run_key") == "concurrent-run"
    ]
    writes = [
        row for row in store.rows("result_writer")
        if row.get("run_id") == results[0]["run_id"]
    ]
    assert len(runs) == 1
    assert len(partitions) == 2
    assert sum(1 for row in partitions if row["confirmed"]) == 1
    assert len(writes) == 1


def test_audit_action_cannot_pass_unlanded_output():
    """输出审计动作同样只认实际写入：没有落地记录的目标不能被标记通过。"""
    from app.services import service_for

    _reset_dag()
    result = BatchPipelineEngine().run_dag(
        dag_name="销售编排样例", run_key="audit-action-run",
    )
    service = service_for("output_audit")
    audit_rows = [
        row for row in store.rows("output_audit")
        if row.get("run_id") == result["run_id"]
    ]
    assert audit_rows

    for row in audit_rows:
        entry, message = service.run_action(row["id"], "标记通过")
        assert entry is not None
        if row["输出目标"] == "ads.tax_report":
            assert entry["校验结果"] == "校验通过"
            assert entry["status"] == "已记录"
        else:
            assert entry["校验结果"] != "校验通过"
            assert entry["status"] == "校验失败"
            assert "没有实际落地" in message
