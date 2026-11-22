"""流批一体管道API：流式处理、批量编排、检查点恢复、Schema兼容、快照管理。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.engine import StreamPipeline, BatchPipelineEngine, PipelineMonitorEngine, DataQualityEngine

router = APIRouter(prefix="/api/pipeline", tags=["数据处理管道"])


@router.post("/stream/run")
def run_stream_pipeline(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    """执行一次流处理管线：消费→清洗→窗口→聚合→迟到→写入→检查点。"""
    pipeline = StreamPipeline()
    return pipeline.run(batch_id=batch_id)


@router.get("/stream/checkpoint/latest")
def get_latest_checkpoint() -> dict[str, Any]:
    """获取最新检查点状态。"""
    pipeline = StreamPipeline()
    return pipeline.recover_from_checkpoint()


@router.post("/batch/dag/plan")
def plan_batch_dag(dag_name: str | None = Query(default=None)) -> dict[str, Any]:
    """启动前解析DAG依赖，返回真实可执行分区清单与阻塞范围。"""
    engine = BatchPipelineEngine()
    return engine.plan_dag(dag_name=dag_name)


@router.post("/batch/dag/run")
def run_batch_dag(
    dag_name: str | None = Query(default=None),
    run_key: str | None = Query(default=None),
    retry_of: str | None = Query(default=None),
    fail_nodes: str | None = Query(default=None),
) -> dict[str, Any]:
    """执行批量DAG编排管道；run_key 相同的并发触发只保留一份结果。"""
    engine = BatchPipelineEngine()
    injections = [
        item.strip() for item in (fail_nodes or "").split(",") if item.strip()
    ]
    return engine.run_dag(
        dag_name=dag_name, run_key=run_key, retry_of=retry_of,
        fail_nodes=injections or None,
    )


@router.post("/batch/dag/retry")
def retry_batch_dag(
    previous_run_id: str = Query(...),
    dag_name: str | None = Query(default=None),
    run_key: str | None = Query(default=None),
) -> dict[str, Any]:
    """失败后重试：新运行关联上一次运行的阻塞范围。"""
    engine = BatchPipelineEngine()
    return engine.retry_run(
        previous_run_id=previous_run_id, dag_name=dag_name, run_key=run_key)


@router.post("/batch/dag/definition")
def update_dag_definition(
    dag_name: str = Query(...),
    nodes: str | None = Query(default=None),
    dependencies: str | None = Query(default=None),
) -> dict[str, Any]:
    """更新DAG节点/依赖定义（修正环后可用同一入口再次执行）。"""
    engine = BatchPipelineEngine()
    return engine.update_dag_definition(
        dag_name=dag_name, nodes=nodes, dependencies=dependencies)


@router.get("/batch/runs")
def list_batch_runs(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    """列出批量DAG运行记录，含阻塞范围与重试关联。"""
    engine = BatchPipelineEngine()
    return engine.list_runs(limit=limit)


@router.get("/batch/runs/{run_id}")
def get_batch_run(run_id: str) -> dict[str, Any]:
    """查看单次运行详情。"""
    engine = BatchPipelineEngine()
    run = engine.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行 {run_id} 不存在")
    return run


@router.get("/batch/audit")
def get_batch_audit(run_id: str | None = Query(default=None)) -> dict[str, Any]:
    """输出审计：只以实际写入结果判定校验通过，并列出未产出目标。"""
    engine = BatchPipelineEngine()
    report = engine.audit_report(run_id=run_id)
    if not report.get("found"):
        raise HTTPException(status_code=404, detail=report.get("reason", "未找到运行记录"))
    return report


@router.get("/batch/partitions/history")
def get_partition_history(
    target: str | None = Query(default=None),
    run_id: str | None = Query(default=None),
    partition_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """跨运行查看分区产出情况，修正环后仍能查到此前未产出的分区。"""
    engine = BatchPipelineEngine()
    return engine.partition_history(
        target=target, run_id=run_id, partition_id=partition_id)


@router.post("/batch/backfill")
def run_backfill(
    time_range: str | None = Query(default=None),
    run_id: str | None = Query(default=None),
    previous_run_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """执行历史回填任务，并关联此前的阻塞范围。"""
    engine = BatchPipelineEngine()
    return engine.run_backfill(
        time_range=time_range, run_id=run_id, previous_run_id=previous_run_id)


@router.get("/monitor/metrics")
def collect_pipeline_metrics() -> dict[str, Any]:
    """采集管道运行时指标。"""
    engine = PipelineMonitorEngine()
    return engine.collect_metrics()


@router.post("/quality/schema/check")
def check_schema_compatibility(
    schema_name: str = Query(...),
    new_fields: str = Query(...),
) -> dict[str, Any]:
    """检查Schema兼容性。"""
    engine = DataQualityEngine()
    fields = [f.strip() for f in new_fields.split(",") if f.strip()]
    return engine.check_schema_compatibility(schema_name=schema_name, new_fields=fields)


@router.post("/quality/iceberg/snapshot")
def create_iceberg_snapshot(table_name: str = Query(...)) -> dict[str, Any]:
    """为Iceberg表创建快照。"""
    engine = DataQualityEngine()
    result = engine.create_snapshot(table_name)
    if not result.get("snapshot_created"):
        raise HTTPException(status_code=404, detail=result.get("reason", "未知错误"))
    return result


@router.post("/quality/sample")
def execute_sampling(
    target_table: str = Query(...),
    sample_ratio: float = Query(default=0.1, ge=0.0, le=1.0),
) -> dict[str, Any]:
    """执行数据采样。"""
    engine = DataQualityEngine()
    return engine.execute_sampling(target_table=target_table, sample_ratio=sample_ratio)


@router.get("/quality/health")
def check_health(
    records_processed: int = Query(default=100),
    records_failed: int = Query(default=0),
) -> dict[str, Any]:
    """检查管道健康状态。"""
    engine = PipelineMonitorEngine()
    return engine.check_pipeline_health(records_processed, records_failed)