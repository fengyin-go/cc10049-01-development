"""跨模块业务管线路由：提供端到端业务链路执行与快照查询。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.pipelines import pipeline_for, list_pipelines
from app.pipelines.data_consumption import DataConsumptionPipeline
from app.pipelines.exception_pipeline import ExceptionPipeline
from app.pipelines.flow_control import FlowControlPipeline
from app.pipelines.ingestion_pipeline import IngestionPipeline
from app.pipelines.storage_pipeline import StoragePipeline

router = APIRouter(prefix="/api/pipelines", tags=["业务管线"])


class IngestionRequest(BaseModel):
    source_key: str = Field(default="app-log", description="日志源标识")
    raw_lines: list[str] = Field(default_factory=list)
    parser_key: str = Field(default="", description="解析模板标识")
    normalize_rules: list[str] = Field(default_factory=list)
    enrich_rules: list[str] = Field(default_factory=list)


class StorageRequest(BaseModel):
    log_entries: list[dict[str, Any]] = Field(default_factory=list)
    filter_rule_keys: list[str] = Field(default_factory=list)
    store_engine: str = Field(default="elasticsearch")
    rotate_policy: str = Field(default="time_based")
    clean_policy: str = Field(default="by_age")


class ExceptionRequest(BaseModel):
    spans: list[dict[str, Any]] = Field(default_factory=list)


class FlowControlRequest(BaseModel):
    sources: list[dict[str, Any]] = Field(default_factory=list)


class ExportRequest(BaseModel):
    query_condition: str = Field(default="")
    export_format: str = Field(default="csv")
    time_range: str = Field(default="")
    field_selection: str = Field(default="")


class ReplayRequest(BaseModel):
    source_storage: str = Field(default="")
    time_range: str = Field(default="")
    replay_rate: str = Field(default="1x")
    target_pipeline: str = Field(default="ingestion")
    dedup_strategy: str = Field(default="by_key")


# ---------------------------------------------------------------------------
# 管线元数据
# ---------------------------------------------------------------------------
@router.get("")
def get_pipelines() -> list[dict[str, Any]]:
    """列出所有跨模块业务管线及其阶段。"""
    return list_pipelines()


@router.get("/snapshot")
def pipeline_snapshots() -> dict[str, Any]:
    """获取所有管线的运行快照。"""
    snapshots: dict[str, Any] = {}
    for name in ("ingestion", "storage", "exception", "flow_control", "data_consumption"):
        pipeline = pipeline_for(name)
        if pipeline and hasattr(pipeline, "snapshot"):
            snapshots[name] = pipeline.snapshot()
    return snapshots


# ---------------------------------------------------------------------------
# 接入解析管线
# ---------------------------------------------------------------------------
@router.post("/ingestion/process")
def ingestion_process(req: IngestionRequest) -> dict[str, Any]:
    pipeline = pipeline_for("ingestion")
    if not isinstance(pipeline, IngestionPipeline):
        raise HTTPException(status_code=500, detail="接入解析管线不可用")
    results = pipeline.process(
        source_key=req.source_key,
        raw_lines=req.raw_lines,
        parser_key=req.parser_key,
        normalize_rules=req.normalize_rules,
        enrich_rules=req.enrich_rules,
    )
    return {"ok": True, "results": results, "snapshot": pipeline.snapshot()}


@router.get("/ingestion/snapshot")
def ingestion_snapshot() -> dict[str, Any]:
    pipeline = pipeline_for("ingestion")
    if not isinstance(pipeline, IngestionPipeline):
        raise HTTPException(status_code=500, detail="接入解析管线不可用")
    return pipeline.snapshot()


# ---------------------------------------------------------------------------
# 存储生命周期管线
# ---------------------------------------------------------------------------
@router.post("/storage/process")
def storage_process(req: StorageRequest) -> dict[str, Any]:
    pipeline = pipeline_for("storage")
    if not isinstance(pipeline, StoragePipeline):
        raise HTTPException(status_code=500, detail="存储生命周期管线不可用")
    results = pipeline.process_batch(
        log_entries=req.log_entries,
        filter_rule_keys=req.filter_rule_keys,
        store_engine=req.store_engine,
        rotate_policy=req.rotate_policy,
        clean_policy=req.clean_policy,
    )
    return {"ok": True, "results": results, "snapshot": pipeline.snapshot()}


@router.get("/storage/snapshot")
def storage_snapshot() -> dict[str, Any]:
    pipeline = pipeline_for("storage")
    if not isinstance(pipeline, StoragePipeline):
        raise HTTPException(status_code=500, detail="存储生命周期管线不可用")
    return pipeline.snapshot()


# ---------------------------------------------------------------------------
# 异常处理管线
# ---------------------------------------------------------------------------
@router.post("/exception/process")
def exception_process(req: ExceptionRequest) -> dict[str, Any]:
    pipeline = pipeline_for("exception")
    if not isinstance(pipeline, ExceptionPipeline):
        raise HTTPException(status_code=500, detail="异常处理管线不可用")
    results = pipeline.process(spans=req.spans)
    return {
        "ok": True,
        "results": results,
        "alerts": pipeline.alert_summary(),
        "snapshot": pipeline.snapshot(),
    }


@router.get("/exception/snapshot")
def exception_snapshot() -> dict[str, Any]:
    pipeline = pipeline_for("exception")
    if not isinstance(pipeline, ExceptionPipeline):
        raise HTTPException(status_code=500, detail="异常处理管线不可用")
    return pipeline.snapshot()


# ---------------------------------------------------------------------------
# 流控监控管线
# ---------------------------------------------------------------------------
@router.post("/flow-control/process")
def flow_control_process(req: FlowControlRequest) -> dict[str, Any]:
    pipeline = pipeline_for("flow_control")
    if not isinstance(pipeline, FlowControlPipeline):
        raise HTTPException(status_code=500, detail="流控监控管线不可用")
    results = pipeline.process_batch(sources=req.sources)
    return {"ok": True, "results": results, "snapshot": pipeline.snapshot()}


@router.get("/flow-control/snapshot")
def flow_control_snapshot() -> dict[str, Any]:
    pipeline = pipeline_for("flow_control")
    if not isinstance(pipeline, FlowControlPipeline):
        raise HTTPException(status_code=500, detail="流控监控管线不可用")
    return pipeline.snapshot()


# ---------------------------------------------------------------------------
# 数据消费管线
# ---------------------------------------------------------------------------
@router.post("/export/create")
def create_export_task(req: ExportRequest) -> dict[str, Any]:
    pipeline = pipeline_for("data_consumption")
    if not isinstance(pipeline, DataConsumptionPipeline):
        raise HTTPException(status_code=500, detail="数据消费管线不可用")
    result = pipeline.create_export_task(
        query_condition=req.query_condition,
        export_format=req.export_format,
        time_range=req.time_range,
        field_selection=req.field_selection,
    )
    return {"ok": True, "task": result}


@router.post("/export/{task_id}/run")
def run_export(task_id: str) -> dict[str, Any]:
    pipeline = pipeline_for("data_consumption")
    if not isinstance(pipeline, DataConsumptionPipeline):
        raise HTTPException(status_code=500, detail="数据消费管线不可用")
    result = pipeline.run_export(task_id)
    return {"ok": True, "task": result}


@router.post("/export/{task_id}/cancel")
def cancel_export(task_id: str) -> dict[str, Any]:
    pipeline = pipeline_for("data_consumption")
    if not isinstance(pipeline, DataConsumptionPipeline):
        raise HTTPException(status_code=500, detail="数据消费管线不可用")
    result = pipeline.cancel_export(task_id)
    return {"ok": True, "task": result}


@router.post("/replay/create")
def create_replay_task(req: ReplayRequest) -> dict[str, Any]:
    pipeline = pipeline_for("data_consumption")
    if not isinstance(pipeline, DataConsumptionPipeline):
        raise HTTPException(status_code=500, detail="数据消费管线不可用")
    result = pipeline.create_replay_task(
        source_storage=req.source_storage,
        time_range=req.time_range,
        replay_rate=req.replay_rate,
        target_pipeline=req.target_pipeline,
        dedup_strategy=req.dedup_strategy,
    )
    return {"ok": True, "task": result}


@router.post("/replay/{task_id}/start")
def start_replay(task_id: str) -> dict[str, Any]:
    pipeline = pipeline_for("data_consumption")
    if not isinstance(pipeline, DataConsumptionPipeline):
        raise HTTPException(status_code=500, detail="数据消费管线不可用")
    result = pipeline.start_replay(task_id)
    return {"ok": True, "task": result}


@router.post("/replay/{task_id}/progress")
def progress_replay(task_id: str, lines: int = 1000) -> dict[str, Any]:
    pipeline = pipeline_for("data_consumption")
    if not isinstance(pipeline, DataConsumptionPipeline):
        raise HTTPException(status_code=500, detail="数据消费管线不可用")
    result = pipeline.progress_replay(task_id, lines)
    return {"ok": True, "task": result}


@router.post("/replay/{task_id}/cancel")
def cancel_replay(task_id: str) -> dict[str, Any]:
    pipeline = pipeline_for("data_consumption")
    if not isinstance(pipeline, DataConsumptionPipeline):
        raise HTTPException(status_code=500, detail="数据消费管线不可用")
    result = pipeline.cancel_replay(task_id)
    return {"ok": True, "task": result}


@router.get("/data-consumption/snapshot")
def data_consumption_snapshot() -> dict[str, Any]:
    pipeline = pipeline_for("data_consumption")
    if not isinstance(pipeline, DataConsumptionPipeline):
        raise HTTPException(status_code=500, detail="数据消费管线不可用")
    return pipeline.snapshot()