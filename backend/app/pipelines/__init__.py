"""跨模块业务管线注册：每条管线串联多个领域服务，提供端到端处理能力。"""

from __future__ import annotations

from app.pipelines.ingestion_pipeline import IngestionPipeline
from app.pipelines.storage_pipeline import StoragePipeline
from app.pipelines.exception_pipeline import ExceptionPipeline
from app.pipelines.flow_control import FlowControlPipeline
from app.pipelines.data_consumption import DataConsumptionPipeline

PIPELINES: dict[str, object] = {
    "ingestion": IngestionPipeline(),
    "storage": StoragePipeline(),
    "exception": ExceptionPipeline(),
    "flow_control": FlowControlPipeline(),
    "data_consumption": DataConsumptionPipeline(),
}


def pipeline_for(name: str) -> object | None:
    return PIPELINES.get(name)


def list_pipelines() -> list[dict]:
    return [
        {
            "name": "ingestion",
            "label": "接入解析链路",
            "desc": "日志源→日志解析→字段标准化→日志增强",
            "stages": ["log_source", "log_parser", "field_normalize", "log_enrich"],
        },
        {
            "name": "storage",
            "label": "存储生命周期链路",
            "desc": "日志过滤→日志存储→日志轮转→保留清理",
            "stages": ["log_filter", "log_store", "log_rotate", "retention_clean"],
        },
        {
            "name": "exception",
            "label": "异常处理链路",
            "desc": "链路追踪→格式探测→错误分类→异常告警",
            "stages": ["trace_link", "format_detect", "error_classify", "anomaly_alert"],
        },
        {
            "name": "flow_control",
            "label": "流控监控链路",
            "desc": "摄入缓冲→采样聚合→采集健康",
            "stages": ["ingest_buffer", "sample_aggregate", "collect_health"],
        },
        {
            "name": "data_consumption",
            "label": "数据消费链路",
            "desc": "日志导出→日志回放",
            "stages": ["log_export", "log_replay"],
        },
    ]