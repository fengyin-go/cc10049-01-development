"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.log_source import LogSourceService
from app.services.log_parser import LogParserService
from app.services.field_normalize import FieldNormalizeService
from app.services.log_enrich import LogEnrichService
from app.services.trace_link import TraceLinkService
from app.services.log_filter import LogFilterService
from app.services.log_store import LogStoreService
from app.services.sample_aggregate import SampleAggregateService
from app.services.error_classify import ErrorClassifyService
from app.services.collect_health import CollectHealthService
from app.services.log_rotate import LogRotateService
from app.services.trace_metric import TraceMetricService
from app.services.log_export import LogExportService
from app.services.retention_clean import RetentionCleanService
from app.services.log_replay import LogReplayService
from app.services.format_detect import FormatDetectService
from app.services.anomaly_alert import AnomalyAlertService
from app.services.ingest_buffer import IngestBufferService

SERVICES: dict[str, BaseModuleService] = {
    "log_source": LogSourceService(),
    "log_parser": LogParserService(),
    "field_normalize": FieldNormalizeService(),
    "log_enrich": LogEnrichService(),
    "trace_link": TraceLinkService(),
    "log_filter": LogFilterService(),
    "log_store": LogStoreService(),
    "sample_aggregate": SampleAggregateService(),
    "error_classify": ErrorClassifyService(),
    "collect_health": CollectHealthService(),
    "log_rotate": LogRotateService(),
    "trace_metric": TraceMetricService(),
    "log_export": LogExportService(),
    "retention_clean": RetentionCleanService(),
    "log_replay": LogReplayService(),
    "format_detect": FormatDetectService(),
    "anomaly_alert": AnomalyAlertService(),
    "ingest_buffer": IngestBufferService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
