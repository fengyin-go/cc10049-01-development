"""业务编排器：串联5条管线执行完整的数据工程工作流。"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.pipelines.data_consumption import DataConsumptionPipeline
from app.pipelines.exception_pipeline import ExceptionPipeline
from app.pipelines.flow_control import FlowControlPipeline
from app.pipelines.ingestion_pipeline import IngestionPipeline
from app.pipelines.storage_pipeline import StoragePipeline


@dataclass
class WorkflowRun:
    """一次完整工作流运行的上下文与结果。"""

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at: str = ""
    finished_at: str = ""
    status: str = "pending"

    ingestion: dict[str, Any] = field(default_factory=dict)
    storage: dict[str, Any] = field(default_factory=dict)
    exception: dict[str, Any] = field(default_factory=dict)
    flow_control: dict[str, Any] = field(default_factory=dict)
    consumption: dict[str, Any] = field(default_factory=dict)

    errors: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "ingestion": self.ingestion,
            "storage": self.storage,
            "exception": self.exception,
            "flow_control": self.flow_control,
            "consumption": self.consumption,
            "errors": self.errors,
            "summary": self.summary,
        }


class WorkflowOrchestrator:
    """工作流编排器：串联接入→存储→异常→流控→消费5条管线。"""

    SAMPLE_LOG_LINES = {
        "app_log": [
            "2026-11-22T08:00:01Z,INFO,api-gateway,Request received from 10.0.1.5,GET /api/orders",
            "2026-11-22T08:00:02Z,ERROR,api-gateway,java.lang.NullPointerException at com.app.Handler.dispatch",
            "2026-11-22T08:00:03Z,INFO,order-service,Order ORD-88421 created successfully",
            "2026-11-22T08:00:04Z,WARN,payment-service,Payment timeout for order ORD-88421 after 30s",
            "2026-11-22T08:00:05Z,ERROR,payment-service,ConnectionRefused: cannot reach payment gateway",
            "2026-11-22T08:00:06Z,INFO,notification-service,SMS sent to user U10042 for order ORD-88421",
            "2026-11-22T08:00:07Z,DEBUG,api-gateway,Heartbeat check from load balancer",
            "2026-11-22T08:00:08Z,ERROR,database-service,OutOfMemoryError: Java heap space exhausted",
            "2026-11-22T08:00:09Z,INFO,auth-service,User U10042 authenticated via OAuth2",
            "2026-11-22T08:00:10Z,WARN,auth-service,RateLimitExceeded: user U10042 exceeded API quota",
        ],
        "nginx_log": [
            "2026-11-22T08:00:01Z,INFO,nginx,200 GET /api/health 0.002s 192.168.1.100",
            "2026-11-22T08:00:02Z,INFO,nginx,200 GET /api/orders/list 0.045s 192.168.1.101",
            "2026-11-22T08:00:03Z,ERROR,nginx,502 GET /api/payment/process 0.120s 192.168.1.102",
            "2026-11-22T08:00:04Z,INFO,nginx,200 POST /api/orders/create 0.150s 192.168.1.103",
        ],
        "system_log": [
            "2026-11-22T08:00:01Z,WARN,system,CPU usage exceeded 85% on host web-node-01",
            "2026-11-22T08:00:02Z,ERROR,system,Disk I/O latency exceeded 200ms on /dev/sda1",
            "2026-11-22T08:00:03Z,INFO,system,Kernel update 5.15.0-89 applied, reboot required",
        ],
    }

    ERROR_SPANS = [
        {"trace_id": "trace-001", "span_id": "span-001-a", "parent_span_id": "",
         "service": "api-gateway", "message": "NullPointerException at dispatch",
         "duration_ms": 150, "status_code": 500},
        {"trace_id": "trace-001", "span_id": "span-001-b", "parent_span_id": "span-001-a",
         "service": "order-service", "message": "TimeoutError: downstream timeout after 30s",
         "duration_ms": 32000, "status_code": 504},
        {"trace_id": "trace-002", "span_id": "span-002-a", "parent_span_id": "",
         "service": "payment-service", "message": "ConnectionRefused: payment gateway unreachable",
         "duration_ms": 5000, "status_code": 502},
        {"trace_id": "trace-003", "span_id": "span-003-a", "parent_span_id": "",
         "service": "database-service", "message": "OutOfMemoryError",
         "duration_ms": 100, "status_code": 500},
        {"trace_id": "trace-004", "span_id": "span-004-a", "parent_span_id": "",
         "service": "auth-service", "message": "AuthenticationFailed: invalid token",
         "duration_ms": 50, "status_code": 401},
    ]

    FLOW_SOURCES = [
        {"source_key": "app-log", "buffer_count": 12000, "sample_method": "stratified"},
        {"source_key": "nginx-log", "buffer_count": 80000, "sample_method": "random"},
        {"source_key": "system-log", "buffer_count": 95000, "sample_method": "reservoir"},
    ]

    def __init__(self):
        self._ingestion = IngestionPipeline()
        self._storage = StoragePipeline()
        self._exception = ExceptionPipeline()
        self._flow_control = FlowControlPipeline()
        self._consumption = DataConsumptionPipeline()
        self._history: list[WorkflowRun] = []

    def run_full_workflow(self, source_keys: list[str] | None = None) -> dict[str, Any]:
        """执行完整数据工程工作流。"""
        run = WorkflowRun()
        run.started_at = datetime.now(timezone.utc).isoformat()
        sources = source_keys or list(self.SAMPLE_LOG_LINES.keys())

        try:
            # 阶段1：接入解析 - 采集并解析所有来源的日志
            run.ingestion = self._run_ingestion(sources)
            all_entries = self._collect_parsed_entries(run.ingestion)

            # 阶段2：存储生命周期 - 过滤存储轮转清理
            run.storage = self._run_storage(all_entries)

            # 阶段3：异常处理 - 链路追踪格式探测错误分类告警
            run.exception = self._run_exception()

            # 阶段4：流控监控 - 缓冲采样健康检查
            run.flow_control = self._run_flow_control()

            # 阶段5：数据消费 - 导出和回放
            run.consumption = self._run_consumption(sources)

            run.status = "completed"
            run.summary = self._build_summary(run)
        except Exception as exc:
            run.errors.append(f"工作流执行异常: {exc}")
            run.status = "failed"

        run.finished_at = datetime.now(timezone.utc).isoformat()
        self._history.append(run)
        return run.to_dict()

    def _run_ingestion(self, source_keys: list[str]) -> dict[str, Any]:
        results_by_source: dict[str, list[dict[str, Any]]] = {}
        for key in source_keys:
            lines = self.SAMPLE_LOG_LINES.get(key, [])
            results = self._ingestion.process(
                source_key=key,
                raw_lines=lines,
                normalize_rules=["ts_to_iso", "level_upper", "svc_lower"],
                enrich_rules=["geo_ip", "device_type", "env_tag"],
            )
            results_by_source[key] = results
        snapshot = self._ingestion.snapshot()
        return {"results_by_source": {k: len(v) for k, v in results_by_source.items()},
                "snapshot": snapshot, "_full_results": results_by_source}

    def _collect_parsed_entries(self, ingestion_result: dict[str, Any]) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        full = ingestion_result.get("_full_results", {})
        for source_key, results in full.items():
            for r in results:
                if r["status"] == "completed":
                    enriched = r.get("enriched", {})
                    entries.append({
                        "source": source_key,
                        "level": enriched.get("log_level",
                                              enriched.get("level", "INFO")),
                        "service": enriched.get("service_name",
                                                enriched.get("service", "unknown")),
                        "message": enriched.get("message", ""),
                        "geo_location": enriched.get("geo_location", ""),
                        "environment": enriched.get("environment", ""),
                    })
        return entries

    def _run_storage(self, entries: list[dict[str, Any]]) -> dict[str, Any]:
        if not entries:
            return {"snapshot": self._storage.snapshot(), "note": "无日志条目需要存储"}
        results = self._storage.process_batch(
            log_entries=entries,
            filter_rule_keys=["drop_debug", "keep_error"],
            store_engine="elasticsearch",
            rotate_policy="time_based",
            clean_policy="by_age",
        )
        filtered = [r for r in results if r["status"] == "filtered_out"]
        completed = [r for r in results if r["status"] == "completed"]
        return {"total": len(results), "stored": len(completed),
                "filtered_out": len(filtered), "snapshot": self._storage.snapshot()}

    def _run_exception(self) -> dict[str, Any]:
        results = self._exception.process(self.ERROR_SPANS)
        alerts = self._exception.alert_summary()
        return {"spans_processed": len(results),
                "alerts_generated": len(alerts),
                "severity_dist": self._exception.severity_distribution(),
                "snapshot": self._exception.snapshot()}

    def _run_flow_control(self) -> dict[str, Any]:
        results = self._flow_control.process_batch(self.FLOW_SOURCES)
        health = self._flow_control.health_summary()
        return {"sources_checked": len(results),
                "health": health, "snapshot": self._flow_control.snapshot()}

    def _run_consumption(self, source_keys: list[str]) -> dict[str, Any]:
        source_key = source_keys[0] if source_keys else "app-log"
        export = self._consumption.create_export_task(
            query_condition=f"source={source_key} AND level=ERROR",
            export_format="csv",
            time_range="最近7天",
        )
        export_id = export["export_task"]["task_id"]
        self._consumption.run_export(export_id)
        self._consumption.complete_export(export_id)

        replay = self._consumption.create_replay_task(
            source_storage=f"ES_{source_key}",
            time_range="2026-10",
            replay_rate="1x",
            dedup_strategy="by_key",
        )
        replay_id = replay["replay_task"]["task_id"]
        self._consumption.start_replay(replay_id)
        self._consumption.progress_replay(replay_id, 5000)

        return {"export_task_id": export_id,
                "replay_task_id": replay_id,
                "snapshot": self._consumption.snapshot()}

    def _build_summary(self, run: WorkflowRun) -> dict[str, Any]:
        ingest_total = run.ingestion.get("snapshot", {}).get("total_processed", 0)
        storage_total = run.storage.get("total", 0)
        exception_total = run.exception.get("spans_processed", 0)
        flow_total = run.flow_control.get("sources_checked", 0)
        alerts = run.exception.get("alerts_generated", 0)
        health = run.flow_control.get("health", {}).get("by_health", {})

        return {
            "total_logs_ingested": ingest_total,
            "total_logs_stored": storage_total,
            "total_spans_analyzed": exception_total,
            "total_sources_monitored": flow_total,
            "alerts_triggered": alerts,
            "healthy_sources": health.get("healthy", 0),
            "degraded_sources": health.get("degraded", 0),
            "critical_sources": health.get("critical", 0),
            "export_replay_tasks": 2,
            "overall_health": "healthy" if health.get("critical", 0) == 0 else "needs_attention",
        }

    def run_history(self) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self._history]

    def last_snapshot(self) -> dict[str, Any] | None:
        if not self._history:
            return None
        return self._history[-1].summary


orchestrator = WorkflowOrchestrator()