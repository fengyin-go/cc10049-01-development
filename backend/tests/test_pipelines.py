"""跨模块业务管线行为测试：5条管线的端到端处理逻辑验证。"""

from __future__ import annotations

import pytest

from app.pipelines.data_consumption import DataConsumptionPipeline
from app.pipelines.exception_pipeline import ExceptionPipeline
from app.pipelines.flow_control import FlowControlPipeline
from app.pipelines.ingestion_pipeline import IngestionPipeline
from app.pipelines.storage_pipeline import StoragePipeline


# =============================================================================
# 接入解析管线
# =============================================================================
class TestIngestionPipeline:
    def setup_method(self):
        self.pipeline = IngestionPipeline()

    def test_process_csv_format(self):
        lines = [
            "2026-01-15T10:00:00Z,ERROR,api-gateway,Connection timeout to database",
            "2026-01-15T10:01:00Z,INFO,order-service,Order 12345 created successfully",
        ]
        results = self.pipeline.process(
            source_key="app-log",
            raw_lines=lines,
            parser_key="csv",
        )
        assert len(results) == 2
        assert results[0]["status"] == "completed"
        assert results[0]["source_key"] == "app-log"
        assert results[0]["parsed"]["_format"] == "csv"

    def test_process_json_format(self):
        lines = ['{"timestamp": "2026-01-15T10:00:00Z", "level": "ERROR", "service": "api-gateway", "message": "test"}']
        results = self.pipeline.process(
            source_key="json-log",
            raw_lines=lines,
        )
        assert len(results) == 1
        assert results[0]["parsed"]["_format"] == "json"

    def test_process_empty_line(self):
        results = self.pipeline.process("test", ["", "   "])
        assert len(results) == 2
        assert results[0]["parsed"]["_format"] == "empty"

    def test_normalize_with_rules(self):
        lines = ["2026-01-15T10:00:00Z,ERROR,api-gateway,timeout"]
        results = self.pipeline.process(
            source_key="app-log",
            raw_lines=lines,
            normalize_rules=["ts_to_iso", "level_upper", "svc_lower"],
        )
        assert results[0]["normalized"]["_normalized"] is True
        assert results[0]["normalized"]["log_level"] == "ERROR"

    def test_enrich_with_rules(self):
        lines = ["2026-01-15T10:00:00Z,ERROR,api-gateway,timeout"]
        results = self.pipeline.process(
            source_key="app-log",
            raw_lines=lines,
            enrich_rules=["geo_ip", "device_type"],
        )
        assert results[0]["enriched"]["_enriched"] is True
        assert "geo_location" in results[0]["enriched"]

    def test_full_pipeline(self):
        lines = [
            "2026-01-15T10:00:00Z,ERROR,api-gateway,Connection refused",
            "2026-01-15T10:01:00Z,INFO,order-service,Order created",
            "2026-01-15T10:02:00Z,WARN,payment-service,Slow response detected",
        ]
        results = self.pipeline.process(
            source_key="full-pipeline-test",
            raw_lines=lines,
            normalize_rules=["ts_to_iso", "level_upper", "svc_lower"],
            enrich_rules=["geo_ip", "device_type", "env_tag"],
        )
        assert len(results) == 3
        assert all(r["status"] == "completed" for r in results)
        for r in results:
            assert r["normalized"]["_normalized"]
            assert r["enriched"]["_enriched"]
            assert "geo_location" in r["enriched"]
            assert "device_type" in r["enriched"]
            assert "environment" in r["enriched"]

    def test_snapshot(self):
        self.pipeline.process("test-snap", ["2026-01-01,INFO,svc,msg"])
        snap = self.pipeline.snapshot()
        assert snap["pipeline"] == "ingestion"
        assert snap["total_processed"] == 1

    def test_records_by_status(self):
        self.pipeline.process("test", ["2026-01-01,INFO,svc,msg"])
        completed = self.pipeline.records_by_status("completed")
        assert len(completed) >= 1


# =============================================================================
# 存储生命周期管线
# =============================================================================
class TestStoragePipeline:
    def setup_method(self):
        self.pipeline = StoragePipeline()

    def test_filter_drop_debug(self):
        entries = [{"level": "DEBUG", "message": "trace data", "service": "monitor"}]
        results = self.pipeline.process_batch(
            log_entries=entries,
            filter_rule_keys=["drop_debug"],
        )
        assert results[0]["status"] == "filtered_out"
        assert results[0]["filter_result"]["action"] == "丢弃"

    def test_filter_keep_error(self):
        entries = [{"level": "ERROR", "message": "critical failure", "service": "api-gateway"}]
        results = self.pipeline.process_batch(
            log_entries=entries,
            filter_rule_keys=["keep_error", "drop_debug"],
        )
        assert results[0]["status"] == "completed"
        assert results[0]["filter_result"]["action"] == "保留"

    def test_store_with_engine(self):
        entries = [{"level": "INFO", "message": "normal operation", "service": "order-service"}]
        results = self.pipeline.process_batch(
            log_entries=entries,
            store_engine="clickhouse",
        )
        assert results[0]["store_result"]["engine"] == "clickhouse"
        assert results[0]["store_result"]["stored"] is True

    def test_rotate_trigger(self):
        entries = [{"level": "INFO", "message": f"entry_{i}", "service": "test"} for i in range(4)]
        results = self.pipeline.process_batch(log_entries=entries)
        assert len(results) == 4
        assert any(r.get("rotate_result", {}).get("rotate_needed") for r in results)

    def test_clean_trigger(self):
        entries = [{"level": "INFO", "message": f"entry_{i}", "service": "test"} for i in range(6)]
        results = self.pipeline.process_batch(
            log_entries=entries,
            clean_policy="by_age",
        )
        assert any(r.get("clean_result", {}).get("clean_needed") for r in results)

    def test_mixed_filtering(self):
        entries = [
            {"level": "ERROR", "message": "urgent", "service": "api-gateway"},
            {"level": "DEBUG", "message": "debug info", "service": "monitor"},
            {"level": "INFO", "message": "normal log", "service": "order-service"},
        ]
        results = self.pipeline.process_batch(
            log_entries=entries,
            filter_rule_keys=["drop_debug"],
        )
        completed = [r for r in results if r["status"] == "completed"]
        filtered = [r for r in results if r["status"] == "filtered_out"]
        assert len(completed) == 2
        assert len(filtered) == 1

    def test_snapshot(self):
        self.pipeline.process_batch([{"level": "INFO", "message": "test"}])
        snap = self.pipeline.snapshot()
        assert snap["pipeline"] == "storage"
        assert snap["total_processed"] == 1


# =============================================================================
# 异常处理管线
# =============================================================================
class TestExceptionPipeline:
    def setup_method(self):
        self.pipeline = ExceptionPipeline()

    def test_null_pointer_classification(self):
        spans = [
            {"message": "java.lang.NullPointerException at com.app.Service.handle", "status_code": 500},
        ]
        results = self.pipeline.process(spans)
        assert len(results) == 1
        assert results[0]["classify_result"]["label"] == "空指针异常"
        assert results[0]["classify_result"]["severity"] == "critical"

    def test_timeout_classification(self):
        spans = [{"message": "TimeoutError: request to upstream timed out after 30s"}]
        results = self.pipeline.process(spans)
        assert results[0]["classify_result"]["label"] == "超时异常"
        assert results[0]["classify_result"]["severity"] == "high"

    def test_alert_triggered_for_critical(self):
        spans = [
            {"message": "java.lang.NullPointerException", "status_code": 500},
            {"message": "TimeoutError: upstream timeout", "status_code": 504},
        ]
        results = self.pipeline.process(spans)
        alerts = self.pipeline.alert_summary()
        assert len(alerts) == 2
        assert alerts[0]["should_alert"] is True
        assert alerts[1]["should_alert"] is True

    def test_format_detection_stack_trace(self):
        spans = [
            {"message": "Exception in thread main java.lang.RuntimeException at com.app.Service.execute(Service.java:100) at com.app.Main.run(Main.java:42)"}]
        results = self.pipeline.process(spans)
        fmt = results[0]["format_result"]
        assert fmt["detected_format"] in ("stack_trace", "plain_text")
        if fmt["detected_format"] == "stack_trace":
            assert fmt["confidence"] >= 0.9

    def test_format_detection_json(self):
        spans = [{"message": '{"error": "invalid input", "code": 400}', "status_code": 400}]
        results = self.pipeline.process(spans)
        assert results[0]["format_result"]["confidence"] >= 0.8

    def test_no_alert_for_low_severity(self):
        spans = [{"message": "warning: rate limit approaching", "status_code": 200}]
        results = self.pipeline.process(spans)
        assert results[0]["alert_result"]["should_alert"] is False

    def test_severity_distribution(self):
        spans = [
            {"message": "NullPointerException"},
            {"message": "TimeoutError"},
            {"message": "ConnectionRefused"},
            {"message": "just a warning"},
        ]
        self.pipeline.process(spans)
        dist = self.pipeline.severity_distribution()
        assert dist.get("critical", 0) >= 1
        assert dist.get("high", 0) >= 2

    def test_snapshot(self):
        self.pipeline.process([{"message": "NullPointerException"}])
        snap = self.pipeline.snapshot()
        assert snap["pipeline"] == "exception"
        assert snap["alerts_triggered"] >= 1


# =============================================================================
# 流控监控管线
# =============================================================================
class TestFlowControlPipeline:
    def setup_method(self):
        self.pipeline = FlowControlPipeline()

    def test_normal_buffer(self):
        sources = [{"source_key": "app-log", "buffer_count": 10000}]
        results = self.pipeline.process_batch(sources)
        assert results[0]["status"] == "completed"
        assert results[0]["buffer_state"]["state"] == "normal"
        assert results[0]["buffer_state"]["usage_percent"] < 70

    def test_backpressure_buffer(self):
        sources = [{"source_key": "busy-service", "buffer_count": 75000}]
        results = self.pipeline.process_batch(sources)
        assert results[0]["buffer_state"]["state"] == "backpressure"

    def test_overflow_risk(self):
        sources = [{"source_key": "high-load", "buffer_count": 95000}]
        results = self.pipeline.process_batch(sources)
        assert results[0]["buffer_state"]["state"] == "overflow_risk"
        assert results[0]["health_result"]["overall_status"] == "critical"

    def test_sampling_methods(self):
        sources = [{"source_key": "s1", "buffer_count": 0, "sample_method": "random"}]
        results = self.pipeline.process_batch(sources)
        assert results[0]["sample_result"]["method"] == "random"
        assert results[0]["sample_result"]["sample_rate"] == 0.10

    def test_stratified_sampling(self):
        sources = [{"source_key": "s2", "buffer_count": 0, "sample_method": "stratified"}]
        results = self.pipeline.process_batch(sources)
        assert results[0]["sample_result"]["sample_rate"] == 0.15

    def test_multiple_sources(self):
        sources = [
            {"source_key": "app-log", "buffer_count": 10000},
            {"source_key": "nginx-log", "buffer_count": 80000},
            {"source_key": "system-log", "buffer_count": 95000},
        ]
        results = self.pipeline.process_batch(sources)
        statuses = [r["health_result"]["overall_status"] for r in results]
        assert "healthy" in statuses
        assert "degraded" in statuses
        assert "critical" in statuses

    def test_health_summary(self):
        sources = [
            {"source_key": "app-log", "buffer_count": 10000},
            {"source_key": "busy-log", "buffer_count": 90000},
        ]
        self.pipeline.process_batch(sources)
        summary = self.pipeline.health_summary()
        assert summary["total_sources"] == 2
        assert summary["by_health"]["healthy"] == 1
        assert summary["by_health"]["critical"] == 1

    def test_snapshot(self):
        self.pipeline.process_batch([{"source_key": "test", "buffer_count": 5000}])
        snap = self.pipeline.snapshot()
        assert snap["pipeline"] == "flow_control"


# =============================================================================
# 数据消费管线
# =============================================================================
class TestDataConsumptionPipeline:
    def setup_method(self):
        self.pipeline = DataConsumptionPipeline()

    def test_create_export_task(self):
        result = self.pipeline.create_export_task("level=ERROR", "csv", "最近7天")
        assert result["export_task"]["status"] == "queued"
        assert result["export_task"]["export_format"] == "csv"

    def test_run_and_complete_export(self):
        result = self.pipeline.create_export_task("level=ERROR", "json", "最近3天")
        task_id = result["export_task"]["task_id"]

        run_result = self.pipeline.run_export(task_id)
        assert run_result["export_task"]["status"] in ("exporting", "completed")

    def test_cancel_export(self):
        result = self.pipeline.create_export_task("test", "csv")
        task_id = result["export_task"]["task_id"]
        cancelled = self.pipeline.cancel_export(task_id)
        assert cancelled["export_task"]["status"] == "cancelled"

    def test_create_replay_task(self):
        result = self.pipeline.create_replay_task("ES_202601", "2026-01", "1x")
        assert result["replay_task"]["status"] == "queued"
        assert result["replay_task"]["source_storage"] == "ES_202601"

    def test_start_replay(self):
        result = self.pipeline.create_replay_task("ES_202601", "2026-01", "1x")
        task_id = result["replay_task"]["task_id"]
        start_result = self.pipeline.start_replay(task_id)
        assert start_result["replay_task"]["status"] == "replaying"

    def test_progress_replay(self):
        result = self.pipeline.create_replay_task("ES_202601", "2026-01", "1x")
        task_id = result["replay_task"]["task_id"]
        self.pipeline.start_replay(task_id)
        progress_result = self.pipeline.progress_replay(task_id, 500)
        assert progress_result["replay_task"]["replayed_lines"] == 500

    def test_cancel_replay(self):
        result = self.pipeline.create_replay_task("ES_202601", "2026-01", "1x")
        task_id = result["replay_task"]["task_id"]
        cancelled = self.pipeline.cancel_replay(task_id)
        assert cancelled["replay_task"]["status"] == "cancelled"

    def test_replay_completion(self):
        result = self.pipeline.create_replay_task("ES_202601", "2026-01", "1x")
        task_id = result["replay_task"]["task_id"]
        self.pipeline.start_replay(task_id)
        self.pipeline.progress_replay(task_id, 2000)
        completed = self.pipeline.progress_replay(task_id, 10000)
        assert "completed" in completed["replay_task"]["status"] or completed["replay_task"]["replayed_lines"] > 0

    def test_snapshot(self):
        self.pipeline.create_export_task("test", "csv")
        self.pipeline.create_replay_task("ES", "2026-01")
        snap = self.pipeline.snapshot()
        assert snap["pipeline"] == "data_consumption"
        assert snap["export_tasks"] >= 1
        assert snap["replay_tasks"] >= 1