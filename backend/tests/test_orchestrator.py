"""工作流编排器行为测试：端到端工作流执行验证。"""

from __future__ import annotations

import pytest

from app.orchestrator import WorkflowOrchestrator, orchestrator


class TestWorkflowOrchestrator:
    def setup_method(self):
        self.orchestrator = WorkflowOrchestrator()

    def test_run_full_workflow(self):
        result = self.orchestrator.run_full_workflow()
        assert result["status"] == "completed"
        assert "summary" in result
        summary = result["summary"]
        assert summary["total_logs_ingested"] >= 10
        assert summary["total_logs_stored"] >= 5
        assert summary["total_spans_analyzed"] == 5
        assert summary["total_sources_monitored"] == 3

    def test_workflow_has_all_stages(self):
        result = self.orchestrator.run_full_workflow()
        assert "ingestion" in result
        assert "storage" in result
        assert "exception" in result
        assert "flow_control" in result
        assert "consumption" in result

    def test_ingestion_stage(self):
        result = self.orchestrator.run_full_workflow()
        ingestion = result["ingestion"]
        snapshot = ingestion["snapshot"]
        assert snapshot["total_processed"] >= 10
        assert snapshot["completed"] >= 10

    def test_storage_stage(self):
        result = self.orchestrator.run_full_workflow()
        storage = result["storage"]
        assert storage["total"] >= 5
        assert storage["stored"] >= 1

    def test_exception_stage(self):
        result = self.orchestrator.run_full_workflow()
        exception = result["exception"]
        assert exception["spans_processed"] == 5
        assert exception["alerts_generated"] >= 1

    def test_flow_control_stage(self):
        result = self.orchestrator.run_full_workflow()
        flow = result["flow_control"]
        assert flow["sources_checked"] == 3
        assert "health" in flow

    def test_consumption_stage(self):
        result = self.orchestrator.run_full_workflow()
        consumption = result["consumption"]
        assert "export_task_id" in consumption
        assert "replay_task_id" in consumption

    def test_overall_health_assessment(self):
        result = self.orchestrator.run_full_workflow()
        summary = result["summary"]
        assert "overall_health" in summary
        assert summary["overall_health"] in ("healthy", "needs_attention")

    def test_run_history(self):
        self.orchestrator.run_full_workflow(["app_log"])
        self.orchestrator.run_full_workflow(["nginx_log"])
        history = self.orchestrator.run_history()
        assert len(history) == 2

    def test_last_snapshot(self):
        self.orchestrator.run_full_workflow(["app_log"])
        snapshot = self.orchestrator.last_snapshot()
        assert snapshot is not None
        assert "total_logs_ingested" in snapshot

    def test_specific_sources(self):
        result = self.orchestrator.run_full_workflow(["system_log"])
        ingestion = result["ingestion"]
        total = ingestion["snapshot"]["total_processed"]
        assert total >= 3

    def test_multiple_runs_consistent(self):
        result1 = self.orchestrator.run_full_workflow()
        result2 = self.orchestrator.run_full_workflow()
        assert result1["status"] == "completed"
        assert result2["status"] == "completed"
        assert result1["run_id"] != result2["run_id"]


class TestGlobalOrchestrator:
    def test_global_instance(self):
        assert isinstance(orchestrator, WorkflowOrchestrator)

    def test_run_and_history(self):
        result = orchestrator.run_full_workflow()
        assert result["status"] == "completed"
        history = orchestrator.run_history()
        assert len(history) >= 1