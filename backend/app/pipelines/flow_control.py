"""流控监控管线：摄入缓冲→采样聚合→采集健康 端到端处理。"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class FlowControlContext:
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    source_key: str = ""
    buffer_state: dict[str, Any] = field(default_factory=dict)
    sample_result: dict[str, Any] = field(default_factory=dict)
    health_result: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    status: str = "pending"

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "source_key": self.source_key,
            "buffer_state": self.buffer_state,
            "sample_result": self.sample_result,
            "health_result": self.health_result,
            "errors": self.errors,
            "status": self.status,
        }


class FlowControlPipeline:
    """流控监控管线：缓冲→采样→健康的完整流控链路。"""

    BUFFER_DEFAULTS = {
        "capacity": 100000,
        "drop_strategy": "drop_oldest",
        "batch_size": 500,
    }

    SAMPLING_METHODS = {
        "random": {"type": "概率采样", "description": "按比例随机选取"},
        "reservoir": {"type": "水库采样", "description": "保证每个元素被选中概率一致"},
        "stratified": {"type": "分层采样", "description": "按日志级别分层抽样"},
        "systematic": {"type": "等距采样", "description": "按固定间隔选取"},
    }

    HEALTH_CHECKS = {
        "throughput": {"check": "吞吐速率", "unit": "lines/sec", "normal_range": (100, 100000)},
        "latency": {"check": "摄入延迟", "unit": "ms", "normal_range": (0, 500)},
        "backlog": {"check": "积压检查", "unit": "lines", "normal_range": (0, 50000)},
        "error_rate": {"check": "错误率", "unit": "%", "normal_range": (0, 5)},
    }

    def __init__(self):
        self._records: list[FlowControlContext] = []

    def process_source(
        self,
        source_key: str,
        current_buffer_count: int = 0,
        sample_method: str = "random",
    ) -> dict[str, Any]:
        ctx = FlowControlContext(source_key=source_key)
        try:
            ctx.buffer_state = self._check_buffer(source_key, current_buffer_count)
            ctx.sample_result = self._perform_sampling(source_key, sample_method)
            ctx.health_result = self._check_health(ctx.buffer_state, ctx.sample_result)
            ctx.status = "completed"
        except Exception as exc:
            ctx.errors.append(str(exc))
            ctx.status = "failed"
        self._records.append(ctx)
        return ctx.to_dict()

    def process_batch(
        self,
        sources: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for src in sources:
            source_key = src.get("source_key", "")
            buffer_count = src.get("buffer_count", 0)
            sample_method = src.get("sample_method", "random")
            results.append(self.process_source(source_key, buffer_count, sample_method))
        return results

    def _check_buffer(self, source_key: str, current_count: int) -> dict[str, Any]:
        capacity = self.BUFFER_DEFAULTS["capacity"]
        usage_pct = min(round(current_count / capacity * 100, 1), 100) if capacity > 0 else 0
        state = "normal"
        drop_policy = self.BUFFER_DEFAULTS["drop_strategy"]
        if usage_pct >= 90:
            state = "overflow_risk"
            drop_policy = "drop_newest"
        elif usage_pct >= 70:
            state = "backpressure"
        return {
            "queue_id": f"buf-{source_key}",
            "capacity": capacity,
            "current_backlog": current_count,
            "usage_percent": usage_pct,
            "state": state,
            "drop_strategy": drop_policy,
            "consumer_rate": 500,
            "producer_rate": current_count // 10 if current_count > 0 else 100,
        }

    def _perform_sampling(self, source_key: str, method: str) -> dict[str, Any]:
        method_info = self.SAMPLING_METHODS.get(method, self.SAMPLING_METHODS["random"])
        sample_rates = {"random": 0.10, "reservoir": 0.05, "stratified": 0.15, "systematic": 0.08}
        rate = sample_rates.get(method, 0.10)
        return {
            "method": method,
            "method_type": method_info["type"],
            "sample_rate": rate,
            "aggregation_window": "60s",
            "aggregation_fields": ["level", "service", "error_type"],
            "reserve_cardinality": 10000,
            "status": "enabled",
        }

    def _check_health(self, buffer_state: dict, sample_result: dict) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        issues: list[dict[str, Any]] = []
        overall = "healthy"

        usage = buffer_state.get("usage_percent", 0)
        if usage >= 90:
            issues.append({"type": "backlog_critical", "detail": f"缓冲使用率{usage}%，已超90%"})
            overall = "critical"
        elif usage >= 70:
            issues.append({"type": "backlog_warning", "detail": f"缓冲使用率{usage}%，已超70%"})
            overall = "degraded"

        actual_rate = buffer_state.get("consumer_rate", 0)
        expected_rate = buffer_state.get("producer_rate", 100)
        if expected_rate > 0 and actual_rate < expected_rate * 0.5:
            issues.append({"type": "throughput_low", "detail": f"消费速率{actual_rate} < 生产速率{expected_rate}的50%"})
            if overall == "healthy":
                overall = "degraded"

        return {
            "check_id": f"hc-{buffer_state.get('queue_id', 'unknown')}",
            "overall_status": overall,
            "check_time": now.isoformat(),
            "issues": issues,
            "metrics": {
                "buffer_usage_pct": usage,
                "sample_rate": sample_result.get("sample_rate", 0),
                "consumer_rate": buffer_state.get("consumer_rate", 0),
                "producer_rate": buffer_state.get("producer_rate", 0),
            },
        }

    def health_summary(self) -> dict[str, Any]:
        records = self._records
        by_health = {"healthy": 0, "degraded": 0, "critical": 0}
        for r in records:
            if r.status != "completed":
                continue
            overall = r.health_result.get("overall_status", "unknown")
            by_health[overall] = by_health.get(overall, 0) + 1
        total_issues = sum(len(r.health_result.get("issues", [])) for r in records)
        return {
            "total_sources": len(records),
            "by_health": by_health,
            "total_issues": total_issues,
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "pipeline": "flow_control",
            **self.health_summary(),
        }