"""异常处理管线：链路追踪→格式探测→错误分类→异常告警 端到端处理。"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class ExceptionContext:
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    trace_span: dict[str, Any] = field(default_factory=dict)
    format_result: dict[str, Any] = field(default_factory=dict)
    classify_result: dict[str, Any] = field(default_factory=dict)
    alert_result: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    status: str = "pending"

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "trace_span": self.trace_span,
            "format_result": self.format_result,
            "classify_result": self.classify_result,
            "alert_result": self.alert_result,
            "errors": self.errors,
            "status": self.status,
        }


class ExceptionPipeline:
    """异常处理管线：从链路追踪到告警的完整异常处理链路。"""

    ERROR_PATTERNS = {
        "NullPointerException": {"label": "空指针异常", "severity": "critical", "service": "api-gateway"},
        "TimeoutError": {"label": "超时异常", "severity": "high", "service": "order-service"},
        "ConnectionRefused": {"label": "连接拒绝", "severity": "high", "service": "database"},
        "OutOfMemoryError": {"label": "内存溢出", "severity": "critical", "service": "java-runtime"},
        "AuthenticationFailed": {"label": "认证失败", "severity": "medium", "service": "auth-service"},
        "RateLimitExceeded": {"label": "限流触发", "severity": "low", "service": "api-proxy"},
    }

    ALERT_TEMPLATES = {
        "critical": {"level": "P0", "channel": "电话+企微+邮件", "silence_min": 0},
        "high": {"level": "P1", "channel": "企微+邮件", "silence_min": 5},
        "medium": {"level": "P2", "channel": "企微", "silence_min": 15},
        "low": {"level": "P3", "channel": "邮件", "silence_min": 60},
    }

    def __init__(self):
        self._records: list[ExceptionContext] = []

    def process(
        self,
        spans: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for span in spans:
            ctx = ExceptionContext(trace_span=span)
            try:
                ctx.format_result = self._detect_format(span)
                ctx.classify_result = self._classify_error(span)
                ctx.alert_result = self._evaluate_alert(ctx.classify_result)
                ctx.status = "completed"
            except Exception as exc:
                ctx.errors.append(str(exc))
                ctx.status = "failed"
            self._records.append(ctx)
            results.append(ctx.to_dict())
        return results

    def _detect_format(self, span: dict[str, Any]) -> dict[str, Any]:
        error_msg = str(span.get("message", span.get("error", "")))
        sample = error_msg[:200]
        detected = {
            "sample": sample,
            "detected_format": "plain_text",
            "confidence": 0.5,
            "line_count": 1,
        }
        if "{" in sample:
            detected["detected_format"] = "json"
            detected["confidence"] = 0.85
        elif "|" in sample:
            detected["detected_format"] = "pipe_delimited"
            detected["confidence"] = 0.75
        elif "<" in sample and ">" in sample:
            detected["detected_format"] = "xml"
            detected["confidence"] = 0.70
        elif " at " in sample:
            detected["detected_format"] = "stack_trace"
            detected["confidence"] = 0.95
            lines = sample.split("\n")
            detected["line_count"] = len(lines)
        for pattern, info in self.ERROR_PATTERNS.items():
            if pattern.lower() in error_msg.lower():
                detected["suggested_template"] = pattern
                detected["confidence"] = 0.90
                break
        return detected

    def _classify_error(self, span: dict[str, Any]) -> dict[str, Any]:
        error_msg = str(span.get("message", span.get("error", "")))
        status_code = span.get("status_code", 200)
        result = {"error_message": error_msg[:100], "status_code": status_code}

        for pattern, info in self.ERROR_PATTERNS.items():
            if pattern.lower() in error_msg.lower():
                result.update(info)
                result["matched_pattern"] = pattern
                result["aggregation_key"] = f"{info['service']}:{pattern}"
                return result

        if status_code >= 500:
            result.update({"label": "服务端错误", "severity": "high", "service": "unknown"})
        elif status_code >= 400:
            result.update({"label": "客户端错误", "severity": "medium", "service": "unknown"})
        else:
            result.update({"label": "未知异常", "severity": "low", "service": "unknown"})
        return result

    def _evaluate_alert(self, classify_result: dict[str, Any]) -> dict[str, Any]:
        severity = classify_result.get("severity", "low")
        alert_config = self.ALERT_TEMPLATES.get(severity, self.ALERT_TEMPLATES["low"])
        now = datetime.now(timezone.utc)
        return {
            "should_alert": severity in ("critical", "high"),
            "alert_level": alert_config["level"],
            "alert_channels": alert_config["channel"].split("+"),
            "silence_window_minutes": alert_config["silence_min"],
            "triggered_at": now.isoformat(),
            "alert_message": f"[{alert_config['level']}] {classify_result.get('label', '未知')} - "
                             f"{classify_result.get('service', 'unknown')}: "
                             f"{classify_result.get('error_message', '')[:50]}",
        }

    def severity_distribution(self) -> dict[str, int]:
        dist: dict[str, int] = {}
        for r in self._records:
            sev = r.classify_result.get("severity", "unknown")
            dist[sev] = dist.get(sev, 0) + 1
        return dist

    def alert_summary(self) -> list[dict[str, Any]]:
        return [r.alert_result for r in self._records
                if r.alert_result.get("should_alert") and r.status == "completed"]

    def snapshot(self) -> dict[str, Any]:
        records = self._records
        return {
            "pipeline": "exception",
            "total_processed": len(records),
            "completed": sum(1 for r in records if r.status == "completed"),
            "failed": sum(1 for r in records if r.status == "failed"),
            "alerts_triggered": sum(1 for r in records
                                     if r.alert_result.get("should_alert")),
            "severity_dist": self.severity_distribution(),
        }