"""接入解析管线：日志源→日志解析→字段标准化→日志增强 端到端处理。"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class IngestionContext:
    """接入解析上下文：追踪每条日志从摄入到增强的完整状态。"""

    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    source_key: str = ""
    raw_line: str = ""
    parsed: dict[str, Any] = field(default_factory=dict)
    normalized: dict[str, Any] = field(default_factory=dict)
    enriched: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""
    status: str = "pending"

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "source_key": self.source_key,
            "raw_line": self.raw_line,
            "parsed": self.parsed,
            "normalized": self.normalized,
            "enriched": self.enriched,
            "errors": self.errors,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
        }


class IngestionPipeline:
    """接入解析管线：模拟从日志源摄入到增强的完整业务处理。"""

    def __init__(self):
        self._records: list[IngestionContext] = []

    def process(
        self,
        source_key: str,
        raw_lines: list[str],
        parser_key: str = "",
        normalize_rules: list[str] | None = None,
        enrich_rules: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """对一批原始日志执行接入解析管线全流程。"""
        results: list[dict[str, Any]] = []
        for line in raw_lines:
            ctx = IngestionContext()
            ctx.source_key = source_key
            ctx.raw_line = line
            ctx.started_at = datetime.now(timezone.utc).isoformat()
            try:
                ctx.parsed = self._parse_line(line, parser_key)
                ctx.normalized = self._normalize_fields(ctx.parsed, normalize_rules or [])
                ctx.enriched = self._enrich_log(ctx.normalized, enrich_rules or [])
                ctx.status = "completed"
            except Exception as exc:
                ctx.errors.append(str(exc))
                ctx.status = "failed"
            ctx.finished_at = datetime.now(timezone.utc).isoformat()
            self._records.append(ctx)
            results.append(ctx.to_dict())
        return results

    def _parse_line(self, line: str, _parser_key: str) -> dict[str, Any]:
        """仿真日志解析：支持CSV、JSON和空格分隔三种格式。"""
        line = line.strip()
        if not line:
            return {"_raw": line, "_format": "empty"}
        if line.startswith("{"):
            import json
            try:
                data = json.loads(line)
                data["_format"] = "json"
                return data
            except json.JSONDecodeError:
                pass
        if "," in line:
            parts = [p.strip() for p in line.split(",")]
            fields = {
                "timestamp": parts[0] if len(parts) > 0 else "",
                "level": parts[1] if len(parts) > 1 else "INFO",
                "service": parts[2] if len(parts) > 2 else "unknown",
                "message": parts[3] if len(parts) > 3 else line,
                "_format": "csv",
            }
            return fields
        parts = line.split()
        fields = {
            "timestamp": parts[0] if len(parts) > 0 else "",
            "level": parts[1] if len(parts) > 1 else "INFO",
            "service": parts[2] if len(parts) > 2 else "unknown",
            "message": " ".join(parts[3:]) if len(parts) > 3 else line,
            "_format": "space_delimited",
        }
        return fields

    def _normalize_fields(self, parsed: dict[str, Any], rule_keys: list[str]) -> dict[str, Any]:
        """字段标准化：统一字段名、类型转换、默认值填充。"""
        normalized = dict(parsed)
        for rule_key in rule_keys:
            rule = self._get_normalize_rule(rule_key)
            if not rule:
                continue
            source_field = rule.get("source_field", "")
            target_field = rule.get("target_field", source_field)
            type_cast = rule.get("type_cast", "string")
            default_val = rule.get("default_value", "")

            value = normalized.get(source_field, default_val)
            if type_cast == "int":
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    value = 0
            elif type_cast == "float":
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = 0.0
            elif type_cast == "boolean":
                value = str(value).lower() in ("true", "1", "yes")
            normalized[target_field] = value
        normalized["_normalized"] = True
        return normalized

    def _enrich_log(self, normalized: dict[str, Any], rule_keys: list[str]) -> dict[str, Any]:
        """日志增强：补充地理、设备、用户等维度信息。"""
        enriched = dict(normalized)
        for rule_key in rule_keys:
            rule = self._get_enrich_rule(rule_key)
            if not rule:
                continue
            enrich_field = rule.get("enrich_field", "")
            enrich_source = rule.get("enrich_source", "static")
            match_key = rule.get("match_key", "")

            if enrich_source == "static":
                enriched[enrich_field] = rule.get("static_value", "unknown")
            elif enrich_source == "lookup" and match_key:
                match_value = enriched.get(match_key, "")
                enriched[enrich_field] = self._lookup_enhancement(match_key, match_value, enrich_field)
        enriched["_enriched"] = True
        return enriched

    @staticmethod
    def _get_normalize_rule(rule_key: str) -> dict[str, Any] | None:
        rules = {
            "ts_to_iso": {"source_field": "timestamp", "target_field": "@timestamp", "type_cast": "string",
                          "default_value": "1970-01-01T00:00:00Z"},
            "level_upper": {"source_field": "level", "target_field": "log_level", "type_cast": "string",
                            "default_value": "INFO"},
            "svc_lower": {"source_field": "service", "target_field": "service_name", "type_cast": "string",
                          "default_value": "unknown"},
            "count_int": {"source_field": "count", "target_field": "event_count", "type_cast": "int",
                          "default_value": "0"},
            "latency_float": {"source_field": "latency", "target_field": "latency_ms", "type_cast": "float",
                              "default_value": "0.0"},
        }
        return rules.get(rule_key)

    @staticmethod
    def _get_enrich_rule(rule_key: str) -> dict[str, Any] | None:
        rules = {
            "geo_ip": {"enrich_field": "geo_location", "enrich_source": "static", "static_value": "中国-北京-海淀",
                        "match_key": "ip_address"},
            "device_type": {"enrich_field": "device_type", "enrich_source": "static", "static_value": "server",
                            "match_key": "hostname"},
            "user_dept": {"enrich_field": "department", "enrich_source": "static", "static_value": "数据工程组",
                          "match_key": "user_id"},
            "env_tag": {"enrich_field": "environment", "enrich_source": "static", "static_value": "production",
                        "match_key": "hostname"},
        }
        return rules.get(rule_key)

    @staticmethod
    def _lookup_enhancement(match_key: str, match_value: str, enrich_field: str) -> str:
        lookup = {
            ("service_name", "api-gateway", "geo_location"): "中国-上海-浦东",
            ("service_name", "order-service", "department"): "交易事业部",
            ("service_name", "payment-service", "department"): "支付平台部",
        }
        return lookup.get((match_key, match_value, enrich_field), "unknown")

    def record_count(self) -> int:
        return len(self._records)

    def records_by_status(self, status: str) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self._records if r.status == status]

    def snapshot(self) -> dict[str, Any]:
        records = self._records
        return {
            "pipeline": "ingestion",
            "total_processed": len(records),
            "completed": sum(1 for r in records if r.status == "completed"),
            "failed": sum(1 for r in records if r.status == "failed"),
            "avg_errors": sum(len(r.errors) for r in records) / max(len(records), 1),
            "sources": list({r.source_key for r in records}),
        }