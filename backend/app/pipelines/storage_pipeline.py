"""存储生命周期管线：日志过滤→日志存储→日志轮转→保留清理 端到端处理。"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class StorageContext:
    """存储生命周期上下文：追踪日志从过滤到清理的全流程。"""

    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    log_entry: dict[str, Any] = field(default_factory=dict)
    filter_result: dict[str, Any] = field(default_factory=dict)
    store_result: dict[str, Any] = field(default_factory=dict)
    rotate_result: dict[str, Any] = field(default_factory=dict)
    clean_result: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    status: str = "pending"

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "log_entry": self.log_entry,
            "filter_result": self.filter_result,
            "store_result": self.store_result,
            "rotate_result": self.rotate_result,
            "clean_result": self.clean_result,
            "errors": self.errors,
            "status": self.status,
        }


class StoragePipeline:
    """存储生命周期管线：从过滤到清理的完整存储链路。"""

    FILTER_RULES = {
        "drop_debug": {"field": "level", "op": "eq", "value": "DEBUG", "action": "丢弃"},
        "keep_error": {"field": "level", "op": "eq", "value": "ERROR", "action": "保留"},
        "drop_health": {"field": "service", "op": "eq", "value": "health-checker", "action": "丢弃"},
        "mark_sensitive": {"field": "message", "op": "contains", "value": "password", "action": "标记"},
    }

    STORE_ENGINES = {
        "elasticsearch": {"type": "全文索引", "default_partition": "daily", "default_retention": 30},
        "clickhouse": {"type": "列存储", "default_partition": "monthly", "default_retention": 90},
        "file_storage": {"type": "对象存储", "default_partition": "hourly", "default_retention": 7},
    }

    ROTATE_POLICIES = {
        "size_based": {"condition": "size>500MB", "action": "切割归档", "compress": "gzip"},
        "time_based": {"condition": "every_24h", "action": "切割归档", "compress": "lz4"},
        "hybrid": {"condition": "size>1GB or every_12h", "action": "切割归档", "compress": "snappy"},
    }

    CLEAN_POLICIES = {
        "by_age": {"condition": "age>30d", "action": "物理删除"},
        "by_size": {"condition": "total>10TB", "action": "归档到冷存储"},
        "by_status": {"condition": "status=archived", "action": "永久删除"},
    }

    def __init__(self):
        self._records: list[StorageContext] = []
        self._id_counter = 0

    def process_batch(
        self,
        log_entries: list[dict[str, Any]],
        filter_rule_keys: list[str] | None = None,
        store_engine: str = "elasticsearch",
        rotate_policy: str = "time_based",
        clean_policy: str = "by_age",
    ) -> list[dict[str, Any]]:
        """批量处理日志的存储生命周期。"""
        results: list[dict[str, Any]] = []
        for entry in log_entries:
            ctx = StorageContext(log_entry=entry)
            try:
                ctx.filter_result = self._apply_filters(entry, filter_rule_keys or [])
                if ctx.filter_result.get("action") == "丢弃":
                    ctx.status = "filtered_out"
                    self._records.append(ctx)
                    results.append(ctx.to_dict())
                    continue

                ctx.store_result = self._store_log(entry, store_engine)
                ctx.rotate_result = self._check_rotate(rotate_policy)
                ctx.clean_result = self._check_clean(clean_policy)
                ctx.status = "completed"
            except Exception as exc:
                ctx.errors.append(str(exc))
                ctx.status = "failed"
            self._records.append(ctx)
            results.append(ctx.to_dict())
        return results

    def _apply_filters(self, entry: dict[str, Any], rule_keys: list[str]) -> dict[str, Any]:
        for key in rule_keys:
            rule = self.FILTER_RULES.get(key)
            if not rule:
                continue
            field_name = rule["field"]
            field_value = entry.get(field_name, "")
            if rule["op"] == "eq" and str(field_value).upper() == rule["value"].upper():
                return {"action": rule["action"], "matched_rule": key,
                        "matched_field": field_name, "matched_value": field_value}
            if rule["op"] == "contains" and rule["value"].lower() in str(field_value).lower():
                return {"action": rule["action"], "matched_rule": key,
                        "matched_field": field_name, "matched_value": str(field_value)[:50]}
        return {"action": "保留", "matched_rule": "default"}

    def _store_log(self, entry: dict[str, Any], engine: str) -> dict[str, Any]:
        engine_config = self.STORE_ENGINES.get(engine, self.STORE_ENGINES["elasticsearch"])
        self._id_counter += 1
        return {
            "stored": True,
            "engine": engine,
            "storage_id": f"log-{self._id_counter:06d}",
            "partition": engine_config["default_partition"],
            "compressed_size": len(str(entry)) * 0.7,
            "index_count": min(len(entry), 5),
        }

    def _check_rotate(self, policy: str) -> dict[str, Any]:
        policy_config = self.ROTATE_POLICIES.get(policy, self.ROTATE_POLICIES["time_based"])
        return {
            "rotate_needed": self._id_counter % 3 == 0,
            "policy": policy,
            "condition": policy_config["condition"],
            "compress_format": policy_config["compress"],
            "archive_path": f"/data/archive/{datetime.now(timezone.utc).strftime('%Y%m%d')}/",
        }

    def _check_clean(self, policy: str) -> dict[str, Any]:
        policy_config = self.CLEAN_POLICIES.get(policy, self.CLEAN_POLICIES["by_age"])
        now = datetime.now(timezone.utc)
        return {
            "clean_needed": self._id_counter % 5 == 0,
            "policy": policy,
            "condition": policy_config["condition"],
            "estimated_space_freed": "200MB" if self._id_counter % 5 == 0 else "0",
            "scheduled_at": now.isoformat(),
        }

    def snapshot(self) -> dict[str, Any]:
        records = self._records
        return {
            "pipeline": "storage",
            "total_processed": len(records),
            "completed": sum(1 for r in records if r.status == "completed"),
            "filtered_out": sum(1 for r in records if r.status == "filtered_out"),
            "failed": sum(1 for r in records if r.status == "failed"),
        }