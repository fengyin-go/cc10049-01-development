"""数据消费管线：日志导出→日志回放 端到端处理。"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class ConsumptionContext:
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    export_task: dict[str, Any] = field(default_factory=dict)
    replay_task: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    status: str = "pending"

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "export_task": self.export_task,
            "replay_task": self.replay_task,
            "errors": self.errors,
            "status": self.status,
        }


class DataConsumptionPipeline:
    """数据消费管线：日志导出和回放的完整数据消费链路。"""

    EXPORT_FORMATS = {
        "csv": {"mime": "text/csv", "ext": ".csv", "header": True},
        "json": {"mime": "application/json", "ext": ".jsonl", "header": False},
        "parquet": {"mime": "application/octet-stream", "ext": ".parquet", "header": False},
    }

    REPLAY_POLICIES = {
        "exact_rate": {"type": "等速回放", "dedup": "by_key"},
        "accelerated": {"type": "加速回放", "dedup": "by_key+ts"},
        "sequential": {"type": "顺序回放", "dedup": "by_ts"},
    }

    def __init__(self):
        self._records: list[ConsumptionContext] = []
        self._id_counter = 0

    def create_export_task(
        self,
        query_condition: str,
        export_format: str = "csv",
        time_range: str = "",
        field_selection: str = "",
    ) -> dict[str, Any]:
        self._id_counter += 1
        task = {
            "task_id": f"export-{self._id_counter:06d}",
            "query_condition": query_condition,
            "export_format": export_format,
            "format_info": self.EXPORT_FORMATS.get(export_format, self.EXPORT_FORMATS["csv"]),
            "time_range": time_range or "最近7天",
            "field_selection": field_selection or "全部字段",
            "estimated_rows": min(self._id_counter * 500, 100000),
            "estimated_size_mb": round(self._id_counter * 500 * 0.5 / 1024, 2),
            "status": "queued",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        ctx = ConsumptionContext(export_task=task)
        self._records.append(ctx)
        return ctx.to_dict()

    def run_export(self, task_id: str) -> dict[str, Any]:
        for ctx in self._records:
            if ctx.export_task.get("task_id") == task_id:
                now = datetime.now(timezone.utc)
                ctx.export_task["status"] = "exporting"
                ctx.export_task["started_at"] = now.isoformat()
                size_mb = ctx.export_task.get("estimated_size_mb", 0)
                row_count = ctx.export_task.get("estimated_rows", 0)
                if row_count > 50000:
                    ctx.export_task["status"] = "completed"
                    ctx.export_task["completed_rows"] = row_count
                    ctx.export_task["file_size_mb"] = round(size_mb, 2)
                    ctx.export_task["finished_at"] = now.isoformat()
                    ctx.status = "completed"
                else:
                    ctx.export_task["status"] = "exporting"
                    ctx.status = "exporting"
                return ctx.to_dict()
        return {"error": "task not found"}

    def complete_export(self, task_id: str) -> dict[str, Any]:
        for ctx in self._records:
            if ctx.export_task.get("task_id") == task_id:
                now = datetime.now(timezone.utc)
                ctx.export_task["status"] = "completed"
                ctx.export_task["completed_rows"] = ctx.export_task.get("estimated_rows", 0)
                ctx.export_task["file_size_mb"] = round(ctx.export_task.get("estimated_size_mb", 0), 2)
                ctx.export_task["finished_at"] = now.isoformat()
                ctx.status = "completed"
                return ctx.to_dict()
        return {"error": "task not found"}

    def cancel_export(self, task_id: str) -> dict[str, Any]:
        for ctx in self._records:
            if ctx.export_task.get("task_id") == task_id:
                ctx.export_task["status"] = "cancelled"
                ctx.status = "cancelled"
                return ctx.to_dict()
        return {"error": "task not found"}

    def create_replay_task(
        self,
        source_storage: str,
        time_range: str,
        replay_rate: str = "1x",
        target_pipeline: str = "ingestion",
        dedup_strategy: str = "by_key",
    ) -> dict[str, Any]:
        rate_info = self.REPLAY_POLICIES.get(replay_rate, self.REPLAY_POLICIES["exact_rate"])
        self._id_counter += 1
        task = {
            "task_id": f"replay-{self._id_counter:06d}",
            "source_storage": source_storage,
            "time_range": time_range,
            "replay_rate": replay_rate,
            "replay_policy": rate_info,
            "target_pipeline": target_pipeline,
            "dedup_strategy": dedup_strategy,
            "estimated_lines": min(self._id_counter * 1000, 500000),
            "replayed_lines": 0,
            "status": "queued",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        ctx = ConsumptionContext(replay_task=task)
        self._records.append(ctx)
        return ctx.to_dict()

    def start_replay(self, task_id: str) -> dict[str, Any]:
        for ctx in self._records:
            if ctx.replay_task.get("task_id") == task_id:
                now = datetime.now(timezone.utc)
                ctx.replay_task["status"] = "replaying"
                ctx.replay_task["started_at"] = now.isoformat()
                ctx.status = "replaying"
                return ctx.to_dict()
        return {"error": "task not found"}

    def progress_replay(self, task_id: str, lines_processed: int) -> dict[str, Any]:
        for ctx in self._records:
            if ctx.replay_task.get("task_id") == task_id:
                ctx.replay_task["replayed_lines"] = lines_processed
                total = ctx.replay_task.get("estimated_lines", 1)
                if lines_processed >= total:
                    ctx.replay_task["status"] = "completed"
                    ctx.replay_task["finished_at"] = datetime.now(timezone.utc).isoformat()
                    ctx.status = "completed"
                return ctx.to_dict()
        return {"error": "task not found"}

    def cancel_replay(self, task_id: str) -> dict[str, Any]:
        for ctx in self._records:
            if ctx.replay_task.get("task_id") == task_id:
                ctx.replay_task["status"] = "cancelled"
                ctx.status = "cancelled"
                return ctx.to_dict()
        return {"error": "task not found"}

    def export_tasks(self) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self._records if r.export_task]

    def replay_tasks(self) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self._records if r.replay_task]

    def snapshot(self) -> dict[str, Any]:
        exports = self.export_tasks()
        replays = self.replay_tasks()
        return {
            "pipeline": "data_consumption",
            "total_tasks": len(self._records),
            "export_tasks": len(exports),
            "export_completed": sum(1 for t in exports if t["export_task"].get("status") == "completed"),
            "export_failed": sum(1 for t in exports if t["export_task"].get("status") == "cancelled"),
            "replay_tasks": len(replays),
            "replay_completed": sum(1 for t in replays if t["replay_task"].get("status") == "completed"),
            "replay_failed": sum(1 for t in replays if t["replay_task"].get("status") == "cancelled"),
        }