"""工作流编排路由：提供完整数据工程工作流的端到端执行与历史查询。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.orchestrator import orchestrator

router = APIRouter(prefix="/api/workflow", tags=["工作流"])


@router.post("/run")
def run_workflow() -> dict[str, Any]:
    """执行完整的数据工程工作流：接入→存储→异常→流控→消费。"""
    result = orchestrator.run_full_workflow()
    return {"ok": True, "workflow": result}


@router.get("/history")
def workflow_history() -> dict[str, Any]:
    """查询工作流执行历史。"""
    history = orchestrator.run_history()
    last = orchestrator.last_snapshot()
    return {"total_runs": len(history), "history": history, "last_snapshot": last}


@router.get("/snapshot")
def workflow_snapshot() -> dict[str, Any]:
    """获取最后一次工作流的摘要快照。"""
    snapshot = orchestrator.last_snapshot()
    if snapshot is None:
        return {"ok": False, "message": "尚无工作流执行记录"}
    return {"ok": True, "snapshot": snapshot}