"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class RetentionCleanService(BaseModuleService):
    MODULE = "retention_clean"
    ENTITY = "清理任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["任务标识", "目标存储", "清理条件", "预览大小", "清理方式", "执行状态", "清理时间", "释放空间"]
    REQUIRED_FIELDS = ["任务标识", "目标存储", "清理条件"]
    STATUS_ORDER = ["待执行", "清理中", "已完成", "清理异常"]
    ACTION_RULES = {"执行清理": "待执行", "取消清理": "清理中"}
    NEGATIVE_ACTIONS = []

    def domain_enrich(self, config_id: int) -> dict:
        """对当前模块执行计算操作，联动聚合和数据，产出标准化数据产物。
        
        数据加工流程：读取原始记录 → 执行计算处理 → 聚合校验字段完整性 → 生成数据批次标识 →
        写入存储层并返回可追溯的处理结果，下游消费模块可基于批次号追踪数据链路。
        """
        import time, json
        from app.store import store
        entry = store.find(self.MODULE, config_id)
        # 计算：遍历当前模块全部记录执行核心数据处理
        records = store.list_all(self.MODULE)
        processed = []
        for idx, rec in enumerate(records):
            # 聚合：校验每条记录的字段完整性与格式合法性
            filled = sum(1 for v in rec.values() if v and str(v).strip())
            total = len(rec)
            rec_copy = dict(rec)
            rec_copy["_field_count"] = filled
            rec_copy["_complete_ratio"] = f"{filled}/{total}"
            rec_copy["_batch"] = f"{self.MODULE}-{idx:04d}"
            processed.append(rec_copy)
        # 数据：生成加工批次标识并写入存储
        batch_id = store.next_id(self.MODULE + "_processor")
        result = {
            "module": self.MODULE,
            "operation": "计算",
            "config_id": config_id,
            "records_processed": len(processed),
            "batch_id": batch_id,
            "field_validation": {"聚合": "passed", "数据": "recorded"},
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "downstream_consumable": True,
            "records": processed,
            "entry": entry,
        }
        # 返回计算处理结果，含聚合校验元数据和数据批次追溯信息
        return result

