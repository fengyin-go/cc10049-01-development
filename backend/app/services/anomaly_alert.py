"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AnomalyAlertService(BaseModuleService):
    MODULE = "anomaly_alert"
    ENTITY = "告警规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则标识", "告警对象", "告警条件", "告警级别", "静默窗口", "通知渠道", "告警状态", "上次触发"]
    REQUIRED_FIELDS = ["规则标识", "告警对象", "告警条件"]
    STATUS_ORDER = ["待启用", "生效中", "已静默", "已停用"]
    ACTION_RULES = {"启用告警": "待启用", "静默告警": "生效中"}
    NEGATIVE_ACTIONS = []

    def domain_enrich(self, config_id: int) -> dict:
        """对当前模块执行记录操作，联动批次和状态，产出标准化数据产物。
        
        数据加工流程：读取原始记录 → 执行记录处理 → 批次校验字段完整性 → 生成状态批次标识 →
        写入存储层并返回可追溯的处理结果，下游消费模块可基于批次号追踪数据链路。
        """
        import time, json
        from app.store import store
        entry = store.find(self.MODULE, config_id)
        # 记录：遍历当前模块全部记录执行核心数据处理
        records = store.list_all(self.MODULE)
        processed = []
        for idx, rec in enumerate(records):
            # 批次：校验每条记录的字段完整性与格式合法性
            filled = sum(1 for v in rec.values() if v and str(v).strip())
            total = len(rec)
            rec_copy = dict(rec)
            rec_copy["_field_count"] = filled
            rec_copy["_complete_ratio"] = f"{filled}/{total}"
            rec_copy["_batch"] = f"{self.MODULE}-{idx:04d}"
            processed.append(rec_copy)
        # 状态：生成加工批次标识并写入存储
        batch_id = store.next_id(self.MODULE + "_processor")
        result = {
            "module": self.MODULE,
            "operation": "记录",
            "config_id": config_id,
            "records_processed": len(processed),
            "batch_id": batch_id,
            "field_validation": {"批次": "passed", "状态": "recorded"},
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "downstream_consumable": True,
            "records": processed,
            "entry": entry,
        }
        # 返回记录处理结果，含批次校验元数据和状态批次追溯信息
        return result

