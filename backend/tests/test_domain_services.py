"""领域服务行为测试：覆盖18个模块服务的查询、登记、动作和快照。"""

from __future__ import annotations

import pytest

from app.services import service_for


class TestLogSourceService:
    def setup_method(self):
        self.service = service_for("log_source")

    def test_list_entries_returns_data(self):
        items, total = self.service.list_entries()
        assert total >= 3
        assert len(items) >= 3

    def test_filter_by_status(self):
        items, total = self.service.list_entries(status="草稿")
        assert all(item["status"] == "草稿" for item in items)

    def test_get_entry_exists(self):
        entry = self.service.get_entry(1)
        assert entry is not None
        assert entry["id"] == 1
        assert "来源标识" in entry

    def test_get_entry_not_found(self):
        entry = self.service.get_entry(99999)
        assert entry is None

    def test_create_entry_valid(self):
        values = {"来源标识": "Nginx访问日志", "日志类型": "access", "采集协议": "filebeat"}
        entry, missing = self.service.create_entry(values)
        assert not missing
        assert entry is not None
        assert entry["status"] == "草稿"
        assert entry["pending"] is True

    def test_create_entry_missing_fields(self):
        values = {"来源标识": "测试"}
        entry, missing = self.service.create_entry(values)
        assert missing
        assert entry is None

    def test_run_action_submit_verify(self):
        entry, msg = self.service.run_action(1, "提交校验")
        assert entry is not None
        assert entry["status"] == "待校验"

    def test_run_action_invalid(self):
        entry, msg = self.service.run_action(1, "不存在的动作")
        assert entry is None

    def test_domain_snapshot(self):
        snap = self.service.domain_snapshot()
        assert snap["module"] == "log_source"
        assert "count" in snap
        assert "by_status" in snap


class TestLogParserService:
    def setup_method(self):
        self.service = service_for("log_parser")

    def test_list_structure(self):
        items, total = self.service.list_entries()
        assert total >= 3

    def test_create_and_action(self):
        values = {"模板标识": "Apache日志模板", "适用来源": "apache-log", "格式类型": "regex"}
        entry, missing = self.service.create_entry(values)
        assert not missing
        eid = entry["id"]
        updated, _ = self.service.run_action(eid, "启用模板")
        assert updated["status"] == "已启用"


class TestFieldNormalizeService:
    def setup_method(self):
        self.service = service_for("field_normalize")

    def test_create_rule(self):
        values = {"规则标识": "时间戳标准化", "目标字段": "@timestamp", "源字段名": "ts"}
        entry, missing = self.service.create_entry(values)
        assert not missing
        assert entry["status"] == "草稿"

    def test_run_action_enable(self):
        entry, msg = self.service.run_action(1, "启用规则")
        assert entry is not None
        assert entry["status"] == "已生效"


class TestLogEnrichService:
    def setup_method(self):
        self.service = service_for("log_enrich")

    def test_basic_ops(self):
        values = {"规则标识": "IP地理增强", "输入字段": "client_ip", "增强来源": "geo_db"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestTraceLinkService:
    def setup_method(self):
        self.service = service_for("trace_link")

    def test_span_lifecycle(self):
        values = {"记录标识": "span-001", "追踪标识": "trace-abc", "跨度标识": "span-abc-1"}
        entry, missing = self.service.create_entry(values)
        assert not missing
        eid = entry["id"]
        updated, _ = self.service.run_action(eid, "关联上下游")
        assert updated["status"] == "已采集"

    def test_mark_broken(self):
        entry, msg = self.service.run_action(1, "标记断裂")
        assert entry["status"] == "链路断裂"


class TestLogFilterService:
    def setup_method(self):
        self.service = service_for("log_filter")

    def test_filter_rule_crud(self):
        values = {"规则标识": "丢弃DEBUG日志", "适用来源": "app-log", "过滤字段": "level"}
        entry, missing = self.service.create_entry(values)
        assert not missing

    def test_enable_rule(self):
        entry, msg = self.service.run_action(1, "启用规则")
        assert entry["status"] == "已生效"


class TestLogStoreService:
    def setup_method(self):
        self.service = service_for("log_store")

    def test_store_policy(self):
        values = {"策略标识": "ES热存储策略", "目标来源": "app-log", "存储引擎": "elasticsearch"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestSampleAggregateService:
    def setup_method(self):
        self.service = service_for("sample_aggregate")

    def test_sample_policy(self):
        values = {"策略标识": "流量采样", "目标来源": "nginx-log", "采样方法": "reservoir"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestErrorClassifyService:
    def setup_method(self):
        self.service = service_for("error_classify")

    def test_classify_rule(self):
        values = {"规则标识": "NPE分类", "错误模式": "NullPointerException", "分类标签": "空指针"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestCollectHealthService:
    def setup_method(self):
        self.service = service_for("collect_health")

    def test_health_check(self):
        values = {"检查标识": "HC-001", "关联来源": "app-log", "检查类型": "throughput_rate"}
        entry, missing = self.service.create_entry(values)
        assert not missing

    def test_run_check(self):
        entry, msg = self.service.run_action(1, "立即检查")
        assert entry is not None


class TestLogRotateService:
    def setup_method(self):
        self.service = service_for("log_rotate")

    def test_rotate_config(self):
        values = {"配置标识": "按日轮转", "目标存储": "ES索引", "轮转条件": "daily"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestTraceMetricService:
    def setup_method(self):
        self.service = service_for("trace_metric")

    def test_metric_ops(self):
        values = {"指标标识": "P99延迟", "关联服务": "api-gateway", "指标类型": "latency"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestLogExportService:
    def setup_method(self):
        self.service = service_for("log_export")

    def test_export_task(self):
        values = {"任务标识": "导出错误日志", "查询条件": "level=ERROR", "导出格式": "CSV"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestRetentionCleanService:
    def setup_method(self):
        self.service = service_for("retention_clean")

    def test_clean_task(self):
        values = {"任务标识": "清理30天前日志", "目标存储": "ES索引", "清理条件": "age>30d"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestLogReplayService:
    def setup_method(self):
        self.service = service_for("log_replay")

    def test_replay_task(self):
        values = {"任务标识": "回放上月日志", "来源存储": "ES_202601", "时间范围": "2026-01"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestFormatDetectService:
    def setup_method(self):
        self.service = service_for("format_detect")

    def test_detect_result(self):
        values = {"结果标识": "DET-001", "样本内容": "2026-01-01 ERROR test", "探测格式": "space_delimited"}
        entry, missing = self.service.create_entry(values)
        assert not missing


class TestAnomalyAlertService:
    def setup_method(self):
        self.service = service_for("anomaly_alert")

    def test_alert_rule(self):
        values = {"规则标识": "错误率告警", "告警对象": "app-log", "告警条件": "错误率 > 5%"}
        entry, missing = self.service.create_entry(values)
        assert not missing

    def test_enable_alert(self):
        entry, msg = self.service.run_action(1, "启用告警")
        assert entry["status"] == "待启用"


class TestIngestBufferService:
    def setup_method(self):
        self.service = service_for("ingest_buffer")

    def test_buffer_queue(self):
        values = {"队列标识": "Q-001", "关联来源": "app-log", "缓冲容量": "100000"}
        entry, missing = self.service.create_entry(values)
        assert not missing