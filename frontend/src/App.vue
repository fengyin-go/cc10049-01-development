<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">日志采集解析与链路追踪平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向多源日志采集、格式解析、结构化转换、链路追踪和采集质量监控的日志数据工程平台</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "日志源", path: "/log_source" }, { label: "日志解析", path: "/log_parser" }, { label: "字段标准化", path: "/field_normalize" }, { label: "日志增强", path: "/log_enrich" }, { label: "链路追踪", path: "/trace_link" }, { label: "日志过滤", path: "/log_filter" }, { label: "日志存储", path: "/log_store" }, { label: "采样聚合", path: "/sample_aggregate" }, { label: "错误分类", path: "/error_classify" }, { label: "采集健康", path: "/collect_health" }, { label: "日志轮转", path: "/log_rotate" }, { label: "链路指标", path: "/trace_metric" }, { label: "日志导出", path: "/log_export" }, { label: "保留清理", path: "/retention_clean" }, { label: "日志回放", path: "/log_replay" }, { label: "格式探测", path: "/format_detect" }, { label: "异常告警", path: "/anomaly_alert" }, { label: "摄入缓冲", path: "/ingest_buffer" }]
</script>
