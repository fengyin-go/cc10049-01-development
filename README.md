# 日志采集解析与链路追踪平台

面向多源日志采集、格式解析、结构化转换、链路追踪和采集质量监控的日志数据工程平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8013/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5177/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8013`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 日志源 | `log_source` | 日志源 | 来源标识、日志类型、采集协议 |
| 日志解析 | `log_parser` | 解析模板 | 模板标识、适用来源、格式类型 |
| 字段标准化 | `field_normalize` | 标准化规则 | 规则标识、目标字段、源字段名 |
| 日志增强 | `log_enrich` | 增强规则 | 规则标识、输入字段、增强来源 |
| 链路追踪 | `trace_link` | 链路记录 | 记录标识、追踪标识、跨度标识 |
| 日志过滤 | `log_filter` | 过滤规则 | 规则标识、适用来源、过滤字段 |
| 日志存储 | `log_store` | 存储策略 | 策略标识、目标来源、存储引擎 |
| 采样聚合 | `sample_aggregate` | 采样策略 | 策略标识、目标来源、采样方法 |
| 错误分类 | `error_classify` | 分类规则 | 规则标识、错误模式、分类标签 |
| 采集健康 | `collect_health` | 健康检查 | 检查标识、关联来源、检查类型 |
| 日志轮转 | `log_rotate` | 轮转配置 | 配置标识、目标存储、轮转条件 |
| 链路指标 | `trace_metric` | 链路指标 | 指标标识、关联服务、指标类型 |
| 日志导出 | `log_export` | 导出任务 | 任务标识、查询条件、导出格式 |
| 保留清理 | `retention_clean` | 清理任务 | 任务标识、目标存储、清理条件 |
| 日志回放 | `log_replay` | 回放任务 | 任务标识、来源存储、时间范围 |
| 格式探测 | `format_detect` | 探测结果 | 结果标识、样本内容、探测格式 |
| 异常告警 | `anomaly_alert` | 告警规则 | 规则标识、告警对象、告警条件 |
| 摄入缓冲 | `ingest_buffer` | 缓冲队列 | 队列标识、关联来源、缓冲容量 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
