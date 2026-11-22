# 流批一体数据处理中心

统一管理流式加工和批量计算的数据处理平台，覆盖数据清洗、转换、关联和结果落地

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

健康检查：`curl http://127.0.0.1:8016/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5196/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8016`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式数据源 | `stream_source` | 流式源 | 源名称、消息队列主题、序列化格式 |
| 转换规则 | `transform_rule` | 转换规则 | 规则名称、输入Schema、输出Schema |
| 批量计算管道 | `batch_pipeline` | 计算管道 | 管道名称、输入源、输出目标 |
| 数据关联合并 | `join_config` | 关联配置 | 关联名称、左表源、右表源 |
| 检查点管理 | `checkpoint_mgr` | 检查点 | 管道名称、检查点序列、创建时间 |
| 异常处理策略 | `error_handler` | 异常策略 | 策略名称、适用管道、异常类型 |
| 结果写入器 | `result_writer` | 写入配置 | 配置名称、写入目标类型、输出格式 |
| 迟到数据处理 | `late_data` | 迟到策略 | 策略名称、最大延迟时间、迟到处理方式 |
| 窗口配置 | `window_config` | 窗口配置 | 窗口名称、窗口类型、窗口长度 |
| 聚合计算规则 | `aggregation_rule` | 聚合规则 | 规则名称、聚合函数、分组键 |
| 数据清洗链 | `data_cleanse` | 清洗步骤 | 步骤序号、清洗类型、输入字段 |
| 管道监控 | `pipeline_monitor` | 监控指标 | 指标名称、监控目标、告警阈值 |
| 历史回填 | `backfill_handler` | 回填任务 | 任务名称、目标管道、回填时间范围 |
| 输出审计 | `output_audit` | 审计记录 | 管道名称、输出目标、输出时间 |
| Schema注册 | `schema_registry` | Schema定义 | Schema名称、字段列表、版本号 |
| 数据分区 | `data_partition` | 分区配置 | 分区名称、分区键、分区类型 |
| 管道编排DAG | `pipeline_dag` | 编排图 | 图名称、节点列表、依赖关系 |
| Iceberg表管理 | `iceberg_table` | Iceberg表 | 表名、所属业务域、存储格式 |
| 数据采样 | `data_sample` | 采样配置 | 配置名称、采样比例、采样方式 |
| 计算血缘追踪 | `lineage_tracker` | 血缘条目 | 输入表、转换管道、输出表 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 管道编排DAG的依赖校验与输出审计

批量编排的入口在 `backend/app/engine/batch_pipeline.py`，依赖解析在
`backend/app/engine/dag_graph.py`（纯图计算，无副作用）。

### 启动前先拿可执行分区

`POST /api/pipeline/batch/dag/plan`（也可在“管道编排DAG / 批量计算管道 /
输出审计”页面点“解析依赖”）返回：

- `cycles`：真正的依赖环（Tarjan 强连通分量），不会把环下游误判成环；
- `blocked_nodes`：环内节点 **加上所有依赖其产物的下游分支**；
- `executable_nodes` / `execution_order`：真实可执行的节点与拓扑顺序；
- `partitions`：按弱连通分量切出的编排分区，`可执行` / `阻塞` / `部分可执行`。

示例数据“销售编排样例”的图：

```text
抽取管道 ──▶ 销售管道 ──▶ 汇总管道        （抽取 ◀─▶ 销售 成环）
税务管道                                   （独立分支）
```

解析结果：`分区1 = 抽取/销售/汇总` 阻塞，`分区2 = 税务` 可执行。值班人因此
可以确认：**独立税务分支仍能给自己的表 `ads.tax_report` 供数**。

### 执行规则

- 成环节点、依赖其产物的下游分支、上游运行失败的节点都只登记阻塞，不写结果写入器；
- 只有真正写入 `result_writer` 的节点才会产出“校验通过”的审计；
- `POST /api/pipeline/batch/dag/run?run_key=...` 的 `run_key` 是幂等键，
  并发触发同一 `run_key` 只执行一次，其余请求返回同一份 `run_id` 与分区结果；
- `fail_nodes=节点名` 可注入一次执行失败，用于演示失败排障。

### 审计与重试

- `GET /api/pipeline/batch/audit?run_id=`：审计消费本次运行的分支执行回执，
  只把实际落地的目标标为校验通过，并给出：
  `summary.blocked_targets`、`summary.cycle_targets`（因环无法出数的目标）、
  `confirmed_partitions` / `unproduced_partitions`，并与 `writes` 相互对照；
- `POST /api/pipeline/batch/dag/retry?previous_run_id=`：重试新运行带
  `retry_of` 与 `previous_blocked_scope`；
- `GET /api/pipeline/batch/partitions/history?target=`：跨运行查分区产出，
  修正环后再执行仍能查到此前未产出的分区；
- `POST /api/pipeline/batch/dag/definition`：更新节点/依赖（修正环），之后用
  同一 `run` 入口再次执行。

“输出审计”的“标记通过 / 重新校验”动作也会先查结果写入器，没有实际写入记录
的目标不会被标成校验通过。

领域：数据工程
子方向：数仓与数据湖
