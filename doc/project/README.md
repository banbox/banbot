# banbot 项目总览

> 本文由 `doc/project/01~08` 各模块文档提炼而成，基于 v0.7.1（`core.Version`）、main 分支源码整理。
> 先读本文了解全貌，改动具体模块时再读对应的详细文档。行号和细节以源码为准。**修改本目录文档前先读第 8 节维护规约。**

## 1. 项目是什么

banbot 是一个 Go 编写的量化交易系统，覆盖：策略配置、交易对筛选、历史数据管理、回测、超参优化、实盘交易、行情采集、WebUI、通知与远程控制。

- **两套策略引擎**，由 `run_policy.engine` 选择：
  - `time_series`：时序策略，每个「策略 × 周期 × 品种」一个 `StratJob`，由 `biz.Trader` 分发 K 线/数据并产生进出场请求。
  - `factor`：截面因子 / 多因子组合策略，含统计套利（配对），用 DAG 计算因子，由组合策略（portfolio policy）生成目标持仓。
  - 两者可混合（mixed），在 `execution.SharedAccount` 中按归属与预算净额执行。
- **任意时序数据**：`orm.DataSeries`（`Values map[string]any`）是统一运行时数据模型，K 线与自定义字段走同一条路径，不得移除或收窄为固定 OHLCV 字段。
- **交易所对接**由外部库 `banexg` 负责，banbot 内禁止写交易所专属分支（见 `AGENTS.md`）。
- **存储**：QuestDB / TimescaleDB 双后端存时序数据；SQLite 存订单、钱包、UI 任务、执行账本。

## 2. 分层架构

```
入口层     main → entry（Cobra 命令、Hub、回测/实盘/因子装配）   web（dev/live/base/ui）
运行态层   runtime.Process → runtime.Runtime（任务专属状态）      config（UnifiedConfig→RunSpec→Snapshot）
交易业务层 goods（选品） strat（策略/Job） biz（Trader、订单、钱包） live（实盘主控） opt（回测/优化）
因子层     factor（DAG、Session、portfolio、policy、statistics） factor/expr factor/runner factor/backtest factor/research
执行层     execution（AccountOwner、SharedAccount、Hub、持久化账本、Banexg 适配）
数据层     data（provider/feeder/spider/watcher） orm（DataSeries、QuestDB/PG、ormo、ormu）
基础层     core btime com utils config audit rpc llm
外部       banexg（交易所） banta（技术指标）
```

关键所有权关系：一个 `Process` 可创建多个 `Runtime`；任务之间不共享可变业务状态，共享物理账户、SID registry、scheduler 须遵守 owner/borrow 契约。`context.Context` 只传取消和 deadline，不作业务状态容器。

## 3. 模块地图与文档索引

| 目录 | 一句话职责 | 详细文档 |
|------|-----------|----------|
| `core/ config/ utils/ btime/ com/ audit/ cmd/ scripts/ docker/` | 常量与全局状态、配置分层合并与 RunSpec、时钟、价格缓存、工具库、审计、启动入口 | [01_foundation.md](01_foundation.md) |
| `orm/ data/` | DataSeries 模型、QuestDB/PG 读写与表替换、历史补齐、回放 feeder、实盘采集、逐笔归档 | [02_data_storage.md](02_data_storage.md) |
| `exg/ goods/` | banexg 适配层与可选能力接口；品种筛选过滤器流水线；国内市场（仅文档） | [03_exchange_goods.md](03_exchange_goods.md) |
| `strat/ runtime/ runtimeplan/` | 策略接口、StratJob、信号意图、Job 加载/轮换、Process→Runtime 所有权、数据计划编译 | [04_strategy_runtime.md](04_strategy_runtime.md) |
| `biz/ live/` | Trader、各类订单管理器（本地/实盘/共享/Hub）、钱包、检查点与接管、远程命令、实盘主控制器 | [05_biz_orders_live.md](05_biz_orders_live.md) |
| `execution/` | 账户级执行：单写入者 AccountOwner、VirtualLot/Plan/StoredOrder、账本、Hub RPC、恢复与对账 | [06_execution.md](06_execution.md) |
| `factor/` | 因子 DAG 与表达式、Session/Batch、组合与生命周期策略、版本存储、统计套利、研究与回测 | [07_factor_engine.md](07_factor_engine.md) |
| `kb/` | 策略知识库（ts/factor/statarb 中英文 Markdown），`kb.GetKB(lang, names...)` 读取嵌入内容 | [../README.md](../README.md)「AI 知识库」 |
| `entry/ opt/ web/ rpc/ llm/` | CLI 与装配、回测与超参优化、开发/实盘 Web API、通知与 Telegram、LLM | [08_entry_opt_web_rpc.md](08_entry_opt_web_rpc.md) |

既有的专题文档（保持独立）：

- 架构流程：[../app_arch.md](../app_arch.md)、[../runtime_context.md](../runtime_context.md)、[../db.md](../db.md)、[../banio.md](../banio.md)、[../help_ui.md](../help_ui.md)
- 时序数据：[../series_usage.md](../series_usage.md)、[../custom_data.md](../custom_data.md)
- 因子/统计套利：`../factor_*.md`、[../statarb_guide.md](../statarb_guide.md)
- 策略编写知识库：[../../kb/](../../kb/)（`kb_{ts,factor,statarb}_{cn,en}.md`；代码中用 `kb.GetKB("cn"|"en", "ts"|"factor"|"statarb")`，见 [../README.md](../README.md)）
- execution 子目录自带：`execution/HUB.md`、`RECOVERY.md`、`TS_MIGRATION.md`、`CROSSVENUE.md`、`OPTIONS.md`

## 4. 核心数据流

1. `entry` 解析命令与配置，生成唯一 RunSpec，创建 `Process` 与 `Runtime`，注入配置快照、存储、交易所、取消信号。
2. 按 `run_policy.engine` 分派：时序 → `goods` 选品 + `strat` 生成 Job；因子 → `factor/runner` 构建定义、Universe、决策计划。
3. `data` 把行情或第三方数据统一转成 `orm.DataSeries`（回测由 `HistProvider`+feeder 回放，实盘由 `LiveProvider`+`LiveSpider`）。
4. 时序：`biz.Trader` 分发到 Job，策略产生 `EnterReq/ExitReq`（只入队不下单），经回调交给订单管理器。因子：快照冻结当轮可见数据 → Session 计算 → 组合/policy 生成目标。
5. 订单：回测用本地 odmgr；实盘用 `LiveOrderMgr`；共享账户/因子/混合走 `execution.SharedAccount`（先落库、后发送的状态机）；Hub 模式经本机 Unix socket RPC。
6. `orm` 持久化 K 线、时序、订单、任务；`web`/`rpc`/`live` 提供监控、控制与通知。

## 5. 关键约定与易踩坑

- **交易所无关**：差异全部在 banexg；banbot 里不得出现 `if exchange == "xxx"`（已知例外见 [08](08_entry_opt_web_rpc.md) 已知限制：`web/base/indicators.go` 硬编码 binance）。
- **DataSeries 兼容**：不引入 typed OHLCV 快速路径；聚合、复权、序列化、DataHub、feeder 必须保留自定义字段及其类型/NULL 语义。
- **QuestDB WAL 写后读**：写入后不能凭一次即时读取做回退或破坏性操作；`DROP/RENAME` 表替换前必须验证替换表快照；可见性超时要保留恢复标记。相关改动必须带超时/轮询路径或删除前验证的回归测试（详见 [02](02_data_storage.md)、[../db.md](../db.md)）。
- **Runtime 路径优先**：新代码用 `*WithRuntimeDeps` / `*WithState` 版本，避免读包级全局；旧门面（`GetOdMgr`、`strat.GetJobs`、`core.Legacy*`）仍并行存在。
- **执行层单写入者**：同一物理账户只有一个 `AccountOwner`，网络写入只能经 `OwnerExecutor`。
- **发版**：先改 `core/data.go` 的 `Version`；若改了 `web/ui` 前端，编译打包后更新 `UIVersion`，再 `git tag`。
- 不对 banbot/banexg/banta 使用 DeepWiki。

## 6. 如何增删/调整功能

| 想做的事 | 先看 |
|----------|------|
| 新增时序策略/回调 | [04](04_strategy_runtime.md)「扩展与修改指南」，知识库 `kb/kb_ts_cn.md` |
| 新增因子/算子/表达式函数 | [07](07_factor_engine.md)，`factor_expression_*.md` |
| 新增品种过滤器 | [03](03_exchange_goods.md)（`goods/filters.go`、注册方式见文档） |
| 新增数据源/自定义序列 | [02](02_data_storage.md)，`custom_data.md`、`series_usage.md` |
| 改订单/钱包/平仓逻辑 | [05](05_biz_orders_live.md) 与 [06](06_execution.md) |
| 新增命令/回测流程/优化器 | [08](08_entry_opt_web_rpc.md) |
| 新增配置项 | [01](01_foundation.md)（配置加载链路），同步 `doc/config.yml` |
| 新增 Web API / 前端页面 | [08](08_entry_opt_web_rpc.md)，`help_ui.md` |
| 改数据库表/迁移 | [02](02_data_storage.md)，`db.md` |

## 7. 已知限制

各模块文档末尾的「已知限制与设计取舍」章节描述当前实现中长期存在的行为约束与设计取舍，改动相关模块前先阅读。

## 8. 文档维护规约（必读）

`doc/project/` 的目的：维护一份**始终与最新代码逻辑对齐、长期有效**的项目介绍文档。修改本目录任何文档时必须遵守：

- **只描述当前实现**：职责、结构、数据流、约束、扩展方式、已知限制，均以“现在代码是什么样”来写。
- **禁止记录临时/一次性结果**：不得出现“已修复”“✅”“不修复”“本轮审阅”“待确认”“未运行测试”“HEAD 某提交”“某次测试失败”“问题清单/问题 N”等流水账或审阅状态。bug 修复结果、审阅发现、测试结论属于提交记录、PR 或 issue，不属于本目录。
- **问题只以长期约束形式存在**：确实长期存在的限制写入各文档末尾的「已知限制与设计取舍」，用陈述句描述行为本身，不写修复状态；某项限制被代码改掉后，应直接删除对应条目，并把正文改为新行为。
- **与代码同步**：改动模块后同步更新对应 `0N_*.md`，再回写本文摘要；删除已不存在的符号、文件与行为。行号只作定位参考，不作为事实依据。
- **不确定就核实**：写入前对照源码确认，不要保留“待确认”类占位；无法确认的内容不写。
- 本文是 `01~08` 的提炼摘要，应在其他文档更新后再更新。
