# 08 应用编排与接口模块：entry / opt / web / rpc / llm

> 基于当前源码（分支 main，`core.Version = v0.7.1`，`core.UIVersion = v0.6.0`）梳理。文中 `文件:行号` 均经过核对。
>
> 本文描述 entry/opt/web/rpc/llm 的当前实现，行号仅作定位参考。

## 1. 定位与职责

| 包 | 职责 |
| --- | --- |
| `entry` | Cobra 命令树、命令注册、显式会话装配（`explicitEntrySession` 持有 Process/Runtime/存储/交易所）、按 `run_policy.engine` 把请求分派到时序或因子路径、Hub 守护进程、因子与混合策略的回测/实盘装配。 |
| `opt` | 回测引擎 `BackTest` / `BackTestLite`、超参优化（bayes / goptuna 系列）、滚动优化（bt-opt）、回测报告 `BTResult`、工具命令（订单对比、bt-factor、资产报告合并、回放）。 |
| `web` | `web/base` 共享 API（K 线、CSV、指标、WebSocket）；`web/dev` 开发 Web UI（策略文件、回测任务、构建、数据工具）；`web/live` 实盘 API（JWT、账户/订单/收益/配置接口）；`web/ui` 前端静态资源下载与服务。 |
| `rpc` | 通知总线（`SendMsg`）、Webhook 渠道（企业微信、通用 Webhook）、Email、Telegram（通知 + 远程命令）、异常日志通知。 |
| `llm` | 模型配置解析（继承、环境变量替换）、模型实例缓存、并发信号量、超时、失败禁用、OpenAI 兼容与 GLM 客户端。当前仓库内没有非测试代码调用 `NewLLMManager` 或 `LLMManager.Call`，仅 `config` 引用其配置类型（作为外部嵌入预留）。 |

前端 `web/ui` 是 SvelteKit 工程（Svelte 5、Tailwind 4、DaisyUI 5、klinecharts），设计约定见 `doc/help_ui.md`。

## 2. 目录与文件地图

### 2.1 entry（82 个文件，按主题分组）

| 主题 | 文件 | 作用 |
| --- | --- | --- |
| 入口与命令 | `main.go` | `RunCmd`、`ExecuteContext`、隐式 web 判断、`NewRootCommand`、信号处理、`normalizeLegacyFlags`（单横线长参数转双横线）。 |
| | `common.go` | 命令组注册表 `AddGroup`/`AddCommand`/`AddCommandFactory`，`registerBuiltInCommands` 命令树，全部 `bind*` 参数绑定器。 |
| | `entry.go` | 薄包装：`RunBackTest`、`RunTrade`、`RunDownData`、kline 维护（correct/adj/export/purge）、`RunSpider`（已不可达）、`runInit`、data export/import。 |
| | `runtime_logger.go`、`runtime_plan.go`、`runtime_tools.go`、`audit.go` | 日志装配、`internal inspect-data-plan`、data-server/corr/kline-consistency、数据审计。 |
| 会话与运行时 | `runtime_entry.go` | `explicitEntrySession`；`openExplicitEntrySession*`；`newRuntime`/`newStorageRuntime`；时序回测/实盘/下载/模拟/优化工厂（`runExplicitBackTest*`、`runExplicitTrade*`、`backtestFactory`）。 |
| | `runtime_maintenance.go`、`runtime_orders.go`、`runtime_web.go` | 日历加载、数据导入导出、订单对账 (`cmp-orders`)、下载交易所订单、`live trade-close`、开发 Web 子 Runtime 工厂。 |
| 时序路径 | `unified_backtest.go` | 因子回测统一入口（`unifiedFactorBacktestOutputContext`），输出 `config.yml`、`resolved.json`、`events.jsonl`、`run.json`。 |
| | `time_series_execution.go` | prod/dry_run 时序策略的账户级执行（需要 execution 账本）。 |
| | `time_series_hub.go` | 时序执行经 Hub 的分支（`timeSeriesUsesHub`、`runTimeSeriesHubSpec`）。 |
| | `series.go` | `series down` / `series list`，第三方时序覆盖范围补齐。 |
| 因子路径 | `factor_config.go` | RunSpec → `[]runner.Config`（`buildFactorConfigs`），含 archive 身份推导、价格推导、账户权重。 |
| | `factor_live.go` | 因子实盘：`FactorLiveBinding`、绑定工厂注册表、`runFactorLiveSpecWithArgs`、`runFactorsLiveWithStartup`（组装 SharedAccount、AccountSink、runner.Live）。 |
| | `factor_banexg_live.go` | 基于 banexg 的默认因子实盘绑定与 funding 能力探测。 |
| | `factor_storage.go`、`factor_history.go`、`factor_resolved.go`、`factor_archive_types.go` | 存储输入装配（K 线/归档输入）、历史路径校验、解析后配置输出、归档字段类型恢复。 |
| | `factor_hub.go`、`factor.go`、`factor_expression_commands.go`、`source_plan.go`、`execution_config.go`、`execution_takeover.go` | Hub 账户分组执行、归档命令、表达式子命令、数据源计划选项、账户执行字段、执行接管所有者。 |
| 混合策略 | `mixed_factor_backtest.go` | 因子 + 时序混合回测回放（`replayMixedEngines`，事件模式）。 |
| | `mixed_factor_live.go` | 混合实盘：时序策略通过 `prepareMixedLiveBinding` 挂到因子账户的 SharedAccount。 |
| | `strategy_commands.go` | `backtest`、`trade`、`research` 三个统一子命令的 RunSpec 分派。 |
| | `ui_preflight.go`、`factor_expression_commands.go` | Web 回测预检（`InspectBacktestRunSpec`）、表达式命令。 |
| Hub 守护进程 | `hub.go` | `hub` 命令（别名 `spider`）：公共行情 + 私有账户执行 socket。 |
| | `hub_account.go`、`hub_feed.go`、`hub_host_manifest.go`、`hub_registration.go`、`hub_robot_config.go` | 账户注册与解码、待处理 feed 的准入与观测、宿主声明清单、机器人侧 Hub 配置与公共快照。 |

### 2.2 opt（10 个非测试文件）

| 文件 | 作用 |
| --- | --- |
| `backtest.go`（1438 行） | `BackTest` 与 `BackTestLite`：`Init`、`FeedDataSeries`、`RunContext`（主循环、清仓、结果收集）、第三方序列补齐、订单回调、清算。 |
| `hyper_opt.go`（1452 行） | 超参优化：`RunOptimize`、`runOptimize`（进程内或子进程并行）、`optAndPrint`、`optForGroup`、`runBayes`、`runGOptuna`、滚动回测 `RunRollBTPicker`/`RunBTOverOpt`、日志收集 `CollectOptLog`。 |
| `common.go`（574 行） | `OptInfo`、`OptGroup`、`ValItem`、`rollBtOpt`，策略参数 `ToPol`，图表 `Chart`/`ChartDs`，`DescGroups`、`AvgGoodDesc`。 |
| `reports.go`（2513 行） | `BTResult`、`ReportDeps`、`PairPicker` 注册表、报告/图表生成、订单统计；`BTResult` 含 `DelistExits`/`GapExits`（退市/数据空洞强平单数）、`GapExitPnlShare`/`DelistExitPnlShare`（对应强平单 Σ|利润| 占全部订单 Σ|利润| 的比例）与 `dailyEquity`（`[UTC 日末 ms, 权益]`，逐日、不抽样，随 Ulcer 日权益采集）；**爆仓契约**（BtFeatureLevel 10）：`BackTestLite.onLiquidation`（`charge_on_bomb` 关闭）只记录 `liquidation`（时刻/品种/原 BOMB 文本）并终止数据循环，不取消运行时 context；`BackTest.RunContext` 照常清仓（`bot_stop` 平仓单改标 `liquidation`），再由 `BTResult.applyLiquidation` 把爆仓时刻到窗口末的权益记为 0（曲线按爆仓前采样密度补零样本、`dailyEquity` 逐日补 0、`minReal` 0、`maxDrawDownPct` 100），`Collect` 令 `totProfit=-totalInvest`、`totProfitPct=-100`、`finBalance=0`；Ulcer 累加器 `AllowZero` 使 0 权益按 −100% 回撤计、CAGR −1（UPI = −100/UI，有限）；`detail.json` 仅爆仓时出现 `liquidated:true`、`liquidated_at_ms`、`liquidated_symbol`、`liquidated_reason`；`out.log` 指标表多一行 `Liquidated`。优化试验与普通回测走同一条路径，同参同区间指标一致。组合引擎（factor/statarb）的 NAV≤0 见下方破产段与 07_factor_engine；`orders.csv` 末列 `funding` 为近似资金费，`exitFee` 不含该部分。 **破产（BtFeatureLevel 12，issue 162）**：`BackTestLite.checkEquityDepleted`（订单变动或新 bar 时间后，在 `FeedDataSeries` 末尾；`charge_on_bomb` 时不判）按 `BanWallets.EquityWithOrders`（余额 + 未平仓订单当下浮盈）≤ 0 记 `liquidation{cause: equity_depleted}`，之后 `FeedDataSeries` 直接返回 false（不再开仓）；`checkFinalBalance` 在收尾平仓后余额仍 ≤ 0 同样记破产；强平路径的原因为 `margin_liquidation`；`BTResult.LiquidatedCause` 输出 `liquidated_cause`；`applyLiquidation` 在 Ulcer 累加器因爆仓前日收盘 ≤ 0 失效时按 `DailyEquity` 重建（≤0 记 0）；`BTResult.equityNow`（曲线、Ulcer）与破产判定共用 `BanWallets.EquityWithOrders`（默认；`bt_fresh_equity: false` 时曲线回到缓存浮盈口径，破产判定不变），该函数仅对合约市场叠加订单浮盈，现货 base 币已在钱包估值里。 |
| `equity_replay.go` | 离线权益重放（纯函数，不改回测行为、不升 `BtFeatureLevel`）：`ReplayEquity(ReplayParams)` 由已平仓订单列表（`ReplayOrder`：品种/周期/方向/入场均价/成交量/入场费/利润/e_create/e_update/x_update）、初始资金、起止时间、时间步 `StepMS`（订单周期的基础周期：4h/8h→1h、3d→1d，`ReplayBaseTF`）、图表采样时刻 `PlotTimes`、取价回调 `ReplayPriceFunc`（基础周期 K 线收盘）逐步重放 `bt_fresh_equity` 口径的权益：权益 = 初始资金 + 已平仓利润 + Σ 持仓订单 Profit；持仓 Profit 只在订单自身周期 bar 收盘刷新（入场后首次收盘前为 0），开仓费在余额与 Profit 各计一次；每步在首个事件后取样，`FirstSymbols`（单品种回测为该品种，多品种为空）之外的品种本步事件尚未处理（入场/出场不计、价格取上一根）；期末 = 初始资金 + 全部利润。指标复用引擎函数：日权益与 Ulcer 经 `BTResult.recordUlcer/collectUlcer`，回撤 `utils.CalcDrawDown`（逐步 → `MaxDrawDownPct`，图表样本 → `ShowDrawDownPct`），Sharpe/Sortino `CalcMeasuresByReal`。重放权益 ≤0 返回 `NeedsRerun`（该回测订单本身会变，不给指标）。对 57 个引擎实跑真值：UPI 绝对误差 p50 0.0002、p90 0.0065、最大 0.086。bansite `backfill-metrics --rebuild-equity` 使用它。 |
| `tools.go`（1862 行） | 订单对比（`CompareExgBTOrdersWithRuntimeDeps`，旧版入口未接入 CLI）、`FacArgs`/`FactorFunc` 因子注册、`BtFactors`、`BuildBtResult`、资产报告合并、结果压缩。 |
| `sim_bt.go` | `sim-bt`：从报告回放一段时间的回测。 |
| `runtime_factory.go` | `BacktestFactory` 类型：`(snapshot, isOpt, outDir) -> (*BackTest, cleanup, err)`。 |

### 2.3 web（约 30 个文件）

| 目录/文件 | 作用 |
| --- | --- |
| `main.go` | 对外门面：`RunDev`、`NewDevCommandWithFactory`、`StartApi`、`StartApiWithRuntimeDeps`。 |
| `base/api_com.go`、`base/api_ws.go`、`base/websocket.go`、`base/ws_hub.go`、`base/biz_com.go` | 参数校验 `VerifyArg`、错误处理、WebSocket 客户端与 Hub（订阅、广播）、`GetExg`。 |
| `base/api_kline.go`、`base/api_csv.go`、`base/indicators.go` | K 线/序列查询（`getSeries`、`getHist`）、CSV 上传/列表/取数、指标计算（`DrawInd`、`AdvInd`）。 |
| `dev/runtime.go` | `DevServer` 状态、`DevDeps`、按 `(exchange, market)` 解析 `data.RuntimeDeps` 的 `dataFor`。 |
| `dev/main.go` | `web` 命令：host/port/password 参数、basic auth、静态资源、`listenWithContext`。 |
| `dev/api_dev.go`（2301 行） | 开发 API 全部路由（`RegAPI`，见 2.3.1）、文件操作 `handleFileOp`、回测提交、构建 `handleBuild`、文本读写、下载。 |
| `dev/common.go`（868 行） | 回测任务调度（`startBtTaskScheduler`）、子进程执行 `executeBtTask`、进度解析、结果收集 `collectBtResults`、报告目录与路径约束。 |
| `dev/strat.go`、`dev/preflight.go`、`dev/unified_backtest_report.go`、`dev/data_tools.go`、`dev/websocket.go` | 策略模板与 Go 注册代码生成、回测预检、统一回测报告读取、数据工具任务、开发端 WebSocket 与构建状态。 |
| `live/main.go` | `apiServer` 生命周期、`startAPIServer`（CORS、路由分组、静态资源、监听）。 |
| `live/auth.go` | 公开接口（`/login`、`/ping`、`/strat_call`）、JWT 签发与中间件、策略回调鉴权。 |
| `live/biz.go`（1349 行） | `/api/bot` 业务接口（余额、订单、统计、收益、平仓、延迟开仓、配置、日志、下载交易记录）。`wrapAccount` 做账户角色校验。 |
| `live/series_api.go` | 实盘时序查询路由（复用 `base`）。 |
| `ui/run.go` | `ServeStaticAt`：检查 `uidist/version.txt` 与 `core.UIVersion` 是否一致，不一致则从 GitHub/Gitee 下载 `dist.zip` 并解压。 |

#### 2.3.1 开发 API 路由（`web/dev/api_dev.go:49` `RegAPI`）

- 策略文件：`strat_tree`、`file_op`、`new_strat`、`text`、`texts`、`save_text`、`strategy_catalog`、`available_strats`
- 构建：`build_envs`、`build`
- 回测：`bt_tasks`、`bt_options`、`backtest_preflight`、`run_backtest`、`bt_detail`、`bt_orders`、`bt_config`、`bt_logs`、`bt_html`、`bt_strat_tree`、`bt_strat_text`、`del_bt_reports`、`update_note`、`compare_assets`
- 数据：`symbol_info`、`symbol_gaps`、`symbol_data`、`series_ranges`、`symbols`、`data_tools`、`download`
- 其他：`ws`、`logs`

### 2.4 rpc（约 12 个文件）

| 文件 | 作用 |
| --- | --- |
| `notify.go` | 全局通知入口 `SendMsg`、`InitRPC*`、`buildChannels`、`Stop`/`Join`/`CleanUp`。包级状态（`channels`、`rpcReady`、`rpcName`、`rpcWebhook`）由 `channelsMu` 保护。 |
| `session.go` | `Session`：一次运行的 RPC 生命周期 owner，`Start`/`Stop`/`Join`/`SendMsg`。 |
| `webhook.go` | `WebHook`：按 `msg_types`/`accounts`/`keywords` 过滤，队列发送，`min_intv_secs` 限速，`retry_delay` 重试，代理客户端缓存。 |
| `wework.go`、`email.go` | 企业微信（access token 缓存、重试）与 SMTP 邮件（批量合并）。 |
| `telegram.go`（1554 行）、`telegram_runtime.go` | Telegram 通知与命令：`initDashBot`（经 banconn 仪表盘中转）、`initCustomTgBot`（自建 bot）、命令处理（/orders、/close、/status、/disable、/enable、/help、/menu）、`isAuthorized`。 |
| `exc_notify.go` | zap core 异常通知，按作用域聚合、缓存、限流（`TrySendExc`）。 |

### 2.5 llm（5 个非测试文件）

| 文件 | 作用 |
| --- | --- |
| `config.go` | `LLMModelConfig`、`ResolveModels`（继承链、循环检测）、`SubstituteEnvVars`（环境变量替换）、成本计算。 |
| `manager.go`（448 行） | `GetLLMModel`（进程级缓存 `modelCache`）、`createChatModelFromConfig`、`LLMModel.Generate`（信号量 + 超时）、`RecordStatus`（连续失败禁用）、`LLMManager.Call`（按顺序回退）。 |
| `glm.go`（484 行） | 智谱 GLM 的 `model.ToolCallingChatModel` 实现（Generate/Stream/WithTools）。 |

## 3. 核心类型与接口

| 名称 | 位置 | 要点 |
| --- | --- | --- |
| `explicitEntrySession` | `entry/runtime_entry.go:29` | 一次 CLI 执行的会话：`process`、`runSpec`、`exchange`、`storage`、日志、profile。所有配置型命令的装配入口。 |
| `FactorLiveBinding` | `entry/factor_live.go:34` | 账户绑定：`Account`、`Transport`、`Legacy`（Bridge）、`Absence`、`VerifyFunding`。`FactorLiveBindingFactory`（:63）按名称注册于 `factorLiveBindings`（:65，`RegisterFactorLiveBinding` :72）。 |
| `FactorSinkFactory` | `entry/factor.go:16` | 自定义因子输出 sink 的工厂，`runFactorReplayCommand` 使用。 |
| `HubAccountConfig` | `entry/hub_account.go:24` | Hub 内账户的声明配置，由 `execution.HubAccountRegistration` 解码。 |
| `hubFeedHost` | `entry/hub_feed.go:21` | 管理待处理 feed 的准入、时钟观测、关闭与排空。 |
| `BackTest` | `opt/backtest.go:44` | 回测主体：`Init`、`FeedDataSeries`、`RunContext`。`isOpt` 标识优化内部运行。 |
| `BackTestLite` | `opt/backtest.go:34` | 轻量回测（优化/滚动场景复用的精简版本）。 |
| `BacktestFactory` | `opt/runtime_factory.go:12` | 优化器每轮调用的工厂，返回 Runtime 清理函数。 |
| `OptInfo` / `OptGroup` | `opt/common.go:56` / `:47` | 一次优化试验的参数、得分、`BTResult`；一组（策略、方向、标的）的结果集。 |
| `BTResult` | `opt/reports.go:44` | 回测结果汇总（收益、回撤、订单、图表数据）。 |
| `ReportDeps` | `opt/reports.go:104` | 报告所需依赖（配置、钱包、数据库、时钟），由 `NewReportDeps(biz.RuntimeDeps)` 构造。 |
| `PairPicker` | `opt/reports.go:474` | 交易对选择器函数类型；`RegisterPairPicker`/`GetPairPicker` 管理注册表。 |
| `FactorFunc` / `FacArgs` | `opt/tools.go:850` / `:835` | `bt-factor` 工具的因子注册。 |
| `DevServer` | `web/dev/runtime.go:23` | 开发 Web 状态：任务表、运行中的 cmd、WebSocket 客户端、数据工具、按请求解析的 Runtime 工厂（`RuntimeFactory` :52）。 |
| `DevDeps` | `web/dev/runtime.go:58` | 构造 `DevServer` 的依赖集合。 |
| `WsHub` / `WsClient` | `web/base/ws_hub.go:18`、`web/base/websocket.go:15` | 订阅管理（按 exchange/symbol/tf），`Publish(*data.SeriesMsg)` 广播。 |
| `apiHandlers` | `web/live/biz.go:44` | 实盘 API 处理器，绑定 `biz.RuntimeDeps`（钱包、订单、策略任务、配置）。 |
| `apiServer` | `web/live/main.go:31` | 实盘 API 服务器，实现 `ServerLifecycle`（:26）的 Stop/Join。 |
| `AuthClaims` | `web/live/auth.go:300` | JWT 载荷（`user` + 注册声明），HS256 签名。 |
| `Session` | `rpc/session.go:18` | RPC 生命周期 owner，持有渠道列表与 `Session.SendMsg`。 |
| `IWebHook` / `WebHook` | `rpc/webhook.go:178` / `:23` | 渠道接口与通用 Webhook 实现（队列、限速、重试）。 |
| `Telegram` | `rpc/telegram.go:98` | Telegram 渠道（通知、命令、仪表盘模式）。`OrderManagerInterface`（:73）与 `WalletInfoProvider`（:88）由 biz 注入。 |
| `ExcNotify` | `rpc/exc_notify.go:60` | zap core，把异常日志转为通知。 |
| `LLMModelConfig` | `llm/config.go:10` | 模型配置：`APIType`（如 `glm-plan`）、`Name`、`BaseURL`、`APIKey`、`Timeout`、`TimeoutRate`、`Concurrency`、`Payload`、`Temperature`。 |
| `LLMModel` / `LLMManager` | `llm/manager.go:35` / `:263` | 单模型的信号量与统计；多模型按顺序回退的调用器。 |

## 4. 关键流程（调用链）

### 4.1 CLI 启动与分派
1. `entry/main.go:30` `RunCmd` 安装信号处理，调用 `ExecuteContext`（:70）。
2. 若首参数是非帮助/版本的 `-xxx`，`isImplicitWebInvocation`（:87）把请求交给 `web.NewDevCommandWithFactory(newDevWebServer)`，即默认启动开发 Web。
3. `NewRootCommand`（:100）先注册命令组（`commandGroups` 与扩展组），再由 `registerBuiltInCommands`（`common.go:122`）和 `registerExtraCommands` 挂载命令。`AddCommand`/`AddCommandFactory`（`common.go:65/72`）供外部嵌入注册。
4. `normalizeLegacyFlags`（`main.go:321`）把单横线长参数（如 `-datadir`）改写为双横线。

### 4.2 时序/因子统一回测 `backtest`
1. `strategy_commands.go:15` `newStrategyBacktestCommand`：`config.LoadRunSpec` 后判断 `spec.Engines()`。
2. 含 `factor` 引擎：`unifiedFactorBacktestOutputContext`（`unified_backtest.go:86`）→ `validatedFactorBacktestConfigs`（:51）→ `prepareFactorStorageInputs`（`factor_storage.go:146`）→ 写 `config.yml`、`resolved.json`、`events.jsonl`（`O_EXCL`）→ 混合走 `runMixedFactorConfigs`，纯因子走 `runFactorConfigs`（events.jsonl 一侧经 `bufferedEventFile` 1MiB 缓冲，stdout 一侧仍逐事件即时写出——bansite 依赖 stdout 的 decision 事件取进度与起算时间；缓冲在 `finishUnifiedFactorBacktest` 的 `Sync` 中先 flush 再关文件，覆盖正常/错误/panic 展开路径，字节与顺序不变） → `finishUnifiedFactorBacktest`（:195）写 `run.json`，状态为 `complete` 或 `incomplete`。
3. 纯时序：`runExplicitBackTestSpecContext`（`runtime_entry.go:392`）→ 若 `args.Separate` 则按 policy 逐个运行 → `runExplicitBackTestOnce`（:480）→ `session.newRuntime(RunModeBackTest)` → `opt.NewBackTestWithRuntimeDeps`（`backtest.go:456`）→ `BackTest.RunContext`（`backtest.go:831`）。

### 4.3 实盘 `trade`
1. `strategy_commands.go:60` `newStrategyTradeCommand`：`--dry-run` 只对因子/混合有效（改写为 events 模式回放）。
2. 因子：`buildFactorConfigs(spec, runner.Trade)` → `runFactorLiveSpecWithArgs`（`factor_live.go:186`）。
   - `factorLiveUsesHub`（:164）决定走 Hub 还是内嵌 provider；同一机器人混用两者会报错（:176）。
   - Hub 分支：`runFactorHubSpec`（`factor_hub.go:21`）。
   - 内嵌分支：`resolveFactorLiveFactories` → `prepareFactorLiveBindings` → `prepareMixedLiveBinding` → 每账户 goroutine 执行 `runFactorsLiveWithStartup`（:460），组装 `SharedExecutionOptions`、`AccountSink`、`runner.NewLive`。
   - 时序策略账户通过 `runTimeSeriesExecutionAccount` 或 `runMixedLiveTSAccount` 并入同一进程。
3. 纯时序：prod/dry_run 且声明了 `execution` 时走 `runTimeSeriesExecution`（`time_series_execution.go:105`）；否则进入旧的 `runExplicitTradeSpecContext` 分支（`runtime_entry.go:535`），通过 `live.NewCryptoTraderWithRuntimeDeps` 启动，并在 prod/dry_run 下直接报错（旧的 live sender 已禁用）。

### 4.4 Hub 守护进程 `hub`（别名 `spider`）
1. `runHubWith`（`hub.go:52`）→ `openExplicitEntrySession` → `publicHubSnapshot`（:34）：仅允许 prod/dry_run，清空 accounts、exchange.items、run_policy。
2. 以 Data 模式创建公共 Runtime，`spiderExchangeFactory` 生成子 Runtime 工厂。
3. `execution.NewHubWithRecovery` 恢复账户状态（`accounts.json`），`ListenHubUnix` 监听私有 socket（默认 `<datadir>/execution/hub.sock`），`feeds.start` 接入 spider 的闭合 K 线事件。
4. `data.RunLiveSpiderWithRuntimeDeps` 对外提供行情（默认 `127.0.0.1:6789`）。

### 4.5 超参优化 `optimize` / `bt-opt`
1. `RunOptimize`（`hyper_opt.go:316`）→ `runOptimize`：`prepareOptimizeData` 除非 `BANBOT_OPT_DATA_PREPARED=1`。
2. 策略组 ≤1 或 `concur ≤ 1`：进程内逐组 `optAndPrint`。否则 `utils.ParallelRun` 并发调用 `os.Executable()` 重入子进程，子进程配置写入 `os.CreateTemp`（:387），子进程设置 `BANBOT_OPT_DATA_PREPARED=1`。
3. `optAndPrint` → `optForGroup` → 采样：`bayes` 走 `runBayes`（:911），其余走 `runGOptuna`（:854，支持 random/cmaes/ipop-cmaes/bipop-cmaes/tpe）。每个 trial 通过工厂构造临时 `BackTest` 并计分。
4. 结束后 `calcBestBy` 选出最优，写回 `pol.Params`，最终 `collectOptLogWithHints` 汇总。

### 4.6 Web 开发回测任务
1. 前端 `POST run_backtest` → 任务写入 `ormu`（`ormu.Task`，含 `Args`）。
2. `startBtTaskScheduler`（`dev/common.go:342`）→ `executeBtTask`（:87）启动子进程 `os.Executable() backtest <task.Args>`（:104），环境变量 `BanDataDir`、`BanStratDir`。
3. `runBtCommand` 解析 `uiPrg` 进度；结束后 `updateBtTaskResult` 与 `collectBtResults`（:393）读取 `report` 目录（统一因子报告通过 `collectUnifiedBtTask`）。

### 4.7 实盘 API
1. `startAPIServer`（`live/main.go:156`）：`recover`、CORS（`CORSOrigins`，:164）、`/api/kline`、`/api/csv`、`/api/ws` 直接注册（无鉴权），`/api/bot` 挂载 `authMiddleware`（JWT，`X-Authorization: Bearer`），`/api` 挂载公开接口（`/login`、`/ping`、`/strat_call`），最后挂静态资源。
2. 登录：`postLogin`（`auth.go:187`）比较用户名与明文密码，检查 `AllowIPs`，签发 HS256 token（默认 168 小时）。
3. 账户范围：`wrapAccount`（`biz.go:239`）读取 `X-Account` 头，并与 JWT 中的 `AccRoles` 比对。

### 4.8 通知
1. 业务调用 `rpc.SendMsg`（`notify.go:217`）：若未就绪则 `InitRPC`；用 `webhook[msgType]` 模板渲染 payload（`FormatWithMap`），写入 `msg["name"]`。
2. 每个渠道的 `WebHook.SendMsg`（`webhook.go:236`）按过滤条件入队；`doSend`（:357）按 `min_intv_secs`/`retry_delay` 节流，失败消息保留重试。
3. Telegram 有两种模式：仪表盘模式（`initDashBot`，`telegram.go:215`，通过 banconn 中转 `telegram_cmd`）与自建 bot 模式（`initCustomTgBot`，`:389`）。
4. 异常日志：`ExcNotify`（`exc_notify.go`）作为 zap core 挂接，按 key 聚合后延迟发送（`sendExcAfter`）。

### 4.9 前端静态资源
`ServeStaticAt`（`web/ui/run.go:29`）：`uidist/index.html` 不存在或 `version.txt` 与 `core.UIVersion` 不一致时，调用 `downNewUI`（:58）下载 `releases/download/{UIVersion}/dist.zip`（简体中文环境优先 Gitee），解压到 `uidist`。

## 5. 配置项与外部依赖

### 5.1 RunSpec / 配置键（与本模块直接相关）
- `run_policy[].engine`：`time_series` 或 `factor`（`config/unified.go:26-27`）。
- `execution.mode`：`weights` 或 `events`，因子回测默认 `events`（`unified_backtest.go:20`）。
- `execution.live_provider`：`banexg` / `hub` / 空值走 Hub；其他值走注册的内嵌绑定（`factor_live.go:164`）。
- `execution.sender_lease_dir`、`execution.store_path`（`time_series_execution.go` 使用）。
- `data.pit_policy`：归档的可见性策略（`factor_config.go:269`，默认 `available-at`，`:389`）。
- `api_server.bind_ip`、`port`、`jwt_secret_key`、`users[].{username,password,allow_ips,acc_roles,expire_hours}`、`cors_origins`（`config/types.go:207-210` 等）。
- `rpc_channels.*`：`type`、`msg_types`、`accounts`、`keywords`、`retry_delay`、`min_intv_secs`、`disable`、`proxy`；Telegram 另有 `token`、`chat_id`。
- `llm_models.*`：`api_type`（`glm-plan` 或 OpenAI 兼容）、`name`、`base_url`、`api_key`、`timeout`、`timeout_rate`、`concurrency`、`temperature`、`payload`。

### 5.2 CLI 参数（节选）
- 通用：`--datadir`、`--config`（可多次）、`--config-data`、`--no-default`、`--logfile`、`--level`、`--max-pool-size`、`--dlock`、`--cpu-profile`、`--mem-profile`（:6060）、`--net-off`。
- `optimize` / `bt-opt`：`--sampler`（默认 `bayes`，`common.go:305`）、`--opt-rounds`、`--concur`、`--picker`、`--each-pairs`、`--review-period`、`--run-period`、`--alpha`、`--pair-picker`。
- `web`：`--host`（默认 `127.0.0.1`，Docker 下 `0.0.0.0`，`dev/main.go:63-65`）、`--port`（8000）、`--password`（basic auth，用户名固定 `banbot`，`dev/main.go:119`）。
- `hub`：`--execution-socket`。
- 因子与时序：`backtest --mode weights|events`、`trade --dry-run --live-provider`。

### 5.3 环境变量
- `BanDataDir`、`BanStratDir`：Web 回测子进程继承（`dev/common.go:105-106`）。
- `BANBOT_OPT_DATA_PREPARED`：优化子进程标记（`hyper_opt.go`）。

### 5.4 外部服务
- banbot 官网 token 接口 `https://www.banbot.site/api/banconn/token`（`rpc/telegram.go:343`），用于 Telegram 仪表盘模式。
- 发布资源 GitHub / Gitee `dist.zip`（`web/ui/run.go`）。
- Telegram Bot API、企业微信 API、SMTP、通用 Webhook。
- 交易所（经 banexg，间接经 biz/execution 使用）。
- `go build`（开发 Web 的构建接口，`dev/api_dev.go:700`）与 `go tool dist list`（`:2072`）需要本机 Go 工具链。

## 6. 与其他模块的交互边界

- `entry` → `config`（RunSpec、Snapshot）、`runtime`（Process、Runtime）、`biz`、`strat`、`data`、`live`、`opt`、`execution`、`factor/runner`、`web`、`rpc`。`entry` 是唯一做装配的地方，下游组件通过 `RuntimeDeps` 接收依赖。
- `opt` → `biz`（订单管理、钱包）、`strat`（任务与策略状态）、`data`（第三方序列）、`orm/ormo`（订单）、`config`。`opt` 通过 `BacktestFactory` 与 `entry` 解耦。
- `web/dev` → `opt`（以子进程方式运行回测）、`orm/ormu`（任务、报告）、`config`、`data`（`RuntimeDeps`）。
- `web/live` → `biz.RuntimeDeps`、`strat`、`orm/ormo`、`exg`（下载交易记录）、`base`（K 线与 WebSocket 复用）。
- `rpc` → `config`、`biz`（通过 `SetOrderManager`/`SetWalletInfoProvider` 注入）、`utils/banio`（仪表盘 ClientIO）、`btime`。
- `llm` → `config`（`config.GetLLMModel`）；未被其他业务模块调用。
- 包级全局状态（属 legacy/进程级注册表）：`rpc` 的 `channels`/`rpcReady`/`rpcWebhook`、`llm` 的 `modelCache`、`opt` 的 `RegisterPairPicker`/`RegisterFactor` 注册表、`entry` 的 `commandGroups`/`extraCommands`/`factorLiveBindings`。
- 边界规范：`AGENTS.md` 要求交易所差异全部在 banexg 内处理。本模块仅发现 `web/base/indicators.go:256` 硬编码 `"binance"`（见第 8 节）。

## 7. 扩展与修改指南

### 7.1 新增 CLI 命令
- 在 `entry` 中用 `newRuntimeConfigCommand` / `newConfigCommandContext` 构造命令，用 `bind*` 绑定参数，并在 `registerBuiltInCommands`（`common.go:122`）中挂载；或在嵌入场景用 `AddCommand`/`AddCommandFactory`。
- 配置型命令应通过 `openExplicitEntrySession*` 装配，不要直接调用全局配置。
- 别名用 `withAliases`。注意 `cmp_orders`、`collect_opt` 等别名挂在 `tool` 组下，不会出现在根命令。
- 若新增长参数，`normalizeLegacyFlags` 会自动识别；修改时注意不要引入与已有长参数同名的单横线输入。

### 7.2 新增或替换因子实盘 provider
- 用 `RegisterFactorLiveBinding(name, factory)`（`factor_live.go:72`）注册，factory 必须返回完整的 `FactorLiveBinding`（账户、传输、Bridge、Absence、funding 校验）。
- 时序账户复用同一绑定结构（`prepareMixedLiveBinding`），新增字段需同时考虑 `runTimeSeriesExecutionAccount` 的校验（`time_series_execution.go:160` 起）。

### 7.3 新增优化采样器
- 在 `runGOptuna`（`hyper_opt.go:854`）增加分支；`bindSampler` 的帮助文本（`common.go:305`）和 `doc/usage.md` 同步更新。
- 不要用 `panic` 处理非法名称。

### 7.4 新增回测报告指标
- 在 `BTResult`（`reports.go:44`）中增加字段，由 `ReportDeps`（`:104`）提供数据；不要引入包级全局状态。
- 若影响 `run.json` 或报告结构，同步检查 `web/dev/common.go` 的结果收集与前端读取。

### 7.5 新增开发 Web API
- 在 `DevServer.RegAPI`（`api_dev.go:49`）登记路由。
- 处理用户路径时必须做包含校验：参考 `common.go` 中的 `pathWithin`（:734）与 `filepath.IsLocal`（:580）。`parsePath`/`handleFileOp` 等文件操作入口必须经过该校验，禁止路径穿越与符号链接逃逸。

### 7.6 新增实盘 API
- 账户维度接口放在 `regApiBiz` 下并使用 `wrapAccount`；公开接口只放 `regApiPub`（`auth.go:32`）。
- 不要把需要登录的接口挂到 `/api/kline`、`/api/csv`、`/api/ws` 这些无鉴权分组下。

### 7.7 新增通知渠道
- 实现 `IWebHook`（`webhook.go:178`），在 `buildChannels`（`notify.go:123`）中按 `type` 分支创建。
- 字段解析参考 `webHookItem`（`webhook.go:152`）。模板变量由 `FormatWithMap` 渲染，`msg` 中的 `account`、`type`、`name` 可用。

### 7.8 修改前端 web/ui
- 前端修改后需编译打包，并更新 `core/data.go` 中的 `UIVersion`（AGENTS 通用要求），否则已部署的 `uidist` 不会被替换。
- 前端 API 请求集中在 `src/lib/netio.ts`；路由对应关系见 `doc/help_ui.md`。

### 7.9 新增 LLM 提供方
- 在 `createChatModelFromConfig`（`llm/manager.go:118`）按 `api_type` 分支；非 OpenAI 兼容协议参考 `glm.go` 实现 `model.ToolCallingChatModel`。
- 注意 `RecordStatus` 的连续失败禁用逻辑。

### 7.10 易踩坑
- 发布版本使用 `git tag`，先修改 `core/data.go` 的 `Version`。
- 优化与回测子进程依赖 `os.Executable()` 重入。修改 `optimizeChildBaseArgs` 时要保留 `-out`、`-config` 等必要参数。
- Web 回测任务通过 `strings.Split(task.Args, " ")` 构造子进程参数（`dev/common.go:104`），路径中不能含空格。
- QuestDB WAL 为写后读模型。`entry/series.go:167` 已使用 `orm.WaitForSeriesCoverageVisible` 等待可见性，新增“写入后立即判断/删除/重命名”的逻辑必须同样等待。
- 实盘路径不应绕过 `OrderMgr` 与 `execution.SharedAccount`，否则会破坏账户归属与对账。

## 8. 已知限制与设计取舍

- `/api/kline`、`/api/csv/upload`、`/api/ws` 位于 JWT 鉴权分组之外（内置 UI 不携带认证头）。部署时应绑定 `127.0.0.1` 或在反向代理处认证；不要把需要登录的接口挂到这些分组下。
- Telegram 授权依赖上游通道，没有 banbot 内部白名单。
- `web/base/indicators.go` 硬编码 binance，是“banbot 内不写交易所分支”的已知例外。
- `llm` 的 `NewLLMManager`/`LLMManager.Call` 在仓库内无非测试调用，仅 `config` 引用其配置类型，保留作外部嵌入。
- 包级全局状态：`rpc` 的 `channels`/`rpcReady`/`rpcWebhook`、`llm` 的 `modelCache`、`opt` 的注册表、`entry` 的 `commandGroups`/`extraCommands`/`factorLiveBindings`。
- 在 macOS 上临时目录经 `/var`→`/private/var` 符号链接，路径比较类测试需先做 `EvalSymlinks`。
