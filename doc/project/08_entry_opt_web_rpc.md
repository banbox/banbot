# 08 应用编排与接口模块：entry / opt / web / rpc / llm

> 基于当前源码（分支 main，`core.Version = v0.8.0`，`core.UIVersion = v0.7.9`）梳理。文中 `文件:行号` 均经过核对。
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
| | `time_series_facade.go` | 内嵌时序机器人（恒用 Hub 订单管理器）的装配：`tsHubLocalBridge`（构造 `LocalAccount` 的 `HubOrderBridgeConfig`，journal 为 `<数据目录>/execution/robots/ts-local-<hash>.json`，hash 取（会话身份/账户/结算域/排序后策略 ID））。`take_over_strat` 受支持（`HubOrderMgr` 渲染接管 lot，`biz/hub_order_takeover.go`）。 |
| | `time_series_hub.go` | 时序执行经 Hub 的分支（`timeSeriesUsesHub`、`runTimeSeriesHubSpec`）。 |
| | `series.go` | `series down` / `series list`，第三方时序覆盖范围补齐。 |
| 因子路径 | `factor_config.go` | RunSpec → `[]runner.Config`（`buildFactorConfigs`），含 archive 身份推导、价格推导、账户权重。 |
| | `factor_live.go` | 因子实盘：`FactorLiveBinding`、绑定工厂注册表、`runFactorLiveSpecWithArgs`、`runFactorsLiveWithStartup`（组装 SharedAccount、AccountSink、runner.Live）。 |
| | `factor_banexg_live.go` | 基于 banexg 的默认因子实盘绑定与 funding 能力探测；因子 funding 数据源每分钟按账户整体轮询一次（一次 `FetchFundingCash` 覆盖全部订阅品种），瞬时场所读错误（`execution.TransientVenueReadError`）保留游标、下次轮询重试而不终止数据源。 |
| | `embedded_banexg_binding.go` | 导出 `NewBanexgLiveBinding`：stock banexg 账户绑定。 |
| | `factor_storage.go`、`factor_history.go`、`factor_resolved.go`、`factor_archive_types.go` | 存储输入装配（K 线/内置多周期时序源/归档输入、`ScopedInputs`/`RefSymbols` 参考标的预加载、`AsOfField` 与同网格 `Field` 起始边界对齐，以及启用滚动品种池重排时自动为候选超集订阅决策 K 线的 `close`/`volume`/`quote` 及重排回看预热根数）、历史路径校验、解析后配置输出（含 `rolling_universe`）、归档字段类型恢复。 |
| | `factor_hub.go`、`factor.go`、`factor_expression_commands.go`、`source_plan.go`、`execution_config.go`、`execution_takeover.go` | Hub 账户分组执行、归档命令、表达式子命令、数据源计划选项、账户执行字段、执行接管所有者。 |
| 混合策略 | `mixed_factor_backtest.go` | 因子 + 时序混合回测回放（`replayMixedEngines`，事件模式）。 |
| | `mixed_factor_live.go` | 混合实盘：时序策略通过 `prepareMixedLiveBinding` 挂到因子账户的 SharedAccount，经 `HubOrderMgr`（进程内 Hub）交易。 |
| | `strategy_commands.go` | `backtest`、`trade`、`research` 三个统一子命令的 RunSpec 分派。 |
| | `ui_preflight.go`、`factor_expression_commands.go` | Web 回测预检（`InspectBacktestRunSpec`）、表达式命令。 |
| Hub 守护进程 | `hub.go` | `hub` 命令（别名 `spider`）：公共行情 + 私有账户执行 socket。 |
| | `execution_robot.go` | execution 路径机器人共用的启动辅助：`startExecutionRobotCrons`、`hourOnlySymbolsFromSubs`、`ignoredExecutionConfigKeys`/`warnIgnoredExecutionConfig`、`rejectExecutionDump`、`verifyEmbeddedAccounts`。 |
| | `runtime_notify.go` | `attachRuntimeNotifications`：execution 机器人的通知会话装配（后台启动 rpc 渠道、异常告警 core 并入 runtime logger、返回关停时发送 `bot stop` 的函数）。 |
| | `hub_account_check.go` | `verifyHubProductionAccount`：Hub 生产账户注册前的账户体检。 |
| | `hub_account.go`、`hub_feed.go`、`hub_host_manifest.go`、`hub_registration.go`、`hub_robot_config.go` | 账户注册与解码、待处理 feed 的准入与观测、宿主声明清单、机器人侧 Hub 配置与公共快照。 |

### 2.2 opt（10 个非测试文件）

| 文件 | 作用 |
| --- | --- |
| `backtest.go`（1438 行） | `BackTest` 与 `BackTestLite`：`Init`、`FeedDataSeries`、`RunContext`（主循环、清仓、结果收集）、第三方序列补齐、订单回调、清算。 |
| `hyper_opt.go`（1452 行） | 超参优化：`RunOptimize`、`runOptimize`（进程内或子进程并行）、`optAndPrint`、`optForGroup`、`runBayes`、`runGOptuna`、滚动回测 `RunRollBTPicker`/`RunBTOverOpt`、日志收集 `CollectOptLog`。 |
| `common.go`（574 行） | `OptInfo`、`OptGroup`、`ValItem`、`rollBtOpt`，策略参数 `ToPol`，图表 `Chart`/`ChartDs`，`DescGroups`、`AvgGoodDesc`。 |
| `reports.go`（2677 行） | `BTResult`、`ReportDeps`、`PairPicker` 注册表、报告/图表生成、订单统计；`BTResult` 的 `dumpDetail` 先写 `detail.json.tmp` 再改名并返回 `*errs.Error`（`MarshalFail`/`IOWriteFail`），`dumpBtFiles`/`printBtResult` 向上传递，最终回测写不出 detail.json 时命令以非 0 退出（NaN/Inf 指标由 sonic 序列化为 null，不报错）；`BTResult` 含 `DelistExits`/`GapExits`（退市/数据空洞强平单数）、`GapExitPnlShare`/`DelistExitPnlShare`（对应强平单 Σ|利润| 占全部订单 Σ|利润| 的比例）与 `dailyEquity`（`[UTC 日末 ms, 权益]`，逐日、不抽样，随 Ulcer 日权益采集；`Collect` 的临时尾行在下一次 `recordUlcer` 前被撤掉，日键严格递增；5 分钟状态转储（`cronDumpBtStatus` 定时 goroutine）与喂入 goroutine 的 `logState`/`applyLiquidation`/`Collect` 由 `BTResult.stateMu` 串行）；**爆仓契约**（BtFeatureLevel 10）：`BackTestLite.onLiquidation`（`charge_on_bomb` 关闭）只记录 `liquidation`（时刻/品种/原 BOMB 文本）并终止数据循环，不取消运行时 context；`BackTest.RunContext` 照常清仓（`bot_stop` 平仓单改标 `liquidation`），再由 `BTResult.applyLiquidation` 把爆仓时刻到窗口末的权益记为 0（曲线按爆仓前采样密度补零样本、`dailyEquity` 逐日补 0、`minReal` 0、`maxDrawDownPct` 100），`Collect` 令 `totProfit=-totalInvest`、`totProfitPct=-100`、`finBalance=0`；Ulcer 累加器 `AllowZero` 使 0 权益按 −100% 回撤计、CAGR −1（UPI = −100/UI，有限）；`detail.json` 仅爆仓时出现 `liquidated:true`、`liquidated_at_ms`、`liquidated_symbol`、`liquidated_reason`；`out.log` 指标表多一行 `Liquidated`。优化试验与普通回测走同一条路径，同参同区间指标一致。组合引擎（factor/statarb）的 NAV≤0 见下方破产段与 07_factor_engine；`orders.csv` 末列 `funding` 为订单资金费（真实 `funding_rate` 双向结算或 `bt_funding_apr` 近似值），`exitFee` 不含该部分。 **破产（BtFeatureLevel 12，issue 162）**：`BackTestLite.checkEquityDepleted`（订单变动或新 bar 时间后，在 `FeedDataSeries` 末尾；`charge_on_bomb` 时不判）按 `BanWallets.EquityWithOrders`（余额 + 未平仓订单当下浮盈）≤ 0 记 `liquidation{cause: equity_depleted}`，之后 `FeedDataSeries` 直接返回 false（不再开仓）；`checkFinalBalance` 在收尾平仓后余额仍 ≤ 0 同样记破产；强平路径的原因为 `margin_liquidation`；`BTResult.LiquidatedCause` 输出 `liquidated_cause`；`applyLiquidation` 在 Ulcer 累加器因爆仓前日收盘 ≤ 0 失效时按 `DailyEquity` 重建（≤0 记 0）；`BTResult.equityNow`（曲线、Ulcer）与破产判定共用 `BanWallets.EquityWithOrders`（默认；`bt_fresh_equity: false` 时曲线回到缓存浮盈口径，破产判定不变），该函数仅对合约市场叠加订单浮盈，现货 base 币已在钱包估值里。 |
| `equity_replay.go` | 离线权益重放（纯函数，不改回测行为、不升 `BtFeatureLevel`）：`ReplayEquity(ReplayParams)` 由已平仓订单列表（`ReplayOrder`：品种/周期/方向/入场均价/成交量/入场费/利润/e_create/e_update/x_update）、初始资金、起止时间、时间步 `StepMS`（订单周期的基础周期：4h/8h→1h、3d→1d，`ReplayBaseTF`）、图表采样时刻 `PlotTimes`、取价回调 `ReplayPriceFunc`（基础周期 K 线收盘）逐步重放 `bt_fresh_equity` 口径的权益：权益 = 初始资金 + 已平仓利润 + Σ 持仓订单 Profit；持仓 Profit 只在订单自身周期 bar 收盘刷新（入场后首次收盘前为 0），开仓费在余额与 Profit 各计一次；每步在首个事件后取样，`FirstSymbols`（单品种回测为该品种，多品种为空）之外的品种本步事件尚未处理（入场/出场不计、价格取上一根）；期末 = 初始资金 + 全部利润。指标复用引擎函数：日权益与 Ulcer 经 `BTResult.recordUlcer/collectUlcer`，回撤 `utils.CalcDrawDown`（逐步 → `MaxDrawDownPct`，图表样本 → `ShowDrawDownPct`），Sharpe/Sortino `CalcMeasuresByReal`。重放权益 ≤0 返回 `NeedsRerun`（该回测订单本身会变，不给指标）。对 57 个引擎实跑真值：UPI 绝对误差 p50 0.0002、p90 0.0065、最大 0.086。bansite `backfill-metrics --rebuild-equity` 使用它。 |
| `tools.go`（1894 行） | 订单对比（`CompareExgBTOrdersWithRuntimeDeps`，旧版入口未接入 CLI）、`FacArgs`/`FactorFunc` 因子注册、`BtFactors`、`BuildBtResult`、资产报告合并、结果压缩（`ZipBacktestResult` 超过 `maxFileSize` 的文件一律略过；`ZipBacktestResultStrict` 供回传站点：`detail.json` 必须在场、不受 `maxFileSize` 约束而受 `maxDetailSize` 约束，缺失/超限/读写失败返回错误且不清理目录）。 |
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
| `exc_notify_sender.go` | `NewExcNotifyWithSender`：把聚合后的告警交给回调而非 `Session`（execution 机器人传入非阻塞队列，渠道故障不影响日志与交易）。 |

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
1. `entry/main.go:30` `RunCmd` 安装信号处理，调用 `ExecuteContext`。命令返回错误（含策略构造参数约束违反透传的 `core.ErrBadConfig`）或未捕获的 `*config.BadParamError` 统一经 `writeCommandError` 向 `os.Stderr` 输出 `Error: <err>`（参数约束错误即 `Error: [BadConfig] <msg>`）并以退出码 1 结束；真正的运行时 panic 则输出 `banbot panic raw stack:`。`ExecuteContext` 仅将未捕获的 `*config.BadParamError` 转为 `*errs.Error` 返回，其余 panic（含 `runtime.Error`）保持原样抛出。
2. 若首参数是非帮助/版本的 `-xxx`，`isImplicitWebInvocation` 把请求交给 `web.NewDevCommandWithFactory(newDevWebServer)`，即默认启动开发 Web。
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
3. 纯时序：prod/dry_run 且声明了 `execution` 时走 `runTimeSeriesExecution`；prod/dry_run 缺少声明时报错（旧的 live sender 已删除）。`runExplicitTradeSpecContext` 与 `runFactorLiveSpecWithArgs` 开头调用 `rejectUnsupportedLiveEnv`：`env: test` 报 `testnet live trading is not supported by the execution path; use env: dry_run or prod`，`market_type` 为 spot/margin/inverse 报 `market_type … is not supported by the execution path`。`runMixedLiveTSAccount` 对非 prod 环境同样报错。

#### execution 路径机器人（时序 Hub/内嵌、因子、混合）的启动与运行辅助
1. 启动前：`rejectExecutionDump` 拒绝 `--out`（K 线/API dump 只被已删除的旧实盘栈消费，接受它只会写出空文件）；`warnIgnoredExecutionConfig` 对 `ignoredExecutionConfigKeys` 列出的键各记一条告警，不使启动失败（现有配置常带这些键）。
2. 被忽略的键（仅当值被用户改动时报告，零值与加载器填入的默认值不报）：`pairlists`（仅当旧路径下它会选品，即没有静态 `pairs`（根级或任一 `run_policy`）或 `pairmgr.force_filters` 为真）、`pairmgr.cron`、`min_open_rate`（≠0.5）、`charge_on_bomb`、`margin_add_rate`（≠0.66）、`put_limit_secs`（≠180）、`odbook_ttl`（≠500）、`account_pull_secs`（≠60）、`bt_in_live`（`cron` 非空）。它们属于已删除的旧钱包、限价摆放与选币机制，完整清单见 `05_biz_orders_live.md`「支持矩阵」。
3. 账户体检：内嵌 provider 的机器人在 `verifyEmbeddedAccounts` 失败时终止启动；Hub 机器人没有凭据，体检在 Hub 侧账户注册时进行（见 `05_biz_orders_live.md`）。
4. 通知：`attachRuntimeNotifications` 在 runtime 建立后后台启动 rpc 渠道（使 Telegram 命令从启动起可用）并把异常告警 core 并入 logger，`NotifyStarted` 发启动状态消息，返回的函数须在 runtime 的 Close/Join defer 之后 defer（先执行），以 3 秒上限等待 `bot stop` 消息入会话。通知前端是 `runtime.Notifier`：容量 256 的队列，满则丢弃并告警，单 worker 独占对 rpc `Session` 的全部调用（含可能慢或 panic 的渠道启动）；渠道启动失败只告警并禁用通知。成交通知由订单管理器回调 `Runtime.OrderNotifyCallback` 构建（`biz.NewOrderMessage`）并入队，非阻塞，故在管理器锁下调用也安全。
5. 多账户：每个账户有自己的 runtime 配置；`mixedLiveAccountConfig` 经 `config.CopyNotifications` 带入通知段，`type: telegram` 渠道（一个 bot token 在一个进程内只能有一个命令监听）只保留给「第一个可交易账户」（`NoTrade` 之外按账户名排序最小者；只有一个账户时即该账户），其他账户去掉并告警；wework、mail 等仅出站渠道对每个账户都启用。
6. 维护任务：`startExecutionRobotCrons`（见 `05_biz_orders_live.md`「execution 路径的维护任务」）。同时把 `rt.SetSeriesObserver` 接到 `(*CryptoTrader).PublishSeries`（仪表盘实时图表；无 API server 时空操作）和延迟监控的收数状态。

### 4.4 Hub 守护进程 `hub`（别名 `spider`）
1. `runHubWith`（`hub.go:52`）→ `openExplicitEntrySession` → `publicHubSnapshot`（:34）：仅允许 prod/dry_run，清空 accounts、exchange.items、run_policy。
2. 以 Data 模式创建公共 Runtime，`spiderExchangeFactory` 生成子 Runtime 工厂。
3. `execution.NewHubWithRecovery` 恢复账户状态（`accounts.json`），`ListenHubUnix` 监听私有 socket（默认 `<datadir>/execution/hub.sock`），`feeds.start` 接入 spider 的闭合 K 线事件。
4. `data.RunLiveSpiderWithRuntimeDeps` 对外提供行情（默认 `127.0.0.1:6789`）。`RunForever` 阻塞在 Accept，运行时 context 取消（`entry/main.go:installSignalHandler` 捕获 SIGINT/SIGTERM）时由 `context.AfterFunc` 调用 `spider.Stop`，进程才会退出；退出时 `HubServer.Close` 删除私有 socket。`trade` 经同一信号处理器取消 context。

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
3. execution 机器人（时序、因子、混合）只在恰有一个账户时启动本 API（`accountDashboardConfig`）；鉴权头为 `X-Authorization: Bearer <token>`，并需 `X-Account`。
4. 账户范围：`wrapAccount`（`biz.go:239`）读取 `X-Account` 头，并与 JWT 中的 `AccRoles` 比对。

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
- `execution.ts_facade` 已移除：键仍在 `config/shallow.go` 的 `accountExecutionKeys` 白名单中，仅为让 `config/advanced.go:validateAdvanced` 在 `execution` 或 `accounts.<名>` 下出现时（任何取值）报错 `... was removed: the shared order facade no longer exists ... delete this key`，而不是通用的未知字段错误。`runTimeSeriesExecutionAccount`（`time_series_execution.go`）对内嵌生产时序机器人（`env: prod` 且 `live_provider` 为注册的自定义内嵌名，非空、非 `banexg`/`hub`；混合机器人的时序账户经 `runMixedLiveTSAccount` 同样走此函数）恒走 Hub 门面：`BorrowAccount` 取得账户 → `execution.NewEmbeddedHub(ctx, 账户, 1s)` → `tsHubLocalBridge` → 以 `HubOrderBridge`（`LocalAccount`）构造 runtime（不传 `SharedOrderBridge`）；`EmbeddedHub` 在 runtime 关闭之后关闭，账户仍由原借用者/构建者关闭。`SharedOrderMgr` 只用于回测（`mixed_factor_backtest.go`），实盘不再构造（`runtime.Options.SharedOrderBridge` 仅 `Mode=backtest` 被接受）。同账户混合因子+时序的内嵌机器人（`runFactorsLiveWithStartup`，`factor_live.go`）对时序部分走同一 Hub 门面：先 `BorrowAccount` + `NewEmbeddedHub` + `tsHubLocalBridge`，runtime 以 `SharedExecution` + `HubOrderBridge` 构造；因子 runner 仍直接使用 runtime 借用的同一账户（策略 ID 与 TS 桥不同）。账户报价源保持新鲜：桥的 `QuoteContext` 是 `adapter.Observe`，runtime 以它注册账户报价（首次注册生效），**不**套 `execution.ReuseRecentQuotes`，因为因子 runner 经 `account.VisibleQuote` 要求报价观测时间不早于其可执行时间（`factor/runner/account_sink.go`）；只有纯时序账户（`runTimeSeriesExecutionAccount`）使用复用包装。默认 socket Hub 下的混合账户（`runFactorHubAccount`）经 `robotTSHubBridge` 让时序部分走 Hub 机器人桥，与纯时序 Hub 机器人一致。从旧共享门面升级的账户在 journal 不存在且 `__legacy_bridge` 检查点有活动订单时，`NewHubOrderMgr` 自动一次性导入（`biz/hub_order_legacy_import.go`，混合账户同样适用，旧数据保持不动，不可回滚到旧门面）；导入被拒绝时构造器 panic，由 `bindTimeSeriesJobs`（纯时序与混合账户都经它绑定 job）转为该账户的启动错误。runtime 自行停止（`rt.Context().Done()`）即结束账户循环，管理器有错误时同时冻结账户。
- `execution.sender_lease_dir`、`execution.store`（`time_series_execution.go` 预检使用；`env: dry_run` 同样要求二者，缺失即报 `requires a durable live ledger`）。相对路径相对于声明该键的配置文件所在目录解析（`RunSpec.ResolvePath`；`$`/`@` 前缀相对数据目录）。
- `data.pit_policy`：归档的可见性策略（`factor_config.go:269`，默认 `available-at`，`:389`）。
- `api_server.bind_ip`、`port`、`jwt_secret_key`、`users[].{username,password,allow_ips,acc_roles,expire_hours}`、`cors_origins`（`config/types.go:207-210` 等）。
- `rpc_channels.*`：`type`、`msg_types`、`accounts`、`keywords`、`retry_delay`、`min_intv_secs`、`disable`、`proxy`；Telegram 另有 `token`、`chat_id`。
- `llm_models.*`：`api_type`（`glm-plan` 或 OpenAI 兼容）、`name`、`base_url`、`api_key`、`timeout`、`timeout_rate`、`concurrency`、`temperature`、`payload`。

### 5.2 CLI 参数（节选）
- 通用：`--datadir`、`--config`（可多次）、`--config-data`、`--no-default`、`--logfile`、`--level`、`--max-pool-size`、`--dlock`、`--cpu-profile`、`--mem-profile`（:6060）、`--net-off`。
- `optimize` / `bt-opt`：`--sampler`（默认 `bayes`，`common.go:305`）、`--opt-rounds`、`--concur`、`--picker`、`--each-pairs`、`--review-period`、`--run-period`、`--alpha`、`--pair-picker`。
- `web`：`--host`（默认 `127.0.0.1`，Docker 下 `0.0.0.0`，`dev/main.go:63-65`）、`--port`（8000）、`--password`（basic auth，用户名固定 `banbot`，`dev/main.go:119`）。
- `hub`：`--execution-socket`（绝对路径，受 Unix socket 路径长度限制，见 `06_execution.md`）。
- `hub` 与含时序策略的 `trade` 的配置必须含 `time_start`：`applyUnifiedArguments`（`config/run_spec.go`）在 `run_policy` 为空或含时序策略时调用 `Root.Apply`，缺少 `time_start`/`timerange` 时返回 `` `time_start` in yml is required ``。
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
- 只需 stock SDK 能力时，用自定义名注册 `entry.NewBanexgLiveBinding`（`embedded_banexg_binding.go`）并设 `execution.live_provider: <名>`，账户即走内嵌执行路径。内嵌入口（`time_series_execution.go`）：拒绝已声明的 `leverage`（须在交易所侧设置）；Bridge `QuoteContext` 与账户注册的报价源均经 `execution.ReuseRecentQuotes(adapter.Observe, EmbeddedQuoteReuse)` 包装（1 秒内同品种复用），因子路径仍抓取新报价；入口自身不再轮询资金费，由账户私有报告 worker 恢复（见 `06_execution.md`）。
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
- **`*errs.Error` 与 `error` 不可混用**（仓库级门禁 `scripts/typednil_test.go` 见 `01_foundation.md`「常见易踩坑」；新增违规必须修复或登记白名单）：`rt.NewFactorLiveProvider`、`live.NewCryptoTraderWithRuntimeDeps`、`trader.StartWebAPI` 返回 `*errs.Error`。把它的结果赋给 `error` 类型变量（例如函数里已有的 `err`）会把 nil 指针变成非 nil 的 `error` 接口，机器人在启动后立即以空错误退出。必须用专用变量（如 `providerErr`、`traderErr`、`apiErr`）接收；`entry/typed_nil_error_test.go` 对 `time_series_hub.go`、`time_series_execution.go` 中 `NewFactorLiveProvider` 的赋值做了静态检查。
- 时序机器人的运行循环（`runTimeSeriesHubAccount`）返回前会记录原因告警 `time-series robot stopping: ...`：`entry context ended`（入口 context 取消）、`runtime ended`（runtime 结束，带 `hubRobotRuntimeError`）或 `market data subscription ended`（行情订阅出错）。取消会被上游当作正常停止，排查退出原因看这条日志。

## 8. 已知限制与设计取舍

- dry_run 的 paper 成本由 `entry/hub_robot_config.go:registerRobotHubAccount` 写入账户注册：纯时序账户（全部 `runner.Config.StrategyID` 为空）在 `Manifest.Costs.FeeRate` 为 0 时取 `configuredTakerFee`（`exchange.<name>.fees.<market>.taker`），未配置则为 0 并告警；含因子策略的账户沿用策略的 `fee_rate`/`slippage_rate`。滑点不另设：Hub 的 dry_run 使用 `PaperBidAsk` 按买一/卖一成交。费率属于注册内容，变更后既有 dry_run 账本因注册不一致而拒绝启动。

- `/api/kline`、`/api/csv/upload`、`/api/ws` 位于 JWT 鉴权分组之外（内置 UI 不携带认证头）。部署时应绑定 `127.0.0.1` 或在反向代理处认证；不要把需要登录的接口挂到这些分组下。
- Telegram 授权依赖上游通道，没有 banbot 内部白名单。
- `web/base/indicators.go` 硬编码 binance，是“banbot 内不写交易所分支”的已知例外。
- `llm` 的 `NewLLMManager`/`LLMManager.Call` 在仓库内无非测试调用，仅 `config` 引用其配置类型，保留作外部嵌入。
- 包级全局状态：`rpc` 的 `channels`/`rpcReady`/`rpcWebhook`、`llm` 的 `modelCache`、`opt` 的注册表、`entry` 的 `commandGroups`/`extraCommands`/`factorLiveBindings`。
- 在 macOS 上临时目录经 `/var`→`/private/var` 符号链接，路径比较类测试需先做 `EvalSymlinks`。
- 因子-only 的 Hub 机器人（没有时序任务，`HubOrderBridge` 未设置）在 `entry/factor_hub.go` 绑定只读的 `biz.HubAccountReader`（`05_biz_orders_live.md`），`ExecutionAccountBalance` 因此判定为 execution 账本支持：`/balance`、`/refresh_wallet` 返回账本权益与可用额（结算币种一行）。`web/live/biz.go:unboundAccount` 仅在 data 模式运行时既无订单管理器、也无 execution 绑定（含读取器）时为真，余额、订单、交易所数据类接口才返回 `errNoAccountBinding`。读取器只读、不提供订单管理器；运行时没有订单管理器时 `/exit_order` 返回 `errNoOrderManager`。
- **仪表盘在 execution 路径上的行为**（`web/live/biz.go`，`biz.ExecutionAccountBalance` 判定）：
  - 余额（`/balance`、`/refresh_wallet`）与统计的账户总值取自 execution 账本：结算币种一行，`total` 为权益（结算现金 + 持仓估值），`free` 为扣除保证金后的可用额；没有按币种的钱包可刷新。
  - `/orders?source=bot`：历史取订单库（Hub 归档行），未平仓订单取内存投影的副本（它们不在订单库中）。
  - `/exit_order` 经 `RemoteCommandService` 走时序订单管理器。
  - `/close_exg_pos` 一律拒绝（仓位归 execution 账户，不允许绕过账本直接下单）。
  - Hub 机器人不持有账户凭据，`/orders?source=exchange|position`、`/incomes`、`/start_down_trade`、`/get_down_trade` 拒绝；内嵌 provider 路径仍可用。
  - `live.RegisterBotAPI` 把上述路由挂到任意 fiber 路由上（无鉴权），供嵌入与测试；机器人使用带鉴权的 `StartApiWithRuntimeDeps`。
- Hub 时序订单的 facade ID 为 52 位随机正整数，保证在按 float64 读取数字的 JSON 消费方（浏览器）中精确。
