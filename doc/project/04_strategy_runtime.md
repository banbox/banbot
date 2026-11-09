# 模块介绍：时序策略引擎与运行时（strat / runtime / runtimeplan）

> 依据：当前工作树源码（2026-11-09）。参考 `doc/runtime_context.md`、`kb/kb_ts_cn.md`、`kb/kb_ts_en.md`，文档内容以源码为准。
> 阅读范围：`strat/` 中 types、state、common、base（开单/平仓主路径）、main（加载与轮换）、execution_state、pair_update、batch_state、data_event、datahub 核心、data_subs；`runtime/runtime.go` 的所有权、构造、停机与回调准入，`time_series_live.go`、`shared_legacy_live.go`、`shared_account.go`、`factor_state.go`；`runtimeplan/inspect.go` 主流程。
> 未逐行精读（只看签名与调用关系）：`strat/goods.go`、`strat/state_accessors.go`、`strat/data.go` 的 WS 注册表、`strat/output_state.go`、`strat/datahub.go` 的字段更新细节、`strat/base.go` 中价格/金额换算段、`runtime/shared_sources*.go` 与 `shared_live.go`（因子实时订阅，归因子模块）、`runtimeplan` 的需求收集细节。

## 定位与职责

- **strat/**：策略域。定义策略接口（`TradeStrat` 配置与回调表）、运行上下文（`StratJob`，每个 策略 × 周期 × 品种 一个）、数据视图（`DataEvent`/`DataHub`）、信号意图（`EnterReq`/`ExitReq`，只入队不下单）、策略注册与实例化、Job 加载/轮换/卸载、动态增删品种（`PairUpdateManager`）、批量执行池（`BatchState`）。策略层不 import `biz`，下单经 `FuncProcessOrders` 回调注入（`strat/execution_state.go:14-25`）。
- **runtime/**：`Process -> Runtime` 所有权树。`Process` 持有跨 Runtime 共享资源（账户 owner、共享账户、SID allocator 与 registry、scheduler 认领）；`Runtime` 组合各领域 typed state，负责构造、停机、Join、重置。另含 TS/因子实时订阅安装（`InstallTimeSeriesLive`）与 legacy 桥接（`BindFactorLegacyJobs`）。
- **runtimeplan/**：无资源的“数据计划”编译。在局部 `core.State`、`btime.ClockState`、冻结 universe 的 `orm.SymbolState` 上实例化策略并调用 `OnSymbols`/`OnStartUp`/`OnPairInfos`/`OnDataSubs`，输出 semantic plan（所需 K 线与自定义序列、unsupported 项）。不连交易所、不下单、不安装全局状态。

## 目录/文件地图

| 文件 | 作用 |
| --- | --- |
| `strat/types.go` | `TradeStrat`、`StratJob`、`EnterReq`、`ExitReq`、`JobEnv`、`BatchMap`、`OdChg*` 常量 |
| `strat/state.go` | `State`（策略域 typed 状态）、`NewState`/`BindRuntime*`、`Reset`、`NewStrategy`、legacy 包级全局视图 `legacyState` |
| `strat/state_accessors.go` | `State` 的 job 注册表读写、快照（`CollectJobs`、`LookupJob`、`SetJobMap`）、env/version 访问 |
| `strat/common.go` | 策略注册表（`RegisterStrategy`/`GetStrategyFactory`）、`New`（legacy 实例化）、`UpdateOrders`/`InitBar`、`CheckCustomExits`、订单事件 `FireOdChange`、历史订单与 Job 评分、fail-open 计数 |
| `strat/base.go` | `StratJob` 开单/平仓校验与入队（`OpenOrder`/`openOrder`、`CloseOrders`/`closeOrders`）、仓位查询、`SetAllStopLoss`、回撤平仓 `drawDownExit`、自定义退出 `customExit` |
| `strat/execution_state.go` | Job 执行状态（`executionStateMu`）、请求队列（`enqueueEntry/Exit`、`DrainOrderRequests`）、订单处理互斥（`Begin/FinishOrderProcessing`）、`ProcessRuntimeOrders`、`WithOrderEvent`（CommandID 派生） |
| `strat/main.go` | Job 生命周期：`LoadStratJobs*`（加载、轮换、info 订阅注册）、`ExitStratJobs*`、`ensureStratJob*`、`resetJobs*`、`FinalizePairRotation`、WS 订阅注册（`regWsJob*`） |
| `strat/pair_update.go` | 运行中增删品种：`TradeStrat.UpdatePairs` → `PairUpdateManager.Apply`，含 `PairUpdateHooks`（外部注入的 SubWarmPairs/ExitOrders/LookupSymbol） |
| `strat/data.go` | 实盘 WS 订阅的 Job 注册表（`WsSubJobRegistry`，不可变快照）及 legacy 包级入口 |
| `strat/data_event.go` | `DataRole`、`DataEvent`、`FnOnData`、`RouteData`、`validateDataCallbacks`（OnData 与 OnBar/OnInfoBar 互斥） |
| `strat/data_access.go` | `NewDataSub(info)`、`StratJob.Data(sub)` |
| `strat/data_subs.go` | 订阅收集 `CollectDataSubs*`（合并 `OnPairInfos` 与 `OnDataSubs`）、K 线字段并集 `CollectKlineSubFields*`、订阅 key 编解码 |
| `strat/datahub.go` | `DataHub`（timeframe:source:sid 三级视图）、`DataFields`（序列/原始值/NULL 语义）、`AllReady` |
| `strat/batch_state.go` | `BatchState`：同一时间戳的主/辅 Job 批量池，`TakeReady` 决定何时批量执行 |
| `strat/goods.go` | 品种周期评分（`CalcPairTfScores*`、`calcKlineScore`）与 policy 品种组（`RelayPolicyGroups*`） |
| `strat/output_state.go` | 策略文本输出缓冲（`WriteOutput` 的 Snapshot/Drain） |
| `runtime/runtime.go` | `Process`、`Options`、`Runtime`、`NewProcess`/`NewRuntime`、`Stop`/`Close`/`Join`、`OnClose`/`OnCloseWait`、`EnterCallback`/`LeaveCallback`、`BizDeps`/`DataDeps` 投影 |
| `runtime/shared_account.go` | `Process.BorrowAccount`、共享账户 `acquireSharedAccount`/`closeSharedAccounts` |
| `runtime/time_series_live.go` | `InstallTimeSeriesLive`：TS 实时订阅安装、`EnvironmentEnded` 回调 |
| `runtime/shared_legacy_live.go` | `BindFactorLegacyJobs`：把 legacy TS Job 绑定到 Runtime 的 StratJob 注册表与 Trader |
| `runtime/shared_migration.go` | `CutoverLegacyExecution`：旧 Runtime 关闭后的执行账户迁移 |
| `runtime/legacy_execution.go` | legacy 发送器（LegacySender）绑定选项与校验 |
| `runtime/factor_state.go` | `FactorState`：因子回放组件的生命周期（Stop/Join） |
| `runtime/shared_live.go`、`shared_sources*.go`、`shared_legacy_live.go` 中因子段 | 因子实时订阅安装、代际切换（归因子模块，本文只列边界） |
| `runtimeplan/types.go` | `RequestV1`、`SemanticPlanV1`、`PolicyV1`、`RequirementV1`、`UnsupportedV1`、`OutputV1`，哈希域常量 |
| `runtimeplan/inspect.go` | `DecodeRequest`、`Inspect`（主入口）、`collectSemanticPlan`/`collectJob`、SHA256 摘要 |

## 核心类型与接口

- **`TradeStrat`**（`strat/types.go:24`）：策略定义。配置字段（`WarmupNum`、`MinTfScore`、`StopLoss`、`StakeRate`、`EachMaxLong/Short`、`OrderOnRotation`、`BatchInOut/BatchInfo`、`RunTimeFrames`、`RefineTF`、`WsSubs` 等）与回调（`OnStartUp`、`OnDataSubs`、`OnSymbols`、`OnData`、`OnCheckExit`、`OnOrderChange`、`OnBatchJobs/OnBatchInfos`、`OnWs*`、`PickTimeFrame`、`GetDrawDownExitRate`、`OnShutDown`、`OnStratExit`）。`OnBar`/`OnInfoBar` 为旧接口，与 `OnData` 同设会在构造时 panic（`data_event.go:50`）。运行期实例按 policy 克隆（`state.go:821 NewStrategy` → `cloneTradeStrat`）。
- **`StratJob`**（`types.go:114`）：运行上下文。公开字段（`Env`、`DataHub`、`Entrys/Exits`、`LongOrders/ShortOrders`、`OrderNum/EnteredNum`、`MaxOpenLong/Short`、`CloseLong/Short`、`ExgStopLoss/ExgTakeProfit`、`LongSLPrice` 等、`More`）保持策略兼容；私有字段绑定 `symbols`、`strategyState`、`runtimeCore`、`runtimePrices`、`runtimeClock`，并通过原子指针懒创建 `executionStateMu`（每 Job 一把细粒度锁，`execution_state.go:92`）。读写可变执行状态应走 `ExecutionSnapshot()` 与 typed 方法。
- **`EnterReq` / `ExitReq`**（`types.go:197,231`）：信号意图。`Tag` 必填（入场；有订单时的平仓）。`Clone()` 与 `GetZapFields()` 在 `common.go:189/227/288/307`，新增字段必须同步。`CommandID` 为幂等键：空时由 `WithOrderEvent` 上下文派生为 `<事件ID>/entry/<序号>`（`execution_state.go:270-311`）。
- **`DataEvent` / `DataRole` / `RouteData`**（`data_event.go`）：`DataRoleMain=1`（主 K 线）、`DataRoleInfo=2`（辅助）、`DataRoleCustom=3`（自定义序列）。`RouteData` 每个事件至多调用一个角色处理器。
- **`DataHub` / `DataFields`**（`datahub.go:15,22`）：`timeframe → source → sid` 三级视图。`DataFields` 同时保留浮点 Series（用于指标）与原始值（`rawMap`，保留显式 nil，区分缺失与 NULL）。`AllReady()` 只检查当前时间会收盘的周期。
- **`State`**（`state.go:29`）：策略域 typed 状态。绑定 `Core/Clock/Config/Symbols/Exchange`、账户与订单状态（`runtimeAccounts`、`runtimeOrders`、`orderProcessor`）；注册表 `accJobs`（账户 → envKey → 策略名 → Job）、`accInfoJobs`（辅助订阅）、`pairStrats`、`envs`、`versions`、`forbidJobs`、`wsSubJobs`；并维护不可变快照与 dirty bit。`legacyState`（`state.go:515`）是包级全局变量的兼容视图，`IsLegacyState` 用于分支。
- **`BatchState` / `JobEnv` / `BatchMap`**（`batch_state.go`、`types.go:89,100`）：批量执行池，键为时间戳+市场组；`TakeReady(currMS, deterministic)` 返回就绪组（确定性模式下等待所有有效任务后按 key 排序输出）。
- **`PairUpdateReq` / `PairUpdateResult` / `PairUpdateHooks`**（`pair_update.go:19,31,39`）：动态品种更新的入参、结果与外部钩子（`SubWarmPairs` 必需；`ExitOrders` 在 `CloseOnRemove` 时必需；`LookupSymbol`）。
- **`FuncProcessOrders` / `BindOrderProcessor`**（`execution_state.go:14,17`）：订单处理器由 `biz/job_orders.go:30` 在组装阶段绑定一次（owner 必须是同一 `Trading` 指针）。
- **`Process` / `Options` / `Runtime`**（`runtime/runtime.go:47,504,559`）：`Process` 为资源 owner（`runtimes`、`sharedAccounts`、`symbolAllocators`、`sidRegistries`、`schedulerClaims`）；`Options` 为构造参数（Storage、Exchange、Config、Scheduler、AccountOwnerKey、SharedExecution、LegacyExecution 等）；`Runtime` 字段见 `doc/runtime_context.md` §2 表格，关键的有 `Core`、`Clock`、`Market`、`Symbols`、`Storage`（借用）、`Batch`、`Strategies`、`Orders`、`Trading`、`Cron`、`Exchange`（借用）。
- **`runtimeplan.RequestV1` / `OutputV1`**（`runtimeplan/types.go`）：JSON 严格解码（`DisallowUnknownFields`，拒绝尾随内容）。输出的 `SemanticPlanSHA256` 为版本化域分隔哈希。

## 关键流程

### 1. 策略注册与实例化
1. 工厂注册：`RegisterStrategy` / `AddStratGroup`（`strat/common.go:945-958`），快照用 `snapshotStratFactories`。
2. 运行期 `State.NewStrategy`（`state.go:821`）：按 `pol.ID()+ToYaml` 缓存；调用工厂 → `cloneTradeStrat` → `validateDataCallbacks` → 默认值（`MinTfScore=0.75`、`StopLoss`、`RunTimeFrames`、`OdBarMax`）→ 绑定 runtime 依赖（`runtimeConfig/Accounts/Core/Clock/runtimeExplicit`）。
3. legacy 路径使用 `common.go:31 New`，缓存在包级 `cacheStrats`；两套实现的默认值逻辑重复。

### 2. 加载 Job（`strat/main.go:57 LoadStratJobsWithState` → `loadStratJobsWithExchange` `main.go:90`）
1. 显式状态下整体持有 `lockJobsWriteForState`，结束后再回调 `OnShutDown` 与 WS 退订（`main.go:105-120` 附近的 defer），避免回调在锁内执行。
2. 备份并重建 `TimeFrame` 与 `OrderMatch` 映射；`resetJobsWithStateLocked` 将旧 Job 设为禁开。
3. 对每个 `RunPolicy`：`NewStrategy` → `getPolicyPairsWithStrategyState`（品种解析） → `callStratSymbolsWithExchange`（`OnSymbols`） → 对每个品种 `pickTimeFrame` 选周期 → 若 `pair_dup` 有专属参数则重新 `NewStrategy` → `ensureStratJobWithRuntimeState`（`main.go:728`）。
4. `ensureStratJobWithRuntimeState`：为每个允许开单的账户创建 `StratJob`，调用 `OnStartUp`，然后 `CollectDataSubs` 注册辅助订阅到 `accInfoJobs`，并为 K 线订阅初始化 `BarEnv`。若有辅助订阅但没有 `OnData`/`OnInfoBar`/（`BatchInfo`+`OnBatchInfos`），直接 panic。
5. 轮换清理（`main.go:281-` 起，`exitOds` 于 `:293`）：`MaxOpenLong==MaxOpenShort==-1` 的旧 Job 若仍有入场则保留并跟踪，否则 `SetPairRemovalPending` 并进入待清理列表；`accInfoJobs` 中已移除的项被剔除。
6. 返回 `pairTfs`（供数据订阅端使用）与 `accExitOds`（需要外部平仓的订单）。

### 3. 单根 K 线事件（调用方：`biz/trader.go`，本模块提供的顺序）
1. 主周期：`job.SetData(evt)`（更新 `DataHub`）→ `job.SetWarmUp(isWarmup)` → `job.InitBar(curOrders)`（`strat/common.go:380`：更新 `CheckMS/LastBarMS`、必要时 `UpdateOrders`；实盘丢弃上一轮未处理请求并打 WARN，回测静默丢弃）。
2. `OnData(DataRoleMain)`；若 `BatchInOut` 则入批量池。
3. 非预热时：`CheckCustomExits`（`common.go:420`，对 `CanClose()` 订单调用 `OnCheckExit`/回撤平仓） → `odMgr.ProcessOrders(job)`（经 `job_orders.go:16 ProcessRuntimeOrders` 回到 `biz`，完成撮合或提交）。
4. 辅助周期：`onAccountInfoSeries`（`biz/trader.go:883`）对订阅该 key 的 Job 调用 `SetData` 与 `OnData(DataRoleInfo)`（或旧接口 `OnInfoBar`），已在主路径处理过的 Job 通过 `handledJobs` 去重。
5. 批量：`BatchState.TakeReady` 就绪后调用 `OnBatchJobs`/`OnBatchInfos`（`biz/biz.go:541`）。

### 4. 开单意图（`strat/base.go:270 OpenOrder` → `openOrder:287`）
1. 基础校验：`Tag` 非空；`CanOpen(short)`（`MaxOpenLong/Short`、现货禁止开空，`base.go:235`）；NaN 与 `CallbackPct` 范围；`Stop` 与 `Limit` 互斥（`base.go:352`）。
2. 价格：预热期用 `Env.Close`，否则 `currentPrice(side)`；限价取更优价。
3. 金额：`LegalCost = 基准金额 × StakeRate × CostRate`，受 `open_vol_rate` 与 `MinStakeAmount` 约束。
4. 止损：`StopLossVal` > `StopLoss` > `LongSLPrice/ShortSLPrice` > `TradeStrat.StopLoss`；方向错误返回错误。受 `ExgStopLoss` 控制（为 false 时整体丢弃并跳过校验，`base.go:429`）。
5. 止盈：`TakeProfitVal` > `TakeProfit`；`LongTPPrice/ShortTPPrice` 不生效。
6. `enqueueEntry`（`execution_state.go:270`）：预热期返回 false（调用方仍得到 nil）；否则追加 `s.Entrys` 并 `OrderNum++`。

### 5. 平仓意图（`base.go:527 CloseOrders` → `closeOrders:544`）
1. 同向无订单直接返回 nil；`Tag` 必填；`CloseLong/CloseShort` 为 false 时报错。
2. 限价退出在有利侧（平多 `Limit<现价`、平空 `Limit>现价`）转为该订单的限价止损（`SetStopLoss`），不入队（`base.go:586-635`）。
3. 其余 `enqueueExit`（`execution_state.go:291`）。

### 6. 订单事件与回调
- 订单状态变化经 `FireOdChange`/`FireOdChangeWithState`（`common.go:885/895`）广播到 `AccOdSubs`；`OrderEventTime` 决定事件时间戳（`common.go:910`）。
- `OdChg*` 常量（`types.go`）：`OdChgNew/Enter/EnterFill/Exit/ExitFill/OrderChanged`。

### 7. 动态增删品种（`TradeStrat.UpdatePairs` → `PairUpdateManager.Apply`，`pair_update.go:166`）
1. 解析 hooks（显式状态用 `State.PairUpdateHooks()`，否则全局 `pairUpdateMgr`）；校验 `SubWarmPairs`。
2. 新增：计算周期评分、`ensureStratJobWithRuntimeState`，若有 `WsSubs` 注册 WS；记录 admission。
3. 删除：`SetOpenLimits(-1,-1)`；若 `CloseOnRemove`，`SetPairRemovalPending(true)`、从 WS 退订、收集订单；释放写锁后调用 `OnShutDown`、退订回调、`hooks.ExitOrders`；再次加锁删除无未决订单的 Job。
4. 最后 `SubWarmPairs(allWarms, true)` 同步数据订阅。
5. 有未决订单的 Job 保留在 `accJobs`，由 `FinalizePairRotation`（`main.go:942`）在终态订单事件后删除（`biz/biz.go` 在 `:195/231/357/436` 调用）。

### 8. 停机
- `Runtime.Stop`（`runtime.go:1236`）：`sharedExecution.Stop()` → `stopScheduler()` → `Core.Stop()`（触发注册的 `OnClose` 钩子）。非阻塞、可重入。
- `Runtime.Close`（`runtime.go:1294`）→ `closeOwned`（`runtime.go:1379`）：Stop → 执行 `OnCloseWait` 钩子（provider/socket/notification join）→ 等待回调租约 `callbackWait` → `Dump.Close` → 依次 `Market/Symbols/Batch/Strategies/Orders/Trading.Reset()` → `Core.Close()`。`Storage` 与 `Exchange` 为借用，不关闭。
- `Process.Close`（`runtime.go:424`）：标记 closed → 关闭全部 Runtime 并 Join → 关闭账户 owner 与共享账户 → 关闭 SID registry。`Process.Stop` 只发布取消。
- 正常 runner 路径：entry 先 `t.Run()` 返回再 `rt.Close()`；live 的 `CryptoTrader` 在停止时先关闭 provider 与 socket（见 `doc/runtime_context.md` §4）。

### 9. Runtime 构造（`runtime/runtime.go:614 Process.NewRuntime`）
1. `beginRuntimeConstruction`（与 `Process.Close` 互斥）→ 校验 `Options`（Hub/Shared/Legacy 组合规则、交易所身份）。
2. 确定存储 namespace（`runtimeStorageNamespace`）、SID registry（`initSIDRegistryOwned`，仅在 `Database.SIDRegistryURL` 配置时）、`initSymbolAllocator`（按 namespace 复用 `SIDAllocator`）。
3. 构造 `core.State`、`btime.ClockState`、`orm.SymbolState`（`BindStorage`、`BindExSymbolRecoveryDir`）、`scheduler`（`claimScheduler`）。
4. 构造 `Runtime{...}`（`runtime.go:875`）：`Strategies: strat.NewState()`、`Batch: strat.NewBatchState()`、`Orders: ormo.NewOrderState()`、`Trading: biz.NewTradingState()`。
5. 可选：legacy 执行（`bindLegacyExchange`）、账户 owner（`accountOwners.Acquire`）、共享执行（`acquireSharedAccount` + 风控/报价注册）。
6. `biz.BindRuntimeDeps(runtime.BizDeps())`（`biz/trader.go:151`）：这是 `strat.State` 绑定 `Core/Clock/Config/Symbols/Exchange/Accounts/Orders` 的实际位置，并由 `biz/job_orders.go:30` 绑定订单处理器。
7. 登记到 `p.runtimes` 并注册活动 Process；`Process` 已关闭则回滚。

### 10. 时序实时订阅安装（`runtime/time_series_live.go:14 InstallTimeSeriesLive`）
1. 前置：`BindFactorLegacyJobs`（`shared_legacy_live.go:16`）已绑定 Job 与订阅，且存在 `sharedOrderBridge`。Job 绑定时将 `BindRuntimeState/BindRuntimeMarket` 写入 Job，并把 Job 放入 `Strategies` 的 `jobMap/infoJobMap`。
2. 每个订阅校验 SID 与 `Symbols` 中的身份一致；`Catalog.CompileSubscriptionPlan` 生成计划（`RequireManagedLive=true`）。
3. `provider.PrepareSubscriptionPlan` → `Activate`；`OnClose(installation.Stop)`、`OnCloseWait(Join)`。
4. `timeSeriesLiveSink.EnvironmentEnded`：进入回调租约后交给 `biz.GetOdMgrWithState(...).OnEnvEnd(row)`；失败则冻结共享执行并 `Stop()`。

### 11. 数据计划编译（`runtimeplan/inspect.go:42 Inspect`）
1. `validateRequest` → `NormalizeRunPolicies` → 用 `InitialSymbols` 与时间范围构造 `config.Snapshot`。
2. 局部 `core.State`（`RunModeBackTest`、`NetDisable=true`）、`btime.ClockState`、从 `MarketUniverse` 冻结的 `SymbolState`，并 `strat.NewStateWithRuntime(..., nil exchange)`。
3. `collectSemanticPlan`：对每个 policy 调用 `makeStrategy`（panic 捕获）→ `OnSymbols` → 校验品种与周期 → 对每个 Job 调用 `collectJob`：`NewInspectionJob`（effect 钩子记录被禁止的 `OpenOrder/CloseOrders/UpdateOrders/InitBar` 等调用）→ `OnStartUp`/`OnPairInfos`/`OnDataSubs` → 生成 `RequirementV1`。
4. `collectFrameworkRequirements` 补充框架级需求（pair 列表等）；`canonicalizePlan` 排序去重；计算三类 SHA256。
5. 存在 `Unsupported` 时返回 output 与错误。

## 配置项与外部依赖

- **RunPolicy（`config.RunPolicyConfig`）**：`stake_rate`（覆盖 `StakeRate`）、`order_bar_max`、`stop_loss`（数值或 `"5%"`）、`run_timeframes`、`max_pair`、`pair_params`（触发 `PairDup` 重新实例化）、`refine_tf`、`filters`（`runtimeplan` 中会报 unsupported）。
- **全局/Config**：`config.OrderBarMax`（默认 500）、`config.Data.BTLegacyIntrabar`（仅非 explicit 路径）、`core.MinStakeAmount`（5.5）、`PairMgr.PosOnRotation`（轮换时是否继续持仓）、`Database.SIDRegistryURL` / `Database.AutoCreate`（SID 权威）。
- **Runtime Options**：`Storage`、`Exchange`（借用）、`Scheduler`/`SchedulerBorrowed`、`Catalog`、`SourcePlanOptions`、`StorageNamespace`、`DataDir`、`StrategyDir`、`Dump`、`LegacyExecution`、`SharedExecution`、`SharedOrderBridge`、`HubOrderBridge`、`AccountOwnerKey`。
- **外部包**：`banta`（`ta.BarEnv`、`ta.Series`）、`banexg`（交易所接口与 `OdSide*`）、`orm`（`SymbolState`、`DataSeries`、`Subscription`、`SIDAllocator`、`SymbolSIDRegistry`）、`biz`（订单管理、Trader、`RuntimeDeps`）、`data`（`LiveProvider`、`DataSourceCatalog`、`SubscriptionPlan`）、`com`（`Scheduler`、`PriceState`、`MarketState`）、`btime`、`factor/runner`（FactorState）、`execution`（账户 owner、共享账户）。

## 与其他模块的交互边界

- **strat -> biz（单向）**：策略不 import `biz`。订单处理经 `BindOrderProcessor` 注入；`biz` 通过 `State` 的 `Get/Version/GetStratPerf/AddAccFailOpens/RefineTimeFrame` 查询策略；`biz/trader.go` 驱动 `SetData/SetWarmUp/InitBar/OnData/CheckCustomExits/ProcessOrders` 的事件顺序。
- **runtime -> biz**：`Runtime.BizDeps()` 投影（`runtime.go:1088`）；`biz.BindRuntimeDeps` 完成一次性绑定；`NewTraderWithRuntimeDeps`、`NewRuntimeNotifications`、`GetOdMgrWithState`。
- **runtime -> data**：`DataDeps()`（`runtime.go:1124`）、`LiveProvider`、订阅计划。
- **live / entry / opt -> runtime**：构造与关闭由调用方负责（`entry` 先关 Process 再释放自建 Storage/Exchange；`opt` 每轮创建并释放 Runtime；`live/common.go:99-119` 与 `live/crypto_trader.go:1681` 调用 `LoadStratJobsWithState`/`ExitStratJobsWithState`/`FinalizePairRotation`）。
- **runtimeplan <- entry/web**：只读 Inspect；不得依赖其副作用，`Inspect` 不安装包级 globals（但 `makeStrategy` 仍可能访问 `config.Data` 等非 explicit 路径）。
- **包级兼容 facade**：`strat/state.go:525-` 的 `legacyState` 将 `Versions/Envs/AccJobs/PairStrats/cacheStrats` 等包级变量作为视图；`core` 的 `LegacyTimeFrameStateSnapshot`/`ReplaceLegacyOrderMatchTfs` 同理。新代码应只使用显式 `State`。

## 扩展与修改指南

- **新增 `TradeStrat` 字段或回调**：改 `strat/types.go`；若回调参与启动/订阅阶段，还需在 `runtimeplan/inspect.go` 的 `collectJob`/`call*` 中加入调用（否则 Inspect 不会覆盖，计划可能漏订阅）；若与 `OnData` 存在互斥，改 `validateDataCallbacks`（`data_event.go:50`）。同时更新 `kb/kb_ts_cn.md` 与 `kb/kb_ts_en.md` §3.1。
- **新增 `EnterReq`/`ExitReq` 字段**：改结构体 → `openOrder`/`closeOrders` 校验与使用 → `Clone()` 与 `GetZapFields()`（`common.go:189,227,288,307`）。漏掉 `Clone` 会导致异步日志与 `CommandID` 丢失。
- **新增订单事件类型**：`types.go` 增加 `OdChg*` → `common.go:FireOdChange`、`OrderEventTime`（`common.go:910`）→ 各调用方（`biz`）。
- **修改 Job 生命周期**：三段逻辑在 `main.go`（创建 → 轮换清理 → info 注册），与 `pair_update.go`、`FinalizePairRotation` 协同。`PairRemovalPending` 与 `jobHasOutstandingOrders` 的语义必须保持一致，否则会出现“已删 Job 仍接收终态事件”或“空 Job 永久残留”。
- **修改注册表**：所有 `accJobs/accInfoJobs/wsSubJobs` 写入必须在 `lockJobsWriteForState`/`lockInfoJobsWrite` 内完成，并设置对应 snapshot dirty bit；读取优先用 `*View`/`Snapshot` 方法。
- **新增 Runtime 组件**：在 `NewRuntime` 中构造，用 `OnClose(stop)` 与 `OnCloseWait(join)` 注册；不要在 `closeOwned` 之外重置状态；若持有借用资源，不要在 `Runtime` 内关闭它。
- **注意两条路径**：`IsLegacyState` 分支（包级全局）与 explicit 分支（`State` 实例）并存。新代码只写 explicit 分支；修改共用函数时两条分支都要验证。
- **易踩坑**：
  - 请求只在当前事件内有效；预热期 `OpenOrder` 静默丢弃并返回 nil。
  - `GetOrders`/`GetOrderNum`/`LongOrders` 是快照，同一回调内刚开的单看不到。
  - `More` 只保存每品种可变数据；超参放 `pol.Def` 并只读。
  - 回调内禁止阻塞 IO、禁止访问其他 Job 的包级状态；跨品种汇总用 `OnBatchJobs`。
  - `Dirt` 零值是 `Both`，只平多/空必须显式设置 `core.OdDirtLong/Short`。
  - `Inspect` 中 `OpenOrder` 等会被标记为 unsupported，不要在 `OnStartUp` 等启动回调中下单。

## 已知限制与设计取舍

- `strat.LoadStratJobsWithRuntimeState` 为导出 API，仓库内无调用方，保留供外部使用。
- 负止盈价会被改写为 `enterPrice*0.001`，随后方向校验报 “takeProfit must > enterPrice”，不视为未设置。
- 当前品种的附加数据源订阅也会触发 `OnDataSubs`，策略必须实现对应的 `OnData` 处理。
- `runtimeMarket` 在 Job 绑定 `symbols` 且 `Symbol.Market` 与 `Env.MarketType` 都为空时返回空串，现货禁空检查不生效。
- legacy `strat.New` 复用 `pol.RunTimeframes` 切片并在缓存命中时修改 `obj.Policy`，并发不安全；`NewStrategy` 则克隆。
- `NewStrategy` 的日志依赖 `state.Core`，所有入口必须先 `BindRuntime`；`Runtime.OnClose` 依赖 `Core` 已设置。
- `BindFactorLegacyJobs` 会对传入的 `subs` 写入默认 `Source`。
- `Strategies`/`Orders` 的绑定位于 `biz/trader.go` 与 `biz/job_orders.go`，修改绑定逻辑需同时查看两个包。
