# 业务核心模块：biz / 订单管理 / 钱包 / 共享账户 / live

> 描述当前源码的实现，行号仅作定位参考。
> 规约参考：`AGENTS.md`（banexg 统一交易所差异；`DataSeries.Values` 不得收窄；QuestDB WAL 写后读需等待可见性）。
> 架构背景见 `doc/app_arch.md`，运行时见 `doc/runtime_context.md`。

## 定位与职责

- **biz**：交易执行核心。包括 `Trader`（K 线/数据系列分发到策略、批量回调、订单事件）、订单管理器（`odmgr` 系列：本地回测、dry-run 本地实盘、实盘 `LiveOrderMgr`、共享账户 `SharedOrderMgr`、Hub 远端 `HubOrderMgr`）、钱包（`BanWallets`）、共享账户检查点/接管/迁移、远程命令（web/telegram/cli 平仓与交易开关）、时序命令桥与投影、gRPC 特征数据服务、回测确定性辅助、离线工具（K 线导入导出、回放、订单缓存）。
- **live**：实盘主控制器 `CryptoTrader`（启动、行情循环、订单管理器注册、账户同步）、账户检查（`account_check.go`）、运行时定时任务（`runtime_cron.go`）、通用实盘函数（`common.go`）、实盘工具（`tools.go`）。
- 边界原则：运行时路径使用 `*WithRuntimeDeps` / `*WithState` 版本，避免读取包级全局状态；旧门面（`GetOdMgr`、`strat.GetJobs`、`FindOpenOrders`、`core.Legacy*`）仍存在并与新路径并行。

## 目录/文件地图

### biz/（非测试文件）

| 文件 | 作用 |
|---|---|
| `biz/biz.go` | 启动装配 `SetupComs`/`SetupComsExg`；选币、任务刷新；订单事件订阅 `InitOdSubsWithRuntimeDeps`；批量任务 `AddBatchJob`/`TryFireBatches` |
| `biz/trader.go` | `Trader` 与 `RuntimeDeps` 定义；`FeedDataSeries` 分发；串行/并行的账户策略回调 |
| `biz/runtime_deps.go` | `RuntimeDeps` 投影为 `data.RuntimeDeps` 副本 |
| `biz/runtime_logger.go` | Trader/RuntimeDeps/OrderMgr 的 logger 取值 |
| `biz/runtime_state.go` | `TradingState`：按账户注册 OrderManager / LiveManager / Wallet |
| `biz/live_runtime.go` | 批量平单 `CloseAccOrdersWithState`、致命亏损熔断 `MakeCheckFatalStopWithRuntime` |
| `biz/determinism.go` | 回测确定性：**所有**回测（`config.OrderedBacktest()` / `RuntimeDeps.OrderedBacktest()`，与 `bt_strict` 无关）按稳定键（账户名/job key/开仓时间+ID）遍历 map；品种间同 bar 先后由 `data/provider.go` 回放堆的哈希序决定（`bt_bar_order_seed`，issue 112，BtFeatureLevel 6），本文件只负责单品种内部的稳定顺序；`bt_strict` 仅再控制重放覆盖与兼容钱包语义（`strictBacktestFor`） |
| `biz/data_server.go` | gRPC FeaFeeder 服务（默认 `:6789`，收发上限 100MB 硬编码） |
| `biz/remote_command.go` | `RemoteCommandService`：校验、幂等、1 秒限频、审计；平仓与交易开关 |
| `biz/job_orders.go` | `ProcessJobOrders`：策略异步回调发起下单（几乎无非测试调用方） |
| `biz/odmgr.go` | `IOrderMgr`/`IOrderMgrLive` 接口；`OrderMgr` 基类（入单、出单、利润、追踪止损、注册表、`CloseAccOrders`） |
| `biz/odmgr_local.go` | `LocalOrderMgr`：回测/模拟撮合（挂单、止损止盈、限价超时、清仓） |
| `biz/bar_gap.go` | 回测 K 线空洞守卫 `guardBarGap`（`ExitTagDataGap`）与退市平仓 `exitDelisted`/`sweepDelisted`（`ExitTagDelist`） |
| `biz/odmgr_local_live.go` | `LocalLiveOrderMgr`：dry-run 实盘，市价按 ask/bid 即时成交，挂单按 K 线推进 |
| `biz/odmgr_live.go` | `LiveOrderMgr`（4710 行）：实盘下单、成交回报、同步对账、触发单、熔断 |
| `biz/odmgr_live_exgs.go` | 交易所成交反推本地订单：`exitByMyOrder`、`traceExgOrder`、`makeInOutOd` |
| `biz/shared_order_mgr.go` | `SharedOrderMgr`：进程内共享账户订单门面（下单/撤单/平仓/改单、准入、投影） |
| `biz/shared_checkpoint.go` | 共享检查点加载/拆分存储（活跃键 + 按需读取历史） |
| `biz/shared_takeover.go` | 外部接管记录恢复为 TS 投影订单 |
| `biz/shared_migration.go` | 旧版订单导入为 execution 虚拟 lot，保护单与源平仓映射 |
| `biz/shared_account.go` | `execution.SharedAccount` 的别名与构造转发 |
| `biz/hub_order_mgr.go` | `HubOrderMgr`：远端 Hub 的机器人侧门面，journal 持久化、写者租约 fencing、命令冻结与重放 |
| `biz/hub_order_projection.go` | Hub 投影：poll 已提交事件，折叠为 `hubProjectedLot`，投递回调 |
| `biz/hub_order_edit.go` | Hub 改单：转为 PendingCommand 冻结后发送 |
| `biz/hub_order_lock_unix.go` / `_windows.go` | journal 独占文件锁（flock / LockFileEx） |
| `biz/intent_bridge.go` | 旧式 `strat.EnterReq`/`ExitReq` 翻译为 `execution.EligibleIntent`，数量换算与风控上限 |
| `biz/time_series_command_bridge.go` | 时序策略请求封装为可持久化 `execution.PendingCommand` |
| `biz/time_series_projection.go` | 时序“已提交事件”投影消费者 `TimeSeriesProjectionConsumer`（非测试代码无调用） |
| `biz/telegram_order_manager.go` | 实现 `rpc.OrderManagerInterface`，Telegram 命令转 `RemoteCommandService` |
| `biz/tools.go` | 离线工具：zip K 线导入、日历导入、K 线导出/清理/聚合、相关性、历史回放、交易所订单缓存（gob） |
| `biz/wallet.go` | `BanWallets`/`ItemWallet`：余额、pending/frozen 占用、成交结算、保证金、爆仓、同步、快照 |
| `biz/aifea.pb.go` / `aifea_grpc.pb.go` | 生成代码（不纳入人工维护） |
| `biz/config.yml`、`biz/config.local.yml` | 嵌入二进制的默认配置（首次写入数据目录） |
| `biz/zh-CN/`、`biz/en-US/` | 文案与 `messages.json`（两份 key 集合一致，各 154 个） |

### live/（非测试文件）

| 文件 | 作用 |
|---|---|
| `live/crypto_trader.go`（1733 行） | `CryptoTrader`：`Init`（:202）装配 provider、账户检查、订单管理器、pair 任务；`runWithDeps`（:670）驱动一次运行；`startJobs`（:1533）注册 cron 与实盘订单流；`FeedDataSeries`/`feedDataSeries`（:763/:776）行情入口；`runtimeShutdownLifecycle`（:89）把停止/等待钩子接入 `runtime`；关停相位 `runShutdownPhases`（:1347）；cleanup `finishRunCleanup`（:1297）；通知 `orderCB`（:1523） |
| `live/common.go`（402 行） | `refreshPairJobsWithRuntime`（:28）选币轮动；`StartLoopBalancePositionsWithRuntime`（:220）余额/仓位轮询；`updateAccBalanceWithRuntime`（:313）；订单消息 `sendOrderMsgWithRuntime`（:341）；旧式 `sendOrderMsg*`（:333/:337）已无调用 |
| `live/account_check.go`（411 行） | 启动前账户体检 `CheckLiveAccountsWithRuntimeDeps`（:92，旧版 `CheckLiveAccounts`:36）：交易/提币/IP 权限、余额与 `wallet_amounts` 对比、持仓/账户/保证金模式；出现 error 即置 `cfg.NoTrade=true` |
| `live/runtime_cron.go`（365 行） | cron 注册（均为 `*WithRuntime` 版本）：选币刷新 :64、小时线补齐 :102、市场加载 :133、致命亏损 :177、K 线延迟监控 :200、10 分钟收数汇总 :283、策略输出落盘 :310、触发单校验 :356；`runtimeRunGate`（:29）防止同一任务重入 |
| `live/tools.go`（192 行） | CLI/运维平仓 `CloseOrdersWithRuntimeDeps`（:27）经 `biz.RemoteCommandService` 执行；`closeSelectedAccounts`（:101）、`cancelPendingOrdersWithRuntimeDeps`（:117）、`closePositionsWithRuntimeDeps`（:141，仅非 shared 路径）、`closeLocalOrdersWithRuntimeDeps`（:167） |

均为非测试文件；测试见 `live/*_test.go`（`crypto_trader_test.go` 1685 行、`crypto_trader_runtime_concurrency_test.go`、`common_test.go`、`runtime_cron_test.go`、`tools_test.go`）。

## 核心类型与接口

- **`Trader`**（`biz/trader.go:31`）：持有 batchState 与 `runtime *RuntimeDeps`。构造 `NewTraderWithRuntimeDeps`（:131）只校验绑定，不补齐依赖；`BindRuntimeDeps`（:151）由组合根一次性绑定。
- **`RuntimeDeps`**（`biz/trader.go:46`）：单个 runtime 的全部可变状态：Core、Clock、Market、Batch、Strategies、Orders、Trading、Config、Accounts、Symbols、Storage、Exchange、Notifications、DefaultAccount。
- **`TradingState`**（`biz/runtime_state.go:9`）：`orderManagers` / `liveManagers` / `wallets` 三张按账户注册表。`Reset`（:174）不关闭管理器，调用方须先等回调结束。
- **`IOrderMgr`**（`biz/odmgr.go:33`）：`ProcessOrders`、`EditOrder`、`RelayOrders`、`EnterOrder`、`ExitOpenOrders`、`ExitOrder`、`UpdateByDataSeries`、`ExitAndFill`、`OnEnvEnd`、`CleanUp`。
- **`IOrderMgrLive`**（`biz/odmgr.go:46`）：增加 `SyncExgOrders`、`WatchMyTrades`、`TrialUnMatchesForever`、`ConsumeOrderQueue`。
- **`runtimeOrderConfig`**（`biz/odmgr.go:59`）：绑定时从 `RuntimeDeps.Config` 拷贝的不可变快照；`makeRuntimeOrderConfig`（:568）填默认值（LimitVolSecs=10、PutLimitSecs=180、OdBookTtl=500、BTNetCost=15）。
- **`OrderMgr`**（`biz/odmgr.go:75`）：基类。每个访问器都有 `runtimeDeps` 双路径（有 Runtime 用注入依赖，否则回退包级全局）。改动时两条路径必须同步。
- **`LocalOrderMgr`**（`biz/odmgr_local.go:24`）：嵌入 `OrderMgr`，附 `simMu`（仅回测 ProcessOrders 加锁，:141-147）、`stopBacktest`。
- **`LocalLiveOrderMgr`**（`biz/odmgr_local_live.go:19`）：嵌入 `LocalOrderMgr`。
- **`LiveOrderMgr`**（`biz/odmgr_live.go:34`）：嵌入 `OrderMgr`。关键字段：`queue chan *OdQItem`（容量 1000，:189）、`doneKeys`/`doneTrades`/`exgIdMap`（各有 60s/5min 清理）、`unMatchTrades`、原子标志 `isWatchMyTrade`/`isConsumeOrderQ`、`workerCtx`/`workerCancel`/`workerWait`/`workerSealed`。覆盖 `ProcessOrders`(:1644)、`UpdateByDataSeries`(:1656)、`EditOrder`(:1681)、`OnEnvEnd`(:4536)、`ExitAndFill`(:4547)、`CleanUp`(:4557)、`finishOrder`(:4398)。
- **`SharedOrderMgr`**（`biz/shared_order_mgr.go:190`）：`projecting`/`projectionPending` 合并并发投影；`sharedTSCheckpoint`（:102）含 Serial、Orders、Feeds、Admission、Commands、Accepted。
- **`HubOrderMgr`**（`biz/hub_order_mgr.go:80`）：`journal *hubOrderJournal`（:64，整体 JSON 落盘）、`polling`/`delivering` 投影标志、`lockFile`。`HubOrderBridgeConfig`（:28）要求绝对 `StatePath`。
- **`hubProjectedLot`**（`biz/hub_order_projection.go:17`）：`Record`、`Entered/Exited`、成本/费用/已实现盈亏、`Terminal`。
- **`LiveOrderMgr` 依赖的 ormo 模型**：`InOutOrder`（`orm/ormo/order.go:36`）、`ExOrder`、`IOrder`、`TriggerState`、`OrderState`。
- **`TimeSeriesEntryPayload` / `TimeSeriesOrderEvent`**（`biz/time_series_command_bridge.go:17`、`biz/time_series_projection.go:15`）：持久化与投递的事件结构。
- **`RemoteCommand` / `RemoteCommandService`**（`biz/remote_command.go:29`、`:52`）：含 seen 幂等表与 lastHit 限频表。
- **`IntentBridgeContext` / `EntryIntentBridge` / `ExitIntentBridge`**（`biz/intent_bridge.go:18/35/151`）。
- **`CryptoTrader`**（`live/crypto_trader.go:33`）：嵌入 `biz.Trader`，另持 `dp *data.LiveProvider`、`retiredProviders`、`symbols`、`dataDeps`、`apiServer`、`scheduler`、`seriesRuntime`，以及一组测试注入点 `initFn`/`startJobsFn`/`loopMainFn`/`nowMSFn`/`collectJobsFn`。并发控制：`runLock`（整次运行）、`runtimeLock`（计数、provider 指针、timer）、`runtimeCount`+`runtimeCond`（在途回调计数，等待用）、`shutdownLock`（关停相位）、`pairRefreshGate`（选币刷新互斥）。构造：`NewCryptoTraderWithRuntimeDeps`（:161）要求 `deps.Callbacks` 实现 `RuntimeLifecycle`，否则拒绝；`NewCryptoTrader`/`NewCryptoTraderWith` 为旧式公开构造。
- **`RuntimeLifecycle`**（`live/crypto_trader.go:70`）：`Context()`、`OnClose(fn)`（取消阶段执行）、`OnCloseWait(fn)`（等待阶段执行）。实现方为 `runtime.Runtime`，经 `runtimeShutdownLifecycle`（:89）适配，并额外暴露 `EnterCallback`/`LeaveCallback` 与 `Exchange()`。
- **关停两段式**：`OnClose` 注册的停止钩子在取消阶段运行（`stopAndJoinRegisteredResources` :1271，逆序，先停后等）；`OnCloseWait` 注册的 join 钩子由 `runShutdownPhases`（:1347）统一调用，之后 `joinRuntimeCallbacks`（:1369）循环关闭 provider 直到 `retiredProviders` 与 `dp` 清空，再 `finishRunCleanup`。
- **`runtimeRunGate`**（`live/runtime_cron.go:29`）：包装 `sync.Mutex`，`run` 用 `TryLock`（重入则跳过本次 cron），`runWait` 阻塞等待（仅 `refreshPairJobs` 直接调用时使用）。
- **`accCheckResult`**（`live/account_check.go:18`）：权限三元组（`*Allowed`/`*Known`）、IP、持仓/账户/保证金模式、余额摘要、`errors`（触发禁用）与 `warns`（仅告警）。
- **`balanceRuntimeDeps`**（`live/common.go:169`）：余额轮询专用快照，复制 exchange、core、config 视图、账户映射（`snapshotBalanceAccounts` :183）、orders、trading、间隔（默认 1 分钟，取 `account_pull_secs`）。目的是避免配置刷新时的 map 并发读写。
- **`TradeCloseRequest` / `TradeCloseResult`**（`live/tools.go:18/23`）：CLI 平仓请求（Accounts/Pairs/Strategies 过滤、Exchange、Confirmed）与结果计数。

## 关键流程

### 1. 启动与运行时绑定

1. `biz/biz.go:SetupComs`（:46）：InitDataDir、`config.LoadConfig`、`core.Setup`、`utils.Setup`、`exg.Setup`、`orm.SetupWithAutoCompact`、`goods.Setup`。`SetupComsExg` 另调 `orm.InitExg`。
2. `runtime/runtime.go:975` 调 `biz.BindRuntimeDeps`，绑定后设置订单 trades 路径 `orders_<name>.db`。
3. 实盘：`live/crypto_trader.go` 创建 `Trader`（`NewTraderWithRuntimeDeps`，约 :162），注册订单管理器（约 :443-461 调 `InitLiveOrderMgr*`），`live/crypto_trader.go:483` 对各账户调用 `SyncExgOrders`，约 :1548-1557 先同步后 `StartLiveOdMgr*`。
4. 选币与任务：`biz/biz.go:RefreshPairsWithRuntimeDeps`（:247）→ `goods.RefreshPairListWithRuntimeDeps` → `strat.CalcPairTfScoresWithState`；`RefreshJobsWithRuntimeDeps`（:212）→ `strat.LoadStratJobsWithState`。轮出的 pair 经 `odMgr.ExitAndFill` 平仓。`RefreshPairsWithSymbolState` 在 symbols 非 nil 时直接报错（`biz/biz.go:123`），须用 RuntimeDeps 版本。

### 2. 数据系列到策略

- `biz/trader.go:FeedDataSeries`（:544）→ `ResolveDataSeriesSymbol`（:403）→ `feedClosedSeries`（:644，更新价格、`UpdateByDataSeries`、`OnEnvSeries`）→ 串行 `feedClosedSeriesSerial`（:731）或并行 `feedClosedSeriesParallel`（:774）→ `onAccountDataSeries`（:824）→ `onAccountDataSeriesJob`（:1059：OnData/OnBar → 批量任务 → CheckCustomExits → ProcessOrders）。
- 非 K 线系列：`feedDataOnlySeries`（:574）→ `deliverDataOnlySeries`（:627）。
- 批量：`biz/biz.go:AddBatchJobWithRuntimeDeps`（:480，延迟 `core.DelayBatchMS`）→ `TryFireBatchesWithRuntimeDeps`（:517）；warmup 时不下单。
- 致命熔断：`trader.go:onAccountInfoSeries`（:883）中 `env.VNum > 1000` 且持续新增时 `setBotRunning(false)`（:922）。

### 3. 订单事件

- `biz/biz.go:InitOdSubsWithRuntimeDeps`（:367）→ 每账户 AddOdSub → LookupJob → 实盘刷新最新价 → HedgeOff 时 closeSideOrders → `OnOrderChange` → ProcessOrders → FinalizePairRotation。

### 4. 策略意图进入订单管理器

- 旧式请求：`shared_order_mgr.go:543` → `BridgeEntryReq`（`biz/intent_bridge.go:74`）：Amount 优先，否则 LegalCost/StrategyNAV×StakeNAVFraction×CostRate；超出 MaxNotional 报错（不截断）；`Intent.Evaluate` 校验。出场：`BridgeExitReq`（:162）先 `ExitSelection.Select`，再按 Amount（先减持仓再撤挂单）或 ExitRate 计算。
- 时序命令桥：`hub_order_mgr.go:540` → `BridgeTimeSeriesEntryCommand`（`time_series_command_bridge.go:28`）复用 `BridgeEntryReq`，清空 StopBars/CreatedBar（由 owner 按持久化 feed 进度绑定）；出场 `hub_order_mgr.go:632` → `BridgeTimeSeriesExitCommand`（:62）。命令在重试间必须保持稳定的 lot 与 CommandID。

### 5. 回测/模拟撮合（LocalOrderMgr）

- `odmgr_local.go:UpdateByDataSeries`（:150）→ `fillPendingOrdersAll`（:213，回调中新开单，最多 30 轮，超出报错 :249）→ `fillPendingOrdersPass`（:321，经 `execution.OHLCProfile.MatchPending`）→ `fillPendingEnter`（:436，`wallets.EnterOd`/`ConfirmOdEnter`）或 `fillPendingExit`（:517，`wallets.ExitOd`/`ConfirmOdExit`、`finishOrder`）。
- 止损止盈：`tryFillTriggers`（:551）→ `MatchProtection`，部分平仓先 `CutOrder`（odmgr.go:1648）。
- 限价超时：:396-412 按 `OdInfoStopAfter` 调 `localExit`（`ExitTagEntExp`）。
- 回调三处触发：fillPendingEnter、fillPendingExit、tryFillTriggers → `callBack(od, isEnter)` 与 `strat.FireOdChange`（odmgr.go:619）。
- 滑点：`LocalOrderMgr.ohlcProfile()` 按 `bt_slippage_bps` 构造 `OHLCProfile.SlippageBps`；市价单、市价止损入场、止损/止盈出场及强平类出场（`ExitAndFill`）按不利方向加滑点，止损不优于开盘跳空价；限价单不加滑点。
- 资金费：`updateOrderFee` 在回测出场腿调用 `addApproxFunding`，对多头合约单按 `bt_funding_apr` 计 入场名义 × 年化 × 持仓年数，加入 `Exit.Fee/FeeQuote`，并写入订单 info `funding`；反向合约不计，标 `funding_skipped=inverse`，空头不计。
- 数据空洞与退市（`bt_gap_bars>0` 且非实盘）：`Trader.feedClosedSeries` 在登记新 bar 价格前调用 `guardBarGap`——上一根 bar 结束到新 bar 开始的缺口同时超过 `bt_gap_bars` 个周期与 `bt_gap_min_hours`（默认 48）小时时，该标的未平订单以最后已知价格强平（tag `data_gap`）；`sweepDelistedAll` 每 6 小时模拟时间扫描一次，按同一规则把数据已终止的标的强平（tag `delist`），`CleanUp` 开头也会执行。`delist` 成交时间取该标的最后有效 bar 的时间（`PriceState.GetLastBarTimeAt`），保证金、持仓时长与 `max_open_orders` 槽位随之释放；`data_gap` 在数据恢复时触发，成交时间为 `min(最后 bar + 确认延迟, 恢复时间)`（`gapFillTime`；确认延迟 = `max(bt_gap_bars×周期, bt_gap_min_hours)`），不早于最后 bar。成交价由 `applyExitPolicy` 决定：`data_gap` 取最后收盘价与缺口后首根开盘价中对持仓更差者（`ExitReq.GapNextOpen/GapRecoverMS`），`delist` 取最后收盘价再按 `bt_delist_penalty_bps` 向不利方向偏移；之后仍叠加 `bt_slippage_bps`。
- 入场后伪时间 +1ms（`makeLocalAfterEnter`，:919-931），防止同 key 钱包扣款冲突。

### 6. dry-run 本地实盘（LocalLiveOrderMgr）

- `odmgr_local_live.go:callLocalLiveOdMgrsData`（:195）逐根 K 线推进挂单；市价单 `tryFillLocalLiveOrder`（:136）多头取 ask、空头取 bid；`Stop>0` 的单跳过。dry-run 时写钱包快照（:250）。

### 7. 实盘下单（LiveOrderMgr，odmgr_live.go）

1. 入口：`biz/trader.go:1098` → `odMgr.ProcessOrders` → `odmgr.go:993` 基类：`DrainOrderRequests` → `allowOrderEnter`（:833）→ `enterOrder`（:1139）。出场 `ExitOpenOrders`（:1324）→ `exitOrder`（:1560）。
2. 入队：`makeAfterEnter`（:1698）判断 `isFarEnter`；远离成交的限价单打 `odInfoLocalTrigger` 并 `addTriggerOd`，不提交交易所；否则 `o.queue <- Enter`。`makeAfterExit`（:1721）同理。
3. 消费：`ConsumeOrderQueue`（:1743）单 worker goroutine → `handleOrderQueue`（:1768）→ `od.Lock()` → `execOrderEnter`（:2456）。
4. 提交：Amount 为 0 时 `wallets.EnterOd`（`biz/wallet.go:1099`）算量，`exg.PrecAmount`；`submitExgOrder`（:2606）依次：杠杆设置（超上限截断并按比例缩放数量，:2623-2648）、限价计算（`getLimitPrice`，:2984）、止损价参数（:2703）、`buildClientOrderID`（:2115，入场每次随机，:2689）、`exchange.CreateOrder`（:2736）。
5. 失败：入场失败 `localExit(FatalErr)`（:2513-2518）；reduceOnly 被拒 `localExit(NoMatch)`（:2738-2746）。
6. 回报：`updateOdByExgRes`（:2772）更新成交、`callBack`、`fireOdChange`。完全入场后 `setTrailingStop`（:4050）与 `editTriggerOd`（:4136）挂 SL/TP。退出完成则 `finishOrder`（:4398）→ odmgr.go:1671。
7. 异步回报：`WatchMyTrades`（:1831）→ `handleMyTrade`（:1992）→ `updateByMyTrade`（:2269）。无匹配者入 `unMatchTrades`，由 `TrialUnMatchesForever`（:2161，约 3s 周期）交 `exitByMyOrder` 或 `traceExgOrder`（`odmgr_live_exgs.go:11/110`）。
8. 触发单：`VerifyTriggerOds` 由 `live/runtime_cron.go:363` 注册为 cron；`verifyAccountTriggerOds`（:3609-3774）依订单簿估算等待量；远离成交（`waitSecs > putLimitSecs*5`）则 `cancelEnterToLocalTrigger`（:3905）撤回本地。

### 8. 启动恢复与对账（SyncExgOrders，odmgr_live.go:618）

- a. `fetchAccountOpenOrders`（:826，上限 1000）；b. `loadOpenOrders`（:856，本地 status=1）；c. 逐单 `restoreInOutOrder`（:925）：无 OrderID 的入场单按 ClientOrderID 认领（多匹配报 ambiguous）；本地触发单重新登记；有 OrderID 的未完成单用 exgOdMap 或 `FetchOrder` 同步。d. `restoreExchangeTriggers`（:3568）认领交易所触发单。e. `loadRecentClosedOrders`（:843）。f. 合约：`FetchAccountPositions`，按 pair 执行 `syncPairOrders`（:1031，FetchOrders 按 7 天分段，限 300 条）→ `applyHisOrder`（:1289）；剩余仓位由 `createOdFromPos`（:1535）生成“用户开仓”跟踪单。g. `healMissingFullEnterTriggers`（:3579）→ `saveDirtyODs` → `markSyncComplete`（:812）。
- 运行期：`live/common.go:304` 周期调用 `SyncLocalOrders`（:374，仅 linear/inverse），超额本地仓位按数量降序用 `localExit(NoMatch)` 关闭。

### 9. 熔断

- `odmgr_live.go:MakeCheckFatalStop`（:4466）→ `checkAccFatalStop`（:4477）→ `calcFatalLoss`（:4517）。同时存在 runtime 版 `biz/live_runtime.go:MakeCheckFatalStopWithRuntime`（:72）→ `checkRuntimeFatalStop`（:108）→ `Core.SetNoEnterUntil(now+fatalHours)`。

### 10. 远程命令（web / telegram / cli）

- `rpc/telegram.go:1237` 解析订单 ID（仅 `ParseInt`）→ `t.orders().CloseOrder`（:1246）→ `TelegramOrderManager.CloseOrder`（`biz/telegram_order_manager.go:98`，硬编码 `ExitTag: telegram_close`、`Confirmed: true`）→ `RemoteCommandService.CloseOrders`（`biz/remote_command.go:236`）→ `Run`（:80：账户与 NoTrade 校验、幂等键、每 source:actor:action 1 秒限频、审计）→ `runClose`（:145）→ `ExecClose`：有 deps 时 `CloseAccOrdersWithState`，否则 `FindOpenOrders` + `CloseAccOrders`。
- 全部平仓 `CloseAllOrders`（:111）置 `All:true, Confirmed:true`。交易开关 `DisableTrading`/`EnableTrading`（:126/:140）→ `runTradingSwitch`（:156，`SetNoEnterUntil`）。

### 11. 钱包记账

- 回测/模拟：`EnterOd`（wallet.go:1099）→ 成交后 `ConfirmOdEnter`（:1199）；出场 `ExitOd`（:1235）→ `ConfirmOdExit`（:1287）；部分平仓 `CutPart`（:1344）；保证金与爆仓 `UpdateOds`（:1378，`marginRatio > 0.99` 返回 `ErrLiquidation` 并 Reset）。
- 实盘（`w.envReal()`）：Confirm/Exit 直接返回，不做本地记账，余额以交易所为准。
- 同步：`SyncBalancesWith`（:1868）→ `FetchBalance` → `UpdateWalletByBalances`（:1766）；下单前 `odmgr_live.go:2468` 调 `RefreshIfStale`（`WalletStaleMS=1h`）；WebSocket 推送 `WatchLiveBalances`（:1953）。
- 估值：`calcLegal` 以 `valuationPrice` 取价。标记价在最后一根 bar 后 60 秒过期；回测中（`bt_legacy_wallet_price`/`bt_strict` 的兼容钱包除外，保持旧行为）过期后回退到最后一根 bar 价，且持有的基础币（如 `BNB`）无同名 key 时依次尝试 `BNB/{USDT,USDC,FDUSD,BUSD,USD}` 及其永续 `:{quote}`（`lastBarPriceOfCoin`）。仍无价的币被跳过，`skipWarned` 保证每币只打一次 Info，之后降为 Debug。
- 快照：实盘 `live/crypto_trader.go:1604-1608` 启动 `StartLiveWalletSnapshots`，5 分钟写 `ormo.SaveWalletSnapshot`，超 7 天按小时压缩。

### 12. 共享检查点、接管与迁移

- 加载：`shared_order_mgr.go:211/323/1395` → `loadSharedCheckpoint`（`shared_checkpoint.go:36`）：读 `__legacy_bridge/legacy-ts`，活跃订单立即加载，其余按 `legacy-ts/order:id:` 与 lot 键按需读取。
- 保存：`shared_order_mgr.go:350` → `splitSharedCheckpoint`（:182）：按快照 lot 与 allocation 判断活跃键，写 ActiveOrders；命令与订单记录各成条 Record，`StorageVersion=1`。
- 接管：`restoreTakeovers`（`shared_takeover.go:17`）读 `AutoTakeoverRecords`，对未知 lot 生成 `sharedTSOrder`（Tag=`EnterTagThird`，`SourceInfo` 标记 `external_takeover`）；候选 job 为 0 时跳过，由 `BindJobs` 之后补恢复。
- 迁移：`ImportLegacyMigration`（`shared_migration.go:21`）→ `legacyProjectionCheckpoint`（:37）把 `RawLegacyMap` 转为 `sharedTSOrder` → 追加 `__legacy_bridge/legacy-ts` 检查点。

### 13. 远端 Hub 与共享执行（Hub / Shared）

- 选择：`odmgr_local.go:48-53`，`HubOrderBridge != nil` 走 Hub，否则 `SharedExecution != nil` 走 Shared。Live 仅 `odmgr_live.go:100-101` 走 Shared。
- Hub 下单：`HubOrderMgr.EnterOrder` → `enterLocked`（`hub_order_mgr.go:410`：幂等 hash、准入、`BridgeTimeSeriesEntryCommand`、`saveLocked`、`sendLocked`）。命令“先冻结后发送”（:545-551）。
- Hub 投影：`run`（:266，100ms ticker）→ `poll`（`hub_order_projection.go:33`）→ 逐事件 fold、`saveLocked`、`deliverPending`（:623）→ `AdvanceProjection`（:247）。启动：`NewHubOrderMgr`（:107）：flock → 校验 journal identity → `ClaimStrategyWriter` fencing → `resumeCommands`（:313）。
- Shared：`initSharedOrderMgr`（:276）→ `SubscribeCommitted`（:292）→ `project(false)`（:1316）→ `projectSnapshot` 回放（:1660-1811）→ 回调 `dispatchOrderEvent`（:1816）→ `AdvanceProjection`。启动 `NewSharedOrderMgr`（:202）→ `BindJobs`（:252）→ `project(true)` 只推进游标。
- 改单：Hub `EditOrder`（`hub_order_edit.go:12`）→ `editLocked`（:27，乐观并发 `CommandRevision`，冻结键 `<strat>/edit/<id>`）；Shared `EditOrder`（`shared_order_mgr.go:1081`）。

### 14. live 控制器生命周期（live/crypto_trader.go）

1. 组合根（`runtime`）创建 `Runtime`，经 `NewCryptoTraderWithRuntimeDeps`（:161）绑定：`biz.NewTraderWithRuntimeDeps` → 校验 `deps.Exchange` → `bindCryptoTraderRuntime`（:183）注册 `OnClose(cancelRuntime)` 与 `OnCloseWait(runShutdownPhases)`，scheduler 取自 lifecycle（若实现 `Scheduler()`）或 `deps.Scheduler`。
2. `Run` → `runWithDeps`（:670）：持 `runLock`；`resetRuntimeState`（:745）先 join 旧回调，若 runtime 已 cancel 则拒绝；`prepareRunCleanup`（:1256）打开 cleanup 所有权；`beginRuntimeCounter`。
3. `Init`（:202）依次：加载 perf（`config.LoadPerfsWithCoreState`）→ 实盘时 `exg.RequireOrderEventCapability` → `data.NewLiveProviderWithRuntimeDeps`（无 dataDeps 时回退 symbol state）→ `setProvider`（:1043）→ 注册 `PairUpdateHooks`（ExitOrders 经 `biz.GetOdMgrWithState`）→ `ormo.InitLiveTasksWithState`（`deps.Orders` 为 nil 时回退全局 `ormo.InitTask`）→ `orm.InitListDatesWithExchange` → `startWebAPI` → 实盘 `CheckLiveAccountsWithRuntimeDeps` → `initOdMgr`（:432）→ `refreshPairJobs(true)`（选币 + `strat.LoadStratJobsWithState` + `dp.SubWarmPairs`）。
4. 账户检查语义：`fetchAccountBalance`（余额）→ `FetchAccountAccess`（权限/IP/模式）→ 若有 `errors`（余额拉取失败、无交易权限、余额低于 `wallet_amounts` 的 1/3）则 `cfg.NoTrade=true`，该账户此后不建订单管理器、不参与平仓；仅 `warns`（允许提币、IP 不受限）只打日志。禁用只在进程内生效，重启重新检查。
5. `initOdMgr`（:432）：非实盘（dry-run）`RestoreDryRunWalletSnapshotWithRuntimeDeps` 或 `InitFakeWalletsWithRuntimeDeps`，再 `InitLocalLiveOrderMgrWithRuntimeDeps`，`dp.OnDataSeries` 指向 `CallLocalLiveOdMgrsDataWithRuntime`；实盘 `InitLiveOrderMgrWithRuntimeDeps`，并对每个未禁用账户 `SyncExgOrders`（:483）恢复本地订单，结果以 `rpc.MsgTypeStatus` 推送。
6. `startJobs`（:1533）：实盘先 `StartLiveOdMgrWithRuntimeDeps`（订单流）；再注册 cron：选币刷新、小时线、市场加载、致命亏损、K 线延迟、收数汇总、策略输出、回测对比（:1567-1589，多数需 deps 且 core 非 nil）；实盘另加触发单校验（:1594）、余额/仓位轮询（`StartLoopBalancePositionsWithRuntime`，:1601）、钱包快照（`StartLiveWalletSnapshotsWithRuntimeDeps`，:1604）；最后 `scheduler.Start()`。
7. `loopMainFn`（即 `dp.LoopMain`）阻塞运行，行情经 `FeedDataSeries`（:763）→ `feedDataSeries`（:776）：`ResolveDataSeriesSymbol` → 推送 apiServer（非预热）→ `evt.OHLCV` 解析 → 预热走 `handleWarmupSeries`（:878，按 TF 触发批量），实盘走 `handleLiveSeries`（:905，`delayExecBatch` 并写 `DumpKline`）→ `Trader.FeedDataSeries`。
8. `Emit`（:802）是策略/第三方数据源回灌入口：克隆 `Values`（`cloneSeriesValues`，保留类型与 NULL 语义）→ 逐行 `feedDataSeries` → `dp.OnDataSeries`。
9. 停止：`cancelRuntime`（:1219）停 timer、标记 stopped、分离 provider；`runShutdownPhases` 逆序执行 stop 与 join 钩子（先 `LiveOdMgr` 与余额 worker，再回调 join），最后 `finishRunCleanup`（:1297）：`exitCleanUpWithRuntime`（:1652）依次停 scheduler（此处传 nil）、`CleanUpOdMgrWithState`、flush dump、`ExitStratJobsWithState`、按账户发送"bot stop, N orders opened"、关闭 `Notifications`。

## 配置项与外部依赖

- **配置键**（`config/types.go`，默认值见 `config/runtime_config.go:118-136`、`config/biz.go:352-416`）：`stake_currency`、`take_over_strat`、`order_type`、`limit_vol_secs`（默认 10）、`put_limit_secs`（默认 180）、`od_book_ttl`（默认 500）、`stop_enter_bars`、`max_open_orders`、`max_simul_open`、`bt_net_cost`（秒，`odmgr_local.go:349` 乘 1000）、`bt_legacy_intrabar`、`bt_slippage_bps`、`bt_funding_apr`、`bt_gap_bars`、`bt_gap_min_hours`、`bt_delist_penalty_bps`、`fatal_stop_hours`（默认 8）、`leverage`、`wallet_amounts`、`accounts[*].leverage/max_open_orders/no_trade`、`close_on_stuck`、`account_pull_secs`。
- **运行时快照**：`runtimeOrderConfig`（含 `slippageBps/gapBars/gapMinHours/fundingApr/delistPenaltyBps`） 与 `RuntimeDeps.Config.View()`；旧路径读 `config.GetTakeOverTF`（`config/biz.go:618`）、`config.GetAccLeverage`（:625）、`config.DefAcc`。
- **策略级**：`RunPolicyConfig.MaxOpen` / `MaxSimulOpen`（`strat.TradeStrat.Policy`）。
- **环境变量**：数据目录 `BanDataDir`（`biz/biz.go:InitDataDir`，:626）。`biz/*` 中其余文件无 Getenv。
- **交易所（banexg）**：`FetchOpenOrders`、`FetchAccountPositions`、`FetchOrder`、`FetchOrders`、`CreateOrder`、`CancelOrder`、`EditOrder`、`SetLeverage`/`GetLeverage`、`FetchOrderBook`、`FetchBalance`、`WatchMyTrades`、`WatchAccountConfig`、`CalculateFee`；参数 `ParamAccount`、`ParamFullSnapshot`、`ParamRetry`、`ParamAlgoOrder`。exg 适配层：`GetOrderEventCapability`、`BuildClientOrderIDChecked`、`PrecAmount`/`PrecPrice`、`SetOrderCallback`。
- **存储**：K 线与 QuestDB/Timescale（`orm`）；交易订单 `ormo.Conn(orm.DbTrades)`（SQLite，`orm/ormo/base.go:21`）；钱包快照 `ormo.SaveWalletSnapshot`；交易所订单缓存 gob 文件 `{DataDir}/exgOrders/{exg}_{market}_{account}_{apiKey前5位}.gob`（`biz/tools.go:1093/1151`）；Hub journal JSON 文件（绝对路径 `StatePath`）。
- **通知/远控**：Telegram（`rpc/telegram.go`，token/secret 读取 :179-180，secret 需 UUID4 :244-245；`config/biz.go:961-963` 公开配置剔除 token）；i18n：`biz/biz.go:40-43` 嵌入、`biz/biz.go:656-730` 合并、`rpc/telegram_runtime.go:39` `langMsg`。
- **gRPC**：`biz/data_server.go:102` 默认 `:6789`；`:108` 收发 100MB 硬编码。
- **配置文件中的敏感示例**：`biz/config.yml:98`（数据库 URL）与 `:115`（`corp_secret`）为示例值，不是真实凭据。

## 与其他模块的交互边界

- **被调用方**：`entry/runtime_tools.go:36`（NewDataServer）、`entry/mixed_factor_backtest.go:323`、`runtime/runtime.go:975`、`runtime/shared_legacy_live.go:108`、`live/crypto_trader.go`（TryFireBatches*、InitLiveOrderMgr*、SyncExgOrders）、`live/common.go:126`（InitOdSubsWithRuntimeDeps）、`live/common.go:304`（SyncLocalOrders）、`live/runtime_cron.go:363`（VerifyTriggerOds）、`opt/backtest.go:170-173,249,457-460,816`、`web/live/*`、`rpc/telegram.go`（经 OrderManagerInterface）。
- **依赖方**：`execution`（EligibleIntent、PendingCommand、ExitSelection、OHLCProfile、Store、HubClient、SharedAccountBorrow）、`strat`（StratJob 的 DrainOrderRequests / FinishOrderProcessing、FireOdChange(WithState)、CalcJobScores）、`orm` / `orm/ormo`（订单与钱包快照）、`data`（DataDeps、DataSeries）、`goods`、`exg`（价格符号解析 `ResolvePriceSymbol`，统一经此处理合约/现货差异）、`com`（PriceState）、`btime`（ClockState）、`core`（State、常量、ExitTag）、`banexg`。
- **live → biz**：`live` 只做装配与编排，不含订单状态机。通过 `biz.Trader`/`RuntimeDeps`、`biz.TryFireBatchesWithRuntimeDeps`、`biz.Init*OrderMgrWithRuntimeDeps`、`biz.GetLiveOdMgrWithState`、`biz.StartLiveOdMgrWithRuntimeDeps`/`StopLiveOdMgr`/`JoinLiveOdMgr`、`biz.SyncWalletBalancesWithRuntime`、`biz.CloseAccOrdersWithState`、`biz.MakeCheckFatalStopWithRuntime`、`biz.VerifyTriggerOdsWithRuntimeDeps`、`biz.NewRemoteCommandServiceWithRuntimeDeps`、`biz.CleanUpOdMgrWithState` 调用。
- **live → strat/goods/data/opt**：`strat.LoadStratJobsWithState`、`CalcPairTfScoresWithState`、`PrintStratGroupsWithState`、`PairStrategiesView`、`DrainOutputs`；`goods.RefreshPairListWithRuntimeDeps`；`data.NewLiveProviderWithRuntimeDeps`、`DownEmitHourKlinesWithRuntimeDeps`、`dp.GetJobs("ohlcv")`；`opt.BacktestToCompareWithRuntime`（经包级变量 `backtestToCompareWithRuntime`，`runtime_cron.go:149`，便于测试替换）。
- **live → web/rpc**：`web.StartApiWithRuntimeDeps` 启动 API，`apiServer.PublishSeries`；消息经 `sendRuntimeMessage`（`crypto_trader.go:1727`）发往 `deps.Notifications`，旧式回退 `rpc.SendMsg`/`rpc.CleanUp`。
- **依赖 runtime 的契约**：`RuntimeLifecycle`（Context、OnClose、OnCloseWait）与 `data.CallbackTracker`（EnterCallback/LeaveCallback）。这两者由 `runtime.Runtime` 提供；live 不得绕过 `beginRuntimeCallback` 直接触达 provider 回调。
- **交易所差异**：生产代码中未发现按交易所名的 `switch` 分支（已对 biz/ 与 live/ 检索 binance/okx/bybit 等）。但 `odmgr_live.go` 中的 PositionSide/触发类型依赖 banexg 字符串约定（见文末“已知限制与设计取舍”）。

## 扩展与修改指南

- **新增实盘行为**：放 `LiveOrderMgr`（`odmgr_live.go`）或其基类。读配置走 `runtimeCfg`，不要读全局。交易所差异只能在 banexg 适配层实现，不能在 biz 加分支。
- **新增 OrderMgr 方法**：需同步实现 `LocalOrderMgr`、`LiveOrderMgr`、`SharedOrderMgr`、`HubOrderMgr` 四类（`EditOrder` 已有 Local 空实现，`odmgr_local.go:1060`）。
- **新增订单配置**：加入 `runtimeOrderConfig` 与 `makeRuntimeOrderConfig`（`odmgr.go:568`），并保证 legacy 访问器读取一致。
- **撮合/止损/强平**：三处都要调 `callBack` 与 `fireOdChange`，漏一处回调不一致。修改订单字段后置 `DirtyMain`/`DirtyEnter`/`DirtyExit`/`DirtyInfo`，否则不落库。
- **新增触发单类型**：需同时修改 `restoreExchangeTriggersLocked`（:3468）、`exchangeTriggerTypeKey`（:3366）、`matchTriggerExitTradeWithID`（:2419），并补 `odmgr_live_test.go` 用例。
- **新增远程动作**：`remote_command.go` 加常量（:25-26）、`Run` 的 switch、runner 方法；注意 `hasIdempotency`（:96）与限频。Telegram 新命令：先 rpc 解析，再加入 `OrderManagerInterface`（`rpc/telegram.go:72`），实现走 `RemoteCommandService.Run`，不要直接调 ormo；deps 与非 deps 两条分支都要改。
- **改钱包记账**：`wallet.go` 的 Enter/Exit/Confirm 同时服务回测与实盘（实盘 Confirm 直接返回），改动后检查 `wallet_exit_test.go`、`wallet_partial_exit_test.go`、`wallet_liquidation_test.go`。
- **改检查点格式**：同时改 `loadSharedCheckpoint` 与 `splitSharedCheckpoint`，并处理 `StorageVersion` 校验（版本不符直接报错，需显式迁移）。键前缀（`legacy-ts/order:id:`、`legacy-ts/order:lot:`、`legacy-ts/command:`）是持久格式，不可随意改。
- **Hub journal 结构变更**：必须 bump `Bridge.Version`，否则旧 journal 被静默接受（`hub_order_mgr.go:149` 只比较 Version）。
- **改 K 线/数据系列处理**：串行与并行两条路径（`feedClosedSeriesSerial/Parallel`、`onAccountDataSeriesSerial/Parallel`）都要同步。所有回测（不仅 strict）强制串行且按 key 排序（`orderedBacktestFor`，issue 111：此前非 strict 回测按 Go map 随机序处理同一 bar 的多品种，资金不足时先到先得 -> 同参数重跑订单数/UPI 不一致）。
- **易踩坑**：
  - 运行时路径不要调用 legacy 门面（`GetOdMgr`、`strat.GetJobs`、`FindOpenOrders`、`core.Legacy*`）。
  - 确定性遍历用 `determinism.go` 的 `executionMapKeys` / `executionStratJobs`；回测中 map 迭代后用 `sortMapOrdersForBacktest`。
  - `priceExp` 无价格时直接 panic（`odmgr.go:219-225`）。
  - `ExitOpenOrders` 会改写传入的 `req.Force`（`odmgr.go:1336`），`CloseAccOrders` 用 Clone 规避。
  - `ProcessOrders` 必须经 `FinishOrderProcessing` 结束（`odmgr.go:1008-1016`），否则留下活跃 drain。
  - `WithOrderEvent` 只设置 provenance，不做去重（`strat/execution_state.go:101-113`）。
  - 时序路径必须保留 CommandID 与 lot 以支持重试幂等。
  - `BridgeEntryReq` 的 `MaxNotional` 为必填；`LegalCost` 不乘杠杆。
  - 锁顺序：先 `od.Lock()`，再取 `lockExgIdMap`、`lockDoneKeys`、`lockDoneTrades`、`lockUnMatches`、openOrders 锁；不要在持 openOrders 锁时取 od 锁。
- **QuestDB WAL**：`odmgr_live.go` 只读 K 线计算均量（`getPairMinsVol` :3071/3107）。若在 WAL 表上写后立即读，须等待可见性，不得因一次空读清除恢复标记。

## 已知限制与设计取舍

- 实盘 `CreateOrder` 的任何错误（含超时等结果不明确的情形）都会按失败处理并本地关单，之后依赖 `SyncExgOrders`/`SyncLocalOrders` 纠正。
- Telegram 订单管理器固定 `Confirmed: true`，因此 `remote_command` 的二次确认对 Telegram 通道不生效。
- 未匹配到本地订单的成交会被忽略（仅记日志）。
- `odmgr_live.go` 中的 PositionSide 与触发类型依赖 banexg 的字符串约定。
- `RemoteCommandService` 的幂等记录（seen）无淘汰。
- 配置示例文件中的数据库 URL、`corp_secret` 仅为示例值。
