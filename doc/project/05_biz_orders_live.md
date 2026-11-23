# 业务核心模块：biz / 订单管理 / 钱包 / 共享账户 / live

> 描述当前源码的实现，行号仅作定位参考。
> 规约参考：`AGENTS.md`（banexg 统一交易所差异；`DataSeries.Values` 不得收窄；QuestDB WAL 写后读需等待可见性）。
> 架构背景见 `doc/app_arch.md`，运行时见 `doc/runtime_context.md`。

## 定位与职责

- **biz**：交易执行核心。包括 `Trader`（K 线/数据系列分发到策略、批量回调、订单事件）、订单管理器（`odmgr` 系列：本地回测 `LocalOrderMgr`、混合因子+时序回测的共享账户 `SharedOrderMgr`（仅回测）、实盘/dry_run 的 `HubOrderMgr`（内嵌或 Hub 远端，含混合账户的时序部分）；实盘与 dry_run 一律经 execution 模块，旧的 `LiveOrderMgr`/`LocalLiveOrderMgr` 已删除）、钱包（`BanWallets`）、共享账户检查点/接管/迁移、远程命令（web/telegram/cli 平仓与交易开关）、时序命令桥与投影、gRPC 特征数据服务、回测确定性辅助、离线工具（K 线导入导出、回放、订单缓存）。
- **live**：execution 机器人借用的外壳 `CryptoTrader`（仪表盘 API、scheduler、关停相位）、账户检查（`account_check.go`）、运行时定时任务（`runtime_cron.go`、`execution_crons.go`）、平仓工具（`tools.go`）。不再有行情循环、订单管理器注册与账户同步。
- 边界原则：运行时路径使用 `*WithRuntimeDeps` / `*WithState` 版本，避免读取包级全局状态；旧门面（`GetOdMgr`、`strat.GetJobs`、`FindOpenOrders`、`core.Legacy*`）仍存在并与新路径并行。

## 目录/文件地图

### biz/（非测试文件）

| 文件 | 作用 |
|---|---|
| `biz/biz.go` | 启动装配 `SetupComs`/`SetupComsExg`；选币、任务刷新；订单事件订阅 `InitOdSubsWithRuntimeDeps`；批量任务 `AddBatchJob`/`TryFireBatches`；`applyHedgeOff`、`flushCallbackOrders`（execution 路径回调后的 HedgeOff 与请求冲刷，见「订单事件」） |
| `biz/trader.go` | `Trader` 与 `RuntimeDeps` 定义；`FeedDataSeries` 分发；串行/并行的账户策略回调 |
| `biz/runtime_deps.go` | `RuntimeDeps` 投影为 `data.RuntimeDeps` 副本 |
| `biz/runtime_logger.go` | Trader/RuntimeDeps/OrderMgr 的 logger 取值 |
| `biz/runtime_state.go` | `TradingState`：按账户注册 OrderManager / Wallet |
| `biz/live_runtime.go` | 批量平单 `CloseAccOrdersWithState`（`close_on_stuck`、CLI 平仓共用） |
| `biz/bt_baseline_test.go` | 回测引擎基准：合成行情下走完整 Trader→策略→订单管理→钱包路径；`TestBacktestBaselineGolden` 对比 `testdata/bt_baseline` 的订单明细与权益曲线（数值相对容差 1e-7，`BANBOT_UPDATE_BASELINE=1` 重建），`BenchmarkBacktestBaseline` 度量同一负载。改动回测撮合或记账必须保持其通过 |
| `biz/determinism.go` | 回测确定性：**所有**回测（`config.OrderedBacktest()` / `RuntimeDeps.OrderedBacktest()`，与 `bt_strict` 无关）按稳定键（账户名/job key/开仓时间+ID）遍历 map；品种间同 bar 先后由 `data/provider.go` 回放堆的哈希序决定（`bt_bar_order_seed`，issue 112，BtFeatureLevel 6），本文件只负责单品种内部的稳定顺序（`executionOpenOrders` 先抽取（开仓时间, ID）键再排序，避免比较器内重复取值）；`bt_strict` 仅再控制重放覆盖与兼容钱包语义（`strictBacktestFor`） |
| `biz/data_server.go` | gRPC FeaFeeder 服务（默认 `:6789`，收发上限 100MB 硬编码） |
| `biz/remote_command.go` | `RemoteCommandService`：校验、幂等、1 秒限频、审计；平仓与交易开关 |
| `biz/job_orders.go` | `ProcessJobOrders`：策略异步回调发起下单；绑定订单处理器与账户资金读取器 |
| `biz/account_metrics.go` | 账户实时权益、可用余额与峰值回撤读取（兼容 `BanWallets`、`SharedAccount`、`HubOrderMgr`、`HubAccountReader`），注入 `strat.State` 并在非预热 K 线推进时更新账户 HWM |
| `biz/hub_account_reader.go` | `HubAccountReader`：Hub 账户的只读缓存视图（仅 `Snapshot` 与 `VisibleQuote` 查询），因子-only Hub 机器人经 `BindHubAccountReader` 绑定，时序 Hub 机器人经 `hubTSAccountReader` 惰性构建 |
| `biz/odmgr.go` | `IOrderMgr` 接口；`OrderMgr` 基类（入单、出单、利润、追踪止损、注册表、`CloseAccOrders`） |
| `biz/odmgr_local.go` | `LocalOrderMgr`：回测/模拟撮合（挂单、止损止盈、限价超时、清仓） |
| `biz/bar_gap.go` | 回测 K 线空洞守卫 `guardBarGap`（`ExitTagDataGap`）与退市平仓 `exitDelisted`/`sweepDelisted`（`ExitTagDelist`）；定价与成交时间规则委托 `execution/backtest_rules.go` |
| `biz/shared_order_mgr.go` | `SharedOrderMgr`：仅回测（混合因子+时序回放）使用的共享账户订单门面（下单/撤单/平仓/改单、准入、投影）；`runtime.Options.SharedOrderBridge` 在非回测模式下被拒绝 |
| `biz/shared_checkpoint.go` | 共享检查点加载/拆分存储（活跃键 + 按需读取历史） |
| `biz/shared_account.go` | `execution.SharedAccount` 的别名与构造转发 |
| `biz/ts_loss_stop.go` | `fatal_stop` 的 execution 路径实现：`timeSeriesLossStop` 把配置换算为 `execution.LossStopPolicy`（非法条目报错，回测不启用），`applyLossStopPause` 把 owner 的暂停截止时间写入本 runtime 的 `NoEnterUntil`（不缩短已有开关）。`SharedOrderMgr` / `HubOrderMgr` 构造时登记策略，每次开仓准入前同步状态 |
| `biz/ts_order_archive.go` | 已平仓订单的只读归档：`EnsureTimeSeriesOrderTask`（实盘 runtime 创建/加载订单库任务，由 `runtime.BindFactorLegacyJobs` 调用，失败只告警）、`orderArchiver.archive`（把已平仓投影经 `ormo.OrderState.ArchiveOrder` 写入订单库，失败后 30 秒退避重试）。只有 `HubOrderMgr` 归档；`SharedOrderMgr` 的回放视图不含成交均价与盈亏，不归档 |
| `biz/ts_admission.go` | `admitTimeSeriesEntry`：execution 支撑的时序管理器共用的开仓数量准入（`max_open_orders`、`max_simul_open`、策略 `MaxOpen`/`MaxSimulOpen`），`SharedOrderMgr` 与 `HubOrderMgr` 都调用它；计数状态由各自的检查点/journal 持久化 |
| `biz/hub_order_mgr.go` | `HubOrderMgr`：远端 Hub 的机器人侧门面，journal 持久化、写者租约 fencing、命令冻结与重放 |
| `biz/hub_order_projection.go` | Hub 投影：poll 已提交事件，折叠为 `hubProjectedLot`，投递回调 |
| `biz/hub_order_edit.go` | Hub 改单：转为 PendingCommand 冻结后发送 |
| `biz/hub_order_ladder.go` | Hub 路径的阶梯止盈与 OCO：机器人侧在既有 owner 命令之上编排（`hubLotPlan`/`hubLadder`/`hubFollowup`，`orchestrateLocked`），见「Hub 阶梯止盈与 OCO」 |
| `biz/hub_callback_orders.go` | `refreshJobForCallback`：回调前给 job 装入「开放订单注册表 + 本次事件订单」的视图（Hub 只在一页投影结束时才发布注册表） |
| `biz/order_notify.go` | `NewOrderMessage`：成交腿的 entry/exit 通知载荷构建，`live` 路径与 execution 路径共用 |
| `biz/ts_price_tick.go` | 时序请求的价格 tick 取整：`roundToTick`、`roundEntryLimits`、`exitTick` |
| `biz/hub_order_lock_unix.go` / `_windows.go` | journal 独占文件锁（flock / LockFileEx） |
| `biz/intent_bridge.go` | 旧式 `strat.EnterReq`/`ExitReq` 翻译为 `execution.EligibleIntent`，数量换算与风控上限 |
| `biz/time_series_command_bridge.go` | 时序策略请求封装为可持久化 `execution.PendingCommand` |
| `biz/time_series_projection.go` | 时序“已提交事件”投影消费者 `TimeSeriesProjectionConsumer`（非测试代码无调用） |
| `biz/telegram_order_manager.go` | 实现 `rpc.OrderManagerInterface`，Telegram 命令转 `RemoteCommandService`；经 `NewRuntimeNotifications` 装入 `rpc.Session`（包级 `InitTelegramOrderManager` 随旧栈删除） |
| `biz/tools.go` | 离线工具：zip K 线导入、日历导入、K 线导出/清理/聚合、相关性、历史回放、交易所订单缓存（gob） |
| `biz/wallet.go` | `BanWallets`/`ItemWallet`：余额、pending/frozen 占用、成交结算、保证金、爆仓、同步（回测/模拟记账；实盘余额快照写入已随旧栈删除，`orm/ormo/wallet_snapshot.go` 仅保留存储层） |
| `biz/aifea.pb.go` / `aifea_grpc.pb.go` | 生成代码（不纳入人工维护） |
| `biz/config.yml`、`biz/config.local.yml` | 嵌入二进制的默认配置（首次写入数据目录） |
| `biz/zh-CN/`、`biz/en-US/` | 文案与 `messages.json`（两份 key 集合一致，各 154 个） |

### live/（非测试文件）

| 文件 | 作用 |
|---|---|
| `live/crypto_trader.go` | `CryptoTrader`：execution 机器人借用的外壳。`NewCryptoTraderWithRuntimeDeps` 绑定 `biz.Trader` 与 lifecycle；`StartWebAPI`/`PublishSeries` 管理仪表盘 API；`schedulerForRun`/`currentTimeMS` 供维护任务取用；`runtimeShutdownLifecycle` 把停止/等待钩子接入 `runtime`；关停相位 `runShutdownPhases`（先逆序 stop 再 join）。不下单、不驱动行情 provider |
| `live/account_check.go` | 启动前账户体检：`VerifyLiveAccountsWithRuntimeDeps` 返回错误（内嵌 provider 机器人失败即终止启动）；`CheckAccountWithExchange` 对显式交易所返回无副作用的 `AccountCheckReport`（Hub 生产账户注册用）。检查项：交易/提币/IP 权限、余额与 `wallet_amounts` 对比、持仓/账户/保证金模式 |
| `live/execution_crons.go` | `StartExecutionRobotCrons`/`ExecutionRobotCronOptions`：execution 路径的维护任务，见「execution 路径的维护任务」 |
| `live/runtime_cron.go` | cron 注册（`*WithRuntime` 版本）：小时线补齐 `fetchHourKlinesFromWithRuntime`、市场加载、K 线延迟监控（含 `close_on_stuck`）、10 分钟收数汇总、策略输出落盘；`runtimeRunGate` 防止同一任务重入；`sendRuntimeMessage` |
| `live/tools.go` | CLI/运维平仓 `CloseOrdersWithRuntimeDeps` 经 `biz.RemoteCommandService` 执行；只接受已绑定 execution 账户（内嵌 `SharedExecution` 或 Hub 桥）的 runtime，否则报 `closing orders requires a bound execution account`；`closeLocalOrdersWithRuntimeDeps` 经账户 `SharedOrderMgr`/`HubOrderMgr` 的 `ExitOrder` 平仓，不触碰交易所原始持仓 |

均为非测试文件；测试见 `live/*_test.go`。

## 核心类型与接口

- **`Trader`**（`biz/trader.go:31`）：持有 batchState 与 `runtime *RuntimeDeps`。构造 `NewTraderWithRuntimeDeps`（:131）只校验绑定，不补齐依赖；`BindRuntimeDeps`（:151）由组合根一次性绑定。
- **`RuntimeDeps`**（`biz/trader.go:46`）：单个 runtime 的全部可变状态：Core、Clock、Market、Batch、Strategies、Orders、Trading、Config、Accounts、Symbols、Storage、Exchange、Notifications、DefaultAccount。
- **`TradingState`**（`biz/runtime_state.go:9`）：`orderManagers` / `wallets` 两张按账户注册表（`Wallet` 懒创建，`LookupWallet` 只读不分配）。`Reset` 不关闭管理器，调用方须先等回调结束。
- **`IOrderMgr`**（`biz/odmgr.go:33`）：`ProcessOrders`、`EditOrder`、`RelayOrders`、`EnterOrder`、`ExitOpenOrders`、`ExitOrder`、`UpdateByDataSeries`、`ExitAndFill`、`OnEnvEnd`、`CleanUp`。
- **`runtimeOrderConfig`**（`biz/odmgr.go:59`）：绑定时从 `RuntimeDeps.Config` 拷贝的不可变快照；`makeRuntimeOrderConfig` 填默认值（LimitVolSecs=10、BTNetCost=15）。
- **`OrderMgr`**（`biz/odmgr.go:75`）：基类。每个访问器都有 `runtimeDeps` 双路径（有 Runtime 用注入依赖，否则回退包级全局）。改动时两条路径必须同步。
- **`LocalOrderMgr`**（`biz/odmgr_local.go:24`）：嵌入 `OrderMgr`，附 `simMu`（仅回测 ProcessOrders 加锁，:141-147）、`stopBacktest`。
- **`SharedOrderMgr`**（`biz/shared_order_mgr.go`，仅用于回测（混合因子+时序回放）；实盘/dry_run 一律 `HubOrderMgr`，含同账户混合因子/时序机器人的时序部分，不再有共享门面选项）：`projecting`/`projectionPending` 合并并发投影；`sharedTSCheckpoint`（:102）含 Serial、Orders、Feeds、Admission、Commands、Accepted。
- **`HubOrderMgr`**（`biz/hub_order_mgr.go:80`）：`journal *hubOrderJournal`（:64，整体 JSON 落盘）、`polling`/`delivering` 投影标志、`lockFile`。`HubOrderBridgeConfig`（:28）要求绝对 `StatePath`；`Client` 的类型是 `execution.HubPort`（`*HubClient` 经 socket，或 `*LocalHubPort` 进程内），`Validate` 与 `NewHubOrderMgr` 用 `hubPortNil` 拒绝 nil 及装在接口里的 typed-nil（`*HubClient`/`*LocalHubPort`）。
  - `LocalAccount`（内嵌生产时序机器人，以及同账户混合因子+时序机器人的时序部分，唯一的门面，见 `08_entry_opt_web_rpc.md`；混合账户中因子 runner 直接使用同一借用账户，策略 ID 与 TS 桥互不相同）：`Client` 必须是 `*LocalHubPort`，且服务的是本 runtime 已借用的 `SharedExecution` 账户。`NewHubOrderMgr` 仅当 `LocalAccount` 且 `deps.SharedExecution.AccountKey()` 等于 `Account` 时允许 `SharedExecution != nil`；`SharedOrderBridge` 恒须为空。此时没有 Hub 宿主观察公共 feed，`UpdateByDataSeries` 对已收盘且非预热的 bar 调用 `ObservePendingFeed`（feed 为 `PendingFeedID(sharedFeedKey(sid, 周期))`），使 `StopBars` 过期推进，随后照常 `poll`；账户指标直接读该账户一次（`account_metrics.go`，使用桥的品种与标记价），不再经端口。
  - `runtime.NewRuntime` 对 `LocalAccount` 桥的准入条件（`runtime/runtime.go`）：`SharedExecution` 非空、`AccountOwnerKey` 等于桥的 `Account`、`SharedOrderBridge`/`LegacyExecution` 为空、`HubMarketData=false`、`SharedMarketData=true`、`Env=prod` 且 `Mode=live`，并通过 `bridge.Validate()`；否则报错 `local-account Hub bridge requires one production runtime borrowing the bridged account`。`SharedOrderBridge` 本身只在 `Mode=backtest` 被接受，其他模式报错 `shared legacy bridge (SharedOrderMgr) is backtest-only`。非 `LocalAccount` 的 Hub 桥仍要求无 `SharedExecution`/`SharedOrderBridge`/`LegacyExecution`/`AccountOwnerKey`/`SharedMarketData`。
- **`hubProjectedLot`**（`biz/hub_order_projection.go:17`）：`Record`、`Entered/Exited`、成本/费用/已实现盈亏、`Terminal`。
- **ormo 订单模型**：`InOutOrder`（`orm/ormo/order.go`）、`ExOrder`、`IOrder`、`TriggerState`、`OrderState`。
- **`ormo.OrderState.ArchiveOrder`**（`orm/ormo/archive.go`）：把一个已平仓订单视图复制为订单库的新历史行（自增 ID，入场/出场腿归属同一任务，不修改传入视图），按 `Info["archive_key"]` 与平仓时间去重；未成交或未平仓的视图返回 0。
- **`TimeSeriesEntryPayload` / `TimeSeriesOrderEvent`**（`biz/time_series_command_bridge.go:17`、`biz/time_series_projection.go:15`）：持久化与投递的事件结构。
- **`RemoteCommand` / `RemoteCommandService`**（`biz/remote_command.go:29`、`:52`）：含 seen 幂等表与 lastHit 限频表。
- **`IntentBridgeContext` / `EntryIntentBridge` / `ExitIntentBridge`**（`biz/intent_bridge.go:18/35/151`）。
- **`CryptoTrader`**（`live/crypto_trader.go`）：嵌入 `biz.Trader`，另持 `dataDeps`、`apiServer`、`scheduler`、`runtime`（`RuntimeLifecycle`）、`shutdownJoins/Stops`。构造 `NewCryptoTraderWithRuntimeDeps` 要求 `deps.Callbacks` 实现 `RuntimeLifecycle` 且 `deps.Exchange` 非空，否则拒绝。
- **`RuntimeLifecycle`**（`live/crypto_trader.go`）：`Context()`、`OnClose(fn)`（取消阶段执行）、`OnCloseWait(fn)`（等待阶段执行）。实现方为 `runtime.Runtime`，经 `runtimeShutdownLifecycle` 适配给 `web.StartApiWithRuntimeDeps`。
- **关停两段式**：`OnClose` 注册的停止钩子在取消阶段运行；`runShutdownPhases`（由 `OnCloseWait` 注册）调用 `stopAndJoinRegisteredResources`，逆序先 stop 后 join。关停开始后再注册的钩子立即执行。
- **`runtimeRunGate`**（`live/runtime_cron.go`）：包装 `sync.Mutex`，`run` 用 `TryLock`（重入则跳过本次 cron）。
- **`accCheckResult`**（`live/account_check.go:18`）：权限三元组（`*Allowed`/`*Known`）、IP、持仓/账户/保证金模式、余额摘要、`errors`（触发禁用）与 `warns`（仅告警）。
- **`TradeCloseRequest` / `TradeCloseResult`**（`live/tools.go`）：CLI 平仓请求（Accounts/Pairs/Strategies 过滤、Exchange、Confirmed）与结果计数。

## 关键流程

### 1. 启动与运行时绑定

1. `biz/biz.go:SetupComs`（:46）：InitDataDir、`config.LoadConfig`、`core.Setup`、`utils.Setup`、`exg.Setup`、`orm.SetupWithAutoCompact`、`goods.Setup`。`SetupComsExg` 另调 `orm.InitExg`。
2. `runtime/runtime.go:975` 调 `biz.BindRuntimeDeps`，绑定后设置订单 trades 路径 `orders_<name>.db`。
3. 实盘与 dry_run：由 `entry` 构造 execution 机器人（`HubOrderMgr` / 内嵌 `LocalAccount` 桥），`live.NewCryptoTraderWithRuntimeDeps` 只提供外壳；订单对账与恢复属于 execution 账户 owner（见 `06_execution.md`）。`env: test` 与非 linear 市场在启动时被拒绝（见「支持矩阵」）。
4. 选币与任务（回测）：`biz/biz.go:RefreshPairsWithRuntimeDeps` → `goods.RefreshPairListWithRuntimeDeps` → `strat.CalcPairTfScoresWithState`；`RefreshJobsWithRuntimeDeps` → `strat.LoadStratJobsWithState`。轮出的 pair 经 `odMgr.ExitAndFill` 平仓。execution 实盘路径的 pair 在启动时固定，不做轮动。`RefreshPairsWithSymbolState` 在 symbols 非 nil 时直接报错（`biz/biz.go:123`），须用 RuntimeDeps 版本。

### 2. 数据系列到策略

- `biz/trader.go:FeedDataSeries`（:544）→ `ResolveDataSeriesSymbol`（:403）→ `feedClosedSeries`（:644，更新价格、`UpdateByDataSeries`、`OnEnvSeries`）→ 串行 `feedClosedSeriesSerial`（:731）或并行 `feedClosedSeriesParallel`（:774）→ `onAccountDataSeries`（:824）→ `onAccountDataSeriesJob`（:1059：OnData/OnBar → 批量任务 → CheckCustomExits → ProcessOrders）。
- 非 K 线系列：`feedDataOnlySeries`（:574）→ `deliverDataOnlySeries`（:627）。
- 批量：`biz/biz.go:AddBatchJobWithRuntimeDeps`（:480，延迟 `core.DelayBatchMS`）→ `TryFireBatchesWithRuntimeDeps`（:517）；warmup 时不下单。
- 致命熔断：`trader.go:onAccountInfoSeries`（:883）中 `env.VNum > 1000` 且持续新增时 `setBotRunning(false)`（:922）。

### 3. 订单事件

- `biz/biz.go:InitOdSubsWithRuntimeDeps`（:367）→ 每账户 AddOdSub → LookupJob → 实盘刷新最新价 → HedgeOff 时 closeSideOrders → `OnOrderChange` → ProcessOrders → FinalizePairRotation。
- execution 支撑的管理器（`SharedOrderMgr.dispatchOrderEvent`、`HubOrderMgr` 的 `deliverPending`）不经上述订阅，自行派发：`strat.FireOdChangeWithState` → （Hub 先 `refreshJobForCallback`）→ `applyHedgeOff`（`HedgeOff` 策略在入场成交事件上平掉反向订单，`closeSideOrders`，标签 `core.ExitTagHedgeOff`）→ `OnOrderChange`，整段在 `job.WithOrderEvent(事件ID, …)` 内执行；随后 `flushCallbackOrders` 在管理器互斥锁之外立即提交回调里排队的请求并收尾轮出的 pair（`ProcessOrders` 的递归调用在 drain 进行中立即返回，由外层 drain）。请求的命令 ID 由事件 ID 派生，投递在确认（`AdvanceProjection`）前崩溃重放时复用同一 ID，不会重复下单。
- `RemoteCommandService.CloseOrders` 跳过已处于终态（`Status >= InOutStatusFullExit`）的订单：execution 路径的开放订单注册表保留终态投影。

### 4. 策略意图进入订单管理器

- 旧式请求：`shared_order_mgr.go:632` → `BridgeEntryReq`（`biz/intent_bridge.go:79`）：Amount 优先，否则 LegalCost，再否则 StrategyNAV×StakeNAVFraction×CostRate（CostRate 为 0 视为 1；`StakeNAVFraction` 来自 `stake_pct/100`，为 0 时取 `stake_amount ÷ 该策略资本`，资本 = 账户初始净值 × `capital_weight`，见 `entry/mixed_factor_live.go`）；入场时策略 NAV = 策略合成现金 + 该策略各 lot 的 `VirtualLot.EquityValue(中间价)`，`SharedOrderMgr`（`shared_order_mgr.go`）与 Hub 路径同口径（现金结算品种的 lot 加市值，永续等于未实现盈亏）；超出 MaxNotional 报错（不截断）；`Intent.Evaluate` 校验。出场：`BridgeExitReq`（:188）先 `ExitSelection.Select`，再按 Amount（先减持仓再撤挂单）或 ExitRate 计算。
- 时序命令桥：`hub_order_mgr.go:575` → `BridgeTimeSeriesEntryCommand`（`time_series_command_bridge.go:28`）复用 `BridgeEntryReq`，清空 StopBars/CreatedBar（由 owner 按持久化 feed 进度绑定）；出场 `hub_order_mgr.go:690` → `BridgeTimeSeriesExitCommand`（:62）。命令在重试间必须保持稳定的 lot 与 CommandID。

### 5. 回测/模拟撮合（LocalOrderMgr）

- `odmgr_local.go:UpdateByDataSeries` → `fillPendingOrdersAll`（回调中新开单，最多 30 轮）→ `fillPendingOrdersPass`（经 `OrderMgr.effectiveMatchTimeFrame` 解析 `refine_tf` 子周期撮合，若合成单测未推送子周期 bar 则自动回退主周期且不重复撮合；经 `execution.OHLCProfile.MatchPending` 撮合，同一根 bar 内多笔非 OCO 待成交挂单按 `(fill.TimeMS, fill.Rate, origIdx)` 时间先后顺序成交，bar 内由 `OnOrderChange` 新建的挂单经 `OHLCProfile.matchMidBarPending` 仅在创建时刻加 `bt_net_cost` 之后的剩余 bar 路径上按严格因果序匹配；待成交子单在 `evt.EndMS <= CreateAt`（如收盘时刻创建、而 refine bar 恰在该时刻结束）时跳过该 bar，下一根 bar 才撮合；`OrderMgr.UpdateByDataSeries` 在入场未成交（`Status < InOutStatusPartEnter`）时不推进跟踪止损；支持 `Stop-Limit` 先按触发价激活再按限价成交或转为挂单；对同一 `(Account, Symbol, Strategy, OCOGroup)` 的待入场挂单组由 `matchOCOPendingGroup` 按 bar 内 OHLC 路径触发先后选出最早成交腿并由 `cancelOCOSiblings` 立即撤销其余未成交兄弟单）→ `fillPendingEnter`（`wallets.EnterOd`/`ConfirmOdEnter`、`od.ResolveTakeProfits`）或 `fillPendingExit`（`wallets.ExitOd`/`ConfirmOdExit`、`finishOrder`）。
- 止损止盈与阶梯止盈：`tryFillTriggers` → `MatchProtection`；若配置多级 `TakeProfits`（`ormo.TPLadderState`），按入场成交均价解析 `Val/Rate` 并按盈利方向由近及远逐级触发，部分平仓经 `CutOrder` 切分退出并在 `MoveSLToBreakeven` 为真时调用 `InOutOrder.TightenSLToBreakeven` 将剩余仓位止损收紧至不差于入场均价，随后在同一 bar 剩余价格路径上继续评估后续止盈级或保本止损。
- 限价超时：按 `OdInfoStopAfter` 调 `localExit`（`ExitTagEntExp`）。
- 回调三处触发：`fillPendingEnter`、`fillPendingExit`、`tryFillTriggers` 在 `LocalOrderMgr.withSimFillContext` 中以当次模拟成交时间与成交价作用域触发 `callBack(od, isEnter)` 与 `strat.FireOdChange`（`biz/odmgr.go:fireOdChange`），随后恢复当前 bar 收盘时钟与价格，使 `OnOrderChange` 内追加的反向限价单/平仓单具备因果创建时间戳且无前视泄漏。`OrderMgr.canClose` 按 `refineTimeFrame`（或模拟撮合回调态 `inSimOrderMatch`）判定可平仓时点。
- 滑点：`LocalOrderMgr.ohlcProfile()` 按 `bt_slippage_bps` 构造 `OHLCProfile.SlippageBps`；市价单、市价止损入场、止损/止盈出场及强平类出场（`ExitAndFill`）按不利方向加滑点，止损不优于开盘跳空价；限价单不加滑点。
- 资金费：`OrderMgr.updateOrderFee` 在回测出场腿调用 `settleBacktestFundingWithMode`（费用计算委托 `execution.RealFundingCost`/`ApproxFundingCost`，本包只负责写入订单）。当标的在存储表 `funding_rate_8h` 或回放流中存在真实 `funding_rate` 结算记录时，按持仓跨越的 `(RealEnterMS, ExitMS]` 结算点双向计费（多头正费率扣款、负费率收款；空头正费率收款、负费率扣款），计入 `Exit.Fee/FeeQuote` 并写订单 info `funding` 与 `funding_mode=real`；当无 `funding_rate` 数据时回退 `addApproxFundingWithMode`（默认双边对称 `addApproxFunding`），对线性合约单按 `bt_funding_apr` 计算 `入场名义 × 年化 × 持仓年数`（正向费率多头支付 `+cost`、空头收取 `-cost`，等名义 1:1 多空对冲净资金费为 0；显式配置 `bt_legacy_approx_funding: true` 时回退单边仅扣多头口径）；反向币本位合约一律跳过并标 `funding_skipped=inverse`。
- 数据空洞与退市（`bt_gap_bars>0` 且非实盘）：`Trader.feedClosedSeries` 在登记新 bar 价格前调用 `guardBarGap`——上一根 bar 结束到新 bar 开始的缺口同时超过 `bt_gap_bars` 个周期与 `bt_gap_min_hours`（默认 48）小时时，该标的未平订单以最后已知价格强平（tag `data_gap`）；`sweepDelistedAll` 每 6 小时模拟时间扫描一次，按同一规则把数据已终止的标的强平（tag `delist`），`CleanUp` 开头也会执行。`delist` 成交时间取该标的最后有效 bar 的时间（`PriceState.GetLastBarTimeAt`），保证金、持仓时长与 `max_open_orders` 槽位随之释放；`data_gap` 在数据恢复时触发，成交时间为 `min(最后 bar + 确认延迟, 恢复时间)`（`gapFillTime`；确认延迟 = `max(bt_gap_bars×周期, bt_gap_min_hours)`），不早于最后 bar。成交价由 `applyExitPolicy` 决定：`data_gap` 取最后收盘价与缺口后首根开盘价中对持仓更差者（`ExitReq.GapNextOpen/GapRecoverMS`），`delist` 取最后收盘价再按 `bt_delist_penalty_bps` 向不利方向偏移；之后仍叠加 `bt_slippage_bps`。
- 入场后伪时间 +1ms（`makeLocalAfterEnter`，:919-931），防止同 key 钱包扣款冲突。

### 6. 支持矩阵（旧 live 订单栈已删除）

生产实盘与 dry_run 一律经 `execution` 模块（`HubOrderMgr`、Hub、内嵌 `LocalAccount` 桥）；回测仍是 `LocalOrderMgr` + `BanWallets`（行为不变，由 `TestBacktestBaselineGolden` 守护）。旧栈（`LiveOrderMgr`、`LocalLiveOrderMgr`、`CryptoTrader.Init/Run`、余额/仓位轮询、触发单校验、选币轮动 cron、钱包快照写入）已删除。仅旧栈提供的能力分三类：**支持**；**拒绝**（启动或请求时返回明确错误）；**忽略**（配置键被忽略，启动时由 `entry/execution_robot.go:ignoredExecutionConfigKeys` 告警，不阻止启动）。以后把某项迁入 `execution`，先读本表的迁移说明。

| 能力 | 状态 | 当前行为 | 迁入 execution 需要做什么 |
|---|---|---|---|
| 对冲（双向）持仓模式 | 拒绝 | Hub 账户注册时体检报硬错误；适配器遇到 `Hedged` 持仓报错（`execution/banexg_adapter.go`） | 账本 lot 与场所快照按多/空腿分开，净额与对账模型改为每品种两条腿 |
| 现货（`market_type: spot`/`margin`） | 拒绝 | 启动时 `rejectUnsupportedLiveEnv` 报 `market_type "spot" is not supported by the execution path (linear USDT-margined perpetuals only)…`；时序/Hub 入口同样要求 linear | 现货现金与基础币账本、借贷、无空头（owner 已有 `spot_short` 拒绝码）、手续费以基础币扣除 |
| 反向/交割合约（inverse、带到期日） | 拒绝 | `execution/banexg_instrument.go` 拒绝；启动时 `market_type: inverse` 被拒 | 币本位盈亏与保证金公式、交割换月与到期平仓 |
| 交易所原生止损/止盈/追踪 | 支持 | 入场请求的 `StopLoss/TakeProfit/TakeProfits/CallbackPct/ActivationPrice` 转为 execution 保护单；`protection_mode: software`（默认）由引擎监控，`native_preferred` 优先挂交易所条件单 | 已在 execution 内（`protection.go`、`pending_native.go`）；旧的 `exg_stop_loss=false` 单项开关无对应键 |
| 单笔订单杠杆不同于账户 | 拒绝 | `HubOrderMgr` 报 `Hub leverage changes are unsupported`；杠杆只能在账户声明（owner 经 `SetLeverage` 应用，内嵌 provider 不支持） | 把杠杆纳入保护/准入的逐 lot 属性，并让 owner 按 lot 切换或限制 |
| 分档杠杆（按名义价值自动截断并缩量） | 拒绝 | 不自动适配；超出档位的下单由交易所拒绝并作为错误返回 | owner 读取分档表，下单前截断杠杆并按比例缩量，保证金预算同步 |
| 测试网（`env: test`） | 拒绝 | 启动报 `testnet live trading is not supported by the execution path; use env: dry_run or prod` | execution 的 Hub/账户 owner 增加 testnet 会话标识与凭据域，并在适配器切换端点 |
| 追加保证金（`margin_add_rate`） | 忽略 | 告警；风险由 owner 的 `risk_limit` 准入与亏损熔断承担 | owner 周期检查维持保证金率并调用追加保证金接口 |
| 爆仓扣费（`charge_on_bomb`） | 忽略 | 告警；仅回测/模拟钱包使用 | 账本在强平事件上扣除残余保证金 |
| 限价摆放（`put_limit_secs`、`odbook_ttl`） | 忽略 | 告警；限价由 `order_mode`、`limit_vol_secs`（`DefaultLimitPrice`）与 `near_limit_bps` 决定，不再按订单簿估算成交等待并回退本地触发 | owner 读取订单簿估算成交时间，远离成交的限价保持本地待发 |
| 账户轮询间隔（`account_pull_secs`） | 忽略 | 告警；余额与权益来自账户 owner 的账本和场所快照 | owner 增加可配置的快照拉取周期 |
| 选币轮动（`pairlists`、`pairmgr.cron`） | 忽略 | 告警；execution 路径的 pair 在启动时固定 | 订阅计划与账本支持运行中增删品种，被移出品种的持仓平仓 |
| 动态 `UpdatePairs` | 拒绝 | 请求时返回 `pair updates are not available in this runtime…`（`strat/pair_update.go`） | 同上，并把 `PairUpdateHooks` 接到 execution 订阅 |
| 实盘内回测对比（`bt_in_live`） | 忽略 | 告警；对比实现已删除 | 需要重新实现并读取 execution 账本的订单视图 |
| 致命亏损熔断（`fatal_stop`） | 支持（语义变化） | 由账户 owner 按权益回落度量并持久化暂停（`ts_loss_stop.go`），不再按已平仓订单亏损求和 | 已在 execution 内；窗口与口径见 `06_execution.md` |
| 接管策略（`take_over_strat`） | 支持（部分） | 外部接管记录经 `hub_order_takeover.go` 恢复为策略订单；旧栈自动把交易所用户持仓生成“用户开仓”跟踪单的流程不再存在 | owner 导入场所持仓并按接管策略分配 lot |
| 改单 | 支持 | `hub_order_edit.go` 支持限价入场/出场、止损、止盈、追踪的改单；旧栈直接修改交易所条件单的路径已删除 | 已在 execution 内 |
| `close_on_stuck` | 支持 | 延迟监控经 `CloseAccOrdersWithState` 调用账户管理器 `ExitOrder`，只覆盖 runtime 已注册管理器的账户 | 多账户 runtime 需为每个账户注册管理器 |
| 钱包快照落库 | 拒绝（不写入） | 不再写 `wallet_snapshot` 表；存储层保留 | 由账户 owner 周期写入权益与余额快照 |
| `--out` 实盘 dump | 拒绝 | `rejectExecutionDump` 报错 | 把 dump sink 接到 execution 的行情与命令流 |

### 7. 熔断

- 账户 owner 按权益回落度量并持久化暂停状态（`SharedOrderMgr` / `HubOrderMgr` 构造时登记 `ts_loss_stop.go` 的策略），见 `06_execution.md` 的"亏损熔断"。旧的按已平仓订单亏损求和的 cron 熔断（`fatal_stop` 旧实现）已删除。

### 8. 远程命令（web / telegram / cli）

- `rpc/telegram.go:1237` 解析订单 ID（仅 `ParseInt`）→ `t.orders().CloseOrder`（:1246）→ `TelegramOrderManager.CloseOrder`（`biz/telegram_order_manager.go:98`，硬编码 `ExitTag: telegram_close`、`Confirmed: true`）→ `RemoteCommandService.CloseOrders`（`biz/remote_command.go:236`）→ `Run`（:80：账户与 NoTrade 校验、幂等键、每 source:actor:action 1 秒限频、审计）→ `runClose`（:145）→ `ExecClose`：有 deps 时 `CloseAccOrdersWithState`（经账户注册的 `HubOrderMgr`/`SharedOrderMgr`），否则 `FindOpenOrders` + `CloseAccOrders`。Telegram 入口需要有人调用 `InitTelegramOrderManager`，当前 execution 路径未接线。
- 全部平仓 `CloseAllOrders`（:111）置 `All:true, Confirmed:true`。交易开关 `DisableTrading`/`EnableTrading`（:126/:140）→ `runTradingSwitch`（:156，`SetNoEnterUntil`）。

### 9. 钱包记账

- 回测/模拟：`EnterOd`（wallet.go:1099）→ 成交后 `ConfirmOdEnter`（:1199）；出场 `ExitOd`（:1235）→ `ConfirmOdExit`（:1287）；部分平仓 `CutPart`（:1344）；保证金与爆仓 `UpdateOds`（:1378，`marginRatio > 0.99` 返回 `ErrLiquidation` 并 Reset）。
- 实盘（`w.envReal()`）：Confirm/Exit 直接返回，不做本地记账。execution 账户的余额与权益由账户 owner 的账本给出（`account_metrics.go`）。
- 同步：`SyncBalancesWith` → `FetchBalance` → `UpdateWalletByBalances`；`WalletStaleMS=1h` 供按需刷新。旧的余额轮询与 WebSocket 余额推送（`StartLoopBalancePositions`、`WatchLiveBalances`）已删除。
- 估值：`calcLegal` 以 `valuationPrice` 取价。标记价在最后一根 bar 后 60 秒过期；回测中（`bt_legacy_wallet_price`/`bt_strict` 的兼容钱包除外，保持旧行为）过期后回退到最后一根 bar 价，且持有的基础币（如 `BNB`）无同名 key 时依次尝试 `BNB/{USDT,USDC,FDUSD,BUSD,USD}` 及其永续 `:{quote}`（`lastBarPriceOfCoin`）。仍无价的币被跳过，`skipWarned` 保证每币只打一次 Info，之后降为 Debug。策略侧只读查询走 `BanWallets.AccountMetrics`（`account_metrics.go`）：模拟/回测合约以结清余额加当前未平订单 `Profit` 计算实时净值，不读取 `ItemWallet.UnrealizedPOL` 缓存以避免同时间步平仓后双计；现货可用余额扣除未平多单持有的基础币。
- 快照：钱包快照的写入（原 `StartLiveWalletSnapshots`）已随旧栈删除；`orm/ormo/wallet_snapshot.go` 的存储层保留，当前无写入方。

### 10. 共享检查点、接管与迁移

- 加载：`shared_order_mgr.go:211/323/1395` → `loadSharedCheckpoint`（`shared_checkpoint.go:36`）：读 `__legacy_bridge/legacy-ts`，活跃订单立即加载，其余按 `legacy-ts/order:id:` 与 lot 键按需读取。
- 保存：`shared_order_mgr.go:350` → `splitSharedCheckpoint`（:182）：按快照 lot 与 allocation 判断活跃键，写 ActiveOrders；命令与订单记录各成条 Record，`StorageVersion=1`。
- 接管：只在 Hub 路径（`hub_order_takeover.go:adoptTakeoversLocked`）读 `AutoTakeoverRecords`，对未知 lot 生成投影订单（Tag=`EnterTagThird`，`SourceInfo` 标记 `external_takeover`）；候选 job 为 0 时下一轮 poll 重试。回测不接管外部持仓，`SharedOrderMgr` 不含接管逻辑。
- 迁移：旧版 `ImportLegacyMigration`/`CutoverLegacyExecution`（0.3.8 → 共享门面的 cutover）已删除。`__legacy_bridge/legacy-ts` 检查点格式仍由 `loadSharedCheckpoint` 读取，供 `HubOrderMgr` 首次启动时一次性导入（`hub_order_legacy_import.go`，旧数据保持不动）。

### 11. 远端 Hub 与共享执行（Hub / Shared）

- 选择：`odmgr_local.go:48-53`，`HubOrderBridge != nil` 走 Hub，否则 `SharedExecution != nil` 走 Shared（`LocalAccount` 桥的 runtime 同时有 `SharedExecution`，仍走 Hub）。
- Hub 下单（facade 订单 ID 为 52 位随机正整数，浏览器按 float64 读取仍精确）：`HubOrderMgr.EnterOrder` → `enterLocked`（`hub_order_mgr.go:432`：幂等 hash、准入 `admitTimeSeriesEntry`、`BridgeTimeSeriesEntryCommand`、`saveLocked`、`sendLocked`）。命令“先冻结后发送”（`enterLocked` 内先 `saveLocked` 再 `sendLocked`）。
- Hub 投影：`run`（:280，100ms ticker）→ `poll`（`hub_order_projection.go:35`）→ 逐事件 fold、`saveLocked`、`deliverPending`（`hub_order_projection.go:636`）→ `AdvanceProjection`（`hub_order_projection.go:249`）。持仓归零、入场不会再成交且当前页已完整回放的 lot，其视图经 `orderArchiver` 归档一次，结果记入 journal 的 `hubProjectedLot.Archived`；归档行使用订单库自增 ID，`Info["archive_key"]`（账户键 + 策略 + lot）保证重复调用与重启后不产生第二行。启动：`NewHubOrderMgr`（:112）：flock → 校验 journal identity → `ClaimStrategyWriter` fencing → `resumeCommands`（:327）。
- Shared：`initSharedOrderMgr`（:276）→ `SubscribeCommitted`（:292）→ `project(false)`（:1316）→ `projectSnapshot` 回放（:1660-1811）→ 回调 `dispatchOrderEvent`（:1816）→ `AdvanceProjection`。启动 `NewSharedOrderMgr`（:202）→ `BindJobs`（:252）→ `project(true)` 只推进游标。
- 改单：Hub `EditOrder`（`hub_order_edit.go:12`）→ `editLocked`（:27，乐观并发 `CommandRevision`，冻结键 `<strat>/edit/<id>`）；Shared `EditOrder`（`shared_order_mgr.go:1081`）。
- Hub 账户指标读取（`hub_account_reader.go`）：`readRuntimeAccountMetrics` 对 `SharedExecution`、`HubOrderBridge`、`HubAccountReader` 绑定三种运行时都经 execution 账户读取。`HubAccountReader` 只读：每次刷新对 Hub 做一次 `Snapshot`，并对有持仓的品种各取一次 `VisibleQuote` 求中间价作标记价，再由 `snapshotToAccountMetrics` 换算；从不发送命令、认领 writer 或写 journal。绑定有两类：因子-only Hub 机器人（无时序订单管理器）在 `entry/factor_hub.go` 中 `binding.Legacy == nil` 时调用 `BindHubAccountReader`（以 `TradingState` 为键，重复绑定或运行时已有 `SharedExecution`/`HubOrderBridge` 则报错，返回的 release 解除绑定）；时序 Hub 机器人由 `hubTSAccountReader` 按运行时缓存一个读取器（桥配置指针变化时重建）。缓存 TTL 1 秒、单次刷新超时 3 秒（`hubAccountReaderTTL`/`hubAccountReaderTimeout`），失败返回空指标（`Valid=false`）并同样缓存一个 TTL；因此策略权益读取与逐 bar 权益观察共享一次 `Snapshot`，亚秒级回调（如 `OnTick`）看到的账户指标最多滞后 1 秒。时序读取器另带 `hubOrderStamp`（本机器人各 `HubOrderMgr` 的 `journal.Serial` 与 `Cursor` 之和）：自己发出命令或轮询应用了账本变化就提前使缓存失效；管理器正忙（`mu` 被占用）时 stamp 不可信，本次读取不入缓存。`ExecutionAccountBalance` 对读取器绑定返回其结算币种并标记 `remote`，仪表盘因此对因子-only Hub 机器人给出余额（见 `08_entry_opt_web_rpc.md`）。

#### Hub 阶梯止盈与 OCO（`hub_order_ladder.go`）

owner 的 lot 只有一个止盈；OCO 组由 owner 在挂单推进中执行互斥（`execution/pending_oco.go`，见 `06_execution.md`），`HubOrderMgr` 在机器人侧用既有命令编排阶梯止盈并补发 OCO 撤销：

- **创建**：`enterLocked` 在 `newHubLotPlan` 中解析 `TakeProfits`（`Val/Rate` 相对参考价：`Limit`，否则 `Stop`，否则中间价；阶梯按价格由近及远排序）与 `OCOGroup`（组名随创建命令写入 `PendingCreateSpec.OCOGroup`，由 owner 持有）；某级解析出非正价格则拒绝入场（`errSharedEntryRejected`，告警 `entry rejected: take-profit ladder / OCO group cannot be expressed on the Hub path`），不静默丢弃。第一级随创建命令作为 lot 的止盈（`BridgeTimeSeriesEntryCommand` → `ladderSpecsFromBase`），其 `Rate` 是占「剩余仓位」的比例（与 `SharedOrderMgr` 的 `cutFrac` 一致，末级且总比例 ≥ 0.999 时为 1）。计划存入 journal 的 `Plans`（键为 `hubLotKey`），进行中的后续命令存入 `Followups`；后者刻意不进 `Commands`，因为通用重发路径把发送失败视为机器人致命错误，这对入场是对的，对派生的后续命令是错的。
- **对账**：`poll` 在一页投影完整回放后调用 `orchestrateLocked`，逐 lot 比较「journal 认为应安装的」与「owner 报告的」，所以丢失或被拒的命令会在后续轮询中重算。
  - 阶梯：owner 报告当前级已执行（`ProtectionDone` + 已结算的出场且仍有持仓，且 owner 的止盈规格等于本级规格，防止同一次执行被重复计数）→ `advance`（`Remaining` 减去该级 `ExitRate`，下一级生效，`MoveSLToBreakeven` 置 `BEPending`）→ 发送 `set_protections`（命令 ID 后缀 `/lad`），重建完整保护集：止损/追踪沿用原值，止盈换成当前级，保本待办时把止损收紧到不差于入场均价（只收紧不放宽）。`BEPending` 以 owner 实际止损为准清除，命令被拒或丢失时标志保留；入场均价未知时推迟并退避。
  - OCO（机器人侧补充）：owner 已在其 tick 内撤销同 tick 可执行的多余成员与「已有成交成员」的未成交兄弟；机器人在组内任一成员入场有成交（`Entered > 0`、持仓非零或 owner 已有成交步数）后，向同组（策略、品种、组名）其余未成交成员发送 `cancel_entry`（后缀 `/oco`，元数据 `ExitTag=oco_cancel`）；已撤销/过期的成员只记 `OCOCancelled`。
  - 微小级合并（`skipTinyRungs`）：非末级的一级若按剩余持仓取整后为 0 个数量步，owner 会把它当作「未配置止盈」，故该级不安装，其份额与保本请求并入下一级且不减少 `Remaining`（累计总量不变）；末级平掉全部剩余、永不跳过。记告警 `hub ladder rung below one quantity step merged into the next rung`。
  - `Val/Rate` 级在创建时按参考价估算，观察到入场成交后用真实成交均价重解一次（`Resolved`），此前触发的价位只是估算。
- **重试**：后续命令先写 journal 再发送，ID 稳定、带写者 epoch/序号。发送失败用同一冻结内容重试（丢失 ACK 时返回原回执，不会二次减仓），间隔 0.5s、1s；累计 `hubFollowupAttempts`（3）次失败后放弃（已生效命令的重放必成功，所以失败说明 owner 未应用），并以指数退避（500ms 起，上限 30s，仅存于内存）后从 owner 最新状态重算新命令。owner 拒绝或发送失败从不使机器人退出，只有 journal 保存与写者序号错误会。
- **清理**：`pruneLocked` 删除已归档/终态的 lot、入场命令被拒的 lot、已撤销且从未成交的 OCO 败者的计划；OCO 成员在仍有未决同组兄弟时保留。
- **改单**：lot 带阶梯时，对其止盈的单级改单被拒绝（否则会悄悄替换剩余各级）。
- **与 `SharedOrderMgr`（进程内）的已知差异**：
  - 同一 tick 内多个 OCO 成员都可执行时，owner 只保留一个（`oco_simultaneous`）；只有已受保护（已有成交、场所订单、预留或出场）的成员不会被撤销，多个成员都已有成交时全部保持成交（owner 记 WARN），`cancel_entry` 只发给未成交成员。
  - `set_protections` 会清空 owner 对该 lot 的保护运行状态（含追踪止损锚点），所以每次换级都会让追踪止损重新起算。
  - `Val/Rate` 级在入场成交被观察到之前不精确（见上）。
  - 阶梯/止损出场在 Hub 视图中没有「某一级」的出场标签（`ExitTag` 只来自显式出场命令或 OCO 撤销）。

#### 时序请求的价格 tick 取整（`ts_price_tick.go`）

owner 在准入时拒绝不是价格 tick 整数倍的订单价（`limit_off_tick`，`06_execution.md`），机器人对被拒的新入场只能丢弃；策略常用 `均价 × 1.0003` 之类的价格，所以两个 execution 管理器在冻结前按品种 `PriceTick` 就近取整（`roundToTick`）：

- 入场（`roundEntryLimits`）：`Limit`、`StopLossLimit`、`TakeProfitLimit` 及每个 `TakeProfits[i].Limit`。
- 出场（`exitTick`）：`ExitReq.Limit`，tick 取 `pairs` 指定的单一品种，否则取被指订单的品种，否则取唯一声明品种；跨多个品种时不取整。
- 限价改单：Hub `editLocked` 的入场/出场限价（`SharedOrderMgr` 没有这类 PendingCommand 改单）。
- 不取整：触发价（`Stop`、`StopLoss`、`TakeProfit`、阶梯各级 `Price`、追踪激活价），它们只与行情比较，可落在 tick 之间。

#### execution 路径的维护任务（`live/execution_crons.go`）

execution 机器人由 `entry/execution_robot.go:startExecutionRobotCrons` 在 provider 就绪后调用 `live.StartExecutionRobotCrons`（失败只记告警，不停止交易），并启动 runtime 自己的 scheduler（随 `Runtime.Close` 停止）。只注册仅依赖公共行情与 runtime 自身策略状态的任务：小时线补齐（`fetchHourKlinesFromWithRuntime`）、市场重载、K 线延迟监控、收数汇总、策略输出落盘（有运行配置时）。旧任务（选币轮动、致命亏损检查、触发单校验、余额/仓位轮询、钱包快照、实盘与回测对比）随旧栈删除，对应配置键的处理见「支持矩阵」。`close_on_stuck`：延迟监控判定停滞后经 `biz.CloseAccOrdersWithState(deps.Trading, account, …)` 平仓，账户管理器即 `HubOrderMgr`/`SharedOrderMgr`（`ExitOrder`，`ExitTagDataStuck`、`Force`）；只处理 runtime 已注册管理器的账户（默认账户），其余账户记错误日志。

- `ExecutionRobotCronOptions.HourOnlySymbols`：最细订阅周期为 1h 及以上的品种集合（provider 只监听亚小时 K 线，它们的实盘 bar 只能来自小时线补齐任务）。execution provider 的符号状态是隔离的，不会列出它们，入口用 `hourOnlySymbolsFromSubs` 从旧订阅（仅 K 线源）算出，并与 `rt.Symbols.GetHourOnlySymbols()` 取并集；为 nil 时回退 runtime 符号状态。没有旧订阅的纯因子入口因此得不到小时线补齐。
- `ExecutionRobotCronOptions.Received`（`com.PairCopiedState`）：延迟监控读取的收数进度（最后一行到达时间；按 pair 的 bar 结束与周期）。入口用 `rt.SetSeriesObserver` 的非预热行更新它，因为 execution provider 的行情状态与 trader 隔离，读 trader 的状态会永久报超时，并经 `close_on_stuck` 平掉所有订单。为 nil 时不注册延迟监控。

### 12. live 外壳生命周期（live/crypto_trader.go）

1. 组合根（`runtime`）创建 `Runtime`，经 `NewCryptoTraderWithRuntimeDeps` 绑定：`biz.NewTraderWithRuntimeDeps` → 校验 `deps.Exchange` → `bindCryptoTraderRuntime` 注册 `OnCloseWait(runShutdownPhases)`，scheduler 取自 lifecycle（若实现 `Scheduler()`）或 `deps.Scheduler`。
2. 入口调用 `StartWebAPI`（`api_server` 启用时启动仪表盘；停止/等待钩子经 `runtimeShutdownLifecycle` 注册），再按需 `startup` 钩子、`StartExecutionRobotCrons`；`PublishSeries` 把行情行推给仪表盘订阅者。
3. 账户检查：内嵌 provider 的机器人在 `entry/time_series_execution.go`、`entry/factor_live.go` 调 `live.VerifyLiveAccountsWithRuntimeDeps`，任一账户出错即返回 `live account check failed: …` 并终止启动（execution 管理器不识别 `NoTrade`，故不用置位方式）；Hub 生产账户注册时由 `entry/hub_account_check.go:verifyHubProductionAccount` 调 `live.CheckAccountWithExchange`（无副作用），在构造适配器、账本之前执行。Hub 检查的硬错误：无交易权限（交易所报告时）、余额/账户权限拉取失败、对冲（双向）持仓模式；仅告警：允许提币、IP 不受限；交易所不报告持仓模式时告警并依赖适配器对对冲持仓的拒绝。不重复的项：余额相对 `wallet_amounts` 的 1/3 规则（传入空预期；首次注册的 `BootstrapCapital` 已核对已结算现金与声明资金）；保证金模式（`marginAware=false`）。干跑（dry_run）账户无凭据，不检查。
4. 停止：`Runtime.Close` 取消 → `OnClose` 停止钩子；`runShutdownPhases` 逆序执行 stop 与 join 钩子。外壳不做 `exitCleanUp`（订单管理器、策略、通知由 `Runtime` 自己关闭）。

## 配置项与外部依赖

- **配置键**（`config/types.go`，默认值见 `config/runtime_config.go`、`config/biz.go`）：`stake_currency`、`take_over_strat`、`order_type`、`limit_vol_secs`（默认 10）、`stop_enter_bars`、`max_open_orders`、`max_simul_open`、`bt_net_cost`（秒，`odmgr_local.go` 乘 1000）、`bt_legacy_intrabar`、`bt_legacy_approx_funding`、`bt_slippage_bps`、`bt_funding_apr`、`bt_gap_bars`、`bt_gap_min_hours`、`bt_delist_penalty_bps`、`fatal_stop_hours`（默认 8）、`leverage`、`wallet_amounts`、`accounts[*].leverage/max_open_orders/no_trade`、`close_on_stuck`。`put_limit_secs`、`odbook_ttl`、`account_pull_secs`、`margin_add_rate`、`charge_on_bomb`、`min_open_rate`、`pairlists`、`pairmgr.cron`、`bt_in_live` 在 execution 路径被忽略并告警，见「支持矩阵」。
- **运行时快照**：`runtimeOrderConfig`（含 `slippageBps/gapBars/gapMinHours/fundingApr/legacyApproxFunding/delistPenaltyBps`） 与 `RuntimeDeps.Config.View()`；旧路径读 `config.GetAccLeverage`、`config.DefAcc`。
- **策略级**：`RunPolicyConfig.MaxOpen` / `MaxSimulOpen`（`strat.TradeStrat.Policy`）。
- **环境变量**：数据目录 `BanDataDir`（`biz/biz.go:InitDataDir`，:626）。`biz/*` 中其余文件无 Getenv。
- **交易所（banexg）**：execution 适配器（`execution/banexg_adapter.go`）使用 `FetchOpenOrders`、`FetchAccountPositions`、`CreateOrder`、`CancelOrder`、`SetLeverage`、`FetchBalance` 等；参数 `ParamAccount`、`ParamClientOrderId` 等。exg 适配层仅保留 `BuildClientOrderID`（经 banexg 能力接口，回退 legacy 格式）。`exg.PutOrderRes` 类型仅为旧 dump 的 gob 解码保留。
- **存储**：K 线与 QuestDB/Timescale（`orm`）；交易订单 `ormo.Conn(orm.DbTrades)`（SQLite，`orm/ormo/base.go:21`）；钱包快照表（`ormo.SaveWalletSnapshot` 存储层保留，当前无写入方）；交易所订单缓存 gob 文件 `{DataDir}/exgOrders/{exg}_{market}_{account}_{apiKey前5位}.gob`（`biz/tools.go:1093/1151`）；Hub journal JSON 文件（绝对路径 `StatePath`）。
- **通知/远控**：Telegram（`rpc/telegram.go`，token/secret 读取 :179-180，secret 需 UUID4 :244-245；`config/biz.go:961-963` 公开配置剔除 token）；i18n：`biz/biz.go:40-43` 嵌入、`biz/biz.go:656-730` 合并、`rpc/telegram_runtime.go:39` `langMsg`。
- **gRPC**：`biz/data_server.go:102` 默认 `:6789`；`:108` 收发 100MB 硬编码。
- **配置文件中的敏感示例**：`biz/config.yml:98`（数据库 URL）与 `:115`（`corp_secret`）为示例值，不是真实凭据。

## 与其他模块的交互边界

- **被调用方**：`entry/runtime_tools.go:36`（NewDataServer）、`entry/mixed_factor_backtest.go:323`、`runtime/runtime.go:975`、`runtime/shared_legacy_live.go:108`、`live/runtime_cron.go`/`live/tools.go`（`CloseAccOrdersWithState`）、`opt/backtest.go:170-173,249,457-460,816`、`web/live/*`、`rpc/telegram.go`（经 OrderManagerInterface）。
- **依赖方**：`execution`（EligibleIntent、PendingCommand、ExitSelection、OHLCProfile、Store、HubClient、SharedAccountBorrow）、`strat`（StratJob 的 DrainOrderRequests / FinishOrderProcessing、FireOdChange(WithState)、CalcJobScores）、`orm` / `orm/ormo`（订单与钱包快照）、`data`（DataDeps、DataSeries）、`goods`、`exg`（价格符号解析 `ResolvePriceSymbol`，统一经此处理合约/现货差异）、`com`（PriceState）、`btime`（ClockState）、`core`（State、常量、ExitTag）、`banexg`。
- **live → biz**：`live` 只做装配与编排，不含订单状态机。通过 `biz.Trader`/`RuntimeDeps`、`biz.CloseAccOrdersWithState`、`biz.NewRemoteCommandServiceWithRuntimeDeps` 调用。
- **live → strat/data**：`strat.DrainOutputs`；`data.DownEmitHourKlinesWithRuntimeDeps`（小时线补齐）、`dp.GetJobs("ohlcv")`。
- **live → web/rpc**：`web.StartApiWithRuntimeDeps` 启动 API，`apiServer.PublishSeries`；消息经 `sendRuntimeMessage`（`runtime_cron.go`）发往 `deps.Notifications`。
- **依赖 runtime 的契约**：`RuntimeLifecycle`（Context、OnClose、OnCloseWait）与 `data.CallbackTracker`（EnterCallback/LeaveCallback）。这两者由 `runtime.Runtime` 提供；
- **交易所差异**：生产代码中未发现按交易所名的 `switch` 分支（已对 biz/ 与 live/ 检索 binance/okx/bybit 等）。

## 扩展与修改指南

- **新增实盘行为**：实盘订单语义在 `execution` 模块与 `HubOrderMgr`/`SharedOrderMgr`（`06_execution.md`），不要恢复旧 `LiveOrderMgr`。把「支持矩阵」中的某项迁入 execution 前，先读该项的迁移说明。交易所差异只能在 banexg 适配层实现，不能在 biz 加分支。
- **新增 OrderMgr 方法**：需同步实现 `LocalOrderMgr`、`SharedOrderMgr`、`HubOrderMgr` 三类（`EditOrder` 已有 Local 空实现）。
- **新增订单配置**：加入 `runtimeOrderConfig` 与 `makeRuntimeOrderConfig`（`odmgr.go:568`），并保证 legacy 访问器读取一致。
- **撮合/止损/强平**：三处都要调 `callBack` 与 `fireOdChange`，漏一处回调不一致。修改订单字段后置 `DirtyMain`/`DirtyEnter`/`DirtyExit`/`DirtyInfo`，否则不落库。
- **新增远程动作**：`remote_command.go` 加常量（:25-26）、`Run` 的 switch、runner 方法；注意 `hasIdempotency`（:96）与限频。Telegram 新命令：先 rpc 解析，再加入 `OrderManagerInterface`（`rpc/telegram.go:72`），实现走 `RemoteCommandService.Run`，不要直接调 ormo；deps 与非 deps 两条分支都要改。
- **改钱包记账**：`wallet.go` 的 Enter/Exit/Confirm 服务回测/模拟（实盘 Confirm 直接返回），改动后检查 `wallet_exit_test.go`、`wallet_partial_exit_test.go`、`wallet_liquidation_test.go`。
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
  - 锁顺序：先 `od.Lock()`，再取 openOrders 锁；不要在持 openOrders 锁时取 od 锁。

## 已知限制与设计取舍

- execution 路径的机器人（时序 `entry/time_series_hub.go`、`entry/time_series_execution.go`，因子/混合 `entry/factor_hub.go`、`entry/factor_live.go`）不再有 `CryptoTrader.Init`；构造 `CryptoTrader` 外壳后调用 `StartWebAPI`；`api_server` 配置经 `accountDashboardConfig` 传入，且只在机器人恰有一个账户时启动（多账户记告警并跳过，避免各账户 runtime 绑定同一端口）。
- execution 路径的订单历史只包含 `HubOrderMgr` 归档的已平仓订单；归档行的 `ProfitRate` 由 `orderArchiver.archive` 按 `Profit / (Enter.Average × Enter.Filled)` 补算（投影只带已实现盈亏），`MaxPftRate`、`MaxDrawDown` 未计算（为 0）。订单列表对已平仓订单不再按当前价重估，直接使用成交均价。
- 时序入场的数量在机器人侧由 `BridgeEntryReq` 推出；小于品种 `min_steps` / `min_notional`（按 `Notional(steps, 参考价)` 计）或超出策略 `MaxNotional` 的入场，在冻结命令之前被丢弃并记告警 `entry rejected: size below the tradable minimum or above the strategy allowance`，不占用命令序号。owner 在净额后若真实残量低于最小值，返回 `execution.ErrVenueMinimum`；`sendLocked` 把该命令标记为 `Done` 且 `Rejected`（`hubFrozenCommand.Rejected`），此后不再重发，重启时 `resumeCommands` 也跳过它。owner 对新入场（`PendingCreate`）的封闭码拒绝（`limit_off_tick`、`risk_limit`、`spot_short`、`self_cross`、`admission_limit`、`account_not_ready`、`stale_entry`，见 `06_execution.md`；后两者分别表示账户未对账、信号已过期）同样处理：丢弃入场并记告警 `hub rejected an entry; it is dropped, not retried`，机器人继续运行，`enter` 返回 `nil, nil`；机器人用 `execution.RejectionCode` 识别，不解析错误文本。退出、改单、保护变更收到这些码时不丢弃，仍按错误失败（保持 fail-closed，避免持仓无人管理）；`venue_minimum` 对退出同样丢弃（`exit` 返回 `nil, nil`）。`enterLocked` 在 `sendLocked` 返回被拒后把 `journal.Admission` 恢复为发送前的值并保存：被 owner 拒绝的入场不占用该 bar 的开单名额（`CommandID` 重试走缓存命令时，`Rejected` 命令直接返回被拒，不再发送）。重启时 `resumeCommands` 对每条未完成的冻结命令先记 INFO `hub resends a frozen command`（含订单 ID、种类，有信号时间时含 `age`），再重发；信号已过期的入场由 owner 以 `stale_entry` 拒绝。原因：冻结命令在 journal 中先冻结后发送，若一个被 owner 持续拒绝的命令一直保持未完成，会阻塞其后全部命令，并使机器人每次重启都因重发失败而退出。

- execution 路径的 `strat_perf` 逐单得分由 `runtime/perf_update.go` 更新（见 `04_strategy_runtime.md`），而不是 `OrderMgr.finishOrder`。与旧路径的差异：历史窗口是内存环而非订单库全量；`SharedOrderMgr` 不归档，重启后环从空开始，只有 Hub 路径可从已归档行预载；投影订单无平均入场价时按 `InitPrice`/`Enter.Price` 估算 `ProfitRate`。
- execution 路径的回调派发不复刻旧订阅在 `OnOrderChange` 之前对最新价的实盘刷新（`prices.RefreshLatestPriceAt`）。
- `HubOrderMgr` 的订单视图由 `poll` 投影得出（100ms 周期）：回调里冲刷的出场请求提交后，视图要到下一次投影才反映。
- Hub 路径的阶梯止盈/OCO 与进程内 `SharedOrderMgr` 的行为差异见「Hub 阶梯止盈与 OCO」。
- Telegram 订单管理器固定 `Confirmed: true`，因此 `remote_command` 的二次确认对 Telegram 通道不生效。
- `RemoteCommandService` 的幂等记录（seen）无淘汰。
- 配置示例文件中的数据库 URL、`corp_secret` 仅为示例值。
