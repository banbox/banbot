# execution 模块：执行 / 账户服务层

> 基准：当前工作区源码（`execution/` 下 85 个非测试 `.go` 文件，约 2.2 万行）。
> 阅读范围：全部声明与 doc 注释已逐一核实；`adapter.go`、`account_service.go`、`account_service_reports.go`、`hub.go`、`hub_rpc.go`、`banexg_adapter.go`（Submit/Reports）、`ledger.go`（ApplyFill 入口）、`store.go`、`coordinator.go`（入口）、`account_owner.go` 为逐行阅读；其余文件以声明、注释与调用关系核实，细节以源码为准。
> 已有配套文档：`execution/HUB.md`（Hub 现状与切换边界）、`execution/RECOVERY.md`（拒单后迟到成交的安全边界与事故流程）、`execution/TS_MIGRATION.md`（时序策略迁移）、`execution/CROSSVENUE.md`（跨场模拟盘）、`execution/OPTIONS.md`（现金欧式期权）。本文不重复其全部细节。

## 定位与职责

- 为策略（TS / factor / 混合）提供**账户级**执行：虚拟仓位（`VirtualLot`）、可执行意图（`EligibleIntent`）、组合计划（`Plan`）、真实订单（`StoredOrder`）、成交账本、现金与风险冻结。
- **单写入者**：同一物理账户只有一个 `AccountOwner`，所有本地写入串行（`AccountHandle.DoLocal`）。网络写入只能经 `OwnerExecutor`，并通过持久化的 `Prepared -> Sending -> ...` 状态机保证"先落库、后发送"。
- **持久化**：生产为 SQLite（`store.go`、`storage_*.go`，表结构见 `storage_schema.go`，当前 `exec_schema` 版本 4）；模拟/回测为进程内 `MemoryStore`；`PaperAdapter` 可选 durable 日志。
- **宿主形态**：`SharedAccount`（进程内共享服务）+ `SharedAccountBorrow`（借用者，每个 Runtime/机器人一个）；独立进程的 `Hub`（`hub.go`）通过本机 Unix socket 的 RPC（`hub_rpc.go`）对外提供账户级命令。
- **不负责**：交易所 SDK 细节（属于 banexg，本包只经 `banexg_*.go` 适配）；策略信号与目标计算；行情拉取（只消费 `VisibleQuote`/`ExecutionObservation`）；回测 OHLC 撮合的历史假设仅在 `ohlc_profile.go` 中以兼容视图消费。

## 目录/文件地图

以下按文件前缀归类（括号内为主要类型/函数，不穷举）。

### 账户服务与生命周期（`account_*`、`account_service*`）
| 文件 | 作用 |
|---|---|
| `account_owner.go` | `AccountKey`（venue 会话/账户/结算域三元组）、`AccountRegistry`、`AccountHandle`、`OwnerToken`；串行化与 Stop/Join |
| `account_service.go` | `SharedExecutionOptions`、`SharedAccount`、`SharedAccountBorrow`；`Reconcile`、`RecoverPersisted`、`Submit`/`Send`/`Rebalance*`、`Close` |
| `account_service_reports.go` | 私有报告循环 `StartReportsContext`、`ReportErrors`、`FreezeFailure`、`ApplyExternalPosition`、`ApplyFunding` |
| `account_transient.go` | 瞬时读失败分类 `transientAccountReadError`、`ErrAccountQuoteNotVisible`、停摆日志/限流暂停 `transientOutage`，及对账/资金费节拍常量 |
| `account_service_policy.go` | 账户报价、`AccountRisk`、`RegisterAccountQuotes`、绑定校验 `ValidateAccountBindings` |
| `account_service_state.go` | 借用者的只读/受控访问器（`WithState`、`ReadQuote`、`AdvancePaperQuote`） |
| `account_bootstrap.go` | `BootstrapCapital`：一次性导入已结算现金与策略分配 |
| `account_recovery.go` | `RefreshWorkingOrders`：周期性修复漏掉的流更新，并撤销过期挂单 |
| `account_policy.go` | `AccountPolicy`：不可变账户策略注册（限额、策略上限不可改） |
| `loss_stop.go` | 账户亏损熔断：`LossStopPolicy`（多个"窗口 + 最大亏损率" + 暂停时长，可替换）、`LossStopStatus`；`SetLossStopPolicy`、`LossStopStatus`（borrow / `Hub` / `HubClient` 同名）。权益口径：结算现金 + 全部持仓按可见中间价估值，已实现盈亏、手续费、资金费与浮亏全部计入 |

### 交易所适配（`adapter.go`、`banexg_*.go`、`legacy_sender.go`、`market*.go`）
| 文件 | 作用 |
|---|---|
| `adapter.go` | `ExecutionAdapter` 接口、`AdapterCapabilities`、`SubmitReceipt`、`QueryResult`、`VenueSnapshot`；`OwnerExecutor` 的 `Send`/`Cancel`/`Recover`/`ApplyTrade`/`ApplyFill` |
| `banexg_adapter.go` | `BanexgAdapter`（能力校验 `NewBanexgAdapter`、`Submit`、`Cancel`、`Query`、`Snapshot`、`Reports`、`Observe`、`MigrationSnapshot`）；`BanexgInstrument.Leverage` 为注册时声明的品种固定杠杆，`ApplyLeverage` 逐品种设置到交易所，未成功前 `Submit` 拒绝发单 |
| `banexg_transport.go` | `BanexgExecutionTransport`：把整次 SDK 调用绑定到 owner 上下文 |
| `banexg_external.go` | 外部（非本策略）订单的恢复 `RecoverExternal`、`RecoverExternalReport` |
| `banexg_funding.go` | 资金费与现金恢复 `RecoverCash` |
| `banexg_instrument.go` | `InstrumentFromBanexgMarket`：从 SDK 市场元数据生成 `Instrument`（支持规范化 `linear_perpetual` 与无杠杆 `spot`） |
| `banexg_limit_price.go` | `DefaultLimitPrice`：基于分钟成交量与盘口的被动限价；盘口时间戳同样经 `observedBookMS` 判定 |
| `quote_reuse.go` | `ReuseRecentQuotes(fetch, maxAge)`：按品种复用最近一次成功的有效报价（`maxAge` 内且未过 `ValidUntilMS`），并发读取共用一次抓取，失败不缓存；`EmbeddedQuoteReuse`（1 秒）供内嵌时序入口与混合账户使用。需要“某时刻之后观测”的报价的调用方用 `WithQuoteNotBefore(ctx, atMS)` 标记（因子 runner 以目标可执行时间标记），缓存报价早于该时刻时重新抓取；`SharedAccountBorrow.VisibleQuote` 把该标记从调用方传给账户报价源 |
| `legacy_sender.go` | `LegacySender`/`LegacyExchange`：旧 TS 实现的 SDK 写操作门控（OS 租约 + owner） |
| `market_stream.go` | `StartBanexgMarketStream`：账户级盘口订阅，驱动报价与模拟成交；REST 快照与 websocket 唤醒订阅均请求 `accountQuoteDepthLevels`（5 档），场所报该档位无效（`CodeParamInvalid`）时退回 SDK 默认深度；同一品种的 REST 快照间隔不小于 min(TTL/4, 1s) |
| `market.go` | `Quote`、`FundingObservation`（引擎中立的观测值） |

### 意图、计划与订单（`intent*.go`、`coordinator.go`、`rebalance.go` 等）
| 文件 | 作用 |
|---|---|
| `intent.go` | `EligibleIntent`（条件、触发、预留、成交、取消的本地状态机）、`ExitSelection` |
| `intent_store.go` | 意图/快照读写、`StrategyCheckpoint`、`StrategyAcceptedEvent`、`StrategyTotals` |
| `ledger_types.go` | `Instrument`、`Plan`、`OrderIntent`、`StoredOrder`、`FillReport`、`VirtualLot`、`CashEvent`、`AccountSnapshot`、`RealOrderState` 等核心类型 |
| `coordinator.go` | `Coordinate`：按账户快照与目标计算每个品种的增减量；`SubmitEligible` |
| `rebalance.go` | `PrepareRebalance*`、`CombinedRebalance`、`PreparedRebalance`；内部对冲在中间价成交，只为真实残量建单；同一策略在同一计划内平多头 lot 与空头 lot（两侧都是退出意图，如“全部平仓”或多头止盈与空头止损同 tick 触发）也在内部互抵，两个 lot 同价平仓、不产生场所订单，其余同策略相反意图（双向开仓、翻转）仍以 `self_cross` 拒绝；净额后任一真实腿低于品种 `MinSteps`/`MinNotional` 时在准备阶段（尚未提交任何状态）返回 `ErrVenueMinimum` |
| `strategy_rebalance.go` | `StrategyRebalance`（Full/Patch 两种目标更新）、`PrepareStrategies*` |
| `strategy_writer.go` | `ClaimStrategyWriter`：策略写者 epoch CAS 认领 |
| `order_policy.go` | `OrderPolicy`：提交与内部撮合的划分（近价限价才可挂单、独占 lot 规则） |
| `protection.go` | `PositionProtection`（软件止损/止盈/追踪）的准备与评估 |
| `conditional.go` | `ConditionalOrder`/`ConditionalReservation`：原生休眠减仓单及其能力证明 |
| `risk.go` | `validatePlanReservation`：发送前按最新权益复核组合预留 |
| `exposure.go` | 确认敞口、预留敞口（`reservedCarriedExposure`） |
| `attempt.go` | `OrderAttempt`：提交/撤单/恢复的不可变审计记录 |
| `quantity.go` | `QuantitySteps`、`AllocateSteps`：步长取整与按需求比例分配 |
| `limit_price.go` | 被动限价算法（与交易所无关的纯函数） |
| `ohlc_profile.go` | 回测用 OHLC 撮合（历史延迟、对齐、止损限价顺序；`SlippageBps` 与 `Slip` 对市价类成交加单边不利滑点） |
| `backtest_rules.go` | 回测记账规则的无状态浮点函数：资金费（`FundingPoint`、`MergeFundingPoints`、`RealFundingCost`、`ApproxFundingCost`）、数据空洞与退市强平定价（`GapConfirmDelayMS`、`GapExitPrice`、`DelistExitPrice`、`GapFillTime`）、爆仓与亏损补保证金判定（`LiquidationBreached`、`MarginWithLossTopUp`）；订单与钱包状态由调用方（当前为 `biz.LocalOrderMgr`）持有，实盘不使用 |

### 待执行（pending）
| 文件 | 作用 |
|---|---|
| `pending_registry.go` | `PendingDefinition`、`LoadPendingBook`、`SeedPendingBook`、`AdvancePendingExecution`（账户自主节拍） |
| `pending_order.go` | `PendingOrder`：单个经济 lot 的入场/保护状态 |
| `pending_command.go` | `PendingCommand`（创建、退出、撤入场）及其 `ApplyPendingCommand` |
| `pending_controller.go` | 挂单推进：撤单、保护、入场的统一规划 |
| `pending_coordination.go` | `CoordinatePending`：输出策略级绝对目标 |
| `pending_feed.go` | 公共时钟（feed）的观测、过期与挂单绑定 |
| `pending_native.go` | 原生保护单的放置、撤销、成交同步 |
| `pending_revocation.go` | 冲突预留撤销（新合约不可继承单 lot 的挂单许可） |
| `pending_sequence.go` | `sequenceOpposite`：同策略相反方向增量/减量在自成交（`self_cross`）拒绝后的跨 tick 排序；`PendingOrder.ReductionHolds` 为其持久化的优先级计数 |
| `pending_oco.go` | OCO 入场组：`PendingCreateSpec.OCOGroup` → `PendingOrder.OCOGroup`/`OCOSeq`；`validOCOGroup`、`nextOCOSeq`、`SharedAccount.resolvePendingOCO` |
| `pending_isolation.go` | `isolateRejections`：自主 tick 中把可归因的入场拒绝隔离为单个入场的终止，而不是冻结账户 |

### 策略政策与时序（`policy_*`、`time_series_*`、`checkpoint_page.go`、`projection.go`）
| 文件 | 作用 |
|---|---|
| `policy_acceptance.go` | `PolicyAcceptance`/`PolicyReceipt`/`PolicyEvidence`；原子接受与恢复（`AcceptPolicyBatchContext`、`ResumePolicyContext`） |
| `policy_reconcile.go` | `CancelPolicyEntriesContext`：先结算入场单再冻结退出锚点 |
| `time_series_command.go` | `TimeSeriesExitCommand`、`TimeSeriesMetadata`、`ApplyTimeSeriesExit`（账户内统一分配数量预算） |
| `time_series_projection.go` | `TimeSeriesProjectionRequest/Page`；借用者、Hub、RPC 三种 `TimeSeriesProjectionPort` 实现 |
| `projection.go` | `EventsAfter`、`ProjectionCursor`、`AdvanceProjection`（游标式投影） |
| `checkpoint_page.go` | `StrategyCheckpointsAfter`：按前缀分页读取检查点 |

### Hub（`hub*.go`、`HUB.md`）
| 文件 | 作用 |
|---|---|
| `hub.go` | `Hub`、`HubAccountRegistration/Binding/Factory`；`Register`、`use`（借用）、`Close`；目标命令的幂等与 epoch 校验 |
| `hub_rejection.go` | 拒绝分类：`RejectedError`（封闭码 `limit_off_tick`/`risk_limit`/`spot_short`/`self_cross`/`admission_limit`/`account_not_ready`/`stale_entry`，`Scope` 仅 owner 内部归因、不经 RPC）、`venueMinimumError`、`hubRPCRejection`（决定哪些失败以固定码回传） |
| `hub_rpc.go` | `HubServer`（Unix socket，`/v1/*` POST JSON）、`HubClient`；请求体上限 4MiB，禁止未知字段。失败原因不随 RPC 返回（可能含凭据；客户端只见 `hub operation failed; inspect account recovery state before retrying`），由 `SetErrorObserver` 交给宿主：`entry/hub.go` 把操作路径与错误写入 Hub 本机日志 |
| `hub_port.go` | `HubPort`：机器人侧命令/查询面（`biz.HubOrderMgr`/`HubAccountReader` 实际用到的 12 个方法）；`*HubClient` 与 `*LocalHubPort` 均实现。`LocalHubPort`（`NewLocalHubPort(hub)`，nil 返回 nil）把每次调用 JSON 编码后交给 `HubServer.serveHTTP`、再由 `HubClient` 解码，只跳过 socket/HTTP 传输，所以错误映射、请求体上限与拷贝语义（不与 owner 共享指针/切片）与 socket 路径一致；`SetErrorObserver` 对应 `HubServer.SetErrorObserver`；`Close` 不释放 Hub |
| `embedded_hub.go` | `EmbeddedHub`（`NewEmbeddedHub(ctx, account, tick)`）：把宿主已构建的 `SharedAccount` 以进程内 Hub 暴露（内嵌生产时序机器人的唯一门面）。永不关闭该账户；feed 准入恒成功（由机器人 runtime 自己转发收盘 bar）；每 `tick`（嵌入时序机器人用 1 秒）在账户已 `Reconciled` 后调 `AdvancePendingExecution`，失败只记日志（去重）并经 `TickError()` 暴露，账户冻结仍由其报告 worker 的同一规则负责；`Close` 幂等 |
| `hub_contributor.go` | `RegisterContributor(s)`：向已认证账户追加策略（资金只来自未分配现金） |
| `hub_feed.go` | `PendingFeeds`、`ObservePublicFeed`、`AuthorizePublicFeed` |
| `hub_policy.go` / `hub_policy_cancel.go` | 策略接受/证据/恢复；入场撤销（持久化后再撤，可恢复） |
| `hub_queries.go` | 只读查询：风险、策略合计、报价、限价、标的是否已结算、写者租约 |
| `hub_target_resume.go` | `ResumeStrategyTarget`：机器人重启后恢复已接受的目标命令 |
| `hub_recovery.go` | `NewHubWithRecovery`：凭据无关的恢复登记（重启后等待重新认证） |

### 账本、结算与期权（`ledger*.go`、`settlement.go`、`*_settlement.go`、`option*.go`）
| 文件 | 作用 |
|---|---|
| `ledger.go` | `ApplyFill`（去重、高水位、分配、lot、现金、费用、账本、检查点同事务）、`ApplyCashEvent`、`Snapshot` |
| `settlement.go` | 内部撮合 `ApplyInternalMatch`、资金费 `ApplyFunding`、外部持仓 `ApplyExternalPosition`、对账 `Reconcile` |
| `external_attribution.go` | `AttributeExternal`：把隔离的外部持仓明确归属给策略（不自动解冻） |
| `auto_takeover.go` | `AutoTakeoverExternal`：仅对有完整成交证据的隔离账本做接管 |
| `terminal_settlement.go` | `ApplyTerminalSettlement`：永久现金结清（无成交报价） |
| `option.go` / `option_expiry.go` | `Instrument` 估值契约辅助（`IsSpot`/`IsOption`/`IsCashSettled`/`ShortAllowed`/`HasPerpetualFunding`、`Collateral`、`tradeCash`）：`spot` 按 `1.0 * Notional` 全额占用抵押且按买卖成交额增减结算现金、禁止裸卖空、不收付永续资金费；现金欧式期权的 payoff、抵押与到期结算（仅研究/模拟） |

### 模拟盘与跨场（`paper*.go`、`crossvenue.go`）
| 文件 | 作用 |
|---|---|
| `paper.go` | `PaperAdapter`：确定性本地撮合（滑点、tick 取整、费用、挂单按可见买卖价触发） |
| `paper_funding.go` | `ApplyPaperFunding`：模拟资金费 |
| `paper_persistence.go` | `NewDurablePaperAdapter`：可选的模拟场 SQLite 操作日志与回放 |
| `paper_history.go` | `BindStore`：冷历史读穿透 |
| `crossvenue.go` / `CROSSVENUE.md` | `CrossVenuePaper`：独立 SQLite 的跨场现货模拟（非 Store，不共享永续 lot） |

### 存储与迁移（`store*.go`、`storage_*.go`、`migration.go`、`archive.go`）
| 文件 | 作用 |
|---|---|
| `store.go` | `Store`：SQLite 账户级记录门面；`OpenStoreWithLeaseDir`、`SavePlan`、`PrepareOrder`、`Order`、`Plan` |
| `store_lock.go` / `store_lock_unix.go` / `store_lock_windows.go` / `store_lock_other.go` | 发送方租约（`SenderIdentity`、`acquireSenderLeases`）；Unix 用 `flock`，Windows 用 `LockFileEx`，其他平台为空实现 |
| `storage_transaction.go` | `storeTxn`：统一提交边界（`commit`/`readRecord`） |
| `storage_sql.go` | 固定的存储操作枚举与 SQLite 语句（SQL 只在这里） |
| `storage_schema.go` | 建表与 v1 → v4 迁移（影子校验后再删除旧表） |
| `storage_memory*.go` | 内存实现（同一领域语义，事务失败整体回滚） |
| `storage_history.go` | 模拟账户冷历史（可选，仅回测输出，不是重启快照） |
| `recovery_page.go` | 启动恢复的分页订单枚举 |
| `migration.go` | 旧版（legacy）迁移表的只读访问（`Migration`、`LegacySource`）与类型；导入入口已删除 |
| `archive.go` | `ArchiveCommittedEvents`：已提交事件导出为分块 Gob + manifest（审计，不是恢复快照） |

## 核心类型与接口

- **`AccountKey`**（`account_owner.go`）：`VenueSessionIdentity` + `Account` + `SettlementDomain`，三者必须非空且无首尾空白。是账户的唯一身份，与 Runtime 无关。
- **`AccountRegistry` / `AccountHandle` / `OwnerToken`**（`account_owner.go`）：进程内的账户所有者。`Acquire` 返回借用句柄；`DoLocal` 串行执行本地工作；`Stop` 关闭准入，`Join` 等待已接受工作。注释明确：`DoLocal` 不是网络写入 API。
- **`ExecutionAdapter`**（`adapter.go`）：`Capabilities`、`Submit`、`Cancel`、`Query`、`Snapshot`。约定：适配器返回报告而非同步回调；不得从瞬时错误推断"权威不存在"。
- **`AdapterCapabilities`**：`PostOnly`、`QueryClientID`、`AuthoritativeNotFound`、`StableTradeID`、`CumulativeReports`。能力必须被证明，不由交易所名称或 `HasApi` 推断。
- **`QueryResult`**：`Found`、`Authoritative`、`Complete`（数量/成本/费用与成交完整，含确认的零成交单）、`Receipt`、`Canceled`。
- **`OwnerExecutor`**（`adapter.go`）：绑定 owner、Store、Adapter 与操作上下文；默认 15 秒超时，与调用方截止时间取较早者。
- **`SharedExecutionOptions`**（`account_service.go`）：`Memory`、`StorePath`、`SenderLeaseDir`、`Adapter`、`AuthoritativeSnapshot`（复合证明：快照包含全部挂单含原生条件单）、`TakeoverStrategy`。
- **`SharedAccount`** / **`SharedAccountBorrow`**（`account_service.go`）：前者由 Process 持有，拥有 Store、Executor 与报告循环；后者按借用者隔离准入，`Release` 后拒绝所有调用。
- **`Store`**（`store.go`）：所有记录的门面；写入经 `commit`，读取经 `readRecord`。
- **`Plan`**（`ledger_types.go`）：`Sequence`（账户级递增）、`DecisionMS`、`ExpiresMS`、`Intents`、`Targets`、`CarriedIntents/Targets`、`RiskValidUntilMS` 等。
- **`OrderIntent` / `StoredOrder` / `RealOrderState`**（`ledger_types.go`）：真实订单状态 `Prepared`、`Sending`、`Acknowledged`、`Partial`、`Filled`、`Unknown`、`CancelPending`、`Canceled`、`Rejected`。`Sending`/`Unknown`/`CancelPending` 阻止普通增减仓。
- **`EligibleIntent` / `IntentState`**（`intent.go`）：`PendingCondition`、`Eligible`、`Reserved`、`Partial`、`Filled`、`Expired`、`Canceled`。本地 `Cancel` 不等于交易所撤单。
- **`VirtualLot` / `FillAllocation`**：策略虚拟仓位与成交到意图的分配记录。
- **`FillReport`**：`EventID`（幂等键）、`OrderID`、`Steps`、`Price`、`Cost`、`Fee`、`Cumulative`、`AuthoritativeSnapshot` 等；增量与累计两种模式不可在同一订单内混用（`ledger.go` `ApplyFill` 注释）。
- **`CashEvent` / `CashPosting`**（`ledger_types.go`）：`CapitalTransfer`、`ExternalCashChange`、`Funding`、`Fee`、`Liquidation`、`Reconciliation`。
- **`AccountSnapshot`**（`ledger_types.go`）：现金、未分配现金、策略合计、lot、检查点、冻结标志。
- **`PendingDefinition` / `PendingOrder` / `PendingCommand`**（`pending_*.go`）：账户自主推进的挂单账本；命令带 `StrategyEpoch` 与 `StrategySequence` 的乐观版本。
- **`StrategyRebalance` / `CombinedRebalance` / `PreparedRebalance`**：策略级目标 → 账户级计划 → 已持久化的准备结果。
- **`PolicyAcceptance` / `PolicyReceipt` / `PolicyEvidence`**（`policy_acceptance.go`）：策略政策的原子接受与重放证据。
- **`Hub` / `HubAccountFactory` / `HubAccountBinding`**（`hub.go`）：账户生命周期独立于机器人连接；`Binding` 必须提供 `Shutdown`（停止并 join 后再关闭存储与传输）。
- **`HubServer` / `HubClient`**（`hub_rpc.go`）：Unix socket 服务与客户端；`HubClient` 的方法与 `Hub` 一一对应。`HubPort`（`hub_port.go`）是机器人侧的最小接口，`HubClient` 与进程内的 `LocalHubPort` 同为实现，`biz.HubOrderBridgeConfig.Client` 的类型即 `HubPort`。
- **`BanexgStreamReport`**（`banexg_adapter.go`）：私有流只产生"唤醒提示"（`OrderID`、`UnassignedExchangeID`、`ExternalOrder`、`AccountChanged`），不直接改账。
- **`PaperAdapter`**（`paper.go`）、**`CrossVenuePaper`**（`crossvenue.go`）：模拟撮合与跨场模拟。

## 关键流程

以下为主要调用链（`文件:函数`）。

1. **账户上线（Hub 或进程内）**
   `hub.go:Hub.Register`（先持久化身份，再调用 factory）→ 宿主 factory 构造 `NewSharedAccount`（`account_service.go`，打开 Store 与发送租约）→ `RecoverPersisted`（`account_service.go`，逐个 `OwnerExecutor.Recover`）→ `StartReportsContext`（`account_service_reports.go`，启动唯一私有报告循环）→ `Reconcile`（`account_service.go`，权威快照对账，成功才置 `ready`）。
   旧账户快照导入入口（`ImportLegacyMigration`/`Store.ImportMigration`）已删除；旧 `__legacy_bridge` 检查点只由 `biz/hub_order_legacy_import.go` 首次启动时一次性导入 Hub journal。

2. **策略目标下单（TS / factor）**
   机器人 `HubClient.ClaimStrategyWriter` → `strategy_writer.go:ClaimStrategyWriter`（epoch CAS）→ `HubClient.RebalanceStrategy` → `hub.go:RebalanceStrategy`（请求哈希与 `hub-target:<PlanID>` 检查点保证重试幂等，报价在首次接受时冻结）→ `strategy_rebalance.go:PrepareStrategiesWithCheckpoints` → `rebalance.go:PrepareRebalance`（`coordinator.go:Coordinate` 计算；内部对冲在中间价成交；只为真实残量生成 `OrderIntent`，同一事务写入计划与检查点）→ `account_service.go:sendPreparedRebalance` → `adapter.go:OwnerExecutor.Send`。

3. **发送（`OwnerExecutor.Send`，`adapter.go`）**
   先做只读预检（执行时间、post-only 能力、条件单能力、组合预留 `risk.go`）→ 事务内：确认 `Prepared`、意图仍可用、计划未过期且仍为最新序号、冻结标志、无不确定订单、递增 `Attempt` 并落 `Started` 审计、状态置 `Sending` → **事务提交后**才调用 `Adapter.Submit` → `acknowledge`（写 `ExchangeID` 与状态，之后逐条 `ApplyFill`）。
   失败路径：提交错误或上下文取消 → `markUncertain`（写 `Unknown`，使用 `context.Background()` 确保持久化）。`Unknown/Sending/CancelPending` 永不被本方法重发。

4. **成交入账（`ledger.go:ApplyFill`）**
   事务内：事件 ID 去重 → 读取订单（拒绝 `Prepared`/`Rejected` 订单的成交，见 RECOVERY.md）→ 期权已结算检查 → 价格 tick 校验 → 增量/累计模式一致性 → 按分配更新 lot、现金、费用、账本与检查点。失败整体回滚，不消耗事件 ID。

5. **撤单（`OwnerExecutor.Cancel`）**
   持久化 `CancelPending` 与审计 → `Adapter.Cancel` → `Adapter.Query` 取得完整的最终成交快照 → `acknowledge` → `Canceled`。撤单未确认、查询不完整时保持 `CancelPending`，由恢复流程处理。

6. **恢复（`OwnerExecutor.Recover`，`adapter.go`）**
   `Sending` 先转 `Unknown` → 用稳定 client ID 查询 → 权威"不存在"且零成交 → `Rejected`；有成交但历史不完整 → 报错保持冻结；查询结果 `Complete` 后 `acknowledge`。`ApplyTrade`（累计模式）只触发权威查询，避免重复计入。

7. **私有报告循环（`account_service_reports.go:StartReportsContext`）**
   独立 goroutine，每 5 秒一个节拍：账户已对账且距上次全量快照不足 `privateReconcileInterval`（30 秒）时，节拍只执行 `AdvancePendingExecution`；否则（含账户变动唤醒、订单报告、瞬时故障中）执行全量快照 `RefreshWorkingOrders` → `Reconcile` → `AdvancePendingExecution`（余额、持仓、全部普通/条件挂单）。资金费 `RecoverCash` 不再每节拍拉取：对账返回已结算的现金不符（`ErrReconciliationCashMismatch`）时按需拉取并重试对账一次，其余情况至多每 5 分钟（`fundingRecoveryInterval`）一次，瞬时失败后退避 30 秒；真实不符（如外部划转）仍冻结账户。流事件分三类：外部订单（`RecoverExternalReport`）、账户变动（全量对账）、订单事件（`Recover` 后对账）。
   - **瞬时读失败不冻结**：网络失败、超时、限流、临时封禁、场所繁忙、结果不确定、`context.DeadlineExceeded` 与 `ErrAccountQuoteNotVisible`（账户盘口尚不可见或过期）经 `transientAccountReadError` 识别后，本节拍跳过、worker 继续运行，账户在恢复前保持未就绪（新入场以 `account_not_ready` 拒绝）；开始时记 WARN，持续超过 1 分钟记 ERROR（一次），恢复时记 INFO。限流按场所 Retry-After（缺省 30 秒）暂停定时与账户变动触发的对账。
   - **其余错误仍 fail-closed**：任一步失败经 `FreezeFailure` 持久化风险冻结并上报 `ReportErrors`，每次冻结都记 ERROR `account frozen by the private report worker`（含原因与错误）。其中 `AdvancePendingExecution` 的计划准备被拒时，先按下一节第 8 步的排序与隔离处理；只有这两步都无法消解的错误（未归类错误、提交之后的错误、减仓或未经 owner 准入的入场所致的拒绝）才作为该步失败冻结账户。

8. **挂单自主推进**
   `pending_registry.go:AdvancePendingExecution`（账户级节拍，不依赖借用者存活）→ `pending_controller.go:AdvancePendingEntries`/`AdvancePendingProtections`（先结算撤单的最终成交，再构建目标）→ `pending_coordination.go:CoordinatePending` → 与策略目标统一经 `Coordinate` 发送。`advancePendingDefinitions`（`pending_registry.go`）在计划被拒时分两层处理，命令准入与自主 tick 共用同一规划器，但只有自主 tick（`commandPlanID == ""`）执行隔离与减仓互让：
   - **OCO 入场组**（`pending_oco.go`）：`resolvePendingOCO` 在 `AdvancePendingProtections` 之后、`CoordinatePending` 之前执行，自主 tick 与命令准入共用（同一 `advancePendingDefinitions`），所以被撤销的同组成员不会成为目标（不触发自成交或风险拒绝）。组按（策略、品种、组名）划分，组名须规范（非空、无首尾空白、≤128 字节、无控制字符），`PendingCreate` 带非法组名被拒；`OCOSeq` 在创建时取账户挂单簿内最大序号加 1，用于按创建先后排序，无组名时必须为 0。仅当某组有至少两个成员且至少一个入场仍开放时才读存储，无 OCO 的计划不受影响。两条规则：(1) 组内任一成员已有成交（lot 持仓非零或 `FilledSteps > 0`）时，其余仍开放的未成交兄弟被撤销，原因 `oco_filled_sibling`；(2) 否则，同一 tick 内入场均「可执行」的成员只保留一个，其余撤销，原因 `oco_simultaneous`。可执行性由 owner 自己评估：在副本上按同一策略与排他性推进入场后再严格评估（挂在场所的限价是委托而非可执行）。保留者取已受保护者（有成交、场所订单、预留或出场）中序号最小者，否则取可执行者中 `OCOSeq` 最小（再按 lot ID）；受保护成员永不被撤销。多个成员都已有成交时全部保留并记 WARN。每个被撤销成员写入检查点 `pending-oco:<lot>`（`Reason`、`Group`、`Instrument`、`Winner`、`AtMS`）及一条 `Rejected` 为该原因的接受事件（`CommandID` 为 `oco-<原因>-<now>`），与计划同一提交；机器人侧 `BridgeTimeSeriesEntryCommand` 把 `EnterReq.OCOGroup` 写入 `PendingCreateSpec.OCOGroup`。
   - **同策略相反增量排序**（`pending_sequence.go`）：只在准备返回 `self_cross` 后触发，已能正常准备的计划不受影响。同一策略的两个 lot 仅在两侧都是退出意图（且 lot 不同）时可内部对冲（`settlement.go:ApplyInternalMatch`，各 lot 仍各自结算）；其余同策略相反意图不能互相对冲（会在冻结中间价上无成交地实现盈亏），因此按每品种每策略处理：减仓（含翻转的减仓部分）的 lot 不被延迟；减仓全部同向时，对侧纯增仓的 lot 被保持（目标重置为已投影数量，约束去除，`Desired` 与入场生命周期不动，下一 tick 重新评估）；无减仓而双向增仓时保留最小 lot ID 所在方向；两侧都是减仓时，仅自主 tick 保留优先级最高的一侧、延迟另一侧本 tick，命令准入下保持拒绝。优先级为该 lot 减仓已被连续延迟的 tick 数（`ReductionHolds`，大者优先），其次最小 lot ID；被延迟的 lot 计数加一，执行后清零，标记只对紧接着的下一个 tick 生效。无饥饿：压过它的 lot 必然至少被延迟过同样次数，会先执行并离开，新冲突的 lot 从 0 起算。
   - **隔离**（`pending_isolation.go`）：排序之后仍被拒时，仅对确定性、提交前的拒绝（`limit_off_tick`、`risk_limit`、`spot_short`、`venue_minimum`；`self_cross` 不隔离，联合错误不隔离）生效。只允许排除"已被 owner 准入（`CommandRevision > 0`）、仅增加持仓（新入场或加仓）、入场仍开放、无 `ReservedSteps`、无在途场所订单"的 lot；减仓、翻转、已有场所订单或预留的 lot、从未经准入的入场都不可排除。有 `Scope.Lot` 时精确排除该 lot；否则按 `Scope` 的策略与品种缩小候选，取账本顺序中最后一个（老 lot 优先保留）；`venue_minimum` 只在同品种内选择。每次排除后重新准备，最多 8 次（`isolationBudget`），每次准备原子回滚。被排除的入场在与计划同一提交内终止：`Canceled`、`Entry.State=Canceled`、`Desired` 与 `FilledSteps` 置为已持有数量、预留与 `VenueLimit` 清空，并写检查点 `pending-isolated:<lot>`（载荷含 `Code`、`Instrument`、`AtMS`）与 `StrategyAcceptedEvent`（`Kind=EntryIntent`，`CommandID=isolated-<code>-<ms>`，`Rejected=<code>`），随后记告警日志。不能归因、无候选或预算耗尽时返回原错误，沿用失败即冻结。
   - 仅回测使用的 `SharedOrderMgr`（实盘/dry_run 的时序部分一律是 `HubOrderMgr`）不经 owner 准入（`ApplyPendingCommand`）而直接写入挂单账本，其 `PendingDefinition.CommandRevision` 恒为 0，因此其入场不是隔离候选，拒绝仍会冻结。

9. **策略政策**
   `hub_policy.go:AcceptPolicyBatch` → `policy_acceptance.go:AcceptPolicyBatchContext`（证据校验、计划接受、状态与检查点同事务，报价冻结）；重放使用 `PolicyEvidence`/`ResumePolicy`；入场撤销 `policy_reconcile.go:CancelPolicyEntriesContext`，Hub 侧经 `hub_policy_cancel.go` 持久化后再撤。

10. **亏损熔断（`loss_stop.go`）**
    owner 在私有报告循环每个周期（及每次 `LossStopStatus` 查询）采样一次账户权益，样本按 `max(1 分钟, 最长窗口/360)` 分桶并只保留最长窗口内的历史，与策略状态同存于 `account-risk/loss-stop-v1` 检查点。对每个窗口取"窗口起点之前最近的样本"作基准；该样本过旧或不存在时改用窗口内最早的样本（度量区间只会缩短，不会超出配置窗口）。`(基准权益 - 当前权益)/基准权益` 达到阈值即写入 `PausedUntilMS = now + PauseMS`；暂停期间不重复触发、不延长。任一持仓缺少有效标记价时跳过本次采样，既不触发也不解除。熔断只暂停新开仓：owner 持久化并对所有机器人给出同一状态，机器人侧在准入时据此拒绝（见下方限制）。

10. **到期与结算**
    `option_expiry.go:ApplyOptionExpiry`、`terminal_settlement.go:ApplyTerminalSettlement`：在同一账户串行边界内关闭全部归属与实际仓位；存在未决订单时拒绝结算（避免"已结算仓位被迟到成交复活"）。

11. **投影与导出**
    `projection.go:EventsAfter` 按账户顺序读取已提交事件；`AdvanceProjection` 推进游标；消费者需按事件 ID 去重。`archive.go:ArchiveCommittedEvents` 将已提交事件导出为分块文件，不删除账本。

12. **模拟盘**
    `paper.go:PaperAdapter.Submit` → `submitLocked`（市价即时成交或按可见价挂单）→ `AdvanceQuote`（仅后续的可见盘口触发挂单）；`NewDurablePaperAdapter` 额外写入账户 SQLite 的操作日志并支持回放。

## 配置项与外部依赖

配置由 `entry/`、`runtime/` 读取后装配，本包只接收结构体（YAML 结构体位置见 `entry/execution_config.go`、`entry/hub_*.go`）。

- `live_provider`：机器人执行来源，取值包括 `banexg`、`hub`（省略时默认走 Hub）。Hub 为 `hub` 时缺失则 fail-closed，不回退到本地发送。
- `execution.hub_socket`：Hub Unix socket 路径，默认 `<datadir>/execution/hub.sock`；socket 目录不得 group/world 可写（`hub_rpc.go:ListenHubUnix`）。
- `execution.sender_lease_dir`：生产账户必填，且必须与旧发送方使用的绝对目录完全一致（`HUB.md`，`store_lock.go`）。
- `env`：Hub 账户仅接受 `prod` 或 `dry_run`（`hub.go:hubRegistrationHash`）。`dry_run` 当前显式要求零资金费。
- `banbot hub --datadir <abs> [--execution-socket <abs>]`：启动 Hub（`spider` 为别名）。Hub 不从 YAML 读取账户凭据，凭据随机器人的 `Register` 请求提供（不得回显/记录）。
- 存储路径：生产为绝对路径 SQLite 文件（`SharedExecutionOptions.StorePath`）；内存模式仅限模拟场，且 `HistoryPath` 只能用于回测输出。
- 杠杆配置：`leverage`（`accounts.<name>.leverage` 优先于根级，1–125 的整数）由机器人在 `registerRobotHubAccount` 中写入每个 `BanexgInstrument.Leverage`，随账户注册；`entry/hub_account.go` 校验 `margin_rate >= 1/leverage` 并在账户上线时调用 `ApplyLeverage`，失败即注册失败。只影响交易所保证金倍数，不改变名义金额；零值不触碰交易所设置，也不改变既有账户的注册内容。内嵌 provider 路径拒绝声明杠杆；请求级逐单 `Leverage` 与声明值不同时由时序管理器拒绝。
- 熔断配置：沿用 `fatal_stop`（`分钟: 亏损率`）与 `fatal_stop_hours`（默认 8），由时序订单管理器启动时换算为 `LossStopPolicy` 并登记到账户；同一账户的多个机器人应配置一致，后登记者覆盖。回测不启用。
- 超时（硬编码）：`OwnerExecutor` 默认 15s；`HubClient` 30s（`hub_rpc.go:NewHubClient`）；`HubServer` 读 15s、写 30s（`hub_rpc.go:112`）；报告 drain 15s（`account_service_reports.go:privateReportJoinTimeout`）；报告节拍 5s，全量场所快照 30s（`privateReconcileInterval`）。
- 外部依赖：`github.com/banbox/banexg`（`go.mod` 为 v0.2.66，见"已发现的问题"第 9 条）、`github.com/shopspring/decimal`（全部金额/价格）、`modernc.org/sqlite`、`golang.org/x/sys/unix`（flock）、`golang.org/x/sys/windows`（LockFileEx）。

## 与其他模块的交互边界

- **biz/**：订单管理与桥接层（`odmgr*.go`、`shared_*.go`、`hub_order_*.go`、`intent_bridge.go`、`time_series_*_bridge.go`）通过 `SharedAccountBorrow` 与 `HubClient` 调用本包；这是策略侧的主要入口。
- **runtime/**：`shared_account.go` 管理 `SharedAccount` 的生命周期与借用，`legacy_execution.go` 包装旧 TS 发送路径，`runtime.go` 装配。
- **entry/**：Hub 启动（`hub.go`、`hub_account.go`、`hub_registration.go`、`hub_feed.go`）、各策略的执行装配（`factor_*.go`、`time_series_*.go`、`mixed_factor_live.go`）、执行接管（`execution_takeover.go`）与配置（`execution_config.go`）。
- **factor/runner/**：`account_sink.go`、`live_policy.go`、`policy_run.go`、`policy_scope.go` 使用政策接受与账户 sink。
- **banexg（外部 SDK）**：只能经 `banexg_adapter.go`、`banexg_*.go`、`legacy_sender.go` 访问；`banexg_transport.go` 规定 SDK 调用必须在 owner 上下文中同步执行。
- **orm（`DataSeries` / `SeriesOHLCV`）**：仅 `ohlc_profile.go` 在回测撮合中读取 OHLC 兼容视图；执行核心不依赖 OHLC 字段，符合 AGENTS.md 中"DataSeries.Values 为统一模型"的约束。
- **时序投影**：策略通过 `TimeSeriesProjectionPort` 获得投影，三种实现（借用者、Hub 本地、Hub RPC）语义一致（`time_series_projection.go`）。

## 扩展与修改指南

- **接入新交易所能力**：先在 banexg 中实现并确认 SDK 行为；在 `BanexgAdapter` 中只做能力证明与字段映射（`Capabilities`、`NativeConditionalCapabilities`），不得在本包或 biz 中写交易所分支（AGENTS.md 规约）。能力未证明时返回明确错误而非降级。
- **新增订单状态或状态迁移**：同时修改 `ledger_types.go`（`RealOrderState`）、`adapter.go`（`Send`/`acknowledge`/`Recover`/`Cancel`）、`store.go`（`setOrderState`/`readOrder`）、`storage_memory.go:terminalOrder` 与 `ledger.go` 中依赖状态的判断；缺一即产生内存与 SQLite 行为分叉。
- **修改持久化结构体**：`payload` 为 JSON。`pending_order.go` 注释明确字段名需兼容既有检查点；新增字段应使用可选默认值，并考虑旧检查点的读取路径（`storage_schema.go`）。结构性变更需递增 `exec_schema` 版本并提供影子校验迁移。
- **新增 SQL 操作**：必须同时在 `storage_sql.go`（操作枚举与 SQL）与 `storage_memory_ops.go`（内存语义）中实现，并补充 `storage_memory_records.go`/`storage_history.go` 的记录映射（若涉及冷历史）。
- **新增 Hub RPC**：在 `hub_rpc.go` 的 `hubRPCRequest`/`hubRPCResponse` 与 `serveHTTP` 的 `switch` 中添加分支，并在 `HubClient` 与 `Hub` 中实现。涉及状态变更的命令必须携带 writer epoch 与 sequence，并保证相同命令的重试幂等（参照 `hub.go:RebalanceStrategy`）。
- **修改风控/组合预留**：`coordinator.go:Coordinate`、`risk.go:validatePlanReservation`、`exposure.go` 三处需同步；发送前复核（`Send`）不可删除。
- **修改撮合语义**：回测专用逻辑在 `ohlc_profile.go`（价格路径）与 `backtest_rules.go`（资金费、空洞/退市定价），实盘/模拟可见报价逻辑在 `intent.go:Evaluate`、`paper.go`；不要让 OHLC 未来区间进入可见报价路径（`ohlc_profile.go` 注释）。
- **必须遵守的约束**：
  - 网络写入只能经 `OwnerExecutor`，且必须在持久化 `Sending` 之后；不得在存储事务内做网络 IO。
  - 不确定结果一律写 `Unknown`/`CancelPending` 并保持冻结，不得以"未找到"推断拒单（除非适配器证明 `AuthoritativeNotFound`）。
  - 增量与累计成交报告不可在同一订单内混用。
  - 不得为绕过恢复而直接修改 SQLite 订单状态（见 `RECOVERY.md`）。
  - 每个写路径都要能在崩溃重放后幂等。
- **测试**：`execution/*_test.go` 覆盖大部分路径（如 `hub_test.go`、`account_service_reports_test.go`、`adapter` 相关用例）。修改后至少运行 `go test ./execution/...`，涉及 Hub/报告的改动建议加 `-race`。

## 已知限制与设计取舍

- **owner 每个 lot 只有一个止盈**：多级止盈由机器人侧 `biz/hub_order_ladder.go` 用 `set_protections` 编排；OCO 的同 tick 互斥与「已有成交则撤销兄弟」由 owner 执行（`pending_oco.go`），机器人仍在观察到成交后补发 `cancel_entry`（见 `05_biz_orders_live.md`）。`set_protections` 替换完整保护集，并清空该 lot 的保护运行状态（`Order.Protections`、`ProtectionLevels`、`ProtectionDone`，含追踪止损锚点）、撤销该 lot 的原生保护单。
- **账户行情要求本机时钟与交易所基本同步**：`market_stream.go:observedBookMS` 拒绝时间戳比本机接收时间晚超过 `accountClockSkewMS`（1 秒）或早于 `QuoteTTL` 的盘口；晚于接收时间但在容差内的盘口按接收时间观测（`AtMS = ReceivedMS`）。该判定适用于所有账户报价读取：行情流（`accountBookQuote`）、`BanexgAdapter.Observe`（内嵌 Bridge/因子报价）与默认限价盘口（`banexg_limit_price.go`）；本机时钟慢于交易所几百毫秒不会使上述读取失败。本机时钟比交易所慢 1 秒以上时，Hub 账户注册会以 `invalid or stale account depth timestamp` 失败（Hub 日志可见）；执行路径使用原始系统时钟，不做 NTP 偏移修正，部署主机必须保持时钟同步。

- **亏损熔断由机器人侧准入执行**，owner 不拒绝已到达的开仓命令：时序命令在机器人 journal 中先冻结后发送，被 owner 拒绝且机器人不会丢弃的冻结命令（见下条）会阻塞后续全部命令。因此 owner 负责度量与持久化暂停状态，`biz` 的时序管理器在冻结命令之前查询状态并拒绝。因子目标仓位路径不受熔断约束。
- 亏损熔断按权益变化度量，外部出入金会被计为权益变化（出入金本身已使账户风险冻结，需显式对账）。

- **时序回测不运行在本包的账户路径上**。实测（内存账本 + `PaperAdapter`，单个持仓）每根 bar 约 0.77 ms、1.15 万次分配；`biz.LocalOrderMgr` + `BanWallets` 的基准为每个 bar 事件约 8 µs（含 4 个策略、约 25 个持仓）。差距约两个数量级，来自每根 bar 的事务化检查点与 decimal 快照，不是可调优项。因此回测保留浮点引擎，两者只共享 `ohlc_profile.go` 与 `backtest_rules.go` 的规则函数。

- **owner 拒绝分类**（`hub_rejection.go`、`hub_rpc.go`）：`PrepareRebalance` 在单个本地事务内准备计划时返回的拒绝没有留下回执、修订、计划或挂单定义，机器人不可原样重发。封闭码为 `limit_off_tick`（贡献者限价不在 tick 上）、`risk_limit`（超出策略/账户总敞口、NAV 或保证金预算，状态相关）、`spot_short`（不可做空的品种出现空头目标）、`self_cross`（同计划同策略相反意图，两侧都是退出的除外，那种情形内部互抵）、`admission_limit`（账户准入策略拒绝新入场）、`account_not_ready`（新入场到达时账户未对账，如瞬时故障或冻结期间；只拒 `create`，其余命令仍为硬错误）、`stale_entry`（时序 `create` 的信号时间 `Metadata.CreatedMS` 早于 max(2×周期, 2 分钟)，`ws` 或无法解析的周期取 2 分钟；已接受的命令重放仍幂等，只拒首次准入）；`RejectedError.Scope`（策略、lot、品种、意图类型）只供 owner 内部归因，从不序列化。服务端按 `hubRPCRejection` 选择回传码：`ErrVenueMinimum`（`venueMinimumError` 带品种，`errors.Is` 仍成立）对任意路径回传 `venue_minimum`；封闭码只对 `/v1/pending-command` 的 `PendingCreate` 回传，因为退出、改单、保护变更被拒可能来自同一联合计划里的另一个 lot，丢弃它会让持仓无人管理，这些仍走 409 与固定文本（`hub operation failed; ...`）。回传形式为 `hubRPCResponse.Rejected`（固定码，不含错误文本），`HubClient` 还原为 `ErrVenueMinimum` 或 `*RejectedError`（`errors.Is` 按码相等）；未知码还原为固定文本的通用错误；服务端对这些被拒仍调用 `SetErrorObserver` 的观察者。机器人对这些码的处理见 `05_biz_orders_live.md`。
- **准入期 tick 校验**（`pending_command.go:checkOrderPriceTick`、`time_series_command.go`）：`PendingCreate`（入场限价 `Conditions.Limit` 与各保护的 `Limit`）、其余 `PendingCommand`（`Limit`、`Exit.Conditions.Limit`、`Protections[].Limit`，先于任何撤单或写入）与 `ApplyTimeSeriesExit` 的 `Conditions.Limit` 在准入时检查，非 tick 整数倍返回 `limit_off_tick`；否则它只会在自主 tick 中反复失败并冻结账户。只检查可能成为真实或场所订单价格的订单价；触发价、止损/止盈触发位、追踪激活价只与行情比较，不要求在 tick 上。原生条件单放置时若触发价或限价不在 tick 上，不放原生单，回退本地保护并记原因 `conditional_price_off_tick`（`pending_native.go`）。
- Hub 私有 socket 是 Unix domain socket，路径受操作系统长度限制（macOS 约 104 字节，Linux 约 108 字节）；`ListenHubUnix` 要求绝对路径、父目录不得对组/其他用户可写，且不会删除已存在的路径。`HubServer.Close` 在关闭 Hub 后删除自己创建的 socket。数据目录较深时用 `--execution-socket` / `execution.hub_socket` 指向短路径。
- Hub RPC 默认 30s 超时，多腿计划按腿串行提交，腿数较多时可能超过超时。
- Hub 投影、回调为 at-least-once 语义，消费方需幂等；journal 重建时需关注事件去重。
- `Reconcile` 不得冻结本地持有的 `CancelPending` 订单。
- **私有成交报告早于提交回执**（`banexg_adapter.go:storedIdentity`）：报告带有我方 client ID 与回执尚未记录的交易所订单 ID（市价单瞬时成交）时归属于该订单，仅作唤醒、权威状态由恢复查询；已记录的两个不同交易所 ID 仍冲突。
