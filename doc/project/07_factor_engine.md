# 07 因子引擎（factor/）模块介绍

> 读者：要修改、增删 `factor/` 功能的开发者或 AI。
> 依据：当前工作区源码（非测试 `.go`，约 3.3 万行）。文档由源码分组阅读整理，行号可能随改动漂移，修改前请以源码为准。
> 配套阅读：`doc/factor_expression_architecture.md`（表达式架构原理）、`doc/factor_expression_guide.md`（使用指南）、`doc/factor_portfolio_guide.md`（组合与持仓生命周期）、`doc/factor_live_trading.md`（实盘）、`doc/statarb_guide.md`（统计套利）。

## 定位与职责

`factor/` 是截面因子与多因子组合策略的计算与装配内核，同时承载统计套利（配对）与研究基础设施。它把“因子定义”变成可复用的计算计划，在回测、回放、实盘三种场景下按同一套语义计算因子值，再经组合策略（portfolio policy）生成目标持仓，最后交给账户/执行层落地。

核心边界：

- **计算内核**（顶层 `factor` 包）：节点 DAG、编译为 `Plan`、增量 `Session`、整段 `Batch`、截面算子、技术指标、快照冻结、组合目标与生命周期策略、决策轮次屏障、版本存储。只依赖标准库、`banta`、`banbot/orm`，不反向依赖子包。
- **表达式前端**（`factor/expr`）：受限算术语法 → 原生 `factor.Node`。不查库、不下单、不执行任意代码。
- **回测账本**（`factor/backtest`）：数量型账本 `Book`，支持权重模式与绝对数量分配（allocation）两条执行路径。
- **研究**（`factor/research`）：标签、IC 合成、诊断、实验台账、模型训练与发布、风险优化、生命周期统计。
- **运行装配**（`factor/runner`）：回放 `Run`、实盘 `Live`、决策引擎、输入（归档/分页存储）、账户接纳（sink）、纸面账户、组合策略运行时。
- **统计工具箱**（`factor/statistics`）：AFML 系列（标签、权重、特征、集成、验证、HRP 等）、Chan 系列（ARIMA/GARCH/HMM/SVM/订单簿等）、协整与配对（Johansen、Vidyamurthy、Kalman、带状交易器）、期权与订单流等。其中仅随机森林与配对 builder 被生产代码直接调用。

不负责：数据采集与数据库写入（由 `orm`/数据源层完成）、交易所对接（由 `banexg`/`execution` 层完成，见 AGENTS.md 开发规约）、配置文件的 YAML 解析（由 `entry/` 完成）。

## 目录/文件地图

### factor/ 顶层（计算内核）

| 文件 | 作用 |
|---|---|
| `dag.go` | `NodeSpec`/`Node`/`Builder`；`Compile` 生成 `Plan`（循环检测、拓扑排序、公共子表达式去重、warmup/retention 推导、`Inputs()`） |
| `session.go` | 有状态增量计算 `Session`（`NewSession`、`Warmup`、`Evaluate`、`fork`/`commit`）；`Frame` |
| `batch.go` | `Plan.Batch` 整段历史计算；静态 universe 走 `banta/tav`，动态 universe 退化为 `Session` |
| `value.go` | `Numeric`（值 + `Validity`）、`Number()`、`contentHash`（按位哈希，`-0` 与 `+0` 不同；`map[string]any` 标量行走无 fmt 的快速编码，输出字节与反射路径一致）、`cloneValues`（标量行直接拷贝 map，其余走反射深拷贝） |
| `operators.go` | 截面算子：rank、zscore、robust-zscore、winsorize、quantile、group 系列、residual |
| `indicators.go` / `indicators_runtime.go` | 技术指标声明、参数校验、lookback；增量与批量实现（含 RSI 的 Wilder 状态） |
| `snapshot.go` | `Universe`、`SnapshotSpec`、`Freeze` 冻结输入（PIT 视图）、`Requirement`/`StreamKey`/`SnapshotStatus` |
| `allocation.go` | `PortfolioTarget`、`Allocation`；十进制精确数量运算（`big.Rat`）、默认量子 1e-18 |
| `portfolio.go` | `PortfolioSpec`、`FrozenBudget`、`TargetPortfolio`（Full/Patch 生效目标）、`TopBottomK*` |
| `policy.go` | `LifecyclePolicy.Propose` 主流程；`LifecycleState`、`AssetLifecycle`、`PortfolioCohort`；checkpoint 编解码；`AssetForgetter`/`LifecyclePolicy.ForgetAssets`（PIT 剔除时清洗 checkpoint） |
| `policy_config.go` | 组合策略 JSON 配置结构（Rebalance/Selection/Holding/Transition/Allocation）与 `NormalizePortfolioPolicyConfig` |
| `policy_cohort.go` | cohort 模式：批次创建、成交对账、过期与内部转移 |
| `policy_constraints.go` | 保留（retention）、剩余预算分配、风险约束（资产/边、组、净值、beta、换手） |
| `policy_selection.go` | 分数排序、多空选股、`AllocateSelected` 权重分配 |
| `barrier.go` | `RoundBarrier`：一个决策轮的 Begin/Observe/Freeze/Compute/Cancel/Stop |
| `version_store.go` | `VersionStore`：有界内存分片；`Export`（gob + `os.Link` 原子发布）与 `OpenVersionStore`（校验 hash） |
| `record_hash.go` | `RecordHasher`：标量 Values 的 `VersionRecord` 不经反射直接写出 canonical 字节再 sha256，摘要与 `ContentHash(record)` 逐位相同（回放 `InputDigest` 逐记录折入的就是它）。键序按键集缓存（长度相同且缓存键全在 map 中即同一键集）。`VersionRecord`/`orm.DataSeries` 的字段名或类型与 `init` 时预期不符、记录带 `ExSymbol`/`Adj`/私有聚合缓存（`DataSeries.HasNoAggRows`）或 Values 含非标量时自动退回反射路径（结果不变，只是变慢）；`TestRecordHasherMatchesLegacy` 对照旧实现并断言快路径仍启用 |
| `version_store.go`（接管/浅复制） | `CloneVersionRecord` 深复制；`AdoptVersionRecord` 校验身份并去掉运行时对象，标量 Values 按引用接管（调用方独占该 map），否则深复制，返回是否全标量；`CloneScalarRecord` 对已接管的全标量记录用 `maps.Clone` 复制 |

### factor/expr（表达式前端）

| 文件 | 作用 |
|---|---|
| `parser.go` | `text/scanner` 词法 + 递归下降解析为 AST；定义上限常量（maxText/maxNodes/maxDepth/maxWindow/maxDefinitions，约 `parser.go:11-18`） |
| `compile.go` | `Spec`/`Binding`/`compiler`；名称绑定、参数检查、`lower` 为 `factor.Node`；每个定义先单独 `factor.Compile` 校验，再整体编译 |

### factor/backtest（回测账本）

| 文件 | 作用 |
|---|---|
| `book.go` | `Book`：现金、数量、标记价、费用、资金费、滑点、权重目标执行（`Execute`） |
| `pit.go` | `Book.SetFundingApr`/`Book.accrueFunding`/`Book.settleFunding`（近似资金费，BtFeatureLevel 13 起按账户整体净多头名义额计：`max(0, Σ多头名义额 − Σ空头名义额) × 年化% × 两次标记间隔`，净空头为 0 不付不收；按计费步（`Mark` 的 `now`，同一批价格）归并，不按各品种的报价时间 `AtMS`：各品种逐个标记，各自带符号项〔多头为正、空头为负，各按自己的间隔加权〕累加后对 0 取下限，现金随累加增量扣减，该步结束后即净额，与标记顺序无关；步内报价时间错位仍抵消；持仓腿在该步缺 bar 时，只要账本持有空头，`settleFunding` 就以其最后有效价的名义额补上该步的项并记 `fundingBilled`，该腿下次标记不再重复计这段时间；没有空头的账本不补项，每条多头腿仍在自己的标记点按自己的间隔计，与旧逐腿口径逐位相同；`settleFunding` 在读写现金/持仓的入口与下一步开始时调用；资金费只记账户级（`AttributionReport.Funding`），不进品种的 `Costs`/`Net`，品种 `Net` 合计 = NAV 变动 + `Funding`；单次最多计 3 天，记入 `State.Funding` 并扣现金，配置键 `manifest.costs.funding_apr`）、`Book.NeverSeenAll`（标的从未有标记/持仓，可安全剔除）、`Book.ExitAtLastMark`（宇宙剔除标的按最后标记价平仓，含费用、滑点与 `SetPITDelistPenaltyBps` 的退市惩罚）、`Book.ForgetPolicyAssets`（lifecycle policy 清洗被剔标的状态）、`DropTargets` |
| `attribution.go` | 多空腿/逐标的归因累加器（`Book.EnableAttribution`、`SampleExposure`、`Attribution`）：只在价格变动点累加，不读写账本数值 |
| `policy.go` | 绝对数量分配执行（`ExecuteAllocation`）、`PolicyEvidence`、`RestorePolicy`、`AcceptProposal` |
| `terminal.go` | 终止结算与终止标的报价豁免（`ApplyTerminalSettlement`） |

### factor/research（研究）

| 文件 | 作用 |
|---|---|
| `experiment.go` | `Trial`/`TrialLedger`（JSONL 追加 + fsync）、`CompareTrials`、`SelectParameters`/`ResolveParameters`（按 AvailableAt 做 PIT 选择）、`ResearchCache`、`FactorRegistry`、随机基线 |
| `manifest.go` | `ManifestSpec`/`Manifest`（身份哈希，`BuildManifest`）、`WritePanel`（面板输出到任意 `io.Writer`） |
| `model.go` | `TrainingSample`/`TrainingWindow`、`RollingWindows`、`Trainer`/`Predictor` 接口、模型注册表 `RegisterModel`、`LinearModel`、`FitModel`/`RestoreModel`/`PublishModel`/`LoadModel` |
| `forest.go` | 随机森林 trainer，调用 `factor/statistics` 的 `TrainRandomForest` |
| `combine.go` | `ComboSpec`、`ICHistory`（滚动 IC）、`Combine`（截面打分）、动量/波动因子默认计划（`MomentumVolPlan`） |
| `labels.go` | `LabelSpec`/`Label`、`ReturnLabel`、`LabelQueue`（Schedule/Resolve/Drain） |
| `diagnostics.go` | `Evaluate`（IC、排序相关、分位收益、换手等报告）、`Accumulator`（跨期累加） |
| `lifecycle.go` | `SummarizeLifecycles`、`SummarizeCapital`、`EstimateCapacityCost`、`Attribute`、`HACMean`、`RankAutocorrelation` |
| `risk.go` | `EstimateCovariance`（收缩估计）、`OptimizePortfolio`（投影梯度，迭代上限 10000） |
| `portfolio_definition.go` | `PortfolioDefinition`：组合定义 → `PortfolioPolicyConfig`，校验与解析 |
| `numerical.go` | `centerScaled` 数值中心化 |

### factor/runner（运行装配）

| 文件 | 作用 |
|---|---|
| `runner.go` | 回放入口 `Run`；`Config`、`Result`、`Mode`（Research/Weights/Events/Trade）；输出接口 `Sink`/`Output` |
| `live.go` | 实盘 `Live`：`NewLive`、`Warmup`、`Observe`、`FlushAt`、`prepareRound`、`process`、`execute`、`Stop`/`Join` |
| `live_policy.go` | 实盘组合策略路径：`proposePolicyRound`、`monitorPolicy`、`executePolicy` |
| `policy_run.go` | 组合策略运行态 `policyRun`：`propose`、`refresh`、`proposalReady`，待提案 `pendingProposal` |
| `policy_scope.go` | `Live.RetainPolicyScope`（保留旧 SID 的代际切换）、`FullZeroOmissions` |
| `policy_registry.go` | 组合策略注册表（内置 `lifecycle-v1` → `NewLifecyclePolicy`） |
| `definition.go` | 全局 definition 注册表、builder 与 portfolio factory 注册；`PreparePortfolioConfig` |
| `computation.go` | 共享计算：`ComputationGroup`、`sharedComputation`、`computationKey`、`RunMany`（多消费者 lockstep） |
| `decision.go` | `compileDecision`、`decisionEngine`（`evaluate`→`combine`→`buildPortfolio`） |
| `input.go` | 归档输入 `archiveInput`（`OpenVersionStore` 全量加载后分批）与 `replayInput` |
| `storage_input.go` | 分页存储输入：strict（需 `VersionPageFactory`）与 static-approximation（`QueryPage`）两种可见性策略；static 路径对 K 线流在后台预读下一页（页内容与顺序不变，`QueryPage` 对 K 线流须可并发调用）。后台并发由 `data.prefetch_concurrency`（缺省 4，范围 1..32，不进入输入身份）限制，且在同一 factory 的所有输入间共享：**每个回测进程额外占用的 Postgres 连接 ≤ `prefetch_concurrency`（缺省 4），另加每个正在回放的输入同步读取的 1 个**。已预读但未消费的页（每流至多一页，≤ `prefetch_rows` = 页大小×流数 ≤ `max_records/2` 行）不计入保留预算，常驻行数上界为 1.5×`max_records`；`pctx` 派生自首次 `Next` 的 ctx，调用方须全程传同一个运行级 ctx。页大小 = `prefetch_rows / 流数`，`prefetch_rows` 缺省取 `max_records / 2`，`max_records` 缺省 500000（`entry/factor_config.go:defaultFactorMaxRecords`；它进入输入身份哈希，因此改默认值会改变 ManifestID，不影响指标与事件） |
| `account_sink.go` | `AccountSink`：`RegisterExecution`、`ObserveQuote`、`StrategyState`（NAV）、`ProcessSnapshot`、资金费观测 |
| `account_policy_sink.go` | `PolicyEvidence` 重建持仓、`RestorePolicy`、`AcceptProposal`（checkpoint 提交）、`CancelIncreasingPending` |
| `account_application.go` | `AccountQueries`/`AccountCommands`/`AccountApplication` 接口与校验 |
| `hub_account_application.go` | 远程 hub 版 application：writer lease、命令序号、`NewHubAccountSink` |
| `paper.go` / `paper_sink.go` / `paper_group.go` | 本地纸面账户；同账户多策略共享 borrow 与 paperMarket |
| `validate.go` / `live_validate.go` | 回放/实盘配置校验，`resolveDecisionPortfolio` |
| `risk_builder.go` / `model_builder.go` | 自定义风险优化与模型组合 builder 的配置与构造 |
| `pairs.go` / `simple.go` | 统计套利配对 builder（`PairsSpec`，使用 `statistics.BandTrader`）、简单因子注册 |
| `pairs_report.go` / `report.go` | 每对独立结果的记账（`pairStats`、`pairsTracker.report`）与结果类型（`PairResult`、`PairEpisode`、`Attribution`、`SymbolContribution`、`buildAttribution`）；`runObservers` 把 factory 内创建的配对状态带回 `Result` |
| `params.go` / `scan.go` / `config.go` | 参数容器（`Params`，`Unused` 校验）、参数扫描、运行配置辅助 |
| `terminal.go` / `option_expiry.go` | 终止标的与期权到期结算相关处理 |
| `source_events.go` / `timeline.go` / `incomplete.go` | 数据源事件、同账户回放时间线排序、未完整数据处理（`IncompleteStream` 汇总；`pitReduceUniverse` 推导决策网格的时点宇宙） |
| `output.go` / `artifact.go` | 输出适配（`TargetAccepted` 优先，回退 `Executed`；可选扩展 `NavOutput.Nav`，`JSONOutput` 实现为 `nav` 事件 `{Kind, AtMS, NAV, Cash}`；`JSONOutput.NoPanel` 跳过每个决策的 panel 行）、运行产物 `.run-*.json` 原子写入 |

### factor/statistics（统计工具箱）

按主题分组（括号内为代表文件，共约 99 个非测试文件）：

- **统计套利/协整**：`johansen.go`（Johansen VECM，临界值需外部给入）、`stationarity.go`（ADF 统计量与 EngleGranger，非 p 值）、`vidyamurthy.go`（滞后价差滤波、Bootstrap 持续期、正则化选择）、`spreads.go`（阈值经验估计、穿越持续时间）、`pairs.go`（`Regress`、`FitOU`、`KalmanPair` 等基础件，内含 `finite` 副本）。
- **配对信号**：`pair_signal.go`（`BandTrader` z 分数滞回状态机、`RollingSpread`）、`kalman_fit.go`（Q/R 的 MLE 拟合）、`vid_*.go`（滞后选择、配对成交校验、二部图流、动态 AR(1)、合并失败概率）。
- **Chan 系列**（`chan_*.go`，24 个）：时序（arima/arma/garch/var/vec/trend/locallevel/hmm）、机器学习（svm/nn/tree/classification/stepwise）、组合与风险（frontier/umd/multisort/calmar）、微观结构（orderbook/displayed_sweep/trade_classification/implied_quotes/crossvenue/moving_volume）。
- **AFML 系列**（`afml*.go`，49 个）：CUSUM 与三重屏障标签、活动/信息 bar、序贯 bootstrap 与随机森林、PurgedKFold/CPCV/PBO、PSR/DSR、HRP 与最小方差、双高斯仓位、熵特征、VPIN/PIN、流式协方差与 PCA。
- **因子分析与组合**：`factor_analysis.go`（EM）、`factor_rotation.go`（varimax）、`pca.go`、`neutral_allocation.go`。
- **期权**：`options.go`（BSM 与隐含波动率）、`options_american.go`（CRR 树）、`option_surface.go`（OI 加权 IV 价差、smirk）。
- **订单流/价差/其他**：`orderflow.go`（BVC、VPIN、Kyle/Amihud、Hasbrouck-Gibbs）、`pole_source_tools.go`、`temporal.go`、`runs.go`（游程检验）、`bet_sizing.go`、`capital.go`（Kelly 与回撤）。

## 核心类型与接口

| 类型 | 位置 | 要点 |
|---|---|---|
| `Node` / `NodeSpec` | `dag.go:44` / `dag.go:19` | 节点身份 = 规范化 NodeSpec 与有序依赖 ID 的内容哈希；相同 ID 合并 |
| `Plan` | `dag.go:229` | 拓扑排序后的节点、输出映射、`Inputs()`（实际可达的 source/timeframe/字段）、warmup 与 retention |
| `Session` | `session.go:45` | 有状态增量计算；按资产维护 `assetState`（`session.go:36`）；`Warmup`/`Evaluate`；`fork`/`commit` 支持屏障试算 |
| `Frame` | `session.go:16` | 一个决策网格上的因子列结果 |
| `Numeric` / `Validity` | `value.go:27` / `value.go:15` | 值 + 有效性（如 Missing、NonFinite、Warmup）；无效值不参与截面统计 |
| `Universe` / `SnapshotSpec` / `Snapshot` | `snapshot.go:11` / `:36` / `:94` | 冻结的输入视图；`Freeze` 按 Requirement 选取可见数据 |
| `PortfolioTarget` / `Allocation` | `allocation.go:28` / `:21` | 目标持仓（十进制数量）与分配项；`NewPortfolioTarget` 规范化数值 |
| `PortfolioSpec` / `TargetPortfolio` / `FrozenBudget` | `portfolio.go:24` / `:45` / `:19` | 组合规格、生效目标、冻结预算 |
| `PortfolioPolicyConfig` | `policy_config.go:83` | 组合策略完整配置，子项 `RebalanceConfig`(:12)、`SelectionConfig`(:21)、`HoldingConfig`(:45)、`TransitionConfig`(:58)、`AllocationConfig`(:72) |
| `LifecyclePolicy` | `policy.go` | `Propose(PortfolioContext) → PortfolioProposal`；构造后配置不可变（已 clone） |
| `LifecycleState` / `AssetLifecycle` / `PortfolioCohort` | `policy.go:160` / `:131` / `:152` | 跨轮次状态，经 checkpoint 持久化；需 `ConfigHash`、`SIDMappingVersion`、`AssetIdentities` 兼容 |
| `RoundBarrier` / `RoundKey` / `RoundToken` | `barrier.go:55` / `:28` / `:34` | 一个决策代的并发屏障，保证同一轮只计算一次 |
| `VersionRecord` / `VersionStore` | `version_store.go:17` / `:62` | 版本化数据记录的有界存储与不可变归档 |
| `Book` | `backtest/book.go` | 回测账本；`Mark`、`ApplyFunding`、`Execute`、`ApplyTerminalSettlement` |
| `expr.Spec` / `expr.Binding` | `expr/compile.go:22` / `:16` | 表达式规格与绑定别名（`kline.close` 中 `kline` 为绑定别名） |
| `ComboSpec` / `ICHistory` | `research/combine.go:25` / `:48` | 合成方法（等权、IC 加权、质量加权等）与滚动 IC 窗口 |
| `Manifest` / `ManifestSpec` | `research/manifest.go:52` / `:40` | 研究产物身份；strategy hash 排除部分字段 |
| `Trial` / `TrialLedger` | `research/experiment.go:89` / `:131` | 实验试验记录与追加式台账 |
| `Trainer` / `Predictor` / `ModelArtifact` | `research/model.go:99` / `:95` / `:201` | 模型训练接口与内容寻址产物 |
| `Label` / `LabelQueue` | `research/labels.go:26` / `:61` | 标签与延迟解析队列 |
| `Runner` 配置与模式 | `runner/runner.go:36`（`Config`）、`:22-29`（`Mode`） | Research / Weights / Events / Trade |
| `Live` | `runner/live.go:19` | 实盘驱动；`mu`/`workMu` 双锁，持有 rows、quotes、pending、policyPending、barrier |
| `AccountSink` | `runner/account_sink.go:29` | 账户视图与下单接纳，依赖 `AccountApplication` |
| `AccountApplication` | `runner/account_application.go:30` | 查询 + 命令接口；本地与 hub 两种实现 |
| `policySinkCheckpoint` | `runner/account_policy_sink.go:19` | 组合策略接纳的原子 checkpoint（Version、State、Target、Sequence 等） |
| `HistoricalInput` / `storageInput` | `runner/input.go:42` / `runner/storage_input.go:150` | 回放输入抽象与分页存储实现 |
| `BandConfig` / `BandTrader` | `statistics/pair_signal.go:15` / `:44` | 价差 z 分数滞回交易器，+1 多价差，-1 空价差 |
| `RollingSpread` / `KalmanPair` | `statistics/pair_signal.go:133` / `statistics/pairs.go:135` | 滚动 OLS 价差；Kalman 新息（预更新） |

## 关键流程

### 1. 因子定义 → 计算计划（编译）

1. 表达式路径：`expr.Compile`（`expr/compile.go:75`）调用 `parse`（`parser.go:37`）→ `named`（`compile.go:164`）→ `lower`（`compile.go:260`）→ `call`（`:330`）或 `indicatorCall`（`:467`），产出原生 `factor.Node`。
2. Go builder 路径：直接调用 `factor.Field`/`EMA`/`Rank` 等 builder（`dag.go` 内 `node()`，`dag.go:50`）。
3. 两条路径汇合于 `factor.Compile`（`dag.go:299`）：`visitNode` 递归，调用 `validateNode`（`dag.go:410`）与 `indicatorLookback`（`indicators.go:160`），做循环检测与公共子表达式去重，输出 `*Plan`。
4. 计划是只读可复用的；会话状态由具体运行拥有。当前没有跨任务的表达式编译缓存。

### 2. 在线增量计算（实盘与逐步回放）

`NewSession`（`session.go:59`）→ `Freeze`（`snapshot.go:101`）→ `Warmup`（`session.go:90`）→ 每个决策网格 `Evaluate`（`session.go:83`）→ `evaluate`（`session.go:95`）→ `snapshotIdentity`（`session.go:263`）→ `evaluateTS`（`session.go:308`）。截面部分走 `crossSection`（`operators.go:38`），技术指标走 `computeTechnicalIncremental`/`asset.evaluateTechnical`（`indicators_runtime.go:250`）。

### 3. 整段历史计算

`Plan.Batch`（`batch.go:15`）：静态 universe 调用 `banta/tav` 的向量化实现；动态 universe 退化为逐步 `Session`。两者都校验时间单调（`batch.go:44`），但 `Session` 路径在 `Warmup` 之后不再记录上一个 DecisionTime。

### 4. 回放运行（archival / storage 输入）

`runner.Run`（`runner.go:119`）：
1. `PreparePortfolioConfig`（`runner.go:175`，`definition.go:126`）解析组合定义。
2. `compileDecision` → `newDecisionEngine`（`decision.go:147`，`decision.go:18` 调用 `expr.Compile`）。
3. 按 chunk 循环 `driver.Next`，每批：更新报价与资金费、终止结算、`ObserveBatch`、策略 propose/accept、`ProcessSnapshot`、`finish`。
4. 在决策网格上：`barrier.Begin/Observe/Freeze`（Weights 模式下快照不完整时见下文时点宇宙）→ Weights 模式向 `NavOutput` 输出该决策时刻的逐价值权益 → `engine.evaluate` → `combine` → `buildPortfolio`（`decision.go:195`）。
5. 输入：归档路径 `openArchiveInput`（`input.go:99`）全量读取 `OpenVersionStore` 记录后按可见性分批；分页路径 `storageInputFactory.Open`（`storage_input.go:109`）→ `fill` → `Next` → `Visible`。
6. 共享回放 `RunMany`（`computation.go:167`）为每个配置启动 goroutine 调用 `Run`（`computation.go:215-225`），同账户事件经 `replayTimeline`（`timeline.go`）排序。

NAV 破产（BtFeatureLevel 12，issue 162）：Weights 模式在每个决策点（价格/资金费/终局结算处理之后、挂单执行之前）检查 `Book.State().NAV`；≤ 0 时 `Book.Liquidate` 按最后标价平掉全部仓位并把现金核销为 0（NAV 0，不计强平成本），`Result.Liquidated/LiquidatedAtMS/LiquidatedCause(equity_depleted)/LiquidatedReason/LiquidatedNAV/WindowEndMS` 写入 `run.json.Results[0]`，最后一个 `nav` 事件为 0，之后不再决策（剩余 chunk 不读）；最后一次决策之后的标价把 NAV 打到 ≤ 0 的同样处理（时刻为最后一个事件）。统计套利配对路径共用同一账本，同样适用；`PairsNAVReturn` 记 −100%。`NewTargetPortfolio` 要求冻结时 NAV 有限且为正，故 NAV ≤ 0 必须在进入下一决策前处理。报告层（bansite）把 NAV 0 延到 `WindowEndMS`，总收益 −100%、最大回撤 100%、UPI 为负的有限值。NAV 始终 > 0 的运行字节不变（新增字段 omitempty）。

时点宇宙（PIT）：Weights 模式（账本存在）下，网格上缺输入或输入无效的 Investable 标的（数据终止/未上市/空洞）被剔除，决策在缩减后的宇宙上继续，而不是整次跳过。生产形态下五个池相同（`Reference=Investable=Tradable=Evaluation=Tracked`，见 `entry/factor_storage.go:assembleFactorStorageInputs`）：可剔除集合是 Investable，`refOnly = Reference \ Investable` 里的标的（仅作参照）缺数据则不缩减；缩减后各池（含 Reference，截面算子 `crossSection` 以它为总体）都去掉被剔标的。若缺失的还有 Investable 之外的流，或剩余 Investable 少于 `PITMinUniverseRatio × 上一次被接受决策时的存活数`（滚动分母，首个决策用名义池大小；默认 0.5，被拒绝的决策不更新分母，完整快照与被接受的缩减决策都会把分母更新为当时的存活数；`< 0` 关闭 PIT），则不缩减（回到跳过整次决策）。逐个退市（30→29→28…）始终被接受，单轮内大面积缺失被拒绝；连续多轮“每轮少一点”的慢中断与逐步退市无法区分，同样被接受；单轮之内就永久失去超过 `1−ratio` 的标的则一直被拒绝（视为数据中断）。缩减后存活不足 2 个但通过门槛时（名义池 ≥3），被剔标的照常平仓并暂停该周期（计入 `PausedPeriods` 与 `UniverseDrops`，不计 `Incomplete`）；名义池只有 2 个时仍拒绝缩减。配对模式 `pitReducePairs` 没有比例门槛。`UniverseNominal` 仍是静态名义池，平均有效宇宙口径不变。`Config.PITMinUniverseRatio` 与 `Config.PITDelistPenaltyBps` 可由策略条目的 `pit_min_universe_ratio`（≤1，0=默认，<0 关 PIT）与 `pit_delist_penalty_bps`（有限数）配置；`PITDelistPenaltyBps`（0=默认 100bp，<0 关）为被剔除持仓平仓时额外的不利惩罚；若该标的数据之后恢复，惩罚被退回并改按“缺口前后更差价”规则计价（累计成本见 `Result.PITGapLoss/PITDelistLoss`）。缩减宇宙的 `Version` 带 `#pit-<hash>` 后缀；被剔除且仍持仓的标的由 `Book.ExitAtLastMark` 按最后有效标记价（含费用与滑点）平仓，并从 pending/executed/previous 目标中移除（`DropTargets`）。`Result` 记录 `UniverseDrops`、`UniverseDropDecisions`、`DelistExits`、`DroppedBySID` 与有界的 `UniverseDropLog`（上限 2000）；决策诊断带 `pit-universe-drop`。配对模式下 PIT 按配对判定（`Config.PITPairs`，显式给出或由已注册配对策略的 `registeredPairLegs` 推导）：某腿缺数据的配对平仓并暂停（诊断 `pair-closed-missing-leg`，`BandTrader.Reset`），其余配对继续，“宇宙过小”回退不适用；两腿恢复后可重新开仓；所有配对同期暂停的周期计入 `Result.PausedPeriods`，不计入 `Incomplete`。**不可用的腿（BtFeatureLevel 11）**：配对在形成期按当时信息选定，运行窗口内完全没有数据的腿（退市品种在留出段）已被池刷新从本次运行的宇宙里拿掉（`goods.RuntimeDeps.SkippedPairs` → `Config.UnavailableSymbols`，由 `entry/factor_storage.go:prepareFactorStorageInputsReport` 写入，`json:"-"`）；`newTrackedPairsBuilderSkipping` 对“腿既不在宇宙里、又在 `UnavailableSymbols` 中”的配对不再报错，而是整次运行跳过该配对（不计价差、不开仓、`PairResult.Unavailable="leg_unavailable"`、`UnavailableLeg` 为缺失的腿），其余配对照常运行。腿既不在宇宙也不在该集合（拼写错误、不在池中）仍报 `has a symbol missing from this run's universe`。每对敞口保持 `Gross`（默认 `1/配对总数`，按原配对数分配）：被跳过配对的敞口闲置，其余配对**不放大**，留出段不会因配对减少而自动加杠杆。全部配对都被跳过时 builder 每个决策给出显式空目标（诊断 `pairs-all-unavailable`），回测正常结束，权益恒为初始值、0 笔交易，`Result.AllPairsUnavailable=true`。窗口中途退市的腿不走这条路径，仍按上面的 PIT 规则在数据结束时平仓并暂停该配对。`run.json` 的变化见 02_data_storage（`skipped_pairs` 条目、`all_pairs_unavailable`）。`Result` 另含 `UniverseNominal/UniverseDecisions`（平均有效宇宙 = Nominal − Drops/Decisions）。

lifecycle policy 下的 PIT：被剔标的若已有标记/持仓/policy 状态（退市、空洞，非“从未出现过”），先由 `Book.ExitAtLastMark` 按最后有效价平仓（与无 policy 路径一致），再由 `Book.ForgetPolicyAssets` 清洗：policy checkpoint（经 `factor.AssetForgetter.ForgetAssets`，`LifecyclePolicy` 删除其 `Assets`/`AssetIdentities` 与 cohort 贡献，空 cohort 丢弃）、`previousAllocation` 与未确认成交事实；有变化时 `stateVersion++`，`policyRun` 据此重载状态；在途 `policyPending` 作废（计入 `Result.Skipped`），随后决策在缩减宇宙上继续，被剔标的对 policy 等同未曾持有（不占名额/持有期/冷却）。数据恢复后该标的当作新标的重新入选。策略未实现 `AssetForgetter`，或账本证据来自外部 `PolicySink` 时，回到“整轮不缩减、计入 `Incomplete`”。

回测账本在 `Run` 中的调用：`NewBook`（`runner.go:258`）→ `Mark`（`:463`）→ `ApplyFunding`（`:502`）→ `ApplyTerminalSettlement`（`:518`）→ `AcceptProposal`（`:572`，allocation 路径）或 `Execute`（`:646`，Weights 模式）→ `EndChunk`（`:881`）。

**存储输入的 1m 价格流**：`StorageInputOptions.OwnedPages`（仅 `entry/factor_storage.go` 对数据库 K 线读取器打开，默认 false）声明 `QueryPage` 返回的是新解码、无他处引用的行：`fill` 用 `AdoptVersionRecord` 接管其 Values，`Next` 给保留历史用 `CloneScalarRecord`。约定：`QueryPage` 返回的行不得被别处引用或复用；给读取器加缓存时必须返回新建的 map，或关闭该开关。批次按 (Sid, Source, TimeFrame) 序访问游标组装（`fill` 顺序不变），`sortHistoricalRecords` 在批次已严格有序时跳过排序（无并列，结果同排序）。以上不改变记录内容、`MaxRawRecords` 与各摘要。`BenchmarkStorageReplay1m` 度量该路径。

### 5. 组合策略（lifecycle-v1）提案

`LifecyclePolicy.Propose`（`policy.go`）：
- 先 `NormalizePortfolioPolicyConfig`（构造时）与 `DecodeLifecycleState`，再 `pruneReleasedLifecycle`；
- cohort 分支 `proposeCohort`（`policy_cohort.go`）：`ReconcileCohortContributions`、`TransferCohortContributions`；
- 普通分支：`SelectPortfolioScore` → `AllocateSelected`（`policy_selection.go`）→ `applyRetention`（`policy_constraints.go`）→ 逐资产过渡循环 → `allocateRemainder` → `constrain` → `finish` → `NewPortfolioTarget`（`portfolio.go`）。

运行态在 `runner/policy_run.go`：`policyRun.propose`（`:73`）与 `refresh`（`:151`）产出 `pendingProposal`（`:36`），版本号递增（`:194`）。

### 6. 实盘 Live

`NewLive`（`live.go:51`）→ `Warmup`（`:248`，加载历史）→ `Observe`（`:362`，provider 推送，在调用方 goroutine 同步 `process`）→ `FlushAt`（`:626`，外部 provider 驱动决策）→ `prepareRound` → `process` → `execute`（`:498`，普通策略）或 `executePolicy`（`live_policy.go:128`，组合策略）。唯一自起 goroutine 的是 `Join`（`:743`）；时间来自注入的 `clock`（`:51`），没有内部定时器。`Stop`（`:724`）结束运行。

### 7. 账户接纳与 checkpoint

组合策略提案经 `AccountPolicySink.AcceptProposal`（`account_policy_sink.go:196`）：计算 quote 与 steps，调用 `application().AcceptPolicyBatchContext`；远程 hub 由 `HubAccountApplication` 注入 epoch 与 sequence（`hub_account_application.go:124` 的同一 PlanID 序号固定）。持仓与 NAV 由 `PolicyEvidence`（`account_policy_sink.go:46`）与 `StrategyState`（`account_sink.go:124`）重建。普通再平衡走 `ProcessSnapshot`（`account_sink.go:168`）。

### 8. 研究流程

- 运行主链（`runner/runner.go` 调用）：`BuildManifest`（`manifest.go:81`，`runner.go:144`）→ `ReturnLabel`（`labels.go:45`，`runner.go:359`）→ `Evaluate`（`diagnostics.go:143`，`runner.go:365`）→ `Accumulator.Add`/`Summary`（`diagnostics.go:349/386`，`runner.go:915`）。
- 合成：`decision.go:178` 调用 `Combine`（`combine.go:273`）；IC 历史 `ICHistory`（`combine.go:48`）滚动窗口默认 256（`runner.go:310` 硬编码）。
- 模型：`FitModel`（`model.go:213`）按窗口训练 → `PublishModel`（`model.go:289`，内容寻址、同 ID 幂等写入）→ `LoadModel`/`RestoreModel`（`model.go:331/258`）按 AvailableAt 校验。
- 实验：`TrialLedger.Append`（`experiment.go:188`）追加 JSONL 并 fsync；`SelectParameters`（`experiment.go:310`）→ `ResolveParameters`（`:438`）按 PIT 选择。
- 落盘只发生在 `TrialLedger`、`PublishModel` 与 `WritePanel`（任意 writer）三处；Manifest 本身不在本包落盘。

### 9. 统计套利（配对）

`runner/pairs.go` 构造 `PairsSpec` 与带状交易器：`statistics.NewBandTrader` + `NewRollingSpread`，逐 bar 调用 `Observe`（`pairs.go:174`）与 `Step`（`:188`）；闭包捕获为空（满足 `Custom` 节点的 CSE 安全要求，`pairs.go:62`）。`BandTrader` 每个实例只能服务一个价差；Stop 之后阻塞再入场直到 |z| ≤ Exit。

### 10. 结果统计（`Result.Attribution` / `Result.Pairs`）

`Result` 与运行产物（`run.json` 的 `Results[]`、`run-completed` 事件）按 Go 字段名序列化（无 json 标签，键名即字段名，首字母大写）；以下新增字段均 `omitempty`，旧消费方按结构体解码时忽略未知字段即可。

**多空腿归因 `Result.Attribution`**（仅 Weights 模式，账本存在时；factor 与 statarb 通用）。`Run` 对 `Book` 调用 `EnableAttribution`，并在每个决策时刻（NAV 事件处）调用 `SampleExposure`。累加器位于账本所有改变持仓数量/标记价/现金的入口：`Mark`（按旧数量重估到新标记价）、`Execute` 与 `ExecuteAllocation`（成交前先把旧数量重估到成交报价，再按持仓变化拆分多/空腿换手）、`ExitAtLastMark`（含惩罚）、`resolveRecovered`（惩罚退回与缺口损失）、`ApplyTerminalSettlement`（按结算价与最后标记价之差）、`ApplyFunding`/`accrueFunding`（只改账户级 `Funding`）。补丁（Patch）模式与 policy 路径共用 `Execute`/`ExecuteAllocation`，无其他入口。字段（货币额，成本为正数）：

- `InitialNAV`、`FinalNAV`；`LongPnL`/`ShortPnL`（价格变动盈亏，按价格变动时持仓方向归入多/空腿，未扣成本）及 `LongPnLPct`/`ShortPnLPct`（占初始 NAV）。
- `LongTurnover`/`ShortTurnover`：成交名义额按腿拆分（多头腿数量 `max(q,0)`、空头腿 `min(q,0)` 的变动），符号翻转两腿都计，二者之和等于 `Book.Turnover`。
- `AvgLongExposure`/`AvgShortExposure`：决策时刻多/空头总市值 / NAV 的均值，`ExposureSamples` 为样本数。
- `Fees`、`Slippage`、`Funding`、`PITCost`（= `PITGapLoss + PITDelistLoss`）。
- 对账：`FinalNAV − InitialNAV = LongPnL + ShortPnL − Fees − Slippage − Funding − PITCost + Residual`，`Residual` 仅为浮点舍入（测试断言 `|Residual| < 1e-9 × InitialNAV`）。
- `TopSymbols`（至多 20 个，按 `|Net|` 降序）：`Symbol`、`PnL`、`Costs`（该标的的费用+滑点+PIT 成本，不含资金费：资金费是账户级一项，只在 `Funding`）、`Net = PnL − Costs`、`NetPct`（占初始 NAV）、`Share = Net / Σ|Net|`（范围 [-1,1]，接近 ±1 表示单一标的决定结果）；所有标的的 `Net` 之和等于 NAV 变化加 `Funding`（`Share` 与集中度只看品种 `Net`）。
- 累加器是账本的纯旁路观察者，开启前后 NAV 序列逐位一致（`attribution_test.go`）。

**配对独立结果 `Result.Pairs`**（仅 `RegisterPairs` 策略）：`PairResult{Y, X, Episodes, EpisodeCount, PnL, Trades, Wins, WinRate, InMarketBars, Bars, PausedBars, AvgHoldBars, LastBeta, Turnover, Unavailable, UnavailableLeg}`（后两项仅整次运行被跳过的配对有值，见下“不可用的腿”）。

- `Episodes[]` 为 `PairEpisode{StartMS, EndMS, Dir(+1 做多价差/-1 做空), EntryZ, ExitZ, Beta(入场冻结), Bars, Reason, PnL}`；总量上限 400 条在各对间均分（每对至少 10 条，保留最早的），真实数量见 `EpisodeCount`；`run-completed` 事件行整体序列化 `Result`，体量受该上限约束。`Reason`：`exit`（|z| ≤ Exit）、`stop`（|z| ≥ Stop）、`maxhold`、`leg_missing`（腿缺数据被平仓并暂停，`ExitZ=0`）、`end`（运行结束时仍持仓，`EndMS` 为最后一次决策时间）。`Bars` 为持仓决策数（含入场当期，不含平仓当期）。
- 记账：`pairsTracker` 随 builder 一起由 portfolio factory 创建（`Config.observe` 经 `runObservers` 在运行结束时汇总；`PreparePortfolioConfig` 在其为空时创建，`Run` 只在为空时创建，所以入口先 Prepare 再 Run 的路径同样有效；`Run` 开始时调用 `reset` 清空记账，同一已 Prepare 的 Config 再次运行也不会累计，但 builder 自身的交易状态按既有约定仍应只用于一次运行），只写不读，不改变 builder 输出的权重。每个决策，该对上一次决策的目标权重（`wY`、`wX`，即叠加进净权重之前的两项）持有到本次决策，收益为 `wY·rY + wX·rX`（`rY/rX` 为两腿在相邻决策之间的简单收益，取自 builder 输入的对数价字段）乘以上一次决策的 `Budget.NAV`；`PnL` 为累计值占初始 NAV 的比例，持仓期的收益同时累加到当前 `Episode.PnL`。同一品种出现在多对时按各对权重自然拆分。
- `Bars` = 双腿有效且回归窗口已满的决策数；`PausedBars` = 因某腿缺数据暂停的决策数；`InMarketBars` = 持仓决策数；`AvgHoldBars = InMarketBars / Trades`（运行结束时仍持仓的一次计入 `Trades`，`Wins` 以 `PnL>0` 计）；`LastBeta` 为最近一次入场冻结的 Beta；`Turnover` 为决策时刻 `|Δ wY| + |Δ wX|` 的累计（NAV 比例，是成本的代理，成本不分摊到各对）。
- 对账（`Result` 上）：`PairsGrossPnL` = 各对 `PnL` 之和，`PairsNAVReturn` = `Book.NAV / InitialNAV − 1`，`PairsResidual = PairsNAVReturn − PairsGrossPnL`，即成本（费用、滑点、资金费、PIT 成本）加执行近似残差（账本按延迟后的价格成交、权重在决策之间随价格漂移、各对共享品种时账本按净权重成交）。成交价与决策价一致且无延迟差时残差等于成本的相反数。

**Panel 行开关**：`research.panel: none`（`Config.Panel`，默认 `all`）使 `JSONOutput` 不再写每个决策的“品种数×列数” panel 行（`Decision` 与 `DecisionAllocation`）；`{"Kind"...}` 事件行、`Result.Summary`（由 runner 的 `Accumulator` 在内存中累计，不读取已写出的行）与所有账本数值不受影响。

## 配置项与外部依赖

### 配置来源

- YAML 由 `entry/factor_config.go` 解析为结构体，本包的 `PortfolioPolicyConfig` 只有 JSON 标签（`policy_config.go`）。YAML 键到字段的映射在调用方完成。
- 组合策略主要键（`policy_config.go`）：
  - `rebalance`：`every_bars`、`anchor`、`phase`、`duration`、`calendar`（daily/weekly/monthly）、`calendar_version`、`timezone`。
  - `selection`：`long_k`、`short_k`、`long/short_quantile`、`retain_rank`、`dropout`、`group_quota`、`missing_scores`（skip/shrink/cash）。
  - `holding`：`min/max_bars`、`min/max_duration`、`by_asset`、`adopt`（adopt/reject）、`cooldown_bars`。
  - `transition`：`mode`（direct/target-step/linear-exit/geometric/cohort）、`basis`、`exit_steps`、`ratio`、`final_threshold`、`alpha`、`period_bars`、`startup`、`sizing`、`on_reselect`、`entry_window_bars`、`by_asset`。
  - `allocation`：`method`、`reserve_ratio`、`fixed_notional`、`vol_target`、`asset_cap`、`group_caps`、`turnover_limit`、`net_cap`、`beta_cap`。
  - cohort 要求 `every_bars > 0`，calendar/duration 调度与 cohort 互斥（`policy_config.go:303`）。
- `run_policy[].factor.research` 键：`labels`、`label_wait_ms`、`panel`（字符串 `all`/`none`，其他类型或取值报错，见第 10 节）。
- 运行配置：`runner/runner.go:36` 的 `Config`（含 `Chunks`、`HistoricalInput`、`ComputationGroup`、`Execution`、`Manifest`、`DecisionInterval`、`LatencyMS`、`ExpiryMS`、`MaxRecords`、`Snapshot.VisibilityPolicy` 等）。`Execution` 中含 `StorePath`、`SenderLeaseDir`、`HistoryPath`、`MarginRate` 等。
- 研究默认值：动量因子窗口 24、1h、winsor 0.01（`research/combine.go:379`）；优化迭代默认 500、上限 10000（`risk.go:141-146`）；滚动窗口上限 100000（`model.go:84`）；Kalman 拟合 4000 次评估（`kalman_fit.go`）。
- 版本常量：`"builtin-1/banta-0.4.1"`（`dag.go:55`）与 `"indicators-1/banta-0.4.1"`（`indicators.go:13`）须与 banta 版本同步；`MaxIndicatorPeriod = 10000`（`indicators.go:9`）。

### 外部依赖

- 上游：`banta`、`banta/tav`（`go.mod` 用 replace 指向 `../banta`）、`banbot/orm`（`DataSeries`，`version_store.go:12`）、`execution`（`SharedAccount`、`HubClient`、`PaperAdapter`、`Quote`、`PolicyReceipt`）、`gonum`（统计子包）、`shopspring/decimal`（`backtest/terminal.go`）。
- 文件系统：`VersionStore.Export/Open`（gob，`version_store.go:240`）、`TrialLedger`（JSONL）、`PublishModel`（JSON）、`Execution.StorePath/HistoryPath`、运行产物 `.run-*.json`（`artifact.go`）。runner 对归档 chunk 做 sha256（`computation.go:58`、`runner.go:232`）。
- 数据库：factor 包本身不直接访问 QuestDB/TimescaleDB。分页输入经 `VersionPageFactory` 或 `QueryPage` 间接读取（`storage_input.go`），由调用方提供实现。
- 无环境变量，无外部服务。

## 与其他模块的交互边界

| 调用方 / 被调方 | 方向与内容 |
|---|---|
| `entry/`（21 个文件引用） | 唯一的装配入口：`entry/factor_config.go`（YAML → 配置）、`entry/factor_live.go`（`PreparePortfolioConfig` → `NewLive`，约 `:481`/`:741`）、`entry/factor_hub.go`（`:174`/`:195`）、`entry/factor_expression_commands.go`（离线编译检查）、`entry/audit.go`、`entry/factor.go`（使用 `VersionStore`） |
| `runtime/`（6 处） | 运行时装配，引用 factor 包的运行入口 |
| `execution`（经 `runner` 的 Sink） | 账户、纸面、hub 的下单与成交接纳；runner 只通过 `AccountApplication` 接口调用，不直接写交易所逻辑（符合 AGENTS.md 规约） |
| `banta` / `banta/tav` | 技术指标与向量化计算后端（`batch.go`、`indicators_runtime.go`） |
| `orm` | `DataSeries` 与版本记录（`version_store.go`、`storage_input.go`） |
| `web/dev`（2 处） | 开发工具引用，未深入核实 |

依赖方向（应保持）：`expr → factor`、`factor/research → factor`、`factor/statistics → factor`（但 `factor` 根包不得 import 子包，否则成环）、`factor/backtest → factor, execution`、`factor/runner → 以上全部`。`research` 与 `statistics` 不依赖 `runner`；`backtest` 与 `expr` 互不依赖。

## 扩展与修改指南

### 新增原子算子或函数（表达式可用）

1. `factor/dag.go` 或 `factor/operators.go` 增加节点 builder，并实现 `validateNode` 分支。
2. `factor/operators.go`（截面）或 `factor/indicators.go` + `indicators_runtime.go`（指标）补齐 **Session 增量** 与 **Batch 整段** 两条实现，两条路径必须语义一致。
3. `factor/expr/compile.go`：在 `arities` 表（约 `:334`）加名字，并在 `call` 显式分支（`:386-436`）或 `indicatorCall` 中处理；指标类还需同步三处：指标名单（`:470-482`）、参数规则（`:501-516`）、switch（`:523-578`）。
4. 若修改了语义，升级 `dag.go:55` 或 `indicators.go:13` 的版本常量，否则 CSE 与历史计划哈希会误合并。

易踩坑：只加 `arities` 而漏写分支，会落入 `compile.go:448` 的 default 被当作 `ts.std` 静默编译。窗口上限有两套常量（`expr/compile.go:17` 的 `maxWindow` 与 `factor.MaxIndicatorPeriod`），需保持一致。

### 新增 Custom 节点

`Custom` 节点的身份只有 version 字符串（`dag.go:203`、`:372-378`）。同一 version 不同闭包会被 CSE 合并。闭包捕获外部可变状态时必须升级 version。

### 新增组合策略 / 选股 / 过渡 / 约束

- 配置字段加在 `policy_config.go`（JSON 标签 + `NormalizePortfolioPolicyConfig` 校验）。
- 逻辑加在 `policy.go` 的 `Propose` 分支，或 `policy_selection.go`/`policy_constraints.go`/`policy_cohort.go`。
- 注意 checkpoint 兼容：状态依赖 `ConfigHash`、`SIDMappingVersion`、`AssetIdentities`（`policy.go:465/475`），改动可能使旧 checkpoint 失效，需同步升级相关版本字段。
- 新配置键需在 `entry/factor_config.go` 做 YAML 映射，并更新 `doc/factor_portfolio_guide.md`。
- 注册的新 policy 名称需加入 `runner/policy_registry.go`。

### 新增回放/实盘数据源或可见性策略

- 归档输入改 `runner/input.go`；分页存储改 `runner/storage_input.go`（strict 与 static-approximation 两种可见性）。
- 若涉及数据库读取可见性或重试，须遵守 AGENTS.md 的 QuestDB WAL 规则：写后读要等待预期行可见，首次空读不能清除恢复标记，并补充针对超时/轮询路径的回归测试。

### 新增研究模型或标签

- 模型：实现 `Trainer`/`Predictor`（`research/model.go:95/99`），在 `init()` 中 `RegisterModel`（参考 `forest.go`、`model.go:180`）。注意 `RestoreModel` 当前不校验系数个数。
- 标签：扩展 `LabelKind` 与 `ReturnLabel`（`research/labels.go`），注意“成熟且可见”原则。

### 新增统计套利配对 builder

在 `runner/pairs.go` 增加 builder 并在 `definition.go` 注册；需保证闭包无状态或 version 正确，BandTrader 实例不可跨价差复用。`simple.go` 的 `RegisterFactor` 不检查 `Params.Err()`，新增时请显式检查。

### 修改回测账本或接纳语义

`backtest/book.go` 的 `Execute` 与 `policy.go` 的 `ExecuteAllocation` 是两条路径，修改时需同时检查 `PolicyEvidence` 的 FillEvents 归因（`backtest/policy.go:136-141`、`factor/policy_cohort.go:54-66`）。

### 测试与验证

- 本包测试文件数量大（仅 afml 相关测试约 43 个），修改后优先运行 `factor/`、`factor/runner/`、`factor/research/` 的单元测试。
- 涉及 QuestDB 可见性、表替换的修改，必须包含专项回归测试（AGENTS.md）。

## 已知限制与设计取舍

- live 路径的 `combine` 没有 IC 历史，回退为等权或固定权重。
- `Result.Pairs` 的逐对盈亏是按决策时刻价格与各对自身目标权重的近似归因，不等于账本成交盈亏；成本不分摊到各对；仅回放（`Run`）路径汇总，实盘不输出；运行出错提前返回时 `Pairs`/`Attribution` 可能缺失。
- `Result.Attribution.TopSymbols` 只统计账本（Weights 模式）持仓的标的，其他模式（Events/Trade 由账户 sink 记账）不输出 `Attribution`。
- `storage_input` 对输入做 prune，属设计行为。
- `Session` 路径在 `Warmup` 之后不再记录上一个 DecisionTime；`research.RestoreModel` 不校验系数个数；`Manifest` 的 strategy hash 排除部分字段。
