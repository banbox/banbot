# 01 基础层模块：core / config / utils / btime / com / internal / cmd / main / scripts / docker / audit

本文描述基础层的职责、文件地图、核心类型、配置加载链路、与其他模块的边界，以及修改时的注意事项。
内容描述当前源码的实现，改动具体行为前请以源码为准。
`utils/banio.go` 的协议细节见 [doc/banio.md](../banio.md)，本文只说明它在基础层中的位置。

## 定位与职责

基础层是所有业务包的地基，包括：

- **core**：常量、错误码、进程级版本号、`core.State`（单个 Runtime 的可变运行状态）、符号解析（`SymbolParser`）、少量兼容用的进程全局变量。
- **config**：配置体系。把 YAML 分层合并为 `UnifiedConfig`，产出只读的 `RunSpec`，再投影为 `Config`（旧 TS 视图）或 `Snapshot`（运行时视图）。也负责 run_policy 解析、账户/密钥、数据目录解析、语言包、原子写回。
- **btime**：时间语义。`ClockState` 是每个 Runtime 独立的回测/实盘时钟；另保留旧的包级时间函数与 `CurTimeMS` 全局。
- **com**：实时价格缓存（`PriceState`）、爬虫进度（`PairCopiedState`）、cron 调度器工厂（`Scheduler`）、旧版价格门面。
- **utils**：通用工具库。包括 BanIO TCP 通信（`banio.go`）、邮件、进度条、文件/CSV/gob、数学与指标（Sortino/Sharpe/Ulcer）、YAML 文本合并、时间周期换算、加解密等。
- **audit**：只读审计。数据刷新的差异分层（`StageRefresh`）、平盘告警（`DetectFlatMarket`）、历史资金费审计（`AuditFunding`）。只产出报告，不写库、不发单。
- **internal/testutil**：仅测试使用的集成测试门槛（`BANBOT_TEST_INTEGRATION=1`）。
- **cmd/ 与 main.go**：`main.go` 只调用 `entry.RunCmd()`。`cmd/orderinspect` 是一次性排查工具。
- **scripts/、docker/**：验收/性能脚本、镜像构建与 compose 样例（非 Go 运行时代码）。

## 目录/文件地图

| 路径 | 作用 |
| --- | --- |
| `main.go` | 进程入口，调用 `entry.RunCmd()` |
| `core/core.go` | `SetRunMode/SetRunEnv`（写全局）、`Sleep`（基于全局 `Ctx`） |
| `core/data.go` | 全局变量与常量：运行模式/环境、`Version="v0.7.1"`、`UIVersion="v0.6.0"`、标签常量（EnterTag/ExitTag）、订单类型、`Ctx/StopAll/BotRunning` |
| `core/runtime_state.go` | `State` 及其类型安全访问器（pairs 准入快照、ban、no_enter、timeframe、JobPerf/PerfSta、OdBook、订单撮合开关、tf hits、exit 回调、Stop/Close）；另有大量 `Legacy*` 进程级门面 |
| `core/runtime_logger.go` | `(*State).Log()`：绑定的 logger，零值 State 返回 nop logger |
| `core/symbol_parser.go` | `SymbolParser`：带热/温缓存的 `Split(pair)`，策略可注入 |
| `core/common.go` | `Cache`（ristretto）与 `Setup()`；perf 导出；`IsFiat`；`Param` 超参数采样（正态映射）；`MarshalYaml`；legacy OdBook 索引；`AddExitCall/RunExitCalls` |
| `core/utils.go` | `SplitSymbol`（按 `/ : -` 切分）、`GroupByPairQuotes` |
| `core/calc.go` | `Ema`、`NumSet`（时间桶聚合） |
| `core/types.go` | `PerfSta/JobPerf`、`Param`、`TimeValueRange`、`ApiClient` 等数据结构 |
| `core/errors.go` | 错误码（-100 ~ -202）与 `errs.UpdateErrNames` 注册 |
| `config/types.go` | 旧 TS 视图的数据结构（`Config`、`RunPolicyConfig`、`AccountConfig`、`DatabaseConfig`、`APIServerConfig`…）；以及大量包级全局（`Accounts`、`Pairs`、`StakeAmount`…） |
| `config/unified.go` | `UnifiedConfig`、`PolicyV2`；`parseUnifiedLayers`、`decodeUnified`、`Validate`、`TimeSeriesConfig`、`MarshalYAML`；引擎常量 `time_series` / `factor` |
| `config/run_spec.go` | `RunSpec`（只读、不可变）、`FieldOrigin`（字段来源）、`LoadRunSpec`、`EffectiveYAML`、`Hash`、`ResolvePath`、CLI 覆盖记录 |
| `config/shallow.go` | v2 扁平化：`normalizeConfigLayer`（处理 `engine`、`factor` 块展开、`execution.accounts` 别名、密码规范化） |
| `config/advanced.go` | `validateAdvanced`：data/execution/accounts/run_policy.factor 的允许键白名单与类型校验 |
| `config/migration.go` | 配置文件读取校验（`loadUnifiedSources`）与原子写回（`WriteConfigAtomic`，带路径锁、过期字节校验、临时文件 + rename） |
| `config/migration_unix.go` / `migration_windows.go` | 文件锁（unix 为 `flock`，windows 为 `LockFileEx` 独占锁，锁文件为 `<path>.migration.lock`）、权限保留、目录 fsync 的平台实现 |
| `config/runtime_config.go` | `LoadRuntimeSnapshot`（旧式直接构造 Snapshot）、`(*Config).NormalizeRuntime`（运行默认值与校验）、`NormalizeRunPolicies` |
| `config/snapshot.go` | `Snapshot`：运行时不可变配置视图（DataDir/StrategyDir/location），`cloneSnapshotConfig` 深拷贝 |
| `config/biz.go` | 旧全局安装器：`LoadConfig`、`GetConfig`、`ApplyConfig`（写 ~60 个包级全局 + `core.*` + `banexg.SetProxy`）、`SetRunPolicy`、`StaticPairs`、账户初始化、`Clone/Desensitize`、perf 加载、语言包、`ParsePairs` |
| `config/historical_coverage.go` | `HistoricalCoverageConfig`（回测历史覆盖声明的归一化与查询） |
| `config/common.go` | 代理解析 `GetExchangeProxy`、`refine_tf` 解析与缓存（`EnsureStratRefineTF`） |
| `config/exchange_capabilities.go` | 通过 banexg 适配器查询能力：`ExchangeUsesOpaqueSymbols`、`ExchangeDefaultLocation` |
| `config/output_dir.go` | `AllocateOutputDir`：用 `Mkdir` 作为原子占位，冲突时加 `_N` 后缀 |
| `config/cmd_types.go` | `CmdArgs`（CLI 参数集合）、`ArrString` |
| `config/cmd_biz.go` | `CmdArgs.Init`（会写全局 `DataDir`）、`parseTimeZone`、`SetLog`（安装 banexg 全局 logger） |
| `config/*_test.go` | 配置行为回归测试（含 `testdata/v0.5.7.yml` 旧版样例） |
| `btime/state.go` | `ClockState`：每 Runtime 时钟（回测为原子递增的模拟时间，实盘为 NTP 校正时间） |
| `btime/main.go` | 旧包级时间函数（`Time/TimeMS/Now`，读写全局 `CurTimeMS`）、`ParseTimeMS`、`ToDateStr*`、`ToTime`、`init()` 设置 `time.Local=UTC` |
| `btime/common.go` | `RetryWaits`：按 key 的指数退避等待表 |
| `com/price_state.go` | `PriceState`：bid/ask/bar 价格表、过期判断、基于 `SymbolParser` 的基础币映射 |
| `com/price.go` | 旧版价格门面（`GetPrice/SetPrice/...`）：按 exchange 懒绑定 `PriceState`，`init()` 初始化 |
| `com/price_live.go` | 实盘价格刷新（`EnsureLatestPrice`、`RefreshLatestPrice`、`GetBookTickers`，3 秒节流） |
| `com/market_state.go` | `PairCopiedState`（爬虫进度，原子 max）、`MarketState`（Prices + PairCopied，Runtime 所有） |
| `com/cron.go` | `Scheduler` 接口与 `NewScheduler*`（隔离 cron 实例）；`Cron()` 为旧单例 |
| `com/common.go` | 旧版 `PairCopied` 门面，`SetPairMs` 同时写 `core.LastBarMs` 全局 |
| `utils/banio.go` | BanIO：`BanConn`、`ServerIO`、`ClientIO`，消息编解码、压缩、可选 AES、重连、心跳（详见 banio.md） |
| `utils/misc.go` | `SplitSolid`、泛型集合工具、`ParallelRun`（按序返回最小出错索引）、AES-GCM 加解密、语言检测、CPU profile（fgprof）、cron 对齐 |
| `utils/email.go` | 邮件队列 worker、`MailSender`（SMTP、附件、multipart） |
| `utils/yaml_merge.go` | `MergeYamlStr`：基于文本规则合并多份 YAML（保留键顺序、深合并） |
| `utils/file_util.go` | 目录复制/移动/删除、CSV 读写（可压缩）、K 线转字符串、文件尾部读取、gob 编解码 |
| `utils/biz_utils.go` | `PrgBar`、`StagedPrg`（分阶段进度）、`FillOHLCVLacks` |
| `utils/tf_utils.go` | `BuildOHLCV`（子周期合成粗周期）、`RoundSecsTF`、`SecsToTfNum`；`init()` 注册 `ws` 周期到 banexg |
| `utils/math.go` `num_utils.go` `metrics.go` `ulcer.go` `asset_score.go` `correlation.go` | 收益/风险指标（Sharpe/Sortino/Ulcer/回撤/期望）、KMeans、数值辅助、资产评分、相关性矩阵与图片渲染 |
| `utils/text_utils.go` `net_utils.go` `index.go` `file_util.go` | 文本/哈希/周期辅助、HTTP 响应包装、`utils.Setup()`（设置 `core.SysLang`） |
| `audit/funding.go` | `AuditFunding`：venue 资金费快照与账本的只读对账 |
| `audit/market.go` | `StageRefresh`（候选行与基线行比对，写入 staged 版本记录）、`DetectFlatMarket`（同周期多标的 OHLC 相同且零量的告警） |
| `audit/README.md` | 审计命令的使用说明与边界（只读，不 apply） |
| `cmd/orderinspect/main.go` | 一次性排查工具（读取 gob 订单，打印固定下标的订单） |
| `internal/testutil/integration.go` | `RequireIntegration(t)` 集成测试跳过门槛 |
| `scripts/` | `accept_backtest_baseline.sh`、`normalize_backtest_orders.py`、`architecture_performance.py`（及其测试与 .md）、`run_architecture_race.ps1`、`check_documentation_links.py`、`module_isolation_test.go`（build tag）、`china.py`（tushare 数据下载脚本） |
| `docker/Dockerfile` | 应用镜像：基于 golang:1.26.8，克隆 banstrats，内置 QuestDB 9.3.2，`bot init` |
| `docker/base/base.Dockerfile` `docker/base/Dockerfile` | 分阶段构建：基础镜像（预下载依赖）与增量镜像（`FROM banbot/banbase`） |
| `docker/bot.yml` `docker/db.yml` | compose 样例：应用容器（8000/8001）与 QuestDB 容器（9000/8812/9009） |
| `docker/READMD.md` `docker/base/README.md` | 镜像构建说明（文件名拼写 `READMD` 为原样） |

## 核心类型与接口

### core

- **`core.State`**（`core/runtime_state.go`）：一个 Runtime 的可变状态。字段分两类：
  - 公开字段（兼容直接访问，但并发读写需走访问器）：`Logger`、`RunMode`、`RunEnv`、`EnvReal`、`LiveMode`、`BackTestMode`、`ExgName`、`Market`、`IsContract`、`ContractType`、`Pairs`、`StopAll`、`BotRunning`、`ConcurNum`、`NumTaCache`、`ParallelOnBar`、`SysLang` 等。
  - 私有字段（必须走访问器，锁分别为 `flagsMu`、`admissionMu`(atomic 快照)、`lockOdMatch`、`tfPairHitsLock`）：`tFSecs`、`stgPairTfs`、`banPairsUntil`、`noEnterUntil`、`jobPerfs`、`stratPerfSta`、`odBooks`、`orderMatchTfs`、`ctx/cancel`、`exitCalls`。
  - 访问器族：`PairEnabled/SetPairs/SetAdmissionPair/SetAdmissionSnapshot`（准入快照）、`IsPairBanned/SetPairBanUntil`、`NoEnterUntilFor/SetNoEnterUntil`、`StrategyTimeFrame/SetStrategyTimeFrame/ReplaceTimeFrameState`、`JobPerf/JobPerfValue/SetJobPerf/PerfSta/SetPerfSta/PerformanceSnapshot/WithPerformance`、`GetOdBook/SetOdBook`、`OrderMatchEnabled/SetOrderMatchEnabled/ReplaceOrderMatchTfs`、`AddTfPairHits/DrainTfPairHits`、`OnExit`、`Stop`（幂等，取消 ctx、置 BotRunning=false、调用 StopAll 与 exitCalls）、`Close`（等价于 Stop）、`Context/Done/Sleep`、`BeginSimOrderMatch/EndSimOrderMatch`（回测撮合计数）。
  - 构造：`NewState(parent)` 派生私有 ctx。零值 State 需 `EnsureRuntimeMaps()`。
- **准入快照**：`admission atomic.Pointer[admissionSnapshot]`，写入时整体替换 map，热路径只做一次 atomic load。
- **`core.SymbolParser`**（`core/symbol_parser.go`）：`Split(pair) (base, quote, settle, id)`，缓存分热/温两级；策略类型 `SymbolParserStrategy func(pair) [4]string`，cache miss 时调用。
- **全局量**（`core/data.go`）：运行模式/环境、`TFSecs`、`Pairs`、`PairsMap`、`BanPairsUntil`、`NoEnterUntil`、`TfPairHits`、`JobPerfs`、`StratPerfSta`、`OdBooks`、`OrderMatchTfs`、`ExitCalls`、`Ctx`、`StopAll`、`BotRunning`、`ConcurNum`、`Version`、`UIVersion`。这些是 legacy 兼容面，新代码应走 `State`。
- **常量**：`RunModeLive/BackTest/Data/Other`；`RunEnvProd/Test/DryRun`；`EnterTag*`、`ExitTag*`；`OrderType*`；`MinStakeAmount=5.5`；`DelayBatchMS=3000`；`PrefMinRate=0.001`；`AmtDust=1e-8`。
- **错误码**（`core/errors.go`）：ErrRunTime(-100)、ErrBadConfig(-110)、ErrDb*(-120..-123)、ErrLiquidation/ErrLowFunds 等交易类(-130..)、ErrNet*(-140..)、ErrIO*(-150..)、ErrAuthFail(-200)、ErrRemoteError(-201)、ErrOutOfResource(-202)。

### config

- **`UnifiedConfig`**（`config/unified.go`）：`ConfigVersion`、`Root *Config`（根键，`RunPolicy` 必须为空）、`RunPolicy []*PolicyV2`（唯一策略列表）、`Data`、`Execution`、`AccountExecution`（按账户名的执行覆盖）。
- **`PolicyV2`**：内嵌 `*RunPolicyConfig`，另有 `Engine`、`ID`、`Account`、`CapitalWeight`、`Factor`、`ExplicitEngine`。
- **`RunSpec`**（`config/run_spec.go`）：`value`、`effective`（生效 YAML 的 map 视图）、`origins`（字段 → `FieldOrigin{Source, Kind}`）、`dataDir`、`strategyDir`、`args`。所有导出方法返回副本。
  - `Config()`、`Origin/Origins`、`Engines()`、`WithExecutionMode(mode, source)`（返回带 CLI 来源的新副本）、`RuntimeSnapshot()`（仅取 time_series 策略，返回 `*Snapshot`）、`EffectiveYAML(redact)`、`Hash()`、`ResolvePath(field)`（按来源文件解析相对路径）。
- **`Config`**（`config/types.go`）：旧 TS 视图的根结构。关键字段：`Env`、`MarketType`、`ContractType`、`StakeCurrency`、`StakeAmount/StakePct/MaxStakeAmt`、`TimeRangeRaw/TimeStart/TimeEnd`（`TimeRange *TimeTuple`）、`TimeFrames/RunTimeframes`、`Pairs`、`RunPolicy []*RunPolicyConfig`、`Accounts map[string]*AccountConfig`、`Exchange *ExchangeConfig`、`Database *DatabaseConfig`、`APIServer`、`Mail`、`Webhook`、`RPCChannels`、`PairMgr`、`PairFilters`、`StratPerf`、`HistoricalCoverage`、`LLMModels`、`BTInLive`、`WatchJobs`、`FatalStop`。
- **`RunPolicyConfig`**（`config/types.go`）：`Name`、`TimeFrames/RunTimeframes`、`Pairs`、`Params`、`PairParams`、`StakeRate`、`MaxPair/MaxOpen/MaxSimulOpen/OrderBarMax`、`Dirt`（long/short/any）、`RefineTF`、`StopLoss`、`More`（`,remain` 收集未知键）、`defs`（超参定义缓存）。方法：`ID()`、`Key()`、`Param/Def/DefInt`（超参取值与定义）、`PairDup`、`Clone()`、`ToYaml()`、`HyperParams()`。
- **`Snapshot`**（`config/snapshot.go`）：`value *Config`（克隆）、`DataDir`、`StrategyDir`、`location`。`View()` 只读；`Clone()`；`DefaultAccount()`；`Location()`；`ParsePath()`。
- **`CmdArgs`**（`config/cmd_types.go`）：CLI 参数集合。关键：`ExplicitFlags`（只有显式指定的 flag 才覆盖 YAML 中的零值）、`Configs`、`ConfigData`、`DataDir`、`NoDefault`、`TimeRange/TimeStart/TimeEnd`、`Pairs/TimeFrames`（Raw 形式需 `Init()` 解析）、`StakeAmount/StakePct`、`LogLevel/Logfile`、`TimeZone`、`Inited`。
- **`AccountConfig`**：`NoTrade`、`StakeRate`、`MaxStakeAmt`、`Leverage`、`MaxPair`、`MaxOpenOrders`、`APIServer *AccPwdRole`、`Exchanges map[string]*ExgApiSecrets`（`,remain` 收集 `<exchange>: {prod, test}`）。`GetApiSecretFor(exchange, env)` 为显式版本。
- **`DatabaseConfig`**：`Url`、`AutoCreate`、`DbType`（questdb/timescale，空则自动探测）、`SIDRegistryURL`、`MaxPoolSize`、`QdbMemPct`、`QdbMaxMemMB`、`Retention`。
- **`Snapshot`/`UnifiedConfig` 的关系**：`UnifiedConfig` 是输入模型（允许 v2 扁平策略与高级覆盖）；`Config` 是旧模型（不允许高级覆盖，`TimeSeriesConfig()` 会显式拒绝）；`Snapshot` 是运行时的克隆视图。

### btime

- **`ClockState`**（`btime/state.go`）：字段 `backtest`、`curTimeMS`（原子）、`locShow`。方法：`SetTimeMS`、`TimeMS`、`AdvanceMS`、`Time`、`Now`。非回测模式下 `TimeMS()` 返回 `UTCStamp()`（经 bntp 校正）。
- **`RetryWaits`**（`btime/common.go`）：`SetFail(key)` 返回下次可重试时间戳；`NextRetry`、`Reset`。等待表默认 `3s, 6s, 12s, 30s`。

### com

- **`PriceState`**（`com/price_state.go`）：`bidPrices/askPrices`（按 side，加 `lockPrices`）、`barPrices`（加 `lockBars`）、`parser *core.SymbolParser`。方法：`GetPriceSafeExpAt`（无效返回 -1，法币直接返回 1）、`GetLastBarPriceAt`、`SetBarPriceAt/SetPriceAt/SetPricesAt`、`IsPriceEmpty`、`Reset`、`RefreshLatestPriceAt`（实盘刷新）。
- **`MarketState`**（`com/market_state.go`）：`Prices *PriceState`、`PairCopied *PairCopiedState`。`NewMarketStateWithExchange(exg, exchange)` 绑定适配器的符号解析。
- **`PairCopiedState`**：`values map[string][2]int64`（pair → [barMS, waitMS]），`lastBarMs`/`lastCopiedMs` 为 atomic，只增不减（`atomicMax`），删除时重算。
- **`Scheduler`**（`com/cron.go`）：`AddFunc/Start/Stop`，内部是 `banbox/cron/v3` 并使用 NTP 时钟。

### audit

- **`FundingSnapshot`**（venue 侧）、**`FundingLedgerSnapshot`**（账本侧）、**`FundingReport`**（`Matched/Missing/Conflicts/LedgerOnly`）。`AuditFunding(venue, ledger)` 为纯函数式比较，拒绝跨账户、重复 ID、窗口外行、未知 instrument。
- **`RefreshReport` / `RowChange`**：`StageRefresh` 的输出，基线行与候选行按 `(source, timeframe, sid, time)` 对齐。`FlatPolicy{MinSymbols, MinFraction}` 必须显式给出（`MinSymbols>=2`，`0<MinFraction<=1`）。

### utils（精选）

- **BanIO**：`IBanConn` 接口；`BanConn`（单连接的读写、订阅标签、等待结果、ping、Stop/Join）；`ServerIO`（`RunForever`、`Broadcast`、`SetVal/GetVal`、`WrapConn`）；`ClientIO`（`NewClientIOWithState` 绑定 State 生命周期，`NewClientIO*` 为兼容入口，会写全局 `banClient`）。
- **邮件**：`EmailTask`、`MailSender`（`SendMail`、`buildMessage`）；`StartEmailWorker` 启动一次性队列 worker；`SendEmail` 在未设置发件人时返回错误。
- **进度**：`PrgBar`（终端进度）、`StagedPrg`（多阶段加权进度 + 触发回调）。
- **并发**：`ParallelRun[T](items, concurNum, handle)`，返回出错下标最小的错误，出错后不再启动新任务。
- **YAML**：`MergeYamlStr(paths, skips...)`（文本规则合并）。
- **指标**：`UlcerAccumulator`、`CalcMaxDrawDown`、`CalcExpectancy`、`SharpeRatio*/SortinoRatio*`（float 与 decimal 两套）、`KMeansVals`。
- **周期**：`BuildOHLCV`（子周期合成，支持额外列求和）、`RoundSecsTF`、`SecsToTfNum`。

## 关键流程

### 1. 配置分层与 RunSpec 构造（只读路径，推荐）

`config.LoadRunSpec(args, showLog)`（`config/run_spec.go`）按以下顺序执行：

1. 规范化 `args`：解析 `RawPairs/RawTimeFrames`（`utils.SplitSolid`）。
2. `ResolveDataDir(args.DataDir)`：显式值优先，其次环境变量 `BanDataDir`。`BanStratDir` 只从环境读取。
3. 组装源文件列表：未设 `NoDefault` 时依次加入 `$DATA/config.yml`、`$DATA/config.local.yml`（存在才加）；再追加 `args.Configs`，`$` 或 `@` 前缀相对 DataDir 解析；`ConfigData`（stdin/内联）作为 inline 层。
4. `loadUnifiedSources`（`config/migration.go`）读取文件，拒绝 `.json`；`parseUnifiedLayers`（`config/unified.go`）逐层处理：
   - `configDocument`：要求单个 YAML 文档、根为 mapping、不能有重复键。
   - `extractLLMSection`：单独抽出 `llm_models`（经 `llm.SubstituteEnvValue`）。
   - `os.ExpandEnv` 作用在整段原始文本上后再 Unmarshal（见文末“已知限制与设计取舍”）。
   - `normalizeConfigLayer`（`config/shallow.go`）：去掉 `config_version`，展开 `factor` 块为扁平键，处理 `execution.accounts` 别名，规范密码为文本。
   - `validateV2Fields`：data/execution/accounts/run_policy 的结构与允许键校验。
   - `recordLayerOrigins` 记录每个字段的来源（用于 `EffectiveYAML` 与路径解析）；`mergeConfigLayer` 合并（`noExtends` 键整块替换，`timerange` 与 `time_start/time_end` 互斥）。
5. `decodeUnified`：根键经 mapstructure 解码到 `Config`；`accountExecutionKeys` 从账户中拆出；`run_policy` 逐项解码为 `PolicyV2`。
6. `applyLLMConfig`、`llm.ResolveModels`、`UnifiedConfig.Validate()`（单独校验：Root.RunPolicy 必须为空、账户存在、engine 合法、ID 唯一、capital_weight 合计不超过 1 等）。
7. `applyUnifiedArguments`：仅当存在 time_series 策略或显式时间范围时才调用 `Root.Apply(args)`（解析时间范围）。
8. `applyCLIOrigins`：把显式 CLI 覆盖写入 effective 与 origins（`Kind="cli"`）。

产出的 `RunSpec` 再由调用方转换：

- `spec.Config().TimeSeriesConfig()`：拒绝 factor 引擎、拒绝高级覆盖、拒绝 capital_weight/account/id，得到旧 `Config`。
- `spec.RuntimeSnapshot()`：只带 time_series 策略，经 `Config.NormalizeRuntime()` 补默认值并解析交易对，返回 `*Snapshot`。
- `spec.WithExecutionMode(mode, source)`：返回带 CLI 来源的副本（因子回测的 mode 选择）。

### 2. 旧全局安装路径（兼容）

`config.LoadConfig(args)`（`config/biz.go:92`）→ `GetConfig` → `LoadRunSpec` + `TimeSeriesConfig` → `ApplyConfig(args, cfg)`（`config/biz.go:~337`）。`ApplyConfig` 会：

- 设置 `Loaded=true` 并把配置写入约 60 个包级变量（`Accounts`、`Pairs`、`StakeAmount`、`Database`、`APIServer`…）。
- 调用 `core.SetRunEnv`、写 `core.ExgName/Market/ContractType/ConcurNum`。
- 调用 `initExgAccs`，写 `btime.LocShow`（时区）。
- 调用 `GetExchangeProxy` 并 `banexg.SetProxy`（进程级 HTTP 代理，所有后续 `NewHttpClient` 继承）。
- 调用 `SetRunPolicy` 写 `RunPolicy` 全局。

唯一的业务调用方是 `biz/biz.go:60`。`LoadConfig` 在 `Loaded==true` 时直接返回，不会用新参数重新安装。

### 3. Runtime 运行配置归一化

`(*Config).NormalizeRuntime()`（`config/runtime_config.go`）为 Runtime 路径补默认值（LimitVolSecs=10、PutLimitSecs=180、AccountPullSecs=60、ConcurNum=2、CloseOnStuck=20、OrderBarMax=500、FatalStopHours=8、MarginAddRate=0.66、OpenVolRate=1、MinOpenRate=0.5、BTNetCost=15、OdBookTtl=500、NTPLangCode=none、ShowLangCode=zh-CN、SpiderAddr=127.0.0.1:6789、Env=dry_run），校验 `exchange`、`stake_currency`、`low_cost_action`，并通过 `parsePairs` 把短名转换为标准 pair（如 `BTC` → `BTC/USDT` 或 `BTC/USDT:USDT`），再 `NormalizeRunPolicies` 处理每个策略的 pairs、params、pair_params。

### 4. 原子写回

所有对配置文件的编辑应使用 `config.WriteConfigAtomic(path, expected, candidate)`（`config/migration.go:164`）：

- 同一路径加进程内锁 + 平台文件锁（`migration_unix.go` flock）。
- `expected` 必须是编辑器读取时的字节，不匹配则拒绝（防止覆盖并发修改）。
- 写入同目录临时文件 → 校验内容 → 再次校验源文件未变 → `os.Rename` → 目录 fsync；权限沿用原文件。

调用方：`web/dev/api_dev.go`（编辑配置文件、新建配置）、`entry/unified_backtest.go`、`entry/factor_resolved.go`。

### 5. 时间语义

- 每个 Runtime 应持有一个 `btime.ClockState`（回测推进模拟时间，实盘读取 NTP 时间）。
- 旧路径调用 `btime.TimeMS()/Time()/Now()` 读写包级 `CurTimeMS`（回测时为全局模拟时间）。当前仍有约 52 处 `btime.TimeMS()` 调用。
- `btime.init()` 把 `time.Local` 设为 UTC（进程级副作用）。展示时区由 `btime.LocShow` 决定（来自 `--timezone` 或交易所默认位置）。

### 6. 价格与爬虫进度

- 新代码：Runtime 持有 `com.MarketState`，直接调用 `PriceState` 方法（带时间戳参数，便于测试）。
- 旧代码：`com.SetPrice/GetPriceSafe/...`（`com/price.go`）通过 `syncLegacyPriceParser()` 按当前 `core.ExgName` 懒绑定一个 `PriceState`，切换交易所时保留各自的价格表。
- 实盘价格：`com.EnsureLatestPrice`/`RefreshLatestPrice` 调 `exg.Default.FetchTickers`，3 秒节流，结果缓存 1.5 秒（`core.Cache`）。

### 7. 停止与清理

- `(*State).Stop()`：幂等。取消私有 ctx → 标记 `stopped` 并取出 `exitCalls` → 调用 `StopAll` → 置 `BotRunning=false` → 依次执行 exitCalls。`OnExit` 在 Stop 之后注册会立即执行。
- `(*State).Close()`：等价于 `Stop()`，不清空 map。
- 进程级：`core.AddExitCall` / `core.RunExitCalls`（旧路径，信号处理与 defer 共用）。`utils.StartCpuProfile` 通过它注册清理。

### 8. 审计（只读）

`entry/audit.go` 注册 `banbot tool data-audit` 子命令，调用：

- `audit.StageRefresh(baseline, candidate)`：以 `factor.VersionStore` 归档形式输出 staged 结果，只写候选记录，不删除基线独有行，不覆盖已存在的 stage 文件。
- `audit.DetectFlatMarket(rows, policy)`：统计同一 `(source, timeframe, time)` 下 OHLC 全相同且成交量为 0 的标的比例。
- `audit.AuditFunding(venue, ledger)`：比较交易所资金费与账本结算事件，输出差异报告。

## 配置项与外部依赖

### 数据目录与环境变量

| 名称 | 作用 |
| --- | --- |
| `BanDataDir` / `-datadir` | 数据目录（配置、日志、K 线、输出的根） |
| `BanStratDir` | 策略目录（`strat_dir`） |
| 环境变量展开 | `config` 文本内的 `${VAR}` / `$VAR` 会被展开（见文末“已知限制与设计取舍”） |

### 顶层配置键（节选，`config/types.go`）

- 交易环境：`env`（prod/test/dry_run，默认 dry_run）、`exchange.name`、`market_type`（spot/linear/inverse/margin…）、`stake_currency`（必填）、`pairs`、`pairlists`、`pairmgr`。
- 资金：`stake_amount`、`stake_pct`、`max_stake_amt`、`wallet_amounts`、`draw_balance_over`、`low_cost_action`（ignore/keepBig/keepAll）、`leverage`、`margin_add_rate`。
- 执行：`order_type`、`limit_vol_secs`、`put_limit_secs`、`stop_enter_bars`、`close_on_stuck`、`order_bar_max`、`max_open_orders`、`max_simul_open`、`fatal_stop`、`fatal_stop_hours`、`relay_sim_unfinish`、`take_over_strat`。
- 回测：`timerange` 或 `time_start/time_end`、`bt_net_cost`、`bt_strict`、`bt_no_kline_download`、`bt_legacy_*`（历史兼容开关）、`historical_coverage`。
- 时间与 K 线：`timeframes`、`run_timeframes`、`kline_source`、`concur_num`、`odbook_ttl`、`ntp_lang_code`、`show_lang_code`。
- 服务：`database`（url、auto_create、db_type、sid_registry_url、max_pool_size、qdb_mem_pct、qdb_max_mem_mb）、`api_server`（bind_ip、port、jwt_secret_key、users、CORS_origins）、`spider_addr`（默认 127.0.0.1:6789）、`mail`、`webhook`、`rpc_channels`、`bt_in_live`、`llm_models`、`strat_perf`。
- 策略：`run_policy[]`（见 `RunPolicyConfig`）。v2 额外支持 `engine`（time_series/factor）、`id`、`account`、`capital_weight`、`factor{...}`（展开为扁平键）。
- 高级覆盖：`data`（namespace、page_rows、prefetch_rows、page_bytes、archive、max_records、pit_policy）、`execution`（mode、store、history、sender_lease_dir、hub_socket、live_provider、funding_policy、order_mode、paper_fill_model、instruments、margin_rate…）、`accounts.<name>.<exec key>`。白名单见 `config/advanced.go` 的 `validateAdvanced`。

### 外部依赖

- banexg：价格符号语义（`exg.Default`、适配器能力 `PriceSymbolCapability`）、`banexg.SetProxy`、订单/K 线类型、`errs`、`log`（全局 logger）。
- bntp：NTP 校正时间（`bntp.Now/UTCStamp`，`bntp.LangCode`）。
- banbox/cron/v3：调度（NTP 时钟）。
- ristretto：`core.Cache`（进程级缓存，需 `core.Setup()`）。
- go-deadlock：多处互斥量（检测死锁，`deadlock.Opts.Disable` 默认关闭，`--deadlock` 打开）。
- zap、gopkg.in/yaml.v3、go-viper/mapstructure/v2、shopspring/decimal、felixge/fgprof、jackc/pgx（`pgconn` 错误判断）。
- 系统：`/proc/1/cgroup` 与 `/.dockerenv` 用于 `IsDocker`；`xdg-open/open/cmd start` 用于 `OpenBrowser`。

### 交易所语义的边界

`config` 只通过 banexg 适配器能力查询（`ExchangeUsesOpaqueSymbols`、`ExchangeDefaultLocation`），不按交易所名写分支。`parsePairs` 对 spot/linear/inverse 之外的市场只支持带 `/` 的完整 pair。

## 与其他模块的交互边界

- **entry**：唯一的命令入口。`entry/runtime_entry.go` 使用 `config.LoadRunSpec`，`entry/common.go` 注册命令，`entry/audit.go` 调用 `audit`。`main.go` 仅转发给 `entry.RunCmd()`。
- **biz**：`biz/biz.go` 调用 `core.Setup()`、`config.LoadLangMessages()`、`utils.Setup()`，并仍使用旧的 `config.LoadConfig`。
- **runtime / strat / orm / data / opt / live / web**：通过 `core.State`、`btime.ClockState`、`com.MarketState`、`config.Snapshot` 获取 Runtime 资源；旧代码仍使用包级 `core.*`、`config.*`、`btime.*`、`com.*`。
- **web**：`web/live/auth.go` 调 `config.GetApiUsers()`（旧路径）；`web/dev` 调 `config.WriteConfigAtomic`、`config.MergeConfigPaths`。
- **banexg / banta / banstrats**：通过 `exg`（`exg.Default`、`exg.NewPriceSymbolParser`）和 banexg 能力接口接入；策略仓库使用 `core.Param`（`PNorm` 等）和 `RunPolicyConfig.Def/DefInt`。
- **execution / factor**：`audit` 依赖 `execution.AccountKey`、`execution.CommittedEvent` 与 `factor.VersionStore`；`config` 的 factor 段由 `config/advanced.go` 校验，编译在 `entry/factor_*`。
- **llm**：`config` 解析 `llm_models` 并调用 `llm.ResolveModels`，`llm.SubstituteEnvValue` 负责环境变量替换。

## 扩展与修改指南

### 新增配置键

1. 在 `config/types.go` 的 `Config`（或 `RunPolicyConfig`）中加字段，带 `yaml` 与 `mapstructure` tag。根键会由 `decodeUnified` 自动解码。
2. 若字段是 map 或 list 且希望整块替换（而不是深合并），加入 `config/types.go` 的 `noExtends`。
3. 若含指针、map、slice，需同步更新深拷贝：`(*Config).Clone()`（`config/biz.go:830`）、`cloneSnapshotConfig`（`config/snapshot.go:55`）、`RunPolicyConfig.Clone()`（`config/biz.go:1133`）。
4. 若是密钥、口令，需更新 `(*Config).Desensitize()`（`config/biz.go:930`）与 `redactConfigFields`（`config/run_spec.go`）。
5. 默认值写在 `NormalizeRuntime`（Runtime 路径）。若旧路径 `ApplyConfig` 也需要，同步修改（注意两处默认值需保持一致）。
6. 若是高级覆盖（data/execution/accounts/factor），还要加入 `config/advanced.go` 的白名单，并在 `config/shallow.go` 的 `accountExecutionKeys` / `factorPolicyKeys` 中登记。
7. 修改 `Config` 结构后需检查 `DumpYaml` / `HashCode` / `EffectiveYAML` 的输出是否仍可回读（`config/shallow_test.go` 有往返测试）。

### 新增 CLI 参数

1. 在 `config/cmd_types.go` 的 `CmdArgs` 加字段。
2. 若需要覆盖 YAML，在 `config/run_spec.go` 的 `applyCLIOrigins` 中加分支（记录为 `Kind="cli"`），并在 `config/biz.go` 的 `applyArguments` 中同步。
3. 若参数需要 `Raw` 字符串拆分，在 `CmdArgs.Init()`（`config/cmd_biz.go`）中处理。
4. 注意 `ExplicitFlags`：只有显式指定才覆盖 YAML 的零值。

### 新增引擎或策略字段

- `UnifiedConfig.Validate`（`config/unified.go:422`）负责语义校验；`TimeSeriesConfig` 必须拒绝未知引擎或 time_series 不支持的字段。
- `PolicyV2` 的 `MarshalYAML`（`config/unified.go:45`）需要同步输出新字段。
- `PolicyV2` 的 `v2PolicyKeys` 与 `decodeUnified` 的 extras 结构需同步。

### 新增 Runtime 状态

- 在 `core.State` 中加私有字段，并配套访问器，锁与其他字段分开、粒度尽量细。
- 只读快照用 `maps.Clone` 返回副本，不要返回内部 map。
- 零值 State 的初始化放进 `EnsureRuntimeMaps()`。
- 不要新增包级全局；如果必须兼容旧代码，只在 `Legacy*` 门面中增加，并说明迁移路径。

### 新增时间或价格函数

- 优先接收 `*btime.ClockState` 或 `nowMS` 参数，不要直接读 `btime.TimeMS()`。
- 价格读写放在 `com.PriceState` 方法上，通过 `MarketState` 注入。旧门面只用于兼容。

### 新增错误码

- 在 `core/errors.go` 中加常量（沿用分组区间），并在 `init()` 的 `errs.UpdateErrNames` 中注册名称。

### 新增交易所相关逻辑

- 禁止在 banbot 中按交易所名写分支（参见 AGENTS.md）。应在 banexg 适配器补充 `PriceSymbolCapability` 或 `Info()` 元数据，然后在 `config/exchange_capabilities.go` 中读取。

### 版本与镜像

- 发版：修改 `core/data.go` 的 `Version`，并使用 `git tag`（当前最新 tag `v0.7.1`）。
- 若 `web/ui` 有变化，需要重新编译前端，并更新 `UIVersion`。当前 `web/ui` 自 `v0.6.0` 起无提交，`UIVersion="v0.6.0"` 与之一致。
- Docker：`docker/base/*.Dockerfile` 的 `go get -u` 与 `git pull` 会拉取最新依赖，修改时注意可复现性。

### 常见易踩坑

- 不要在 Runtime 路径调用 `config.LoadConfig` / `ApplyConfig` / `ParseConfig*`，它们会写全局。
- `config.GetDataDir()` 在未设置时直接 panic；Runtime 路径应使用 `ResolveDataDir` 或 `RunSpec.ResolvePath`。
- `core.Cache` 在 `core.Setup()` 之前为 nil，调用 `GetCacheVal` 或 `GetBookTickers` 会 panic。
- `btime.Time()` 等函数在 `core.BackTestMode` 为 true 且 `CurTimeMS==0` 时会用实际 UTC 时间初始化，之后需要有人推进。
- `utils.ParallelRun` 出错后不再启动新任务，但已启动的任务仍会执行完。
- `com.GetPrice` 使用 10 秒过期（10000 毫秒），`GetPriceSafe` 使用 60 秒过期（`PriceExpireMS`），两者不一致，请按调用场景选择。
- `core.IsFiat` 只检查子串 `USD` / `CNY`，凡包含这两个子串且不含 `/` 的符号都会按价格 1 返回。

## 已知限制与设计取舍

- 配置中的 `os.ExpandEnv` 作用于整段原文（含注释与密码里的字面量 `$`）；改为值级展开会让未加引号的 `port: ${PORT}` 变成字符串，因此保持整段展开。`llm_models` 段由 `extractLLMSection` 另行做值级展开。
- legacy 全局入口（`ApplyConfig` 写大量包级变量、`LoadConfig` 在 `Loaded==true` 后直接返回）为兼容保留；新路径使用 `RunSpec`/`Snapshot`/`Runtime` 隔离。
- `core.IsFiat` 为子串匹配，有意把 USDT/USDC 等稳定币视为价格 1。
- `State.LiveMode/EnvReal` 是供热路径直接读取的导出字段，读取不加锁，setter 仅在启动阶段调用。
- `scripts/china.py` 的 tushare token 从环境变量 `TUSHARE_TOKEN` 读取，端点可用 `TUSHARE_HTTP_URL` 覆盖，密钥不得写入代码库。
- `cmd/orderinspect` 为一次性排查工具，不属于正式功能。
