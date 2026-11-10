# 数据与存储模块（orm/ + data/）

> 描述当前源码的实现。文中行号仅作定位参考，修改前请重新定位。

## 定位与职责

- **orm/**：数据持久化与运行时数据模型。负责 `DataSeries` / `DataRecord` / `SeriesInfo` 等统一数据结构；QuestDB 与 TimescaleDB（PostgreSQL 协议）双后端的建表、读写、删除、覆盖范围（sranges）、K 线与复权因子；标的（ExSymbol）与 SID 注册表；压缩与表替换；历史覆盖计算。
- **data/**：数据接入与流转。负责数据源接口（`DataSource`）与注册目录；历史补齐（回测/实盘启动时补缺口）；回测回放（`HistProvider` + feeder）；实盘接入（`LiveProvider` + `LiveSpider` 采集进程）；逐笔成交归档下载与缓存；订阅计划编译与 Live 安装生命周期。
- 两者的边界：orm 只管“怎么存、怎么查、怎么替换表”；data 管“数据从哪来、按什么顺序喂给策略”。交易所相关逻辑全部经 banexg 接口，不在本模块内写交易所专属分支（符合 AGENTS.md 开发规约）。
- 不属于本模块：`orm/ormo`（订单与钱包的 SQLite）、`orm/ormu`（UI 任务的 SQLite）。它们与 orm 同包目录但职责独立，仅在本文末尾说明。

## 目录/文件地图

### orm/ 核心数据模型与序列

| 文件 | 作用 |
|---|---|
| series.go | `SeriesInfo` / `SeriesBinding` / `SeriesField` / `DataRecord` / `DataSeries`；字段类型推断、OHLCV 视图、通用重采样（`ResampleDataSeriesWithSymbolState`） |
| series_store.go | `SeriesStore` 门面：校验、规范化、写入、读取、缺口计算与补齐 |
| series_repo.go | `SeriesRepo` 接口与 `dbSeriesRepo` 实现：DDL、批量插入、删除、缺口、QuestDB 表重写、可见性等待 |
| series_access.go | 包级便捷函数（`DefaultSeriesStore` 等）与 `BoundSeriesStore` |
| series_range.go | 覆盖区间的 holes 计算与 coverage 更新 |
| series_budget.go | 解码后单页字节预算（反射估算，不是进程内存限制） |
| kline_series_store.go | `KLineSeriesStore`：向已有 K 线行写扩展列（只 UPDATE，不新建行） |
| kline_schema.go | 只读查询 K 线物理表 schema |
| kline_runtime.go | K 线运行时选项（回测/实盘/下载开关、时钟、StrictReplay） |
| kline_physical.go | TimescaleDB 物理 K 线覆盖清单与校验（只检查不修复） |
| kline_insert_lock.go | K 线插入的进程级文件锁与等待完成 |
| ohlcv_series.go | K 线 DataSeries 的查询/写入（PG COPY、QuestDB INSERT）、聚合合并、保留字段与 NULL 语义 |
| kline.go | 旧 `banexg.Kline` 接口的查询与写入，K 线区间与 holes 更新，插入所有权 |
| kdata.go | 历史 K 线读取、复权因子加载与缓存（`adjMap`）、`ApplyAdj` 复权计算 |
| kdata.pb.go | protobuf 生成代码（KlineBlock、AdjFactorBlock 等），用于导出导入 |
| srange.go | sranges 进程内缓存、合并、`UpdateSRanges`（QuestDB 分支） |
| srange_query.go | sranges 分页与汇总查询（管理接口用） |
| pg_srange.go | TimescaleDB 版 sranges 管理（事务内 DELETE+INSERT） |
| types.go / models.go | `AdjInfo`、`InfoKline`、`SeriesOHLCV`、`ExSymbol`、`SRange` 等结构 |

### orm/ 存储基础设施

| 文件 | 作用 |
|---|---|
| db.go | `Queries` 句柄、`DBTX` 抽象、`WithXxx` 绑定（文件头标注 sqlc 生成，实为手写） |
| base.go | 全局连接池与 legacy 入口（`Setup`）、后端识别、迁移入口、分区修复、错误分类、SQLite 池 |
| storage.go / storage_identity.go | `Storage`（显式依赖，`OpenStorage`）；identity 规范化与按 identity 的一次性迁移 |
| metadata_migration.go / metadata_version.go | QuestDB 增量迁移；微秒级元数据版本预留（进程锁 + highwater 文件） |
| sid_registry.go | PG 侧 SID 权威注册表（QuestDB 无序列与唯一约束，故需要它） |
| questdb.go | 本机 QuestDB 安装、启动、端口等待、server.conf |
| questdb_visibility.go | 可见性等待（`waitForQuestCondition`）、快照指纹、表替换恢复标记（swap intent）与恢复 |
| compact.go | 压缩 worker、LATEST BY 去重重写、CTAS 与 RENAME 替换 |
| compact_process_lock.go | 跨进程锁（flock / LockFileEx）加进程内 RWMutex |
| verify.go | sranges 与 K 线的缺口、重复、孤儿检查 |
| dump.go | gob 调试转储（`DumpSink`） |
| pg_compression.go | TimescaleDB 压缩策略同步（无扩展时 no-op） |
| pg_queries.go | TimescaleDB 的 K 线读写、聚合、exsymbol 等 PG 实现 |
| pub_kline_queries.go | QuestDB 的 kline_un_q、sranges_q、adj_factors_q 清理与范围查询；PG 分支委托 pg_* |
| pub_meta_queries.go | QuestDB 的 calendars_q、adj_factors_q、ins_kline_q、exsymbol_q 读写，含插入锁、SID 分配、恢复标记 |
| symbol_state.go / symbol_queries.go | `SymbolState`（内存符号索引、订阅集合）与 `SIDAllocator`（SID 分配与 reservation 围栏） |
| exsymbol.go | 标的加载、Ensure（新增）、上市日探测、ParseShort/ToShort、聚合规则 |
| exsymbol_recovery.go | exsymbol_q 写入的恢复标记与共享 SID ledger 的持久化、对账、回放 |
| historical_coverage.go | 历史覆盖区间计算（纯函数）、上市前缀证明、分段读取 |
| market_snapshot.go | 回测/数据模式下加载并校验历史市场快照（sha256 校验） |
| subscription.go | `orm.Subscription` 规范化、`StreamKey`、warmup 起点 |
| banpub.go | banpub SQLite 连接入口 |
| tools.go | protobuf 导出导入、KlineBlock 批处理、查询构造辅助 |

### data/ 数据接入与流转

| 文件 | 作用 |
|---|---|
| series_source.go | `DataSource` 接口、`SeriesRuntime` / `SeriesPlan`、覆盖范围补齐（`EnsureSeriesRange`）、数据源激活 |
| source_catalog.go | `DataSourceCatalog`：数据源注册、工厂、状态；按 Runtime 生成独立实例 |
| func_source.go | 用函数回调注册数据源（`FuncDataSource`，`RegisterFuncDataSource`） |
| source_helpers.go | 订阅字段规范化、JSON 数值解析 |
| history_page.go | 分页历史读取（`PagedHistorySource`、`ReadSourceHistory`、`ensurePagedSeriesRange`） |
| storage_page.go | 按订阅读取一页序列数据（非 K 线与 K 线两条路径） |
| series_prefetch.go | HistProvider 的分页大小与预取行预算配置 |
| hist_series_feeder.go | 独立时序（非 K 线）的分页回放 feeder（`HistSeriesFeeder`） |
| feeder.go | 单交易对 feeder：`SeriesFeeder` / `DBSeriesFeeder`，多周期聚合、复权、预热、回调触发；`TfSeriesLoader` 分批读库 |
| provider.go | 泛型 `Provider[T]`；`HistProvider`（回测回放堆）与 `LiveProvider`（实时 ingress、wait-bar） |
| ws_feeder.go | `TradeFeeder`：按小时加载逐笔成交并回放 |
| ws_loader.go | `WsDataLoader`：归档 zip 下载、按小时切分、二进制缓存读写 |
| spider.go | `LiveSpider` 实时采集进程：miner、ws 订阅、轮询 K 线、写入队列消费 |
| spider_feed.go | 收盘事实观察者（`ClosedSeriesObserver`）与 pinned feed 注册 |
| subscription.go | 订阅规范化、历史 Provider 一次性安装、Live kline 订阅安装 |
| subscription_plan.go | 订阅计划编译（`SubscriptionPlan`）、预热校验、Live 安装生命周期（`SubscriptionInstallation`） |
| live_generation.go | Live Provider 代际创建、deps 隔离、Prepare 与 StartSubscriptionLoop |
| live_revision.go | kline 修正（revision）账本与准入判定 |
| watcher.go | 爬虫 WebSocket 客户端（`SeriesWatcher`）：订阅/取消、K 线/价格/成交/深度回调、重连重放 |
| runtime_deps.go | 单 Runtime 依赖集合 `RuntimeDeps`（身份解析、时钟、配置视图） |
| common.go | 1m/1h K 线落库、缺口补齐、小时级修复推送 |
| tools.go | Tick 转 1m、ZIP 批处理、CSV 绩效指标计算（离线工具） |

## 核心类型与接口

### orm

| 名称 | 位置 | 要点 |
|---|---|---|
| `SeriesField` / `SeriesBinding` / `SeriesInfo` | orm/series.go:13 / :62 / :70 | 字段名、类型（float/int/string/bool/json）；`SeriesInfo` 含 Name、TimeFrame、Binding。`NewSeriesInfo(name,tf,fields)` 默认表名 `name_tf`，列 ts/end_ms/sid |
| `DataRecord` | orm/series.go:97 | 持久行：Sid、TimeMS、EndMS、Closed、Values。时间为毫秒，区间 `[TimeMS, EndMS)` |
| `DataSeries` | orm/series.go:105 | 运行时事件：Source、Sid、时间区间、TimeFrame、Closed、IsWarmUp、`Values map[string]any`、ExSymbol、Adj |
| `SeriesStore` | orm/series_store.go:12 | 通用门面：Write/WriteBatch、WriteSeries/WriteSeriesBatch、Read、Missing、FillMissing、Coverage、UpdateCoverage、Delete |
| `SeriesRepo` / `SeriesRangeRepo` | orm/series_repo.go:25 / :35 | 仓储接口；`dbSeriesRepo`（:58）为两端共用实现 |
| `KLineSeriesStore` | orm/kline_series_store.go:19 | 向已有 K 线行写扩展列；缺少对应 K 线行时失败 |
| `Storage` | orm/storage.go:18 | 显式连接依赖；`OpenStorage`（:37）、`EnsureSchema`（:66）。创建者负责 Close |
| `Queries` / `DBTX` | orm/db.go:36 / :18 | 查询句柄，绑定到连接或事务 |
| `SymbolState` / `SIDAllocator` | orm/symbol_state.go:219 / :15 | 符号索引（逻辑键 → 快照、SID → 快照）与 SID 分配、reservation 围栏 |
| `SymbolSIDRegistry` | orm/sid_registry.go:34 | PG 侧 SID 权威注册表 |
| `Subscription` / `StreamKey` | orm/subscription.go:29 / :43 | 订阅规范化与去重键 |
| `AdjInfo` / `SRange` | orm/types.go:19 / orm/models.go:38 | 复权段信息 / 覆盖区间记录 |
| `questRewriteSwapIntent` | orm/questdb_visibility.go:67 | 表替换的恢复意图（source/temp/backup） |

### data

| 名称 | 位置 | 要点 |
|---|---|---|
| `DataSource` | data/series_source.go:22 | `Info()`、`FetchHistory(ctx, sub, startMS, endMS)`、`SubscribeLive(ctx, subs, sink)` |
| `PagedHistorySource` | data/history_page.go:16 | 分页历史接口 `FetchHistoryPage` |
| `ObservationWarmupSource` | data/series_source.go:35 | event 流按真实观测计数预热 |
| `SeriesRuntime` / `SeriesPlan` | data/series_source.go:94 / :263 | 回测补齐与实盘激活的统一入口（`ThirdPartySeriesBootstrap` 为兼容别名） |
| `DataSourceCatalog` / `DataSourceFactory` | data/source_catalog.go:13 / :22 | 数据源注册表；工厂为每个 Runtime 生成独立实例 |
| `RuntimeDeps` | data/runtime_deps.go:37 | Runtime 依赖集合（Catalog、Storage、Symbols、Config、Clock 等） |
| `FuncDataSource` | data/func_source.go:15 | 用函数回调实现 DataSource |
| `IDataFeeder` / `SeriesFeeder` / `DBSeriesFeeder` | data/feeder.go:674 / :711 / :1163 | 单交易对回放与多周期聚合 |
| `HistSeriesFeeder` | data/hist_series_feeder.go:14 | 独立时序分页回放 |
| `Provider[T]` / `HistProvider` / `LiveProvider` | data/provider.go:88 / :303 / :997 | 回测堆调度与实盘 ingress |
| `TradeFeeder` / `WsDataLoader` | data/ws_feeder.go:17 / data/ws_loader.go:41 | 逐笔回放与归档缓存 |
| `LiveSpider` / `Miner` | data/spider.go:301 / :155 | 实时采集进程与交易所 miner |
| `SubscriptionPlan` / `SubscriptionInstallation` | data/subscription_plan.go:52 / :568 | 订阅计划（不可变）与 Live 安装生命周期 |
| `LiveSeriesRevision` | data/live_revision.go:13 | kline 修正记录（Revision、SourceVersion、EventTime） |
| `SeriesWatcher` | data/watcher.go:23 | 爬虫 WebSocket 订阅客户端 |

`DataHub` 类型位于 `strat/datahub.go`（AllReady 见 :109），不在本模块。`data/` 内没有 DataHub。

## 关键流程

### 1. 序列写入与读取（通用路径）

- 写入：`SeriesStore.WriteSeriesBatch`（orm/series_store.go:76）→ `WriteBatch`（:40）→ `NormalizeDataRecords`（:204）→ `dbSeriesRepo.WriteSeriesBatch`（orm/series_repo.go:366）。非 atomic 分支为 `InsertSeriesBatch` 加 `UpdateCoverage`。
- 读取：`SeriesStore.Read`（:94）→ `QuerySeriesRange`（series_repo.go:462）→ `CheckDataRecordPage`（series_store.go:117）→ `RecordsToSeries` → `CheckDataSeriesBytes`（series_budget.go:74）。
- 缺口补齐：`FillMissing`（series_store.go:154）→ `Missing`（:139）→ `MissingSeriesRanges`（series_repo.go:617）→ `ListSRanges`（orm/srange.go:312）。
- 删除：`DeleteSeriesRange`（series_repo.go:556）。TimescaleDB 分支为事务内 DELETE 加 `UpdateSRanges`；QuestDB 分支先写 sranges 墓碑，删除比例达到阈值（`seriesQuestRewriteDeleteRatio = 0.5`，series_repo.go:45）才进入 `rewriteQuestSeriesTableLocked`（series_repo.go:728）重写表。

### 2. K 线写入与覆盖

- 新 DataSeries 写入：`InsertOHLCVSeriesAuto`（orm/ohlcv_series.go:1119）→ `normalizeOHLCVSeries`（:1488）→ `AddInsKline`（orm/pub_meta_queries.go:328）→ `insertOHLCVRowsLocked`（:1211）→ `finalizeKlineInsert`（orm/kline.go:822）→ `UpdateKRange`（kline.go:858）→ `updateKLineRange` / `updateKHoles`（kline.go:146）→ `UpdateSRanges`（orm/srange.go:551）→ `DelInsKline`。旧接口 `InsertKLinesAuto`（kline.go:769）流程相同。
- K 线读取：`Queries.QuerySeriesFields`（ohlcv_series.go:339）；带覆盖的入口为 `GetSeriesFields`（:742）。旧接口 `QueryOHLCV`（kline.go:50）经 `SeriesToKLines`（ohlcv_series.go:32）转换。
- 写入 NULL 语义（`klineWriteValue`，ohlcv_series.go:1352）：缺失的 quote、buy_volume 写 0.0，缺失的 trade_num 写 0，其他缺失字段写 NULL。读取时 NULL 以 key 存在、值为 nil 的形式保留（kline_series_store.go:450）。

### 3. 复权与聚合

- 数据序列路径不改价格：只把 `AdjInfo` 挂到每行的 `Adj` 上（ohlcv_series.go:866）。
- 旧接口复权：`GetOHLCV`（kdata.go:646）→ `getAdjs`（:668）→ `GetAdjOHLCV`（:758）→ `ApplyAdj`（:821），前/后复权由 `CumFactor` 累乘决定（:882-898）。
- feeder 复权（data 侧）：`feeder.go` 复制 Values，调整 open/high/low/close/volume/buy_volume；quote、trade_num 与自定义字段不调整。
- 聚合规则：`seriesAggRule`（series.go:698）。K 线默认 open=first、high=max、low=min、close=last、volume/quote/buy_volume/trade_num=sum，其他字段 last。`ExSymbol.AggRules`（JSON）可覆盖；`orm.RegisterAggRule` 可注册自定义规则；未配置或非法默认 last。
- OHLCV 合并：`mergeOHLCVCore`（ohlcv_series.go:215）跳过成交量 <= 0 的行；扩展字段由 `mergeOHLCVExtraFields`（:273）对整桶聚合。

### 4. 历史补齐（data 侧）

- `EnsureRuntimeSeriesRange` / `ensureSeriesSubsRange`（data/series_source.go:583）：有分页或字节预算时走 `ensurePagedSeriesRange`（history_page.go:78），即 `ReadSourceHistory` 分页、`store.WriteBatch`、`UpdateCoverage`；否则走 `FillMissing`。
- `EnsureSeriesRange`（series_source.go:701）：校验 source 与 TF、计算缺口、`FetchHistory`、归一化 Sid、写入、更新覆盖。
- 显式补齐 CLI：`./bot series list`、`./bot series down -pairs ... -tables ... -timestart ... -timeend ...`（data 的 `series down` 按 source 名过滤）。

### 5. 回测回放

- `NewHistProviderWithRuntimeDeps`（data/provider.go:353）→ `SetSeriesSubs`（:439，独立序列建 HistSeriesFeeder）→ `SubWarmPairs`（:628）→ `subWarmPairs`（:187）→ newFeeder（:390，`NewDBSeriesFeederWithRuntimeDeps`）→ `WarmTfs`（feeder.go:815）→ `getTfKlines`（:319，`AutoFetchSeriesWithOptions`）。
- 运行：`LoopMain`（provider.go:771）→ `runHistFeedersWithDrainObserver`（:860）按 `getNextMS` 的最小堆调度（同一 bar 时间的并列项：回测 `histBarOrderFor` 为哈希序，键 = `BarPriority(barMS, symbol, bt_bar_order_seed)`，64 位 FNV-1a 加终混，中性、可复现、与输入顺序及品种增删无关；非回测保持品种名升序；再并列按品种名/插入序，issue 112）；每步 `GetBatch` → `CallNext` → `RunBatch` → `onNewData`（feeder.go:1001）→ `onStateOhlcvsWithErr`（:272）→ `fireCallBacks`（:394）。
- 独立序列回放：`HistSeriesFeeder.CallNext`（hist_series_feeder.go:143）→ `orm.SeriesStore.Read`。
- 逐笔回放：`TradeFeeder.CallNext`（ws_feeder.go:161）→ `loadNextBatch`（:205）→ `WsDataLoader.LoadTrades`（ws_loader.go:270）。缓存未命中时 `submitTask` → `downloadWorker` → `downloadJob`（:616，归档 URL 经 `exg.BuildArchiveURL*`）→ `SplitBigZip`（:347）→ 每小时 `.bin`（`writeBinaryTrades`，:773）。

### 6. 实盘接入

- `NewLiveProviderWithRuntimeDeps`（provider.go:1042）→ `newLiveProviderWithCatalog`（:1057）：创建 SeriesWatcher，挂载 `makeOnSeriesMsg` / `makeOnTrade` / `makeOnDepth`。
- 消息处理：`makeOnSeriesMsg`（:1419）做 ingress 锁与 wait-bar 判定 → `runHandler`（:1339）→ `onNewData` → `OnDataSeries`。
- 订阅安装：`InstallLivePlan`（data/subscription_plan.go:946）/ `PrepareLivePlan`（:977）→ `prepareLivePlan`（:1006）：每个源建 group，托管源调 `SubscribeManaged`，其余源以 goroutine 调 `SubscribeLive`。随后 `warmupLive`（:415）读闭合历史，`verifiedLiveWarmup`（:480）校验边界与连续性，`validateSources`，最后 `activate`（:809）冲刷 pending。
- 代际隔离：`NewGeneration`（data/live_generation.go:34）；有 kline 时必须由 `generationFactory` 返回独立 provider。`PrepareSubscriptionPlan`（:278）→ `configureSubscriptionIngress`（:92）→ `isolateSubscriptionDeps`（:61）→ `StartSubscriptionLoop`（:258）。

### 7. 爬虫采集（LiveSpider）

- `RunLiveSpiderWithRuntimeDeps`（data/spider.go:1099）→ `prepareLiveSpider`（:1119）：storage Prepare → startup → `consumeSeriesWriteQ`（:49）与 `monitorSubscriptions`。
- 客户端 `watch_pairs` / `unwatch_pairs`：`makeInitConn`（:1199）→ `getMiner`（:1154）→ `newMiner`（:467）。`Miner.SubPairs`（:522）分发到 `loopKLines`（:922，分钟 ticker 调 `FetchOHLCV`）、`watchKLines`（:784）、`watchTrades`（:622）、`watchOdBooks`（:722）、`watchPrices`（:676）。
- K 线：`sidMap` / `writeQ`（spider.go:1043-1067）→ `consumeSeriesWriteQ` → `trySaveSeriesWithRuntimeDeps`（common.go:71）→ `observeClosedSeries` → Broadcast。`klineParallelNum=6`（spider.go:38）；writeQ 容量 9999（:337）。
- 启动时 `data/spider.go:405` 调用 `EnsureTimescaleCompressionWithStorage`，随后 :408 调用 `purgeSpiderKlineUn`。

### 8. 修正（revision）

- 上游调用 `RememberSeriesRevision`（data/live_revision.go:47）登记 `(stream, start, end, closed)` 的版本，账本容量为 `2*numTACache*streams`。
- 新行经 `acceptRevisionRows`（:77）判断迟到或修正：修正需要 Revision、SourceVersion、EventTime；版本回退的行丢弃；身份改变报错；粗粒度周期修正直接拒绝（:154）。

### 9. 数据库表替换（QuestDB）

- 主路径 `maintainCompactTableAtRoot`（orm/compact.go:550）→ `execCompactLockedAtRoot`（:780）→ CTAS（:810）→ `waitCompactVisibleCount`（:743）→ 校验临时表快照（:818）→ `replaceVerifiedCompactTableAtRoot`（:837）：校验源表（:841）→ 写恢复标记（:848）→ RENAME 源为 backup（:851）→ RENAME 临时表为源（:854）→ 激活后校验（:869）→ DROP backup（:886）→ 清除标记（:891）。
- 可见性等待统一走 `waitForQuestCondition`（orm/questdb_visibility.go:195），通用超时 3s / 50ms（:183-184），压缩 30s / 100ms（compact.go:107-108），K 线插入可见性 30s（kline_insert_lock.go:25）。
- 恢复：`questdb_visibility.go` 中的 swap intent 文件在启动时检查 source/temp/backup 三表存在性并分支处理（约 :1683-1810）。

### 10. SID 与标的

- 标的身份由 `exchange + market + symbol` 决定，`exg_real` 仅作元数据（见 doc/custom_data.md 3.3）。
- 加载：`LoadExgSymbols`（orm/exsymbol.go:50）→ `ListSymbols`（pub_meta_queries.go:439，`LATEST BY sid`）→ `cacheExgSymbolsForState`（exsymbol.go:95）。
- 新增：`EnsureSymbols`（exsymbol.go:428）→ `reserveStateSymbols`（:549）→ `addSymbolsLocked`（pub_meta_queries.go:495）。QuestDB 路径：先写 pending marker，再 INSERT，逐行标记 Inserted，可见后确认并删除标记。
- SID 分配：`SIDAllocator`（symbol_state.go:15）原子递增；reservation 通过 `reservationKeys` 反查，保证同一 SID 不被两个逻辑键占用。PG 下由 `SymbolSIDRegistry.Reserve`（sid_registry.go:193，ON CONFLICT 返回既有行）保证唯一。

## 配置项与外部依赖

### 配置（`database` 段，config/types.go:194-203）

- `db_type`：`questdb` 或 `timescale`（别名 `timescaledb` / `postgres` / `postgresql` / `postgresql+timescale`）。未设置时按端口识别：8812 为 QuestDB，5432 为 PostgreSQL，其他端口探测。建议显式配置。
- `url`：连接串，例如 `postgresql://banbot:...@127.0.0.1:5432/banbot?sslmode=disable`。
- `max_pool_size`：默认 `max(40, NumCPU*4)`（base.go:267）。
- `auto_create`：初始化/升级 banbot schema（不创建 PG 服务、账号或数据库）。
- `sid_registry_url`、`qdb_mem_pct`、`qdb_max_mem_mb`、`retention`。

### 其他配置与环境

- `config.TimeRange`、`config.HistoricalCoverage`、`config.Data.BTNoKlineDownload`、`config.StrictBacktest()`、`config.SpiderAddr`、`config.PreFire`（data 侧经 `RuntimeDeps.Config` 读取）。
- 数据目录：`config.GetDataDir()`；K 线插入锁文件位于 `<dataDir>/locks/kline_insert`（kline_insert_lock.go:47）；逐笔缓存目录 `wscache`（ws_loader.go:199-204）。
- 环境变量：`BanDataDir`、`BanStratDir`（config/biz.go）。`QDB_JAVA_OPTS` 在 questdb.go:226-227 被无条件覆盖。
- 默认值：序列分页 20000 行（series_prefetch.go:40）；回放批大小 3000/20000（feeder.go:1624、1633）；`dirtyVers` 缓冲 5（provider.go:415）；ws 下载/切分 worker 为 min(CPU,8)（ws_loader.go:213-240）；HTTP 超时 120s（ws_loader.go:186）。

### 外部依赖

- QuestDB（PGWire 8812，使用 `LATEST BY`、`WAL DEDUP UPSERT KEYS`、`wal_tables()`、`tables()`）。
- TimescaleDB / PostgreSQL（pgxpool、COPY、ON CONFLICT、timescaledb_information、`add/remove_compression_policy`；无扩展时压缩逻辑为 no-op）。
- SQLite：banpub.db 与 trades，单写连接（base.go:423）。
- banexg：`WatchTrades` / `WatchOHLCVs` / `WatchOrderBooks` / `WatchMarkPrices` / `FetchOHLCV` / `CheckSymbols` / `GetMarket` / `HasApi`；`exg.BuildArchiveURL*`；`exg.GetAlignOffForExchange`。
- 其他：zap、`sasha-s/go-deadlock`（data/common.go:16）、HTTP 归档下载、`utils.ServerIO` socket。

## 与其他模块的交互边界

- **strat / DataHub**：策略通过 `DataSub` 声明订阅（`strat.DataSub` 与 `orm.Subscription` 为同一别名，strat/types.go:112）；`DataHub`（strat/datahub.go）按 `(tf, source, sid)` 缓存 `DataFields`。data 只负责把 `DataSeries` 投递给 `OnDataSeries` / 回调，不持有 DataHub。
- **biz / live / opt**：`Trader.FeedDataSeries`、`CryptoTrader`、`BackTest` 消费 `DataSeries`。这些模块不应直接读写 sranges 或 QuestDB 表。
- **entry / base.go**：启动时的 `Setup` / `OpenStorage`、`EnsureExgSymbols`、`EnsureListDates`、`SetListMS` 由上层调用（data/ 内部不直接调用这些）。
- **web / dev、api**：`/api/kline/series` 为只读查看接口（web/dev/api_dev.go:1922 调用 `FindSRanges`）。写入、补齐、删除必须经 `SeriesStore` / 数据源运行时。
- **banbot 与 banexg**：交易所差异只在 banexg 内解决。banbot 的 data/ 不得出现 `if exchange == ...` 类分支。
- **进程边界**：LiveSpider 可作为独立进程运行（`SpiderAddr`），通过 BanIO 的 JSON payload 与 Runtime 通信；payload 类型恢复需由接收方 schema 校验。

## 扩展与修改指南

### 新增一个自定义时序数据源

1. 用 `orm.NewSeriesInfo(name, tf, fields)` 定义结构（字段类型限 float/int/string/bool/json）。
2. 实现 `DataSource`（复杂源）或用 `data.NewFuncDataSource` / `RegisterFuncDataSource`（简单源）。注册时需要工厂（`RegisterDataSourceFactory`），否则 Runtime catalog 会拒绝（source_catalog.go）。
3. 历史抓取必须区分“真实空结果”与“抓取失败”，不能把失败写成确认无数据（参见 doc/series_usage.md）。
4. 策略侧在 `OnDataSubs` 声明 `DataSub`，`ExSymbol` 通过 `orm.EnsureExSymbol` 获得 sid。

### 修改表结构或写入路径

- 同时检查两端：`series_repo.go` 的 DDL（PG 与 QuestDB 类型映射不同）、`pg_queries.go` / `pub_kline_queries.go`。
- 任何 QuestDB 写后读都要使用 `waitForQuestCondition` 等待预期行、时间戳、范围或记录数可见，不能依赖一次空读（AGENTS.md）。
- 修改表替换或删除逻辑必须同时：验证替换表快照再 DROP/RENAME；超时保留恢复标记；补充针对超时/轮询路径或删除前验证路径的回归测试（AGENTS.md）。已有参考：compact_test.go:188-223、compact_test.go:655、questdb_visibility_test.go:616、:1055。

### 修改复权或聚合

- 复权只通过 `AdjInfo` 与 `ApplyAdj` 路径；不要为性能引入 typed OHLCV 快速路径，所有字段统一走 `DataSeries.Values map[string]any`（AGENTS.md）。
- 新增聚合规则用 `orm.RegisterAggRule`，并在 `ExSymbol.AggRules` JSON 中引用。

### 修改订阅与 Live 安装

- 订阅计划对象不可变（`SubscriptionPlan`），修改需生成新计划。Live 安装必须经 `Activate` / `CommitPrepared` 线性化，不要绕过 `SubscriptionInstallation` 直接调用 sink。
- 修改 `StreamKey` 或 `DataSub.Key()` 会影响 map 去重，同时影响 strat 与 data 两侧，需一并检查。

### 易踩坑

- K 线的 `NewSeriesInfo("kline", ...)` 会设置 `EndColumn="end_ms"`，与 K 线表的 `end_ms` 约定不同。
- 扩展列名不能与内置列冲突：sid、time、ts、open、high、low、close、volume、quote、buy_volume、trade_num。
- `DataSeries` 的 JSON 往返不保留任意整数宽度或自定义 Go 类型；需要严格类型的源必须做 schema 解码校验。
- 同进程写后读优先用定向可见性等待或进程内锁，不要用盲目重试。

## 已知限制与设计取舍

- **QuestDB 表替换/恢复**：源表校验失败时无法区分“激活后已有新写入”与“激活结果损坏”，恢复分支会走回滚；恢复标记与锁文件位于 `os.TempDir()` 下；压缩路径持排他锁期间执行 CTAS 与最长 2×30s 的可见性等待。
- **强制删除分区**：`tryRepairQuestDBMissingPartition` 在错误匹配时直接执行 `FORCE DROP PARTITION`，不做快照校验。
- **PG K 线写入**：`CopyFrom` 失败后先 DELETE 窗口再重试 COPY，二者不在同一事务内，且假定输入按时间升序。
- **后端判定**：auto 模式下探测失败被吞掉并乐观选择 QuestDB；空 `db_type` 且非 8812 端口时 `storage_identity` 视为非 QuestDB。
- **部分查询把数据库错误当作“无数据”**（如 `GetKlineRange` 一类），调用方需自行区分。
- **SID 与订阅去重键**：`Key()` 与 `StreamKey.String` 作为 map key 使用（`strat/data_subs.go`、`data/subscription_plan.go`），修改格式需两侧同步检查。
- **逐笔回放**：无成交的小时不写缓存文件；`SetSeek` 对齐到整点后首次加载会先前进 1 小时；缓存文件非原子写入，截断文件会被视为命中。
- **订阅管理**：watcher 的 `initMsgs` 只增不减，重连会重放已取消的订阅；`UnWatchJobs` 对分钟级合约只取消 ohlcv tag。
- **spider**：保存失败仅记日志，无重试；`monitorSubscriptions` 持 `minersMu` 读锁调用交易所 Watch。
- **成交量复权**：`volume`、`buy_volume` 与价格使用同一复权因子，`quote` 不调整。
- `orm/db.go` 头部的 “Code generated by sqlc” 标注与实际不符，该文件为手写逻辑，不可再生成覆盖。
- `orm/banpub.go` 的 SQLite 事务级别为 `LevelDefault`，DSN 不含 `_txlock`。
- 缓存符号快照 `GetExSymbols` 等返回内部共享指针，调用方不得修改。
