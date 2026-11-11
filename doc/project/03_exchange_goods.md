# 交易所封装（exg）、品种筛选（goods）与国内市场（cn_markets）

> 基线：仓库 main 分支。banexg 依赖为 `github.com/banbox/banexg v0.2.66`（go.mod:11），交易所侧能力以该模块缓存源码核实。行号仅作定位参考。

## 定位与职责

- **exg/**：banbot 与 banexg 之间的薄适配层。负责：
  - 创建并缓存 banexg 实例（`Setup` / `GetWith` / `NewForRuntime`），把 config 中的交易所选项、账户凭证、市场类型翻译为 banexg options；
  - 用 `BotExchange` 包装实例，挂接下单后回调；
  - 通过**可选能力接口**（optional capability）获取交易所专有语义（符号解析、client order id、订单事件、归档 URL、账户导出、K 线日对齐），未实现时给出确定的降级或错误；
  - 精度换算（`PrecCost` / `PrecPrice` / `PrecAmount`）。
- **goods/**：品种（交易对）筛选流水线。第一个过滤器必须是 `IProducer`，负责生成候选标的；其余过滤器按 config 顺序逐个过滤；最后应用 pairmgr 的 offset / limit，写入 `core` 运行时状态。
- **doc/cn_markets/**：纯文档，无对应 Go 包。内容为国内期货/股票的数据源、柜台接入（CTP、XTP 等）、期货交易规则（主力连续、复权、合约编码）。仓库代码中与国内市场直接相关的只有：banexg `china` 适配器（通过 `exg/data.go` 的 `allowedExgIDs` 注册）、`config` 中按 `Countries=CN` 推导的 `Asia/Shanghai` 时区，以及 `ExchangeUsesOpaqueSymbols` 对 `china` 的短名不展开规则。

## 目录/文件地图

### exg/

| 文件 | 作用 |
|------|------|
| `data.go` | 全局 `Default` 交易所、`exgMap` 缓存、`allowedExgIDs`（binance/okx/bybit/china）与 `IsAllowedExgID` |
| `biz.go` | `Setup`、`NewForRuntime`、`createConfigured`（config→options）、`GetWith`（缓存）、`Prec*`、`GetLeverage`、`GetOdBook`、`GetTickers24Hr` |
| `ext.go` | `BotExchange` 包装类型、`getExchangeCapability[T]` 泛型解包、下单回调（`SetOrderCallback` / 旧全局 `AfterCreateOrder`）、`FetchOHLCVArchive` 转发 |
| `symbol.go` | 符号解析：`ResolvePriceSymbol`、`ResolveRuntimePriceSymbol`、`NewPriceSymbolParser*`、`NewLegacyPriceSymbolParser` |
| `client_order.go` | client order id：`BuildClientOrderID`（兼容，失败回落 banexg 旧格式）、`BuildClientOrderIDChecked`（严格） |
| `order_events.go` | `OrderEventCapability` 获取与 `RequireOrderEventCapability`（实盘严格模式） |
| `alignment.go` | 日线以上周期的对齐偏移：`GetAlignOffForMarket`（由 DayTimes/NightTimes 推导）及各层包装 |
| `account_download.go` | 账户导出（`StartAccountDownload` / `GetAccountDownload`）与归档 URL（`BuildArchiveURL*`）的能力接口 |
| `*_test.go` | 行为佐证（symbol、alignment、order_events、account_download、biz_runtime） |

### goods/

| 文件 | 作用 |
|------|------|
| `biz.go` | `Setup`（legacy 全局）、`GetPairFiltersWithConfig`、`RefreshPairList`、`RefreshPairListWithRuntimeDeps`（runtime 主入口）、`FilterPairsWithRuntimeDeps`、static pairs 辅助函数 |
| `types.go` | `IFilter` 及可选接口、`RuntimeDeps`、各过滤器配置结构体、注册表（`RegisterFilter` / `CreateFilter`）、`init()` 注册内置过滤器 |
| `filters.go` | 所有过滤器的实现、`getSymbolVolsWithOptions`（历史量计算）、`filterByMinCostWithRuntime`（最小下单额过滤）、`CorrelationFilter` 聚类排序 |
| `biz_test.go` | 刷新与过滤流程测试 |

### doc/cn_markets/

| 文件 | 内容 |
|------|------|
| `readme.md` | 数据源（tushare、akshare/aktools、天勤 TqSdk、立得）、股票/期货实盘通道分类（柜台 API、文件单、量化终端、破解接口）、AKShare 本地启动方式 |
| `future.md` | 期货基础规则（结算价、涨跌停、交易日、自然人最后交易日、主力/指数合约编码、前复权算法、合约代码规则、手续费）及回测主力切换方案对比 |
| `ctp.md` | CTP 柜台架构、tick 与 bar 合成的局限、各软件 K 线边界差异、CTP 登录参数、OpenCTP 仿真 |

## 核心类型与接口

### exg

- **`BotExchange`**（`exg/ext.go`）：嵌入 `banexg.BanExchange`，额外持有 `orderCallback`（带读写锁）。`CreateOrder` 在底层下单后调用回调，回调错误会覆盖返回的 error，但 `order` 仍被返回（调用方需同时处理）。`UnderlyingExchange()` 用于解包。
- **`PutOrderRes`**（`exg/ext.go`）：下单回调参数（Symbol/OrderType/Side/Amount/Price/Params/Order/Err）。
- **可选能力接口**（adapter 实现即生效，未实现则走降级）：

| 接口 | 定义位置 | 作用 | banexg v0.2.66 实际实现 |
|------|----------|------|------------------------|
| `PriceSymbolCapability` | exg/symbol.go（同名接口亦在 banexg/symbol.go:7） | 解析交易所专有符号 | 仅 `china` |
| `ClientOrderCapability` | banexg/client_order.go，exg/client_order.go 别名 | 生成 client order id | `binance`、`okx`、`bybit` |
| `OrderEventCapability` | exg/order_events.go（banexg/order_event.go 提供默认实现） | 解析 client id、订单事件 id、订单时间戳 | `binance`、`okx`、`bybit` 提供 Parse/Build 等方法 |
| `OHLCVArchiveFetcher` | banexg/intf.go:13 | 按归档读取 K 线 | `binance`（binance/vision.go） |
| `AccountDownloadCapability` | exg/account_download.go:12 | 异步账户导出 | **未见任何实现** |
| `ArchiveURLCapability` | exg/account_download.go:71 | 构造归档下载 URL | **未见任何实现** |

- **下单回调**：`BotExchange.SetOrderCallback` 为实例级（优先）；包级 `AfterCreateOrder` 为旧版全局回退。显式设置的 nil 回调会禁用回退（ext.go:91-101）。

### goods

- **`IFilter`**（types.go）：`GetName()`、`IsDisable()`、`Filter(pairs, timeMS)`。
- **`IProducer`**：`IFilter` + `GenSymbols(timeMS)`。只有 `VolumePairFilter` 实现。
- **可选接口**：`SymbolStateFilter`（带 `*orm.SymbolState` 与 exchange）、`RuntimeFilter`（带 `*RuntimeDeps`）、`SymbolStateProducer`、`RuntimeProducer`（`GenSymbolsWithRuntimeDeps`）。runtime 路径优先使用 `RuntimeFilter`，其次 `SymbolStateFilter`，最后 legacy `IFilter`。
- **`RuntimeDeps`**（types.go）：`Core`、`Clock`、`Config`、`DataDir`、`Symbols`、`Storage`、`Exchange`、`ShowLog`。显式运行时只用这些字段，不读包级全局（`goods/biz.go` 与 `filters.go` 中的 `deps == nil` 分支才回落全局）。
- **`BaseFilter`**：`Name`、`Disable`、`AllowEmpty`。`AllowEmpty` 由 `GetPairFiltersWithConfig` 按 `PairMgr.Cron == ""` 计算后经 `CreateFilter` 传入（goods/biz.go:46-60）。
- **注册表**：`RegisterFilter(name, factory)` / `GetFilterFactory` / `CreateFilter`（types.go:181-220）。`RegisterFilter` 重名直接覆盖，无告警。
- **内置过滤器**（types.go `init()` 注册，实现见 filters.go）：

| 名称 | 结构体 | 主要语义 | 实现要点 |
|------|--------|----------|----------|
| `VolumePairList` | `VolumePairFilter` | producer；按近 `back_period` 的 mean(close*volume) 倒序取前 `limit`/`limit_rate` | 最小成交额 `min_value`，AllowEmpty=false 时默认 `core.AmtDust`；随后按最小下单额过滤 |
| `PriceFilter` | `PriceFilter` | 价格精度（一个 tick 占比）、单位价值、min/max | 取 1h 最后收盘价（AdjFront） |
| `RateOfChangeFilter` | `RateOfChangeFilter` | 近 N 日 (high-low)/low | 1d K 线 |
| `VolatilityFilter` | `VolatilityFilter` | 近 N 日收益率标准差 | 1d K 线 |
| `CorrelationFilter` | `CorrelationFilter` | 与全市场的平均相关性；可按 asc/desc 贪心排序，取 top_n / top_rate | 依赖 `utils.CalcCorrMat`，少于 4 个标的时直接放行 |
| `AgeFilter` | `AgeFilter` | 上市天数区间 | 需 `ListMs`；AllowEmpty 时不删除而是 `SetPairBanUntil` |
| `SpreadFilter` | `SpreadFilter` | 买卖价差（配置项 `max_ratio`） | **未实现**，`Filter` 直接返回输入（filters.go:900） |
| `BlockFilter` | `BlockFilter` | 黑名单 | pairs 在 `GetPairFiltersWithConfig` 中解析 |
| `OffsetFilter` | `OffsetFilter` | reverse / offset / rate / limit | `Offset >= len` 返回空；不修改入参（复制后处理） |
| `ShuffleFilter` | `ShuffleFilter` | 按 seed 洗牌 | 复制后洗牌，不修改入参 |

- **`SymbolVol`**（filters.go）：`Symbol`、`Vol`（近 N 根的 mean(close*volume)）、`Price`（最新收盘价）。

## 关键流程

### 1. 交易所初始化（legacy 全局）

`biz/biz.go:85` `exg.Setup()` → `exg/biz.go:19` `Setup`：若 `Default` 已存在直接返回；否则 `GetWith(config.Exchange.Name, core.Market, core.ContractType)`，并设置 `core.IsContract`。

`GetWith`（exg/biz.go:140）：cache key 为 `name@market@contractType`，命中则 `SetMarketType` 复用，未命中则 `create`。`create` 使用包级 `config.Exchange`、`config.Accounts`、`config.BakAccounts`。

### 2. 交易所初始化（runtime）

`entry/runtime_entry.go:128` 调用 `exg.NewForRuntime(snapshot, netDisable)`（exg/biz.go:40）：

- 账户选择：`NoTrade` 账户进入 backups；prod 环境下所有非 NoTrade 账户都进入 accounts；非 prod 只取名为 `default` 的账户，否则取排序后的第一个；非 prod 且无账户时注入空 `default`。
- 调用 `createConfigured`（exg/biz.go:66）：`exgOpts = exchangeConfig.Items[name]`，key 经 `SnakeToCamel` 转换；`fees` 用 mapstructure 解为 `map[string]map[string]float64`；账户凭证写入 `OptAccCreds`，`OptAccName` 取最后一个处理的账户；`env == test` 时写 `OptEnv`；最终 `bex.New(name, options)` 并包装为 `BotExchange`。
- 该路径**不经过** `GetWith` 缓存，每个 runtime 独立实例，不写 `Default`。

### 3. 下单回调

`biz/odmgr_live.go:216`：有 runtime deps 时调用 `exg.SetOrderCallback(deps.Exchange, logPutOrderWithDump(...))`（实例级）；否则若包级 `AfterCreateOrder` 为空则设为 `logPutOrder`（odmgr_live.go:217-218）。`BotExchange.CreateOrder`（ext.go:62）对成功与失败的下单都会触发回调。

### 4. 精度换算

`biz/odmgr_live.go:2504`（`PrecAmount`）、`:2661`（`PrecPrice`）、`biz/wallet.go:1181`（`PrecCost`）→ `precNum`（exg/biz.go:161）。对 `amount`：若 `market.Contract && ContractSize != 1`，先除以合约乘数再 `PrecAmount`，结果再乘回。`source` 不在 cost/price/amount/fee 中会 `panic`。

### 5. 符号解析

`biz/odmgr.go:` / `biz/wallet.go:` 的 `ResolvePriceSymbol` / `ResolveRuntimePriceSymbol` → `resolvePriceSymbol`（exg/symbol.go:55）：

1. `exchange == nil`：仅用 `core.SplitSymbol`。
2. 有 `PriceSymbolCapability`：调用 adapter（带 panic 保护，错误直接返回，不回落）。
3. 否则 `MapMarket(pair, 0)` 取 market，再用 `Base/Quote/Settle` 修正结果。

`ResolveRuntimePriceSymbol` 在无 capability 时直接用通用解析，避免对已是标准符号的 runtime 再次 `MapMarket`。

### 6. 日线对齐偏移

`orm/kline.go:649`、`orm/ohlcv_series.go:56` 调用 `exg.GetAlignOffForSymbol(exchange, market, symbol, tfSecs)` → `GetWith` → `GetAlignOffForExchange`（alignment.go:81，出错打日志并返回 0）→ `GetAlignOffForExchangeChecked`（alignment.go:52，`GetMarket` 失败时回落 `MapMarket`）→ `GetAlignOffForMarket`（alignment.go:17）：

- 取所有 `DayTimes` 的最大结束时刻 `dayEnd`，与 `NightTimes` 的最早开始时刻 `nightStart`（对日取模）；`boundary = (dayEnd + nightStart) / 2`；偏移为 `dayMS - boundary`（秒）。
- 仅当 `tfSecs >= 86400` 且两类时段都存在时才非零。无夜盘的品种返回 0。

### 7. 品种刷新（legacy）

`biz/biz.go:93` `goods.Setup()`：若 `config.PairFilters` 非空，`GetPairFilters` 后取 `fts[0]` 作为 producer，其余赋给包级 `filters`。`biz/biz.go:146` `RefreshPairList(timeMS)` → `RefreshPairListWithSymbolState(nil, exg.Default, ...)`：

- 有静态 `config.GetStaticPairs()`：若未冻结则用 1h×1 根算成交额，`filterByMinCost`；`allowFilter = PairMgr.ForceFilters`。
- 否则 `allowFilter = true`，由 `pairProducer.GenSymbols` 生成。
- `orm.EnsureCurSymbols(pairs)` → 依次应用 `filters`（有 state 的走 `SymbolStateFilter`）→ offset/limit → `core.SetLegacyPairs`。

### 8. 品种刷新（runtime，主路径）

`RefreshPairListWithRuntimeDeps`（goods/biz.go:222）被 `biz/biz.go:259`、`biz/tools.go:700`、`entry/runtime_entry.go:630`、`entry/series.go:83`、`entry/factor_storage.go:177`、`live/common.go:86`、`live/runtime_cron.go:98` 等调用。顺序如下：

1. 校验 `deps` 各字段（Symbols / Exchange / Config / Core 必须非空）。
2. `staticPairsWithConfig(cfg)`：合并 `cfg.Pairs` 与各 `RunPolicy.Pairs` 并去重。
3. 有静态标的：`EnsureCurSymbolsWithRuntimeConfig`；若非冻结（`useFrozenStaticPairsWithRuntime`，回测严格模式下可冻结）则用 `GetSymbolVolsWithRuntimeDeps` 计算量并 `filterByMinCostWithRuntime`，仅 `RuntimeDeps.AllowRetiredPairs`（因子 Weights 存储回放）时，因交易所已无该市场而被过滤、但库内 K 线范围与回测窗口相交的静态品种由 `keepRetiredStaticPairs` 放回；`allowFilter = ForceFilters`。
4. 无静态标的：重建过滤器链（`GetPairFiltersWithConfig(pairFilters(cfg), false, cfg)`），`filters[0]` 必须是 `IProducer`；优先 `RuntimeProducer` → `SymbolStateProducer` → `GenSymbols`。
5. 无论静态与否，`EnsureCurSymbolsWithRuntimeConfig` 都会在候选确定后执行一次。
6. `allowFilter` 为真时调用 `FilterPairsWithRuntimeDeps(filters[1:], deps, pairs, timeMS)`（goods/biz.go:340），逐个 `RuntimeFilter` / `SymbolStateFilter` / `IFilter`。
7. pairmgr `offset` / `limit` 截断。
8. `deps.Core.SetPairs(pairs, additional)`；对已被禁用的 banned 标的清除 ban 时间。

### 9. 历史量与 OHLCV 过滤的公共管线

- `getSymbolVolsWithOptions`（filters.go）→ `orm.FastBulkOHLCVWithSymbolStateAndOptions`，回调中对每个标的取最近 `num` 根，计算 `mean(close*volume)`，并以最新收盘价作为 `Price`。`bt_no_kline_download` 下（非 strict）不下载、直接读库；strict 且无 `historical_coverage` 时返回 download-disabled 错误。
- `filterByOHLCVWithRuntimeDeps`（filters.go）→ 同一批量读取，`orm.ApplyAdj(adjs, arr, core.AdjFront, endMS, 0)` 后交给各过滤器回调，结果按原顺序保留。
- `RuntimeDeps.querySeries`（filters.go）用于 `CorrelationFilter` 的单标的序列读取，会附带 `WithSeriesSymbolState` / `WithExchange` / `WithKlineRuntimeOptions`。

### 10. 最小下单额过滤

`filterByMinCostWithRuntime`（filters.go）：取所有非 NoTrade 账户的 `stakeAmount` 最大值（若为 0 则用 default 账户的 stake），对每个标的 `minCost = Limits.Amount.Min * Price`，超过则跳过并记录。无市场或无 `Limits.Amount` 的标的同样不进入结果（`AllowRetiredPairs` 时库内有窗口内数据的由 `keepRetiredStaticPairs` 放回）。

## 配置项与外部依赖

### 配置（doc/config.yml 为权威模板）

- 交易所：`exchange.name`、`exchange.items.<name>`（传给 banexg 的选项，key 自动转驼峰；`fees` 为嵌套 map）、`market_type`、`contract_type`、`env`。
- 账户：`accounts.<name>` 的 `api_key` / `api_secret` / `password`、`no_trade`、`stake_rate`、`max_stake_amt`、`max_pair` 等（凭证由 `GetApiSecretFor(exchange, env)` 按环境取）。
- 品种：`pairs`（非空则 pairlists 忽略，除非 `pairmgr.force_filters`）、`pairmgr`（`cron`、`offset`、`limit`、`force_filters`、`use_latest`）、`pairlists`（过滤器列表，字段见 types.go 的 `yaml` tag）、`stake_currency`、`stake_amount`、`max_stake_amt`。
- 策略级：`run_policy[].filters`（同样的过滤器配置，经 `strat/main.go:1234` 调用 `GetPairFiltersWithConfig`）、`run_policy[].pairs`。
- 短名展开（`config/biz.go:parsePairs`）：spot → `X/Q`；linear → `X/Q:Q`；inverse → `X/Q:X`；需要 `stake_currency`。`ExchangeUsesOpaqueSymbols` 为真时原样返回。

### 外部依赖

- **banexg v0.2.66**：`bex.New`、`banexg.BanExchange`、`Market`（`DayTimes`/`NightTimes`/`Limits`/`Precision`/`ContractSize`）、`MapMarket`、`PriceOnePip`、`GetCurMarkets`、`FetchOrderBook`、`FetchTickers`。
- **bntp**：`bntp.LangCode`（NTP 语言，config.NTPLangCode）。
- **orm**：`SymbolState`、`ExSymbol`、`EnsureCurSymbols*`、`FastBulkOHLCVWithSymbolState*`、`GetSeries`、`ApplyAdj`、`GetAlignOff`（orm 自有，非 exg 的）。
- **core**：`State`（SetPairs / SetPairBanUntil / BannedPairs / PairEnabled）、`SplitSymbol`、`AdjFront`、`AmtDust`、`GetOdBook` / `SetOdBook`、全局 `ExgName` / `Market` / `ContractType` / `IsContract`。
- **utils / gonum**：`utils.CalcCorrMat`、`utils.StdDevVolatility`、`floats.Sum`。
- **pgx**：`goods/filters.go` 的 `querySeries` 使用 `pgxpool.Conn`。

## 与其他模块的交互边界

- **banexg（上游）**：banbot 只能通过 `banexg.BanExchange` 接口和上表可选能力接口访问交易所。`exg` 是唯一允许 `import banexg/bex` 的业务侧位置（另有 config/exchange_capabilities.go）。
- **config**：`config` 向 exg 提供 `Exchange`、`Accounts`、`BakAccounts`、`Snapshot`；向 goods 提供 `PairFilters`、`PairMgr`、`RunPolicy`、`StakeCurrencyMap` 等。`config/exchange_capabilities.go` 通过 `bex.New(name, nil)` 询问 adapter 的符号规则与默认时区。
- **orm**：goods 通过 `SymbolState` / `ExSymbol` 读取品种与上市日期，写入 `EnsureCurSymbols`；exg 的对齐偏移被 `orm/kline.go`、`orm/ohlcv_series.go` 使用。
- **core**：goods 写入 `core.State` 的 pairs 与 ban 状态；`com/price_state.go` 使用 exg 的符号解析策略。
- **biz / live / entry / runtime / strat**：通过 `exg.Default`（legacy）或 runtime 注入的 `Exchange`（runtime）使用本模块；下单回调在 `biz/odmgr_live.go`；品种刷新由各 entry 调用 `RefreshPairListWithRuntimeDeps`。
- **data**：`data/ws_loader.go:149`（`DownUrl`）与 `:623` 通过 `exg.BuildArchiveURL*` 构造归档 URL；`web/live/biz.go:1274/1296` 调用账户导出。
- **web/dev**：`web/dev/api_dev.go:1747` 用 `IsAllowedExgID` 校验开发接口的交易所参数。

## 扩展与修改指南

### 新增交易所专有行为

1. 在 banexg 对应适配器中实现能力接口（如 `PriceSymbolParts`、`BuildClientOrderID`）。**不要**在 banbot 中按交易所名写分支（AGENTS.md 开发规约）。
2. 如需新的能力，在 `exg/` 定义本地接口，使用 `getExchangeCapability[T]` 解包，并参照 `callClientOrderCapability` 做 recover 与空值保护；提供 `Checked` 与兼容两种入口（参照 `BuildClientOrderID` / `BuildClientOrderIDChecked`）。
3. 若新增的是 `Prec*` / `GetLeverage` 类语义，优先放在 banexg 内；banbot 只做通用换算。

### 新增允许的交易所 ID

- 修改 `exg/data.go` 的 `allowedExgIDs`，并确认 `bex.New(name, ...)` 可用。该列表同时影响 `web/dev/api_dev.go:1747`。

### 新增品种过滤器

1. 在 `goods/types.go` 增加结构体（嵌入 `BaseFilter`，字段加 `yaml` 与 `mapstructure` tag）。
2. 实现 `Filter`（legacy），并按需实现 `RuntimeFilter.FilterWithRuntimeDeps`（推荐；需要历史数据时复用 `filterByOHLCVWithRuntimeDeps` 或 `deps.querySeries`）。
3. 在 `types.go` 的 `init()` 中 `RegisterFilter`（或在外部包 `init` 中注册，见 types.go 注释示例）。
4. 如需作为 producer，实现 `GenSymbols`（及 `RuntimeProducer`）。
5. 更新 `doc/config.yml` 的 `pairlists` 段（以及 `run_policy.filters` 的示例，如适用）。
6. 注意 `AllowEmpty` 语义：未启用 cron 时为 true，缺数据的标的应按此返回。

### 删除过滤器

- 删除 `types.go` 中结构体与 `init()` 的注册，删除 `filters.go` 中对应方法。用户配置中残留的名称会在 `CreateFilter` 报 `unknown symbol filter`。同时检查 `doc/config.yml` 与示例配置。

### 调整 offset / limit / producer 语义

- legacy（`RefreshPairList`）与 runtime（`RefreshPairListWithRuntimeDeps`）各有一份逻辑，修改时两处都要改。

### 注意事项与易踩坑

- `exg.Default` 只由 legacy `Setup` 设置。`NewForRuntime` 与 `GetWith` 的实例不会写入 `Default`；但 `exg.GetLeverage`、`exg.GetOdBook` 等仍直接读 `Default`（见文末“已知限制与设计取舍”）。
- `GetWith` 的缓存 key 不含 env 与账户，同 key 复用实例时会调用 `SetMarketType`。
- `precNum` 对合约市场的 amount 会按 `ContractSize` 换算；新增交易所时要确认 banexg 返回的 `ContractSize` 语义。
- `BotExchange.CreateOrder` 对成功与失败都会触发回调，回调收到的 `Err` 可能非空。回调返回的错误会覆盖原错误，但 `order` 仍返回。
- 过滤器对象在 runtime 下每次刷新都会重新构造（`GetPairFiltersWithConfig`），legacy 全局过滤器则跨刷新复用（`CorrelationFilter` 的 TopN 已改用局部变量，不再写回字段）。
- `OffsetFilter` / `ShuffleFilter` 已改为先复制入参再处理，不会修改调用方切片。
- 过滤器的 `AllowEmpty` 来源于 `PairMgr.Cron`（经 `GetPairFiltersWithConfig` 传入），改动定时刷新配置会改变过滤器的空数据行为。
- 对齐偏移只对 `tfSecs >= 86400` 生效，日内周期永远为 0。
- 测试：`exg/symbol_test.go`、`exg/alignment_test.go`、`exg/order_events_test.go`、`goods/biz_test.go` 覆盖主要路径；过滤器的 `SpreadFilter` 与 `cache_secs` 没有行为测试。

## 已知限制与设计取舍

- `SpreadFilter` 未实现，直接放行输入；`VolumePairFilter`/`RateOfChangeFilter` 的 `cache_secs` 字段不生效（`doc/config.yml` 已注明）。
- `CorrelationFilter` 仅设置 `min` 不会生效，只有 `max`、`top_n`、`top_rate` 至少一个非 0 才会执行。
- `VolumePairFilter` 无成交量数据时返回错误（快速失败），而非空列表。
- `RefreshPairListWithSymbolState` 对非空 state 返回 `ErrBadConfig`：显式 state 必须走 runtime 完整依赖路径，避免静默回落到全局。
- `exg` 的名字空间方法依赖 `Default` 已由启动流程初始化。
- banexg v0.2.66 未实现归档 URL 与账户导出，`BuildArchiveURLForExchange` 恒返回 `CodeNotSupport`，除非预设 `WsSymbol.ArchiveURL`。
- banexg 的 `china` 适配器行情/下单/查询接口均标记为不支持，实盘不可用；`doc/cn_markets` 描述的 CTP、主力连续合约（888/88）与拼接复权在仓库中没有实现。
- 最小下单额按 `Limits.Amount.Min * Price` 计算，单位语义取决于 banexg 对 `Limits.Amount` 的归一化。
- `config` 查询符号规则时每次新建并关闭一个交易所实例，仅在配置解析阶段调用。
