# banbot 自定义时序数据接入方案（当前实现）

## 1. 文档目的

本文描述 **当前代码已经落地的自定义时序数据架构**，用于说明：

- 框架内部统一数据模型是什么
- 自定义数据如何注册、落库、回放、进入策略
- 旧版 Kline 策略接口还保留到什么边界
- TimescaleDB / QuestDB / 回测链路当前如何验证

本文只保留当前实现结论，不再保留历史讨论过程与旧版候选方案。

---

## 2. 快速接入入口

banbot 推荐两种接入方式：

- 数据天然和 K 线一一对齐：使用 `orm.KLineSeriesStore` 写入已有 `kline_<timeframe>` 表的扩展列。
- 数据有自己的发布频率或字段结构：使用 `orm.SeriesStore` 创建独立时序表，并注册为 `data.DataSource`。

### 2.1 写入已有 kline 表扩展列

适用场景：

- 每条记录对应某个已存在的 K 线时间戳。
- 不希望新增一张表，只想把字段挂在 `kline_1h` / `kline_1d` 等表上。
- 数据用于研究、图表或策略计算，并希望和对应 K 线在同一个 `DataSeries` 事件中消费。

外部示例见 `../banstrats/openinterest/store.go`：`Info` 使用 `orm.NewKLineSeriesInfo(...)`，写入和读取通过 `orm.NewKLineSeriesStore(Info).Write/Read(...)` 完成。

`KLineSeriesStore` 会做这些事：

- `Ensure` / `Write` 时自动给目标表增加扩展列。
- TimescaleDB 的 kline 表使用 `time`，QuestDB 的 kline 表使用 `ts`，`NewKLineSeriesInfo` 会按后端选择。
- 按 `(sid, time)` 更新已有行；如果目标行不存在，会返回错误，避免静默插入一条不完整 OHLCV。
- QuestDB 写入后会等待目标行可见，避免 WAL read-after-write 立即读取为空。

注意：

- kline 扩展列名不能和内置列冲突：`sid`、`time`、`ts`、`open`、`high`、`low`、`close`、`volume`、`quote`、`buy_volume`、`trade_num`。
- 未指定字段的旧 `GetOHLCV` / K 线订阅仍只读取默认字段，保持兼容。
- 策略可在 `OnDataSubs` 的 `DataSub.Fields` 中声明扩展列；运行时会把相同 `(source, sid, timeframe)` 的字段合并后一次查询，完整结果通过 `OnData` 和 `DataHub` 提供。
- `OnBar` 只消费框架支持的 OHLCV 字段，扩展字段不会改变指标环境的结构。

### 2.2 独立时序表 + 函数式数据源

适用场景：

- 数据频率独立，例如每 8 小时一条。
- 字段不属于 OHLCV 本体。
- 希望回测和实盘启动时由 banbot 自动补齐、缓存，并通过 `OnData` / `DataHub` 进入策略。

简单数据源示例见 `../banstrats/fundingrate/source.go`：定义 `orm.SeriesInfo`，再用 `data.RegisterFuncDataSource(...)` 注册 `FetchHistory`。

复杂数据源示例见 `../banstrats/longshort/source.go`：把分页、限流、解析校验和可选 live 订阅封装在 `Source` 中，再注册到同一个 `DataSource` 入口。

#### 2.2.1 手动写入和读取

不需要接入策略运行时、只想自己存取时，直接使用 `orm.DefaultSeriesStore().Write/Read(...)`。如果写入的是抽象标的，而不是交易所真实交易对，先用 `orm.EnsureExSymbol(...)` 注册或复用 sid。

#### 2.2.2 策略中订阅

策略订阅示例见 `../banstrats/fundingrate/strategy.go`：在 `OnDataSubs` 返回 `strat.DataSub`，在 `OnData` 中接收嵌入了 `strat.DataFields` 的 `strat.DataEvent`，跨数据源读取使用 `job.DataHub.Get(...)`。

回测和实盘启动时，banbot 会从 `OnDataSubs` 收集订阅，按 `(source, sid, timeframe)` 去重，取最大的 `WarmupNum` 并合并 `Fields` 字段并集，再调用已注册的 `DataSource.FetchHistory` 补齐缺口。

`Fields` 控制数据源读取投影；为空时，kline 使用 `open, high, low, close, volume, quote, buy_volume, trade_num`，独立数据源使用其 `SeriesInfo.Binding.Fields`。`SeriesFields` 控制哪些字段维护为 `banta.Series`；未配置时默认选择 schema 中的 `float*` 字段，运行时无 schema 的数据按 `float32/float64` 值识别。显式 `SeriesFields` 会自动并入 `Fields`。

读取 kline 扩展列时仍使用同一个订阅入口，例如 `DataSub{Source: "kline", SeriesFields: []string{"open_interest"}, ...}`。主 kline 的默认字段会和显式字段合并，因此 `OnData` 可通过 `data.Series("open_interest")` 或 `data.Float64("open_interest")` 读取；未实现 `OnData` 的旧策略仍可通过 `OnBar` 消费 OHLCV。

### 2.3 WebUI / DashboardUI 管理与查看

已注册的任意独立时序数据都可以在两个 UI 中查看：

- 开发端：`数据`页面的 `Series Ranges` 和 `Data Sources` 表都提供 `View` 入口。
- Dashboard：侧栏 `数据`入口提供相同的序列查看器，读取当前连接的 bot API。
- 查看器按 `source + sid + timeframe` 查询，可限定起止时间、最大行数和字段子集；未填写时间时默认读取最近 30 天，最大单次返回 1000 行。
- 返回值保留 `DataSeries` 的时间区间、闭合状态和 `Values`，所以 `float`、`int`、`string`、`bool`、`json` 字段均可直接检查。字段筛选只接受注册 `SeriesInfo.Binding.Fields` 中定义的列。

底层接口为只读 `GET /api/kline/series`：

```text
source=<source>&sid=<sid>&timeframe=<tf>&start=<ms>&end=<ms>&limit=<n>&fields=a,b
```

其中 `source` 为已注册数据源名，`sid` 为 `ExSymbol.ID`。内置 `kline` 也可通过该接口读取默认 OHLCV 字段，或在 `fields` 中声明数值型扩展列。写入、补齐与删除仍应通过 `SeriesStore` / 数据源运行时完成；查看器不执行任何修改。

---

## 3. 当前实现结论

### 3.1 统一运行时事件：`DataSeries`

banbot 内部主链路已经统一改为 `orm.DataSeries`：

- `data.Feeder` / `data.Provider` 回调统一为 `FnDataSeries`
- `biz.Trader` 主入口为 `FeedDataSeries` / `FeedSeries`
- `live.CryptoTrader`、`opt.BackTestLite`、`opt.BackTest` 统一消费 `DataSeries`
- `strat.DataHub` 将 `DataSeries` 处理为按订阅隔离的 `DataFields`

### 3.2 统一仓储名称：`SeriesRepo`

仓储接口规范名称已切换为 `SeriesRepo`：

```go
type SeriesRepo interface {
    EnsureSeriesTable(ctx context.Context, info *SeriesInfo) *errs.Error
    InsertSeriesBatch(ctx context.Context, info *SeriesInfo, rows []*DataRecord) *errs.Error
    QuerySeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*DataRecord, *errs.Error)
    DeleteSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error
    UpdateSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error
    UpdateSeriesCoverage(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, rows []*DataRecord) *errs.Error
    GetSeriesRange(ctx context.Context, info *SeriesInfo, sid int32) (int64, int64, *errs.Error)
}
```

业务代码优先使用更高层的 `orm.SeriesStore`：

```go
store := orm.DefaultSeriesStore()
target, err := orm.EnsureExSymbol("custom", "funding", "BTCUSDT", "binance")
info := orm.NewSeriesInfo("funding_rate", "1h", []orm.SeriesField{
    {Name: "rate", Type: "float", Role: "value"},
})

_ = store.Write(ctx, info, target, &orm.DataRecord{
    TimeMS: ts,
    EndMS:  ts + 3600_000,
    Values: map[string]any{"rate": 0.0001},
})
_ = store.WriteSeries(ctx, info, target, &orm.DataSeries{
    Source:    info.Name,
    TimeMS:    ts,
    EndMS:     ts + 3600_000,
    TimeFrame: info.TimeFrame,
    Values:    map[string]any{"rate": 0.0001},
})
rows, _ := store.Read(ctx, info, target, startMS, endMS, 500)
missing, _ := store.Missing(ctx, info, target, startMS, endMS)
_ = store.Delete(ctx, info, target, startMS, endMS)
```

`SeriesStore` 统一处理：

- 单条写入：`Write`
- 批量写入：`WriteBatch`
- 运行时事件写入：`WriteSeries` / `WriteSeriesBatch`
- 读取并转成运行时事件：`Read`
- 删除：`Delete`
- 缺口查询：`Missing`
- 缺口补齐：`FillMissing`
- 覆盖范围查询：`Coverage`
- `Sid=0` 行自动补成目标 `ExSymbol.ID`
- 写入后通过 `SeriesRepo.UpdateSeriesCoverage(...)` 更新覆盖范围

### 3.3 `ExSymbol` / source 身份只由 `exchange + market + symbol` 确定

当前实现中，`ExSymbol` 的 sid 解析、缓存 key、查询接口均以三元组为准：

- `exchange`
- `market`
- `symbol`

`exg_real` 继续保留在 `ExSymbol` 中作为来源元信息，但 **不再参与 sid 身份判定**，也不参与 `GetExSymbol2` 查询命中。

因此当前规则是：

> 一个 source / ExSymbol 由 `exchange + market + symbol` 唯一确定；`exg_real` 只是附带元数据，不再是身份维度。

自定义/抽象标的通过 `orm.EnsureExSymbol(exchange, market, symbol, exgReal...)` 创建或复用：

```go
macroTarget, err := orm.EnsureExSymbol("custom", "macro", "CPI_US", "fred")
```

它和交易所交易对共用同一个 `ExSymbol`/sid 注册表，因此资金费率、持仓、宏观指标、用户自定义指标都可以挂到统一 sid 上。

### 3.4 `SeriesBinding.SIDColumn` 可留空，默认 `sid`

`SeriesBinding` 已支持省略 `SIDColumn`，内部归一化时默认补成 `sid`：

```go
func normalizedSeriesBinding(binding SeriesBinding) SeriesBinding {
    if binding.SIDColumn == "" {
        binding.SIDColumn = "sid"
    }
    return binding
}
```

因此自定义数据只要表结构遵循默认 sid 列，即可不显式填写 `SIDColumn`。

### 3.5 Kline 已降级为内置适配能力，不再是内核唯一数据本体

框架内部不再要求所有事件都必须先变成 `InfoKline` 才能流转。

当前做法是：

- 主事件是 `DataSeries`
- OHLCV 数据通过 `NewDataSeriesFromKline(...)` 进入统一总线
- 只有在兼容旧策略 / 旧订单处理时，才通过 `AsKline(...)` 临时投影成 `InfoKline`

也就是说：

> `Kline` 现在是内置 OHLCV 适配层，而不是统一数据抽象。

### 3.6 策略兼容边界已收敛到 `TradeStrat` / `StratJob`

当前策略层同时支持新旧两套接口，但旧接口只保留在策略兼容层：

#### 新接口

- `OnData`
- `OnWsData`
- `OnDataSubs`
- `DataHub`

#### 兼容旧接口

- `OnBar`
- `OnInfoBar`
- `OnWsKline`
- `OnPairInfos`

内部主链路的新实现统一围绕 `DataSeries`；旧 Kline 接口只用于兼容未迁移完成的策略代码。

---

## 4. 当前核心数据模型

### 4.1 描述数据表结构：`SeriesInfo` / `SeriesBinding`

```go
type SeriesField struct {
    Name string
    Type string
    Role string
}

type SeriesBinding struct {
    Table      string
    TimeColumn string
    EndColumn  string
    SIDColumn  string
    Fields     []SeriesField
}

type SeriesInfo struct {
    Name      string
    TimeFrame string
    Binding   SeriesBinding
}
```

说明：

- `Name`：数据源名称
- `TimeFrame`：该 source 的基础周期
- `Binding`：表名、时间列、sid 列、字段列定义
- `Fields`：声明列名、类型、语义角色
- `orm.NewSeriesInfo(name, timeframe, fields)` 会按 `name + "_" + timeframe` 生成默认表名，例如 `funding_rate_1h`，并默认使用 `ts` / `end_ms` / `sid`

当前支持的字段类型：

- `float`
- `int`
- `string`
- `bool`
- `json`

### 4.2 列级聚合规则：`ExSymbol.AggRules`

`ExSymbol` 现在带有 `agg_rules` 文本列，两套后端均支持：

- TimescaleDB / PostgreSQL：`exsymbol.agg_rules`
- QuestDB：`exsymbol_q.agg_rules`

字段内容是 JSON 对象，key 为列名，value 为聚合规则：

```json
{"open":"first","high":"max","low":"min","close":"last","volume":"sum","rate":"avg"}
```

支持规则：`min`、`max`、`last`、`first`、`sum`、`avg`、`mid`，也可以通过 `orm.RegisterAggRule(name, fn)` 注册自定义聚合方法后在 JSON 中使用。未配置、未注册或非法值默认按 `last` 处理。

K 线不再有特殊 `info` 字段，也没有按市场分支的 `InfoBy()` 兼容层。内置 OHLCV 字段仍按固定 K 线语义聚合；自定义扩展列按 `AggRule(field)` 读取列级规则。

### 4.3 存储行：`DataRecord`

```go
type DataRecord struct {
    Sid    int32
    TimeMS int64
    EndMS  int64
    Closed bool
    Values map[string]any
}
```

说明：

- `Sid` 是最终存储主键的一部分
- `TimeMS` / `EndMS` 表示该记录覆盖区间
- `Values` 按 `SeriesBinding.Fields` 写入固定列

`EnsureSeriesRange(...)` 会自动把 `Sid=0` 的记录补成目标 `sub.ExSymbol.ID`，但最终入库前 sid 必须确定。

### 4.4 运行时事件：`DataSeries`

```go
type DataSeries struct {
    Source    string
    Sid       int32
    TimeMS    int64
    EndMS     int64
    TimeFrame string
    Closed    bool
    IsWarmUp  bool
    Values    map[string]any
    ExSymbol  *ExSymbol
    Adj       *AdjInfo
}
```

说明：

- `Source` 统一用来区分 `kline` / `macro_xxx` / `open_interest` 等来源
- `Sid` 是运行时检索与分发的主键
- `ExSymbol` 在 kline/交易路径下可直接带上标的信息
- `Adj` 仅对内置 OHLCV 适配链路有意义

### 4.5 OHLCV 兼容适配：`AsKline`

当 `Values` 至少包含以下字段时，可被兼容投影成 `InfoKline`：

- `open`
- `high`
- `low`
- `close`
- `volume`

可选字段：

- `quote`
- `buy_volume`
- `trade_num`

这使得一条 `DataSeries` 既能作为通用数据进入 `OnData`，也能在需要时落到旧版 `OnBar` / `OnInfoBar` / 订单撮合逻辑。

---

## 5. 自定义数据源与存储

### 5.1 数据源接口

```go
type DataSource interface {
    Info() *orm.SeriesInfo
    FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)
    SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink DataSink) error
}
```

注册入口：

```go
func RegisterDataSource(src DataSource) error
```

注册时会校验：

- `SeriesInfo` 合法
- `Name` 非空
- source 名称不能重复

### 5.2 数据订阅模型：`DataSub`

```go
type DataSub struct {
    Source    string
    ExSymbol  *orm.ExSymbol
    TimeFrame string
    WarmupNum int
    Fields    []string
}
```

说明：

- `Source`：数据源名；空值会归一化为 `kline`
- `ExSymbol`：所有订阅最终都必须挂到一个 sid 上
- `TimeFrame`：订阅周期
- `WarmupNum`：回测 / 实盘初始化预热量

### 5.3 历史补齐：`EnsureSeriesRange`

统一补齐流程：

1. 读取 `src.Info()`
2. 校验 `sub.Source` / `sub.TimeFrame` 与 source 定义一致
3. 通过 `SeriesStore.Missing(...)` / `SeriesRangeRepo.MissingSeriesRanges(...)` 计算缺口
4. 调 `FetchHistory(...)`
5. 归一化 `DataRecord`
6. 通过 `SeriesStore.WriteBatch(...)` 入库
7. 通过 `SeriesStore.UpdateCoverage(...)` 记录空洞或覆盖范围

`data.EnsureRuntimeSeriesRange(...)` / `data.EnsureSeriesSubsRange(...)` 是当前中性的运行时补齐入口；旧的
`EnsureThirdPartySeriesRange(...)` / `EnsureThirdPartySeriesSubsRange(...)` 仍保留为兼容 alias。

也可以在回测或参数优化前显式补齐已注册的数据源：

```bash
./bot series list
./bot series down -pairs BTC/USDT,ETH/USDT -tables funding_rate \
  -timestart 2025-01-01 -timeend 2026-01-01
```

`series list` 输出包含 source 名称、周期、物理表和字段定义的 JSON。`series down` 中的 `-tables`
按 source 名称筛选；不传时补齐全部已注册 source。下载结束时间会按各 source 的周期截断到最后一个
已闭合区间，QuestDB 下还会等待 `sranges_q` 覆盖范围可见后再退出。

### 5.4 `sranges` 仍然复用 `(sid, table, timeframe)`

当前实现没有为 custom data 另建覆盖范围系统，而是继续复用现有 `sranges` 机制：

- `sid`
- `binding.Table`
- `info.TimeFrame`

这意味着自定义时序和内置 OHLCV 走同一套覆盖范围管理。

覆盖范围更新已经归入 `SeriesStore` / `SeriesRepo`，调用者不再需要在写入后直接拼装 `sranges` 更新逻辑。

### 5.5 运行时管理：`SeriesRuntime` / `SeriesPlan`

回测和实盘启动时不再各自拼装自定义数据补齐逻辑，而是统一由 `data.SeriesRuntime` 管理：

- `NewSeriesPlan(...)` 从已加载的 `StratJob` 收集非 `kline` 的 `DataSub`
- 按 `(source, sid, timeframe)` 去重
- 重复订阅保留最大的 `WarmupNum`
- 根据最大预热需求计算统一 `StartMS`
- 回测调用 `SeriesRuntime.Ensure(...)`，只补齐历史
- 实盘调用 `SeriesRuntime.SyncLive(...)`，先补历史，再通过 `ActivateDataSources(...)` 激活实时源

实盘定时刷新交易对后会重新生成计划并补齐新增订阅；`SeriesRuntime` 会记住已激活的 `(source, sid, timeframe)`，避免重复订阅。旧的 `ThirdPartySeriesBootstrap` 名称保留为 `SeriesPlan` 的兼容 alias。

### 5.6 TimescaleDB / QuestDB 的字段映射

当前 `SeriesRepo` 两端统一支持：

| 逻辑类型 | TimescaleDB | QuestDB |
|---|---|---|
| `float` | `DOUBLE PRECISION` | `DOUBLE` |
| `int` | `BIGINT` | `LONG` |
| `string` | `TEXT` | `STRING` |
| `bool` | `BOOLEAN` | `BOOLEAN` |
| `json` | `JSONB` | `STRING` |

说明：

- TimescaleDB 中 `json` 会落 `JSONB`
- QuestDB 中 `json` 第一阶段按字符串保存
- QuestDB 建表使用 `timestamp(...) ... WAL DEDUP UPSERT KEYS(sid, time)`；分区粒度按 timeframe 选择（如 `1m` 用 `WEEK`，`1h` / `1d` 用 `YEAR`，其他默认 `MONTH`）
- PostgreSQL/TimescaleDB 使用 `(sid, time)` 主键 upsert

### 5.7 QuestDB 删除语义：先 `sranges`，超过阈值再物理整理

QuestDB 自定义时序删除不是“永远逻辑删除”，也不是每次删除都立即重写表。

当前规则必须保持为混合删除：

1. 删除请求先写入 `sranges`，把目标 `(sid, table, timeframe, start, stop)` 范围标记为 `has_data=false`。
2. 读取路径必须按 `sranges` 覆盖范围过滤，因此旧 WAL 行即使还留在物理表里，也不会再作为有效数据返回。
3. 当本次删除命中的物理行数占该 sid 在该表总物理行数的比例达到阈值时，才触发物理整理。
4. 物理整理通过创建新表并复制仍然有效的数据来回收空间；新表行数验证通过后，才进行表替换。

当前阈值是 `50%`（`seriesQuestRewriteDeleteRatio = 0.5`）。

重点约束：

- 不要把 QuestDB 自定义时序删除改成始终只依赖 `sranges` 的逻辑删除。
- 不要在低删除比例时频繁创建新表、复制数据、替换表。
- 不要在新表未验证之前删除或替换旧表。
- `sranges` 是删除语义的第一来源；物理整理只是达到阈值后的空间回收手段。

---

## 6. 运行时主链路

### 6.1 Built-in OHLCV 也先转成 `DataSeries`

交易所 Kline 当前仍然是系统内置 source，但进入主链路时已经先转换成 `DataSeries`：

- `orm.NewDataSeriesFromKline(...)`
- `orm.NewDataSeriesFromInfoKline(...)`
- `orm.KlineToDataSeries(...)`

因此无论来源是内置 kline 还是第三方数据，进入 `Trader` 之后都先按统一 `DataSeries` 处理。

### 6.2 `Trader` 的分发规则

`biz.Trader.FeedDataSeries(...)` 当前逻辑：

1. 先尝试 `AsKline(evt)`
2. 若不能转成 bar，则走 `feedDataOnlySeries(...)`
3. 若能转成 bar，则走 `feedClosedSeries(...)`

因此：

- **纯通用数据**：进入 `OnData` + `DataHub`
- **bar 形态数据**：既可进入 `OnData`，也可继续兼容旧 `OnBar` / `OnInfoBar`

### 6.3 新旧策略回调优先级

当前实现遵循：

#### 闭合主序列 / 闭合 side-input

- 若策略实现了 `OnData`，优先调用 `OnData`
- 否则若该事件可 `AsKline`，再回落到 `OnBar` / `OnInfoBar`

#### websocket 数据

- 若策略实现了 `OnWsData`，优先调用 `OnWsData`
- 否则仍可回落到 `OnWsKline` / `OnWsTrades` / `OnWsDepth`

也就是说：

> 新接口优先，旧接口只做兼容兜底。

### 6.4 `DataHub` 作为统一运行时缓存

`StratJob.DataHub` 统一缓存按 `(timeframe, source, sid)` 索引的处理后字段：

```go
type DataHub struct {
    // 内部按 timeframe -> source -> sid 保存 *DataFields
}

func (d *DataHub) Get(tf, source string, sid int32) *DataFields
func (d *DataHub) AllReady() bool

type DataFields struct {
    DoneMS    int64
    TimeMS    int64
    Source    string
    Sid       int32
    TimeFrame string
    Closed    bool
    IsWarmUp  bool
}

func (d *DataFields) Series(name string) *banta.Series
func (d *DataFields) Float64(name string) float64
func (d *DataFields) Int64(name string) int64
func (d *DataFields) Raw(name string) any
```

`AllReady()` 使用当前 Hub 已处理事件的最大 `EndMS` 作为事件时间，只检查在该时间点应当闭合的周期。例如 16:05 会要求 1m 和 5m 的全部订阅 `DoneMS >= 16:05`，不会要求尚未闭合的 15m。订阅会在首个事件前预注册，因此尚未收到过的数据源不会被误判为已就绪。

---

## 7. 策略层使用规范

### 7.1 新策略应优先使用的新接口

推荐使用：

- `OnData`
- `OnWsData`
- `OnDataSubs`
- `job.DataHub`

示例：

```go
func init() {
    strat.AddStrat(&strat.TradeStrat{
        Name: "macro_demo",
        OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
            return []*strat.DataSub{
                {
                    Source:    "macro_cpi",
                    ExSymbol:  job.Symbol,
                    TimeFrame: "1d",
                    WarmupNum: 30,
                    Fields:       []string{"value", "revision"},
                    SeriesFields: []string{"value"},
                },
            }
        },
        OnData: strat.RouteData(strat.DataHandlers{
            Custom: func(job *strat.StratJob, data strat.DataEvent) {
                if data.Source != "macro_cpi" {
                    return
                }
                value := data.Series("value")
                revision := data.Int64("revision")
                if !job.DataHub.AllReady() {
                    return
                }
                latest := job.DataHub.Get("1d", "macro_cpi", data.Sid)
                _, _, _ = value, revision, latest
            },
        }),
    })
}
```

`DataEvent` 嵌入了 `*DataFields`，因此字段读取方式不变；同时提供 `Role`、`Symbol`、`IsMain()` 和 `IsKline()`。没有辅助或自定义订阅的策略可直接赋值 `OnData`，无需包装。需要过滤或分别处理主周期、辅助 K 线和自定义数据时，使用：

```go
OnData: strat.RouteData(strat.DataHandlers{
    Main: func(job *strat.StratJob, data strat.DataEvent) {
        // 原 OnBar 逻辑
    },
    Info: func(job *strat.StratJob, data strat.DataEvent) {
        // 原 OnInfoBar 逻辑
    },
    Custom: func(job *strat.StratJob, data strat.DataEvent) {
        // 自定义时序逻辑
    },
})
```

### 7.2 旧策略兼容边界

以下接口保留，仅用于兼容老策略：

- `OnBar`
- `OnInfoBar`
- `OnWsKline`
- `OnPairInfos`

其中：

- `OnPairInfos` 会在内部桥接为 `DataSub{Source: "kline", ...}`
- `OnInfoBar` 仅在 side-input 能适配成 kline 且策略未实现 `OnData` 时触发
- `OnBar` 仅在主 kline 事件且策略未实现 `OnData` 时触发；同一事件不会同时调用两个回调
- `OnData` 不能与 `OnBar` 或 `OnInfoBar` 同时配置；策略构建会直接报错，请把旧回调逻辑迁移到 `RouteData` 的 `Main` / `Info` 处理器

### 7.3 新代码不要再把 Kline 当作通用自定义数据契约

当前规范是：

- 通用数据 -> `DataSeries`
- OHLCV 兼容 -> `AsKline`
- 旧策略接口 -> 仅兼容层使用

不要再新增以固定 `kline` 字段命名的通用数据接口、存储契约或回调。

---

## 8. 回测与实盘说明

### 8.1 回测链路

`BackTestLite` / `BackTest` 已统一改为消费 `DataSeries`：

- `FeedDataSeries(evt *orm.DataSeries)` 成为回测主入口
- 对可 `AsKline` 的事件，继续执行原有撮合、账户、报表逻辑
- 对纯通用数据，直接进入 `Trader` 的 `OnData` 分发链路
- 独立时序表通过通用历史 feeder 读取，并和 kline feeder 一起按事件结束时间排序回放

### 8.2 实盘链路

`CryptoTrader` 已统一改为消费 `DataSeries`：

- 闭合 kline 仍通过内置 OHLCV 适配器进入 `FeedDataSeries`
- 有 kline 扩展字段订阅时，闭合行会按字段并集从数据库回读并合并到 `DataSeries.Values`
- websocket 闭合/未闭合事件优先走 `OnWsData`
- 旧 `OnWsKline` 仅作为兼容回退

### 8.3 策略编译与回测验证位置

策略编译和回测仍应在 `../banstrats` 侧完成，以确保：

- 策略代码和 banbot 接口同步编译
- 兼容层（`OnBar` / `OnInfoBar` / `OnPairInfos`）与新接口同时得到验证
- 回测报表对比基于真实策略项目，而不是只跑 banbot 单仓库单元测试

---

## 9. 当前测试与回归关注点

### 9.1 已覆盖的关键回归点

当前仓库中已有针对这次重构的关键测试：

#### `orm`

- `TestGetExSymbol2UsesThreeFieldIdentity`
- `TestEnsureSymbolsReusesIdentityAcrossExgReal`
- `TestValidateSeriesInfoDefaultsSIDColumn`
- `TestSeriesRepoTimescaleRoundTrip`
- `TestSeriesRepoQuestDBRoundTrip`

#### `data`

- `TestRegisterDataSourceRejectsDuplicates`
- `TestEnsureSeriesRangeTimescale`
- `TestEnsureSeriesRangeQuestDB`
- `TestHistSeriesFeederJoinsUnifiedTimeline`

#### `web/base`

- `TestResolveSeriesQueryInfoProjectsRegisteredFields`
- `TestResolveSeriesQueryInfoBuildsKlineMetadata`

#### `biz`

- `TestOHLCVSeriesUsesOnDataWithMainRole`
- `TestNonKlineOHLCVShapeTriggersOnlyOnData`
- `TestFeedSeriesRoutesNonKlineDataSubs`
- `TestFeedSeriesFallsBackToOnInfoBarForLegacyKlineSubs`

#### `strat`

- `TestDataHubBuildsConfiguredAndDefaultSeries`
- `TestDataHubAllReadyOnlyChecksClosingTimeframes`
- `TestCollectDataSubsBridgesLegacyPairInfos`
- `TestUpdatePairs_RebuildsWarmsFromCurrentDataSubs`

### 9.2 推荐回归顺序

每次继续扩展 custom data 时，至少按下面顺序回归：

1. 在 `banstrats` 仓库根目录执行 `bash scripts/verify_third_party_regression.sh`
2. 如需单独查看某一层失败，再按脚本打印出的原始 `go test` 命令逐层重跑
3. 针对 TimescaleDB / QuestDB 的真实 round-trip，只在需要数据库验证时单独执行对应测试
4. 进入 `../banstrats` 完成策略编译
5. 用同一组配置分别跑基线回测与改造后回测
6. 对比订单、资金曲线、报表摘要是否一致

这份脚本是当前第三方时序接入回归的**规范入口**：默认只跑 hermetic proof layers，不依赖 `BANBOT_TEST_DATA_SERVER`、`BANBOT_TEST_STRAT_RUN` 或外部实时网络。脚本会在每一层开始前打印层名和完整命令，并在首个失败处停止，便于直接定位是示例注册/抓取/策略行为、框架启动激活链路，还是 legacy OHLCV/DataHub 兼容语义发生了回归。

### 9.3 回归目标

重点确认：

- 自定义数据不会破坏原有 OHLCV 回测结果
- 老策略只实现 `OnBar` / `OnInfoBar` 时行为不变
- 新策略只实现 `OnData` / `OnDataSubs` 时可独立工作
- TimescaleDB / QuestDB 的 series 落库、查询、coverage 更新一致
- 回测报告与原有策略结果保持一致

---

## 10. 最终规范

1. **统一数据模型使用 `DataSeries`，不是 `Kline` / `InfoKline`。**
2. **统一仓储名称使用 `SeriesRepo`。**
3. **source / ExSymbol 身份只认 `exchange + market + symbol`。**
4. **`exg_real` 仅保留为元信息，不参与 sid 身份判定。**
5. **`SeriesBinding.SIDColumn` 可为空，默认 `sid`。**
6. **内部通用数据链路一律使用 `DataSeries` / `DataRecord` / `SeriesInfo` / `SeriesBinding`。**
7. **旧 Kline 接口只保留在 `TradeStrat` / `StratJob` 的兼容层，不再作为新功能设计基准。**
8. **所有新接入的第三方时序数据都应先注册 source，再通过 `DataSub + ExSymbol.sid` 接入统一主链路。**
9. **管理端和 Dashboard 的明细查看统一使用 `/api/kline/series`；该接口只读，不替代数据源的写入与补齐流程。**
