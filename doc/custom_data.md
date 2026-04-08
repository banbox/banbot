# banbot 自定义时序数据接入方案（当前实现）

## 1. 文档目的

本文描述 **当前代码已经落地的自定义时序数据架构**，用于说明：

- 框架内部统一数据模型是什么
- 自定义数据如何注册、落库、回放、进入策略
- 旧版 Kline 策略接口还保留到什么边界
- TimescaleDB / QuestDB / 回测链路当前如何验证

本文只保留当前实现结论，不再保留历史讨论过程与旧版候选方案。

---

## 2. 当前实现结论

### 2.1 统一运行时事件：`DataSeries`

banbot 内部主链路已经统一改为 `orm.DataSeries`：

- `data.Feeder` / `data.Provider` 回调统一为 `FnDataSeries`
- `biz.Trader` 主入口为 `FeedDataSeries` / `FeedSeries`
- `live.CryptoTrader`、`opt.BackTestLite`、`opt.BackTest` 统一消费 `DataSeries`
- `strat.DataHub` 统一缓存 `DataSeries`

### 2.2 统一仓储名称：`SeriesRepo`

仓储接口规范名称已切换为 `SeriesRepo`：

```go
type SeriesRepo interface {
    EnsureSeriesTable(ctx context.Context, info *SeriesInfo) *errs.Error
    InsertSeriesBatch(ctx context.Context, info *SeriesInfo, rows []*DataRecord) *errs.Error
    QuerySeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*DataRecord, *errs.Error)
    UpdateSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error
    GetSeriesRange(ctx context.Context, info *SeriesInfo, sid int32) (int64, int64, *errs.Error)
}
```

### 2.3 `ExSymbol` / source 身份只由 `exchange + market + symbol` 确定

当前实现中，`ExSymbol` 的 sid 解析、缓存 key、查询接口均以三元组为准：

- `exchange`
- `market`
- `symbol`

`exg_real` 继续保留在 `ExSymbol` 中作为来源元信息，但 **不再参与 sid 身份判定**，也不参与 `GetExSymbol2` 查询命中。

因此当前规则是：

> 一个 source / ExSymbol 由 `exchange + market + symbol` 唯一确定；`exg_real` 只是附带元数据，不再是身份维度。

### 2.4 `SeriesBinding.SIDColumn` 可留空，默认 `sid`

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

### 2.5 Kline 已降级为内置适配能力，不再是内核唯一数据本体

框架内部不再要求所有事件都必须先变成 `InfoKline` 才能流转。

当前做法是：

- 主事件是 `DataSeries`
- OHLCV 数据通过 `NewDataSeriesFromKline(...)` 进入统一总线
- 只有在兼容旧策略 / 旧订单处理时，才通过 `AsKline(...)` 临时投影成 `InfoKline`

也就是说：

> `Kline` 现在是内置 OHLCV 适配层，而不是统一数据抽象。

### 2.6 策略兼容边界已收敛到 `TradeStrat` / `StratJob`

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

## 3. 当前核心数据模型

### 3.1 描述数据表结构：`SeriesInfo` / `SeriesBinding`

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

当前支持的字段类型：

- `float`
- `int`
- `string`
- `bool`
- `json`

### 3.2 存储行：`DataRecord`

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

### 3.3 运行时事件：`DataSeries`

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

### 3.4 OHLCV 兼容适配：`AsKline`

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

## 4. 自定义数据源与存储

### 4.1 数据源接口

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

### 4.2 数据订阅模型：`DataSub`

```go
type DataSub struct {
    Source    string
    ExSymbol  *orm.ExSymbol
    TimeFrame string
    WarmupNum int
}
```

说明：

- `Source`：数据源名；空值会归一化为 `kline`
- `ExSymbol`：所有订阅最终都必须挂到一个 sid 上
- `TimeFrame`：订阅周期
- `WarmupNum`：回测 / 实盘初始化预热量

### 4.3 历史补齐：`EnsureSeriesRange`

统一补齐流程：

1. 读取 `src.Info()`
2. 校验 `sub.Source` / `sub.TimeFrame` 与 source 定义一致
3. 通过 `sranges` 计算缺口
4. 调 `FetchHistory(...)`
5. 归一化 `DataRecord`
6. 通过 `SeriesRepo.InsertSeriesBatch(...)` 入库
7. 通过 `UpdateSeriesCoverage(...)` 更新覆盖范围

### 4.4 `sranges` 仍然复用 `(sid, table, timeframe)`

当前实现没有为 custom data 另建覆盖范围系统，而是继续复用现有 `sranges` 机制：

- `sid`
- `binding.Table`
- `info.TimeFrame`

这意味着自定义时序和内置 OHLCV 走同一套覆盖范围管理。

### 4.5 TimescaleDB / QuestDB 的字段映射

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
- QuestDB 建表使用 `timestamp(...) PARTITION BY MONTH WAL DEDUP UPSERT KEYS(sid, time)`
- PostgreSQL/TimescaleDB 使用 `(sid, time)` 主键 upsert

---

## 5. 运行时主链路

### 5.1 Built-in OHLCV 也先转成 `DataSeries`

交易所 Kline 当前仍然是系统内置 source，但进入主链路时已经先转换成 `DataSeries`：

- `orm.NewDataSeriesFromKline(...)`
- `orm.NewDataSeriesFromInfoKline(...)`
- `orm.KlineToDataSeries(...)`

因此无论来源是内置 kline 还是第三方数据，进入 `Trader` 之后都先按统一 `DataSeries` 处理。

### 5.2 `Trader` 的分发规则

`biz.Trader.FeedDataSeries(...)` 当前逻辑：

1. 先尝试 `AsKline(evt)`
2. 若不能转成 bar，则走 `feedDataOnlySeries(...)`
3. 若能转成 bar，则走 `feedClosedSeries(...)`

因此：

- **纯通用数据**：进入 `OnData` + `DataHub`
- **bar 形态数据**：既可进入 `OnData`，也可继续兼容旧 `OnBar` / `OnInfoBar`

### 5.3 新旧策略回调优先级

当前实现遵循：

#### 闭合主序列 / 闭合 side-input

- 若策略实现了 `OnData`，优先调用 `OnData`
- 否则若该事件可 `AsKline`，再回落到 `OnBar` / `OnInfoBar`

#### websocket 数据

- 若策略实现了 `OnWsData`，优先调用 `OnWsData`
- 否则仍可回落到 `OnWsKline` / `OnWsTrades` / `OnWsDepth`

也就是说：

> 新接口优先，旧接口只做兼容兜底。

### 5.4 `DataHub` 作为统一运行时缓存

`StratJob.DataHub` 统一缓存按 `(source, sid, timeframe)` 索引的运行时数据：

```go
type DataHub interface {
    Set(evt *orm.DataSeries)
    Latest(source string, sid int32, tf string) *orm.DataSeries
    Window(source string, sid int32, tf string, n int) []*orm.DataSeries
}
```

这使策略可以直接读取 side-input，而不再依赖 `pair_tf` 这种只适用于 kline 的单一视角。

---

## 6. 策略层使用规范

### 6.1 新策略应优先使用的新接口

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
                },
            }
        },
        OnData: func(job *strat.StratJob, evt *orm.DataSeries) {
            if evt.Source != "macro_cpi" {
                return
            }
            latest := job.DataHub.Latest("macro_cpi", evt.Sid, evt.TimeFrame)
            _ = latest
        },
    })
}
```

### 6.2 旧策略兼容边界

以下接口保留，仅用于兼容老策略：

- `OnBar`
- `OnInfoBar`
- `OnWsKline`
- `OnPairInfos`

其中：

- `OnPairInfos` 会在内部桥接为 `DataSub{Source: "kline", ...}`
- `OnInfoBar` 仅在 side-input 能适配成 kline 且策略未实现 `OnData` 时触发

### 6.3 新代码不要再把 Kline 当作通用自定义数据契约

当前规范是：

- 通用数据 -> `DataSeries`
- OHLCV 兼容 -> `AsKline`
- 旧策略接口 -> 仅兼容层使用

不要再新增以固定 `kline` 字段命名的通用数据接口、存储契约或回调。

---

## 7. 回测与实盘说明

### 7.1 回测链路

`BackTestLite` / `BackTest` 已统一改为消费 `DataSeries`：

- `FeedDataSeries(evt *orm.DataSeries)` 成为回测主入口
- 对可 `AsKline` 的事件，继续执行原有撮合、账户、报表逻辑
- 对纯通用数据，直接进入 `Trader` 的 `OnData` 分发链路

### 7.2 实盘链路

`CryptoTrader` 已统一改为消费 `DataSeries`：

- 闭合 kline 仍通过内置 OHLCV 适配器进入 `FeedDataSeries`
- websocket 闭合/未闭合事件优先走 `OnWsData`
- 旧 `OnWsKline` 仅作为兼容回退

### 7.3 策略编译与回测验证位置

策略编译和回测仍应在 `../banstrats` 侧完成，以确保：

- 策略代码和 banbot 接口同步编译
- 兼容层（`OnBar` / `OnInfoBar` / `OnPairInfos`）与新接口同时得到验证
- 回测报表对比基于真实策略项目，而不是只跑 banbot 单仓库单元测试

---

## 8. 当前测试与回归关注点

### 8.1 已覆盖的关键回归点

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

#### `biz`

- `TestOnDataTakesPrecedenceOverOnBar`
- `TestFeedSeriesRoutesNonKlineDataSubs`
- `TestFeedSeriesFallsBackToOnInfoBarForLegacyKlineSubs`

#### `strat`

- `TestDataHubLatestAndWindow`
- `TestCollectDataSubsBridgesLegacyPairInfos`
- `TestUpdatePairs_RebuildsWarmsFromCurrentDataSubs`

### 8.2 推荐回归顺序

每次继续扩展 custom data 时，至少按下面顺序回归：

1. `go test ./orm ./data ./biz ./strat`
2. 针对 TimescaleDB / QuestDB 分别执行 series round-trip 与 coverage 测试
3. 进入 `../banstrats` 完成策略编译
4. 用同一组配置分别跑基线回测与改造后回测
5. 对比订单、资金曲线、报表摘要是否一致

### 8.3 回归目标

重点确认：

- 自定义数据不会破坏原有 OHLCV 回测结果
- 老策略只实现 `OnBar` / `OnInfoBar` 时行为不变
- 新策略只实现 `OnData` / `OnDataSubs` 时可独立工作
- TimescaleDB / QuestDB 的 series 落库、查询、coverage 更新一致
- 回测报告与原有策略结果保持一致

---

## 9. 最终规范

1. **统一数据模型使用 `DataSeries`，不是 `DataSeries`。**
2. **统一仓储名称使用 `SeriesRepo`，不是 `SeriesRepo`。**
3. **source / ExSymbol 身份只认 `exchange + market + symbol`。**
4. **`exg_real` 仅保留为元信息，不参与 sid 身份判定。**
5. **`SeriesBinding.SIDColumn` 可为空，默认 `sid`。**
6. **内部通用数据链路一律使用 `DataSeries` / `DataRecord` / `SeriesInfo` / `SeriesBinding`。**
7. **旧 Kline 接口只保留在 `TradeStrat` / `StratJob` 的兼容层，不再作为新功能设计基准。**
8. **所有新接入的第三方时序数据都应先注册 source，再通过 `DataSub + ExSymbol.sid` 接入统一主链路。**

