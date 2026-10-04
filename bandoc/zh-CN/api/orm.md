# orm 包

orm 包提供了数据库访问和数据模型定义的功能。

## Runtime 存储与符号状态

显式运行器将 `orm.Storage` 和 `orm.SymbolState` 注入 Runtime。Storage 是连接与后端能力的显式依赖，SymbolState 保存任务自己的符号索引、订阅和恢复目录；创建共享外部资源的入口负责关闭它们。`Setup`、`Conn` 等包级接口保留给旧调用链，不能用来获取另一个 Runtime 的任务状态。

## 重要结构体

### AdjFactor
价格调整因子结构体，用于处理前复权、后复权等价格调整。

字段：
- `Sid int32` - 交易对ID
- `SubID int32` - 子交易对ID
- `StartMs int64` - 开始时间戳（毫秒）
- `Factor float64` - 调整因子值

### Calendar
交易日历结构体，用于记录交易时间段。

字段：
- `Market string` - 市场或日历名称
- `StartMs int64` - 开始时间戳（毫秒）
- `StopMs int64` - 结束时间戳（毫秒）

### ExSymbol
交易对信息结构体，包含交易所和交易对的基本信息。

字段：
- `ID int32` - 交易对ID
- `Exchange string` - 交易所名称
- `ExgReal string` - 实际交易所标识
- `Market string` - 市场类型
- `Symbol string` - 交易对符号
- `Combined bool` - 是否为组合交易对
- `ListMs int64` - 上市时间戳（毫秒）
- `DelistMs int64` - 退市时间戳（毫秒）
- `AggRules string` - 自定义字段聚合规则 JSON

### InsKline
K线插入任务结构体，用于管理K线数据的插入操作。

字段：
- `Sid int32` - 交易对ID
- `Timeframe string` - 时间周期
- `Ts time.Time` - 任务创建时间
- `StartMs int64` - 开始时间戳（毫秒）
- `StopMs int64` - 结束时间戳（毫秒）

### SRange
时序数据覆盖范围结构体，用于记录 K 线和自定义序列的已覆盖或确认无数据区间。

字段：
- `Sid int32` - 交易对ID
- `Table string` - 物理时序表名
- `Timeframe string` - 时间周期
- `StartMs int64` - 开始时间戳（毫秒）
- `StopMs int64` - 结束时间戳（毫秒）
- `HasData bool` - 此区间是否包含有效数据

### KlineUn
未完成 K 线数据结构体，包含尚未闭合的原始数据。

字段：
- `Sid int32` - 交易对ID
- `StartMs int64` - 开始时间戳（毫秒）
- `StopMs int64` - 结束时间戳（毫秒）
- `ExpireMs int64` - 未完成 K 线过期时间戳（毫秒）
- `Timeframe string` - 时间周期
- `Open float64` - 开盘价
- `High float64` - 最高价
- `Low float64` - 最低价
- `Close float64` - 收盘价
- `Volume float64` - 成交量
- `Quote float64` - 成交额
- `BuyVolume float64` - 主动买入成交量
- `TradeNum int64` - 成交笔数

### InfoKline
带有附加信息的K线数据结构体。

字段：
- `PairTFKline *banexg.PairTFKline` - 基础K线数据
- `Sid int32` - 交易对ID
- `Adj *AdjInfo` - 价格调整信息
- `IsWarmUp bool` - 是否为预热数据

### AdjInfo
价格调整信息结构体，包含复权相关的详细信息。

字段：
- `ExSymbol *ExSymbol` - 交易对信息
- `Factor float64` - 原始相邻复权因子
- `CumFactor float64` - 累计复权因子
- `StartMS int64` - 开始时间戳（毫秒）
- `StopMS int64` - 结束时间戳（毫秒）

### KlineAgg
K线数据聚合配置结构体，用于管理不同时间周期的K线聚合。

字段：
- `TimeFrame string` - 时间周期
- `MSecs int64` - 周期毫秒数
- `Table string` - 数据表名
- `AggFrom string` - 聚合来源
- `AggStart string` - 聚合开始时间
- `AggEnd string` - 聚合结束时间
- `AggEvery string` - 聚合间隔
- `CpsBefore string` - 补全截止时间
- `Retention string` - 数据保留时间

### SeriesInfo、DataRecord 与 DataSeries

`SeriesInfo{Name, TimeFrame, Binding}` 描述 source、周期和物理表；`SeriesBinding` 包含 `Table/TimeColumn/EndColumn/SIDColumn/Fields`，省略 SIDColumn 时为 `sid`。`NewSeriesInfo` 默认使用 `ts/end_ms/sid`，生成 `name_timeframe` 表名；字段逻辑类型为 `float/int/string/bool/json`。

```go
type DataRecord struct {
    Sid int32
    TimeMS, EndMS int64
    Closed bool
    Values map[string]any
}
type DataSeries struct {
    Source string
    Sid int32
    TimeMS, EndMS int64
    TimeFrame string
    Closed, IsWarmUp bool
    Values map[string]any
    ExSymbol *ExSymbol
    Adj *AdjInfo
}
```

DataRecord 是持久行，DataSeries 是回测/实盘事件。时间单位为毫秒，区间为 `[TimeMS,EndMS)`。Values 保留 Go 具体类型和显式 nil；直接 map 查找可区分 NULL 与缺键。存储只写 schema 声明列：`int` 规范化为 int64，`float` 为 float64，JSON 经数据库编码；固定列往返不能恢复原始缺键或所有 Go 类型。

### Subscription

`orm.Subscription` 是引擎无关的数据依赖，`strat.DataSub` 为其 alias。除 Source/ExSymbol/TimeFrame/WarmupNum/Fields/SeriesFields 外，还包含 Frequency（bar/event）和 Projection（default/all/selected）。`NormalizeSubscription` 验证声明；合法 event 声明不代表每个 reader 都能读取它。`StreamKey{Source,SID,TimeFrame}` 不包含 consumer 或账户，隔离由所属 catalog/repository 负责。

### SeriesStore 与 BoundSeriesStore

```go
store := orm.NewSeriesStore(orm.NewSeriesRepo(storage))
rates := store.Bind(info, target)
err := rates.WriteBatch(ctx, rows) // []*orm.DataRecord
events, err := rates.Read(ctx, startMS, endMS, limit) // []*orm.DataSeries
```

示例假定已初始化 Storage、schema、target、ctx 和时间范围。`Bind` 保留定义和标的引用，不执行 I/O。下列方法可在 SeriesStore 上传入 info/target，或在绑定后的 store 上省略它们：

| 方法 | 用途 |
| --- | --- |
| `Ensure` | 确保 schema |
| `Write/WriteBatch` | 写单条/批量持久行 |
| `WriteSeries/WriteSeriesBatch` | 写运行时事件 |
| `Read` | 读取 DataSeries |
| `Missing/FillMissing` | 查缺口并抓取补齐 |
| `Coverage/UpdateCoverage` | 覆盖范围与确认无数据区间 |
| `Delete` | 删除指定时间范围 |

`NormalizeDataRecords` 跳过 nil 行、补齐 Sid=0、拒绝不同 sid 或无效区间并排序。`RecordToSeries` 规范化 source，借用 Values；`RecordsToSeries` 会跳过 nil 行。`SeriesToRecord` 和 `CloneWithExSymbol` 不提供递归所有权复制，异步排队需自行隔离可变输入。`WithSeriesReadByteLimit` 限制解码页预算，不能作为进程 RSS 上限。

### KLineSeriesStore 与 K 线字段读取

独立 SeriesStore 写完整记录。`NewKLineSeriesInfo` / `NewKLineSeriesStoreWithStorage(info, storage)` 定义并写既有 K 线的扩展列，`Write(ctx,target,rows)` 更新已有 `(sid,time)` 行；缺少目标 K 线时失败。不要把扩展标签 Name 当成独立 source，策略订阅仍使用 `Source: "kline"`。

`GetSeries/AutoFetchSeries` 返回 `[]*DataSeries`；显式 Queries 提供 `GetSeriesFields`、`QuerySeriesFields` 和批量字段投影接口。字段读取包含默认 OHLCV 与所需扩展列，后续 feeder/回调保留 Values。下文 GetOHLCV/AutoFetchOHLCV 是默认 K 线兼容视图，不应通过这些固定字段 API 传递任意自定义列。

### 聚合与复权

`ResampleSeriesRecords`、`ResampleDataSeries` 按字段聚合；`ExSymbol.AggRules` 存储 JSON 列规则，`RegisterAggRule` 注册自定义规则。支持 first/last/min/max/sum/avg/mid；未配置的扩展字段使用 last。first/last 保留选中原始值，数值规则进行类型转换，并有各自 NULL/缺字段校验；聚合结果不等于完整复制输入。

`SeriesOHLCV`、`DataSeries.OHLCV` 与 `AsKline` 是局部兼容视图，不能替代 Values。feeder 复权复制字段 map，按当前实现调整 open/high/low/close/volume/buy_volume；其余自定义字段、quote、trade_num 保留原值，不自动套用价格倍率。

### QuestDB WAL

INSERT/CTAS 完成后数据仍可能不可读。依赖新数据的流程必须等待目标行、时间戳、范围或记录数可见；超时保留恢复标记。表替换前先核实替换表快照，不能据单次空查询 DROP 旧表。同进程元数据写后读优先使用定向可见性等待或所属缓存/锁。

详见[自定义时序数据](../guide/custom_data.md)与[数据库](../guide/database.md)。

## 数据库连接相关

### Setup（兼容接口）
初始化数据库连接池。

返回：
- `*errs.Error` - 初始化过程中的错误信息

### Conn（兼容接口）
获取数据库连接和查询对象。

参数：
- `ctx context.Context` - 上下文对象，用于控制请求的生命周期

返回：
- `*Queries` - 数据库查询对象
- `*pgxpool.Conn` - 数据库连接对象
- `*errs.Error` - 错误信息

### SetDbPath
设置数据库路径。

参数：
- `key string` - 数据库标识键
- `path string` - 数据库文件路径

### DbLite
创建 banbot 本地 SQLite 辅助数据库连接。它只用于本地辅助状态；行情和通用时序数据通过 `Setup`/`Conn` 使用配置的 QuestDB 或 TimescaleDB。

参数：
- `src string` - 数据源名称
- `path string` - 数据库文件路径
- `write bool` - 是否可写

返回：
- `*sql.DB` - 数据库连接对象
- `*errs.Error` - 错误信息

### NewDbErr
创建数据库错误对象。

参数：
- `code int` - 错误码
- `err_ error` - 原始错误

返回：
- `*errs.Error` - 格式化的错误信息

## 交易所相关

### LoadMarkets
加载交易所市场数据。

参数：
- `exchange banexg.BanExchange` - 交易所接口
- `reload bool` - 是否强制重新加载

返回：
- `banexg.MarketMap` - 市场数据映射
- `*errs.Error` - 错误信息

### InitExg
初始化交易所配置。

参数：
- `exchange banexg.BanExchange` - 交易所接口

返回：
- `*errs.Error` - 错误信息

## 交易对相关

### GetExSymbols
获取指定交易所和市场的所有交易对信息。

参数：
- `exgName string` - 交易所名称
- `market string` - 市场名称

返回：
- `map[int32]*ExSymbol` - 交易对ID到交易对信息的映射

### GetExSymbolMap
获取指定交易所和市场的所有交易对信息(以交易对名称为键)。

参数：
- `exgName string` - 交易所名称
- `market string` - 市场名称

返回：
- `map[string]*ExSymbol` - 交易对名称到交易对信息的映射

### GetSymbolByID
通过ID获取交易对信息。

参数：
- `id int32` - 交易对ID

返回：
- `*ExSymbol` - 交易对信息

### GetExSymbolCur
获取当前默认交易所的交易对信息。

参数：
- `symbol string` - 交易对名称

返回：
- `*ExSymbol` - 交易对信息
- `*errs.Error` - 错误信息

### GetExSymbol
获取指定交易所的交易对信息。

参数：
- `exchange banexg.BanExchange` - 交易所接口
- `symbol string` - 交易对名称

返回：
- `*ExSymbol` - 交易对信息
- `*errs.Error` - 错误信息

### EnsureExgSymbols
确保交易所的交易对信息已加载。

参数：
- `exchange banexg.BanExchange` - 交易所接口

返回：
- `*errs.Error` - 错误信息

### EnsureCurSymbols
确保当前交易所的指定交易对信息已加载。

参数：
- `symbols []string` - 交易对名称列表

返回：
- `*errs.Error` - 错误信息

### EnsureSymbols
确保指定交易所的交易对信息已加载。

参数：
- `symbols []*ExSymbol` - 交易对信息列表
- `exchanges ...string` - 交易所名称列表

返回：
- `*errs.Error` - 错误信息

### LoadAllExSymbols
加载所有交易对信息。

返回：
- `*errs.Error` - 错误信息

### GetAllExSymbols
获取所有已加载的交易对信息。

返回：
- `map[int32]*ExSymbol` - 交易对ID到交易对信息的映射

### InitListDates
初始化交易对的上市日期信息。

返回：
- `*errs.Error` - 错误信息

### EnsureListDates
确保交易对的上市日期信息已加载。

参数：
- `sess *Queries` - 数据库查询对象
- `exchange banexg.BanExchange` - 交易所接口
- `exsMap map[int32]*ExSymbol` - 交易对映射
- `exsList []*ExSymbol` - 交易对列表

返回：
- `*errs.Error` - 错误信息

### ParseShort
解析简短格式的交易对名称。

参数：
- `exgName string` - 交易所名称
- `short string` - 简短格式的交易对名称

返回：
- `*ExSymbol` - 交易对信息
- `*errs.Error` - 错误信息

### MapExSymbols
将交易对名称列表映射为交易对信息映射。

参数：
- `exchange banexg.BanExchange` - 交易所接口
- `symbols []string` - 交易对名称列表

返回：
- `map[int32]*ExSymbol` - 交易对ID到交易对信息的映射
- `*errs.Error` - 错误信息

## K线数据相关

### AutoFetchOHLCV
自动获取K线数据，支持数据补全和未完成K线。

参数：
- `exchange banexg.BanExchange` - 交易所接口
- `exs *ExSymbol` - 交易对信息
- `timeFrame string` - 时间周期
- `startMS int64` - 开始时间(毫秒)
- `endMS int64` - 结束时间(毫秒)
- `limit int` - 限制数量
- `withUnFinish bool` - 是否包含未完成K线
- `pBar *utils.PrgBar` - 进度条

返回：
- `[]*AdjInfo` - 价格调整信息
- `[]*banexg.Kline` - K线数据
- `*errs.Error` - 错误信息

### GetOHLCV
获取K线数据。

参数：
- `exs *ExSymbol` - 交易对信息
- `timeFrame string` - 时间周期
- `startMS int64` - 开始时间(毫秒)
- `endMS int64` - 结束时间(毫秒)
- `limit int` - 限制数量
- `withUnFinish bool` - 是否包含未完成K线

返回：
- `[]*AdjInfo` - 价格调整信息
- `[]*banexg.Kline` - K线数据
- `*errs.Error` - 错误信息

### BulkDownOHLCV
批量下载K线数据。

参数：
- `exchange banexg.BanExchange` - 交易所接口
- `exsList map[int32]*ExSymbol` - 交易对列表
- `timeFrame string` - 时间周期
- `startMS int64` - 开始时间(毫秒)
- `endMS int64` - 结束时间(毫秒)
- `limit int` - 限制数量
- `prg utils.PrgCB` - 进度回调

返回：
- `*errs.Error` - 错误信息

### FetchApiOHLCV
从交易所API获取K线数据。

参数：
- `ctx context.Context` - 上下文对象
- `exchange banexg.BanExchange` - 交易所接口
- `pair string` - 交易对名称
- `timeFrame string` - 时间周期
- `startMS int64` - 开始时间(毫秒)
- `endMS int64` - 结束时间(毫秒)
- `out chan []*banexg.Kline` - K线数据输出通道

返回：
- `*errs.Error` - 错误信息

### ApplyAdj
应用价格调整因子到K线数据。

参数：
- `adjs []*AdjInfo` - 价格调整信息
- `klines []*banexg.Kline` - K线数据
- `adj int` - 调整类型
- `cutEnd int64` - 截止时间
- `limit int` - 限制数量

返回：
- `[]*banexg.Kline` - 调整后的K线数据

### FastBulkOHLCV
快速批量获取K线数据。

参数：
- `exchange banexg.BanExchange` - 交易所接口
- `symbols []string` - 交易对名称列表
- `timeFrame string` - 时间周期
- `startMS int64` - 开始时间(毫秒)
- `endMS int64` - 结束时间(毫秒)
- `limit int` - 限制数量
- `handler func(string, string, []*banexg.Kline, []*AdjInfo)` - 数据处理回调函数

返回：
- `*errs.Error` - 错误信息

### GetAlignOff
获取K线时间对齐偏移量。

参数：
- `sid int32` - 交易对ID
- `toTfMSecs int64` - 目标时间周期(毫秒)

返回：
- `int64` - 时间偏移量(毫秒)

### NewKlineAgg
创建新的K线聚合配置。

参数：
- `TimeFrame string` - 时间周期
- `Table string` - 数据表名
- `AggFrom string` - 聚合来源
- `AggStart string` - 聚合开始时间
- `AggEnd string` - 聚合结束时间
- `AggEvery string` - 聚合间隔
- `CpsBefore string` - 补全截止时间
- `Retention string` - 数据保留时间

返回：
- `*KlineAgg` - K线聚合配置

### SyncKlineTFs
同步不同时间周期的K线数据。

参数：
- `args *config.CmdArgs` - 命令行参数
- `pb *utils.StagedPrg` - 进度条

返回：
- `*errs.Error` - 错误信息

### CalcAdjFactors
计算价格调整因子。

参数：
- `args *config.CmdArgs` - 命令行参数

返回：
- `*errs.Error` - 错误信息

## 数据导入导出

### ExportKData
导出K线数据。

参数：
- `configFile string` - 配置文件路径
- `outputDir string` - 输出目录
- `numWorkers int` - 工作线程数
- `pb *utils2.StagedPrg` - 进度条

返回：
- `*errs.Error` - 错误信息

### ImportData
导入K线数据。

参数：
- `dataDir string` - 数据目录
- `numWorkers int` - 工作线程数
- `pb *utils2.StagedPrg` - 进度条

返回：
- `*errs.Error` - 错误信息

## 工具函数

### GetDownTF
获取下一级别的时间周期。

参数：
- `timeFrame string` - 时间周期

返回：
- `string` - 下一级别时间周期
- `*errs.Error` - 错误信息

### GetKlineAggs
获取所有K线聚合配置。

返回：
- `[]*KlineAgg` - K线聚合配置列表

## 因子引擎集成

DataSeries.Values 保留任意类型/NULL/缺失；RecordToSeries 转换不代替异步深复制；QuestDB 写后读等待和替换前验证保留。

[因子 API](factor.md) / [指南](../guide/factor.md)
