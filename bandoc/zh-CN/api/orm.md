# orm 包

orm 包提供了数据库访问和数据模型定义的功能。

## 重要结构体

### AdjFactor
价格调整因子结构体，用于处理前复权、后复权等价格调整。

字段：
- `ID int32` - 调整因子ID
- `Sid int32` - 交易对ID
- `SubID int32` - 子交易对ID
- `StartMs int64` - 开始时间戳（毫秒）
- `Factor float64` - 调整因子值

### Calendar
交易日历结构体，用于记录交易时间段。

字段：
- `ID int32` - 日历ID
- `Name string` - 日历名称
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

### InsKline
K线插入任务结构体，用于管理K线数据的插入操作。

字段：
- `ID int32` - 任务ID
- `Sid int32` - 交易对ID
- `Timeframe string` - 时间周期
- `StartMs int64` - 开始时间戳（毫秒）
- `StopMs int64` - 结束时间戳（毫秒）

### KHole
K线数据空洞结构体，用于记录K线数据缺失的时间段。

字段：
- `ID int64` - 空洞记录ID
- `Sid int32` - 交易对ID
- `Timeframe string` - 时间周期
- `Start int64` - 开始时间戳（毫秒）
- `Stop int64` - 结束时间戳（毫秒）
- `NoData bool` - 是否确认无数据

### KInfo
K线信息结构体，用于记录K线数据的基本信息。

字段：
- `Sid int32` - 交易对ID
- `Timeframe string` - 时间周期
- `Start int64` - 开始时间戳（毫秒）
- `Stop int64` - 结束时间戳（毫秒）

### KlineUn
未复权K线数据结构体，包含原始K线数据。

字段：
- `Sid int32` - 交易对ID
- `StartMs int64` - 开始时间戳（毫秒）
- `StopMs int64` - 结束时间戳（毫秒）
- `Timeframe string` - 时间周期
- `Open float64` - 开盘价
- `High float64` - 最高价
- `Low float64` - 最低价
- `Close float64` - 收盘价
- `Volume float64` - 成交量
- `Info float64` - 附加信息

### InfoKline
带有附加信息的K线数据结构体。

字段：
- `PairTFKline *banexg.PairTFKline` - 基础K线数据
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

## 数据库连接相关

### Setup
初始化数据库连接池。

返回：
- `*errs.Error` - 初始化过程中的错误信息

### Conn
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
创建SQLite数据库连接。

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
