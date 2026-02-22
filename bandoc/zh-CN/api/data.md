# data 包

data 包提供了数据处理和管理相关的功能。

此package的重要概念如下：
* Provider：K线数据提供者，可包含多个有相同起止时间的Feeder
* Feeder：对应一个品种的数据源，可以包含多个周期的数据
* Spider：实时监听多个交易所数据、存储数据库，并TCP通知订阅方
* Miner：每个交易所+市场对应一个Miner
* KLineWatcher：用于和Spider通信的客户端

## Provider和Feeder
回测和实盘时都需要订阅K线数据，也可能同时运行多个策略，每个策略同时订阅多个时间周期；故提出接口`IProvider`支持回测(HistProvider)和实盘(LiveProvider)。

每个品种的K线数据可能被多个策略同时使用，为避免每个策略重复获取，提出接口`IKlineFeeder`支持回测(DBKlineFeeder)和实盘(KlineFeeder+KLineWatcher)。

每个KlineFeeder对应一个品种，可包含多个周期的数据，比如品种BTC可能被多个策略使用，订阅的周期有5m,1h,1d三个；为避免冗余数据读取，只会对最小周期(这里是5m)获取K线数据；其他更大周期数据会从最小周期聚合得到。

#### Provider和Feeder适合的场景
**起止时间一致的多品种、多周期数据读取**；比如回测或实盘都是对特定一段时间，运行一组策略，涉及一些品种的不同周期数据，建议使用Provider+Feeder。如果需要对多组不同的时间段分别获取数据，应实例化多个Provider+Feeder分开进行。

#### Provider和Feeder不适合的场景
**不同周期数据起止时间不一致**，比如对BTC的1m和1h都希望获得最近1k个K线用于回测或其他任务，如果强制使用Feeder，则1m实际会读取60*1k个K线，不如直接调用`orm.GetOHLCV`逐周期获取，或者分两次初始化Feeder分开读取。


## 重要结构体

### Feeder
每个Feeder对应一个交易对,可包含多个时间维度。

公开字段:
- `ExSymbol *orm.ExSymbol` - 交易所交易对信息
- `States []*PairTFCache` - 各时间维度的缓存状态
- `WaitBar *banexg.Kline` - 等待中的K线数据
- `CallBack FnPairKline` - K线数据回调函数
- `OnEnvEnd FuncEnvEnd` - 环境结束回调函数(期货主力切换或股票除权时需要先平仓)
- `isWarmUp bool` - 当前是否预热状态

### IKlineFeeder
K线数据馈送器接口。

公开方法:
- `getSymbol() string` - 获取交易对名称
- `getWaitBar() *banexg.Kline` - 获取等待中的K线
- `setWaitBar(bar *banexg.Kline)` - 设置等待中的K线
- `SubTfs(timeFrames []string, delOther bool) []string` - 订阅指定时间周期的数据
- `WarmTfs(curMS int64, tfNums map[string]int, pBar *utils.PrgBar) (int64, *errs.Error)` - 预热时间周期
- `onNewBars(barTfMSecs int64, bars []*banexg.Kline) (bool, *errs.Error)` - 处理新K线数据
- `getStates() []*PairTFCache` - 获取缓存状态

### KlineFeeder
每个Feeder对应一个交易对,可包含多个时间维度。实盘使用。

公开字段:
- `Feeder` - 继承自Feeder
- `PreFire float64` - 提前触发bar的比率
- `showLog bool` - 是否显示日志

### IHistKlineFeeder
历史K线数据馈送器接口,继承自IKlineFeeder。

额外公开方法:
- `getNextMS() int64` - 获取下一个bar的结束时间戳
- `DownIfNeed(sess *orm.Queries, exchange banexg.BanExchange, pBar *utils.PrgBar) *errs.Error` - 下载整个范围的K线
- `SetSeek(since int64)` - 设置读取位置
- `GetBar() *banexg.Kline` - 获取当前K线
- `RunBar(bar *banexg.Kline) *errs.Error` - 运行K线对应的回调函数
- `CallNext()` - 移动指针到下一个K线

### HistKLineFeeder
历史数据反馈器,是文件反馈器和数据库反馈器的基类。

公开字段:
- `KlineFeeder` - 继承自KlineFeeder
- `TimeRange *config.TimeTuple` - 时间范围
- `TradeTimes [][2]int64` - 可交易时间

### DBKlineFeeder
数据库读取K线的Feeder,用于回测。

公开字段:
- `HistKLineFeeder` - 继承自HistKLineFeeder
- `offsetMS int64` - 偏移时间戳

### IProvider
数据提供者接口。

公开方法:
- `LoopMain() *errs.Error` - 主循环
- `SubWarmPairs(items map[string]map[string]int, delOther bool) *errs.Error` - 订阅并预热交易对
- `UnSubPairs(pairs ...string) *errs.Error` - 取消订阅交易对
- `SetDirty()` - 设置脏标记

### Provider
数据提供者基类。

公开字段:
- `holders map[string]T` - 持有的Feeder映射
- `newFeeder func(pair string, tfs []string) (T, *errs.Error)` - 创建新Feeder的函数
- `dirtyVers chan int` - 脏版本通道
- `showLog bool` - 是否显示日志

### HistProvider
历史数据提供者。

公开字段:
- `Provider[IHistKlineFeeder]` - 继承自Provider
- `pBar *utils.StagedPrg` - 进度条

### LiveProvider
实时数据提供者。

公开字段:
- `Provider[IKlineFeeder]` - 继承自Provider
- `*KLineWatcher` - K线监视器

### NotifyKLines
K线通知消息。

公开字段:
- `TFSecs int` - 时间周期(秒)
- `Interval int` - 更新间隔(秒)
- `Arr []*banexg.Kline` - K线数组

### KLineMsg
K线消息。

公开字段:
- `ExgName string` - 交易所名称
- `Market string` - 市场类型
- `Pair string` - 交易对
- `TFSecs int` - 时间周期(秒)
- `Interval int` - 更新间隔(秒)
- `Arr []*banexg.Kline` - K线数组

### SaveKline
保存K线的任务。

公开字段:
- `Sid int32` - 交易对ID
- `TimeFrame string` - 时间周期
- `Arr []*banexg.Kline` - K线数组
- `SkipFirst bool` - 是否跳过第一个
- `MsgAction string` - 消息动作

### FetchJob
K线获取任务。

公开字段:
- `PairTFCache` - K线缓存
- `Pair string` - 交易对
- `CheckSecs int` - 检查间隔(秒)
- `Since int64` - 开始时间戳
- `NextRun int64` - 下次运行时间戳

### Miner
数据挖掘器。

公开字段:
- `ExgName string` - 交易所名称
- `Market string` - 市场类型
- `Fetchs map[string]*FetchJob` - 获取任务映射
- `KlineReady bool` - K线是否就绪
- `KlinePairs map[string]bool` - K线交易对映射
- `TradeReady bool` - 交易是否就绪
- `TradePairs map[string]bool` - 交易交易对映射
- `BookReady bool` - 订单簿是否就绪
- `BookPairs map[string]bool` - 订单簿交易对映射
- `IsWatchPrice bool` - 是否监控价格

### LiveSpider
实时数据爬虫。

公开字段:
- `*utils.ServerIO` - 服务器IO
- `miners map[string]*Miner` - 挖掘器映射

### SubKLineState
K线订阅状态。

公开字段:
- `Sid int32` - 交易对ID
- `NextNotify float64` - 下次通知时间
- `PrevBar *banexg.Kline` - 前一个K线

### KLineWatcher
K线监视器。

公开字段:
- `*utils.ClientIO` - 客户端IO
- `jobs map[string]*PairTFCache` - 任务映射
- `OnKLineMsg func(msg *KLineMsg)` - 收到K线消息的回调
- `OnTrade func(exgName, market string, trade *banexg.Trade)` - 收到交易的回调

### WatchJob
监视任务。

公开字段:
- `Symbol string` - 交易对
- `TimeFrame string` - 时间周期
- `Since int64` - 开始时间戳

## K线数据相关

### NewKlineFeeder
创建一个新的K线数据馈送器，用于处理实时K线数据。

参数：
- `exs *orm.ExSymbol` - 交易所交易对信息
- `callBack FnPairKline` - K线数据回调函数
- `showLog bool` - 是否显示日志

返回：
- `*KlineFeeder` - K线馈送器实例
- `*errs.Error` - 错误信息

### NewDBKlineFeeder
创建一个新的数据库K线馈送器，用于从数据库读取历史K线数据。

参数：
- `exs *orm.ExSymbol` - 交易所交易对信息
- `callBack FnPairKline` - K线数据回调函数
- `showLog bool` - 是否显示日志

返回：
- `*DBKlineFeeder` - 数据库K线馈送器实例
- `*errs.Error` - 错误信息

### NewHistProvider
创建一个新的历史数据提供者，用于管理历史K线数据的获取和处理。

参数：
- `callBack FnPairKline` - K线数据回调函数
- `envEnd FuncEnvEnd` - 环境结束回调函数
- `showLog bool` - 是否显示日志
- `pBar *utils.StagedPrg` - 进度条对象

返回：
- `*HistProvider` - 历史数据提供者实例

### RunHistFeeders
运行历史K线馈送器集合，用于批量处理历史数据。

参数：
- `makeFeeders func() []IHistKlineFeeder` - 创建馈送器列表的函数
- `versions chan int` - 版本控制通道
- `pBar *utils.PrgBar` - 进度条对象

返回：
- `*errs.Error` - 错误信息

### SortFeeders
对K线馈送器进行排序或插入操作。

参数：
- `holds []IHistKlineFeeder` - 现有的馈送器列表
- `hold IHistKlineFeeder` - 待处理的馈送器
- `insert bool` - 是否为插入操作

返回：
- `[]IHistKlineFeeder` - 处理后的馈送器列表

### NewLiveProvider
创建一个新的实时数据提供者，用于处理实时K线数据。

参数：
- `callBack FnPairKline` - K线数据回调函数
- `envEnd FuncEnvEnd` - 环境结束回调函数

返回：
- `*LiveProvider` - 实时数据提供者实例
- `*errs.Error` - 错误信息

## 数据工具相关

### FindPathNames
查找指定路径下符合后缀名的所有文件。

参数：
- `inPath string` - 输入路径
- `suffix string` - 文件后缀名

返回：
- `[]string` - 文件路径列表
- `*errs.Error` - 错误信息

### ReadZipCSVs
读取ZIP压缩包中的CSV文件。

参数：
- `inPath string` - ZIP文件路径
- `pBar *utils.PrgBar` - 进度条对象
- `handle FuncReadZipItem` - 处理每个CSV文件的回调函数
- `arg interface{}` - 传递给回调函数的参数

返回：
- `*errs.Error` - 错误信息

### RunSpider
运行数据爬虫服务。

参数：
- `addr string` - 服务监听地址

返回：
- `*errs.Error` - 错误信息

### NewKlineWatcher
创建一个新的K线数据监视器。

参数：
- `addr string` - 连接地址

返回：
- `*KLineWatcher` - K线监视器实例
- `*errs.Error` - 错误信息

### RunFormatTick
运行Tick数据格式化工具。

参数：
- `args *config.CmdArgs` - 命令行参数

返回：
- `*errs.Error` - 错误信息

### Build1mWithTicks
使用Tick数据构建1分钟K线。

参数：
- `args *config.CmdArgs` - 命令行参数

返回：
- `*errs.Error` - 错误信息

### CalcFilePerfs
计算文件性能指标。

参数：
- `args *config.CmdArgs` - 命令行参数

返回：
- `*errs.Error` - 错误信息
