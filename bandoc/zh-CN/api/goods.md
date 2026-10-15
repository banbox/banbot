# goods 包

goods 包提供了商品和交易对相关的功能。

## Runtime 过滤与品种池

RuntimeDeps 直接持有 Core、Clock、Config、DataDir、Symbols、Storage、Exchange、ShowLog。RuntimeFilter.FilterWithRuntimeDeps 与 RuntimeProducer.GenSymbolsWithRuntimeDeps 是实例扩展；SymbolStateFilter/Producer 和旧 IFilter/IProducer 保留兼容。RefreshPairListWithRuntimeDeps、FilterPairsWithRuntimeDeps 使用任务配置、时钟和 symbol 身份，不能回退读取另一任务的 globals。冻结静态池、强制过滤和排序有各自合同。

## 重要结构体

### IFilter

过滤器接口：GetName() string、IsDisable() bool、Filter(pairs []string, timeMS int64) ([]string, *errs.Error)。已无 IsNeedTickers 方法，行情/配置通过明确的 RuntimeDeps 或实现内部取得。

### IProducer

继承 IFilter，并提供 GenSymbols(timeMS int64) ([]string, *errs.Error)。旧文档的 tickers 参数不是当前签名。

### BaseFilter

公共字段为 Name string、Disable bool、AllowEmpty bool；没有 NeedTickers 字段。

### VolumePairFilter

字段为 BaseFilter、Limit int、LimitRate float64、MinValue float64、CacheSecs int、BackPeriod string。BackPeriod 是时间周期字符串，不能按旧 BackTimeframe/整数乘数配置。

### PriceFilter
价格过滤器配置结构体。
- `MaxUnitValue float64` - 最大允许的单位价格变动对应的价值(针对定价货币，一般是USDT)
- `Precision float64` - 价格精度，默认要求价格变动最小单位是0.1%
- `Min float64` - 最低价格
- `Max float64` - 最高价格

### RateOfChangeFilter

字段为 BaseFilter、BackDays int、Min/Max float64、CacheSecs int；缓存秒数使用 CacheSecs，不是 RefreshPeriod。

### SpreadFilter
流动性过滤器。
- `MaxRatio float32` - 买卖价差占价格的最大比率，公式：1-bid/ask

### CorrelationFilter
相关性过滤器。
- `Min float64` - 最小相关性
- `Max float64` - 最大相关性
- `Timeframe string` - 时间周期
- `BackNum int` - 回溯数量
- `TopN int` - 取前N个
- `Sort string` - 排序方式

### VolatilityFilter
波动率过滤器，使用 StdDev(ln(close / prev_close)) * sqrt(num) 计算。
- `BackDays int` - 回顾的K线天数
- `Max float64` - 波动分数最大值
- `Min float64` - 波动分数最小值

### BlockFilter
品种黑名单过滤器，用于过滤指定品种。
- `Pairs string[]` - 需要过滤的品种

### AgeFilter
上市时间过滤器。
- `Min int` - 最小上市天数
- `Max int` - 最大上市天数

### OffsetFilter
偏移过滤器。
- `Reverse bool` - 是否反转
- `Offset int` - 偏移量
- `Limit int` - 限制数量
- `Rate float64` - 比率

### ShuffleFilter
随机打乱过滤器。
- `Seed int` - 随机种子

### Setup
初始化商品包的配置。主要用于设置交易对过滤器。

返回：
- `*errs.Error` - 初始化过程中的错误信息，如果成功则返回 nil

### GetPairFilters
根据配置创建交易对过滤器列表。

参数：
- `items []*config.CommonPairFilter` - 过滤器配置列表
- `withInvalid bool` - 是否包含无效的过滤器

返回：
- `[]IFilter` - 过滤器接口列表
- `*errs.Error` - 创建过程中的错误信息

### RefreshPairList
刷新交易对列表，获取最新的有效交易对。

返回：
- `[]string` - 有效的交易对列表
- `*errs.Error` - 刷新过程中的错误信息
