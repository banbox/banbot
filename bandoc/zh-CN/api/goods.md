# goods 包

goods 包提供了商品和交易对相关的功能。

## 重要结构体

### IFilter
过滤器接口，所有过滤器都必须实现此接口。
- `GetName() string` - 获取过滤器名称
- `IsDisable() bool` - 判断过滤器是否禁用
- `IsNeedTickers() bool` - 判断是否需要行情数据
- `Filter(pairs []string, tickers map[string]*banexg.Ticker) ([]string, *errs.Error)` - 过滤方法

### IProducer
生产者接口，继承自 IFilter 接口，用于生成交易对列表。
- 继承 IFilter 的所有方法
- `GenSymbols(tickers map[string]*banexg.Ticker) ([]string, *errs.Error)` - 生成交易对列表

### BaseFilter
基础过滤器结构体，所有具体过滤器的基类。
- `Name string` - 过滤器名称
- `Disable bool` - 是否禁用
- `NeedTickers bool` - 是否需要行情数据
- `AllowEmpty bool` - 是否允许空结果

### VolumePairFilter
成交量过滤器，按成交量价值倒序排序所有交易对。
- `Limit int` - 返回结果的数量限制，取前100个
- `LimitRate float64` - 限制比率
- `MinValue float64` - 最低成交量价值
- `RefreshSecs int` - 缓存时间，以秒为单位
- `BackTimeframe string` - 计算成交量的时间周期，默认为天
- `BackPeriod int` - 与BackTimeframe相乘得到的时间范围的乘数

### PriceFilter
价格过滤器配置结构体。
- `MaxUnitValue float64` - 最大允许的单位价格变动对应的价值(针对定价货币，一般是USDT)
- `Precision float64` - 价格精度，默认要求价格变动最小单位是0.1%
- `Min float64` - 最低价格
- `Max float64` - 最高价格

### RateOfChangeFilter
价格变动比率过滤器，计算一段时间内(high-low)/low比值。
- `BackDays int` - 回顾的K线天数
- `Min float64` - 最小价格变动比率
- `Max float64` - 最大价格变动比率
- `RefreshPeriod int` - 缓存时间，秒

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
