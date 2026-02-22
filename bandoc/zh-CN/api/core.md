# core 包

core 包提供了系统核心的数据结构、常量和错误定义。

## 核心结构体

### Param
参数配置结构体，用于定义和管理系统中的参数值，特别适用于超参数搜索场景。

字段说明：
- `Name string` - 参数名称
- `VType int` - 参数值类型，可以是：
  - `VTypeUniform(0)` - 均匀线性分布
  - `VTypeNorm(1)` - 正态分布
- `Min float64` - 参数最小值
- `Max float64` - 参数最大值
- `Mean float64` - 均值（在正态分布类型时有效）
- `IsInt bool` - 是否为整数类型
- `Rate float64` - 正态分布的比率参数，默认为1。值越大，随机值越趋向于Mean
- `edgeY float64` - 正态分布边缘y值的计算缓存

### PerfSta
策略性能统计结构体，用于记录某个策略针对所有交易标的的统计信息。

字段说明：
- `OdNum int` - 订单数量
- `LastGpAt int` - 上次执行聚类的订单数量
- `Splits *[4]float64` - 分割点数组，用于性能分组
- `Delta float64` - 对总利润进行对数处理前的乘数

### JobPerf
任务性能结构体，用于记录策略在特定交易对和时间周期上的表现。

字段说明：
- `Num int` - 订单数量
- `TotProfit float64` - 总利润
- `Score float64` - 开单倍率，小于1表示需要减少开单量。当Score等于PrefMinRate(0.001)时，将直接使用最小开单金额(MinStakeAmount)

## 核心功能方法

### Setup
初始化系统核心组件。

返回：
- `*errs.Error` - 初始化过程中的错误信息，如果成功则返回nil

### SetRunMode
设置系统运行模式。

参数：
- `mode string` - 运行模式标识符

### SetRunEnv
设置系统运行环境。

参数：
- `env string` - 运行环境标识符

### SetPairMs
设置交易对的时间参数。

参数：
- `pair string` - 交易对名称
- `barMS int64` - K线时间间隔(毫秒)
- `waitMS int64` - 等待时间(毫秒)

### Sleep
带有中断检查的休眠函数。

参数：
- `d time.Duration` - 休眠时长

返回：
- `bool` - 是否被中断(true表示正常休眠完成，false表示被中断)

## 缓存相关方法

### GetCacheVal
从缓存中获取指定键的值，支持泛型。

参数：
- `key string` - 缓存键名
- `defVal T` - 默认值

返回：
- `T` - 缓存值或默认值

### SnapMem
获取内存快照。

参数：
- `name string` - 快照名称

## 性能统计方法

### GetPerfSta
获取策略性能统计信息。

参数：
- `stagy string` - 策略名称

返回：
- `*PerfSta` - 性能统计对象

### DumpPerfs
导出性能统计数据到文件。

参数：
- `outDir string` - 输出目录路径

## 价格相关方法

### GetPrice
获取交易对的最新价格。

参数：
- `symbol string` - 交易对符号
- `side string` - banexg.OdSideBuy/OdSideBuy/""

返回：
- `float64` - 最新价格

### GetPriceSafe
安全地获取交易对价格，包含法币处理逻辑。

参数：
- `symbol string` - 交易对符号
- `side string` - banexg.OdSideBuy/OdSideBuy/""

返回：
- `float64` - 处理后的价格

### SetPrice
设置交易对的最新价格。

参数：
- `pair string` - 交易对名称
- `ask float64` - 卖一价
- `bid float64` - 买一价

### SetPrices
批量设置多个交易对的价格。

参数：
- `data map[string]float64` - 交易对价格映射
- `side string` - banexg.OdSideBuy/OdSideBuy/""

### IsMaker
判断当前价格是否为做市商价格。

参数：
- `pair string` - 交易对名称
- `side string` - 交易方向
- `price float64` - 价格

返回：
- `bool` - 是否为做市商价格

## 工具方法

### IsFiat
判断是否为法币代码。

参数：
- `code string` - 货币代码

返回：
- `bool` - 是否为法币

### KeyStratPairTf
生成策略-交易对-时间周期的组合键名。

参数：
- `stagy string` - 策略名称
- `pair string` - 交易对名称
- `tf string` - 时间周期

返回：
- `string` - 组合后的键名

### MarshalYaml
将对象序列化为YAML格式。

参数：
- `v any` - 要序列化的对象

返回：
- `[]byte` - 序列化后的字节数组
- `error` - 错误信息

### GroupByPairQuotes
按交易对和报价分组。

参数：
- `items map[string][]string` - 待分组的项目

返回：
- `string` - 分组结果的字符串表示

### SplitSymbol
分割交易对符号为基础货币和计价货币。

参数：
- `pair string` - 交易对名称

返回：
- `string` - 基础货币
- `string` - 计价货币
- `string` - 基础货币代码
- `string` - 计价货币代码

## 参数生成方法

### PNorm
创建正态分布参数。

参数：
- `min float64` - 最小值
- `max float64` - 最大值

返回：
- `*Param` - 参数对象

### PNormF
创建带有均值和比率的正态分布参数。

参数：
- `min float64` - 最小值
- `max float64` - 最大值
- `mean float64` - 均值
- `rate float64` - 比率

返回：
- `*Param` - 参数对象

### PUniform
创建均匀分布参数。

参数：
- `min float64` - 最小值
- `max float64` - 最大值

返回：
- `*Param` - 参数对象

## 订单相关方法

### IsLimitOrder
判断是否为限价单。

参数：
- `t int` - 订单类型

返回：
- `bool` - 是否为限价单

## 系统控制方法

### RunExitCalls
执行退出时的回调函数。

### IsPriceEmpty
检查价格缓存是否为空。

返回：
- `bool` - 价格缓存是否为空

### PrintStratGroups
打印策略分组信息。
