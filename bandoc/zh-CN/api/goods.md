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

公共字段为 Name string、Disable bool、AllowEmpty bool；没有 NeedTickers 字段。`Disable`对应yaml键`disable`，停用该条目；`AllowEmpty`不来自yaml，未配置`pairmgr.cron`时为true（允许数据为空的品种保留）。

### VolumePairFilter

字段为 BaseFilter、Limit int、LimitRate float64、MinValue float64、CacheSecs int、BackPeriod string。BackPeriod 是时间周期字符串，不能按旧 BackTimeframe/整数乘数配置。

### PriceFilter
价格过滤器配置结构体。
- `MaxUnitValue float64` - 最小下单数量步长乘价格的最大值（计价币）
- `Precision float64` - 最大 tick/price 比率，0 禁用
- `Min float64` - 最低价格
- `Max float64` - 最高价格

### RateOfChangeFilter

字段为 BaseFilter、BackDays int、Min/Max float64、CacheSecs int；缓存秒数使用 CacheSecs，不是 RefreshPeriod。

### SpreadFilter
占位过滤器，当前原样返回输入；MaxRatio 不产生过滤效果。
- `MaxRatio float32` - 买卖价差占价格的最大比率，公式：1-bid/ask

### CorrelationFilter
相关性过滤器。
- `Min float64` - 最小相关性
- `Max float64` - 最大相关性
- `Timeframe string` - 时间周期
- `BackNum int` - 回溯数量
- `TopN int` - 取前N个
- `TopRate float64` - 最大返回数量占全部品种的比例（yaml `top_rate`）
- `Sort string` - 排序方式

仅当设置了`Timeframe`、非零`BackNum`，且`Max`、`TopN`、`TopRate`至少一个非零时才过滤；`BackNum`小于10报错。

### VolatilityFilter
波动率过滤器，使用 StdDev(ln(close / prev_close)) * sqrt(num) 计算，`num`为日线收益率个数（BackDays-1，历史不足时更少）。
- `BackDays int` - 回顾的K线天数
- `Max float64` - 波动分数最大值
- `Min float64` - 波动分数最小值

### BlockFilter
品种黑名单过滤器，用于过滤指定品种。
- `Pairs []string` - 需要过滤的品种

### AgeFilter
上市时间过滤器。
- `Min int` - 最小上市天数
- `Max int` - 最大上市天数，0不限制；上市天数超过Max的品种被移除

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
- `withInvalid bool` - 是否包含 disable=true 的过滤器

返回：
- `[]IFilter` - 过滤器接口列表
- `*errs.Error` - 创建过程中的错误信息

### RefreshPairList

`RefreshPairList(timeMS int64) ([]string, *errs.Error)`
刷新交易对列表，获取最新的有效交易对。

返回：
- `[]string` - 有效的交易对列表
- `*errs.Error` - 刷新过程中的错误信息

## 注册与显式调用

以下完整示例注册一个按符号列表过滤的过滤器；在策略项目中导入此包后，用 pairlists 的 name: KeepBTC 引用。第一个过滤器仍必须是 producer。

```go
package filters

import (
    "strings"
    "github.com/banbox/banbot/goods"
    "github.com/banbox/banexg/errs"
)

type KeepBTC struct { goods.BaseFilter }

func (f *KeepBTC) Filter(pairs []string, timeMS int64) ([]string, *errs.Error) {
    result := make([]string, 0, len(pairs))
    for _, pair := range pairs {
        if strings.HasPrefix(pair, "BTC/") { result = append(result, pair) }
    }
    return result, nil
}

func init() {
    goods.RegisterFilter("KeepBTC", func(base goods.BaseFilter) goods.IFilter {
        return &KeepBTC{BaseFilter: base}
    })
}
```

`RegisterFilter(name, factory)` 重名会覆盖；`GetFilterFactory(name string) (FilterFactory, bool)` 按名查询已注册的工厂；`CreateFilter(cfg, allowEmpty)` 返回 `(IFilter, *errs.Error)` 并将配置解码到实例。`GetPairFiltersWithConfig(items []*config.CommonPairFilter, withInvalid bool, cfg *config.Config) ([]IFilter, *errs.Error)` 按给定配置构造过滤器；`GetPairFilters(items, withInvalid)` 等价于 cfg 为 nil（使用包级配置）。withInvalid 表示包含已禁用过滤器，不是容忍未知名称。

可选扩展接口优先级：`FilterWithRuntimeDeps`（`RuntimeFilter`）> `FilterWithSymbolState`（`SymbolStateFilter`）> `Filter`；产出器同理为`GenSymbolsWithRuntimeDeps`（`RuntimeProducer`）> `GenSymbolsWithSymbolState`（`SymbolStateProducer`）> `GenSymbols`。

`RuntimeDeps`另有`AllowRetiredPairs bool`和`SkippedPairs []orm.SkippedPair`：仅因子权重（Weights）回测使用，前者允许已退市但回测区间内有数据的固定品种留在池中，后者由`RefreshPairListWithRuntimeDeps`填充，记录回测区间内没有数据而被排除的固定品种。

嵌入程序用 `RefreshPairListWithRuntimeDeps(deps *RuntimeDeps, timeMS int64)`，返回 `([]string, *errs.Error)`。必须提供同一任务的 Core/Clock/Config/Symbols/Storage/Exchange 等依赖；仅有 SymbolState 不能代替完整依赖。自定义过滤器可实现 `FilterWithRuntimeDeps(deps *RuntimeDeps, pairs []string, timeMS int64) ([]string, *errs.Error)`；它优先于 SymbolStateFilter 和旧 Filter。

VolumePairFilter 的 MinValue 是每根 bar 平均 close*volume，CacheSecs 当前不控制缓存。Sort 仅识别 asc/desc，CorrelationFilter 少于四个可用序列时跳过过滤。PriceFilter 固定使用最近1根1h K线收盘价；RateOfChangeFilter 与 VolatilityFilter 使用 1d K线（BackDays根）。详见[筛选指南](../guide/pair_filters.md)。
