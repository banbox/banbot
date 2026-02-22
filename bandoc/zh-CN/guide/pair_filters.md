您可以直接在yaml配置文件中通过`pairs`指定交易的品种列表，也可以通过品种管理器自动筛选交易列表。

当您使用`pairs`指定品种列表时，默认不启用品种过滤器。但您可以将`pairmgr.force_filters`设为true启用，对给定的品种列表进行过滤。

## 运行时机
在机器人启动时，默认会运行一次品种过滤器，得到可交易品种列表。  
您也可以指定cron表达式，定期执行并刷新可交易列表。  
当某个品种需要被移除时，如果有未平仓订单，则暂时不会被移除。  
回测时也会执行corn表达式，定期刷新品种列表，以达到和实时交易类似的效果。

## 所有支持的过滤器
* VolumePairList
* PriceFilter
* RateOfChangeFilter
* SpreadFilter
* CorrelationFilter
* VolatilityFilter
* AgeFilter
* BlockFilter
* OffsetFilter
* ShuffleFilter

yaml的品种过滤器列表的第一个必须是`VolumePairList`，它可以从市场所有品种返回一个品种列表。

## VolumePairList
根据交易量对货币对进行排序/过滤。当其处于第一个时，会计算由`back_period`指定的周期内，所有品种的交易量，并降序返回。
```yaml
  - name: VolumePairList  # 按成交量价值倒序排序所有交易对
    limit: 100  # 取前100个
    min_value: 100000  # 最低成交量价值
    cache_secs: 7200  # 缓存时间
    back_period: 3d  # 计算成交量时间周期
```
## PriceFilter
允许按价格筛选品种列表。
```yaml
  - name: PriceFilter  # 价格过滤器
    max_unit_value: 100  # 最大允许的单位价格变动对应的价值(针对定价货币，一般是USDT)。
    precision: 0.0015  # 按价格精度过滤交易对，默认要求价格变动最小单位是0.1%
    min: 0.001  # 最低价格
    max: 100000  # 最高价格
```
## RateOfChangeFilter
波动性过滤器。
```yaml
  - name: RateOfChangeFilter  # 波动性过滤器
    back_days: 5  # 回顾的K线天数
    min: 0.03  # 最小价格变动比率
    max: 10  # 最大价格变动比率
    cache_secs: 1440  # 缓存时间，秒
```
## SpreadFilter
流动性过滤器。公式：1-bid/ask，买卖价差占价格的最大比率
```yaml
  - name: SpreadFilter  # 流动性过滤器
    max_ratio: 0.005  # 公式：1-bid/ask，买卖价差占价格的最大比率
```
## CorrelationFilter
相关系数过滤器。
```yaml
  - name: CorrelationFilter  # 相关性过滤器
    min: -1  # 用于过滤当前币种与全市场平均相关性；默认0，表示不启用
    max: 1  # 用于过滤当前币种与全市场平均相关性；默认0，表示不启用
    timeframe: 5m  # 用于计算相关性的数据周期
    back_num: 70  # 计算相关性回顾数据长度
    top_n: 50  # 只返回相关性最低的前n个币种，默认0不限制
```
## VolatilityFilter
波动性过滤器。公式：std(log(c/c1)) * sqrt(back_days)
```yaml
  - name: VolatilityFilter  # 波动性过滤器，公式：std(log(c/c1)) * sqrt(back_days)
    back_days: 10  # 回顾的K线天数
    max: 1  # 波动分数最大值，此值越大，允许一些在1d级别上变化非常剧烈的标的
    min: 0.05  # 波动分数最小值，此值越小，允许一些在1d级别上变化非常小的标的
```
## AgeFilter
按品种上市时间过滤。
```yaml
  - name: AgeFilter  # 按标的的上市天数过滤
    min: 5
```
## BlockFilter
品种黑名单过滤器，用于过滤指定品种。
```yaml
 - name: BlockFilter
   pairs: [BTC]
```
## OffsetFilter
按给定偏移，取指定数量的品种。一般用在最后
```yaml
  - name: OffsetFilter  # 偏移限定数量选择。一般用在最后
    offset: 10  # 从第10个开始取
    limit: 30  # 最多取30个
```
## ShuffleFilter
随机打乱
```yaml
  - name: ShuffleFilter  # 随机打乱
    seed: 42  # 随机数种子，可选
```
