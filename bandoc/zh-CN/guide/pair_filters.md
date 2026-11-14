您可以直接在yaml配置文件中通过`pairs`指定交易的品种列表，也可以通过品种管理器自动筛选交易列表。

当您使用`pairs`指定品种列表时，默认不启用品种过滤器。但您可以将`pairmgr.force_filters`设为true启用，对给定的品种列表进行过滤。

## 运行时机
在机器人启动时，默认会运行一次品种过滤器，得到可交易品种列表。  
您也可以指定cron表达式，定期执行并刷新可交易列表。  
当某个品种需要被移除时，`pairmgr.pos_on_rotation` 决定处理方式：`hold` 保留仓位但禁止继续开仓，`close` 会先退出仓位再移除。
回测时也会执行cron 表达式，定期刷新品种列表，以达到和实时交易类似的效果。

```yaml
pairmgr:
  pos_on_rotation: hold  # hold 或 close
  use_latest: false      # 回测未启用 cron 时，是否按最新日期确定品种列表
```

## 所有支持的过滤器
* VolumePairList
* PriceFilter
* RateOfChangeFilter
* SpreadFilter（占位，不执行过滤）
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
    min_value: 100000  # 每根 bar 的平均 close*volume 下限
    back_period: 3d  # 计算成交量时间周期
```
## PriceFilter
允许按价格筛选品种列表。
```yaml
  - name: PriceFilter  # 价格过滤器
    max_unit_value: 100  # 最小下单数量步长乘价格的最大值（计价币）。
    precision: 0.0015  # 按价格精度过滤交易对，允许一个 tick 占价格的最大比例；0 禁用
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
```
## SpreadFilter
当前仅保留配置结构；Filter 原样返回输入，`max_ratio` 不产生过滤效果。不要用它作为流动性保护。
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
    sort: asc  # 贪心优先选择与已选品种低相关的品种
    top_n: 50  # 最大返回数量，0 不限制
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
    offset: 10  # 跳过前 10 个取
    limit: 30  # 最多取30个
```
## ShuffleFilter
随机打乱
```yaml
  - name: ShuffleFilter  # 随机打乱
    seed: 42  # 随机数种子，可选
```

## 配置与数据限制

把过滤器放在根配置的 `pairlists` 列表中；按顺序执行，最后再应用 `pairmgr.offset`/`limit`。例如：

```yaml
stake_currency: [USDT]
pairmgr:
  cron: "0 0 */6 * * *"
  pos_on_rotation: hold
pairlists:
  - name: VolumePairList
    back_period: 3d
    limit: 100
    min_value: 100000
  - name: AgeFilter
    min: 30
  - name: PriceFilter
    precision: 0.0015
  - name: OffsetFilter
    limit: 20
```

`VolumePairList` 按历史 bar 的平均 `close*volume` 排名，并检查最小下单额；它不是交易所 24 小时成交额，`min_value` 随计算周期变化。`limit_rate` 可先按比例截取，再受 limit 限制。`cache_secs` 在 VolumePairList/RateOfChangeFilter 中虽可解析，但当前未实现缓存控制，不要用它替代 cron。

固定 `pairs` 默认绕过配置过滤器，但不保证绕过最小下单额与可用市场校验。启用 force_filters 后仍需把 producer 放在过滤器列表第一项；固定池经过后续过滤器。短名 BTC 根据 market_type 与 stake_currency 展开；完整现货符号如 BTC/USDT，线性永续如 BTC/USDT:USDT。

PriceFilter 使用最近 1h bar 收盘价；`max_unit_value` 检查最小数量步长乘价格，不是价格 tick 的价值。RateOfChangeFilter 使用 1d bar 的 `(最高价-最低价)/最低价`；VolatilityFilter 使用日收益率波动分数。历史数据不足会影响结果。未设 cron 时允许空数据的模式可能保留品种；AgeFilter 对过新标的可能设置禁止开仓时间而非立即移除。

CorrelationFilter 要求 back_num≥10；少于四个可用序列时返回原列表。未设置 sort: asc/desc 时保留原始顺序，top_n 不会自动选择最低相关性。sort 使用贪心相关性排序，不是聚类优化。仅设 min 不能启用过滤，还须设置 max、top_n 或 top_rate。

ShuffleFilter 使用 seed；固定 seed 可复现。OffsetFilter 先 reverse，再 offset，再 rate/limit；offset 从 0 起，超出列表返回空。自定义过滤器注册方式见 [goods API](../api/goods.md)。
