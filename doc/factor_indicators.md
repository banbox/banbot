# 因子图常见技术指标

`factor` 新增 18 类常见技术指标，共 22 个标量算子。全部支持 `Plan.Batch`、持续推进的 `Session` 和 `factor/expr` 表达式；它们可以接自定义字段或价格表达式，再传给截面变换。实现沿用 banta/tav v0.4.1 数值口径，新增节点版本为 `indicators-1/banta-0.4.1`。原有 `Lag`、`Return`、`EMA`、`StdDev` 的版本和行为保留。

下表中 `x` 为任意数值节点，`h/l/c/v` 是显式提供的 high/low/close/volume 节点，不要求来自固定 K 线结构。`n`、`fast`、`slow`、`signal` 都必须为 `[1,10000]` 的整数；MACD 还要求 `fast < slow`。布林带的 `up/down` 是有限非负数，允许为零。所有参数都必须显式提供。

| Go builder | 表达式函数 | 公式或含义 |
| --- | --- | --- |
| `SMA(x,n)` | `ts.sma(x,n)` | 最近 n 个有效观察的算术均值 |
| `RMA(x,n)` | `ts.rma(x,n)` | Wilder 平滑；以 n 个观察的均值初始化，`alpha=1/n` |
| `WMA(x,n)` | `ts.wma(x,n)` | 最旧到最新的权重为 `1..n` |
| `VWMA(c,v,n)` | `ts.vwma(c,v,n)` | `sum(c*v)/sum(v)` |
| `RSI(x,n)` | `ts.rsi(x,n)` | `100*平均涨幅/(平均涨幅+平均跌幅)`，涨跌幅使用 Wilder 平滑 |
| `ROC(x,n)` | `ts.roc(x,n)` | `100*(x[t]-x[t-n])/x[t-n]`，单位为百分比 |
| `MOM(x,n)` | `ts.mom(x,n)` | `x[t]-x[t-n]`，单位与原字段相同 |
| `TR(h,l,c)` | `ts.tr(h,l,c)` | `max(h-l, abs(h-prevClose), abs(l-prevClose))` |
| `ATR(h,l,c,n)` | `ts.atr(h,l,c,n)` | 对 TR 做 n 期 RMA |
| `CCI(x,n)` | `ts.cci(x,n)` | `(x-SMA(x,n))/(0.015*平均绝对偏差)`；典型价格需显式构造 |
| `Stoch(h,l,c,n)` | `ts.stoch(h,l,c,n)` | 原始 `%K=100*(c-lowest(l,n))/(highest(h,n)-lowest(l,n))` |
| `WillR(h,l,c,n)` | `ts.willr(h,l,c,n)` | `100*(c-highest(h,n))/(highest(h,n)-lowest(l,n))`，通常在 -100 到 0 之间 |
| `OBV(c,v)` | `ts.obv(c,v)` | 初值为第一条有效成交量；上涨加 v、下跌减 v、持平不变 |
| `MFI(h,l,c,v,n)` | `ts.mfi(h,l,c,v,n)` | 使用 `(h+l+c)/3` 与成交量，按典型价格涨跌累计正/负资金流 |
| `Highest(x,n)` | `ts.highest(x,n)` | 最近 n 个有效观察的最大值 |
| `Lowest(x,n)` | `ts.lowest(x,n)` | 最近 n 个有效观察的最小值 |
| `MACD(x,fast,slow,signal)` 的 line | `ts.macd(x,fast,slow,signal)` | `EMA(x,fast)-EMA(x,slow)` |
| 同上 signalLine | `ts.macd_signal(x,fast,slow,signal)` | MACD line 的 signal 期 EMA |
| 同上 hist | `ts.macd_hist(x,fast,slow,signal)` | `line-signalLine`，不乘 2 |
| `BBands(x,n,up,down)` 的 upper | `ts.bbands_upper(x,n,up,down)` | `SMA(x,n)+up*StdDev(x,n,0)` |
| 同上 middle | `ts.bbands_middle(x,n,up,down)` | `SMA(x,n)` |
| 同上 lower | `ts.bbands_lower(x,n,up,down)` | `SMA(x,n)-down*StdDev(x,n,0)` |

`ROC` 与原有 `Return` 的单位不同：价格从 100 变到 110 时，`ROC(...,1)` 得到 `10`，`Return(...,1)` 得到 `0.1`。新 `ROC/MOM` 的 n 按有效观察计数；原有 `Return/Lag` 保留原有观察位置语义，遇到缺失时不保证 `ROC == 100*Return`。

`MACD` 和 `BBands` 的 Go 构造函数各返回三个独立的 `*Node`，按需加入输出。DSL 每个函数只返回一个标量节点，不提供元组或隐式选列。

```go
price := factor.Field("prices", "close", "1h")
high := factor.Field("prices", "high", "1h")
low := factor.Field("prices", "low", "1h")
volume := factor.Field("prices", "volume", "1h")
typical := factor.Div(factor.Add(factor.Add(high, low), price), factor.Constant(3, "1h"))
line, signal, hist := factor.MACD(price, 12, 26, 9)
upper, middle, lower := factor.BBands(price, 20, 2, 2)
plan, err := factor.New().
    Add("rsi", factor.RSI(price, 14)).
    Add("atr", factor.ATR(high, low, price, 14)).
    Add("vwma", factor.VWMA(price, volume, 20)).
    Add("cci", factor.CCI(typical, 20)).
    Add("macd", line).Add("signal", signal).Add("hist", hist).
    Add("upper", upper).Add("middle", middle).Add("lower", lower).
    Compile()
```

对应的独立表达式配置可用 `banbot validate --spec indicators.yml` 或 `banbot explain --spec indicators.yml` 检查，无需连接数据库：

```yaml
schema_version: 1
timeframe: 1h
bindings:
  prices: {source: prices, timeframe: 1h}
params: {rsi_period: 14, band_period: 20, fast: 12, slow: 26, signal: 9}
lets:
  typical: '(prices.high + prices.low + prices.close) / 3'
outputs:
  rsi: 'ts.rsi(prices.close,param.rsi_period)'
  atr: 'ts.atr(prices.high,prices.low,prices.close,param.rsi_period)'
  cci: 'ts.cci(factor.typical,param.band_period)'
  hist: 'ts.macd_hist(prices.close,param.fast,param.slow,param.signal)'
  band_position: '(prices.close-ts.bbands_middle(prices.close,param.band_period,2,2))/max(ts.atr(prices.high,prices.low,prices.close,14),1e-8)'
  rsi_rank: 'cs.rank(ts.rsi(prices.close,param.rsi_period))'
```

窗口参数只接受数值常量或 `param.name`，不接受字段或 `param.period+1` 等计算表达式。函数严格校验输入数量、参数名、整数范围、有限性及参数关系；无效且未被引用的 `lets` 也会报错。表达式允许先时序后截面；对任意输入包含截面/回归结果的时序指标会报错，包括多输入函数的非首个输入。

## 缺失、预热与历史保留

新指标统一使用 `skip-invalid`：当任一输入不是 `Valid`，当前输出按参数顺序传播第一个无效输入的 `Missing/Null/NotNumeric/NonFinite/Warmup` 原因，该时点不推进本指标状态。多输入函数只接收全部字段有效的完整元组，不会分别跳过 high、low、close 或 volume 后错位配对。后续有效元组继续已有窗口或递推状态。

原始数据依然由 `orm.DataSeries.Values map[string]any` 提供；数值计算不删除扩展字段，不填零，也不改变原始字段的类型或 NULL 语义。不同字段、输入顺序、数据源、采样策略、参数和算子版本都进入图的内容身份，不能共享不等价的缓存。

`Plan.WarmupLength()` 是在没有缺失、上游已就绪时需要的前置观察数，第一条可能有效的观察序号为 `warmup+1`。下面是单层指标的 lookback；计划会加上各输入最大的上游 warmup，并在所有节点中取最大值。`StateRetention()` 是计划需要保留的历史下界，整个计划至少保留 2 个观察；状态平滑仍由同一个 Session 连续维护。

| 指标 | 前置有效元组数 | 节点保留长度下界 |
| --- | --- | --- |
| SMA/RMA/WMA/VWMA/CCI/Stoch/WillR/Highest/Lowest/BBands/MFI | `n-1` | `n` |
| RSI/ROC/MOM/ATR | `n` | `n+1` |
| TR | `1` | `2` |
| OBV | `0` | `2` |
| MACD line | `slow-1` | `slow` |
| MACD signal/hist | `slow+signal-2` | `slow+signal-1` |

缺失会延长经过的时钟时间；预热数不是保证有效输出的日历长度。达到 lookback 后仍可能遇到数学上无定义的结果：VWMA 总成交量为零、ROC 历史价格为零、CCI 平均偏差为零、WillR 高低价范围为零。Stoch 在平坦区间返回 `50`，RSI 在平坦序列按 tav 口径返回 `100`。MFI 在负资金流为零时沿用 banta/tav 的无定义结果，不强行输出 `100`。产生 NaN 的计算结果沿用现有 `Warmup` 有效性，产生无穷大的结果为 `NonFinite`；这些结果不会变成可交易的有限分数。

全部输入必须具有相同决策周期。跨源周期通过显式 `AsOfField` 或 binding 的 `sampling: asof`、正 `max_age_ms` 采样到决策网格；指标期数随后按有效决策观察计数。小时网格反复采样日频值后做 20 期 SMA，表示 20 个有效小时观察，不代表 20 个交易日。需要原生日频指标时，先在日频源生成，再采样其输出。

`Batch` 从传入历史起点初始化，适用于有明确行数上限的一段历史。历史分块或在线行情应持续推进同一个 `Session`，避免重新初始化 RMA、RSI、ATR、OBV 和 MACD。计算 owner 在内部隔离候选状态，验证后再接纳，避免失败的候选计算污染已接纳状态。
