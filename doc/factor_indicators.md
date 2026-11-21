# 因子图常见技术指标

`factor` 内置均线与自适应滤波、趋势/方向/通道、动量/成交量/K 线原生订单流、OHLC 极值波动率与滚动统计等全部高频技术算子，均支持 `Plan.Batch`、持续推进且可 `fork`/`commit` 隔离的 `Session` 与 `factor/expr` 表达式；它们可以消费任意字段、算术表达式或上游截面/分组输出（`StageCS -> StageTS`），再传给下游截面变换。节点版本为 `indicators-1/banta-0.4.1`，原有 `Lag`、`Return`、`EMA`、`StdDev` 的版本和行为保留。

下表中 `x/y` 为任意数值标量节点，`o/h/l/c/v/bv` 是显式提供的 open/high/low/close/volume/buy_volume 节点（不要求来自固定 K 线结构；当表达式 `bindings` 中声明了 `kline` 时，多字段 OHLC/OHLCV/订单流函数还支持省略字段节点的简写形式，如 `ts.adx(14)`、`ts.parkinson_vol(20)`、`ts.volume_delta()`）。窗口参数为 `[1,10000]` 的整数（`HMA`/`CHOP`/`YangZhangVol` 要求 `>=2`，`TsSkew` 要求 `>=3`，`TsKurt` 要求 `>=4`，`Hurst` 要求 `>=8`）；`MACD`/`KAMABy`/`STC`/`ChaikinOsc` 要求 `fast < slow`。

| Go builder | 表达式函数 | 公式或含义 |
| --- | --- | --- |
| `SMA(x,n)` | `ts.sma(x,n)` | 最近 n 个有效观察的算术均值 |
| `RMA(x,n)` | `ts.rma(x,n)` | Wilder 平滑；以 n 个观察的均值初始化，`alpha=1/n` |
| `WMA(x,n)` / `DecayLinear(x,n)` | `ts.wma(x,n)` / `ts.decay_linear(x,n)` | 最旧到最新的权重为 `1..n`（两者为同一节点身份） |
| `DEMA(x,n)` / `TEMA(x,n)` | `ts.dema(x,n)` / `ts.tema(x,n)` | 双重 / 三重指数移动平均 |
| `HMA(x,n)` | `ts.hma(x,n)` | Hull 均线 `WMA(2*WMA(x,n/2)-WMA(x,n), floor(sqrt(n)))`（`n>=2`） |
| `TRIMA(x,n)` | `ts.trima(x,n)` | 三角加权移动平均 `SMA(SMA(x, ceil(n/2)), floor(n/2)+1)` |
| `ER(x,n)` | `ts.er(x,n)` | Kaufman 效率比 `|x[t]-x[t-n]| / sum(|Δx|, n)` |
| `KAMA(x,n)` / `KAMABy(x,n,f,s)` | `ts.kama(x,n)` / `ts.kama(x,n,f,s)` | Kaufman 自适应均线（缺省 `fast=2, slow=30`；自定义要求 `f<s`） |
| `ALMA(x,n,offset,sigma)` | `ts.alma(x,n[,offset,sigma])` | Arnaud Legoux 高斯均线（`offset∈[0,1]` 缺省 `0.85`，`sigma>0` 缺省 `6.0`） |
| `Kalman1D(x,q,r)` | `ts.kalman1d(x,q,r)` | 一维随机游走卡尔曼滤波状态估计（`q>0, r>0`） |
| `Hurst(x,n)` | `ts.hurst(x,n)` | 滚动重标极差 R/S Hurst 指数 `∈[0,1]`（要求 `n>=8`） |
| `VWMA(c,v,n)` | `ts.vwma([c,v,]n)` | `sum(c*v)/sum(v)` |
| `TsCorr(x,y,n)` | `ts.corr(x,y,n)` | 最近 n 个同时有效配对的 Pearson 相关系数；方差为 0 返回 `0` |
| `TsCov(x,y,n)` | `ts.cov(x,y,n)` | 最近 n 个同时有效配对的总体协方差 |
| `TsBeta(y,x,n)` | `ts.beta(y,x,n)` | `Cov(y,x)/Var(x)`；`Var(x)==0` 返回 `0` |
| `TsSum(x,n)` | `ts.sum(x,n)` | 最近 n 个有效观察的累加和 |
| `TsProduct(x,n)` | `ts.product(x,n)` | 最近 n 个有效观察的连乘积 |
| `TsRank(x,n)` | `ts.rank(x,n)` | 最新值在最近 n 个有效观察中的 `[0,1]` 归一化平均名次（`n==1` 返回 `0.5`） |
| `TsZScore(x,n)` | `ts.zscore(x,n)` | `(x-SMA(x,n))/StdDev(x,n,0)`；标准差为 0 返回 `0` |
| `TsArgMax(x,n)` / `TsArgMin(x,n)` | `ts.argmax(x,n)` / `ts.argmin(x,n)` | 极值距当前的偏移 `0..n-1`（`0` 为当前最新观察，对齐 `banta.HighestBar/LowestBar`） |
| `TsSkew(x,n)` | `ts.skew(x,n)` | 无偏样本偏度（要求 `n >= 3`；标准差为 0 返回 `0`） |
| `TsKurt(x,n)` | `ts.kurt(x,n)` | 无偏样本超额峰度（要求 `n >= 4`；标准差为 0 返回 `0`） |
| `TsSlope(x,n)` | `ts.slope(x,n)` | 对时间索引 `0..n-1` 的 OLS 斜率 |
| `TsRSquare(x,n)` | `ts.rsquare(x,n)` | 对时间索引 `0..n-1` 的 OLS 拟合优度 `R² ∈ [0,1]`（方差为 0 返回 `0`） |
| `RSI(x,n)` | `ts.rsi(x,n)` | `100*平均涨幅/(平均涨幅+平均跌幅)`，涨跌幅使用 Wilder 平滑 |
| `ROC(x,n)` | `ts.roc(x,n)` | `100*(x[t]-x[t-n])/x[t-n]`，单位为百分比 |
| `MOM(x,n)` | `ts.mom(x,n)` | `x[t]-x[t-n]`，单位与原字段相同 |
| `TR(h,l,c)` | `ts.tr([h,l,c])` | `max(h-l, abs(h-prevClose), abs(l-prevClose))` |
| `ATR(h,l,c,n)` / `NATR(h,l,c,n)` | `ts.atr([h,l,c,]n)` / `ts.natr([h,l,c,]n)` | 对 TR 做 n 期 RMA / 归一化波动率 `100*ATR/c` |
| `ADX(h,l,c,n)` / `PlusDI` / `MinusDI`（`DMI`） | `ts.adx` / `ts.dmi_plus`(`ts.plus_di`) / `ts.dmi_minus`(`ts.minus_di`)`([h,l,c,]n)` | Wilder 方向运动指标 `+DI`、`-DI` 与 `ADX`（0–100） |
| `CHOP(h,l,c,n)` | `ts.chop([h,l,c,]n)` | Choppiness 指数 `100*log10(sum(ATR(1),n)/(hh-ll))/log10(n)`（`n>=2`） |
| `AroonUp/AroonDown/AroonOsc`（`Aroon`） | `ts.aroon_up/aroon_down/aroon_osc([h,l,]n)` | `n+1` 观察窗口内极值距今位置百分比及差值 |
| `VortexPlus/VortexMinus`（`Vortex`） | `ts.vortex_plus/vortex_minus([h,l,c,]n)` | 涡旋指标 `VI+` / `VI-` |
| `DonchianUpper/Mid/Lower`（`Donchian`） | `ts.donchian_upper/donchian_mid/donchian_lower([h,l,]n)` | 唐奇安通道上轨、中轨、下轨 |
| `KeltnerUpper/Mid/Lower`（`Keltner`） | `ts.keltner_upper/keltner_mid/keltner_lower([h,l,c,]n,mult)` | 肯特纳通道 `EMA(c,n) ± mult*ATR(n)`（`mult>=0`） |
| `Supertrend` / `SupertrendDir` | `ts.supertrend` / `ts.supertrend_dir([h,l,c,]n,mult)` | ATR 棘轮趋势线与多空方向 `+1/-1`（`mult>0`） |
| `SAR(h,l,start,inc,max)` | `ts.sar([h,l][,start,inc,max])` | 抛物线转向指标（缺省 `0.02, 0.02, 0.2`，要求 `max>=start>0, inc>0`） |
| `STC(x,fast,slow,cycle)` | `ts.stc(x,fast,slow,cycle)` | Schaff 趋势周期（0–100，要求 `fast<slow`） |
| `IchimokuConversion` / `IchimokuBase` | `ts.ichimoku_conversion` / `ts.ichimoku_base([h,l,]n)` | 一目均衡表转换线 Tenkan / 基准线 Kijun |
| `CCI(x,n)` | `ts.cci(x,n)` | `(x-SMA(x,n))/(0.015*平均绝对偏差)`；典型价格需显式构造 |
| `Stoch(h,l,c,n)` | `ts.stoch([h,l,c,]n)` | 原始 `%K=100*(c-lowest(l,n))/(highest(h,n)-lowest(l,n))` |
| `WillR(h,l,c,n)` | `ts.willr([h,l,c,]n)` | `100*(c-highest(h,n))/(highest(h,n)-lowest(l,n))`，通常在 -100 到 0 之间 |
| `OBV(c,v)` / `ADL(h,l,c,v)` | `ts.obv([c,v])` / `ts.adl([h,l,c,v])` | 能量潮 / 累积派发线 |
| `MFI(h,l,c,v,n)` / `CMF(h,l,c,v,n)` | `ts.mfi([h,l,c,v,]n)` / `ts.cmf([h,l,c,v,]n)` | 资金流量指数 / 佳庆资金流 |
| `ChaikinOsc(h,l,c,v,fast,slow)` / `ADOSC` | `ts.adosc` / `ts.chaikin_osc([h,l,c,v,]fast,slow)` | 佳庆振荡器 `EMA(ADL,fast)-EMA(ADL,slow)`（`fast<slow`） |
| `VolumeDelta(v,bv)` / `CVD(v,bv)` | `ts.volume_delta([v,bv])` / `ts.cvd([v,bv])` | 单根 K 线净主动买量 `2*bv-v` / 累计净主动买量 |
| `RollingCVD(v,bv,n)` / `TakerBuyRatio(v,bv,n)` | `ts.rolling_cvd([v,bv,]n)` / `ts.taker_buy_ratio([v,bv,]n)` | 滚动净主动买量 / 主动买入成交占比 `sum(bv)/sum(v)∈[0,1]` |
| `ParkinsonVol(h,l,n)` | `ts.parkinson_vol([h,l,]n)` | Parkinson 高低极差波动率估计 |
| `GarmanKlassVol(o,h,l,c,n)` | `ts.garman_klass_vol([o,h,l,c,]n)` | Garman-Klass OHLC 波动率估计 |
| `YangZhangVol(o,h,l,c,n)` | `ts.yang_zhang_vol([o,h,l,c,]n)` | Yang-Zhang 漂移无关隔夜+日内波动率估计（`n>=2`） |
| `Highest(x,n)` / `Lowest(x,n)` | `ts.highest(x,n)` / `ts.lowest(x,n)` | 最近 n 个有效观察的最大值 / 最小值 |
| `MACD(x,fast,slow,signal)` 的 line/signal/hist | `ts.macd` / `ts.macd_signal` / `ts.macd_hist(x,fast,slow,signal)` | MACD 差离值、信号线与柱状图 |
| `BBands(x,n,up,down)` 的 upper/middle/lower | `ts.bbands_upper` / `ts.bbands_middle` / `ts.bbands_lower(x,n,up,down)` | `SMA(x,n)+up*StdDev` / `SMA` / `SMA-down*StdDev` |

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

窗口参数只接受数值常量或 `param.name`，不接受字段或 `param.period+1` 等计算表达式。函数严格校验输入数量、参数名、整数范围、有限性及参数关系；无效且未被引用的 `lets` 也会报错。支持任意无环 `TS ↔ CS/GROUP` 多阶段组合，标量与多输入技术指标均可消费截面或分组回归输出。

## 缺失、预热与历史保留

新指标统一使用 `skip-invalid`：当任一输入不是 `Valid`，当前输出按参数顺序传播第一个无效输入的 `Missing/Null/NotNumeric/NonFinite/Warmup` 原因，该时点不推进本指标状态。多输入函数只接收全部字段有效的完整元组，不会分别跳过 high、low、close 或 volume 后错位配对。后续有效元组继续已有窗口或递推状态。

原始数据依然由 `orm.DataSeries.Values map[string]any` 提供；数值计算不删除扩展字段，不填零，也不改变原始字段的类型或 NULL 语义。不同字段、输入顺序、数据源、采样策略、参数和算子版本都进入图的内容身份，不能共享不等价的缓存。

`Plan.WarmupLength()` 是在没有缺失、上游已就绪时需要的前置观察数，第一条可能有效的观察序号为 `warmup+1`。下面是单层指标的 lookback；计划会加上各输入最大的上游 warmup，并在所有节点中取最大值。`StateRetention()` 是计划需要保留的历史下界，整个计划至少保留 2 个观察；状态平滑仍由同一个 Session 连续维护。

| 指标 | 前置有效元组数 | 节点保留长度下界 |
| --- | --- | --- |
| SMA/RMA/WMA/TRIMA/ALMA/Hurst/VWMA/CCI/Stoch/WillR/Highest/Lowest/BBands/MFI/CMF/Donchian/KeltnerMid/Ichimoku/RollingCVD/TakerBuyRatio/ParkinsonVol/GarmanKlassVol | `n-1` | `n` |
| RSI/ROC/MOM/ATR/NATR/KAMA/ER/PlusDI/MinusDI/CHOP/Aroon/Vortex/KeltnerUpper/KeltnerLower/Supertrend/YangZhangVol | `n` | `n+1` |
| DEMA / TEMA | `2(n-1)` / `3(n-1)` | `2n-1` / `3n-2` |
| HMA | `n + floor(sqrt(n)) - 2` | `n + floor(sqrt(n)) - 1` |
| ADX | `2n-1` | `2n` |
| TR / SAR | `1` | `2` |
| OBV / ADL / Kalman1D / VolumeDelta / CVD | `0` | `2` |
| MACD line / STC / ChaikinOsc(ADOSC) | `slow-1` | `slow` |
| MACD signal/hist | `slow+signal-2` | `slow+signal-1` |

缺失会延长经过的时钟时间；预热数不是保证有效输出的日历长度。达到 lookback 后仍可能遇到数学上无定义的结果：VWMA 总成交量为零、ROC 历史价格为零、CCI 平均偏差为零、WillR 高低价范围为零。Stoch 在平坦区间返回 `50`，RSI 在平坦序列按 tav 口径返回 `100`。MFI 在负资金流为零时沿用 banta/tav 的无定义结果，不强行输出 `100`。产生 NaN 的计算结果沿用现有 `Warmup` 有效性，产生无穷大的结果为 `NonFinite`；这些结果不会变成可交易的有限分数。

全部输入必须具有相同决策周期。跨源周期通过显式 `AsOfField` 或 binding 的 `sampling: asof`、正 `max_age_ms` 采样到决策网格；指标期数随后按有效决策观察计数。小时网格反复采样日频值后做 20 期 SMA，表示 20 个有效小时观察，不代表 20 个交易日。需要原生日频指标时，先在日频源生成，再采样其输出。

`Batch` 从传入历史起点初始化，适用于有明确行数上限的一段历史。历史分块或在线行情应持续推进同一个 `Session`，避免重新初始化 RMA、RSI、ATR、OBV 和 MACD。计算 owner 在内部隔离候选状态，验证后再接纳，避免失败的候选计算污染已接纳状态。
