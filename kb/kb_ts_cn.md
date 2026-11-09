# banbot 时序策略引擎参考（AI 版）

banbot 时序策略（`engine: time_series`，默认）：接口、回调、下单/仓位 API、配置、回测实盘差异、坑、示例。基准 banbot v0.7.1，banta v0.4.1，banexg v0.2.66。截面因子、统计套利是另外的引擎。
每个 `(run_policy, 周期, 品种)` 对应一个 `*strat.StratJob`，每根K线（或自定义数据事件）触发一次回调。

## 0. 快速上手
```go
// 1) 注册：注册名 = 组名:策略名；YAML run_policy[].name 必须完全一致
func init() { strat.AddStratGroup("demo", map[string]strat.FuncMakeStrat{"ma": MaFn}) } // 或 strat.RegisterStrategy("demo:ma", MaFn)
// 2) 工厂：每个 run_policy 调用一次，返回 *strat.TradeStrat；超参用 pol.Def/DefInt
// 3) 入口：package main 匿名 import 策略包，然后 entry.RunCmd()
```

命令 `banbot <cmd> --config x.yml`（`--config` 可重复；`--config-data` 内联YAML；`--no-default` 忽略默认配置文件）：
- `backtest`：`--timerange 20240101-20250101`、`--timestart/--timeend`、`--pairs BTC/USDT,ETH/USDT`、`--timeframes`、`--separate`（各 policy 单独）、`--bt-strict`、`--stake-amount`（覆盖 stake_amount）、`--prg <前缀>`。
- `trade`：实盘/模拟盘（`env: dry_run`）。
- `kline down|load|agg|verify|export|purge|adj-calc`，`series down|list`，`data export|import`，`init`，`optimize`/`bt-opt`（§8），`hub`。

## 1. 公共基础
### 1.1 时间与周期
- 时间戳 **毫秒 int64**。`TimeStart` 本根开始；`TimeStop`/`EndMS` 本根结束（=下一根开始）。
- 周期 `ta.ParseTimeFrame`：`<正整数><单位>`，单位 `s m h d w M(月,大写) q y`，`m`≠`M`。例 `1m 5m 1h 4h 1d 1w 1M`。
- `BarEnv.TFMSecs` 周期毫秒数；`s.Env.BarCount(ms)`：ms 到当前 bar 结束的 bar 数（float64）。

### 1.2 品种与市场
- 品种：现货 `BASE/QUOTE`，合约 `BASE/QUOTE:SETTLE`（交割另带标识段）。`core.SplitSymbol(pair) (base, quote, settle, id)`。
- 市场常量（banexg）：`MarketSpot="spot" MarketMargin MarketLinear MarketInverse MarketOption MarketSwap MarketFuture`；配置 `market_type: spot|linear|inverse|option`，`contract_type: swap|future`。
- `*orm.ExSymbol`：`ID int32`（即 Sid）、`Exchange`、`ExgReal`、`Market`、`Symbol`、`Combined bool`、`ListMs`、`DelistMs int64`、`AggRules string`。`s.Symbol` 为当前任务品种。
- 策略禁止为某交易所写特判分支。

### 1.3 数值与单位约定
- 价格/数量 `float64`；未就绪 `NaN`；
- 比例类：`TradeStrat.StopLoss`、`EnterReq.StopLossRate/TakeProfitRate`、`ExitRate`、`CallbackPct`（百分数，[0.1,10]）；`0.05`=5%，不带杠杆。
- 距离类：`StopLossVal`/`TakeProfitVal` 是**价格距离**（绝对值，非倍率），如 `atr*2`；`StopLoss`/`TakeProfit`/`ActivationPrice` 是**价格**。
- `InOutOrder.ProfitRate` **不含杠杆**（盈亏/入场名义价值）；`Profit` 为扣费后计价货币金额。
- `core.AmtDust=1e-8`；`core.MinStakeAmount=5.5`（最小开单金额，法币）。`LegalCost` 法币金额，`Amount` 标的数量。

### 1.4 banexg 公共类型
- `banexg.Kline`：`Time int64`(ms) `Open High Low Close Volume Quote BuyVolume float64` `TradeNum int64`（同 `ta.Kline`）；`PairTFKline`=Kline+`Symbol TimeFrame`
- `Ticker`：`Symbol TimeStamp Bid/Ask/BidVolume/AskVolume High/Low/Open/Close/Last BaseVolume/QuoteVolume MarkPrice/IndexPrice`；`Trade`：`ID Symbol Side Type Amount Price Cost Order Timestamp Maker Fee`
- `OrderBook`：`Symbol TimeStamp Asks/Bids *OdBookSide`(`Price []float64`、`Size []float64`；bid 降序、ask 升序)
- `Order`：`ID ClientOrderID Status Symbol Type Side Price Average Amount Filled Remaining Cost Fee Timestamp …`；`Position`：`Symbol Side(long/short) Contracts EntryPrice MarkPrice Notional Leverage UnrealizedPnl LiquidationPrice MarginMode …`
- `Market`：`Symbol Base Quote Settle Type Spot Swap Future Linear Inverse Active Taker Maker ContractSize Precision *Precision Limits *MarketLimits`；`Precision`：`Amount Price Base Quote` 及 `Mode*`(DecimalPlace/SignifDigits/TickSize)；`Balances`：`Free Used Total map[string]float64`、`Assets map[string]*Asset`

常量：买卖 `OdSideBuy="buy" OdSideSell="sell"`；订单类型 `OdTypeMarket OdTypeLimit OdTypeLimitMaker OdTypeStop* OdTypeTakeProfit* OdTypeTrailingStopMarket`；状态 `OdStatusOpen/PartFilled/Filled/Canceled/Canceling/Rejected/Expired`（字符串）；持仓方向 `PosSideLong/Short/Both`；保证金 `MarginCross/MarginIsolated`；持仓模式 `PosModeHedge/PosModeOneWay`；TimeInForce `GTC IOC FOK GTX GTD PO`。

### 1.5 core 常量
- `core.OdDirt*`：`OdDirtShort=-1 OdDirtBoth=0 OdDirtLong=1`。**零值是 Both**（`ExitReq{}` 默认平所有方向）。
- `core.OrderType*`：`OrderTypeEmpty=0`（按市价）、`OrderTypeMarket/Limit/LimitMaker`；`core.IsLimitOrder(t)`。
- `core.RunEnvProd="prod" RunEnvTest="test" RunEnvDryRun="dry_run"`。
- 分布 `core.VTypeUniform/VTypeNorm`；`PNorm(min,max)`（均值默认初值）、`PNormF(min,max,mean,rate)`、`PUniform(min,max)`。
- 系统平仓 tag（可自定义）：`stop_loss take_profit trailing_stop draw_down bot_stop pair_del force_exit data_stuck liquidation env_end` 等。

### 1.6 配置（YAML 通用部分）
分层：数据目录 `config.yml` + `config.local.yml`，再叠加 `--config`。根键（全部可选）：
- `name`、`env`：机器人名；prod/test/dry_run
- `market_type`、`contract_type`、`leverage`：市场、合约类型、默认杠杆
- `stake_amount` / `stake_pct` / `max_stake_amt`：单笔基准金额（法币）；百分比（优先于 stake_amount）；上限
- `open_vol_rate`(1)：开单量/近5根平均成交量 上限比
- `min_open_rate`(0.5)、`low_cost_action`：余额不足时开单策略
- `wallet_amounts`：回测初始钱包，如 `{USDT: 10000}`
- `stake_currency`：限定计价币
- `time_start`/`time_end` 或 `timerange`：数据起止（时间戳/日期/日期时间）
- `run_timeframes`：允许的周期（策略可覆盖）
- `run_policy`：策略列表，可同时跑多个
- `pairs`/`pairlists`/`pairmgr`：静态品种/动态过滤器链/定时刷新
- `max_open_orders`、`max_simul_open`：全局最大未平仓数；同一 bar 最大同时开单数
- `order_bar_max`(500)：回测查找"开始前未平仓订单"向前模拟的最大 bar 数
- `bt_net_cost`(15 秒)：回测下单延迟（模拟滑点）
- `bt_strict`、`bt_no_kline_download`、`bt_legacy_intrabar`、`bt_legacy_order_metrics`、`bt_legacy_wallet_price`：回测严格/旧版行为开关
- `close_on_stuck`(20 分钟)：实盘超时无K线则全部平仓
- `fatal_stop`、`fatal_stop_hours`(8)：全局亏损熔断
- `exchange.name`、`exchange.<name>`：交易所（内置 binance/okx/bybit）；`<name>` 下键按 banexg 初始化参数传入（snake_case→camelCase），如 `proxy`、`fees.linear.taker`
- `accounts.<acc>`：`stake_rate stake_pct leverage max_stake_amt max_pair max_open_orders no_trade`；密钥 `<exchange>.<env>.api_key/api_secret`
- `database`：`url`、`db_type: questdb/timescale`（空则自动识别）、`auto_create`、`max_pool_size` 等
- `api_server`、`rpc_channels`、`webhook`、`mail`：服务与通知

`run_policy[]` 键：
- `name`：注册名 `组:策略名`；`engine` 默认 `time_series`
- `run_timeframes`：此策略允许的周期（覆盖根级）；旧键 `timeframes` 为逗号分隔字符串
- `refine_tf`：撮合周期：字符串（`1m`）或整数（相对主周期的倍数，`5`=1/5）
- `filters` / `pairs`：品种过滤器链（格式同 `pairlists` 元素）/ 固定品种列表
- `max_pair`、`max_open`、`max_simul_open`、`order_bar_max`：品种数、开单数上限（`order_bar_max` 覆盖 `TradeStrat.OdBarMax`）
- `stake_rate`：开单倍率（>0 覆盖 `TradeStrat.StakeRate`）
- `stop_loss`：策略默认止损比例：`0.05` 或 `"5%"`（覆盖 `TradeStrat.StopLoss`）；整数写法（如 `5`）报错，0 忽略，负数报错
- `dirt`：`any/long/short`
- `params`：标量超参 `{name: float}`，由 `pol.Def/DefInt` 读取
- `pair_params`：`{pair: {name: value}}` 品种专属超参；存在时对该品种**重新调用工厂**，闭包变量按品种独立
- `strat_perf`：策略-品种绩效自动降权：`enable min_od_num max_od_num min_job_num mid_weight bad_weight`
- 其它未知键：进入 `Policy.More`，用 `pol.More[...]` 读取

超参：`pol.Def("name", dv, core.PNorm(lo,hi)) float64`、`pol.DefInt(...) int`。名称须为合法标识符；值取自 `params`，未配置取 `dv`。`pol` 只读且被所有 job 共享，**超参勿存进结构体或 `More`**。

### 1.7 运行时隔离（v0.5+）
- 工厂签名固定 `func(pol *config.RunPolicyConfig) *strat.TradeStrat`。不得在包级变量保存订单、指标、逐品种状态（放 `StratJob.More`），不得自行保存 Runtime/State 对象。

## 2. banta 指标库（`github.com/banbox/banta`，别名 `ta`）
### 2.1 调用模型
- 输入输出都是 `*ta.Series`，每根K线只算一次并缓存，同参数同 bar 重复调用返回缓存。
- 多输出指标返回多个 `*Series`（或 `.Cols`），一次接收。
- 取值：`x.Get(0)` 当前，`x.Get(1)` 前一根，`x.Get(i)` 往前第 i 根。数据不足输出 NaN（`e.Close.Len()` 判断，或让 `WarmupNum` 覆盖最长周期）。
- 向量（非缓存）版在 `github.com/banbox/banta/tav`（批量研究）；策略回调用 `ta`。
- Series 有界：`MaxCache`(=`NumTaCache`，默认 1500)；达 1.5×MaxCache 时 OHLC 被 Cut 回 MaxCache，历史长度常在 1500~2249。
- 自定义指标（每根K线增量计算，`To` 复用缓存）：

```go
func MyMax(obj *ta.Series, period int) *ta.Series {
	res := obj.To("_mymax", period) // 派生序列按 (名称, period) 缓存
	if res.Cached() {
		return res
	}
	if obj.Len() < period {
		return res.Append(math.NaN())
	}
	return res.Append(slices.Max(obj.Range(0, period))) // Range 返回最近在前
}
```

### 2.2 BarEnv 与 Series
```go
type BarEnv struct { // s.Env 主周期；辅助/自定义用 DataFields.Series()
    TimeStart, TimeStop int64; Exchange, MarketType, Symbol, TimeFrame string; TFMSecs int64
    BarNum, MaxCache, VNum int
    Open, High, Low, Close, Volume, Quote, BuyVolume, TradeNum *Series
}
type Series struct { ID int; Env *BarEnv; Data []float64; Cols []*Series; Time int64; More interface{}; ... }
```

- `Get(i int) float64`：i=0 最新，越界 NaN
- `Range(start, stop int) []float64`：最近在前，`[start, stop)`；返回副本
- `RangeValid(start, stop) ([]float64, []int)`：跳过 NaN，返回值与 bar 偏移
- `Len() int`：历史长度（有 Cols 取第一列）
- `Back(n int) *Series`：向前平移 n 根：`x.Back(1).Get(0)`=上一根值
- `To(name string, v int) *Series`：按 (name, v) 获取/创建派生序列（**推荐**）
- `Cached() bool`：本 bar 是否已计算
- `Append(v)/Set(v)`：写入（`float64/int/[]float64/[]*Series`），仅自定义指标用
- `Add/Sub/Mul/Div/Min/Max(obj)`：`obj` 为 `*Series/int/float32/float64`，返回派生序列
- `Abs()`、`Cut(keep int)`：绝对值；截断历史（之后 `Back/Range` 可用长度变短）
- `Cross(obj) int`、`CrossUp(obj)/CrossDown(obj) bool`：见下
- `CopyTo(e *BarEnv)`：复制到另一 BarEnv
- `BarEnv.OnBar(barMs,o,h,l,c,v,quote,buyVol,tradeNum) error`/`OnBar2(...)`、`ta.NewBarEnv(exg,market,symbol,tf) (*BarEnv,error)`：自建 BarEnv 喂数据（策略回调无需调用）

`Cross`：
- 有符号整数：正=上穿，负=下穿，0=无记录；|值|−1=距最近交叉的 bar 数。**本根刚上穿 `1`，之后 `2,3,…`；下穿 `-1,-2,…`**。
- 事件用 `== 1`/`== -1`；`CrossUp()/CrossDown()`=`Cross>0/<0` 是"最近交叉方向"**状态**。
- 记录从**该 Series 首次调用 Cross 起**，不回放历史；须每根K线无条件调用。
- 比较对象：`Series`（比 `Get(0)`，缓存键为另一序列 ID）或常量（`int`/`float`，缓存键 `int(v*100)`，相差<0.01 的两个常量可能冲突）。

**浮点常量缓存冲突**：`Add/Sub/Mul/Div/Min/Max` 传 `float64` 常量时派生键为 `int(v*10)`，同序列同 bar 内 `Add(0.11)` 与 `Add(0.14)` 返回同一缓存。规避：在 Go 里直接算标量，或用 `int`/不同派生名。

### 2.3 指标清单
约定：签名省略包名。**Go 无默认参数，必须显式传**（仅 `CCI`、`ADX`、`DMI`、`StochF/STOCHF` 的可变参数有隐式默认）；`〔…〕`为默认/常用参数值。
**(全)** = 每个新 bar 复制整个 Series 历史（1500~2249 根）后用 `tav` 向量版全量重算取末值：O(历史) 且有分配（约 2000 根时 Fisher≈32µs/次，SMA<0.1µs/次），开销有界，多品种×多周期×多次调用要控制；同 bar 重复调用仍只算一次。窗口型（Fisher、WilliamsPercent、KST、Ichimoku、DonchianPBand、Correlation）不受 Cut 影响；**递归型（Supertrend、PMAX、Keltner*、VIDYA、SSF、MAMA、DX 等）值依赖窗口起点**，Cut 后与全历史计算略有差异。

**价格与组合**

- `AvgPrice(e *BarEnv)`：(H+L+C)/3
- `HL2(h,l)`、`HLC3(h,l,c)`、`HLCC4(h,l,c)`、`OHLC4(o,h,l,c)`：(H+L)/2、(H+L+C)/3、(H+L+2C)/4、(O+H+L+C)/4
- `Abs(a)`、`Change(a)`、`Sub(a,b)`：\|a\|、a−a[1]、a−b
- `HeikinAshi(e) (ho,hh,hl,hc *Series)`：平均K线

**移动平均**

- `SMA(obj, period)`：简单均值；有效值不足 period 为 NaN
- `EMA(obj, period)`：=`EMABy(obj,period,0)`，α=2/(n+1)
- `EMABy(obj, period, initType int)` 〔0〕：0：前 period 根 SMA 初始化；1：首个有效值初始化
- `RMA(obj, period)`、`SMMA`：=`RMABy(obj,period,0,NaN)`，Wilder α=1/n
- `RMABy(obj, period, initType int, initVal float64)` 〔0, NaN〕：initVal 非 NaN 作首个前值；其余同 EMABy
- `WMA(obj, period)` 〔9〕：线性加权，权重 1..n
- `HMA(obj, period)` 〔9〕：WMA(2·WMA(p/2)−WMA(p), √p)
- `DEMA(obj, period)` / `TEMA(obj, period)`：2·EMA−EMA(EMA) / 3·EMA1−3·EMA2+EMA3
- `T3(obj, period)`：Tillson T3，vFactor 固定 0.7
- `TRIMA(obj, period)`：SMA(SMA(obj,(p+1)/2), p/2+1)
- `ZLMA(obj, period)`：零延迟 EMA：EMA(obj+obj−obj[(p−1)/2])
- `VIDYA(obj, period)`：\|CMO\| 调节 α=2/(p+1) 的变长 EMA，首值 SMA (全)
- `KAMA(obj, period)` 〔10〕：=`KAMABy(obj,period,2,30)`
- `KAMABy(obj, period, fast, slow int)` 〔10, 2, 30〕：效率比 ER 自适应；ER 未就绪 NaN
- `ALMA(obj, period int, sigma, distOff float64)` 〔10, 6.0, 0.85〕：Arnaud Legoux 高斯加权均线
- `SSF(obj, period)`：Ehlers 双极超平滑滤波 (全)
- `SWMA(obj)`：固定 4 点权重 (1,2,2,1)/6
- `MAMA(obj, fast, slow float64) (m, f)`：**非 Ehlers MAMA/FAMA**：p=max(1,round((fast+slow)/2))；m=EMA(obj,p)，f=EMA(m,p) (全)。fast/slow=0.5/0.05 时 p=1，m 即原序列
- `VWMA(price, vol, period)` 〔20〕：Σ(价·量)/Σ量，滚动窗口
- `VWAP(first, second *Series, rest ...*Series)`：累计 VWAP，无周期、无日内重置；2 参 (close, volume)，4 参 (high, low, close, volume)
- `Sum(obj, period)`：滚动和，跳过 NaN；有效值不足 period 为 NaN

**动量与震荡**

- `RSI(obj, period)` 〔14〕：Wilder RSI 0~100；前 period 个增量用简单均值初始化
- `RSI50(obj, period)`：RSI−50
- `ROC(obj, period)` 〔9〕：(close−close[n])/close[n]×100
- `ROCR(obj, period)`：close/close[n]（比值，非百分比）；分母为 0/NaN 时 NaN
- `MOM(obj, period)`：close−close[period]
- `CMO(obj, period)` 〔9〕：=`CMOBy(obj,period,0)`
- `CMOBy(obj, period, maType int)` 〔9, 0〕：Chande 动量震荡 −100~100；0=TA-Lib Wilder 平滑，1=TradingView 简单求和
- `RMI(obj, period, montLen int)` 〔14, 3〕：相对动量指数 0~100（RMA 平滑 montLen 涨跌幅）
- `CRSI(obj, period, upDn, roc int)` 〔3, 2, 100〕：Connors RSI =`CRSIBy(...,0)`
- `CRSIBy(obj, period, upDn, roc, vtype int)` 〔3, 2, 100, 0〕：(RSI + UpDown 的 RSI + 排名)/3；vtype 0=ROC(1) 的 PercentRank，1=ROC(roc)
- `PercentRank(obj, period)`：窗口（period 根，含当前）中**不含当前**且 ≤当前值的个数×100/**period**（最大 (period−1)/period×100）；窗口不足 NaN
- `TD(obj)`：DeMark 连续计数（与 4 根前比较，同向累加，反向重置），前 4 根 NaN
- `UpDown(obj, vtype int)`：连续涨跌计数；0=TradingView 口径，1=经典仅 ±1
- `KDJ(high, low, close, period, sm1, sm2 int) (K, D, RSV)` 〔9, 3, 3〕：RMA 平滑
- `KDJBy(high, low, close, period, sm1, sm2 int, maBy string) (K, D, RSV)` 〔maBy `"rma"`/`"sma"`〕：其他 maBy 值 panic
- `Stoch(high, low, close, period)` 〔14〕：100·(close−LL)/(HH−LL)，区间为 0 输出 50
- `STOCH(close, high, low, period)` 〔14〕：同 Stoch，**参数顺序 close,high,low**（勿混用）
- `StochF(high, low, close, period int, smooth ...int) (fastK, fastD)` 〔smooth 省略或≤0=3〕：fastK 原始值，fastD=SMA(K,smooth)
- `STOCHF(close, high, low, period int, smooth ...int)` 〔同 StochF〕：参数顺序 close,high,low
- `StochRSI(obj, rsiLen, stochLen, maK, maD int) (fastK, fastD)` 〔14,14,3,3〕：RSI→Stoch→SMA(maK)→SMA(maD)
- `WillR(e *BarEnv, period)` 〔14〕：Williams %R −100~0；读 e 的 High/Low/Close，不收 Series
- `WilliamsPercent(high, low, close, period)`：同 %R：−100·(HH−close)/(HH−LL)，窗口含当前 (全)
- `CCI(obj, args ...interface{})` 〔周期 20〕：`CCI(close, period)`；`CCI(high, low, close, period)` 用 HLC3；(src−SMA)/(0.015·AvgDev)
- `AO(high, low, fast, slow int)`：SMA(HL2,fast)−SMA(HL2,slow)
- `DPO(obj, period)`、`Dpo`：pandas-ta 非居中：close−SMA(close,period) 滞后 floor(period/2)+1 根
- `Fisher(high, low, period)`：x=2·((H+L)/2−LL)/(HH−LL)−1（窗口含当前）截断 ±0.999，输出 0.5·ln((1+x)/(1−x))。**无 0.33/0.67 递推平滑、无信号线**；HH==LL 或前 period−1 根为 NaN (全)
- `ULTOSC(high, low, close, short, medium, long int)` 〔7, 14, 28〕：终极振荡器，权重 4/2/1
- `TRIX(obj, period)`：三重 EMA 的单根百分比变化，无信号线
- `TSI(obj, short, long int)`：EMA(EMA(Δ,short),long)/EMA(EMA(\|Δ\|,short),long)×100
- `STC(obj, period, fast, slow int, alpha float64)` 〔12, 26, 50, 0.5〕：Schaff 趋势周期 0~100：MACD→period 窗口百分位→α 平滑→period 窗口百分位→α 平滑；窗口全平为 0；输入 NaN 重置状态；增量
- `KST(obj, r1,r2,r3,r4, s1,s2,s3,s4 int)` 〔常用 10,15,20,30,10,10,10,15〕：Σ wᵢ·SMA(ROC(rᵢ)%, sᵢ)，权重 1,2,3,4；**无信号线**；首个有效值在 max(rᵢ+sᵢ)−1 (全)
- `CTI(obj, period)` 〔20〕：相关趋势指标 −1~1（线性回归相关）
- `Stiffness(obj, maLen, stiffLen, stiffMa int)` 〔100, 60, 3〕：0~100：bound=SMA−0.2·StdDev，统计 stiffLen 内收盘>bound 的根数，再 EMA(stiffMa)
- `DV2(h, l, c, period, maLen int)` 〔252, 2〕：有界 DV2 0~100，maLen 均值的百分位排名（非 backtrader 版）
- `ER(obj, period)` 〔8〕：Kaufman 效率比 0~1=\|净变化\|/Σ\|逐根变化\|

**趋势**

- `MACD(obj, fast, slow, smooth int) (macd, signal)` 〔12, 26, 9〕：SMA 初始化
- `MACDBy(obj, fast, slow, smooth, initType int)` 〔initType 0〕：0=SMA 初始化（国外口径），1=首个有效值（国内/MyTT 口径）
- `ADX(high, low, close, period int, smoothing ...int)` 〔14；smoothing 省略/0=period〕：经典 ADX
- `ADXBy(high, low, close, period, smoothing, method int)` 〔14, 0, 0〕：method 0：RMA 平滑 DX；1：SMA 平滑 DX
- `DX(high, low, close, period)`：**非标准**：单根 \|+DM−(−DM)\|/(+DM+(−DM))×100，原始 DM 未平滑，噪声大；要平滑版用 `ADX`/`DMI` (全)
- `DMI(high, low, close, period int, smoothing ...int) (+DI, −DI, ADX)` 〔smoothing 省略=period〕：smoothing 只作用于 ADX，DI 用 period
- `PluMinDI(high, low, close, period) (+DI, −DI)` 〔14〕：Wilder 口径
- `PluMinDM(high, low, close, period) (+DM, −DM)` 〔14〕：Wilder 平滑和
- `Aroon(high, low, period) (Up, Osc, Dn)`：Up=(period−HighestBar(period+1))/period×100，Dn 同理用 LowestBar；Osc=Up−Dn
- `AroonOsc(high, low, period)`、`AROONOSC`：Up−Dn，−100~100
- `SAR(high, low, step, max float64)`、`PSAR` 〔0.02, 0.2〕：抛物线转向（TA-Lib 口径）：AF 从 step 起、每创新极值 +step、封顶 max；首根 NaN，第二根起以该根最低价为初始 SAR（初始上升）；反转时 SAR 取前极值点；返回**价格**
- `Supertrend(high, low, close, period int, multiplier float64)` 〔常用 10, 3〕：返回**趋势线价格，非方向**：上升段为下轨，下降段为上轨；(H+L)/2±mult·ATR 棘轮，close 穿越对侧轨道翻转；初始 close≥(H+L)/2 为上升。方向用 `line < close`（多头）判断 (全)
- `PMAX(high, low, close, period int, multiplier float64) (line, dir)` 〔常用 10, 3〕：EMA(close,period)±mult·ATR(period) 棘轮线（MA 周期=ATR 周期，不可单独指定），MA 与线交叉翻转；dir=+1 上升（line 为下轨）/−1 下降（上轨）(全)
- `Ichimoku(high, low, close, conversion, base, span int) (conv, base, spanA, spanB, lag)` 〔9, 26, 52〕：conv/base=窗口 (HH+LL)/2；spanA=(conv+base)/2；spanB=span 窗口 (HH+LL)/2；lag=**close[i−base]**（`Close.Get(base)`）。**spanA/spanB 未向前平移 base 根**（比较"当前云层"需自取 `Back(base)`）；conv 前 conversion−1 根、spanB 前 span−1 根 NaN (全)
- `UTBot(c, atr *Series, rate float64)` 〔rate 常用 1〕：信号（**事件，仅突破当根非零**）：1=买，−1=卖，0=无；移动止损线 c±atr·rate 被穿越时触发，线不对外暴露。atr 须自己传入（如 `ta.ATR(...)`）；缓存与状态挂在 atr 上，键为 rate
- `LinReg(obj, period)`：线性回归终点值
- `LinRegAdv(obj, period int, angle, intercept, degrees, r, slope, tsf bool)` 〔全 false=终点值〕：标志选输出，优先级 slope>intercept>angle>r>tsf>终点值
- `LinearRegAngle(obj, period)`、`LINEARREG_ANGLE`：回归斜率的 atan，单位度；前 period−1 根 NaN
- `Slope(obj, period)`：最小二乘斜率
- `Correlation(a, b, period)`：Pearson 相关 (全)
- `CHOP(e *BarEnv, period)` 〔14〕：波动指数；读 e 的 High/Low/Close；越高越震荡，越低越趋势

**波动率与通道**

- `TR(high, low, close)`：真实波幅，首根 NaN
- `ATR(high, low, close, period)` 〔14〕：=RMA(TR, period)
- `NATR(high, low, close, period)`：100·ATR/close
- `StdDev(obj, period)`、`STDDEV` 〔20〕：=`StdDevBy(obj,period,0)` 总体标准差
- `StdDevBy(obj, period, ddof int) (stddev, mean)` 〔ddof 0〕：方差除以 (period−ddof)
- `AvgDev(obj, period)`：Σ\|x−SMA\|/period
- `BBANDS(obj, period int, stdUp, stdDn float64) (upper, mid, lower)` 〔20, 2, 2〕：mid=SMA
- `Donchian(high, low, period) (upper, middle, lower)`：窗口含当前根
- `DonchianPBand(high, low, close, period)`：(close−lower)/(upper−lower)，通道内位置，宽度 0 为 NaN；缓存只挂 close (全)
- `KeltnerChannel(high, low, close, period int, multiplier float64) (upper, mid, lower)`：EMA(close,period) ± mult·ATR(period) (全)
- `KeltnerWBand(high, low, close, period int, mult float64)`：(upper−lower)/mid 相对带宽 (全)
- `Squeeze(high, low, close, period)`：TTM 挤压：BB（SMA±2σ 总体）完全在 KC 内为 1 否则 0；KC=SMA±1.5×窗口平均 TR（SMA 口径）；历史不足 NaN
- `Highest/Lowest(obj, period)`：含当前根的周期最高/最低
- `HighestBar/LowestBar(obj, period)`：最高/最低值距当前的偏移（0=当前）

**成交量**

- `OBV(close, volume)`：累计能量潮：涨加跌减
- `ADL(env)`：累计 A/D 线，读 env.Volume
- `ADOSC(env, fast, slow int)`：EMA(ADL,fast)−EMA(ADL,slow)
- `ChaikinOsc(env, shortLen, longLen int)` 〔3, 10〕：同 ADOSC 口径
- `CMF(env, period)` 〔20〕：Σ(MFV)/Σ量 −1~1；volume≤0 的根输出 NaN
- `MFI(e, period)` 〔14〕：资金流量指数 0~100，典型价×env.Volume
- `EFI(env, period)`：Elder 力量指数：EMA(period) of (close 逐根变化 × volume)，首根 NaN
- `VPCI(close, volume, period)`：**名不副实**：仅 VWMA(close,volume,period)−SMA(close,period)（价量均线差），非完整 VPCI (全)

**形态与枢轴**

- `PivotHigh/PivotLow(src, left, right int)`：右侧 right 根确认后输出枢轴高/低点，否则 NaN，滞后 right 根
- `CDL3INSIDE/CDL3LINESTRIKE/CDL3OUTSIDE/CDLENGULFING(open, high, low, close)`：三内包/三线打击/三外包/吞没；100/−100/0
- `CDLDRAGONFLYDOJI/CDLGRAVESTONEDOJI(open, high, low, close)`：蜻蜓/墓碑十字；100/0
- `CDLHAMMER/CDLHANGINGMAN/CDLMORNINGSTAR/CDLSHOOTINGSTAR(open, high, low, close)`：锤子/上吊/启明星/流星；仅 100/0，不做趋势上下文判断（启明星未判缺口与穿透）

多输入指标缓存只挂在某一输入上（`HLC3`/`AvgPrice`/`DonchianPBand`/`Supertrend`/`Fisher`(挂 high)/`UTBot`(挂 atr) 等）：同一挂载序列配不同其余输入会命中旧值，输入组合必须固定。
`chanlun.go` 的缠论图结构（CGraph/CPoint/CPen/CSeg/CCentre/CTrend）非数值指标，不展开。

## 3. 策略结构与回调
### 3.1 TradeStrat

- `Name`（string）：框架写入
- `Version`（int）：写入订单 `StgVer`
- `WarmupNum`（int）：预热K线数；预热期 `IsWarmUp=true`，下单/平仓入队被忽略
- `OdBarMax`（int）：回测查找未完成持仓最多向前模拟的 bar 数；优先级 `run_policy.order_bar_max` > 本字段 > 全局 `order_bar_max`(500)
- `MinTfScore`（float64）：周期质量阈值，0 时取 0.75（注释写 0.8 已过时）
- `DrawDownExit`（bool）：默认 false；true 对完全入场订单启用"盈利回撤平仓"（§4.2）
- `HedgeOff`（bool）：关闭合约双向持仓（单向模式）
- `BatchInOut`/`BatchInfo`（bool）：主/辅周期 OnData 后 job 是否入批量池（需 `OnBatchJobs`/`OnBatchInfos`）
- `StakeRate`（float64）：开单倍率（乘在 stake 上）；`run_policy.stake_rate>0` 覆盖
- `StopLoss`（float64）：默认止损比例（不带杠杆）；`run_policy.stop_loss` 覆盖
- `StopEnterBars`（int）：限价入场单超过此 bar 数未成交则撤单（0 不启用）
- `OrderOnRotation`（string）：品种切换时旧 job：`close` 平仓 / `hold` 持有不开新单 / `open` 允许开单与持有
- `EachMaxLong`/`EachMaxShort`（int）：单品种同向最大订单数；0 不限，-1 禁止该向
- `TimeFrames`（string）：旧写法：逗号分隔周期
- `RunTimeFrames`（[]string）：允许的周期（空则取 `run_policy.run_timeframes`，再 `TimeFrames`，再根配置）
- `RefineTF`（interface{}）：撮合周期（同 `refine_tf`）
- `WsSubs`（map[string]string）：实盘 Ws 订阅配置（`OnWs*` 需要）
- `Outputs`（[]string）：策略输出文本
- `OnStartUp`（`func(*StratJob)`）：每个 job 创建时调用一次；在此初始化 `s.More`
- `OnDataSubs`（`func(*StratJob) []*DataSub`）：声明额外订阅（辅助周期、其他品种、自定义序列）
- `OnSymbols`（`func([]string) []string`）：改写品种列表
- `OnData`（`FnOnData`）：**主回调** `func(s *StratJob, data DataEvent)`，配 `RouteData`
- `OnBar`/`OnInfoBar`/`OnPairInfos`（旧接口）：仅兼容；**不得与 OnData 同时设置**（构建 panic）
- `OnCheckExit`（`func(*StratJob,*ormo.InOutOrder) *ExitReq`）：每根主K线（非预热）对每个已完全入场且 `CanClose()` 的订单调用；返回非 nil 即平仓，框架强制把返回值的 `OrderID`=该订单、`Dirt`=该订单方向
- `GetDrawDownExitRate`（`func(*StratJob,*ormo.InOutOrder,maxChg float64) float64`）：回撤比例 r；`>0` 采用，`0` 不设置，`<0`/未设回调用默认表
- `OnOrderChange`（`func(*StratJob,*ormo.InOutOrder,chgType int)`）：§3.7
- `OnBatchJobs`（`func([]*StratJob)`）：同一时间戳所有主周期 job（需 `BatchInOut`）
- `OnBatchInfos`（`func(tf string, jobs map[string]*JobEnv)`）：同一时间戳所有辅助 job（需 `BatchInfo`）
- `OnWsTrades(s,pair,[]*banexg.Trade)`/`OnWsDepth(s,*banexg.OrderBook)`/`OnWsKline(s,pair,*banexg.Kline)`/`OnWsData(s,*orm.DataSeries)`（实盘）：逐笔、盘口、实时K线（可能未收盘）、通用时序事件
- `PickTimeFrame`（`func(symbol string, tfScores []*core.TfScore) string`）：为品种选周期（默认取第一个 Score≥MinTfScore 的）
- `OnPostApi`（`func(*core.ApiClient, map[string]interface{}, jobs) error`）：PostAPI 回调
- `OnShutDown`（`func(*StratJob)`）：job 被移除时调用；`OnStratExit`（`func()`）：策略退出时调用一次

注册：`strat.RegisterStrategy(name, fn)`、`strat.AddStratGroup(group, map[string]FuncMakeStrat)`、`strat.GetStrategyFactory(name)`。

### 3.2 回调生命周期（时序）
1. 工厂 `fn(pol)`（有 `pair_params` 时按品种再调）。
2. 每个 (周期, 品种) 创建 `StratJob` → `OnStartUp(job)` → `OnDataSubs(job)`（品种列表变化时重算）。
3. 每个数据事件（按时间序）：`job.SetData(evt)` 更新 `DataFields`（Series 自动追加）、设置 `IsWarmUp` → `OnData(job, DataEvent{Role, Symbol, DataFields})`：`DataRoleMain` 主K线、`DataRoleInfo` 辅助K线、`DataRoleCustom` 自定义序列。主K线后若 `BatchInOut` 入批量池，辅助K线 `BatchInfo` 同理。
4. 主K线事件末（非预热）：`OnCheckExit` 与回撤平仓 → 处理请求队列（回测撮合/实盘提交）。
5. 批量：同时间戳 job 收集后统一调 `OnBatchJobs`/`OnBatchInfos`（实盘有 `DelayBatchMS`=3000ms 等待窗口），再处理请求。
6. `OnOrderChange(job, od, chg)`：订单创建/入场/出场/真实订单变化时触发。
7. 实盘 `OnWs*` 独立于 bar 驱动。
8. 停止：`OnShutDown(job)`（job 移除）、`OnStratExit()`（策略退出）。

- 请求只在**触发它的事件内**有效：入队后本事件末处理，未处理的下一根 `InitBar` 丢弃（实盘 WARN `ignore unhandle Entry/Exit`）。
- 预热期：OnData 照常调用，下单/平仓入队被忽略且返回 nil。
- 同一根K线可多次调用 OpenOrder 产生多个请求，需自行去重（`len(s.Entrys)`、`s.GetOrderNum(0)`；`s.OrderNum` 含已入队请求）。

### 3.3 StratJob（每个 (策略, 周期, 品种) 的运行上下文）
- `Strat *TradeStrat`：所属策略
- `Env *ta.BarEnv`：主周期K线环境（`Env.Close/High/Low/Open/Volume/Quote/BuyVolume/TradeNum`，`Env.TimeStop` 本 bar 结束）
- `DataHub *DataHub`：多订阅数据视图；用 `s.Data(sub)` 或 `data.Series()`
- `Entrys []*EnterReq`、`Exits []*ExitReq`：本事件内待处理请求（只读）
- `LongOrders`、`ShortOrders []*ormo.InOutOrder`：未完成订单快照（以 GetOrders 为准）；`IsWarmUp bool` 是否预热
- `Symbol *orm.ExSymbol`、`TimeFrame`、`Account string`：当前品种、周期、账户
- `OrderNum`、`EnteredNum int`：未完成订单数（含排队中）；已（部分）入场订单数
- `CheckMS`、`LastBarMS int64`：上次处理时间、上一根K线结束时间
- `MaxOpenLong`、`MaxOpenShort int`：0 不限，-1 禁止，>0 上限（由 EachMaxLong/Short 初始化，回调中可改）
- `CloseLong`、`CloseShort bool`：是否允许平多/平空（false 时对应方向平仓报错 `close order disabled`）
- `ExgStopLoss`、`ExgTakeProfit bool`：默认 true。**false 时 `EnterReq` 的止损/止盈（含 LongSLPrice/Strat.StopLoss 默认止损）整个被丢弃，既不挂交易所也不由框架监控**（实盘仅 WARN），并跳过方向校验；不影响 `SetAllStopLoss/od.SetStopLoss` 等运行中设置
- `LongSLPrice`、`ShortSLPrice`：任务级默认止损价（开单时请求未指定则采用）
- `LongTPPrice`、`ShortTPPrice`：**不生效**，止盈写在 `EnterReq`
- `More interface{}`：每个 job 独立的可变状态（在 OnStartUp 初始化）

方法：
- `OpenOrder(req *EnterReq) *errs.Error` / `CloseOrders(req *ExitReq) *errs.Error`：入队开单/平仓，§3.5/§3.6
- `GetOrders(dirt float64) []*ormo.InOutOrder`：`dirt>0` 多、`<0` 空、`0` 全部（空单在前）；快照
- `GetOrderNum(dirt float64) int`：同向未完成订单数
- `Position(dirt float64, enterTag string) float64`：持仓占用成本 ÷ 单笔基准金额（倍数，非数量）；`enterTag` 空不过滤
- `PositionAvgPrice(dirt float64, enterTag string) float64`：持仓均价；dirt 为 0 时 **panic**，必须多或空
- `GetAvgCostPrice(ods ...*ormo.InOutOrder) float64`：指定订单加权入场均价
- `CanOpen(short bool) bool`：按 MaxOpenLong/Short 与现货限制（现货不能开空）判断
- `SetAllStopLoss(dirt float64, args *ormo.ExitTrigger) *errs.Error` / `SetAllTakeProfit(...)`：对方向内订单设置止损/止盈触发；`args=nil` 取消；仅作用于状态在部分入场~部分出场间的订单；`args.Rate∈(0,1)` 时按总持仓×Rate 在订单间分配；`dirt=0` 且多空并存时 panic
- `Data(sub *DataSub) *DataFields`：取订阅视图；`ExSymbol==nil` 为当前品种
- `GetStakeAmount`：`s.Strat.GetStakeAmount(s)` 单笔基准金额（法币）；`OrderCounts() (orderNum, enteredNum int)` 同 OrderNum/EnteredNum

### 3.4 DataEvent / DataFields / DataHub / DataSub
```go
type DataRole uint8 // DataRoleMain=1, DataRoleInfo=2, DataRoleCustom=3
type DataEvent struct { *DataFields; Role DataRole; Symbol *orm.ExSymbol }
func (e DataEvent) IsMain() bool   // 主K线
func (e DataEvent) IsKline() bool  // 主或辅助K线
type DataHandlers struct { Main, Info, Custom FnOnData }
func RouteData(h DataHandlers) FnOnData // 每个事件至多调用一个 handler
```

`DataFields`（内嵌于 DataEvent）：
- 字段：`DoneMS, TimeMS int64`、`Source string`、`Sid int32`、`TimeFrame string`、`Closed, IsWarmUp bool`。
- 方法：`Series(name) *ta.Series`（数值历史序列，无则 nil）、`Float64(name)`（最新值；缺失/无法转换为 0，要区分缺失用 `RawValue`/`Has`）、`Int64(name)`、`String(name)`、`Raw(name) any`、`RawValue(name) (any, bool)`（区分缺失与显式 nil）、`Has(name) bool`。
- 浮点字段默认全部成为 Series；`DataSub.SeriesFields` 非空时仅列出的字段成为 Series；原始值始终可读。

`DataSub`（即 `orm.Subscription`）：
- `Source string`：数据源名；K线 `"kline"`（`orm.SeriesSourceKline`）；自定义序列为注册名
- `ExSymbol *orm.ExSymbol`：nil=当前品种；可指定其他品种（须在品种集合内）
- `TimeFrame string`：周期
- `WarmupNum int`：该订阅预热数量
- `Fields []string`：原始字段投影（K线默认 `open high low close volume quote buy_volume trade_num`）
- `SeriesFields []string`：需要数值历史序列的字段
- `Frequency`/`Projection`（高级，一般留空）：`bar/event`、`default/all/selected`

- `strat.NewDataSub(info *orm.SeriesInfo) *DataSub`：由自定义序列信息派生订阅（`Source`=序列名）。
- `s.DataHub.Get(tf, source, sid)` 取视图；`DataHub.AllReady()` 判断所有订阅是否推进到当前时间。
- 辅助K线信号应基于 `Closed==true`（实盘 WebSocket 数据可能未收盘）。

### 3.5 EnterReq（开仓请求）
- `Tag string`：**必填**，入场信号名
- `Short bool`：true 做空，默认做多
- `OrderType int`：`core.OrderType*`；`Limit>0` 且未指定时自动为限价
- `Limit float64`：限价入场价（指定即限价单）；与 `Stop` 互斥
- `Stop float64`：触发价：做多价格涨到才入场（做空相反）；已越过按限价处理（`Limit=Stop`）；回测 `bt_legacy_intrabar` 下已越过的突破单按当前价入场
- `CostRate float64`：开单倍率，默认 1（乘基准金额）
- `LegalCost float64`：法币金额；指定则忽略 CostRate
- `Amount float64`：标的数量；指定则不按金额计算
- `Leverage float64`：杠杆；非 0 覆盖账户杠杆
- `StopLossVal float64`：止损距离（价格差），换算为止损价；优先于 StopLoss
- `StopLoss float64`：止损触发价；>0 时作为交易所侧止损（`ExgStopLoss` 为 true）
- `StopLossLimit float64`：止损限价；仅设 StopLossLimit 而 StopLoss=0 时，StopLoss 取该值
- `StopLossRate float64`：止损退出比例 (0,1]，0=全部
- `StopLossTag string`：止损原因
- `ActivationPrice`、`CallbackPct float64`：追踪止损：激活价、回撤百分比（[0.1,10]）
- `TakeProfitVal/TakeProfit/TakeProfitLimit/TakeProfitRate/TakeProfitTag`：同止损（距离/价格/限价/比例/原因）
- `StopBars int`：限价单超过多少根K线未成交则撤单（0 用 `StopEnterBars`）
- `ClientID string`：交易所 ClientOrderID 后缀
- `Infos map[string]string`：附加信息（写入订单 Info）
- `Log bool`：失败时记录告警日志
- `CommandID string`：框架幂等 ID（留空）

`OpenOrder` 校验：
1. `Tag` 空、`CanOpen` 假、任何数值 NaN、`CallbackPct` 超 [0.1,10]、`Stop` 与 `Limit` 同设→错误。
2. 入场价：当前价（预热期 `Env.Close`）；限价按方向取更优价。
3. 金额（未指定 Amount/LegalCost）：`LegalCost=基准金额×StakeRate×CostRate`；数量超近 5 根平均成交量×`open_vol_rate` 则截断；低于 `MinStakeAmount`(5.5)：`ignore` 报错；`keepBig` 仅当成本 >5.5 的 40% 时提升到 5.5×1.1，否则报错；`keepAll` 一律提升到 5.5×1.1。
4. 止损优先级：`StopLossVal` > `StopLoss` > `LongSLPrice/ShortSLPrice` > `TradeStrat.StopLoss`（入场价×(1∓比例)）；方向不对（做多止损≥入场价等）→错误；受 `ExgStopLoss` 控制。
5. 止盈：`TakeProfitVal` > `TakeProfit`（`TakeProfitLimit` 同 StopLossLimit 规则）；方向不对→错误；不采用 `LongTPPrice`。
6. 入队返回 nil（`s.Entrys`、`OrderNum++`）；`Log=true` 时错误写日志。

### 3.6 ExitReq（平仓请求）
- `Tag string`：**有订单时必填**
- `Dirt int`：`core.OdDirt*`；零值 Both
- `EnterTag string`：只平入场信号为此值的订单
- `OrderType int`、`Limit float64`：限价退出（`Limit>0` 时须为限价类型，未设自动限价）；`limit_maker` 在有利侧（平多 Limit<现价/平空 Limit>现价）时转为该订单的限价止损
- `ExitRate float64`：退出比例 (0,1]，0 视为 1；>1 报错
- `Amount float64`：退出数量；指定时 ExitRate 无效
- `OrderID int64`：只平指定订单
- `UnFillOnly`/`FilledOnly bool`：只平未入场部分/只平已入场订单
- `Force bool`：强制退出
- `StratName`、`Log`、`CommandID`：同 EnterReq

- 方向内无订单返回 nil（不检查 Tag）。
- `Dirt=Both` 且多空并存又指定 `Limit`→错误（需明确 Dirt）；仅一侧有单则自动取该侧。
- `CloseLong/CloseShort=false` 时对应方向平仓报错。

### 3.7 订单对象 ormo
`*ormo.InOutOrder`（一次"入场+出场"完整订单）：`*IOrder` + `Enter, Exit *ExOrder`（`Exit` 可能 nil）+ `Info map[string]interface{}`。

`IOrder`：`ID, TaskID int64`；`Symbol string`；`Sid int64`；`Timeframe string`；`Short bool`；`Status int64`；`EnterTag, ExitTag string`；`Stop float64`；`InitPrice`（信号价）；`QuoteCost`（计价金额）；`Leverage`；`EnterAt, ExitAt int64`(ms)；`Strategy string`；`StgVer int64`；`MaxPftRate, MaxDrawDown, ProfitRate, Profit float64`；`Info string`。
`ExOrder`（一笔真实/模拟交易所订单）：`ID, TaskID, InoutID int64`；`Symbol`；`Enter bool`；`OrderType, OrderID, Side string`；`CreateAt, UpdateAt int64`；`Price, Average, Amount, Filled float64`；`Status int64`；`Fee, FeeQuote float64`；`FeeType string`。
常量：`InOutStatusInit=0 PartEnter=1 FullEnter=2 PartExit=3 FullExit=4 Delete=5`（前缀 `ormo.InOutStatus`）；ExOrder 状态 `OdStatusInit=0 PartOK=1 Closed=2`。

- `EnterCost()`：Enter.Filled×均价
- `HoldCost()`/`HoldAmount()`：未平仓占用成本/数量
- `CalcProfit(price)`：按价格算盈亏（未扣费）
- `UpdateProfits(price)`：刷新 Profit/ProfitRate/MaxPftRate/MaxDrawDown
- `CanClose() bool`：是否可平仓
- `SetStopLoss(*ExitTrigger) *errs.Error`/`SetTakeProfit(...)`：设置/修改止损、止盈
- `SetExitTrigger(key string, args, price)`：通用触发器（key `ormo.OdInfoStopLoss/OdInfoTakeProfit`）
- `GetStopLoss()/GetTakeProfit() *TriggerState`：当前触发器状态（Hit、OrderId 等）
- `SetTrailingStop(callbackPct, activationPrice float64) *errs.Error`：追踪止损
- `SetEnterLimit(price) *errs.Error`：修改未成交入场限价
- `SetInfo(key,val)`、`GetInfoFloat64/Int64/String(key)`：订单扩展信息
- `RealEnterMS()/RealExitMS() int64`：实际入/出场时间(ms)
- `Key()/KeyAlign() string`：唯一键
- `Clone() *InOutOrder`

`ExitTrigger{Price, Limit, Rate float64; Tag string}`：`Price` 触发价，`Limit` 触发后限价（0=市价），`Rate` 退出比例 (0,1]（0=全部）。

订单事件（`OnOrderChange` 的 chgType，`strat.OdChg*`）：`OdChgNew`(0) 新订单、`OdChgEnter`(1) 入场请求、`OdChgEnterFill`(2) 入场完成、`OdChgExit`(3) 出场请求、`OdChgExitFill`(4) 出场完成、`OdChgOrderChanged`(5) 真实订单被确认/修改/撤销（不含合成成交）。

### 3.8 常用模式
- 同 bar 只开一次：`len(s.Entrys)==0 && s.GetOrderNum(0)==0`。
- 空单移动止损：`s.SetAllStopLoss(core.OdDirtShort, &ormo.ExitTrigger{Price: s.Env.Close.Get(0)+1.5*atr})`。

## 4. 下单、止损止盈与仓位细则
### 4.1 开仓与平仓
- 回测成交时间=下单时间+`bt_net_cost` 秒，价格取其后价格路径（§6）。
- 默认市价（`OrderTypeEmpty` 同）。`Limit` 限价单可配 `StopBars`；`OrderTypeLimitMaker` 要求挂单侧（做多限价≤市价，做空≥市价），否则报错（币安合约无 limit_maker，banexg 转 limit+GTX）。
- 杠杆：`EnterReq.Leverage` 优先，其次账户 `leverage`。
- 仓位大小：`stake_amount`/`stake_pct`（账户）× `accounts.<acc>.stake_rate` × `run_policy.stake_rate` × `TradeStrat.StakeRate` × `CostRate`；`max_stake_amt` 封顶；`strat_perf` 降权再乘系数。
- 平仓：默认全平市价；`ExitRate=0.5` 平一半（每订单按比例）；`EnterTag`/`OrderID` 过滤；`Limit` 限价。部分平仓后剩余订单仍在 `GetOrders`。

### 4.2 止损与止盈
- `StopLossVal/TakeProfitVal`（距离）最常用，`StopLoss/TakeProfit` 为绝对价；优先级见 §3.5。`*Rate` 触发后退出比例（0=全部），`*Limit` 触发后挂限价单（否则市价）。
- 追踪止损：`ActivationPrice`（达到后激活）+ `CallbackPct`（从最高点回撤%，[0.1,10]）。运行中修改：`s.SetAllStopLoss(dirt, &ormo.ExitTrigger{Price: …})` 或 `od.SetStopLoss(...)`。

**交易所侧挂单**：
- 回测、`dry_run`：本地模拟触发，不访问交易所。
- 实盘（`prod`）：订单**完全入场后**（`InOutStatusFullEnter`，部分成交期间不挂）为止损、止盈各单独调用 `CreateOrder` 挂条件单：止损 `stop_loss`（有 Limit 为 `stop_loss_limit`），止盈 `take_profit_market`（有 Limit 为 `take_profit_limit`），数量=当前持仓×Rate（0<Rate<1 时）；不用 ClosePosition（避免多订单互相覆盖），双向持仓靠 positionSide。追踪止损（`CallbackPct`）挂 `trailing_stop_market`（callbackRate、可选 activationPrice）；框架 tag 为 `trailing_stop` 的止损不另挂止损单。
- banexg 适配：币安（现货/杠杆 STOP_LOSS(_LIMIT)/TAKE_PROFIT(_LIMIT)；合约 STOP_MARKET/STOP/TAKE_PROFIT_MARKET/TAKE_PROFIT/TRAILING_STOP_MARKET）、OKX（algo）、Bybit（conditional）均实现；不支持的品类/类型下单报错。
- 失败：报"会立即触发"→立即市价平仓；其它失败只记 ERROR，更新失败不撤旧单，**无自动重试**，仅同步/重启时补挂缺失触发单。`SetAll*`/`od.SetStopLoss` 修改后实盘重编辑/重挂条件单。
- `ExgStopLoss/ExgTakeProfit=false` 见 §3.3（回测同样）；要保留监控而不挂单，需自行在 `OnData`/`OnCheckExit` 检查平仓。
- 回撤平仓（`DrawDownExit=true`）：框架跟踪最大浮盈比例 maxChg；r 取 `GetDrawDownExitRate`（`>0` 采用、`0` 不设置、`<0`/未设用默认表）：maxChg >10%→r=0.15，>4%→0.17，>2.5%→0.25，>1.5%→0.37，>0.7%→0.5，否则不启用。回撤价=入场价×(1+maxChg×(1−r)×方向)，当前收盘越过则 tag `draw_down` 市价平仓，否则**用该价覆盖订单的止损触发器**（会替换你设置的止损，实盘同步到交易所）。仅在 `OnCheckExit` 返回 nil 时检查。

### 4.3 多空限制
- `MaxOpenLong/Short`（job 级，由 `EachMaxLong/Short` 初始化）决定能否开新单；现货不能开空。反手：同一回调调 `CloseOrders` 与 `OpenOrder`，本事件末统一处理。

## 5. 自定义时序数据

### 5.1 写入（数据源/导入器/应用层，勿在策略回调中写库）
```go
info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{{Name: "rate", Type: "float"}})
store := orm.NewSeriesStore(orm.NewSeriesRepo(storage)) // storage: 当前 Runtime 的 *orm.Storage
_ = store.Ensure(ctx, info)
_ = store.Write(ctx, info, exs, &orm.DataRecord{TimeMS: startMS, EndMS: endMS, Values: map[string]any{"rate": rate}})
rows, _ := store.Read(ctx, info, exs, startMS, endMS, 0) // []*orm.DataSeries
v, err := rows[0].FloatValue("rate")
```
- `SeriesField.Type`：`float int string bool json`。`SeriesStore`：`Ensure/Write/WriteBatch/WriteSeries/Read/Delete/Missing/FillMissing(ctx,info,exs,start,end,fetch)/Coverage`。
- K线扩展列（逐 `(sid,time)`）：`orm.NewKLineSeriesInfo` + `orm.NewKLineSeriesStoreWithStorage`（只更新已存在的K线行）。
- QuestDB WAL 表写后读异步可见：写入后立即读可能看不到新行，需等待可见。

### 5.2 数据源注册（回测/实盘自动补齐与推送）
- `data.RegisterFuncDataSource(info, fetchHistory, subscribeLive)`：`FetchHistoryFunc func(ctx, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)`；`SubscribeLiveFunc func(ctx, subs []*strat.DataSub, sink data.DataSink) error`（可 nil）。
- `data.RegisterDataSource(src)`/`RegisterDataSourceFactory(name, f)`：`DataSource{Info(); FetchHistory(); SubscribeLive()}`。
- 无历史拉取的源回测不可用。

### 5.3 策略消费
```go
// info 同 §5.1
OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
    sub := strat.NewDataSub(info)     // ExSymbol 为 nil = 当前品种
    sub.WarmupNum = 20
    sub.Fields = []string{"rate"}
    sub.SeriesFields = []string{"rate"}
    return []*strat.DataSub{sub}
},
OnData: strat.RouteData(strat.DataHandlers{
    Custom: func(s *strat.StratJob, d strat.DataEvent) { /* d.Float64("rate") / d.Series("rate") */ },
    Main:   func(s *strat.StratJob, d strat.DataEvent) { /* 读取 s.More 中的状态 */ },
}),
```
- 自定义事件只进入 `Custom`；`s.Env` 不对应它。
- 重采样聚合：内置 `min max last first sum avg mid`，用 `ExSymbol.SetAggRules(map[field]rule)` 配置；特殊语义用 `orm.RegisterAggRule(name, fn)`。

## 6. 回测与实盘差异
- 成交时间：回测=下单时间+`bt_net_cost` 秒（默认15）；实盘=交易所回报（模拟盘本地撮合）。
- 价格路径：回测依 `refine_tf` 或 1m 数据；`bt_legacy_intrabar` 回退旧版 bar 内路径；实盘为实时行情。
- 历史缺口：`bt_no_kline_download` 禁止隐式下载；`historical_coverage` 配置覆盖检查。
- `bt_strict: true` 固定关键执行顺序（可复现，可能较慢），仅回测。
- 实盘请求下一根K线到达前未处理→丢弃并 WARN；bar 过期后的入场被跳过（`bar expired`）。
- 止损止盈：回测/`dry_run` 本地模拟；`prod` 为交易所条件单（§4.2）。
- 断线：`close_on_stuck`（分钟，默认 20）超时无K线则全部平仓。
- 账户：回测 `wallet_amounts`；实盘为交易所余额与持仓。
- 不要依赖"回测成交价=当前收盘价"。
- 实盘重启后未完成订单重新同步到 `LongOrders/ShortOrders`；`OnStartUp` 重新执行、`More` 重建，跨重启状态写订单 `Info` 或外部存储。

## 7. 关键规则与常见坑
1. **OnData 与 OnBar/OnInfoBar 不能同时设置**（构建 panic）；新策略只用 `OnData`+`RouteData`，别把 `OnBar` 直接改名为 `OnData`（须区分 Main 与 Info）。
2. **指标每根K线无条件计算**再用 `if` 判断（放进分支会使缓存序列错位）。
3. **派生序列用 `Series.To`**，别在回调反复 `NewSeries`，否则序列数增长触发致命错误 `series too many … try replace NewSeries with Series.To` 并停机。
4. 浮点常量缓存冲突、`Cross` 用 `== 1`（§2.2）。
6. **比较 Close 与 Highest/Lowest 前先 `Back(1)`**，否则当前收盘永远无法突破含自身的最高价：`e.Close.Get(0) > ta.Highest(e.High, n).Back(1).Get(0)`。
7. `s.Env` 是主周期，读辅助/自定义数据用 `data.Series(...)`。
9. `OpenOrder` 的 `Tag` 必填；`CloseOrders` 有订单时 `Tag` 必填。`ExitReq{}` 的 `Dirt` 零值是 Both，只平多/空须显式设 `core.OdDirtLong/OdDirtShort`。
10. 超参只读共享：不要写入 `More`；按品种不同用 `pair_params`。`More` 只存每品种会变化的数据。
11. `GetOrders/GetOrderNum` 基于快照：同回调内刚 `OpenOrder` 不会出现；切片不要跨事件复用。
13. 回调里不调交易所接口/阻塞 IO（外部数据用 §5 自定义序列）；不用包级全局遍历品种/其他 job（跨品种汇总用 `OnBatchJobs/OnBatchInfos`）。
14. 多输入指标缓存只挂一个输入，组合须固定；标 (全) 指标勿在循环里多次调用（§2.3）。
16. 对 `Series.Get` 的 NaN 做判断（`math.IsNaN(atr)`），尤其 `WarmupNum` 不够大时。
17. 同品种不同周期的 job 是独立 Env；跨周期数据用 `DataSub`。
18. `DrawDownExit` 覆盖订单止损触发器（§4.2）；`ExgStopLoss=false` 丢弃入场止损（§3.3）；`MAMA/VPCI/DX/Fisher` 与标准定义不同；旧 API 新代码不用。

## 8. 超参与优化
超参用 `pol.Def/DefInt` 声明（分布见 §1.5；`DefInt` 自动取整）。`banbot optimize --config x.yml`（另有 `bt-opt` 滚动回测）；`--sampler tpe|bayes|random|cmaes|ipop-cmaes|bipop-cmaes`（默认 bayes），`--opt-rounds`（默认 30）。

## 9. 完整示例
### 示例 A：单周期 EMA 交叉 + ATR 止损止盈

```go
package demo

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

func init() {
	strat.RegisterStrategy("demo:ma_atr", MaAtr)
}

// MaAtr: EMA 金叉做多、死叉做空，ATR 倍数止损止盈。
func MaAtr(pol *config.RunPolicyConfig) *strat.TradeStrat {
	fast := pol.DefInt("fast", 10, core.PNorm(5, 30))
	slow := pol.DefInt("slow", 30, core.PNorm(20, 100))
	atrLen := pol.DefInt("atrLen", 14, core.PNorm(7, 30))
	slMul := pol.Def("slMul", 2, core.PNorm(1, 4))
	tpMul := pol.Def("tpMul", 4, core.PNorm(2, 8))
	return &strat.TradeStrat{
		WarmupNum:    slow * 3,
		EachMaxLong:  1,
		EachMaxShort: 1,
		OnData: strat.RouteData(strat.DataHandlers{
			Main: func(s *strat.StratJob, _ strat.DataEvent) {
				e := s.Env
				cross := ta.EMA(e.Close, fast).Cross(ta.EMA(e.Close, slow)) // ==1 仅本根K线金叉
				atr := ta.ATR(e.High, e.Low, e.Close, atrLen).Get(0)
				if cross == 1 {
					s.CloseOrders(&strat.ExitReq{Tag: "golden_close_short", Dirt: core.OdDirtShort})
					s.OpenOrder(&strat.EnterReq{Tag: "golden_long", StopLossVal: atr * slMul, TakeProfitVal: atr * tpMul})
				} else if cross == -1 {
					s.CloseOrders(&strat.ExitReq{Tag: "death_close_long", Dirt: core.OdDirtLong})
					s.OpenOrder(&strat.EnterReq{Tag: "death_short", Short: true, StopLossVal: atr * slMul, TakeProfitVal: atr * tpMul})
				}
			},
		}),
	}
}
```

```yaml
run_timeframes: [1h]
pairs: [BTC/USDT:USDT]
market_type: linear
stake_amount: 100
run_policy:
  - name: demo:ma_atr
    params: {fast: 12, slow: 40}
    stop_loss: 0.1
```

示例 A、§2.1 `MyMax`、§5 代码已通过 `go vet`；其余为接口片段。辅助周期：Info 处理器 `!data.Closed||data.IsWarmUp` 则 return，用 `data.Series("close")`（§3.4/§5.3/§10）。超时平仓：`OnCheckExit` 判断 `s.Env.BarCount(od.EnterAt)>=N` 返回 `&strat.ExitReq{Tag:"timeout"}`。

## 10. 旧接口迁移（OnBar → OnData）
- 旧 `OnBar(s)`：改为 `OnData: strat.RouteData(strat.DataHandlers{Main: func(s, _ strat.DataEvent){…}})`。
- 旧 `OnInfoBar(s, e, pair, tf)`：放入 `Info` 处理器，用 `data.Series("close")` 等替代 `e.Close`，并在 `OnDataSubs` 声明 `{Source:"kline", TimeFrame: tf}`。
- 旧 `OnPairInfos`：改为 `OnDataSubs`（`ExSymbol` nil=当前品种）。
- 旧 `ta.Cross(a, b)`：改为 `a.Cross(b)`。
