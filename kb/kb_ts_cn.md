# banbot 时序策略引擎参考（AI 版）

banbot 时序策略（`engine: time_series`，默认）：接口、回调、下单/仓位 API、配置、回测实盘差异、坑、示例。基准 banbot v0.7.6，banta v0.5.0，banexg v0.2.67。截面因子、统计套利是另外的引擎。
每个 `(run_policy, 周期, 品种)` 对应一个 `*strat.StratJob`，每根K线（或自定义数据事件）触发一次回调。

## 0. 快速上手
```go
// 1) 注册：注册名 = 组名:策略名；YAML run_policy[].name 必须完全一致
func init() { strat.AddStratGroup("demo", map[string]strat.FuncMakeStrat{"ma": MaFn}) } // 或 strat.RegisterStrategy("demo:ma", MaFn)
// 2) 工厂：每个 run_policy 调用一次，返回 *strat.TradeStrat；超参用 pol.Def/DefInt，跨参数约束用 pol.Require/Requiref
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
- **UTC 虚拟交易时段原语（`github.com/banbox/banbot/btime`）**：
  - 锚点常量（距 `00:00 UTC` 分钟数）：`UTC0Minutes=0`、`AsiaOpenMinutes=0`、`AsiaCloseMinutes=480`(`08:00`)、`LondonOpenMinutes=420`(`07:00`)、`LondonCashOpenMinutes=480`(`08:00`)、`LondonCloseMinutes=960`(`16:00`)、`NYKillzoneOpenMinutes=720`(`12:00`)、`NewYorkOpenMinutes=810`(`13:30`)、`USCashCloseMinutes=1200`(`20:00`)、`NewYorkCloseMinutes=1260`(`21:00`)、`CMEOpenMinutes=1380`(`23:00`)、`CMECloseMinutes=1320`(`22:00`)。
  - 预定义窗口（`btime.UTCSessionWindow{Name, StartMin, EndMin}`，方法 `.Contains(ms)`/`.DurationMinutes()`）：`SessionUTC0`、`SessionAsia`(`00:00–08:00`)、`SessionLondon`(`07:00–16:00`)、`SessionNewYork`(`13:30–21:00`)、`SessionUSCash`(`13:30–20:00`)、`SessionCME`(`23:00–22:00` 跨午夜)、`KillzoneAsia`(`00:00–04:00`)、`KillzoneLondon`(`07:00–10:00`)、`KillzoneNewYork`(`12:00–15:00`)、`KillzoneLondonExit`(`15:00–17:00`)。
  - 入场时段/交易窗口过滤：`btime.BarInUTCWindow(e.TimeStart, e.TimeStop, startMin, endMin) bool`（本根 bar 与窗口有重叠即为 true；bar 不短于窗口间隔时恒为 true，粗周期不会因时段过滤零交易）；星期过滤：`btime.BarTouchesUTCWeekdays(e.TimeStart, e.TimeStop, time.Monday, ...) bool`（bar 覆盖任一指定 UTC 星期即为 true，1d/3d bar 不再依赖开盘那天）。
  - 函数：`btime.InUTCWindow(ms, startMin, endMin) bool`（只判断单个时间戳；半开区间 `[startMin, endMin)`，`startMin > endMin` 自动按跨午夜窗口判定）；`btime.SessionDayIndex(ms, anchorMinutesUTC) int64`（欧几里得向下整除切分任意锚点交易日）；`btime.SessionStartMS(ms, anchorMinutesUTC) int64`；`btime.MinutesIntoSession(ms, anchorMinutesUTC) int`（`[0, 1439]`）；`btime.IsNearSessionEnd(e.TimeStart, e.TimeStop, closeMinutesUTC) bool`（自适应任意 `<1d` 周期判定本根 bar 到达或跨过日终清仓时刻，支持负偏移如 `-15`=`23:45 UTC`；`>=1d` 安全返回 `false`）。
- **UTC 日历、季节性、月末月初、交割日与跨周期定投原语（`github.com/banbox/banbot/btime`）**：
  - 日历分解与月末月初（零分配，自动兼容平/闰年与大小月）：`btime.UTCCalendarParts(ms) (year int, month time.Month, day int, weekday time.Weekday, hour, minute int)`、`btime.IsLeapYearUTC(year) bool`、`btime.DaysInMonthUTC(year, month) int`、`btime.DaysFromMonthStartUTC(ms) int`（当月 1 日为 `0`）、`btime.DaysBeforeMonthEndUTC(ms) int`（当月最后一天如 2 月 28/29 日或 3 月 31 日为 `0`）、`btime.IsTurnOfMonthUTC(ms, lastNDays, firstMDays) bool`（判断处于月末最后 `lastNDays` 天或月初前 `firstMDays` 天的 Turn-of-Month 窗口）。
  - 星期、月内周次与月度交割日：`btime.DayOfWeekUTC(ms) int`（`0=Sunday..6=Saturday`）、`btime.ISODayOfWeekUTC(ms) int`（ISO `1=Monday..7=Sunday`）、`btime.WeekOfMonthUTC(ms) int`（`1..5`，按 `(day-1)/7+1`）、`btime.NthWeekdayOfMonthUTC(year, month, weekday, n) int` 与 `btime.IsNthWeekdayOfMonthUTC(ms, weekday, n) bool`（如 `n=3, time.Friday` 美股/CME 月度期权交割日；不存在返 `0`/`false`）、`btime.LastWeekdayOfMonthUTC(year, month, weekday) int` 与 `btime.IsMonthlyExpiryDayUTC(ms, weekday) bool`（如 `btime.IsMonthlyExpiryDayUTC(ms, time.Friday)` 判定加密市场每月最后一个周五期权/季度交割日）。
  - 季节性月份窗口与跨周期定投触发：`btime.InMonthRangeUTC(ms, startMonth, endMonth) bool`（`1..12` 闭区间，`startMonth > endMonth` 自动跨年如 `InMonthRangeUTC(ms, 11, 4)` 表示 11 月至次年 4 月 Halloween 效应）；`btime.IsFirstBarOfPeriodUTC(prevBarMS, curBarMS, period) bool`（`period` 支持 `"day"`/`"week"`[UTC 周一]/`"month"`/`"quarter"`/`"year"`；在 `s.More` 中无条件维护上一根 `e.TimeStart` 传入，首根 `prevBarMS <= 0` 或乱序安全返回 `false`，在 `5m/15m/1h/4h/1d/3d` 任意周期上均保证每期精确触发一次，避免写死 `day == 1 && hour == 0` 在粗周期或非零点偏移时漏触发）。

### 1.2 品种与市场
- 品种：现货 `BASE/QUOTE`，合约 `BASE/QUOTE:SETTLE`（交割另带标识段）。`core.SplitSymbol(pair) (base, quote, settle, id)`。
- 市场常量（banexg）：`MarketSpot="spot" MarketMargin MarketLinear MarketInverse MarketOption MarketSwap MarketFuture`；配置 `market_type: spot|linear|inverse|option`，`contract_type: swap|future`；线性永续策略若需同时订阅或交易同交易所现货标的（如 `BTC/USDT` 现货 + `BTC/USDT:USDT` 永续做期现基差/资金费套利），可在根配置加 `extra_markets: [spot]`（现货腿仅支持做多、无资金费）。
- `*orm.ExSymbol`：`ID int32`（即 Sid）、`Exchange`、`ExgReal`、`Market`、`Symbol`、`Combined bool`、`ListMs`、`DelistMs int64`、`AggRules string`。`s.Symbol` 为当前任务品种。
- 策略禁止为某交易所写特判分支。

### 1.3 数值与单位约定
- 价格/数量 `float64`；未就绪 `NaN`；
- 比例类：`TradeStrat.StopLoss`、`EnterReq.RiskRate/StopLossRate/TakeProfitRate`、`TakeProfitTarget.Rate/ExitRate`、`ExitRate`、`CallbackPct`（百分数，[0.05,50]）；`0.05`=5%（`RiskRate: 0.01`=1% 账户权益风险），不带杠杆。
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
- `market_type`、`contract_type`、`leverage`、`extra_markets`：市场、合约类型、默认杠杆、额外挂载市场（如 `[spot]`）
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

超参：`pol.Def("name", dv, core.PNorm(lo,hi)) float64`、`pol.DefInt(...) int`。名称须为合法标识符；值取自 `params`，未配置取 `dv`。跨参数约束在工厂函数内用 `pol.Require(cond, msg)` 或 `pol.Requiref(cond, format, args...)` 断言（如 `pol.Require(fast < slow, "require fast < slow")`），条件不满足时以 `Error: [BadConfig] <msg>` 拒绝该组参数（参数搜索将其标为无效试验跳过，勿手写普通 `panic`）。`pol` 只读且被所有 job 共享，**超参勿存进结构体或 `More`**。

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
type BarEnv struct { // s.Env 主周期；辅助/跨品种K线用 s.RefEnv(symbol, tf) 或 DataFields.BarEnv()
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
- 比较对象：`Series`（比 `Get(0)`）或数值常量（`int`/`float64`，缓存键基于完整位模式 `math.Float64bits`，任意精度常量互不冲突）。
- **合成砖图（Renko / Range / Volume / Dollar Bars）**：在 `s.More` 持有构建器（如 `ta.NewRenkoATR(14, 1.0)`、`ta.NewRangeBarPct(0.005)`、`ta.NewDollarBarBuilder(quoteThresh)`），在 `1m`/`5m` 主回调中调用 `builder.UpdateFromEnv(e, func(renkoEnv *ta.BarEnv) { ... })`：每当闭合一根或多根合成砖时按因果时间顺序回调 `renkoEnv`，内部可直接调用全部 `ta.*` 指标。

### 2.3 指标清单
约定：签名省略包名。**Go 无默认参数，必须显式传**（仅 `CCI`、`ADX`、`DMI`、`StochF/STOCHF` 的可变参数有隐式默认）；`〔…〕`为默认/常用参数值。
所有常用递推与滑窗指标均为 O(1)/O(W) 增量状态机，且多输入指标的缓存键包含全部输入 `*Series` 身份与精确浮点参数位模式（不同输入组合或浮点参数互不串缓存）；仅极少数遗留兼容包装标 **(全)**（有标准增量替代版）。

**价格、组合与合成砖**

- `AvgPrice(e *BarEnv)`：(H+L+C)/3
- `HL2(h,l)`、`HLC3(h,l,c)`、`HLCC4(h,l,c)`、`OHLC4(o,h,l,c)`：(H+L)/2、(H+L+C)/3、(H+L+2C)/4、(O+H+L+C)/4
- `Abs(a)`、`Change(a)`、`Sub(a,b)`：\|a\|、a−a[1]、a−b
- `HeikinAshi(e) (ho,hh,hl,hc *Series)`：平均K线
- `NewRenkoFixed(brickSize float64)`、`NewRenkoPct(pct float64)`、`NewRenkoATR(atrPeriod int, atrMult float64) *RenkoBuilder`：固定点差/百分比/动态 ATR 砖形图构建器（支持 `.UpdateFromEnv(srcEnv, onBrick)`）
- `NewRangeBarBuilder(rangeSize float64)`、`NewRangeBarPct(pct float64) *RangeBarBuilder`、`NewVolumeBarBuilder(volumeThreshold float64) *VolumeBarBuilder`、`NewDollarBarBuilder(quoteThreshold float64) *DollarBarBuilder`：等波幅 Range / 等量 / 等成交额合成 K 线构建器

**移动平均与在线滤波**

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
- `VIDYA(obj, period)`：\|CMO\| 调节 α=2/(p+1) 的变长 EMA，首值 SMA
- `KAMA(obj, period)` 〔10〕：=`KAMABy(obj,period,2,30)`
- `KAMABy(obj, period, fast, slow int)` 〔10, 2, 30〕：效率比 ER 自适应；ER 未就绪 NaN
- `ALMA(obj, period int, sigma, distOff float64)` 〔10, 6.0, 0.85〕：Arnaud Legoux 高斯加权均线
- `McGinleyDynamic(src, period)`：McGinley 自适应动态均线
- `SSF(obj, period)`：Ehlers 双极超平滑滤波（SuperSmoother）
- `Butterworth2(src, period)`、`GaussianFilter(src, period, poles int)`〔poles 1..4〕、`LaguerreFilter(src, gamma float64)`〔0..1〕：二阶巴特沃斯 / 多极高斯 / Ehlers Laguerre 低延迟滤波器
- `Kalman1D(src, processNoise, measNoise float64)`、`KalmanTrend(src, processNoise, measNoise float64) (level, velocity)` 〔常用 0.01, 0.1〕：一维随机游走与二维局部线性趋势（位置+速度）在线卡尔曼滤波
- `SWMA(obj)`：固定 4 点权重 (1,2,2,1)/6
- `EhlersMAMA(obj, fastLimit, slowLimit float64) (mama, fama)` 〔0.5, 0.05〕：标准 Ehlers Hilbert 相位自适应 MAMA/FAMA（旧 `MAMA(obj, fast, slow)` 为简化兼容函数 (全)，新代码用 `EhlersMAMA`）
- `VWMA(price, vol, period)` 〔20〕：Σ(价·量)/Σ量，滚动窗口
- `VWAP(first, second *Series, rest ...*Series)`：累计不重置 VWAP；按 UTC 时段锚点重置用下文的 `SessionVWAP`/`SessionVWAPBands`/`WeeklyVWAP`
- `Sum(obj, period)`：滚动和，跳过 NaN；有效值不足 period 为 NaN

**动量与震荡**

- `RSI(obj, period)` 〔14〕：Wilder RSI 0~100；前 period 个增量用简单均值初始化
- `RSI50(obj, period)`：RSI−50
- `LaguerreRSI(src, gamma float64)` 〔0.5〕：Ehlers 四阶 Laguerre RSI（`0~100`）
- `ROC(obj, period)` 〔9〕：(close−close[n])/close[n]×100
- `ROCR(obj, period)`：close/close[n]（比值，非百分比）；分母为 0/NaN 时 NaN
- `MOM(obj, period)`：close−close[period]
- `PPO(src, fast, slow, signal int) (ppo, sig, hist)` 〔12, 26, 9〕：百分比价格振荡器
- `CMO(obj, period)` 〔9〕：=`CMOBy(obj,period,0)`
- `CMOBy(obj, period, maType int)` 〔9, 0〕：Chande 动量震荡 −100~100；0=TA-Lib Wilder 平滑，1=TradingView 简单求和
- `RMI(obj, period, montLen int)` 〔14, 3〕：相对动量指数 0~100（RMA 平滑 montLen 涨跌幅）
- `CRSI(obj, period, upDn, roc int)` 〔3, 2, 100〕：Connors RSI =`CRSIBy(...,0)`
- `CRSIBy(obj, period, upDn, roc, vtype int)` 〔3, 2, 100, 0〕：(RSI + UpDown 的 RSI + 排名)/3；vtype 0=ROC(1) 的 PercentRank，1=ROC(roc)
- `PercentRank(obj, period)`：窗口（period 根，含当前）中**不含当前**且 ≤当前值的个数×100/**period**；窗口不足 NaN
- `TD(obj)`：DeMark 连续计数（与 4 根前比较，同向累加，反向重置），前 4 根 NaN
- `UpDown(obj, vtype int)`：连续涨跌计数；0=TradingView 口径，1=经典仅 ±1
- `KDJ(high, low, close, period, sm1, sm2 int) (K, D, RSV)` 〔9, 3, 3〕：RMA 平滑
- `KDJBy(high, low, close, period, sm1, sm2 int, maBy string) (K, D, RSV)` 〔maBy `"rma"`/`"sma"`〕：其他 maBy 值 panic
- `Stoch(high, low, close, period)` 〔14〕：100·(close−LL)/(HH−LL)，区间为 0 输出 50
- `STOCH(close, high, low, period)` 〔14〕：同 Stoch，**参数顺序 close,high,low**（勿混用）
- `StochF(high, low, close, period int, smooth ...int) (fastK, fastD)` 〔smooth 省略或≤0=3〕：fastK 原始值，fastD=SMA(K,smooth)
- `STOCHF(close, high, low, period int, smooth ...int)` 〔同 StochF〕：参数顺序 close,high,low
- `StochRSI(obj, rsiLen, stochLen, maK, maD int) (fastK, fastD)` 〔14,14,3,3〕：RSI→Stoch→SMA(maK)→SMA(maD)
- `WillR(e *BarEnv, period)`、`WillRHL(high, low, close *Series, period int)`、`WilliamsPercent(high, low, close, period)` 〔14〕：Williams %R −100~0（`WillR` 读 `BarEnv`，`WillRHL`/`WilliamsPercent` 直接收 `Series`）
- `CCI(obj, args ...interface{})` 〔周期 20〕：`CCI(close, period)`；`CCI(high, low, close, period)` 用 HLC3；(src−SMA)/(0.015·AvgDev)
- `AO(high, low, fast, slow int)`：SMA(HL2,fast)−SMA(HL2,slow)
- `DPO(obj, period)`、`Dpo`：pandas-ta 非居中：close−SMA(close,period) 滞后 floor(period/2)+1 根
- `FisherTransform(high, low, period) (fisher, trigger)` 〔9〕：标准 Ehlers 0.33/0.67 递推平滑 Fisher 变换及上一根 `trigger` 信号线（旧 `Fisher(high, low, period)` 为无递推单线版）
- `WaveTrend(high, low, close, chLen, avgLen, maLen int) (wt1, wt2)` 〔10, 21, 4〕：LazyBear WaveTrend 振荡器
- `QQE(src, rsiPeriod, sf int, wp float64) (rsiMA, trailing, dir)` 〔14, 5, 4.236〕：Quantitative Qualitative Estimation 平滑 RSI 动态追踪线与方向 `±1`
- `ULTOSC(high, low, close, short, medium, long int)` 〔7, 14, 28〕：终极振荡器，权重 4/2/1
- `TRIX(obj, period)`：三重 EMA 的单根百分比变化，无信号线
- `TSI(obj, short, long int)`：EMA(EMA(Δ,short),long)/EMA(EMA(\|Δ\|,short),long)×100
- `STC(obj, period, fast, slow int, alpha float64)` 〔12, 26, 50, 0.5〕：Schaff 趋势周期 0~100
- `KST(obj, r1,r2,r3,r4, s1,s2,s3,s4 int)` 〔常用 10,15,20,30,10,10,10,15〕：Σ wᵢ·SMA(ROC(rᵢ)%, sᵢ)，权重 1,2,3,4；**无信号线**
- `CTI(obj, period)` 〔20〕：相关趋势指标 −1~1（线性回归相关）
- `Stiffness(obj, maLen, stiffLen, stiffMa int)` 〔100, 60, 3〕：0~100：bound=SMA−0.2·StdDev，统计 stiffLen 内收盘>bound 的根数，再 EMA(stiffMa)
- `DV2(h, l, c, period, maLen int)` 〔252, 2〕：有界 DV2 0~100，maLen 均值的百分位排名（非 backtrader 版）
- `ER(obj, period)` 〔8〕：Kaufman 效率比 0~1=\|净变化\|/Σ\|逐根变化\|

**趋势**

- `MACD(obj, fast, slow, smooth int) (macd, signal)` 〔12, 26, 9〕：SMA 初始化
- `MACDBy(obj, fast, slow, smooth, initType int)` 〔initType 0〕：0=SMA 初始化（国外口径），1=首个有效值（国内/MyTT 口径）
- `ADX(high, low, close, period int, smoothing ...int)` 〔14；smoothing 省略/0=period〕：经典 ADX
- `ADXBy(high, low, close, period, smoothing, method int)` 〔14, 0, 0〕：method 0：RMA 平滑 DX；1：SMA 平滑 DX
- `WilderDX(high, low, close, period)` 〔14〕：标准 Wilder RMA 平滑方向指数 DX（旧 `DX` 为未平滑单根版 (全)，新代码用 `WilderDX`/`ADX`/`DMI`）
- `DMI(high, low, close, period int, smoothing ...int) (+DI, −DI, ADX)` 〔smoothing 省略=period〕：smoothing 只作用于 ADX，DI 用 period
- `PluMinDI(high, low, close, period) (+DI, −DI)`、`PluMinDM(high, low, close, period) (+DM, −DM)` 〔14〕：Wilder 口径
- `Vortex(high, low, close, period) (viPlus, viMinus)` 〔14〕：涡旋趋势指标 VI+/VI−
- `Aroon(high, low, period) (Up, Osc, Dn)`、`AroonOsc(high, low, period)`、`AROONOSC`：Up−Dn，−100~100
- `SAR(high, low, step, max float64)`、`PSAR` 〔0.02, 0.2〕：抛物线转向（TA-Lib 口径），返回**价格**
- `Supertrend(high, low, close, period int, multiplier float64)`、`SupertrendDir(high, low, close, period int, multiplier float64) (line, dir)` 〔常用 10, 3〕：`Supertrend` 返回趋势线价格；`SupertrendDir` 同时返回方向（`dir`：`+1` 多 / `-1` 空）
- `ChandelierExit(high, low, close, period int, mult float64) (longStop, shortStop, dir)` 〔22, 3.0〕：吊灯止损线与棘轮方向 `±1`
- `SSLChannel(high, low, close, period) (sslUp, sslDown)` 〔10〕：高低 SMA 通道翻转线
- `Alligator(src, jawLen, teethLen, lipsLen, jawOff, teethOff, lipsOff int) (jaw, teeth, lips)` 〔13, 8, 5, 8, 5, 3〕、`Fractals(high, low, n int) (upFractal, downFractal)` 〔2，右侧确认无前视〕：比尔·威廉姆斯鳄鱼线与分形
- `ElderRay(high, low, close, period) (bullPower, bearPower)` 〔13〕、`WaddahAttar(close, fast, slow, bbLen int, bbMult, mult float64) (trend, explosion)` 〔20, 40, 20, 2.0, 150〕：Elder 多空力量与 Waddah Attar 爆发力
- `PMAX(high, low, close, period int, multiplier float64) (line, dir)` 〔常用 10, 3〕：EMA(close,period)±mult·ATR(period) 棘轮线与方向 `±1`
- `Ichimoku(high, low, close, conversion, base, span int) (conv, base, spanA, spanB, lag)` 〔9, 26, 52〕：**spanA/spanB 未向前平移 base 根**（比较"当前云层"需自取 `Back(base)`）
- `UTBot(c, atr *Series, rate float64)` 〔rate 常用 1〕：突破事件信号（1=买，−1=卖，0=无）
- `LinReg(obj, period)`、`LinRegAdv(obj, period int, angle, intercept, degrees, r, slope, tsf bool)`、`LinearRegAngle(obj, period)`/`LINEARREG_ANGLE`、`Slope(obj, period)`、`RSquare(src, period)`：线性回归终点/斜率/角度/决定系数 R²
- `Correlation(a, b, period)`、`RollingBeta(y, x, period) (beta, alpha)`：滚动 Pearson 相关系数与 OLS 回归斜率/截距
- `CHOP(e *BarEnv, period)`、`CHOPHLC(high, low, close *Series, period int)` 〔14〕：Choppiness 震荡指数（越高越震荡，越低越趋势）

**波动率、通道与时段锚点 VWAP/枢轴**

- `TR(high, low, close)`、`ATR(high, low, close, period)` 〔14〕、`NATR(high, low, close, period)`：真实波幅、Wilder ATR 与百分比 ATR
- `StdDev(obj, period)`/`STDDEV` 〔20〕、`StdDevBy(obj, period, ddof int) (stddev, mean)`、`AvgDev(obj, period)`：标准差与平均绝对偏差
- `ParkinsonVol(high, low, period)`、`GarmanKlassVol(open, high, low, close, period)`、`YangZhangVol(open, high, low, close, period)`：高低价 / OHLC 跳空修正历史波动率估计器（每 bar 对数波动率）
- `BBANDS(obj, period int, stdUp, stdDn float64) (upper, mid, lower)` 〔20, 2, 2〕：布林带
- `Donchian(high, low, period) (upper, middle, lower)`、`DonchianPBand(high, low, close, period)`：唐奇安通道与通道内相对位置
- `KeltnerChannel(high, low, close, period int, multiplier float64) (upper, mid, lower)`、`KeltnerWBand(high, low, close, period int, mult float64)`：肯特纳通道与相对带宽
- `Squeeze(high, low, close, period)`、`SqueezeMomentum(high, low, close, bbLen int, bbMult float64, kcLen int, kcMult float64) (val, squeezeOn)` 〔20, 2.0, 20, 1.5〕：TTM Squeeze 挤压标志与 LazyBear 线性回归动量柱（`squeezeOn`：`1`=挤压/`0`=释放）
- `SessionVWAP(e *BarEnv, anchorMinutesUTC int)`、`SessionVWAPBands(e *BarEnv, anchorMinutesUTC int, mult float64) (vwap, upper, lower)`、`WeeklyVWAP(e *BarEnv)`：按 UTC 时段锚点（如 `0`、`480`、`810`）每日重置的日内 VWAP / 加权标准差上下轨及 UTC 周一重置的周度 VWAP（`>=1d` 周期上安全回退典型价）
- `DailyPivotPoints(e *BarEnv, method string, anchorMinutesUTC int) (p, r1, s1, r2, s2, r3, s3)`：基于上一完整锚点交易日 OHLC 计算的无前视枢轴点（`method`: `"classic"`/`"fibonacci"`/`"camarilla"`/`"woodie"`/`"demark"`）
- `Highest/Lowest(obj, period)`、`HighestBar/LowestBar(obj, period)`：含当前根的周期最高/最低及偏移

**成交量、成交流（OrderFlow / CVD）与筹码分布**

- `OBV(close, volume)`：累计能量潮
- `ADL(env)`、`ADLHLCV(high, low, close, volume *Series)`、`ADOSC(env, fast, slow int)`、`ChaikinOsc(env, shortLen, longLen int)` 〔3, 10〕：累积派发线与佳庆振荡器
- `CMF(env, period)`、`CMFHLCV(high, low, close, volume *Series, period int)` 〔20〕：佳庆资金流 −1~1
- `MFI(e, period)`、`MFIHLCV(high, low, close, volume *Series, period int)` 〔14〕：资金流量指数 0~100
- `EFI(env, period)`、`EFICV(close, volume *Series, period int)`：Elder 力量指数
- `StandardVPCI(close, volume, shortPeriod, longPeriod int)` 〔5, 20〕：标准 Buff Dormeier 量价确认指标（旧 `VPCI(close, volume, period)` 为简化差值 (全)）
- `RollingVolumeProfile(high, low, close, volume, period, numBins int, valueAreaPct float64) (poc, vah, val)` 〔100, 24, 0.70〕：滚动成交量筹码分布控制点 POC 与价值区上下沿 VAH/VAL
- `VolumeDelta(volume, buyVolume)` / `EnvVolumeDelta(e)`、`CVD(volume, buyVolume)` / `EnvCVD(e)`、`RollingCVD(volume, buyVolume, period)` / `EnvRollingCVD(e, period)`、`SessionCVD(e, anchorMinutesUTC int)`：K 线主动买卖净成交量 Delta（`2*BuyVolume - Volume`）及累计/滚动/日内重置 CVD
- `TakerBuyRatio(volume, buyVolume, period)` / `EnvTakerBuyRatio(e, period)`、`AvgTradeQuote(quote, tradeNum, period)` / `EnvAvgTradeQuote(e, period)`、`AvgTradeSize(volume, tradeNum, period)` / `EnvAvgTradeSize(e, period)`：滚动主动买入占比与笔均成交额/笔均成交量

**滚动统计、分形熵、轻量状态分类与 Pine 状态工具**

- `ZScore(src, period)`、`Skew(src, period)`、`Kurt(src, period)`、`RollingQuantile(src, period int, q float64)`：滚动标准分、样本偏度、超额峰度与精确分位数（`q=0.5` 中位数）
- `Hurst(src, period)`、`FDI(src, period)`、`ShannonEntropy(src, period, numBins int)` 〔numBins 常用 10〕：重标极差 Hurst 指数（`<0.5` 均值回归、`>0.5` 趋势）、分形维数指标（`1~2`）与一阶差分香农信息熵（归一化 `0~1`）
- `LorentzianKNN(close, f1, f2, f3, f4 *Series, lookback, k, horizon, stride int)` 〔常用 200, 8, 4, 1〕、`RollingKMeans3(src, period, maxIter int) (cluster, lowC, midC, highC)` 〔常用 100, 15〕：严格无前视历史窗口洛伦兹距离 k-NN 投票信号与一维滚动三态 k-Means 聚类（`cluster`：`0/1/2` 低/中/高态）
- `BarsSince(cond)`、`ValueWhen(cond, src, occurrence int)`〔`0` 为最近一次非零且非 NaN〕、`Cum(src)`、`Rising(src, length int)`、`Falling(src, length int)`：Pine `ta.barssince` / `ta.valuewhen` / `ta.cum` / `ta.rising` / `ta.falling` 等价状态原语

**形态与枢轴**

- `PivotHigh/PivotLow(src, left, right int)`：右侧 right 根确认后输出枢轴高/低点，否则 NaN，滞后 right 根
- `CDL3INSIDE/CDL3LINESTRIKE/CDL3OUTSIDE/CDLENGULFING/CDLHARAMI(open, high, low, close)`：三内包/三线打击/三外包/吞没/孕线；`100`/`−100`/`0`
- `CDLMARUBOZU(o, h, l, c)`、`CDL3WHITESOLDIERS(o, h, l, c)`、`CDL3BLACKCROWS(o, h, l, c)`、`CDLPIERCING(o, h, l, c)`、`CDLDARKCLOUDCOVER(o, h, l, c)`、`CDLEVENINGSTAR(o, h, l, c)`：光头光脚/红三兵/三只乌鸦/刺透/乌云盖顶/黄昏星
- `CDLDOJI(o, h, l, c)`、`CDLDRAGONFLYDOJI(o, h, l, c)`、`CDLGRAVESTONEDOJI(o, h, l, c)`、`CDLHAMMER(o, h, l, c)`、`CDLHANGINGMAN(o, h, l, c)`、`CDLMORNINGSTAR(o, h, l, c)`、`CDLSHOOTINGSTAR(o, h, l, c)`：十字星/蜻蜓/墓碑/锤子/上吊/启明星/流星

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
- `RefineTF`（interface{}）：策略默认子周期撮合粒度（如 `"5m"`、`"1m"`、`12`、`"3-6"`；YAML `run_policy[].refine_tf` 未设置时自动生效，显式配置优先覆盖）
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
- `OnBatchJobs`（`func([]*StratJob)`）：同一时间戳所有主周期 job（需 `BatchInOut`；确定性回测下按 `strat.BarPriority(barMS, symbol, bt_bar_order_seed)` 中性哈希序排列，不偏向字母靠前品种）
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
- `ExgStopLoss`、`ExgTakeProfit bool`：默认 true。为 false 时实盘**不在远端交易所订单簿预挂止损/止盈条件单，但回测与实盘均继续在本地价格监控并触发止损/止盈**（`StopLoss==0` 仍表示不设止损）
- `LongSLPrice`、`ShortSLPrice`：任务级默认止损价（开单时请求未指定则采用）
- `LongTPPrice`、`ShortTPPrice`：任务级默认止盈价（开单时请求未指定 `TakeProfit/TakeProfitVal/TakeProfits` 则采用）
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
- `RefKlineSub(symbol, timeframe string, warmupNum ...int) *DataSub`（或 `strat.NewRefKlineSub(s, symbol, tf, warmupNum...)`）：构造只读跨周期/跨品种 K 线订阅（用于 `OnDataSubs`）。`symbol==""` 或 `"_cur_"` 表示当前 `s.Symbol` 的辅助周期；非空如 `"BTC"` 或 `"BTC/USDT:USDT"` 自动按当前任务交易所与市场规范化并在 `SymbolState` 解析/登记只读 `*orm.ExSymbol`（即使不在交易 `pairs` 池内也会预热并推送，且**不创建交易任务、不下单**）；省略 `warmupNum` 时默认取 `s.Strat.WarmupNum`
- `RefEnv(symbol, timeframe string) *ta.BarEnv`：直接从 `s.DataHub` 读取对应辅助周期或参考品种 K 线的完整 `*ta.BarEnv`；尚未收到首根 bar 时返回 `nil`，非 `nil` 时 `Open/High/Low/Close/Volume/Quote/BuyVolume/TradeNum` 均已就绪，可直接传给 `ta.SMA(env.Close, ...)`、`ta.ATR(env.High, env.Low, env.Close, ...)`、`ta.MFI(env, 14)`、`ta.WillR(env, 14)`、`ta.HeikinAshi(env)`
- `AccountEquity() float64`：当前账户实时总权益（计价法币，现金余额+未实现盈亏）；预热期或尚未初始化钱包时回退初始配置资金或 `GetStakeAmount(s)` 有限正值，不会除零
- `AvailableBalance() float64`：当前账户可用余额/可用保证金（计价法币）
- `AccountDrawdown() float64`：当前账户净值相对运行以来峰值净值（High-Water Mark）的回撤比例 `[0, 1]`（无回撤为 0）
- `GetStakeAmount`：`s.Strat.GetStakeAmount(s)` 单笔基准金额（法币）；`OrderCounts() (orderNum, enteredNum int)` 同 OrderNum/EnteredNum
- **无前视日内区间与前日极值组件（按 `StratJob` 自动隔离、自动回放预热期历史，多品种零串味）**：
  - `OpeningRange(anchorMinutesUTC, rangeMinutes int) OpeningRangeSnapshot`（及 `OpeningRangeWithOpts(..., OpeningRangeOptions{AllowSingleBarFallback})`）：跟踪锚点交易日开盘前 `rangeMinutes` 分钟 ORB（`Open/High/Low/Close/Mid/Width/Volume/BarCount`）。形成过程中 `Forming=true, Ready=false`；仅当 `TimeStop >= RangeEndMS` 且至少有 1 根有效 bar 时 `Ready=true`（首次完成当根 `JustCompleted=true`，区间外后续 bar `InPostRange=true`）；`TFMSecs >= 1d`、`TFMSecs > rangeMinutes*60000` 或网格与锚点不对齐时安全返回 `Applicable=false, Ready=false`。
  - `SessionRange(startMinUTC, endMinUTC int) SessionRangeSnapshot`：统计周期性 UTC 会话窗口（如 `0, 480` 亚盘 `00:00–08:00 UTC`，或跨午夜窗口 `1320, 120`）完整高低点；窗口内 `InSession=true, Completed=false`，窗口收盘后锁定 `High/Low/Open/Close/Mid/Width/Volume` 且 `Completed=true` 直至下一周期窗口开启。
  - `PrevDayOHLC(anchorMinutesUTC int) (PrevOHLC, bool)`：返回上一**完整结束**锚点交易日的 `Open/High/Low/Close/Pivot/Range/Volume`（PDO/PDH/PDL/PDC 菲阿里四价），当日全程恒定无前视；非零锚点下的首日残缺片段自动丢弃。
  - `PrevNDaysStats(anchorMinutesUTC, nDays int) (PrevNDaysSummary, bool)`：滚动汇总过去 `nDays` 个完整交易日的 `HH, HC, LC, LL, DualThrustRange, AvgRange(ADR), TotalVolume, AvgVolume, History`。
  - `DualThrustBands(anchorMinutesUTC, nDays int, k1, k2 float64) (upper, lower, dayOpen float64, ok bool)`：`Range = max(HH-LC, HC-LL)`，输出今日锚点开盘价 `dayOpen` 及 `dayOpen + k1*Range`、`dayOpen - k2*Range`（仅 `<1d` 周期生效）。
  - `RBreakerLevels(anchorMinutesUTC int) (RBreakerSnap, bool)`（及参数化版 `RBreakerLevelsBy(anchor, f1, f2, f3)`）：基于昨日完整 `(H, L, C)` 输出六档价位 `BBreak > SSetup > SEnter > Pivot > BEnter > BSetup > SBreak` 及今日盘中已走出的 `TodayOpen/TodayHigh/TodayLow`（仅 `<1d` 周期生效）。也可独立实例化 `NewOpeningRangeTracker` / `NewSessionRangeTracker` / `NewPrevPeriodTracker`。

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
- 字段：`DoneMS, TimeMS int64`、`Source string`、`Sid int32`、`Symbol string`、`TimeFrame string`、`Closed, IsWarmUp bool`。
- 方法：`BarEnv() *ta.BarEnv`（K线/OHLCV 流完整同步的 `*ta.BarEnv`，未到首根 bar 或非 OHLCV 流返回 `nil`）、`Series(name) *ta.Series`（数值历史序列，K线流 `Series("close")` 与 `BarEnv().Close` 同一指针，`Series("close").Env.High` 始终非 nil；无则 nil）、`SetMaxCache(limit int)`/`MaxCache() int`（默认缓存上限 `max(1500, NumTaCache, WarmupNum+100)`，长周期均线如 `SMA(1000)` 不会因截断返回 NaN）、`Float64(name)`（最新值；缺失/无法转换为 0，要区分缺失用 `RawValue`/`Has`）、`Int64(name)`、`String(name)`、`Raw(name) any`、`RawValue(name) (any, bool)`（区分缺失与显式 nil）、`Has(name) bool`。
- 浮点字段默认全部成为 Series；`DataSub.SeriesFields` 非空时仅列出的字段（及 K 线标准 OHLCV 字段）成为 Series；自定义扩展列与原始值/NULL 语义始终保留可读。

`DataSub`（即 `orm.Subscription`）：
- `Source string`：数据源名；K线 `"kline"`（`orm.SeriesSourceKline`）；自定义序列为注册名
- `ExSymbol *orm.ExSymbol`：nil=当前品种；跨品种只读参考 K 线推荐用 `s.RefKlineSub("BTC/USDT:USDT", "1d")`（或 `"BTC"`）自动解析，无需手工查 `SymbolState`
- `TimeFrame string`：周期
- `WarmupNum int`：该订阅预热数量
- `Fields []string`：原始字段投影（K线默认 `open high low close volume quote buy_volume trade_num`）
- `SeriesFields []string`：需要数值历史序列的字段
- `Frequency`/`Projection`（高级，一般留空）：`bar/event`、`default/all/selected`

- `s.RefKlineSub(symbol, tf, warmupNum...)` + `s.RefEnv(symbol, tf)`：跨品种大盘过滤（如山寨币策略读 `BTC/USDT:USDT` 日线）与多周期共振首选写法。注意 `banta` 增量指标每根参考 bar 推进一次：回测预热按品种串行，若希望参考品种指标在预热结束即就绪，应在 `Info` 处理器（或 `OnData` 入口 `if !d.IsMain()` 之前，满足 `d.Closed` 时）对 `s.RefEnv(...)` 调用相同指标推进缓存，再在 `Main` 中读取 `.Get(0)`。
- `strat.NewDataSub(info *orm.SeriesInfo) *DataSub`：由自定义序列信息派生订阅（`Source`=序列名）。
- `s.DataHub.Get(tf, source, sid)` 取视图；`DataHub.SetMaxCache(limit)` 调整保留上限；`DataHub.AllReady()` 判断所有订阅是否推进到当前时间。
- 辅助K线信号应基于 `Closed==true`（实盘 WebSocket 数据可能未收盘）。

### 3.5 EnterReq（开仓请求）
- `Tag string`：**必填**，入场信号名
- `Short bool`：true 做空，默认做多
- `OrderType int`：`core.OrderType*`；`Limit>0` 且未指定时自动为限价
- `Limit float64`：限价入场价（仅设 `Limit>0` 为限价单；与 `Stop>0` 同设为 `Stop-Limit` 触发限价单：价格触及 `Stop` 后以 `Limit` 挂限价单）
- `Stop float64`：触发价：做多价格涨到才触发（做空相反）；未设 `Limit` 且已越过时按限价处理（`Limit=Stop`）；回测 `bt_legacy_intrabar` 下已越过的纯突破单按当前价入场
- `OCOGroup string`：OCO（One-Cancels-the-Other）挂单组名；同一 `(Account, Symbol, Strategy, OCOGroup)` 内任一待入场挂单成交后，自动撤销同组其余未成交挂单（同一 bar 内多空双向突破挂单按 OHLC 路径先后触发）
- `CostRate float64`：开单倍率，默认 1（乘基准金额）
- `RiskRate float64`：单笔账户权益风险比例 `[0, 1]`（如 `0.01` 表示触发止损最多亏损当前 `AccountEquity()` 的 1%）；未指定 `Amount`/`LegalCost` 且解析出有效止损时自动按止损距离计算 `LegalCost`
- `LegalCost float64`：法币金额；指定则忽略 CostRate/RiskRate
- `Amount float64`：标的数量；指定则不按金额计算
- `Leverage float64`：杠杆；非 0 覆盖账户杠杆
- `StopLossVal float64`：止损距离（价格差），换算为止损价；优先于 StopLoss
- `StopLoss float64`：止损触发价；>0 时生效（`ExgStopLoss=true` 实盘预挂交易所止损单，`false` 仅本地监控）
- `StopLossLimit float64`：止损限价；仅设 StopLossLimit 而 StopLoss=0 时，StopLoss 取该值
- `StopLossRate float64`：止损退出比例 (0,1]，0=全部
- `StopLossTag string`：止损原因
- `ActivationPrice`、`CallbackPct float64`：追踪止损：激活价、回撤百分比（`[0.05, 50]`；实盘超出交易所原生回调率上限时自动降级为本地价格监控追踪止损）
- `TakeProfitVal/TakeProfit/TakeProfitLimit/TakeProfitRate/TakeProfitTag`：同止损（距离/价格/限价/比例/原因）
- `TakeProfits []TakeProfitTarget`：多级阶梯止盈（与单级 `TakeProfit*` 互斥）；每项 `TakeProfitTarget{Price, Val, Rate, Limit, ExitRate float64; MoveSLToBreakeven bool; Tag string}` 在入场成交后按实际入场均价解析 `Val`（价差）/`Rate`（比例，如 `0.02`=+2%）并按盈利方向由近及远排序，逐级按初始仓位比例 `ExitRate`（总和 `<=1`）分批止盈；`MoveSLToBreakeven=true` 时将该单剩余仓位止损收紧至不差于入场均价
- `StopBars int`：限价单超过多少根K线未成交则撤单（0 用 `StopEnterBars`）
- `ClientID string`：交易所 ClientOrderID 后缀
- `Infos map[string]string`：附加信息（写入订单 Info）
- `Log bool`：失败时记录告警日志
- `CommandID string`：框架幂等 ID（留空）

`OpenOrder` 校验：
1. `Tag` 空、`CanOpen` 假、任何数值 NaN、`RiskRate` 超 [0,1]、`CallbackPct` 超 [0.05,50]、显式 `OrderTypeMarket` 时设 `Limit>0`→错误。
2. 入场价：当前价（预热期 `Env.Close`）；限价按方向取更优价；`Stop>0 && Limit>0` 按 `Stop-Limit` 计算参考入场价。
3. 金额（未指定 Amount/LegalCost）：若 `RiskRate>0` 且解析出有效止损价（`|enterPrice-slPrice|>0`），`LegalCost=(s.AccountEquity()*RiskRate)/(|enterPrice-slPrice|/enterPrice)`，并受最大杠杆（`AccountEquity()*leverage`）与 `max_stake_amt` 封顶；否则 `LegalCost=基准金额×StakeRate×CostRate`；数量超近 5 根平均成交量×`open_vol_rate` 则截断；低于 `MinStakeAmount`(5.5)：`ignore` 报错；`keepBig` 仅当成本 >5.5 的 40% 时提升到 5.5×1.1，否则报错；`keepAll` 一律提升到 5.5×1.1。
4. 止损优先级：`StopLossVal` > `StopLoss` > `LongSLPrice/ShortSLPrice` > `TradeStrat.StopLoss`（入场价×(1∓比例)）；方向不对（做多止损≥入场价等）→错误；`ExgStopLoss=false` 时仅禁止远端预挂单，仍保留本地止损监控。
5. 止盈：设 `TakeProfits` 时校验各级目标与 `ExitRate` 总和 `<=1`；否则按 `TakeProfitVal` > `TakeProfit` > `LongTPPrice/ShortTPPrice` 解析（`TakeProfitLimit` 同 StopLossLimit 规则），方向不对→错误。
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

### 3.9 批量截面目标权重调仓与波动率倒数配权（`OnBatchJobs` 专用）
适用于多品种趋势风险平价 CTA、横截面强弱选币 + 个币止损混血、龙头（如 BTC）突破带动滞涨高 Beta 山寨币（Lead-Lag）等需在 `TradeStrat{BatchInOut: true, OnBatchJobs: ...}` 中统一分配账户总权益名义权重的策略（注意：若需支持同向分批补仓，请保持 `EachMaxLong: 0, EachMaxShort: 0` 不限同向单数）：

- **`strat.RebalanceBatchWeights(jobs []*strat.StratJob, targetWeights map[string]float64, opts strat.BatchRebalanceOpts) strat.BatchRebalanceResult`**：
  - `targetWeights`：`symbol -> 目标有符号名义权重`（相对 `AccountEquity()` 的比例，如 `0.4` 做多 40% 名义敞口，`-0.3` 做空 30%，`0` 或未出现表示清仓）。
  - `BatchRebalanceOpts`：
    - `Tag string`：入场/平仓信号标签（默认 `"rebalance"`）。
    - `DriftThreshold float64`：免调仓偏离缓冲带（默认 `0.02` 即 2% 权益；仅当当前持仓与目标权重**同号**且 `|target - cur| < DriftThreshold` 时跳过微调以节省手续费；从空仓新建仓不受缓冲带阻挡；传 `< 0` 禁用缓冲带）。
    - `MaxGrossExposure float64`：总绝对敞口上限（`> 0` 且 `Σ|w| > MaxGrossExposure` 时自动将所有目标权重等比缩放到上限）。
    - `StopLossRate` / `TakeProfitRate` / `CallbackPct` / `Leverage float64`：新开仓与同向补仓单自动附带的个币止损比例（`enterPrice * StopLossRate` 换算为 `StopLossVal`）、止盈比例、追踪止损回撤百分比 `[0.05, 50]` 与杠杆倍数，打通“横截面组合配权 + 个币实时保护单”。
  - **两阶段执行顺序**：
    1. 第一遍扫描全部 `jobs`：目标为 `0` 的全平；方向相反（多翻空/空翻多）的先 `CloseOrders` 平掉旧向持仓并将全额目标记入待开队列；同向超配（`|target| < |cur| - DriftThreshold`）的按 `ExitRate = (|cur| - |target|) / |cur|` 部分减仓，优先释放保证金。
    2. 第二遍对待开新仓与同向补仓（`|target| > |cur| + DriftThreshold`）按 `LegalCost = AccountEquity() * deltaWeight` **从大到小**排序（同额按 `strat.BarPriority(barMS, symbol, seed)` 中性哈希与品种名确定性决胜）依次调用 `OpenOrder`，低于 `core.MinStakeAmount`（`5.5`）的碎单自动静默跳过（计入 `SkippedDustCount`）。
  - 返回 `BatchRebalanceResult`（含 `Equity`、`RawGrossExposure`、`EffectiveGrossExposure`、`ScaleFactor`、`ClosedCount`、`ReducedCount`、`ReversedCount`、`OpenedCount`、`ScaledInCount`、`SkippedDriftCount`、`SkippedDustCount`、`ErrorCount` 及按实际执行序排列的 `Actions []BatchRebalanceAction`）。
- **`strat.InverseVolWeights(jobs []*strat.StratJob, signals map[string]float64, atrPeriod int, targetGross float64) map[string]float64`**：
  - 输入各品种方向/强度信号 `signals`（如 `+1`/`-1`/`0` 或连续打分）、`atrPeriod`（`<=0` 默认 `14`）及目标总绝对敞口 `targetGross`（如 `0.8`）。
  - 内部对每个 `job` **无条件计算** `ta.NATR(e.High, e.Low, e.Close, atrPeriod)`（保证 `signal==0` 时增量 RMA 序列缓存不错位），按 `signal / NATR` 波动率倒数赋予低波动品种更高权重、高波动品种更低权重，并精确归一化使 `Σ|w| == targetGross`。
- **`strat.BarPriority(barMS int64, symbol string, seed uint64) uint64`**：同 bar 多品种中性可复现 64 位哈希优先级（数值越小越优先）。

### 3.10 粗周期 K 线网格与双边限价做市（`RefineTF` + `OnOrderChange` 因果补单）
适用于 `1h`/`4h`/`1d` 等中粗周期上同时挂多层买/卖限价单（网格、通道回归、ATR 阶梯挂单）并在某档成交后立刻挂出反向止盈/补单的策略：
- **声明子周期撮合**：在 `TradeStrat` 设置 `RefineTF: "5m"`（或 `"1m"` / 整数等分倍数如 `12`；YAML `run_policy[].refine_tf` 可覆盖），并保持 `EachMaxLong: 0, EachMaxShort: 0`（或设为网格档位数上限），同时确保全局 `max_open_orders` / `max_simul_open` 足够容纳挂单档位数。
- **子 K 线时序推进与 `OnOrderChange` 即时补单**：主周期 `OnData(Main)` 挂出的多档 `Limit` / `Stop-Limit` / 止盈止损单在回测中按 `RefineTF` 子 K 线逐根推进，同一子 K 线内触发的多档挂单按模拟成交时间先后顺序依次成交并触发 `OnOrderChange(s, od, strat.OdChgEnterFill)` / `OdChgExitFill`。
- **快照同步与严格无前视**：进入 `OnOrderChange` 前框架已同步刷新 `s.GetOrders` / `s.GetOrderNum` / `s.Position`；在 `OnOrderChange` 内调用 `s.OpenOrder`（反向对冲限价单或下一档补单）或 `s.CloseOrders`（如指定 `OrderID: od.ID, Limit: tpPrice` 挂限价平仓单）会被立即排空激活，且严格从当前成交时刻 + `bt_net_cost` 之后的剩余子 K 线及后续子 K 线开始匹配（绝不回看成交前的历史极值）。
- **回测近似资金费对称结算**：线性永续合约在未加载历史 `funding_rate` 序列时，`bt_funding_apr` 双边对称计费（正费率下多头支付 `+cost`、空头获得 `-cost` 补贴，1:1 多空对冲净资金费为 `0`）。

```go
// TradeStrat 设 RefineTF: "5m", EachMaxLong: 0, EachMaxShort: 0：
// 1) OnData(Main) 在 1h 收盘按中枢布设上下网格限价单（或直接给每笔买单附带 TakeProfit: buyPx + step）；
// 2) OnOrderChange 在 5m 子 K 线某档买单成交时立即补挂高一档限价平仓/反向单，同一小时内后续 5m 子 K 线冲高即可成交：
OnOrderChange: func(s *strat.StratJob, od *ormo.InOutOrder, chgType int) {
	st := s.More.(*gridState)
	if chgType == strat.OdChgEnterFill && strings.HasPrefix(od.EnterTag, "grid_buy_") {
		_ = s.CloseOrders(&strat.ExitReq{
			Tag:     "grid_tp_" + od.EnterTag,
			OrderID: od.ID,
			Dirt:    core.OdDirtLong,
			Limit:   od.Enter.Average + st.step,
		})
	}
},
```

## 4. 下单、止损止盈与仓位细则
### 4.1 开仓与平仓
- 回测成交时间=下单时间+`bt_net_cost` 秒，价格取其后价格路径（§6）。
- 默认市价（`OrderTypeEmpty` 同）。`Limit` 限价单可配 `StopBars`；`OrderTypeLimitMaker` 要求挂单侧（做多限价≤市价，做空≥市价），否则报错（币安合约无 limit_maker，banexg 转 limit+GTX）。
- 杠杆：`EnterReq.Leverage` 优先，其次账户 `leverage`。
- 仓位大小：固定基准路径为 `stake_amount`/`stake_pct`（账户）× `accounts.<acc>.stake_rate` × `run_policy.stake_rate` × `TradeStrat.StakeRate` × `CostRate`（`max_stake_amt` 封顶，`strat_perf` 降权再乘系数）；也可在 `EnterReq` 设 `RiskRate`（如 `0.01`）配合止损距离按 `AccountEquity()` 风险百分比自动定仓。
- 平仓：默认全平市价；`ExitRate=0.5` 平一半（每订单按比例）；`EnterTag`/`OrderID` 过滤；`Limit` 限价。部分平仓后剩余订单仍在 `GetOrders`。

### 4.2 止损与止盈
- `StopLossVal/TakeProfitVal`（距离）最常用，`StopLoss/TakeProfit` 为绝对价；优先级见 §3.5。`*Rate` 触发后退出比例（0=全部），`*Limit` 触发后挂限价单（否则市价）。多级阶梯止盈用 `TakeProfits []TakeProfitTarget`（可配 `MoveSLToBreakeven: true` 在首级止盈后自动移损保本）。
- 追踪止损：`ActivationPrice`（达到后激活）+ `CallbackPct`（从最高点回撤%，[0.05,50]）。运行中修改：`s.SetAllStopLoss(dirt, &ormo.ExitTrigger{Price: …})` 或 `od.SetStopLoss(...)`。

**交易所侧挂单**：
- 回测、`dry_run`：本地模拟触发，不访问交易所。
- 实盘（`prod`）与实时模拟（`dry_run`）：由 execution 路径处理保护单，`execution.protection_mode: software`（默认）由引擎按价格监控触发，`native_preferred` 在适配器支持时优先挂交易所条件单（有效模式见订单 `Info["execution_protection"]`）；追踪止损（`CallbackPct`/`ActivationPrice`）目前由引擎监控。旧 `LiveOrderMgr` 的逐单 `CreateOrder` 挂条件单与本地降级流程已删除。
- banexg 适配：币安（现货/杠杆 STOP_LOSS(_LIMIT)/TAKE_PROFIT(_LIMIT)；合约 STOP_MARKET/STOP/TAKE_PROFIT_MARKET/TAKE_PROFIT/TRAILING_STOP_MARKET）、OKX（algo）、Bybit（conditional）均实现；不支持的品类/类型下单报错。
- 失败与重试：由 execution 的待发命令与恢复流程处理（见 `06_execution.md`），不再有旧栈“仅同步/重启补挂”的行为。`SetAll*`/`od.SetStopLoss` 修改经改单命令发送。
- `ExgStopLoss/ExgTakeProfit=false` 只影响回测本地监控语义；execution 路径的挂单方式由 `execution.protection_mode` 决定（见实盘指南）。
- 回撤平仓（`DrawDownExit=true`）：框架跟踪最大浮盈比例 maxChg；r 取 `GetDrawDownExitRate`（`>0` 采用、`0` 不设置、`<0`/未设用默认表）：maxChg >10%→r=0.15，>4%→0.17，>2.5%→0.25，>1.5%→0.37，>0.7%→0.5，否则不启用。回撤价=入场价×(1+maxChg×(1−r)×方向)，当前收盘越过则 tag `draw_down` 市价平仓，否则**用该价覆盖订单的止损触发器**（会替换你设置的止损，实盘同步到交易所）。仅在 `OnCheckExit` 返回 nil 时检查。

### 4.3 多空限制
- `MaxOpenLong/Short`（job 级，由 `EachMaxLong/Short` 初始化）决定能否开新单；现货不能开空。反手：同一回调调 `CloseOrders` 与 `OpenOrder`，本事件末统一处理。

## 5. 内置衍生品数据源（`funding_rate` / `open_interest` / `premium_kline` / `index_kline` / `mark_kline`）与自定义时序数据

- **内置 `funding_rate` 数据源**：框架默认注册 `orm.SeriesSourceFundingRate`（`"funding_rate"`，默认周期 `"8h"`，表 `funding_rate_8h`，字段 `"rate"` 单次结算费率，如 `0.0001` 表示 +0.01%），时间语义 `EndMS = 结算时刻`（无前视）。支持 `./bot series list`、`./bot series down -tables funding_rate ...` 预下载；策略直接在 `OnDataSubs` 返回 `&strat.DataSub{Source: orm.SeriesSourceFundingRate, TimeFrame: "8h", WarmupNum: 10}` 即可在 `Custom` 分支用 `d.Float64("rate")` / `d.Series("rate")` 读取信号。线性永续合约回测在存在 `funding_rate` 数据时自动按持仓跨越的结算点双向计入订单资金费（多头正费率付、负费率收；空头正费率收、负费率付），无数据时回退 `bt_funding_apr`。
- **内置 `open_interest` 数据源**：框架默认注册 `orm.SeriesSourceOpenInterest`（`"open_interest"`，默认周期 `"5m"`，支持 `"5m" "15m" "30m" "1h" "2h" "4h" "6h" "12h" "1d"`，表 `open_interest_<tf>`），包含 6 个浮点字段：`"oi"`（持仓量标的数量/张数）、`"oi_value"`（持仓量名义价值）、`"top_ls_pos_ratio"`（大户持仓量多空比）、`"top_ls_acc_ratio"`（大户账户数多空比）、`"global_ls_ratio"`（全市场账户多空比）、`"taker_ls_vol_ratio"`（合约主动买卖量比）。所有字段注册 `last` 期末快照重采样规则，缺失指标存为显式 `NULL`（`d.RawValue(col)` 为 `(nil, true)`，`d.Series(col)` 为 `NaN`），时间区间 `[T-tfMS, T)`、`EndMS = T`（无前视）。策略直接声明 `&strat.DataSub{Source: orm.SeriesSourceOpenInterest, TimeFrame: "1h", WarmupNum: 20}` 即可在 `Custom` 中读取。
- **内置 `premium_kline` / `index_kline` / `mark_kline` 价格指数 OHLC 数据源**：框架默认注册 `orm.SeriesSourcePremiumKline`（`"premium_kline"`，永续相对现货指数的期现溢价率/基差 OHLC，支持负值如 `-0.0015`）、`orm.SeriesSourceIndexKline`（`"index_kline"`，现货指数价格 OHLC）与 `orm.SeriesSourceMarkKline`（`"mark_kline"`，合约标记价格 OHLC），默认周期 `"1m"`（支持 `"1m" "3m" "5m" "15m" "30m" "1h" "2h" "4h" "6h" "8h" "12h" "1d" "3d" "1w" "1M"`，表 `<source>_<tf>`），字段为 `"open"`、`"high"`、`"low"`、`"close"`（无成交量列，支持细周期按 `open=first, high=max, low=min, close=last` 无损重采样，区间 `[TimeMS, TimeMS+tfMS)` 无前视）。策略直接声明 `&strat.DataSub{Source: orm.SeriesSourcePremiumKline, TimeFrame: "1h", WarmupNum: 20}` 即可在 `Custom` 中用 `d.Float64("close")` / `d.Series("close")` 读取期现基差信号。

### 5.1 自定义表写入（数据源/导入器/应用层，勿在策略回调中写库）
```go
info := orm.NewSeriesInfo("chain_flow", "1h", []orm.SeriesField{{Name: "netflow", Type: "float"}})
store := orm.NewSeriesStore(orm.NewSeriesRepo(storage)) // storage: 当前 Runtime 的 *orm.Storage
_ = store.Ensure(ctx, info)
_ = store.Write(ctx, info, exs, &orm.DataRecord{TimeMS: startMS, EndMS: endMS, Values: map[string]any{"netflow": val}})
rows, _ := store.Read(ctx, info, exs, startMS, endMS, 0) // []*orm.DataSeries
v, err := rows[0].FloatValue("netflow")
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
4. `Cross` 交叉事件判断用 `== 1` / `== -1`（`CrossUp()`/`CrossDown()` 为方向持续状态，见 §2.2）。
6. **比较 Close 与 Highest/Lowest 前先 `Back(1)`**，否则当前收盘永远无法突破含自身的最高价：`e.Close.Get(0) > ta.Highest(e.High, n).Back(1).Get(0)`。
7. `s.Env` 是主周期；读辅助周期或跨品种参考 K 线优先用 `s.RefKlineSub(symbol, tf)` + `s.RefEnv(symbol, tf)`（或 `data.BarEnv()`/`data.Series(...)`），读自定义序列用 `data.Series(...)`/`data.Float64(...)`。
9. `OpenOrder` 的 `Tag` 必填；`CloseOrders` 有订单时 `Tag` 必填。`ExitReq{}` 的 `Dirt` 零值是 Both，只平多/空须显式设 `core.OdDirtLong/OdDirtShort`。
10. 超参只读共享：不要写入 `More`；按品种不同用 `pair_params`。`More` 只存每品种会变化的数据。
11. `GetOrders/GetOrderNum` 基于快照：同回调内刚 `OpenOrder` 不会出现；切片不要跨事件复用。
13. 回调里不调交易所接口/阻塞 IO（外部数据用 §5 自定义序列）；不用包级全局遍历品种/其他 job（跨品种汇总用 `OnBatchJobs/OnBatchInfos`）。
14. 合成砖构建器（`RenkoBuilder`/`RangeBarBuilder`/`VolumeBarBuilder`/`DollarBarBuilder`）属于逐品种有状态对象，必须在 `OnStartUp` 放入 `s.More` 中持有，切勿每根 K 线重新 `New*`。
16. 对 `Series.Get` 的 NaN 做判断（`math.IsNaN(atr)`），尤其 `WarmupNum` 不够大时。
17. 同品种不同周期的 job 是独立 Env；跨周期数据用 `DataSub`。
18. `DrawDownExit` 覆盖订单止损触发器（§4.2）；`ExgStopLoss=false` 仅隐藏远端挂单仍保留本地止损监控（§3.3）；`MAMA/VPCI/DX/Fisher` 与标准定义不同；旧 API 新代码不用。
19. **`TradeStrat` 被该策略的所有品种共用**（工厂每个 policy 只调用一次，除非配置了 `pair_params`）：工厂函数体内的闭包变量和包级变量对所有品种共享。逐品种变化的状态（上一根信号、峰值、追踪止损位、计数器）定义成结构体，在 `OnStartUp` 赋给 `s.More`，回调里 `s.More.(*T)` 读取；不要用闭包变量或按品种做键的 map。
20. 常见编译错误：品种名字符串是 `s.Symbol.Symbol`（`s.Symbol` 是 `*orm.ExSymbol`，不能当字符串或 map 键）；`pol.Params` 是 `map[string]float64`，超参只有数值（开关用 0/1），用 `pol.Def/DefInt` 读取，没有 `DefBool/DefStr/core.PInt`；方向参数 `dirt` 是 `float64`；K线时间用 `e.TimeStart/TimeStop`、周期用 `e.TFMSecs`（没有 `e.Time/TFSecs`）；订单类型在 `github.com/banbox/banbot/orm/ormo`（`*ormo.InOutOrder`、`*ormo.ExitTrigger`）；`EnterReq` 没有 `Qty/Dirt` 字段（数量 `Amount`、金额 `LegalCost`/`CostRate`、按风险比例定仓 `RiskRate`、做空 `Short: true`）；查询账户权益/可用余额/峰值回撤用 `s.AccountEquity()` / `s.AvailableBalance()` / `s.AccountDrawdown()`（无 `GetEquity`），单笔基准金额用 `s.Strat.GetStakeAmount(s)`。

## 8. 超参与优化
超参用 `pol.Def/DefInt` 声明（分布见 §1.5；`DefInt` 自动取整）；存在跨参数合法性关系（如 `fast < slow`、`maFast < maMid && maMid < maSlow`、`callbackMin <= callbackMax`）时，必须在工厂函数内调用 `pol.Require(cond, "require ...")` / `pol.Requiref(...)` 显式校验，使非法参数组合以 `[BadConfig]` 快速返回而非触发崩溃重试。`banbot optimize --config x.yml`（另有 `bt-opt` 滚动回测）；`--sampler tpe|bayes|random|cmaes|ipop-cmaes|bipop-cmaes`（默认 bayes），`--opt-rounds`（默认 30）。

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
	pol.Require(fast < slow, "require fast < slow")
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

### 示例 B：日内会话突破片段（ORB / Asian Range 伦敦突破 / Dual Thrust / R-Breaker）

```go
// 1) ORB 美股开盘区间突破（13:30 UTC 前 30m 锁定区间，20:00 UTC 日终平仓）
orb := s.OpeningRange(btime.NewYorkOpenMinutes, 30)
if btime.IsNearSessionEnd(e.TimeStart, e.TimeStop, btime.USCashCloseMinutes) {
	s.CloseOrders(&strat.ExitReq{Tag: "ny_eod"})
	return
}
if orb.Ready && orb.InPostRange && btime.SessionUSCash.Contains(e.TimeStart) && len(s.Entrys) == 0 && s.GetOrderNum(0) == 0 {
	if c := e.Close.Get(0); c > orb.High {
		s.OpenOrder(&strat.EnterReq{Tag: "orb_long", StopLoss: orb.Mid, RiskRate: 0.01})
	} else if c < orb.Low {
		s.OpenOrder(&strat.EnterReq{Tag: "orb_short", Short: true, StopLoss: orb.Mid, RiskRate: 0.01})
	}
}

// 2) Asian Range（00:00–08:00 UTC）伦敦 Killzone（08:00–10:00 UTC）突破
asia := s.SessionRange(btime.AsiaOpenMinutes, btime.AsiaCloseMinutes)
if asia.Completed && btime.InUTCWindow(e.TimeStart, btime.LondonCashOpenMinutes, 10*60) && s.GetOrderNum(0) == 0 {
	if e.Close.Get(0) > asia.High {
		s.OpenOrder(&strat.EnterReq{Tag: "ldn_brk_long", StopLoss: asia.Low})
	}
}

// 3) Dual Thrust（00:00 UTC 日切，过去 4 个完整日计算 Range）
if upper, lower, _, ok := s.DualThrustBands(btime.UTC0Minutes, 4, 0.5, 0.5); ok {
	if c := e.Close.Get(0); c > upper && s.GetOrderNum(1) == 0 {
		s.CloseOrders(&strat.ExitReq{Tag: "dt_rev_short", Dirt: core.OdDirtShort})
		s.OpenOrder(&strat.EnterReq{Tag: "dt_long", StopLoss: lower})
	} else if c < lower && s.GetOrderNum(-1) == 0 {
		s.CloseOrders(&strat.ExitReq{Tag: "dt_rev_long", Dirt: core.OdDirtLong})
		s.OpenOrder(&strat.EnterReq{Tag: "dt_short", Short: true, StopLoss: upper})
	}
}

// 4) R-Breaker 六档趋势突破 + 日内观察位反转
if rb, ok := s.RBreakerLevels(btime.UTC0Minutes); ok {
	c := e.Close.Get(0)
	if s.GetOrderNum(0) == 0 {
		if c > rb.BBreak {
			s.OpenOrder(&strat.EnterReq{Tag: "rb_trend_long", StopLoss: rb.Pivot})
		} else if c < rb.SBreak {
			s.OpenOrder(&strat.EnterReq{Tag: "rb_trend_short", Short: true, StopLoss: rb.Pivot})
		}
	} else if s.GetOrderNum(1) > 0 && rb.TodayHigh >= rb.SSetup && c < rb.SEnter {
		s.CloseOrders(&strat.ExitReq{Tag: "rb_rev_to_short", Dirt: core.OdDirtLong})
		s.OpenOrder(&strat.EnterReq{Tag: "rb_rev_short", Short: true, StopLoss: rb.BBreak})
	} else if s.GetOrderNum(-1) > 0 && rb.TodayLow <= rb.BSetup && c > rb.BEnter {
		s.CloseOrders(&strat.ExitReq{Tag: "rb_rev_to_long", Dirt: core.OdDirtShort})
		s.OpenOrder(&strat.EnterReq{Tag: "rb_rev_long", StopLoss: rb.SBreak})
	}
}

// 5) 跨周期安全每月定投 + 月末月初(TOM) + 季节性窗口（st := s.More.(*jobState) 无条件维护 prevMS）
prevMS := st.prevMS
st.prevMS = e.TimeStart
if !s.IsWarmUp && btime.IsFirstBarOfPeriodUTC(prevMS, e.TimeStart, "month") && btime.InMonthRangeUTC(e.TimeStart, 11, 4) {
	s.OpenOrder(&strat.EnterReq{Tag: "monthly_dca_halloween"})
}
```

### 示例 C：多品种风险平价 CTA / Lead-Lag 批量目标权重调仓（`BatchInOut` + `OnBatchJobs`）

```go
// TradeStrat 设 BatchInOut: true，保持 EachMaxLong: 0, EachMaxShort: 0 以允许同向动态补仓；
// OnData(Main) 每根 bar 无条件更新各品种指标并存入 s.More.(*symState)，随后在 OnBatchJobs 统一截面配权与调仓：
OnBatchJobs: func(jobs []*strat.StratJob) {
	signals := make(map[string]float64, len(jobs))
	for _, j := range jobs {
		st := j.More.(*symState)
		if st.ready {
			signals[j.Symbol.Symbol] = st.signal // 如趋势突破 +1/-1/0，或 BTC 突破时给滞涨高 Beta 币赋 +1
		}
	}
	weights := strat.InverseVolWeights(jobs, signals, 14, 1.0) // 按 1/NATR(14) 归一化至总敞口 1.0x
	strat.RebalanceBatchWeights(jobs, weights, strat.BatchRebalanceOpts{
		Tag:              "rp_cta",
		DriftThreshold:   0.03,
		MaxGrossExposure: 1.2,
		StopLossRate:     0.06, // 每笔附带 6% 个币保护止损
	})
},
```

## 10. 旧接口迁移（OnBar → OnData）
- 旧 `OnBar(s)`：改为 `OnData: strat.RouteData(strat.DataHandlers{Main: func(s, _ strat.DataEvent){…}})`。
- 旧 `OnInfoBar(s, e, pair, tf)`：放入 `Info` 处理器，用 `data.Series("close")` 等替代 `e.Close`，并在 `OnDataSubs` 声明 `{Source:"kline", TimeFrame: tf}`。
- 旧 `OnPairInfos`：改为 `OnDataSubs`（`ExSymbol` nil=当前品种）。
- 旧 `ta.Cross(a, b)`：改为 `a.Cross(b)`。
