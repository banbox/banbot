# banbot Time-Series Strategy Engine Reference (AI Edition)

banbot time-series strategy (`engine: time_series`, default): interfaces, callbacks, order/position APIs, configuration, backtest vs live differences, pitfalls, examples. Baseline: banbot v0.7.1, banta v0.4.1, banexg v0.2.66. Cross-sectional factor and statistical arbitrage are separate engines.
Each `(run_policy, timeframe, symbol)` maps to one `*strat.StratJob`, and every bar (or custom data event) triggers one callback.

## 0. Quick Start
```go
// 1) Register: registered name = group:strategy; YAML run_policy[].name must match exactly
func init() { strat.AddStratGroup("demo", map[string]strat.FuncMakeStrat{"ma": MaFn}) } // or strat.RegisterStrategy("demo:ma", MaFn)
// 2) Factory: called once per run_policy, returns *strat.TradeStrat; use pol.Def/DefInt for hyperparameters and pol.Require/Requiref for cross-parameter constraints
// 3) Entry: package main blank-imports the strategy package, then entry.RunCmd()
```

Command `banbot <cmd> --config x.yml` (`--config` is repeatable; `--config-data` inline YAML; `--no-default` ignores the default config files):
- `backtest`: `--timerange 20240101-20250101`, `--timestart/--timeend`, `--pairs BTC/USDT,ETH/USDT`, `--timeframes`, `--separate` (run each policy separately), `--bt-strict`, `--stake-amount` (overrides stake_amount), `--prg <prefix>`.
- `trade`: live / paper trading (`env: dry_run`).
- `kline down|load|agg|verify|export|purge|adj-calc`, `series down|list`, `data export|import`, `init`, `optimize`/`bt-opt` (§8), `hub`.

## 1. Common Basics
### 1.1 Time and Timeframes
- Timestamps are **int64 milliseconds**. `TimeStart` = start of the current bar; `TimeStop`/`EndMS` = end of the current bar (= start of the next bar).
- Timeframes via `ta.ParseTimeFrame`: `<positive integer><unit>`, units `s m h d w M(month, uppercase) q y`; `m` != `M`. E.g. `1m 5m 1h 4h 1d 1w 1M`.
- `BarEnv.TFMSecs` is the timeframe in milliseconds; `s.Env.BarCount(ms)`: number of bars from ms to the end of the current bar (float64).
- **UTC Virtual Trading Session Primitives (`github.com/banbox/banbot/btime`)**:
  - Anchor constants (minutes from `00:00 UTC`): `UTC0Minutes=0`, `AsiaOpenMinutes=0`, `AsiaCloseMinutes=480`(`08:00`), `LondonOpenMinutes=420`(`07:00`), `LondonCashOpenMinutes=480`(`08:00`), `LondonCloseMinutes=960`(`16:00`), `NYKillzoneOpenMinutes=720`(`12:00`), `NewYorkOpenMinutes=810`(`13:30`), `USCashCloseMinutes=1200`(`20:00`), `NewYorkCloseMinutes=1260`(`21:00`), `CMEOpenMinutes=1380`(`23:00`), `CMECloseMinutes=1320`(`22:00`).
  - Predefined windows (`btime.UTCSessionWindow{Name, StartMin, EndMin}`, methods `.Contains(ms)`/`.DurationMinutes()`): `SessionUTC0`, `SessionAsia`(`00:00–08:00`), `SessionLondon`(`07:00–16:00`), `SessionNewYork`(`13:30–21:00`), `SessionUSCash`(`13:30–20:00`), `SessionCME`(`23:00–22:00` cross-midnight), `KillzoneAsia`(`00:00–04:00`), `KillzoneLondon`(`07:00–10:00`), `KillzoneNewYork`(`12:00–15:00`), `KillzoneLondonExit`(`15:00–17:00`).
  - Functions: `btime.InUTCWindow(ms, startMin, endMin) bool` (half-open `[startMin, endMin)`, wraps across midnight when `startMin > endMin`); `btime.SessionDayIndex(ms, anchorMinutesUTC) int64` (Euclidean floor division for any anchor trading day); `btime.SessionStartMS(ms, anchorMinutesUTC) int64`; `btime.MinutesIntoSession(ms, anchorMinutesUTC) int` (`[0, 1439]`); `btime.IsNearSessionEnd(e.TimeStart, e.TimeStop, closeMinutesUTC) bool` (timeframe-adaptive EOD flatten check across `<1d` bars, supporting negative buffers like `-15`=`23:45 UTC`; returns `false` for `>=1d`).
- **UTC Calendar, Seasonality, Turn-of-Month, Expiry & Cross-Timeframe DCA Primitives (`github.com/banbox/banbot/btime`)**:
  - Calendar decomposition & month boundaries (zero-allocation, leap-year aware): `btime.UTCCalendarParts(ms) (year int, month time.Month, day int, weekday time.Weekday, hour, minute int)`, `btime.IsLeapYearUTC(year) bool`, `btime.DaysInMonthUTC(year, month) int`, `btime.DaysFromMonthStartUTC(ms) int` (`0` on the 1st), `btime.DaysBeforeMonthEndUTC(ms) int` (`0` on the last day of the month, e.g. Feb 28/29 or Mar 31), `btime.IsTurnOfMonthUTC(ms, lastNDays, firstMDays) bool` (Turn-of-Month window covering the last `lastNDays` or first `firstMDays` calendar days).
  - Weekdays, week-of-month & monthly settlement/expiry: `btime.DayOfWeekUTC(ms) int` (`0=Sunday..6=Saturday`), `btime.ISODayOfWeekUTC(ms) int` (ISO `1=Monday..7=Sunday`), `btime.WeekOfMonthUTC(ms) int` (`1..5` via `(day-1)/7+1`), `btime.NthWeekdayOfMonthUTC(year, month, weekday, n) int` & `btime.IsNthWeekdayOfMonthUTC(ms, weekday, n) bool` (e.g. `n=3, time.Friday` for US equity/CME monthly OPEX; returns `0`/`false` if absent), `btime.LastWeekdayOfMonthUTC(year, month, weekday) int` & `btime.IsMonthlyExpiryDayUTC(ms, weekday) bool` (e.g. `btime.IsMonthlyExpiryDayUTC(ms, time.Friday)` for crypto monthly/quarterly options & futures settlement on the last Friday).
  - Seasonal month windows & timeframe-agnostic periodic triggers: `btime.InMonthRangeUTC(ms, startMonth, endMonth) bool` (`1..12` inclusive; wraps across year-end when `startMonth > endMonth`, e.g. `InMonthRangeUTC(ms, 11, 4)` for the Nov–Apr Halloween / "Sell in May" window); `btime.IsFirstBarOfPeriodUTC(prevBarMS, curBarMS, period) bool` (`period` supports `"day"`/`"week"`[UTC Monday]/`"month"`/`"quarter"`/`"year"`; track the previous `e.TimeStart` unconditionally in `s.More`, safely returns `false` when `prevBarMS <= 0` or out-of-order, and guarantees exactly one trigger per period across `5m/15m/1h/4h/1d/3d` without missing coarse bars that skip `day == 1` or `hour == 0`).

### 1.2 Symbols and Markets
- Symbols: spot `BASE/QUOTE`, contract `BASE/QUOTE:SETTLE` (delivery contracts carry an extra identifier segment). `core.SplitSymbol(pair) (base, quote, settle, id)`.
- Market constants (banexg): `MarketSpot="spot" MarketMargin MarketLinear MarketInverse MarketOption MarketSwap MarketFuture`; config `market_type: spot|linear|inverse|option`, `contract_type: swap|future`.
- `*orm.ExSymbol`: `ID int32` (i.e. Sid), `Exchange`, `ExgReal`, `Market`, `Symbol`, `Combined bool`, `ListMs`, `DelistMs int64`, `AggRules string`. `s.Symbol` is the current job's symbol.
- Strategies must not contain per-exchange special-case branches.

### 1.3 Numeric and Unit Conventions
- Prices/amounts are `float64`; not ready = `NaN`;
- Ratio fields: `TradeStrat.StopLoss`, `EnterReq.RiskRate/StopLossRate/TakeProfitRate`, `TakeProfitTarget.Rate/ExitRate`, `ExitRate`, `CallbackPct` (percentage, [0.05,50]); `0.05` = 5% (`RiskRate: 0.01` = 1% account equity risk), leverage not applied.
- Distance fields: `StopLossVal`/`TakeProfitVal` are **price distances** (absolute values, not multiples), e.g. `atr*2`; `StopLoss`/`TakeProfit`/`ActivationPrice` are **prices**.
- `InOutOrder.ProfitRate` **excludes leverage** (PnL / entry notional value); `Profit` is the quote-currency amount after fees.
- `core.AmtDust=1e-8`; `core.MinStakeAmount=5.5` (minimum order size, fiat). `LegalCost` is a fiat amount, `Amount` is the quantity of the asset.

### 1.4 banexg Common Types
- `banexg.Kline`: `Time int64`(ms) `Open High Low Close Volume Quote BuyVolume float64` `TradeNum int64` (same as `ta.Kline`); `PairTFKline` = Kline + `Symbol TimeFrame`
- `Ticker`: `Symbol TimeStamp Bid/Ask/BidVolume/AskVolume High/Low/Open/Close/Last BaseVolume/QuoteVolume MarkPrice/IndexPrice`; `Trade`: `ID Symbol Side Type Amount Price Cost Order Timestamp Maker Fee`
- `OrderBook`: `Symbol TimeStamp Asks/Bids *OdBookSide` (`Price []float64`, `Size []float64`; bids descending, asks ascending)
- `Order`: `ID ClientOrderID Status Symbol Type Side Price Average Amount Filled Remaining Cost Fee Timestamp …`; `Position`: `Symbol Side(long/short) Contracts EntryPrice MarkPrice Notional Leverage UnrealizedPnl LiquidationPrice MarginMode …`
- `Market`: `Symbol Base Quote Settle Type Spot Swap Future Linear Inverse Active Taker Maker ContractSize Precision *Precision Limits *MarketLimits`; `Precision`: `Amount Price Base Quote` and `Mode*` (DecimalPlace/SignifDigits/TickSize); `Balances`: `Free Used Total map[string]float64`, `Assets map[string]*Asset`

Constants: side `OdSideBuy="buy" OdSideSell="sell"`; order types `OdTypeMarket OdTypeLimit OdTypeLimitMaker OdTypeStop* OdTypeTakeProfit* OdTypeTrailingStopMarket`; statuses `OdStatusOpen/PartFilled/Filled/Canceled/Canceling/Rejected/Expired` (strings); position side `PosSideLong/Short/Both`; margin `MarginCross/MarginIsolated`; position mode `PosModeHedge/PosModeOneWay`; TimeInForce `GTC IOC FOK GTX GTD PO`.

### 1.5 core Constants
- `core.OdDirt*`: `OdDirtShort=-1 OdDirtBoth=0 OdDirtLong=1`. **The zero value is Both** (`ExitReq{}` closes all directions by default).
- `core.OrderType*`: `OrderTypeEmpty=0` (market price), `OrderTypeMarket/Limit/LimitMaker`; `core.IsLimitOrder(t)`.
- `core.RunEnvProd="prod" RunEnvTest="test" RunEnvDryRun="dry_run"`.
- Distributions `core.VTypeUniform/VTypeNorm`; `PNorm(min,max)` (mean is the default initial value), `PNormF(min,max,mean,rate)`, `PUniform(min,max)`.
- System exit tags (custom ones allowed): `stop_loss take_profit trailing_stop draw_down bot_stop pair_del force_exit data_stuck liquidation env_end`, etc.

### 1.6 Configuration (common YAML parts)
Layering: data-dir `config.yml` + `config.local.yml`, then overlaid by `--config`. Root keys (all optional):
- `name`, `env`: bot name; prod/test/dry_run
- `market_type`, `contract_type`, `leverage`: market, contract type, default leverage
- `stake_amount` / `stake_pct` / `max_stake_amt`: base amount per order (fiat); percentage (takes precedence over stake_amount); upper limit
- `open_vol_rate`(1): upper ratio of order volume / average volume of the last 5 bars
- `min_open_rate`(0.5), `low_cost_action`: order policy when the balance is insufficient
- `wallet_amounts`: initial backtest wallet, e.g. `{USDT: 10000}`
- `stake_currency`: restrict the quote currency
- `time_start`/`time_end` or `timerange`: data start/end (timestamp/date/datetime)
- `run_timeframes`: allowed timeframes (strategy may override)
- `run_policy`: strategy list, multiple can run simultaneously
- `pairs`/`pairlists`/`pairmgr`: static symbols / dynamic filter chain / scheduled refresh
- `max_open_orders`, `max_simul_open`: global max open orders; max simultaneous new orders within the same bar
- `order_bar_max`(500): max number of bars simulated backward in backtest when searching for "orders still open before start"
- `bt_net_cost`(15 seconds): backtest order latency (simulated slippage)
- `bt_strict`, `bt_no_kline_download`, `bt_legacy_intrabar`, `bt_legacy_order_metrics`, `bt_legacy_wallet_price`: backtest strict / legacy-behavior switches
- `close_on_stuck`(20 minutes): in live trading, close all positions if no bar arrives within the timeout
- `fatal_stop`, `fatal_stop_hours`(8): global loss circuit breaker
- `exchange.name`, `exchange.<name>`: exchange (built-in binance/okx/bybit); keys under `<name>` are passed as banexg init params (snake_case→camelCase), e.g. `proxy`, `fees.linear.taker`
- `accounts.<acc>`: `stake_rate stake_pct leverage max_stake_amt max_pair max_open_orders no_trade`; credentials `<exchange>.<env>.api_key/api_secret`
- `database`: `url`, `db_type: questdb/timescale` (auto-detected if empty), `auto_create`, `max_pool_size`, etc.
- `api_server`, `rpc_channels`, `webhook`, `mail`: services and notifications

`run_policy[]` keys:
- `name`: registered name `group:strategy`; `engine` defaults to `time_series`
- `run_timeframes`: timeframes allowed for this strategy (overrides root level); legacy key `timeframes` is a comma-separated string
- `refine_tf`: matching timeframe: string (`1m`) or integer (multiple relative to the main timeframe, `5` = 1/5)
- `filters` / `pairs`: symbol filter chain (same format as `pairlists` elements) / fixed symbol list
- `max_pair`, `max_open`, `max_simul_open`, `order_bar_max`: limits on symbol count and order count (`order_bar_max` overrides `TradeStrat.OdBarMax`)
- `stake_rate`: order multiplier (>0 overrides `TradeStrat.StakeRate`)
- `stop_loss`: strategy default stop-loss ratio: `0.05` or `"5%"` (overrides `TradeStrat.StopLoss`); integer form (e.g. `5`) errors, 0 is ignored, negative errors
- `dirt`: `any/long/short`
- `params`: scalar hyperparameters `{name: float}`, read via `pol.Def/DefInt`
- `pair_params`: `{pair: {name: value}}` per-symbol hyperparameters; when present, the factory is **called again** for that symbol, so closure variables are independent per symbol
- `strat_perf`: automatic strategy-symbol performance down-weighting: `enable min_od_num max_od_num min_job_num mid_weight bad_weight`
- Other unknown keys: go into `Policy.More`, read via `pol.More[...]`

Hyperparameters: `pol.Def("name", dv, core.PNorm(lo,hi)) float64`, `pol.DefInt(...) int`. Names must be valid identifiers; values come from `params`, falling back to `dv` when not configured. Validate cross-parameter constraints inside the factory with `pol.Require(cond, msg)` or `pol.Requiref(cond, format, args...)` (e.g. `pol.Require(fast < slow, "require fast < slow")`); when false, the process exits with `Error: [BadConfig] <msg>` so parameter search marks the trial invalid and skips it instead of crashing (do not write ad-hoc `panic(...)`). `pol` is read-only and shared by all jobs; **do not store hyperparameters in structs or `More`**.

### 1.7 Runtime Isolation (v0.5+)
- The factory signature is fixed: `func(pol *config.RunPolicyConfig) *strat.TradeStrat`. Do not keep orders, indicators, or per-symbol state in package-level variables (put them in `StratJob.More`), and do not store Runtime/State objects yourself.

## 2. banta Indicator Library (`github.com/banbox/banta`, alias `ta`)
### 2.1 Call Model
- Inputs and outputs are all `*ta.Series`; each bar is computed only once and cached; repeated calls with the same args on the same bar return the cached result.
- Multi-output indicators return multiple `*Series` (or `.Cols`), received together.
- Reading values: `x.Get(0)` current, `x.Get(1)` previous bar, `x.Get(i)` the i-th bar back. Insufficient data outputs NaN (check with `e.Close.Len()`, or let `WarmupNum` cover the longest period).
- The vector (non-cached) version is in `github.com/banbox/banta/tav` (batch research); strategy callbacks use `ta`.
- Series are bounded: `MaxCache` (= `NumTaCache`, default 1500); when it reaches 1.5×MaxCache, OHLC is Cut back to MaxCache, so history length is typically 1500~2249.
- Custom indicators (computed incrementally per bar, `To` reuses the cache):

```go
func MyMax(obj *ta.Series, period int) *ta.Series {
	res := obj.To("_mymax", period) // derived series cached by (name, period)
	if res.Cached() {
		return res
	}
	if obj.Len() < period {
		return res.Append(math.NaN())
	}
	return res.Append(slices.Max(obj.Range(0, period))) // Range returns most recent first
}
```

### 2.2 BarEnv and Series
```go
type BarEnv struct { // s.Env is the main timeframe; for auxiliary/cross-symbol bars use s.RefEnv(symbol, tf) or DataFields.BarEnv()
    TimeStart, TimeStop int64; Exchange, MarketType, Symbol, TimeFrame string; TFMSecs int64
    BarNum, MaxCache, VNum int
    Open, High, Low, Close, Volume, Quote, BuyVolume, TradeNum *Series
}
type Series struct { ID int; Env *BarEnv; Data []float64; Cols []*Series; Time int64; More interface{}; ... }
```

- `Get(i int) float64`: i=0 is the latest, out of range gives NaN
- `Range(start, stop int) []float64`: most recent first, `[start, stop)`; returns a copy
- `RangeValid(start, stop) ([]float64, []int)`: skips NaN, returns values and bar offsets
- `Len() int`: history length (uses the first column if Cols exist)
- `Back(n int) *Series`: shift back n bars: `x.Back(1).Get(0)` = previous bar's value
- `To(name string, v int) *Series`: get/create a derived series by (name, v) (**recommended**)
- `Cached() bool`: whether it has already been computed on this bar
- `Append(v)/Set(v)`: write (`float64/int/[]float64/[]*Series`), for custom indicators only
- `Add/Sub/Mul/Div/Min/Max(obj)`: `obj` is `*Series/int/float32/float64`, returns a derived series
- `Abs()`, `Cut(keep int)`: absolute value; truncate history (afterwards the usable length of `Back/Range` shrinks)
- `Cross(obj) int`, `CrossUp(obj)/CrossDown(obj) bool`: see below
- `CopyTo(e *BarEnv)`: copy to another BarEnv
- `BarEnv.OnBar(barMs,o,h,l,c,v,quote,buyVol,tradeNum) error`/`OnBar2(...)`, `ta.NewBarEnv(exg,market,symbol,tf) (*BarEnv,error)`: build your own BarEnv and feed data (not needed in strategy callbacks)

`Cross`:
- Signed integer: positive = cross up, negative = cross down, 0 = no record; |value|−1 = number of bars since the latest cross. **A cross up on this bar is `1`, then `2,3,…`; cross down `-1,-2,…`**.
- For events use `== 1`/`== -1`; `CrossUp()/CrossDown()` = `Cross>0/<0` is a **state** of "direction of the latest cross".
- Recording starts **from the first time Cross is called on that Series**, history is not replayed; it must be called unconditionally on every bar.
- Comparison target: `Series` (compares `Get(0)`, cache key is the other series' ID) or a constant (`int`/`float`, cache key `int(v*100)`; two constants differing by <0.01 may collide).

**Float-constant cache collision**: when `Add/Sub/Mul/Div/Min/Max` receive a `float64` constant, the derived key is `int(v*10)`, so on the same series and bar `Add(0.11)` and `Add(0.14)` return the same cache. Workarounds: compute the scalar directly in Go, or use `int`/different derived names.

### 2.3 Indicator List
Conventions: signatures omit the package name. **Go has no default parameters, so you must pass them explicitly** (only the variadic params of `CCI`, `ADX`, `DMI`, `StochF/STOCHF` have implicit defaults); `〔…〕` denotes default/common parameter values.
**(full)** = on every new bar, copies the entire Series history (1500~2249 bars) and fully recomputes with the `tav` vector version to take the last value: O(history) with allocations (at ~2000 bars Fisher ≈ 32µs/call, SMA < 0.1µs/call); overhead is bounded, but control it for many symbols × many timeframes × many calls; repeated calls on the same bar are still computed only once. Window-type ones (Fisher, WilliamsPercent, KST, Ichimoku, DonchianPBand, Correlation) are unaffected by Cut; **recursive ones (Supertrend, PMAX, Keltner*, VIDYA, SSF, MAMA, DX, etc.) have values that depend on the window start**, so after Cut they differ slightly from full-history computation.

**Price and Composite**

- `AvgPrice(e *BarEnv)`: (H+L+C)/3
- `HL2(h,l)`, `HLC3(h,l,c)`, `HLCC4(h,l,c)`, `OHLC4(o,h,l,c)`: (H+L)/2, (H+L+C)/3, (H+L+2C)/4, (O+H+L+C)/4
- `Abs(a)`, `Change(a)`, `Sub(a,b)`: \|a\|, a−a[1], a−b
- `HeikinAshi(e) (ho,hh,hl,hc *Series)`: Heikin-Ashi candles

**Moving Averages**

- `SMA(obj, period)`: simple mean; NaN when valid values are fewer than period
- `EMA(obj, period)`: = `EMABy(obj,period,0)`, α=2/(n+1)
- `EMABy(obj, period, initType int)` 〔0〕: 0: initialize with SMA of the first period bars; 1: initialize with the first valid value
- `RMA(obj, period)`, `SMMA`: = `RMABy(obj,period,0,NaN)`, Wilder α=1/n
- `RMABy(obj, period, initType int, initVal float64)` 〔0, NaN〕: non-NaN initVal is used as the first previous value; otherwise same as EMABy
- `WMA(obj, period)` 〔9〕: linearly weighted, weights 1..n
- `HMA(obj, period)` 〔9〕: WMA(2·WMA(p/2)−WMA(p), √p)
- `DEMA(obj, period)` / `TEMA(obj, period)`: 2·EMA−EMA(EMA) / 3·EMA1−3·EMA2+EMA3
- `T3(obj, period)`: Tillson T3, vFactor fixed at 0.7
- `TRIMA(obj, period)`: SMA(SMA(obj,(p+1)/2), p/2+1)
- `ZLMA(obj, period)`: zero-lag EMA: EMA(obj+obj−obj[(p−1)/2])
- `VIDYA(obj, period)`: variable-length EMA with α=2/(p+1) modulated by \|CMO\|, first value SMA (full)
- `KAMA(obj, period)` 〔10〕: = `KAMABy(obj,period,2,30)`
- `KAMABy(obj, period, fast, slow int)` 〔10, 2, 30〕: adaptive by efficiency ratio ER; NaN when ER is not ready
- `ALMA(obj, period int, sigma, distOff float64)` 〔10, 6.0, 0.85〕: Arnaud Legoux Gaussian-weighted MA
- `SSF(obj, period)`: Ehlers two-pole super smoother filter (full)
- `SWMA(obj)`: fixed 4-point weights (1,2,2,1)/6
- `MAMA(obj, fast, slow float64) (m, f)`: **not Ehlers MAMA/FAMA**: p=max(1,round((fast+slow)/2)); m=EMA(obj,p), f=EMA(m,p) (full). With fast/slow=0.5/0.05, p=1 and m is the original series
- `VWMA(price, vol, period)` 〔20〕: Σ(price·vol)/Σvol, rolling window
- `VWAP(first, second *Series, rest ...*Series)`: cumulative VWAP, no period, no intraday reset; 2 args (close, volume), 4 args (high, low, close, volume)
- `Sum(obj, period)`: rolling sum, skips NaN; NaN when valid values are fewer than period

**Momentum and Oscillators**

- `RSI(obj, period)` 〔14〕: Wilder RSI 0~100; the first period deltas are initialized with a simple mean
- `RSI50(obj, period)`: RSI−50
- `ROC(obj, period)` 〔9〕: (close−close[n])/close[n]×100
- `ROCR(obj, period)`: close/close[n] (ratio, not percentage); NaN when the denominator is 0/NaN
- `MOM(obj, period)`: close−close[period]
- `CMO(obj, period)` 〔9〕: = `CMOBy(obj,period,0)`
- `CMOBy(obj, period, maType int)` 〔9, 0〕: Chande momentum oscillator −100~100; 0 = TA-Lib Wilder smoothing, 1 = TradingView simple sum
- `RMI(obj, period, montLen int)` 〔14, 3〕: relative momentum index 0~100 (RMA smoothing of montLen up/down moves)
- `CRSI(obj, period, upDn, roc int)` 〔3, 2, 100〕: Connors RSI = `CRSIBy(...,0)`
- `CRSIBy(obj, period, upDn, roc, vtype int)` 〔3, 2, 100, 0〕: (RSI + RSI of UpDown + rank)/3; vtype 0 = PercentRank of ROC(1), 1 = ROC(roc)
- `PercentRank(obj, period)`: within the window (period bars, including current), the count of values **excluding current** that are ≤ the current value ×100/**period** (max (period−1)/period×100); NaN if the window is insufficient
- `TD(obj)`: DeMark sequential count (compares with 4 bars ago, accumulates in the same direction, resets on reversal), first 4 bars NaN
- `UpDown(obj, vtype int)`: consecutive up/down count; 0 = TradingView convention, 1 = classic ±1 only
- `KDJ(high, low, close, period, sm1, sm2 int) (K, D, RSV)` 〔9, 3, 3〕: RMA smoothing
- `KDJBy(high, low, close, period, sm1, sm2 int, maBy string) (K, D, RSV)` 〔maBy `"rma"`/`"sma"`〕: other maBy values panic
- `Stoch(high, low, close, period)` 〔14〕: 100·(close−LL)/(HH−LL), outputs 50 when the range is 0
- `STOCH(close, high, low, period)` 〔14〕: same as Stoch, **parameter order close,high,low** (do not mix up)
- `StochF(high, low, close, period int, smooth ...int) (fastK, fastD)` 〔smooth omitted or ≤0 = 3〕: fastK raw value, fastD=SMA(K,smooth)
- `STOCHF(close, high, low, period int, smooth ...int)` 〔same as StochF〕: parameter order close,high,low
- `StochRSI(obj, rsiLen, stochLen, maK, maD int) (fastK, fastD)` 〔14,14,3,3〕: RSI→Stoch→SMA(maK)→SMA(maD)
- `WillR(e *BarEnv, period)` 〔14〕: Williams %R −100~0; reads High/Low/Close of e, does not accept Series
- `WilliamsPercent(high, low, close, period)`: same %R: −100·(HH−close)/(HH−LL), window includes current (full)
- `CCI(obj, args ...interface{})` 〔period 20〕: `CCI(close, period)`; `CCI(high, low, close, period)` uses HLC3; (src−SMA)/(0.015·AvgDev)
- `AO(high, low, fast, slow int)`: SMA(HL2,fast)−SMA(HL2,slow)
- `DPO(obj, period)`, `Dpo`: pandas-ta non-centered: close−SMA(close,period) lagged by floor(period/2)+1 bars
- `Fisher(high, low, period)`: x=2·((H+L)/2−LL)/(HH−LL)−1 (window includes current) clipped to ±0.999, outputs 0.5·ln((1+x)/(1−x)). **No 0.33/0.67 recursive smoothing, no signal line**; NaN when HH==LL or for the first period−1 bars (full)
- `ULTOSC(high, low, close, short, medium, long int)` 〔7, 14, 28〕: ultimate oscillator, weights 4/2/1
- `TRIX(obj, period)`: single-bar percentage change of the triple EMA, no signal line
- `TSI(obj, short, long int)`: EMA(EMA(Δ,short),long)/EMA(EMA(\|Δ\|,short),long)×100
- `STC(obj, period, fast, slow int, alpha float64)` 〔12, 26, 50, 0.5〕: Schaff trend cycle 0~100: MACD→period-window percentile→α smoothing→period-window percentile→α smoothing; 0 when the window is flat; NaN input resets state; incremental
- `KST(obj, r1,r2,r3,r4, s1,s2,s3,s4 int)` 〔common 10,15,20,30,10,10,10,15〕: Σ wᵢ·SMA(ROC(rᵢ)%, sᵢ), weights 1,2,3,4; **no signal line**; first valid value at max(rᵢ+sᵢ)−1 (full)
- `CTI(obj, period)` 〔20〕: correlation trend indicator −1~1 (linear-regression correlation)
- `Stiffness(obj, maLen, stiffLen, stiffMa int)` 〔100, 60, 3〕: 0~100: bound=SMA−0.2·StdDev, count bars within stiffLen where close>bound, then EMA(stiffMa)
- `DV2(h, l, c, period, maLen int)` 〔252, 2〕: bounded DV2 0~100, percentile rank of the maLen average (not the backtrader version)
- `ER(obj, period)` 〔8〕: Kaufman efficiency ratio 0~1 = \|net change\|/Σ\|per-bar change\|

**Trend**

- `MACD(obj, fast, slow, smooth int) (macd, signal)` 〔12, 26, 9〕: SMA initialization
- `MACDBy(obj, fast, slow, smooth, initType int)` 〔initType 0〕: 0 = SMA initialization (international convention), 1 = first valid value (Chinese/MyTT convention)
- `ADX(high, low, close, period int, smoothing ...int)` 〔14; smoothing omitted/0 = period〕: classic ADX
- `ADXBy(high, low, close, period, smoothing, method int)` 〔14, 0, 0〕: method 0: RMA-smoothed DX; 1: SMA-smoothed DX
- `DX(high, low, close, period)`: **non-standard**: per-bar \|+DM−(−DM)\|/(+DM+(−DM))×100, raw DM unsmoothed, noisy; for the smoothed version use `ADX`/`DMI` (full)
- `DMI(high, low, close, period int, smoothing ...int) (+DI, −DI, ADX)` 〔smoothing omitted = period〕: smoothing applies only to ADX, DI uses period
- `PluMinDI(high, low, close, period) (+DI, −DI)` 〔14〕: Wilder convention
- `PluMinDM(high, low, close, period) (+DM, −DM)` 〔14〕: Wilder smoothed sums
- `Aroon(high, low, period) (Up, Osc, Dn)`: Up=(period−HighestBar(period+1))/period×100, Dn likewise using LowestBar; Osc=Up−Dn
- `AroonOsc(high, low, period)`, `AROONOSC`: Up−Dn, −100~100
- `SAR(high, low, step, max float64)`, `PSAR` 〔0.02, 0.2〕: parabolic SAR (TA-Lib convention): AF starts at step, +step on each new extreme, capped at max; first bar NaN, from the second bar the low of that bar is the initial SAR (initially rising); on reversal SAR takes the previous extreme point; returns a **price**
- `Supertrend(high, low, close, period int, multiplier float64)` 〔common 10, 3〕: returns the **trend line price, not direction**: lower band in an uptrend, upper band in a downtrend; (H+L)/2±mult·ATR ratchet, flips when close crosses the opposite band; initially up if close≥(H+L)/2. Determine direction via `line < close` (bullish) (full)
- `PMAX(high, low, close, period int, multiplier float64) (line, dir)` 〔common 10, 3〕: EMA(close,period)±mult·ATR(period) ratchet line (MA period = ATR period, cannot be specified separately), flips when MA crosses the line; dir=+1 up (line is the lower band) / −1 down (upper band) (full)
- `Ichimoku(high, low, close, conversion, base, span int) (conv, base, spanA, spanB, lag)` 〔9, 26, 52〕: conv/base = window (HH+LL)/2; spanA=(conv+base)/2; spanB = span-window (HH+LL)/2; lag=**close[i−base]** (`Close.Get(base)`). **spanA/spanB are not shifted forward by base bars** (to compare against the "current cloud" take `Back(base)` yourself); conv is NaN for the first conversion−1 bars, spanB for the first span−1 bars (full)
- `UTBot(c, atr *Series, rate float64)` 〔rate commonly 1〕: signal (**event, non-zero only on the breakout bar**): 1=buy, −1=sell, 0=none; fires when the trailing stop line c±atr·rate is crossed, the line is not exposed. atr must be passed in by yourself (e.g. `ta.ATR(...)`); cache and state are attached to atr, keyed by rate
- `LinReg(obj, period)`: linear regression endpoint value
- `LinRegAdv(obj, period int, angle, intercept, degrees, r, slope, tsf bool)` 〔all false = endpoint value〕: flags choose the output, priority slope>intercept>angle>r>tsf>endpoint value
- `LinearRegAngle(obj, period)`, `LINEARREG_ANGLE`: atan of the regression slope, in degrees; first period−1 bars NaN
- `Slope(obj, period)`: least-squares slope
- `Correlation(a, b, period)`: Pearson correlation (full)
- `CHOP(e *BarEnv, period)` 〔14〕: choppiness index; reads High/Low/Close of e; higher = choppier, lower = more trending

**Volatility and Channels**

- `TR(high, low, close)`: true range, first bar NaN
- `ATR(high, low, close, period)` 〔14〕: = RMA(TR, period)
- `NATR(high, low, close, period)`: 100·ATR/close
- `StdDev(obj, period)`, `STDDEV` 〔20〕: = `StdDevBy(obj,period,0)` population standard deviation
- `StdDevBy(obj, period, ddof int) (stddev, mean)` 〔ddof 0〕: variance divided by (period−ddof)
- `AvgDev(obj, period)`: Σ\|x−SMA\|/period
- `BBANDS(obj, period int, stdUp, stdDn float64) (upper, mid, lower)` 〔20, 2, 2〕: mid=SMA
- `Donchian(high, low, period) (upper, middle, lower)`: window includes the current bar
- `DonchianPBand(high, low, close, period)`: (close−lower)/(upper−lower), position within the channel, NaN when width is 0; cache attached only to close (full)
- `KeltnerChannel(high, low, close, period int, multiplier float64) (upper, mid, lower)`: EMA(close,period) ± mult·ATR(period) (full)
- `KeltnerWBand(high, low, close, period int, mult float64)`: (upper−lower)/mid relative bandwidth (full)
- `Squeeze(high, low, close, period)`: TTM squeeze: 1 if BB (SMA±2σ population) is fully inside KC, else 0; KC=SMA±1.5×window-average TR (SMA convention); NaN when history is insufficient
- `Highest/Lowest(obj, period)`: highest/lowest over the period including the current bar
- `HighestBar/LowestBar(obj, period)`: offset of the highest/lowest value from the current bar (0 = current)

**Volume**

- `OBV(close, volume)`: cumulative on-balance volume: add on up, subtract on down
- `ADL(env)`: cumulative A/D line, reads env.Volume
- `ADOSC(env, fast, slow int)`: EMA(ADL,fast)−EMA(ADL,slow)
- `ChaikinOsc(env, shortLen, longLen int)` 〔3, 10〕: same convention as ADOSC
- `CMF(env, period)` 〔20〕: Σ(MFV)/Σvolume −1~1; bars with volume≤0 output NaN
- `MFI(e, period)` 〔14〕: money flow index 0~100, typical price × env.Volume
- `EFI(env, period)`: Elder force index: EMA(period) of (per-bar close change × volume), first bar NaN
- `VPCI(close, volume, period)`: **misnamed**: only VWMA(close,volume,period)−SMA(close,period) (price-volume MA difference), not the full VPCI (full)

**Patterns and Pivots**

- `PivotHigh/PivotLow(src, left, right int)`: outputs the pivot high/low after confirmation by the right-side `right` bars, otherwise NaN, lagging by right bars
- `CDL3INSIDE/CDL3LINESTRIKE/CDL3OUTSIDE/CDLENGULFING(open, high, low, close)`: three inside / three-line strike / three outside / engulfing; 100/−100/0
- `CDLDRAGONFLYDOJI/CDLGRAVESTONEDOJI(open, high, low, close)`: dragonfly / gravestone doji; 100/0
- `CDLHAMMER/CDLHANGINGMAN/CDLMORNINGSTAR/CDLSHOOTINGSTAR(open, high, low, close)`: hammer / hanging man / morning star / shooting star; only 100/0, no trend-context check (morning star does not check gap and penetration)

Multi-input indicators attach their cache to only one of the inputs (`HLC3`/`AvgPrice`/`DonchianPBand`/`Supertrend`/`Fisher` (attached to high)/`UTBot` (attached to atr), etc.): the same attached series paired with different other inputs hits stale values, so the input combination must stay fixed.
The Chan-theory graph structures in `chanlun.go` (CGraph/CPoint/CPen/CSeg/CCentre/CTrend) are not numeric indicators and are not expanded here.

## 3. Strategy Structure and Callbacks
### 3.1 TradeStrat

- `Name` (string): written by the framework
- `Version` (int): written to the order's `StgVer`
- `WarmupNum` (int): number of warmup bars; during warmup `IsWarmUp=true`, and order/exit enqueues are ignored
- `OdBarMax` (int): max bars simulated backward in backtest when searching for unfinished positions; priority `run_policy.order_bar_max` > this field > global `order_bar_max`(500)
- `MinTfScore` (float64): timeframe quality threshold, 0 means 0.75 (the comment saying 0.8 is outdated)
- `DrawDownExit` (bool): default false; true enables "profit drawdown exit" for fully-entered orders (§4.2)
- `HedgeOff` (bool): disable contract hedge (two-way) positions (one-way mode)
- `BatchInOut`/`BatchInfo` (bool): whether jobs enter the batch pool after OnData of the main/auxiliary timeframe (requires `OnBatchJobs`/`OnBatchInfos`)
- `StakeRate` (float64): order multiplier (multiplied onto stake); overridden by `run_policy.stake_rate>0`
- `StopLoss` (float64): default stop-loss ratio (without leverage); overridden by `run_policy.stop_loss`
- `StopEnterBars` (int): cancel a limit entry order not filled within this many bars (0 = disabled)
- `OrderOnRotation` (string): for the old job when a symbol is rotated out: `close` close positions / `hold` keep holding without opening new orders / `open` allow both opening and holding
- `EachMaxLong`/`EachMaxShort` (int): max same-direction orders per symbol; 0 unlimited, -1 forbids that direction
- `TimeFrames` (string): legacy form: comma-separated timeframes
- `RunTimeFrames` ([]string): allowed timeframes (if empty use `run_policy.run_timeframes`, then `TimeFrames`, then root config)
- `RefineTF` (interface{}): strategy-level default sub-bar matching timeframe (e.g. `"5m"`, `"1m"`, `12`, `"3-6"`; takes effect automatically when YAML `run_policy[].refine_tf` is unset, and overridden when explicitly configured)
- `WsSubs` (map[string]string): live Ws subscription config (required by `OnWs*`)
- `Outputs` ([]string): strategy output text
- `OnStartUp` (`func(*StratJob)`): called once when each job is created; initialize `s.More` here
- `OnDataSubs` (`func(*StratJob) []*DataSub`): declare extra subscriptions (auxiliary timeframes, other symbols, custom series)
- `OnSymbols` (`func([]string) []string`): rewrite the symbol list
- `OnData` (`FnOnData`): **main callback** `func(s *StratJob, data DataEvent)`, used with `RouteData`
- `OnBar`/`OnInfoBar`/`OnPairInfos` (legacy interfaces): compatibility only; **must not be set together with OnData** (build panic)
- `OnCheckExit` (`func(*StratJob,*ormo.InOutOrder) *ExitReq`): called on every main bar (non-warmup) for each fully-entered order with `CanClose()`; a non-nil return closes it, and the framework forcibly sets the returned `OrderID` = that order and `Dirt` = that order's direction
- `GetDrawDownExitRate` (`func(*StratJob,*ormo.InOutOrder,maxChg float64) float64`): drawdown ratio r; `>0` is used, `0` means not set, `<0`/callback unset uses the default table
- `OnOrderChange` (`func(*StratJob,*ormo.InOutOrder,chgType int)`): §3.7
- `OnBatchJobs` (`func([]*StratJob)`): all main-timeframe jobs at the same timestamp (requires `BatchInOut`; ordered in deterministic backtests by neutral `strat.BarPriority(barMS, symbol, bt_bar_order_seed)` hash priority rather than alphabetical order)
- `OnBatchInfos` (`func(tf string, jobs map[string]*JobEnv)`): all auxiliary jobs at the same timestamp (requires `BatchInfo`)
- `OnWsTrades(s,pair,[]*banexg.Trade)`/`OnWsDepth(s,*banexg.OrderBook)`/`OnWsKline(s,pair,*banexg.Kline)`/`OnWsData(s,*orm.DataSeries)` (live): tick trades, order book, real-time bars (may be unclosed), generic time-series events
- `PickTimeFrame` (`func(symbol string, tfScores []*core.TfScore) string`): choose a timeframe for a symbol (default: the first with Score≥MinTfScore)
- `OnPostApi` (`func(*core.ApiClient, map[string]interface{}, jobs) error`): PostAPI callback
- `OnShutDown` (`func(*StratJob)`): called when a job is removed; `OnStratExit` (`func()`): called once when the strategy exits

Registration: `strat.RegisterStrategy(name, fn)`, `strat.AddStratGroup(group, map[string]FuncMakeStrat)`, `strat.GetStrategyFactory(name)`.

### 3.2 Callback Lifecycle (sequence)
1. Factory `fn(pol)` (called again per symbol when `pair_params` exist).
2. For each (timeframe, symbol), create a `StratJob` → `OnStartUp(job)` → `OnDataSubs(job)` (recomputed when the symbol list changes).
3. For each data event (in time order): `job.SetData(evt)` updates `DataFields` (Series appended automatically) and sets `IsWarmUp` → `OnData(job, DataEvent{Role, Symbol, DataFields})`: `DataRoleMain` main bar, `DataRoleInfo` auxiliary bar, `DataRoleCustom` custom series. After a main bar, if `BatchInOut` the job enters the batch pool; likewise for auxiliary bars with `BatchInfo`.
4. At the end of a main-bar event (non-warmup): `OnCheckExit` and drawdown exits → process the request queue (backtest matching / live submission).
5. Batch: jobs at the same timestamp are collected and then `OnBatchJobs`/`OnBatchInfos` are called together (live has a `DelayBatchMS`=3000ms wait window), then requests are processed.
6. `OnOrderChange(job, od, chg)`: triggered on order creation/entry/exit/real-order changes.
7. Live `OnWs*` are independent of bar driving.
8. Stop: `OnShutDown(job)` (job removed), `OnStratExit()` (strategy exit).

- A request is valid only **within the event that triggered it**: it is processed at the end of this event after being enqueued; unprocessed ones are discarded at the next `InitBar` (live WARN `ignore unhandle Entry/Exit`).
- Warmup: OnData is still called, but order/exit enqueues are ignored and return nil.
- OpenOrder may be called multiple times in the same bar producing multiple requests; deduplicate yourself (`len(s.Entrys)`, `s.GetOrderNum(0)`; `s.OrderNum` includes enqueued requests).

### 3.3 StratJob (runtime context for each (strategy, timeframe, symbol))
- `Strat *TradeStrat`: owning strategy
- `Env *ta.BarEnv`: main-timeframe bar environment (`Env.Close/High/Low/Open/Volume/Quote/BuyVolume/TradeNum`, `Env.TimeStop` = end of this bar)
- `DataHub *DataHub`: multi-subscription data view; use `s.Data(sub)` or `data.Series()`
- `Entrys []*EnterReq`, `Exits []*ExitReq`: pending requests within this event (read-only)
- `LongOrders`, `ShortOrders []*ormo.InOutOrder`: snapshot of unfinished orders (GetOrders is authoritative); `IsWarmUp bool` whether warming up
- `Symbol *orm.ExSymbol`, `TimeFrame`, `Account string`: current symbol, timeframe, account
- `OrderNum`, `EnteredNum int`: number of unfinished orders (including queued); number of (partially) entered orders
- `CheckMS`, `LastBarMS int64`: last processing time, end time of the previous bar
- `MaxOpenLong`, `MaxOpenShort int`: 0 unlimited, -1 forbidden, >0 upper limit (initialized from EachMaxLong/Short, modifiable in callbacks)
- `CloseLong`, `CloseShort bool`: whether closing longs/shorts is allowed (when false, closing in that direction errors with `close order disabled`)
- `ExgStopLoss`, `ExgTakeProfit bool`: default true. When false, live trading **does not pre-post conditional stop-loss/take-profit orders on the remote exchange order book, while still enforcing local price-monitored stop-loss/take-profit in both backtest and live** (`StopLoss==0` still means no stop-loss)
- `LongSLPrice`, `ShortSLPrice`: job-level default stop-loss price (used when the request doesn't specify one at order time)
- `LongTPPrice`, `ShortTPPrice`: job-level default take-profit price (used when the request doesn't specify `TakeProfit/TakeProfitVal/TakeProfits` at order time)
- `More interface{}`: mutable state independent per job (initialize in OnStartUp)

Methods:
- `OpenOrder(req *EnterReq) *errs.Error` / `CloseOrders(req *ExitReq) *errs.Error`: enqueue open/close, §3.5/§3.6
- `GetOrders(dirt float64) []*ormo.InOutOrder`: `dirt>0` long, `<0` short, `0` all (shorts first); snapshot
- `GetOrderNum(dirt float64) int`: number of unfinished orders in the same direction
- `Position(dirt float64, enterTag string) float64`: held cost ÷ base stake amount (a multiple, not a quantity); empty `enterTag` doesn't filter
- `PositionAvgPrice(dirt float64, enterTag string) float64`: average position price; **panics** when dirt is 0, must be long or short
- `GetAvgCostPrice(ods ...*ormo.InOutOrder) float64`: weighted average entry price of the specified orders
- `CanOpen(short bool) bool`: judged by MaxOpenLong/Short and spot restrictions (spot cannot short)
- `SetAllStopLoss(dirt float64, args *ormo.ExitTrigger) *errs.Error` / `SetAllTakeProfit(...)`: set stop-loss/take-profit triggers for orders in a direction; `args=nil` cancels; only applies to orders whose status is between partially-entered and partially-exited; when `args.Rate∈(0,1)`, allocated across orders by total position×Rate; panics when `dirt=0` and both long and short exist
- `Data(sub *DataSub) *DataFields`: get a subscription view; `ExSymbol==nil` means the current symbol
- `RefKlineSub(symbol, timeframe string, warmupNum ...int) *DataSub` (or `strat.NewRefKlineSub(s, symbol, tf, warmupNum...)`): build a read-only auxiliary-timeframe or cross-symbol K-line subscription for `OnDataSubs`. `symbol==""` or `"_cur_"` targets the current `s.Symbol`; non-empty symbols such as `"BTC"` or `"BTC/USDT:USDT"` are normalized to the job's exchange/market and resolved/ensured as read-only `*orm.ExSymbol` in `SymbolState` (warmed up and delivered even when absent from trading `pairs`, **without creating a trading StratJob or placing orders on it**); when `warmupNum` is omitted it defaults to `s.Strat.WarmupNum`
- `RefEnv(symbol, timeframe string) *ta.BarEnv`: read the synchronized `*ta.BarEnv` of an auxiliary timeframe or reference symbol directly from `s.DataHub`; returns `nil` before the first bar arrives, and once non-`nil` guarantees `Open/High/Low/Close/Volume/Quote/BuyVolume/TradeNum` are all non-`nil` for `ta.SMA(env.Close, ...)`, `ta.ATR(env.High, env.Low, env.Close, ...)`, `ta.MFI(env, 14)`, `ta.WillR(env, 14)`, `ta.HeikinAshi(env)`
- `AccountEquity() float64`: real-time total equity of the owning account (pricing fiat, cash balance + unrealized PnL); during warmup or before wallet initialization, falls back to configured initial capital or a finite positive `GetStakeAmount(s)` value (never 0)
- `AvailableBalance() float64`: current available balance / available margin of the owning account (pricing fiat)
- `AccountDrawdown() float64`: current drawdown ratio in `[0, 1]` relative to the account's run-to-date peak equity (High-Water Mark; 0 when no drawdown)
- `GetStakeAmount`: `s.Strat.GetStakeAmount(s)` base stake amount (fiat); `OrderCounts() (orderNum, enteredNum int)` same as OrderNum/EnteredNum
- **No-Lookahead Intraday Session & Previous-Day Range Components (automatically isolated per `StratJob` with automatic warmup replay, zero cross-symbol contamination)**:
  - `OpeningRange(anchorMinutesUTC, rangeMinutes int) OpeningRangeSnapshot` (and `OpeningRangeWithOpts(..., OpeningRangeOptions{AllowSingleBarFallback})`): tracks the opening `rangeMinutes` ORB (`Open/High/Low/Close/Mid/Width/Volume/BarCount`) of each anchor-aligned day. While forming, `Forming=true, Ready=false`; `Ready=true` ONLY when `TimeStop >= RangeEndMS` and `BarCount >= 1` (`JustCompleted=true` on the completion bar, `InPostRange=true` on subsequent bars); safely returns `Applicable=false, Ready=false` when `TFMSecs >= 1d`, `TFMSecs > rangeMinutes*60000`, or the grid is misaligned with the anchor.
  - `SessionRange(startMinUTC, endMinUTC int) SessionRangeSnapshot`: tracks recurring UTC session windows (e.g. `0, 480` Asian Range `00:00–08:00 UTC`, or cross-midnight `1320, 120`); `InSession=true, Completed=false` during the window, then locks `High/Low/Open/Close/Mid/Width/Volume` with `Completed=true` until the next cycle's window starts.
  - `PrevDayOHLC(anchorMinutesUTC int) (PrevOHLC, bool)`: returns the previous **fully completed** anchor-aligned trading day's `Open/High/Low/Close/Pivot/Range/Volume` (PDO/PDH/PDL/PDC — Fairy Four Prices), constant throughout the active day with zero lookahead; partial initial day fragments on non-zero anchors are automatically discarded.
  - `PrevNDaysStats(anchorMinutesUTC, nDays int) (PrevNDaysSummary, bool)`: rolling statistics (`HH, HC, LC, LL, DualThrustRange, AvgRange(ADR), TotalVolume, AvgVolume, History`) over the past `nDays` completed trading days.
  - `DualThrustBands(anchorMinutesUTC, nDays int, k1, k2 float64) (upper, lower, dayOpen float64, ok bool)`: computes `Range = max(HH-LC, HC-LL)` over the past `nDays` completed trading days and outputs today's `dayOpen`, `dayOpen + k1*Range`, `dayOpen - k2*Range` (`<1d` timeframes only).
  - `RBreakerLevels(anchorMinutesUTC int) (RBreakerSnap, bool)` (and parameterized `RBreakerLevelsBy(anchor, f1, f2, f3)`): outputs the six standard R-Breaker levels `BBreak > SSetup > SEnter > Pivot > BEnter > BSetup > SBreak` from yesterday's completed `(H, L, C)` plus today's running `TodayOpen/TodayHigh/TodayLow` (`<1d` timeframes only). Standalone `NewOpeningRangeTracker` / `NewSessionRangeTracker` / `NewPrevPeriodTracker` are also exported.

### 3.4 DataEvent / DataFields / DataHub / DataSub
```go
type DataRole uint8 // DataRoleMain=1, DataRoleInfo=2, DataRoleCustom=3
type DataEvent struct { *DataFields; Role DataRole; Symbol *orm.ExSymbol }
func (e DataEvent) IsMain() bool   // main bar
func (e DataEvent) IsKline() bool  // main or auxiliary bar
type DataHandlers struct { Main, Info, Custom FnOnData }
func RouteData(h DataHandlers) FnOnData // at most one handler is called per event
```

`DataFields` (embedded in DataEvent):
- Fields: `DoneMS, TimeMS int64`, `Source string`, `Sid int32`, `Symbol string`, `TimeFrame string`, `Closed, IsWarmUp bool`.
- Methods: `BarEnv() *ta.BarEnv` (complete synchronized `*ta.BarEnv` for K-line/OHLCV streams; `nil` before the first bar or for non-OHLCV streams), `Series(name) *ta.Series` (numeric history series; on K-line streams `Series("close")` is the exact `BarEnv().Close` pointer and `Series("close").Env.High` is always non-nil; nil if absent), `SetMaxCache(limit int)`/`MaxCache() int` (default cache limit `max(1500, NumTaCache, WarmupNum+100)`, so long-period indicators like `SMA(1000)` never get truncated to NaN), `Float64(name)` (latest value; missing/unconvertible gives 0, use `RawValue`/`Has` to distinguish missing), `Int64(name)`, `String(name)`, `Raw(name) any`, `RawValue(name) (any, bool)` (distinguishes missing from explicit nil), `Has(name) bool`.
- Float fields all become Series by default; when `DataSub.SeriesFields` is non-empty only the listed fields (plus standard OHLCV fields on K-line streams) become Series; custom extension columns and raw/NULL semantics remain readable.

`DataSub` (i.e. `orm.Subscription`):
- `Source string`: data source name; bars `"kline"` (`orm.SeriesSourceKline`); custom series use the registered name
- `ExSymbol *orm.ExSymbol`: nil = current symbol; for cross-symbol read-only reference bars prefer `s.RefKlineSub("BTC/USDT:USDT", "1d")` (or `"BTC"`) instead of manual `SymbolState` lookups
- `TimeFrame string`: timeframe
- `WarmupNum int`: warmup count for this subscription
- `Fields []string`: raw field projection (bar default `open high low close volume quote buy_volume trade_num`)
- `SeriesFields []string`: fields that need numeric history series
- `Frequency`/`Projection` (advanced, normally leave empty): `bar/event`, `default/all/selected`

- `s.RefKlineSub(symbol, tf, warmupNum...)` + `s.RefEnv(symbol, tf)`: preferred idiom for cross-symbol market regime filters (e.g. reading `BTC/USDT:USDT` `1d` while trading altcoins) and multi-timeframe alignment. Note that `banta` incremental indicators advance once per reference bar: since backtest warmup runs sequentially per symbol, call the `s.RefEnv(...)` indicators in the `Info` handler (or at the top of `OnData` before `if !d.IsMain()`, when `d.Closed`) so warmup bars advance the indicator cache, then read `.Get(0)` in `Main`.
- `strat.NewDataSub(info *orm.SeriesInfo) *DataSub`: derive a subscription from custom series info (`Source` = series name).
- `s.DataHub.Get(tf, source, sid)` gets a view; `DataHub.SetMaxCache(limit)` adjusts retained history; `DataHub.AllReady()` checks whether all subscriptions have advanced to the current time.
- Auxiliary-bar signals should be based on `Closed==true` (live WebSocket data may be unclosed).

### 3.5 EnterReq (open request)
- `Tag string`: **required**, entry signal name
- `Short bool`: true to short, default long
- `OrderType int`: `core.OrderType*`; automatically a limit order when `Limit>0` and unspecified
- `Limit float64`: limit entry price (`Limit>0` alone submits a limit order; combining `Stop>0` and `Limit>0` submits a `Stop-Limit` entry that triggers at `Stop` and rests as a limit order at `Limit`)
- `Stop float64`: trigger price: for longs triggers once price rises to it (shorts the opposite); when `Limit==0` and already crossed it is treated as a limit order (`Limit=Stop`); in backtest under `bt_legacy_intrabar`, an already-crossed pure breakout stop enters at the current price
- `OCOGroup string`: One-Cancels-the-Other pending entry group identifier; when any pending entry in the same `(Account, Symbol, Strategy, OCOGroup)` fills, all other still-unfilled orders in the group are automatically canceled (same-bar straddles trigger the earlier leg along the intrabar OHLC path first)
- `CostRate float64`: order multiplier, default 1 (multiplied onto base amount)
- `RiskRate float64`: per-trade account equity risk ratio `[0, 1]` (e.g. `0.01` risks at most 1% of current `AccountEquity()` if stop-loss triggers); automatically computes `LegalCost` from stop-loss distance when `Amount`/`LegalCost` are unset and a valid stop-loss is resolved
- `LegalCost float64`: fiat amount; if specified CostRate/RiskRate are ignored
- `Amount float64`: asset quantity; if specified, not computed from amount
- `Leverage float64`: leverage; non-zero overrides account leverage
- `StopLossVal float64`: stop-loss distance (price difference), converted to a stop price; takes precedence over StopLoss
- `StopLoss float64`: stop-loss trigger price; active when >0 (pre-posted on the exchange in live when `ExgStopLoss=true`, or locally price-monitored when `ExgStopLoss=false`)
- `StopLossLimit float64`: stop-loss limit price; when only StopLossLimit is set and StopLoss=0, StopLoss takes this value
- `StopLossRate float64`: stop-loss exit ratio (0,1], 0=all
- `StopLossTag string`: stop-loss reason
- `ActivationPrice`, `CallbackPct float64`: trailing stop: activation price, drawdown percentage (`[0.05, 50]`; in live trading, if `CallbackPct` exceeds the venue's native callback cap, it automatically falls back to local price-monitored trailing stop)
- `TakeProfitVal/TakeProfit/TakeProfitLimit/TakeProfitRate/TakeProfitTag`: same as stop-loss (distance/price/limit/ratio/reason)
- `TakeProfits []TakeProfitTarget`: multi-stage take-profit ladder (mutually exclusive with single `TakeProfit*`); each `TakeProfitTarget{Price, Val, Rate, Limit, ExitRate float64; MoveSLToBreakeven bool; Tag string}` resolves `Val` (distance) / `Rate` (ratio, e.g. `0.02` = +2%) from the actual entry fill price, sorts rungs from nearest to farthest in the profit direction, and partial-exits `ExitRate` of the initial position (total `<= 1`); when `MoveSLToBreakeven=true`, tightens the remaining position's `StopLoss` to at least the entry fill price
- `StopBars int`: cancel a limit order not filled after this many bars (0 uses `StopEnterBars`)
- `ClientID string`: exchange ClientOrderID suffix
- `Infos map[string]string`: extra info (written to the order Info)
- `Log bool`: log a warning on failure
- `CommandID string`: framework idempotency ID (leave empty)

`OpenOrder` validation:
1. Error if `Tag` is empty, `CanOpen` is false, any numeric is NaN, `RiskRate` is outside [0,1], `CallbackPct` is outside [0.05,50], or `Limit>0` is set with explicit `OrderTypeMarket`.
2. Entry price: current price (`Env.Close` during warmup); for limit orders take the better price by direction; for `Stop>0 && Limit>0` compute the `Stop-Limit` reference entry price.
3. Amount (when neither Amount nor LegalCost is specified): if `RiskRate>0` and a valid stop-loss is resolved (`|enterPrice-slPrice|>0`), `LegalCost=(s.AccountEquity()*RiskRate)/(|enterPrice-slPrice|/enterPrice)`, capped by max leverage (`AccountEquity()*leverage`) and `max_stake_amt`; otherwise `LegalCost=base amount×StakeRate×CostRate`; quantity exceeding the last-5-bar average volume×`open_vol_rate` is truncated; below `MinStakeAmount`(5.5): `ignore` errors; `keepBig` raises to 5.5×1.1 only when cost is >40% of 5.5, otherwise errors; `keepAll` always raises to 5.5×1.1.
4. Stop-loss priority: `StopLossVal` > `StopLoss` > `LongSLPrice/ShortSLPrice` > `TradeStrat.StopLoss` (entry price×(1∓ratio)); wrong direction (long stop ≥ entry price, etc.) → error; `ExgStopLoss=false` skips remote pre-posting while keeping local stop-loss monitoring active.
5. Take-profit: when `TakeProfits` is set, validates each rung and total `ExitRate <= 1`; otherwise resolves `TakeProfitVal` > `TakeProfit` > `LongTPPrice/ShortTPPrice` (`TakeProfitLimit` follows the same rule as StopLossLimit), wrong direction → error.
6. Returns nil on enqueue (`s.Entrys`, `OrderNum++`); with `Log=true` errors are written to the log.

### 3.6 ExitReq (close request)
- `Tag string`: **required when there are orders**
- `Dirt int`: `core.OdDirt*`; zero value is Both
- `EnterTag string`: only close orders whose entry signal equals this value
- `OrderType int`, `Limit float64`: limit exit (when `Limit>0` it must be a limit type, auto-limit if unset); `limit_maker` on the favorable side (closing long with Limit<price / closing short with Limit>price) is converted into that order's limit stop-loss
- `ExitRate float64`: exit ratio (0,1], 0 is treated as 1; >1 errors
- `Amount float64`: exit quantity; when specified ExitRate is ignored
- `OrderID int64`: close only the specified order
- `UnFillOnly`/`FilledOnly bool`: close only the unentered part / only entered orders
- `Force bool`: forced exit
- `StratName`, `Log`, `CommandID`: same as EnterReq

- Returns nil if there are no orders in the direction (Tag not checked).
- `Dirt=Both` with both long and short present and `Limit` specified → error (Dirt must be explicit); if only one side has orders that side is chosen automatically.
- When `CloseLong/CloseShort=false`, closing in that direction errors.

### 3.7 Order Objects ormo
`*ormo.InOutOrder` (one complete "entry+exit" order): `*IOrder` + `Enter, Exit *ExOrder` (`Exit` may be nil) + `Info map[string]interface{}`.

`IOrder`: `ID, TaskID int64`; `Symbol string`; `Sid int64`; `Timeframe string`; `Short bool`; `Status int64`; `EnterTag, ExitTag string`; `Stop float64`; `InitPrice` (signal price); `QuoteCost` (quote amount); `Leverage`; `EnterAt, ExitAt int64`(ms); `Strategy string`; `StgVer int64`; `MaxPftRate, MaxDrawDown, ProfitRate, Profit float64`; `Info string`.
`ExOrder` (one real/simulated exchange order): `ID, TaskID, InoutID int64`; `Symbol`; `Enter bool`; `OrderType, OrderID, Side string`; `CreateAt, UpdateAt int64`; `Price, Average, Amount, Filled float64`; `Status int64`; `Fee, FeeQuote float64`; `FeeType string`.
Constants: `InOutStatusInit=0 PartEnter=1 FullEnter=2 PartExit=3 FullExit=4 Delete=5` (prefix `ormo.InOutStatus`); ExOrder status `OdStatusInit=0 PartOK=1 Closed=2`.

- `EnterCost()`: Enter.Filled × average price
- `HoldCost()`/`HoldAmount()`: held cost/quantity of the open position
- `CalcProfit(price)`: compute PnL at a price (before fees)
- `UpdateProfits(price)`: refresh Profit/ProfitRate/MaxPftRate/MaxDrawDown
- `CanClose() bool`: whether it can be closed
- `SetStopLoss(*ExitTrigger) *errs.Error`/`SetTakeProfit(...)`: set/modify stop-loss, take-profit
- `SetExitTrigger(key string, args, price)`: generic trigger (key `ormo.OdInfoStopLoss/OdInfoTakeProfit`)
- `GetStopLoss()/GetTakeProfit() *TriggerState`: current trigger state (Hit, OrderId, etc.)
- `SetTrailingStop(callbackPct, activationPrice float64) *errs.Error`: trailing stop
- `SetEnterLimit(price) *errs.Error`: modify the limit price of an unfilled entry
- `SetInfo(key,val)`, `GetInfoFloat64/Int64/String(key)`: order extension info
- `RealEnterMS()/RealExitMS() int64`: actual entry/exit time (ms)
- `Key()/KeyAlign() string`: unique key
- `Clone() *InOutOrder`

`ExitTrigger{Price, Limit, Rate float64; Tag string}`: `Price` trigger price, `Limit` limit price after triggering (0=market), `Rate` exit ratio (0,1] (0=all).

Order events (chgType of `OnOrderChange`, `strat.OdChg*`): `OdChgNew`(0) new order, `OdChgEnter`(1) entry request, `OdChgEnterFill`(2) entry completed, `OdChgExit`(3) exit request, `OdChgExitFill`(4) exit completed, `OdChgOrderChanged`(5) real order confirmed/modified/canceled (excluding synthetic fills).

### 3.8 Common Patterns
- Open only once per bar: `len(s.Entrys)==0 && s.GetOrderNum(0)==0`.
- Trailing stop for shorts: `s.SetAllStopLoss(core.OdDirtShort, &ormo.ExitTrigger{Price: s.Env.Close.Get(0)+1.5*atr})`.

### 3.9 Cross-Symbol Target-Weight Rebalancing & Inverse-Volatility Sizing (`OnBatchJobs`)
Designed for multi-asset risk-parity trend CTA, cross-sectional selection + per-coin stop-loss hybrids, and leader-laggard (e.g. BTC breakout driving lagging high-beta altcoins) strategies running inside `TradeStrat{BatchInOut: true, OnBatchJobs: ...}` (keep `EachMaxLong: 0, EachMaxShort: 0` when enabling same-direction scale-in orders):

- **`strat.RebalanceBatchWeights(jobs []*strat.StratJob, targetWeights map[string]float64, opts strat.BatchRebalanceOpts) strat.BatchRebalanceResult`**:
  - `targetWeights`: `symbol -> signed target notional fraction of AccountEquity()` (e.g. `0.4` = 40% long notional, `-0.3` = 30% short notional, `0` or omitted = flat).
  - `BatchRebalanceOpts`:
    - `Tag string`: entry/exit signal tag (default `"rebalance"`).
    - `DriftThreshold float64`: no-trade weight drift buffer (default `0.02` = 2% of equity; skips micro-adjustments when current and target weights share the **same non-zero sign** and `|target - cur| < DriftThreshold`; opening from flat is never blocked by the drift buffer; pass `< 0` to disable).
    - `MaxGrossExposure float64`: gross exposure cap (`> 0` proportionally scales all target weights down whenever `Σ|w| > MaxGrossExposure`).
    - `StopLossRate` / `TakeProfitRate` / `CallbackPct` / `Leverage float64`: automatically attaches per-symbol protective stop-loss distance (`enterPrice * StopLossRate` via `StopLossVal`), take-profit distance, trailing callback percentage `[0.1, 10]`, and leverage to newly opened / scaled-in orders.
  - **Two-pass execution order**:
    1. Pass 1 scans all `jobs`: closes flat targets (`0`), closes opposite-direction positions on reversals before queueing the full new-side target, and partially closes overweight same-direction positions (`|target| < |cur| - DriftThreshold`) with `ExitRate = (|cur| - |target|) / |cur|` so margin is released first.
    2. Pass 2 sorts new entries and same-direction top-ups (`|target| > |cur| + DriftThreshold`) by `LegalCost = AccountEquity() * deltaWeight` **descending** (tie-broken deterministically by neutral `strat.BarPriority(barMS, symbol, seed)` then symbol) before calling `OpenOrder`, silently skipping sub-`core.MinStakeAmount` (`5.5`) dust orders (`SkippedDustCount`).
  - Returns `BatchRebalanceResult` (`Equity`, `RawGrossExposure`, `EffectiveGrossExposure`, `ScaleFactor`, `ClosedCount`, `ReducedCount`, `ReversedCount`, `OpenedCount`, `ScaledInCount`, `SkippedDriftCount`, `SkippedDustCount`, `ErrorCount`, and chronological `Actions []BatchRebalanceAction`).
- **`strat.InverseVolWeights(jobs []*strat.StratJob, signals map[string]float64, atrPeriod int, targetGross float64) map[string]float64`**:
  - Given directional/strength `signals` (e.g. `+1`/`-1`/`0` or continuous scores), `atrPeriod` (`<=0` defaults to `14`), and `targetGross` (e.g. `0.8`), unconditionally evaluates `ta.NATR(e.High, e.Low, e.Close, atrPeriod)` across every job in `jobs` (preserving incremental RMA cache alignment even when `signal == 0`), weights active legs by `signal / NATR`, and normalizes so `Σ|w| == targetGross`.
- **`strat.BarPriority(barMS int64, symbol string, seed uint64) uint64`**: neutral, reproducible 64-bit per-bar symbol hash priority (lower runs first).

### 3.10 Coarse-Timeframe K-Line Grid & Two-Sided Limit Strategies (`RefineTF` + Causal `OnOrderChange` Replenishment)
Designed for K-line grid, range-reversion, and multi-rung limit ladder strategies on coarse timeframes (`1h`/`4h`/`1d`) that maintain simultaneous buy/sell limit orders and replenish counter-orders immediately upon intrabar fills:
- **Sub-bar matching declaration**: Set `RefineTF: "5m"` (or `"1m"` / integer divisor such as `12`; YAML `run_policy[].refine_tf` overrides when set) on `TradeStrat`, keep `EachMaxLong: 0, EachMaxShort: 0` (or set to the grid rung cap), and ensure `max_open_orders` / `max_simul_open` accommodate all resting rungs.
- **Chronological sub-bar progression & `OnOrderChange` replenishment**: Resting `Limit`, `Stop-Limit`, and TP/SL orders submitted in `OnData(Main)` advance sub-bar by sub-bar (`5m`/`1m`) in backtests; multiple rungs triggered within the same sub-bar fill in chronological intrabar timestamp order and fire `OnOrderChange(s, od, strat.OdChgEnterFill)` / `OdChgExitFill`.
- **Synchronized order snapshots & strict causality**: `s.GetOrders`, `s.GetOrderNum`, and `s.Position` are refreshed before `OnOrderChange` runs; counter-orders queued via `s.OpenOrder` or `s.CloseOrders` (e.g. `OrderID: od.ID, Limit: tpPrice`) inside `OnOrderChange` are drained immediately and matched strictly after `fillTime + bt_net_cost` on the remaining sub-bar path and subsequent sub-bars (zero lookahead into pre-fill highs/lows).
- **Bilateral symmetric approximate funding**: On linear perpetual contracts without a loaded `funding_rate` series, `bt_funding_apr` settles symmetrically (under a positive baseline rate, longs pay `+cost` and shorts receive `-cost`, yielding zero net funding drift on equal-notional 1:1 long+short hedges).

```go
// Set RefineTF: "5m", EachMaxLong: 0, EachMaxShort: 0 on TradeStrat:
// 1) OnData(Main) posts multi-level buy/sell limit rungs on 1h closes (or attaches TakeProfit: buyPx + step);
// 2) OnOrderChange immediately submits a counter limit exit / replenishment order when a 5m sub-bar fills a rung:
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

## 4. Order, Stop-Loss/Take-Profit and Position Details
### 4.1 Opening and Closing
- Backtest fill time = order time + `bt_net_cost` seconds, price taken from the subsequent price path (§6).
- Default is market (`OrderTypeEmpty` is the same). `Limit` orders can be combined with `StopBars`; `OrderTypeLimitMaker` requires the resting side (long limit ≤ market, short ≥ market), otherwise errors (Binance futures has no limit_maker, banexg converts to limit+GTX).
- Leverage: `EnterReq.Leverage` first, then account `leverage`.
- Position size: fixed base sizing is `stake_amount`/`stake_pct` (account) × `accounts.<acc>.stake_rate` × `run_policy.stake_rate` × `TradeStrat.StakeRate` × `CostRate` (capped by `max_stake_amt`, multiplied by `strat_perf` down-weighting); alternatively set `EnterReq.RiskRate` (e.g. `0.01`) with a stop-loss distance for automatic fixed-fractional sizing by `AccountEquity()`.
- Closing: default full close at market; `ExitRate=0.5` closes half (proportionally per order); filter via `EnterTag`/`OrderID`; `Limit` for limit orders. After a partial close the remaining order is still in `GetOrders`.

### 4.2 Stop-Loss and Take-Profit
- `StopLossVal/TakeProfitVal` (distance) are most commonly used, `StopLoss/TakeProfit` are absolute prices; priority see §3.5. `*Rate` is the exit ratio after triggering (0=all), `*Limit` places a limit order after triggering (otherwise market). Multi-stage take-profit ladders use `TakeProfits []TakeProfitTarget` (with optional `MoveSLToBreakeven: true` to move the remaining stop-loss to entry price).
- Trailing stop: `ActivationPrice` (activates once reached) + `CallbackPct` (drawdown % from the peak, [0.05,50]). Modify at runtime: `s.SetAllStopLoss(dirt, &ormo.ExitTrigger{Price: …})` or `od.SetStopLoss(...)`.

**Exchange-side conditional orders**:
- Backtest, `dry_run`: triggered by local simulation, no exchange access.
- Live (`prod`): **after the order is fully entered** (`InOutStatusFullEnter`; not placed during partial fills), stop-loss and take-profit each call `CreateOrder` separately to place conditional orders: stop-loss `stop_loss` (with Limit it is `stop_loss_limit`), take-profit `take_profit_market` (with Limit it is `take_profit_limit`), quantity = current position×Rate (when 0<Rate<1); ClosePosition is not used (to avoid multiple orders overwriting each other), hedge mode relies on positionSide. Trailing stop (`CallbackPct`) places `trailing_stop_market` (callbackRate, optional activationPrice); if `CallbackPct` exceeds the exchange's native cap or is rejected, it automatically falls back to `LiveOrderMgr` local price-monitored trailing stop without failing the order; a stop-loss with framework tag `trailing_stop` does not place a separate stop order.
- banexg adaptation: Binance (spot/margin STOP_LOSS(_LIMIT)/TAKE_PROFIT(_LIMIT); futures STOP_MARKET/STOP/TAKE_PROFIT_MARKET/TAKE_PROFIT/TRAILING_STOP_MARKET), OKX (algo), Bybit (conditional) are all implemented; unsupported categories/types error on order placement.
- Failure: if the error is "would trigger immediately" → close immediately at market; other failures only log ERROR, a failed update does not cancel the old order, **no automatic retry**, missing trigger orders are only re-placed on sync/restart. After `SetAll*`/`od.SetStopLoss` modifications, live re-edits/re-places the conditional orders.
- `ExgStopLoss/ExgTakeProfit=false` skips remote conditional order pre-posting in live trading while preserving local price-monitored triggers in both backtest and live (see §3.3).
- Drawdown exit (`DrawDownExit=true`): the framework tracks the max floating-profit ratio maxChg; r comes from `GetDrawDownExitRate` (`>0` used, `0` not set, `<0`/unset uses the default table): maxChg >10% → r=0.15, >4% → 0.17, >2.5% → 0.25, >1.5% → 0.37, >0.7% → 0.5, otherwise disabled. Drawdown price = entry price×(1+maxChg×(1−r)×direction); if the current close has crossed it, close at market with tag `draw_down`, otherwise **override the order's stop-loss trigger with this price** (this replaces the stop-loss you set, and is synced to the exchange in live). Checked only when `OnCheckExit` returns nil.

### 4.3 Long/Short Restrictions
- `MaxOpenLong/Short` (job level, initialized from `EachMaxLong/Short`) decides whether new orders can be opened; spot cannot short. Reversal: call `CloseOrders` and `OpenOrder` in the same callback, processed together at the end of the event.

## 5. Custom Time-Series Data

### 5.1 Writing (data source/importer/application layer; do not write to the DB in strategy callbacks)
```go
info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{{Name: "rate", Type: "float"}})
store := orm.NewSeriesStore(orm.NewSeriesRepo(storage)) // storage: *orm.Storage of the current Runtime
_ = store.Ensure(ctx, info)
_ = store.Write(ctx, info, exs, &orm.DataRecord{TimeMS: startMS, EndMS: endMS, Values: map[string]any{"rate": rate}})
rows, _ := store.Read(ctx, info, exs, startMS, endMS, 0) // []*orm.DataSeries
v, err := rows[0].FloatValue("rate")
```
- `SeriesField.Type`: `float int string bool json`. `SeriesStore`: `Ensure/Write/WriteBatch/WriteSeries/Read/Delete/Missing/FillMissing(ctx,info,exs,start,end,fetch)/Coverage`.
- Bar extension columns (per `(sid,time)`): `orm.NewKLineSeriesInfo` + `orm.NewKLineSeriesStoreWithStorage` (only updates existing bar rows).
- QuestDB WAL tables are asynchronously visible after write: reading immediately after writing may not show the new rows; wait for visibility.

### 5.2 Data Source Registration (automatic backfill and push in backtest/live)
- `data.RegisterFuncDataSource(info, fetchHistory, subscribeLive)`: `FetchHistoryFunc func(ctx, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)`; `SubscribeLiveFunc func(ctx, subs []*strat.DataSub, sink data.DataSink) error` (may be nil).
- `data.RegisterDataSource(src)`/`RegisterDataSourceFactory(name, f)`: `DataSource{Info(); FetchHistory(); SubscribeLive()}`.
- Sources without history fetching are unusable in backtest.

### 5.3 Strategy Consumption
```go
// info same as §5.1
OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
    sub := strat.NewDataSub(info)     // nil ExSymbol = current symbol
    sub.WarmupNum = 20
    sub.Fields = []string{"rate"}
    sub.SeriesFields = []string{"rate"}
    return []*strat.DataSub{sub}
},
OnData: strat.RouteData(strat.DataHandlers{
    Custom: func(s *strat.StratJob, d strat.DataEvent) { /* d.Float64("rate") / d.Series("rate") */ },
    Main:   func(s *strat.StratJob, d strat.DataEvent) { /* read state from s.More */ },
}),
```
- Custom events go only to `Custom`; `s.Env` does not correspond to them.
- Resample aggregation: built-in `min max last first sum avg mid`, configured with `ExSymbol.SetAggRules(map[field]rule)`; for special semantics use `orm.RegisterAggRule(name, fn)`.

## 6. Backtest vs Live Differences
- Fill time: backtest = order time + `bt_net_cost` seconds (default 15); live = exchange report (paper trading matches locally).
- Price path: backtest uses `refine_tf` or 1m data; `bt_legacy_intrabar` falls back to the legacy intra-bar path; live uses real-time quotes.
- Historical gaps: `bt_no_kline_download` forbids implicit download; `historical_coverage` configures coverage checks.
- `bt_strict: true` fixes key execution order (reproducible, may be slower), backtest only.
- Live requests not processed before the next bar arrives → discarded with WARN; entries after the bar has expired are skipped (`bar expired`).
- Stop-loss/take-profit: backtest/`dry_run` simulated locally; `prod` uses exchange conditional orders (§4.2).
- Disconnection: `close_on_stuck` (minutes, default 20): close all positions if no bar arrives within the timeout.
- Account: backtest `wallet_amounts`; live uses exchange balances and positions.
- Do not rely on "backtest fill price = current close".
- After a live restart, unfinished orders are re-synced into `LongOrders/ShortOrders`; `OnStartUp` runs again and `More` is rebuilt; for state across restarts, write to order `Info` or external storage.

## 7. Key Rules and Common Pitfalls
1. **OnData and OnBar/OnInfoBar cannot be set together** (build panic); new strategies use only `OnData`+`RouteData`; do not simply rename `OnBar` to `OnData` (Main and Info must be distinguished).
2. **Compute indicators unconditionally on every bar** and then use `if` to judge (putting them inside a branch misaligns the cached series).
3. **Use `Series.To` for derived series**; do not repeatedly `NewSeries` in callbacks, otherwise the series count grows and triggers the fatal error `series too many … try replace NewSeries with Series.To` and stops the bot.
4. Float-constant cache collision, and use `== 1` for `Cross` (§2.2).
6. **Before comparing Close with Highest/Lowest, apply `Back(1)` first**, otherwise the current close can never break a highest price that includes itself: `e.Close.Get(0) > ta.Highest(e.High, n).Back(1).Get(0)`.
7. `s.Env` is the main timeframe; read auxiliary-timeframe or cross-symbol reference bars via `s.RefKlineSub(symbol, tf)` + `s.RefEnv(symbol, tf)` (or `data.BarEnv()`/`data.Series(...)`), and custom series via `data.Series(...)`/`data.Float64(...)`.
9. `Tag` is required in `OpenOrder`; `CloseOrders` requires `Tag` when there are orders. The zero value of `ExitReq{}` `Dirt` is Both; to close only longs/shorts you must explicitly set `core.OdDirtLong/OdDirtShort`.
10. Hyperparameters are read-only shared: do not write them into `More`; use `pair_params` for per-symbol differences. `More` only stores per-symbol data that changes.
11. `GetOrders/GetOrderNum` are snapshot-based: an order just opened with `OpenOrder` in the same callback will not appear; do not reuse slices across events.
13. Do not call exchange APIs/blocking IO in callbacks (use §5 custom series for external data); do not iterate symbols/other jobs via package-level globals (use `OnBatchJobs/OnBatchInfos` for cross-symbol aggregation).
14. Multi-input indicator caches attach to only one input, the combination must stay fixed; do not call indicators marked (full) multiple times in a loop (§2.3).
16. Check NaN from `Series.Get` (`math.IsNaN(atr)`), especially when `WarmupNum` is not large enough.
17. Jobs of the same symbol with different timeframes are independent Envs; use `DataSub` for cross-timeframe data.
18. `DrawDownExit` overrides the order's stop-loss trigger (§4.2); `ExgStopLoss=false` hides remote pre-posting while keeping local stop-loss monitoring active (§3.3); `MAMA/VPCI/DX/Fisher` differ from standard definitions; do not use legacy APIs in new code.
19. **A `TradeStrat` is shared by every symbol of the strategy** (the factory runs once per policy unless `pair_params` is configured): closure variables of the factory body and package-level variables are shared by all symbols. Keep state that changes per symbol (previous signal, peaks, trailing stop level, counters) in a struct assigned to `s.More` in `OnStartUp` and read it with `s.More.(*T)` in the callbacks; never use closure variables or a map keyed by symbol for it.
20. Common compile errors: the symbol name string is `s.Symbol.Symbol` (`s.Symbol` is `*orm.ExSymbol`, not a string or map key); `pol.Params` is `map[string]float64`, hyperparameters are numeric only (0/1 switches), read with `pol.Def/DefInt`, there is no `DefBool/DefStr/core.PInt`; the direction argument `dirt` is a `float64`; bar times are `e.TimeStart/TimeStop` and the period is `e.TFMSecs` (no `e.Time/TFSecs`); order types live in `github.com/banbox/banbot/orm/ormo` (`*ormo.InOutOrder`, `*ormo.ExitTrigger`); `EnterReq` has no `Qty/Dirt` field (quantity `Amount`, amount `LegalCost`/`CostRate`, risk-fractional sizing `RiskRate`, short `Short: true`); read account equity/available balance/peak drawdown via `s.AccountEquity()` / `s.AvailableBalance()` / `s.AccountDrawdown()` (no `GetEquity`), and the base stake is `s.Strat.GetStakeAmount(s)`.

## 8. Hyperparameters and Optimization
Declare hyperparameters with `pol.Def/DefInt` (distributions see §1.5; `DefInt` rounds automatically). Enforce cross-parameter constraints (such as `fast < slow`, `maFast < maMid && maMid < maSlow`, `callbackMin <= callbackMax`) inside the factory with `pol.Require(cond, "require ...")` / `pol.Requiref(...)` so invalid sampled combinations exit cleanly with `[BadConfig]` rather than consuming crash retries. `banbot optimize --config x.yml` (there is also `bt-opt` rolling backtest); `--sampler tpe|bayes|random|cmaes|ipop-cmaes|bipop-cmaes` (default bayes), `--opt-rounds` (default 30).

## 9. Complete Examples
### Example A: Single-timeframe EMA Cross + ATR Stop-Loss/Take-Profit

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

// MaAtr: EMA golden cross goes long, death cross goes short, ATR-multiple stop-loss/take-profit.
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
				cross := ta.EMA(e.Close, fast).Cross(ta.EMA(e.Close, slow)) // ==1 only for the golden cross on this bar
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

Example A, `MyMax` in §2.1, and the §5 code have passed `go vet`; the rest are interface fragments. Auxiliary timeframes: in the Info handler `return` if `!data.Closed||data.IsWarmUp`, and use `data.Series("close")` (§3.4/§5.3/§10). Timeout exit: in `OnCheckExit` check `s.Env.BarCount(od.EnterAt)>=N` and return `&strat.ExitReq{Tag:"timeout"}`.

### Example B: Intraday Session Breakout Snippets (ORB / Asian Range London Breakout / Dual Thrust / R-Breaker)

```go
// 1) US Cash ORB (30m opening range from 13:30 UTC, EOD flatten at 20:00 UTC)
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

// 2) Asian Range (00:00–08:00 UTC) London Killzone (08:00–10:00 UTC) Breakout
asia := s.SessionRange(btime.AsiaOpenMinutes, btime.AsiaCloseMinutes)
if asia.Completed && btime.InUTCWindow(e.TimeStart, btime.LondonCashOpenMinutes, 10*60) && s.GetOrderNum(0) == 0 {
	if e.Close.Get(0) > asia.High {
		s.OpenOrder(&strat.EnterReq{Tag: "ldn_brk_long", StopLoss: asia.Low})
	}
}

// 3) Dual Thrust (00:00 UTC rollover, 4 completed days for Range)
if upper, lower, _, ok := s.DualThrustBands(btime.UTC0Minutes, 4, 0.5, 0.5); ok {
	if c := e.Close.Get(0); c > upper && s.GetOrderNum(1) == 0 {
		s.CloseOrders(&strat.ExitReq{Tag: "dt_rev_short", Dirt: core.OdDirtShort})
		s.OpenOrder(&strat.EnterReq{Tag: "dt_long", StopLoss: lower})
	} else if c < lower && s.GetOrderNum(-1) == 0 {
		s.CloseOrders(&strat.ExitReq{Tag: "dt_rev_long", Dirt: core.OdDirtLong})
		s.OpenOrder(&strat.EnterReq{Tag: "dt_short", Short: true, StopLoss: upper})
	}
}

// 4) R-Breaker Six-Level Trend Breakout + Intraday Setup Reversal
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

// 5) Timeframe-safe Monthly DCA + Halloween Seasonal Filter (track prevMS unconditionally in s.More)
prevMS := st.prevMS
st.prevMS = e.TimeStart
if !s.IsWarmUp && btime.IsFirstBarOfPeriodUTC(prevMS, e.TimeStart, "month") && btime.InMonthRangeUTC(e.TimeStart, 11, 4) {
	s.OpenOrder(&strat.EnterReq{Tag: "monthly_dca_halloween"})
}
```

### Example C: Multi-Symbol Risk-Parity CTA / Lead-Lag Batch Rebalancing (`BatchInOut` + `OnBatchJobs`)

```go
// Set BatchInOut: true on TradeStrat and keep EachMaxLong: 0, EachMaxShort: 0 to allow same-side scale-ins;
// update per-symbol indicators unconditionally in OnData(Main) into s.More.(*symState), then rebalance in OnBatchJobs:
OnBatchJobs: func(jobs []*strat.StratJob) {
	signals := make(map[string]float64, len(jobs))
	for _, j := range jobs {
		st := j.More.(*symState)
		if st.ready {
			signals[j.Symbol.Symbol] = st.signal // e.g. +1/-1/0 trend breakout, or +1 on lagging beta alts after BTC breakout
		}
	}
	weights := strat.InverseVolWeights(jobs, signals, 14, 1.0) // 1/NATR(14) normalized to 1.0x gross exposure
	strat.RebalanceBatchWeights(jobs, weights, strat.BatchRebalanceOpts{
		Tag:              "rp_cta",
		DriftThreshold:   0.03,
		MaxGrossExposure: 1.2,
		StopLossRate:     0.06, // attach 6% per-symbol protective stop-loss to entries
	})
},
```

## 10. Legacy Interface Migration (OnBar → OnData)
- Legacy `OnBar(s)`: change to `OnData: strat.RouteData(strat.DataHandlers{Main: func(s, _ strat.DataEvent){…}})`.
- Legacy `OnInfoBar(s, e, pair, tf)`: move into the `Info` handler, use `data.Series("close")` etc. instead of `e.Close`, and declare `{Source:"kline", TimeFrame: tf}` in `OnDataSubs`.
- Legacy `OnPairInfos`: change to `OnDataSubs` (nil `ExSymbol` = current symbol).
- Legacy `ta.Cross(a, b)`: change to `a.Cross(b)`.
