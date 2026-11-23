You can quickly get familiar with and use our [sample strategy project](https://github.com/banbox/banstrats). Of course, you may have your own trading strategy to test. This page helps you understand how to implement a custom strategy.

## Overview
To start a new strategy, there are only two steps.

First, implement a strategy function of `func Demo1(pol *config.RunPolicyConfig) *strat.TradeStrat`.

Second, you only need to register this strategy in the current go package's `init` function.

Let's see how to implement our custom logic in the strategy function.

::: tip
We strongly recommend that you use our online AI assistant to aid in development: [Link](https://www.banbot.site/en-US/backtest/new){target="_self"}.  
While the AI assistant is very convenient, it can still make mistakes. You must check and understand the strategy code output by the AI to ensure it aligns with your logic; otherwise, the subsequent backtesting and live trading will be meaningless.
:::

::: tip Statistical arbitrage and cross-sectional strategies
Time-series strategies can also reuse engine-independent primitives: `NewRollingSpread`/`NewBandTrader` in `github.com/banbox/banbot/factor/statistics` (pair-spread z-score with entry/exit/stop hysteresis). For the shortest path to cross-sectional and pairs strategies, see [Multi-factor strategies](./factor.md).
:::

## Strategy Naming
The recommended naming format is `folder:strategy_name`. You may have many different categories of trading strategies. For easy long-term maintenance, it is recommended to put similar strategies in the same folder. The depth is not limited.

For example:
```text
root
 |-org
 |  |-ma
 |  |  |-demo.go
 |  |  |-trend.go
 | grid
 |  |-inv.go
 |-main.go
```
Above there are two valid strategy groups `ma` and `grid`. You can also continue to create subfolders in `ma` to manage strategies. Note that all strategy groups need to be registered in `main.go` in the root directory. then you can register the strategy in any go file under `ma`:  
```go
func init() {
    // Register the strategy with banbot. You can reference this strategy in the configuration file using ma:demo
    // The `init` function is a special Go function that executes automatically when the package is imported
	strat.AddStratGroup("ma", map[string]strat.FuncMakeStrat{
		"demo": Demo,
	})
}
```
```go
import (
	"github.com/banbox/banbot/entry"
	_ "github.com/banbox/banstrats/org/ma"
	_ "github.com/banbox/banstrats/grid"
)
```

## RunPolicyConfig parameters

The parameter `pol` of the strategy function is an item in the `run_policy` list in the yaml configuration file.

You can get the parameters passed in from the configuration file through `atrLen := pol.Param("atrLen", 9)` so that different parameters can be used for different symbols.
You can also define a hyperparameter using `atrLen := pol.Def("atrLen", 9, core.PNorm(7, 20))`, which works exactly the same as `pol.Param` during backtesting, but when switching to hyperparameter tuning mode, the value of `atrLen` will be randomly generated using a normal distribution with a default value of the mean and upper and lower limits of `(7, 20)`.
You can also replace `core.PNorm` with `core.PNormF` to specify a different mean and multiplier.
Similarly, you can also use `core.PUniform` to specify a uniform linear distribution hyperparameter.

Here are several complete code examples:
```go
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    atrLen := pol.Param("atrLen", 9)
    atrLen1 := pol.Def("atrLen1", 9, core.PNorm(3, 20))
    atrLen2 := pol.Def("atrLen2", 9, core.PNormF(3, 20, 12, 1))
    atrLen3 := pol.Def("atrLen3", 9, core.PUniform(3, 20))
    return &strat.TradeStrat{
        WarmupNum: max(int(atrLen), int(atrLen1), int(atrLen2), int(atrLen3)),
    }
}
```
You can set the corresponding policy parameters in `yml`, which will overwrite the default values above:
```yaml
run_policy:
  - name: demo:demo
    params:
      atrLen: 9
      atrLen1: 10
      atrLen2: 11
      atrLen3: 12
    other1: value1
    other2: 123
    other3: 1.23
```
::: warning Tips
Each item in the yaml configuration `run_policy` corresponds to a call to the strategy function, generating a strategy with specific parameters.
The same strategy can appear in multiple `run_policy` items, that is, the same strategy function may be executed multiple times.
The `*strat.TradeStrat` returned by the strategy function will be applied to multiple varieties, corresponding to multiple `*strat.StratJob`.
Therefore, any variables related to a single symbol should not be saved in the strategy function, but should be saved through `*strat.StratJob.More`.
The `TradeStrat` and its closures are shared by all symbols and accounts of the strategy, so counters or flags kept in a closure are overwritten by different symbols.
The variables in the strategy function should remain unchanged. Otherwise, it will lead to unexpected states.
:::

### Parameter validation {#require}

Validate parameters in the constructor with `pol.Require(cond, msg)` or `pol.Requiref(cond, format, args...)`. When the condition fails, construction of that strategy fails and is reported as a configuration error (BadConfig) instead of a runtime crash; genuine runtime errors (such as a nil dereference) are not reclassified as configuration errors.
```go
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    fast := pol.DefInt("fast", 10, core.PNorm(5, 30))
    slow := pol.DefInt("slow", 30, core.PNorm(20, 100))
    pol.Require(fast < slow, "require fast < slow")
    return &strat.TradeStrat{WarmupNum: slow * 3}
}
```

## strat.TradeStrat Strategy Object

`TradeStrat` provides a wealth of property configurations and callback functions. Use `OnData` for new strategies; `OnBar` remains a supported K-line compatibility callback.

Common public fields of `TradeStrat` (other configuration fields omitted):
```go
type TradeStrat struct {
	Name          string  // The strategy name does not need to be set and will be automatically set to the name used during registration
	Version       int     
	WarmupNum     int     // candle preheating length
	OdBarMax      int     // Max bars a backtest simulates forward to find unfinished positions, default 500; run_policy.order_bar_max takes precedence
	MinTfScore    float64 // Minimum time cycle quality, default 0.75
    WsSubs        map[string]string    // websocket subscription: core.WsSubKLine, core.WsSubTrade, core.WsSubDepth
	DrawDownExit  bool    // Whether to enable retracement stop loss (i.e. trailing stop loss)
    HedgeOff      bool    // turn off future hedge mode
	BatchInOut    bool    // Whether to batch execute entry/exit after main OnData(RoleMain)/OnBar
	BatchInfo     bool    // Whether to batch process after auxiliary OnData(RoleInfo)/OnInfoBar
	StakeRate     float64 // Relative basic amount billing rate
    StopLoss      float64 // Default stop loss rate for all orders opened by this strategy (without leverage).
	StopEnterBars int     // If the limit order exceeds the given K line and still does not enter the market, it will be cancelled
	OrderOnRotation string // Old job after its symbol is rotated out: "open" keeps allowing new orders while it holds orders; whether positions are closed is set by pairmgr.pos_on_rotation
	EachMaxLong   int      // max number of long open orders for one pair
	EachMaxShort  int      // max number of short open orders for one pair
	RunTimeFrames []string // Allowed running periods; when omitted, use the current Runtime configuration snapshot default
	RefineTF      interface{} // Strategy default order-matching timeframe, e.g. "5m", 12, "3-6"; an explicit run_policy.refine_tf takes precedence
	Outputs       []string // The content of the text file output by the strategy, where each string is one line
	Policy        *config.RunPolicyConfig

	OnDataSubs          func(s *StratJob) []*DataSub
	OnData              func(s *StratJob, data DataEvent)
	OnWsData            func(s *StratJob, evt *orm.DataSeries)
	OnSymbols           func(items []string) []string
	OnPostApi           FnOnPostApi
	OnPairInfos         func(s *StratJob) []*PairSub                        // The strategy requires additional data on other types or periods
	OnStartUp           func(s *StratJob)                                   // Callback at startup. Called before the first execution
	OnBar               func(s *StratJob)                                   // Callback function for each K line
	OnInfoBar           func(s *StratJob, e *ta.BarEnv, pair, tf string)   // Other dependent bar data
    OnWsTrades          func(s *StratJob, pair string, trades []*banexg.Trade) // Websocket public trade data
    OnWsDepth           func(s *StratJob, dep *banexg.OrderBook)               // Websocket order book
    OnWsKline           func(s *StratJob, pair string, k *banexg.Kline)        // websocket real-time kline(may unfinish)
	OnBatchJobs         func(jobs []*StratJob)                             // All target jobs at the current time, used for bulk opening/closing of orders
	OnBatchInfos        func(tf string, jobs map[string]*JobEnv)            // All info marked jobs at the current time, used for batch processing (open/close requests here are not submitted automatically)
	OnCheckExit         func(s *StratJob, od *ormo.InOutOrder) *ExitReq     // Custom order exit logic
	OnOrderChange       func(s *StratJob, od *ormo.InOutOrder, chgType int) // Order update callback
	GetDrawDownExitRate CalcDDExitRate                                     // Calculate the ratio of tracking profit taking, drawdown, and exit
	PickTimeFrame       PickTimeFrameFunc                                  // Choose a suitable trading cycle for the specified currency
	OnShutDown          func(s *StratJob)                                  // Callback when each strategy task stops
	OnStratExit         func()                                             // Callback when strategy config exits, executed after all OnShutDown
}
```
:::tip tip
`MinTfScore` is a score calculated for each trading pair over a specified period of k-line data, based on factors such as price movement and the proportion of the bar's real body. It is used to filter out trading pairs with insufficient movement or volatility, or to determine the optimal trading period for a pair. The default threshold is set to 0.75, and trading pairs with scores below this threshold will be filtered out. To disable this filter, set the threshold to a very small non-zero value such as 0.0001.
:::

## Simple strategy example

Use the unified callback for new strategies. This complete example registers as `data:demo`. Setting OnData together with OnBar or OnInfoBar panics during construction; later OnBar examples remain valid when used separately. Route auxiliary and custom events with RouteData.

```go
package dataexample

import (
    "github.com/banbox/banbot/config"
    "github.com/banbox/banbot/strat"
    ta "github.com/banbox/banta"
)

func init() { strat.RegisterStrategy("data:demo", Demo) }

func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    return &strat.TradeStrat{
        WarmupNum: 100,
        OnData: strat.RouteData(strat.DataHandlers{
            Main: func(s *strat.StratJob, data strat.DataEvent) {
                fast, slow := ta.SMA(s.Env.Close, 5), ta.SMA(s.Env.Close, 20)
                cross := ta.Cross(fast, slow)
                if s.IsWarmUpState() { return }
                if cross == 1 {
                    _ = s.OpenOrder(&strat.EnterReq{Tag: "cross_up", Log: true})
                } else if cross == -1 {
                    _ = s.CloseOrders(&strat.ExitReq{Tag: "cross_down", Log: true})
                }
            },
        }),
    }
}
```


```go
package ma

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

func init() {
	// Register the strategy in Banbot, and use ma: demo in the configuration file to reference this strategy later
	// `init`The function is a special function in Go that will be executed immediately when the current package is imported
	strat.AddStratGroup("ma", map[string]strat.FuncMakeStrat{
		"demo": Demo,
	})
}

func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	smlLen := int(pol.Def("smlLen", 5, core.PNorm(3, 10)))
	bigLen := int(pol.Def("bigLen", 20, core.PNorm(10, 40)))
	return &strat.TradeStrat{
		WarmupNum: 100,
		OnBar: func(s *strat.StratJob) {
			e := s.Env
			ma5 := ta.SMA(e.Close, smlLen)
			ma20 := ta.SMA(e.Close, bigLen)
			maCrx := ta.Cross(ma5, ma20)

			if maCrx == 1 {
				s.OpenOrder(&strat.EnterReq{Tag: "open"})
			} else if maCrx == -1 {
				s.CloseOrders(&strat.ExitReq{Tag: "exit"})
			}
		},
	}
}
```
> You can use indicators in any callback function, but note that **all indicators must be called unconditionally once in the primary `OnData` handler**; otherwise, indicator calculations may be incorrect.

## banta: Technical Analysis Library
The high-performance indicator library [banta](https://github.com/banbox/banta) is used in banbot. It caches the calculation state of indicators for each bar, which is the key to the high performance of banbot. See the [banta upstream repository](https://github.com/banbox/banta) and [Go API](https://pkg.go.dev/github.com/banbox/banta) for indicators and call signatures.

`banta.BarEnv` and `banta.Series` are two key structures in banta.

`banta.BarEnv` is the operating environment of a technical indicator, which stores information such as the current exchange, market, symbol, time period, etc.
A strategy task will require at least one `banta.BarEnv`. If other symbols or time periods are subscribed through `OnPairInfos`, multiple operating environments will be required.
Within one Runtime, jobs can share a BarEnv for the same exchange, market, symbol and timeframe; different Runtimes have independent environments.

`banta.BarEnv` has several built-in original `Series`, namely: `Open`, `High`, `Low`, `Close`, `Volume`, which respectively save the sequence information of opening price, highest price, lowest price, closing price, and trading volume.

`Series` is a special structure in `banta` for storing serialized data. All technical indicator functions in `banta` receive one or more `Series`, and return a `Series` after calculation.

Current exports also include: `VWMA/VWAP`, `DEMA/T3/TEMA/MAMA/SSF/SWMA/TRIMA/VIDYA/ZLMA`, `DMI/ADX/AROONOSC/DX/SAR/PSAR/Ichimoku/KST`, `StochF/STOCHF/ULTOSC/Fisher/ROCR/TRIX/TSI/DPO/MOM/WilliamsPercent`, `Squeeze/NATR/Supertrend/Donchian/KeltnerChannel/PMAX`, `CMF/ADL/ADOSC/EFI/OBV/VPCI/MFI`, `LinReg/Slope/Correlation/LINEARREG_ANGLE`, `PivotHigh/PivotLow`, and `CDL*` candlestick patterns. Multi-return functions must receive values in the documented order.

You can use `Series.Get(0)` to get the latest value of a series, such as `e.Close.Get(0)` to get the latest closing price;
You can also use `Series.Range(0,5)` to get the 5 most recent values ​​of a series, which returns up to five values as a `[]float64` slice; insufficient history produces fewer.

When performing multiple repeated calculations at the same bar time (such as running the same batch of strategies on multiple accounts in real trading), `Series` will automatically use cached results to avoid repeated calculations.

For the Bollinger Bands indicator `banta.BBANDS`, which returns three parts `upper/mid/lower`, you need to extract them into three sequence objects as follows:
```go
    upper, mid, lower := ta.BBANDS(e.Close, 10, 2, 2)
```
Then you can execute `Get(i)` for `upper/mid/lower` to get the value.
::: tip Indicator operation tips
`banta` is an event-driven technical indicator library. The `OnBar` function will be executed once for each candle, which means that each time the technical indicator is called, only the latest value will be updated to `Series`.
Instead of vectorized calculation like `ta-lib`, all historical series are calculated at once.

`Series` saves the latest status of each indicator calculation. When a new candle is received, it will be quickly calculated from the last calculation status without repeating all the data of the series.

For example, `banta.EMA` requires the previous moving average value when calculating, and the `ta-lib` vector indicator library needs to calculate from scratch given the entire column of historical data, while `banta` will only calculate once from the last cached value.
:::
## Custom indicators

You can implement your custom indicator logic quickly and easily, taking `Highest` as an example:
```go:line-numbers {1}
func Highest(obj *ta.Series, period int) *ta.Series {
	res := obj.To("_hh", period)
	if res.Cached() {
		return res
	}
	if obj.Len() < period {
		return res.Append(math.NaN())
	}
	resVal := slices.Max(obj.Range(0, period))
	return res.Append(resVal)
}
```
The above is a simple technical indicator of the highest value in a specified period. The first 4 lines of all technical indicators are almost similar. A `Series` object is derived from `obj` to save the results.
The `_hh` and `period` parameters are used as hashes here.

In fact, `Series` has a member attribute:
```go
Subs  map[string]map[int]*ta.Series
```
It saves all `Series` derived from this `Series`. In essence, all `Series` in the strategy are derived from the original `Open/High/Low/Close/Volume`.

Since the indicator function only updates one value each time it is called, when the length of lines 6-8 is insufficient, nan is directly returned. Line 9 is the key to this indicator, which calculates the maximum value of the most recent n series and returns it.

## More complex custom indicators

Your custom indicator may sometimes need to cache more intermediate state information. In this case, you can customize the structure and save it in `Series.More`.
```go:line-numbers {1}
type sumState struct {
	sumVal float64
	addLen int
}

func Sum(obj *ta.Series, period int) *ta.Series {
	res := obj.To("_sum", period)
	if res.Cached() {
		return res
	}
	sta, _ := res.More.(*sumState)
	if sta == nil {
		sta = &sumState{}
		res.More = sta
	}
	curVal := obj.Get(0)
	if math.IsNaN(curVal) {
		// Invalid input value, reset and restart accumulation
		curVal = 0
		sta.sumVal = 0
		sta.addLen = 0
	} else {
		if sta.addLen < period {
			sta.sumVal += curVal
			sta.addLen += 1
		} else {
			oldVal := obj.Get(period)
			if math.IsNaN(oldVal) {
				sta.sumVal = 0
				sta.addLen = 0
			} else {
				sta.sumVal += curVal - oldVal
			}
		}
	}
	if sta.addLen < period {
		return res.Append(math.NaN())
	}
	return res.Append(sta.sumVal)
}
```
The above is an indicator that calculates the sum of the sequences of the most recent n periods. Lines 11 to 39 can actually be replaced by the following line, but the above approach of caching the last calculation result is significantly more efficient than the following two loops.
```go
    // gonum.org/v1/gonum/floats
    return res.Append(floats.Sum(obj.Range(0, period)))
```
## Strategy Warmup

Most indicators have an unstable startup period during which they are either unavailable (NaN) or calculated incorrectly. This can cause inconsistencies because the banbot does not know how long this unstable period should be.
To solve this problem, the `WarmupNum` property can be set for the strategy. This should be set to the maximum number of candlesticks required for the strategy to calculate a stable indicator.

For additional symbols or periods that `OnPairInfos` subscribes to, you can specify them in its return value:
```go
    OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
        return []*strat.PairSub{
            {"_cur_", "1h", 30},
        }
    },
```
The above additional subscription is for the `1h` period candle of the current symbol, and the preheating quantity is 30.

## Use CandleSticks of other periods/symbols {#info_bar}

Your strategy may not only require a single symbol and a single period, but sometimes you may need candlesticks of other periods or other symbols. You can subscribe and use them through `OnPairInfos` and `OnInfoBar`.

```go
package ma

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

type Demo2Sta struct {
	bigDirt int
}

func Demo2(pol *config.RunPolicyConfig) *strat.TradeStrat {
	smlLen := int(pol.Def("smlLen", 5, core.PNorm(3, 10)))
	bigLen := int(pol.Def("bigLen", 20, core.PNorm(10, 40)))
	return &strat.TradeStrat{
		WarmupNum: 100,
		OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
			return []*strat.PairSub{
				{"_cur_", "1h", 30},
			}
		},
		OnStartUp: func(s *strat.StratJob) {
			s.More = &Demo2Sta{}
		},
		OnBar: func(s *strat.StratJob) {
			e := s.Env
			m, _ := s.More.(*Demo2Sta)
			ma5 := ta.SMA(e.Close, smlLen)
			ma20 := ta.SMA(e.Close, bigLen)
			maCrx := ta.Cross(ma5, ma20)

			if maCrx == 1 && m.bigDirt > 0 {
				s.OpenOrder(&strat.EnterReq{Tag: "open"})
			} else if maCrx == -1 {
				s.CloseOrders(&strat.ExitReq{Tag: "exit"})
			}
		},
		OnInfoBar: func(s *strat.StratJob, e *ta.BarEnv, pair, tf string) {
			m, _ := s.More.(*Demo2Sta)
			ma5 := ta.SMA(e.Close, smlLen)
			ma20 := ta.SMA(e.Close, bigLen)
			m.bigDirt = ta.Cross(ma5, ma20)
		},
	}
}
```
The above is a moving average crossover strategy that combines both large and small timeframes. It enters the market only when the short moving average is above the long moving average on the larger timeframe, and the short moving average crosses above the long moving average on the smaller timeframe.

This strategy can be configured in the YML file to run on a `5m` small timeframe, while internally it specifies the subscription to `1h` large timeframe data. In `OnPairInfos`, `_cur_` represents the current trading pair. If other trading pairs are needed, you can hardcode the value in this section. A symbol that cannot be found in the current exchange and market is skipped silently.

::: tip Tip
`OnInfoBar` is solely intended for the consumption of additional candlestick data. Please refrain from executing logic related to opening/closing positions or updating stop-loss orders within this function.
:::

## Reference data: other symbols or timeframes {#ref_kline}

Strategies that use `OnData` can declare read-only K-line reference subscriptions with `RefKlineSub`; no trading job is created for the referenced symbol.

| Method | Description |
| --- | --- |
| `s.RefKlineSub(symbol, timeframe string, warmupNum ...int) *DataSub` | An empty `symbol` or `_cur_` means another timeframe of the current symbol; `BTC` or `BTC/USDT:USDT` are also accepted and normalized to the current exchange and market. When `warmupNum` is omitted, `TradeStrat.WarmupNum` is used |
| `s.RefEnv(symbol, timeframe string) *ta.BarEnv` | The synchronized K-line environment of that subscription; `nil` until its first bar arrives, after which `Open/High/Low/Close/Volume/Quote/BuyVolume/TradeNum` are all available |
| `s.Data(sub *DataSub) *DataFields` | The data view of a subscription; a nil `sub.ExSymbol` means the current symbol |

Each reference bar arrives as an auxiliary (`Info`) event. Calculate indicators in that event and let the main callback only read the result:

```go
type refState struct{ btcUp bool }

func RefDemo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    return &strat.TradeStrat{
        WarmupNum: 100,
        OnStartUp: func(s *strat.StratJob) { s.More = &refState{} },
        OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
            return []*strat.DataSub{s.RefKlineSub("BTC/USDT:USDT", "1h", 100)}
        },
        OnData: strat.RouteData(strat.DataHandlers{
            Info: func(s *strat.StratJob, d strat.DataEvent) {
                env := s.RefEnv("BTC/USDT:USDT", "1h")
                if env == nil { return }
                s.More.(*refState).btcUp = env.Close.Get(0) > ta.SMA(env.Close, 20).Get(0)
            },
            Main: func(s *strat.StratJob, d strat.DataEvent) {
                if s.IsWarmUpState() || !s.More.(*refState).btcUp { return }
                _ = s.OpenOrder(&strat.EnterReq{Tag: "btc_trend", Log: true})
            },
        }),
    }
}
```
For K-line subscriptions, `DataFields.BarEnv()` returns a full `*ta.BarEnv` (likewise non-`nil` only after the first bar, and always `nil` for non-K-line sources), so indicators that need full OHLCV such as `ta.MFI(env, 14)`, `ta.WillR(env, 14)` and `ta.HeikinAshi(env)` can be used directly on reference data.

### Built-in derivatives data

Built-in sources such as funding rate, open interest and mark/index/premium K-lines are subscribed by name in `OnDataSubs` and arrive as `Custom` events; download the data first:
```go
OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
    return []*strat.DataSub{
        {Source: orm.SeriesSourceFundingRate, TimeFrame: "8h", WarmupNum: 3},
        {Source: orm.SeriesSourcePremiumKline, TimeFrame: "1h", WarmupNum: 3},
    }
},
OnData: strat.RouteData(strat.DataHandlers{
    Custom: func(s *strat.StratJob, d strat.DataEvent) {
        if d.Source == orm.SeriesSourceFundingRate { s.More.(*derivState).funding = d.Float64("rate") }
    },
    Main: func(s *strat.StratJob, d strat.DataEvent) { /* ... */ },
}),
```
Fields, default timeframes, downloading and backtest semantics of each source are covered in [Custom time-series data](./custom_data); a complete strategy is `derivatives.go` in [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures).

## Intraday session helpers {#intraday}

`StratJob` has built-in intraday/session statistics without lookahead; their state is kept per job (symbol). Anchor arguments are UTC minutes of the day (`0`-`1439`, taken modulo 1440); the `btime` package provides common anchors such as `btime.NewYorkOpenMinutes` (13:30 UTC), `btime.LondonCashOpenMinutes` (08:00 UTC) and `btime.AsiaCloseMinutes` (08:00 UTC).

| Method | Description |
| --- | --- |
| `OpeningRange(anchorMinutesUTC, rangeMinutes int) OpeningRangeSnapshot` | Opening range over `[anchor, anchor+rangeMinutes)`: `Open/High/Low/Close/Mid/Width/Volume` |
| `OpeningRangeWithOpts(anchorMinutesUTC, rangeMinutes int, opts OpeningRangeOptions) OpeningRangeSnapshot` | Same; `AllowSingleBarFallback` lets a timeframe coarser than the range fall back to a single bar (`DegradedSingleBar=true`) |
| `SessionRange(startMinUTC, endMinUTC int) SessionRangeSnapshot` | Range of a recurring session window (may cross midnight, e.g. 22:00-02:00); fields include `Completed/JustCompleted/InSession/InPostSession` |
| `PrevDayOHLC(anchorMinutesUTC int) (PrevOHLC, bool)` | `Open/High/Low/Close/Pivot/Range/Volume` of the previous complete trading day |
| `PrevNDaysStats(anchorMinutesUTC, nDays int) (PrevNDaysSummary, bool)` | Statistics over the last `nDays` (1-366) complete trading days: `HH/HC/LC/LL/AvgRange/DualThrustRange/History`, etc. |
| `DualThrustBands(anchorMinutesUTC, nDays int, k1, k2 float64) (upper, lower, dayOpen float64, ok bool)` | Dual Thrust bands: `dayOpen +/- k x Range` |
| `RBreakerLevels(anchorMinutesUTC int) (RBreakerSnap, bool)` / `RBreakerLevelsBy(anchorMinutesUTC int, f1, f2, f3 float64)` | The six R-Breaker levels plus today's running open/high/low |

Notes:
- Before `Ready` (`Completed` for `SessionRange`) the range values such as `High/Low` are 0, so an unfinished range never leaks; `Applicable=false` means the current timeframe does not fit (1d or coarser, range not aligned to the timeframe, or timeframe coarser than the range without single-bar fallback), and no result will appear.
- With a non-zero anchor, `PrevDayOHLC` and related methods do not treat an incomplete leading day as the previous day; warmup should cover at least one full trading day, and do not trade while `ok` is `false`.
- Call them on every bar in the main callback so their state advances with the bars; repeated calls within one bar return the same result.
- Without an environment (`s.Env==nil`) they return zero values and `false`.

```go
Main: func(s *strat.StratJob, _ strat.DataEvent) {
    orb := s.OpeningRange(btime.NewYorkOpenMinutes, 30) // call on every bar
    if !orb.Ready || !orb.InPostRange || s.GetOrderNum(0) > 0 { return }
    if c := s.Env.Close.Get(0); c > orb.High {
        // Stop at the range midpoint, size by risk rate
        _ = s.OpenOrder(&strat.EnterReq{Tag: "orb_long", StopLoss: orb.Mid, TakeProfitVal: orb.Width * 2, RiskRate: 0.005})
    }
},
```
The run timeframe must not be larger than the range (e.g. `run_timeframes: [15m]` with a 30-minute range). See `orb.go` in [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) for a complete example.

## Save temp variables

Sometimes you may need to save some temp variables for the next calculation; or you may need to synchronize some information between different callback functions.

As mentioned in the [previous section](#info_bar), if you need to use the information from `OnInfoBar` within `OnBar`, it must be relayed through `StratJob.More`.

First, initialize `StratJob.More` in `OnStartUp`.
Then perform type conversion in `OnBar` and `OnInfoBar` and assign it to the `m` variable:
```go
m, _ := s.More.(*Demo2Sta)
```

## Send an entry signal {#send_entry}

To Send an entry signal, you only need to call the `StratJob.OpenOrder` method and pass a `*strat.EnterReq` parameter.

The simplest case is to only assign a value to the `Tag` property of `EnterReq`, and use the default parameters for all others. This requests a long entry using configured stake and leverage. The strategy default StopLoss can still supply a stop loss.

Common strategy parameters of `EnterReq` (see the strat API for the full reference):
```go
type EnterReq struct {
    CommandID string // Stable request identity
	Tag             string  // Entry signal
	StratName        string  // Strategy Name
	Short           bool    // Whether to short sell
	OrderType       int     // Order type, core.OrderType*
	Limit           float64 // Limit order entry price, submitted as limit order when specified; with Stop it makes a Stop-Limit entry
    Stop            float64 // Entry trigger price (not a stop loss): a long enters when price rises to it, a short when price falls to it
	OCOGroup        string  // OCO pending-entry group: when one entry in the group fills, the other unfilled entries are cancelled
	CostRate        float64 // Opening ratio, default 1 times. Used for calculating LegalCost
	RiskRate        float64 // Share of account equity risked at the stop loss, [0,1]; sizes the position from the stop distance when Amount/LegalCost are unset
	LegalCost       float64 // Spend amount in fiat. Ignores CostRate when specified
	Leverage        float64 // Leverage ratio
	Amount          float64 // Amount of target asset, calculated from LegalCost or CostRate
	StopLossVal     float64 // Distance (price difference, not a ratio) from entry price to stop loss price, used to calculate StopLoss
	StopLoss        float64 // Stop loss trigger price; submitted as an exchange conditional order, or only monitored locally when StratJob.ExgStopLoss=false
	StopLossLimit   float64 // Limit price used after the stop triggers (0 = market); also the trigger price when StopLoss is 0
	StopLossRate    float64 // Stop loss exit ratio. 0 means 100%. Must be (0,1]
	StopLossTag     string  // Reason for Stop Loss
	ActivationPrice float64 // Trailing stop activation price, 0 = active immediately; only used when CallbackPct>0
	CallbackPct     float64 // Trailing stop callback percent (unit %, 1.5 means 1.5%), 0 = off, valid range [0.05, 50]
	TakeProfitVal   float64 // Distance from entry price to take profit price, used to calculate TakeProfit
	TakeProfit      float64 // Take profit trigger price; submitted as an exchange order, or only monitored locally when StratJob.ExgTakeProfit=false
	TakeProfitLimit float64 // Limit price used after the take profit triggers (0 = market); also the trigger price when TakeProfit is 0
	TakeProfitRate  float64 // Take profit exit ratio. 0 means 100%. Must be (0,1]
	TakeProfitTag   string  // Reason for profit taking
	TakeProfits     []strat.TakeProfitTarget // Multi-stage take-profit ladder; cannot be combined with TakeProfit/TakeProfitVal/TakeProfitLimit
	StopBars        int     // Cancel entry limit order if not filled after N bars
    ClientID        string  // Used to set the trailing part of the ClientOrderID submitted to the exchange.
    Infos           map[string]string // Used to set additional information saved in the order.
	Log             bool // Whether to automatically log error messages
}
```
To trigger an entry at a specific price, you can set the `Limit` or `Stop` parameters. On the default prod / dry-run shared-account (Hub) path, order prices (`Limit`, `StopLossLimit`, `TakeProfitLimit`, the `Limit` of a take-profit stage, and the `Limit` of exit requests and edited orders) are rounded to the instrument's price tick before submission; trigger prices (`Stop`, `StopLoss`, `TakeProfit`, stage `Price`) are not rounded.

`Limit` would put a limit order. For a long order, you can set a lower price to enter the position. For a short order, you can set a higher price to enter the position.

`Stop` is the entry trigger price (not a stop loss; use `StopLoss` for that). For a long order, you can set a higher price to enter after a breakout. For a short order, you can set a lower price to enter after a breakdown.

Setting both `Stop` and `Limit` makes a Stop-Limit entry: once price reaches `Stop`, a limit order rests at `Limit`; if `Stop` has already been crossed by the current price, the limit order is placed directly. `Limit` cannot be set when `OrderType` is `core.OrderTypeMarket`.

For a long breakout above 120 when the price is 100, use `&strat.EnterReq{Tag: "breakout", Stop: 120}`. To buy after the breakout at no more than 121, with a stop loss:
```go
_ = s.OpenOrder(&strat.EnterReq{Tag: "breakout", Stop: 120, Limit: 121, StopLoss: 110, StopBars: 3})
```

When `OpenOrder` returns an error, it indicates that opening the order failed. You can use `log.Error` to record error information, or set `Log` to true in `EnterReq` to let the system automatically log error messages.

### Position sizing by risk {#risk_rate}

With `RiskRate` set, the position is derived from the stop distance. It applies when `Amount` and `LegalCost` are unset, `RiskRate>0`, and a stop price can be resolved (`StopLoss`/`StopLossVal`, `StratJob.LongSLPrice/ShortSLPrice`, or `TradeStrat.StopLoss`):

```text
LegalCost = AccountEquity() x RiskRate / (|entry price - stop price| / entry price)
```
The result is capped by leverage (1x for spot) and the account's `max_stake_amt`; without a stop it falls back to the default stake amount x `CostRate`. `open_vol_rate` and the minimum order amount still apply afterwards. `RiskRate` must be within `[0, 1]` and is ignored when `Amount` or `LegalCost` is given.

`StratJob` provides three account-state methods for sizing and risk checks:

| Method | Meaning |
| --- | --- |
| `AccountEquity() float64` | Account equity (balance + unrealized PnL, in the pricing currency); during warmup or before the wallet is ready it falls back to the initial capital / default stake amount, and never returns 0 or NaN |
| `AvailableBalance() float64` | Available balance / margin; 0 when unavailable |
| `AccountDrawdown() float64` | Drawdown ratio versus the equity peak since the run started, in `[0,1]`; 0 during warmup. Shared by all symbols of the account |

On Hub-path robots these account metrics share a cache of up to 1 second: inside sub-second callbacks such as `OnTick`, `AccountEquity()` and the other methods may be up to 1 second old; bar callbacks of 1 minute or longer are not affected.

```go
Main: func(s *strat.StratJob, _ strat.DataEvent) {
    e := s.Env
    cross := ta.EMA(e.Close, 10).Cross(ta.EMA(e.Close, 30))
    atr := ta.ATR(e.High, e.Low, e.Close, 14).Get(0)
    if cross != 1 || math.IsNaN(atr) || atr <= 0 { return }
    if s.AccountDrawdown() >= 0.10 || s.AvailableBalance() < s.AccountEquity()*0.2 { return }
    // Risk at most 1% of equity per trade: the stop distance is 2 ATR
    _ = s.OpenOrder(&strat.EnterReq{Tag: "ema_long", StopLossVal: 2 * atr, RiskRate: 0.01})
},
```
See [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) for complete examples.

:::tip
`OpenOrder`/`CloseOrders` validate and enqueue intents; they do not guarantee fills. Main `OnData`/`OnBar`, `OnBatchJobs`, order callbacks and successful `OnPostApi` callbacks have framework processing paths. Do not assume automatic submission from auxiliary or high-frequency callbacks.

For explicit submission use `_, _, err, bound := s.ProcessRuntimeOrders()` and check err and bound. bound=false means no explicit Runtime is bound; do not substitute a global account of the same name. Process another symbol through its own job. During warmup OpenOrder may return nil without enqueuing anything.
:::
## Send an exit signal

To send an exit signal, just call the `StratJob.CloseOrders` method and pass a `*strat.ExitReq` parameter.

The simplest case is to only assign a value to the `Tag` property of `ExitReq` and use the default parameters for all other parameters. That is, close all orders with market orders, including long orders and short orders.

Common strategy parameters of `ExitReq`:
```go
type ExitReq struct {
    CommandID string // Stable request identity
	Tag        string  // Exit signal
	StratName   string  // Strategy Name
	EnterTag   string  // Only exit orders with EnterTag as the entry signal
	Dirt       int     // core.OdDirt* long/short/both; the zero value means both
	OrderType  int     // Order type, core.OrderType*
	Limit      float64 // Limit order exit price. Submitted as limit order when specified
	ExitRate   float64 // Exit rate, default 100% (all orders)
	Amount     float64 // Amount to exit. ExitRate is invalid when specified
	OrderID    int64   // Only exit specified order
	UnFillOnly bool    // If true, only exit orders that have not yet entered
	FilledOnly bool    // If true, only exit orders that have already entered
	Force      bool    // Whether to force exit
	Log        bool    // Whether to automatically log error messages
}
```

When `CloseOrders` returns an error, it indicates that closing the position failed. You can use `log.Error` to record error information, or set `Log` to true in `ExitReq` to let the system automatically log error messages.

The zero value of `Dirt` equals `core.OdDirtBoth`; when both long and short orders exist and `Limit` is set, `Dirt` must be specified explicitly or an error is returned.

For `ExitReq.Tag`, you can pass in a fixed string to distinguish different exit reasons. The `core` package has built-in common tags:

```go
const (
    ExitTagUnknown     = "unknown"      // Unknown reason
    ExitTagCancel      = "cancel"       // User cancel
    ExitTagHedgeOff    = "hedge_off"    // Hedge close when two-way positions are turned off
    ExitTagBotStop     = "bot_stop"     // Bot stop
    ExitTagForceExit   = "force_exit"   // Force exit
    ExitTagNoMatch     = "no_match"     // No matching orders
    ExitTagUserExit    = "user_exit"    // User initiated exit
    ExitTagThird       = "third"        // Third party triggered
    ExitTagCli         = "cli"          // Command line triggered
    ExitTagFatalErr    = "fatal_err"    // Fatal error
    ExitTagPairDel     = "pair_del"     // Symbol deleted
    ExitTagStopLoss    = "stop_loss"    // Stop loss triggered with loss
    ExitTagSLTake      = "sl_take"      // Stop loss triggered with profit
    ExitTagTrailingStop = "trailing_stop" // Trailing stop triggered
    ExitTagTakeProfit  = "take_profit"  // Take profit triggered
    ExitTagDrawDown    = "draw_down"    // Drawdown stop loss
    ExitTagDataStuck   = "data_stuck"   // Auto close due to no K-line data for extended period
    ExitTagLiquidation = "liquidation"  // Forced liquidation
    ExitTagEnvEnd      = "env_end"      // Due to ex-dividend, main contract switch, etc.
    ExitTagEntExp      = "ent_expire"   // Entry limit order expired
    ExitTagExitDelay   = "exit_delay"   // Orders that should have exited earlier during live trading restart
)
```
Backtests can also produce two more exit tags: `data_gap` (forced close after a hole in the K-line data) and `delist` (forced close at the end of a backtest for a symbol whose data ended long before). They only apply to backtests; see the [backtest guide](./backtest.md).

## Stop Loss and Take Profit

In addition to setting the stop loss and take profit of an order when entering the market, you can also directly set the stop loss and take profit of a certain order at any time:
```go
if len(s.LongOrders) > 0 {
    od := s.LongOrders[0]
    ma5Val := ma5.Get(0)
    od.SetStopLoss(&ormo.ExitTrigger{Price: ma5Val * 0.97})
    od.SetTakeProfit(&ormo.ExitTrigger{Price: ma5Val * 1.03})
}
```
You can also set stop loss and take profit for all orders:
```go
ma5Val := ma5.Get(0)
s.SetAllStopLoss(core.OdDirtLong, &ormo.ExitTrigger{
    Price: ma5Val * 0.97,
    Limit: ma5Val * 0.965,
    Rate:  0.5,
    Tag:   "stop half",
})
```
For all open long orders, a stop loss of 50% of the position is set. When the price triggers 0.97 times the 5-period moving average, half of the position is closed with a limit order stop loss of 0.965 times.

Passing `nil` as `args` cancels all stop losses (take profits) in that direction. Passing `core.OdDirtBoth` (0) as `dirt` panics if long and short orders exist at the same time; call it separately with `core.OdDirtLong` and `core.OdDirtShort` in that case.

### Exchange conditional orders versus local monitoring

`StratJob.ExgStopLoss` and `StratJob.ExgTakeProfit` default to true, so stop loss / take profit are submitted to the exchange as conditional orders. When set to false, no conditional order is placed in advance and the trigger is monitored locally by price (in both backtest and live). A `CallbackPct` trailing stop is also monitored locally when `ExgStopLoss=false`.

### Trailing stop

`CallbackPct` (unit %, range `[0.05, 50]`) closes the position when price pulls back from its best level by that percentage; when `ActivationPrice` is greater than 0, tracking only starts after price reaches the activation price. In live trading the exchange's native trailing order is preferred; if the exchange does not support it or rejects it, it falls back to local price monitoring and the order does not fail.
```go
_ = s.OpenOrder(&strat.EnterReq{Tag: "trail", StopLossVal: atr * 2, ActivationPrice: price + atr, CallbackPct: 1.5})
```

### Take-profit ladder {#take_profits}

`EnterReq.TakeProfits` sets several take-profit stages; each is a `strat.TakeProfitTarget`:

| Field | Description |
| --- | --- |
| `Price` / `Val` / `Rate` | Exactly one is needed (must be greater than 0): absolute trigger price; price distance from the entry fill average; ratio relative to the entry fill average (0.02 = 2%). When several are given `Price` wins, then `Val`. `Price` must be on the profit side |
| `Limit` | Limit price after the trigger, 0 = market |
| `ExitRate` | Share of the initial position closed at this stage, `(0,1]`; 0 is treated as 1 when there is a single stage; the stages must sum to at most 1 |
| `MoveSLToBreakeven` | After this stage fills, tighten the remaining position's stop loss to no worse than the entry price (an already tighter stop is never loosened) |
| `Tag` | Exit reason for this stage |

Stages trigger from nearest to farthest from the entry price, and cannot be combined with `TakeProfit`, `TakeProfitVal` or `TakeProfitLimit`; once `TakeProfits` is set, `LongTPPrice/ShortTPPrice` no longer apply.

Under the default prod / dry-run shared-account (Hub) path the ladder is driven by the robot: the first stage is the position's take-profit, and each next stage is installed after the previous one is reported as executed (seen on the robot's next poll, so it can lag slightly). A non-final stage smaller than one quantity step is merged into the next stage. `Val`/`Rate` stages are estimated from the reference price (`Limit`, else `Stop`, else the mid price) and recomputed from the real average fill once the entry fill is observed. A stage that resolves to a non-positive price makes the entry be rejected, and a single-level `TakeProfit` edit of an order that has a ladder is rejected. Every stage change restarts trailing-stop tracking, and an exit by a stage or by the stop carries no per-stage exit tag in the order view.
```go
_ = s.OpenOrder(&strat.EnterReq{
    Tag: "ladder", StopLossVal: atr * 2,
    TakeProfits: []strat.TakeProfitTarget{
        {Val: atr * 1.5, ExitRate: 0.4, MoveSLToBreakeven: true, Tag: "tp1"},
        {Val: atr * 3, ExitRate: 0.3, Tag: "tp2"},
    },
    ActivationPrice: price + atr*1.5, CallbackPct: 1.5, // the remaining 30% is trailed
})
```

### Default stop-loss and take-profit prices

`StratJob.LongSLPrice/ShortSLPrice` and `LongTPPrice/ShortTPPrice` are default stop-loss / take-profit prices applied when an entry is opened: they are used when `EnterReq` does not specify the corresponding stop loss (take profit), and the default take-profit price is ignored when `TakeProfits` is set. A long stop must be below the entry price and a take profit above it (reversed for shorts); otherwise `OpenOrder` returns an error.

### OCO pending-entry groups

Give several pending entries the same `OCOGroup`; when one of them fills, the other unfilled entries in the group are cancelled automatically. This is typically used for breakout orders on both sides of a range:
```go
_ = s.OpenOrder(&strat.EnterReq{Tag: "brk_long", Stop: hi, Limit: hi + slip, OCOGroup: "donchian", StopBars: 2, StopLossVal: atr * 2})
_ = s.OpenOrder(&strat.EnterReq{Tag: "brk_short", Short: true, Stop: lo, Limit: lo - slip, OCOGroup: "donchian", StopBars: 2, StopLossVal: atr * 2})
```
On the default prod / dry-run shared-account (Hub) path the account execution service enforces OCO itself: when several members are executable in the same tick only the earliest-created one is kept and the others are cancelled; once a member of the group has a fill, the other still-open unfilled members are cancelled too. A member that already has a fill, a live exchange order, a reservation or an exit is never cancelled; if several members are filled they all stay and a warning is logged. The robot additionally sends a cancel after it observes a fill, and a member it cancels is reported with exit tag `oco_cancel`.

Groups are identified by (symbol, strategy, `OCOGroup`) and are meant for pending entries (with `Stop` or `Limit` set). In backtests, pair it with `refine_tf` so the order in which the two sides trigger within one bar is judged more accurately. Pending entries occupy open-order slots, so `max_simul_open` must be large enough. See `bracket.go` in [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) for a complete example.

## Batch task processing
Sometimes you may need to perform some calculations (such as correlation coefficients) for all symbols of the current strategy together, get some intermediate states to save, or open or close orders together.
In this case, you can use the `OnBatchJobs` or `OnBatchInfos` callback function.
`OnBatchJobs` is triggered only after main `OnData(RoleMain)`/`OnBar`; `OnBatchInfos` is triggered only after auxiliary `OnData(RoleInfo)`/`OnInfoBar`. They are enabled by `BatchInOut` and `BatchInfo`, respectively.
> In backtests, within the same bar the main-batch symbols are handed to `OnBatchJobs` in the neutral hash order of `strat.BarPriority(barMS, symbol, seed)` (not alphabetical), so alphabetically early symbols do not systematically claim funds first; the seed is the `bt_bar_order_seed` setting (default 0), and changing it changes multi-symbol backtest results. Live batch order is not guaranteed. If simultaneous entries compete for funds, sort by strategy score inside the callback. The example stores correlations for subsequent callbacks without assuming main/auxiliary callback order at the same timestamp.
>
> Open/close requests made in `OnBatchJobs` are submitted automatically by the framework after the callback (except during warmup); requests made in `OnBatchInfos` are **not** submitted automatically and only log a WARN, so do not place orders there.
```go
// Imports: slices, config, strat, utils, banexg/log, banta as ta, zap.
type BatchSta struct {
    smlCorr, bigCorr float64
    smlReady, bigReady bool
}

func calcCorrs(jobs []*strat.StratJob, envs []*ta.BarEnv, isBig bool) {
    if len(jobs) < 2 { return }
    dataArr := make([][]float64, 0, len(envs))
    for _, env := range envs {
        values := env.Close.Range(0, 70)
        if len(values) < 70 { return }
        dataArr = append(dataArr, values)
    }
    _, values, err := utils.CalcCorrMat(70, dataArr, true)
    if err != nil {
        log.Error("calc corr mat fail", zap.Error(err))
        return
    }
    for i, job := range jobs {
        state := job.More.(*BatchSta)
        if isBig { state.bigCorr, state.bigReady = values[i], true
        } else { state.smlCorr, state.smlReady = values[i], true }
    }
}

func BatchDemo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    return &strat.TradeStrat{
        WarmupNum: 100, BatchInOut: true, BatchInfo: true,
        OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
            return []*strat.PairSub{{Pair: "_cur_", TimeFrame: "1h", WarmupNum: 100}}
        },
        OnStartUp: func(s *strat.StratJob) { s.More = &BatchSta{} },
        OnBar: func(s *strat.StratJob) {
            state := s.More.(*BatchSta)
            if !state.smlReady || !state.bigReady { return }
            if state.bigCorr < 0.5 && state.smlCorr < 0.5 {
                _ = s.OpenOrder(&strat.EnterReq{Tag: "open", Log: true})
            } else if state.smlCorr > 0.9 {
                _ = s.CloseOrders(&strat.ExitReq{Tag: "close", Log: true})
            }
        },
        OnBatchJobs: func(jobs []*strat.StratJob) {
            envs := make([]*ta.BarEnv, 0, len(jobs))
            for _, job := range jobs { envs = append(envs, job.Env) }
            calcCorrs(jobs, envs, false)
        },
        OnBatchInfos: func(tf string, items map[string]*strat.JobEnv) {
            keys := utils.KeysOfMap(items)
            slices.Sort(keys)
            jobs := make([]*strat.StratJob, 0, len(keys))
            envs := make([]*ta.BarEnv, 0, len(keys))
            for _, key := range keys {
                item := items[key]
                jobs = append(jobs, item.Job)
                envs = append(envs, item.Env)
            }
            calcCorrs(jobs, envs, true)
        },
    }
}
```
> Update stateful indicators on every event for their own environment: main OnData/OnBar or auxiliary OnData/OnInfoBar. Batch callbacks should read updated results, not be the only place that advances an indicator.

### Rebalancing to target weights

For cross-symbol portfolio rebalancing, use `strat.RebalanceBatchWeights` inside `OnBatchJobs`: pass each symbol's target weight (a signed notional share of `AccountEquity()`; positive = long, negative = short, missing or 0 = flat). It first closes/reduces positions to free margin, then opens/scales in from the largest required amount down; orders below the minimum order amount are skipped silently.

```go
func RebalanceBatchWeights(jobs []*StratJob, targetWeights map[string]float64, opts BatchRebalanceOpts) BatchRebalanceResult
func InverseVolWeights(jobs []*StratJob, signals map[string]float64, atrPeriod int, targetGross float64) map[string]float64
```

| `BatchRebalanceOpts` field | Description |
| --- | --- |
| `Tag` | Tag of the open/close signals, default `rebalance` |
| `DriftThreshold` | No rebalancing when a same-direction position's weight differs by no more than this, default 0.02, negative disables |
| `MaxGrossExposure` | Cap on the sum of absolute target weights; targets are scaled down proportionally when exceeded |
| `StopLossRate` / `TakeProfitRate` | Stop-loss / take-profit distance attached to newly opened or scaled-in orders (as a ratio of the entry price) |
| `CallbackPct` | Trailing-stop callback percent attached to new orders, range `[0.05, 50]` |
| `Leverage` | Overrides the leverage of new orders when greater than 0 |

The returned `BatchRebalanceResult` carries the raw and effective gross exposure, action counts (`ClosedCount/ReducedCount/OpenedCount/ScaledInCount/ErrorCount`, etc.) and the per-order `Actions`. `InverseVolWeights` turns directional signals (e.g. +1/-1/0) into risk-parity weights using `1/NATR`, normalized to `targetGross`; it must be called on every bar (`atrPeriod<=0` means 14) to keep its internal indicator caches in sync.

```go
OnBatchJobs: func(jobs []*strat.StratJob) {
    weights := map[string]float64{} // symbol -> target weight, built from your ranking/signals
    // ...
    strat.RebalanceBatchWeights(jobs, weights, strat.BatchRebalanceOpts{
        Tag: "mom_rebalance", DriftThreshold: 0.03, MaxGrossExposure: 1.0, StopLossRate: 0.15,
    })
},
```
`BatchInOut` must be on, and `EachMaxLong/EachMaxShort` set to 0 so positions can be scaled into. See `rebalance.go` in [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) for a complete example.

## Custom exit logic
You can perform custom exit logic checks on each candlestick, for each open order:
```go
func CustomExitDemo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		OnBar: func(s *strat.StratJob) {
			if len(s.LongOrders) == 0 {
				s.OpenOrder(&strat.EnterReq{Tag: "long"})
			} else if rand.Float64() < 0.1 {
				s.CloseOrders(&strat.ExitReq{Tag: "close"})
			}
		},
		OnCheckExit: func(s *strat.StratJob, od *ormo.InOutOrder) *strat.ExitReq {
			if od.ProfitRate > 0.1 {
				// Exit if profit exceeds 10%
				return &strat.ExitReq{Tag: "profit"}
			}
			return nil
		},
	}
}
```

## Stop loss by retracement (trailing stop loss)
You can easily implement stop loss by retracement through `DrawDownExit` and `GetDrawDownExitRate`.
For example, when the best historical profit of an order exceeds 10%, the order will be exited when the retracement exceeds 50%:
```go
func DrawDown(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		DrawDownExit: true,
		OnBar: func(s *strat.StratJob) {
			if len(s.LongOrders) == 0 {
				s.OpenOrder(&strat.EnterReq{Tag: "long"})
			}
		},
		GetDrawDownExitRate: func(s *strat.StratJob, od *ormo.InOutOrder, maxChg float64) float64 {
			if maxChg > 0.1 {
				// After the best profit of the order exceeds 10%, withdraw 50% and exit
				return 0.5
			}
			return 0
		},
	}
}
```
## Dynamic Modification the Subscription Symbols 
You can directly specify the `pairs` symbol list in the yml or perform dynamic filtering through `pairlists`. However, you can also listen to the symbol list and modify the return through the `OnSymbols` callback function in the strategy:
```go
func editPairs(p *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		OnSymbols: func(items []string) []string {
			hasBTC := false
			for _, it := range items {
				if it == "BTC" {
					hasBTC = true
				}
			}
			if !hasBTC {
				return append([]string{"BTC"}, items...)
			}
			return items
		},
	}
}
```
The above checks whether BTC is in the subscribed varieties, and adds it if it's not.
:::tip tip
You need to return the modified symbol list. Not all these varieties will be traded, which is restricted by `run_policy.max_pair` and `TradeStrat.MinTfScore`. So the order of the varieties is important.
:::

## Dynamic Pair Updates
You can dynamically add or remove trading pairs during strategy execution using the `UpdatePairs` method without restarting the bot.

```go
// botruntime aliases github.com/banbox/banbot/runtime; errs is banexg/errs.
// rt must be the Runtime that owns s, supplied by your embedding application.
func updatePairs(rt *botruntime.Runtime, s *strat.StratJob) (*strat.PairUpdateResult, *errs.Error) {
    return s.Strat.UpdatePairs(strat.PairUpdateReq{
        StrategyState: rt.Strategies,
        Core: rt.Core,
        Exchange: rt.Exchange,
        Add: []string{"BTC", "ETH"},
        Remove: []string{s.Symbol.Symbol},
        CloseOnRemove: true,
    })
}
```

Ordinary strategy callbacks have no public Runtime state getter. Without an rt supplied by the host, prefer configured rotation or OnSymbols. Updates require SubWarmPairs hooks; CloseOnRemove also requires ExitOrders. Check the error, Added/Removed/Skipped and Warnings. Removed jobs with pending orders remain until terminal order events. ForceAdd bypasses normal symbol admission restrictions, not data, scoring or execution requirements.


**Parameters:**
- `Add`: List of pairs to add
- `Remove`: List of pairs to remove
- `CloseOnRemove`: Whether to close orders when removing
- `ForceAdd`: Bypass normal symbol admission restrictions; data, scores and exchange checks can still fail
- `Reason`: Reason for update (optional)

## Strategy Exit Callbacks
When the bot stops (backtest ends or live trading stops), the system calls strategy exit callback functions in sequence to clean up resources or save state.

### OnShutDown
`OnShutDown` is called when each strategy task (`StratJob`) stops, with the current task object as parameter. Since a strategy may run on multiple accounts, multiple symbols, and multiple timeframes, this function will be called multiple times.

Use cases:
- Save individual symbol's running state
- Clean up symbol-related resources
- Record symbol-level statistics

```go
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		OnBar: func(s *strat.StratJob) {
			// Strategy logic
		},
		OnShutDown: func(s *strat.StratJob) {
			log.Info("job shutdown",
				zap.String("pair", s.Symbol.Symbol),
				zap.String("tf", s.TimeFrame),
				zap.String("account", s.Account))
		},
	}
}
```

### OnStratExit
`OnStratExit` is called when the strategy config exits, with no parameters. It's called once per strategy config, after all `OnShutDown` executions complete.

Use cases:
- Close strategy-level global resources (e.g., database connections, file handles)
- Save strategy-wide statistics
- Clean up strategy-shared state

```go
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	// Strategy-level resources
	var logFile *os.File
	var err error

	return &strat.TradeStrat{
		OnBar: func(s *strat.StratJob) {
			if logFile == nil {
				logFile, err = os.Create("strategy.log")
				if err != nil {
					log.Error("create log file fail", zap.Error(err))
				}
			}
			// Strategy logic
		},
		OnStratExit: func() {
			if logFile != nil {
				logFile.Close()
				log.Info("strategy log file closed")
			}
		},
	}
}
```

:::warning Note
- `OnShutDown` receives `*StratJob` parameter, can access symbol, account info
- `OnStratExit` has no parameters, used to clean up strategy-level global resources
- Execution order: all `OnShutDown` first, then `OnStratExit`
- Same strategy function may be called multiple times with different parameters, but each config's `OnStratExit` executes only once
:::

## Websocket High-Frequency Data Subscription
You can subscribe to high-frequency data pushed by exchanges via Websocket in the strategy, including: incomplete K-lines, tick-by-tick trades, and order book depth. Live subscriptions depend on exchange and data-source support. Ordinary K-line backtests do not include historical trades or depth and cannot reproduce these callbacks.

Actual latency depends on the source, network and workload; there is no fixed millisecond guarantee.

Here is an example:
```go
import (
	"fmt"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
    "go.uber.org/zap"
)

func ws(p *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		WsSubs: map[string]string{
			core.WsSubDepth: "",
			core.WsSubTrade: "",
			core.WsSubKLine: "",
		},
		OnBar: func(s *strat.StratJob) {
			e := s.Env
			log.Info(fmt.Sprintf("OnBar %v: %v", e.TimeStop, e.Close.Get(0)))
		},
		OnWsKline: func(s *strat.StratJob, pair string, k *banexg.Kline) {
			log.Info(fmt.Sprintf("OnWsKline %v: %v", k.Time, k.Close))
            s.OpenOrder(&strat.EnterReq{Tag: "long"})
            _, _, err, bound := s.ProcessRuntimeOrders()
            if err != nil {
                log.Error("process order fail", zap.Error(err))
            } else if !bound {
                log.Warn("runtime order processor is not bound")
            }
		},
		OnWsTrades: func(s *strat.StratJob, pair string, trades []*banexg.Trade) {
			if len(trades) == 0 { return }
			last := trades[len(trades)-1]
			log.Info(fmt.Sprintf("OnWsTrades %v %v, %v", last.Timestamp, last.Price, last.Amount))
		},
		OnWsDepth: func(s *strat.StratJob, dep *banexg.OrderBook) {
			if dep == nil || dep.Bids == nil || dep.Asks == nil || len(dep.Bids.Price) == 0 || len(dep.Asks.Price) == 0 { return }
			bp1, bm1 := dep.Bids.Price[0], dep.Bids.Size[0]
			ap1, am1 := dep.Asks.Price[0], dep.Asks.Size[0]
			log.Info(fmt.Sprintf("OnWsDepth %v %v, %v,, %v, %v", dep.TimeStamp, bp1, bm1, ap1, am1))
		},
	}
}
```
As above, you can subscribe to the required data through `WsSubs`. The value can be left empty by default, or use `_cur_` to represent the current variety. You can also use other varieties or subscribe to multiple varieties simultaneously, such as `BTC,_cur_`. Multiple varieties should be separated by commas.

In these three high-frequency callbacks, opening and closing positions are not supported by default. If needed, you can follow the example code above and call the current job’s `ProcessRuntimeOrders` and check both the error and bound.

## HTTP Post Interface Callback
You can initiate a callback to your strategy through an HTTP POST request from an external source. This is only supported for live trading and simulated live trading, and it will only take effect when you enable the `api_server`.

You can call this interface to trigger a strategy callback when a signal appears in platforms like TradingView or Python.

To support executing some logic when receiving a POST request, you need to first configure the `OnPostApi` callback function in the strategy:
```go
func PostApi(p *config.RunPolicyConfig) *strat.TradeStrat {
    return &strat.TradeStrat{
        OnBar: func(s *strat.StratJob) {
            // do nothing
        },
        OnPostApi: func(client *core.ApiClient, msg map[string]interface{}, jobs map[string]map[string]*strat.StratJob) error {
			// You can parse the received request by yourself. The following is just an example for reference.
            action, _ := msg["action"].(string)
            for acc, pairMap := range jobs {
                for pairTF, job := range pairMap {
                    if action == "openLong" {
                        log.Info("open long from api", zap.String("acc", acc), zap.String("pairTF", pairTF))
                        job.OpenOrder(&strat.EnterReq{
                            Tag: "long",
                        })
                    } else {
                        log.Warn("unknown action", zap.String("action", action))
                    }
                }
            }
            return nil
        },
    }
}
```
Then you can refer to [Live Trading](live_trading.md) to configure and start the robot. Note that you need to configure the login password in the yml and enable it:
```yaml
api_server:  # For controlling the robot through the api from an external source
  enable: true
  bind_ip: 0.0.0.0
  port: 8001
  users:
    - user: api
      pwd: very_strong_password
      allow_ips: []
      acc_roles: 
        user1: admin
```
Then make a POST request to the api at http://127.0.0.1:8001/api/strat_call
```json
{
    "token": "very_strong_password",
    "strategy": "ma:postApi",
    "action": "openLong",
    "data1": 123,
    "data2": "hello"
}
``` 
## All Member Functions of StratJob
**CanOpen(short bool) bool**  
Determine whether it is currently allowed to open an order (pass `true` to check if short selling is allowed, `false` to check if long buying is allowed).

**OpenOrder(req \*EnterReq) \*errs.Error**  
Send a request to open an order. If it fails, return an error message.

**CloseOrders(req \*ExitReq) \*errs.Error**  
Send a request to close an order, which can close one or multiple orders. If it fails, return an error message.

**GetOrderNum(dirt float64) int**  
Return the number of orders for the current strategy task. The parameter `dirt` can be `core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`.

**GetOrders(dirt float64) []\*ormo.InOutOrder**  
Return all orders for the current strategy task in the specified direction. The parameter `dirt` can be `core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`.

**Position(dirt float64, enterTag string) float64**  
Get the position size of the current strategy task, returning a multiple of the base amount. For example, if the default order amount is 30U, a return value of 2 indicates that 60U has been spent on orders.

**SetAllStopLoss(dirt float64, args \*ormo.ExitTrigger) \*errs.Error**
Set stop-loss for all orders in the specified direction for the current strategy task. The parameter `dirt` can be `core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`.

**SetAllTakeProfit(dirt float64, args \*ormo.ExitTrigger) \*errs.Error**
Set take-profit for all orders in the specified direction for the current strategy task. The parameter `dirt` can be `core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`.

**PositionAvgPrice(dirt float64, enterTag string) float64**  
Return the average entry price of the current task's positions in the given direction (and entry tag).

**AccountEquity() float64 / AvailableBalance() float64 / AccountDrawdown() float64**  
Account equity, available balance and drawdown ratio; see [Position sizing by risk](#risk_rate).

**RefKlineSub(symbol, timeframe string, warmupNum ...int) \*DataSub / RefEnv(symbol, timeframe string) \*ta.BarEnv / Data(sub \*DataSub) \*DataFields**  
Reference-data subscription, environment and data view; see [Reference data](#ref_kline).

**OpeningRange / SessionRange / PrevDayOHLC / PrevNDaysStats / DualThrustBands / RBreakerLevels**  
Intraday session statistics; see [Intraday session helpers](#intraday).

**ProcessRuntimeOrders() (entered, exited []\*ormo.InOutOrder, err \*errs.Error, bound bool)**  
Explicitly submit the open/close requests queued by the current task; see [Send an entry signal](#send_entry).

**ExecutionSnapshot() ExecutionSnapshot**  
Return a snapshot of the current task's orders and counts; order pointers are borrowed, so do not hold them across events.

**IsWarmUpState() bool**  
Whether the task is currently warming up; indicators may be updated during warmup, but open/close requests are not enqueued.

## Factor and cross-sectional engine

TradeStrat/OnBar belongs to time_series. Factors register native Plan/ComboSpec through runner.RegisterDefinition or declare multiple expressions outputs instead of per-symbol callbacks. Portfolio builders consume frozen Frame/Universe, never future labels. Mixed same-account policies declare id/account/capital_weight while state and lifecycle borrows remain independent.

See [Multi-factor strategies](./factor.md) and [Factor API](../api/factor.md).

## Runtime and callback state

Ordinary commands let entry create and close a task Runtime. Its configuration snapshot is read-only; accounts, clock, strategy jobs, prices and batch queue belong to that run. Embedded or concurrent tasks should use explicit Runtime dependencies instead of reading or writing package-level config/core/btime compatibility state inside callbacks. Job context and data events must come from the current task; live time and the advancing backtest clock are distinct.

`BatchInOut`/`BatchInfo` use the trader’s `BatchState`. All backtests order ready batches and jobs deterministically; live execution does not promise map order. Factor decisions freeze the entire Universe and produce target portfolios, which is a separate contract from time-series batch callbacks. See [Runtime API](../api/runtime.md) and the [factor guide](./factor.md).
