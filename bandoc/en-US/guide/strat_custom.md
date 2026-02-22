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
    other1 := pol.More["other1"].(string)
    other2 := pol.More["other2"].(int)
    other3 := pol.More["other3"].(float64)
    return &strat.TradeStrat{
		// more
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
The same strategy can appear in multiple `run_poolicy` items, that is, the same strategy function may be executed multiple times.
The `*strat.TradeStrat` returned by the strategy function will be applied to multiple varieties, corresponding to multiple `*strat.StratJob`.
Therefore, any variables related to a single symbol should not be saved in the strategy function, but should be saved through `*strat.StratJob.More`.
The variables in the strategy function should remain unchanged. Otherwise, it will lead to unexpected states.
:::

## strat.TradeStrat Strategy Object

`TradeStrat` provides a wealth of property configurations and callback functions. The most commonly used is `OnBar`.

The following is the complete definition of `TradeStrat`:
```go
type TradeStrat struct {
	Name          string  // The strategy name does not need to be set and will be automatically set to the name used during registration
	Version       int     
	WarmupNum     int     // candle preheating length
	MinTfScore    float64 // Minimum time cycle quality, default 0.75
    WsSubs        map[string]string    // websocket subscription: core.WsSubKLine, core.WsSubTrade, core.WsSubDepth
	DrawDownExit  bool    // Whether to enable retracement stop loss (i.e. trailing stop loss)
    HedgeOff      bool    // turn off future hedge mode
	BatchInOut    bool    // Whether to batch execute entry/exit
	BatchInfo     bool    // whether to perform batch processing after OninfoBar
	StakeRate     float64 // Relative basic amount billing rate
    StopLoss      float64 // Default stop loss rate for all orders opened by this strategy (without leverage).
	StopEnterBars int     // If the limit order exceeds the given K line and still does not enter the market, it will be cancelled
	EachMaxLong   int      // max number of long open orders for one pair
	EachMaxShort  int      // max number of short open orders for one pair
	AllowTFs      []string // Allow running time period, use global configuration when not provided
	Outputs       []string // The content of the text file output by the strategy, where each string is one line
	Policy        *config.RunPolicyConfig

	OnPairInfos         func(s *StratJob) []*PairSub                        // The strategy requires additional data on other types or periods
	OnStartUp           func(s *StratJob)                                   // Callback at startup. Called before the first execution
	OnBar               func(s *StratJob)                                   // Callback function for each K line
	OnInfoBar           func(s *StratJob, e *ta.BarEnv, pair, tf string)   // Other dependent bar data
    OnWsTrades          func(s *StratJob, pair string, trades []*banexg.Trade) // Websocket public trade data
    OnWsDepth           func(s *StratJob, dep *banexg.OrderBook)               // Websocket order book
    OnWsKline           func(s *StratJob, pair string, k *banexg.Kline)        // websocket real-time kline(may unfinish)
	OnBatchJobs         func(jobs []*StratJob)                             // All target jobs at the current time, used for bulk opening/closing of orders
	OnBatchInfos        func(tf string, jobs map[string]*JobEnv)            // All info marked jobs at the current time, used for batch processing
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
> You can use indicators in any callback function, but please note: **all indicators must be registered and called once in OnBar first**; otherwise, indicator calculations will result in errors.

## banta: Technical Analysis Library
The high-performance indicator library [banta](https://github.com/banbox/banta) is used in banbot. It caches the calculation state of indicators for each bar, which is the key to the high performance of banbot. You can visit [DeepWiki](https://deepwiki.com/banbox/banta) to learn more information about banta.

`banta.BarEnv` and `banta.Series` are two key structures in banta.

`banta.BarEnv` is the operating environment of a technical indicator, which stores information such as the current exchange, market, symbol, time period, etc.
A strategy task will require at least one `banta.BarEnv`. If other symbols or time periods are subscribed through `OnPairInfos`, multiple operating environments will be required.
But it should be noted that only one operating environment `BarEnv` will be created for each exchange, each market, each symbol, and each time period.

`banta.BarEnv` has several built-in original `Series`, namely: `Open`, `High`, `Low`, `Close`, `Volume`, which respectively save the sequence information of opening price, highest price, lowest price, closing price, and trading volume.

`Series` is a special structure in `banta` for storing serialized data. All technical indicator functions in `banta` receive one or more `Series`, and return a `Series` after calculation.

You can use `Series.Get(0)` to get the latest value of a series, such as `e.Close.Get(0)` to get the latest closing price;
You can also use `Series.Range(0,5)` to get the 5 most recent values ​​of a series, which returns a `[]float64` array of length 5.

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
func Highest(obj *Series, period int) *Series {
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
Subs  map[string]map[int]*Series
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

func Sum(obj *Series, period int) *Series {
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

This strategy can be configured in the YML file to run on a `5m` small timeframe, while internally it specifies the subscription to `1h` large timeframe data. In `OnPairInfos`, `_cur_` represents the current trading pair. If other trading pairs are needed, you can hardcode the value in this section.

::: tip Tip
`OnInfoBar` is solely intended for the consumption of additional candlestick data. Please refrain from executing logic related to opening/closing positions or updating stop-loss orders within this function.
:::

## Save temp variables

Sometimes you may need to save some temp variables for the next calculation; or you may need to synchronize some information between different callback functions.

As mentioned in the [previous section](#info_bar), if you need to use the information from `OnInfoBar` within `OnBar`, it must be relayed through `StratJob.More`.

First, initialize `StratJob.More` in `OnStartUp`.
Then perform type conversion in `OnBar` and `OnInfoBar` and assign it to the `m` variable:
```go
m, _ := s.More.(*Demo2Sta)
```

## Send an entry signal

To Send an entry signal, you only need to call the `StratJob.OpenOrder` method and pass a `*strat.EnterReq` parameter.

The simplest case is to only assign a value to the `Tag` property of `EnterReq`, and use the default parameters for all others. That is, open a long order according to the default order amount and leverage multiple. There is no stop profit or stop loss.

The following are all the parameters of `EnterReq`:
```go
type EnterReq struct {
	Tag             string  // Entry signal
	StgyName        string  // Strategy Name
	Short           bool    // Whether to short sell
	OrderType       int     // Order type, core.OrderType*
	Limit           float64 // Limit order entry price, submitted as limit order when specified
    Stop            float64 // Stop price, buy orders enter when price rises to trigger, sell when falls.
	CostRate        float64 // Opening ratio, default 1 times. Used for calculating LegalCost
	LegalCost       float64 // Spend amount in fiat. Ignores CostRate when specified
	Leverage        float64 // Leverage ratio
	Amount          float64 // Amount of target asset, calculated from LegalCost or CostRate
	StopLossVal     float64 // Distance from entry price to stop loss price, used to calculate StopLoss
	StopLoss        float64 // Stop loss trigger price. Submits stop loss order if not empty
	StopLossLimit   float64 // Stop loss limit price. If not provided, uses StopLoss
	StopLossRate    float64 // Stop loss exit ratio. 0 means 100%. Must be (0,1]
	StopLossTag     string  // Reason for Stop Loss
	TakeProfitVal   float64 // Distance from entry price to take profit price, used to calculate TakeProfit
	TakeProfit      float64 // Take profit trigger price. Submits take profit order if not empty
	TakeProfitLimit float64 // Take profit limit price. If not provided, uses TakeProfit
	TakeProfitRate  float64 // Take profit exit ratio. 0 means 100%. Must be (0,1]
	TakeProfitTag   string  // Reason for profit taking
	StopBars        int     // Cancel entry limit order if not filled after N bars
    ClientID        string  // Used to set the trailing part of the ClientOrderID submitted to the exchange.
    Infos           map[string]string // Used to set additional information saved in the order.
	Log             bool // Whether to automatically log error messages
}
```
To trigger an entry at a specific price, you can set the `Limit` or `Stop` parameters.

`Limit` would put a limit order. For a long order, you can set a lower price to enter the position. For a short order, you can set a higher price to enter the position.

`Stop` would put a trigger order (stop order). For a long order, you can set a higher price to enter after a breakout. For a short order, you can set a lower price to enter after a breakdown.

For example, if the current price is 100 and you want to enter a long position with a limit price of 122 when the price rises to 120, you can set `{Stop: 120, Limit: 122}`.

When `OpenOrder` returns an error, it indicates that opening the order failed. You can use `log.Error` to record error information, or set `Log` to true in `EnterReq` to let the system automatically log error messages.

:::tip tip
Calls to `OpenOrder` do not guarantee order execution at arbitrary positions within the strategy. By default, opening and closing positions will only be executed immediately when called within the following callback functions: `OnBar`, `OnOrderChange`, `OnBatchJobs`, and `OnPostApi`.

If you need to submit enter order immediately in other callbacks as well, you can call `_, _, err := biz.GetOdMgr(s.Account).ProcessOrders(nil, s)` on your own to submit and process the order.

Note that `OnOrderChange` by default only processes open/close signals for the current symbol. If you need to trigger open/close positions for other symbols, you need to manually call `ProcessOrders(nil, job)` for other jobs.
:::
## Send an exit signal

To send an exit signal, just call the `StratJob.CloseOrders` method and pass a `*strat.ExitReq` parameter.

The simplest case is to only assign a value to the `Tag` property of `ExitReq` and use the default parameters for all other parameters. That is, close all orders with market orders, including long orders and short orders.

The following are all the parameters of `ExitReq`:
```go
type ExitReq struct {
	Tag        string  // Exit signal
	StgyName   string  // Strategy Name
	EnterTag   string  // Only exit orders with EnterTag as the entry signal
	Dirt       int     // core.OdDirt* long/short/both
	OrderType  int     // Order type, core.OrderType*
	Limit      float64 // Limit order exit price. Submitted as limit order when specified
	ExitRate   float64 // Exit rate, default 100% (all orders)
	Amount     float64 // Amount to exit. ExitRate is invalid when specified
	OrderID    int64   // Only exit specified order
	UnOpenOnly bool    // If true, only exit orders that have not yet entered
	FilledOnly bool    // If true, only exit orders that have already entered
	Force      bool    // Whether to force exit
	Log        bool    // Whether to automatically log error messages
}
```

When `CloseOrders` returns an error, it indicates that closing the position failed. You can use `log.Error` to record error information, or set `Log` to true in `ExitReq` to let the system automatically log error messages.

For `ExitReq.Tag`, you can pass in a fixed string to distinguish different exit reasons. The `core` package has built-in common tags:

```go
const (
    ExitTagUnknown     = "unknown"      // Unknown reason
    ExitTagCancel      = "cancel"       // User cancel
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
    ExitTagTakeProfit  = "take_profit"  // Take profit triggered
    ExitTagDrawDown    = "draw_down"    // Drawdown stop loss
    ExitTagDataStuck   = "data_stuck"   // Auto close due to no K-line data for extended period
    ExitTagLiquidation = "liquidation"  // Forced liquidation
    ExitTagEnvEnd      = "env_end"      // Due to ex-dividend, main contract switch, etc.
    ExitTagEntExp      = "ent_expire"   // Entry limit order expired
    ExitTagExitDelay   = "exit_delay"   // Orders that should have exited earlier during live trading restart
)
```

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
    Limit: ma5Val * 0.975,
    Rate:  0.5,
    Tag:   "stop half",
})
```
For all open long orders, a stop loss of 50% of the position is set. When the price triggers 0.97 times the 5-period moving average, half of the position is closed with a limit order stop loss of 0.975 times.

## Batch task processing
Sometimes you may need to perform some calculations (such as correlation coefficients) for all symbols of the current strategy together, get some intermediate states to save, or open or close orders together.
In this case, you can use the `OnBatchJobs` or `OnBatchInfos` callback function.
> Note that the jobs parameter of OnBatchJobs is obtained from a map, so the order is not guaranteed.
```go
func calcCorrs(jobs []*strat.StratJob, isBig bool) {
	// Calculate the average correlation coefficient between each symbol and other varieties, and save it to More
	dataArr := make([][]float64, 0, len(jobs))
	for _, j := range jobs {
		dataArr = append(dataArr, j.Env.Close.Range(0, 70))
	}
	_, arr, err := utils.CalcCorrMat(dataArr, true)
	if err != nil {
		log.Error("calc corr mat fail", zap.Error(err))
		return
	}
	for i, j := range jobs {
		m, _ := j.More.(*BatchSta)
		if isBig {
			m.bigCorr = arr[i]
		} else {
			m.smlCorr = arr[i]
		}
	}
}

type BatchSta struct {
	smlCorr float64
	bigCorr float64
}

func BatchDemo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		WarmupNum:  100,
		BatchInOut: true,
		BatchInfo:  true,
		OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
			return []*strat.PairSub{
				{"_cur_", "1h", 100},
			}
		},
		OnStartUp: func(s *strat.StratJob) {
			s.More = &BatchSta{}
		},
		OnBar: func(s *strat.StratJob) {
			m, _ := s.More.(*BatchSta)
			if m.bigCorr < 0.5 && m.smlCorr < 0.5 {
				// When the correlation between the large and small cycles is less than 50%, the order will be opened.
				s.OpenOrder(&strat.EnterReq{Tag: "open"})
			} else if m.smlCorr > 0.9 {
				// The current small cycle correlation of the symbol is higher than 90%, close the position
				s.CloseOrders(&strat.ExitReq{Tag: "close"})
			}
		},
		OnBatchJobs: func(jobs []*strat.StratJob) {
			if jobs[0].IsWarmUp {
				return
			}
			calcCorrs(jobs, false)
		},
		OnBatchInfos: func(tf string, jobs map[string]*JobEnv) {
			jobList := utils.ValsOfMap(jobs)
			if jobList[0].IsWarmUp {
				return
			}
			calcCorrs(jobList, true)
		},
	}
}
```
> Note that all indicators used in OnBatchJobs or OnBatchInfos must also be called once in OnBar; otherwise, the indicator calculation will be incorrect.

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
You can directly specify the `pairs` symbol list in the yml or perform dynamic filtering through `pairlist`. However, you can also listen to the symbol list and modify the return through the `OnSymbols` callback function in the strategy:
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
// Add/Remove pairs and close positions
s.Strat.UpdatePairs(strat.PairUpdateReq{
	Add:    []string{"BTC", "ETH"},
	Remove:        []string{s.Symbol.Symbol},
	CloseOnRemove: true,
})
```

**Parameters:**
- `Add`: List of pairs to add
- `Remove`: List of pairs to remove
- `CloseOnRemove`: Whether to close orders when removing
- `ForceAdd`: Force add (bypass pairlist filters)
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
You can subscribe to high-frequency data pushed by exchanges via Websocket in the strategy, including: incomplete K-lines, tick-by-tick trades, and order book depth. Supports both live trading and backtesting.

**Note**: There is approximately a 1ms delay from the spider sending data to the robot process. Additionally, there is a certain delay from the exchange to your local system depending on network conditions.

Here is an example:
```go
import (
	"fmt"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
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
            _, _, err := biz.GetOdMgr(s.Account).ProcessOrders(nil, s)
            if err != nil {
                log.Error("process order fail", zap.Error(err))
            }
		},
		OnWsTrades: func(s *strat.StratJob, pair string, trades []*banexg.Trade) {
			last := trades[len(trades)-1]
			log.Info(fmt.Sprintf("OnWsTrades %v %v, %v", last.Timestamp, last.Price, last.Amount))
		},
		OnWsDepth: func(s *strat.StratJob, dep *banexg.OrderBook) {
			bp1, bm1 := dep.Bids.Price[0], dep.Bids.Size[0]
			ap1, am1 := dep.Asks.Price[0], dep.Asks.Size[0]
			log.Info(fmt.Sprintf("OnWsDepth %v %v, %v,, %v, %v", dep.TimeStamp, bp1, bm1, ap1, am1))
		},
	}
}
```
As above, you can subscribe to the required data through `WsSubs`. The value can be left empty by default, or use `_cur_` to represent the current variety. You can also use other varieties or subscribe to multiple varieties simultaneously, such as `BTC,_cur_`. Multiple varieties should be separated by commas.

In these three high-frequency callbacks, opening and closing positions are not supported by default. If needed, you can follow the example code above and call `ProcessOrders` on your own to submit and process orders.

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
            action := utils.PopMapVal(msg, "action", "")
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
    "token": "very_strong_password", // This is the password in the api_server
    "strategy": "ma:postApi", // This is the name of the requested strategy
    // The first two are fixed and required. The following fields are optional, and you can parse them in the strategy by yourself.
    "action": "openLong",
    "data1": 123, // Other arbitrary data to be sent to the strategy
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

**SetAllStopLoss(dirt float64, args \*ormo.ExitTrigger)**  
Set stop-loss for all orders in the specified direction for the current strategy task. The parameter `dirt` can be `core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`.

**SetAllTakeProfit(dirt float64, args \*ormo.ExitTrigger)**  
Set take-profit for all orders in the specified direction for the current strategy task. The parameter `dirt` can be `core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`.
