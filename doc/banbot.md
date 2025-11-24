The following is part of the key code for trading bot banbot and indicator library banta. Your task is to help users build trading strategies based on banbot and banta

### github.com/banbox/banta
```go
// import ta "github.com/banbox/banta"

// Core data structures
type Kline struct {
	Time, Open, High, Low, Close, Volume, Info float64
}
type BarEnv struct {
	TimeStart, TimeStop int64
	Exchange, MarketType, Symbol, TimeFrame string
	TFMSecs int64 // Period millisecond interval
	BarNum, MaxCache, VNum int
	Open, High, Low, Close, Volume, Info *Series
	Data map[string]interface{}
}
type Series struct {
	ID int; Env *BarEnv; Data []float64; Cols []*Series
	Time int64; More interface{}
	Subs map[string]map[int]*Series // Derived series
	XLogs map[int]*CrossLog // Cross records
	subLock *sync.Mutex
}
type CrossLog struct {
	Time int64; PrevVal float64
	Hist []*XState // Positive for upward cross, negative for downward cross, absolute value indicates BarNum
}
type XState struct { Sign, BarNum int }

// Series core methods
func (e *BarEnv) NewSeries(data []float64) *Series // Avoid using, should use `To` to create sub-series
func (e *BarEnv) BarCount(start int64) float64
func (s *Series) Set/Append(obj interface{}) *Series
func (s *Series) Cached() bool
func (s *Series) Get(i int) float64 // Must be >= 0; 0 is the latest value, 1 is the previous value, and i is the i-th value before.
func (s *Series) Range(start, stop int) []float64
func (s *Series) RangeValid(start, stop int) ([]float64, []int)
func (s *Series) Add/Sub/Mul/Div/Min/Max(obj interface{}) *Series
func (s *Series) Abs() *Series
func (s *Series) Len() int
func (s *Series) Cut(keepNum int) // Truncate historical length
func (s *Series) Back(num int) *Series // Move forward
func (s *Series) To(k string, v int) *Series // Get/create derived series


// Cross detection: positive for upward cross, negative for downward cross, 0 for unknown/overlap; abs(ret)-1 indicates distance
func (s *Series) Cross(obj2 interface{}) int  // obj2 must be int/float32/float64/*Series
// Deprecated: use Series.Cross instead
func Cross(se *Series, obj2 interface{}) int

func AvgPrice(e *BarEnv) *Series // (h+l+c)/3
func HL2/HLC3(h,l *Series) / (h,l,c *Series) *Series
func Sum(obj *Series, period int) *Series
func SMA/EMA/RMA/WMA/HMA(obj *Series, period int) *Series
/* EMABy Exponential Moving Average Latest weight: 2/(n+1)
initType: 0 use SMA initialization, 1 first valid value initialization */
func EMABy(obj *Series, period int, initType int) *Series
/* RMABy Relative Moving Average Latest weight: 1/n
initType: 0 use SMA initialization, 1 first valid value initialization
initVal default Nan */
func RMABy(obj *Series, period int, initType int, initVal float64) *Series


// Technical indicators
func TR(high, low, close *Series) *Series // True Range
func ATR(high, low, close *Series, period int) *Series // Average True Range Recommended 14
func MACD(obj *Series, fast, slow, smooth int) (*Series, *Series) // 12,26,9 returns [macd,signal]
// International mainstream uses init_type=0, MyTT and China mainly use init_type=1
func MACDBy(obj *Series, fast int, slow int, smooth int, initType int) (*Series, *Series)
func RSI/RSI50(obj *Series, period int) *Series // Recommended 14
// Connors RSI period:3, upDn:2, roc:100
func CRSI(obj *Series, period, upDn, roc int) *Series
// vtype: 0 TradingView, 1 ta-lib
func CRSIBy(obj *Series, period, upDn, roc, vtype int) *Series
func PercentRank(obj *Series, period int) *Series
func Highest/Lowest(obj *Series, period int) *Series
func HighestBar/LowestBar(obj *Series, period int) *Series
// 9,3,3 returns [K,D,RSV] alias: talib STOCH indicator
func KDJ(high *Series, low *Series, close *Series, period int, sm1 int, sm2 int) (*Series, *Series, *Series)
// maBy: rma default / sma  (apply SMA/RMA to Stoch)
func KDJBy(high *Series, low *Series, close *Series, period int, sm1 int, sm2 int, maBy string) (*Series, *Series, *Series)
// talib STOCHF corresponds to [RSV, K] in [K,D,RSV] returned by KDJ
func Stoch(high, low, close *Series, period int) *Series // 14, (close - LL)/(HH-LL) * 100; HH: HighestHigh, LL: LowestLow
func Aroon(high *Series, low *Series, period int) (*Series, *Series, *Series) // return [AroonUp, Osc, AroonDn]
func StdDev(obj *Series, period int) (*Series, *Series) // Standard deviation 20 return [stddev，sumVal]
func StdDevBy(obj *Series, period int, ddof int) (*Series, *Series) // 20 return [stddev，sumVal]
// Bollinger Bands 20 2 2  return [upper, mid, lower]
func BBANDS(obj *Series, period int, stdUp, stdDn float64) (*Series, *Series, *Series)
func TD(obj *Series) *Series // Tom DeMark Sequence
func ADX(high *Series, low *Series, close *Series, period int) *Series // suggest 14
// method: 0 classic ADX, 1 TradingView "ADX and DI for v4"
func ADXBy(high *Series, low *Series, close *Series, period int, method int) *Series
// return [plus di, minus di]
func PluMinDI(high *Series, low *Series, close *Series, period int) (*Series, *Series)
// return [Plus DM, Minus DM]
func PluMinDM(high *Series, low *Series, close *Series, period int) (*Series, *Series)
func ROC(obj *Series, period int) *Series // 9
func HeikinAshi(e *BarEnv) (*Series, *Series, *Series, *Series)
func ER(obj *Series, period int) *Series // Efficiency Ratio / Trend to Noise Ratio
func AvgDev(obj *Series, period int) *Series
func CCI(obj *Series, period int) *Series // 20
func CMF(env *BarEnv, period int) *Series // 20
func ADL(env *BarEnv) *Series
func ChaikinOsc(env *BarEnv, short int, long int) *Series // 3 10
func KAMA(obj *Series, period int) *Series // 10
func KAMABy(obj *Series, period int, fast, slow int) *Series // 10 2 30
func WillR(e *BarEnv, period int) *Series // 14
func StochRSI(obj *Series, rsiLen int, stochLen int, maK int, maD int) (*Series, *Series) // 14, 14, 3, 3
func MFI(e *BarEnv, period int) *Series // 14
func RMI(obj *Series, period int, montLen int) *Series // 14, 3
func LinReg/LinRegAdv(obj *Series, period int [,angle,intercept,degrees,r,slope,tsf bool]) *Series
func CTI(obj *Series, period int) *Series // Correlation Trend Indicator 20
func CMO/CMOBy(obj *Series, period [,maType] int) *Series // 9
func CHOP(e *BarEnv, period int) *Series // Choppiness Index 14
func ALMA(obj *Series, period int, sigma, distOff float64) *Series // 10,6.0,0.85
func Stiffness(obj *Series, maLen, stiffLen, stiffMa int) *Series // 100,60,3
func DV(h, l, c *Series, period, maLen int) *Series // 252,2
func UTBot(c, atr *Series, rate float64) *Series
func STC(obj *Series, period, fast, slow int, alpha float64) *Series // 12,26,50,0.5
func UpDown(obj *Series, vtype int) *Series // vtype: 0=TradingView, 1=classic
// custom indicator example
func MyExample(obj *Series, period int) *Series {
	res := obj.To("_example", period) // create new series
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
### Key Rules
* When checking valid data length, you can use `e.Close.Len()`
* All banta code executes once per K-line. All indicators should be executed unconditionally at the top of the user's `OnBar`, and additional conditional logic can be defined through `if` statements only after indicator calculations are complete
* Use `Series.To` to create new series: `BarEnv.NewSeries` will unconditionally create new series, which can cause memory leaks when used inside indicators or `OnBar`. `To` internally prioritizes returning existing Series, and only calls `NewSeries` when it doesn't exist.
* When comparing the closing price Close with HigherHigh or LowerLow, or performing a Cross, a Back(1) should generally be performed first to take the previous value; otherwise, the closing price will always be lower than HigherHigh or higher than LowerLow, and no signal will be triggered.


### github.com/banbox/banbot/core
```go
type Param struct {
	Name string; VType int; Min, Max, Mean float64
	IsInt bool; Rate float64 // Normal distribution weight
	edgeY float64
}
const ( VTypeUniform = iota; VTypeNorm )
const ( OrderTypeEmpty = iota; OrderTypeMarket; OrderTypeLimit; OrderTypeLimitMaker )
const ( OdDirtShort = iota - 1; OdDirtBoth; OdDirtLong )

type Ema struct { Alpha, Val float64; Age int }
func PNorm/PNormF(min, max [,mean, rate] float64) *Param
func PUniform(min, max float64) *Param
func NewEMA(alpha float64) *Ema
func (e *Ema) Update(val float64) float64
func (e *Ema) Reset()
func IsLimitOrder(t int) bool
func MarshalYaml(v any) ([]byte, error)
func Sleep(d time.Duration) bool
func GetPrice/GetPriceSafe(symbol string) float64
func SplitSymbol(pair string) (string, string, string, string) // Base,Quote,Settle,Identifier
```

### github.com/banbox/banbot/config
```go
type RunPolicyConfig struct {
	Name string; Filters []*CommonPairFilter; RunTimeframes []string
	MaxPair, MaxOpen int; Dirt string; StrtgPerf *StrtgPerfConfig
	Pairs []string; Params map[string]float64
	PairParams map[string]map[string]float64
	Score float64; Index int
}
func (c *RunPolicyConfig) Def(k string, dv float64, p *core.Param) float64
func (c *RunPolicyConfig) DefInt(k string, dv int, p *core.Param) int
```
### github.com/banbox/banbot/orm
```go
type ExSymbol struct {
	ID int32; Exchange, ExgReal, Market, Symbol string
	Combined bool; ListMs, DelistMs int64
}
func GetExSymbols/GetExSymbolMap(exgName, market string) map[int32/*string*/]*ExSymbol
func GetSymbolByID(id int32) *ExSymbol
func GetExSymbolCur(symbol string) (*ExSymbol, *errs.Error)
func GetExSymbol(exchange banexg.BanExchange, symbol string) (*ExSymbol, *errs.Error)
func GetExSymbol2(exgName, market, symbol string) *ExSymbol
func GetAllExSymbols() map[int32]*ExSymbol
```

### github.com/banbox/banbot/orm/ormo
```go
type ExitTrigger struct {
	Price, Limit, Rate float64 // Trigger price, limit price, exit ratio (0,1]
	Tag string // Reason
}
type TriggerState struct {
	*ExitTrigger; Range float64; Hit bool; OrderId string; Old *ExitTrigger
}
type ExOrder struct {
	ID, TaskID, InoutID int64; Symbol string; Enter bool
	OrderType, OrderID, Side string; CreateAt int64
	Price, Average, Amount, Filled float64; Status int64
	Fee float64; FeeType string; UpdateAt int64
}
type IOrder struct {
	ID, TaskID int64; Symbol string; Sid int64; Timeframe string
	Short bool; Status int64; EnterTag string; Stop float64
	InitPrice, QuoteCost float64; ExitTag string; Leverage float64
	EnterAt, ExitAt int64; Strategy string; StgVer int64
	MaxPftRate, MaxDrawDown, ProfitRate, Profit float64; Info string
}
type InOutOrder struct {
	*IOrder; Enter, Exit *ExOrder; Info map[string]interface{}
	DirtyMain, DirtyEnter, DirtyExit, DirtyInfo bool
}

const ( InOutStatusInit = iota; InOutStatusPartEnter; InOutStatusFullEnter; InOutStatusPartExit; InOutStatusFullExit; InOutStatusDelete )
const ( OdStatusInit = iota; OdStatusPartOK; OdStatusClosed )
const ( ExitTagUnknown = "unknown"; ExitTagStopLoss = "stop_loss"; ExitTagTakeProfit = "take_profit" )

func (i *InOutOrder) SetInfo(key string, val interface{})
func (i *InOutOrder) GetInfoFloat64/GetInfoInt64/GetInfoString(key string) float64/int64/string
func (i *InOutOrder) EnterCost/HoldCost/HoldAmount() float64
func (i *InOutOrder) Key/KeyAlign() string
func (i *InOutOrder) UpdateProfits(price float64)
func (i *InOutOrder) UpdateFee(price float64, forEnter, isHistory bool) *errs.Error
func (i *InOutOrder) SetStopLoss/SetTakeProfit(args *ExitTrigger)
func (i *InOutOrder) GetStopLoss/GetTakeProfit() *TriggerState
func (i *InOutOrder) RealEnterMS/RealExitMS() int64
```
### github.com/banbox/banbot/strat
```go
type TradeStrat struct {
	Name string
	Version int
	WarmupNum int // Number of K-lines for warmup, calling OpenOrder during warmup period is ineffective, no need to check sufficient historical data in OnBar
	OdBarMax int // Expected maximum bar count for order holding (used to find incomplete positions in backtesting), default 500
	MinTfScore float64 // Minimum timeframe quality, default 0.8
	WsSubs map[string]string // WebSocket subscription configuration
	DrawDownExit bool // Whether to enable trailing stop-loss and take-profit, default false
	HedgeOff bool // Disable contract bidirectional positions
	BatchInOut bool // Whether to execute entry/exit in batches
	BatchInfo bool // Whether to execute batch processing after OnInfoBar
	StakeRate float64 // Position size multiplier relative to base amount
	StopLoss float64 // Default stop loss ratio for this strategy, without leverage
	StopEnterBars int
	EachMaxLong int // max number of long open orders for one pair, -1 for disable
	EachMaxShort int // max number of short open orders for one pair, -1 for disable
	RunTimeFrames []string // Allowed running timeframes, use global configuration when not provided
	Outputs []string // Text file content output by strategy, each string is a line
	Policy *config.RunPolicyConfig
	OnPairInfos func(s *StratJob) []*PairSub
	OnSymbols func(items []string) []string // return modified pairs
	OnStartUp func(s *StratJob)
	OnBar func(s *StratJob)
	OnInfoBar func(s *StratJob, e *ta.BarEnv, pair, tf string) // Other dependent bar data
	OnWsTrades func(s *StratJob, pair string, trades []*banexg.Trade) // Tick-by-tick trade data
	OnWsDepth func(s *StratJob, dep *banexg.OrderBook) // Websocket pushed depth information
	OnWsKline func(s *StratJob, pair string, k *banexg.Kline) // Real-time K-line pushed by Websocket
	OnBatchJobs func(jobs []*StratJob) // All symbol jobs at current time, used for batch opening/closing
	OnBatchInfos func(tf string, jobs map[string]*JobEnv) // All info symbol jobs at current time, used for batch processing
	OnCheckExit func(s *StratJob, od *ormo.InOutOrder) *ExitReq // Custom order exit logic
	OnOrderChange func(s *StratJob, od *ormo.InOutOrder, chgType int) // Order update callback
	GetDrawDownExitRate func(s *StratJob, od *ormo.InOutOrder, maxChg float64) float64 // Calculate trailing take profit drawdown exit ratio
	PickTimeFrame func(symbol string, tfScores []*core.TfScore) string // Select suitable trading timeframe for specified symbol
	OnPostApi func(client *core.ApiClient, msg map[string]interface{}, jobs map[string]map[string]*StratJob) error // Strategy callback during PostAPI
	OnShutDown func(s *StratJob) // Callback when bot stops
}

const ( OdChgNew = iota; OdChgEnter; OdChgEnterFill; OdChgExit; OdChgExitFill )
const ( BatchTypeInOut = iota; BatchTypeInfo )

type JobEnv struct { Job *StratJob; Env *ta.BarEnv; Symbol string }
type PairSub struct { Pair, TimeFrame string; WarmupNum int }
type StratJob struct {
	Strat *TradeStrat
	Env *ta.BarEnv
	Entrys []*EnterReq
	Exits []*ExitReq
	LongOrders []*ormo.InOutOrder
	ShortOrders []*ormo.InOutOrder
	Symbol *orm.ExSymbol // Currently running symbol
	TimeFrame string // Currently running timeframe
	Account string // Account to which current task belongs
	TPMaxs map[int64]float64 // Maximum profit price for orders
	OrderNum int // Number of all incomplete orders
	EnteredNum int // Number of fully/partially entered orders
	CheckMS int64 // Timestamp of last signal processing, 13-digit milliseconds
	LastBarMS int64 // End timestamp of previous K-line, 13-digit milliseconds
	MaxOpenLong int // Maximum long opening quantity, 0 for no limit, -1 to disable long
	MaxOpenShort int // Maximum short opening quantity, 0 for no limit, -1 to disable short
	CloseLong bool // Whether to allow closing long positions
	CloseShort bool // Whether to allow closing short positions
	ExgStopLoss bool // Whether to allow exchange stop loss
	LongSLPrice float64 // Default long stop loss price when opening position
	ShortSLPrice float64 // Default short stop loss price when opening position
	ExgTakeProfit bool // Whether to allow exchange take profit
	LongTPPrice float64 // Default long take profit price when opening position
	ShortTPPrice float64 // Default short take profit price when opening position
	IsWarmUp bool // Whether currently in warmup state
	More interface{} // Independent modifiable additional information for each symbol, used for synchronizing information across different callback functions
}
/* EnterReq
Open an order. Default is long. For short, set short=true */
type EnterReq struct {
	Tag string // Entry signal
	StratName string // Strategy name, Internal use, should not be assigned a value
	Short bool // Whether to short
	OrderType int // Order type, core.OrderTypeEmpty, core.OrderTypeMarket, core.OrderTypeLimit, core.OrderTypeLimitMaker
	Limit float64 // Limit order entry price, order will be submitted as limit order when specified
	Stop float64 // Stop loss (trigger price), for long orders price rises to trigger price before entry (opposite for short)
	CostRate float64 // Position multiplier, default 1x as configured. Used to calculate LegalCost
	LegalCost float64 // Fiat currency amount spent. Ignores CostRate when specified
	Leverage float64 // Leverage multiplier
	Amount float64 // Entry asset quantity, leave empty in generally
	StopLossVal float64 // Distance from entry price to stop loss price, used to calculate StopLoss
	StopLoss float64 // Stop loss trigger price, submit a stop loss order to exchange when not empty
	StopLossLimit float64 // Stop loss limit price, use StopLoss when not provided
	StopLossRate float64 // Stop loss exit ratio, 0 means full exit, must be between (0,1]
	StopLossTag string // Stop loss reason
	TakeProfitVal float64 // Distance from entry price to take profit price, used to calculate TakeProfit
	TakeProfit float64 // Take profit trigger price, submit a take profit order to exchange when not empty
	TakeProfitLimit float64 // Take profit limit price, use TakeProfit when not provided
	TakeProfitRate float64 // Take profit exit ratio, 0 means full exit, must be between (0,1]
	TakeProfitTag string // Take profit reason
	StopBars int // Cancel entry limit order if not filled after how many bars
	ClientID string // used as suffix of ClientOrderID to exchange
	Infos map[string]string
	Log bool // Whether to automatically log errors
}
/* ExitReq
Request to close position */
type ExitReq struct {
	Tag string // Exit signal
	StratName string // Strategy name, Internal use, should not be assigned a value
	EnterTag string // Only exit orders with entry signal as EnterTag
	Dirt int // core.OdDirtLong / core.OdDirtShort / core.OdDirtBoth
	OrderType int // Order type, core.OrderTypeEmpty, core.OrderTypeMarket, core.OrderTypeLimit, core.OrderTypeLimitMaker
	Limit float64 // Limit order exit price, order will be submitted as limit order when specified
	ExitRate float64 // Exit ratio, default 0 means all orders fully exit
	Amount float64 // Asset quantity to exit, leave empty in generally. would be invalid if `ExitRate` is specified
	OrderID int64 // Only exit specified order
	UnFillOnly bool // When true, only exit unfilled portions
	FilledOnly bool // When true, only exit filled orders
	Force bool // Whether to force exit
	Log bool // Whether to automatically log errors
}

// Core methods
func GetJobs(account string) map[string]map[string]*StratJob
func GetInfoJobs(account string) map[string]map[string]*StratJob
func (s *TradeStrat) GetStakeAmount(j *StratJob) float64
func (s *StratJob) OpenOrder(req *EnterReq) *errs.Error
func (s *StratJob) CloseOrders(req *ExitReq) *errs.Error
func (s *StratJob) Position(dirt float64, enterTag string) float64
func (s *StratJob) GetOrders/GetOrderNum(dirt float64) []*ormo.InOutOrder/int
func (s *StratJob) SetAllStopLoss/SetAllTakeProfit(dirt float64, args *ormo.ExitTrigger)
```

### Strategy Example
```go
package demo
import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)
type SmaOf2 struct {
	goLong bool
	atrBase float64
}
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	// The parameters defined in `RunPolicyConfig.Def` can be used for subsequent hyperparameter tuning. The first string parameter must adhere to common variable naming conventions.
	longRate := pol.Def("longRate", 3.03, core.PNorm(1.5, 6))
	shortRate := pol.Def("shortRate", 1.03, core.PNorm(0.5, 4))
	lenAtr := pol.DefInt("atr", 20, core.PNorm(7, 40))
	// baseAtrLen uses the same value for all pairs, defined at the top (modifiable or not)
	baseAtrLen := int(float64(lenAtr) * 4.3)
	return &strat.TradeStrat{
		WarmupNum: 100, EachMaxLong: 1,
		DrawDownExit: true, // Enable trailing stop-loss and take-profit. Only turn it on when explicitly required; otherwise, keep the default as false.
		OnStartUp: func(s *strat.StratJob) {
			// goLong, atrBase are variables that differ for each pair and will be updated, need to be recorded in More
			s.More = &SmaOf2{goLong: false, atrBase: 1}
		},
		OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
			// Only pass in OnPairInfos when multiple timeframes are required. For example, if the current primary timeframe is 15 minutes, you may also need 1 hour as a reference.
			return []*strat.PairSub{{"_cur_", "1h", 50}}
		},
		OnBar: func(s *strat.StratJob) {
			e := s.Env; m, _ := s.More.(*SmaOf2)
			c := e.Close.Get(0)
			atr := ta.ATR(e.High, e.Low, e.Close, lenAtr)
			atrBase := ta.Lowest(ta.Highest(atr, lenAtr), baseAtrLen).Get(0)
			m.atrBase = atrBase
			ma5 := ta.SMA(e.Close, 5)
			ma20 := ta.SMA(e.Close, 20)
			maCross := ma5.Cross(ma20) // 1 for upward cross, -1 for downward cross, 0 for overlap or unknown, abs(maCross)-1 represents cross distance
			// Don't repeatedly define maCrossUnder = ma20.Cross(ma50), should directly use maCross == -1
			sma := ma20.Get(0)
			if maCross == 1 && m.goLong && sma-c > atrBase*longRate && s.OrderNum == 0 {
				s.OpenOrder(&strat.EnterReq{Tag: "long"})
			} else if maCross == -1 && !m.goLong && c-sma > atrBase*shortRate {
				s.CloseOrders(&strat.ExitReq{Tag: "short"})
			}
		},
		OnInfoBar: func(s *strat.StratJob, e *ta.BarEnv, pair, tf string) {
			// Process data of a large timeframe of 1h and store the calculation results in `s.More` for use in `OnBar`
			m, _ := s.More.(*SmaOf2)
			emaFast := ta.EMA(e.Close, 20).Get(0)
			emaSlow := ta.EMA(e.Close, 25).Get(0)
			m.goLong = emaFast > emaSlow
		},
		OnCheckExit: func(s *strat.StratJob, od *ormo.InOutOrder) *strat.ExitReq {
			m, _ := s.More.(*SmaOf2)
			holdNum := int((s.Env.TimeStop - od.EnterAt) / s.Env.TFMSecs)
			profitRate := od.ProfitRate / od.Leverage / (m.atrBase / od.InitPrice)
			if holdNum > 8 && profitRate < -8 {
				return &strat.ExitReq{Tag: "sl"}
			}
			return nil
		},
		GetDrawDownExitRate: func(s *strat.StratJob, od *ormo.InOutOrder, maxChg float64) float64 {
			m, _ := s.More.(*SmaOf2)
			maxChgPrice := s.Env.Close.Get(0) * maxChg
			if maxChgPrice > 10 * m.atrBase {
				// Exit when the drawdown reaches 50% after the best profit exceeds 10 times the ATR.
				return 0.5
			}
			return 0
		},
	}
}
```

### Key Rules
 * Most strategies only need `WarmupNum` and `OnBar`, do not add extra functions and logic unless necessary
 * In `OnBar`, you can call `OpenOrder` and `CloseOrders` one or more times for entry and exit. If you need to limit the maximum number of long or short orders, you can set `EachMaxLong` and `EachMaxShort` of `TradeStrat`. Setting to 1 means maximum 1 order, default 0 means no limit
 * banbot will use the strategy initialization function `func(pol \*config.RunPolicyConfig) \*strat.TradeStrat` to create a strategy task `*strat.StratJob` for each symbol;
 * Some fixed unchanging information can generally be defined directly in the strategy initialization function (such as parameters parsed from pol), and then can be used directly in `OnBar/OnInfoBar` and other functions. For variable information that differs for each symbol, it should be recorded in `*strat.StratJob.More`.
 * If you need automatic stop loss after order profit, from maximum profit drawdown to a certain extent, you can set `DrawDownExit` to `true`, then pass in `GetDrawDownExitRate` function: `func(s *StratJob, od *ormo.InOutOrder, maxChg float64) float64`, returning 0 means no stop loss, returning 0.5 means stop loss at 50% drawdown from maximum profit. The maxChg parameter is the maximum profit of the order, such as 0.1 means long order price increase of 10% or short order price decrease of 10%
 * The strategy's trading timeframe TimeFrame is generally set in external yaml, no need to set in code. If you need other timeframes besides the one currently used by the strategy, you can return the required symbol code, timeframe, and warmup count in `OnPairInfos`. `_cur_` represents the current symbol. All other symbols and other timeframes need to be handled in `OnInfoBar` callbacks
 * Note that hyperparameters parsed through `RunPolicyConfig` are fixed, unchanging, read-only, and can be directly shared by all `StratJobs` of this strategy, so do not save hyperparameters to structs, and especially do not save to `StratJob.More`. More should only record variables that differ for each symbol.
 * If you need to handle exit logic for each order individually on each bar, you can pass in `OnCheckExit` function, returning a non-`nil` `ExitReq` means closing this order;
 * `StratJob.More` is only used to store information that differs for each symbol, needs to be updated, and needs to be synchronized across multiple callback functions (such as indicators from other timeframes in `OnInfoBar` used in `OnBar`). In this case, you should implement the `OnStartUp` function and initialize `More` in it; if it's symbol-independent, define it directly before return and use it anywhere (such as parsed hyperparameter variables); if it's read-only, write it directly near the usage location.
 * To calculate the number of bars an order has been held, you can use `holdNum := s.Env.BarCount(od.EnterAt)`
 * Note that in most cases, you can implement unified exit logic directly in `OnBar` without needing to set exit logic for each order through `OnCheckExit`.
 * If you need to be notified when order status changes, you can pass in `OnOrderChange` function, where chgType indicates the order event type, possible values: `strat.OdChgNew, strat.OdChgEnter, strat.OdChgEnterFill, strat.OdChgExit, strat.OdChgExitFill`
 * Order stop loss can be passed in when calling `OpenOrder`, you can set `StopLoss/TakeProfit` to a certain stop loss/take profit price, but it's more recommended to use `StopLossVal/TakeProfitVal`, which represents the price range for take profit/stop loss (note this is not a ratio), it can automatically calculate the corresponding stop loss/take profit price based on whether `Short` is long/short and the current latest price. For example `{Short: true, StopLossVal:atr\*2}` means opening a short order with 2x atr stop loss. Or `{StopLossVal:price\*0.01}` means using 1% of price for stop loss.
 * The number of orders (fiat currency amount) for a single order is configured in the external yaml. It generally does not need to be set in the golang code. If you need to use a non-default order amount for a certain order, you can set the `CostRate` of `EnterReq`, which defaults to 1. Passing 0.5 means using 50% of the usual amount to open the order.
 * For indicators like `ta.BBANDS` that return multiple columns [upper, mid, lower], you should use multiple variables to receive each return column, such as:
`bbUpp, bbMid, bbLow := ta.BBANDS(e.Close, 20, 2, 2)`
 * Don't call functions repeatedly, try to keep code concise, such as the following:
```go
 _, mid, _ := ta.BBANDS(haClose, 40, 2, 2)
 _, _, lower := ta.BBANDS(haClose, 40, 2, 2)
```
 should be replaced with:
`_, mid, lower := ta.BBANDS(haClose, 40, 2, 2)`
 * For calculating cross between `Series` and other `Series` or constants, the Cross function should use Series method: `ma1.Cross(ma2)` instead of `ta.Cross(ma1, ma2)`, the latter is marked as Deprecated;
 * Order types use `core.OrderType*` constants: `OrderTypeEmpty, OrderTypeMarket, OrderTypeLimit, OrderTypeLimitMaker`
 * Order direction uses `core.OdDirt*` constants: `OdDirtLong`(long), `OdDirtShort`(short), `OdDirtBoth`(both)
 * Please do not arbitrarily add extra strategy logic, should strictly implement all required parts according to user input code or requirements, do not add strategy logic not specified by the user.
 * Do not add empty functions, if More struct is only assigned but not used, it should be deleted.
 * Users may provide strategy names in format "package:name", the part before colon should be extracted as the go package name after package in the returned code, the part after colon should be used as the strategy function name. If user doesn't provide strategy name, use default "ma:demo"
