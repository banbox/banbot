The following interfaces and rules help convert strategy ideas or other code into banbot time-series, cross-sectional or multi-factor strategies. Cross-sectional and multi-factor strategies support YAML formulas or Go code.

> API baseline: Banbot v0.6.0-beta.8. Select the engine by strategy semantics; see “Cross-sectional and multi-factor strategies” below. New time-series code follows the arbitrary-series subscription and Runtime Context rules; legacy callbacks such as `OnBar` remain for compatibility. Configuration retains v0.5 key locations; see the [compatibility comparison](config_compatibility.md).

### github.com/banbox/banta
```go
// import ta "github.com/banbox/banta"

// Core data structures
type Kline struct {
	Time int64
	Open, High, Low, Close, Volume, Quote, BuyVolume float64
	TradeNum int64
}
type BarEnv struct {
	TimeStart, TimeStop int64
	Exchange, MarketType, Symbol, TimeFrame string
	TFMSecs int64 // Period millisecond interval
	BarNum, MaxCache, VNum int
	Open, High, Low, Close, Volume, Quote, BuyVolume, TradeNum *Series
	Data sync.Map; Items map[int]*Series; Lock sync.Mutex
}
func NewBarEnv(exgName, market, symbol, timeframe string) (*BarEnv, error)
func ParseTimeFrame(timeframe string) (int, error)
type Series struct {
	ID int; Env *BarEnv; Data []float64; Cols []*Series
	Time int64; More interface{}
	Subs map[string]map[int]*Series // Derived series
	XLogs map[int]*CrossLog // Cross records
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
func (s *Series) CrossUp(obj2 interface{}) bool; func (s *Series) CrossDown(obj2 interface{}) bool
func HLCC4(h,l,c *Series) *Series; func OHLC4(o,h,l,c *Series) *Series
func Sub(a,b *Series) *Series; func Abs(a *Series) *Series; func Change(a *Series) *Series
func ADX(high, low, close *Series, period int, smoothing ...int) *Series
func CCI(obj *Series, args ...interface{}) *Series // close,period or high,low,close,period
func DEMA(obj *Series, period int) *Series
func T3(obj *Series, period int) *Series
func SSF(obj *Series, period int) *Series
func TRIMA(obj *Series, period int) *Series
func VIDYA(obj *Series, period int) *Series
func ZLMA(obj *Series, period int) *Series
func SWMA(obj *Series) *Series
func MAMA(obj *Series, fast, slow float64) (*Series, *Series) // [MAMA, FAMA]
func MOM(obj *Series, period int) *Series
func AO(high, low *Series, fast, slow int) *Series
func DPO(obj *Series, period int) *Series
func Dpo(obj *Series, period int) *Series // alias of DPO
func StochF(high, low, close *Series, period int, smooth ...int) (*Series, *Series) // [fastK, fastD]
func STOCHF(close, high, low *Series, period int, smooth ...int) (*Series, *Series) // [fastK, fastD]
func ULTOSC(high, low, close *Series, short, medium, long int) *Series
func Fisher(high, low *Series, period int) *Series
func WilliamsPercent(high, low, close *Series, period int) *Series
func ROCR(obj *Series, period int) *Series
func TRIX(obj *Series, period int) *Series
func TSI(obj *Series, short, long int) *Series
func Squeeze(high, low, close *Series, period int) *Series
func NATR(high, low, close *Series, period int) *Series
func Supertrend(high, low, close *Series, period int, multiplier float64) *Series
func SAR(high, low *Series, step, max float64) *Series
func PSAR(high, low *Series, step, max float64) *Series // alias of SAR
func DX(high, low, close *Series, period int) *Series
func AroonOsc(high, low *Series, period int) *Series
func AROONOSC(high, low *Series, period int) *Series // alias of AroonOsc
func Ichimoku(high, low, close *Series, conversion, base, span int) (*Series, *Series, *Series, *Series, *Series) // [conversion, base, spanA, spanB, lagging]
func KST(obj *Series, r1, r2, r3, r4, s1, s2, s3, s4 int) *Series
func ADOSC(env *BarEnv, fast, slow int) *Series
func EFI(env *BarEnv, period int) *Series
func OBV(close, volume *Series) *Series
func VPCI(close, volume *Series, period int) *Series
func Donchian(high, low *Series, period int) (*Series, *Series, *Series) // [upper, middle, lower]
func DonchianPBand(high, low, close *Series, period int) *Series
func KeltnerChannel(high, low, close *Series, period int, multiplier float64) (*Series, *Series, *Series) // [upper, middle, lower]
func KeltnerWBand(high, low, close *Series, period int, mult float64) *Series
func PMAX(high, low, close *Series, period int, multiplier float64) (*Series, *Series) // [pmax, moving average]
func Correlation(a, b *Series, period int) *Series
func Slope(obj *Series, period int) *Series
func LINEARREG_ANGLE(obj *Series, period int) *Series
func LinearRegAngle(obj *Series, period int) *Series // alias of LINEARREG_ANGLE
func PivotHigh(src *Series, left, right int) *Series
func PivotLow(src *Series, left, right int) *Series
func WrapFloatArr(res *Series, period int, inVal float64) []float64 // internal helper
func CDL3INSIDE(open, high, low, close *Series) *Series
func CDL3LINESTRIKE(open, high, low, close *Series) *Series
func CDL3OUTSIDE(open, high, low, close *Series) *Series
func CDLDRAGONFLYDOJI(open, high, low, close *Series) *Series
func CDLENGULFING(open, high, low, close *Series) *Series
func CDLGRAVESTONEDOJI(open, high, low, close *Series) *Series
func CDLHAMMER(open, high, low, close *Series) *Series
func CDLHANGINGMAN(open, high, low, close *Series) *Series
func CDLMORNINGSTAR(open, high, low, close *Series) *Series
func CDLSHOOTINGSTAR(open, high, low, close *Series) *Series
func VWMA(price, vol *Series, period int) *Series
func VWAP(first, second *Series, rest ...*Series) *Series // 2 args (close,volume), 4 args (high,low,close,volume); cumulative over replay segment, no automatic daily reset
func DMI(high, low, close *Series, period int, smoothing ...int) (*Series, *Series, *Series) // [+DI,-DI,ADX]
func STOCH(close, high, low *Series, period int) *Series // talib argument order; do not confuse with Stoch(high,low,close,period)
func DV2(h, l, c *Series, period, maLen int) *Series
func STDDEV(obj *Series, period int) *Series // alias of StdDev
func HL2(h, l *Series) *Series
func HLC3(h, l, c *Series) *Series
func SMA(obj *Series, period int) *Series
func EMA(obj *Series, period int) *Series
func RMA(obj *Series, period int) *Series
func WMA(obj *Series, period int) *Series
func HMA(obj *Series, period int) *Series
func SMMA(obj *Series, period int) *Series // alias of RMA
func TEMA(obj *Series, period int) *Series
func RSI(obj *Series, period int) *Series
func RSI50(obj *Series, period int) *Series
func CMO(obj *Series, period int) *Series
func CMOBy(obj *Series, period, maType int) *Series
func Highest(obj *Series, period int) *Series
func Lowest(obj *Series, period int) *Series
func HighestBar(obj *Series, period int) *Series
func LowestBar(obj *Series, period int) *Series
func LinReg(obj *Series, period int) *Series
func LinRegAdv(obj *Series, period int, angle, intercept, degrees, r, slope, tsf bool) *Series
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
* Calculate indicators on every relevant closed event, before branching on the signal. For new strategies, primary-K-line calculations belong in the `OnData` `Main` handler; auxiliary and custom streams use their own `Info` and `Custom` handlers.
* Use `Series.To` to create new series: `BarEnv.NewSeries` unconditionally creates new series and can grow memory when called repeatedly from indicators or `OnData`. `To` first reuses an existing Series and creates one only when needed.
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
func SplitSymbol(pair string) (string, string, string, string) // Base,Quote,Settle,Identifier
```

### github.com/banbox/banbot/com

The following com functions are compatibility price facades. Explicit tasks use Runtime.Market.Prices (PriceState.GetPriceSafeExpAt / SetPriceAt / SetPricesAt) with their own nowMS; package functions do not provide multi-Runtime isolation.

```go
func GetPrice(symbol, side string) float64
func GetPriceSafe(symbol, side string) float64
func GetPriceExp(symbol, side string, expMS int64) float64
func GetPriceSafeExp(symbol, side string, expMS int64) float64
```

### github.com/banbox/banbot/config
```go
type RunPolicyConfig struct {
	Name, TimeFrames string; Filters []*CommonPairFilter; RunTimeframes []string; RefineTF interface{}
	MaxPair, MaxOpen, MaxSimulOpen, OrderBarMax int; StakeRate float64; Dirt string; StopLoss interface{}; StratPerf *StratPerfConfig
	Pairs []string; Params map[string]float64; PairParams map[string]map[string]float64; More map[string]interface{}
	Score float64; Index int
}
type DatabaseConfig struct {
	Url, Retention, DbType, SIDRegistryURL string
	MaxPoolSize int; AutoCreate bool; QdbMemPct float64; QdbMaxMemMB int
}
func (c *RunPolicyConfig) Def(k string, dv float64, p *core.Param) float64
func (c *RunPolicyConfig) DefInt(k string, dv int, p *core.Param) int
```
### github.com/banbox/banbot/orm
```go
type AdjInfo struct {
	*ExSymbol; Factor, CumFactor float64; StartMS, StopMS int64
}
type InfoKline struct { *banexg.PairTFKline; Sid int32; Adj *AdjInfo; IsWarmUp bool }
type SeriesOHLCV struct {
	Sid int32; ExSymbol *ExSymbol; Source string; Time, EndMS int64; TimeFrame string
	Open, High, Low, Close, Volume, Quote, BuyVolume float64; TradeNum int64
	Adj *AdjInfo; IsWarmUp, Closed bool
}
func (s *SeriesOHLCV) Symbol() string
func (s *SeriesOHLCV) Bar() *banexg.Kline
func (s *SeriesOHLCV) ToInfoKline() *InfoKline
type ExSymbol struct {
	ID int32; Exchange, ExgReal, Market, Symbol string
	Combined bool; ListMs, DelistMs int64
	AggRules string
}
type SeriesField struct { Name, Type, Role string }
type SeriesBinding struct { Table, TimeColumn, EndColumn, SIDColumn string; Fields []SeriesField }
type SeriesInfo struct { Name, TimeFrame string; Binding SeriesBinding }
type DataRecord struct { Sid int32; TimeMS, EndMS int64; Closed bool; Values map[string]any }
type DataSeries struct {
	Source string; Sid int32; TimeMS, EndMS int64; TimeFrame string
	Closed, IsWarmUp bool; Values map[string]any; ExSymbol *ExSymbol; Adj *AdjInfo
}
func (evt *DataSeries) CloneWithExSymbol(exs *ExSymbol) *DataSeries
func (evt *DataSeries) Symbol() string
func (evt *DataSeries) EnsureExSymbol(extras ...*ExSymbol) *ExSymbol
func (evt *DataSeries) FloatValue(key string) (float64, error)
func (evt *DataSeries) FloatValueDefault(key string) (float64, bool)
func (evt *DataSeries) IntValueDefault(key string) (int64, bool)
func (evt *DataSeries) OpenValue/HighValue/LowValue/CloseValue/VolumeValue() (float64, error)
func (evt *DataSeries) QuoteValue/BuyVolumeValue() float64
func (evt *DataSeries) TradeNumValue() int64
func (evt *DataSeries) HasOHLCV() bool
func (evt *DataSeries) OHLCV(extras ...*ExSymbol) (*SeriesOHLCV, error)
func GetExSymbols/GetExSymbolMap(exgName, market string) map[int32/*string*/]*ExSymbol
func GetSymbolByID(id int32) *ExSymbol
func GetExSymbolCur(symbol string) (*ExSymbol, *errs.Error)
func GetExSymbol(exchange banexg.BanExchange, symbol string) (*ExSymbol, *errs.Error)
func GetExSymbol2(exgName, market, symbol string, exgReal ...string) *ExSymbol
func GetAllExSymbols() map[int32]*ExSymbol
func EnsureExSymbol(exchange, market, symbol string, exgReal ...string) (*ExSymbol, error)
func DefaultKlineFields() []string
func NormalizeSeriesFields(source string, fields []string) []string
func MergeSeriesFields(groups ...[]string) []string
func SeriesTableName(name, timeFrame string) string
func NewSeriesInfo(name, timeFrame string, fields []SeriesField) *SeriesInfo
func NewKLineSeriesInfo(name, timeFrame string, fields []SeriesField) *SeriesInfo
func NewSeriesRepo(storage *Storage) SeriesRepo
func NewSeriesStore(repo SeriesRepo) *SeriesStore
func NewKLineSeriesStoreWithStorage(info *SeriesInfo, storage *Storage) *KLineSeriesStore
func ResolveSeriesExSymbol(evt *DataSeries, extras ...*ExSymbol) *ExSymbol
func DefaultSeriesStore() *SeriesStore
func RegisterAggRule(name string, fn AggRuleFunc) bool
```

### github.com/banbox/banbot/data
```go
type DataSource interface {
	Info() *orm.SeriesInfo
	FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)
	SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink DataSink) error
}
type DataSink interface { Emit(sub *strat.DataSub, rows []*orm.DataRecord) error }
type DataSourceFactory func() DataSource
type FetchHistoryFunc func(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)
type SubscribeLiveFunc func(ctx context.Context, subs []*strat.DataSub, sink DataSink) error
func RegisterDataSource(src DataSource) error
func RegisterDataSourceFactory(name string, factory DataSourceFactory) error
func RegisterFuncDataSource(info *orm.SeriesInfo, fetch FetchHistoryFunc, subscribe SubscribeLiveFunc) error
func GetDataSource(name string) DataSource
func ListDataSources() []string
```

### v0.5.2 arbitrary-series storage and Runtime Context

* TimescaleDB and QuestDB use the same ORM. A `SeriesInfo` declares a stream name, timeframe, and fields (`float`, `int`, `string`, `bool`, or `json`). Use `NewSeriesInfo` / `SeriesStore` for independent timestamps or shapes; use `NewKLineSeriesInfo` / `KLineSeriesStore` for extension fields on existing K-lines. K-line extension writes update existing `(sid, time)` rows and do not create bars without OHLCV.
* Runtime-owned storage must be explicit: construct a store with `orm.NewSeriesStore(orm.NewSeriesRepo(storage))`, where `storage` belongs to that Runtime. For K-line extensions use `orm.NewKLineSeriesStoreWithStorage(info, storage)`. Do not use `DefaultSeriesStore` or package-level ORM configuration in code owned by an explicit Runtime.
* A data provider/source owns ingestion and persistence. Register its schema and source with the runner so backtests can fetch history and live runs can subscribe. Strategy code should not issue SQL, perform database writes per candle, invent source names, or register a global source from a strategy factory.
* When converting strategies, the strategy layer usually needs only a subscription and read path. Create a `DataSub` from a known registered series, return it from `OnDataSubs`, then read events through `OnData` / `DataFields` / `StratJob.Data`. A source without `FetchHistory` cannot feed a historical backtest.

Storage/read example (application or data-provider code; `storage` is supplied by the current Runtime):
```go
func saveFundingRate(ctx context.Context, storage *orm.Storage, exs *orm.ExSymbol,
	startMS, endMS int64, rate float64) error {
	info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{{Name: "rate", Type: "float"}})
	store := orm.NewSeriesStore(orm.NewSeriesRepo(storage))
	if err := store.Ensure(ctx, info); err != nil { return err }
	row := &orm.DataRecord{TimeMS: startMS, EndMS: endMS, Values: map[string]any{"rate": rate}}
	if err := store.Write(ctx, info, exs, row); err != nil { return err }
	rows, err := store.Read(ctx, info, exs, startMS, endMS, 0)
	if err != nil { return err }
	if len(rows) > 0 { _, valueErr := rows[0].FloatValue("rate"); return valueErr }
	return nil
}
```

Strategy-side subscription/read example (`funding_rate` must already be a registered historical source):
```go
type FundingState struct { Rate float64 }

func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{{Name: "rate", Type: "float"}})
	return &strat.TradeStrat{
		WarmupNum: 50,
		OnStartUp: func(s *strat.StratJob) { s.More = &FundingState{} },
		OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			sub := strat.NewDataSub(info) // nil ExSymbol means the current job's symbol
			sub.WarmupNum = 20
			sub.Fields = []string{"rate"}
			sub.SeriesFields = []string{"rate"} // include when numeric history is needed
			return []*strat.DataSub{sub}
		},
		OnData: strat.RouteData(strat.DataHandlers{
			Custom: func(s *strat.StratJob, data strat.DataEvent) {
				if !data.Closed || data.IsWarmUp { return }
				if state, ok := s.More.(*FundingState); ok { state.Rate = data.Float64("rate") }
			},
			Main: func(s *strat.StratJob, data strat.DataEvent) {
				if data.IsWarmUp { return }
				state, ok := s.More.(*FundingState)
				if !ok || state.Rate <= 0 || s.GetOrderNum(-1) > 0 { return }
				s.OpenOrder(&strat.EnterReq{Short: true, Tag: "positive_funding"})
			},
		}),
	}
}
```
`DataFields.Series("rate")` returns numeric history; `Float64` reads the current value. Use `RawValue` / `Has` when missing values, explicit `nil`, or exact integer values matter.

Runtime Context guidance for generated strategy code:
* The runner creates and binds one Runtime Context per run; the factory signature remains `func(pol *config.RunPolicyConfig) *strat.TradeStrat`. Do not construct or retain a `Runtime` or `State` in a strategy, and do not use mutable package globals for orders, indicators, or per-symbol state.
* Parse parameters and other read-only settings in the factory and capture them in callback closures. Initialize state that changes for one symbol/job in `OnStartUp` and store it in `s.More`; type-assert it in callbacks. Read the current symbol, timeframe, K-lines, and orders from `s.Symbol`, `s.TimeFrame`, `s.Env`, `s.GetOrders`, `s.GetOrderNum`, and `s.Position`.
* Read auxiliary/custom streams from the `data` event. Custom data is not a K-line; do not assume `s.Env` describes it. Use `OnBatchJobs` / `OnBatchInfos` for cross-symbol aggregation instead of iterating package-level job maps. Historical global getters may remain for compatibility; new strategies must not use them.

### Current custom-series strategy contracts
```go
type DataSub struct {
	Source string; ExSymbol *orm.ExSymbol; TimeFrame string; WarmupNum int
	Fields, SeriesFields []string
}
type DataRole uint8
const ( DataRoleMain DataRole = iota + 1; DataRoleInfo; DataRoleCustom )
type DataEvent struct { *DataFields; Role DataRole; Symbol *orm.ExSymbol }
func (e DataEvent) IsMain() bool
func (e DataEvent) IsKline() bool
type FnOnData func(s *StratJob, data DataEvent)
type DataHandlers struct { Main, Info, Custom FnOnData }
func RouteData(handlers DataHandlers) FnOnData
type DataFields struct {
	DoneMS, TimeMS int64; Source string; Sid int32; TimeFrame string
	Closed, IsWarmUp bool
}
func (d *DataFields) Series(name string) *ta.Series
func (d *DataFields) Float64(name string) float64
func (d *DataFields) Int64(name string) int64
func (d *DataFields) String(name string) string
func (d *DataFields) Raw(name string) any
func (d *DataFields) RawValue(name string) (any, bool)
func (d *DataFields) Has(name string) bool
func NewDataSub(info *orm.SeriesInfo) *DataSub
type DataHub struct { /* runtime-managed subscription state */ }
func (d *DataHub) Get(tf, source string, sid int32) *DataFields
func (d *DataHub) AllReady() bool
func (s *StratJob) Data(sub *DataSub) *DataFields
```

### github.com/banbox/banbot/orm/ormo
```go
type ExitTrigger struct {
	Price, Limit, Rate float64 // Trigger price, limit price, exit ratio (0,1]
	Tag string // Reason
}
type TriggerState struct {
	*ExitTrigger; Range float64; Hit bool; OrderId, ClientId string; Old *ExitTrigger
}
type ExOrder struct {
	ID, TaskID, InoutID int64; Symbol string; Enter bool
	OrderType, OrderID, Side string; CreateAt int64
	Price, Average, Amount, Filled float64; Status int64
	Fee, FeeQuote float64; FeeType string; UpdateAt int64
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
	WarmupNum int // Number of K-lines for warmup; OpenOrder calls during warmup are ignored
	OdBarMax int // Expected maximum bar count for order holding (used to find incomplete positions in backtesting), default 500
	MinTfScore float64 // Minimum timeframe quality, default 0.8
	WsSubs map[string]string // WebSocket subscription configuration
	DrawDownExit bool // Whether to enable trailing stop-loss and take-profit, default false
	HedgeOff bool // Disable contract bidirectional positions
	BatchInOut bool // Whether to batch execute entry/exit after main OnData(RoleMain)/OnBar
	BatchInfo bool // Whether to batch process after auxiliary OnData(RoleInfo)/OnInfoBar
	StakeRate float64 // Position size multiplier relative to base amount
	StopLoss float64 // Default stop loss ratio for this strategy, without leverage
	StopEnterBars int
	OrderOnRotation string // close, hold, or open when a symbol rotates out
	EachMaxLong int // max number of long open orders for one pair, -1 for disable
	EachMaxShort int // max number of short open orders for one pair, -1 for disable
	TimeFrames string // comma-separated strategy timeframes
	RunTimeFrames []string // Allowed running timeframes; when omitted, use the current Runtime configuration snapshot default
	RefineTF interface{} // matching timeframe selector, e.g. "5m", "3-6", or 5
	Outputs []string // Text file content output by strategy, each string is a line
	Policy *config.RunPolicyConfig
	OnPairInfos func(s *StratJob) []*PairSub // legacy compatibility API; new strategies use OnDataSubs
	OnDataSubs func(s *StratJob) []*DataSub
	OnSymbols func(items []string) []string // return modified pairs
	OnStartUp func(s *StratJob)
	OnBar func(s *StratJob) // legacy compatibility API; do not combine with OnData
	OnData FnOnData
	OnInfoBar func(s *StratJob, e *ta.BarEnv, pair, tf string) // legacy compatibility API; do not combine with OnData
	OnWsTrades func(s *StratJob, pair string, trades []*banexg.Trade) // Tick-by-tick trade data
	OnWsDepth func(s *StratJob, dep *banexg.OrderBook) // Websocket pushed depth information
	OnWsKline func(s *StratJob, pair string, k *banexg.Kline) // Real-time K-line pushed by Websocket
	OnWsData func(s *StratJob, evt *orm.DataSeries)
	OnBatchJobs func(jobs []*StratJob) // All symbol jobs at current time, used for batch opening/closing
	OnBatchInfos func(tf string, jobs map[string]*JobEnv) // All info symbol jobs at current time, used for batch processing
	OnCheckExit func(s *StratJob, od *ormo.InOutOrder) *ExitReq // Custom order exit logic
	OnOrderChange func(s *StratJob, od *ormo.InOutOrder, chgType int) // Order update callback
	GetDrawDownExitRate func(s *StratJob, od *ormo.InOutOrder, maxChg float64) float64 // Calculate trailing take profit drawdown exit ratio
	PickTimeFrame func(symbol string, tfScores []*core.TfScore) string // Select suitable trading timeframe for specified symbol
	OnPostApi func(client *core.ApiClient, msg map[string]interface{}, jobs map[string]map[string]*StratJob) error // Strategy callback during PostAPI
	OnShutDown func(s *StratJob) // Callback when bot stops
	OnStratExit func() // Callback when the strategy exits
}

const ( OdChgNew = iota; OdChgEnter; OdChgEnterFill; OdChgExit; OdChgExitFill )
const ( BatchTypeInOut = iota; BatchTypeInfo )

type JobEnv struct { Job *StratJob; Env *ta.BarEnv; Symbol string }
type PairSub struct { Pair, TimeFrame string; WarmupNum int }
type StratJob struct {
	Strat *TradeStrat
	Env *ta.BarEnv
	DataHub *DataHub
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
	ActivationPrice float64 // Trailing-stop activation price
	CallbackPct float64 // Trailing-stop callback percentage, [0.1, 10]
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

type PairUpdateReq struct {
	Add []string
	Remove []string
	CloseOnRemove bool
	ForceAdd bool
	Reason string
}

type PairUpdateResult struct {
	Added []string
	Removed []string
	Skipped []string
	ExitOrders map[string][]*ormo.InOutOrder
	Warnings []string
}

// Core methods
type FuncMakeStrat = func(pol *config.RunPolicyConfig) *TradeStrat
func RegisterStrategy(name string, factory FuncMakeStrat)
func GetJobs(account string) map[string]map[string]*StratJob
func GetInfoJobs(account string) map[string]map[string]*StratJob
func (s *TradeStrat) UpdatePairs(req PairUpdateReq) (*PairUpdateResult, *errs.Error)
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
		OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			// Subscribe to 1h K-lines in addition to the primary timeframe.
			return []*strat.DataSub{{Source: "kline", TimeFrame: "1h", WarmupNum: 50}}
		},
		OnData: strat.RouteData(strat.DataHandlers{
			Info: func(s *strat.StratJob, data strat.DataEvent) {
				// Store the 1h signal in this job's state for the main handler.
				m, _ := s.More.(*SmaOf2)
				closeSeries := data.Series("close")
				m.goLong = ta.EMA(closeSeries, 20).Get(0) > ta.EMA(closeSeries, 25).Get(0)
			},
			Main: func(s *strat.StratJob, _ strat.DataEvent) {
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
			if maCross == 1 && m.goLong && sma-c > atrBase*longRate && s.GetOrderNum(0) == 0 {
				s.OpenOrder(&strat.EnterReq{Tag: "long"})
			} else if maCross == -1 && !m.goLong && c-sma > atrBase*shortRate {
				s.CloseOrders(&strat.ExitReq{Tag: "short"})
			}
			},
		}),
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
 * New strategies use `OnData`; do not combine it with legacy `OnBar` or `OnInfoBar`. Use `strat.RouteData(strat.DataHandlers{Main: ..., Info: ..., Custom: ...})` when the strategy consumes multiple data roles.
 * Put primary-K-line order logic in the `Main` handler. Read auxiliary/custom inputs from their `data` event; custom data is not an OHLCV bar. Call `OpenOrder` and `CloseOrders` from the intended handler, and use `EachMaxLong` / `EachMaxShort` to limit concurrent orders per symbol.
 * Do not migrate a legacy strategy by just renaming `OnBar` to `OnData`; route `DataRoleMain`, `DataRoleInfo`, and `DataRoleCustom` explicitly.
 * banbot will use the strategy initialization function `func(pol \*config.RunPolicyConfig) \*strat.TradeStrat` to create a strategy task `*strat.StratJob` for each symbol;
 * Read-only settings parsed in the strategy factory may be captured by the `OnData` callbacks. Mutable per-job state shared between handlers belongs in `*strat.StratJob.More`, initialized in `OnStartUp`.
 * If you need automatic stop loss after order profit, from maximum profit drawdown to a certain extent, you can set `DrawDownExit` to `true`, then pass in `GetDrawDownExitRate` function: `func(s *StratJob, od *ormo.InOutOrder, maxChg float64) float64`, returning 0 means no stop loss, returning 0.5 means stop loss at 50% drawdown from maximum profit. The maxChg parameter is the maximum profit of the order, such as 0.1 means long order price increase of 10% or short order price decrease of 10%
 * Configure the primary timeframe in YAML. Return additional symbols, K-line timeframes, or custom series from `OnDataSubs`; handle auxiliary K-lines in `RouteData.Info` and custom series in `RouteData.Custom`. `OnPairInfos` / `OnInfoBar` are legacy compatibility interfaces.
 * Note that hyperparameters parsed through `RunPolicyConfig` are fixed, unchanging, read-only, and can be directly shared by all `StratJobs` of this strategy, so do not save hyperparameters to structs, and especially do not save to `StratJob.More`. More should only record variables that differ for each symbol.
 * If you need to handle exit logic for each order individually on each bar, you can pass in `OnCheckExit` function, returning a non-`nil` `ExitReq` means closing this order;
 * Use `StratJob.More` only for mutable per-job state shared between handlers (such as an auxiliary-timeframe signal read by the main handler). Initialize it in `OnStartUp`; capture immutable parameters in the factory closure.
 * To calculate the number of bars an order has been held, you can use `holdNum := s.Env.BarCount(od.EnterAt)`
 * In most cases, unified exit logic can live in the `OnData` `Main` handler; use `OnCheckExit` only when each order needs separate exit logic.
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

## Cross-sectional and multi-factor strategies

### Engines and definitions

Use this section to convert ranking, rotation, long/short selection, factor combinations and holding lifecycles to banbot. The preceding `TradeStrat`/banta callback rules apply to the time-series engine. The native factor engine uses a `factor.Plan`; its runner builds portfolio targets for account execution.

| Strategy | Entry point |
| --- | --- |
| Per-symbol event signals and per-order exits | `engine: time_series` (default), `strat.TradeStrat.OnData` |
| Cross-symbol ranking, neutralization, multi-factor portfolios and rebalancing | `engine: factor`, YAML `expressions` or Go `runner.RegisterDefinition` |
| Batch comparisons/orders added to an existing time-series strategy | `TradeStrat.BatchInOut` + `OnBatchJobs`; auxiliary batches use `BatchInfo` + `OnBatchInfos` |

A factor strategy returns a `Plan`, rather than `TradeStrat` callbacks that place ranked orders per symbol. In YAML, `expressions`, `definition`, `portfolio`, `prices`, `decision` and `research` belong directly under `run_policy[]`, alongside `engine`. The old `factor: {...}` nesting is accepted for compatibility; no version marker is needed.

* Use one `run_timeframes` decision timeframe per factor entry. An expression strategy may use any name; a Go strategy selects its registered `definition` explicitly or uses name as the registered definition name. The built-in Go definition is `momentum-vol`.
* `id` identifies the strategy; `account` selects the account. When an account with factor strategies has multiple participants, all participants require explicit `capital_weight`, totaling at most 1. This assigns strategy budgets; time-series `stake_rate` remains an order sizing multiplier.
* `run_policy.params.k` / `portfolio.k` controls legacy selection. `expressions.params` belongs to `param.name` and does not inherit outer parameters. Go builders read outer `params` from `c.Manifest.Parameters`.

### YAML multi-factor example

This weights backtest requires `data.gob` with at least six assets, sufficient warmup history and subsequent visible prices. It combines momentum and low volatility, selects the top/bottom three assets, and assigns each side 50% of strategy NAV.

```yaml
wallet_amounts: {USD: 10000}
execution: {mode: weights, funding_policy: explicit-zero}
run_policy:
  - name: MultiFactor
    id: multi_v1
    engine: factor
    run_timeframes: [1h]
    params: {k: 3}
    archive: data.gob
    prices: {source: kline, timeframe: 1h, field: close}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
    research: {labels: []}
    expressions:
      schema_version: 1
      timeframe: 1h
      bindings:
        kline: {source: kline, timeframe: 1h}
      params: {window: 24}
      lets:
        price: 'positive(kline.close)'
        ret1: 'ts.return(factor.price, 1)'
      outputs:
        momentum: 'cs.zscore(ts.return(factor.price, param.window))'
        low_vol: 'cs.zscore(-ts.std(factor.ret1, param.window, 0))'
      combine:
        method: fixed
        weights: {momentum: 0.7, low_vol: 0.3}
```

Run `banbot backtest --mode weights --config strategy.yml`. For an ordinary history database, remove `archive`, load base database/market/universe/`time_range` settings, and explicitly set `data: {pit_policy: static-approximation}`. A latest-value history store cannot prove strict point-in-time (PIT) visibility.

For research, remove `research: {labels: []}` from the example; the default label spans one decision period. Run `banbot research --config strategy.yml`. Multiple horizons use `research.labels: [{name: forward_16h, kind: executable-return, horizon: 57600000, periods_per_year: 547.5, overlapping: true}]`. Horizon is milliseconds; immature labels remain unresolved. History combiners require labels and cannot disable them.

### Expression syntax and functions

`alias.field` reads a binding field; `field("alias","field-name")` supports special field names. `factor.name` references this spec's lets/outputs; `param.name` references numeric parameters. Forward references are allowed; cycles are rejected, and unused declarations are validated. Supported syntax is numbers, scientific notation, parentheses, unary signs, arithmetic and the functions below; comparisons, conditional branches and future `label.*` references are unsupported.

| Go (factor) | Formula |
| --- | --- |
| `Add/Sub/Mul/Div(a,b)`, `Neg(x)` | `+ - * /`, unary `-` |
| `Positive/Abs/Log/Sqrt(x)` | `positive/abs/log/sqrt(x)` |
| `Pow/Min/Max(a,b)` | `pow/min/max(a,b)` |
| `Lag/Return/EMA(x,n)` | `ts.lag/return/ema(x,n)` |
| `StdDev(x,n,ddof)` | `ts.std(x,n,ddof)` |
| `SMA/RMA/WMA/RSI/ROC/MOM/CCI/Highest/Lowest(x,n)` | `ts.sma/rma/wma/rsi/roc/mom/cci/highest/lowest(x,n)` |
| `VWMA(c,v,n)`, `TR(h,l,c)`, `ATR(h,l,c,n)` | `ts.vwma(c,v,n)`, `ts.tr(h,l,c)`, `ts.atr(h,l,c,n)` |
| `Stoch/WillR(h,l,c,n)`, `OBV(c,v)`, `MFI(h,l,c,v,n)` | `ts.stoch/willr(h,l,c,n)`, `ts.obv(c,v)`, `ts.mfi(h,l,c,v,n)` |
| `MACD(x,fast,slow,signal)` | `ts.macd/macd_signal/macd_hist(x,fast,slow,signal)` |
| `BBands(x,n,up,down)` | `ts.bbands_upper/bbands_middle/bbands_lower(x,n,up,down)` |
| `Rank/ZScore/RobustZScore(x)` | `cs.rank/zscore/robust_zscore(x)` |
| `Winsorize(x,tail)`, `MADWinsorize(x,k)`, `Quantile(x,q)` | `cs.winsorize(x,tail)`, `cs.mad_winsorize(x,k)`, `cs.quantile(x,q)` |
| `Residual(y,x)`, `MultiResidual(y,xs...)`, `WeightedResidual(y,w,xs...)` | `group.residual(y,x)`, `group.ols(y,x1,x2,...)`, `group.wls(y,w,x1,x2,...)` |
| `GroupDemean/GroupZScore(x,source,field,sourceTF...)` | `group.demean/zscore(x,"alias","field")` |

* Window arguments are integer constants or direct `param.name` references in [1,10000]; lag also permits 0. Expressions such as `param.window+1` are not window arguments. `ddof` must satisfy `0 <= ddof < n`; MACD requires `fast < slow`. Both nonnegative Bollinger multipliers are explicit.
* `cs.rank` returns ascending, zero-based average ranks for ties, not percentiles. `cs.zscore` uses population standard deviation and returns zero for constant sections. `tail` is in [0,0.5), `q` in [0,1], and the MAD multiplier is positive. Robust z-score scales by `1.4826*MAD`.
* Cross-sectional/regression statistics fit on frozen `Universe.Reference`. OLS/WLS include an intercept; WLS needs positive weights. Group fields retain raw types/NULL; the formula source argument is a binding alias.
* Time series may precede cross-sectional transforms, e.g. `cs.rank(ts.rsi(kline.close,14))`. TS windows over CS/regression results are rejected, including through pointwise wrappers; zero lag is the identity exception.
* `ts.return` is a ratio; `ts.roc` is a percentage. `max(x,1e-8)` bounds valid values and does not fill NULL, missing or warmup inputs. New technical indicators advance on complete valid tuples; lag/return/ema/std retain their individual contracts. `factor.MACD/BBands` return three nodes; distinguish them from the preceding banta signatures.

Slow/event sources require explicit `asof` sampling onto the decision grid with a positive age bound. For example, add `funding: {source: funding, timeframe: event, sampling: asof, max_age_ms: 28800000}` to bindings and use `cs.zscore(-funding.rate)`; the input must provide that stream. Downstream windows count decision observations: 24 hourly samples are not 24 trading days.

Save the `expressions` mapping alone as `formula.yml`; `banbot validate --spec formula.yml` / `banbot explain --spec formula.yml` checks compilation and dependencies. These commands do not validate actual data or accounts. A standalone spec needs an explicit timeframe, one YAML document and a size of at most 1 MiB.

### github.com/banbox/banbot/factor/expr

```go
type Binding struct { Source, TimeFrame, Sampling string; MaxAgeMS int64 }
type Spec struct {
    SchemaVersion int; TimeFrame string
    Bindings map[string]Binding; Params map[string]float64
    Lets, Outputs map[string]string; Combine research.ComboSpec
}
func Compile(spec Spec) (*factor.Plan, error)
```

### github.com/banbox/banbot/factor

Public strategy-building interfaces follow. Construct `Node`, `Builder` and `Plan` through these functions; `Node` inputs are numeric nodes, not banta *Series.

```go
type Validity string // Valid, Missing, Null, NotNumeric, NonFinite, Warmup
type Numeric struct { Value float64; Validity Validity }
func Number(values map[string]any, field string) Numeric

// Node, Builder, Plan: construct nodes through these functions.
func Field(source, field, timeframe string) *Node
func AsOfField(source, field, sourceTimeFrame, decisionTimeFrame string, maxAge int64) *Node
func Constant(value float64, timeframe string) *Node
func Add(a, b *Node) *Node
func Sub(a, b *Node) *Node
func Mul(a, b *Node) *Node
func Div(a, b *Node) *Node
func Pow(a, b *Node) *Node
func Min(a, b *Node) *Node
func Max(a, b *Node) *Node
func Neg(input *Node) *Node
func Abs(input *Node) *Node
func Log(input *Node) *Node
func Sqrt(input *Node) *Node
func Positive(input *Node) *Node
func Linear(inputs []*Node, weights []float64) *Node
func Lag(input *Node, period int) *Node
func Return(input *Node, period int) *Node
func EMA(input *Node, period int) *Node
func StdDev(input *Node, period, ddof int) *Node
func SMA(input *Node, period int) *Node
func RMA(input *Node, period int) *Node
func WMA(input *Node, period int) *Node
func VWMA(price, volume *Node, period int) *Node
func RSI(input *Node, period int) *Node
func ROC(input *Node, period int) *Node
func MOM(input *Node, period int) *Node
func TR(high, low, close *Node) *Node
func ATR(high, low, close *Node, period int) *Node
func CCI(input *Node, period int) *Node
func Stoch(high, low, close *Node, period int) *Node
func WillR(high, low, close *Node, period int) *Node
func OBV(close, volume *Node) *Node
func MFI(high, low, close, volume *Node, period int) *Node
func Highest(input *Node, period int) *Node
func Lowest(input *Node, period int) *Node
func MACD(input *Node, fast, slow, signal int) (line, signalLine, hist *Node)
func BBands(input *Node, period int, stdUp, stdDown float64) (upper, middle, lower *Node)
func Rank(input *Node) *Node
func ZScore(input *Node) *Node
func RobustZScore(input *Node) *Node
func Winsorize(input *Node, tail float64) *Node
func MADWinsorize(input *Node, multiple float64) *Node
func Quantile(input *Node, q float64) *Node
func GroupDemean(input *Node, source, field string, sourceTimeFrame ...string) *Node
func GroupZScore(input *Node, source, field string, sourceTimeFrame ...string) *Node
func Residual(y, x *Node) *Node
func MultiResidual(y *Node, exposures ...*Node) *Node
func WeightedResidual(y, weights *Node, exposures ...*Node) *Node
func Custom(version string, inputs []*Node, evaluate func([]Numeric) Numeric) *Node

func New() *Builder
func (b *Builder) Add(name string, n *Node) *Builder
func (b *Builder) Compile() (*Plan, error)
func Compile(outputs map[string]*Node) (*Plan, error)
type InputSpec struct {
    Source, TimeFrame string; Fields []string
    WarmupLength int; AsOfLatest bool; MaxAge int64
}
func (p *Plan) Hash() string
func (p *Plan) TimeFrame() string
func (p *Plan) Outputs() []string
func (p *Plan) Inputs() []InputSpec
func (p *Plan) WarmupLength() int
func (p *Plan) StateRetention() int
```

`Custom` is a versioned pure pointwise function with explicit dependencies. Propagate invalid input reasons; do not read accounts/future quotes or hide rolling state in its closure. Change version when its implementation changes. A missing historical operator requires a kernel extension with Session/Batch verification.

Frozen inputs and results (normally assembled by the runner; useful for manual numerical checks):

```go
type Universe struct {
    Version string
    Investable, Reference, Tradable, Evaluation, Tracked []int32
    Static bool
}
type Frame struct {
    GridTime, DecisionTime int64
    SnapshotID, PlanHash string
    Values map[string]map[int32]Numeric
}
type VersionRecord struct {
    Series orm.DataSeries; EventTime int64; Revision uint64
    AvailableAt, IngestedAt int64; SourceVersion string
}
type SnapshotSpec struct {
    TrackedQuotesOnly bool
    GridTime, DecisionTime, ReplayTime int64
    Universe Universe; SIDMap map[int32]string
    Schemas, SourceVersions map[string]string
    AdjustmentVersion, VisibilityPolicy string
}
type Requirement struct {
    SID int32; Source, TimeFrame string
    EventTime int64; AsOfLatest bool; MaxAge int64
}
func Record(series orm.DataSeries, revision uint64, availableAt, ingestedAt int64, sourceVersion string) VersionRecord
func Freeze(spec SnapshotSpec, records []VersionRecord, requirements []Requirement) (*Snapshot, error)
func NewSession(plan *Plan) (*Session, error)
func (s *Session) Warmup(snapshot *Snapshot) error
func (s *Session) Evaluate(snapshot *Snapshot) (Frame, error)
func (p *Plan) Batch(snapshots []*Snapshot, maxRows int) ([]Frame, error)
```

`Investable` is the candidate pool, `Reference` the statistical pool, `Tradable` the eligible trading pool, `Evaluation` the research pool and `Tracked` the assets still monitored after removal. `Frame.Values[column][sid]` is a derived numeric view; `orm.DataSeries.Values map[string]any` retains arbitrary fields, types, missing values and NULL.

`AvailableAt` is publication/visibility time, `IngestedAt` reception time and `Revision` revision identity. Freeze selects only versions visible and received at the cutoff. Incomplete closed data/barriers skip decisions instead of shrinking declared pools. Reuse one `Session` across history chunks; `Batch` initializes from the supplied history start. Past revisions require a new replay, leaving frozen results intact. `WarmupLength()` counts preceding observations without missing data; elapsed calendar bars do not guarantee valid output.

### github.com/banbox/banbot/factor/research

```go
type ComboMethod string // Equal, Fixed, HistoryIC, HistoryRankIC, HistoryICIR, HistoryRankICIR, HistoryEWMA
type ComboSpec struct {
    Method ComboMethod; Columns []string; Weights map[string]float64
    Label string; MinSamples, MinPairs int
    MinConfidence, Decay float64; Direction, Fallback string
}
type PortfolioDefinition struct {
    Builder, BuilderConfigHash string; K int
    LongNotional, ShortNotional float64; Mode factor.PortfolioMode
    Policy string; PolicyParams json.RawMessage
    Rebalance *factor.RebalanceConfig; Selection *factor.SelectionConfig
    Holding *factor.HoldingConfig; Transition *factor.TransitionConfig
    Allocation *factor.AllocationConfig
}
```

equal averages selected columns, defaulting to all outputs. fixed sums explicit weights, allows negatives and does not normalize them. An invalid nonzero-weight column invalidates that asset's score; weights are not reassigned per asset. Zero-weight columns do not affect validity. Selected columns must exist and be unique; avoid naming raw outputs score, which the runner generates. An explicit outer `combo.method` replaces the entire inner combination config.

History methods are history-ic/history-rank-ic/history-icir/history-rank-icir/history-ewma and consume only matured, decision-visible samples. label selects the `horizon`; with multiple labels, omission selects the shortest `horizon`, then name. `min_samples` counts historical sections; `min_pairs` counts assets per section; `min_confidence` is a mean/standard-error threshold. EWMA `decay` is alpha (zero defaults to 0.2); direction is signed|positive and `fallback` is equal|fixed|error. Live currently rejects all history combiners.

### Portfolio targets and holding policies (factor)

```go
type PortfolioMode string // Full = "full", Patch = "patch"
type FrozenBudget struct { Version, Currency string; NAV float64 }
type Diagnostic struct { Code, Detail string }
type PortfolioSpec struct {
    StrategyID, AccountID string
    DecisionTime, ExecutableAt, ExpireAt int64; PlanSequence uint64
    SnapshotID, PlanHash, FactorPlanHash, UniverseVersion string
    Budget FrozenBudget; Mode PortfolioMode; Diagnostics []Diagnostic
}
func NewTargetPortfolio(spec PortfolioSpec, targets map[int32]float64) (*TargetPortfolio, error)
func (p *TargetPortfolio) Spec() PortfolioSpec
func (p *TargetPortfolio) ID() string
func (p *TargetPortfolio) Targets() map[int32]float64
func (p *TargetPortfolio) EffectiveTargets(previous *TargetPortfolio) (map[int32]float64, error)
func (p *TargetPortfolio) Notional(sid int32) float64
func TopBottomK(frame Frame, scoreName string, universe Universe, spec PortfolioSpec, k int) (*TargetPortfolio, []Diagnostic, error)
func TopBottomKNotional(frame Frame, scoreName string, universe Universe, spec PortfolioSpec, k int, longNotional, shortNotional float64) (*TargetPortfolio, []Diagnostic, error)

type AllocationBasis string // NAVFraction = "nav-fraction", AbsoluteQuantity = "absolute-quantity"
type Allocation struct { Basis AllocationBasis; Value string }
func NewPortfolioTarget(spec PortfolioSpec, allocations map[int32]Allocation) (*PortfolioTarget, error)
func (p *PortfolioTarget) Spec() PortfolioSpec
func (p *PortfolioTarget) ID() string
func (p *PortfolioTarget) Version() int
func (p *PortfolioTarget) AsWeightPortfolio() (*TargetPortfolio, error)
func (p *PortfolioTarget) Allocations() map[int32]Allocation
func (p *PortfolioTarget) EffectiveAllocations(previous *PortfolioTarget) (map[int32]Allocation, error)
func PortfolioTargetFromWeights(p *TargetPortfolio) (*PortfolioTarget, error)
```

Positive weights are long, negative weights short; `FrozenBudget.NAV` is strategy NAV. `Full` zeros omitted prior targets owned by this strategy; `Patch` preserves omitted targets, and explicit zero exits. Default `TopBottomK` requires at least `2*k` valid investable/tradable scores even when one side's notional is zero. Insufficient/constant scores return a nil target and diagnostics; the runner skips replacement and preserves existing positions. Direct callers must check for nil. Use lifecycle selection or a custom builder for selection based only on the active side.

`TargetPortfolio` represents ideal weights. `PortfolioTarget` supports both NAV fractions and absolute quantities. `Allocation.Value` is a signed plain decimal string; empty strings and scientific notation are rejected. `AbsoluteQuantity` means standard asset units, not contract counts; `AsWeightPortfolio` rejects quantity targets. EffectiveTargets/EffectiveAllocations validate strategy, account, currency and increasing plan sequence.

`portfolio.policy: lifecycle-v1` enables rebalance and holding lifecycles. Replace the example's portfolio with:

```yaml
portfolio:
  long_notional: 1
  short_notional: 0
  policy: lifecycle-v1
  selection: {long_k: 3}
  rebalance: {every_bars: 2}
  holding: {min_bars: 16}
  transition: {mode: linear-exit, exit_steps: 8, basis: quantity}
  allocation: {method: equal, reserve_ratio: 0.02}
```

This computes hourly and normally rebalances every two hours. After 16 hours from the first actual fill, an asset begins an eight-step exit only when it drops out of selection. `holding.max_bars`/max_duration forces zero at a monitoring grid, bypassing normal rebalance gates and exit curves; use min when gradual exits should start only after the minimum age. These field types also support Go policies:

```go
type RebalanceConfig struct {
    EveryBars int; Anchor int64; Phase int
    Duration, Calendar, CalendarVersion, Timezone string
}
type SelectionConfig struct {
    LongK, ShortK int; LongQuantile, ShortQuantile float64
    RetainRank, Dropout int; GroupQuota map[string]int; MissingScores string
}
type HoldingRule struct { MinBars, MaxBars int; MinDuration, MaxDuration string }
type HoldingOverride struct { MinBars, MaxBars *int; MinDuration, MaxDuration *string }
type HoldingConfig struct {
    MinBars, MaxBars int; MinDuration, MaxDuration string
    ByAsset map[string]HoldingOverride; Adopt string; CooldownBars int
}
type TransitionRule struct { ExitSteps int; Ratio float64 }
type TransitionConfig struct {
    Mode string; ExitSteps int; Basis string; PeriodBars int
    Startup, Sizing, OnReselect string; Ratio, FinalThreshold, Alpha float64
    EntryWindowBars int; ByAsset map[string]TransitionRule
}
type AllocationConfig struct {
    Method string; ReserveRatio, FixedNotional, VolTarget, AssetCap float64
    GroupCaps map[string]float64; TurnoverLimit, NetCap, BetaCap float64
}
type PortfolioPolicyConfig struct {
    Policy string; PolicyParams json.RawMessage
    Rebalance RebalanceConfig; Selection SelectionConfig; Holding HoldingConfig
    Transition TransitionConfig; Allocation AllocationConfig
    LongNotional, ShortNotional float64
}
type PositionFillEvidence struct {
    Quantity string; LedgerCursor, PlanSequence uint64; AtMS int64
}
type PositionEvidence struct {
    Quantity, PendingQuantity string; FirstFillTime int64
    IncreasingPending, PendingUnknown bool; Quantum string
    FillEvents []PositionFillEvidence
}
type PortfolioContext struct {
    Frame Frame; Universe Universe; Ideal *TargetPortfolio; Spec PortfolioSpec
    GridTime, BarMillis int64; ScoreName string
    Positions map[int32]PositionEvidence; Marks map[int32]float64
    Groups map[int32]string; Volatility, Beta map[int32]float64
    AssetNames map[int32]string; SIDMappingVersion string
    StateVersion, LedgerCursor uint64; ForceExit map[int32]string
    Previous *PortfolioTarget; PreserveIdealWeights bool
    HoldingRules map[int32]HoldingRule; TransitionRules map[int32]TransitionRule
    RebalanceDue *bool; CapitalLimit float64; RiskOnly bool
}
type PortfolioProposal struct {
    Target *PortfolioTarget; NextState json.RawMessage; Reasons []Diagnostic
    AcceptanceID string; ReconcileSIDs []int32; PlanSequence uint64
    DecisionTime, ExpireAt int64
}
type PortfolioPolicy interface {
    Propose(PortfolioContext, json.RawMessage) (PortfolioProposal, error)
}
type PortfolioPolicyFactory func(PortfolioPolicyConfig) (PortfolioPolicy, error)
func NewLifecyclePolicy(config PortfolioPolicyConfig) (PortfolioPolicy, error)
func SelectPortfolio(frame Frame, universe Universe, spec PortfolioSpec, c PortfolioPolicyConfig) (*TargetPortfolio, []Diagnostic, error)
func SelectPortfolioScore(frame Frame, universe Universe, spec PortfolioSpec, c PortfolioPolicyConfig, scoreName string, groups map[int32]string) (*TargetPortfolio, []Diagnostic, error)
func AllocateSelected(selected map[int32]float64, c PortfolioPolicyConfig, nav float64, scores, vol map[int32]float64) (map[int32]float64, error)
```

* rebalance supports every_bars, anchor/phase, duration or calendar/timezone/calendar_version; do not mix schedules. selection supports separate side K or quantiles, retain_rank buffers, dropout replacement counts and group_quota.
* `transition.mode` is direct, linear-exit (exit_steps), cohort (period_bars is a multiple of every_bars), geometric (ratio/final_threshold) or target-step (alpha). Exit basis is quantity or weight: quantity anchors confirmed units; weight is converted using current NAV and can replenish units.
* For cohorts, `startup: gradual|seed-all` and `sizing: entry-nav|current-nav` explicitly select budget behavior. `on_reselect: restore|resume|finish|new-cohort` controls reselection. Scheduled cohort windows are not actual fill holding durations.
* `holding.by_asset` / `transition.by_asset` use SIDMap asset names. Holding pointers distinguish omission from explicit zero. Unknown first-fill age is not guessed; `adopt: adopt` explicitly starts age at adoption.
* allocation.method supports equal, score, fixed-notional, inverse-volatility and vol-target. Supply `Groups`, `Volatility`, `Beta`, visible parameter rules and custom calendars via `Config.PolicyContext`; similarly named factor columns are not implicit risk evidence.
* Each run receives a private policy instance. A policy reads frozen context and accepted JSON state, and returns targets, next state and diagnostics without sending orders or writing accounts. Advance state after atomic account acceptance. Accepted targets with send errors must not roll back state or generate duplicate plans. Preserve pending/partial-fill evidence; direction reversal waits for confirmed zero.

### github.com/banbox/banbot/factor/runner

```go
type DefinitionBuilder func(Config) (*factor.Plan, research.ComboSpec, error)
type PortfolioBuilder func(factor.Frame, factor.Universe, factor.PortfolioSpec, research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error)
type PortfolioPolicyFactory = factor.PortfolioPolicyFactory
func RegisterDefinition(name string, builder DefinitionBuilder) error
func CompileDefinition(c Config) (*factor.Plan, research.ComboSpec, error)
func RegisterPortfolioBuilder(name string, builder PortfolioBuilder) error
func RegisterPortfolioBuilderIdentity(name, hash string) error
func RegisterPortfolioPolicy(name string, factory PortfolioPolicyFactory) error
```

A Go `definition` returns a compiled graph and combiner. A stateless `PortfolioBuilder` consumes a frozen Frame containing original outputs and combined score, and returns ideal weights. Register complete custom policies with versioned names such as rotation-v1; YAML selects `portfolio.builder` / `portfolio.policy`. Use immutable versions/config hashes for strategy identity. Definition/builder code changes require recompilation.

`Config` fields used by builders (strategy-facing fields only; account/input assembly fields omitted):

```go
// Selected fields used by DefinitionBuilder; other assembly fields omitted.
type Config struct { // package runner
    Definition string; Expressions *expr.Spec; Plan *factor.Plan
    Factor research.MomentumVolConfig; Combo research.ComboSpec
    Manifest research.ManifestSpec; PortfolioBuilder PortfolioBuilder
    PolicyContext func(context.Context, *factor.PortfolioContext) error
}
// package research
type MomentumVolConfig struct {
    Source, Field, TimeFrame string; Window, DDOF int
    WinsorTail float64; Standardize bool
}
type ManifestSpec struct { // selected strategy-facing fields
    Parameters map[string]float64; Portfolio PortfolioDefinition
    Combo ComboSpec; Labels []LabelSpec
}
type LabelKind string // ExecutableReturn, CloseToClose
type LabelSpec struct {
    Name string; Kind LabelKind; Horizon int64
    Overlapping bool; PeriodsPerYear float64
}
```

### Go multi-factor example

```go
package factors

import (
    "fmt"
    "math"
    "github.com/banbox/banbot/factor"
    "github.com/banbox/banbot/factor/research"
    "github.com/banbox/banbot/factor/runner"
)

func MultiFactorV1(c runner.Config) (*factor.Plan, research.ComboSpec, error) {
    window := 24
    if v, ok := c.Manifest.Parameters["window"]; ok {
        if math.IsNaN(v) || math.IsInf(v, 0) || v < 2 || v > 10000 || v != math.Trunc(v) {
            return nil, research.ComboSpec{}, fmt.Errorf("window must be an integer in [2,10000]")
        }
        window = int(v)
    }
    price := factor.Positive(factor.Field(c.Factor.Source, c.Factor.Field, c.Factor.TimeFrame))
    momentum := factor.ZScore(factor.Return(price, window))
    lowVol := factor.ZScore(factor.Neg(factor.StdDev(factor.Return(price, 1), window, 0)))
    plan, err := factor.New().Add("momentum", momentum).Add("low_vol", lowVol).Compile()
    return plan, research.ComboSpec{
        Method: research.Fixed, Columns: []string{"momentum", "low_vol"},
        Weights: map[string]float64{"momentum": 0.7, "low_vol": 0.3},
    }, err
}

func init() {
    if err := runner.RegisterDefinition("MultiFactorV1", MultiFactorV1); err != nil { panic(err) }
}
```

Compile and import this package so init runs; the application's main calls `entry.RunCmd()`. In the earlier YAML, keep the display name, add `definition: MultiFactorV1`, change `params` to `{window: 24, k: 3}`, and remove `expressions`. Portfolio/price settings remain applicable. The unified command entry supplies `c.Factor` defaults for kline/close/decision timeframe; direct builder calls must supply them.

### Execution and conversion rules

* YAML `expressions` and Go definitions produce the same `Plan`. Expressions cannot coexist with `definition` or Go `Config.Plan`. A directly supplied `Plan` also needs explicit `Combo`. Strategies use the existing barriers, account ledger and exchange adapters.
* `weights` is an approximate weight replay. Ordinary backtest defaults to `events` (overridden by `--mode` / `execution.mode`); mixed time-series/factor replay requires `events`. `events`/live need independent tick/event or 1m observable execution prices, instrument units and account evidence. Hourly/daily factor candles do not replace execution streams.
* `decision.delay_ms` controls the visibility cutoff; `latency_ms` controls executable delay; `expiry_ms` controls target validity. Execution prices must be subsequent visible observations strictly after the decision and satisfy latency. Do not infer intrabar paths from current complete OHLC. Fees/slippage use `manifest.costs`; `funding_policy: explicit-zero` explicitly ignores funding, while actual funding requires `required-stream`.
* Factor/mixed `trade --dry-run` is an events historical replay; pure time-series real-time simulation still uses `env: dry_run`. For live, remove `archive` and use a capability-verified built-in banexg binding or one registered through `entry.RegisterFactorLiveBinding`. Missing data, instrument units, transport or reconciliation capabilities fail startup. The built-in path currently supports production linear perpetuals with one-way net positions; exchange differences belong to banexg.
* Preserve the requested factor direction, normalization, universe, rebalance schedule, holding and exit rules. Add no unrequested risk controls or hyperparameters. Future labels are research inputs, never current selection features; full-sample optima cannot set historical parameters. Pure CLI research has no position evidence and cannot run stateful policies.

See the [expression guide](factor_expression_guide.md), [indicators](factor_indicators.md), [portfolio guide](factor_portfolio_guide.md), [live guide](factor_live_trading.md) and [API](../bandoc/en-US/api/factor.md). [Research extensions](factor_research_extensions.md) cover models, risk optimization, learned parameters and cost attribution as needed; they are optional for ordinary strategy conversion.
