以下是交易机器人banbot和指标库banta的策略接口与关键规则。你的任务是帮助用户将策略灵感或其他代码转为banbot支持的时序、截面或多因子策略；截面与多因子可使用YAML公式或Go代码。

> 接口基准：Banbot v0.6.0-beta.8。先按策略语义选择引擎，见本文末尾“截面与多因子策略”；时序新代码遵循任意时序数据订阅和 Runtime Context 规则，`OnBar` 等旧接口用于兼容。配置保留 v0.5 key 位置；参见[兼容性对比](config_compatibility.md)。

### github.com/banbox/banta
```go
// import ta "github.com/banbox/banta"

// 核心数据结构
type Kline struct {
	Time int64
	Open, High, Low, Close, Volume, Quote, BuyVolume float64
	TradeNum int64
}
type BarEnv struct {
	TimeStart, TimeStop int64
	Exchange, MarketType, Symbol, TimeFrame string
	TFMSecs int64 // 周期毫秒间隔
	BarNum, MaxCache, VNum int
	Open, High, Low, Close, Volume, Quote, BuyVolume, TradeNum *Series
	Data sync.Map; Items map[int]*Series; Lock sync.Mutex
}
func NewBarEnv(exgName, market, symbol, timeframe string) (*BarEnv, error)
func ParseTimeFrame(timeframe string) (int, error)
type Series struct {
	ID int; Env *BarEnv; Data []float64; Cols []*Series
	Time int64; More interface{}
	Subs map[string]map[int]*Series // 派生序列
	XLogs map[int]*CrossLog // 交叉记录
}
type CrossLog struct {
	Time int64; PrevVal float64
	Hist []*XState // 正数上穿，负数下穿，绝对值表示BarNum
}
type XState struct { Sign, BarNum int }

// Series核心方法
func (e *BarEnv) NewSeries(data []float64) *Series // 避免使用，应使用`To`创建子序列
func (e *BarEnv) BarCount(start int64) float64
func (s *Series) Set/Append(obj interface{}) *Series
func (s *Series) Cached() bool
func (s *Series) Get(i int) float64 // 必须>=0; 0是最新值，1表示前一个值，i表示往前第i个值
func (s *Series) Range(start, stop int) []float64
func (s *Series) RangeValid(start, stop int) ([]float64, []int)
func (s *Series) Add/Sub/Mul/Div/Min/Max(obj interface{}) *Series
func (s *Series) Abs() *Series
func (s *Series) Len() int
func (s *Series) Cut(keepNum int) // 截取历史长度
func (s *Series) Back(num int) *Series // 向前移动
func (s *Series) To(k string, v int) *Series // 获取/创建派生序列


// 交叉检测：正数上穿，负数下穿，0未知/重合；abs(ret)-1表示距离
func (s *Series) Cross(obj2 interface{}) int // obj2 must be int/float32/float64/*Series
// Deprecated: use Series.Cross instead
func Cross(se *Series, obj2 interface{}) int

func AvgPrice(e *BarEnv) *Series // (h+l+c)/3
func HL2/HLC3(h,l *Series) / (h,l,c *Series) *Series
func Sum(obj *Series, period int) *Series
func SMA/EMA/RMA/WMA/HMA(obj *Series, period int) *Series
/* EMABy 指数移动均线 最近一个权重：2/(n+1)
initType：0使用SMA初始化，1第一个有效值初始化 */
func EMABy(obj *Series, period int, initType int) *Series
/* RMABy 相对移动均线 最近一个权重：1/n
initType：0使用SMA初始化，1第一个有效值初始化
initVal 默认Nan */
func RMABy(obj *Series, period int, initType int, initVal float64) *Series


// 技术指标
func TR(high, low, close *Series) *Series // True Range
func ATR(high, low, close *Series, period int) *Series // Average True Range 建议14
func MACD(obj *Series, fast, slow, smooth int) (*Series, *Series) // 12,26,9 返回[macd,signal]
// 国际主流使用init_type=0，MyTT和中国主要使用init_type=1
func MACDBy(obj *Series, fast int, slow int, smooth int, initType int) (*Series, *Series)
func RSI/RSI50(obj *Series, period int) *Series // 建议14
// Connors RSI period:3, upDn:2, roc:100
func CRSI(obj *Series, period, upDn, roc int) *Series
// vtype: 0 TradingView, 1 ta-lib
func CRSIBy(obj *Series, period, upDn, roc, vtype int) *Series
func PercentRank(obj *Series, period int) *Series
func Highest/Lowest(obj *Series, period int) *Series
func HighestBar/LowestBar(obj *Series, period int) *Series
// 9,3,3 返回[K,D,RSV]; alias: talib STOCH indicator
func KDJ(high *Series, low *Series, close *Series, period int, sm1 int, sm2 int) (*Series, *Series, *Series)
// maBy: rma default / sma  (apply SMA/RMA to Stoch)
func KDJBy(high *Series, low *Series, close *Series, period int, sm1 int, sm2 int, maBy string) (*Series, *Series, *Series)
// talib STOCHF 对应 KDJBy返回的[K,D,RSV]中的[RSV, K]
func Stoch(high, low, close *Series, period int) *Series // 14, (close - LL)/(HH-LL) * 100; HH: HighestHigh, LL: LowestLow
func Aroon(high *Series, low *Series, period int) (*Series, *Series, *Series) // return [AroonUp, Osc, AroonDn]
func StdDev(obj *Series, period int) *Series // Standard deviation，默认ddof=0
func StdDevBy(obj *Series, period int, ddof int) (*Series, *Series) // 返回[stddev，sumVal]
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
func CTI(obj *Series, period int) *Series // 相关趋势指标 20
func CMO/CMOBy(obj *Series, period [,maType] int) *Series // 9
func CHOP(e *BarEnv, period int) *Series // 波动指数 14
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
func CCI(obj *Series, args ...interface{}) *Series // close,period 或 high,low,close,period
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
func Dpo(obj *Series, period int) *Series // DPO 别名
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
func PSAR(high, low *Series, step, max float64) *Series // SAR 别名
func DX(high, low, close *Series, period int) *Series
func AroonOsc(high, low *Series, period int) *Series
func AROONOSC(high, low *Series, period int) *Series // AroonOsc 别名
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
func LinearRegAngle(obj *Series, period int) *Series // LINEARREG_ANGLE 别名
func PivotHigh(src *Series, left, right int) *Series
func PivotLow(src *Series, left, right int) *Series
func WrapFloatArr(res *Series, period int, inVal float64) []float64 // 内部辅助函数
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
func VWAP(first, second *Series, rest ...*Series) *Series // 2参(close,volume)，4参(high,low,close,volume)；按 replay 段累计，不自动按日重置
func DMI(high, low, close *Series, period int, smoothing ...int) (*Series, *Series, *Series) // [+DI,-DI,ADX]
func STOCH(close, high, low *Series, period int) *Series // talib 参数顺序；不要与 Stoch(high,low,close,period) 混用
func DV2(h, l, c *Series, period, maLen int) *Series
func STDDEV(obj *Series, period int) *Series // StdDev 别名
func HL2(h, l *Series) *Series
func HLC3(h, l, c *Series) *Series
func SMA(obj *Series, period int) *Series
func EMA(obj *Series, period int) *Series
func RMA(obj *Series, period int) *Series
func WMA(obj *Series, period int) *Series
func HMA(obj *Series, period int) *Series
func SMMA(obj *Series, period int) *Series // RMA 别名
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
### 关键规则
* 检查有效数据长度时，可使用`e.Close.Len()`
* banta的所有代码都是每个K线执行一次，所有主周期指标应该在`OnData`的主数据处理器顶部无条件执行，指标计算完之后才可通过`if`等定义额外条件逻辑
* 创建新序列使用`Series.To`：`BarEnv.NewSeries`会无条件创建新序列，在指标内或主数据处理器中使用会导致内存泄露。而`To`内部会优先返回已存在的Series，不存在时才调用`NewSeries`。
* 当需要将收盘价Close与HighestHigh或LowesLow比较或进行Cross时，一般应先进行Back(1)，取前一个，否则收盘价永远低于HighestHigh或高于LowesLow不会触发信号

### github.com/banbox/banbot/core
```go
type Param struct {
	Name string; VType int; Min, Max, Mean float64
	IsInt bool; Rate float64 // 正态分布权重
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

下面是 com 包级兼容价格 facade。显式任务使用 Runtime.Market.Prices（PriceState.GetPriceSafeExpAt / SetPriceAt / SetPricesAt），传入本任务的 nowMS；不以这些包级函数获得多 Runtime 隔离。

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
	Url string
	Retention string
	MaxPoolSize int
	AutoCreate bool
	DbType string // "questdb" 或 "timescale"；留空时按连接信息自动识别
	SIDRegistryURL string
	QdbMemPct float64
	QdbMaxMemMB int
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
	AggRules string // JSON字段聚合规则；未配置字段默认last
}
type SeriesField struct { Name, Type, Role string }
type SeriesBinding struct {
	Table, TimeColumn, EndColumn, SIDColumn string
	Fields []SeriesField
}
type SeriesInfo struct {
	Name, TimeFrame string
	Binding SeriesBinding
}
type DataRecord struct {
	Sid int32; TimeMS, EndMS int64; Closed bool
	Values map[string]any
}
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
type SeriesFetchFunc func(ctx context.Context, target *ExSymbol, startMS, endMS int64) ([]*DataRecord, error)

func GetExSymbols/GetExSymbolMap(exgName, market string) map[int32/*string*/]*ExSymbol
func GetSymbolByID(id int32) *ExSymbol
func GetExSymbolCur(symbol string) (*ExSymbol, *errs.Error)
func GetExSymbol(exchange banexg.BanExchange, symbol string) (*ExSymbol, *errs.Error)
func GetExSymbol2(exgName, market, symbol string, exgReal ...string) *ExSymbol
func GetAllExSymbols() map[int32]*ExSymbol
func EnsureExSymbol(exchange, market, symbol string, exgReal ...string) (*ExSymbol, error)
func NewSeriesInfo(name, timeFrame string, fields []SeriesField) *SeriesInfo
func NewSeriesRepo(storage *Storage) SeriesRepo
func NewSeriesStore(repo SeriesRepo) *SeriesStore
func DefaultKlineFields() []string
func NormalizeSeriesFields(source string, fields []string) []string
func MergeSeriesFields(groups ...[]string) []string
func ResolveSeriesExSymbol(evt *DataSeries, extras ...*ExSymbol) *ExSymbol
func DefaultSeriesStore() *SeriesStore
func (s *SeriesStore) Ensure(ctx context.Context, info *SeriesInfo) *errs.Error
func (s *SeriesStore) Write(ctx context.Context, info *SeriesInfo, target *ExSymbol, row *DataRecord) *errs.Error
func (s *SeriesStore) WriteBatch(ctx context.Context, info *SeriesInfo, target *ExSymbol, rows []*DataRecord) *errs.Error
func (s *SeriesStore) WriteSeries(ctx context.Context, info *SeriesInfo, target *ExSymbol, evt *DataSeries) *errs.Error
func (s *SeriesStore) WriteSeriesBatch(ctx context.Context, info *SeriesInfo, target *ExSymbol, rows []*DataSeries) *errs.Error
func (s *SeriesStore) Read(ctx context.Context, info *SeriesInfo, target *ExSymbol, startMS, endMS int64, limit int) ([]*DataSeries, *errs.Error)
func (s *SeriesStore) Delete(ctx context.Context, info *SeriesInfo, target *ExSymbol, startMS, endMS int64) *errs.Error
func (s *SeriesStore) Missing(ctx context.Context, info *SeriesInfo, target *ExSymbol, startMS, endMS int64) ([]MSRange, *errs.Error)
func (s *SeriesStore) FillMissing(ctx context.Context, info *SeriesInfo, target *ExSymbol, startMS, endMS int64, fetch SeriesFetchFunc) *errs.Error
func (s *SeriesStore) Coverage(ctx context.Context, info *SeriesInfo, target *ExSymbol) (int64, int64, *errs.Error)
func NewKLineSeriesInfo(name, timeFrame string, fields []SeriesField) *SeriesInfo
func NewKLineSeriesStore(info *SeriesInfo) *KLineSeriesStore
func NewKLineSeriesStoreWithStorage(info *SeriesInfo, storage *Storage) *KLineSeriesStore
func (s *KLineSeriesStore) Ensure(ctx context.Context) *errs.Error
func (s *KLineSeriesStore) Write(ctx context.Context, target *ExSymbol, rows []*DataRecord) *errs.Error
func (s *KLineSeriesStore) Read(ctx context.Context, target *ExSymbol, startMS, endMS int64, limit int) ([]*DataSeries, *errs.Error)
type AggRuleFunc func(rows []*DataRecord, field SeriesField) (any, error)
func (s *ExSymbol) SetAggRules(rules map[string]string) error
func (s *ExSymbol) AggRule(col string) string
func RegisterAggRule(name string, fn AggRuleFunc) bool
```

### github.com/banbox/banbot/data
```go
type DataSink interface {
	Emit(sub *strat.DataSub, rows []*orm.DataRecord) error
}
type DataSource interface {
	Info() *orm.SeriesInfo
	FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)
	SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink DataSink) error
}
type FetchHistoryFunc func(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)
type SubscribeLiveFunc func(ctx context.Context, subs []*strat.DataSub, sink DataSink) error
type DataSourceFactory func() DataSource

func RegisterDataSource(src DataSource) error
func RegisterDataSourceFactory(name string, factory DataSourceFactory) error
func RegisterFuncDataSource(info *orm.SeriesInfo, fetch FetchHistoryFunc, subscribe SubscribeLiveFunc) error
func GetDataSource(name string) DataSource
func ListDataSources() []string
```

数据库和自定义时序数据规则：
* banbot 同时支持 TimescaleDB 和 QuestDB。可用 `database.db_type: timescale|questdb` 显式指定；留空时自动识别。业务层应使用统一 ORM，不应在策略中拼接后端专用 SQL。
* 与已有 K 线按时间戳一一对应的扩展字段，使用 `NewKLineSeriesInfo` 和 `KLineSeriesStore`；独立频率或独立结构的数据，使用 `NewSeriesInfo` 和 `SeriesStore`。
* 需要由回测/实盘自动补齐和消费的独立数据，应实现 `DataSource`，或用 `RegisterFuncDataSource` 注册历史拉取和可选实时订阅函数。
* `SeriesStore`、`KLineSeriesStore` 和数据源运行时会适配两种数据库的时间列、写入和可见性差异。K 线扩展写入只更新已存在的 `(sid, time)` 行，不会静默生成缺少 OHLCV 的行。
* `DataRecord.Values` / `DataSeries.Values` 可保存 schema 声明的任意时序字段；策略通过 `OnData`、`DataFields` 和 `DataHub` 统一读取。
* 时序聚合内置 `min`、`max`、`last`、`first`、`sum`、`avg`、`mid`。用 `ExSymbol.SetAggRules` 按字段配置；特殊语义可用 `RegisterAggRule` 注册自定义聚合函数。

任意时序的显式存储示例（由数据源、导入器或应用层调用，不要从每根 K 线的策略回调写数据库）：
```go
func saveFundingRate(ctx context.Context, storage *orm.Storage, exs *orm.ExSymbol,
	startMS, endMS int64, rate float64) error {
	info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{{Name: "rate", Type: "float"}})
	store := orm.NewSeriesStore(orm.NewSeriesRepo(storage)) // 绑定当前 Runtime 的 Storage
	if err := store.Ensure(ctx, info); err != nil { return err }
	row := &orm.DataRecord{TimeMS: startMS, EndMS: endMS, Values: map[string]any{"rate": rate}}
	if err := store.Write(ctx, info, exs, row); err != nil { return err }
	rows, err := store.Read(ctx, info, exs, startMS, endMS, 0)
	if err != nil { return err }
	if len(rows) > 0 {
		_, valueErr := rows[0].FloatValue("rate")
		return valueErr
	}
	return nil
}
```
`SeriesField.Type` 支持 `float`、`int`、`string`、`bool`、`json`。与已有 K 线一一对应的扩展列改用 `NewKLineSeriesInfo` 和 `NewKLineSeriesStoreWithStorage`；它只更新已存在的 K 线行。显式 Runtime 代码通过 `orm.NewSeriesRepo(storage)` 绑定自己的 Storage，不要依赖 `DefaultSeriesStore` 或其他包级默认连接。

策略消费独立时序的示例（`funding_rate` 必须是 runner 已注册且提供历史数据的 source 名称）：
```go
type FundingState struct { Rate float64 }

func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{{Name: "rate", Type: "float"}})
	return &strat.TradeStrat{
		WarmupNum: 50,
		OnStartUp: func(s *strat.StratJob) { s.More = &FundingState{} },
		OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			sub := strat.NewDataSub(info) // nil ExSymbol 表示当前任务品种
			sub.WarmupNum = 20
			sub.Fields = []string{"rate"}
			sub.SeriesFields = []string{"rate"} // rate 需要指标历史时才放入
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
`DataFields.Series("rate")` 提供数值历史序列，`Float64` 读取当前值；需要区分字段缺失、显式 `nil` 或保留整数精度时用 `RawValue` / `Has`。回测要有历史数据源；只有实时 `SubscribeLive` 而没有 `FetchHistory` 的 source 无法为历史区间生成这些事件。

### github.com/banbox/banbot/orm/ormo
```go
type ExitTrigger struct {
	Price, Limit, Rate float64 // 触发价格，限价，退出比例(0,1]
	Tag string // 原因
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
	WarmupNum int // 预热的K线数量，预热期间调用OpenOrder无效
	OdBarMax int // 预计订单持仓最大bar数量（用于查找回测未完成持仓），默认500
	MinTfScore float64 // 最小时间周期质量，默认0.8
	WsSubs map[string]string // WebSocket订阅配置
	DrawDownExit bool // 是否启用回撤止盈，默认false
	HedgeOff bool // 关闭合约双向持仓
	BatchInOut bool // 是否在主周期OnData(RoleMain)/OnBar后批量执行入场/出场
	BatchInfo bool // 是否在辅助周期OnData(RoleInfo)/OnInfoBar后执行批量处理
	StakeRate float64 // 相对基础金额开单倍率
	StopLoss float64 // 此策略默认止损比率，不带杠杆
	StopEnterBars int
	OrderOnRotation string // 品种切换时旧job：close、hold或open
	EachMaxLong int // max number of long open orders for one pair, -1 for disable
	EachMaxShort int // max number of short open orders for one pair, -1 for disable
	TimeFrames string // 逗号分隔的策略时间周期
	RunTimeFrames []string // 允许运行的时间周期，不提供时使用当前 Runtime 配置快照中的默认周期
	RefineTF interface{} // 指定撮合周期，如"5m"、"3-6"或5
	Outputs []string // 策略输出的文本文件内容，每个字符串是一行
	Policy *config.RunPolicyConfig
	OnPairInfos func(s *StratJob) []*PairSub // 旧接口，仅兼容已有策略；新策略用OnDataSubs
	OnDataSubs func(s *StratJob) []*DataSub
	OnSymbols func(items []string) []string // return modified pairs
	OnStartUp func(s *StratJob)
	OnBar func(s *StratJob) // 旧接口，仅兼容已有策略；不要与OnData同时配置
	OnData FnOnData
	OnInfoBar func(s *StratJob, e *ta.BarEnv, pair, tf string) // 旧接口，仅兼容已有策略；不要与OnData同时配置
	OnWsTrades func(s *StratJob, pair string, trades []*banexg.Trade) // 逐笔交易数据
	OnWsDepth func(s *StratJob, dep *banexg.OrderBook) // Websocket推送深度信息
	OnWsKline func(s *StratJob, pair string, k *banexg.Kline) // Websocket推送的实时K线
	OnWsData func(s *StratJob, evt *orm.DataSeries)
	OnBatchJobs func(jobs []*StratJob) // 当前时间所有标的job，用于批量开单/平仓
	OnBatchInfos func(tf string, jobs map[string]*JobEnv) // 当前时间所有info标的job，用于批量处理
	OnCheckExit func(s *StratJob, od *ormo.InOutOrder) *ExitReq // 自定义订单退出逻辑
	OnOrderChange func(s *StratJob, od *ormo.InOutOrder, chgType int) // 订单更新回调
	GetDrawDownExitRate func(s *StratJob, od *ormo.InOutOrder, maxChg float64) float64 // 计算跟踪止盈回撤退出的比率
	PickTimeFrame func(symbol string, tfScores []*core.TfScore) string // 为指定币选择适合的交易周期
	OnPostApi func(client *core.ApiClient, msg map[string]interface{}, jobs map[string]map[string]*StratJob) error // PostAPI时的策略回调
	OnShutDown func(s *StratJob) // 机器人停止时回调
	OnStratExit func() // 策略退出时回调
}

const ( OdChgNew = iota; OdChgEnter; OdChgEnterFill; OdChgExit; OdChgExitFill )
const ( BatchTypeInOut = iota; BatchTypeInfo )

type JobEnv struct { Job *StratJob; Env *ta.BarEnv; Symbol string }
type PairSub struct { Pair, TimeFrame string; WarmupNum int }
type DataSub struct {
	Source string
	ExSymbol *orm.ExSymbol // nil 表示当前任务品种
	TimeFrame string
	WarmupNum int
	Fields []string       // 从数据源读取的字段
	SeriesFields []string // 维护为 banta.Series 的字段；默认选择浮点字段
}
type DataRole uint8
const (
	DataRoleMain DataRole = iota + 1
	DataRoleInfo
	DataRoleCustom
)
type DataEvent struct {
	*DataFields
	Role DataRole
	Symbol *orm.ExSymbol
}
func (e DataEvent) IsMain() bool
func (e DataEvent) IsKline() bool
type FnOnData func(s *StratJob, data DataEvent)
type DataHandlers struct { Main, Info, Custom FnOnData }
func RouteData(handlers DataHandlers) FnOnData
type DataFields struct {
	DoneMS, TimeMS int64
	Source string; Sid int32; TimeFrame string
	Closed, IsWarmUp bool
}
func (d *DataFields) Series(name string) *ta.Series
func (d *DataFields) Float64(name string) float64
func (d *DataFields) Int64(name string) int64
func (d *DataFields) String(name string) string
func (d *DataFields) Raw(name string) any
func (d *DataFields) RawValue(name string) (any, bool)
func (d *DataFields) Has(name string) bool
type StratJob struct {
	Strat *TradeStrat
	Env *ta.BarEnv
	DataHub *DataHub
	Entrys []*EnterReq
	Exits []*ExitReq
	LongOrders []*ormo.InOutOrder
	ShortOrders []*ormo.InOutOrder
	Symbol *orm.ExSymbol // 当前运行的币种
	TimeFrame string // 当前运行的时间周期
	Account string // 当前任务所属账号
	TPMaxs map[int64]float64 // 订单最大盈利时价格
	OrderNum int // 所有未完成订单数量
	EnteredNum int // 已完全/部分入场的订单数量
	CheckMS int64 // 上次处理信号的时间戳，13位毫秒
	LastBarMS int64 // 上个K线的结束时间戳，13位毫秒
	MaxOpenLong int // 最大开多数量，0不限制，-1禁止开多
	MaxOpenShort int // 最大开空数量，0不限制，-1禁止开空
	CloseLong bool // 是否允许平多
	CloseShort bool // 是否允许平空
	ExgStopLoss bool // 是否允许交易所止损
	LongSLPrice float64 // 开仓时默认做多止损价格
	ShortSLPrice float64 // 开仓时默认做空止损价格
	ExgTakeProfit bool // 是否允许交易所止盈
	LongTPPrice float64 // 开仓时默认做多止盈价格
	ShortTPPrice float64 // 开仓时默认做空止盈价格
	IsWarmUp bool // 当前是否处于预热状态
	More interface{} // 每个品种独立的会被修改的额外信息，用于跨不同回调函数同步信息
}
/* EnterReq
打开一个订单。默认开多。如需开空short=False */
type EnterReq struct {
	Tag string // 入场信号
	StratName string // 策略名称，内部使用，不应赋值
	Short bool // 是否做空
	OrderType int // 订单类型, core.OrderTypeEmpty, core.OrderTypeMarket, core.OrderTypeLimit, core.OrderTypeLimitMaker
	Limit float64 // 限价单入场价格，指定时订单将作为限价单提交
	Stop float64 // 止损(触发价格)，做多订单时价格上涨到触发价格才入场（做空相反）
	CostRate float64 // 开仓倍率、默认按配置1倍。用于计算LegalCost
	LegalCost float64 // 花费法币金额。指定时忽略CostRate
	Leverage float64 // 杠杆倍数
	Amount float64 // quantity 入场标的数量，一般无需设置
	StopLossVal float64 // 入场价格到止损价格的距离，用于计算StopLoss
	StopLoss float64 // 止损触发价格，不为空时在交易所提交一个止损单
	StopLossLimit float64 // 止损限制价格，不提供使用StopLoss
	StopLossRate float64 // 止损退出比例，0表示全部退出，需介于(0,1]之间
	StopLossTag string // 止损原因
	ActivationPrice float64 // 追踪止损激活价格
	CallbackPct float64 // 追踪止损回调百分比，[0.1, 10]
	TakeProfitVal float64 // 入场价格到止盈价格的距离，用于计算TakeProfit
	TakeProfit float64 // 止盈触发价格，不为空时在交易所提交一个止盈单。
	TakeProfitLimit float64 // 止盈限制价格，不提供使用TakeProfit
	TakeProfitRate float64 // 止盈退出比率，0表示全部退出，需介于(0,1]之间
	TakeProfitTag string // 止盈原因
	StopBars int // 入场限价单超过多少个bar未成交则取消
	ClientID string // used as suffix of ClientOrderID to exchange
	Infos map[string]string
	Log bool // 是否自动记录错误日志
}
/* ExitReq
请求平仓 */
type ExitReq struct {
	Tag string // 退出信号
	StratName string // 策略名称，内部使用，不应赋值
	EnterTag string // 只退出入场信号为EnterTag的订单
	Dirt int // core.OdDirtLong / core.OdDirtShort / core.OdDirtBoth
	OrderType int // 订单类型, core.OrderTypeEmpty, core.OrderTypeMarket, core.OrderTypeLimit, core.OrderTypeLimitMaker
	Limit float64 // 限价单退出价格，指定时订单将作为限价单提交
	ExitRate float64 // 退出比率，默认0表示所有订单全部退出
	Amount float64 // quantity 要退出的标的数量，一般无需设置。指定时ExitRate无效
	OrderID int64 // 只退出指定订单
	UnFillOnly bool // True时只退出尚未入场的部分
	FilledOnly bool // True时只退出已入场的订单
	Force bool // 是否强制退出
	Log bool // 是否自动记录错误日志
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

// 核心方法
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
func (d *DataHub) Get(tf, source string, sid int32) *DataFields
func (d *DataHub) AllReady() bool
```

### 策略示例
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
	// `RunPolicyConfig.Def`定义的参数可用于后续超参数调优，第一个字符串参数必须符合常见变量命名规范
	longRate := pol.Def("longRate", 3.03, core.PNorm(1.5, 6))
	shortRate := pol.Def("shortRate", 1.03, core.PNorm(0.5, 4))
	lenAtr := pol.DefInt("atr", 20, core.PNorm(7, 40))
	// baseAtrLen 对所有品种使用同一个值，在顶部直接定义（修改与否均可）
	baseAtrLen := int(float64(lenAtr) * 4.3)
	return &strat.TradeStrat{
		WarmupNum: 100, EachMaxLong: 1,
		DrawDownExit: true, // 启用回撤止盈，仅当明确需要时才开启，否则保持默认false
		OnStartUp: func(s *strat.StratJob) {
			// goLong, atrBase是每个品种都不同的变量，会更新，需要记录到More中
			s.More = &SmaOf2{goLong: false, atrBase: 1}
		},
		OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			// 当前主周期之外，再订阅当前品种1h K线；ExSymbol为nil表示当前品种
			return []*strat.DataSub{{Source: "kline", TimeFrame: "1h", WarmupNum: 50}}
		},
		OnData: strat.RouteData(strat.DataHandlers{
			Info: func(s *strat.StratJob, data strat.DataEvent) {
				// 只处理辅助订阅，等价于原OnInfoBar逻辑
				m, _ := s.More.(*SmaOf2)
				closeSeries := data.Series("close")
				emaFast := ta.EMA(closeSeries, 20).Get(0)
				emaSlow := ta.EMA(closeSeries, 25).Get(0)
				m.goLong = emaFast > emaSlow
			},
			Main: func(s *strat.StratJob, _ strat.DataEvent) {
				// 只处理主订阅，等价于原OnBar逻辑
				e := s.Env; m, _ := s.More.(*SmaOf2)
				c := e.Close.Get(0)
				atr := ta.ATR(e.High, e.Low, e.Close, lenAtr)
				atrBase := ta.Lowest(ta.Highest(atr, lenAtr), baseAtrLen).Get(0)
				m.atrBase = atrBase
				ma5 := ta.SMA(e.Close, 5)
				ma20 := ta.SMA(e.Close, 20)
				maCross := ma5.Cross(ma20) // 1 上穿，-1 下穿，0 重合或未知，abs(maCross)-1表示交叉距离
				sma := ma20.Get(0)
				if maCross == 1 && m.goLong && sma-c > atrBase*longRate && s.OrderNum == 0 {
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
				// 订单最佳盈利超过10倍Atr后，回撤50%退出
				return 0.5
			}
			return 0
		},
	}
}
```

### 关键规则
 * v0.5 的 Runtime Context 由 runner 按运行实例创建并绑定；策略初始化签名仍为 `func(pol *config.RunPolicyConfig) *strat.TradeStrat`。策略不得自行构造或长期保存 `Runtime`、`State`，不得用可变包级变量保存订单、指标或逐品种状态。
 * 参数和其他只读策略配置在初始化函数中解析并由回调闭包共享；每个品种/任务会变化的状态在 `OnStartUp` 中初始化到 `s.More`，在回调中断言为对应状态类型。当前品种、周期、K 线和订单通过回调的 `s.Symbol`、`s.TimeFrame`、`s.Env`、`s.GetOrders`、`s.GetOrderNum`、`s.Position` 等读取。
 * 自定义数据和辅助周期回调读取传入的 `data`；自定义数据不是 K 线，不得假设 `s.Env` 对应它。跨品种汇总用 `OnBatchJobs` / `OnBatchInfos`，不要遍历包级全局 job map。历史全局 getter 若仍存在，只为旧兼容调用保留，新策略不要使用。
 * 新策略优先使用`OnData`。没有辅助或自定义订阅时可直接编写`OnData`；存在多类数据时使用`strat.RouteData(strat.DataHandlers{Main: ..., Info: ..., Custom: ...})`，保证每个事件至多进入一个处理器。
 * 迁移旧策略时不能把`OnBar`直接改名为`OnData`。手写分流时，仅当`data.Role == strat.DataRoleInfo`才执行原`OnInfoBar`逻辑并返回；随后还要用`if !data.IsMain() { return }`排除自定义数据，最后才执行原`OnBar`逻辑。更推荐使用`RouteData`分别放入`Info`和`Main`。
 * `data.IsMain()`只表示当前任务的主K线；`data.IsKline()`包含主K线和辅助K线。`DataRoleCustom`不能访问`s.Env`来假定它是OHLCV，应通过`DataFields`读取声明字段。
 * `OnData`不能与旧`OnBar`或`OnInfoBar`同时配置，策略构建会直接报错。请将主周期和辅助K线逻辑迁移到`RouteData`的`Main`和`Info`处理器；旧接口只用于未实现`OnData`的兼容策略。
 * 主数据处理器中可调用一次或多次`OpenOrder`和`CloseOrders`进行入场和出场。如果需要限制做多或做空的最大订单数量，可设置`TradeStrat`的`EachMaxLong`和`EachMaxShort`，设为1表示最多开1单，默认0不限制。
 * banbot会使用策略初始化函数 `func(pol \*config.RunPolicyConfig) \*strat.TradeStrat` 返回的策略对每一个品种创建一个策略任务`*strat.StratJob`；
 * 一些固定不变的信息一般可直接在策略初始化函数中定义好（如从pol解析的参数），然后在`OnData`等回调中直接使用。对于每个品种不同的变量信息，则应记录到`*strat.StratJob.More`中。
 * 如果需要在订单盈利后，从最大盈利回撤到一定程度自动止损，可设置`DrawDownExit`为`true`，然后传入`GetDrawDownExitRate`函数：`func(s *StratJob, od *ormo.InOutOrder, maxChg float64) float64`，返回0表示不设置止损，返回0.5表示从最大盈利回撤50%止损。其中macChg参数是订单的最大盈利，如0.1表示做多订单价格增长10%或做空订单价格下跌10%
 * 策略的主周期一般设置在外部yaml中。其他品种、周期或自定义数据使用`OnDataSubs`返回`DataSub`；辅助K线在`RouteData.Info`处理，自定义时序在`RouteData.Custom`处理。旧`OnPairInfos`仅用于兼容，内部会桥接为`Source: "kline"`的订阅。
 * 注意通过`RunPolicyConfig`解析的超参数，是固定不变的，只读的，是此策略的所有`StratJob`可以直接共享使用的，所以不要把超参数保存到结构体，更不要保存到`StratJob.More`中。More应该只记录每个品种都不同的变量。
 * 如果需要对每个订单在每个bar单独处理退出逻辑，可传入`OnCheckExit`函数，返回一个非`nil`的`ExitReq`表示对此订单平仓；
 * `StratJob.More`仅用于存储每个品种都不同、会更新、且需要在多个处理器间同步的信息（如辅助周期指标供主周期使用）。这时应实现`OnStartUp`并初始化`More`；与品种无关的只读参数直接由闭包共享。
 * 如需计算订单已持仓的bar数量，可`holdNum := s.Env.BarCount(od.EnterAt)`
 * 注意，大多数情况下可在`OnData`主数据处理器中直接实现统一的出场逻辑，无需通过`OnCheckExit`设置每个订单的出场逻辑。
 * 如果需要在订单状态发生变化时得到通知，可传入`OnOrderChange`函数，其中chgType表示订单事件类型，可能的值: `strat.OdChgNew, strat.OdChgEnter, strat.OdChgEnterFill, strat.OdChgExit, strat.OdChgExitFill`
 * 订单的止损可在调用`OpenOrder`时传入，可以设置`StopLoss/TakeProfit`为某个止损止盈价格，但更推荐的是使用`StopLossVal/TakeProfitVal`，表示止盈止损的价格范围（注意不是倍率），它能根据`Short`是否是做多/做空，以及当前最新价格，自动计算出对应的止损止盈价格。如`{Short: true, StopLossVal:atr\*2}`表示打开做空订单，使用2倍atr止损。或`{StopLossVal:price\*0.01}`表示使用价格的1%止损。
 * 单笔订单的开单数量（法币金额）是在外部yaml中配置的，golang代码中一般不需要设置，如果需要对某个订单使用非默认开单金额，可设置`EnterReq`的`CostRate`，默认为1，传入0.5表示使用平时50%的金额开单。
 * 对于`ta.BBANDS`等这样返回多列的指标[upper, mid, lower]，应使用使用多个变量接收每个返回列，如：
`bbUpp, bbMid, bbLow := ta.BBANDS(e.Close, 20, 2, 2)`
 * 不要重复调用函数，尽量保持代码精简，如下面的：
```go
 _, mid, _ := ta.BBANDS(haClose, 40, 2, 2)
 _, _, lower := ta.BBANDS(haClose, 40, 2, 2)
 ```
 应该替换为：
`_, mid, lower := ta.BBANDS(haClose, 40, 2, 2)`
 * 计算`Series`与其他`Series`或常量交叉的Cross函数应使用Series的方法：`ma1.Cross(ma2)`而不是`ta.Cross(ma1, ma2)`，后者已标记为Deprecated；
 * 订单类型使用`core.OrderType*`常量：`OrderTypeEmpty, OrderTypeMarket, OrderTypeLimit, OrderTypeLimitMaker`
 * 订单方向使用`core.OdDirt*`常量：`OdDirtLong`(做多), `OdDirtShort`(做空), `OdDirtBoth`(双向)
 * 注意请不要擅自添加额外的策略逻辑，应严格按用户输入的代码或要求实现所有需要的部分，不要额外添加用户未说明的策略逻辑。
 * 注意不要添加空函数，如果More结构体只被赋值，没有被使用，则应该删除掉。
 * 用户可能会提供策略名称，格式如"package:name"，冒号前面部分你应该提取作为返回代码中package后的go包名，冒号后面部分应作为策略函数名。如果用户未提供策略名，则使用默认"ma:demo"

## 截面与多因子策略

### 引擎与定义方式

本节用于将选股、轮动、多空排序、因子合成和持仓生命周期策略转为 banbot。前面的 `TradeStrat` / banta 回调规则适用于时序引擎；原生因子引擎使用 `factor.Plan`，由 runner 统一生成目标组合并交给账户执行。

| 场景 | 入口 |
| --- | --- |
| 单品种逐事件信号、逐订单止盈止损 | `engine: time_series`（默认），`strat.TradeStrat.OnData` |
| 同时点跨品种排名、中性化、多因子和统一调仓 | `engine: factor`，YAML `expressions` 或 Go `runner.RegisterDefinition` |
| 延续已有时序策略，只增加批量比较/下单 | `TradeStrat.BatchInOut` + `OnBatchJobs`；辅助数据批处理用 `BatchInfo` + `OnBatchInfos` |

因子策略不返回 `*strat.TradeStrat`，也不在逐品种回调中自行提交排名后的订单。YAML 中 `expressions`、`definition`、`portfolio`、`prices`、`decision`、`research` 与 `engine` 同在 `run_policy[]` 浅层；旧 `factor: {...}` 仅为兼容输入，不添加版本标记。

* 每个因子条目使用一个 `run_timeframes` 决策周期。表达式策略的 `name` 可自定；Go 策略的 `definition` 选择已注册名称，省略时使用 `name`。内置 Go definition 为 `momentum-vol`。
* `id` 标识策略，`account` 指定账户。含因子策略的共享账户有多个策略时，每个参与者都须显式给出 `capital_weight`，总和不超过 1；它分配策略预算，与时序 `stake_rate` 的开单倍率不同。
* `run_policy.params.k` / `portfolio.k` 用于旧选股器；`expressions.params` 仅供 `param.name` 使用，不自动继承外层参数。Go builder 从 `c.Manifest.Parameters` 读外层 `params`。

### YAML 多因子示例

以下是权重回测配置；`data.gob` 须为含至少六个资产、足够预热历史及后续可见价格的归档。示例按动量和低波动合成分数，最高/最低各选三只，名义敞口各占策略 NAV 的 50%。

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

`banbot backtest --mode weights --config strategy.yml` 运行权重回测。使用普通行情库时移除 `archive`，合并数据库、市场、品种池和 `time_range` 基础配置，并显式设置 `data: {pit_policy: static-approximation}`；最新值历史库不能证明严格 PIT（决策时点可见性）。

研究时移除示例的 `research: {labels: []}`，默认产生一个决策周期的收益标签；用 `banbot research --config strategy.yml` 计算研究结果。多期限写 `research.labels: [{name: forward_16h, kind: executable-return, horizon: 57600000, periods_per_year: 547.5, overlapping: true}]`，`horizon` 为毫秒；未成熟标签保留为 unresolved，不填零。历史合成方法要求标签，不能关闭研究标签。

### 表达式语法与函数

`alias.field` 读取 binding 字段，`field("alias","field-name")` 支持特殊字段名；`factor.name` 引用当前 `lets` / `outputs`，`param.name` 引用数值参数。可前向引用，不允许环；未使用声明仍校验。支持数字、科学计数法、括号、一元正负号、四则运算和以下函数；没有比较、条件分支或未来 `label.*` 语法。

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

* 窗口参数是整数常量或直接的 `param.name`，范围 1–10000；`lag` 另允许 0，不能写 `param.window+1`。`ddof` 必须满足 `0 <= ddof < n`；MACD 要求 `fast < slow`；布林带上下倍数均须显式给出且非负。
* `cs.rank` 为升序、0 起始名次，并列取平均名次，不是百分位；`cs.zscore` 使用总体标准差，常数截面输出 0。`tail` 在 `[0,0.5)`，`q` 在 `[0,1]`，MAD 倍数须为正；robust z-score 使用 `1.4826*MAD`。
* 所有截面/回归统计在冻结的 `Universe.Reference` 上拟合；OLS/WLS 含截距，WLS 需要正权重。分组字段保留原始类型/NULL，公式中的 source 是 binding 别名。
* 可以先时序再截面，例如 `cs.rank(ts.rsi(kline.close,14))`；不支持对截面/回归结果再做时序窗口，即使包在逐点函数中也不行，零 lag 原值例外。
* `ts.return` 返回比例，`ts.roc` 返回百分比。`max(x,1e-8)` 只对有效值设置下界，不补 NULL、缺字段或预热不足。新技术指标按完整有效输入元组推进；原有 lag/return/ema/std 保持各自语义。Go `factor.MACD/BBands` 返回三个节点，与前面的 banta 返回类型/列数分别看待。

慢频或 event 源必须显式 asof 采样到决策周期，并限制数据年龄。例如在 `bindings` 增加 `funding: {source: funding, timeframe: event, sampling: asof, max_age_ms: 28800000}`，即可使用 `cs.zscore(-funding.rate)`；真实输入须提供该流。asof 后的窗口按决策观察次数计数，小时网格的 24 个观察不等于 24 个交易日。

将 `expressions` 内的映射单独保存为 `formula.yml`，用 `banbot validate --spec formula.yml` / `banbot explain --spec formula.yml` 检查编译与依赖。它们不验证真实数据或账户；独立文件必须显式 `timeframe`、单 YAML 文档且不超过 1 MiB。

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

下面是策略构图所需的公开接口；`Node`、`Builder`、`Plan` 的内部实现不在策略中重写。`Node` 接受数值节点，不接受 banta `*Series`。

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

`Custom` 是有版本、显式依赖的纯逐点函数；回调必须传播输入无效原因，不读取账户/未来行情，不在闭包保存滚动状态。改变实现需改变 version。缺少原生历史算子时须扩展内核并验证 Session/Batch 语义，不能用 Custom 隐藏状态。

冻结输入与计算结果（一般由 runner 装配；手工数值验证时使用）：

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

`Investable` 是候选池，`Reference` 是统计池，`Tradable` 是可交易池，`Evaluation` 是研究池，`Tracked` 用于退出后仍需监控的资产。`Frame.Values[column][sid]` 是数值视图；源数据一直保留 `orm.DataSeries.Values map[string]any` 的任意字段、类型、缺失和 NULL。

`AvailableAt` 为源发布/可见时间，`IngestedAt` 为接收时间，`Revision` 标识修订；冻结只能选择当时可见且已接收的版本。闭合数据不足或屏障未齐时跳过该决策，不用缺席资产临时改变声明池。Session 跨历史分块连续推进，Batch 从传入历史起点初始化；已经处理的过去修订应另建回放，不改写已冻结结果。`WarmupLength()` 是无缺失条件下所需前置观察数，不保证经过相同数量的日历 bar 就有效。

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

`equal` 对所选列等权，默认选全部输出；`fixed` 按权重直接相加，允许负数且不自动归一化。非零权重列失效会令该资产 score 失效，不临时重分配权重；零权重列不影响有效性。组合列必须存在且不重复，避免把原始输出命名为 runner 生成的 `score`。显式外层 `combo.method` 覆盖完整内层组合配置。

历史方法为 `history-ic/history-rank-ic/history-icir/history-rank-icir/history-ewma`，只使用已成熟且决策时可见的样本。`label` 选期限；多标签省略时取最短 horizon、同期限按名称排序。`min_samples` 统计历史截面数，`min_pairs` 是每截面资产数，`min_confidence` 为均值/标准误门槛；EWMA `decay` 是 alpha，零值默认 0.2；`direction: signed|positive`，`fallback: equal|fixed|error`。当前 live 拒绝全部历史合成方法。

### 目标组合与持仓策略（factor）

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

权重正数做多、负数做空；`FrozenBudget.NAV` 是策略净值。`Full` 将本策略旧目标中省略的资产归零，`Patch` 保留省略目标，显式零表示清仓。默认 `TopBottomK` 按分数两端选股，要求至少 `2*k` 个有效、可投资且可交易候选，即使某侧 notional 为零；不足或分数全相同返回 nil 目标和诊断，runner 跳过替换并保留旧组合。直接调用者须检查目标非 nil。纯多头需仅按单侧数量选股时使用 lifecycle selection 或自定义 builder。

`TargetPortfolio` 是理想权重；`PortfolioTarget` 可混合 NAV 权重和绝对数量。`Allocation.Value` 是有符号普通十进制字符串，不接受空值或科学计数法；`AbsoluteQuantity` 使用标准资产数量而非合约张数，`AsWeightPortfolio` 遇到数量目标会报错。EffectiveTargets/EffectiveAllocations 校验策略、账户、币种及递增计划序号。

`portfolio.policy: lifecycle-v1` 启用调仓与持仓生命周期。例如用下段替换示例的 portfolio：

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

每小时计算、每两小时普通调仓；首次实际成交满 16 小时后，只有排名落选才进入八步退出。`holding.max_bars/max_duration` 到期则在监控网格强制零目标，绕过普通调仓门和退出曲线；与“满期才开始渐退”的 min 不同。以下字段类型也适用于 Go policy：

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

* `rebalance` 支持 every_bars、anchor/phase、duration 或 calendar/timezone/calendar_version，不混用日程。`selection` 支持两侧 K 或 quantile、retain_rank 缓冲、dropout 换仓数和 group_quota。
* `transition.mode` 支持 direct、linear-exit（exit_steps）、cohort（period_bars 为 every_bars 整数倍）、geometric（ratio/final_threshold）、target-step（alpha）。退出 basis 为 quantity 或 weight；quantity 锚定已确认数量，weight 每轮依最新 NAV 换算，可能回补。
* cohort 的 `startup: gradual|seed-all`、`sizing: entry-nav|current-nav` 显式选择批次预算；`on_reselect: restore|resume|finish|new-cohort` 控制重选。计划批次窗口不等于所有成交订单的真实持仓时长。
* `holding.by_asset` / `transition.by_asset` 使用 SIDMap 资产名覆盖默认值；holding 指针区分省略与显式零。缺首次成交证据时不猜年龄，明确接管才设 `adopt: adopt`。
* `allocation.method` 支持 equal、score、fixed-notional、inverse-volatility、vol-target；Groups、Volatility、Beta、可见参数规则及自定义交易日程由 `Config.PolicyContext` 注入，不能把同名因子列当作隐式风险证据。
* policy 每 run 创建私有实例，只读冻结 context 和已接纳 JSON 状态，返回目标、下一状态和诊断，不直接提交订单或写账户。账户原子接纳后才能推进状态；已接纳但发送失败不能回滚或重复生成新计划。保留在途/部分成交证据，反手等待原方向实际归零。

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

Go definition 编译图并返回合成规则；无状态 PortfolioBuilder 接收包含原始输出及合成 `score` 的冻结 Frame 并返回理想权重。完整自定义 policy 用带版本名（如 `rotation-v1`）注册，YAML 通过 `portfolio.builder` / `portfolio.policy` 选择；策略参数进入身份时使用不可变版本/配置 hash。definition 及 builder 改动后须重新编译程序。

builder 使用的配置字段（只列策略侧字段，省略账户/输入装配字段）：

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

### Go 多因子示例

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

将包编入策略程序并导入以执行 init；主入口调用 `entry.RunCmd()`。前面的 YAML 保留展示名称，增加 `definition: MultiFactorV1`、将 `params` 改为 `{window: 24, k: 3}` 并移除 `expressions`，即可使用同一组合/价格配置。统一命令入口提供 `c.Factor` 的 kline/close/决策周期默认值；手工调用 builder 须自行提供。

### 运行与策略转换规则

* 表达式和 Go definition 生成同种 Plan；表达式不能同时设置 definition 或 Go Config.Plan。直接传 Plan 还需显式 Combo；策略不自行重写屏障、资金账本或交易所适配。
* `weights` 用于权重近似回放；普通 `backtest` 默认 events（可由 `--mode` / `execution.mode` 覆盖），混合时序/因子回放必须 events。events 和 live 使用独立 tick/event 或 1m 可见执行价格、合约单位与账户证据，小时/日因子 K 线不能代替成交流。
* `decision.delay_ms` 控制数据可见性截止，`latency_ms` 控制可执行延迟，`expiry_ms` 控制目标有效期。成交需使用严格晚于决策并满足延迟的后续可见价格，不按当前完整 OHLC 虚构盘中路径。费用/滑点用 `manifest.costs`；`funding_policy: explicit-zero` 是显式忽略资金费，真实费用需要 required-stream。
* `trade --dry-run` 对因子/混合是 events 历史回放；纯时序实时模拟仍用 `env: dry_run`。实盘移除 archive，使用经能力验证的内置 banexg 或 `entry.RegisterFactorLiveBinding` 注册绑定，缺数据、执行单位、transport 或对账能力会失败。当前内置路径支持生产环境线性永续、单向净持仓；策略不写交易所特例。
* 保留原策略的因子方向、标准化、选股池、调仓日程、持仓和退出语义；未要求的风险控制或超参数不额外添加。未来标签用于研究，不能进入当期选股或从全样本最优反推历史参数。纯 CLI research 无真实持仓证据，不能使用有状态 policy。

更完整用法见[表达式指南](factor_expression_guide.md)、[指标](factor_indicators.md)、[组合与持仓](factor_portfolio_guide.md)、[实盘](factor_live_trading.md)和[API](../bandoc/zh-CN/api/factor.md)。模型、风险优化、参数学习、成本归因等按需查看[研究扩展](factor_research_extensions.md)；这些扩展不作为普通策略转换的默认步骤。
