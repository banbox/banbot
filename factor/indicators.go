package factor

import (
	"fmt"
	"math"
	"strings"
)

// MaxIndicatorPeriod bounds the history needed by technical indicator nodes.
const MaxIndicatorPeriod = 10_000

func technicalNode(operator string, inputs ...*Node) *Node {
	n := node(operator, TS, inputs...)
	n.Spec.Version = "indicators-1/banta-0.4.1"
	return n
}

func periodIndicator(operator string, period int, inputs ...*Node) *Node {
	n := technicalNode(operator, inputs...)
	n.Spec.Parameters["period"] = float64(period)
	return n
}

// SMA computes the arithmetic mean of the last period valid observations.
func SMA(input *Node, period int) *Node { return periodIndicator("sma", period, input) }

// RMA uses Wilder smoothing, seeded with a period-observation arithmetic mean.
func RMA(input *Node, period int) *Node { return periodIndicator("rma", period, input) }

// WMA weights the oldest observation by 1 and the newest by period.
func WMA(input *Node, period int) *Node { return periodIndicator("wma", period, input) }

// VWMA averages price with volume weights over valid price/volume pairs.
func VWMA(price, volume *Node, period int) *Node {
	return periodIndicator("vwma", period, price, volume)
}

// RSI is Wilder's relative strength index on a 0–100 scale.
func RSI(input *Node, period int) *Node { return periodIndicator("rsi", period, input) }

// ROC returns percentage change; Return returns fractional change.
func ROC(input *Node, period int) *Node { return periodIndicator("roc", period, input) }

// MOM returns the difference from the observation period valid samples ago.
func MOM(input *Node, period int) *Node { return periodIndicator("mom", period, input) }

// TR requires a previous valid close, so its first observation is warmup.
func TR(high, low, close *Node) *Node { return technicalNode("tr", high, low, close) }

// ATR smooths true range with Wilder's period-observation RMA.
func ATR(high, low, close *Node, period int) *Node {
	return periodIndicator("atr", period, high, low, close)
}

// CCI accepts a price expression, commonly (high + low + close) / 3.
func CCI(input *Node, period int) *Node { return periodIndicator("cci", period, input) }

// Stoch returns raw stochastic %K, without additional K/D smoothing.
func Stoch(high, low, close *Node, period int) *Node {
	return periodIndicator("stoch", period, high, low, close)
}

// WillR returns Williams %R on the conventional -100–0 scale.
func WillR(high, low, close *Node, period int) *Node {
	return periodIndicator("willr", period, high, low, close)
}

// OBV starts with the first valid volume and adds/subtracts subsequent volumes
// according to the close change. Equal closes leave the value unchanged.
func OBV(close, volume *Node) *Node { return technicalNode("obv", close, volume) }

// MFI returns the money flow index using typical price and volume.
func MFI(high, low, close, volume *Node, period int) *Node {
	return periodIndicator("mfi", period, high, low, close, volume)
}

func Highest(input *Node, period int) *Node { return periodIndicator("highest", period, input) }
func Lowest(input *Node, period int) *Node  { return periodIndicator("lowest", period, input) }

// DecayLinear is a semantic alias for WMA (linearly decaying weights 1..period).
func DecayLinear(input *Node, period int) *Node { return WMA(input, period) }

// TsCorr computes rolling Pearson correlation over complete valid (x, y) pairs.
// Zero variance on either series yields 0.
func TsCorr(x, y *Node, period int) *Node { return periodIndicator("ts-corr", period, x, y) }

// TsCov computes rolling population covariance over complete valid (x, y) pairs.
func TsCov(x, y *Node, period int) *Node { return periodIndicator("ts-cov", period, x, y) }

// TsBeta computes rolling OLS slope Cov(y, x) / Var(x) over complete valid pairs.
// Zero variance on x yields 0.
func TsBeta(y, x *Node, period int) *Node { return periodIndicator("ts-beta", period, y, x) }

// TsSum computes the rolling sum of the last period valid observations.
func TsSum(input *Node, period int) *Node { return periodIndicator("ts-sum", period, input) }

// TsProduct computes the rolling product of the last period valid observations.
func TsProduct(input *Node, period int) *Node { return periodIndicator("ts-product", period, input) }

// TsRank computes the normalized rank in [0, 1] of the latest observation within
// the last period valid observations (average ties; period == 1 yields 0.5).
func TsRank(input *Node, period int) *Node { return periodIndicator("ts-rank", period, input) }

// TsZScore standardizes the latest observation by the rolling mean and population
// standard deviation over period valid observations (zero std yields 0).
func TsZScore(input *Node, period int) *Node { return periodIndicator("ts-zscore", period, input) }

// TsArgMax returns the offset in 0..period-1 from the latest valid observation
// to the window maximum (0 when the latest observation is the maximum).
func TsArgMax(input *Node, period int) *Node { return periodIndicator("ts-argmax", period, input) }

// TsArgMin returns the offset in 0..period-1 from the latest valid observation
// to the window minimum (0 when the latest observation is the minimum).
func TsArgMin(input *Node, period int) *Node { return periodIndicator("ts-argmin", period, input) }

// TsSkew computes rolling unbiased sample skewness (requires period >= 3; zero std yields 0).
func TsSkew(input *Node, period int) *Node { return periodIndicator("ts-skew", period, input) }

// TsKurt computes rolling unbiased sample excess kurtosis (requires period >= 4; zero std yields 0).
func TsKurt(input *Node, period int) *Node { return periodIndicator("ts-kurt", period, input) }

// TsSlope computes the OLS slope against equally spaced time index 0..period-1.
func TsSlope(input *Node, period int) *Node { return periodIndicator("ts-slope", period, input) }

// TsRSquare computes the OLS R-squared in [0, 1] against time index 0..period-1 (zero variance yields 0).
func TsRSquare(input *Node, period int) *Node { return periodIndicator("ts-rsquare", period, input) }

// MACD returns the fast-minus-slow EMA line, its signal EMA, and line-minus-
// signal histogram. Each output is an independently usable scalar DAG node.
func MACD(input *Node, fast, slow, signal int) (line, signalLine, hist *Node) {
	makeNode := func(operator string) *Node {
		n := technicalNode(operator, input)
		n.Spec.Parameters = map[string]float64{"fast": float64(fast), "slow": float64(slow), "signal": float64(signal)}
		return n
	}
	return makeNode("macd"), makeNode("macd-signal"), makeNode("macd-hist")
}

// BBands returns SMA plus stdUp population standard deviations, SMA, and SMA
// minus stdDown deviations. Deviation multipliers must be finite and >= 0.
func BBands(input *Node, period int, stdUp, stdDown float64) (upper, middle, lower *Node) {
	makeNode := func(operator string) *Node {
		n := periodIndicator(operator, period, input)
		n.Spec.Parameters["std_up"], n.Spec.Parameters["std_down"] = stdUp, stdDown
		return n
	}
	return makeNode("bbands-upper"), makeNode("bbands-middle"), makeNode("bbands-lower")
}

// DEMA computes the double exponential moving average 2*EMA(x, period) - EMA(EMA(x, period), period).
func DEMA(input *Node, period int) *Node { return periodIndicator("dema", period, input) }

// TEMA computes the triple exponential moving average 3*EMA1 - 3*EMA2 + EMA3.
func TEMA(input *Node, period int) *Node { return periodIndicator("tema", period, input) }

// HMA computes Hull Moving Average WMA(2*WMA(x, period/2) - WMA(x, period), floor(sqrt(period))) (requires period >= 2).
func HMA(input *Node, period int) *Node { return periodIndicator("hma", period, input) }

// KAMA computes Kaufman's Adaptive Moving Average with standard fast=2, slow=30 smoothing constants.
func KAMA(input *Node, period int) *Node { return periodIndicator("kama", period, input) }

// KAMABy computes Kaufman's Adaptive Moving Average with explicit period, fast, and slow smoothing lengths (fast < slow).
func KAMABy(input *Node, period, fast, slow int) *Node {
	n := periodIndicator("kama-by", period, input)
	n.Spec.Parameters["fast"] = float64(fast)
	n.Spec.Parameters["slow"] = float64(slow)
	return n
}

// ALMA computes Arnaud Legoux Moving Average with Gaussian center offset in [0, 1] and sigma > 0.
func ALMA(input *Node, period int, offset, sigma float64) *Node {
	n := periodIndicator("alma", period, input)
	n.Spec.Parameters["offset"] = offset
	n.Spec.Parameters["sigma"] = sigma
	return n
}

// TRIMA computes the triangular moving average SMA(SMA(x, ceil(period/2)), floor(period/2)+1).
func TRIMA(input *Node, period int) *Node { return periodIndicator("trima", period, input) }

// ER computes Kaufman's Efficiency Ratio |x[t] - x[t-period]| / sum(|x[i] - x[i-1]|, period).
func ER(input *Node, period int) *Node { return periodIndicator("er", period, input) }

// Kalman1D computes a 1D random-walk Kalman filter state estimate (processNoise > 0, measNoise > 0).
func Kalman1D(input *Node, processNoise, measNoise float64) *Node {
	n := technicalNode("kalman1d", input)
	n.Spec.Parameters["process_noise"] = processNoise
	n.Spec.Parameters["meas_noise"] = measNoise
	return n
}

// Hurst computes the rolling rescaled-range (R/S) Hurst exponent in [0, 1] (requires period >= 8).
func Hurst(input *Node, period int) *Node { return periodIndicator("hurst", period, input) }

// ADX computes Wilder's Average Directional Index on a 0–100 scale.
func ADX(high, low, close *Node, period int) *Node {
	return periodIndicator("adx", period, high, low, close)
}

// PlusDI computes Wilder's +DI directional indicator on a 0–100 scale.
func PlusDI(high, low, close *Node, period int) *Node {
	return periodIndicator("dmi-plus", period, high, low, close)
}

// MinusDI computes Wilder's -DI directional indicator on a 0–100 scale.
func MinusDI(high, low, close *Node, period int) *Node {
	return periodIndicator("dmi-minus", period, high, low, close)
}

// DMI returns +DI, -DI, and ADX as independent scalar DAG nodes.
func DMI(high, low, close *Node, period int) (plusDI, minusDI, adx *Node) {
	return PlusDI(high, low, close, period), MinusDI(high, low, close, period), ADX(high, low, close, period)
}

// NATR computes Normalized Average True Range 100 * ATR(period) / close.
func NATR(high, low, close *Node, period int) *Node {
	return periodIndicator("natr", period, high, low, close)
}

// CHOP computes the Choppiness Index 100 * log10(sum(ATR(1), period) / (highest - lowest)) / log10(period) (requires period >= 2).
func CHOP(high, low, close *Node, period int) *Node {
	return periodIndicator("chop", period, high, low, close)
}

// AroonUp computes Aroon Up in [0, 100] over a (period+1)-observation window.
func AroonUp(high, low *Node, period int) *Node {
	return periodIndicator("aroon-up", period, high, low)
}

// AroonDown computes Aroon Down in [0, 100] over a (period+1)-observation window.
func AroonDown(high, low *Node, period int) *Node {
	return periodIndicator("aroon-down", period, high, low)
}

// AroonOsc computes the Aroon Oscillator (AroonUp - AroonDown) in [-100, 100].
func AroonOsc(high, low *Node, period int) *Node {
	return periodIndicator("aroon-osc", period, high, low)
}

// Aroon returns AroonUp, AroonDown, and AroonOsc as independent scalar DAG nodes.
func Aroon(high, low *Node, period int) (up, down, osc *Node) {
	return AroonUp(high, low, period), AroonDown(high, low, period), AroonOsc(high, low, period)
}

// VortexPlus computes the positive Vortex movement VI+ over period observations.
func VortexPlus(high, low, close *Node, period int) *Node {
	return periodIndicator("vortex-plus", period, high, low, close)
}

// VortexMinus computes the negative Vortex movement VI- over period observations.
func VortexMinus(high, low, close *Node, period int) *Node {
	return periodIndicator("vortex-minus", period, high, low, close)
}

// Vortex returns VI+ and VI- as independent scalar DAG nodes.
func Vortex(high, low, close *Node, period int) (plus, minus *Node) {
	return VortexPlus(high, low, close, period), VortexMinus(high, low, close, period)
}

// DonchianUpper computes the rolling period-bar high channel upper bound.
func DonchianUpper(high, low *Node, period int) *Node {
	return periodIndicator("donchian-upper", period, high, low)
}

// DonchianMid computes the midpoint (DonchianUpper + DonchianLower) / 2.
func DonchianMid(high, low *Node, period int) *Node {
	return periodIndicator("donchian-mid", period, high, low)
}

// DonchianLower computes the rolling period-bar low channel lower bound.
func DonchianLower(high, low *Node, period int) *Node {
	return periodIndicator("donchian-lower", period, high, low)
}

// Donchian returns upper, mid, and lower Donchian channel nodes.
func Donchian(high, low *Node, period int) (upper, middle, lower *Node) {
	return DonchianUpper(high, low, period), DonchianMid(high, low, period), DonchianLower(high, low, period)
}

func keltnerBand(operator string, high, low, close *Node, period int, multiplier float64) *Node {
	n := periodIndicator(operator, period, high, low, close)
	n.Spec.Parameters["mult"] = multiplier
	return n
}

// KeltnerUpper computes EMA(close, period) + multiplier * ATR(high, low, close, period) (multiplier >= 0).
func KeltnerUpper(high, low, close *Node, period int, multiplier float64) *Node {
	return keltnerBand("keltner-upper", high, low, close, period, multiplier)
}

// KeltnerMid computes the Keltner Channel center EMA(close, period).
func KeltnerMid(high, low, close *Node, period int, multiplier float64) *Node {
	return keltnerBand("keltner-mid", high, low, close, period, multiplier)
}

// KeltnerLower computes EMA(close, period) - multiplier * ATR(high, low, close, period) (multiplier >= 0).
func KeltnerLower(high, low, close *Node, period int, multiplier float64) *Node {
	return keltnerBand("keltner-lower", high, low, close, period, multiplier)
}

// Keltner returns upper, mid, and lower Keltner Channel scalar nodes.
func Keltner(high, low, close *Node, period int, multiplier float64) (upper, middle, lower *Node) {
	return KeltnerUpper(high, low, close, period, multiplier),
		KeltnerMid(high, low, close, period, multiplier),
		KeltnerLower(high, low, close, period, multiplier)
}

// Supertrend computes the ratcheted ATR-band Supertrend trailing stop line (multiplier > 0).
func Supertrend(high, low, close *Node, period int, multiplier float64) *Node {
	n := periodIndicator("supertrend", period, high, low, close)
	n.Spec.Parameters["mult"] = multiplier
	return n
}

// SupertrendDir computes the Supertrend regime direction (+1 bullish, -1 bearish; multiplier > 0).
func SupertrendDir(high, low, close *Node, period int, multiplier float64) *Node {
	n := periodIndicator("supertrend-dir", period, high, low, close)
	n.Spec.Parameters["mult"] = multiplier
	return n
}

// SAR computes Wilder's Parabolic SAR with initial AF start > 0, increment inc > 0, and cap max >= start.
func SAR(high, low *Node, start, inc, max float64) *Node {
	n := technicalNode("sar", high, low)
	n.Spec.Parameters["start"] = start
	n.Spec.Parameters["inc"] = inc
	n.Spec.Parameters["max"] = max
	return n
}

// STC computes the Schaff Trend Cycle in [0, 100] with fast < slow EMA lengths and stochastic cycle window.
func STC(input *Node, fast, slow, cycle int) *Node {
	n := technicalNode("stc", input)
	n.Spec.Parameters = map[string]float64{"fast": float64(fast), "slow": float64(slow), "cycle": float64(cycle)}
	return n
}

// IchimokuConversion computes the Ichimoku Tenkan-sen midpoint (Highest(high, period) + Lowest(low, period)) / 2.
func IchimokuConversion(high, low *Node, period int) *Node {
	return periodIndicator("ichimoku-conversion", period, high, low)
}

// IchimokuBase computes the Ichimoku Kijun-sen midpoint (Highest(high, period) + Lowest(low, period)) / 2.
func IchimokuBase(high, low *Node, period int) *Node {
	return periodIndicator("ichimoku-base", period, high, low)
}

// CMF computes Chaikin Money Flow over period positive-volume bars.
func CMF(high, low, close, volume *Node, period int) *Node {
	return periodIndicator("cmf", period, high, low, close, volume)
}

// ADL computes the cumulative Accumulation/Distribution Line from the first valid OHLCV tuple.
func ADL(high, low, close, volume *Node) *Node {
	return technicalNode("adl", high, low, close, volume)
}

// ChaikinOsc computes the Chaikin Oscillator EMA(ADL, fast) - EMA(ADL, slow) (requires fast < slow).
func ChaikinOsc(high, low, close, volume *Node, fast, slow int) *Node {
	n := technicalNode("adosc", high, low, close, volume)
	n.Spec.Parameters = map[string]float64{"fast": float64(fast), "slow": float64(slow)}
	return n
}

// ADOSC is an alias for ChaikinOsc.
func ADOSC(high, low, close, volume *Node, fast, slow int) *Node {
	return ChaikinOsc(high, low, close, volume, fast, slow)
}

// VolumeDelta computes single-bar net taker buy volume: 2*buyVolume - volume.
func VolumeDelta(volume, buyVolume *Node) *Node {
	return technicalNode("volume-delta", volume, buyVolume)
}

// CVD computes Cumulative Volume Delta by accumulating 2*buyVolume - volume from the first valid tuple.
func CVD(volume, buyVolume *Node) *Node {
	return technicalNode("cvd", volume, buyVolume)
}

// RollingCVD computes the rolling sum of VolumeDelta (2*buyVolume - volume) over period valid tuples.
func RollingCVD(volume, buyVolume *Node, period int) *Node {
	return periodIndicator("rolling-cvd", period, volume, buyVolume)
}

// TakerBuyRatio computes rolling sum(buyVolume) / sum(volume) in [0, 1] over period valid non-negative tuples.
func TakerBuyRatio(volume, buyVolume *Node, period int) *Node {
	return periodIndicator("taker-buy-ratio", period, volume, buyVolume)
}

// ParkinsonVol computes Parkinson's high-low range volatility estimator over period positive bars.
func ParkinsonVol(high, low *Node, period int) *Node {
	return periodIndicator("parkinson-vol", period, high, low)
}

// GarmanKlassVol computes Garman-Klass OHLC range volatility estimator over period positive bars.
func GarmanKlassVol(open, high, low, close *Node, period int) *Node {
	return periodIndicator("garman-klass-vol", period, open, high, low, close)
}

// YangZhangVol computes Yang-Zhang drift-independent OHLC volatility estimator (requires period >= 2).
func YangZhangVol(open, high, low, close *Node, period int) *Node {
	return periodIndicator("yang-zhang-vol", period, open, high, low, close)
}

type indicatorContract struct {
	arity      int
	parameters []string
	minPeriod  int
}

func indicatorDefinition(operator string) (indicatorContract, bool) {
	switch operator {
	case "sma", "rma", "wma", "rsi", "roc", "mom", "cci", "highest", "lowest",
		"dema", "tema", "kama", "trima", "er",
		"ts-sum", "ts-product", "ts-rank", "ts-zscore", "ts-argmax", "ts-argmin", "ts-slope", "ts-rsquare":
		return indicatorContract{1, []string{"period"}, 1}, true
	case "hma":
		return indicatorContract{1, []string{"period"}, 2}, true
	case "ts-skew":
		return indicatorContract{1, []string{"period"}, 3}, true
	case "ts-kurt":
		return indicatorContract{1, []string{"period"}, 4}, true
	case "hurst":
		return indicatorContract{1, []string{"period"}, 8}, true
	case "kama-by":
		return indicatorContract{1, []string{"period", "fast", "slow"}, 1}, true
	case "alma":
		return indicatorContract{1, []string{"period", "offset", "sigma"}, 1}, true
	case "kalman1d":
		return indicatorContract{1, []string{"process_noise", "meas_noise"}, 0}, true
	case "stc":
		return indicatorContract{1, []string{"fast", "slow", "cycle"}, 1}, true
	case "vwma", "ts-corr", "ts-cov", "ts-beta",
		"aroon-up", "aroon-down", "aroon-osc",
		"donchian-upper", "donchian-mid", "donchian-lower",
		"ichimoku-conversion", "ichimoku-base",
		"rolling-cvd", "taker-buy-ratio", "parkinson-vol":
		return indicatorContract{2, []string{"period"}, 1}, true
	case "obv", "volume-delta", "cvd":
		return indicatorContract{2, nil, 0}, true
	case "sar":
		return indicatorContract{2, []string{"start", "inc", "max"}, 0}, true
	case "tr":
		return indicatorContract{3, nil, 0}, true
	case "atr", "stoch", "willr", "adx", "dmi-plus", "dmi-minus", "natr", "vortex-plus", "vortex-minus":
		return indicatorContract{3, []string{"period"}, 1}, true
	case "chop":
		return indicatorContract{3, []string{"period"}, 2}, true
	case "keltner-upper", "keltner-mid", "keltner-lower", "supertrend", "supertrend-dir":
		return indicatorContract{3, []string{"period", "mult"}, 1}, true
	case "adl":
		return indicatorContract{4, nil, 0}, true
	case "mfi", "cmf", "garman-klass-vol":
		return indicatorContract{4, []string{"period"}, 1}, true
	case "yang-zhang-vol":
		return indicatorContract{4, []string{"period"}, 2}, true
	case "adosc":
		return indicatorContract{4, []string{"fast", "slow"}, 1}, true
	case "macd", "macd-signal", "macd-hist":
		return indicatorContract{1, []string{"fast", "slow", "signal"}, 1}, true
	case "bbands-upper", "bbands-middle", "bbands-lower":
		return indicatorContract{1, []string{"period", "std_up", "std_down"}, 1}, true
	default:
		return indicatorContract{}, false
	}
}

func validateIndicator(spec NodeSpec, count int, contract indicatorContract) error {
	if count != contract.arity {
		return fmt.Errorf("factor: %s requires %d dependencies", spec.Operator, contract.arity)
	}
	if !spec.Batch {
		return fmt.Errorf("factor: %s requires incremental and batch capability", spec.Operator)
	}
	if len(spec.Parameters) != len(contract.parameters) {
		return fmt.Errorf("factor: %s requires exactly parameters %v", spec.Operator, contract.parameters)
	}
	minPeriod := max(1, contract.minPeriod)
	for _, name := range contract.parameters {
		value, exists := spec.Parameters[name]
		if !exists {
			return fmt.Errorf("factor: %s requires parameter %s", spec.Operator, name)
		}
		switch name {
		case "std_up", "std_down":
			if value < 0 {
				return fmt.Errorf("factor: %s must be nonnegative", name)
			}
		case "mult":
			if spec.Operator == "supertrend" || spec.Operator == "supertrend-dir" {
				if value <= 0 {
					return fmt.Errorf("factor: %s must be positive", name)
				}
			} else if value < 0 {
				return fmt.Errorf("factor: %s must be nonnegative", name)
			}
		case "offset":
			if value < 0 || value > 1 {
				return fmt.Errorf("factor: offset must be in [0,1]")
			}
		case "sigma", "process_noise", "meas_noise", "start", "inc", "max":
			if value <= 0 {
				return fmt.Errorf("factor: %s must be positive", name)
			}
		default:
			if value < float64(minPeriod) || value > MaxIndicatorPeriod || value != math.Trunc(value) {
				return fmt.Errorf("factor: %s must be an integer in [%d,%d]", name, minPeriod, MaxIndicatorPeriod)
			}
		}
	}
	if _, exists := spec.Parameters["fast"]; exists && spec.Parameters["fast"] >= spec.Parameters["slow"] {
		if strings.HasPrefix(spec.Operator, "macd") {
			return fmt.Errorf("factor: MACD requires fast < slow")
		}
		return fmt.Errorf("factor: %s requires fast < slow", spec.Operator)
	}
	if _, exists := spec.Parameters["start"]; exists && spec.Parameters["max"] < spec.Parameters["start"] {
		return fmt.Errorf("factor: sar requires max >= start")
	}
	return nil
}

// indicatorLookback counts preceding valid input tuples on a gap-free input.
// Missing tuples can extend the elapsed warmup; they never advance new nodes.
func indicatorLookback(spec NodeSpec) (warmup, retention int) {
	period := int(spec.Parameters["period"])
	switch spec.Operator {
	case "tr", "sar":
		return 1, 2
	case "obv", "kalman1d", "adl", "volume-delta", "cvd":
		return 0, 2
	case "dema":
		w := 2 * (period - 1)
		return w, w + 1
	case "tema":
		w := 3 * (period - 1)
		return w, w + 1
	case "hma":
		w := period + int(math.Floor(math.Sqrt(float64(period)))) - 2
		return w, w + 1
	case "rsi", "roc", "mom", "atr",
		"kama", "kama-by", "er",
		"dmi-plus", "dmi-minus", "natr", "chop",
		"aroon-up", "aroon-down", "aroon-osc",
		"vortex-plus", "vortex-minus",
		"keltner-upper", "keltner-lower",
		"supertrend", "supertrend-dir",
		"yang-zhang-vol":
		return period, period + 1
	case "adx":
		return 2*period - 1, 2 * period
	case "macd", "stc", "adosc":
		return int(spec.Parameters["slow"]) - 1, int(spec.Parameters["slow"])
	case "macd-signal", "macd-hist":
		length := int(spec.Parameters["slow"] + spec.Parameters["signal"] - 1)
		return length - 1, length
	default:
		return period - 1, period
	}
}
