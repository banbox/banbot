package factor

import (
	"fmt"
	"math"
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

type indicatorContract struct {
	arity      int
	parameters []string
	minPeriod  int
}

func indicatorDefinition(operator string) (indicatorContract, bool) {
	switch operator {
	case "sma", "rma", "wma", "rsi", "roc", "mom", "cci", "highest", "lowest",
		"ts-sum", "ts-product", "ts-rank", "ts-zscore", "ts-argmax", "ts-argmin", "ts-slope", "ts-rsquare":
		return indicatorContract{1, []string{"period"}, 1}, true
	case "ts-skew":
		return indicatorContract{1, []string{"period"}, 3}, true
	case "ts-kurt":
		return indicatorContract{1, []string{"period"}, 4}, true
	case "vwma", "ts-corr", "ts-cov", "ts-beta":
		return indicatorContract{2, []string{"period"}, 1}, true
	case "tr":
		return indicatorContract{3, nil, 0}, true
	case "atr", "stoch", "willr":
		return indicatorContract{3, []string{"period"}, 1}, true
	case "obv":
		return indicatorContract{2, nil, 0}, true
	case "mfi":
		return indicatorContract{4, []string{"period"}, 1}, true
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
		if name == "std_up" || name == "std_down" {
			if value < 0 {
				return fmt.Errorf("factor: %s must be nonnegative", name)
			}
		} else if value < float64(minPeriod) || value > MaxIndicatorPeriod || value != math.Trunc(value) {
			return fmt.Errorf("factor: %s must be an integer in [%d,%d]", name, minPeriod, MaxIndicatorPeriod)
		}
	}
	if _, exists := spec.Parameters["fast"]; exists && spec.Parameters["fast"] >= spec.Parameters["slow"] {
		return fmt.Errorf("factor: MACD requires fast < slow")
	}
	return nil
}

// indicatorLookback counts preceding valid input tuples on a gap-free input.
// Missing tuples can extend the elapsed warmup; they never advance new nodes.
func indicatorLookback(spec NodeSpec) (warmup, retention int) {
	period := int(spec.Parameters["period"])
	switch spec.Operator {
	case "tr":
		return 1, 2
	case "obv":
		return 0, 2
	case "rsi", "roc", "mom", "atr":
		return period, period + 1
	case "macd":
		return int(spec.Parameters["slow"]) - 1, int(spec.Parameters["slow"])
	case "macd-signal", "macd-hist":
		length := int(spec.Parameters["slow"] + spec.Parameters["signal"] - 1)
		return length - 1, length
	default:
		return period - 1, period
	}
}
