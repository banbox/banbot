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
}

func indicatorDefinition(operator string) (indicatorContract, bool) {
	switch operator {
	case "sma", "rma", "wma", "rsi", "roc", "mom", "cci", "highest", "lowest":
		return indicatorContract{1, []string{"period"}}, true
	case "vwma":
		return indicatorContract{2, []string{"period"}}, true
	case "tr":
		return indicatorContract{3, nil}, true
	case "atr", "stoch", "willr":
		return indicatorContract{3, []string{"period"}}, true
	case "obv":
		return indicatorContract{2, nil}, true
	case "mfi":
		return indicatorContract{4, []string{"period"}}, true
	case "macd", "macd-signal", "macd-hist":
		return indicatorContract{1, []string{"fast", "slow", "signal"}}, true
	case "bbands-upper", "bbands-middle", "bbands-lower":
		return indicatorContract{1, []string{"period", "std_up", "std_down"}}, true
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
	for _, name := range contract.parameters {
		value, exists := spec.Parameters[name]
		if !exists {
			return fmt.Errorf("factor: %s requires parameter %s", spec.Operator, name)
		}
		if name == "std_up" || name == "std_down" {
			if value < 0 {
				return fmt.Errorf("factor: %s must be nonnegative", name)
			}
		} else if value < 1 || value > MaxIndicatorPeriod || value != math.Trunc(value) {
			return fmt.Errorf("factor: %s must be an integer in [1,%d]", name, MaxIndicatorPeriod)
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
