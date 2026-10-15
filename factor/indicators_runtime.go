package factor

import (
	"math"

	ta "github.com/banbox/banta"
	"github.com/banbox/banta/tav"
)

// technicalOperatorArity lists scalar outputs, including individual outputs of
// multi-column indicators. Each dependency tuple advances together: invalid
// observations never advance one leg of a multi-input indicator independently.
func technicalOperatorArity(operator string) (int, bool) {
	contract, ok := indicatorDefinition(operator)
	return contract.arity, ok
}

func computeTechnicalBatch(spec NodeSpec, inputs [][]float64) ([]float64, bool) {
	arity, ok := technicalOperatorArity(spec.Operator)
	if !ok || len(inputs) != arity {
		return nil, false
	}
	n := len(inputs[0])
	for _, input := range inputs {
		if len(input) != n {
			return nil, false
		}
	}
	// tav's recursive seeds require contiguous values. Compact complete tuples
	// to implement the declared skip-invalid policy, then restore logical grids.
	compact := make([][]float64, arity)
	indices := make([]int, 0, n)
	for i := 0; i < n; i++ {
		valid := true
		for _, input := range inputs {
			if numeric(input[i]).Validity != Valid {
				valid = false
				break
			}
		}
		if valid {
			indices = append(indices, i)
			for j, input := range inputs {
				compact[j] = append(compact[j], input[i])
			}
		}
	}
	values := technicalBatchValues(spec, compact)
	result := make([]float64, n)
	for i := range result {
		result[i] = math.NaN()
	}
	for i, index := range indices {
		result[index] = values[i]
	}
	return result, true
}

func technicalBatchValues(spec NodeSpec, inputs [][]float64) []float64 {
	data := inputs[0]
	period := int(spec.Parameters["period"])
	switch spec.Operator {
	case "sma":
		return tav.SMA(data, period)
	case "rma":
		return tav.RMA(data, period)
	case "wma":
		return tav.WMA(data, period)
	case "vwma":
		return tav.VWMA(data, inputs[1], period)
	case "rsi":
		return tav.RSI(data, period)
	case "roc":
		return tav.ROC(data, period)
	case "mom":
		return tav.MOM(data, period)
	case "tr":
		return tav.TR(data, inputs[1], inputs[2])
	case "atr":
		return tav.ATR(data, inputs[1], inputs[2], period)
	case "cci":
		return tav.CCI(data, period)
	case "stoch":
		return tav.Stoch(data, inputs[1], inputs[2], period)
	case "willr":
		return tav.WillR(data, inputs[1], inputs[2], period)
	case "obv":
		return tav.OBV(data, inputs[1])
	case "mfi":
		return tav.MFI(data, inputs[1], inputs[2], inputs[3], period)
	case "highest":
		return tav.Highest(data, period)
	case "lowest":
		return tav.Lowest(data, period)
	case "macd", "macd-signal", "macd-hist":
		line, signal := tav.MACD(data, int(spec.Parameters["fast"]), int(spec.Parameters["slow"]), int(spec.Parameters["signal"]))
		if spec.Operator == "macd-signal" {
			return signal
		}
		if spec.Operator == "macd-hist" {
			for i := range line {
				line[i] -= signal[i]
			}
		}
		return line
	case "bbands-upper", "bbands-middle", "bbands-lower":
		upper, middle, lower := tav.BBANDS(data, period, spec.Parameters["std_up"], spec.Parameters["std_down"])
		switch spec.Operator {
		case "bbands-middle":
			return middle
		case "bbands-lower":
			return lower
		default:
			return upper
		}
	default:
		return nil
	}
}

// Inputs are dedicated to one compiled node, with only complete finite tuples
// appended. Native banta cache keys omit secondary input IDs, so borrowing the
// shared DAG input series would alias distinct multi-input indicators.
func computeTechnicalIncremental(spec NodeSpec, inputs []*ta.Series) (float64, bool) {
	arity, ok := technicalOperatorArity(spec.Operator)
	if !ok || len(inputs) != arity {
		return math.NaN(), false
	}
	data := inputs[0]
	period := int(spec.Parameters["period"])
	switch spec.Operator {
	case "sma":
		return ta.SMA(data, period).Get(0), true
	case "rma":
		return ta.RMA(data, period).Get(0), true
	case "wma":
		return ta.WMA(data, period).Get(0), true
	case "vwma":
		return ta.VWMA(data, inputs[1], period).Get(0), true
	case "rsi":
		return technicalRSI(data, period).Get(0), true
	case "roc":
		return ta.ROC(data, period).Get(0), true
	case "mom":
		return ta.MOM(data, period).Get(0), true
	case "tr":
		return ta.TR(data, inputs[1], inputs[2]).Get(0), true
	case "atr":
		return ta.ATR(data, inputs[1], inputs[2], period).Get(0), true
	case "cci":
		return ta.CCI(data, period).Get(0), true
	case "stoch":
		return ta.Stoch(data, inputs[1], inputs[2], period).Get(0), true
	case "willr", "mfi":
		// These native APIs accept BarEnv; supply the declared arbitrary field
		// dependencies without changing the actual environment's price roots.
		env := &ta.BarEnv{High: data, Low: inputs[1], Close: inputs[2]}
		if spec.Operator == "willr" {
			return ta.WillR(env, period).Get(0), true
		}
		env.Volume = inputs[3]
		return ta.MFI(env, period).Get(0), true
	case "obv":
		return ta.OBV(data, inputs[1]).Get(0), true
	case "highest":
		return ta.Highest(data, period).Get(0), true
	case "lowest":
		return ta.Lowest(data, period).Get(0), true
	case "macd", "macd-signal", "macd-hist":
		line, signal := ta.MACD(data, int(spec.Parameters["fast"]), int(spec.Parameters["slow"]), int(spec.Parameters["signal"]))
		if spec.Operator == "macd-signal" {
			return signal.Get(0), true
		}
		if spec.Operator == "macd-hist" {
			return line.Get(0) - signal.Get(0), true
		}
		return line.Get(0), true
	case "bbands-upper", "bbands-middle", "bbands-lower":
		upper, middle, lower := ta.BBANDS(data, period, spec.Parameters["std_up"], spec.Parameters["std_down"])
		if spec.Operator == "bbands-middle" {
			return middle.Get(0), true
		}
		if spec.Operator == "bbands-lower" {
			return lower.Get(0), true
		}
		return upper.Get(0), true
	default:
		return math.NaN(), false
	}
}

type technicalRSIState struct {
	previous, gain, loss float64
	count                int
}

// Native v0.4.1 RSI divides every seed delta by period before adding, while
// tav adds the deltas then divides the seed. They differ for overflowing sums
// and for flat seeds. Keep tav's exact arithmetic order and zero-loss behavior
// in a constant-size, forkable Wilder state.
func technicalRSI(input *ta.Series, period int) *ta.Series {
	result := input.To("_factor_rsi", period)
	if result.Cached() {
		return result
	}
	state, _ := result.More.(*technicalRSIState)
	if state == nil {
		state = &technicalRSIState{previous: math.NaN()}
		result.More = state
		result.DupMore = func(value any) any {
			cloned := *value.(*technicalRSIState)
			return &cloned
		}
	}
	current := input.Get(0)
	if math.IsNaN(state.previous) {
		state.previous = current
		return result.Append(math.NaN())
	}
	state.count++
	delta := current - state.previous
	state.previous = current
	gain, loss := 0.0, 0.0
	if delta >= 0 {
		gain = delta
	} else {
		loss = -delta
	}
	if state.count > period {
		state.gain = (state.gain*float64(period-1) + gain) / float64(period)
		state.loss = (state.loss*float64(period-1) + loss) / float64(period)
	} else {
		state.gain += gain
		state.loss += loss
		if state.count == period {
			state.gain /= float64(period)
			state.loss /= float64(period)
		}
	}
	value := math.NaN()
	if state.count >= period {
		value = 100.0
		if state.gain+state.loss != 0 {
			value = 100 * state.gain / (state.gain + state.loss)
		}
	}
	return result.Append(value)
}

func (asset *assetState) evaluateTechnical(node compiledNode, inputs []Numeric) Numeric {
	for _, input := range inputs {
		if input.Validity != Valid {
			return Numeric{math.NaN(), input.Validity}
		}
	}
	if asset.technical == nil {
		asset.technical = make(map[string][]*ta.Series)
	}
	series := asset.technical[node.id]
	if series == nil {
		series = make([]*ta.Series, len(inputs))
		for i := range series {
			series[i] = asset.env.NewSeries(nil)
		}
		asset.technical[node.id] = series
	}
	for i, input := range inputs {
		series[i].Append(input.Value)
	}
	value, _ := computeTechnicalIncremental(node.spec, series)
	result := numeric(value)
	if math.IsNaN(value) {
		result.Validity = Warmup
	}
	return result
}
