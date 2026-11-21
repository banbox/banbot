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
	case "ts-argmax":
		return tav.HighestBar(data, period)
	case "ts-argmin":
		return tav.LowestBar(data, period)
	case "ts-sum", "ts-product", "ts-rank", "ts-zscore", "ts-skew", "ts-kurt", "ts-slope", "ts-rsquare":
		return computeRollingBatch1(spec.Operator, data, period)
	case "ts-corr", "ts-cov", "ts-beta":
		return computeRollingBatch2(spec.Operator, data, inputs[1], period)
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
	case "dema":
		return tav.DEMA(data, period)
	case "tema":
		e1 := tav.EMA(data, period)
		e2 := tav.EMA(e1, period)
		e3 := tav.EMA(e2, period)
		out := make([]float64, len(data))
		for i := range out {
			out[i] = 3*e1[i] - 3*e2[i] + e3[i]
		}
		return out
	case "hma":
		return tav.HMA(data, period)
	case "kama":
		return tav.KAMA(data, period)
	case "kama-by":
		return tav.KAMABy(data, period, int(spec.Parameters["fast"]), int(spec.Parameters["slow"]))
	case "alma":
		return tav.ALMA(data, period, spec.Parameters["sigma"], spec.Parameters["offset"])
	case "trima":
		return tav.TRIMA(data, period)
	case "er":
		return tav.ER(data, period)
	case "kalman1d":
		return tav.Kalman1D(data, spec.Parameters["process_noise"], spec.Parameters["meas_noise"])
	case "hurst":
		return tav.Hurst(data, period)
	case "adx", "dmi-plus", "dmi-minus":
		return technicalDMIBatch(spec.Operator, data, inputs[1], inputs[2], period)
	case "natr":
		return tav.NATR(data, inputs[1], inputs[2], period)
	case "chop":
		return tav.CHOP(data, inputs[1], inputs[2], period)
	case "aroon-up", "aroon-down", "aroon-osc":
		up, osc, down := tav.Aroon(data, inputs[1], period)
		switch spec.Operator {
		case "aroon-down":
			return down
		case "aroon-osc":
			return osc
		default:
			return up
		}
	case "vortex-plus", "vortex-minus":
		plus, minus := tav.Vortex(data, inputs[1], inputs[2], period)
		if spec.Operator == "vortex-minus" {
			return minus
		}
		return plus
	case "donchian-upper", "donchian-mid", "donchian-lower":
		upper, mid, lower := tav.Donchian(data, inputs[1], period)
		switch spec.Operator {
		case "donchian-mid":
			return mid
		case "donchian-lower":
			return lower
		default:
			return upper
		}
	case "keltner-upper", "keltner-mid", "keltner-lower":
		upper, mid, lower := tav.KeltnerChannel(data, inputs[1], inputs[2], period, spec.Parameters["mult"])
		switch spec.Operator {
		case "keltner-mid":
			return mid
		case "keltner-lower":
			return lower
		default:
			return upper
		}
	case "supertrend", "supertrend-dir":
		line, dir := tav.SupertrendDir(data, inputs[1], inputs[2], period, spec.Parameters["mult"])
		if spec.Operator == "supertrend-dir" {
			return dir
		}
		return line
	case "sar":
		return technicalSARBatch(data, inputs[1], spec.Parameters["start"], spec.Parameters["inc"], spec.Parameters["max"])
	case "stc":
		return tav.STC(data, int(spec.Parameters["cycle"]), int(spec.Parameters["fast"]), int(spec.Parameters["slow"]), 0.5)
	case "ichimoku-conversion", "ichimoku-base":
		_, mid, _ := tav.Donchian(data, inputs[1], period)
		return mid
	case "cmf":
		return tav.CMF(data, inputs[1], inputs[2], inputs[3], period)
	case "adl":
		return tav.ADL(data, inputs[1], inputs[2], inputs[3])
	case "adosc":
		return tav.ADOSC(data, inputs[1], inputs[2], inputs[3], int(spec.Parameters["fast"]), int(spec.Parameters["slow"]))
	case "volume-delta":
		return tav.VolumeDelta(data, inputs[1])
	case "cvd":
		return tav.CVD(data, inputs[1])
	case "rolling-cvd":
		return tav.RollingCVD(data, inputs[1], period)
	case "taker-buy-ratio":
		return tav.TakerBuyRatio(data, inputs[1], period)
	case "parkinson-vol":
		return tav.ParkinsonVol(data, inputs[1], period)
	case "garman-klass-vol":
		return tav.GarmanKlassVol(data, inputs[1], inputs[2], inputs[3], period)
	case "yang-zhang-vol":
		return tav.YangZhangVol(data, inputs[1], inputs[2], inputs[3], period)
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
	case "willr":
		return ta.WillRHL(data, inputs[1], inputs[2], period).Get(0), true
	case "mfi":
		return ta.MFIHLCV(data, inputs[1], inputs[2], inputs[3], period).Get(0), true
	case "obv":
		return ta.OBV(data, inputs[1]).Get(0), true
	case "highest":
		return ta.Highest(data, period).Get(0), true
	case "lowest":
		return ta.Lowest(data, period).Get(0), true
	case "ts-argmax":
		return ta.HighestBar(data, period).Get(0), true
	case "ts-argmin":
		return ta.LowestBar(data, period).Get(0), true
	case "ts-sum", "ts-product", "ts-rank", "ts-zscore", "ts-skew", "ts-kurt", "ts-slope", "ts-rsquare":
		if len(data.Data) < period {
			return math.NaN(), true
		}
		return evalRolling1(spec.Operator, data.Data[len(data.Data)-period:]), true
	case "ts-corr", "ts-cov", "ts-beta":
		second := inputs[1]
		if len(data.Data) < period || len(second.Data) < period {
			return math.NaN(), true
		}
		return evalRolling2(spec.Operator, data.Data[len(data.Data)-period:], second.Data[len(second.Data)-period:]), true
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
	case "dema":
		return ta.DEMA(data, period).Get(0), true
	case "tema":
		return ta.TEMA(data, period).Get(0), true
	case "hma":
		return ta.HMA(data, period).Get(0), true
	case "kama":
		return ta.KAMA(data, period).Get(0), true
	case "kama-by":
		return ta.KAMABy(data, period, int(spec.Parameters["fast"]), int(spec.Parameters["slow"])).Get(0), true
	case "alma":
		return ta.ALMA(data, period, spec.Parameters["sigma"], spec.Parameters["offset"]).Get(0), true
	case "trima":
		return ta.TRIMA(data, period).Get(0), true
	case "er":
		return ta.ER(data, period).Get(0), true
	case "kalman1d":
		return ta.Kalman1D(data, spec.Parameters["process_noise"], spec.Parameters["meas_noise"]).Get(0), true
	case "hurst":
		return ta.Hurst(data, period).Get(0), true
	case "adx", "dmi-plus", "dmi-minus":
		return technicalDMI(spec.Operator, data, inputs[1], inputs[2], period).Get(0), true
	case "natr":
		return ta.NATR(data, inputs[1], inputs[2], period).Get(0), true
	case "chop":
		return ta.CHOPHLC(data, inputs[1], inputs[2], period).Get(0), true
	case "aroon-up", "aroon-down", "aroon-osc":
		hb := ta.HighestBar(data, period+1).Get(0)
		lb := ta.LowestBar(inputs[1], period+1).Get(0)
		if math.IsNaN(hb) || math.IsNaN(lb) {
			return math.NaN(), true
		}
		fac := -100.0 / float64(period)
		up := fac*hb + 100.0
		down := fac*lb + 100.0
		switch spec.Operator {
		case "aroon-down":
			return down, true
		case "aroon-osc":
			return up - down, true
		default:
			return up, true
		}
	case "vortex-plus", "vortex-minus":
		plus, minus := ta.Vortex(data, inputs[1], inputs[2], period)
		if spec.Operator == "vortex-minus" {
			return minus.Get(0), true
		}
		return plus.Get(0), true
	case "donchian-upper", "donchian-mid", "donchian-lower":
		upper, mid, lower := ta.Donchian(data, inputs[1], period)
		switch spec.Operator {
		case "donchian-mid":
			return mid.Get(0), true
		case "donchian-lower":
			return lower.Get(0), true
		default:
			return upper.Get(0), true
		}
	case "keltner-upper", "keltner-mid", "keltner-lower":
		upper, mid, lower := ta.KeltnerChannel(data, inputs[1], inputs[2], period, spec.Parameters["mult"])
		switch spec.Operator {
		case "keltner-mid":
			return mid.Get(0), true
		case "keltner-lower":
			return lower.Get(0), true
		default:
			return upper.Get(0), true
		}
	case "supertrend", "supertrend-dir":
		line, dir := ta.SupertrendDir(data, inputs[1], inputs[2], period, spec.Parameters["mult"])
		if spec.Operator == "supertrend-dir" {
			return dir.Get(0), true
		}
		return line.Get(0), true
	case "sar":
		return technicalSAR(data, inputs[1], spec.Parameters["start"], spec.Parameters["inc"], spec.Parameters["max"]).Get(0), true
	case "stc":
		return ta.STC(data, int(spec.Parameters["cycle"]), int(spec.Parameters["fast"]), int(spec.Parameters["slow"]), 0.5).Get(0), true
	case "ichimoku-conversion", "ichimoku-base":
		_, mid, _ := ta.Donchian(data, inputs[1], period)
		return mid.Get(0), true
	case "cmf":
		return ta.CMFHLCV(data, inputs[1], inputs[2], inputs[3], period).Get(0), true
	case "adl":
		return ta.ADLHLCV(data, inputs[1], inputs[2], inputs[3]).Get(0), true
	case "adosc":
		return technicalADOSC(data, inputs[1], inputs[2], inputs[3], int(spec.Parameters["fast"]), int(spec.Parameters["slow"])).Get(0), true
	case "volume-delta":
		return ta.VolumeDelta(data, inputs[1]).Get(0), true
	case "cvd":
		return ta.CVD(data, inputs[1]).Get(0), true
	case "rolling-cvd":
		return ta.RollingCVD(data, inputs[1], period).Get(0), true
	case "taker-buy-ratio":
		return ta.TakerBuyRatio(data, inputs[1], period).Get(0), true
	case "parkinson-vol":
		return ta.ParkinsonVol(data, inputs[1], period).Get(0), true
	case "garman-klass-vol":
		return ta.GarmanKlassVol(data, inputs[1], inputs[2], inputs[3], period).Get(0), true
	case "yang-zhang-vol":
		return ta.YangZhangVol(data, inputs[1], inputs[2], inputs[3], period).Get(0), true
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

type technicalDMIState struct {
	prevH, prevL, prevC    float64
	dmPosMA, dmNegMA, trMA float64
	adx, dxSum             float64
	num, dxCount           int
	hasPrev, adxReady      bool
}

func (s *technicalDMIState) step(h, l, c float64, period int) (plusDI, minusDI, adx float64) {
	if !s.hasPrev {
		s.prevH, s.prevL, s.prevC = h, l, c
		s.hasPrev = true
		return math.NaN(), math.NaN(), math.NaN()
	}
	dmh := h - s.prevH
	dml := s.prevL - l
	tr := max(h-l, math.Abs(h-s.prevC), math.Abs(l-s.prevC))
	s.prevH, s.prevL, s.prevC = h, l, c
	s.num++
	plusDM, minusDM := 0.0, 0.0
	if dmh > max(dml, 0) {
		plusDM = dmh
	} else if dml > max(dmh, 0) {
		minusDM = dml
	}
	if s.num <= period-1 {
		s.dmPosMA += plusDM
		s.dmNegMA += minusDM
		s.trMA += tr
		return math.NaN(), math.NaN(), math.NaN()
	}
	alpha := 1.0 / float64(period)
	s.dmPosMA = s.dmPosMA*(1-alpha) + plusDM
	s.dmNegMA = s.dmNegMA*(1-alpha) + minusDM
	s.trMA = s.trMA*(1-alpha) + tr
	if math.IsNaN(s.trMA) || s.trMA <= 0 {
		return math.NaN(), math.NaN(), math.NaN()
	}
	plusDI = 100 * s.dmPosMA / s.trMA
	minusDI = 100 * s.dmNegMA / s.trMA
	sumDI := plusDI + minusDI
	if math.IsNaN(sumDI) || sumDI <= 0 {
		return plusDI, minusDI, math.NaN()
	}
	dx := math.Abs(plusDI-minusDI) / sumDI * 100
	if !s.adxReady {
		s.dxSum += dx
		s.dxCount++
		if s.dxCount < period {
			return plusDI, minusDI, math.NaN()
		}
		s.adx = s.dxSum / float64(period)
		s.adxReady = true
		return plusDI, minusDI, s.adx
	}
	s.adx = alpha*dx + (1-alpha)*s.adx
	return plusDI, minusDI, s.adx
}

func technicalDMI(operator string, high, low, close *ta.Series, period int) *ta.Series {
	result := close.To("_factor_"+operator, period)
	if result.Cached() {
		return result
	}
	state, _ := result.More.(*technicalDMIState)
	if state == nil {
		state = &technicalDMIState{}
		result.More = state
		result.DupMore = func(value any) any {
			cloned := *value.(*technicalDMIState)
			return &cloned
		}
	}
	plusDI, minusDI, adx := state.step(high.Get(0), low.Get(0), close.Get(0), period)
	switch operator {
	case "dmi-plus":
		return result.Append(plusDI)
	case "dmi-minus":
		return result.Append(minusDI)
	default:
		return result.Append(adx)
	}
}

func technicalDMIBatch(operator string, high, low, close []float64, period int) []float64 {
	n := len(close)
	out := make([]float64, n)
	var state technicalDMIState
	for i := 0; i < n; i++ {
		plusDI, minusDI, adx := state.step(high[i], low[i], close[i], period)
		switch operator {
		case "dmi-plus":
			out[i] = plusDI
		case "dmi-minus":
			out[i] = minusDI
		default:
			out[i] = adx
		}
	}
	return out
}

type technicalSARState struct {
	sar, ep, af  float64
	prevH1       float64
	prevH2       float64
	prevL1       float64
	prevL2       float64
	bars         int
	up           bool
}

func (s *technicalSARState) step(h, l, start, inc, maxAF float64) float64 {
	if s.bars == 0 {
		s.up = true
		s.sar = l
		s.ep = h
		s.af = start
		s.prevH1, s.prevL1 = h, l
		s.bars = 1
		return math.NaN()
	}
	if s.bars == 1 {
		s.sar = l
		s.ep = h
		s.af = start
		s.prevH2, s.prevL2 = s.prevH1, s.prevL1
		s.prevH1, s.prevL1 = h, l
		s.bars = 2
		return s.sar
	}
	s.sar = s.sar + s.af*(s.ep-s.sar)
	if s.up {
		s.sar = math.Min(s.sar, s.prevL1)
		if s.bars > 2 {
			s.sar = math.Min(s.sar, s.prevL2)
		}
		if h > s.ep {
			s.ep = h
			s.af = math.Min(maxAF, s.af+inc)
		}
		if l < s.sar {
			s.up = false
			s.sar = s.ep
			s.ep = l
			s.af = start
		}
	} else {
		s.sar = math.Max(s.sar, s.prevH1)
		if s.bars > 2 {
			s.sar = math.Max(s.sar, s.prevH2)
		}
		if l < s.ep {
			s.ep = l
			s.af = math.Min(maxAF, s.af+inc)
		}
		if h > s.sar {
			s.up = true
			s.sar = s.ep
			s.ep = h
			s.af = start
		}
	}
	s.prevH2, s.prevL2 = s.prevH1, s.prevL1
	s.prevH1, s.prevL1 = h, l
	s.bars++
	return s.sar
}

func technicalSAR(high, low *ta.Series, start, inc, maxAF float64) *ta.Series {
	result := high.To("_factor_sar", 0)
	if result.Cached() {
		return result
	}
	state, _ := result.More.(*technicalSARState)
	if state == nil {
		state = &technicalSARState{}
		result.More = state
		result.DupMore = func(value any) any {
			cloned := *value.(*technicalSARState)
			return &cloned
		}
	}
	return result.Append(state.step(high.Get(0), low.Get(0), start, inc, maxAF))
}

func technicalSARBatch(high, low []float64, start, inc, maxAF float64) []float64 {
	out := make([]float64, len(high))
	var state technicalSARState
	for i := range high {
		out[i] = state.step(high[i], low[i], start, inc, maxAF)
	}
	return out
}

type technicalADOSCState struct {
	cum, fast, slow float64
	count           int
}

func technicalADOSC(high, low, close, volume *ta.Series, fast, slow int) *ta.Series {
	result := close.To("_factor_adosc", fast*100000+slow)
	if result.Cached() {
		return result
	}
	state, _ := result.More.(*technicalADOSCState)
	if state == nil {
		state = &technicalADOSCState{}
		result.More = state
		result.DupMore = func(value any) any {
			cloned := *value.(*technicalADOSCState)
			return &cloned
		}
	}
	h, l, c, v := high.Get(0), low.Get(0), close.Get(0), volume.Get(0)
	mf := 0.0
	if h > l {
		mf = ((c - l) - (h - c)) / (h - l)
	}
	state.cum += mf * v
	state.count++
	if state.count == 1 {
		state.fast = state.cum
		state.slow = state.cum
	} else {
		af := 2.0 / float64(fast+1)
		as := 2.0 / float64(slow+1)
		state.fast = af*state.cum + (1-af)*state.fast
		state.slow = as*state.cum + (1-as)*state.slow
	}
	if state.count < slow || math.IsNaN(state.fast) || math.IsNaN(state.slow) {
		return result.Append(math.NaN())
	}
	return result.Append(state.fast - state.slow)
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
