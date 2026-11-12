package opt

import (
	"fmt"
	"math"
	"sort"

	"github.com/banbox/banbot/utils"
	utils2 "github.com/banbox/banexg/utils"
)

// Offline equity replay: rebuilds the equity curve a time-series backtest logs under bt_fresh_equity (BtFeatureLevel 12
// default) from its closed-order ledger and the K-lines, and derives the curve metrics with the engine's own
// functions (BTResult.recordUlcer / collectUlcer for the daily Ulcer metrics, utils.CalcDrawDown, CalcMeasuresByReal).
// It is a pure function: it changes no backtest behavior.
//
// Model (BTResult.logState, biz.BanWallets.EquityWithOrders):
//
//	equity(T) = initial capital + sum(profit of orders closed by T) + sum(Profit of the orders still open at T)
//
// An open order's Profit is only refreshed on a close of the order's OWN timeframe: (close of the last finished bar
// of that timeframe - entry average) * filled * side - entry fee * 2 (the entry fee is booked on the balance and in the
// order's Profit); before the first such close after the entry it is still 0. The state is sampled at the first event
// of each time step: only the symbol(s) in FirstSymbols already processed that step, every other symbol still shows
// the state of the previous bar (its entries / exits of exactly this step are not counted, its price is the previous
// bar's close). Which symbol of a multi-symbol pool is first is not recorded in the ledger, so FirstSymbols is empty
// there (residual error up to ~0.3% of the capital on single steps).
//
// The final value (EndMS) is the capital plus all order profits (the engine closes every position at the end).

// ReplayOrder is one closed order of the ledger (orders.gob / orders.csv.gz columns noted).
type ReplayOrder struct {
	Symbol   string
	TF       string  // tf: the order's own timeframe
	Short    bool    // short
	EntryAvg float64 // e_avg (e_price when 0)
	Filled   float64 // e_filled
	EntryFee float64 // e_feeq
	Profit   float64 // profit: net of fees and funding
	CreateMS int64   // e_create: used to infer the bar grid offset of 3d orders
	EnterMS  int64   // e_update: the entry fill time
	ExitMS   int64   // x_update: the exit fill time (0: held to the end)
}

// ReplayPriceFunc returns the close of the base-timeframe bar of symbol that STARTS at barStartMS. The base timeframe
// of an order timeframe is ReplayBaseTF (4h / 8h read 1h bars, 3d reads 1d). ok=false: no bar (the order is then marked
// at its entry average); a source that wants "last close before" semantics for halted symbols implements it itself.
type ReplayPriceFunc func(symbol, baseTF string, barStartMS int64) (price float64, ok bool)

// ReplayParams is the input of ReplayEquity.
type ReplayParams struct {
	Orders       []ReplayOrder
	Init         float64 // initial capital (BTResult.TotalInvest)
	StartMS      int64   // detail.json startMS
	EndMS        int64   // detail.json endMS
	StepMS       int64   // the engine's time step (the base timeframe of the run: ReplayBaseTF of the smallest order timeframe)
	PlotTimes    []int64 // sample times of the saved chart (detail.json plots.labels as UTC ms): the Sharpe input
	FirstSymbols map[string]bool
	Price        ReplayPriceFunc
}

// ReplayResult holds the rebuilt equity metrics. When NeedsRerun is set the account reached zero (or worse) in the
// replay: the order ledger of such a run is itself different (liquidation / margin), so no metric is reported.
type ReplayResult struct {
	NeedsRerun  bool
	Reason      string
	MinEquity   float64 // lowest replayed equity over the steps and the plot samples
	Steps       int
	UlcerIndex  *float64
	UPI         *float64
	DailyEquity [][2]float64
	// MaxDrawDownPct / Val: over every time step (BTResult.MaxDrawDownPct); ShowDrawDownPct / Val: over the plot samples.
	MaxDrawDownPct  float64
	MaxDrawDownVal  float64
	ShowDrawDownPct float64
	ShowDrawDownVal float64
	SharpeRatio     float64
	SortinoRatio    float64
	PlotReal        []float64
}

// ReplayBaseTF is the stored K-line timeframe whose bars are fed (and whose closes mark the open orders) for an order
// timeframe: the library keeps 1m / 5m / 15m / 1h / 1d, an order timeframe reads the largest one that divides it
// (30m -> 15m, 4h / 8h -> 1h, 3d -> 1d).
func ReplayBaseTF(tf string) string {
	switch tf {
	case "1m", "5m", "15m", "1h", "1d":
		return tf
	}
	secs := int64(utils2.TFToSecs(tf))
	for _, b := range []string{"1d", "1h", "15m", "5m", "1m"} {
		if bs := int64(utils2.TFToSecs(b)); secs >= bs && secs%bs == 0 {
			return b
		}
	}
	return tf
}

// ReplayStepMS is the engine's time step (ms) of a run whose smallest order timeframe is tf: its base timeframe.
func ReplayStepMS(tf string) int64 { return replayTFMS(ReplayBaseTF(tf)) }

func replayTFMS(tf string) int64 {
	switch tf {
	case "1m":
		return 60000
	case "3d":
		return 3 * 86400000
	}
	return int64(utils2.TFToSecs(tf)) * 1000
}

type replayOrd struct {
	ReplayOrder
	g, off, base int64
	sign         float64
	lastE        int64 // price cache: the bar end the cached price belongs to
	lastP        float64
	hasP         bool
}

// replayOffsets: the bar grid of tfs of 3d and longer is not aligned to the epoch; the offset is the most common
// e_create % tf among the orders of that timeframe.
func replayOffsets(ods []ReplayOrder) map[string]int64 {
	cnt := map[string]map[int64]int{}
	for _, o := range ods {
		g := replayTFMS(o.TF)
		if g < 3*86400000 {
			continue
		}
		m := cnt[o.TF]
		if m == nil {
			m = map[int64]int{}
			cnt[o.TF] = m
		}
		m[o.CreateMS%g]++
	}
	out := map[string]int64{}
	for tf, m := range cnt {
		best, bestN := int64(0), -1
		for off, n := range m {
			if n > bestN || (n == bestN && off < best) {
				best, bestN = off, n
			}
		}
		out[tf] = best
	}
	return out
}

// ReplayEquity replays the ledger step by step. See the file comment for the model.
func ReplayEquity(p ReplayParams) (*ReplayResult, error) {
	if p.Init <= 0 || p.StepMS <= 0 || p.EndMS <= p.StartMS {
		return nil, fmt.Errorf("replay equity: init, step and a positive time range are required")
	}
	if p.Price == nil {
		return nil, fmt.Errorf("replay equity: a price source is required")
	}
	offs := replayOffsets(p.Orders)
	ods := make([]*replayOrd, 0, len(p.Orders))
	totalPft := 0.0
	for _, src := range p.Orders {
		o := &replayOrd{ReplayOrder: src}
		o.g = replayTFMS(o.TF)
		if o.g <= 0 {
			return nil, fmt.Errorf("replay equity: unknown timeframe %q", o.TF)
		}
		o.off = offs[o.TF]
		o.base = replayTFMS(ReplayBaseTF(o.TF))
		o.sign = 1
		if o.Short {
			o.sign = -1
		}
		if o.ExitMS <= 0 {
			o.ExitMS = p.EndMS
		}
		totalPft += o.Profit
		ods = append(ods, o)
	}
	sort.SliceStable(ods, func(i, j int) bool { return ods[i].EnterMS < ods[j].EnterMS })

	// the evaluation times: the engine's steps (multiples of StepMS) and the saved plot samples
	type tpoint struct {
		ms   int64
		step bool
		plot bool
	}
	times := map[int64]*tpoint{}
	at := func(ms int64) *tpoint {
		t := times[ms]
		if t == nil {
			t = &tpoint{ms: ms}
			times[ms] = t
		}
		return t
	}
	for ms := (p.StartMS + p.StepMS - 1) / p.StepMS * p.StepMS; ms <= p.EndMS; ms += p.StepMS {
		at(ms).step = true
	}
	at(p.StartMS).step = true
	for _, ms := range p.PlotTimes {
		at(ms).plot = true
	}
	ordered := make([]*tpoint, 0, len(times))
	for _, t := range times {
		ordered = append(ordered, t)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ms < ordered[j].ms })

	var (
		active   []*replayOrd
		next     int
		doneSum  float64
		minEq    = math.Inf(1)
		stepReal []float64
		res      = &ReplayResult{}
		ulcer    = &BTResult{TotalInvest: p.Init, StartMS: p.StartMS, EndMS: p.EndMS}
		plotAt   = map[int64]float64{}
	)
	for _, t := range ordered {
		T := t.ms
		for next < len(ods) && ods[next].EnterMS <= T {
			active = append(active, ods[next])
			next++
		}
		eq := p.Init + doneSum
		keep := active[:0]
		for _, o := range active {
			if o.ExitMS <= T-o.g {
				doneSum += o.Profit // closed at least one bar ago: final from now on
				eq += o.Profit
				continue
			}
			keep = append(keep, o)
			stepBar := (T-o.off)%o.g == 0
			late := stepBar && !p.FirstSymbols[o.Symbol]
			if late && o.EnterMS == T {
				continue // not entered yet at the first event of this step
			}
			exitInStep := o.ExitMS > T-o.g && o.ExitMS <= T
			if o.ExitMS <= T && !(late && exitInStep) {
				eq += o.Profit
				continue
			}
			bar := (T-o.off)/o.g*o.g + o.off // end of the last finished bar of the order's timeframe
			if late && bar == T {
				bar -= o.g
			}
			if o.EnterMS > bar {
				continue // no close of its own timeframe since the entry: Profit still 0
			}
			if !o.hasP || o.lastE != bar {
				px, ok := p.Price(o.Symbol, ReplayBaseTF(o.TF), bar-o.base)
				if !ok || px <= 0 {
					px = o.entryPrice()
				}
				o.lastE, o.lastP, o.hasP = bar, px, true
			}
			eq += (o.lastP-o.entryPrice())*o.Filled*o.sign - 2*o.EntryFee
		}
		active = keep
		if t.ms >= p.EndMS {
			eq = p.Init + totalPft // every position is closed at the end
		}
		minEq = math.Min(minEq, eq)
		if t.step {
			stepReal = append(stepReal, eq)
			ulcer.recordUlcer(T, eq)
		}
		if t.plot {
			plotAt[T] = eq
		}
	}
	// the engine's final logState (all positions closed) always lands on the end time
	ulcer.recordUlcer(p.EndMS, p.Init+totalPft)
	res.Steps = len(stepReal)
	res.MinEquity = minEq
	if minEq <= 0 {
		res.NeedsRerun, res.Reason = true, fmt.Sprintf("replayed equity %.2f <= 0", minEq)
		return res, nil
	}
	ulcer.collectUlcer()
	if ulcer.UlcerPerformanceIndex == nil && ulcer.ulcerDaily.Invalid() {
		res.NeedsRerun, res.Reason = true, "daily equity <= 0"
		return res, nil
	}
	res.UlcerIndex, res.UPI, res.DailyEquity = ulcer.UlcerIndex, ulcer.UlcerPerformanceIndex, ulcer.DailyEquity
	rate, val := utils.CalcDrawDown(stepReal, 0)
	res.MaxDrawDownPct, res.MaxDrawDownVal = rate*100, val
	plotTimes := append([]int64(nil), p.PlotTimes...)
	sort.Slice(plotTimes, func(i, j int) bool { return plotTimes[i] < plotTimes[j] })
	for _, ms := range plotTimes {
		res.PlotReal = append(res.PlotReal, plotAt[ms])
	}
	rate, val = utils.CalcDrawDown(res.PlotReal, 0)
	res.ShowDrawDownPct, res.ShowDrawDownVal = rate*100, val
	sharpe, sortino, err := CalcMeasuresByReal(res.PlotReal, (p.EndMS-p.StartMS)/1000, "", 0, 0)
	if err != nil {
		return nil, err
	}
	res.SharpeRatio, res.SortinoRatio = sharpe, sortino
	return res, nil
}

func (o *replayOrd) entryPrice() float64 { return o.EntryAvg }
