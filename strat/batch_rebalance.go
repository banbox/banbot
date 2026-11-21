package strat

import (
	"cmp"
	"math"
	"slices"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	ta "github.com/banbox/banta"
	"github.com/banbox/banta/tav"
)

const (
	// DefaultBatchRebalanceTag is the default entry/exit tag used by RebalanceBatchWeights.
	DefaultBatchRebalanceTag = "rebalance"
	// DefaultBatchDriftThreshold is the default no-trade weight drift buffer (2% of AccountEquity).
	DefaultBatchDriftThreshold = 0.02
	// DefaultBatchATRPeriod is the default NATR lookback period used by InverseVolWeights.
	DefaultBatchATRPeriod = 14

	rebalanceWeightZeroEps = 1e-12
	rebalanceCostTieEps    = 1e-9
)

// BatchRebalanceActionKind classifies a single execution step emitted by RebalanceBatchWeights.
type BatchRebalanceActionKind string

const (
	RebalanceActionClose   BatchRebalanceActionKind = "close"
	RebalanceActionReduce  BatchRebalanceActionKind = "reduce"
	RebalanceActionOpen    BatchRebalanceActionKind = "open"
	RebalanceActionScaleIn BatchRebalanceActionKind = "scale_in"
)

// BatchRebalanceOpts configures cross-symbol target-weight portfolio rebalancing inside OnBatchJobs.
type BatchRebalanceOpts struct {
	// Tag is used as both ExitReq.Tag and EnterReq.Tag (default "rebalance").
	Tag string
	// DriftThreshold is the no-trade buffer on notional weight difference |target - cur| when
	// curWeight and targetWeight have the same non-zero sign (default 0.02; set < 0 to disable).
	DriftThreshold float64
	// MaxGrossExposure caps total absolute target weight Σ|w|. When > 0 and Σ|w| > MaxGrossExposure,
	// all target weights are scaled down proportionally so Σ|w| == MaxGrossExposure.
	MaxGrossExposure float64
	// StopLossRate attaches a per-symbol protective stop-loss distance (enterPrice * StopLossRate)
	// to newly opened / scaled-in orders (e.g. 0.05 = 5% adverse price move).
	StopLossRate float64
	// TakeProfitRate attaches a per-symbol protective take-profit distance (enterPrice * TakeProfitRate)
	// to newly opened / scaled-in orders (e.g. 0.10 = 10% favorable price move).
	TakeProfitRate float64
	// CallbackPct attaches an exchange/simulated trailing stop callback percentage in [0.1, 10].
	CallbackPct float64
	// Leverage overrides per-order leverage on newly opened / scaled-in orders when > 0.
	Leverage float64
}

// BatchRebalanceAction records one order action issued during RebalanceBatchWeights in execution order.
type BatchRebalanceAction struct {
	Symbol       string
	Kind         BatchRebalanceActionKind
	Short        bool
	PrevWeight   float64
	TargetWeight float64
	DeltaWeight  float64
	ExitRate     float64
	LegalCost    float64
	Err          *errs.Error
}

// BatchRebalanceResult summarizes a RebalanceBatchWeights invocation.
type BatchRebalanceResult struct {
	Equity                 float64
	RawGrossExposure       float64
	EffectiveGrossExposure float64
	ScaleFactor            float64
	ClosedCount            int
	ReducedCount           int
	ReversedCount          int
	OpenedCount            int
	ScaledInCount          int
	SkippedDriftCount      int
	SkippedDustCount       int
	ErrorCount             int
	Actions                []BatchRebalanceAction
}

type pendingBatchOpen struct {
	job          *StratJob
	symbol       string
	short        bool
	scaleIn      bool
	prevWeight   float64
	targetWeight float64
	deltaWeight  float64
	legalCost    float64
	priority     uint64
}

// RebalanceBatchWeights reconciles open positions across jobs in OnBatchJobs toward targetWeights
// (symbol -> signed notional fraction of AccountEquity: >0 long, <0 short, 0 or absent flat).
//
// Execution runs strictly in two passes:
//  1. Pass 1 closes flat targets, closes opposite-direction legs on reversals, and partially closes
//     overweight same-direction legs where |target| < |cur| - DriftThreshold, freeing margin first.
//  2. Pass 2 opens new legs and scales into underweight legs (|target| > |cur| + DriftThreshold)
//     in descending LegalCost order (tie-broken by neutral BarPriority then symbol), silently
//     skipping sub-MinStakeAmount dust orders.
func RebalanceBatchWeights(jobs []*StratJob, targetWeights map[string]float64, opts BatchRebalanceOpts) BatchRebalanceResult {
	tag := opts.Tag
	if tag == "" {
		tag = DefaultBatchRebalanceTag
	}
	drift := opts.DriftThreshold
	if math.IsNaN(drift) || math.IsInf(drift, 0) || drift == 0 {
		drift = DefaultBatchDriftThreshold
	} else if drift < 0 {
		drift = 0
	}

	rawGross := 0.0
	for _, w := range targetWeights {
		if !math.IsNaN(w) && !math.IsInf(w, 0) && math.Abs(w) > rebalanceWeightZeroEps {
			rawGross += math.Abs(w)
		}
	}
	scale := 1.0
	if isFinitePositive(opts.MaxGrossExposure) && rawGross > opts.MaxGrossExposure {
		scale = opts.MaxGrossExposure / rawGross
	}
	res := BatchRebalanceResult{
		RawGrossExposure:       rawGross,
		EffectiveGrossExposure: rawGross * scale,
		ScaleFactor:            scale,
	}
	if len(jobs) == 0 {
		return res
	}

	var equity float64
	for _, job := range jobs {
		if job != nil && job.Symbol != nil && job.Symbol.Symbol != "" {
			if job.IsWarmUpState() {
				return res
			}
			equity = job.AccountEquity()
			break
		}
	}
	res.Equity = equity
	if !isFinitePositive(equity) {
		return res
	}

	barMS, seed := batchJobsBarMSAndSeed(jobs)
	seen := make(map[string]struct{}, len(jobs))
	var pendingOpens []pendingBatchOpen

	// Pass 1: full closes, opposite-side reversal closes, and same-side partial reductions.
	for _, job := range jobs {
		if job == nil || job.Symbol == nil || job.Symbol.Symbol == "" || job.IsWarmUpState() {
			continue
		}
		sym := job.Symbol.Symbol
		if _, dup := seen[sym]; dup {
			continue
		}
		seen[sym] = struct{}{}

		targetW := 0.0
		if w, ok := targetWeights[sym]; ok && !math.IsNaN(w) && !math.IsInf(w, 0) {
			targetW = w * scale
		}
		if math.Abs(targetW) <= rebalanceWeightZeroEps {
			targetW = 0
		}

		longOrders := job.GetOrders(core.OdDirtLong)
		shortOrders := job.GetOrders(core.OdDirtShort)
		hasLong := len(longOrders) > 0
		hasShort := len(shortOrders) > 0
		longWeight := calcJobOrdersNotional(job, longOrders, false) / equity
		shortWeight := calcJobOrdersNotional(job, shortOrders, true) / equity
		netPrevWeight := longWeight - shortWeight

		if targetW == 0 {
			if hasLong || hasShort {
				err := job.CloseOrders(&ExitReq{Tag: tag, Dirt: core.OdDirtBoth})
				if err != nil {
					res.ErrorCount++
				} else {
					res.ClosedCount++
				}
				res.Actions = append(res.Actions, BatchRebalanceAction{
					Symbol:       sym,
					Kind:         RebalanceActionClose,
					Short:        shortWeight > longWeight,
					PrevWeight:   netPrevWeight,
					TargetWeight: 0,
					DeltaWeight:  -netPrevWeight,
					ExitRate:     1,
					Err:          err,
				})
			}
			continue
		}

		wantShort := targetW < 0
		absTarget := math.Abs(targetW)
		var prio uint64
		if barMS > 0 {
			prio = BarPriority(barMS, sym, seed)
		}

		if !wantShort {
			// Target is LONG: close any opposite SHORT orders first.
			if hasShort {
				err := job.CloseOrders(&ExitReq{Tag: tag, Dirt: core.OdDirtShort})
				if err != nil {
					res.ErrorCount++
				} else {
					res.ClosedCount++
					if !hasLong {
						res.ReversedCount++
					}
				}
				res.Actions = append(res.Actions, BatchRebalanceAction{
					Symbol:       sym,
					Kind:         RebalanceActionClose,
					Short:        true,
					PrevWeight:   -shortWeight,
					TargetWeight: targetW,
					DeltaWeight:  shortWeight,
					ExitRate:     1,
					Err:          err,
				})
			}
			if !hasLong || longWeight <= rebalanceWeightZeroEps {
				pendingOpens = append(pendingOpens, pendingBatchOpen{
					job:          job,
					symbol:       sym,
					short:        false,
					scaleIn:      false,
					prevWeight:   netPrevWeight,
					targetWeight: targetW,
					deltaWeight:  absTarget,
					legalCost:    equity * absTarget,
					priority:     prio,
				})
				continue
			}
			diff := absTarget - longWeight
			if math.Abs(diff) < drift || math.Abs(diff) <= rebalanceWeightZeroEps {
				res.SkippedDriftCount++
				continue
			}
			if diff < 0 {
				exitRate := clampExitRate((-diff) / longWeight)
				err := job.CloseOrders(&ExitReq{Tag: tag, Dirt: core.OdDirtLong, ExitRate: exitRate})
				if err != nil {
					res.ErrorCount++
				} else {
					res.ReducedCount++
				}
				res.Actions = append(res.Actions, BatchRebalanceAction{
					Symbol:       sym,
					Kind:         RebalanceActionReduce,
					Short:        false,
					PrevWeight:   longWeight,
					TargetWeight: targetW,
					DeltaWeight:  diff,
					ExitRate:     exitRate,
					Err:          err,
				})
				continue
			}
			pendingOpens = append(pendingOpens, pendingBatchOpen{
				job:          job,
				symbol:       sym,
				short:        false,
				scaleIn:      true,
				prevWeight:   longWeight,
				targetWeight: targetW,
				deltaWeight:  diff,
				legalCost:    equity * diff,
				priority:     prio,
			})
			continue
		}

		// Target is SHORT: close any opposite LONG orders first.
		if hasLong {
			err := job.CloseOrders(&ExitReq{Tag: tag, Dirt: core.OdDirtLong})
			if err != nil {
				res.ErrorCount++
			} else {
				res.ClosedCount++
				if !hasShort {
					res.ReversedCount++
				}
			}
			res.Actions = append(res.Actions, BatchRebalanceAction{
				Symbol:       sym,
				Kind:         RebalanceActionClose,
				Short:        false,
				PrevWeight:   longWeight,
				TargetWeight: targetW,
				DeltaWeight:  -longWeight,
				ExitRate:     1,
				Err:          err,
			})
		}
		if !hasShort || shortWeight <= rebalanceWeightZeroEps {
			pendingOpens = append(pendingOpens, pendingBatchOpen{
				job:          job,
				symbol:       sym,
				short:        true,
				scaleIn:      false,
				prevWeight:   netPrevWeight,
				targetWeight: targetW,
				deltaWeight:  absTarget,
				legalCost:    equity * absTarget,
				priority:     prio,
			})
			continue
		}
		diff := absTarget - shortWeight
		if math.Abs(diff) < drift || math.Abs(diff) <= rebalanceWeightZeroEps {
			res.SkippedDriftCount++
			continue
		}
		if diff < 0 {
			exitRate := clampExitRate((-diff) / shortWeight)
			err := job.CloseOrders(&ExitReq{Tag: tag, Dirt: core.OdDirtShort, ExitRate: exitRate})
			if err != nil {
				res.ErrorCount++
			} else {
				res.ReducedCount++
			}
			res.Actions = append(res.Actions, BatchRebalanceAction{
				Symbol:       sym,
				Kind:         RebalanceActionReduce,
				Short:        true,
				PrevWeight:   -shortWeight,
				TargetWeight: targetW,
				DeltaWeight:  -diff,
				ExitRate:     exitRate,
				Err:          err,
			})
			continue
		}
		pendingOpens = append(pendingOpens, pendingBatchOpen{
			job:          job,
			symbol:       sym,
			short:        true,
			scaleIn:      true,
			prevWeight:   -shortWeight,
			targetWeight: targetW,
			deltaWeight:  diff,
			legalCost:    equity * diff,
			priority:     prio,
		})
	}

	// Pass 2: sort pending entries by LegalCost descending, tie-broken by neutral BarPriority then symbol.
	slices.SortFunc(pendingOpens, func(a, b pendingBatchOpen) int {
		if math.Abs(a.legalCost-b.legalCost) > rebalanceCostTieEps {
			return cmp.Compare(b.legalCost, a.legalCost)
		}
		if barMS > 0 && a.priority != b.priority {
			return cmp.Compare(a.priority, b.priority)
		}
		return cmp.Compare(a.symbol, b.symbol)
	})

	for _, item := range pendingOpens {
		if !isFinitePositive(item.legalCost) || item.legalCost < core.MinStakeAmount {
			res.SkippedDustCount++
			continue
		}
		req := buildRebalanceEnterReq(item.job, tag, item.short, item.legalCost, opts)
		err := item.job.OpenOrder(req)
		kind := RebalanceActionOpen
		if item.scaleIn {
			kind = RebalanceActionScaleIn
		}
		if err != nil {
			res.ErrorCount++
		} else if item.scaleIn {
			res.ScaledInCount++
		} else {
			res.OpenedCount++
		}
		signedDelta := item.deltaWeight
		if item.short {
			signedDelta = -item.deltaWeight
		}
		res.Actions = append(res.Actions, BatchRebalanceAction{
			Symbol:       item.symbol,
			Kind:         kind,
			Short:        item.short,
			PrevWeight:   item.prevWeight,
			TargetWeight: item.targetWeight,
			DeltaWeight:  signedDelta,
			LegalCost:    item.legalCost,
			Err:          err,
		})
	}

	return res
}

// InverseVolWeights computes cross-sectional risk-parity target weights from directional/strength
// signals (e.g. +1/-1/0) scaled by 1 / ta.NATR(atrPeriod), normalized so Σ|w| == targetGross.
//
// It evaluates ta.NATR unconditionally across every job in jobs on each call so incremental banta
// series caches stay aligned even when a symbol's signal is 0 on intermediate bars.
func InverseVolWeights(jobs []*StratJob, signals map[string]float64, atrPeriod int, targetGross float64) map[string]float64 {
	weights := make(map[string]float64)
	if len(jobs) == 0 {
		return weights
	}
	if atrPeriod <= 0 {
		atrPeriod = DefaultBatchATRPeriod
	}

	type rawCandidate struct {
		symbol string
		raw    float64
	}
	seen := make(map[string]struct{}, len(jobs))
	candidates := make([]rawCandidate, 0, len(jobs))

	for _, job := range jobs {
		if job == nil || job.Symbol == nil || job.Symbol.Symbol == "" || job.Env == nil {
			continue
		}
		sym := job.Symbol.Symbol
		if _, dup := seen[sym]; dup {
			continue
		}
		seen[sym] = struct{}{}
		if job.Env.High == nil || job.Env.Low == nil || job.Env.Close == nil {
			continue
		}
		// Unconditional indicator evaluation preserves banta incremental RMA cache alignment.
		natr := jobCurrentNATR(job.Env, atrPeriod)
		if len(signals) == 0 || !isFinitePositive(targetGross) {
			continue
		}
		sig, ok := signals[sym]
		if !ok || sig == 0 || math.IsNaN(sig) || math.IsInf(sig, 0) {
			continue
		}
		if !isFinitePositive(natr) {
			continue
		}
		raw := sig / natr
		if math.IsNaN(raw) || math.IsInf(raw, 0) || raw == 0 {
			continue
		}
		candidates = append(candidates, rawCandidate{symbol: sym, raw: raw})
	}

	if len(candidates) == 0 || !isFinitePositive(targetGross) {
		return weights
	}

	slices.SortFunc(candidates, func(a, b rawCandidate) int {
		return cmp.Compare(a.symbol, b.symbol)
	})
	sumAbs := 0.0
	for _, c := range candidates {
		sumAbs += math.Abs(c.raw)
	}
	if !isFinitePositive(sumAbs) {
		return weights
	}
	norm := targetGross / sumAbs
	for _, c := range candidates {
		w := c.raw * norm
		if !math.IsNaN(w) && !math.IsInf(w, 0) && w != 0 {
			weights[c.symbol] = w
		}
	}
	return weights
}

func jobCurrentNATR(env *ta.BarEnv, atrPeriod int) float64 {
	if env == nil || env.High == nil || env.Low == nil || env.Close == nil {
		return math.NaN()
	}
	natr := ta.NATR(env.High, env.Low, env.Close, atrPeriod).Get(0)
	if !math.IsNaN(natr) {
		return natr
	}
	// If BarEnv already holds pre-warmed OHLC history (e.g. first post-warmup OnBatchJobs call
	// or newly rotated symbol via SubWarmPairs where ta.NATR was not evaluated inside OnData),
	// compute Wilder NATR directly over the historical tail while ta.NATR warms up in parallel.
	nh, nl, nc := len(env.High.Data), len(env.Low.Data), len(env.Close.Data)
	n := min(nh, nl, nc)
	if n <= atrPeriod {
		return math.NaN()
	}
	window := max(atrPeriod*10, 128)
	if window > n {
		window = n
	}
	vals := tav.NATR(env.High.Data[nh-window:nh], env.Low.Data[nl-window:nl], env.Close.Data[nc-window:nc], atrPeriod)
	if len(vals) == 0 {
		return math.NaN()
	}
	return vals[len(vals)-1]
}

func batchJobsBarMSAndSeed(jobs []*StratJob) (int64, uint64) {
	var barMS int64
	var seed uint64
	var resolvedSeed bool
	var fallbackLegacy bool
	for _, job := range jobs {
		if job == nil {
			continue
		}
		if job.Env != nil {
			if job.Env.TimeStop > barMS {
				barMS = job.Env.TimeStop
			} else if job.Env.TimeStart > barMS {
				barMS = job.Env.TimeStart
			}
		}
		if job.LastBarMS > barMS {
			barMS = job.LastBarMS
		}
		if !resolvedSeed && job.Strat != nil {
			if cfg := job.Strat.runtimeConfigFor(job); cfg != nil {
				seed = cfg.BTBarOrderSeed
				resolvedSeed = true
			} else if !job.isExplicitRuntimeJob() {
				fallbackLegacy = true
			}
		}
	}
	if !resolvedSeed && fallbackLegacy {
		seed = config.Data.BTBarOrderSeed
	}
	return barMS, seed
}

func jobMarkPrice(job *StratJob) float64 {
	if job == nil {
		return 0
	}
	if !job.IsWarmUpState() && job.Symbol != nil && job.Symbol.Symbol != "" {
		if job.runtimePrices != nil {
			if px := job.currentPrice(""); isFinitePositive(px) {
				return px
			}
		} else if !job.isExplicitRuntimeJob() {
			if px := com.GetPriceSafe(job.Symbol.Symbol, ""); isFinitePositive(px) {
				return px
			}
		}
	}
	if job.Env != nil && job.Env.Close != nil && job.Env.Close.Len() > 0 {
		if px := job.Env.Close.Get(0); isFinitePositive(px) {
			return px
		}
	}
	return 0
}

func jobEntryRefPrice(job *StratJob, short bool) float64 {
	if job == nil {
		return 0
	}
	odSide := banexg.OdSideBuy
	if short {
		odSide = banexg.OdSideSell
	}
	if !job.IsWarmUpState() && job.Symbol != nil && job.Symbol.Symbol != "" {
		if job.runtimeCore != nil {
			if px := job.currentPrice(odSide); isFinitePositive(px) {
				return px
			}
		} else if !job.isExplicitRuntimeJob() {
			if px := com.GetPriceSafe(job.Symbol.Symbol, odSide); isFinitePositive(px) {
				return px
			}
		}
	}
	if job.Env != nil && job.Env.Close != nil && job.Env.Close.Len() > 0 {
		if px := job.Env.Close.Get(0); isFinitePositive(px) {
			return px
		}
	}
	return 0
}

func calcJobOrdersNotional(job *StratJob, orders []*ormo.InOutOrder, _ bool) float64 {
	if len(orders) == 0 {
		return 0
	}
	markPx := jobMarkPrice(job)
	total := 0.0
	for _, od := range orders {
		if od == nil || od.IOrder == nil || od.Status >= ormo.InOutStatusFullExit {
			continue
		}
		if od.Enter != nil && od.Enter.Filled > 0 {
			amt := od.HoldAmount()
			if amt <= core.AmtDust {
				continue
			}
			if isFinitePositive(markPx) {
				total += amt * markPx
			} else if hc := od.HoldCost(); isFinitePositive(hc) {
				total += hc
			}
			continue
		}
		if isFinitePositive(od.QuoteCost) {
			total += od.QuoteCost
		} else if od.Enter != nil && isFinitePositive(od.Enter.Amount) {
			px := markPx
			if !isFinitePositive(px) && isFinitePositive(od.Enter.Price) {
				px = od.Enter.Price
			}
			if isFinitePositive(px) {
				total += od.Enter.Amount * px
			}
		}
	}
	return total
}

func clampExitRate(rate float64) float64 {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate >= 1-1e-9 {
		return 1
	}
	if rate <= 0 {
		return core.AmtDust
	}
	return rate
}

func buildRebalanceEnterReq(job *StratJob, tag string, short bool, legalCost float64, opts BatchRebalanceOpts) *EnterReq {
	req := &EnterReq{
		Tag:       tag,
		Short:     short,
		LegalCost: legalCost,
	}
	if isFinitePositive(opts.Leverage) {
		req.Leverage = opts.Leverage
	}
	if isFinitePositive(opts.CallbackPct) {
		req.CallbackPct = opts.CallbackPct
	}
	refPrice := jobEntryRefPrice(job, short)
	if isFinitePositive(opts.StopLossRate) && isFinitePositive(refPrice) {
		slRate := opts.StopLossRate
		if !short && slRate >= 1 {
			slRate = 0.999
		}
		req.StopLossVal = refPrice * slRate
	}
	if isFinitePositive(opts.TakeProfitRate) && isFinitePositive(refPrice) {
		tpRate := opts.TakeProfitRate
		if short && tpRate >= 1 {
			tpRate = 0.999
		}
		req.TakeProfitVal = refPrice * tpRate
	}
	return req
}
