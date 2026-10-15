package research

import (
	"errors"
	"math"
	"sort"

	"github.com/banbox/banbot/factor"
)

// TradeLifecycle contains observed execution facts, never desired target ages.
// Returns and costs are fractions of the same frozen strategy NAV.
type TradeLifecycle struct {
	SID                                          int32
	Group, BatchID                               string
	EntryAt, ExitRequestedAt, ExitAt             int64
	GrossReturn, Fees, Slippage, Impact, Funding float64
}
type LifecycleSummary struct {
	Trades                                      int
	Gross, Net, Fees, Slippage, Impact, Funding float64
	MeanHoldingMS, MeanExitDelayMS              float64
	MaxDrawdown                                 float64
}
type LifecycleReport struct {
	Total            LifecycleSummary
	ByAsset          map[int32]LifecycleSummary
	ByGroup, ByBatch map[string]LifecycleSummary
}

func SummarizeLifecycles(trades []TradeLifecycle) (LifecycleReport, error) {
	rows := append([]TradeLifecycle(nil), trades...)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].ExitAt != rows[j].ExitAt {
			return rows[i].ExitAt < rows[j].ExitAt
		}
		return rows[i].SID < rows[j].SID
	})
	out := LifecycleReport{ByAsset: map[int32]LifecycleSummary{}, ByGroup: map[string]LifecycleSummary{}, ByBatch: map[string]LifecycleSummary{}}
	assetRows := map[int32][]TradeLifecycle{}
	groupRows, batchRows := map[string][]TradeLifecycle{}, map[string][]TradeLifecycle{}
	for _, row := range rows {
		if row.SID <= 0 || row.EntryAt <= 0 || row.ExitRequestedAt < row.EntryAt || row.ExitAt < row.ExitRequestedAt || row.Fees < 0 || row.Slippage < 0 || row.Impact < 0 || !finiteValues(row.GrossReturn, row.Fees, row.Slippage, row.Impact, row.Funding) {
			return LifecycleReport{}, errors.New("research: invalid execution lifecycle")
		}
		assetRows[row.SID] = append(assetRows[row.SID], row)
		groupRows[row.Group] = append(groupRows[row.Group], row)
		if row.BatchID != "" {
			batchRows[row.BatchID] = append(batchRows[row.BatchID], row)
		}
	}
	summarize := func(rows []TradeLifecycle) LifecycleSummary {
		var s LifecycleSummary
		peak, pnl := 0.0, 0.0
		for _, row := range rows {
			s.Trades++
			s.Gross += row.GrossReturn
			s.Fees += row.Fees
			s.Slippage += row.Slippage
			s.Impact += row.Impact
			s.Funding += row.Funding
			s.MeanHoldingMS += float64(row.ExitAt - row.EntryAt)
			s.MeanExitDelayMS += float64(row.ExitAt - row.ExitRequestedAt)
			pnl += row.GrossReturn - row.Fees - row.Slippage - row.Impact - row.Funding
			peak = max(peak, pnl)
			s.MaxDrawdown = max(s.MaxDrawdown, peak-pnl)
		}
		s.Net = s.Gross - s.Fees - s.Slippage - s.Impact - s.Funding
		if s.Trades > 0 {
			s.MeanHoldingMS /= float64(s.Trades)
			s.MeanExitDelayMS /= float64(s.Trades)
		}
		return s
	}
	out.Total = summarize(rows)
	for key, rows := range assetRows {
		out.ByAsset[key] = summarize(rows)
	}
	for key, rows := range groupRows {
		out.ByGroup[key] = summarize(rows)
	}
	for key, rows := range batchRows {
		out.ByBatch[key] = summarize(rows)
	}
	return out, nil
}
func finiteValues(values ...float64) bool {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

type CapitalSample struct {
	DecisionTime                                                          int64
	NAV, GrossNotional, Cash, ExternalTradedNotional, TargetDeltaNotional float64
}
type CapitalSummary struct {
	Samples                                                                                               int
	MeanUtilization, MeanCashRatio, ExternalTradedNotional, TargetDeltaNotional, MeanOneWayTargetTurnover float64
}

// SummarizeCapital keeps desired target changes and real external execution
// separate; same-asset internal cohort transfers therefore have zero real cost.
func SummarizeCapital(samples []CapitalSample) (CapitalSummary, error) {
	var report CapitalSummary
	last := int64(0)
	for _, row := range samples {
		if row.DecisionTime <= last || row.NAV <= 0 || min(row.GrossNotional, row.Cash, row.ExternalTradedNotional, row.TargetDeltaNotional) < 0 || !finiteValues(row.NAV, row.GrossNotional, row.Cash, row.ExternalTradedNotional, row.TargetDeltaNotional) {
			return CapitalSummary{}, errors.New("research: invalid capital utilization evidence")
		}
		last = row.DecisionTime
		report.Samples++
		report.MeanUtilization += row.GrossNotional / row.NAV
		report.MeanCashRatio += row.Cash / row.NAV
		report.ExternalTradedNotional += row.ExternalTradedNotional
		report.TargetDeltaNotional += row.TargetDeltaNotional
		report.MeanOneWayTargetTurnover += .5 * row.TargetDeltaNotional / row.NAV
	}
	if report.Samples > 0 {
		denom := float64(report.Samples)
		report.MeanUtilization /= denom
		report.MeanCashRatio /= denom
		report.MeanOneWayTargetTurnover /= denom
	}
	return report, nil
}

type CapacityCostSpec struct {
	FeeRate, SlippageRate, ImpactCoefficient, MaxParticipation float64
}
type CapacityCost struct {
	Participation, Fee, Slippage, Impact, Funding, Total float64
	Feasible                                             bool
}

// EstimateCapacityCost uses quote-currency turnover and ADV, square-root
// impact, and signed held notional for funding. Funding can be a credit.
func EstimateCapacityCost(spec CapacityCostSpec, tradedNotional, adv, volatility, heldNotional, fundingRate float64) (CapacityCost, error) {
	if !finiteValues(spec.FeeRate, spec.SlippageRate, spec.ImpactCoefficient, spec.MaxParticipation, tradedNotional, adv, volatility, heldNotional, fundingRate) || min(spec.FeeRate, spec.SlippageRate, spec.ImpactCoefficient, spec.MaxParticipation, tradedNotional, volatility) < 0 || adv <= 0 || spec.MaxParticipation > 1 {
		return CapacityCost{}, errors.New("research: invalid capacity/cost inputs")
	}
	r := CapacityCost{Participation: tradedNotional / adv, Fee: tradedNotional * spec.FeeRate, Slippage: tradedNotional * spec.SlippageRate, Funding: heldNotional * fundingRate}
	r.Impact = tradedNotional * spec.ImpactCoefficient * volatility * math.Sqrt(r.Participation)
	r.Total = r.Fee + r.Slippage + r.Impact + r.Funding
	r.Feasible = spec.MaxParticipation == 0 || r.Participation <= spec.MaxParticipation
	return r, nil
}

type AttributionInput struct {
	PortfolioGross, BenchmarkReturn, Selection, Transition, Sizing, ExecutionCosts float64
	Style, Group                                                                   map[string]float64
}
type AttributionReport struct {
	Net, Excess, Selection, Transition, Sizing, ExecutionCosts, Residual float64
	Style, Group                                                         map[string]float64
}

// Attribute keeps style/group views separate from the additive implementation
// stages to avoid counting the same PnL twice. Residual reconciles stages.
func Attribute(input AttributionInput) (AttributionReport, error) {
	if !finiteValues(input.PortfolioGross, input.BenchmarkReturn, input.Selection, input.Transition, input.Sizing, input.ExecutionCosts) || input.ExecutionCosts < 0 {
		return AttributionReport{}, errors.New("research: invalid attribution")
	}
	r := AttributionReport{Net: input.PortfolioGross - input.ExecutionCosts, Selection: input.Selection, Transition: input.Transition, Sizing: input.Sizing, ExecutionCosts: input.ExecutionCosts, Style: map[string]float64{}, Group: map[string]float64{}}
	r.Excess = r.Net - input.BenchmarkReturn
	r.Residual = input.PortfolioGross - input.BenchmarkReturn - input.Selection - input.Transition - input.Sizing
	for key, value := range input.Style {
		if !finiteValues(value) {
			return AttributionReport{}, errors.New("research: nonfinite style attribution")
		}
		r.Style[key] = value
	}
	for key, value := range input.Group {
		if !finiteValues(value) {
			return AttributionReport{}, errors.New("research: nonfinite group attribution")
		}
		r.Group[key] = value
	}
	return r, nil
}

// RankAutocorrelation joins only valid members of the supplied frozen pool.
func RankAutocorrelation(sids []int32, previous, current map[int32]factor.Numeric) (factor.Numeric, int) {
	rows := paired(uniqueSIDs(sids), previous, current)
	return correlation(rows, true), len(rows)
}

// HACMean uses Bartlett/Newey-West lag weights for overlapping return samples.
// The caller chooses lag from the actual sampling/holding scheme.
func HACMean(values []float64, lag int) (mean, standardError float64, err error) {
	if len(values) < 2 || lag < 0 || lag >= len(values) || !finiteValues(values...) {
		return 0, 0, errors.New("research: invalid HAC samples/lag")
	}
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))
	variance := 0.0
	for _, value := range values {
		variance += (value - mean) * (value - mean)
	}
	variance /= float64(len(values))
	for k := 1; k <= lag; k++ {
		cov := 0.0
		for i := k; i < len(values); i++ {
			cov += (values[i] - mean) * (values[i-k] - mean)
		}
		variance += 2 * (1 - float64(k)/float64(lag+1)) * cov / float64(len(values))
	}
	standardError = math.Sqrt(max(0, variance) / float64(len(values)))
	return
}
