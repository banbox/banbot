package statistics

import (
	"errors"
	"math"
	"sort"
	"time"
)

type BoundaryFlowPeriod struct {
	StartMS, EndMS, AvailableMS, ReceivedMS int64
	// BeginNAV precedes beginning inflow; EndNAV follows ending withdrawal.
	// NAV includes all realized/unrealized PnL, income and execution costs.
	BeginNAV, EndNAV, StartInflow, EndOutflow float64
}

type TWRRReport struct {
	Returns                               []float64
	Wealth, TotalReturn, AnnualizedReturn float64
}

func linkTWRR(returns []float64, years float64) (TWRRReport, error) {
	if len(returns) == 0 || len(returns) > 1000000 || !afmlFinite(years) || years <= 0 {
		return TWRRReport{}, errors.New("statistics: invalid TWRR sample/elapsed years")
	}
	logwealth := 0.
	bankrupt := false
	for _, r := range returns {
		if !afmlFinite(r) || r < -1 {
			return TWRRReport{}, errors.New("statistics: invalid TWRR subperiod return")
		}
		if r == -1 {
			bankrupt = true
		} else {
			logwealth += math.Log1p(r)
		}
	}
	out := TWRRReport{Returns: append([]float64(nil), returns...)}
	if bankrupt {
		out.TotalReturn = -1
		out.AnnualizedReturn = -1
		return out, nil
	}
	out.Wealth = math.Exp(logwealth)
	out.TotalReturn = math.Expm1(logwealth)
	out.AnnualizedReturn = math.Expm1(logwealth / years)
	if !afmlFinite(out.Wealth) || !afmlFinite(out.AnnualizedReturn) {
		return TWRRReport{}, errors.New("statistics: TWRR accumulation overflow")
	}
	return out, nil
}

// BoundaryFlowTWRR implements source14.4.1's geometric link and beginning-day
// inflows/end-day outflows. Every internal flow needs a valuation/split boundary;
// this is not a Modified Dietz approximation. Caller declares elapsed years and
// includes terminal liquidation costs in final NAV if claiming liquidated PnL.
func BoundaryFlowTWRR(periods []BoundaryFlowPeriod, years float64, cutoffMS int64) (TWRRReport, error) {
	if len(periods) == 0 || len(periods) > 1000000 || cutoffMS < 0 {
		return TWRRReport{}, errors.New("statistics: invalid TWRR periods")
	}
	returns := make([]float64, len(periods))
	for i, p := range periods {
		if p.StartMS < 0 || p.EndMS <= p.StartMS || p.AvailableMS < p.EndMS || p.ReceivedMS < p.AvailableMS || p.ReceivedMS > cutoffMS || (i > 0 && (p.StartMS != periods[i-1].EndMS || math.Abs(p.BeginNAV-periods[i-1].EndNAV) > 1e-12*math.Max(1, math.Abs(p.BeginNAV)))) {
			return TWRRReport{}, errors.New("statistics: discontinuous or unavailable TWRR valuation")
		}
		for _, v := range []float64{p.BeginNAV, p.EndNAV, p.StartInflow, p.EndOutflow} {
			if !afmlFinite(v) || v < 0 {
				return TWRRReport{}, errors.New("statistics: invalid TWRR NAV/flow")
			}
		}
		capital := p.BeginNAV + p.StartInflow
		end := p.EndNAV + p.EndOutflow
		if capital <= 0 || !afmlFinite(capital) || !afmlFinite(end) {
			return TWRRReport{}, errors.New("statistics: invalid TWRR invested capital")
		}
		returns[i] = end/capital - 1
	}
	return linkTWRR(returns, years)
}

type SourceTWRRLeg struct {
	PreviousHolding, Holding, PreviousClean, Clean, PreviousDirty, Accrual float64
	TransactedClean, TransactedDirty                                       float64
}

// SourceTWRRSubperiod implements the holdings equations printed in14.4.1.
// Transacted prices are average prices for the current subperiod (the displayed
// equation indexes their bars t-1). Accrual is income per prior-held unit;
// PreviousDirty includes prior accrued interest. Net purchases are funded once
// by max(0,sum(transacted_dirty*deltaHolding)), not max per instrument. Broker
// costs must be provided separately in dollars to subtract from the period PnL.
func SourceTWRRSubperiod(legs []SourceTWRRLeg, executionCosts float64) (returnRate, pnl, capital float64, err error) {
	if len(legs) == 0 || len(legs) > 10000 || !afmlFinite(executionCosts) || executionCosts < 0 {
		return 0, 0, 0, errors.New("statistics: invalid source TWRR holdings/costs")
	}
	purchases := 0.
	pnl = -executionCosts
	for _, l := range legs {
		for _, v := range []float64{l.PreviousHolding, l.Holding, l.PreviousClean, l.Clean, l.PreviousDirty, l.Accrual, l.TransactedClean, l.TransactedDirty} {
			if !afmlFinite(v) {
				return 0, 0, 0, errors.New("statistics: nonfinite source TWRR leg")
			}
		}
		delta := l.Holding - l.PreviousHolding
		pnl += (l.Clean-l.PreviousClean+l.Accrual)*l.PreviousHolding + delta*(l.Clean-l.TransactedClean)
		capital += l.PreviousDirty * l.PreviousHolding
		purchases += l.TransactedDirty * delta
	}
	capital += math.Max(0, purchases)
	if capital <= 0 || !afmlFinite(capital) || !afmlFinite(pnl) {
		return 0, 0, 0, errors.New("statistics: undefined source TWRR capital")
	}
	returnRate = pnl / capital
	if !afmlFinite(returnRate) || returnRate < -1 {
		return 0, 0, 0, errors.New("statistics: undefined source TWRR return")
	}
	return
}

type ExecutionCostReport struct {
	NetPnL, Turnovers, Fees, Slippage, TotalExecutionCosts                          float64
	FeesPerTurnover, SlippagePerTurnover, NetPnLPerTurnover, ReturnOnExecutionCosts float64
	HasTurnoverMetrics, HasReturnOnCosts                                            bool
}

// ExecutionCostMetrics implements14.6 with explicitly supplied portfolio
// turnover units. The source does not define whether a one-way or round-trip
// turnover is used: caller must keep this denominator consistent across reports.
// NetPnL already includes fees/slippage, so they are not subtracted again.
// Slippage can be negative for price improvement; nonpositive net execution
// cost has undefined return-on-costs, indicated by its flag.
func ExecutionCostMetrics(netPnL, turnovers, fees, slippage float64) (ExecutionCostReport, error) {
	r := ExecutionCostReport{NetPnL: netPnL, Turnovers: turnovers, Fees: fees, Slippage: slippage, TotalExecutionCosts: fees + slippage}
	if !afmlFinite(netPnL) || !afmlFinite(turnovers) || turnovers < 0 || !afmlFinite(fees) || fees < 0 || !afmlFinite(slippage) || !afmlFinite(r.TotalExecutionCosts) {
		return ExecutionCostReport{}, errors.New("statistics: invalid execution-cost report")
	}
	if turnovers > 0 {
		r.HasTurnoverMetrics = true
		r.FeesPerTurnover = fees / turnovers
		r.SlippagePerTurnover = slippage / turnovers
		r.NetPnLPerTurnover = netPnL / turnovers
	}
	if r.TotalExecutionCosts > 0 {
		r.HasReturnOnCosts = true
		r.ReturnOnExecutionCosts = netPnL / r.TotalExecutionCosts
	}
	for _, v := range []float64{r.FeesPerTurnover, r.SlippagePerTurnover, r.NetPnLPerTurnover, r.ReturnOnExecutionCosts} {
		if !afmlFinite(v) {
			return ExecutionCostReport{}, errors.New("statistics: execution-cost ratio overflow")
		}
	}
	return r, nil
}

type ConcentrationReport struct {
	PositiveHHI, NegativeHHI, MonthlyCountHHI, Drawdown95, SourceTuW95MS              float64
	HasPositiveHHI, HasNegativeHHI, HasMonthlyCountHHI, HasDrawdown95, HasSourceTuW95 bool
	DrawdownEpisodes                                                                  int
}

// LifecycleConcentration combines source14.3-14.4 statistics. Return timestamps
// are bet realization dates, monthly counts are UTC calendar groups (observed
// groups only, source pandas behavior). Positive/negative returns are separate.
// Percentiles use linear interpolation; source TuW is the next-drawdown-HWM
// proxy from SourceDrawdownEpisodes, not actual peak-to-recovery time.
func LifecycleConcentration(betTimes []int64, betReturns []float64, equityTimes []int64, equity []float64, dollarDrawdown bool) (ConcentrationReport, error) {
	if len(betTimes) != len(betReturns) || len(betTimes) > 1000000 || len(equityTimes) > 1000000 || len(equity) > 1000000 {
		return ConcentrationReport{}, errors.New("statistics: invalid lifecycle bet observations")
	}
	r := ConcentrationReport{}
	pos, neg := []float64{}, []float64{}
	months := map[int64]float64{}
	for i, v := range betReturns {
		if betTimes[i] < 0 || (i > 0 && betTimes[i] < betTimes[i-1]) || !afmlFinite(v) {
			return r, errors.New("statistics: invalid lifecycle chronology/return")
		}
		if v > 0 {
			pos = append(pos, v)
		} else if v < 0 {
			neg = append(neg, v)
		}
		date := time.UnixMilli(betTimes[i]).UTC()
		months[int64(date.Year())*12+int64(date.Month())]++
	}
	counts := make([]float64, 0, len(months))
	for _, v := range months {
		counts = append(counts, v)
	}
	sort.Float64s(counts)
	var err error
	r.PositiveHHI, err = NormalizedHHI(pos)
	if err != nil {
		return r, err
	}
	r.NegativeHHI, err = NormalizedHHI(neg)
	if err != nil {
		return r, err
	}
	r.MonthlyCountHHI, err = NormalizedHHI(counts)
	if err != nil {
		return r, err
	}
	r.HasPositiveHHI = afmlFinite(r.PositiveHHI)
	r.HasNegativeHHI = afmlFinite(r.NegativeHHI)
	r.HasMonthlyCountHHI = afmlFinite(r.MonthlyCountHHI)
	if !r.HasPositiveHHI {
		r.PositiveHHI = 0
	}
	if !r.HasNegativeHHI {
		r.NegativeHHI = 0
	}
	if !r.HasMonthlyCountHHI {
		r.MonthlyCountHHI = 0
	}
	episodes, err := SourceDrawdownEpisodes(equityTimes, equity, dollarDrawdown)
	if err != nil {
		return r, err
	}
	dd, tuw := []float64{}, []float64{}
	for _, e := range episodes {
		dd = append(dd, e.Drawdown)
		if e.TimeValid {
			tuw = append(tuw, float64(e.TimeToNextDrawdownMS))
		}
	}
	r.DrawdownEpisodes = len(episodes)
	q95 := func(v []float64) float64 {
		sort.Float64s(v)
		h := .95 * float64(len(v)-1)
		i := int(h)
		if i+1 == len(v) {
			return v[i]
		}
		return v[i]*(1-(h-float64(i))) + v[i+1]*(h-float64(i))
	}
	if len(dd) > 0 {
		r.Drawdown95 = q95(dd)
		r.HasDrawdown95 = true
	}
	if len(tuw) > 0 {
		r.SourceTuW95MS = q95(tuw)
		r.HasSourceTuW95 = true
	}
	return r, nil
}
