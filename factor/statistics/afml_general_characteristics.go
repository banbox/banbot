package statistics

import (
	"errors"
	"math"
)

// LifecyclePeriod holds complete, received research valuations and executions
// on [StartMS,EndMS). GrossPositionDollars includes absolute long/short exposure;
// AUMDollars is the caller's funded AUM definition, not signed net exposure.
// Returns use the same interval for strategy and underlying universe.
type LifecyclePeriod struct {
	StartMS, EndMS, AvailableMS, ReceivedMS         int64
	AUMDollars, GrossPositionDollars, TradedDollars float64
	StrategyReturn, UnderlyingReturn                float64
}

type GeneralCharacteristicsReport struct {
	StartMS, EndMS                                                      int64
	Bets, CompletedBets, LongBets                                       int
	AverageAUM, AverageDollarPosition, MaximumDollarPosition            float64
	Leverage, LongFraction, BetsPerYear, AnnualizedTurnover             float64
	UnderlyingCorrelation                                               float64
	HasLeverage, HasLongFraction, HasTurnover, HasUnderlyingCorrelation bool
	Holding                                                             HoldingPeriodReport
}

// LifecycleGeneralCharacteristics implements the descriptions in AFML14.3.
// Period dollar valuations use duration-weighted averages, an explicit choice
// for irregular data. Position quantities define consecutive same-side bets;
// open terminal bets count in frequency/long fraction, not completed holding.
// Explicit elapsed years avoids an invented trading-calendar annualization.
func LifecycleGeneralCharacteristics(positionRows []PITScalar, periods []LifecyclePeriod, years float64, cutoffMS int64) (GeneralCharacteristicsReport, error) {
	var out GeneralCharacteristicsReport
	if len(positionRows) < 2 || len(positionRows) > 1000000 || len(periods) != len(positionRows)-1 || !afmlFinite(years) || years <= 0 || cutoffMS < 0 {
		return out, errors.New("statistics: invalid lifecycle general-characteristics bounds")
	}
	times, positions := make([]int64, len(positionRows)), make([]float64, len(positionRows))
	for i, p := range positionRows {
		if !validScalar(p) || !visibleScalar(p, cutoffMS) {
			return out, errors.New("statistics: invalid or unavailable lifecycle position")
		}
		times[i], positions[i] = p.EventMS, p.Value
	}
	if err := validatePositionSeries(times, positions); err != nil {
		return out, err
	}
	out.StartMS, out.EndMS = times[0], times[len(times)-1]
	totalDuration := float64(out.EndMS - out.StartMS)
	strategy, underlying := make([]float64, len(periods)), make([]float64, len(periods))
	traded := 0.
	for i, p := range periods {
		if p.StartMS != times[i] || p.EndMS != times[i+1] || p.AvailableMS < p.EndMS || p.ReceivedMS < p.AvailableMS || p.ReceivedMS > cutoffMS {
			return out, errors.New("statistics: discontinuous or unavailable lifecycle period")
		}
		for _, v := range []float64{p.AUMDollars, p.GrossPositionDollars, p.TradedDollars, p.StrategyReturn, p.UnderlyingReturn} {
			if !afmlFinite(v) {
				return out, errors.New("statistics: nonfinite lifecycle period")
			}
		}
		if p.AUMDollars < 0 || p.GrossPositionDollars < 0 || p.TradedDollars < 0 {
			return out, errors.New("statistics: negative lifecycle dollar magnitude")
		}
		fraction := float64(p.EndMS-p.StartMS) / totalDuration
		out.AverageAUM += fraction * p.AUMDollars
		out.AverageDollarPosition += fraction * p.GrossPositionDollars
		out.MaximumDollarPosition = math.Max(out.MaximumDollarPosition, p.GrossPositionDollars)
		traded += p.TradedDollars
		strategy[i], underlying[i] = p.StrategyReturn, p.UnderlyingReturn
	}
	for i, p := range positions {
		old := 0.
		if i > 0 {
			old = positions[i-1]
		}
		flip := old != 0 && p != 0 && math.Signbit(old) != math.Signbit(p)
		if old != 0 && (p == 0 || flip) {
			out.CompletedBets++
		}
		if p != 0 && (old == 0 || flip) {
			out.Bets++
			if p > 0 {
				out.LongBets++
			}
		}
	}
	out.BetsPerYear = float64(out.Bets) / years
	if out.Bets > 0 {
		out.HasLongFraction = true
		out.LongFraction = float64(out.LongBets) / float64(out.Bets)
	}
	if out.AverageAUM > 0 {
		out.HasLeverage, out.HasTurnover = true, true
		out.Leverage = out.AverageDollarPosition / out.AverageAUM
		out.AnnualizedTurnover = traded / years / out.AverageAUM
	}
	out.UnderlyingCorrelation, out.HasUnderlyingCorrelation = cpcvCorrelation(strategy, underlying)
	var err error
	out.Holding, err = PositionHoldingPeriod(times, positions)
	if err != nil {
		return out, err
	}
	if !out.Holding.Valid {
		out.Holding.MeanMS = 0
	}
	for _, v := range []float64{out.AverageAUM, out.AverageDollarPosition, traded, out.Leverage, out.AnnualizedTurnover, out.BetsPerYear, out.UnderlyingCorrelation} {
		if !afmlFinite(v) {
			return out, errors.New("statistics: lifecycle metric overflow")
		}
	}
	return out, nil
}

type EvaluatedCapacityPoint struct {
	AUM, RiskAdjustedPerformance PITScalar
}

// EvaluatedCapacity selects the highest observed AUM meeting a declared target.
// It cannot estimate execution costs or capacity beyond supplied scenarios and
// never interpolates an untested AUM. All evaluated points must be received.
func EvaluatedCapacity(points []EvaluatedCapacityPoint, target float64, cutoffMS int64) (aum float64, valid bool, err error) {
	if len(points) == 0 || len(points) > 1000000 || !afmlFinite(target) || cutoffMS < 0 {
		return 0, false, errors.New("statistics: invalid evaluated capacity frontier")
	}
	for _, p := range points {
		if !validScalar(p.AUM) || !validScalar(p.RiskAdjustedPerformance) || !visibleScalar(p.AUM, cutoffMS) || !visibleScalar(p.RiskAdjustedPerformance, cutoffMS) || p.AUM.Value <= 0 {
			return 0, false, errors.New("statistics: invalid or unavailable capacity point")
		}
		if p.RiskAdjustedPerformance.Value >= target && (!valid || p.AUM.Value > aum) {
			aum, valid = p.AUM.Value, true
		}
	}
	return
}
