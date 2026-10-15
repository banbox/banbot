package statistics

import (
	"math"
	"testing"
)

func generalFixture() ([]PITScalar, []LifecyclePeriod) {
	positions := []PITScalar{{0, 0, 0, 0}, {10, 10, 10, 2}, {30, 30, 30, 1}, {40, 40, 40, -3}, {50, 50, 50, 0}}
	periods := []LifecyclePeriod{
		{0, 10, 10, 10, 100, 0, 100, .01, .02},
		{10, 30, 30, 30, 200, 200, 200, .02, .04},
		{30, 40, 40, 40, 100, 100, 400, .03, .06},
		{40, 50, 50, 50, 100, 300, 300, .04, .08},
	}
	return positions, periods
}

func TestLifecycleGeneralCharacteristicsAnalytic(t *testing.T) {
	p, periods := generalFixture()
	r, err := LifecycleGeneralCharacteristics(p, periods, 2, 50)
	if err != nil {
		t.Fatal(err)
	}
	// Duration weights .2,.4,.2,.2; adds/reductions stay in the long bet.
	if math.Abs(r.AverageAUM-140) > 1e-12 || math.Abs(r.AverageDollarPosition-160) > 1e-12 || r.MaximumDollarPosition != 300 || math.Abs(r.Leverage-160./140) > 1e-12 || math.Abs(r.AnnualizedTurnover-1000./2/140) > 1e-12 || r.Bets != 2 || r.CompletedBets != 2 || r.LongBets != 1 || r.LongFraction != .5 || r.BetsPerYear != 1 || math.Abs(r.UnderlyingCorrelation-1) > 1e-12 || !r.Holding.Valid || r.Holding.ClosedQuantity != 5 || r.Holding.MeanMS != 16 {
		t.Fatal(r)
	}
	// Completed durations: 1*(30-10)+1*(40-10)+3*(50-40)=80; /5=16.
	p[len(p)-1].Value = -3
	r, err = LifecycleGeneralCharacteristics(p, periods, 2, 50)
	if err != nil || r.CompletedBets != 1 || r.Bets != 2 || r.Holding.MeanMS != 25 || r.Holding.ClosedQuantity != 2 {
		t.Fatal("open terminal bet closed", r, err)
	}
}

func TestLifecycleGeneralCharacteristicsUndefinedAndPIT(t *testing.T) {
	p, periods := generalFixture()
	for i := range p {
		p[i].Value = 0
	}
	for i := range periods {
		periods[i].AUMDollars = 0
		periods[i].StrategyReturn = 1
	}
	r, err := LifecycleGeneralCharacteristics(p, periods, 1, 50)
	if err != nil || r.HasLeverage || r.HasTurnover || r.HasLongFraction || r.HasUnderlyingCorrelation || r.Holding.Valid || r.Holding.MeanMS != 0 {
		t.Fatal(r, err)
	}
	p[0].ReceivedMS = 51
	if _, err := LifecycleGeneralCharacteristics(p, periods, 1, 50); err == nil {
		t.Fatal("unreceived position accepted")
	}
	p[0].ReceivedMS = 0
	periods[0].ReceivedMS = 51
	if _, err := LifecycleGeneralCharacteristics(p, periods, 1, 50); err == nil {
		t.Fatal("unreceived valuation accepted")
	}
	periods[0].ReceivedMS = 10
	periods[0].TradedDollars = -1
	if _, err := LifecycleGeneralCharacteristics(p, periods, 1, 50); err == nil {
		t.Fatal("negative traded magnitude accepted")
	}
}

func TestEvaluatedCapacityNoExtrapolation(t *testing.T) {
	points := []EvaluatedCapacityPoint{{PITScalar{0, 0, 0, 100}, PITScalar{1, 1, 1, 2}}, {PITScalar{0, 0, 0, 300}, PITScalar{1, 1, 1, .5}}, {PITScalar{0, 0, 0, 200}, PITScalar{1, 1, 1, 1}}}
	a, valid, err := EvaluatedCapacity(points, 1, 1)
	if err != nil || !valid || a != 200 {
		t.Fatal(a, valid, err)
	}
	if _, valid, err = EvaluatedCapacity(points, 3, 1); err != nil || valid {
		t.Fatal(valid, err)
	}
	points[0].RiskAdjustedPerformance.ReceivedMS = 2
	if _, _, err = EvaluatedCapacity(points, 1, 1); err == nil {
		t.Fatal("unreceived capacity score accepted")
	}
}
