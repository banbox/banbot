package statistics

import (
	"math"
	"testing"
	"time"
)

func TestBoundaryFlowTWRRSourceTiming(t *testing.T) {
	p := []BoundaryFlowPeriod{{0, 10, 10, 10, 100, 200, 100, 20}, {10, 20, 20, 20, 200, 170, 0, 10}}
	r, err := BoundaryFlowTWRR(p, 2, 20)
	if err != nil {
		t.Fatal(err)
	}
	// (200+20)/(100+100)=1.1; (170+10)/200=.9; wealth=.99.
	if math.Abs(r.Wealth-.99) > 1e-12 || math.Abs(r.TotalReturn+.01) > 1e-12 || math.Abs(r.AnnualizedReturn-(math.Sqrt(.99)-1)) > 1e-12 {
		t.Fatal(r)
	}
	p[0].ReceivedMS = 21
	if _, err = BoundaryFlowTWRR(p, 2, 20); err == nil {
		t.Fatal("unreceived valuation accepted")
	}
	p[0].ReceivedMS = 10
	p[1].BeginNAV = 201
	if _, err = BoundaryFlowTWRR(p, 2, 20); err == nil {
		t.Fatal("unvalued external jump accepted")
	}
}

func TestSourceTWRRHoldingsAndNetRampup(t *testing.T) {
	legs := []SourceTWRRLeg{{PreviousHolding: 1, Holding: 2, PreviousClean: 100, Clean: 110, PreviousDirty: 101, Accrual: 2, TransactedClean: 105, TransactedDirty: 106}, {PreviousHolding: 1, Holding: 0, PreviousClean: 50, Clean: 50, PreviousDirty: 50, TransactedClean: 50, TransactedDirty: 50}}
	r, pnl, capital, err := SourceTWRRSubperiod(legs, 1)
	if err != nil {
		t.Fatal(err)
	}
	// PnL=(10+2)*1+1*(110-105)-fee1=16. K=101+50+max(0,106-50)=207.
	if pnl != 16 || capital != 207 || math.Abs(r-16./207) > 1e-12 {
		t.Fatalf("r=%v pnl=%v capital=%v", r, pnl, capital)
	}
	if _, _, _, err = SourceTWRRSubperiod([]SourceTWRRLeg{{PreviousHolding: -1, Holding: -1, PreviousDirty: 1}}, 0); err == nil {
		t.Fatal("negative denominator accepted")
	}
}

func TestLifecycleCostsAndConcentration(t *testing.T) {
	c, err := ExecutionCostMetrics(100, 2, 10, 15)
	if err != nil || c.FeesPerTurnover != 5 || c.SlippagePerTurnover != 7.5 || c.NetPnLPerTurnover != 50 || c.ReturnOnExecutionCosts != 4 {
		t.Fatal(c, err)
	}
	c, err = ExecutionCostMetrics(1, 0, 0, -1)
	if err != nil || c.HasTurnoverMetrics || c.HasReturnOnCosts {
		t.Fatal(c, err)
	}
	bt := []int64{}
	for i := 0; i < 6; i++ {
		bt = append(bt, time.Date(2020, time.Month(1+i/2), 1+i%2, 0, 0, 0, 0, time.UTC).UnixMilli())
	}
	r, err := LifecycleConcentration(bt, []float64{1, -1, 1, -1, 1, -1}, []int64{0, 10, 20, 30, 40, 50, 60}, []float64{100, 80, 100, 110, 120, 90, 120}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !r.HasPositiveHHI || !r.HasNegativeHHI || !r.HasMonthlyCountHHI || r.PositiveHHI != 0 || r.NegativeHHI != 0 || r.MonthlyCountHHI != 0 || math.Abs(r.Drawdown95-.2475) > 1e-12 || r.SourceTuW95MS != 40 {
		t.Fatal(r)
	}
	// Final drawdown lacks source-proxy duration; it must not be zero-filled.
	if r.DrawdownEpisodes != 2 || !r.HasSourceTuW95 {
		t.Fatal(r)
	}
}
