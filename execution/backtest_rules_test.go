package execution

import (
	"math"
	"testing"
)

func TestMergeFundingPointsOrdersAndReplacesDuplicates(t *testing.T) {
	stored := []FundingPoint{{AtMS: 10, Rate: 0.1}, {AtMS: 30, Rate: 0.3}}
	if got := MergeFundingPoints(stored, nil); len(got) != 2 || &got[0] != &stored[0] {
		t.Fatalf("empty second input must return the first unchanged, got %+v", got)
	}
	got := MergeFundingPoints(stored, []FundingPoint{{AtMS: 30, Rate: 0.9}, {AtMS: 20, Rate: 0.2}})
	want := []FundingPoint{{AtMS: 10, Rate: 0.1}, {AtMS: 20, Rate: 0.2}, {AtMS: 30, Rate: 0.9}}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("point %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if stored[1].Rate != 0.3 {
		t.Fatal("merge must not modify its first input")
	}
	if got := MergeFundingPoints(nil, []FundingPoint{{AtMS: 2}, {AtMS: 1}}); len(got) != 2 || got[0].AtMS != 1 {
		t.Fatalf("unordered second input must be sorted, got %+v", got)
	}
}

func TestRealFundingCostUsesHalfOpenHoldingPeriod(t *testing.T) {
	points := []FundingPoint{{AtMS: 100, Rate: 0.001}, {AtMS: 200, Rate: -0.0025}, {AtMS: 300, Rate: 0.01}}
	// Entry exactly at a settlement does not pay it; exit exactly at one does.
	cost, crossed := RealFundingCost(points, 1000, 100, 200)
	if crossed != 1 || math.Abs(cost+2.5) > 1e-12 {
		t.Fatalf("long cost=%v crossed=%d, want -2.5 and 1", cost, crossed)
	}
	cost, crossed = RealFundingCost(points, -1000, 0, 250)
	if crossed != 2 || math.Abs(cost-1.5) > 1e-12 {
		t.Fatalf("short cost=%v crossed=%d, want 1.5 and 2", cost, crossed)
	}
	if cost, crossed = RealFundingCost(points, 1000, 300, 400); cost != 0 || crossed != 0 {
		t.Fatalf("no settlement in period: cost=%v crossed=%d", cost, crossed)
	}
}

func TestApproxFundingCost(t *testing.T) {
	year := int64(365 * 86400000)
	if got := ApproxFundingCost(1000, 10, year); math.Abs(got-100) > 1e-9 {
		t.Fatalf("one year at 10%% on 1000 = %v, want 100", got)
	}
	if got := ApproxFundingCost(1000, -10, year/2); math.Abs(got+50) > 1e-9 {
		t.Fatalf("negative rate half year = %v, want -50", got)
	}
	for _, zero := range []float64{
		ApproxFundingCost(1000, 0, year), ApproxFundingCost(0, 10, year), ApproxFundingCost(1000, 10, 0),
		ApproxFundingCost(1000, math.NaN(), year), ApproxFundingCost(1000, math.Inf(1), year),
	} {
		if zero != 0 {
			t.Fatalf("degenerate input must cost nothing, got %v", zero)
		}
	}
}

func TestGapRules(t *testing.T) {
	hour := int64(3600_000)
	if got := GapConfirmDelayMS(15*60_000, 20, 0); got != 48*hour+1 {
		t.Fatalf("default minimum hours must dominate 20 bars of 15m: %d", got)
	}
	if got := GapConfirmDelayMS(24*hour, 20, 48); got != 20*24*hour+1 {
		t.Fatalf("bar count must dominate for 1d bars: %d", got)
	}
	if got := GapExitPrice(false, 100, 90); got != 90 {
		t.Fatalf("long gap exit = %v, want the lower 90", got)
	}
	if got := GapExitPrice(true, 100, 90); got != 100 {
		t.Fatalf("short gap exit = %v, want the higher 100", got)
	}
	if got := GapExitPrice(true, 100, 0); got != 100 {
		t.Fatalf("unknown next open must keep the last close, got %v", got)
	}
	if got := DelistExitPrice(false, 100, 30); math.Abs(got-99.7) > 1e-12 {
		t.Fatalf("long delist exit = %v, want 99.7", got)
	}
	if got := DelistExitPrice(true, 100, 30); math.Abs(got-100.3) > 1e-12 {
		t.Fatalf("short delist exit = %v, want 100.3", got)
	}
	if got := DelistExitPrice(true, 100, 0); got != 100 {
		t.Fatalf("no penalty must keep the last close, got %v", got)
	}
	for _, tc := range []struct{ last, now, recover, delay, want int64 }{
		{1000, 5000, 0, 100, 1100},    // last bar + delay
		{1000, 5000, 1050, 100, 1050}, // capped by recovery
		{1000, 1020, 0, 100, 1020},    // capped by now
		{1000, 900, 0, 100, 1000},     // never before the last bar
	} {
		if got := GapFillTime(tc.last, tc.now, tc.recover, tc.delay); got != tc.want {
			t.Fatalf("GapFillTime(%+v) = %d", tc, got)
		}
	}
}

func TestLiquidationAndMarginTopUp(t *testing.T) {
	for _, tc := range []struct {
		pnl, balance float64
		want         bool
	}{
		{-99.1, 100, true}, {-99, 100, false}, {-1, 0, true}, {10, 0, false}, {0, 100, false}, {-50, -10, false},
	} {
		if got := LiquidationBreached(tc.pnl, tc.balance); got != tc.want {
			t.Fatalf("LiquidationBreached(%v, %v) = %t", tc.pnl, tc.balance, got)
		}
	}
	// Buffer is 100-10=90; at addRate 0.5 the top-up starts at a loss of 45.
	if got := MarginWithLossTopUp(100, 10, -44, 0.5); got != 100 {
		t.Fatalf("loss below the threshold must keep the margin, got %v", got)
	}
	if got := MarginWithLossTopUp(100, 10, -45, 0.5); got != 145 {
		t.Fatalf("loss at the threshold must be added to the margin, got %v", got)
	}
}
