package backtest

import (
	"math"
	"testing"
)

// 081: long notional pays apr%/year between marks; short pays nothing; off by default; capped across holes.
func TestFundingAprAccrual(t *testing.T) {
	const year = 365.25 * 24 * 3600 * 1000
	run := func(qty, apr float64, dtMS int64) (State, float64) {
		b := pitBook(t, qty)
		b.SetFundingApr(apr)
		if err := b.Mark(1, Quote{AtMS: 10 + dtMS, AvailableAt: 10 + dtMS, Price: 100}, 10+dtMS); err != nil {
			t.Fatal(err)
		}
		return b.State(), 0
	}
	// long 10 units at 100 = 1000 notional, 10%/y, one day
	st, _ := run(10, 10, 86400000)
	want := 1000 * 0.10 * 86400000 / year
	if math.Abs(st.Funding-want) > 1e-9 || math.Abs(st.NAV-(1000-want)) > 1e-9 {
		t.Fatalf("long funding %v want %v nav %v", st.Funding, want, st.NAV)
	}
	if st, _ = run(-10, 10, 86400000); st.Funding != 0 {
		t.Fatalf("short must not be charged: %v", st.Funding)
	}
	if st, _ = run(10, 0, 86400000); st.Funding != 0 {
		t.Fatalf("off by default: %v", st.Funding)
	}
	st, _ = run(10, 10, 30*86400000) // 30 day hole is capped at 3 days
	if want = 1000 * 0.10 * 3 * 86400000 / year; math.Abs(st.Funding-want) > 1e-9 {
		t.Fatalf("capped funding %v want %v", st.Funding, want)
	}
}
