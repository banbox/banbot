package backtest

import (
	"math"
	"testing"
)

// netBook holds the given quantities (instrument 1.. in order) bought at 100, all last marked at atMS 0.
func netBook(t *testing.T, apr float64, qtys ...float64) *Book {
	t.Helper()
	b, err := NewBook(100000)
	if err != nil {
		t.Fatal(err)
	}
	b.SetFundingApr(apr)
	b.EnableAttribution()
	for i, q := range qtys {
		sid := int32(i + 1)
		b.quantities[sid] = q
		b.marks[sid] = Quote{AtMS: 0, AvailableAt: 0, Price: 100}
		b.cash -= q * 100
	}
	return b
}

func markAll(t *testing.T, b *Book, at int64, n int) {
	t.Helper()
	for sid := int32(1); sid <= int32(n); sid++ {
		if err := b.Mark(sid, Quote{AtMS: at, AvailableAt: at, Price: 100}, at); err != nil {
			t.Fatal(err)
		}
	}
}

func wantFunding(notional float64, dtMS int64, apr float64) float64 {
	return notional * apr / 100 * float64(dtMS) / fundingYearMS
}

// Funding is charged on the net long notional of the book, floored at 0.
func TestFundingNetNotional(t *testing.T) {
	const day = int64(86400000)
	cases := []struct {
		name string
		qtys []float64
		net  float64 // net long notional
	}{
		{"pure long", []float64{10, 5}, 1500},
		{"hedged", []float64{10, -10}, 0},
		{"net long partly hedged", []float64{10, -4}, 600},
		{"net short", []float64{4, -10}, 0},
		{"pure short", []float64{-10, -5}, 0},
	}
	for _, c := range cases {
		b := netBook(t, 10, c.qtys...)
		markAll(t, b, day, len(c.qtys))
		st := b.State()
		if want := wantFunding(c.net, day, 10); math.Abs(st.Funding-want) > 1e-9 {
			t.Fatalf("%s: funding %v want %v", c.name, st.Funding, want)
		}
		if math.Abs(st.NAV-(100000-st.Funding)) > 1e-9 {
			t.Fatalf("%s: NAV %v funding %v", c.name, st.NAV, st.Funding)
		}
		// funding is an account-level item: no symbol's Costs / Net carries it
		rep := b.Attribution()
		var sumCosts, sumNet float64
		for _, s := range rep.Symbols {
			sumCosts += s.Costs
			sumNet += s.Net
		}
		if sumCosts != 0 || math.Abs(rep.Funding-st.Funding) > 1e-12 || math.Abs(rep.Residual) > 1e-9 || math.Abs(sumNet) > 1e-9 /* no price move: symbols' Net sums to NAV change + Funding = 0 */ {
			t.Fatalf("%s: attribution funding %v symbol costs %v net %v residual %v want %v", c.name, rep.Funding, sumCosts, sumNet, rep.Residual, st.Funding)
		}
	}
}

// The order the instruments of one instant are marked in does not change the charge (a short may be marked first).
func TestFundingNetOrderIndependent(t *testing.T) {
	const day = int64(86400000)
	b := netBook(t, 10, 10, -4)
	for _, sid := range []int32{2, 1} {
		if err := b.Mark(sid, Quote{AtMS: day, AvailableAt: day, Price: 100}, day); err != nil {
			t.Fatal(err)
		}
	}
	if want := wantFunding(600, day, 10); math.Abs(b.State().Funding-want) > 1e-9 {
		t.Fatalf("funding %v want %v", b.State().Funding, want)
	}
}

// Exposure changes between instants: each instant is netted with the quantities held at that time.
func TestFundingNetExposureChangesWithTurnover(t *testing.T) {
	const day = int64(86400000)
	b := netBook(t, 10, 10, -4) // net long 600
	markAll(t, b, day, 2)
	f1 := b.State().Funding
	// turnover: the short grows to -12 (net short), then to -6 (net long 400)
	b.cash -= (-12 - b.quantities[2]) * -100
	b.quantities[2] = -12
	markAll(t, b, 2*day, 2)
	f2 := b.State().Funding
	b.cash -= (-6 - b.quantities[2]) * -100
	b.quantities[2] = -6
	markAll(t, b, 3*day, 2)
	f3 := b.State().Funding
	if math.Abs(f1-wantFunding(600, day, 10)) > 1e-9 || math.Abs(f2-f1) > 1e-12 || math.Abs(f3-f2-wantFunding(400, day, 10)) > 1e-9 {
		t.Fatalf("funding steps %v %v %v", f1, f2, f3)
	}
}

// A book without shorts charges exactly what the per-instrument long accrual did (bitwise).
func TestFundingNetPureLongBitwiseSameAsPerLeg(t *testing.T) {
	b := netBook(t, 10, 7, 3, 11)
	var legacy float64
	at := int64(0)
	for i := 0; i < 50; i++ {
		prev := at
		at += 3600000
		for sid := int32(1); sid <= 3; sid++ {
			qty := b.quantities[sid]
			legacy += qty * b.marks[sid].Price * 10 / 100 * float64(at-prev) / fundingYearMS
			if err := b.Mark(sid, Quote{AtMS: at, AvailableAt: at, Price: 100 + float64(i%5)}, at); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := b.State().Funding; got != legacy {
		t.Fatalf("funding %v legacy %v", got, legacy)
	}
}

// Every fifth step the short leg has no bar: the long leg is still hedged by it (last valid notional), so the total
// stays ~0; the same when the long leg misses the bar.
func TestFundingNetMissingBarKeepsHedge(t *testing.T) {
	const hour = int64(3600000)
	for _, missing := range []int32{2, 1} {
		b := netBook(t, 10, 10, -10)
		for i := int64(1); i <= 40; i++ {
			for sid := int32(1); sid <= 2; sid++ {
				if sid == missing && i%5 == 0 {
					continue
				}
				if err := b.Mark(sid, Quote{AtMS: i * hour, AvailableAt: i * hour, Price: 100}, i*hour); err != nil {
					t.Fatal(err)
				}
			}
			if f := b.State().Funding; math.Abs(f) > 1e-12 {
				t.Fatalf("missing leg %d step %d: funding %v want 0", missing, i, f)
			}
		}
		if rep := b.Attribution(); math.Abs(rep.Residual) > 1e-9 {
			t.Fatalf("residual %v", rep.Residual)
		}
	}
}

// A partly hedged book with a missing short bar is charged exactly like the same book with every bar present.
func TestFundingNetMissingBarEqualsFullBars(t *testing.T) {
	const hour = int64(3600000)
	run := func(skip bool) float64 {
		b := netBook(t, 10, 10, -4)
		for i := int64(1); i <= 20; i++ {
			for sid := int32(1); sid <= 2; sid++ {
				if skip && sid == 2 && i%5 == 0 {
					continue
				}
				if err := b.Mark(sid, Quote{AtMS: i * hour, AvailableAt: i * hour, Price: 100}, i*hour); err != nil {
					t.Fatal(err)
				}
			}
		}
		return b.State().Funding
	}
	full, gappy := run(false), run(true)
	if math.Abs(full-gappy) > 1e-9 || math.Abs(full-wantFunding(600, 20*hour, 10)) > 1e-9 {
		t.Fatalf("full %v gappy %v", full, gappy)
	}
}

// Quote times of the two legs one second apart inside one billing step still offset.
func TestFundingNetQuoteTimeSkewStillHedges(t *testing.T) {
	const hour = int64(3600000)
	b := netBook(t, 10, 10, -10)
	for i := int64(1); i <= 10; i++ {
		at := i * hour
		if err := b.Mark(1, Quote{AtMS: at, AvailableAt: at, Price: 100}, at+1000); err != nil {
			t.Fatal(err)
		}
		if err := b.Mark(2, Quote{AtMS: at + 1000, AvailableAt: at + 1000, Price: 100}, at+1000); err != nil {
			t.Fatal(err)
		}
	}
	if f := b.State().Funding; math.Abs(f) > 1e-12 {
		t.Fatalf("funding %v want 0", f)
	}
}

// Missing bars of a long-only book change nothing: every leg is charged at its own marks over its own interval.
func TestFundingNetPureLongMissingBarBitwiseSameAsPerLeg(t *testing.T) {
	const hour = int64(3600000)
	b := netBook(t, 10, 7, 3)
	var legacy float64
	last := map[int32]int64{1: 0, 2: 0}
	for i := int64(1); i <= 30; i++ {
		for sid := int32(1); sid <= 2; sid++ {
			if sid == 2 && i%4 == 0 {
				continue
			}
			legacy += b.quantities[sid] * 100 * 10 / 100 * float64(i*hour-last[sid]) / fundingYearMS
			last[sid] = i * hour
			if err := b.Mark(sid, Quote{AtMS: i * hour, AvailableAt: i * hour, Price: 100}, i*hour); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := b.State().Funding; got != legacy {
		t.Fatalf("funding %v legacy %v", got, legacy)
	}
}
