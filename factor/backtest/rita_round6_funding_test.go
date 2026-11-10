package backtest

import (
	"math"
	"math/rand"
	"testing"
)

// rita round6: manifest.costs.funding_apr accrual. NAV must fall by exactly the booked funding (charged once), for
// 15m / 1h / 1d mark steps, the per-step sum must equal qty*price_old*apr*dt, a PIT exit stops accrual, and the 3-day
// cap applies per step.
func TestRitaFundingNAVConservationAcrossSteps(t *testing.T) {
	const year = 365.25 * 24 * 3600 * 1000
	rng := rand.New(rand.NewSource(11))
	for _, stepMS := range []int64{15 * 60000, 3600000, 86400000} {
		b, _ := NewBook(10000)
		b.SetFundingApr(10)
		qty, px := 20.0, 100.0
		b.quantities[1] = qty
		b.marks[1] = Quote{AtMS: 0, AvailableAt: 0, Price: px}
		b.cash -= qty * px
		var want float64
		at := int64(0)
		for i := 0; i < 2000; i++ {
			at += stepMS
			np := px * (0.99 + rng.Float64()*0.02)
			want += qty * px * 0.10 * float64(stepMS) / year
			if err := b.Mark(1, Quote{AtMS: at, AvailableAt: at, Price: np}, at); err != nil {
				t.Fatal(err)
			}
			px = np
		}
		st := b.State()
		if math.Abs(st.Funding-want) > 1e-6*max(1, want) {
			t.Fatalf("step %d funding %v want %v", stepMS, st.Funding, want)
		}
		if nav := 10000 + qty*(px-100) - st.Funding; math.Abs(st.NAV-nav) > 1e-6 {
			t.Fatalf("step %d NAV %v want %v (funding deducted once)", stepMS, st.NAV, nav)
		}
	}
}

func TestRitaFundingPITExitStopsAccrualAndNoDoubleCount(t *testing.T) {
	const year = 365.25 * 24 * 3600 * 1000
	b, _ := NewBook(10000)
	b.SetFundingApr(10)
	b.SetPITDelistPenaltyBps(100)
	b.quantities[1] = 10
	b.marks[1] = Quote{AtMS: 0, AvailableAt: 0, Price: 100}
	b.cash -= 1000
	day := int64(86400000)
	_ = b.Mark(1, Quote{AtMS: day, AvailableAt: day, Price: 100}, day)
	f1 := b.State().Funding
	b.ExitAtLastMark([]int32{1}, 0, 0)
	// 20 day hole then recovery: no position -> no funding; gapLoss is a separate counter
	_ = b.Mark(1, Quote{AtMS: 21 * day, AvailableAt: 21 * day, Price: 100}, 21*day)
	st := b.State()
	if want := 1000 * 0.1 * float64(day) / year; math.Abs(f1-want) > 1e-9 || st.Funding != f1 {
		t.Fatalf("funding after exit %v (first day %v want %v)", st.Funding, f1, want)
	}
	gap, del := b.PITExitLosses()
	// fee=slip=0, price unchanged: penalty refunded on recovery, NAV = start - funding (once) - gapLoss
	if want := 10000 - f1 - gap; math.Abs(st.NAV-want) > 1e-6 || del != 0 {
		t.Fatalf("NAV %v want %v funding %v gap %v delist %v", st.NAV, want, st.Funding, gap, del)
	}
}

// A long held across a 10 day hole (policy mode: held instruments are not PIT-exited) is charged 3 days only.
func TestRitaFundingHoleCapUndercharges(t *testing.T) {
	const year = 365.25 * 24 * 3600 * 1000
	b, _ := NewBook(10000)
	b.SetFundingApr(10)
	b.quantities[1] = 10
	b.marks[1] = Quote{AtMS: 0, AvailableAt: 0, Price: 100}
	b.cash -= 1000
	day := int64(86400000)
	_ = b.Mark(1, Quote{AtMS: 10 * day, AvailableAt: 10 * day, Price: 100}, 10*day)
	got := b.State().Funding
	full := 1000 * 0.1 * 10 * float64(day) / year
	t.Logf("10d hole: charged %.4f of %.4f (%.0f%%), shortfall %.4f%% of NAV", got, full, got/full*100, (full-got)/10000*100)
	if math.Abs(got-full*0.3) > 1e-9 {
		t.Fatalf("cap %v", got)
	}
}
