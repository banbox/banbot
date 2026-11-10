package backtest

import (
	"math"
	"math/rand"
	"testing"
)

// rita round5: NAV conservation of the 071 PIT exit accounting. Across ExitAtLastMark and a later recovery mark:
//
//	NAV_after_exit     = NAV_before - fees - slippage - delistPenalty
//	NAV_after_recovery = NAV_before - fees - slippage - gapLoss - (penalty of instruments that never came back)
//
// i.e. no NAV appears or disappears other than through the booked cost counters.
func TestRitaPITNAVConservation(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 200; iter++ {
		b, _ := NewBook(10000)
		b.SetPITDelistPenaltyBps(float64(rng.Intn(300)))
		fee, slip := rng.Float64()*0.002, rng.Float64()*0.002
		type pos struct {
			qty, px, next float64
			recover       bool
		}
		ps := map[int32]*pos{}
		for sid := int32(1); sid <= 4; sid++ {
			p := &pos{qty: (rng.Float64() - 0.5) * 100, px: 10 + rng.Float64()*100, recover: rng.Intn(3) > 0}
			if rng.Intn(2) == 0 {
				p.next = p.px * (0.2 + rng.Float64()*1.8) // includes >50% jumps either way
			} else {
				p.next = p.px * (0.99 + rng.Float64()*0.02)
			}
			ps[sid] = p
			b.quantities[sid] = p.qty
			b.marks[sid] = Quote{AtMS: 10, AvailableAt: 10, Price: p.px}
			b.cash -= p.qty * p.px
		}
		nav0 := b.State().NAV
		if math.Abs(nav0-10000) > 1e-6 {
			t.Fatalf("setup nav %v", nav0)
		}
		b.ExitAtLastMark([]int32{1, 2, 3, 4}, fee, slip)
		st := b.State()
		_, del := b.PITExitLosses()
		if want := nav0 - st.Fees - st.Slippage - del; math.Abs(st.NAV-want) > 1e-6 {
			t.Fatalf("iter %d after exit NAV %v want %v", iter, st.NAV, want)
		}
		neverBack := 0.0
		for sid, p := range ps {
			if p.recover {
				if err := b.Mark(sid, Quote{AtMS: 500, AvailableAt: 500, Price: p.next}, 500); err != nil {
					t.Fatal(err)
				}
			} else {
				neverBack += math.Abs(p.qty) * p.px * b.pitPenaltyBps / 10000
			}
		}
		st = b.State()
		gap, del := b.PITExitLosses()
		if math.Abs(del-neverBack) > 1e-6 {
			t.Fatalf("iter %d delist cost %v want %v (refund of recovered only)", iter, del, neverBack)
		}
		if want := nav0 - st.Fees - st.Slippage - gap - del; math.Abs(st.NAV-want) > 1e-6 {
			t.Fatalf("iter %d after recovery NAV %v want %v", iter, st.NAV, want)
		}
		// a second mark of a recovered instrument must not charge again
		navBefore := st.NAV
		for sid, p := range ps {
			if p.recover {
				_ = b.Mark(sid, Quote{AtMS: 600, AvailableAt: 600, Price: p.next * 2}, 600)
			}
		}
		if math.Abs(b.State().NAV-navBefore) > 1e-9 {
			t.Fatalf("iter %d re-resolved on a later mark", iter)
		}
	}
}
