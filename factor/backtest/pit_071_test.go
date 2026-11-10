package backtest

import (
	"math"
	"testing"
)

func pitBook(t *testing.T, qty float64) *Book {
	t.Helper()
	b, err := NewBook(1000)
	if err != nil {
		t.Fatal(err)
	}
	b.quantities[1] = qty
	b.marks[1] = Quote{AtMS: 10, AvailableAt: 10, Price: 100}
	b.cash -= qty * 100 // paid for the position
	b.SetPITDelistPenaltyBps(100)
	return b
}

// 071: PIT exit charges the delist penalty (adverse), recovered data refunds it and charges the move to the worse
// price instead; both directions.
func TestPITExitDelistPenaltyAndGapRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		qty      float64
		next     float64
		wantGap  float64
		wantNAV0 float64 // NAV right after the exit: 1000 - penalty (1% of 5000 notional)
	}{
		{"long, gap down", 50, 90, 50 * 10, 1000 - 50},
		{"long, gap up (no adjustment)", 50, 120, 0, 1000 - 50},
		{"short, gap up", -50, 110, 50 * 10, 1000 - 50},
		{"short, gap down (no adjustment)", -50, 80, 0, 1000 - 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := pitBook(t, tc.qty)
			if got := b.ExitAtLastMark([]int32{1}, 0, 0); len(got) != 1 {
				t.Fatalf("closed %v", got)
			}
			gap, del := b.PITExitLosses()
			if gap != 0 || math.Abs(del-50) > 1e-9 {
				t.Fatalf("after exit gap=%v delist=%v", gap, del)
			}
			if nav := b.State().NAV; math.Abs(nav-tc.wantNAV0) > 1e-9 {
				t.Fatalf("NAV after exit %v want %v", nav, tc.wantNAV0)
			}
			if err := b.Mark(1, Quote{AtMS: 500, AvailableAt: 500, Price: tc.next}, 500); err != nil {
				t.Fatal(err)
			}
			gap, del = b.PITExitLosses()
			if math.Abs(gap-tc.wantGap) > 1e-9 || math.Abs(del) > 1e-9 {
				t.Fatalf("after recovery gap=%v (want %v) delist=%v (want 0)", gap, tc.wantGap, del)
			}
			if nav := b.State().NAV; math.Abs(nav-(1000-tc.wantGap)) > 1e-9 {
				t.Fatalf("NAV after recovery %v want %v", nav, 1000-tc.wantGap)
			}
		})
	}
}
