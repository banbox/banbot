package statistics

import (
	"math"
	"testing"
)

func TestChanUMDPreviousCloseMembershipAndNormalization(t *testing.T) {
	// Two historical winners earn1%/3%, one loser loses2%; unchanged asset ignored.
	value, err := WinnerMinusLoserReturn([]float64{110, 120, 90, 100}, []float64{111.1, 123.6, 88.2, 200}, []float64{100, 100, 100, 100})
	if err != nil || math.Abs(value-.04) > 1e-12 {
		t.Fatalf("gross-two winner-average minus loser-average %g %v", value, err)
	}
	// Today's large reversal must not change yesterday's memberships.
	value, err = WinnerMinusLoserReturn([]float64{110, 90}, []float64{80, 120}, []float64{100, 100})
	if err != nil || math.Abs(value-((80.0/110-1)-(120.0/90-1))) > 1e-12 {
		t.Fatal("UMD used future membership")
	}
	if _, err := WinnerMinusLoserReturn([]float64{110}, []float64{111}, []float64{100}); err == nil {
		t.Fatal("missing loser portfolio accepted")
	}
}
