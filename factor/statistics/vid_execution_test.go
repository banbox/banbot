package statistics

import (
	"math"
	"testing"
)

func TestPairExecutionsSourceExampleAndAverageCounterexample(t *testing.T) {
	b := []PairedFill{{19, 100}, {19.5, 100}, {20, 100}}
	a := []PairedFill{{19, 50}, {18.5, 150}, {18, 150}}
	r, err := PairExecutions(b, a, 1, 0, 1, true)
	if err != nil || r.MatchedTarget != 300 || r.FullyPaired || !r.AverageSatisfies || len(r.Pairs) != 4 {
		t.Fatal(r, err)
	}
	for _, pair := range r.Pairs {
		if b[pair.BidderIndex].Price-a[pair.TargetIndex].Price < 1 {
			t.Fatal("infeasible edge", pair)
		}
	}
	r, err = PairExecutions([]PairedFill{{19, 1}, {21, 1}}, []PairedFill{{19, 2}}, 1, 0, 1, true)
	if err != nil || !r.AverageSatisfies || r.FullyPaired || r.MatchedTarget != 1 {
		t.Fatal("average cannot establish conservative pairing", r, err)
	}
}

func TestPairExecutionsRatioCashAndUnwind(t *testing.T) {
	for _, putOn := range []bool{true, false} {
		r, err := PairExecutions([]PairedFill{{20, 2}}, []PairedFill{{8, 4}}, .5, 1, 3, putOn)
		if err != nil || !r.FullyPaired || !r.AverageSatisfies || r.AverageSpread != 3 || r.MatchedTarget != 4 {
			t.Fatal(r, err)
		}
	}
	if _, err := PairExecutions([]PairedFill{{math.MaxFloat64, 2}}, []PairedFill{{1, 2}}, 2, 0, 1, true); err == nil {
		t.Fatal("overflow accepted")
	}
	if _, err := PairExecutions([]PairedFill{{20, -2}}, []PairedFill{{8, 4}}, .5, 1, 3, true); err == nil {
		t.Fatal("invalid fill accepted")
	}
}
