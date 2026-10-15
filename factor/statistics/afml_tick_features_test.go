package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestTickRuleFeaturesSourceAndPrefix(t *testing.T) {
	prices := []PITScalar{{0, 0, 0, -2}, {1, 1, 1, -2}, {2, 2, 2, -3}, {2, 2, 2, -3}, {3, 3, 3, 0}, {4, 4, 4, -1}}
	w, err := FractionalDiffWeights(1, .01, 10)
	if err != nil {
		t.Fatal(err)
	}
	r, err := TickRuleFeatures(prices, w, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := []float64{1, 1, -1, -1, 1, -1}
	cumsum := []float64{1, 2, 1, 0, 1, 0}
	for i := range want {
		if r.Signs[i].Value != want[i] || r.Cumulative[i].Value != cumsum[i] || r.FractionalValid[i] != (i > 0) || (i > 0 && r.Fractional[i].Value != want[i]) {
			t.Fatal(i, r)
		}
	}
	for n := 1; n <= len(prices); n++ {
		p, err := TickRuleFeatures(prices[:n], w, 4, 100)
		if err != nil || !reflect.DeepEqual(p.Signs, r.Signs[:n]) || !reflect.DeepEqual(p.Fractional, r.Fractional[:n]) {
			t.Fatal("prefix changed", n, err)
		}
	}
	// A fractional d=.5 prefix kernel is not the d=1 sign identity.
	fractional, err := TickRuleFeatures(prices, []float64{1, -.5, -.125}, 4, 100)
	if err != nil || fractional.FractionalValid[1] || fractional.Fractional[2].Value != -.125 || fractional.Fractional[3].Value != -.75 {
		t.Fatal("fractional cumulative-sign oracle", fractional, err)
	}
}

func TestTickRuleFeaturesVisibilityAndLineage(t *testing.T) {
	prices := []PITScalar{{0, 5, 9, 10}, {1, 1, 1, 11}, {2, 2, 20, 100}}
	r, err := TickRuleFeatures(prices, []float64{1, -1}, 10, 100)
	if err != nil || len(r.Signs) != 2 || r.Signs[1].AvailableMS != 5 || r.Signs[1].ReceivedMS != 9 {
		t.Fatal(r, err)
	}
	prices[2].Value = -10000
	p, err := TickRuleFeatures(prices, []float64{1, -1}, 10, 100)
	if err != nil || !reflect.DeepEqual(r, p) {
		t.Fatal("future receipt changed snapshot", p, err)
	}
	for _, bad := range [][]PITScalar{{{0, 0, 0, math.NaN()}}, {{1, 1, 1, 1}, {0, 0, 0, 2}}, {{0, 2, 1, 1}}} {
		if _, err := TickRuleFeatures(bad, []float64{1}, 10, 100); err == nil {
			t.Fatal("invalid tape accepted")
		}
	}
	if _, err := TickRuleFeatures(prices, []float64{1, -1}, 10, 1); err == nil {
		t.Fatal("work bound ignored")
	}
	if _, err := TickRuleFeatures(prices, []float64{2}, 10, 100); err == nil {
		t.Fatal("invalid kernel accepted")
	}
}
