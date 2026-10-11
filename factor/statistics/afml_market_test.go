package statistics

import (
	"math"
	"testing"
)

func TestAFMLDailyVolatilityAlignmentAndPrefix(t *testing.T) {
	d := afmlDayMillis
	ts := []int64{0, d, 2 * d, 3 * d, 4 * d}
	prices := []float64{100, 110, 132, 145.2, 160}
	strict, err := DailyVolatility(ts, prices, 3, true)
	if err != nil {
		t.Fatal(err)
	}
	inclusive, err := DailyVolatility(ts, prices, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(strict[2]) || !math.IsNaN(inclusive[1]) {
		t.Fatal("missing EW variance warmup")
	}
	afmlNear(t, strict[3], 0) // source samples two equal two-day returns .32
	afmlNear(t, inclusive[2], .1/math.Sqrt(2))
	prefix, err := DailyVolatility(ts[:4], prices[:4], 3, true)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, prefix[3], strict[3])
	if _, err = DailyVolatility([]int64{d, d}, []float64{1, 2}, 3, true); err == nil {
		t.Fatal("duplicate timestamps accepted")
	}
}

func TestAFMLDailyVolatilityIrregular(t *testing.T) {
	d := afmlDayMillis
	got, err := DailyVolatility([]int64{0, d / 2, 5 * d / 4, 8 * d / 5, 21 * d / 10}, []float64{100, 105, 110, 120, 130}, 3, true)
	if err != nil {
		t.Fatal(err)
	}
	// At1.25day reference is t0; at1.6day reference is t.5day.
	afmlNear(t, got[3], ((120./105-1)-.1)/math.Sqrt(2))
}

func TestAFMLRollGaps(t *testing.T) {
	contracts := []string{"A", "A", "B", "B", "C"}
	open := []float64{100, 100, 120, 121, 115}
	close := []float64{100, 102, 121, 122, 116}
	forward, err := RollGaps(contracts, open, close, false)
	if err != nil {
		t.Fatal(err)
	}
	backward, err := RollGaps(contracts, open, close, true)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []float64{0, 0, 18, 18, 11} {
		afmlNear(t, forward[i], want)
		afmlNear(t, backward[i], want-11)
	}
	// Pure contract-price jump does not generate roll-adjusted PnL.
	gap, err := RollGaps([]string{"A", "B"}, []float64{100, 120}, []float64{100, 120}, false)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, 120-gap[1], 100)
	if _, err = RollGaps([]string{"A"}, nil, []float64{100}, false); err == nil {
		t.Fatal("bad dimensions accepted")
	}
}
