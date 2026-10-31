package execution

import (
	"math"
	"testing"
)

func testLimitPriceBook() LimitPriceBook {
	return LimitPriceBook{
		Bids: []LimitPriceLevel{{100, 2}, {98, 4}, {96, 4}},
		Asks: []LimitPriceLevel{{102, 2}, {104, 4}, {106, 4}},
	}
}

func TestDefaultLimitPrice(t *testing.T) {
	for _, tc := range []struct {
		name       string
		side       OrderSide
		volume     LimitPriceVolume
		wait       int
		tick, want float64
	}{
		{"buy full crossing level", Buy, LimitPriceVolume{60, 60}, 3, 0, 592.0 / 6},
		{"sell full crossing level", Sell, LimitPriceVolume{60, 60}, 3, 0, 620.0 / 6},
		{"average volume caps depth", Buy, LimitPriceVolume{30, 600}, 3, 0, 592.0 / 6},
		{"last minute caps depth", Sell, LimitPriceVolume{600, 60}, 3, 0, 620.0 / 6},
		{"buy thin depth extrapolation", Buy, LimitPriceVolume{60, 60}, 20, 0, 94.4},
		{"sell thin depth extrapolation", Sell, LimitPriceVolume{60, 60}, 20, 0, 107.6},
		{"zero wait takes best level", Buy, LimitPriceVolume{60, 60}, 0, 0, 100},
		{"zero volume takes best level", Sell, LimitPriceVolume{}, 10, 0, 102},
		{"buy nearest tick", Buy, LimitPriceVolume{60, 60}, 3, .25, 98.75},
		{"sell nearest tick", Sell, LimitPriceVolume{60, 60}, 3, .25, 103.25},
		{"decimal tick", Buy, LimitPriceVolume{60, 60}, 3, .01, 98.67},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DefaultLimitPrice(testLimitPriceBook(), tc.volume, tc.side, tc.wait, tc.tick)
			if err != nil || math.Abs(got-tc.want) > 1e-10 {
				t.Fatalf("price = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
	if got, err := DefaultLimitPrice(LimitPriceBook{}, LimitPriceVolume{60, 60}, Buy, 3, .01); err != nil || got != 0 {
		t.Fatalf("empty depth = %v, %v", got, err)
	}
}

func TestLimitPriceWait(t *testing.T) {
	for _, tc := range []struct {
		name     string
		side     OrderSide
		price    float64
		volume   LimitPriceVolume
		seconds  int
		coverage float64
	}{
		{"buy at best", Buy, 100, LimitPriceVolume{60, 60}, 0, 1},
		{"sell at best", Sell, 102, LimitPriceVolume{60, 60}, 0, 1},
		{"buy exact level excludes level", Buy, 98, LimitPriceVolume{60, 60}, 2, 1},
		{"sell exact level excludes level", Sell, 104, LimitPriceVolume{60, 60}, 2, 1},
		{"buy between levels", Buy, 97, LimitPriceVolume{60, 60}, 6, 1},
		{"sell between levels", Sell, 105, LimitPriceVolume{60, 60}, 6, 1},
		{"buy beyond depth", Buy, 92, LimitPriceVolume{60, 60}, 10, .5},
		{"sell beyond depth", Sell, 110, LimitPriceVolume{60, 60}, 10, .5},
		{"larger last minute", Buy, 97, LimitPriceVolume{60, 240}, 2, 1},
		{"larger average minute", Sell, 105, LimitPriceVolume{240, 60}, 2, 1},
		{"no volume", Buy, 97, LimitPriceVolume{}, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seconds, coverage, err := LimitPriceWait(testLimitPriceBook(), tc.volume, tc.side, tc.price)
			if err != nil || seconds != tc.seconds || coverage != tc.coverage {
				t.Fatalf("wait = %d, %v, %v; want %d, %v", seconds, coverage, err, tc.seconds, tc.coverage)
			}
		})
	}
}

func TestSummarizeLimitPriceVolume(t *testing.T) {
	got, err := SummarizeLimitPriceVolume([]float64{0, 60, 120})
	if err != nil || got != (LimitPriceVolume{60, 120}) {
		t.Fatalf("volume = %+v, %v", got, err)
	}
	got, err = SummarizeLimitPriceVolume(nil)
	if err != nil || got != (LimitPriceVolume{}) {
		t.Fatalf("empty volume = %+v, %v", got, err)
	}
	for _, invalid := range []float64{-1, math.NaN(), math.Inf(1)} {
		if _, err := SummarizeLimitPriceVolume([]float64{invalid}); err == nil {
			t.Fatalf("accepted invalid volume %v", invalid)
		}
	}
}

func TestLimitPriceRejectsInvalidInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		book   LimitPriceBook
		volume LimitPriceVolume
		side   OrderSide
		wait   int
		tick   float64
	}{
		{"side", testLimitPriceBook(), LimitPriceVolume{}, "invalid", 1, 0},
		{"wait", testLimitPriceBook(), LimitPriceVolume{}, Buy, -1, 0},
		{"tick", testLimitPriceBook(), LimitPriceVolume{}, Buy, 1, -.1},
		{"volume", testLimitPriceBook(), LimitPriceVolume{-1, 1}, Buy, 1, 0},
		{"unordered", LimitPriceBook{Bids: []LimitPriceLevel{{98, 1}, {100, 1}}}, LimitPriceVolume{}, Buy, 1, 0},
		{"invalid price", LimitPriceBook{Bids: []LimitPriceLevel{{math.NaN(), 1}}}, LimitPriceVolume{}, Buy, 1, 0},
		{"invalid size", LimitPriceBook{Bids: []LimitPriceLevel{{100, -1}}}, LimitPriceVolume{}, Buy, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DefaultLimitPrice(tc.book, tc.volume, tc.side, tc.wait, tc.tick); err == nil {
				t.Fatal("accepted invalid input")
			}
		})
	}
	if _, _, err := LimitPriceWait(testLimitPriceBook(), LimitPriceVolume{60, 60}, Buy, math.NaN()); err == nil {
		t.Fatal("accepted NaN target")
	}
}
