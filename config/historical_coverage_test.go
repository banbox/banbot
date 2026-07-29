package config

import "testing"

func TestHistoricalCoverageNormalizeAndAllows(t *testing.T) {
	coverage := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {
				"5m": {{StartMS: 300, StopMS: 400}, {StartMS: 100, StopMS: 250}, {StartMS: 240, StopMS: 320}},
			},
		},
	}
	if err := coverage.Normalize(&TimeTuple{StartMS: 200, EndMS: 700}); err != nil {
		t.Fatal(err)
	}
	ranges := coverage.Bars["BNB/USDT:USDT"]["5m"]
	if len(ranges) != 1 || ranges[0] != (HistoricalCoverageRange{StartMS: 100, StopMS: 400}) {
		t.Fatalf("unexpected normalized ranges: %#v", ranges)
	}
	for _, test := range []struct {
		time int64
		want bool
	}{{99, false}, {100, true}, {399, true}, {400, false}, {499, false}, {500, true}, {700, true}} {
		if got := coverage.Allows("5m", test.time); got != test.want {
			t.Fatalf("Allows(5m, %d) = %v, want %v", test.time, got, test.want)
		}
	}
	if coverage.Allows("1h", 300) {
		t.Fatal("missing timeframe was allowed before baseline end")
	}
}

func TestHistoricalCoverageRejectsInvalidRange(t *testing.T) {
	coverage := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 501}}},
		},
	}
	if err := coverage.Normalize(&TimeTuple{StartMS: 200, EndMS: 700}); err == nil {
		t.Fatal("range extending past baseline end was accepted")
	}
}

func TestHistoricalCoverageAllowsBaselineAtBacktestEnd(t *testing.T) {
	coverage := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 500}}},
		},
	}
	if err := coverage.Normalize(&TimeTuple{StartMS: 50, EndMS: 500}); err != nil {
		t.Fatalf("equal-end historical coverage was rejected: %v", err)
	}
	coverage.BaselineEndMS = 501
	if err := coverage.Normalize(&TimeTuple{StartMS: 50, EndMS: 500}); err == nil {
		t.Fatal("historical coverage beyond the backtest end was accepted")
	}
}

func TestHistoricalCoverageCloneIsIndependent(t *testing.T) {
	original := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 400}}},
		},
	}
	clone := original.Clone()
	clone.Bars["BNB/USDT:USDT"]["5m"][0].StopMS = 300
	if original.Bars["BNB/USDT:USDT"]["5m"][0].StopMS != 400 {
		t.Fatal("clone mutated original coverage")
	}
}
