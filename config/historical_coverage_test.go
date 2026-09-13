package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

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
	if coverage.Allows("1h", 700) {
		t.Fatal("missing timeframe was allowed in the extension tail")
	}
}

func TestHistoricalCoverageUnknownSymbolRejectsExtensionTail(t *testing.T) {
	previous := HistoricalCoverage
	HistoricalCoverage = &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 500}}},
		},
	}
	t.Cleanup(func() { HistoricalCoverage = previous })

	unknown := HistoricalCoverageFor("NEW/USDT:USDT")
	if unknown.Allows("5m", 200) || unknown.Allows("5m", 700) {
		t.Fatal("unknown symbol was allowed by another symbol's archived bar plan")
	}
	known := HistoricalCoverageFor("BNB/USDT:USDT")
	if !known.Allows("5m", 700) {
		t.Fatal("known symbol/timeframe extension tail was rejected")
	}
}

func TestHistoricalCoverageRejectsRowsAfterBacktestEnd(t *testing.T) {
	previous := TimeRange
	TimeRange = &TimeTuple{StartMS: 50, EndMS: 700}
	t.Cleanup(func() { TimeRange = previous })
	coverage := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 500}}},
		},
	}
	if !coverage.Allows("5m", 699) || coverage.Allows("5m", 700) || coverage.Allows("5m", 900) {
		t.Fatal("historical coverage was not capped at the frozen backtest end")
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

func TestHistoricalCoverageValidatesHistoricalResultEnd(t *testing.T) {
	coverage := &HistoricalCoverageConfig{
		BaselineEndMS: 500, HistoricalResultEndMS: 600,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 500}}},
		},
	}
	if err := coverage.Normalize(&TimeTuple{StartMS: 200, EndMS: 700}); err != nil {
		t.Fatalf("valid historical result end was rejected: %v", err)
	}
	coverage.HistoricalResultEndMS = 499
	if err := coverage.Normalize(&TimeTuple{StartMS: 200, EndMS: 700}); err == nil {
		t.Fatal("historical result end before baseline was accepted")
	}
	coverage.HistoricalResultEndMS = 701
	if err := coverage.Normalize(&TimeTuple{StartMS: 200, EndMS: 700}); err == nil {
		t.Fatal("historical result end after the backtest end was accepted")
	}
}

func TestHistoricalCoverageCloneIsIndependent(t *testing.T) {
	original := &HistoricalCoverageConfig{
		BaselineEndMS: 500, HistoricalResultEndMS: 600,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 400}}},
		},
		ListingPrefixes: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"1m": {{StartMS: 100, StopMS: 200}}},
		},
	}
	clone := original.Clone()
	if clone.HistoricalResultEndMS != original.HistoricalResultEndMS {
		t.Fatal("clone dropped historical result end")
	}
	clone.Bars["BNB/USDT:USDT"]["5m"][0].StopMS = 300
	clone.ListingPrefixes["BNB/USDT:USDT"]["1m"][0].StopMS = 150
	if original.Bars["BNB/USDT:USDT"]["5m"][0].StopMS != 400 {
		t.Fatal("clone mutated original coverage")
	}
	if original.ListingPrefixes["BNB/USDT:USDT"]["1m"][0].StopMS != 200 ||
		original.Allows("1m", 150) || original.Allows("1m", 600) {
		t.Fatal("listing prefix mutated or expanded the runtime allow-list")
	}
}

func TestHistoricalCoverageListingPrefixesAreEvidenceOnly(t *testing.T) {
	coverage := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 500}}},
		},
		ListingPrefixes: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"1m": {{StartMS: 100, StopMS: 200}}},
		},
	}
	if err := coverage.Normalize(&TimeTuple{StartMS: 50, EndMS: 700}); err != nil {
		t.Fatal(err)
	}
	if coverage.Allows("1m", 150) || coverage.Allows("1m", 600) {
		t.Fatal("listing prefix expanded the runtime allow-list")
	}
}

func TestHistoricalCoverageRejectsInvalidListingPrefix(t *testing.T) {
	coverage := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 500}}},
		},
		ListingPrefixes: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"1m": {{StartMS: 200, StopMS: 200}}},
		},
	}
	if err := coverage.Normalize(&TimeTuple{StartMS: 50, EndMS: 700}); err == nil {
		t.Fatal("invalid listing prefix was accepted")
	}
}

func TestHistoricalCoverageForPreservesListingPrefixes(t *testing.T) {
	previous := HistoricalCoverage
	HistoricalCoverage = &HistoricalCoverageConfig{
		BaselineEndMS: 500, HistoricalResultEndMS: 600,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 500}}},
		},
		ListingPrefixes: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"1m": {{StartMS: 100, StopMS: 200}}},
		},
	}
	t.Cleanup(func() { HistoricalCoverage = previous })

	coverage := HistoricalCoverageFor("BNB/USDT:USDT")
	if coverage.HistoricalResultEndMS != 600 || HistoricalCoverageFor("NEW/USDT:USDT").HistoricalResultEndMS != 600 {
		t.Fatal("per-symbol coverage dropped historical result end")
	}
	ranges := coverage.ListingPrefixes["BNB/USDT:USDT"]["1m"]
	if len(ranges) != 1 || ranges[0] != (HistoricalCoverageRange{StartMS: 100, StopMS: 200}) {
		t.Fatalf("listing prefixes were not preserved: %#v", ranges)
	}
}

func TestHistoricalCoverageEmptyListingPrefixesSurviveYAMLRoundTrip(t *testing.T) {
	original := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 500}}},
		},
		ListingPrefixes: map[string]map[string][]HistoricalCoverageRange{},
	}
	data, err := yaml.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded HistoricalCoverageConfig
	if err = yaml.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ListingPrefixes == nil {
		t.Fatalf("empty listing-prefix evidence domain was omitted:\n%s", data)
	}
}

func TestHistoricalCoverageLegacyPhysicalBarsRemainAbsentOnYAMLRoundTrip(t *testing.T) {
	original := &HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BNB/USDT:USDT": {"10m": {{StartMS: 100, StopMS: 500}}},
		},
	}
	raw, err := yaml.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "physical_bars") {
		t.Fatalf("legacy config unexpectedly emitted physical_bars: %s", raw)
	}
	var decoded HistoricalCoverageConfig
	if err = yaml.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.PhysicalBars != nil {
		t.Fatalf("legacy physical evidence became non-nil: %#v", decoded.PhysicalBars)
	}
}
