package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
)

func enableStrictHistoricalIntrabarTest(t *testing.T) {
	t.Helper()
	oldMode, oldData, oldCoverage := core.BackTestMode, config.Data, config.HistoricalCoverage
	t.Cleanup(func() {
		core.BackTestMode, config.Data, config.HistoricalCoverage = oldMode, oldData, oldCoverage
	})
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 2,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 0, StopMS: 2}}},
		},
	}
}

func TestLegacyIntrabarRestoresHistoricalPricePath(t *testing.T) {
	enableStrictHistoricalIntrabarTest(t)
	bar := &orm.SeriesOHLCV{Open: 100, High: 110, Low: 90, Close: 105}

	config.Data.BTLegacyIntrabar = false
	currentPrice := simMarketPrice(bar, 0.1)
	config.Data.BTLegacyIntrabar = true
	legacyPrice := simMarketPrice(bar, 0.1)
	legacyRate := simMarketRate(bar, 95, true, true, 0)

	if math.Abs(legacyPrice-96.5) > 1e-12 {
		t.Fatalf("legacy price = %.12f, want 96.5", legacyPrice)
	}
	if math.Abs(legacyRate-5.0/35.0) > 1e-12 {
		t.Fatalf("legacy trigger rate = %.12f, want %.12f", legacyRate, 5.0/35.0)
	}
	if currentPrice == legacyPrice {
		t.Fatalf("current and legacy intrabar prices both = %.12f", currentPrice)
	}
}

func TestLegacyIntrabarMatchesHistoricalJob13Prices(t *testing.T) {
	enableStrictHistoricalIntrabarTest(t)

	entry := &orm.SeriesOHLCV{Open: 0.021696, High: 0.021858, Low: 0.021502, Close: 0.021551}
	exit := &orm.SeriesOHLCV{Open: 0.020683, High: 0.020779, Low: 0.020201, Close: 0.020485}
	config.Data.BTLegacyIntrabar = false
	if got := simMarketPrice(entry, 1.0/960.0); math.Abs(got-0.0216952928125) > 1e-15 {
		t.Fatalf("current entry price = %.15f, want %.15f", got, 0.0216952928125)
	}
	config.Data.BTLegacyIntrabar = true
	if got := simMarketPrice(entry, 1.0/960.0); math.Abs(got-0.021696590625) > 1e-15 {
		t.Fatalf("historical entry price = %.15f, want %.15f", got, 0.021696590625)
	}
	if got := simMarketPrice(exit, 1.0/960.0); math.Abs(got-0.0206839979166667) > 1e-15 {
		t.Fatalf("historical exit price = %.15f, want %.15f", got, 0.0206839979166667)
	}
}

func TestLegacyIntrabarFlagDoesNotAffectNonBacktestSimulation(t *testing.T) {
	enableStrictHistoricalIntrabarTest(t)
	bar := &orm.SeriesOHLCV{Open: 100, High: 110, Low: 90, Close: 105}

	core.BackTestMode = false
	config.Data.BTLegacyIntrabar = false
	want := simMarketPrice(bar, 0.1)
	config.Data.BTLegacyIntrabar = true
	if got := simMarketPrice(bar, 0.1); got != want {
		t.Fatalf("non-backtest price = %.12f, want current semantics %.12f", got, want)
	}
}

func TestStopEntryTriggerPreservesModeSemantics(t *testing.T) {
	enableStrictHistoricalIntrabarTest(t)

	tests := []struct {
		name             string
		backtest, legacy bool
		isBuy            bool
		trigger          float64
		want             bool
	}{
		{"legacy buy crossed inside bar", true, true, true, 105, true},
		{"legacy buy gap already crossed", true, true, true, 85, true},
		{"legacy buy not reached", true, true, true, 115, false},
		{"legacy sell crossed inside bar", true, true, false, 95, true},
		{"legacy sell gap already crossed", true, true, false, 115, true},
		{"legacy sell not reached", true, true, false, 85, false},
		{"current buy gap remains pending", true, false, true, 85, false},
		{"current sell gap remains pending", true, false, false, 115, false},
		{"non-backtest buy gap remains pending", false, true, true, 85, false},
		{"non-backtest sell gap remains pending", false, true, false, 115, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core.BackTestMode = test.backtest
			config.Data.BTLegacyIntrabar = test.legacy
			if got := stopEntryTriggered(test.isBuy, test.trigger, 90, 110); got != test.want {
				t.Fatalf("triggered = %v, want %v", got, test.want)
			}
		})
	}
}

func TestLegacyEntryStopAlreadyCrossed(t *testing.T) {
	enableStrictHistoricalIntrabarTest(t)

	config.Data.BTLegacyIntrabar = true
	tests := []struct {
		name        string
		short       bool
		stop, price float64
		want        bool
	}{
		{"long already crossed", false, 90, 100, true},
		{"long equal remains pending like v0.2.22", false, 100, 100, false},
		{"long not crossed", false, 110, 100, false},
		{"short already crossed", true, 110, 100, true},
		{"short equal clears like v0.2.22", true, 100, 100, true},
		{"short not crossed", true, 90, 100, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := legacyEntryStopAlreadyCrossed(test.short, test.stop, test.price); got != test.want {
				t.Fatalf("crossed = %v, want %v", got, test.want)
			}
		})
	}

	config.Data.BTLegacyIntrabar = false
	if legacyEntryStopAlreadyCrossed(false, 90, 100) {
		t.Fatal("current backtests must retain current stop validation")
	}
	config.Data.BTLegacyIntrabar = true
	core.BackTestMode = false
	if legacyEntryStopAlreadyCrossed(false, 90, 100) {
		t.Fatal("non-backtest simulation must retain current stop validation")
	}
}

func TestEntryInitPricePreservesLegacyPendingStops(t *testing.T) {
	enableStrictHistoricalIntrabarTest(t)

	config.Data.BTLegacyIntrabar = true
	for _, test := range []struct {
		name               string
		short              bool
		stop, limit, price float64
		want               float64
	}{
		{"long pending stop", false, 110, 0, 100, 100},
		{"long equal stop", false, 100, 0, 100, 100},
		{"short pending stop", true, 90, 0, 100, 100},
		{"long limit", false, 0, 90, 100, 90},
		{"short limit", true, 0, 110, 100, 110},
	} {
		t.Run("legacy_"+test.name, func(t *testing.T) {
			if got := entryInitPrice(test.short, test.stop, test.limit, test.price); got != test.want {
				t.Fatalf("init price = %v, want %v", got, test.want)
			}
		})
	}

	config.Data.BTLegacyIntrabar = false
	if got := entryInitPrice(false, 110, 0, 100); got != 110 {
		t.Fatalf("current long stop init price = %v, want 110", got)
	}
	if got := entryInitPrice(true, 90, 0, 100); got != 90 {
		t.Fatalf("current short stop init price = %v, want 90", got)
	}
}

func TestLegacyIntrabarRequiresStrictHistoricalReplay(t *testing.T) {
	enableStrictHistoricalIntrabarTest(t)
	config.Data.BTLegacyIntrabar = true
	bar := &orm.SeriesOHLCV{Open: 100, High: 110, Low: 90, Close: 105}
	config.Data.BTLegacyIntrabar = false
	want := simMarketPrice(bar, 0.1)
	config.Data.BTLegacyIntrabar = true
	strictCoverage := config.HistoricalCoverage

	for _, test := range []struct {
		name   string
		change func()
	}{
		{name: "non-strict", change: func() { config.Data.BTStrict = false }},
		{name: "download-enabled", change: func() { config.Data.BTNoKlineDownload = false }},
		{name: "missing coverage", change: func() { config.HistoricalCoverage = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config.Data.BTStrict = true
			config.Data.BTNoKlineDownload = true
			config.HistoricalCoverage = strictCoverage
			test.change()
			if legacyIntrabarEnabled() {
				t.Fatal("legacy intrabar enabled outside strict historical replay")
			}
			if got := simMarketPrice(bar, 0.1); got != want {
				t.Fatalf("intrabar price = %v, want current semantics %v", got, want)
			}
		})
	}
}
