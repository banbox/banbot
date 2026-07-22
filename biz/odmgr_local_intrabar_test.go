package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
)

func TestLegacyIntrabarRestoresHistoricalPricePath(t *testing.T) {
	bar := &orm.SeriesOHLCV{Open: 100, High: 110, Low: 90, Close: 105}
	original := config.Data.BTLegacyIntrabar
	originalBacktest := core.BackTestMode
	t.Cleanup(func() {
		config.Data.BTLegacyIntrabar = original
		core.BackTestMode = originalBacktest
	})

	core.BackTestMode = true
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
	original := config.Data.BTLegacyIntrabar
	originalBacktest := core.BackTestMode
	t.Cleanup(func() {
		config.Data.BTLegacyIntrabar = original
		core.BackTestMode = originalBacktest
	})

	entry := &orm.SeriesOHLCV{Open: 0.021696, High: 0.021858, Low: 0.021502, Close: 0.021551}
	exit := &orm.SeriesOHLCV{Open: 0.020683, High: 0.020779, Low: 0.020201, Close: 0.020485}
	core.BackTestMode = true
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
	bar := &orm.SeriesOHLCV{Open: 100, High: 110, Low: 90, Close: 105}
	original := config.Data.BTLegacyIntrabar
	originalBacktest := core.BackTestMode
	t.Cleanup(func() {
		config.Data.BTLegacyIntrabar = original
		core.BackTestMode = originalBacktest
	})

	core.BackTestMode = false
	config.Data.BTLegacyIntrabar = false
	want := simMarketPrice(bar, 0.1)
	config.Data.BTLegacyIntrabar = true
	if got := simMarketPrice(bar, 0.1); got != want {
		t.Fatalf("non-backtest price = %.12f, want current semantics %.12f", got, want)
	}
}
