package orm

import (
	"testing"

	"github.com/banbox/banexg"
)

func TestMergeHistoricalMarketSnapshotRestoresExecutionRules(t *testing.T) {
	current := &banexg.Market{
		ID: "HIFIUSDT", Symbol: "HIFI/USDT:USDT", Type: banexg.MarketLinear,
		Linear: true, Contract: true, Active: false, ContractSize: 1,
		Precision: &banexg.Precision{Price: 0.00001, Amount: 1,
			ModePrice: banexg.PrecModeTickSize, ModeAmount: banexg.PrecModeTickSize},
	}
	historical := &banexg.Market{
		ID: "HIFIUSDT", Symbol: "HIFI/USDT:USDT", Type: "swap",
		Linear: true, Contract: true, Active: true, ContractSize: 1,
		Precision: &banexg.Precision{Price: 4, Amount: 0, Base: 8, Quote: 8},
		Limits:    &banexg.MarketLimits{Amount: &banexg.LimitRange{Min: 1, Max: 10_000_000}},
	}
	markets := banexg.MarketMap{current.Symbol: current}
	info := &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}
	count, err := mergeHistoricalMarketSnapshot(info, markets, &historicalMarketSnapshot{
		Exchange: "binance", MarketType: banexg.MarketLinear,
		Markets: banexg.MarketMap{historical.Symbol: historical},
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || current.Precision.Price != 4 || current.Precision.ModePrice != banexg.PrecModeDecimalPlace {
		t.Fatalf("historical precision was not applied: %#v", current.Precision)
	}
	if current.Limits == historical.Limits || current.Limits.Amount == historical.Limits.Amount {
		t.Fatal("historical limits must be copied")
	}
	if info.Markets[current.Symbol] != current || len(info.MarketsById[current.ID]) != 1 {
		t.Fatal("market indexes were not rebuilt")
	}
}

func TestMergeHistoricalMarketSnapshotAddsDelistedMarket(t *testing.T) {
	historical := &banexg.Market{
		ID: "OLDUSDT", Symbol: "OLD/USDT:USDT", Linear: true, Contract: true, Active: true,
		Precision: &banexg.Precision{Price: 3, Amount: 0},
	}
	markets := make(banexg.MarketMap)
	info := &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}
	if _, err := mergeHistoricalMarketSnapshot(info, markets, &historicalMarketSnapshot{
		Exchange: "binance", MarketType: banexg.MarketLinear,
		Markets: banexg.MarketMap{historical.Symbol: historical},
	}); err != nil {
		t.Fatal(err)
	}
	got := markets[historical.Symbol]
	if got == nil || got.Active || got.Type != banexg.MarketLinear {
		t.Fatalf("delisted market = %#v", got)
	}
}

func TestMergeHistoricalMarketSnapshotRejectsWrongExchange(t *testing.T) {
	info := &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}
	_, err := mergeHistoricalMarketSnapshot(info, make(banexg.MarketMap), &historicalMarketSnapshot{
		Exchange: "other", MarketType: banexg.MarketLinear,
		Markets: banexg.MarketMap{"OLD/USDT:USDT": {Symbol: "OLD/USDT:USDT", Linear: true,
			Precision: &banexg.Precision{Price: 3}}},
	})
	if err == nil {
		t.Fatal("expected exchange identity mismatch")
	}
}

func TestRegistrationMarketsIncludesInactiveSnapshotMarket(t *testing.T) {
	inactive := &banexg.Market{Symbol: "OLD/USDT:USDT", Type: banexg.MarketLinear, Active: false}
	info := &banexg.ExgInfo{MarketType: banexg.MarketLinear, Markets: banexg.MarketMap{inactive.Symbol: inactive}}
	got := registrationMarkets(info, make(banexg.MarketMap), true)
	if got[inactive.Symbol] != inactive {
		t.Fatal("inactive snapshot market was not registered for backtest identity")
	}
}
