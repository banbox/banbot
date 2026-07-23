package orm

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type snapshotLeverageExchange struct {
	banexg.BanExchange
	loadCalls int
	initCalls int
	loadErr   *errs.Error
}

type snapshotMarketExchange struct {
	banexg.BanExchange
	info      *banexg.ExgInfo
	loadCalls int
}

func (e *snapshotMarketExchange) Info() *banexg.ExgInfo {
	return e.info
}

func (e *snapshotMarketExchange) LoadMarkets(bool, map[string]interface{}) (banexg.MarketMap, *errs.Error) {
	e.loadCalls++
	return banexg.MarketMap{"LIVE/USDT:USDT": {
		ID: "LIVEUSDT", Symbol: "LIVE/USDT:USDT", Linear: true, Precision: &banexg.Precision{Price: 1},
	}}, nil
}

func (e *snapshotLeverageExchange) LoadLeverageBrackets(bool, map[string]interface{}) *errs.Error {
	e.loadCalls++
	return e.loadErr
}

func (e *snapshotLeverageExchange) InitLeverageBrackets() *errs.Error {
	e.initCalls++
	return nil
}

func TestMergeHistoricalMarketSnapshotRestoresExecutionRules(t *testing.T) {
	current := &banexg.Market{
		ID: "HIFIUSDT", Symbol: "HIFI/USDT:USDT", Type: banexg.MarketLinear,
		Linear: true, Contract: true, Active: false, ContractSize: 1, Maker: 0.9,
		Precision: &banexg.Precision{Price: 0.00001, Amount: 1,
			ModePrice: banexg.PrecModeTickSize, ModeAmount: banexg.PrecModeTickSize},
	}
	historical := &banexg.Market{
		ID: "HIFIUSDT", Symbol: "HIFI/USDT:USDT", Type: "swap",
		Linear: true, Contract: true, Active: true, ContractSize: 1, Maker: 0.001,
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
	got := markets[historical.Symbol]
	if count != 1 || got == current || got.Precision.Price != 4 ||
		got.Precision.ModePrice != banexg.PrecModeDecimalPlace || got.Maker != historical.Maker || !got.Active {
		t.Fatalf("historical market was not applied authoritatively: %#v", got)
	}
	if got.Limits == historical.Limits || got.Limits.Amount == historical.Limits.Amount {
		t.Fatal("historical limits must be copied")
	}
	if info.Markets[got.Symbol] != got || len(info.MarketsById[got.ID]) != 1 {
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
	if got == nil || got == historical || !got.Active || got.Type != banexg.MarketLinear {
		t.Fatalf("delisted market = %#v", got)
	}
}

func TestMergeHistoricalMarketSnapshotReplacesLiveMarketSet(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	markets := banexg.MarketMap{
		symbol: {ID: "BTCUSDT", Symbol: symbol, Linear: true, Precision: &banexg.Precision{Price: 1}, Maker: 0.9},
		"LIVE/USDT:USDT": {ID: "LIVEUSDT", Symbol: "LIVE/USDT:USDT", Linear: true,
			Precision: &banexg.Precision{Price: 1}},
	}
	historical := &banexg.Market{
		ID: "BTCUSDT", Symbol: symbol, Linear: true, Contract: true, Maker: 0.001,
		Precision: &banexg.Precision{Price: 2},
	}
	info := &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}
	if _, err := mergeHistoricalMarketSnapshot(info, markets, &historicalMarketSnapshot{
		Exchange: "binance", MarketType: banexg.MarketLinear,
		Markets: banexg.MarketMap{symbol: historical},
	}); err != nil {
		t.Fatal(err)
	}
	if len(markets) != 1 || markets["LIVE/USDT:USDT"] != nil || markets[symbol].Maker != historical.Maker {
		t.Fatalf("live market data survived authoritative snapshot: %#v", markets)
	}
}

func TestMergeHistoricalMarketSnapshotRejectsMalformedEntryWithoutMutation(t *testing.T) {
	live := &banexg.Market{ID: "BTCUSDT", Symbol: "BTC/USDT:USDT", Linear: true,
		Precision: &banexg.Precision{Price: 1}}
	markets := banexg.MarketMap{live.Symbol: live}
	info := &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}
	_, err := mergeHistoricalMarketSnapshot(info, markets, &historicalMarketSnapshot{
		Exchange: "binance", MarketType: banexg.MarketLinear,
		Markets: banexg.MarketMap{"BROKEN/USDT:USDT": {
			ID: "BROKENUSDT", Symbol: "BROKEN/USDT:USDT", Linear: true,
		}},
	})
	if err == nil {
		t.Fatal("malformed snapshot market was accepted")
	}
	if len(markets) != 1 || markets[live.Symbol] != live || info.Markets != nil {
		t.Fatalf("failed snapshot mutated live market state: markets=%#v info=%#v", markets, info.Markets)
	}
}

func TestMergeHistoricalMarketSnapshotSupportsSpot(t *testing.T) {
	spot := &banexg.Market{ID: "BTCUSDT", Symbol: "BTC/USDT", Spot: true, Active: true,
		Precision: &banexg.Precision{Price: 2, Amount: 6}}
	markets := banexg.MarketMap{"LIVE/USDT": {
		ID: "LIVEUSDT", Symbol: "LIVE/USDT", Spot: true, Precision: &banexg.Precision{Price: 1},
	}}
	info := &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketSpot}
	if _, err := mergeHistoricalMarketSnapshot(info, markets, &historicalMarketSnapshot{
		Exchange: "binance", MarketType: banexg.MarketSpot, Markets: banexg.MarketMap{spot.Symbol: spot},
	}); err != nil {
		t.Fatal(err)
	}
	if len(markets) != 1 || markets[spot.Symbol] == nil || markets["LIVE/USDT"] != nil {
		t.Fatalf("spot snapshot was not authoritative: %#v", markets)
	}
}

func TestLoadMarketsUsesSnapshotWithoutLiveMarketRequest(t *testing.T) {
	originalBacktest := core.BackTestMode
	originalExchange := config.Exchange
	originalDataDir := config.DataDir
	t.Cleanup(func() {
		core.BackTestMode = originalBacktest
		config.Exchange = originalExchange
		config.DataDir = originalDataDir
	})
	core.BackTestMode = true
	config.DataDir = t.TempDir()
	const symbol = "BTC/USDT:USDT"
	snapshot := historicalMarketSnapshot{
		Exchange: "binance", MarketType: banexg.MarketLinear,
		Markets: banexg.MarketMap{symbol: {
			ID: "BTCUSDT", Symbol: symbol, Linear: true, Contract: true,
			Precision: &banexg.Precision{Price: 2, Amount: 3},
		}},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(config.DataDir, "snapshot.json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	config.Exchange = &config.ExchangeConfig{
		Name: "binance", Items: map[string]map[string]interface{}{
			"binance": {"market_snapshot": "@snapshot.json", "market_snapshot_sha256": hash},
		},
	}
	exchange := &snapshotMarketExchange{info: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}}
	markets, loadErr := LoadMarkets(exchange, false)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if exchange.loadCalls != 0 || len(markets) != 1 || markets[symbol] == nil || markets["LIVE/USDT:USDT"] != nil {
		t.Fatalf("load calls=%d markets=%#v", exchange.loadCalls, markets)
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

func TestMarketSnapshotUsesDeterministicLeverageBrackets(t *testing.T) {
	originalBacktest := core.BackTestMode
	originalExchange := config.Exchange
	t.Cleanup(func() {
		core.BackTestMode = originalBacktest
		config.Exchange = originalExchange
	})
	core.BackTestMode = true
	config.Exchange = &config.ExchangeConfig{
		Name: "binance",
		Items: map[string]map[string]interface{}{
			"binance": {"market_snapshot": "@market-snapshots/frozen.json"},
		},
	}
	exchange := &snapshotLeverageExchange{}
	initializeLeverageBrackets(exchange, "default")
	if exchange.loadCalls != 0 || exchange.initCalls != 1 {
		t.Fatalf("load calls = %d, init calls = %d; want 0, 1", exchange.loadCalls, exchange.initCalls)
	}
}

func TestLeverageBracketLoadingPreservesNormalFallback(t *testing.T) {
	originalBacktest := core.BackTestMode
	originalExchange := config.Exchange
	t.Cleanup(func() {
		core.BackTestMode = originalBacktest
		config.Exchange = originalExchange
	})
	core.BackTestMode = false
	config.Exchange = nil

	success := &snapshotLeverageExchange{}
	initializeLeverageBrackets(success, "default")
	if success.loadCalls != 1 || success.initCalls != 0 {
		t.Fatalf("successful load calls = %d, init calls = %d; want 1, 0", success.loadCalls, success.initCalls)
	}

	fallback := &snapshotLeverageExchange{loadErr: errs.NewMsg(errs.CodeRunTime, "load failed")}
	initializeLeverageBrackets(fallback, "default")
	if fallback.loadCalls != 1 || fallback.initCalls != 1 {
		t.Fatalf("fallback load calls = %d, init calls = %d; want 1, 1", fallback.loadCalls, fallback.initCalls)
	}
}
