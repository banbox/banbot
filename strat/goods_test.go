package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestCalcPairTfScoresDoesNotSkipDiscoveryForFrozenStaticPairs(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	oldTimeframes, oldPolicies := config.RunTimeframes, config.RunPolicy
	oldFilters, oldPairMgr := config.PairFilters, config.PairMgr
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
		config.RunTimeframes, config.RunPolicy = oldTimeframes, oldPolicies
		config.PairFilters, config.PairMgr = oldFilters, oldPairMgr
	})

	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.RunTimeframes = []string{"15m", "1h"}
	config.RunPolicy = nil
	config.PairFilters = nil
	config.PairMgr = &config.PairMgrConfig{}

	// A nil exchange proves strict static pairs still reach K-line discovery.
	_, err := CalcPairTfScores(nil, []string{"BTC/USDT:USDT", "ETH/USDT:USDT"})
	if err == nil {
		t.Fatal("CalcPairTfScores unexpectedly skipped K-line discovery")
	}
}

func TestCalcPairTfScoresDoesNotSkipWhenFiltersAreForced(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	oldTimeframes, oldPolicies := config.RunTimeframes, config.RunPolicy
	oldFilters, oldPairMgr := config.PairFilters, config.PairMgr
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
		config.RunTimeframes, config.RunPolicy = oldTimeframes, oldPolicies
		config.PairFilters, config.PairMgr = oldFilters, oldPairMgr
	})

	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.RunTimeframes = []string{"15m"}
	config.RunPolicy = nil
	config.PairFilters = nil
	config.PairMgr = &config.PairMgrConfig{ForceFilters: true}

	if config.IsFrozenStaticPairs([]string{"BTC/USDT:USDT"}) {
		t.Fatal("forced pair filters unexpectedly enabled frozen static pairs")
	}
}

func TestCalcPairTfScoresWithSymbolStateDoesNotUseLegacyDefault(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	oldTimeframes, oldDefault := config.RunTimeframes, exg.Default
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
		config.RunTimeframes, exg.Default = oldTimeframes, oldDefault
	})

	core.BackTestMode = true
	config.Data.BTNoKlineDownload = true
	config.RunTimeframes = []string{"15m"}
	exg.Default = &pairUpdateTestExchange{}
	symbols := orm.NewSymbolStateWithIdentity("runtime", "linear")

	_, err := CalcPairTfScoresWithSymbolState(symbols, nil, []string{"BTC/USDT:USDT"})
	if err == nil || err.Code != core.ErrExgNotInit {
		t.Fatalf("explicit scoring fallback error = %v, want ErrExgNotInit", err)
	}
}

// noMarketExchange is a stub exchange that knows no market: symbol resolution fails with an error instead of
// dereferencing the nil embedded interface, and the calls prove which exchange was consulted.
type noMarketExchange struct {
	banexg.BanExchange
	getMarketCalls int
}

func (e *noMarketExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	e.getMarketCalls++
	return nil, errs.NewMsg(errs.CodeNoMarketForPair, "unknown %s", symbol)
}

func (e *noMarketExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "binance", MarketType: "linear"}
}

// A legacy (nil state / nil config) scoring run falls back to exg.Default. Without download (bt_no_kline_download,
// non-strict) it reads the library: it gets as far as resolving the symbol through the default exchange and fails
// there, not with the old "download is disabled" error.
func TestCalcPairTfScoresLegacyNilKeepsDefaultFallback(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	oldTimeframes, oldDefault := config.RunTimeframes, exg.Default
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
		config.RunTimeframes, exg.Default = oldTimeframes, oldDefault
	})

	core.BackTestMode = true
	config.Data.BTNoKlineDownload = true
	config.RunTimeframes = []string{"15m"}
	stub := &noMarketExchange{}
	exg.Default = stub

	_, err := CalcPairTfScores(nil, []string{"BTC/USDT:USDT"})
	if err == nil || err.Code == errs.CodeNotSupport {
		t.Fatalf("legacy scoring fallback error = %v, want a symbol resolution error (no download-disabled rejection)", err)
	}
	if stub.getMarketCalls == 0 {
		t.Fatal("the default exchange was not consulted")
	}

	// strict replay without historical coverage is still rejected before any symbol is resolved
	config.Data.BTStrict = true
	stub.getMarketCalls = 0
	_, err = CalcPairTfScores(nil, []string{"BTC/USDT:USDT"})
	if err == nil || err.Code != errs.CodeNotSupport || stub.getMarketCalls != 0 {
		t.Fatalf("strict without coverage: error = %v, calls %d, want CodeNotSupport and no resolution", err, stub.getMarketCalls)
	}
}
