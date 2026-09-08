package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
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
	exg.Default = &pairUpdateTestExchange{}

	_, err := CalcPairTfScores(nil, []string{"BTC/USDT:USDT"})
	if err == nil || err.Code != errs.CodeNotSupport {
		t.Fatalf("legacy scoring fallback error = %v, want CodeNotSupport", err)
	}
}
