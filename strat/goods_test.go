package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
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
