package strat

import (
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
)

func TestCallStratSymbolsWithSymbolStateKeepsLegacySeparate(t *testing.T) {
	oldExg, oldExgName, oldMarket := exg.Default, core.ExgName, core.Market
	t.Cleanup(func() {
		exg.Default = oldExg
		core.ExgName, core.Market = oldExgName, oldMarket
	})
	exg.Default = nil
	core.ExgName, core.Market = "state-test", "spot"

	restore, restoreErr := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 1, Exchange: core.ExgName, Market: core.Market, Symbol: "BTC/USDT",
	}})
	if restoreErr != nil {
		t.Fatal(restoreErr)
	}
	t.Cleanup(restore)

	explicit := orm.NewSymbolState()
	explicit.CacheExSymbol(&orm.ExSymbol{
		ID: 2, Exchange: core.ExgName, Market: core.Market, Symbol: "BTC/USDT",
	})

	got, callErr := CallStratSymbolsWithSymbolState(explicit, &TradeStrat{}, []string{"BTC/USDT"}, map[string]map[string]float64{})
	if callErr != nil {
		t.Fatalf("explicit lookup failed: %v", callErr)
	}
	if len(got) != 1 || got[0].ID != 2 {
		t.Fatalf("explicit lookup returned %+v, want sid 2", got)
	}

	legacy, legacyErr := CallStratSymbols(&TradeStrat{}, []string{"BTC/USDT"}, map[string]map[string]float64{})
	if legacyErr != nil {
		t.Fatalf("legacy lookup failed: %v", legacyErr)
	}
	if len(legacy) != 1 || legacy[0].ID != 1 {
		t.Fatalf("legacy lookup returned %+v, want sid 1", legacy)
	}
}

func TestCollectDataSubsWithSymbolStateKeepsLegacySeparate(t *testing.T) {
	oldExg, oldExgName, oldMarket := exg.Default, core.ExgName, core.Market
	t.Cleanup(func() {
		exg.Default = oldExg
		core.ExgName, core.Market = oldExgName, oldMarket
	})
	exg.Default = nil
	core.ExgName, core.Market = "state-test", "spot"

	restore, restoreErr := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 3, Exchange: core.ExgName, Market: core.Market, Symbol: "SIDE/USDT",
	}})
	if restoreErr != nil {
		t.Fatal(restoreErr)
	}
	t.Cleanup(restore)

	explicit := orm.NewSymbolState()
	explicit.CacheExSymbol(&orm.ExSymbol{
		ID: 4, Exchange: core.ExgName, Market: core.Market, Symbol: "SIDE/USDT",
	})
	job := &StratJob{
		Symbol: &orm.ExSymbol{ID: 5, Exchange: core.ExgName, Market: core.Market, Symbol: "MAIN/USDT"},
		Strat: &TradeStrat{
			OnPairInfos: func(*StratJob) []*PairSub {
				return []*PairSub{{Pair: "SIDE/USDT", TimeFrame: "1h"}}
			},
		},
	}

	explicitSubs := CollectDataSubsWithSymbolState(explicit, job)
	if len(explicitSubs) != 1 || explicitSubs[0].ExSymbol.ID != 4 {
		t.Fatalf("explicit subscription returned %+v, want sid 4", explicitSubs)
	}
	legacySubs := CollectDataSubs(job)
	if len(legacySubs) != 1 || legacySubs[0].ExSymbol.ID != 3 {
		t.Fatalf("legacy subscription returned %+v, want sid 3", legacySubs)
	}
}

func TestBoundStratJobUsesExplicitSymbolStateForSubsAndSetData(t *testing.T) {
	oldExg, oldExgName, oldMarket := exg.Default, core.ExgName, core.Market
	t.Cleanup(func() {
		exg.Default = oldExg
		core.ExgName, core.Market = oldExgName, oldMarket
	})
	exg.Default = nil
	core.ExgName, core.Market = "state-bound-test", "spot"

	restore, restoreErr := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 3, Exchange: core.ExgName, Market: core.Market, Symbol: "SIDE/USDT",
	}})
	if restoreErr != nil {
		t.Fatal(restoreErr)
	}
	t.Cleanup(restore)

	explicit := orm.NewSymbolState()
	explicit.CacheExSymbol(&orm.ExSymbol{
		ID: 4, Exchange: core.ExgName, Market: core.Market, Symbol: "SIDE/USDT",
	})
	job := &StratJob{
		symbols:   explicit,
		Symbol:    &orm.ExSymbol{ID: 5, Exchange: core.ExgName, Market: core.Market, Symbol: "MAIN/USDT"},
		TimeFrame: "1m",
		Strat: &TradeStrat{
			OnPairInfos: func(*StratJob) []*PairSub {
				return []*PairSub{{Pair: "SIDE/USDT", TimeFrame: "1h"}}
			},
		},
	}

	subs := CollectDataSubs(job)
	if len(subs) != 1 || subs[0].ExSymbol.ID != 4 {
		t.Fatalf("bound subscription returned %+v, want sid 4", subs)
	}
	job.SetData(&orm.DataSeries{
		Source: orm.SeriesSourceKline, Sid: job.Symbol.ID, TimeMS: 100, EndMS: 200,
		TimeFrame: "1m", Values: map[string]any{"close": 1.0},
	})
	if job.DataHub.Get("1h", orm.SeriesSourceKline, 4) == nil {
		t.Fatal("SetData did not configure the explicit side-input symbol")
	}
	if job.DataHub.Get("1h", orm.SeriesSourceKline, 3) != nil {
		t.Fatal("SetData configured the legacy side-input symbol")
	}
}
