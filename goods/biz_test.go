package goods

import (
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
)

func TestFilterRegistrySupportsConcurrentRegistrationAndLookup(t *testing.T) {
	const prefix = "runtime-filter-registry-test-"
	const count = 32
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := prefix + string(rune('a'+i))
			RegisterFilter(name, func(base BaseFilter) IFilter { return &ShuffleFilter{BaseFilter: base} })
			if _, ok := GetFilterFactory(name); !ok {
				t.Errorf("filter %q was not found", name)
			}
		}(i)
	}
	wg.Wait()
}

func TestFilterRegistryRejectsInvalidRegistration(t *testing.T) {
	for _, test := range []struct {
		name    string
		factory FilterFactory
	}{{factory: func(base BaseFilter) IFilter { return &ShuffleFilter{BaseFilter: base} }}, {name: "nil-factory"}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("RegisterFilter(%q) did not panic", test.name)
				}
			}()
			RegisterFilter(test.name, test.factory)
		}()
	}
	if _, err := CreateFilter(nil, false); err == nil {
		t.Fatal("CreateFilter accepted nil config")
	}
}

func TestUseFrozenStaticPairsRequiresUnfilteredStrictHistoricalReplay(t *testing.T) {
	previousBackTest, previousData := core.BackTestMode, config.Data
	previousCoverage, previousFilters, previousMgr := config.HistoricalCoverage, config.PairFilters, config.PairMgr
	t.Cleanup(func() {
		core.BackTestMode, config.Data = previousBackTest, previousData
		config.HistoricalCoverage, config.PairFilters, config.PairMgr = previousCoverage, previousFilters, previousMgr
	})
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.HistoricalCoverage = nil
	config.PairFilters = nil
	config.PairMgr = &config.PairMgrConfig{}

	if !useFrozenStaticPairs([]string{"BTC/USDT:USDT"}) {
		t.Fatal("strict no-download replay did not preserve frozen static pairs")
	}

	tests := []struct {
		name   string
		change func()
	}{
		{name: "no static pairs", change: func() {}},
		{name: "pair filters", change: func() {
			config.PairFilters = []*config.CommonPairFilter{{Name: "VolumePairFilter"}}
		}},
		{name: "forced filters", change: func() { config.PairMgr.ForceFilters = true }},
		{name: "non-strict backtest", change: func() { config.Data.BTStrict = false }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config.PairFilters = nil
			config.PairMgr.ForceFilters = false
			config.Data.BTStrict = true
			test.change()
			pairs := []string{"BTC/USDT:USDT"}
			if test.name == "no static pairs" {
				pairs = nil
			}
			if useFrozenStaticPairs(pairs) {
				t.Fatal("unexpected frozen static-pair shortcut")
			}
		})
	}
}

func TestBlockFilter(t *testing.T) {
	f := BlockFilter{
		BaseFilter: BaseFilter{
			Name: "BlockFilter",
		},
		Pairs: []string{"BTC/USDT:USDT"},
	}
	src := []string{"BTC/USDT:USDT", "ETH/USDT:USDT"}
	out, err := f.Filter(src, 0)
	if err != nil {
		panic(err)
	}
	if len(out) != 1 || out[0] != "ETH/USDT:USDT" {
		t.Errorf("FAIL BlockFilter, get: %v, expect: %v", out, []string{"ETH/USDT:USDT"})
	}
}

func TestGetPairFiltersWithConfigParsesBlockPairsFromRuntimeConfig(t *testing.T) {
	oldExchange, oldMarket, oldStake := config.Exchange, core.Market, config.StakeCurrency
	t.Cleanup(func() {
		config.Exchange, core.Market, config.StakeCurrency = oldExchange, oldMarket, oldStake
	})
	config.Exchange = &config.ExchangeConfig{Name: "legacy"}
	core.Market = banexg.MarketLinear
	config.StakeCurrency = []string{"BTC"}

	runtimeConfig := &config.Config{
		Exchange:      &config.ExchangeConfig{Name: "runtime"},
		MarketType:    banexg.MarketSpot,
		StakeCurrency: []string{"USDT"},
		PairMgr:       &config.PairMgrConfig{},
	}
	filters, err := GetPairFiltersWithConfig([]*config.CommonPairFilter{{
		Name:  "BlockFilter",
		Items: map[string]interface{}{"pairs": []string{"ETH"}},
	}}, false, runtimeConfig)
	if err != nil {
		t.Fatal(err)
	}
	block, ok := filters[0].(*BlockFilter)
	if !ok || len(block.Pairs) != 1 || block.Pairs[0] != "ETH/USDT" {
		t.Fatalf("runtime block pairs = %#v, want ETH/USDT", filters)
	}
}

func TestRefreshPairListWithRuntimeDepsRequiresSymbolState(t *testing.T) {
	_, err := RefreshPairListWithRuntimeDeps(&RuntimeDeps{
		Exchange: &runtimeDepsExchange{},
	}, 100)
	if err == nil || err.Code != core.ErrRunTime {
		t.Fatalf("missing runtime symbol state error = %v, want ErrRunTime", err)
	}
}

func TestRefreshPairListWithSymbolStateRejectsLegacyFallback(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("runtime", banexg.MarketSpot)
	oldPairs, oldPairsMap := core.LegacyPairStateSnapshot()
	t.Cleanup(func() { core.ReplaceLegacyPairState(oldPairs, oldPairsMap) })
	core.SetLegacyPairs([]string{"legacy"}, []string{"legacy-additional"})

	_, err := RefreshPairListWithSymbolState(state, &runtimeDepsExchange{}, 100)
	if err == nil || err.Code != core.ErrBadConfig {
		t.Fatalf("symbol-only refresh error = %v, want ErrBadConfig", err)
	}
	pairs, pairsMap := core.LegacyPairStateSnapshot()
	if len(pairs) != 1 || pairs[0] != "legacy" || !pairsMap["legacy"] {
		t.Fatalf("legacy pair state changed on rejected refresh: %v/%v", pairs, pairsMap)
	}
}

func TestExplicitAgeFilterDoesNotFallBackToGlobalCoreState(t *testing.T) {
	previous := core.BanPairsUntil
	core.BanPairsUntil = map[string]int64{"legacy": 1}
	t.Cleanup(func() { core.BanPairsUntil = previous })

	filter := &AgeFilter{BaseFilter: BaseFilter{AllowEmpty: true}, Min: 1}
	_, err := filter.FilterWithRuntimeDeps(&RuntimeDeps{
		Symbols:  orm.NewSymbolState(),
		Exchange: &runtimeDepsExchange{},
	}, []string{"BTC/USDT"}, 100)
	if err == nil || err.Code != core.ErrRunTime {
		t.Fatalf("missing runtime core error = %v, want ErrRunTime", err)
	}
	if len(core.BanPairsUntil) != 1 || core.BanPairsUntil["legacy"] != 1 {
		t.Fatalf("global ban state changed on explicit filter failure: %#v", core.BanPairsUntil)
	}
}

func TestExplicitVolumeFilterDoesNotFallBackToGlobalConfig(t *testing.T) {
	filter := &VolumePairFilter{}
	_, err := filter.FilterWithRuntimeDeps(&RuntimeDeps{
		Symbols:  orm.NewSymbolState(),
		Exchange: &runtimeDepsExchange{},
	}, nil, 100)
	if err == nil || err.Code != core.ErrBadConfig {
		t.Fatalf("missing runtime config error = %v, want ErrBadConfig", err)
	}
}

type runtimeDepsExchange struct{ banexg.BanExchange }

func TestShuffleFilterUsesSeedDeterministically(t *testing.T) {
	input := []string{"BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX"}
	want := slices.Clone(input)
	rand.New(rand.NewSource(42)).Shuffle(len(want), func(i, j int) {
		want[i], want[j] = want[j], want[i]
	})
	filter := &ShuffleFilter{Seed: 42}
	for run := 0; run < 2; run++ {
		got, err := filter.Filter(slices.Clone(input), 0)
		if err != nil {
			t.Fatalf("Filter returned error: %v", err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("run %d shuffle = %v, want %v", run, got, want)
		}
	}
}

func TestBetterCorrelationCandidateUsesStableIDTieBreak(t *testing.T) {
	for _, ascending := range []bool{true, false} {
		if !betterCorrelationCandidate(0.5, 1, 0.5, 2, ascending) {
			t.Fatalf("ascending=%v did not prefer lower ID on tie", ascending)
		}
		if betterCorrelationCandidate(0.5, 2, 0.5, 1, ascending) {
			t.Fatalf("ascending=%v preferred higher ID on tie", ascending)
		}
	}
}

func TestBetterCorrelationCandidateTreatsNonFiniteValuesAsWorst(t *testing.T) {
	for _, ascending := range []bool{true, false} {
		if !betterCorrelationCandidate(0.5, 2, math.NaN(), 1, ascending) {
			t.Fatalf("ascending=%v did not prefer finite correlation", ascending)
		}
		if betterCorrelationCandidate(math.Inf(1), 1, 0.5, 2, ascending) {
			t.Fatalf("ascending=%v preferred non-finite correlation", ascending)
		}
		if !betterCorrelationCandidate(math.NaN(), 1, math.Inf(-1), 2, ascending) {
			t.Fatalf("ascending=%v did not use ID tie-break for non-finite correlations", ascending)
		}
	}
}

func TestVolumeMarketSymbolsUsesStableMarketOrder(t *testing.T) {
	oldStakeCurrencies := config.StakeCurrencyMap
	config.StakeCurrencyMap = map[string]bool{"USDT": true}
	t.Cleanup(func() { config.StakeCurrencyMap = oldStakeCurrencies })

	markets := banexg.MarketMap{
		"SOL/USDT": {},
		"BTC/USDT": {},
		"ETH/USDT": {},
		"AAA/BTC":  {},
		"RAW-USDT": {Quote: "USDT"},
	}
	want := []string{"BTC/USDT", "ETH/USDT", "RAW-USDT", "SOL/USDT"}
	for run := 0; run < 20; run++ {
		if got := volumeMarketSymbols(markets); !slices.Equal(got, want) {
			t.Fatalf("run %d market symbols = %v, want %v", run, got, want)
		}
	}
}

func TestCompareSymbolVolUsesNumericTotalOrderAndSymbolTieBreak(t *testing.T) {
	items := []*SymbolVol{
		{Symbol: "nan-z", Vol: math.NaN()},
		{Symbol: "tie-z", Vol: 500},
		{Symbol: "close-low", Vol: 1000.25},
		{Symbol: "pos-inf", Vol: math.Inf(1)},
		{Symbol: "close-high", Vol: 1000.5},
		{Symbol: "tie-a", Vol: 500},
		{Symbol: "neg-inf", Vol: math.Inf(-1)},
		{Symbol: "nan-a", Vol: math.NaN()},
	}
	want := []string{"pos-inf", "close-high", "close-low", "tie-a", "tie-z", "neg-inf", "nan-a", "nan-z"}
	permutations := [][]*SymbolVol{
		slices.Clone(items),
		slices.Clone(items),
		slices.Clone(items),
	}
	slices.Reverse(permutations[1])
	permutations[2][0], permutations[2][5] = permutations[2][5], permutations[2][0]
	for index, permutation := range permutations {
		slices.SortFunc(permutation, compareSymbolVol)
		got := make([]string, len(permutation))
		for itemIndex, item := range permutation {
			got[itemIndex] = item.Symbol
		}
		if !slices.Equal(got, want) {
			t.Fatalf("permutation %d volume order = %v, want %v", index, got, want)
		}
	}
}
