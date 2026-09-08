package goods

import (
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
)

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
