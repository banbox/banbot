package strat

import (
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/goods"
)

func TestNewCacheUsesCompletePolicyIdentity(t *testing.T) {
	name := "cache_policy_identity_probe"
	oldCache := cacheStrats
	cacheStrats = make(map[string]*TradeStrat)
	t.Cleanup(func() {
		deleteStratFactory(name)
		cacheStrats = oldCache
	})

	makeCalls := 0
	RegisterStrategy(name, func(*config.RunPolicyConfig) *TradeStrat {
		makeCalls++
		return &TradeStrat{}
	})

	policies := []*config.RunPolicyConfig{
		{Name: name, StopLoss: "2%", Params: map[string]float64{"threshold": 0.0036}},
		{Name: name, StopLoss: "6%", Params: map[string]float64{"threshold": 0.0036}},
		{Name: name, StopLoss: "6%", Params: map[string]float64{"threshold": 0.0047}},
		{Index: 1, Name: name, StopLoss: "6%", Params: map[string]float64{"threshold": 0.0047}},
	}
	seen := make(map[*TradeStrat]bool)
	for _, pol := range policies {
		stgy := New(pol)
		if seen[stgy] {
			t.Fatalf("distinct policy reused cached strategy: %#v", pol)
		}
		seen[stgy] = true
		if stgy.Policy != pol {
			t.Fatal("strategy did not retain its policy")
		}
	}
	if makeCalls != len(policies) {
		t.Fatalf("strategy factory called %d times, want %d", makeCalls, len(policies))
	}
}

func TestNewCacheIsConcurrentSafe(t *testing.T) {
	name := "cache_concurrent_probe"
	oldCache := cacheStrats
	cacheStrats = make(map[string]*TradeStrat)
	t.Cleanup(func() {
		deleteStratFactory(name)
		cacheStrats = oldCache
	})

	makeCalls := 0
	RegisterStrategy(name, func(*config.RunPolicyConfig) *TradeStrat {
		makeCalls++
		return &TradeStrat{}
	})
	base := &config.RunPolicyConfig{Name: name, StopLoss: "2%", Params: map[string]float64{"threshold": 0.003938123456789}}
	const count = 32
	results := make(chan *TradeStrat, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- New(base.Clone())
		}()
	}
	wg.Wait()
	close(results)
	var first *TradeStrat
	for stgy := range results {
		if first == nil {
			first = stgy
		} else if stgy != first {
			t.Fatal("equivalent concurrent policy missed cache")
		}
	}
	if makeCalls != 1 {
		t.Fatalf("strategy factory called %d times, want 1", makeCalls)
	}
}

func TestLegacyPolicyFiltersAreConcurrentSafe(t *testing.T) {
	old := polFilters
	polFilters = make(map[string][]goods.IFilter)
	t.Cleanup(func() { polFilters = old })
	pol := &config.RunPolicyConfig{
		Name:    "legacy-policy-filter-concurrency",
		Filters: []*config.CommonPairFilter{{Name: "ShuffleFilter"}},
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := getPolicyPairs(pol, []string{"BTC/USDT", "ETH/USDT"}); err != nil {
				t.Errorf("get policy pairs: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestNewCacheHitKeepsHyperParams(t *testing.T) {
	name := "cache_hit_hyper_probe"
	oldCache := cacheStrats
	cacheStrats = make(map[string]*TradeStrat)
	t.Cleanup(func() {
		deleteStratFactory(name)
		cacheStrats = oldCache
	})
	makeCalls := 0
	RegisterStrategy(name, func(pol *config.RunPolicyConfig) *TradeStrat {
		makeCalls++
		pol.Def("zEntry", 1.5, core.PNorm(0.8, 3.0))
		pol.Def("atrMult", 2.5, core.PNorm(1.0, 5.0))
		return &TradeStrat{}
	})
	initPol := &config.RunPolicyConfig{Name: name, RunTimeframes: []string{"1h"}}
	for win := 1; win <= 3; win++ {
		gp := initPol.Clone()
		New(gp)
		if n := len(gp.HyperParams()); n != 2 {
			t.Fatalf("window %d: hyper params = %d, want 2", win, n)
		}
	}
	if makeCalls != 1 {
		t.Fatalf("factory calls = %d, want 1 (cache hits)", makeCalls)
	}
}
