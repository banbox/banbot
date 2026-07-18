package strat

import (
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
)

func TestNewCacheUsesCompletePolicyIdentity(t *testing.T) {
	name := "cache_policy_identity_probe"
	oldCache := cacheStrats
	cacheStrats = make(map[string]*TradeStrat)
	t.Cleanup(func() {
		delete(StratMake, name)
		cacheStrats = oldCache
	})

	makeCalls := 0
	StratMake[name] = func(*config.RunPolicyConfig) *TradeStrat {
		makeCalls++
		return &TradeStrat{}
	}

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
		delete(StratMake, name)
		cacheStrats = oldCache
	})

	makeCalls := 0
	StratMake[name] = func(*config.RunPolicyConfig) *TradeStrat {
		makeCalls++
		return &TradeStrat{}
	}
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
