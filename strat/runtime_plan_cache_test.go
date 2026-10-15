package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
)

func TestNewRefreshesEquivalentPolicyPointerOnCacheHit(t *testing.T) {
	const name = "runtime_plan_cache_policy_fixture"
	RegisterStrategy(name, func(*config.RunPolicyConfig) *TradeStrat { return &TradeStrat{} })
	t.Cleanup(func() { deleteStratFactory(name) })

	firstPolicy := &config.RunPolicyConfig{Name: name, Params: map[string]float64{}}
	secondPolicy := firstPolicy.Clone()
	first := New(firstPolicy)
	second := New(secondPolicy)
	if first != second {
		t.Fatal("equivalent policy did not hit the strategy cache")
	}
	if second.Policy != secondPolicy {
		t.Fatal("cache hit retained a stale RunPolicyConfig pointer")
	}
}
