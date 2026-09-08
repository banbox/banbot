package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
)

func TestStateRegistriesAreIndependent(t *testing.T) {
	first := NewState()
	second := NewState()

	first.Versions["first"] = 1
	first.Envs["first"] = nil
	first.TmpEnvs["first"] = nil
	first.AccJobs["first"] = nil
	first.AccInfoJobs["first"] = nil
	first.PairStrats["first"] = nil
	first.ForbidJobs["first"] = nil
	first.WsSubJobs["first"] = nil
	first.AccOdSubs["first"] = []FnOdChange{nil}
	first.AccFailOpens["first"] = map[string]int{"reason": 1}
	first.SetCachedStrategy("first", &TradeStrat{Name: "first"})

	if len(second.Versions) != 0 || len(second.Envs) != 0 || len(second.TmpEnvs) != 0 ||
		len(second.AccJobs) != 0 || len(second.AccInfoJobs) != 0 || len(second.PairStrats) != 0 ||
		len(second.ForbidJobs) != 0 || len(second.WsSubJobs) != 0 || len(second.AccOdSubs) != 0 ||
		len(second.AccFailOpens) != 0 || len(second.CachedStrategies()) != 0 {
		t.Fatal("state registries are shared")
	}
}

func TestStateResetIsLocal(t *testing.T) {
	first := NewState()
	second := NewState()
	first.Versions["first"] = 1
	second.Versions["second"] = 2
	first.AddOdSub("first", nil)
	second.AddOdSub("second", nil)
	first.AddAccFailOpen("first", "reason")
	second.AddAccFailOpen("second", "reason")
	first.SetCachedStrategy("first", &TradeStrat{Name: "first"})
	second.SetCachedStrategy("second", &TradeStrat{Name: "second"})
	first.WsSubUnWatch = func(map[string][]string) {}

	first.Reset()

	if len(first.Versions) != 0 || len(first.AccOdSubs) != 0 || len(first.AccFailOpens) != 0 ||
		len(first.CachedStrategies()) != 0 || first.WsSubUnWatch != nil {
		t.Fatal("reset did not clear the first state")
	}
	if second.Versions["second"] != 2 || len(second.AccOdSubs) != 1 ||
		second.AccFailOpens["second"]["reason"] != 1 {
		t.Fatal("reset changed the second state")
	}
	if cached, ok := second.GetCachedStrategy("second"); !ok || cached == nil || cached.Name != "second" {
		t.Fatal("reset changed the second strategy cache")
	}
}

func TestLegacyStateTracksPackageGlobals(t *testing.T) {
	oldVersions := Versions
	t.Cleanup(func() { Versions = oldVersions })
	const key = "legacy_state_test"
	Versions = map[string]int{key: 3}

	state := LegacyState()
	if state.Versions[key] != 3 {
		t.Fatal("legacy state did not track the current global map")
	}
	state.Versions[key] = 4
	if oldVersions[key] != 0 {
		t.Fatal("legacy state did not bind the current global map")
	}
}

func TestExplicitStrategyStatesUseIndependentCacheAndCleanup(t *testing.T) {
	const name = "explicit_state_cleanup_probe"
	oldFactory, hadFactory := StratMake[name]
	oldCache := cacheStrats
	cacheStrats = make(map[string]*TradeStrat)
	exits := 0
	StratMake[name] = func(*config.RunPolicyConfig) *TradeStrat {
		return &TradeStrat{
			WsSubs:     map[string]string{core.WsSubTrade: "_cur_"},
			OnWsTrades: func(*StratJob, string, []*banexg.Trade) {},
			OnStratExit: func() {
				exits++
			},
		}
	}
	t.Cleanup(func() {
		if hadFactory {
			StratMake[name] = oldFactory
		} else {
			delete(StratMake, name)
		}
		cacheStrats = oldCache
	})

	policy := &config.RunPolicyConfig{Name: name}
	legacy := New(policy)
	first, second := NewState(), NewState()
	firstStgy := newStrategyWithState(first, policy)
	secondStgy := newStrategyWithState(second, policy)
	if firstStgy == secondStgy || firstStgy == legacy {
		t.Fatal("explicit states reused a global strategy object")
	}
	if newStrategyWithState(first, policy) != firstStgy {
		t.Fatal("state-local strategy cache missed an equivalent policy")
	}
	firstStgy.WsSubs[core.WsSubTrade] = "FIRST"
	if secondStgy.WsSubs[core.WsSubTrade] != "_cur_" {
		t.Fatal("explicit strategy websocket maps are shared")
	}
	firstStgy.WsSubs[core.WsSubTrade] = "_cur_"

	var firstUnwatch, secondUnwatch, legacyUnwatch int
	oldUnwatch := WsSubUnWatch
	first.WsSubUnWatch = func(map[string][]string) { firstUnwatch++ }
	second.WsSubUnWatch = func(map[string][]string) { secondUnwatch++ }
	WsSubUnWatch = func(map[string][]string) { legacyUnwatch++ }
	oldJobs, oldWsJobs := AccJobs, WsSubJobs
	t.Cleanup(func() {
		AccJobs, WsSubJobs = oldJobs, oldWsJobs
		WsSubUnWatch = oldUnwatch
	})
	job := func(state *State, stgy *TradeStrat, pair string) *StratJob {
		job := &StratJob{Strat: stgy, Symbol: &orm.ExSymbol{Symbol: pair}, TimeFrame: "1m"}
		state.AccJobs[config.DefAcc] = map[string]map[string]*StratJob{
			pair + "_1m": {stgy.Name: job},
		}
		state.WsSubJobs[core.WsSubTrade] = map[string]map[*StratJob]bool{
			pair: {job: true},
		}
		return job
	}
	job(first, firstStgy, "FIRST/USDT")
	job(second, secondStgy, "SECOND/USDT")
	WsSubJobs = map[string]map[string]map[*StratJob]bool{
		core.WsSubTrade: {"FIRST/USDT": {first.AccJobs[config.DefAcc]["FIRST/USDT_1m"][firstStgy.Name]: true}},
	}

	ExitStratJobsWithState(first)
	if firstUnwatch != 1 || secondUnwatch != 0 || legacyUnwatch != 0 {
		t.Fatalf("first cleanup callbacks = first %d, second %d, legacy %d", firstUnwatch, secondUnwatch, legacyUnwatch)
	}
	if len(first.WsSubJobs) != 1 || len(first.WsSubJobs[core.WsSubTrade]) != 1 {
		t.Fatal("first cleanup did not preserve independent registry shape")
	}
	if exits != 1 {
		t.Fatalf("first cleanup invoked %d strategy exit callbacks, want 1", exits)
	}

	ExitStratJobsWithState(second)
	if secondUnwatch != 1 || legacyUnwatch != 0 || exits != 2 {
		t.Fatalf("second cleanup callbacks = second %d, legacy %d, exits %d", secondUnwatch, legacyUnwatch, exits)
	}
}
