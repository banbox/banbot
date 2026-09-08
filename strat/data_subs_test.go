package strat

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
)

func TestCollectDataSubsExplicitStateCanonicalizesSymbols(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("binance", "spot")
	if err := state.SetExSymbols([]*orm.ExSymbol{
		{ID: 1, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		{ID: 2, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"},
	}); err != nil {
		t.Fatal(err)
	}
	main := state.GetSymbolByID(1)
	side := state.GetSymbolByID(2)
	legacyMain := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	legacySide := &orm.ExSymbol{ID: 2, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	job := &StratJob{
		Symbol: legacyMain,
		Strat: &TradeStrat{
			OnPairInfos: func(*StratJob) []*PairSub {
				return []*PairSub{
					{Pair: "_cur_", TimeFrame: "5m"},
					{Pair: "ETH/USDT", TimeFrame: "1h"},
				}
			},
			OnDataSubs: func(*StratJob) []*DataSub {
				return []*DataSub{{Source: "macro", ExSymbol: legacySide, TimeFrame: "1d"}}
			},
		},
	}

	subs := CollectDataSubsWithSymbolState(state, job)
	if len(subs) != 3 {
		t.Fatalf("subscriptions = %+v, want current, ordinary, and data subscriptions", subs)
	}
	if subs[0].ExSymbol != main {
		t.Fatalf("current-pair symbol = %p, want canonical %p", subs[0].ExSymbol, main)
	}
	if subs[1].ExSymbol != side || subs[2].ExSymbol != side {
		t.Fatalf("side symbols = %p/%p, want canonical %p", subs[1].ExSymbol, subs[2].ExSymbol, side)
	}
}

func TestCollectDataSubsExplicitStateSkipsMismatchedSymbols(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("binance", "spot")
	if err := state.SetExSymbols([]*orm.ExSymbol{{
		ID: 1, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
	}}); err != nil {
		t.Fatal(err)
	}
	job := &StratJob{
		Symbol: &orm.ExSymbol{ID: 99, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		Strat: &TradeStrat{
			OnPairInfos: func(*StratJob) []*PairSub {
				return []*PairSub{{Pair: "_cur_", TimeFrame: "5m"}}
			},
			OnDataSubs: func(*StratJob) []*DataSub {
				return []*DataSub{
					{Source: "kline", ExSymbol: &orm.ExSymbol{ID: 99, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}, TimeFrame: "1h"},
					{Source: "kline", ExSymbol: &orm.ExSymbol{ID: 1, Exchange: "okx", Market: "spot", Symbol: "BTC/USDT"}, TimeFrame: "1h"},
				}
			},
		},
	}
	if subs := CollectDataSubsWithSymbolState(state, job); len(subs) != 0 {
		t.Fatalf("mismatched subscriptions were not skipped: %+v", subs)
	}
}

func TestCollectDataSubsNilStateKeepsLegacySymbols(t *testing.T) {
	legacy := &orm.ExSymbol{ID: 9, Exchange: "legacy", Market: "macro", Symbol: "CPI_US"}
	job := &StratJob{
		Symbol: legacy,
		Strat: &TradeStrat{
			OnPairInfos: func(*StratJob) []*PairSub {
				return []*PairSub{{Pair: "_cur_", TimeFrame: "1d"}}
			},
			OnDataSubs: func(*StratJob) []*DataSub {
				return []*DataSub{{Source: "macro", ExSymbol: legacy, TimeFrame: "1d"}}
			},
		},
	}
	subs := CollectDataSubsWithSymbolState(nil, job)
	if len(subs) != 2 || subs[0].ExSymbol != legacy || subs[1].ExSymbol != legacy {
		t.Fatalf("legacy symbols changed: %+v", subs)
	}
}

func TestWsSubJobViewsSerializeWithRotation(t *testing.T) {
	oldJobs := WsSubJobs
	t.Cleanup(func() {
		LockJobsWrite()
		WsSubJobs = oldJobs
		UnlockJobsWrite()
		RefreshWsSubJobsSnapshot()
	})
	job := &StratJob{Strat: &TradeStrat{}}
	LockJobsWrite()
	WsSubJobs = map[string]map[string]map[*StratJob]bool{
		core.WsSubTrade: {"BTC/USDT": {job: true}},
	}
	UnlockJobsWrite()
	RefreshWsSubJobsSnapshot()

	const iterations = 1000
	var seen atomic.Int32
	var pairsSeen atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range iterations {
			for _, pair := range []string{"BTC/USDT", "ETH/USDT"} {
				ForEachWsSubJob(core.WsSubTrade, pair, func(*StratJob) {
					seen.Add(1)
					runtime.Gosched()
				})
			}
			if len(WsSubJobPairs(core.WsSubTrade)) > 0 {
				pairsSeen.Add(1)
			}
			_ = WsSubJobTypes()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			LockJobsWrite()
			if i%2 == 0 {
				WsSubJobs = map[string]map[string]map[*StratJob]bool{
					core.WsSubTrade: {"BTC/USDT": {job: true}},
				}
			} else {
				WsSubJobs = map[string]map[string]map[*StratJob]bool{
					core.WsSubTrade: {"ETH/USDT": {job: true}},
				}
			}
			UnlockJobsWrite()
			RefreshWsSubJobsSnapshot()
		}
	}()
	close(start)
	wg.Wait()
	if seen.Load() == 0 || pairsSeen.Load() == 0 {
		t.Fatalf("websocket job views returned no subscriptions: callbacks=%d pairs=%d", seen.Load(), pairsSeen.Load())
	}
}

func TestWsSubJobRegistriesIsolateRuntimeStates(t *testing.T) {
	oldJobs := WsSubJobs
	t.Cleanup(func() {
		LockJobsWrite()
		WsSubJobs = oldJobs
		UnlockJobsWrite()
		RefreshWsSubJobsSnapshot()
	})

	stateA := orm.NewSymbolState()
	stateB := orm.NewSymbolState()
	jobA := &StratJob{symbols: stateA}
	jobB := &StratJob{symbols: stateB}
	legacyJob := &StratJob{}
	LockJobsWrite()
	WsSubJobs = map[string]map[string]map[*StratJob]bool{
		core.WsSubTrade: {"BTC/USDT": {jobA: true, jobB: true, legacyJob: true}},
	}
	UnlockJobsWrite()
	RefreshWsSubJobsSnapshot()

	registryA := NewWsSubJobRegistry(stateA)
	registryB := NewWsSubJobRegistry(stateB)
	legacy := LegacyWsSubJobRegistry()
	assertRegistryJob := func(name string, registry *WsSubJobRegistry, want *StratJob) {
		t.Helper()
		var got []*StratJob
		registry.ForEach(core.WsSubTrade, "BTC/USDT", func(job *StratJob) {
			got = append(got, job)
		})
		if len(got) != 1 || got[0] != want {
			t.Fatalf("%s jobs = %p, want %p", name, got, want)
		}
	}
	assertRegistryJob("runtime A", registryA, jobA)
	assertRegistryJob("runtime B", registryB, jobB)

	var legacyJobs []*StratJob
	legacy.ForEach(core.WsSubTrade, "BTC/USDT", func(job *StratJob) {
		legacyJobs = append(legacyJobs, job)
	})
	if len(legacyJobs) != 3 {
		t.Fatalf("legacy jobs = %d, want 3", len(legacyJobs))
	}

	LockJobsWrite()
	WsSubJobs = map[string]map[string]map[*StratJob]bool{
		core.WsSubTrade: {"ETH/USDT": {jobB: true}},
	}
	UnlockJobsWrite()
	registryA.Refresh()
	registryB.Refresh()
	if pairs := registryA.Pairs(core.WsSubTrade); len(pairs) != 0 {
		t.Fatalf("runtime A pairs after rotation = %v, want none", pairs)
	}
	if pairs := registryB.Pairs(core.WsSubTrade); len(pairs) != 1 || pairs[0] != "ETH/USDT" {
		t.Fatalf("runtime B pairs after rotation = %v, want ETH/USDT", pairs)
	}
}
