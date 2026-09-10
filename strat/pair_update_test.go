package strat

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	ta "github.com/banbox/banta"
)

func resetStratGlobals() {
	Versions = map[string]int{}
	Envs = map[string]*ta.BarEnv{}
	TmpEnvs = map[string]*ta.BarEnv{}
	AccJobs = map[string]map[string]map[string]*StratJob{
		config.DefAcc: {},
	}
	AccInfoJobs = map[string]map[string]map[string]*StratJob{
		config.DefAcc: {},
	}
	PairStrats = map[string]map[string]*TradeStrat{}
	ForbidJobs = map[string]map[string]bool{}
	WsSubJobs = map[string]map[string]map[*StratJob]bool{}
	RefreshWsSubJobsSnapshot()
	core.StgPairTfs = map[string]map[string]string{}
	core.Pairs = nil
	core.PairsMap = map[string]bool{}
	core.TFSecs = map[string]int{}
}

func TestUpdatePairs_NoHooks(t *testing.T) {
	resetStratGlobals()
	stg := &TradeStrat{Name: "test", Policy: &config.RunPolicyConfig{}}
	_, err := stg.UpdatePairs(PairUpdateReq{Add: []string{"BTC/USDT"}})
	if err == nil {
		t.Fatalf("expected error when hooks are not set")
	}
}

func TestLegacyAdmissionHelpersSerializeConcurrentAdds(t *testing.T) {
	resetStratGlobals()

	const workers = 8
	const pairsPerWorker = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < pairsPerWorker; i++ {
				enableAdmissionPair(nil, fmt.Sprintf("PAIR-%d-%d/USDT", worker, i))
			}
		}(worker)
	}
	wg.Wait()

	pairs := core.LegacyAdmissionPairs()
	seen := make(map[string]bool, len(pairs))
	for _, pair := range pairs {
		if seen[pair] {
			t.Fatalf("duplicate legacy pair: %s/%v", pair, pairs)
		}
		seen[pair] = true
		if !core.LegacyPairEnabled(pair) {
			t.Fatalf("legacy pair is not enabled: %s", pair)
		}
	}
	if len(pairs) != workers*pairsPerWorker {
		t.Fatalf("legacy pair count = %d, want %d", len(pairs), workers*pairsPerWorker)
	}

	active := map[string]bool{"REFRESHED/USDT": true}
	setAdmissionSnapshot(nil, active)
	active["REFRESHED/USDT"] = false
	if !core.LegacyPairEnabled("REFRESHED/USDT") || !slices.Contains(core.LegacyAdmissionPairs(), "REFRESHED/USDT") {
		t.Fatal("legacy snapshot did not publish a newly discovered pair")
	}
}

var lastWarmPairs map[string]map[string]int
var exitCalls int

func setTestHooks() {
	lastWarmPairs = nil
	exitCalls = 0
	SetPairUpdateHooks(PairUpdateHooks{
		SubWarmPairs: func(items map[string]map[string]int, delOther bool) *errs.Error {
			lastWarmPairs = items
			return nil
		},
		ExitOrders: func(acc string, orders []*ormo.InOutOrder, req *ExitReq) *errs.Error {
			exitCalls++
			return nil
		},
		LookupSymbol: func(pair string) (*orm.ExSymbol, *errs.Error) {
			return &orm.ExSymbol{ID: 1, Symbol: pair}, nil
		},
	})
}

type pairUpdateTestExchange struct {
	banexg.BanExchange
}

func TestUpdatePairs_AddCreatesJobs(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	config.RunTimeframes = []string{"1s"}
	core.Pairs = []string{"BTC/USDT"}
	stg := &TradeStrat{
		Name:       "stg",
		WarmupNum:  50,
		MinTfScore: 0.1,
		Policy:     &config.RunPolicyConfig{RunTimeframes: []string{"1s"}},
	}
	res, err := stg.UpdatePairs(PairUpdateReq{Add: []string{"BTC/USDT"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.Added) != 1 {
		t.Fatalf("expected 1 add, got %v", res.Added)
	}
	envKey := "BTC/USDT_1s"
	jobs := AccJobs[config.DefAcc][envKey]
	if jobs == nil || jobs[stg.Name] == nil {
		t.Fatalf("expected job created for %s", envKey)
	}
	if _, ok := core.StgPairTfs[stg.Name]["BTC/USDT"]; !ok {
		t.Fatalf("expected core.StgPairTfs updated")
	}
	if lastWarmPairs["BTC/USDT"]["1s"] == 0 {
		t.Fatalf("expected SubWarmPairs called with warmup")
	}
}

func TestUpdatePairsRuntimeAdmissionIsIsolated(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	config.RunTimeframes = []string{"1s"}
	oldExchange := exg.Default
	exg.Default = nil
	t.Cleanup(func() { exg.Default = oldExchange })
	first, firstErr := core.NewState(nil)
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	second, secondErr := core.NewState(nil)
	if secondErr != nil {
		first.Close()
		t.Fatal(secondErr)
	}
	t.Cleanup(func() {
		first.Close()
		second.Close()
	})
	first.SetPairs([]string{"FIRST/USDT"}, nil)
	second.SetPairs([]string{"SECOND/USDT"}, nil)

	stg := &TradeStrat{
		Name:       "isolated",
		WarmupNum:  1,
		MinTfScore: 0.1,
		Policy:     &config.RunPolicyConfig{RunTimeframes: []string{"1s"}},
	}
	res, updateErr := stg.UpdatePairs(PairUpdateReq{
		Core:     first,
		Exchange: &pairUpdateTestExchange{},
		Add:      []string{"FIRST-ADDED/USDT"},
		Strat:    stg,
	})
	if updateErr != nil {
		t.Fatalf("unexpected err: %v", updateErr)
	}
	if len(res.Added) != 1 || !first.PairEnabled("FIRST-ADDED/USDT") {
		t.Fatalf("first runtime admission = %v/%v", res.Added, first.PairsMap)
	}
	secondRes, secondUpdateErr := stg.UpdatePairs(PairUpdateReq{
		Core:     second,
		Exchange: &pairUpdateTestExchange{},
		Add:      []string{"SECOND-ADDED/USDT"},
		Strat:    stg,
	})
	if secondUpdateErr != nil {
		t.Fatalf("second runtime update failed: %v", secondUpdateErr)
	}
	if len(secondRes.Added) != 1 || !second.PairEnabled("SECOND-ADDED/USDT") {
		t.Fatalf("second runtime admission = %v/%v", secondRes.Added, second.PairsMap)
	}
	if first.PairEnabled("SECOND-ADDED/USDT") || second.PairEnabled("FIRST-ADDED/USDT") {
		t.Fatalf("pair rotation leaked between runtimes: first=%v second=%v", first.PairsMap, second.PairsMap)
	}
	if len(first.Pairs) != 2 || len(second.Pairs) != 2 || first.Pairs[0] != "FIRST/USDT" || second.Pairs[0] != "SECOND/USDT" {
		t.Fatalf("runtime pair snapshots changed unexpectedly: first=%v second=%v", first.Pairs, second.Pairs)
	}
	if core.PairsMap["FIRST-ADDED/USDT"] || core.PairsMap["SECOND-ADDED/USDT"] {
		t.Fatal("explicit runtime pair rotation changed the legacy global map")
	}
}

func TestUpdatePairsExplicitStrategyStateWinsOverHookState(t *testing.T) {
	resetStratGlobals()
	oldHooks := SnapshotPairUpdateHooks()
	t.Cleanup(func() { SetPairUpdateHooks(oldHooks) })
	setTestHooks()
	config.RunTimeframes = []string{"1s"}
	firstCore, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	secondCore, err := core.NewState(nil)
	if err != nil {
		firstCore.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		firstCore.Close()
		secondCore.Close()
	})
	firstCore.EnsureRuntimeMaps()
	secondCore.EnsureRuntimeMaps()
	first := NewState()
	second := NewState()
	first.AccJobs[config.DefAcc] = map[string]map[string]*StratJob{}
	second.AccJobs[config.DefAcc] = map[string]map[string]*StratJob{}
	hooks := SnapshotPairUpdateHooks()
	hooks.StrategyState = second
	hooks.Core = secondCore
	SetPairUpdateHooks(hooks)
	first.SetPairUpdateHooks(PairUpdateHooks{
		SubWarmPairs: hooks.SubWarmPairs,
		ExitOrders:   hooks.ExitOrders,
		LookupSymbol: hooks.LookupSymbol,
	})

	stg := &TradeStrat{
		Name:       "explicit-state-pair-update",
		MinTfScore: 0.1,
		Policy:     &config.RunPolicyConfig{RunTimeframes: []string{"1s"}},
	}
	res, updateErr := stg.UpdatePairs(PairUpdateReq{
		Strat:         stg,
		StrategyState: first,
		Core:          firstCore,
		Exchange:      &pairUpdateTestExchange{},
		ForceAdd:      true,
		Add:           []string{"FIRST/USDT"},
	})
	if updateErr != nil {
		t.Fatalf("explicit state pair update failed: %v", updateErr)
	}
	if len(res.Added) != 1 {
		t.Fatalf("explicit state pair update added %v", res.Added)
	}
	if first.AccJobs[config.DefAcc]["FIRST/USDT_1s"][stg.Name] == nil {
		t.Fatal("explicit strategy state did not receive the new job")
	}
	if len(second.AccJobs[config.DefAcc]) != 0 {
		t.Fatalf("global hook strategy state was mutated: %+v", second.AccJobs)
	}
	if _, ok := secondCore.StgPairTfs[stg.Name]; ok {
		t.Fatalf("global hook core state was mutated: %+v", secondCore.StgPairTfs)
	}
}

func TestUpdatePairsExplicitStrategyStateDoesNotUseLegacyHooks(t *testing.T) {
	resetStratGlobals()
	oldHooks := SnapshotPairUpdateHooks()
	t.Cleanup(func() { SetPairUpdateHooks(oldHooks) })
	setTestHooks()

	state := NewState()
	stg := &TradeStrat{
		Name:   "explicit-hooks-required",
		Policy: &config.RunPolicyConfig{RunTimeframes: []string{"1s"}},
	}
	_, updateErr := stg.UpdatePairs(PairUpdateReq{
		Strat:         stg,
		StrategyState: state,
		Exchange:      &pairUpdateTestExchange{},
		ForceAdd:      true,
		Add:           []string{"BTC/USDT"},
	})
	if updateErr == nil || updateErr.Code != core.ErrRunTime {
		t.Fatalf("explicit state without hooks error = %v, want ErrRunTime", updateErr)
	}
}

func TestUpdatePairsExplicitRuntimeRequiresExchange(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	config.RunTimeframes = []string{"1s"}
	runtimeState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtimeState.Close)

	stg := &TradeStrat{
		Name:       "missing-exchange",
		WarmupNum:  1,
		MinTfScore: 0.1,
		Policy:     &config.RunPolicyConfig{RunTimeframes: []string{"1s"}},
	}
	_, updateErr := stg.UpdatePairs(PairUpdateReq{
		Core:  runtimeState,
		Add:   []string{"MISSING/USDT"},
		Strat: stg,
	})
	if updateErr == nil || updateErr.Code != core.ErrExgNotInit {
		t.Fatalf("missing runtime exchange error = %v, want ErrExgNotInit", updateErr)
	}
}

func TestCallStratSymbolsExplicitRuntimeUsesHookExchange(t *testing.T) {
	resetStratGlobals()
	oldHooks := SnapshotPairUpdateHooks()
	t.Cleanup(func() { SetPairUpdateHooks(oldHooks) })
	setTestHooks()
	hooks := SnapshotPairUpdateHooks()
	hooks.Exchange = &pairUpdateTestExchange{}
	SetPairUpdateHooks(hooks)
	oldExchange := exg.Default
	exg.Default = nil
	t.Cleanup(func() { exg.Default = oldExchange })
	oldTimeframes := config.RunTimeframes
	config.RunTimeframes = []string{"1s"}
	t.Cleanup(func() { config.RunTimeframes = oldTimeframes })

	runtimeState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtimeState.Close)
	runtimeState.SetPairs([]string{"BASE/USDT"}, nil)
	symbols := orm.NewSymbolStateWithIdentity("runtime", "spot")
	symbols.CacheExSymbol(&orm.ExSymbol{ID: 1, Exchange: "runtime", Market: "spot", Symbol: "BASE/USDT"})
	symbols.CacheExSymbol(&orm.ExSymbol{ID: 2, Exchange: "runtime", Market: "spot", Symbol: "ADDED/USDT"})
	stg := &TradeStrat{
		Name: "runtime-symbols",
		OnSymbols: func(pairs []string) []string {
			return append(append([]string(nil), pairs...), "ADDED/USDT")
		},
	}

	got, callErr := CallStratSymbolsWithRuntimeState(runtimeState, symbols, stg, []string{"BASE/USDT"}, map[string]map[string]float64{})
	if callErr != nil {
		t.Fatalf("explicit exchange lookup failed: %v", callErr)
	}
	if len(got) != 2 || got[1].Symbol != "ADDED/USDT" {
		t.Fatalf("runtime symbol rotation = %+v", got)
	}
	if !runtimeState.PairEnabled("ADDED/USDT") || core.PairsMap["ADDED/USDT"] {
		t.Fatalf("runtime admission leaked into legacy state: runtime=%v legacy=%v", runtimeState.PairsMap, core.PairsMap)
	}
}

func TestPairUpdateHooksDefaultLookupUsesSymbolState(t *testing.T) {
	oldHooks := SnapshotPairUpdateHooks()
	t.Cleanup(func() { SetPairUpdateHooks(oldHooks) })

	symbols := orm.NewSymbolStateWithIdentity("runtime", "spot")
	symbols.CacheExSymbol(&orm.ExSymbol{ID: 9, Exchange: "runtime", Market: "spot", Symbol: "RUNTIME/USDT"})
	SetPairUpdateHooks(PairUpdateHooks{
		SymbolState: symbols,
		SubWarmPairs: func(map[string]map[string]int, bool) *errs.Error {
			return nil
		},
	})

	got, lookupErr := SnapshotPairUpdateHooks().LookupSymbol("RUNTIME/USDT")
	if lookupErr != nil {
		t.Fatalf("runtime hook lookup failed: %v", lookupErr)
	}
	if got == nil || got.ID != 9 {
		t.Fatalf("runtime hook lookup returned %+v, want sid 9", got)
	}
}

func TestInitBarEnvUsesSymbolIdentity(t *testing.T) {
	resetStratGlobals()
	oldExgName, oldMarket := core.ExgName, core.Market
	t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })
	core.ExgName, core.Market = "legacy", "spot"

	env := initBarEnv(&orm.ExSymbol{
		Exchange: "runtime",
		Market:   "linear",
		Symbol:   "RUNTIME/USDT:USDT",
	}, "1s")
	if env.Exchange != "runtime" || env.MarketType != "linear" {
		t.Fatalf("bar env identity = %s/%s, want runtime/linear", env.Exchange, env.MarketType)
	}
}

func TestUpdatePairs_AddIgnoresPolicyPairs(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	config.RunTimeframes = []string{"1s"}
	core.Pairs = []string{"ETH/USDT"}
	stg := &TradeStrat{
		Name:       "stg",
		WarmupNum:  50,
		MinTfScore: 0.1,
		Policy:     &config.RunPolicyConfig{RunTimeframes: []string{"1s"}, Pairs: []string{"ETH/USDT"}},
	}
	res, err := stg.UpdatePairs(PairUpdateReq{Add: []string{"BTC/USDT"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.Added) != 1 || res.Added[0] != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT added, got %v", res.Added)
	}
	envKey := "BTC/USDT_1s"
	jobs := AccJobs[config.DefAcc][envKey]
	if jobs == nil || jobs[stg.Name] == nil {
		t.Fatalf("expected job created for %s", envKey)
	}
}

func seedJobWithOrder(pair, tf, stratName string, entered bool) {
	exs := &orm.ExSymbol{ID: 1, Symbol: pair}
	stg := &TradeStrat{Name: stratName, WarmupNum: 10, Policy: &config.RunPolicyConfig{}}
	job := &StratJob{
		Strat:     stg,
		Symbol:    exs,
		TimeFrame: tf,
		Account:   config.DefAcc,
		TPMaxs:    map[int64]float64{},
	}
	if entered {
		job.EnteredNum = 1
		job.LongOrders = []*ormo.InOutOrder{{}}
	}
	envKey := pair + "_" + tf
	AccJobs[config.DefAcc][envKey] = map[string]*StratJob{stratName: job}
	items, ok := PairStrats[pair]
	if !ok {
		items = map[string]*TradeStrat{}
		PairStrats[pair] = items
	}
	items[stratName] = stg
	if _, ok := core.StgPairTfs[stratName]; !ok {
		core.StgPairTfs[stratName] = map[string]string{}
	}
	core.StgPairTfs[stratName][pair] = tf
	core.PairsMap[pair] = true
	core.Pairs = append(core.Pairs, pair)
}

func TestUpdatePairs_RemoveClose(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	seedJobWithOrder("BTC/USDT", "1s", "stg", true)
	stg := PairStrats["BTC/USDT"]["stg"]
	res, err := stg.UpdatePairs(PairUpdateReq{Remove: []string{"BTC/USDT"}, CloseOnRemove: true})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.Removed) != 1 {
		t.Fatalf("expected removal")
	}
	job := AccJobs[config.DefAcc]["BTC/USDT_1s"]["stg"]
	if job == nil || !job.pairRemovalPending {
		t.Fatalf("non-terminal removal should remain routable: %+v", job)
	}
	if _, ok := core.StgPairTfs["stg"]["BTC/USDT"]; !ok {
		t.Fatal("pending pair removal lost its admission mapping before terminal state")
	}
	if exitCalls == 0 {
		t.Fatalf("expected ExitOrders called")
	}
	job.EnteredNum = 0
	job.LongOrders = nil
	FinalizePairRotation(nil)
	if AccJobs[config.DefAcc]["BTC/USDT_1s"] != nil {
		t.Fatalf("terminal pair-removal job was not finalized")
	}
	if _, ok := core.StgPairTfs["stg"]["BTC/USDT"]; ok {
		t.Fatal("terminal pair-removal mapping was not finalized")
	}
}

func TestFinalizePairRotationKeepsPendingOrderRoutable(t *testing.T) {
	resetStratGlobals()
	stg := &TradeStrat{Name: "stg", Policy: &config.RunPolicyConfig{}}
	job := &StratJob{
		Strat:     stg,
		Symbol:    &orm.ExSymbol{ID: 1, Symbol: "BTC/USDT"},
		TimeFrame: "1s",
		Account:   config.DefAcc,
		TPMaxs:    map[int64]float64{},
		ShortOrders: []*ormo.InOutOrder{{
			IOrder: &ormo.IOrder{ID: 1, Status: ormo.InOutStatusInit},
			Enter:  &ormo.ExOrder{Status: ormo.OdStatusInit},
		}},
		pairRemovalPending: true,
	}
	AccJobs[config.DefAcc]["BTC/USDT_1s"] = map[string]*StratJob{stg.Name: job}
	PairStrats["BTC/USDT"] = map[string]*TradeStrat{stg.Name: stg}

	FinalizePairRotation(nil)
	if AccJobs[config.DefAcc]["BTC/USDT_1s"][stg.Name] != job {
		t.Fatal("pending pair-removal job was removed before its order reached terminal state")
	}

	job.ShortOrders[0].Status = ormo.InOutStatusFullExit
	job.ShortOrders[0].Enter.Status = ormo.OdStatusClosed
	FinalizePairRotation(nil)
	if AccJobs[config.DefAcc]["BTC/USDT_1s"] != nil {
		t.Fatal("terminal pair-removal job was not finalized")
	}
	if PairStrats["BTC/USDT"] != nil {
		t.Fatal("terminal pair-removal strategy mapping was not finalized")
	}
}

func TestUpdatePairs_RemoveHold(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	seedJobWithOrder("BTC/USDT", "1s", "stg", false)
	stg := PairStrats["BTC/USDT"]["stg"]
	res, err := stg.UpdatePairs(PairUpdateReq{Remove: []string{"BTC/USDT"}, CloseOnRemove: false})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.Removed) != 1 {
		t.Fatalf("expected removal")
	}
	job := AccJobs[config.DefAcc]["BTC/USDT_1s"]["stg"]
	if job == nil || job.MaxOpenLong != -1 || job.MaxOpenShort != -1 {
		t.Fatalf("job should be kept but disabled")
	}
	if exitCalls != 0 {
		t.Fatalf("should not exit orders on hold")
	}
}

func TestUpdatePairs_RebuildsAllWarms(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	config.RunTimeframes = []string{"1s"}
	core.Pairs = []string{"BTC/USDT", "ETH/USDT", "XRP/USDT"}
	seedJobWithOrder("BTC/USDT", "1s", "stg1", false)
	seedJobWithOrder("ETH/USDT", "5s", "stg1", false)
	stg := PairStrats["BTC/USDT"]["stg1"]
	_, err := stg.UpdatePairs(PairUpdateReq{Add: []string{"XRP/USDT"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if lastWarmPairs["ETH/USDT"]["5s"] == 0 {
		t.Fatalf("expected existing pairs to be included in allWarms")
	}
}

func TestUpdatePairs_RebuildsWarmsFromCurrentDataSubs(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	config.RunTimeframes = []string{"1s"}
	core.Pairs = []string{"BTC/USDT", "ETH/USDT"}

	job := &StratJob{
		Strat: &TradeStrat{
			Name:      "stg",
			WarmupNum: 20,
			Policy:    &config.RunPolicyConfig{RunTimeframes: []string{"1s"}},
			OnData: func(s *StratJob, data DataEvent) {
			},
			OnDataSubs: func(s *StratJob) []*DataSub {
				return []*DataSub{
					{Source: "kline", TimeFrame: "15m", WarmupNum: 9},
					{Source: "macro", TimeFrame: "1d", WarmupNum: 3},
				}
			},
		},
		Symbol:    &orm.ExSymbol{ID: 1, Symbol: "BTC/USDT"},
		TimeFrame: "1s",
		Account:   config.DefAcc,
		TPMaxs:    map[int64]float64{},
	}
	AccJobs[config.DefAcc]["BTC/USDT_1s"] = map[string]*StratJob{"stg": job}
	PairStrats["BTC/USDT"] = map[string]*TradeStrat{"stg": job.Strat}
	core.StgPairTfs["stg"] = map[string]string{"BTC/USDT": "1s"}
	core.PairsMap["BTC/USDT"] = true

	stg := job.Strat
	_, err := stg.UpdatePairs(PairUpdateReq{Add: []string{"ETH/USDT"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if lastWarmPairs["BTC/USDT"]["15m"] != 9 {
		t.Fatalf("expected current-symbol kline side input warmup to be preserved, got %+v", lastWarmPairs["BTC/USDT"])
	}
	if _, ok := lastWarmPairs["BTC/USDT"]["1d"]; ok {
		t.Fatalf("non-kline side inputs should not be added to warm kline subscriptions: %+v", lastWarmPairs["BTC/USDT"])
	}
}

func TestEnsureStratJobAllowsBatchInfoSideSubscription(t *testing.T) {
	resetStratGlobals()
	core.OrderMatchTfs = map[string]bool{}

	exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}
	env, err := ta.NewBarEnv(exs.Exchange, exs.Market, exs.Symbol, "15m")
	if err != nil {
		t.Fatalf("create bar env: %v", err)
	}
	stgy := &TradeStrat{
		Name:      "batch-info",
		BatchInfo: true,
		Policy:    &config.RunPolicyConfig{},
		OnPairInfos: func(s *StratJob) []*PairSub {
			return []*PairSub{{Pair: "_cur_", TimeFrame: "1h", WarmupNum: 10}}
		},
		OnBatchInfos: func(string, map[string]*JobEnv) {},
	}

	ensureStratJob(stgy, "15m", exs, env, core.OdDirtBoth, func(string, string, int) {}, accStratLimits{})

	job := AccJobs[config.DefAcc]["BTC/USDT:USDT_15m"][stgy.Name]
	if job == nil {
		t.Fatal("expected main job to be created")
	}
	key := DataSubKey(orm.SeriesSourceKline, exs.ID, "1h")
	if AccInfoJobs[config.DefAcc][key][stgy.Name+"_"+exs.Symbol] != job {
		t.Fatal("expected batch-info strategy to be registered for its side input")
	}
}

func TestEnsureStratJobWithRuntimeStateUsesBoundCore(t *testing.T) {
	resetStratGlobals()
	core.OrderMatchTfs = map[string]bool{"legacy": true}

	runtimeCore, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeCore.Close()
	strategyState := NewStateWithRuntime(runtimeCore, nil, nil, nil, nil)
	strategyState.AccJobs[config.DefAcc] = map[string]map[string]*StratJob{}

	exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}
	env, envErr := ta.NewBarEnv(exs.Exchange, exs.Market, exs.Symbol, "15m")
	if envErr != nil {
		t.Fatal(envErr)
	}
	stgy := &TradeStrat{Name: "runtime-match", Policy: &config.RunPolicyConfig{}}
	ensureStratJobWithRuntimeState(strategyState, nil, stgy, "15m", exs, env, core.OdDirtBoth,
		func(string, string, int) {}, accStratLimits{}, nil)

	if !runtimeCore.OrderMatchTfs["15m"] {
		t.Fatalf("bound runtime core was not updated: %v", runtimeCore.OrderMatchTfs)
	}
	if runtimeCore.OrderMatchTfs["legacy"] || core.OrderMatchTfs["15m"] {
		t.Fatalf("order-match state leaked to the wrong owner: runtime=%v legacy=%v",
			runtimeCore.OrderMatchTfs, core.OrderMatchTfs)
	}
}

func TestEnsureStratJobWithRuntimeStateWithoutCoreDoesNotUseLegacy(t *testing.T) {
	resetStratGlobals()
	core.OrderMatchTfs = map[string]bool{"legacy": true}

	strategyState := NewState()
	strategyState.AccJobs[config.DefAcc] = map[string]map[string]*StratJob{}
	exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}
	env, err := ta.NewBarEnv(exs.Exchange, exs.Market, exs.Symbol, "15m")
	if err != nil {
		t.Fatal(err)
	}
	stgy := &TradeStrat{Name: "runtime-without-core", Policy: &config.RunPolicyConfig{}}
	ensureStratJobWithRuntimeState(strategyState, nil, stgy, "15m", exs, env, core.OdDirtBoth,
		func(string, string, int) {}, accStratLimits{}, nil)

	if len(core.OrderMatchTfs) != 1 || !core.OrderMatchTfs["legacy"] {
		t.Fatalf("unbound explicit state changed legacy order-match state: %v", core.OrderMatchTfs)
	}
}

func TestFinalizePairRotationUsesBoundCoreAndDoesNotTouchLegacy(t *testing.T) {
	resetStratGlobals()
	core.StgPairTfs = map[string]map[string]string{
		"legacy": {"LEGACY/USDT": "1s"},
	}
	core.SetLegacyAdmissionPair("LEGACY/USDT", true)

	runtimeCore, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeCore.Close()
	runtimeCore.SetPairs([]string{"BTC/USDT"}, nil)
	runtimeCore.StgPairTfs["runtime"] = map[string]string{"BTC/USDT": "1s"}
	strategyState := NewStateWithRuntime(runtimeCore, nil, nil, nil, nil)
	stgy := &TradeStrat{Name: "runtime", Policy: &config.RunPolicyConfig{}}
	job := &StratJob{
		Strat:              stgy,
		Symbol:             &orm.ExSymbol{ID: 1, Symbol: "BTC/USDT"},
		TimeFrame:          "1s",
		Account:            config.DefAcc,
		TPMaxs:             map[int64]float64{},
		pairRemovalPending: true,
	}
	strategyState.AccJobs[config.DefAcc] = map[string]map[string]*StratJob{
		"BTC/USDT_1s": {stgy.Name: job},
	}
	strategyState.PairStrats["BTC/USDT"] = map[string]*TradeStrat{stgy.Name: stgy}

	FinalizePairRotation(strategyState)

	if _, ok := runtimeCore.StgPairTfs[stgy.Name]["BTC/USDT"]; ok {
		t.Fatalf("bound runtime mapping was not finalized: %v", runtimeCore.StgPairTfs)
	}
	if runtimeCore.PairEnabled("BTC/USDT") {
		t.Fatal("finalized runtime pair remains admitted")
	}
	if _, ok := core.StgPairTfs["legacy"]["LEGACY/USDT"]; !ok || !core.LegacyPairEnabled("LEGACY/USDT") {
		t.Fatalf("legacy state was changed while finalizing runtime pair: stg=%v pairs=%v",
			core.StgPairTfs, core.LegacyAdmissionPairs())
	}
}

func TestFinalizePairRotationWithoutCoreDoesNotUseLegacy(t *testing.T) {
	resetStratGlobals()
	core.StgPairTfs = map[string]map[string]string{
		"runtime": {"BTC/USDT": "1s"},
	}
	core.SetLegacyAdmissionPair("BTC/USDT", true)
	strategyState := NewState()
	stgy := &TradeStrat{Name: "runtime", Policy: &config.RunPolicyConfig{}}
	job := &StratJob{
		Strat:              stgy,
		Symbol:             &orm.ExSymbol{ID: 1, Symbol: "BTC/USDT"},
		TimeFrame:          "1s",
		Account:            config.DefAcc,
		TPMaxs:             map[int64]float64{},
		pairRemovalPending: true,
	}
	strategyState.AccJobs[config.DefAcc] = map[string]map[string]*StratJob{
		"BTC/USDT_1s": {stgy.Name: job},
	}
	strategyState.PairStrats["BTC/USDT"] = map[string]*TradeStrat{stgy.Name: stgy}

	FinalizePairRotation(strategyState)

	if _, ok := core.StgPairTfs[stgy.Name]["BTC/USDT"]; !ok || !core.LegacyPairEnabled("BTC/USDT") {
		t.Fatalf("unbound explicit state changed legacy state: stg=%v pairs=%v",
			core.StgPairTfs, core.LegacyAdmissionPairs())
	}
}
