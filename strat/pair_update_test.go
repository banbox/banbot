package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
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
	if AccJobs[config.DefAcc]["BTC/USDT_1s"]["stg"] != nil {
		t.Fatalf("job should be removed")
	}
	if exitCalls == 0 {
		t.Fatalf("expected ExitOrders called")
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
