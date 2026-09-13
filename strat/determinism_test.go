package strat

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	ta "github.com/banbox/banta"
)

func TestPickTimeFrameUsesExactScoreAndNameTieBreak(t *testing.T) {
	oldTfs := config.RunTimeframes
	config.RunTimeframes = []string{"1m", "3m", "5m"}
	t.Cleanup(func() { config.RunTimeframes = oldTfs })
	strategy := &TradeStrat{Policy: &config.RunPolicyConfig{}, MinTfScore: -1}
	scores := map[string]float64{"5m": 0.1008, "3m": 0.1004, "1m": 0.1004}

	for range 32 {
		var gotOrder []string
		strategy.PickTimeFrame = func(_ string, items []*core.TfScore) string {
			for _, item := range items {
				gotOrder = append(gotOrder, item.TF)
			}
			return items[0].TF
		}
		if got := strategy.pickTimeFrame("BTC/USDT", scores); got != "1m" {
			t.Fatalf("picked %s, want 1m from exact score/name order %v", got, gotOrder)
		}
		if !slices.Equal(gotOrder, []string{"1m", "3m", "5m"}) {
			t.Fatalf("timeframe order = %v", gotOrder)
		}
	}
}

func TestResetJobsUsesCanonicalOpenOrderView(t *testing.T) {
	cfg := &config.Config{BTStrict: true, Accounts: map[string]*config.AccountConfig{"default": {}}}
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.SetRunMode(core.RunModeBackTest)
	strategies := NewStateWithRuntime(state, nil, cfg, nil, nil)
	strategies.BindRuntimeAccounts(cfg.Accounts)
	orders := ormo.NewOrderState()
	openOrders, lock := orders.GetOpenODs("default")
	lock.Lock()
	for id := int64(6); id >= 1; id-- {
		openOrders[id] = &ormo.InOutOrder{IOrder: &ormo.IOrder{
			ID: id, Symbol: "BTC/USDT", Timeframe: "1m", Strategy: "rotation",
			EnterAt: ((id + 1) / 2) * 100, Short: id%2 == 1, Status: ormo.InOutStatusFullEnter,
		}}
	}
	lock.Unlock()

	job := &StratJob{
		Strat: &TradeStrat{Name: "rotation", OrderOnRotation: "open"},
		Env:   &ta.BarEnv{}, Symbol: &orm.ExSymbol{Symbol: "BTC/USDT"}, TimeFrame: "1m", OrderNum: 6,
	}
	strategies.SetJobMap("default", "BTC/USDT_1m", map[string]*StratJob{"rotation": job})
	resetJobsWithState(strategies, state, orders)

	longIDs := make([]int64, len(job.LongOrders))
	for i, od := range job.LongOrders {
		longIDs[i] = od.ID
	}
	shortIDs := make([]int64, len(job.ShortOrders))
	for i, od := range job.ShortOrders {
		shortIDs[i] = od.ID
	}
	if !slices.Equal(longIDs, []int64{2, 4, 6}) || !slices.Equal(shortIDs, []int64{1, 3, 5}) {
		t.Fatalf("rotation order views long=%v short=%v", longIDs, shortIDs)
	}
}
