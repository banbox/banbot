package biz

import (
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func TestLegacyAdmissionCountsExistingOrders(t *testing.T) {
	clock := btime.NewClockState(false, nil)
	clock.SetTimeMS(1700000000000)
	orders := ormo.NewOrderState()
	manager := &OrderMgr{Account: "default", clock: clock, runtimeCore: &core.State{RunMode: core.RunModeBackTest}, runtimeDeps: true, walletDeps: RuntimeDeps{Orders: orders, Strategies: strat.NewState()}, runtimeCfg: runtimeOrderConfig{maxOpenOrders: 1}}
	req := &strat.EnterReq{StratName: "legacy", Tag: "first"}
	exs := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	allowed, _ := manager.allowOrderEnter(exs, "1m", []*strat.EnterReq{req})
	if len(allowed) != 1 {
		t.Fatal("one free slot did not admit the first order")
	}
	open, lock := orders.GetOpenODs("default")
	lock.Lock()
	open[1] = &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1, Strategy: "legacy"}}
	lock.Unlock()
	allowed, _ = manager.allowOrderEnter(exs, "1m", []*strat.EnterReq{req})
	if len(allowed) != 0 {
		t.Fatal("existing order did not consume the account slot")
	}
}
