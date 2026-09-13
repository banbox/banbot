package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func TestLiveOrderMgrRuntimeDepsDispatchesOnlyRuntimeCallbacks(t *testing.T) {
	account := "g007-runtime-dispatch"
	var legacyCalls, runtimeCalls int
	strat.AddOdSub(account, func(string, *ormo.InOutOrder, int) { legacyCalls++ })
	runtimeState := strat.NewState()
	runtimeState.AddOdSub(account, func(string, *ormo.InOutOrder, int) { runtimeCalls++ })

	mgr := NewLiveOrderMgrWithRuntimeDeps(completeTraderDepsForTest(RuntimeDeps{
		Core:       &core.State{},
		Config:     config.NewSnapshot(&config.Config{Name: "g007-runtime"}),
		Orders:     ormo.NewOrderState(),
		Strategies: runtimeState,
		Exchange:   &issue138Exchange{},
	}), account, nil)
	mgr.fireOdChange(&ormo.InOutOrder{Enter: &ormo.ExOrder{}}, strat.OdChgEnter)

	if runtimeCalls != 1 || legacyCalls != 0 {
		t.Fatalf("runtime event crossed callback registries: runtime=%d legacy=%d", runtimeCalls, legacyCalls)
	}
}
