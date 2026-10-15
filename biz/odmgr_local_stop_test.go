package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestInitLocalOrderMgrUsesScopedBacktestStop(t *testing.T) {
	originalStopAll := core.StopAll
	t.Cleanup(func() { core.StopAll = originalStopAll })
	trader := newCompleteTraderForTest(t, RuntimeDeps{Config: config.NewSnapshotWithDirs(&config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}}, t.TempDir(), "")})
	deps := *trader.RuntimeDependencies()
	liveStopCalls := 0
	localStopCalls := 0
	core.StopAll = func() { liveStopCalls++ }
	InitLocalOrderMgrWithRuntimeDeps(deps, nil, false, func() { localStopCalls++ })

	odMgr, ok := deps.Trading.OrderManager("default").(*LocalOrderMgr)
	if !ok {
		t.Fatalf("order manager type = %T, want *LocalOrderMgr", deps.Trading.OrderManager("default"))
	}
	odMgr.stopBacktest()
	if localStopCalls != 1 || liveStopCalls != 0 {
		t.Fatalf("stop calls local=%d live=%d, want 1/0", localStopCalls, liveStopCalls)
	}
}
