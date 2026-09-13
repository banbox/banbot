package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestInitLocalLiveOrderMgrWithRuntimeDepsUsesOwnedAccounts(t *testing.T) {
	oldAccounts := config.Accounts
	oldManagers := accOdMgrs
	config.Accounts = map[string]*config.AccountConfig{"legacy-only": {}}
	accOdMgrs = make(map[string]IOrderMgr)
	t.Cleanup(func() {
		config.Accounts = oldAccounts
		accOdMgrs = oldManagers
	})

	trading := NewTradingState()
	deps := completeTraderDepsForTest(RuntimeDeps{
		Core:     &core.State{RunEnv: core.RunEnvDryRun},
		Config:   config.NewSnapshot(&config.Config{Accounts: map[string]*config.AccountConfig{"runtime-only": {}}}),
		Accounts: map[string]*config.AccountConfig{"runtime-only": {}},
		Trading:  trading,
	})
	InitLocalLiveOrderMgrWithRuntimeDeps(deps, nil, false)

	if trading.OrderManager("runtime-only") == nil {
		t.Fatal("runtime-only local live manager was not initialized")
	}
	if trading.OrderManager("legacy-only") != nil {
		t.Fatal("legacy account leaked into runtime local live managers")
	}
	if len(accOdMgrs) != 0 {
		t.Fatalf("legacy local live managers changed: %#v", accOdMgrs)
	}
}
