package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestRuntimeDepsNormalizeNonRealAccountsForOrderDispatch(t *testing.T) {
	deps := RuntimeDeps{
		Core: &core.State{EnvReal: false},
		Config: config.NewSnapshot(&config.Config{
			Accounts: map[string]*config.AccountConfig{
				"user1": {},
			},
		}),
		DefaultAccount: "default",
	}
	trader := NewTraderWithRuntimeDeps(deps)
	bound := trader.RuntimeDependencies()
	if bound == nil {
		t.Fatal("runtime dependencies were not bound")
	}
	accounts := bound.AccountConfigs()
	if len(accounts) != 1 || accounts["default"] == nil {
		t.Fatalf("normalized accounts = %#v, want only default", accounts)
	}
	if accounts["user1"] != nil {
		t.Fatal("non-real runtime retained the raw live account key")
	}
	if _, ok := bound.Strategies.AccJobs["default"]; !ok {
		t.Fatal("runtime strategy jobs were not seeded for the normalized account")
	}
	if bound.Strategies.InfoJobs("default") == nil {
		t.Fatal("runtime strategy info jobs were not seeded for the normalized account")
	}

	InitLocalOrderMgrWithRuntimeDeps(*bound, nil, false)
	if bound.Trading.OrderManager("default") == nil {
		t.Fatal("normalized default account did not receive an order manager")
	}
	if bound.Trading.OrderManager("user1") != nil {
		t.Fatal("order manager was initialized under the raw account key")
	}
}
