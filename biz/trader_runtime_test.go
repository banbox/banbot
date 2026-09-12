package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func TestNewTraderWithRuntimeDepsCopiesAccounts(t *testing.T) {
	source := map[string]*config.AccountConfig{
		"runtime": {
			StakeRate:   1.5,
			StakePctAmt: 73,
			RPCChannels: []map[string]interface{}{{
				"options": map[string]interface{}{"format": "compact"},
			}},
			APIServer: &config.AccPwdRole{Pwd: "password", Role: "admin"},
			Exchanges: map[string]*config.ExgApiSecrets{
				"runtime-exchange": {Prod: &config.ApiSecretConfig{APIKey: "key"}},
			},
		},
	}
	trader := NewTraderWithRuntimeDeps(RuntimeDeps{
		Core:     &core.State{EnvReal: true},
		Accounts: source,
	})
	owned := trader.RuntimeDependencies().AccountConfigs()
	if owned == nil || owned["runtime"] == nil {
		t.Fatalf("owned runtime accounts = %#v", owned)
	}
	if owned["runtime"] == source["runtime"] {
		t.Fatal("trader retained caller-owned account config")
	}
	if owned["runtime"].StakePctAmt != source["runtime"].StakePctAmt {
		t.Fatalf("runtime StakePctAmt = %v, want %v", owned["runtime"].StakePctAmt, source["runtime"].StakePctAmt)
	}
	owned["owned-only"] = &config.AccountConfig{}
	if _, ok := source["owned-only"]; ok {
		t.Fatal("trader retained caller-owned account map")
	}
	delete(owned, "owned-only")
	owned["runtime"].RPCChannels[0]["options"].(map[string]interface{})["format"] = "verbose"
	owned["runtime"].APIServer.Pwd = "changed"
	owned["runtime"].Exchanges["runtime-exchange"].Prod.APIKey = "changed"
	if source["runtime"].RPCChannels[0]["options"].(map[string]interface{})["format"] != "compact" ||
		source["runtime"].APIServer.Pwd != "password" ||
		source["runtime"].Exchanges["runtime-exchange"].Prod.APIKey != "key" {
		t.Fatal("mutating trader accounts changed caller-owned configuration")
	}
}

func TestRefreshPairsWithSymbolStateRejectsLegacyFallback(t *testing.T) {
	symbols := orm.NewSymbolStateWithIdentity("runtime", "spot")
	oldPairs, oldPairsMap := core.LegacyPairStateSnapshot()
	t.Cleanup(func() { core.ReplaceLegacyPairState(oldPairs, oldPairsMap) })
	core.SetLegacyPairs([]string{"legacy"}, nil)

	_, _, err := RefreshPairsWithSymbolState(symbols, false, 100, nil)
	if err == nil || err.Code != core.ErrBadConfig {
		t.Fatalf("symbol-only pair refresh error = %v, want ErrBadConfig", err)
	}
	pairs, pairsMap := core.LegacyPairStateSnapshot()
	if len(pairs) != 1 || pairs[0] != "legacy" || !pairsMap["legacy"] {
		t.Fatalf("legacy pair state changed on rejected refresh: %v/%v", pairs, pairsMap)
	}
}

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

func TestNewTraderWithRuntimeDepsRejectsLegacyStatePointers(t *testing.T) {
	legacyStrategies := strat.LegacyState()
	legacyOrders := ormo.LegacyState()
	trader := NewTraderWithRuntimeDeps(RuntimeDeps{
		Core:       &core.State{},
		Strategies: legacyStrategies,
		Orders:     legacyOrders,
	})
	deps := trader.RuntimeDependencies()
	if deps == nil {
		t.Fatal("runtime dependencies were not bound")
	}
	if deps.Strategies == nil || deps.Strategies == legacyStrategies {
		t.Fatal("explicit trader retained legacy strategy state")
	}
	if deps.Orders == nil || deps.Orders == legacyOrders {
		t.Fatal("explicit trader retained legacy order state")
	}
}

func TestTraderExplicitRuntimeDoesNotReadLegacyStrategyJobs(t *testing.T) {
	oldAccounts, oldInfoJobs := config.Accounts, strat.AccInfoJobs
	config.Accounts = map[string]*config.AccountConfig{config.DefAcc: {}}
	called := 0
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{
		config.DefAcc: {
			strat.DataSubKey("macro", 1, "1d"): {
				"legacy": {
					Strat: &strat.TradeStrat{OnData: func(*strat.StratJob, strat.DataEvent) { called++ }},
				},
			},
		},
	}
	t.Cleanup(func() {
		config.Accounts, strat.AccInfoJobs = oldAccounts, oldInfoJobs
	})

	trader := Trader{runtime: &RuntimeDeps{Strategies: nil}}
	err := trader.feedDataOnlySeries(
		&orm.DataSeries{Source: "macro", Sid: 1, TimeFrame: "1d"},
		&orm.ExSymbol{ID: 1, Symbol: "BTC/USDT"},
	)
	if err == nil {
		t.Fatal("explicit trader with missing strategy state was not rejected")
	}
	if called != 0 {
		t.Fatalf("explicit trader dispatched %d legacy callbacks", called)
	}
}
