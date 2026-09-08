package biz

import (
	"reflect"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
)

func TestLiveOrderMgrRuntimeDepsUseOwnedPairAndOrderState(t *testing.T) {
	oldPairs := core.PairsMap
	oldName := config.Name
	core.PairsMap = map[string]bool{"legacy/USDT": true}
	config.Name = "legacy-bot"
	t.Cleanup(func() {
		core.PairsMap = oldPairs
		config.Name = oldName
	})

	state := &core.State{PairsMap: map[string]bool{"runtime/USDT": true}}
	deps := RuntimeDeps{
		Core:     state,
		Config:   config.NewSnapshot(&config.Config{Name: "runtime-bot"}),
		Exchange: &issue138Exchange{},
	}
	mgr := NewLiveOrderMgrWithRuntimeDeps(deps, "runtime-account", nil)

	if !mgr.pairEnabled("runtime/USDT") || mgr.pairEnabled("legacy/USDT") {
		t.Fatalf("pair admission escaped runtime state: runtime=%v legacy=%v",
			mgr.pairEnabled("runtime/USDT"), mgr.pairEnabled("legacy/USDT"))
	}
	if got := mgr.getClientOrderID("runtime-bot_42_7_"); got != 42 {
		t.Fatalf("runtime order namespace parsed as %d, want 42", got)
	}
	if got := mgr.getClientOrderID("legacy-bot_42_7_"); got != 0 {
		t.Fatalf("legacy order namespace parsed as %d, want 0", got)
	}
	if got := mgr.orderEventOrderID(&banexg.MyTrade{AlgoId: "123"}); got != "algo:123" {
		t.Fatalf("adapter order relation = %q, want algo:123", got)
	}
}

func TestLiveOrderMgrRuntimeDepsDoNotUseGlobalExchangeCapability(t *testing.T) {
	oldExchange := exg.Default
	oldName := config.Name
	exg.Default = &issue138Exchange{}
	config.Name = "legacy-bot"
	t.Cleanup(func() {
		exg.Default = oldExchange
		config.Name = oldName
	})

	// A runtime with no exchange capability must remain conservative even when
	// the process-wide legacy exchange happens to expose one.
	mgr := NewLiveOrderMgrWithRuntimeDeps(RuntimeDeps{
		Core:   &core.State{PairsMap: map[string]bool{"runtime/USDT": true}},
		Config: config.NewSnapshot(&config.Config{Name: "runtime-bot"}),
	}, "runtime-account", nil)
	if got := mgr.getClientOrderID("legacy-bot_42_7_"); got != 0 {
		t.Fatalf("runtime manager used legacy exchange capability, got %d", got)
	}
}

func TestInitLiveOrderMgrWithRuntimeDepsKeepsLegacyManagersUntouched(t *testing.T) {
	oldAccounts := config.Accounts
	oldLiveManagers := accLiveOdMgrs
	oldOrderManagers := accOdMgrs
	oldListener := ormo.OdEditListener
	t.Cleanup(func() {
		config.Accounts = oldAccounts
		accLiveOdMgrs = oldLiveManagers
		accOdMgrs = oldOrderManagers
		ormo.OdEditListener = oldListener
	})
	legacyManager := &LiveOrderMgr{}
	accLiveOdMgrs = map[string]*LiveOrderMgr{"legacy": legacyManager}
	accOdMgrs = map[string]IOrderMgr{"legacy": legacyManager}
	config.Accounts = map[string]*config.AccountConfig{"legacy": {}}

	trading := NewTradingState()
	orders := ormo.NewOrderState()
	deps := RuntimeDeps{
		Core:     &core.State{LiveMode: true, EnvReal: true, Market: banexg.MarketSpot},
		Config:   config.NewSnapshot(&config.Config{Accounts: map[string]*config.AccountConfig{"runtime": {}}}),
		Orders:   orders,
		Trading:  trading,
		Exchange: &issue138Exchange{},
	}
	InitLiveOrderMgrWithRuntimeDeps(deps, nil)

	if trading.LiveManager("runtime") == nil {
		t.Fatal("runtime manager was not initialized from runtime accounts")
	}
	if trading.LiveManager("legacy") != nil {
		t.Fatal("runtime manager initialization imported a legacy account")
	}
	if accLiveOdMgrs["legacy"] != legacyManager || len(accLiveOdMgrs) != 1 {
		t.Fatalf("legacy live manager registry changed: %#v", accLiveOdMgrs)
	}
	if len(accOdMgrs) != 1 || accOdMgrs["legacy"] != legacyManager {
		t.Fatalf("legacy order manager registry changed: %#v", accOdMgrs)
	}
	if got := orders.GetEditListener(); got == nil {
		t.Fatal("runtime order state did not receive an edit listener")
	}
	if (oldListener == nil) != (ormo.OdEditListener == nil) ||
		(oldListener != nil && reflect.ValueOf(ormo.OdEditListener).Pointer() != reflect.ValueOf(oldListener).Pointer()) {
		t.Fatal("runtime manager initialization changed the legacy edit listener")
	}
}
