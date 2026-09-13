package biz

import (
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type nonComparableRuntimeExchange struct {
	banexg.BanExchange
	values []string
}

type panicInfoProjectionExchange struct{ banexg.BanExchange }

func (*panicInfoProjectionExchange) Info() *banexg.ExgInfo { panic("projection identity") }

func completeTraderDepsForTest(overrides RuntimeDeps) RuntimeDeps {
	if overrides.Core == nil {
		overrides.Core = &core.State{}
	}
	if overrides.Clock == nil {
		overrides.Clock = btime.NewClockState(false, nil)
	}
	if overrides.Market == nil {
		overrides.Market = com.NewMarketState("test")
	}
	if overrides.Batch == nil {
		overrides.Batch = strat.NewBatchState()
	}
	if overrides.Strategies == nil {
		overrides.Strategies = strat.NewState()
	}
	if overrides.Orders == nil {
		overrides.Orders = ormo.NewOrderState()
	}
	if overrides.Trading == nil {
		overrides.Trading = NewTradingState()
	}
	if overrides.Config == nil {
		overrides.Config = config.NewSnapshot(&config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}})
	}
	if overrides.Accounts == nil {
		overrides.Accounts = config.CloneAccountConfigsForRuntime(overrides.Config.View().Accounts)
	}
	if len(overrides.Accounts) == 0 {
		overrides.Accounts = map[string]*config.AccountConfig{"default": {}}
	}
	if overrides.AccountsMu == nil {
		overrides.AccountsMu = &sync.RWMutex{}
	}
	if overrides.Symbols == nil {
		overrides.Symbols = orm.NewSymbolState()
	}
	if overrides.DefaultAccount == "" {
		overrides.DefaultAccount = "default"
	}
	if overrides.Exchange == nil {
		overrides.Exchange = &banexg.Exchange{}
	}
	if err := BindRuntimeDeps(overrides); err != nil {
		panic(err)
	}
	return overrides
}

func newCompleteTraderForTest(t *testing.T, overrides RuntimeDeps) Trader {
	t.Helper()
	trader, err := NewTraderWithRuntimeDeps(completeTraderDepsForTest(overrides))
	if err != nil {
		t.Fatal(err)
	}
	return trader
}

type runtimeDepsCallbackStub struct{}

func (runtimeDepsCallbackStub) EnterCallback() bool { return true }
func (runtimeDepsCallbackStub) LeaveCallback()      {}

func TestRuntimeDepsDataProjectionRetainsAllDataDependencies(t *testing.T) {
	wantRuntimeFields := []string{
		"Core", "Clock", "Market", "Batch", "Strategies", "Orders", "Trading", "Config", "Accounts",
		"AccountsMu", "Symbols", "Catalog", "Callbacks", "Storage", "Exchange", "Scheduler", "Notifications",
		"Dump", "DefaultAccount",
	}
	if got := structFieldNames(reflect.TypeOf(RuntimeDeps{})); !reflect.DeepEqual(got, wantRuntimeFields) {
		t.Fatalf("RuntimeDeps fields = %v, want %v; classify every added dependency in each narrow projection", got, wantRuntimeFields)
	}
	wantDataFields := []string{
		"Core", "Clock", "Config", "Market", "Symbols", "Storage", "Strategies", "Catalog", "Dump",
		"Callbacks", "Exchange", "IdentityErr", "ExchangeName", "MarketType",
	}
	if got := structFieldNames(reflect.TypeOf(data.RuntimeDeps{})); !reflect.DeepEqual(got, wantDataFields) {
		t.Fatalf("data.RuntimeDeps fields = %v, want %v; update RuntimeDeps.DataDeps for every added field", got, wantDataFields)
	}
	coreState := &core.State{ExgName: "runtime-exchange", Market: banexg.MarketSpot}
	clock := btime.NewClockState(false, nil)
	snapshot := config.NewSnapshot(&config.Config{})
	market := com.NewMarketState("test")
	symbols := orm.NewSymbolState()
	storage := &orm.Storage{}
	strategies := strat.NewState()
	catalog := data.NewDataSourceCatalog()
	dump := &orm.DumpSink{}
	callbacks := runtimeDepsCallbackStub{}
	exchange := &banexg.Exchange{}
	deps := RuntimeDeps{
		Core: coreState, Clock: clock, Config: snapshot, Market: market, Symbols: symbols,
		Storage: storage, Strategies: strategies, Catalog: catalog, Dump: dump, Callbacks: callbacks,
		Exchange: exchange,
	}
	projected := deps.DataDeps()
	if projected.Core != coreState || projected.Clock != clock || projected.Config != snapshot ||
		projected.Market != market || projected.Symbols != symbols || projected.Storage != storage ||
		projected.Strategies != strategies || projected.Catalog != catalog || projected.Dump != dump ||
		projected.Callbacks != callbacks || projected.Exchange != exchange {
		t.Fatalf("data dependency projection dropped a sentinel: %#v", projected)
	}
	if projected.ExchangeName != coreState.ExgName || projected.MarketType != coreState.Market {
		t.Fatalf("data identity = %q/%q, want %q/%q", projected.ExchangeName, projected.MarketType, coreState.ExgName, coreState.Market)
	}
}

func TestRuntimeDepsDataProjectionPreservesIdentityFailures(t *testing.T) {
	projected := (RuntimeDeps{Exchange: &panicInfoProjectionExchange{}}).DataDeps()
	if projected.IdentityErr == nil || !strings.Contains(projected.IdentityErr.Error(), "projection identity") {
		t.Fatalf("identity error = %v, want adapter panic", projected.IdentityErr)
	}
}

func structFieldNames(typ reflect.Type) []string {
	fields := make([]string, typ.NumField())
	for index := range fields {
		fields[index] = typ.Field(index).Name
	}
	return fields
}

func TestBindRuntimeDepsDoesNotLeaveStrategyHalfBound(t *testing.T) {
	strategies := strat.NewState()
	orders := ormo.NewOrderState()
	first := completeTraderDepsForTest(RuntimeDeps{})
	second := completeTraderDepsForTest(RuntimeDeps{})
	first.Strategies, first.Orders = strategies, orders
	second.Strategies, second.Orders = strategies, orders

	if !orders.BindRuntimeOnce(second.Core, second.Clock, second.Market.Prices, second.Exchange, second.ConfigView()) {
		t.Fatal("failed to prepare order state owned by the second runtime")
	}
	if err := BindRuntimeDeps(first); err == nil || !strings.Contains(err.Error(), "order state") {
		t.Fatalf("first bind error = %v, want foreign order state", err)
	}
	if !strategies.CanBindRuntime(second.Core, second.Clock, second.ConfigView(), second.Symbols, second.Exchange, second.AccountsMu, orders) {
		t.Fatal("failed bind left the shared strategy state claimed")
	}
	if err := BindRuntimeDeps(second); err != nil {
		t.Fatalf("second runtime could not claim untouched strategy state: %v", err)
	}
}

func TestNewTraderWithRuntimeDepsBindsRootOwnedAccounts(t *testing.T) {
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
	trader := newCompleteTraderForTest(t, RuntimeDeps{
		Core:     &core.State{EnvReal: true},
		Accounts: source,
	})
	owned := trader.RuntimeDependencies().AccountConfigs()
	if owned == nil || owned["runtime"] == nil {
		t.Fatalf("runtime accounts = %#v", owned)
	}
	if owned["runtime"] != source["runtime"] {
		t.Fatal("trader did not retain the runtime-root account config")
	}
	if owned["runtime"].StakePctAmt != source["runtime"].StakePctAmt {
		t.Fatalf("runtime StakePctAmt = %v, want %v", owned["runtime"].StakePctAmt, source["runtime"].StakePctAmt)
	}
	owned["runtime"].StakePctAmt = 11
	if source["runtime"].StakePctAmt != 11 {
		t.Fatal("trader did not share root-owned execution state")
	}
}

func TestNewTraderWithRuntimeDepsOwnsAccountsForWalletUpdates(t *testing.T) {
	makeTrader := func(account string, balance float64) (Trader, *BanWallets) {
		trader := newCompleteTraderForTest(t, RuntimeDeps{
			Core:           &core.State{EnvReal: true},
			Config:         config.NewSnapshot(&config.Config{StakePct: 100, Accounts: map[string]*config.AccountConfig{account: {}}}),
			Accounts:       map[string]*config.AccountConfig{account: {}},
			DefaultAccount: account,
		})
		deps := trader.RuntimeDependencies()
		wallet := deps.Trading.Wallet(account)
		wallet.Items["USDT"] = &ItemWallet{Coin: "USDT", Available: balance}
		wallet.bindRuntimeDeps(*deps)
		return trader, wallet
	}

	first, firstWallet := makeTrader("first", 1_000)
	second, secondWallet := makeTrader("second", 2_000)
	firstDeps, secondDeps := first.RuntimeDependencies(), second.RuntimeDependencies()
	if firstDeps.AccountsMu == nil || secondDeps.AccountsMu == nil {
		t.Fatal("explicit runtimes must own an account lock")
	}
	if firstWallet.runtimeAccounts["first"] != firstDeps.Accounts["first"] ||
		secondWallet.runtimeAccounts["second"] != secondDeps.Accounts["second"] {
		t.Fatal("wallet did not bind the trader-owned account state")
	}

	firstWallet.TryUpdateStakePctAmt()
	secondWallet.TryUpdateStakePctAmt()
	if got := firstDeps.Accounts["first"].StakePctAmt; got != 1_000 {
		t.Fatalf("first runtime stake amount = %v, want 1000", got)
	}
	if got := secondDeps.Accounts["second"].StakePctAmt; got != 2_000 {
		t.Fatalf("second runtime stake amount = %v, want 2000", got)
	}
	if secondDeps.Accounts["first"] != nil || firstDeps.Accounts["second"] != nil {
		t.Fatalf("runtime accounts crossed instances: first=%v second=%v", firstDeps.Accounts, secondDeps.Accounts)
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

func TestNewTraderWithRuntimeDepsRejectsNonComparableBoundExchange(t *testing.T) {
	exchange := nonComparableRuntimeExchange{values: []string{"runtime"}}
	deps := RuntimeDeps{
		Core:           &core.State{},
		Clock:          btime.NewClockState(false, nil),
		Market:         com.NewMarketState("test"),
		Batch:          strat.NewBatchState(),
		Strategies:     strat.NewState(),
		Orders:         ormo.NewOrderState(),
		Trading:        NewTradingState(),
		Config:         config.NewSnapshot(&config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}}),
		Accounts:       map[string]*config.AccountConfig{"default": {}},
		AccountsMu:     &sync.RWMutex{},
		Symbols:        orm.NewSymbolState(),
		Exchange:       exchange,
		DefaultAccount: "default",
	}
	if err := BindRuntimeDeps(deps); err != nil {
		t.Fatal(err)
	}
	_, err := NewTraderWithRuntimeDeps(deps)
	if err == nil || (!strings.Contains(err.Error(), "strategy state") && !strings.Contains(err.Error(), "order state")) {
		t.Fatalf("non-comparable exchange bind error = %v", err)
	}
}

func TestBindRuntimeDepsClaimsSharedStateOnce(t *testing.T) {
	strategies := strat.NewState()
	orders := ormo.NewOrderState()
	makeDeps := func(exchange banexg.BanExchange) RuntimeDeps {
		return RuntimeDeps{
			Core:           &core.State{},
			Clock:          btime.NewClockState(false, nil),
			Market:         com.NewMarketState("test"),
			Batch:          strat.NewBatchState(),
			Strategies:     strategies,
			Orders:         orders,
			Trading:        NewTradingState(),
			Config:         config.NewSnapshot(&config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}}),
			Accounts:       map[string]*config.AccountConfig{"default": {}},
			AccountsMu:     &sync.RWMutex{},
			Symbols:        orm.NewSymbolState(),
			Exchange:       exchange,
			DefaultAccount: "default",
		}
	}
	first := makeDeps(&banexg.Exchange{})
	second := makeDeps(&banexg.Exchange{})
	results := make(chan *errs.Error, 2)
	go func() { results <- BindRuntimeDeps(first) }()
	go func() { results <- BindRuntimeDeps(second) }()
	successes := 0
	for range 2 {
		if err := <-results; err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("shared state bind successes = %d, want 1", successes)
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
