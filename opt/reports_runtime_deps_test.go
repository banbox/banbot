package opt

import (
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
)

func TestReportDepsProjectionCompleteness(t *testing.T) {
	wantFields := []string{
		"Config", "Core", "Clock", "Market", "Symbols", "Storage", "Strategies",
		"Orders", "Trading", "Exchange", "Accounts", "AccountsMu", "DefaultAccount", "legacy",
	}
	typ := reflect.TypeOf(ReportDeps{})
	gotFields := make([]string, typ.NumField())
	for index := range gotFields {
		gotFields[index] = typ.Field(index).Name
	}
	if !reflect.DeepEqual(gotFields, wantFields) {
		t.Fatalf("ReportDeps fields = %v, want %v; update the projection manifest for every added field", gotFields, wantFields)
	}

	source := completeBacktestDepsForTest(biz.RuntimeDeps{Storage: orm.NewStorage(nil, false, "projection-test")})
	projected := NewReportDeps(source)
	if projected.Config != source.Config || projected.Core != source.Core || projected.Clock != source.Clock ||
		projected.Market != source.Market || projected.Symbols != source.Symbols || projected.Storage != source.Storage ||
		projected.Strategies != source.Strategies || projected.Orders != source.Orders || projected.Trading != source.Trading ||
		projected.Exchange != source.Exchange || projected.AccountsMu != source.AccountsMu ||
		projected.DefaultAccount != source.DefaultAccount ||
		reflect.ValueOf(projected.Accounts).Pointer() != reflect.ValueOf(source.Accounts).Pointer() {
		t.Fatal("NewReportDeps did not preserve every report-owned runtime dependency")
	}

	roundTrip := projected.bizRuntimeDeps()
	if roundTrip.Config != source.Config || roundTrip.Core != source.Core || roundTrip.Clock != source.Clock ||
		roundTrip.Market != source.Market || roundTrip.Symbols != source.Symbols || roundTrip.Storage != source.Storage ||
		roundTrip.Strategies != source.Strategies || roundTrip.Orders != source.Orders || roundTrip.Trading != source.Trading ||
		roundTrip.Exchange != source.Exchange || roundTrip.AccountsMu != source.AccountsMu ||
		roundTrip.DefaultAccount != source.DefaultAccount ||
		reflect.ValueOf(roundTrip.Accounts).Pointer() != reflect.ValueOf(source.Accounts).Pointer() {
		t.Fatal("ReportDeps round trip lost a report-owned runtime dependency")
	}
}

func TestReportDepsDoesNotReadLegacyOrderHistory(t *testing.T) {
	previous := ormo.HistODs
	ormo.HistODs = []*ormo.InOutOrder{{IOrder: &ormo.IOrder{ID: 99}}}
	t.Cleanup(func() { ormo.HistODs = previous })

	result := &BTResult{reportDeps: &ReportDeps{}}
	if got := result.historyOrders(); got != nil {
		t.Fatalf("explicit report history = %#v, want nil when order state is missing", got)
	}
}

func TestLegacyReportDepsBindCompatibilityStateAtFacadeBoundary(t *testing.T) {
	previousOrders := ormo.HistODs
	ormo.HistODs = []*ormo.InOutOrder{{IOrder: &ormo.IOrder{ID: 99, Symbol: "BTC/USDT", Profit: 99}}}
	t.Cleanup(func() { ormo.HistODs = previousOrders })

	deps := legacyReplayReportDeps()
	if !deps.legacy || deps.Core == nil || deps.Clock == nil || deps.Market == nil || deps.Market.Prices == nil ||
		deps.Orders == nil || deps.Trading == nil || deps.AccountsMu == nil {
		t.Fatal("legacy report facade did not build owned replay dependencies")
	}
	if ormo.IsLegacyState(deps.Orders) {
		t.Fatal("legacy report facade exposed package registry state")
	}
	if _, err := calcBtResultWithDeps(nil, nil, "", deps); err != nil {
		t.Fatalf("empty legacy report failed dependency validation: %v", err)
	}
	wallet := biz.InitFakeWalletsWithRuntimeDeps(deps.bizRuntimeDeps())
	if wallet == nil || wallet != deps.Trading.Wallet(deps.account()) {
		t.Fatal("legacy report replay wallet is not owned by report dependencies")
	}
	order := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1, Symbol: "BTC/USDT", Profit: 2}}
	deps.Orders.AddHistoricalOrder(order)
	deps.Clock.SetTimeMS(1)
	deps.Market.Prices.SetPricesAt(1, map[string]float64{"USDT": 1}, "")
	result := &BTResult{reportDeps: deps}
	orders := result.historyOrders()
	if len(orders) != 1 || orders[0] != order {
		t.Fatalf("legacy replay history read global orders: %#v", orders)
	}
	if profit := result.doneProfits(0); profit != 2 {
		t.Fatalf("legacy replay done profit = %v, want 2", profit)
	}
}

func TestLegacyReplayStakeUpdateDoesNotMutateGlobalAccount(t *testing.T) {
	previousData, previousAccounts, previousDefault := config.Data, config.Accounts, config.DefAcc
	config.Data = config.Config{
		StakePct:      100,
		WalletAmounts: map[string]float64{"USDT": 1_000},
	}
	config.DefAcc = "legacy-report"
	config.Accounts = map[string]*config.AccountConfig{
		config.DefAcc: {StakePctAmt: 50},
	}
	t.Cleanup(func() {
		config.Data, config.Accounts, config.DefAcc = previousData, previousAccounts, previousDefault
	})

	deps := legacyReplayReportDeps()
	if deps.Accounts[config.DefAcc] == config.Accounts[config.DefAcc] {
		t.Fatal("legacy replay retained the global account pointer")
	}
	deps.Accounts[config.DefAcc].StakePctAmt = 0
	biz.InitFakeWalletsWithRuntimeDeps(deps.bizRuntimeDeps())
	if got := deps.Accounts[config.DefAcc].StakePctAmt; got != 1_000 {
		t.Fatalf("replay stake amount = %v, want 1000", got)
	}
	if got := config.Accounts[config.DefAcc].StakePctAmt; got != 50 {
		t.Fatalf("global stake amount = %v after replay update, want 50", got)
	}
}

func TestLegacyReplayCapturesDefaultAccountForPrivateWallet(t *testing.T) {
	previousData, previousAccounts, previousDefault := config.Data, config.Accounts, config.DefAcc
	config.Data = config.Config{WalletAmounts: map[string]float64{"USDT": 1_000}}
	config.Accounts = nil
	config.DefAcc = ""
	t.Cleanup(func() {
		config.Data, config.Accounts, config.DefAcc = previousData, previousAccounts, previousDefault
	})

	deps := legacyReplayReportDeps()
	if deps.DefaultAccount != "default" || deps.Accounts[deps.DefaultAccount] == nil {
		t.Fatalf("replay default account = %q, accounts = %#v", deps.DefaultAccount, deps.Accounts)
	}
	wallet := biz.InitFakeWalletsWithRuntimeDeps(deps.bizRuntimeDeps())
	config.DefAcc = "changed-after-construction"
	if got := deps.wallet(); got != wallet {
		t.Fatal("legacy replay report wallet did not use its captured default account")
	}
}

func TestReportDepsRequiresExplicitStorageForSeriesQueries(t *testing.T) {
	deps := &ReportDeps{Symbols: orm.NewSymbolStateWithIdentity("binance", "spot")}
	if _, _, err := deps.queries(); err == nil || !strings.Contains(err.Error(), "storage") {
		t.Fatalf("missing explicit storage error = %v, want storage error", err)
	}
	if _, err := calcPairStats(nil, 0, 1, "1m", deps); err == nil || !strings.Contains(err.Error(), "storage") {
		t.Fatalf("missing explicit storage pair-stats error = %v, want storage error", err)
	}
}

func TestReportDepsRejectsUnboundSymbolsWithExplicitStorage(t *testing.T) {
	storage := orm.NewStorage(nil, false, "runtime-report")
	deps := &ReportDeps{
		Symbols: orm.NewSymbolStateWithIdentity("binance", "spot"),
		Storage: storage,
	}
	if err := deps.validateSeries(); err == nil || !strings.Contains(err.Error(), "share an owner") {
		t.Fatalf("unbound symbol state validation error = %v, want ownership error", err)
	}
}

func TestRuntimeReportRejectsMissingOwnedDependencies(t *testing.T) {
	accounts := map[string]*config.AccountConfig{"default": {}}
	base := &ReportDeps{
		Config:         config.NewSnapshotWithDirs(&config.Config{}, t.TempDir(), ""),
		Market:         com.NewMarketState("binance"),
		Symbols:        orm.NewSymbolStateWithIdentity("binance", "spot"),
		Storage:        orm.NewStorage(nil, false, "runtime-report"),
		Trading:        biz.NewTradingState(),
		Orders:         ormo.NewOrderState(),
		Accounts:       accounts,
		AccountsMu:     &sync.RWMutex{},
		DefaultAccount: "default",
	}
	cases := []struct {
		name   string
		mutate func(*ReportDeps)
		want   string
	}{
		{name: "orders", mutate: func(deps *ReportDeps) { deps.Orders = nil }, want: "orders"},
		{name: "storage", mutate: func(deps *ReportDeps) { deps.Storage = nil }, want: "storage"},
		{name: "market", mutate: func(deps *ReportDeps) { deps.Market = nil }, want: "market prices"},
		{name: "accounts", mutate: func(deps *ReportDeps) { deps.Accounts = nil }, want: "accounts"},
		{name: "accounts lock", mutate: func(deps *ReportDeps) { deps.AccountsMu = nil }, want: "accounts lock"},
		{name: "default account", mutate: func(deps *ReportDeps) { deps.DefaultAccount = "" }, want: "default account"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			deps := *base
			testCase.mutate(&deps)
			if _, err := calcBtResultWithDeps(nil, nil, "", &deps); err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("missing %s report dependency error = %v", testCase.name, err)
			}
		})
	}
}

func TestReportDepsPreservesRuntimeAccountStateForStakePct(t *testing.T) {
	accounts := map[string]*config.AccountConfig{"report": {}}
	accountsMu := &sync.RWMutex{}
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(1_000)
	market := com.NewMarketState("report")
	market.Prices.SetPricesAt(clock.TimeMS(), map[string]float64{"USDT": 1}, "")
	runtimeDeps := biz.RuntimeDeps{
		Core:           &core.State{},
		Clock:          clock,
		Market:         market,
		Config:         config.NewSnapshot(&config.Config{StakePct: 100, WalletAmounts: map[string]float64{"USDT": 1_000}}),
		Trading:        biz.NewTradingState(),
		Accounts:       accounts,
		AccountsMu:     accountsMu,
		DefaultAccount: "report",
	}
	projected := NewReportDeps(runtimeDeps).bizRuntimeDeps()
	if projected.Accounts["report"] != accounts["report"] || projected.AccountsMu != accountsMu {
		t.Fatal("report projection lost runtime account ownership")
	}
	biz.InitFakeWalletsWithRuntimeDeps(projected)
	if got := accounts["report"].StakePctAmt; got != 1_000 {
		t.Fatalf("report stake amount = %v, want 1000", got)
	}
}

func TestBtFactorsWithRuntimeDepsRejectsMissingOwnedDependencies(t *testing.T) {
	if err := BtFactorsWithRuntimeDeps(nil, biz.RuntimeDeps{}); err == nil || !strings.Contains(err.Error(), "config") {
		t.Fatalf("missing factor runtime dependency error = %v, want config", err)
	}
}

func TestFactorRegistrySnapshotIsDetached(t *testing.T) {
	const name = "runtime-factor-registry-test"
	RegisterFactor(name, func(FacArgs) ([]string, error) { return nil, nil })
	t.Cleanup(func() { UnregisterFactor(name) })
	if factor, ok := GetFactor(name); !ok || factor == nil {
		t.Fatal("registered factor was not found")
	}
	snapshot := SnapshotFactors()
	delete(snapshot, name)
	if _, ok := GetFactor(name); !ok {
		t.Fatal("factor snapshot mutation changed registry")
	}
}

func TestReportDatesUseSnapshotLocation(t *testing.T) {
	previous := btime.LocShow
	btime.LocShow = time.FixedZone("legacy", -8*60*60)
	t.Cleanup(func() { btime.LocShow = previous })

	snapshot := config.NewSnapshotWithDirs(&config.Config{
		Exchange: &config.ExchangeConfig{Name: "china"},
	}, t.TempDir(), "")
	deps := &ReportDeps{Config: snapshot}
	stamp := int64(1704069000000) // 2024-01-01 00:30:00 UTC
	want := btime.MSToTime(stamp).In(snapshot.Location()).Format("2006-01-02 15:04:05")
	if got := deps.dateStrLoc(stamp, ""); got != want {
		t.Fatalf("runtime report date = %q, want snapshot location %q", got, want)
	}
	if got := deps.dateStrLoc(stamp, ""); got == btime.ToDateStrLoc(stamp, "") {
		t.Fatalf("runtime report date unexpectedly used legacy location: %q", got)
	}
}
