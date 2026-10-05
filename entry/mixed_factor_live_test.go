package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

func TestMixedLiveAccountConfigScopesPoliciesAndAccounts(t *testing.T) {
	cfg := &config.Config{Accounts: map[string]*config.AccountConfig{"a": {}, "b": {}}, RunPolicy: []*config.RunPolicyConfig{{Name: "a"}, {Name: "b"}}}
	got := mixedLiveAccountConfig(cfg, "a", []*config.RunPolicyConfig{{Name: "a"}})
	if len(got.Accounts) != 1 || got.Accounts["a"] == nil || len(got.RunPolicy) != 1 {
		t.Fatal("unscoped runtime")
	}
	if len(cfg.Accounts) != 2 || len(cfg.RunPolicy) != 2 {
		t.Fatal("mutated source")
	}
}

func TestMixedLiveBuildsRealJobsOnSharedAccount(t *testing.T) {
	dir := t.TempDir()
	body := "config_version: 2\ntime_start: '20240101'\ntime_end: '20240102'\nexchange: {name: mixedfixture}\nmarket_type: linear\nstake_currency: [USD]\npairs: ['ASSET/USD:USD']\naccounts: {default: {}}\nrun_policy:\n  - name: mixed-live-ts\n    capital_weight: 0.5\n    run_timeframes: [1m]\n"
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: dir, ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := spec.RuntimeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	callbacks := 0
	strat.RegisterStrategy("mixed-live-ts", func(*config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{OnBar: func(job *strat.StratJob) {
			callbacks++
			if err := job.OpenOrder(&strat.EnterReq{Tag: "mixed-live", Amount: 1}); err != nil {
				t.Error(err)
			}
		}}
	})
	t.Cleanup(func() { strat.UnregisterStrategy("mixed-live-ts") })
	ex := &mixedReplayExchange{}
	unit, unitErr := mixedReplayInstrument(ex, "ASSET/USD:USD", "USD")
	if unitErr != nil {
		t.Fatal(unitErr)
	}
	c := runner.Config{AccountID: "default", AccountInitialNAV: 1000, InitialNAV: 500, ExpiryMS: 60000, Manifest: research.ManifestSpec{Currency: "USD"}, Execution: runner.ExecutionConfig{Instruments: map[int32]execution.Instrument{1: unit}, MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(2000), StrategyGrossLimit: decimal.NewFromInt(1000)}}
	symbol := &orm.ExSymbol{ID: 1, Symbol: unit.ID, Exchange: "mixedfixture", Market: "linear"}
	binding := FactorLiveBinding{Symbols: map[int32]*orm.ExSymbol{1: symbol}}
	session := &explicitEntrySession{runSpec: spec, exchange: ex}
	capital, prepareErr := session.prepareMixedLiveBinding(snapshot, c, &binding)
	if prepareErr != nil {
		t.Fatal(prepareErr)
	}
	if !capital["mixed-live-ts"].Equal(decimal.NewFromInt(500)) || binding.Legacy == nil {
		t.Fatal("TS allocation missing", capital)
	}
	custom := binding
	custom.Legacy = &FactorLegacyLiveBinding{Bridge: binding.Legacy.Bridge}
	if _, err := session.prepareMixedLiveBinding(snapshot, c, &custom); err == nil {
		t.Fatal("custom live binding silently omitted configured TS jobs")
	}
	binding.Legacy.Bridge.Quote = func(_ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000}, nil
	}
	p := runtimepkg.NewProcess()
	t.Cleanup(p.Close)
	adapter, adapterErr := runner.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if adapterErr != nil {
		t.Fatal(adapterErr)
	}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USD"}
	rt, rtErr := p.NewRuntime(runtimepkg.Options{Context: context.Background(), Config: snapshot.View(), Mode: core.RunModeBackTest, Exchange: ex, ExchangeName: "mixedfixture", Market: "linear", NetDisable: true, AccountOwnerKey: &key, SharedExecution: &biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "execution.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: adapter, AuthoritativeSnapshot: true}, SharedOrderBridge: binding.Legacy.Bridge})
	if rtErr != nil {
		t.Fatal(rtErr)
	}
	t.Cleanup(func() { rt.Close(); rt.Join() })
	for _, event := range []execution.CashEvent{
		{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(1000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(1000)}}, AtMS: 1},
		{ID: "allocate", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-1000)}, {Strategy: "mixed-live-ts", Amount: decimal.NewFromInt(500)}, {Strategy: "cs", Amount: decimal.NewFromInt(500)}}, AtMS: 1},
	} {
		if err := rt.SharedExecution().CashEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt.SharedExecution().Reconcile("test-bootstrap", 1); err != nil {
		t.Fatal(err)
	}
	if cacheErr := rt.Symbols.CacheExSymbolChecked(symbol); cacheErr != nil {
		t.Fatal(cacheErr)
	}
	if loadErr := loadMixedLiveJobs(rt, &binding); loadErr != nil {
		t.Fatal(loadErr)
	}
	if len(binding.Legacy.Jobs) != 1 || len(rt.FactorLegacySubscriptions()) == 0 {
		t.Fatal("real TS jobs not bound")
	}
	if _, ok := biz.GetOdMgrWithState(rt.Trading, "default").(*biz.SharedOrderMgr); !ok {
		t.Fatal("TS uses independent ledger")
	}
	trader, traderErr := biz.NewTraderWithRuntimeDeps(rt.BizDeps())
	if traderErr != nil {
		t.Fatal(traderErr)
	}
	rt.Clock.SetTimeMS(120000)
	if feedErr := trader.FeedDataSeries(&orm.DataSeries{Sid: 1, Source: orm.SeriesSourceKline, TimeFrame: "1m", TimeMS: 60000, EndMS: 120000, Closed: true, Values: map[string]any{"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0, "integer": int64(9), "nullable": nil}}); feedErr != nil {
		t.Fatal(feedErr)
	}
	if callbacks != 1 {
		t.Fatalf("TS callback did not execute: %d", callbacks)
	}
	orders, lock := rt.Orders.GetOpenODs("default")
	lock.Lock()
	defer lock.Unlock()
	if len(orders) != 1 {
		t.Fatalf("TS order missing from shared account: %+v", orders)
	}
	for _, order := range orders {
		if order.Strategy != "mixed-live-ts" || order.Enter.Filled != 1 {
			t.Fatalf("TS execution attribution/fill: %+v", order)
		}
	}
}

func TestMixedLiveUnknownTSFailsBeforeSessionIO(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	factorSpec, err := config.LoadRunSpec(&config.CmdArgs{Configs: []string{path}, NoDefault: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	configs, buildErr := buildFactorConfigs(factorSpec, runner.Events)
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	c := configs[0]
	c.Mode = runner.Trade
	c.Chunks = nil
	c.Execution = runner.ExecutionConfig{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Instruments: map[int32]execution.Instrument{}, MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(10000), MaxVirtualGross: decimal.NewFromInt(20000), StrategyGrossLimit: decimal.NewFromInt(10000)}
	for sid, symbol := range c.Snapshot.SIDMap {
		c.Execution.Instruments[sid] = execution.Instrument{ID: symbol, Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.01"), PriceTick: decimal.RequireFromString("0.01"), ContractSize: decimal.NewFromInt(1), MoneyScale: 8}
	}
	body := "config_version: 2\ntime_start: '20240101'\ntime_end: '20240102'\nexchange: {name: mixedfixture}\nmarket_type: linear\nstake_currency: [USD]\naccounts: {default: {}}\nrun_policy:\n  - name: definitely-unregistered-mixed-live-ts\n    run_timeframes: [1m]\n"
	spec, loadErr := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: dir, ConfigData: body}, false)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	runErr := runFactorLiveSpecWithArgs(context.Background(), &config.CmdArgs{}, spec, []runner.Config{c}, "", io.Discard, nil)
	if runErr == nil || !strings.Contains(runErr.Error(), "TS strategy definitely-unregistered-mixed-live-ts is not registered") {
		t.Fatalf("expected resource-free TS validation, got %v", runErr)
	}
	if _, statErr := os.Stat(c.Execution.StorePath); !os.IsNotExist(statErr) {
		t.Fatalf("ledger created before TS validation: %v", statErr)
	}
}

type automaticMixedLiveExchange struct{ legacyEntryExchange }

func (*automaticMixedLiveExchange) Info() *banexg.ExgInfo { return (&mixedReplayExchange{}).Info() }
func (*automaticMixedLiveExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	return (&mixedReplayExchange{}).GetMarket(symbol)
}
func (*automaticMixedLiveExchange) CheckSymbols(symbols ...string) ([]string, []string) {
	return symbols, nil
}
func (*automaticMixedLiveExchange) GetLeverage(string, float64, string) (float64, float64) {
	return 1, 1
}

func TestMixedLiveAutomaticBindingRunsBothEnginesOutsideFactorUniverse(t *testing.T) {
	testMixedLiveAutomaticBinding(t, false)
}

func TestMixedLiveAutomaticBindingUsesFactoryTimeframes(t *testing.T) {
	testMixedLiveAutomaticBinding(t, true)
}

func testMixedLiveAutomaticBinding(t *testing.T, factoryFrames bool) {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	socketJoined := make(chan struct{})
	go func() {
		defer close(socketJoined)
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			_, _ = io.Copy(io.Discard, conn)
		}
	}()
	name := fmt.Sprintf("automatic_live_%d", time.Now().UnixNano())
	body := "config_version: 2\ntime_start: '20240101'\ntime_end: '20240102'\nexchange: {name: mixedfixture}\nmarket_type: linear\nstake_currency: [USD]\npairs: ['ASSET1/USD:USD']\naccounts: {default: {}}\nspider_addr: '" + listener.Addr().String() + "'\nrun_policy:\n  - name: automatic-live-ts\n    capital_weight: 0.5\n    run_timeframes: [1m]\n"
	if factoryFrames {
		body = strings.Replace(body, "    run_timeframes: [1m]\n", "", 1)
	}
	spec, loadErr := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: dir, ConfigData: body}, false)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	snapshot, loadErr := spec.RuntimeSnapshot()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err = json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Mode = runner.Trade
	c.Chunks = nil
	c.AccountID = "default"
	c.StrategyID = "cs"
	c.InitialNAV = 500
	c.AccountInitialNAV = 1000
	c.DecisionInterval = 100
	c.LatencyMS = 1
	c.Manifest.Currency = "USD"
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.Prices = runner.PriceStream{Source: name, TimeFrame: "event", Field: "close"}
	c.Snapshot.SourceVersions = map[string]string{name: "v1"}
	c.Snapshot.Schemas = map[string]string{name: "s1"}
	c.Snapshot.Universe = factor.Universe{Version: "factor-only", Static: true, Tracked: []int32{2, 3}, Investable: []int32{2, 3}, Reference: []int32{2, 3}, Tradable: []int32{2, 3}, Evaluation: []int32{2, 3}}
	c.Snapshot.SIDMap = map[int32]string{2: "ASSET2/USD:USD", 3: "ASSET3/USD:USD"}
	c.Plan, err = factor.New().Add("signal", factor.Field(name, "integer", "event")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"signal"}, Weights: map[string]float64{"signal": 1}}
	c.Execution.StorePath = filepath.Join(dir, "ledger.db")
	c.Execution.SenderLeaseDir = filepath.Join(dir, "leases")
	c.Execution.Instruments = map[int32]execution.Instrument{}
	exchange := &automaticMixedLiveExchange{legacyEntryExchange: legacyEntryExchange{liveEntryExchange: liveEntryExchange{trades: make(chan *banexg.MyTrade)}, positions: map[string]float64{}, orders: map[string]*banexg.Order{}, prices: map[string]float64{"ASSET1/USD:USD": 100, "ASSET2/USD:USD": 100, "ASSET3/USD:USD": 100}, books: map[string]int{}, cash: 1000}}
	for sid, symbol := range c.Snapshot.SIDMap {
		unit, e := mixedReplayInstrument(exchange, symbol, "USD")
		if e != nil {
			t.Fatal(e)
		}
		c.Execution.Instruments[sid] = unit
	}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USD"}
	binding := FactorLiveBinding{Account: key, Transport: &liveEntryTransport{key: key}, BootstrapCapital: true, Symbols: map[int32]*orm.ExSymbol{}, VerifyFunding: func(context.Context, string) (string, error) { return "flat-no-funding-fixture", nil }, Record: func(series *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		return factor.VersionRecord{Series: *series, EventTime: series.EndMS, AvailableAt: series.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
	}}
	for sid := int32(1); sid <= 3; sid++ {
		binding.Symbols[sid] = &orm.ExSymbol{ID: sid, Symbol: fmt.Sprintf("ASSET%d/USD:USD", sid), Exchange: "mixedfixture", Market: "linear"}
	}
	var callbacks atomic.Int32
	strat.RegisterStrategy("automatic-live-ts", func(*config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{RunTimeFrames: []string{"1m"}, WarmupNum: 1, OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: name, TimeFrame: "event", ExSymbol: job.Symbol, Fields: []string{"close", "integer", "nullable"}}}
		}, OnData: func(job *strat.StratJob, event strat.DataEvent) {
			if event.IsWarmUp {
				return
			}
			if _, ok := event.Raw("integer").(int64); !ok || event.Raw("nullable") != nil {
				t.Error("custom type/NULL changed")
			}
			if callbacks.Add(1) == 1 {
				if err := job.OpenOrder(&strat.EnterReq{Tag: "automatic-live", Amount: 1}); err != nil {
					t.Error(err)
				}
				if _, _, err := biz.ProcessJobOrders(job); err != nil {
					t.Error(err)
				}
			}
		}}
	})
	t.Cleanup(func() { strat.UnregisterStrategy("automatic-live-ts") })
	sourceJoined := make(chan struct{})
	var output bytes.Buffer
	err = data.RegisterDataSourceFactory(name, func() data.DataSource {
		return &legacyEntrySource{liveEntrySource: liveEntrySource{name: name}, run: func(sourceCtx context.Context, subs []*strat.DataSub, sink data.DataSink) error {
			defer close(sourceJoined)
			bySID := map[int32]*strat.DataSub{}
			for _, sub := range subs {
				bySID[sub.ExSymbol.ID] = sub
			}
			if len(bySID) != 3 || bySID[1] == nil {
				return fmt.Errorf("external TS stream omitted: %+v", subs)
			}
			grid := (time.Now().UnixMilli()/100 - 2) * 100
			emit := func(sid int32, stamp int64, closed bool) error {
				return sink.Emit(bySID[sid], []*orm.DataRecord{{Sid: sid, TimeMS: stamp, EndMS: stamp, Closed: closed, Values: map[string]any{"close": 100.0, "integer": int64(sid), "nullable": nil}}})
			}
			if err := emit(1, grid, false); err != nil {
				return err
			}
			for _, sid := range []int32{2, 3} {
				if err := emit(sid, grid, true); err != nil {
					return err
				}
			}
			select {
			case <-time.After(3 * time.Millisecond):
			case <-sourceCtx.Done():
				return sourceCtx.Err()
			}
			for _, sid := range []int32{2, 3} {
				if err := emit(sid, time.Now().UnixMilli()-1, false); err != nil {
					return err
				}
			}
			exchange.mu.Lock()
			tsFilled := exchange.positions["ASSET1/USD:USD"] != 0
			csFilled := exchange.positions["ASSET2/USD:USD"] != 0 && exchange.positions["ASSET3/USD:USD"] != 0
			exchange.mu.Unlock()
			if callbacks.Load() != 1 || !tsFilled || !csFilled || output.Len() == 0 {
				return fmt.Errorf("both engines did not execute: TS callbacks=%d ts=%v cs=%v output=%d", callbacks.Load(), tsFilled, csFilled, output.Len())
			}
			cancel()
			<-sourceCtx.Done()
			return sourceCtx.Err()
		}}
	})
	if err != nil {
		t.Fatal(err)
	}
	sessionCtx, sessionCancel := context.WithCancel(context.Background())
	storage, _ := factorKlineStorageFixture(t, time.Now().UnixMilli()/60000*60000)
	session := &explicitEntrySession{process: runtimepkg.NewProcess(), runSpec: spec, ctx: sessionCtx, cancel: sessionCancel, exchange: exchange, storage: storage, netDisable: true}
	startupCalls := 0
	err = session.runFactorsLiveWithStartup(ctx, snapshot, []runner.Config{c}, binding, &output, func(_ context.Context, trader *live.CryptoTrader) error {
		startupCalls++
		if len(trader.RuntimeDependencies().Strategies.CollectJobs()) != 1 {
			return fmt.Errorf("startup lacks actual TS job")
		}
		return nil
	})
	session.close()
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if startupCalls != 1 || callbacks.Load() != 1 {
		t.Fatal("mixed startup/callback lost", startupCalls, callbacks.Load())
	}
	for _, joined := range []<-chan struct{}{sourceJoined, socketJoined} {
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Fatal("live resource did not join")
		}
	}
	if binding.Legacy != nil || len(binding.AccountInstruments) != 0 {
		t.Fatal("mutated caller binding")
	}
}

func TestMixedLiveUnspecifiedTSPolicyRunsAllProductionAccounts(t *testing.T) {
	body := "config_version: 2\ntime_start: '20240101'\ntime_end: '20240102'\nenv: prod\nexchange: {name: mixedfixture}\nmarket_type: linear\nstake_currency: [USD]\naccounts: {a: {}, b: {}, disabled: {no_trade: true}}\nrun_policy:\n  - name: automatic-account-ts\n    run_timeframes: [1m]\n"
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, account := range []string{"a", "b"} {
		if len(mixedLivePolicies(spec, account)) != 1 {
			t.Fatalf("unspecified TS omitted account %s", account)
		}
	}
	if len(mixedLivePolicies(spec, "disabled")) != 0 {
		t.Fatal("TS selected disabled account")
	}
	for _, account := range []string{"a", "b"} {
		snapshot, err := spec.RuntimeSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		cfg := mixedLiveAccountConfig(snapshot.View(), account, mixedLivePolicies(spec, account))
		if config.NewSnapshot(cfg).DefaultAccount() != account {
			t.Fatal("account identity collapsed to default")
		}
	}
}

func TestMixedLiveTSAccountPrecancelledGroupOpensNoResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	session := &explicitEntrySession{}
	started := false
	err := session.runMixedLiveTSAccount(ctx, nil, "default", func(context.Context, *live.CryptoTrader) error { started = true; return nil })
	if !errors.Is(err, context.Canceled) || started || session.process != nil || session.storage != nil || session.exchange != nil {
		t.Fatalf("cancelled group allocated account resources or started source: %v", err)
	}
}
