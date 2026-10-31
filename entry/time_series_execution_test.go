package entry

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

func TestTimeSeriesExecutionRequiresExplicitLivePolicyBeforeIO(t *testing.T) {
	body := "time_start: '20240101'\ntime_end: '20240102'\nenv: prod\nexchange: {name: test}\nmarket_type: linear\nstake_currency: [USDT]\nwallet_amounts: {USDT: 1000}\naccounts: {default: {}}\nrun_policy:\n  - name: ts-fixture\n    run_timeframes: [1m]\nexecution: {store: ledger.db}\n"
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildTimeSeriesLiveConfigs(spec); err == nil {
		t.Fatal("incomplete shared policy accepted")
	}
	err = runExplicitTradeSpecContext(context.Background(), &config.CmdArgs{}, spec, nil)
	if err == nil || !strings.Contains(err.Error(), "time-series execution") {
		t.Fatalf("shared preflight did not precede database/session I/O: %v", err)
	}
}

func timeSeriesExecutionYAML() string {
	return `time_start: '20240101'
time_end: '20240102'
env: prod
exchange: {name: test}
market_type: linear
stake_currency: [USDT]
wallet_amounts: {USDT: 1000}
pairs: ['BTC/USDT:USDT']
accounts: {default: {}}
run_policy:
  - name: ts-fixture
    run_timeframes: [1m]
execution:
  store: ledger.db
  sender_lease_dir: leases
  protection_mode: software
  funding_policy: required-stream
  margin_rate: '0.1'
  max_account_margin: '100'
  max_virtual_gross: '1000'
  strategy_gross_limit: '100'
  instruments:
    1: {id: 'BTC/USDT:USDT', version: v1, valuation: linear_perpetual, settlement_currency: USDT, quantity_step: '0.01', price_tick: '0.1', contract_size: '1', money_scale: 8}
`
}

func TestTimeSeriesExecutionBuildsExplicitAccountWithoutFactor(t *testing.T) {
	dir := t.TempDir()
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: dir, ConfigData: timeSeriesExecutionYAML()}, false)
	if err != nil {
		t.Fatal(err)
	}
	configs, buildErr := buildTimeSeriesLiveConfigs(spec)
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	c := configs["default"]
	if len(configs) != 1 || c.StrategyID != "" || c.InitialNAV != 1000 || c.Execution.StorePath != filepath.Join(dir, "ledger.db") || c.Snapshot.SIDMap[1] != "BTC/USDT:USDT" {
		t.Fatalf("account declaration changed or synthetic factor added: %+v", c)
	}
	if len(spec.Engines()) != 1 || spec.Engines()[0] != config.EngineTimeSeries {
		t.Fatal("input strategy engine changed")
	}
}

func TestTimeSeriesExecutionRejectsUnmigratedChoices(t *testing.T) {
	for _, change := range []struct{ name, from, to, reason string }{
		{"native", "protection_mode: software", "protection_mode: native", "protection_mode"},
		{"fatal-stop", "env: prod", "env: prod\nfatal_stop: {'60': 0.1}", "fatal_stop"},
		{"leverage", "env: prod", "env: prod\nleverage: 5", "leverage"},
		{"spot", "market_type: linear", "market_type: spot", "linear"},
		{"missing-unit", "pairs: ['BTC/USDT:USDT']", "pairs: ['ETH/USDT:USDT']", "explicit SID"},
	} {
		t.Run(change.name, func(t *testing.T) {
			body := strings.Replace(timeSeriesExecutionYAML(), change.from, change.to, 1)
			spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
			if err != nil {
				if change.name == "native" && strings.Contains(err.Error(), "protection_mode") {
					return
				}
				t.Fatal(err)
			}
			if _, err := buildTimeSeriesLiveConfigs(spec); err == nil || !strings.Contains(err.Error(), change.reason) {
				t.Fatalf("unmigrated choice silently accepted: %v", err)
			}
		})
	}
}

func TestTimeSeriesExecutionDoesNotGuessCapital(t *testing.T) {
	body := "time_start: '20240101'\ntime_end: '20240102'\nenv: prod\nexchange: {name: test}\nmarket_type: linear\nstake_currency: [USDT]\naccounts: {default: {}}\nrun_policy:\n  - name: ts-fixture\n    run_timeframes: [1m]\nexecution: {store: ledger.db, sender_lease_dir: leases, margin_rate: '0.1', max_account_margin: '100', max_virtual_gross: '1000', strategy_gross_limit: '100', funding_policy: required-stream}\n"
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildTimeSeriesLiveConfigs(spec); err == nil || !strings.Contains(err.Error(), "capital") {
		t.Fatalf("missing explicit strategy capital not rejected: %v", err)
	}
}

func TestTimeSeriesExecutionOrderModeAndDefaultProtection(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		mode         execution.OrderMode
		invalid      bool
	}{
		{name: "default"},
		{name: "local", fields: "  order_mode: local\n", mode: execution.LocalFirst},
		{name: "exchange-first", fields: "  order_mode: exchange_first\n  near_limit_bps: '25'\n", mode: execution.ExchangeFirst},
		{name: "native-preferred", fields: "  order_mode: local\n  protection_mode: native_preferred\n", mode: execution.LocalFirst},
		{name: "invalid-mode", fields: "  order_mode: unknown\n", invalid: true},
		{name: "invalid-distance", fields: "  near_limit_bps: '-1'\n", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(timeSeriesExecutionYAML(), "  protection_mode: software\n", tc.fields, 1)
			spec, loadErr := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
			if loadErr != nil {
				if tc.invalid {
					return
				}
				t.Fatal(loadErr)
			}
			configs, err := buildTimeSeriesLiveConfigs(spec)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid order policy accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("omitted protection_mode rejected: %v", err)
			}
			if tc.name == "native-preferred" && configs["default"].Execution.ProtectionMode != execution.NativePreferredProtection {
				t.Fatal("native protection policy lost during configuration decoding")
			}
			mode := configs["default"].Execution.OrderMode
			if tc.name == "default" {
				if mode != "" && mode != execution.LocalFirst {
					t.Fatalf("default is not local-first: %q", mode)
				}
			} else if mode != tc.mode {
				t.Fatalf("order mode not propagated: got %q, want %q", mode, tc.mode)
			}
			if tc.name == "exchange-first" && !configs["default"].Execution.NearLimitBPS.Equal(decimal.NewFromInt(25)) {
				t.Fatal("near_limit_bps not propagated")
			}
		})
	}
}

func TestTimeSeriesProductionHasNoLegacyFallback(t *testing.T) {
	body := strings.Split(timeSeriesExecutionYAML(), "execution:")[0]
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	err = runExplicitTradeSpecContext(context.Background(), &config.CmdArgs{}, spec, nil)
	if err == nil || !strings.Contains(err.Error(), "legacy live sender is disabled") {
		t.Fatalf("missing shared policy reached old sender or session I/O: %v", err)
	}
}

func TestTimeSeriesDryRunRequiresHubDeclarationsWithoutLegacyFallback(t *testing.T) {
	for _, funding := range []string{"required-stream", "explicit-zero"} {
		body := strings.Replace(timeSeriesExecutionYAML(), "env: prod", "env: dry_run", 1)
		body = strings.Replace(body, "funding_policy: required-stream", "funding_policy: "+funding, 1)
		spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
		if err != nil {
			t.Fatal(err)
		}
		configs, buildErr := buildTimeSeriesLiveConfigs(spec)
		if funding == "required-stream" {
			if buildErr == nil || !strings.Contains(buildErr.Error(), "simulated funding") {
				t.Fatalf("dry_run silently omitted funding: %v", buildErr)
			}
			continue
		}
		if buildErr != nil || len(configs) != 1 || !timeSeriesUsesHub(spec, []string{"default"}) {
			t.Fatalf("dry_run did not select Hub: configs=%v err=%v", configs, buildErr)
		}
		embeddedBody := strings.Replace(body, "execution:\n", "execution:\n  live_provider: test-embedded-private\n", 1)
		embeddedSpec, loadErr := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: embeddedBody}, false)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if err := runTimeSeriesExecution(context.Background(), nil, embeddedSpec, nil); err == nil || !strings.Contains(err.Error(), "embedded live adapters are forbidden") {
			t.Fatalf("dry_run reached a private embedded factory: %v", err)
		}
	}
	body := strings.Replace(strings.Split(timeSeriesExecutionYAML(), "execution:")[0], "env: prod", "env: dry_run", 1)
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := runExplicitTradeSpecContext(context.Background(), &config.CmdArgs{}, spec, nil); err == nil || !strings.Contains(err.Error(), "legacy live sender is disabled") {
		t.Fatalf("dry_run reached legacy runtime: %v", err)
	}
}

func TestFactorHubDryRunRejectsUnimplementedFundingBeforeIO(t *testing.T) {
	body := strings.Replace(timeSeriesExecutionYAML(), "env: prod", "env: dry_run", 1)
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	c := runner.Config{AccountID: "default"}
	c.Manifest.Costs.FundingPolicy = "required-stream"
	if err := runFactorLiveSpecWithArgs(context.Background(), nil, spec, []runner.Config{c}, "", nil, nil); err == nil || !strings.Contains(err.Error(), "simulated funding") {
		t.Fatalf("dry_run silently dropped funding or reached IO: %v", err)
	}
}

func TestMixedSharedAccountRejectsUnmigratedTSChoicesBeforeBinding(t *testing.T) {
	for _, change := range []struct{ name, from, to, reason string }{
		{"fatal-stop", "env: prod", "env: prod\nfatal_stop: {'60': 0.1}", "fatal_stop"},
		{"root-leverage", "env: prod", "env: prod\nleverage: 5", "leverage"},
		{"account-leverage", "accounts: {default: {}}", "accounts: {default: {leverage: 5}}", "leverage"},
	} {
		t.Run(change.name, func(t *testing.T) {
			body := strings.Replace(timeSeriesExecutionYAML(), change.from, change.to, 1)
			spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
			if err != nil {
				t.Fatal(err)
			}
			session := &explicitEntrySession{runSpec: spec}
			// No snapshot, transport, or strategy factory is available: this guard
			// must fail before those resources or a custom binding are touched.
			_, prepareErr := session.prepareMixedLiveBinding(nil, runner.Config{AccountID: "default"}, &FactorLiveBinding{Legacy: &FactorLegacyLiveBinding{}})
			if prepareErr == nil || !strings.Contains(prepareErr.Error(), change.reason) {
				t.Fatalf("unsafe mixed TS configuration reached binding: %v", prepareErr)
			}
		})
	}
}

func timeSeriesAccountStartupFixture(t *testing.T) (*explicitEntrySession, *config.Snapshot, runner.Config, FactorLiveBinding, *liveEntryExchange) {
	t.Helper()
	dir := t.TempDir()
	body := strings.ReplaceAll(timeSeriesExecutionYAML(), "USDT", "USD")
	body = strings.Replace(body, "exchange: {name: test}", "exchange: {name: entrytest}", 1)
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: dir, ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	configs, buildErr := buildTimeSeriesLiveConfigs(spec)
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	c := configs["default"]
	c.Execution.StorePath = filepath.Join(dir, "nested", "account", "ledger.db")
	c.Execution.SenderLeaseDir = filepath.Join(dir, "nested", "leases")
	snapshot, snapshotErr := spec.RuntimeSnapshot()
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: c.AccountID, SettlementDomain: "USD"}
	exchange := &liveEntryExchange{trades: make(chan *banexg.MyTrade), cash: 1000}
	ctx, cancel := context.WithCancel(context.Background())
	session := &explicitEntrySession{process: runtimepkg.NewProcess(), runSpec: spec, ctx: ctx, cancel: cancel, exchange: exchange, netDisable: true}
	t.Cleanup(func() { session.close() })
	unit := c.Execution.Instruments[1]
	symbol := &orm.ExSymbol{ID: 1, Symbol: unit.ID, Exchange: "entrytest", Market: "linear"}
	bridge := &biz.SharedOrderBridgeConfig{
		Version: "v1", IntentTTLMS: 60000,
		Instruments: map[string]execution.Instrument{unit.ID: unit},
		Strategies:  map[string]biz.SharedStrategyBinding{"ts-fixture": {ID: "current", MaxNotional: decimal.NewFromInt(100)}},
		Risk:        execution.PortfolioRisk{MarginRate: c.Execution.MarginRate, MaxAccountMargin: c.Execution.MaxAccountMargin, MaxVirtualGross: c.Execution.MaxVirtualGross, StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"current": c.Execution.StrategyGrossLimit}},
	}
	binding := FactorLiveBinding{
		Account: key, Transport: &liveEntryTransport{key: key}, Symbols: map[int32]*orm.ExSymbol{1: symbol}, BootstrapCapital: true,
		VerifyFunding: func(context.Context, string) (string, error) { return "verified-required-stream", nil },
		Legacy: &FactorLegacyLiveBinding{
			Bridge: bridge, capital: map[execution.StrategyID]decimal.Decimal{"current": decimal.NewFromInt(1000)},
			Jobs:          []*strat.StratJob{{Account: c.AccountID, Symbol: symbol, TimeFrame: "event", Strat: &strat.TradeStrat{Name: "ts-fixture"}}},
			Subscriptions: []*strat.DataSub{{Source: "startup-fixture", ExSymbol: symbol, TimeFrame: "event"}},
		},
	}
	return session, snapshot, c, binding, exchange
}

func TestTimeSeriesExecutionAccountInjectsQuoteAndCreatesLedgerParents(t *testing.T) {
	session, snapshot, c, binding, exchange := timeSeriesAccountStartupFixture(t)
	want := errors.New("startup reached")
	err := session.runTimeSeriesExecutionAccount(context.Background(), snapshot, c, binding, func(context.Context, *live.CryptoTrader) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("account did not finish bootstrap/recovery and reach startup: %v", err)
	}
	if binding.Legacy.Bridge.QuoteContext != nil || binding.Legacy.Bridge.Quote != nil {
		t.Fatal("startup mutated caller bridge instead of injecting SDK quote into a copy")
	}
	for _, path := range []string{c.Execution.StorePath, c.Execution.SenderLeaseDir} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("execution path was not created: %s: %v", path, err)
		}
	}
	if exchange.snapshots.Load() == 0 {
		t.Fatal("startup bypassed real account bootstrap/recovery")
	}
}

func TestTimeSeriesHubAccountStartsWithoutLocalExecutionOrPrivateIO(t *testing.T) {
	session, snapshot, c, binding, exchange := timeSeriesAccountStartupFixture(t)
	ctx := context.Background()
	registry := &execution.AccountRegistry{}
	owner, err := registry.Acquire(binding.Account)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	paper, err := execution.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	service, err := execution.NewSharedAccount(owner, execution.SharedExecutionOptions{Memory: true, Adapter: paper, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	borrow := service.Borrow()
	defer borrow.Release()
	if err := borrow.RegisterRiskPolicy("hub-v1", binding.Legacy.Bridge.Risk); err != nil {
		t.Fatal(err)
	}
	if err := borrow.BootstrapCapital(ctx, binding.Legacy.capital, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	unit := c.Execution.Instruments[1]
	if err := borrow.RegisterAccountQuotes([]execution.Instrument{unit}, func(_ context.Context, _ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000}, nil
	}, func() int64 { return time.Now().UnixMilli() }); err != nil {
		t.Fatal(err)
	}
	if err := borrow.Reconcile("ready", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	hub, err := execution.NewHub(func(context.Context, execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		return execution.HubAccountBinding{Account: service, Shutdown: func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	dir, err := os.MkdirTemp("", "ban-ts-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "s")
	server, err := execution.ListenHubUnix(socket, hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := execution.NewHubClient(socket)
	defer client.Close()
	if err := client.Register(ctx, execution.HubAccountRegistration{Account: binding.Account, Env: "prod", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	want := errors.New("remote startup reached")
	err = session.runTimeSeriesHubAccount(ctx, snapshot, c, binding, client, func(_ context.Context, trader *live.CryptoTrader) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("remote account startup failed: %v", err)
	}
	if exchange.snapshots.Load() != 0 {
		t.Fatal("robot queried private exchange inventory")
	}
	if _, err := os.Stat(c.Execution.StorePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("robot created execution ledger", err)
	}
}

func TestTimeSeriesExecutionAccountValidatesRestoredBindingsBeforeRecovery(t *testing.T) {
	for _, scenario := range []string{"removed-strategy", "declared-without-job", "changed-instrument"} {
		t.Run(scenario, func(t *testing.T) {
			session, snapshot, c, binding, exchange := timeSeriesAccountStartupFixture(t)
			unit := c.Execution.Instruments[1]
			strategy := execution.StrategyID("current")
			reason := "restored contributor has no configured live job"
			switch scenario {
			case "removed-strategy":
				strategy = "removed"
			case "declared-without-job":
				strategy = "unused"
				binding.Legacy.Bridge.Strategies["unused"] = biz.SharedStrategyBinding{ID: strategy, MaxNotional: decimal.NewFromInt(100)}
				binding.Legacy.Bridge.Risk.StrategyGrossLimits[strategy] = decimal.NewFromInt(100)
			case "changed-instrument":
				unit.Version = "old-version"
				reason = "restored account instrument mapping missing or changed"
			}
			if err := os.MkdirAll(filepath.Dir(c.Execution.StorePath), 0755); err != nil {
				t.Fatal(err)
			}
			store, err := execution.OpenStoreWithLeaseDir(c.Execution.StorePath, binding.Account, c.Execution.SenderLeaseDir)
			if err != nil {
				t.Fatal(err)
			}
			plan := execution.Plan{ID: "persisted-plan", Sequence: 1, DecisionMS: 1, ExpiresMS: 2, Instruments: []execution.Instrument{unit}, Targets: []execution.ExecutableTarget{{Strategy: strategy, Lot: "old-lot", Instrument: unit.ID, SignedSteps: 1}}}
			if err := store.SavePlan(context.Background(), plan); err != nil {
				store.Close()
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			started := false
			err = session.runTimeSeriesExecutionAccount(context.Background(), snapshot, c, binding, func(context.Context, *live.CryptoTrader) error { started = true; return nil })
			if err == nil || !strings.Contains(err.Error(), reason) {
				t.Fatalf("restored mismatch was not rejected before recovery: %v", err)
			}
			if started || exchange.snapshots.Load() != 0 {
				t.Fatal("invalid restored binding reached account bootstrap, reconciliation or startup")
			}
		})
	}
}
