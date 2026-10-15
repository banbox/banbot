package entry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

// This opt-in test always uses production credentials. It refuses existing
// exposure, caps each entry at 10 USDT and registers cleanup before admission.
// Keep the ledger outside t.TempDir so failed cleanup remains recoverable.
func TestFactorLiveProductionSmoke(t *testing.T) {
	path := os.Getenv("BANBOT_LIVE_SMOKE_CONFIG")
	if path == "" {
		t.Skip("set BANBOT_LIVE_SMOKE_CONFIG to a production credential YAML")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var cfg config.Config
	for _, file := range []string{filepath.Join(filepath.Dir(path), "config.yml"), path} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal(body, &cfg); err != nil {
			t.Fatal(err)
		}
	}
	if cfg.Env != core.RunEnvProd || cfg.MarketType != banexg.MarketLinear {
		t.Fatal("smoke requires env: prod and market_type: linear")
	}
	var names []string
	for name, account := range cfg.Accounts {
		if account != nil && !account.NoTrade {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	accountName := os.Getenv("BANBOT_LIVE_SMOKE_ACCOUNT")
	if accountName == "" && len(names) == 1 {
		accountName = names[0]
	}
	if accountName == "" {
		t.Fatal("select BANBOT_LIVE_SMOKE_ACCOUNT when multiple accounts are configured")
	}
	if cfg.Accounts[accountName] == nil || cfg.Accounts[accountName].NoTrade {
		t.Fatal("unknown or nontrading smoke account")
	}
	exchange, sdkErr := exg.NewForRuntime(config.NewSnapshot(&cfg), false)
	if sdkErr != nil {
		t.Fatal(sdkErr.Short())
	}
	t.Cleanup(func() { exchange.Close() })
	if _, err := exchange.LoadMarkets(false, map[string]any{banexg.ParamAccount: accountName, banexg.ParamContext: ctx}); err != nil {
		t.Fatal(err.Short())
	}
	symbol := os.Getenv("BANBOT_LIVE_SMOKE_SYMBOL")
	if symbol == "" {
		symbol = "DOGE/USDT:USDT"
	}
	market, sdkErr := exchange.GetMarket(symbol)
	if sdkErr != nil {
		t.Fatal(sdkErr.Short())
	}
	unit, err := execution.InstrumentFromBanexgMarket(symbol, market, exchange.GetExg().CurrenciesByCode[market.Settle])
	if err != nil {
		t.Fatal(err)
	}
	if unit.SettlementCurrency != "USDT" {
		t.Fatal("smoke limits are denominated in USDT")
	}
	key := execution.AccountKey{VenueSessionIdentity: fmt.Sprintf("%s/%s/prod", cfg.Exchange.Name, cfg.MarketType), Account: accountName, SettlementDomain: "USDT"}
	newAdapter := func() *execution.BanexgAdapter {
		adapter, err := execution.NewBanexgAdapter(ctx, exchange, execution.BanexgAdapterConfig{Account: key, Instruments: []execution.BanexgInstrument{{Symbol: symbol, Instrument: unit}}, Transport: execution.NewBanexgTransport()})
		if err != nil {
			t.Fatal(err)
		}
		return adapter
	}
	ledger := os.Getenv("BANBOT_LIVE_SMOKE_LEDGER")
	if ledger == "" {
		ledger = filepath.Join(filepath.Dir(path), "factor-live-smoke", "ledger.db")
	}
	if !filepath.IsAbs(ledger) {
		t.Fatal("smoke ledger path must be absolute")
	}
	if err := os.MkdirAll(filepath.Dir(ledger), 0700); err != nil {
		t.Fatal(err)
	}
	var process *runtime.Process
	var account *execution.SharedAccountBorrow
	var adapter *execution.BanexgAdapter
	open := func() {
		adapter = newAdapter()
		process = runtime.NewProcess()
		borrow, err := process.BorrowAccount(key, biz.SharedExecutionOptions{StorePath: ledger, SenderLeaseDir: filepath.Join(filepath.Dir(ledger), "leases"), Adapter: adapter, AuthoritativeSnapshot: true})
		if err != nil {
			process.Close()
			t.Fatal(err)
		}
		account = borrow
	}
	open()
	cleanupArmed := false
	// Every path after account creation runs cleanup using an independent
	// deadline, even when the main operation context was canceled.
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cleanCancel()
		defer func() { account.Release(); process.Close() }()
		if !cleanupArmed {
			return
		}
		// A failed restart must not leave cleanup using the previous closed
		// borrow. Reacquire the same durable ledger with the cleanup deadline.
		if _, err := account.Snapshot(cleanCtx); err != nil {
			fresh, err := execution.NewBanexgAdapter(cleanCtx, exchange, execution.BanexgAdapterConfig{Account: key, Instruments: []execution.BanexgInstrument{{Symbol: symbol, Instrument: unit}}, Transport: execution.NewBanexgTransport()})
			if err != nil {
				t.Errorf("LIVE CLEANUP reconnect failed; retain ledger %s: %v", ledger, err)
				return
			}
			process.Close()
			process = runtime.NewProcess()
			borrow, err := process.BorrowAccount(key, biz.SharedExecutionOptions{StorePath: ledger, SenderLeaseDir: filepath.Join(filepath.Dir(ledger), "leases"), Adapter: fresh, AuthoritativeSnapshot: true})
			if err != nil {
				t.Errorf("LIVE CLEANUP ledger reopen failed: %v", err)
				return
			}
			account, adapter = borrow, fresh
		}
		err := cleanupFactorSmoke(cleanCtx, account, adapter, exchange, symbol, unit)
		if err != nil {
			t.Errorf("LIVE CLEANUP FAILED; retain ledger %s for recovery: %v", ledger, err)
		} else {
			t.Log("production cleanup verified: no orders or positions")
		}
	})
	capital := map[execution.StrategyID]decimal.Decimal{"smoke-long": decimal.NewFromInt(10), "smoke-short": decimal.NewFromInt(10)}
	if err := account.BootstrapCapital(ctx, capital, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := account.RecoverPersisted(ctx); err != nil {
		t.Fatal(err)
	}
	if err := adapter.RecoverCash(ctx, account); err != nil {
		t.Fatal(err)
	}
	if err := account.Reconcile("smoke-startup", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	venue, err := adapter.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(venue.OpenOrders) != 0 || venue.Positions[unit.ID] != 0 {
		t.Fatal("smoke requires a flat account")
	}
	if os.Getenv("BANBOT_LIVE_SMOKE_READ_ONLY") == "1" {
		t.Log("production account and capability preflight passed; no trade submitted")
		return
	}
	cleanupArmed = true
	quote, err := adapter.Observe(ctx, unit.ID)
	if err != nil {
		t.Fatal(err)
	}
	price := quote.Bid.Add(quote.Ask).Div(decimal.NewFromInt(2))
	steps := decimal.NewFromInt(6).Div(unit.Notional(1, price)).Ceil().IntPart()
	steps = max(steps, unit.MinSteps)
	if unit.Notional(steps, quote.Ask).GreaterThan(decimal.NewFromInt(10)) || unit.Notional(steps, quote.Bid).LessThan(unit.MinNotional) {
		t.Fatal("venue minimum cannot fit the 10 USDT smoke cap")
	}
	risk := execution.PortfolioRisk{MarginRate: decimal.NewFromInt(1), MaxAccountMargin: decimal.NewFromInt(10), MaxVirtualGross: decimal.NewFromInt(20), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"smoke-long": decimal.NewFromInt(10), "smoke-short": decimal.NewFromInt(10)}}
	makeSink := func(id string) *runner.AccountSink {
		sink := &runner.AccountSink{Account: account, AccountID: accountName, StrategyID: id, Currency: "USDT", Instruments: map[int32]execution.Instrument{1: unit}, Risk: risk, Clock: func() int64 { return time.Now().UnixMilli() }, VisibleQuote: func(ctx context.Context, id string, _ int64) (execution.VisibleQuote, error) {
			return adapter.Observe(ctx, id)
		}}
		if err := sink.RegisterExecution(); err != nil {
			t.Fatal(err)
		}
		return sink
	}
	long := makeSink("smoke-long")
	short := makeSink("smoke-short")
	apply := func(sink *runner.AccountSink, sequence uint64, steps int64) {
		now := time.Now().UnixMilli()
		quote, err := adapter.Observe(ctx, unit.ID)
		if err != nil {
			t.Fatal(err)
		}
		mid := quote.Bid.Add(quote.Ask).Div(decimal.NewFromInt(2))
		// Add half a step to avoid float rounding below an integer quantity.
		weight := decimal.NewFromInt(steps).Add(decimal.RequireFromString("0.5")).Mul(unit.Notional(1, mid)).Div(decimal.NewFromInt(10)).InexactFloat64()
		if steps < 0 {
			weight = decimal.NewFromInt(steps).Sub(decimal.RequireFromString("0.5")).Mul(unit.Notional(1, mid)).Div(decimal.NewFromInt(10)).InexactFloat64()
		}
		if steps == 0 {
			weight = 0
		}
		portfolio, err := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: sink.StrategyID, AccountID: accountName, DecisionTime: now - 2, ExecutableAt: now - 1, ExpireAt: now + 15000, PlanSequence: sequence, SnapshotID: fmt.Sprintf("smoke-%d", now), PlanHash: "production-smoke", FactorPlanHash: "production-smoke", UniverseVersion: "smoke-v1", Budget: factor.FrozenBudget{Version: "smoke-v1", Currency: "USDT", NAV: 10}, Mode: factor.Full}, map[int32]float64{1: weight})
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.ProcessSnapshot(ctx, portfolio, map[int32]backtest.Quote{1: {AtMS: now - 1, AvailableAt: now - 1, Price: mid.InexactFloat64(), Bid: quote.Bid.InexactFloat64(), Ask: quote.Ask.InexactFloat64()}}, time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if err := account.RecoverPersisted(ctx); err != nil {
			t.Fatal(err)
		}
		if err := account.Reconcile(fmt.Sprintf("smoke-step-%d", now), time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	apply(long, 1, steps)
	account.Release()
	process.Close()
	if err := process.CloseError(); err != nil {
		t.Fatal(err)
	}
	open()
	if err := account.RecoverPersisted(ctx); err != nil {
		t.Fatal(err)
	}
	if err := account.Reconcile("smoke-restart", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	long, short = makeSink("smoke-long"), makeSink("smoke-short")
	apply(short, 1, -steps)
	apply(long, 2, 0)
	apply(short, 2, 0)
	t.Logf("production factor intents/restart verified; entry notional below 10 USDT, quantity steps=%d", steps)
}

func cleanupFactorSmoke(ctx context.Context, account *execution.SharedAccountBorrow, adapter *execution.BanexgAdapter, exchange banexg.BanExchange, symbol string, unit execution.Instrument) error {
	// Cancel only persisted identities belonging to this test ledger.
	_ = account.RecoverPersisted(ctx)
	local, err := account.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, order := range local.Orders {
		if order.ExchangeID != "" && (order.State == execution.OrderAcknowledged || order.State == execution.OrderPartial || order.State == execution.OrderCancelPending || order.State == execution.OrderUnknown) {
			if err := account.Cancel(order.Intent.ID, time.Now().UnixMilli()); err != nil {
				return err
			}
		}
	}
	// Cancellation may commit fills that raced the cancel request. Verify
	// ownership using the resulting durable position, not the earlier snapshot.
	local, err = account.Snapshot(ctx)
	if err != nil {
		return err
	}
	venue, err := adapter.Snapshot(ctx)
	if err != nil {
		return err
	}
	steps := venue.Positions[unit.ID]
	if steps != 0 {
		var owned int64
		for _, position := range local.ActualPositions {
			if position.Instrument.ID == unit.ID {
				owned += position.SignedSteps
			}
		}
		if owned != steps {
			return errors.New("cleanup refuses venue position inconsistent with recovered test ledger")
		}
		quote, err := adapter.Observe(ctx, unit.ID)
		if err != nil {
			return err
		}
		if unit.Notional(steps, quote.Ask).Abs().GreaterThan(decimal.NewFromInt(12)) {
			return errors.New("cleanup refuses unexplained position larger than smoke budget")
		}
		side := banexg.OdSideSell
		if steps < 0 {
			side = banexg.OdSideBuy
			steps = -steps
		}
		_, sdkErr := exchange.CreateOrder(symbol, banexg.OdTypeMarket, side, decimal.NewFromInt(steps).Mul(unit.QuantityStep).InexactFloat64(), 0, map[string]any{banexg.ParamAccount: account.AccountKey().Account, banexg.ParamContext: ctx, banexg.ParamRetry: 0, banexg.ParamReduceOnly: true, banexg.ParamClientOrderId: fmt.Sprintf("smoke-clean-%d", time.Now().UnixMilli())})
		if sdkErr != nil {
			return sdkErr
		}
	}
	for {
		venue, err := adapter.Snapshot(ctx)
		if err == nil && len(venue.OpenOrders) == 0 && venue.Positions[unit.ID] == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(300 * time.Millisecond):
		}
	}
}
