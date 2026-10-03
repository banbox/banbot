package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

type sharedTriggerFixture struct {
	process    *Process
	key        execution.AccountKey
	opts       *biz.SharedExecutionOptions
	bridge     *biz.SharedOrderBridgeConfig
	rt         *Runtime
	manager    biz.IOrderMgr
	job        *strat.StratJob
	adapter    *runner.PaperAdapter
	price      decimal.Decimal
	bar        int64
	cutoverErr error
}

func newSharedTriggerFixture(t *testing.T) *sharedTriggerFixture {
	return newSharedTriggerFixtureWithAdapter(t, nil)
}
func newSharedTriggerFixtureWithAdapter(t *testing.T, wrap func(*runner.PaperAdapter) execution.ExecutionAdapter, manualProjection ...bool) *sharedTriggerFixture {
	t.Helper()
	f := &sharedTriggerFixture{price: decimal.NewFromInt(100), bar: 1}
	dir := t.TempDir()
	p := NewProcess()
	f.process = p
	t.Cleanup(p.Close)
	i := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	var err error
	f.adapter, err = runner.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USDT"}
	opts := &biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: f.adapter, AuthoritativeSnapshot: true}
	if wrap != nil {
		opts.Adapter = wrap(f.adapter)
	}
	bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{"BTC": i}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000)}}, Risk: execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(1000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}, Quote: func(_ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: f.price, Ask: f.price, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000, Bar: f.bar}, nil
	}, IntentTTLMS: 1000}
	f.key, f.opts, f.bridge = key, opts, bridge
	f.rt, err = p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &key, SharedExecution: opts, SharedOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.rt.Clock.SetTimeMS(100)
	for _, event := range []execution.CashEvent{{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(1000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(1000)}}, AtMS: 100}, {ID: "allocate", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-1000)}, {Strategy: "ts", Amount: decimal.NewFromInt(1000)}}, AtMS: 100}} {
		if err := f.rt.SharedExecution().CashEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.rt.SharedExecution().Reconcile("startup", 100); err != nil {
		t.Fatal(err)
	}
	if len(manualProjection) == 0 || !manualProjection[0] {
		biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), nil, false)
		f.manager = biz.GetOdMgrWithState(f.rt.Trading, "default")
	}
	f.job = &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: "ws", Account: "default", CloseLong: true, CloseShort: true, ExgStopLoss: true, ExgTakeProfit: true}
	f.job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	f.job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
	f.rt.Market.Prices.SetBarPriceAt(100, "BTC", 100)
	return f
}
func (f *sharedTriggerFixture) entry(t *testing.T, req *strat.EnterReq) *ormo.InOutOrder {
	t.Helper()
	if err := f.job.OpenOrder(req); err != nil {
		t.Fatal(err)
	}
	rows, _, err := f.manager.ProcessOrders(f.job)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("entry rows: %v", rows)
	}
	return rows[0]
}
func (f *sharedTriggerFixture) observe(t *testing.T, price float64, bar int64) {
	t.Helper()
	f.price = decimal.NewFromFloat(price)
	f.bar = bar
	now := 100 + bar
	f.rt.Clock.SetTimeMS(now)
	f.rt.Market.Prices.SetBarPriceAt(now, "BTC", price)
	if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: "1m", TimeMS: now - 1, EndMS: now, Closed: true, Values: map[string]any{"close": price}}); err != nil {
		t.Fatal(err)
	}
}
func (f *sharedTriggerFixture) steps(t *testing.T) int64 {
	t.Helper()
	s, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Lots) == 0 {
		return 0
	}
	return s.Lots[0].SignedSteps
}

func TestSharedLegacyProtectionAndCallbackMatrix(t *testing.T) {
	t.Run("stopbars expires only pending", func(t *testing.T) {
		f := newSharedTriggerFixture(t)
		od := f.entry(t, &strat.EnterReq{Tag: "limit", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 2})
		if od.Enter.Filled != 0 {
			t.Fatal("future limit filled early")
		}
		f.observe(t, 100, 3)
		if f.steps(t) != 0 || f.adapter.Metrics().Fills != 0 {
			t.Fatal("StopBars cancellation manufactured fill")
		}
	})
	t.Run("takeprofit partial once", func(t *testing.T) {
		f := newSharedTriggerFixture(t)
		f.entry(t, &strat.EnterReq{Tag: "tp", Amount: 1, TakeProfit: 110, TakeProfitRate: 0.5, TakeProfitTag: "profit"})
		f.observe(t, 109, 2)
		if f.steps(t) != 10 {
			t.Fatal("takeprofit triggered early")
		}
		f.observe(t, 110, 3)
		if f.steps(t) != 5 {
			t.Fatal("takeprofit partial ratio lost")
		}
		f.observe(t, 120, 4)
		if f.steps(t) != 5 {
			t.Fatal("one protection triggered twice")
		}
	})
	t.Run("trailing anchor and activation", func(t *testing.T) {
		f := newSharedTriggerFixture(t)
		f.entry(t, &strat.EnterReq{Tag: "trail", Amount: 1, ActivationPrice: 110, CallbackPct: 10})
		f.observe(t, 105, 2)
		f.observe(t, 115, 3)
		f.observe(t, 120, 4)
		f.observe(t, 109, 5)
		if f.steps(t) != 10 {
			t.Fatal("trailing threshold triggered early")
		}
		f.observe(t, 107, 6)
		if f.steps(t) != 0 {
			t.Fatal("persisted trailing anchor failed")
		}
	})
	t.Run("existing oncheckexit path", func(t *testing.T) {
		f := newSharedTriggerFixture(t)
		f.entry(t, &strat.EnterReq{Tag: "custom", Amount: 1})
		f.job.Strat.OnCheckExit = func(*strat.StratJob, *ormo.InOutOrder) *strat.ExitReq {
			return &strat.ExitReq{Tag: "check", ExitRate: 0.5, FilledOnly: true}
		}
		if err := strat.CheckCustomExits(f.job); err != nil {
			t.Fatal(err)
		}
		if _, _, err := f.manager.ProcessOrders(f.job); err != nil {
			t.Fatal(err)
		}
		if f.steps(t) != 5 {
			t.Fatal("OnCheckExit escaped shared attribution")
		}
	})
	t.Run("edit stop listener", func(t *testing.T) {
		f := newSharedTriggerFixture(t)
		od := f.entry(t, &strat.EnterReq{Tag: "edit", Amount: 1})
		if err := od.SetStopLoss(&ormo.ExitTrigger{Price: 95, Tag: "edited"}); err != nil {
			t.Fatal(err)
		}
		if err := f.manager.(*biz.SharedOrderMgr).LastError(); err != nil {
			t.Fatal(err)
		}
		f.observe(t, 94, 2)
		if f.steps(t) != 0 {
			t.Fatal("edit listener did not persist software protection")
		}
	})
}

func TestSharedTrailingRestoresActualAnchor(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "restart-trail", Amount: 1, ActivationPrice: 110, CallbackPct: 10})
	f.observe(t, 115, 2)
	f.observe(t, 120, 3)
	f.process.Close()
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	rt.Clock.SetTimeMS(104)
	if err := rt.SharedExecution().Reconcile("restart-trailing", 104); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	f.manager = biz.GetOdMgrWithState(rt.Trading, "default")
	f.observe(t, 109, 4)
	if f.steps(t) != 10 {
		t.Fatal("restart changed the saved120 anchor")
	}
	f.observe(t, 107, 5)
	if f.steps(t) != 0 {
		t.Fatal("restart lost trailing activation/anchor")
	}
}

func TestSharedRelativeProtectionUsesFilledCostBasis(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "relative", Amount: 1, StopLossVal: 5, Infos: map[string]string{"user-field": "kept"}})
	f.observe(t, 110, 2)
	f.observe(t, 104, 3)
	if f.steps(t) != 10 {
		t.Fatal("relative stop anchored to later110 mark instead of actual100 fill")
	}
	f.observe(t, 94, 4)
	if f.steps(t) != 0 {
		t.Fatal("actual95 stop did not trigger")
	}
}
