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

type sharedCaptureAdapter struct {
	execution.ExecutionAdapter
	orders []execution.OrderIntent
}

func (a *sharedCaptureAdapter) Submit(ctx context.Context, o execution.OrderIntent, client string) (execution.SubmitReceipt, error) {
	a.orders = append(a.orders, o)
	return a.ExecutionAdapter.Submit(ctx, o, client)
}

func TestSharedStratJobEntryStopExitPreservesFactorShort(t *testing.T) {
	dir := t.TempDir()
	p := NewProcess()
	defer p.Close()
	instrument := execution.Instrument{ID: "BTC/USDT", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	price := decimal.NewFromInt(100)
	bar := int64(1)
	quote := func(_ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: price, Ask: price, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000, Bar: bar}, nil
	}
	risk := execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(2000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000), "cs": decimal.NewFromInt(1000)}}
	adapter, err := runner.NewPaperAdapter(decimal.NewFromInt(1000), decimal.RequireFromString("0.001"), decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	capture := &sharedCaptureAdapter{ExecutionAdapter: adapter}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USDT"}
	opts := biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: capture, AuthoritativeSnapshot: true}
	bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{instrument.ID: instrument}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000)}}, Risk: risk, Quote: quote, IntentTTLMS: 1000}
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &key, SharedExecution: &opts, SharedOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	rt.Clock.SetTimeMS(100)
	account := rt.SharedExecution()
	if err := account.CashEvent(execution.CashEvent{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(1000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(1000)}}, AtMS: 100}); err != nil {
		t.Fatal(err)
	}
	if err := account.CashEvent(execution.CashEvent{ID: "allocate", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-1000)}, {Strategy: "ts", Amount: decimal.NewFromInt(500)}, {Strategy: "cs", Amount: decimal.NewFromInt(500)}}, AtMS: 100}); err != nil {
		t.Fatal(err)
	}
	if err := account.Reconcile("startup", 100); err != nil {
		t.Fatal(err)
	}
	var callbacks []bool
	var callbackExits []float64
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), func(od *ormo.InOutOrder, entry bool) {
		callbacks = append(callbacks, entry)
		if !entry && od.Exit != nil {
			callbackExits = append(callbackExits, od.Exit.Filled)
		}
	}, false)
	manager := biz.GetOdMgrWithState(rt.Trading, "default")
	if _, ok := manager.(*biz.SharedOrderMgr); !ok {
		t.Fatal("legacy callback installed independent local manager")
	}
	if err := account.RegisterRiskPolicy("shared-risk-v1", risk); err != nil {
		t.Fatal(err)
	}
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: &orm.ExSymbol{ID: 1, Symbol: instrument.ID}, TimeFrame: "1m", Account: "default"}
	job.BindRuntimeState(rt.Strategies, rt.Core, rt.Clock)
	job.BindRuntimeMarket(rt.Market.Prices, rt.Clock)
	job.ExgStopLoss = true
	rt.Market.Prices.SetBarPriceAt(100, instrument.ID, 100)
	job.Strat.OnBar = func(s *strat.StratJob) {
		if err := s.OpenOrder(&strat.EnterReq{Tag: "signal", Amount: 1, StopLoss: 95}); err != nil {
			t.Fatal(err)
		}
	}
	job.Strat.OnBar(job)
	entries, _, e := manager.ProcessOrders(job)
	if e != nil {
		t.Fatal(e)
	}
	if len(entries) != 1 || entries[0] == nil || entries[0].Enter.Filled != 1 {
		t.Fatalf("legacy callback entry projection missing: %#v", entries)
	}
	rt.Clock.SetTimeMS(101)
	q, _ := quote(instrument.ID, 101)
	risk.Marks = map[string]decimal.Decimal{instrument.ID: price}
	if err := account.Rebalance(execution.CombinedRebalance{PlanID: "factor-short", DecisionMS: 101, ExpiresMS: 1000, Requests: []execution.InstrumentRebalance{{Instrument: instrument, Quote: q, Targets: []execution.ExecutableTarget{{Strategy: "ts", Lot: execution.VirtualLotID(entries[0].Info["shared_lot"].(string)), SignedSteps: 10}, {Strategy: "cs", Lot: "factor-lot", SignedSteps: -6}}}}, Risk: risk}, 101); err != nil {
		t.Fatal(err)
	}
	beforeStop, err := account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeStop.ActualPositions) != 1 || beforeStop.ActualPositions[0].SignedSteps != 4 {
		t.Fatal("TS+1 and CS-0.6 did not net to +0.4")
	}
	price = decimal.NewFromInt(90)
	bar = 2
	rt.Clock.SetTimeMS(102)
	if e := manager.UpdateByDataSeries(entries, &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: "1m", TimeMS: 101, EndMS: 102, Closed: true, Values: map[string]any{"close": 90.0}}); e != nil {
		t.Fatal(e)
	}
	snapshot, err := account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Lots) != 1 || snapshot.Lots[0].Strategy != "cs" || snapshot.Lots[0].SignedSteps != -6 || len(snapshot.ActualPositions) != 1 || snapshot.ActualPositions[0].SignedSteps != -6 {
		t.Fatalf("TS stop changed factor attribution: %#v", snapshot)
	}
	if len(callbacks) != 3 || !callbacks[0] || callbacks[1] || callbacks[2] {
		t.Fatalf("commit fill callbacks: %v", callbacks)
	}
	if len(capture.orders) != 4 {
		t.Fatalf("unexpected venue submit count: %d", len(capture.orders))
	}
	decrease, increase := capture.orders[2], capture.orders[3]
	if decrease.Side != execution.Sell || decrease.Steps != 4 || !decrease.ReduceOnly || increase.Side != execution.Sell || increase.Steps != 6 || increase.ReduceOnly || len(increase.RequiresFilled) != 1 || increase.RequiresFilled[0] != decrease.ID {
		t.Fatalf("actual bridge reversal legs: %#v %#v", decrease, increase)
	}
	if len(callbackExits) != 2 || callbackExits[0] != 0.4 || callbackExits[1] != 1 {
		t.Fatalf("callback cumulative exits %v", callbackExits)
	}
	rt.Clock.SetTimeMS(103)
	q, _ = quote(instrument.ID, 103)
	latest, err := account.LatestPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var tsLot execution.VirtualLotID
	for _, target := range latest.Targets {
		if target.Strategy == "ts" {
			tsLot = target.Lot
		}
	}
	if err := account.Rebalance(execution.CombinedRebalance{PlanID: "factor-shrink", DecisionMS: 103, ExpiresMS: 1000, Requests: []execution.InstrumentRebalance{{Instrument: instrument, Quote: q, Targets: []execution.ExecutableTarget{{Strategy: "ts", Lot: tsLot, SignedSteps: 0}, {Strategy: "cs", Lot: "factor-lot", SignedSteps: -2}}}}, Risk: risk}, 103); err != nil {
		t.Fatal(err)
	}
	beforeDuplicate, err := account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := account.ApplyTrade(adapter.Metrics().LastFill); err != nil {
		t.Fatal(err)
	}
	afterDuplicate, err := account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if beforeDuplicate.Checkpoint != afterDuplicate.Checkpoint || len(afterDuplicate.Lots) != 1 || afterDuplicate.Lots[0].SignedSteps != -2 || afterDuplicate.ActualPositions[0].SignedSteps != -2 || !afterDuplicate.AccountSettledCash.Equal(decimal.RequireFromString("995.714")) {
		t.Fatalf("factor shrink/duplicate report changed cash attribution: %#v", afterDuplicate)
	}
	tsTotals, err := account.StrategyTotals(context.Background(), "ts")
	if err != nil {
		t.Fatal(err)
	}
	csTotals, err := account.StrategyTotals(context.Background(), "cs")
	if err != nil {
		t.Fatal(err)
	}
	if !tsTotals.Fees.Equal(decimal.RequireFromString("0.19")) || !csTotals.Fees.Equal(decimal.RequireFromString("0.096")) {
		t.Fatalf("per-strategy fees: %s/%s", tsTotals.Fees, csTotals.Fees)
	}
	p.Close()
	p2 := NewProcess()
	defer p2.Close()
	restarted, err := p2.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &key, SharedExecution: &opts, SharedOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Clock.SetTimeMS(103)
	if err := restarted.SharedExecution().Reconcile("restart", 103); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(restarted.BizDeps(), func(*ormo.InOutOrder, bool) { t.Fatal("restore replayed trade-generating callback") }, false)
	if len(capture.orders) != 5 {
		t.Fatal("restart repeated venue send")
	}
	restored, lock := restarted.Orders.GetOpenODs("default")
	lock.Lock()
	row := restored[entries[0].ID]
	lock.Unlock()
	if row == nil || row.Enter.Filled != 1 || row.Exit == nil || row.Exit.Filled != 1 || row.Status != ormo.InOutStatusFullExit {
		t.Fatalf("restart lost committed virtual exit projection: %#v", row)
	}
}
