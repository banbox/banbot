package biz_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedCheckpointHistoricalCommandAndProjection(t *testing.T) {
	p := runtime.NewProcess()
	defer p.Close()
	i := execution.Instrument{ID: "BTC/USD", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.NewFromInt(1), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	quote := func(_ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000}, nil
	}
	risk := execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(2000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}
	adapter, err := execution.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	key := execution.AccountKey{VenueSessionIdentity: "checkpoint-test", Account: "default", SettlementDomain: "USD"}
	opts := biz.SharedExecutionOptions{Memory: true, HistoryPath: filepath.Join(t.TempDir(), "history.sqlite"), Adapter: adapter, AuthoritativeSnapshot: true}
	bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{i.ID: i}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", MaxNotional: decimal.NewFromInt(1000)}}, Risk: risk, Quote: quote, IntentTTLMS: 1000}
	rt, err := p.NewRuntime(runtime.Options{Mode: core.RunModeBackTest, AccountOwnerKey: &key, SharedExecution: &opts, SharedOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	rt.Clock.SetTimeMS(100)
	a := rt.SharedExecution()
	if err := a.CashEvent(execution.CashEvent{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(1000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(1000)}}, AtMS: 100}); err != nil {
		t.Fatal(err)
	}
	if err := a.CashEvent(execution.CashEvent{ID: "allocate", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-1000)}, {Strategy: "ts", Amount: decimal.NewFromInt(1000)}}, AtMS: 100}); err != nil {
		t.Fatal(err)
	}
	if err := a.Reconcile("startup", 100); err != nil {
		t.Fatal(err)
	}
	if err := a.RegisterRiskPolicy("shared-risk-v1", risk); err != nil {
		t.Fatal(err)
	}
	var callbacks int
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), func(*ormo.InOutOrder, bool) { callbacks++ }, false)
	m := biz.GetOdMgrWithState(rt.Trading, "default")
	symbol := &orm.ExSymbol{ID: 1, Symbol: i.ID}
	var first *ormo.InOutOrder
	var midpoint execution.MemoryHistoryStats
	for n := 0; n < 130; n++ {
		rt.Clock.SetTimeMS(int64(101 + n*2))
		od, err := m.EnterOrder(symbol, "1m", &strat.EnterReq{StratName: "legacy", CommandID: fmt.Sprintf("entry-%d", n), Amount: 1})
		if err != nil || od == nil {
			t.Fatalf("entry %d: %v %v", n, od, err)
		}
		if n == 0 {
			first = od.Clone()
		}
		rt.Clock.SetTimeMS(int64(102 + n*2))
		if _, err := m.ExitOrder(od, &strat.ExitReq{StratName: "legacy", CommandID: fmt.Sprintf("exit-%d", n), ExitRate: 1, Force: true}); err != nil {
			t.Fatal(err)
		}
		if n == 64 {
			if err := a.WithState(func(s *execution.SharedAccount) error {
				var e error
				midpoint, e = s.Store().MemoryHistoryStats(context.Background())
				return e
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	before, err := a.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	latest, err := a.LatestPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(latest.Targets) > 2 {
		t.Fatalf("latest hot plan retained settled zero targets: %d", len(latest.Targets))
	}
	count := callbacks
	orders, lock := rt.Orders.GetOpenODs("default")
	lock.Lock()
	retained := len(orders)
	delete(orders, first.ID) // force history point-read rather than a retained facade
	lock.Unlock()
	if retained != 0 {
		t.Fatalf("closed compatibility rows remain in the hot registry: %d", retained)
	}
	if len(rt.Orders.HistoricalOrders()) != 0 {
		t.Fatal("cold simulation also retained a historical-order result collection")
	}
	rt.Clock.SetTimeMS(500)
	replayed, replayErr := m.EnterOrder(symbol, "1m", &strat.EnterReq{StratName: "legacy", CommandID: "entry-0", Amount: 1})
	if replayErr != nil || replayed == nil || replayed.ID != first.ID {
		t.Fatalf("historical command lost original result: %v %v", replayed, replayErr)
	}
	if replayed.Status < ormo.InOutStatusFullExit {
		t.Fatal("historical replay restored a closed lot as an open order", replayed.Status)
	}
	if _, err := m.EnterOrder(symbol, "1m", &strat.EnterReq{StratName: "legacy", CommandID: "entry-0", Amount: 2}); err == nil {
		t.Fatal("changed historical command accepted")
	}
	after, err := a.Snapshot(context.Background())
	if err != nil || after.Checkpoint != before.Checkpoint || callbacks != count {
		t.Fatalf("retry changed account or callbacks: %v %d %d", err, callbacks, count)
	}
	var raw json.RawMessage
	if err := a.WithState(func(s *execution.SharedAccount) error {
		var e error
		raw, e = s.Store().StrategyCheckpoint(context.Background(), "__legacy_bridge", "legacy-ts")
		return e
	}); err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Orders   map[string]json.RawMessage
		Commands map[string]json.RawMessage
	}
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Orders) > 1 || len(saved.Commands) != 0 {
		t.Fatalf("historical facts remain in wide checkpoint: orders=%d commands=%d", len(saved.Orders), len(saved.Commands))
	}
	var final execution.MemoryHistoryStats
	if err := a.WithState(func(s *execution.SharedAccount) error {
		var e error
		final, e = s.Store().MemoryHistoryStats(context.Background())
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if final.HotRecords > midpoint.HotRecords+2 || final.ColdRecords <= midpoint.ColdRecords {
		t.Fatalf("doubling settled history grew hot records: midpoint=%+v final=%+v", midpoint, final)
	}
	views := 0
	if err := m.(*biz.SharedOrderMgr).VisitOrderViews(context.Background(), func(order *ormo.InOutOrder) error {
		views++
		if order.Status < ormo.InOutStatusFullExit || order.Enter.Filled != 1 || order.Exit == nil || order.Exit.Filled != 1 {
			t.Fatalf("cold report row lost final quantities: %#v", order)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if views != 130 || callbacks != count {
		t.Fatalf("paged report missing rows or replayed callbacks: rows=%d callbacks=%d/%d", views, callbacks, count)
	}
	lock.Lock()
	remaining := len(orders)
	lock.Unlock()
	if remaining != 0 {
		t.Fatal("report visitor left closed rows in the hot registry", remaining)
	}
	// A never-triggered software entry has no venue order whose terminal event
	// could refresh its facade when StopBars expires. Its last open registry key
	// must point-read the archived terminal metadata before being discarded.
	rt.Clock.SetTimeMS(600)
	pending, pendingErr := m.EnterOrder(symbol, "1m", &strat.EnterReq{StratName: "legacy", CommandID: "pending-expiry", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 1})
	if pendingErr != nil || pending == nil || pending.Enter.Filled != 0 {
		t.Fatal("pending software entry was not admitted", pending, pendingErr)
	}
	rt.Clock.SetTimeMS(601)
	if err := m.UpdateByDataSeries(nil, &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: "1m", TimeMS: 600, EndMS: 601, Closed: true, Values: map[string]any{"close": 100.0}}); err != nil {
		t.Fatal(err)
	}
	lock.Lock()
	remaining = len(orders)
	lock.Unlock()
	if remaining != 0 || pending.Status != ormo.InOutStatusDelete {
		t.Fatal("software expiry without a venue event left a stale pending facade", remaining, pending.Status)
	}
	expired, expiredErr := m.EnterOrder(symbol, "1m", &strat.EnterReq{StratName: "legacy", CommandID: "pending-expiry", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 1})
	if expiredErr != nil || expired == nil || expired.ID != pending.ID || expired.Status != ormo.InOutStatusDelete || callbacks != count {
		t.Fatal("expired command did not return its terminal cold result", expired, expiredErr)
	}
}
