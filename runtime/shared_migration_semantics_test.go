package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/shopspring/decimal"
)

type semanticMigrationAdapter struct {
	*runner.PaperAdapter
	venue   execution.LegacyVenueSnapshot
	active  *execution.StoredOrder
	submits []execution.OrderIntent
}

func (a *semanticMigrationAdapter) Submit(ctx context.Context, o execution.OrderIntent, client string) (execution.SubmitReceipt, error) {
	a.submits = append(a.submits, o)
	return a.PaperAdapter.Submit(ctx, o, client)
}

func (a *semanticMigrationAdapter) Query(ctx context.Context, client, id string) (execution.QueryResult, error) {
	if a.active != nil && (a.active.ClientID == client || a.active.ExchangeID == id) {
		o := a.active
		result := execution.QueryResult{Found: true, Authoritative: true, Complete: true, Canceled: o.State == execution.OrderCanceled, Receipt: execution.SubmitReceipt{ExchangeID: o.ExchangeID}}
		if o.FilledSteps > 0 {
			result.Receipt.Fills = []execution.FillReport{{EventID: "native-exit-report/" + decimal.NewFromInt(o.FilledSteps).String(), OrderID: o.Intent.ID, Steps: o.FilledSteps, Cost: o.ReportedCost, Fee: o.ReportedFee, Price: o.Intent.Observation.Price, Cumulative: true, AtMS: 101}}
		}
		return result, nil
	}
	return a.PaperAdapter.Query(ctx, client, id)
}

func (a *semanticMigrationAdapter) Snapshot(ctx context.Context) (execution.VenueSnapshot, error) {
	snapshot, err := a.PaperAdapter.Snapshot(ctx)
	if a.active != nil && a.active.State != execution.OrderFilled && a.active.State != execution.OrderCanceled {
		query, queryErr := a.Query(ctx, a.active.ClientID, a.active.ExchangeID)
		if queryErr != nil {
			return snapshot, queryErr
		}
		snapshot.OpenOrders = append(snapshot.OpenOrders, query)
	}
	return snapshot, err
}

func (a *semanticMigrationAdapter) MigrationSnapshot(context.Context) (execution.LegacyVenueSnapshot, error) {
	return a.venue, nil
}

// This goes through the immutable raw source, actual joined-owner cutover, and
// a new process before its first observation. No post-import edits repair state.
func migratedSemanticFixture(t *testing.T, row *ormo.InOutOrder) *sharedTriggerFixture {
	return semanticCutoverFixture(t, row, false)
}

func semanticCutoverFixture(t *testing.T, row *ormo.InOutOrder, reject bool) *sharedTriggerFixture {
	return semanticCutoverFixtureWithOrders(t, row, reject, nil)
}

func semanticCutoverFixtureWithOrders(t *testing.T, row *ormo.InOutOrder, reject bool, prepare func(*execution.LegacyMigration, *semanticMigrationAdapter, execution.AccountKey), manualProjection ...bool) *sharedTriggerFixture {
	t.Helper()
	dir := t.TempDir()
	i := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	steps := decimal.NewFromFloat(row.Enter.Filled).Div(i.QuantityStep).IntPart()
	if row.Exit != nil {
		steps -= decimal.NewFromFloat(row.Exit.Filled).Div(i.QuantityStep).IntPart()
	}
	lot := execution.VirtualLot{Strategy: "ts", ID: "old-42", Instrument: i, SignedSteps: steps, CostBasis: decimal.NewFromInt(steps).Mul(i.QuantityStep).Mul(decimal.NewFromInt(100))}
	lot.Fees = decimal.NewFromFloat(row.Enter.FeeQuote)
	if row.Exit != nil {
		lot.Fees = lot.Fees.Add(decimal.NewFromFloat(row.Exit.FeeQuote))
	}
	seedSide := execution.Buy
	if row.Short {
		lot.SignedSteps = -lot.SignedSteps
		seedSide = execution.Sell
	}
	actual := lot
	actual.Strategy, actual.ID = "", ""
	paper, _ := runner.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if steps > 0 {
		_, err := paper.Submit(context.Background(), execution.OrderIntent{ID: "seed", Instrument: i, Side: seedSide, Steps: steps, SubmitAtMS: 100, Observation: execution.ExecutionObservation{Price: decimal.NewFromInt(100), AtMS: 100}}, "seed-client")
		if err != nil {
			t.Fatal(err)
		}
	}
	venue := execution.LegacyVenueSnapshot{Complete: true, AccountCash: decimal.NewFromInt(1000), Positions: []execution.VirtualLot{actual}, AtMS: 100}
	adapter := &semanticMigrationAdapter{PaperAdapter: paper, venue: venue}
	f := &sharedTriggerFixture{price: decimal.NewFromInt(100), adapter: paper, key: execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USDT"}}
	f.opts = &biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: adapter, AuthoritativeSnapshot: true}
	f.bridge = &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{"BTC": i}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000)}}, Risk: execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(1000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}, IntentTTLMS: 1000, Quote: func(_ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: f.price, Ask: f.price, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000}, nil
	}}
	p := NewProcess()
	t.Cleanup(p.Close)
	old, err := p.NewRuntime(Options{Mode: core.RunModeBackTest})
	if err != nil {
		t.Fatal(err)
	}
	old.Trading.SetOrderManager("default", &biz.LocalOrderMgr{})
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(dir, "source.json")
	if err := os.WriteFile(backup, raw, 0600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	request := execution.LegacyMigration{ID: "semantic-cutover", SourceVersion: "0.3.8", SourceSnapshotID: "source", StoppedOwnerProof: "runtime-close-join", BackupPath: backup, BackupSHA256: hex.EncodeToString(hash[:]), AccountCash: venue.AccountCash, StrategyCash: map[execution.StrategyID]decimal.Decimal{"ts": venue.AccountCash}, Lots: []execution.VirtualLot{lot}, ActualPositions: []execution.VirtualLot{actual}, VenueSnapshot: venue, RawLegacyMap: []execution.LegacySourceMapping{{Strategy: "ts", Lot: lot.ID, TaskID: row.TaskID, IOrderID: row.ID, RawJSON: raw}}}
	if prepare != nil {
		prepare(&request, adapter, f.key)
	}
	applied, importErr := rt.CutoverLegacyExecution(old, request)
	f.cutoverErr = importErr
	if reject {
		if importErr == nil || applied {
			t.Fatal("ambiguous source admitted", applied, importErr)
		}
		state, err := rt.SharedExecution().Snapshot(context.Background())
		if err != nil || len(state.Lots) != 0 || len(state.Orders) != 0 {
			t.Fatal("refused cutover partially switched ledger", state, err)
		}
		if err := rt.SharedExecution().Rebalance(execution.CombinedRebalance{}, 101); err == nil {
			t.Fatal("refused cutover published ready")
		}
		if got, err := os.ReadFile(backup); err != nil || string(got) != string(raw) || string(request.RawLegacyMap[0].RawJSON) != string(raw) {
			t.Fatal("refused migration changed immutable source", err)
		}
		return f
	}
	if importErr != nil || !applied {
		t.Fatal(applied, importErr)
	}
	p.Close()
	f.process = NewProcess()
	t.Cleanup(f.process.Close)
	f.rt, err = f.process.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.rt.Clock.SetTimeMS(101)
	if err := f.rt.SharedExecution().RecoverPersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.rt.SharedExecution().Reconcile("semantic-restart", 101); err != nil {
		t.Fatal(err)
	}
	if len(manualProjection) == 0 || !manualProjection[0] {
		biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), nil, false)
		f.manager = biz.GetOdMgrWithState(f.rt.Trading, "default")
	}
	rows, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	projected := rows[42]
	lock.Unlock()
	manual := len(manualProjection) > 0 && manualProjection[0]
	if !manual && (projected == nil || projected.Stop != row.Stop || projected.Enter.OrderType != row.Enter.OrderType || projected.GetInfoString(ormo.OdInfoClientID) != "original-client") {
		t.Fatal("original entry identity/conditions not projected", projected)
	}
	if trigger := row.GetStopLoss(); !manual && trigger != nil && trigger.Hit && !projected.GetStopLoss().Hit {
		t.Fatal("source activation latch not projected")
	}
	if got, err := os.ReadFile(backup); err != nil || string(got) != string(raw) {
		t.Fatal("migration changed immutable source", err)
	}
	return f
}

func TestSharedCutoverRejectsAmbiguousOrMalformedExecutableSource(t *testing.T) {
	for _, name := range []string{"missing-expiry", "malformed-trigger", "unsupported-style", "malformed-anchor"} {
		t.Run(name, func(t *testing.T) {
			row := semanticLegacyRow(0)
			switch name {
			case "missing-expiry":
				row.Info[ormo.OdInfoStopBars] = 2
			case "malformed-trigger":
				row.Info[ormo.OdInfoStopLoss] = "broken"
			case "unsupported-style":
				row.Enter.OrderType = "limit_maker"
			case "malformed-anchor":
				row.Info[ormo.OdInfoTrailingBest] = "NaN"
			}
			semanticCutoverFixture(t, row, true)
		})
	}
}

func semanticLegacyRow(filled float64) *ormo.InOutOrder {
	return &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 42, TaskID: 1, Symbol: "BTC", Sid: 1, Timeframe: "1m", Strategy: "legacy", EnterTag: "original", EnterAt: 1, InitPrice: 100, Status: ormo.InOutStatusInit}, Enter: &ormo.ExOrder{Enter: true, Symbol: "BTC", Side: "buy", OrderType: "limit", Amount: 1, Filled: filled, Price: 90, Average: 100, CreateAt: 1}, Info: map[string]any{ormo.OdInfoClientID: "original-client", "custom": nil}}
}

func TestSharedCutoverPendingStopAndAbsoluteExpirySurviveImmediateRestart(t *testing.T) {
	for _, orderType := range []string{"market", "limit"} {
		t.Run("pending-stop-"+orderType, func(t *testing.T) {
			row := semanticLegacyRow(0)
			row.Stop, row.Enter.Price = 110, 0
			row.Enter.OrderType = orderType
			if orderType == "limit" {
				row.Enter.Price = 115
			}
			f := migratedSemanticFixture(t, row)
			f.observe(t, 100, 2)
			if f.adapter.Metrics().Fills != 0 || f.steps(t) != 0 {
				t.Fatal("pending stop admitted below original trigger")
			}
			f.observe(t, 110, 3)
			if f.steps(t) != 10 {
				t.Fatal("original pending stop did not activate")
			}
		})
	}
	for _, held := range []float64{0, .5} {
		t.Run(decimal.NewFromFloat(held).String(), func(t *testing.T) {
			row := semanticLegacyRow(held)
			row.Info[ormo.OdInfoStopBars], row.Info[ormo.OdInfoStopAfter] = 9, int64(104)
			f := migratedSemanticFixture(t, row)
			f.observe(t, 100, 3)
			rows, lock := f.rt.Orders.GetOpenODs("default")
			lock.Lock()
			before := rows[42]
			lock.Unlock()
			if before == nil || before.Enter.Amount != 1 {
				t.Fatal("entry expired before original deadline")
			}
			f.observe(t, 100, 4)
			f.observe(t, 90, 5)
			if f.steps(t) != int64(held*10) || f.adapter.Metrics().Fills != int(held/.5) {
				t.Fatal("original absolute expiry lost or reduced held lot", f.steps(t), f.adapter.Metrics())
			}
		})
	}
}

func TestSharedCutoverLatchedStopLimitAndTrailingAnchorSurviveImmediateRestart(t *testing.T) {
	for _, triggerKey := range []string{ormo.OdInfoStopLoss, ormo.OdInfoTakeProfit} {
		t.Run("latched-"+triggerKey+"-limit", func(t *testing.T) {
			row := semanticLegacyRow(.5)
			level := float64(95)
			if triggerKey == ormo.OdInfoTakeProfit {
				level = 120
			}
			row.Info[triggerKey] = &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: level, Limit: 110, Rate: .5, Tag: "original-trigger"}, Hit: true}
			f := migratedSemanticFixture(t, row)
			f.observe(t, 100, 2)
			if f.steps(t) != 5 {
				t.Fatal("latched stop ignored original limit")
			}
			f.observe(t, 110, 3)
			if f.steps(t) != 3 {
				t.Fatal("latched stop/rate lost after restart", f.steps(t))
			}
			f.observe(t, 110, 4)
			if f.steps(t) != 3 {
				t.Fatal("latched partial protection executed twice")
			}
		})
	}
	t.Run("active-trailing", func(t *testing.T) {
		row := semanticLegacyRow(.5)
		row.Info[ormo.OdInfoCallbackPct], row.Info[ormo.OdInfoActivePrice], row.Info[ormo.OdInfoTrailingBest] = 10., 0., 120.
		f := migratedSemanticFixture(t, row)
		f.observe(t, 110, 2)
		if f.steps(t) != 5 {
			t.Fatal("trailing triggered before original threshold")
		}
		f.observe(t, 108, 3)
		if f.steps(t) != 0 {
			t.Fatal("active trailing anchor reset on cutover", f.steps(t))
		}
	})
}
