package runner

import (
	"context"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/shopspring/decimal"
	"path/filepath"
	"testing"
)

func patchPortfolio(t *testing.T, sequence uint64, mode factor.PortfolioMode, nav float64, weights map[int32]float64) *factor.TargetPortfolio {
	t.Helper()
	p, err := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: "s", AccountID: "a", DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100, PlanSequence: sequence, SnapshotID: "snap", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "b", Currency: "USD", NAV: nav}, Mode: mode}, weights)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLivePatchDoesNotWaitForOmittedQuote(t *testing.T) {
	sink := &liveSink{}
	l := &Live{sink: sink, previous: patchPortfolio(t, 1, factor.Full, 1000, map[int32]float64{1: .5}), pending: patchPortfolio(t, 2, factor.Patch, 1500, map[int32]float64{2: .2}), quotes: map[int32]backtest.Quote{2: {AtMS: 20, AvailableAt: 20, Price: 100}}}
	if err := l.execute(context.Background(), 20); err != nil {
		t.Fatal(err)
	}
	if len(sink.targets) != 1 || l.pending != nil || l.previous.Targets()[1] != .5 {
		t.Fatal("Patch waited for omitted quote or lost Full history")
	}
}

func TestAccountPatchKeepsDriftedQuantityAndAbsoluteTarget(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Manifest.Costs.FeeRate = 0
	c.Manifest.Costs.SlippageRate = 0
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ctx := context.Background()
	makePortfolio := func(sequence uint64, mode factor.PortfolioMode, nav float64, weights map[int32]float64) *factor.TargetPortfolio {
		p := patchPortfolio(t, sequence, mode, nav, weights)
		sp := p.Spec()
		sp.AccountID = c.AccountID
		sp.StrategyID = c.StrategyID
		p, err := factor.NewTargetPortfolio(sp, weights)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	q := backtest.Quote{AtMS: 11, AvailableAt: 11, Price: 100}
	if err = sink.ObserveQuote(ctx, 1, q, 11); err != nil {
		t.Fatal(err)
	}
	if err = sink.ProcessSnapshot(ctx, makePortfolio(1, factor.Full, 10000, map[int32]float64{1: .5}), map[int32]backtest.Quote{1: q}, 11); err != nil {
		t.Fatal(err)
	}
	if err = sink.ObserveQuote(ctx, 1, backtest.Quote{AtMS: 20, AvailableAt: 20, Price: 200}, 20); err != nil {
		t.Fatal(err)
	}
	q = backtest.Quote{AtMS: 20, AvailableAt: 20, Price: 100}
	if err = sink.ObserveQuote(ctx, 2, q, 20); err != nil {
		t.Fatal(err)
	}
	if err = sink.ProcessSnapshot(ctx, makePortfolio(2, factor.Patch, 15000, map[int32]float64{2: .2}), map[int32]backtest.Quote{2: q}, 20); err != nil {
		t.Fatal(err)
	}
	state, err := sink.StrategyState(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	if state.Quantities[1] != 50 || state.Quantities[2] != 30 || state.NAV != 15000 {
		t.Fatalf("Patch drifted quantity: %+v", state)
	}
	plan, err := sink.Account.LatestPlan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var steps int64
	for _, target := range plan.Targets {
		if target.Instrument == sink.Instruments[1].ID {
			steps += target.SignedSteps
		}
	}
	if steps != 5000 {
		t.Fatalf("absolute target resized: %d", steps)
	}
}

func TestPaperLimitRejectsSlippageAndTickCrossingWithoutMutation(t *testing.T) {
	for _, side := range []execution.OrderSide{execution.Buy, execution.Sell} {
		for _, slip := range []string{"0.01", "0"} {
			t.Run(string(side)+slip, func(t *testing.T) {
				a, _ := NewPaperAdapter(decimal.NewFromInt(1000), decimal.RequireFromString("0.01"), decimal.RequireFromString(slip))
				price, limit := "100", "100"
				if slip == "0" {
					if side == execution.Buy {
						price, limit = "100.1", "100.2"
					} else {
						price, limit = "99.9", "99.8"
					}
				}
				o := execution.OrderIntent{ID: "o", Instrument: execution.Instrument{ID: "i", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.NewFromInt(1), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 8}, Side: side, Steps: 1, Limit: decimal.RequireFromString(limit), Observation: execution.ExecutionObservation{Price: decimal.RequireFromString(price), AtMS: 11}, SubmitAtMS: 11}
				r, err := a.Submit(context.Background(), o, "c")
				if err != nil {
					t.Fatal(err)
				}
				if !r.Rejected || len(r.Fills) != 0 || a.Metrics().Fills != 0 {
					t.Fatal("out-of-limit fill accepted")
				}
				snap, err := a.Snapshot(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if snap.Cash != "1000" || len(snap.Positions) != 0 {
					t.Fatalf("rejection mutated account: %+v", snap)
				}
				// A wider limit accepts the same tick-rounded price and charges fees.
				fillPrice := decimal.NewFromInt(101)
				if side == execution.Sell {
					fillPrice = decimal.NewFromInt(99)
				}
				o.Limit = fillPrice
				r, err = a.Submit(context.Background(), o, "accepted")
				if err != nil || r.Rejected || len(r.Fills) != 1 {
					t.Fatalf("valid limit rejected: %+v %v", r, err)
				}
				fee := fillPrice.Mul(decimal.RequireFromString("0.01"))
				if !r.Fills[0].Price.Equal(fillPrice) || !r.Fills[0].Fee.Equal(fee) {
					t.Fatalf("tick/fee changed: %+v", r.Fills[0])
				}
			})
		}
	}
}

func TestPaperLimitRejectionPersistsWithoutLedgerFill(t *testing.T) {
	ctx := context.Background()
	key := execution.AccountKey{VenueSessionIdentity: "paper-limit", Account: "a", SettlementDomain: "USD"}
	store, err := execution.OpenStore(filepath.Join(t.TempDir(), "ledger.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := &execution.AccountRegistry{}
	handle, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	a, _ := NewPaperAdapter(decimal.NewFromInt(1000), decimal.RequireFromString("0.01"), decimal.RequireFromString("0.01"))
	i := execution.Instrument{ID: "i", Version: "v", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.NewFromInt(1), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 8}
	intent := execution.EligibleIntent{ID: "intent", Account: key, Strategy: "s", Lot: "lot", Instrument: i.ID, Side: execution.Buy, Kind: execution.EntryIntent, QuantitySteps: 1, State: execution.Eligible, Conditions: execution.IntentConditions{Limit: decimal.NewFromInt(100)}}
	plan := execution.Plan{ID: "plan", Sequence: 1, DecisionMS: 10, ExpiresMS: 100, Intents: []execution.EligibleIntent{intent}}
	order := execution.OrderIntent{ID: "order", PlanID: plan.ID, Instrument: i, Side: execution.Buy, Steps: 1, Limit: decimal.NewFromInt(100), Observation: execution.ExecutionObservation{Price: decimal.NewFromInt(100), AtMS: 11, ValidUntilMS: 100}, Allocations: []execution.FillAllocation{{ID: "allocation", IntentID: intent.ID, Strategy: intent.Strategy, Lot: intent.Lot, Side: intent.Side, Kind: intent.Kind, Steps: 1}}}
	if err = store.SavePlan(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err = store.PrepareOrder(ctx, order, 11); err != nil {
		t.Fatal(err)
	}
	owner := &execution.OwnerExecutor{Store: store, Handle: handle, Token: handle.Token(), Adapter: a}
	if err = owner.Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Order(ctx, order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != execution.OrderRejected || stored.FilledSteps != 0 || !stored.ReportedFee.IsZero() {
		t.Fatalf("phantom fill: %+v", stored)
	}
	snap, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Lots) != 0 || len(snap.ActualPositions) != 0 || !snap.AccountSettledCash.IsZero() || a.Metrics().Fills != 0 {
		t.Fatalf("rejected order posted ledger fill: %+v", snap)
	}
}
