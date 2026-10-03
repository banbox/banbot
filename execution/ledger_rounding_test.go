package execution

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/shopspring/decimal"
)

func TestFillCostRoundingFiveContributors(t *testing.T) {
	for _, fixture := range []struct {
		name       string
		cumulative bool
		price      string
		cost       string
		lastBasis  string
	}{
		{"incremental", false, "0.6", "3", "0"},
		{"cumulative", true, "0.6", "3", "0"},
		{"incremental-subunit", false, "0.7", "3.5", "0.5"},
		{"cumulative-subunit", true, "0.7", "3.5", "0.5"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			s, err := OpenStoreWithLeaseDir(filepath.Join(dir, "rounding.db"), testIntent(Buy).Account, filepath.Join(dir, "lease"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			instrument := ledgerInstrument()
			instrument.QuantityStep = decimal.NewFromInt(1)
			instrument.PriceTick = intentPrice("0.1")
			instrument.MoneyScale = 0
			plan := Plan{ID: "rounding-plan", Sequence: 1, DecisionMS: 10, ExpiresMS: 1000}
			order := OrderIntent{ID: "rounding-order", PlanID: plan.ID, Instrument: instrument, Side: Buy, Steps: 5, Observation: ExecutionObservation{Price: intentPrice(fixture.price), AtMS: 10, ValidUntilMS: 1000, Bar: 5}}
			// Deliberately unsorted: residual assignment must use allocation IDs.
			for _, id := range []string{"e", "b", "d", "a", "c"} {
				intent := EligibleIntent{ID: VirtualIntentID("intent-" + id), Account: s.key, Strategy: StrategyID(id), Lot: VirtualLotID("lot-" + id), Instrument: instrument.ID, Kind: EntryIntent, Side: Buy, QuantitySteps: 1, State: Eligible, Conditions: IntentConditions{CreatedBar: 5}}
				plan.Intents = append(plan.Intents, intent)
				order.Allocations = append(order.Allocations, FillAllocation{ID: id, IntentID: intent.ID, Strategy: intent.Strategy, Lot: intent.Lot, Side: Buy, Kind: EntryIntent, Steps: 1})
			}
			if err := s.SavePlan(ctx, plan); err != nil {
				t.Fatal(err)
			}
			if err := s.PrepareOrder(ctx, order, 10); err != nil {
				t.Fatal(err)
			}
			// This ledger fixture acknowledges locally; it performs no venue IO.
			if err := s.commit(ctx, func(tx *storeTxn) error { return s.setOrderState(tx, order.ID, OrderAcknowledged) }); err != nil {
				t.Fatal(err)
			}
			fill := FillReport{EventID: "rounding-fill", OrderID: order.ID, Steps: 5, Price: intentPrice(fixture.price), Cost: intentPrice(fixture.cost), Fee: intentPrice("0.3"), Cumulative: fixture.cumulative, AtMS: 12}
			if applied, err := s.ApplyFill(ctx, fill); err != nil || !applied {
				t.Fatal(applied, err)
			}
			snapshot, err := s.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Lots) != 5 || len(snapshot.ActualPositions) != 1 {
				t.Fatalf("unexpected positions: %+v", snapshot)
			}
			total := decimal.Zero
			for _, lot := range snapshot.Lots {
				want := decimal.Zero
				if lot.Strategy <= "c" {
					want = decimal.NewFromInt(1)
				} else if lot.Strategy == "e" {
					want = intentPrice(fixture.lastBasis)
				}
				if lot.CostBasis.IsNegative() || !lot.CostBasis.Equal(want) || lot.SignedSteps != 1 {
					t.Errorf("strategy %s: basis=%s steps=%d, want basis=%s steps=1", lot.Strategy, lot.CostBasis, lot.SignedSteps, want)
				}
				total = total.Add(lot.CostBasis)
			}
			actual := snapshot.ActualPositions[0]
			if !total.Equal(fill.Cost) || !actual.CostBasis.Equal(fill.Cost) || actual.SignedSteps != 5 || !actual.Fees.Equal(fill.Fee) {
				t.Fatalf("cost/quantity/fee not conserved: total=%s actual=%+v", total, actual)
			}
			cash := snapshot.UnassignedCash
			for _, value := range snapshot.SyntheticStrategyCash {
				cash = cash.Add(value)
			}
			if !snapshot.AccountSettledCash.Equal(fill.Fee.Neg()) || !cash.Equal(snapshot.AccountSettledCash) || !snapshot.UnassignedCash.Equal(fill.Fee.Neg()) || !snapshot.PnLReclassification.IsZero() {
				t.Fatalf("cash/fee residual changed: %+v", snapshot)
			}
			stored, err := s.Order(ctx, order.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(stored.Intent.Allocations, order.Allocations) || stored.FilledSteps != 5 || !stored.ReportedCost.Equal(fill.Cost) || !stored.ReportedFee.Equal(fill.Fee) {
				t.Fatalf("frozen allocation/highwater changed: %+v", stored)
			}
			for _, filled := range stored.AllocationFilled {
				if filled != 1 {
					t.Fatalf("quantity redistributed: %+v", stored.AllocationFilled)
				}
			}
			if applied, err := s.ApplyFill(ctx, fill); err != nil || applied {
				t.Fatal("duplicate changed ledger", applied, err)
			}
			if fixture.cumulative {
				fill.EventID = "rounding-same-highwater"
				if applied, err := s.ApplyFill(ctx, fill); err != nil || applied {
					t.Fatal("same cumulative highwater changed ledger", applied, err)
				}
			}
			after, err := s.Snapshot(ctx)
			if err != nil || !reflect.DeepEqual(after, snapshot) {
				t.Fatal("dedup changed snapshot", after, err)
			}
		})
	}
}

func TestLotBasisRoundingPartialCloseAndReverse(t *testing.T) {
	for _, openingSide := range []OrderSide{Buy, Sell} {
		closingSide := Sell
		sign := int64(1)
		if openingSide == Sell {
			closingSide, sign = Buy, -1
		}
		t.Run(string(openingSide), func(t *testing.T) {
			instrument := ledgerInstrument()
			instrument.MoneyScale = 0
			t.Run("partial-close", func(t *testing.T) {
				lot := VirtualLot{Instrument: instrument, SignedSteps: 3 * sign, CostBasis: intentPrice("0.9")}
				first, err := updateLot(&lot, closingSide, ExitIntent, 2, intentPrice("1.2"), intentPrice("0.1"))
				if err != nil {
					t.Fatal(err)
				}
				consumed := intentPrice("0.9").Sub(lot.CostBasis)
				if lot.CostBasis.IsNegative() || consumed.IsNegative() || !consumed.Add(lot.CostBasis).Equal(intentPrice("0.9")) || !lot.CostBasis.Equal(intentPrice("0.9")) || lot.SignedSteps != sign {
					t.Errorf("partial basis exceeded original: %+v consumed=%s", lot, consumed)
				}
				last, err := updateLot(&lot, closingSide, ExitIntent, 1, intentPrice("0.6"), intentPrice("0.2"))
				if err != nil {
					t.Fatal(err)
				}
				want := intentPrice("0.9").Mul(decimal.NewFromInt(sign))
				if lot.SignedSteps != 0 || !lot.CostBasis.IsZero() || !first.Add(last).Equal(want) || !lot.RealizedPnL.Equal(want) || !lot.Fees.Equal(intentPrice("0.3")) {
					t.Fatalf("partial/final close not conserved: %+v", lot)
				}
			})
			t.Run("reverse", func(t *testing.T) {
				lot := VirtualLot{Instrument: instrument, SignedSteps: 2 * sign, CostBasis: intentPrice("0.6")}
				realized, err := updateLot(&lot, closingSide, EntryIntent, 3, intentPrice("0.9"), intentPrice("0.1"))
				if err != nil {
					t.Fatal(err)
				}
				proceeds := intentPrice("0.6").Add(realized.Mul(decimal.NewFromInt(sign)))
				if lot.CostBasis.IsNegative() || proceeds.IsNegative() || !proceeds.Add(lot.CostBasis).Equal(intentPrice("0.9")) || !lot.CostBasis.Equal(intentPrice("0.9")) || lot.SignedSteps != -sign {
					t.Errorf("reversal proceeds/open basis not conserved: %+v proceeds=%s", lot, proceeds)
				}
				if _, err := updateLot(&lot, openingSide, ExitIntent, 1, intentPrice("0.3"), intentPrice("0.2")); err != nil {
					t.Fatal(err)
				}
				if lot.SignedSteps != 0 || !lot.CostBasis.IsZero() || !lot.RealizedPnL.IsZero() || !lot.Fees.Equal(intentPrice("0.3")) {
					t.Fatalf("reversal/final close not conserved: %+v", lot)
				}
			})
		})
	}
}
