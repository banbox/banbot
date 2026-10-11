package execution

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestTerminalSettlementPlanAndIntentExpiry(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore"} {
		for _, tc := range []struct {
			name       string
			at, expiry int64
			orderState RealOrderState
			wantBlock  string
		}{
			{"canceled-before-plan-expiry", 999, 0, OrderCanceled, "pending terminal intent"},
			{"canceled-at-plan-expiry", 1000, 0, OrderCanceled, ""},
			{"canceled-after-plan-expiry", 1001, 0, OrderCanceled, ""},
			{"intent-before-expiry", 29, 30, OrderCanceled, "pending terminal intent"},
			{"intent-at-expiry", 30, 30, OrderCanceled, ""},
			{"unknown-after-plan-expiry", 1001, 0, OrderUnknown, "pending or uncertain"},
			{"sending-after-plan-expiry", 1001, 0, OrderSending, "pending or uncertain"},
			{"cancel-pending-after-plan-expiry", 1001, 0, OrderCancelPending, "pending or uncertain"},
		} {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				key := testIntent(Buy).Account
				var s *Store
				var err error
				if backend == "SQLite" {
					s, err = OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "ledger.db"), key, t.TempDir())
				} else {
					s, err = NewMemoryStore(key)
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				intent := EligibleIntent{ID: "terminal-intent", Account: key, Strategy: "a", Lot: "lot-a", Instrument: "BTC", Kind: EntryIntent, Side: Buy, QuantitySteps: 1, State: Eligible, Conditions: IntentConditions{ExpiresAtMS: tc.expiry}}
				plan := Plan{ID: "terminal-plan", Sequence: 1, DecisionMS: 10, ExpiresMS: 1000, Intents: []EligibleIntent{intent}}
				if err := s.SavePlan(ctx, plan); err != nil {
					t.Fatal(err)
				}
				order := OrderIntent{ID: "terminal-order", PlanID: plan.ID, Instrument: ledgerInstrument(), Side: Buy, Steps: 1, Observation: ExecutionObservation{Price: intentPrice("100"), AtMS: 10, ValidUntilMS: 1000}, Allocations: []FillAllocation{{ID: "a", IntentID: intent.ID, Strategy: intent.Strategy, Lot: intent.Lot, Side: Buy, Kind: EntryIntent, Steps: 1}}}
				if err := s.PrepareOrder(ctx, order, 10); err != nil {
					t.Fatal(err)
				}
				// Exercise the durable order guard independently of intent expiry.
				// A canceled prepared order leaves its intent available for replanning.
				if err := s.commit(ctx, func(tx *storeTxn) error { return s.setOrderState(tx, order.ID, tc.orderState) }); err != nil {
					t.Fatal(err)
				}
				event := TerminalSettlement{Source: "terminal", SourceVersion: "v1", Instrument: ledgerInstrument(), CashValue: intentPrice("0"), EventMS: 20, PublishedMS: 21, ReceivedMS: 22, AtMS: tc.at}
				applied, err := s.ApplyTerminalSettlement(ctx, event)
				if tc.wantBlock == "" {
					if err != nil || !applied {
						t.Fatalf("expired intent still blocks settlement: applied=%v err=%v", applied, err)
					}
				} else if err == nil || applied || !strings.Contains(err.Error(), tc.wantBlock) {
					t.Fatalf("want %q guard: applied=%v err=%v", tc.wantBlock, applied, err)
				}
				settled, err := s.TerminalSettled(ctx, event.Instrument)
				if err != nil || settled != applied {
					t.Fatalf("settlement marker disagrees with transaction: settled=%v applied=%v err=%v", settled, applied, err)
				}
			})
		}
	}
}
