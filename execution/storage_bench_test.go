package execution

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// The seed retains terminal orders, closed lots and committed audit events.
// Timed operations exercise public account APIs with a fixed active set.
func benchmarkStore(b *testing.B, backend string, history int) (*Store, string, int64) {
	b.Helper()
	key := testIntent(Buy).Account
	var store *Store
	var err error
	if backend == "MemoryStore" {
		store, err = NewMemoryStore(key)
	} else {
		store, err = OpenStoreWithLeaseDir(filepath.Join(b.TempDir(), "execution.db"), key, b.TempDir())
	}
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	ctx := context.Background()
	instrument := ledgerInstrument()
	err = store.commit(ctx, func(tx *storeTxn) error {
		for n := range history {
			id := fmt.Sprintf("history-%06d", n)
			planBody, _ := payload(Plan{ID: id, Sequence: int64(n), DecisionMS: 10, ExpiresMS: 1000})
			if _, err := tx.Exec(opInsertPlan, store.accountID, id, n, planBody); err != nil {
				return err
			}
			orderBody, _ := payload(OrderIntent{ID: id, PlanID: id, Instrument: instrument, Side: Buy, Steps: 1})
			if _, err := tx.Exec(opInsertOrder, store.accountID, id, id, orderBody, "client-"+id, string(OrderFilled)); err != nil {
				return err
			}
			lotBody, _ := payload(VirtualLot{Strategy: "a", ID: VirtualLotID(id), Instrument: instrument})
			if _, err := tx.Exec(opPutLot, store.accountID, "a", id, 0, lotBody); err != nil {
				return err
			}
			if _, err := tx.Exec(opInsertEvent, store.accountID, id, "ArchivedAudit", `{}`); err != nil {
				return err
			}
			if err := store.commitAccountEvent(tx, id, intentPrice("0"), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	intent := testIntent(Buy)
	intent.ID, intent.Strategy, intent.Lot, intent.Instrument = "current-intent", "a", "active", instrument.ID
	intent.QuantitySteps, intent.State, intent.Conditions.CreatedBar = 1, Eligible, 0
	plan := Plan{ID: "current", Sequence: int64(history), DecisionMS: 10, ExpiresMS: 1000, Intents: []EligibleIntent{intent}}
	if err := store.SavePlan(ctx, plan); err != nil {
		b.Fatal(err)
	}
	order := OrderIntent{ID: "current", PlanID: plan.ID, Instrument: instrument, Side: Buy, Steps: 1, Observation: ExecutionObservation{Price: intentPrice("100"), AtMS: 10, ValidUntilMS: 1000}, Allocations: []FillAllocation{{ID: "active", IntentID: intent.ID, Strategy: "a", Lot: "active", Side: Buy, Kind: intent.Kind, Steps: 1}}}
	if err := store.PrepareOrder(ctx, order, 10); err != nil {
		b.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil || len(snapshot.Orders) != 1 {
		b.Fatal("invalid fixed-active-set fixture", snapshot, err)
	}
	return store, order.ID, snapshot.Checkpoint
}

func BenchmarkExecutionStoreRetainedHistory(b *testing.B) {
	for _, backend := range []string{"SQLite", "MemoryStore"} {
		for _, history := range []int{100, 10000} {
			b.Run(fmt.Sprintf("%s/history_%d", backend, history), func(b *testing.B) {
				store, orderID, checkpoint := benchmarkStore(b, backend, history)
				ctx := context.Background()
				b.Run("Order", func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if _, err := store.Order(ctx, orderID); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("LatestSequence", func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if _, err := store.LatestPlanSequence(ctx); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("ActiveSnapshot", func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if _, err := store.Snapshot(ctx); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("EventTail16", func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if _, err := store.EventsAfter(ctx, checkpoint-16, 16); err != nil {
							b.Fatal(err)
						}
					}
				})
				var nextAttempt int64
				b.Run("AttemptCommit", func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						nextAttempt++
						err := store.commit(ctx, func(tx *storeTxn) error {
							if _, err := tx.Exec(opUpdateOrderAttempt, nextAttempt, "1", store.accountID, orderID); err != nil {
								return err
							}
							return store.recordOrderAttempt(tx, OrderAttempt{OrderID: orderID, Number: nextAttempt, Kind: SubmitAttempt, Generation: 1, AtMS: 11, Phase: AttemptStarted, Result: "Sending"})
						})
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}
