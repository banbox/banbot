package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestStartupRecoveryPreservesDefinitiveZeroFillRejections(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, outcome := range []string{"unknown-absent", "submit-rejected", "cancel-absent"} {
				t.Run(outcome, func(t *testing.T) {
					s, _, h, path := testStore(t)
					order := planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
					adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, AuthoritativeNotFound: true}}
					if outcome == "unknown-absent" {
						adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
							return SubmitReceipt{}, errors.New("ack lost")
						}
					} else if outcome == "submit-rejected" {
						adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
							return SubmitReceipt{Rejected: true}, nil
						}
					}
					e := executorFor(s, h, adapter)
					if err := e.Send(order.ID, 11); (err != nil) != (outcome == "unknown-absent") {
						t.Fatal(err)
					}
					if outcome == "cancel-absent" {
						adapter.cancel = func(context.Context, string) (bool, error) { return false, errors.New("cancel ack lost") }
						if err := e.Cancel(order.ID, 12); err == nil {
							t.Fatal("cancel loss hidden")
						}
					}
					adapter.query = func(context.Context, string, string) (QueryResult, error) {
						return QueryResult{Authoritative: true}, nil
					}
					borrow := recoveryBorrow(s, h, adapter)
					defer borrow.Release()
					if err := borrow.RecoverPersisted(context.Background()); err != nil {
						t.Fatal("definitive rejection blocked startup", err)
					}
					before, _ := s.Snapshot(context.Background())
					if backend == "SQLite" {
						borrow.Release()
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
						var err error
						s, err = OpenStore(path, s.key)
						if err != nil {
							t.Fatal(err)
						}
						defer s.Close()
						borrow = recoveryBorrow(s, h, adapter)
						defer borrow.Release()
					}
					for repeat := 0; repeat < 2; repeat++ {
						if err := borrow.RecoverPersisted(context.Background()); err != nil {
							t.Fatal("confirmed zero-fill rejection failed on repeat/restart", err)
						}
						stored, err := s.Order(context.Background(), order.ID)
						after, snapshotErr := s.Snapshot(context.Background())
						if err != nil || snapshotErr != nil || stored.State != OrderRejected || stored.FilledSteps != 0 || stored.Attempt < 1 || before.Checkpoint != after.Checkpoint {
							t.Fatal("rejection replay changed durable facts", stored, before.Checkpoint, after.Checkpoint, err, snapshotErr)
						}
					}
					calls := adapter.calls()
					want := 1
					if outcome == "unknown-absent" {
						want = 2
					}
					if outcome == "cancel-absent" {
						want = 5
					}
					if len(calls) != want {
						t.Fatal("recovery resent or queried a no-created order", calls)
					}
					if outcome == "cancel-absent" {
						// A known venue identity still requires verified absence;
						// Rejected alone cannot certify unavailable venue history.
						adapter.capabilities.AuthoritativeNotFound = false
						if err := borrow.RecoverPersisted(context.Background()); err == nil {
							t.Fatal("unverified historical absence accepted")
						}
						adapter.capabilities.AuthoritativeNotFound = true
						adapter.query = func(context.Context, string, string) (QueryResult, error) { return QueryResult{}, nil }
						if err := borrow.RecoverPersisted(context.Background()); err == nil {
							t.Fatal("inconclusive historical absence accepted")
						}
						after, _ := s.Snapshot(context.Background())
						if after.Checkpoint != before.Checkpoint {
							t.Fatal("unverified absence changed rejection facts")
						}
					}
				})
			}
		})
	}
}

func recoveryBorrow(s *Store, h *AccountHandle, adapter *fakeExecutionAdapter) *SharedAccountBorrow {
	service := &SharedAccount{ctx: context.Background(), owner: h, store: s, executor: executorFor(s, h, adapter), opts: SharedExecutionOptions{Adapter: adapter}, ready: true}
	return service.Borrow()
}

func TestStartupRecoversTerminalHistoryInPages(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
			e := executorFor(s, h, adapter)
			for n := 0; n < 65; n++ {
				id := fmt.Sprintf("order-%03d", n)
				order := planOrder(t, s, id, int64(n+1), Buy, EntryIntent, map[string]int64{"a": 10})
				if err := e.Send(id, 11); err != nil {
					t.Fatal(err)
				}
				steps := int64(10)
				if n%2 == 0 {
					steps = 2
				}
				if err := e.ApplyFill(FillReport{EventID: "initial-" + id, OrderID: id, Steps: steps, Price: intentPrice("100"), Fee: intentPrice("0.02"), AtMS: 12}); err != nil {
					t.Fatal(err)
				}
				if steps == 2 {
					if err := s.commit(context.Background(), func(tx *storeTxn) error { return s.setOrderState(tx, order.ID, OrderCanceled) }); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Legacy imported orders can have a venue identity without a
			// locally numbered submit attempt; they still need recovery.
			if err := s.commit(context.Background(), func(tx *storeTxn) error {
				_, err := tx.Exec(opUpdateOrderAttempt, 0, "0", s.accountID, "order-000")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			planOrder(t, s, "unsent", 66, Buy, EntryIntent, map[string]int64{"a": 10})
			first, err := s.recoveryOrdersAfter(context.Background(), "")
			if err != nil || len(first) != 64 || first[0] != "order-000" || first[63] != "order-063" {
				t.Fatal("history page must be bounded and stable", first, err)
			}
			adapter.query = func(_ context.Context, _, exchange string) (QueryResult, error) {
				id := exchange[len("exchange-"):]
				steps := int64(10)
				var n int
				fmt.Sscanf(id, "order-%d", &n)
				if n%2 == 0 {
					steps = 4
				}
				// Historical status may still say open; a known canceled order
				// must remain terminal while its late cumulative fill is applied.
				return QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: exchange, Fills: []FillReport{{EventID: "offline-" + id, OrderID: id, Steps: steps, Price: intentPrice("100"), Cost: ledgerInstrument().Notional(steps, intentPrice("100")), Fee: intentPrice("0.05"), Cumulative: true, AtMS: 14}}}}, nil
			}
			borrow := recoveryBorrow(s, h, adapter)
			defer borrow.Release()
			var checkpoint int64
			for repeat := 0; repeat < 2; repeat++ {
				if err := borrow.RecoverPersisted(context.Background()); err != nil {
					t.Fatal(err)
				}
				totals, err := s.StrategyTotals(context.Background(), "a")
				if err != nil || !totals.Fees.Equal(intentPrice("3.25")) {
					t.Fatal("terminal offline fees omitted or replayed", totals, err)
				}
				last, _ := s.Order(context.Background(), "order-064")
				if last.FilledSteps != 4 || last.State != OrderCanceled {
					t.Fatal("last page omitted late fill", last)
				}
				snapshot, err := s.Snapshot(context.Background())
				if err != nil || repeat > 0 && snapshot.Checkpoint != checkpoint {
					t.Fatal("repeated terminal query changed committed highwater", checkpoint, snapshot.Checkpoint, err)
				}
				checkpoint = snapshot.Checkpoint
			}
			if calls := adapter.calls(); len(calls) != 65*3 {
				t.Fatal("recovery omitted historical orders or sent unsent order", len(calls))
			}
		})
	}
}

func TestTerminalRecoveryRejectsMissingOrStaleSnapshot(t *testing.T) {
	for _, failure := range []string{"absent", "incomplete", "untrusted", "identity", "empty", "regressed", "cost", "batch"} {
		t.Run(failure, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			order := planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
			adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, AuthoritativeNotFound: true}}
			e := executorFor(s, h, adapter)
			if err := e.Send(order.ID, 11); err != nil {
				t.Fatal(err)
			}
			if err := e.ApplyFill(FillReport{EventID: "initial", OrderID: order.ID, Steps: 2, Price: intentPrice("100"), AtMS: 12}); err != nil {
				t.Fatal(err)
			}
			if err := s.commit(context.Background(), func(tx *storeTxn) error { return s.setOrderState(tx, order.ID, OrderCanceled) }); err != nil {
				t.Fatal(err)
			}
			before, _ := s.Snapshot(context.Background())
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				result := QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "exchange-order", Fills: []FillReport{{EventID: "stale", OrderID: order.ID, Steps: 2, Price: intentPrice("100"), Cost: intentPrice("20"), Cumulative: true, AtMS: 13}}}}
				switch failure {
				case "absent":
					result.Found = false
				case "incomplete":
					result.Complete = false
				case "untrusted":
					result.Authoritative = false
				case "identity":
					result.Receipt.ExchangeID = "foreign"
				case "empty":
					result.Receipt.Fills = nil
				case "regressed":
					result.Receipt.Fills[0].Steps = 1
					result.Receipt.Fills[0].Cost = intentPrice("10")
				case "cost":
					result.Receipt.Fills[0].Cost = intentPrice("21")
				case "batch":
					result.Receipt.Fills[0].Fee = intentPrice("1")
					foreign := result.Receipt.Fills[0]
					foreign.OrderID = "foreign"
					result.Receipt.Fills = append(result.Receipt.Fills, foreign)
				}
				return result, nil
			}
			borrow := recoveryBorrow(s, h, adapter)
			defer borrow.Release()
			if err := borrow.RecoverPersisted(context.Background()); err == nil {
				t.Fatal("incomplete historical recovery accepted")
			}
			if borrow.service.ready {
				t.Fatal("failed startup recovery allowed trading")
			}
			after, _ := s.Snapshot(context.Background())
			stored, _ := s.Order(context.Background(), order.ID)
			if stored.State != OrderCanceled || stored.ExchangeID != "exchange-order" || stored.FilledSteps != 2 || after.Checkpoint != before.Checkpoint || !after.AccountSettledCash.Equal(before.AccountSettledCash) {
				t.Fatal("failed recovery mutated terminal facts", stored, before.Checkpoint, after.Checkpoint)
			}
		})
	}
}
