package execution

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTerminalSettlementReclassificationAfterReversal(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		for _, value := range []string{"0", "60"} {
			for _, external := range []bool{false, true} {
				name := backend + "/" + value + "/strategy"
				if external {
					name = backend + "/" + value + "/external"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					key := testIntent(Buy).Account
					path, leases := filepath.Join(t.TempDir(), "execution.db"), t.TempDir()
					var s *Store
					var err error
					switch backend {
					case "SQLite":
						s, err = OpenStoreWithLeaseDir(path, key, leases)
					case "MemoryStore":
						s, err = NewMemoryStore(key)
					case "MemoryHistory":
						s, err = NewMemoryStoreWithHistory(key, path)
					}
					if err != nil {
						t.Fatal(err)
					}
					defer s.Close()
					registry := &AccountRegistry{}
					h, err := registry.Acquire(key)
					if err != nil {
						t.Fatal(err)
					}
					defer registry.Close()
					e, _ := openMixed(t, s, h)
					// The internal cross opened b at 100. Reversing the actual
					// account at 110 leaves b's historical basis unchanged.
					plan, err := e.PrepareRebalance(domainRequest("exit-a", 2, 20, "110", 0, -6))
					if err != nil {
						t.Fatal(err)
					}
					for n, id := range plan.OrderIDs {
						if err := e.Send(id, 21+int64(n)); err != nil {
							t.Fatal(err)
						}
						order, err := s.Order(ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						if err := e.ApplyFill(FillReport{EventID: "fill-" + id, OrderID: id, Steps: order.Intent.Steps, Price: intentPrice("110"), AtMS: 21 + int64(n)}); err != nil {
							t.Fatal(err)
						}
					}
					if external {
						if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "manual", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 2, Price: intentPrice("120"), AtMS: 23}); err != nil {
							t.Fatal(err)
						}
					}
					before, err := s.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					assertNAVConservation(t, before, "120")
					if before.PnLReclassification.IsZero() {
						t.Fatal("scenario did not create a historical basis difference")
					}
					if backend == "SQLite" {
						s.Close()
						s, err = OpenStoreWithLeaseDir(path, key, leases)
						if err != nil {
							t.Fatal(err)
						}
						defer s.Close()
					}
					event := TerminalSettlement{Source: "terminal", SourceVersion: "v1", Instrument: ledgerInstrument(), CashValue: intentPrice(value), EventMS: 24, PublishedMS: 25, ReceivedMS: 26, AtMS: 26}
					if applied, err := s.ApplyTerminalSettlement(ctx, event); err != nil || !applied {
						t.Fatal(applied, err)
					}
					after, err := s.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					assertNAVConservation(t, after, value)
					if !after.PnLReclassification.IsZero() || len(after.Lots) != 0 || len(after.ActualPositions) != 0 {
						t.Fatal("terminal closure must clear the basis bridge and all exposure", after)
					}
					for _, position := range after.ExternalPositions {
						if position.SignedSteps != 0 || !position.CostBasis.IsZero() {
							t.Fatal("external terminal position remained open", position)
						}
					}
					expected := before.AccountSettledCash
					for _, position := range before.ActualPositions {
						expected = expected.Add(position.Unrealized(event.CashValue))
					}
					if !after.AccountSettledCash.Equal(expected) {
						t.Fatal("terminal cash differs from pre-settlement equity", expected, after.AccountSettledCash)
					}
					if backend == "SQLite" {
						s.Close()
						s, err = OpenStoreWithLeaseDir(path, key, leases)
						if err != nil {
							t.Fatal(err)
						}
						defer s.Close()
					}
					event.ReceivedMS, event.AtMS = 30, 31
					if applied, err := s.ApplyTerminalSettlement(ctx, event); err != nil || applied {
						t.Fatal("retry after reopen", applied, err)
					}
					retried, err := s.Snapshot(ctx)
					if err != nil || !reflect.DeepEqual(after, retried) {
						t.Fatal("retry changed committed state", retried, err)
					}
				})
			}
		}
	}
}
