package execution

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestLotEntryTimeReadsCommittedScopedTypedFills(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			store, _, _, _ := testStore(t)
			ctx := context.Background()
			entries := []LedgerEntry{
				{EventID: "entry", Kind: "ExchangeFill", Strategy: "a", Lot: "lot", QuantityDelta: 2, AtMS: 20},
				{EventID: "fee", Kind: "FeeCorrection", Strategy: "a", Lot: "lot", AtMS: 90},
				{EventID: "exit", Kind: "ExchangeFill", Strategy: "a", Lot: "lot", QuantityDelta: -1, AtMS: 40},
				{EventID: "internal", Kind: "InternalFill", Strategy: "a", Lot: "lot", QuantityDelta: 1, AtMS: 30},
				{EventID: "foreign", Kind: "ExchangeFill", Strategy: "b", Lot: "lot", QuantityDelta: 1, AtMS: 99},
				{EventID: "genesis", Kind: "LegacyGenesis", Strategy: "a", Lot: "lot", QuantityDelta: 7, AtMS: 120},
				{EventID: "adjustment", Kind: "Reconciliation", Strategy: "a", Lot: "lot", QuantityDelta: -9, AtMS: 130},
			}
			for _, entry := range entries {
				if err := store.commit(ctx, func(tx *storeTxn) error {
					if _, err := store.recordEvent(tx, entry.EventID, entry.Kind, "{}"); err != nil {
						return err
					}
					return store.ledger(tx, entry)
				}); err != nil {
					t.Fatal(err)
				}
			}
			for _, check := range []struct {
				strategy StrategyID
				side     OrderSide
				want     int64
				steps    int64
			}{{"a", Buy, 30, 3}, {"a", Sell, 40, 1}, {"b", Buy, 99, 1}, {"missing", Buy, 0, 0}} {
				got, err := store.LotEntryTime(ctx, check.strategy, "lot", check.side)
				if err != nil || got != check.want {
					t.Fatal(check, got, err)
				}
				steps, err := store.LotEntrySteps(ctx, check.strategy, "lot", check.side)
				if err != nil || steps != check.steps {
					t.Fatal("entry quantity included non-fill postings", check, steps, err)
				}
			}
			failure := errors.New("rollback")
			err := store.atomically(ctx, func(scope context.Context) error {
				return store.commit(scope, func(tx *storeTxn) error {
					entry := LedgerEntry{EventID: "rollback", Kind: "InternalFill", Strategy: "a", Lot: "lot", QuantityDelta: 1, AtMS: 100}
					if _, err := store.recordEvent(tx, entry.EventID, entry.Kind, "{}"); err != nil {
						return err
					}
					if err := store.ledger(tx, entry); err != nil {
						return err
					}
					return failure
				})
			})
			if !errors.Is(err, failure) {
				t.Fatal(err)
			}
			got, err := store.LotEntryTime(ctx, "a", "lot", Buy)
			if err != nil || got != 30 {
				t.Fatal("uncommitted posting leaked into entry clock", got, err)
			}
			if _, err := store.LotEntryTime(ctx, "a", "lot", OrderSide("invalid")); err == nil {
				t.Fatal("invalid entry side accepted")
			}
		})
	}
}

func TestLotEntryStepsRejectsOverflow(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			store, _, _, _ := testStore(t)
			ctx := context.Background()
			for _, side := range []OrderSide{Buy, Sell} {
				for _, id := range []string{"one", "two"} {
					steps := int64(math.MaxInt64)
					if side == Sell {
						steps = -steps
					}
					entry := LedgerEntry{EventID: string(side) + id, Kind: "ExchangeFill", Strategy: "a", Lot: VirtualLotID(side), QuantityDelta: steps, AtMS: 10}
					if err := store.commit(ctx, func(tx *storeTxn) error {
						if _, err := store.recordEvent(tx, entry.EventID, entry.Kind, "{}"); err != nil {
							return err
						}
						return store.ledger(tx, entry)
					}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := store.LotEntrySteps(ctx, "a", VirtualLotID(side), side); err == nil {
					t.Fatal("entry quantity overflow silently accepted", side)
				}
			}
		})
	}
}
