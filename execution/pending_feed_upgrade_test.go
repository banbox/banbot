package execution

import (
	"context"
	"math"
	"testing"
)

func TestPendingFeedUpgradePreservesExecutionStateAndRemainingLifetime(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, existingBook := range []bool{false, true} {
			for _, existingFeeds := range []bool{false, true} {
				name := "SQL"
				if memory {
					name = "Memory"
				}
				if existingBook {
					name += "/ExistingBook"
				}
				if existingFeeds {
					name += "/ExistingFeeds"
				}
				t.Run(name, func(t *testing.T) {
					account, _ := strategyService(t, memory)
					store := account.service.store
					ctx := context.Background()
					definition := PendingDefinition{Order: *pendingTestOrder(store, "a"), Instrument: ledgerInstrument()}
					definition.Order.Entry.Conditions.StopBars = 2
					if existingBook {
						definition.Order.Desired = 7
						definition.Order.Entry.FilledSteps = 3
						definition.Order.LastProtection = TakeProfitProtection
						saveTestPendingBook(t, store, []PendingDefinition{definition})
					}
					if existingFeeds {
						if err := store.savePendingFeeds(ctx, map[PendingFeedID]PendingFeedProgress{"1/1m": {EndMS: 20, Bars: 7}}); err != nil {
							t.Fatal(err)
						}
					}
					definition.Order.Desired, definition.Order.Entry.FilledSteps, definition.Order.LastProtection = 0, 0, ""
					definition.Order.Entry.Conditions.StopBars = 0
					bindings := []LegacyPendingFeedBinding{{Strategy: definition.Order.Strategy, Lot: definition.Order.Lot, Expiry: PendingFeedExpiry{Feed: "1/1m", ExpiresAtBar: 4}}}
					legacyFeeds := map[PendingFeedID]PendingFeedProgress{"1/1m": {EndMS: 12, Bars: 3}}
					if err := account.WithState(func(s *SharedAccount) error {
						return s.SeedPendingBookWithFeeds(ctx, []PendingDefinition{definition}, legacyFeeds, bindings)
					}); err != nil {
						t.Fatal(err)
					}
					book, err := store.LoadPendingBook(ctx)
					if err != nil || len(book) != 1 {
						t.Fatal(book, err)
					}
					wantBars := int64(3)
					if existingFeeds {
						wantBars = 7
					}
					if book[0].Order.EntryFeedExpiry == nil || book[0].Order.EntryFeedExpiry.ExpiresAtBar != wantBars+1 || book[0].Order.Entry.Conditions.StopBars != 0 {
						t.Fatal("remaining feed lifetime lost", book)
					}
					if existingBook && (book[0].Order.Desired != 7 || book[0].Order.Entry.FilledSteps != 3 || book[0].Order.LastProtection != TakeProfitProtection) {
						t.Fatal("newer execution state overwritten", book)
					}
					bindings[0].Expiry.ExpiresAtBar = 100
					legacyFeeds["1/1m"] = PendingFeedProgress{EndMS: 99, Bars: 10}
					if err := account.WithState(func(s *SharedAccount) error {
						return s.SeedPendingBookWithFeeds(ctx, []PendingDefinition{definition}, legacyFeeds, bindings)
					}); err != nil {
						t.Fatal(err)
					}
					retried, err := store.LoadPendingBook(ctx)
					feeds, feedErr := store.LoadPendingFeeds(ctx)
					if err != nil || feedErr != nil || feeds["1/1m"].Bars != wantBars || retried[0].Order.EntryFeedExpiry.ExpiresAtBar != wantBars+1 {
						t.Fatal("repeat upgrade reset counter/deadline", retried, feeds, err, feedErr)
					}
				})
			}
		}
	}
}

func TestPendingFeedUpgradeRejectsOverflowWithoutChangingBook(t *testing.T) {
	account, _ := strategyService(t, false)
	store := account.service.store
	ctx := context.Background()
	definition := PendingDefinition{Order: *pendingTestOrder(store, "a"), Instrument: ledgerInstrument()}
	saveTestPendingBook(t, store, []PendingDefinition{definition})
	if err := store.savePendingFeeds(ctx, map[PendingFeedID]PendingFeedProgress{"1/1m": {EndMS: 10, Bars: math.MaxInt64}}); err != nil {
		t.Fatal(err)
	}
	bindings := []LegacyPendingFeedBinding{{Strategy: definition.Order.Strategy, Lot: definition.Order.Lot, Expiry: PendingFeedExpiry{Feed: "1/1m", ExpiresAtBar: 1}}}
	if err := account.WithState(func(s *SharedAccount) error {
		return s.SeedPendingBookWithFeeds(ctx, []PendingDefinition{definition}, nil, bindings)
	}); err == nil {
		t.Fatal("overflow migration accepted")
	}
	book, err := store.LoadPendingBook(ctx)
	if err != nil || book[0].Order.EntryFeedExpiry != nil {
		t.Fatal("failed migration changed book", book, err)
	}
}

func TestPendingFeedSeedPreservesAlreadyExecutionOwnedDeadline(t *testing.T) {
	account, _ := strategyService(t, false)
	ctx := context.Background()
	store := account.service.store
	definition := PendingDefinition{Order: *pendingTestOrder(store, "a"), Instrument: ledgerInstrument()}
	definition.Order.EntryFeedExpiry = &PendingFeedExpiry{Feed: "1/1m", ExpiresAtBar: 2}
	if err := store.savePendingFeeds(ctx, map[PendingFeedID]PendingFeedProgress{"1/1m": {EndMS: 10, Bars: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := account.WithState(func(s *SharedAccount) error {
		return s.SeedPendingBookWithFeeds(ctx, []PendingDefinition{definition}, nil, nil)
	}); err != nil {
		t.Fatal(err)
	}
	book, err := store.LoadPendingBook(ctx)
	if err != nil || len(book) != 1 || book[0].Order.EntryFeedExpiry == nil || book[0].Order.EntryFeedExpiry.ExpiresAtBar != 2 {
		t.Fatal("execution deadline was rebased as a legacy counter", book, err)
	}
}
