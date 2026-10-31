package execution

import (
	"context"
	"math"
	"strings"
	"testing"
)

func TestPendingFeedObservationFiltersAndIsolatesProgress(t *testing.T) {
	borrow, _ := strategyService(t, true)
	borrow.service.ready = false
	ctx := context.Background()
	for _, step := range []struct {
		observation PendingFeedObservation
		accepted    bool
	}{
		{PendingFeedObservation{Feed: "1/1m", EndMS: 10, Closed: false}, false},
		{PendingFeedObservation{Feed: "1/1m", EndMS: 10, Closed: true, WarmUp: true}, false},
		{PendingFeedObservation{Feed: "1/1m", EndMS: 10, Closed: true}, true},
		{PendingFeedObservation{Feed: "1/1m", EndMS: 10, Closed: true}, false},
		{PendingFeedObservation{Feed: "1/1m", EndMS: 9, Closed: true}, false},
		{PendingFeedObservation{Feed: "1/5m", EndMS: 10, Closed: true}, true},
		{PendingFeedObservation{Feed: "1/1m", EndMS: 10000, Closed: true}, true},
	} {
		accepted, err := borrow.ObservePendingFeed(ctx, step.observation)
		if err != nil || accepted != step.accepted {
			t.Fatalf("observation=%+v accepted=%v want=%v err=%v", step.observation, accepted, step.accepted, err)
		}
	}
	feeds, err := borrow.service.store.LoadPendingFeeds(ctx)
	if err != nil || len(feeds) != 2 || feeds["1/1m"] != (PendingFeedProgress{EndMS: 10000, Bars: 2}) || feeds["1/5m"] != (PendingFeedProgress{EndMS: 10, Bars: 1}) {
		t.Fatal("feed observations mixed clocks or inferred missing bars", feeds, err)
	}
	if snapshot, err := borrow.Snapshot(ctx); err != nil || len(snapshot.Orders) != 0 || len(snapshot.Lots) != 0 {
		t.Fatal("observation traded while account was unready", snapshot, err)
	}
}

func TestPendingFeedObservationPersistsAcrossStoreReopen(t *testing.T) {
	borrow, _ := strategyService(t, false)
	ctx := context.Background()
	if accepted, err := borrow.ObservePendingFeed(ctx, PendingFeedObservation{Feed: "1/1m", EndMS: 42, Closed: true}); err != nil || !accepted {
		t.Fatal(accepted, err)
	}
	reopened, err := OpenStore(borrow.service.opts.StorePath, borrow.service.store.Account())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	feeds, err := reopened.LoadPendingFeeds(ctx)
	if err != nil || feeds["1/1m"] != (PendingFeedProgress{EndMS: 42, Bars: 1}) {
		t.Fatal("accepted feed fact was not durable", feeds, err)
	}
}

func TestPendingFeedRejectsInvalidObservationsAndCounterOverflow(t *testing.T) {
	borrow, _ := strategyService(t, true)
	ctx := context.Background()
	for _, observation := range []PendingFeedObservation{
		{Feed: "", EndMS: 1, Closed: true},
		{Feed: " invalid ", EndMS: 1, Closed: true},
		{Feed: PendingFeedID(strings.Repeat("a", 513)), EndMS: 1, Closed: true},
		{Feed: "1/1m", EndMS: -1, Closed: true},
		{Feed: "1/1m", EndMS: 0, Closed: true},
	} {
		if accepted, err := borrow.ObservePendingFeed(ctx, observation); err == nil || accepted {
			t.Fatalf("invalid observation accepted: %+v accepted=%v err=%v", observation, accepted, err)
		}
	}
	store := borrow.service.store
	if err := store.savePendingFeeds(ctx, map[PendingFeedID]PendingFeedProgress{"1/1m": {EndMS: 10, Bars: math.MaxInt64}}); err != nil {
		t.Fatal(err)
	}
	if accepted, err := borrow.ObservePendingFeed(ctx, PendingFeedObservation{Feed: "1/1m", EndMS: 11, Closed: true}); err == nil || accepted {
		t.Fatal("counter overflow accepted", accepted, err)
	}
	feeds, err := store.LoadPendingFeeds(ctx)
	if err != nil || feeds["1/1m"] != (PendingFeedProgress{EndMS: 10, Bars: math.MaxInt64}) {
		t.Fatal("overflow changed durable progress", feeds, err)
	}
	pending := pendingTestOrder(store, "a")
	pending.Entry.Conditions.StopBars = 2
	if err := borrow.WithState(func(account *SharedAccount) error {
		return account.BindPendingFeedExpiry(ctx, pending, "1/1m", 2)
	}); err == nil || pending.EntryFeedExpiry != nil || pending.Entry.Conditions.StopBars != 2 {
		t.Fatal("deadline overflow mutated binding", pending, err)
	}
}

func TestPendingFeedAccountOnlyTickExpiresRestingEntryWithZeroQuoteBar(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "SQLite"
		if memory {
			name = "Memory"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			borrow, paper := strategyService(t, memory)
			market := borrow.service.markets["BTC"]
			market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000, Bar: 0}, nil
			}
			borrow.service.markets["BTC"] = market
			pending := pendingTestOrder(borrow.service.store, "a")
			pending.Entry.Conditions.CreatedBar = 0
			pending.Entry.Conditions.StopBars = 1
			pending.Entry.Conditions.Limit = intentPrice("90")
			if err := borrow.WithState(func(account *SharedAccount) error {
				return account.BindPendingFeedExpiry(ctx, pending, "1/1m", 1)
			}); err != nil {
				t.Fatal(err)
			}
			if pending.Entry.Conditions.StopBars != 0 || pending.EntryFeedExpiry == nil || pending.EntryFeedExpiry.ExpiresAtBar != 1 {
				t.Fatal("binding mixed quote and feed clocks", pending)
			}
			saveTestPendingBook(t, borrow.service.store, []PendingDefinition{{Order: *pending, Instrument: ledgerInstrument(), Policy: OrderPolicy{Mode: ExchangeFirst}}})
			if err := borrow.AdvancePendingExecution(ctx, 12); err != nil {
				t.Fatal(err)
			}
			snapshot, err := borrow.Snapshot(ctx)
			if err != nil || len(snapshot.Orders) != 1 {
				t.Fatal("entry did not rest before feed expiry", snapshot.Orders, err)
			}
			orderID := snapshot.Orders[0].Intent.ID
			if accepted, err := borrow.ObservePendingFeed(ctx, PendingFeedObservation{Feed: "1/1m", EndMS: 13, Closed: true}); err != nil || !accepted {
				t.Fatal(accepted, err)
			}
			if err := borrow.AdvancePendingExecution(ctx, 13); err != nil {
				t.Fatal(err)
			}
			order, err := borrow.service.store.Order(ctx, orderID)
			if err != nil || order.State != OrderCanceled || order.FilledSteps != 0 || paper.Metrics().Fills != 0 {
				t.Fatal("feed expiry failed to cancel zero-bar resting entry", order, err)
			}
			book, err := borrow.service.store.LoadPendingBook(ctx)
			if err != nil || len(book) != 1 || !book[0].Order.Canceled || book[0].Order.Desired != 0 {
				t.Fatal("account tick did not checkpoint feed expiry", book, err)
			}
		})
	}
}
