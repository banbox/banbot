package execution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHubPolicyCancellationMarkerSurvivesReconciliationAdmission(t *testing.T) {
	ctx := context.Background()
	h, account, lease := policyCancelHub(t, true)
	now := time.Now().UnixMilli()
	create := pendingCreate("review-pending", "a")
	create.StrategyEpoch, create.StrategySequence = lease.Epoch, 1
	if _, err := h.ApplyPendingCommand(ctx, account.AccountKey(), create); err != nil {
		t.Fatal(err)
	}
	for id, market := range account.service.markets {
		market.quote = func(context.Context, string, int64) (VisibleQuote, error) {
			return VisibleQuote{}, errors.New("temporary cancellation mark outage")
		}
		account.service.markets[id] = market
	}
	command := HubPolicyEntriesCancel{ID: "review-interrupted", Strategy: "a", StrategyEpoch: lease.Epoch, StrategySequence: 2, Instruments: []string{"BTC"}}
	if err := h.CancelPolicyEntries(ctx, account.AccountKey(), command); !errors.Is(err, ErrPolicyCancellationRecoveryRequired) {
		t.Fatal("expected accepted but unfinished cancellation", err)
	}
	for id, market := range account.service.markets {
		market.quote = func(_ context.Context, _ string, at int64) (VisibleQuote, error) {
			return VisibleQuote{Bid: intentPrice("89"), Ask: intentPrice("89"), AtMS: at, ReceivedMS: at, ValidUntilMS: at + 60000, Bar: at}, nil
		}
		account.service.markets[id] = market
	}
	// Matching physical balances do not complete the logical cancellation.
	_ = account.Reconcile("review-authoritative", now+100)
	_ = account.AdvancePendingExecution(ctx, now+101)
	snapshot, err := account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, lot := range snapshot.Lots {
		if lot.Strategy == "a" && lot.SignedSteps != 0 {
			t.Fatalf("unfinished cancellation marker allowed autonomous re-entry: %+v", lot)
		}
	}
}

func TestHubPublicFeedRetryAdvancesPreviouslyAcceptedObservation(t *testing.T) {
	ctx := context.Background()
	h, account, _ := policyCancelHub(t, true)
	order := pendingTestOrder(account.service.store, "a")
	order.Entry.Conditions.Limit = intentPrice("90")
	if err := account.WithState(func(service *SharedAccount) error { return service.BindPendingFeedExpiry(ctx, order, "17/1m", 1) }); err != nil {
		t.Fatal(err)
	}
	saveTestPendingBook(t, account.service.store, []PendingDefinition{{Order: *order, Instrument: ledgerInstrument()}})
	if err := h.AuthorizePublicFeed(HubPendingFeed{Account: account.AccountKey(), Instrument: "BTC", Feed: "17/1m"}); err != nil {
		t.Fatal(err)
	}
	// Feed persistence succeeds while owner execution is temporarily unavailable.
	if err := account.WithState(func(service *SharedAccount) error { service.ready = false; return nil }); err != nil {
		t.Fatal(err)
	}
	observation := PendingFeedObservation{Feed: "17/1m", EndMS: time.Now().UnixMilli(), Closed: true}
	if err := h.ObservePublicFeed(ctx, observation); err == nil {
		t.Fatal("expected first advancement to fail after feed acceptance")
	}
	if err := account.WithState(func(service *SharedAccount) error { service.ready = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := h.ObservePublicFeed(ctx, observation); err != nil {
		t.Fatal(err)
	}
	book, err := account.PendingOrders(ctx)
	if err != nil || len(book) != 1 || !book[0].Order.Canceled {
		t.Fatal("retry dropped accepted observation without retrying expiry", book, err)
	}
}
