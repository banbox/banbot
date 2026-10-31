package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
)

func TestSharedFeedAccountOnlyExpirySurvivesRestartAndQuoteFailure(t *testing.T) {
	ctx := context.Background()
	f := newSharedTriggerFixture(t)
	original := f.bridge.Quote
	quoteFailure := errors.New("feed quote unavailable")
	failQuote := false
	f.bridge.Quote = func(id string, now int64) (execution.VisibleQuote, error) {
		if failQuote {
			return execution.VisibleQuote{}, quoteFailure
		}
		quote, err := original(id, now)
		quote.Bar = 0
		return quote, err
	}
	f.job.TimeFrame = "1m"
	f.entry(t, &strat.EnterReq{Tag: "autonomous-feed", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 2})
	feed := execution.PendingFeedID("1/1m")
	// Use the execution binding to avoid coupling this test to the opaque
	// facade SID/timeframe key encoding.
	if err := f.rt.SharedExecution().WithState(func(account *execution.SharedAccount) error {
		book, err := account.Store().LoadPendingBook(ctx)
		if err == nil && len(book) == 1 && book[0].Order.EntryFeedExpiry != nil {
			feed = book[0].Order.EntryFeedExpiry.Feed
		} else if err == nil {
			t.Fatalf("zero-bar facade entry did not bind feed expiry: %+v", book)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if accepted, err := f.rt.SharedExecution().ObservePendingFeed(ctx, execution.PendingFeedObservation{Feed: feed, EndMS: 101, Closed: true}); err != nil || !accepted {
		t.Fatal(accepted, err)
	}
	f.process.Close()
	p := NewProcess()
	t.Cleanup(p.Close)
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	rt.Clock.SetTimeMS(102)
	if err := rt.SharedExecution().Reconcile("autonomous-feed-restart", 102); err != nil {
		t.Fatal(err)
	}
	// No facade manager is initialized after restart. Duplicate and unrelated
	// feed observations must not consume the remaining entry lifetime.
	for _, observation := range []execution.PendingFeedObservation{
		{Feed: feed, EndMS: 101, Closed: true},
		{Feed: feed, EndMS: 100, Closed: true},
		{Feed: feed, EndMS: 102, Closed: true, WarmUp: true},
		{Feed: feed, EndMS: 102, Closed: false},
	} {
		if accepted, err := rt.SharedExecution().ObservePendingFeed(ctx, observation); err != nil || accepted {
			t.Fatal("replayed or nonclosed feed consumed lifetime", observation, accepted, err)
		}
	}
	if accepted, err := rt.SharedExecution().ObservePendingFeed(ctx, execution.PendingFeedObservation{Feed: "unrelated/5m", EndMS: 102, Closed: true}); err != nil || !accepted {
		t.Fatal(accepted, err)
	}
	if err := rt.SharedExecution().AdvancePendingExecution(ctx, 102); err != nil {
		t.Fatal(err)
	}
	checkBook := func(canceled bool) {
		t.Helper()
		if err := rt.SharedExecution().WithState(func(account *execution.SharedAccount) error {
			book, err := account.Store().LoadPendingBook(ctx)
			if err == nil && (len(book) != 1 || book[0].Order.Canceled != canceled) {
				t.Fatalf("unexpected autonomous entry lifetime: %+v", book)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	checkBook(false)
	failQuote = true
	if accepted, err := rt.SharedExecution().ObservePendingFeed(ctx, execution.PendingFeedObservation{Feed: feed, EndMS: 103, Closed: true}); err != nil || !accepted {
		t.Fatal("quote-independent feed observation failed", accepted, err)
	}
	if err := rt.SharedExecution().AdvancePendingExecution(ctx, 103); !errors.Is(err, quoteFailure) {
		t.Fatal("quote failure was not propagated", err)
	}
	if err := rt.SharedExecution().WithState(func(account *execution.SharedAccount) error {
		feeds, err := account.Store().LoadPendingFeeds(ctx)
		if err == nil && feeds[feed] != (execution.PendingFeedProgress{EndMS: 103, Bars: 2}) {
			t.Fatalf("failed account tick lost accepted feed observation: %+v", feeds)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	failQuote = false
	if accepted, err := rt.SharedExecution().ObservePendingFeed(ctx, execution.PendingFeedObservation{Feed: feed, EndMS: 103, Closed: true}); err != nil || accepted {
		t.Fatal("failed quote caused accepted feed fact to replay", accepted, err)
	}
	if err := rt.SharedExecution().AdvancePendingExecution(ctx, 104); err != nil {
		t.Fatal(err)
	}
	checkBook(true)
	if f.steps(t) != 0 || f.adapter.Metrics().Fills != 0 {
		t.Fatal("account-only feed expiry manufactured a fill")
	}
}
