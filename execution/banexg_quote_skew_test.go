package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/banbox/banexg"
)

// A host a few hundred milliseconds behind the venue must still read quotes:
// a book within accountClockSkewMS ahead is observed at the receive time, one
// further ahead is refused. Applies to Observe (embedded bridge quotes).
func TestBanexgObserveToleratesSmallClockSkew(t *testing.T) {
	_, _, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	book := func(ahead time.Duration) *banexg.OrderBook {
		return &banexg.OrderBook{Symbol: "BTC/USDT:USDT", TimeStamp: time.Now().Add(ahead).UnixMilli(), Bids: &banexg.OdBookSide{Price: []float64{99}}, Asks: &banexg.OdBookSide{Price: []float64{101}}}
	}
	exchange.book = book(300 * time.Millisecond)
	quote, err := adapter.Observe(context.Background(), "BTC")
	if err != nil || quote.AtMS > quote.ReceivedMS || quote.ValidUntilMS <= quote.ReceivedMS {
		t.Fatal("book 300ms ahead refused or observed in the future", quote, err)
	}
	exchange.book = book(time.Duration(accountClockSkewMS+500) * time.Millisecond)
	if _, err := adapter.Observe(context.Background(), "BTC"); err == nil {
		t.Fatal("book beyond the clock tolerance accepted")
	}
}

func TestObservedBookMS(t *testing.T) {
	ttl := 2 * time.Second
	for _, tc := range []struct {
		book, received int64
		observed       int64
		fresh          bool
	}{
		{10_000, 10_000, 10_000, true},
		{10_300, 10_000, 10_000, true}, // ahead within tolerance: receive time
		{11_001, 10_000, 0, false},     // beyond tolerance
		{7_999, 10_000, 7_999, false},  // older than ttl
		{0, 10_000, 0, false},          // missing timestamp
	} {
		observed, fresh := observedBookMS(tc.book, tc.received, ttl)
		if observed != tc.observed || fresh != tc.fresh {
			t.Fatalf("%+v: got %d %v", tc, observed, fresh)
		}
	}
}

// Repeated reads of one instrument within the reuse window share one fetch;
// a later read fetches again; failures are not cached.
func TestReuseRecentQuotes(t *testing.T) {
	calls := 0
	fail := false
	fetch := func(context.Context, string) (VisibleQuote, error) {
		calls++
		if fail {
			return VisibleQuote{}, errors.New("venue down")
		}
		now := time.Now().UnixMilli()
		return VisibleQuote{Bid: intentPrice("99"), Ask: intentPrice("101"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 2000}, nil
	}
	quote := ReuseRecentQuotes(fetch, 100*time.Millisecond)
	for i := 0; i < 5; i++ {
		if _, err := quote(context.Background(), "BTC"); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("five reads within the window made %d fetches, want 1", calls)
	}
	if _, err := quote(context.Background(), "ETH"); err != nil || calls != 2 {
		t.Fatal("instruments must not share a cached quote", calls, err)
	}
	time.Sleep(150 * time.Millisecond)
	fail = true
	if _, err := quote(context.Background(), "BTC"); err == nil || calls != 3 {
		t.Fatal("expired quote reused or failure hidden", calls, err)
	}
	if _, err := quote(context.Background(), "BTC"); err == nil || calls != 4 {
		t.Fatal("a failed read must not be cached", calls, err)
	}
}

// A caller requiring a quote observed at or after a time gets a refetch when
// the reused quote is older; a satisfied requirement keeps reusing it.
func TestReuseRecentQuotesHonorsNotBefore(t *testing.T) {
	calls := 0
	var at int64 = 1000
	fetch := func(context.Context, string) (VisibleQuote, error) {
		calls++
		now := time.Now().UnixMilli()
		return VisibleQuote{Bid: intentPrice("99"), Ask: intentPrice("101"), AtMS: at, ReceivedMS: now, ValidUntilMS: now + 2000}, nil
	}
	quote := ReuseRecentQuotes(fetch, time.Minute)
	if _, err := quote(context.Background(), "BTC"); err != nil || calls != 1 {
		t.Fatal(calls, err)
	}
	if q, err := quote(WithQuoteNotBefore(context.Background(), 1000), "BTC"); err != nil || calls != 1 || q.AtMS != 1000 {
		t.Fatal("satisfied requirement must reuse the quote", calls, err)
	}
	at = 2000
	if q, err := quote(WithQuoteNotBefore(context.Background(), 1500), "BTC"); err != nil || calls != 2 || q.AtMS != 2000 {
		t.Fatal("older reused quote must be refetched", calls, q.AtMS, err)
	}
}

// SharedAccountBorrow.VisibleQuote carries the caller's requirement to the
// account's registered quote source, whose context derives from the owner's.
func TestVisibleQuoteCarriesQuoteNotBefore(t *testing.T) {
	borrow, _ := bootstrapService(t)
	defer borrow.Release()
	var seen int64 = -1
	if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(ctx context.Context, _ string, now int64) (VisibleQuote, error) {
		if atMS, ok := quoteNotBefore(ctx); ok {
			seen = atMS
		}
		return VisibleQuote{Bid: intentPrice("99"), Ask: intentPrice("101"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 2000}, nil
	}, func() int64 { return time.Now().UnixMilli() }); err != nil {
		t.Fatal(err)
	}
	if _, err := borrow.VisibleQuote(WithQuoteNotBefore(context.Background(), 1234), ledgerInstrument().ID, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if seen != 1234 {
		t.Fatal("quote source did not receive the caller's not-before mark", seen)
	}
}
