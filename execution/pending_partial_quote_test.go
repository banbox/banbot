package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// partialQuotes serves per-instrument prices and can hide an instrument with
// ErrAccountQuoteNotVisible (or any other error) like the market stream does.
type partialQuotes struct {
	mu     sync.Mutex
	price  map[string]string
	hidden map[string]error
}

func (p *partialQuotes) set(id, price string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.price[id] = price
}

func (p *partialQuotes) hide(id string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil {
		delete(p.hidden, id)
	} else {
		p.hidden[id] = err
	}
}

func (p *partialQuotes) quote(_ context.Context, id string, now int64) (VisibleQuote, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.hidden[id]; err != nil {
		return VisibleQuote{}, err
	}
	price := intentPrice(p.price[id])
	return VisibleQuote{Bid: price, Ask: price, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1_000_000_000, Bar: now}, nil
}

func ethInstrument() Instrument {
	i := ledgerInstrument()
	i.ID = "ETH"
	return i
}

// twoInstrumentRig: strategy "a" trades BTC, strategy "b" trades ETH. Each has
// a stop entry (105) and a stop-loss (95).
func twoInstrumentRig(t *testing.T) (*SharedAccountBorrow, *partialQuotes, []PendingDefinition) {
	t.Helper()
	borrow, _ := strategyService(t, true)
	quotes := &partialQuotes{price: map[string]string{"BTC": "100", "ETH": "100"}, hidden: map[string]error{}}
	err := borrow.use(func(s *SharedAccount) error {
		for _, i := range []Instrument{ledgerInstrument(), ethInstrument()} {
			s.markets[i.ID] = accountMarket{instrument: i.Clone(), quote: quotes.quote}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	store := borrow.service.store
	var definitions []PendingDefinition
	for _, tc := range []struct {
		strategy string
		inst     Instrument
	}{{"a", ledgerInstrument()}, {"b", ethInstrument()}} {
		d := PendingDefinition{Order: *pendingTestOrder(store, tc.strategy), Instrument: tc.inst, Specs: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("95")}}}
		d.Order.Entry.Instrument = tc.inst.ID
		d.Order.Entry.Conditions.Stop = intentPrice("105")
		definitions = append(definitions, d)
	}
	definitions[1].Order.Entry.Instrument = "ETH"
	saveTestPendingBook(t, store, definitions)
	return borrow, quotes, definitions
}

func lotSteps(t *testing.T, b *SharedAccountBorrow, instrument string) int64 {
	t.Helper()
	snapshot, err := b.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, lot := range snapshot.Lots {
		if lot.Instrument.ID == instrument {
			total += lot.SignedSteps
		}
	}
	return total
}

func bookByInstrument(t *testing.T, b *SharedAccountBorrow) map[string]PendingDefinition {
	t.Helper()
	book, err := b.service.store.LoadPendingBook(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]PendingDefinition{}
	for _, d := range book {
		out[d.Instrument.ID] = d
	}
	return out
}

// A stale instrument without any exposure: the healthy instrument still fills
// its entry; the stale definition is untouched; the caller sees a transient
// condition; recovery advances it.
func TestPendingPartialQuoteSkipsStaleEntryOnly(t *testing.T) {
	borrow, quotes, _ := twoInstrumentRig(t)
	ctx := context.Background()
	quotes.set("BTC", "110")
	quotes.set("ETH", "110")
	quotes.hide("ETH", fmt.Errorf("%w: ETH", ErrAccountQuoteNotVisible))
	before := bookByInstrument(t, borrow)["ETH"]
	if err := borrow.AdvancePendingExecution(ctx, 12); err != nil {
		t.Fatalf("partial tick must succeed for the healthy instrument: %v", err)
	}
	if skipped := borrow.service.PendingQuoteSkipped(); fmt.Sprint(skipped) != "[ETH]" {
		t.Fatalf("skipped instruments not reported: %v", skipped)
	}
	if got := lotSteps(t, borrow, "BTC"); got != 10 {
		t.Fatalf("healthy entry not filled: %d", got)
	}
	if got := lotSteps(t, borrow, "ETH"); got != 0 {
		t.Fatalf("stale instrument traded: %d", got)
	}
	after := bookByInstrument(t, borrow)["ETH"]
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("stale definition changed:\n%v\n%v", before, after)
	}
	quotes.hide("ETH", nil)
	if err := borrow.AdvancePendingExecution(ctx, 13); err != nil {
		t.Fatal(err)
	}
	if got := lotSteps(t, borrow, "ETH"); got != 10 {
		t.Fatalf("recovered instrument did not proceed: %d", got)
	}
	if skipped := borrow.service.PendingQuoteSkipped(); len(skipped) != 0 {
		t.Fatalf("skipped set not cleared on recovery: %v", skipped)
	}
}

// The reported production case: the stale instrument carries an open lot while
// the healthy one's stop-loss must fire.
func TestPendingPartialQuoteHealthyStopLossFiresWhileOtherStale(t *testing.T) {
	borrow, quotes, _ := twoInstrumentRig(t)
	ctx := context.Background()
	quotes.set("BTC", "110")
	quotes.set("ETH", "110")
	if err := borrow.AdvancePendingExecution(ctx, 12); err != nil {
		t.Fatal(err)
	}
	if lotSteps(t, borrow, "BTC") != 10 || lotSteps(t, borrow, "ETH") != 10 {
		t.Fatal("fixture: entries did not fill")
	}
	quotes.set("BTC", "90")
	quotes.hide("ETH", fmt.Errorf("%w: ETH", ErrAccountQuoteNotVisible))
	before := bookByInstrument(t, borrow)["ETH"]
	if err := borrow.AdvancePendingExecution(ctx, 13); err != nil {
		t.Fatalf("partial tick must not fail: %v", err)
	}
	if skipped := borrow.service.PendingQuoteSkipped(); fmt.Sprint(skipped) != "[ETH]" {
		t.Fatalf("skipped instruments not reported: %v", skipped)
	}
	if got := lotSteps(t, borrow, "BTC"); got != 0 {
		t.Fatalf("healthy stop-loss did not execute: %d", got)
	}
	if got := lotSteps(t, borrow, "ETH"); got != 10 {
		t.Fatalf("stale instrument lot changed: %d", got)
	}
	after := bookByInstrument(t, borrow)["ETH"]
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("stale definition changed:\n%v\n%v", before, after)
	}
	// Recovery: ETH now below its stop-loss and proceeds.
	quotes.set("ETH", "90")
	quotes.hide("ETH", nil)
	if err := borrow.AdvancePendingExecution(ctx, 14); err != nil {
		t.Fatal(err)
	}
	if got := lotSteps(t, borrow, "ETH"); got != 0 {
		t.Fatalf("recovered stop-loss did not execute: %d", got)
	}
}

// Any other quote error keeps the old behaviour: the instrument is skipped like
// before, but account risk still fails on its carried exposure (no substitute
// mark), so the tick errors and nothing advances.
func TestPendingPartialQuoteOtherErrorKeepsOldBehaviour(t *testing.T) {
	borrow, quotes, _ := twoInstrumentRig(t)
	ctx := context.Background()
	quotes.set("BTC", "110")
	quotes.set("ETH", "110")
	if err := borrow.AdvancePendingExecution(ctx, 12); err != nil {
		t.Fatal(err)
	}
	quotes.set("BTC", "90")
	quotes.hide("ETH", errors.New("boom"))
	err := borrow.AdvancePendingExecution(ctx, 13)
	if err == nil || errors.Is(err, ErrAccountQuoteNotVisible) || transientAccountReadError(err) {
		t.Fatalf("want hard error, got %v", err)
	}
	if got := lotSteps(t, borrow, "BTC"); got != 10 {
		t.Fatalf("healthy instrument advanced despite hard quote error: %d", got)
	}
}

// With no usable last mark (too old, or none after a restart) the valuation of
// the stale instrument's exposure is refused safely: a transient error, and
// nothing advances.
func TestPendingPartialQuoteRefusesWithoutFreshMark(t *testing.T) {
	borrow, quotes, _ := twoInstrumentRig(t)
	ctx := context.Background()
	quotes.set("BTC", "110")
	quotes.set("ETH", "110")
	if err := borrow.AdvancePendingExecution(ctx, 12); err != nil {
		t.Fatal(err)
	}
	quotes.set("BTC", "90")
	quotes.hide("ETH", fmt.Errorf("%w: ETH", ErrAccountQuoteNotVisible))
	err := borrow.AdvancePendingExecution(ctx, 12+maxStaleMarkAgeMS+1)
	if !errors.Is(err, ErrAccountQuoteNotVisible) || !transientAccountReadError(err) {
		t.Fatalf("want transient refusal, got %v", err)
	}
	if got := lotSteps(t, borrow, "BTC"); got != 10 {
		t.Fatalf("tick advanced on an unvalued exposure: %d", got)
	}
}

// One instrument only: a not-visible quote is a transient error, nothing moves.
func TestPendingPartialQuoteSingleInstrumentUnchanged(t *testing.T) {
	borrow, quotes, _ := twoInstrumentRig(t)
	store := borrow.service.store
	d := PendingDefinition{Order: *pendingTestOrder(store, "a"), Instrument: ledgerInstrument()}
	d.Order.Entry.Conditions.Stop = intentPrice("105")
	saveTestPendingBook(t, store, []PendingDefinition{d})
	quotes.set("BTC", "110")
	quotes.hide("BTC", fmt.Errorf("%w: BTC", ErrAccountQuoteNotVisible))
	if err := borrow.AdvancePendingExecution(context.Background(), 12); !errors.Is(err, ErrAccountQuoteNotVisible) {
		t.Fatalf("got %v", err)
	}
	if lotSteps(t, borrow, "BTC") != 0 {
		t.Fatal("entry filled without a visible quote")
	}
	quotes.hide("BTC", nil)
	if err := borrow.AdvancePendingExecution(context.Background(), 13); err != nil {
		t.Fatal(err)
	}
	if lotSteps(t, borrow, "BTC") != 10 {
		t.Fatal("recovery did not fill")
	}
}
