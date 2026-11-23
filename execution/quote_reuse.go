package execution

import (
	"context"
	"sync"
	"time"
)

// EmbeddedQuoteReuse is how long the embedded time-series path reuses a
// fetched account quote. Its pending execution reads quotes several times per
// pass and its ticker runs every 0.5s; without reuse every read is a REST book
// request, spending request weight shared by every process on the host IP.
const EmbeddedQuoteReuse = time.Second

type quoteNotBeforeKey struct{}

// WithQuoteNotBefore marks ctx so a reusing quote source (ReuseRecentQuotes)
// returns only a quote observed at or after atMS, fetching a fresh one when
// its cached quote is older. SharedAccountBorrow.VisibleQuote carries the mark
// from the caller to the account's quote source.
func WithQuoteNotBefore(ctx context.Context, atMS int64) context.Context {
	return context.WithValue(ctx, quoteNotBeforeKey{}, atMS)
}

func quoteNotBefore(ctx context.Context) (int64, bool) {
	atMS, ok := ctx.Value(quoteNotBeforeKey{}).(int64)
	return atMS, ok
}

// ReuseRecentQuotes wraps a per-instrument quote fetch: reads within maxAge of
// the last successful fetch reuse its quote (while still valid), and
// concurrent reads of one instrument share a single fetch. Failures are never
// cached. A caller that needs a quote observed after a given time (the factor
// runner's executable window) marks its context with WithQuoteNotBefore; an
// older cached quote is then refetched.
func ReuseRecentQuotes(fetch func(context.Context, string) (VisibleQuote, error), maxAge time.Duration) func(context.Context, string) (VisibleQuote, error) {
	type entry struct {
		mu    sync.Mutex
		quote VisibleQuote
		atMS  int64
	}
	var mu sync.Mutex
	entries := map[string]*entry{}
	return func(ctx context.Context, id string) (VisibleQuote, error) {
		mu.Lock()
		e := entries[id]
		if e == nil {
			e = &entry{}
			entries[id] = e
		}
		mu.Unlock()
		e.mu.Lock()
		defer e.mu.Unlock()
		now := time.Now().UnixMilli()
		notBefore, _ := quoteNotBefore(ctx)
		if e.atMS > 0 && now-e.atMS < maxAge.Milliseconds() && now < e.quote.ValidUntilMS && e.quote.AtMS >= notBefore {
			return e.quote, nil
		}
		quote, err := fetch(ctx, id)
		if err != nil {
			return quote, err
		}
		e.quote, e.atMS = quote, time.Now().UnixMilli()
		return quote, nil
	}
}
