package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// AccountMarketStream belongs to the account host, not a strategy or socket.
// Quotes and paper fills continue advancing after a requesting robot leaves.
type AccountMarketStream struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error
}

func (s *AccountMarketStream) Close() error {
	s.cancel()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *AccountMarketStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// StartBanexgMarketStream uses only the SDK's normalized depth subscription.
// Its exchange must be account-owned: closing it is the host's responsibility.
// validate runs after quote registration but before any execution worker starts.
// activate starts private reporting only after market subscription succeeds.
func StartBanexgMarketStream(parent context.Context, account *SharedAccount, exchange banexg.BanExchange, units []BanexgInstrument, ttl time.Duration, validate, activate func(*SharedAccountBorrow) error) (*AccountMarketStream, error) {
	if parent == nil || account == nil || exchange == nil || len(units) == 0 || ttl <= 0 {
		return nil, errors.New("execution: account market stream needs an owner, exchange, units and positive TTL")
	}
	ctx, cancel := context.WithCancel(parent)
	bySymbol := make(map[string]Instrument, len(units))
	ids := make(map[string]bool, len(units))
	var instruments []Instrument
	var symbols []string
	for _, unit := range units {
		if err := unit.Instrument.Validate(); err != nil {
			cancel()
			return nil, err
		}
		if unit.Symbol == "" || bySymbol[unit.Symbol].ID != "" || ids[unit.Instrument.ID] {
			cancel()
			return nil, errors.New("execution: duplicate or empty market stream symbol")
		}
		bySymbol[unit.Symbol] = unit.Instrument.Clone()
		ids[unit.Instrument.ID] = true
		instruments = append(instruments, unit.Instrument.Clone())
		symbols = append(symbols, unit.Symbol)
	}
	quotes := map[string]VisibleQuote{}
	sourceErrors := map[string]error{}
	var quotesMu sync.RWMutex
	borrow := account.Borrow()
	// The SDK reuses websocket book pointers and mutates their timestamp
	// without a snapshot lock. Use them only as identity wake-ups; REST returns
	// an owned normalized snapshot. Never read mutable websocket prices here.
	readQuote := func(symbol string) (VisibleQuote, error) {
		readCtx, done := context.WithTimeout(ctx, min(ttl, 15*time.Second))
		defer done()
		book, err := exchange.FetchOrderBook(symbol, 0, map[string]any{banexg.ParamContext: readCtx, banexg.ParamRetry: 0})
		if err != nil {
			return VisibleQuote{}, errors.New("execution: account depth snapshot unavailable")
		}
		if book == nil || book.Symbol != symbol {
			return VisibleQuote{}, errors.New("execution: account depth snapshot identity mismatch")
		}
		return accountBookQuote(book, time.Now().UnixMilli(), ttl)
	}
	for _, unit := range units {
		q, err := readQuote(unit.Symbol)
		if err != nil {
			borrow.Release()
			cancel()
			return nil, err
		}
		quotes[unit.Instrument.ID] = q
	}
	if err := borrow.RegisterAccountQuotes(instruments, func(caller context.Context, id string, now int64) (VisibleQuote, error) {
		if err := caller.Err(); err != nil {
			return VisibleQuote{}, err
		}
		if err := ctx.Err(); err != nil {
			return VisibleQuote{}, err
		}
		quotesMu.RLock()
		q, ok := quotes[id]
		failed := sourceErrors[id]
		quotesMu.RUnlock()
		if failed != nil {
			return VisibleQuote{}, failed
		}
		if !ok || q.ValidUntilMS <= now {
			return VisibleQuote{}, fmt.Errorf("execution: account depth not yet visible or stale: %s", id)
		}
		return q, nil
	}, func() int64 { return time.Now().UnixMilli() }); err != nil {
		borrow.Release()
		cancel()
		return nil, err
	}
	if validate != nil {
		if err := validate(borrow); err != nil {
			borrow.Release()
			cancel()
			return nil, err
		}
	}
	updates, watchErr := exchange.WatchOrderBooks(symbols, 0, map[string]any{banexg.ParamContext: ctx})
	if watchErr != nil || updates == nil {
		borrow.Release()
		cancel()
		return nil, errors.New("execution: account depth subscription unavailable")
	}
	if activate != nil {
		if err := activate(borrow); err != nil {
			borrow.Release()
			cancel()
			return nil, err
		}
	}
	stream := &AccountMarketStream{cancel: cancel, done: make(chan struct{})}
	// Each symbol refreshes independently. Websocket messages are identity
	// wake-ups only; the SDK does not yet expose immutable price-path events.
	wake := make(map[string]chan struct{}, len(symbols))
	var workers sync.WaitGroup
	setFailure := func(id string, cause error) {
		quotesMu.Lock()
		if cause != nil {
			delete(quotes, id)
			sourceErrors[id] = cause
		} else {
			delete(sourceErrors, id)
		}
		var failures []error
		for _, err := range sourceErrors {
			failures = append(failures, err)
		}
		stream.mu.Lock()
		stream.err = errors.Join(failures...)
		stream.mu.Unlock()
		quotesMu.Unlock()
	}
	refreshEvery := max(time.Nanosecond, min(ttl/2, time.Second))
	for _, symbol := range symbols {
		wake[symbol] = make(chan struct{}, 1)
	}
	for _, symbol := range symbols {
		workers.Add(1)
		go func(symbol string) {
			defer workers.Done()
			ticker := time.NewTicker(refreshEvery)
			defer ticker.Stop()
			unit := bySymbol[symbol]
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				case <-wake[symbol]:
				}
				q, err := readQuote(symbol)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					setFailure(unit.ID, err)
					continue
				}
				now := max(time.Now().UnixMilli(), q.ReceivedMS)
				quotesMu.Lock()
				old := quotes[unit.ID]
				if q.AtMS < old.AtMS {
					quotesMu.Unlock()
					continue
				}
				quotes[unit.ID] = q
				quotesMu.Unlock()
				setFailure(unit.ID, nil)
				err = borrow.WithState(func(service *SharedAccount) error {
					if !service.Reconciled() {
						return nil
					}
					if err := service.AdvancePaperQuote(ctx, unit.ID, q, now); err != nil {
						return err
					}
					return service.AdvancePendingExecution(ctx, now)
				})
				if err != nil {
					// Missing account marks are a recoverable admission failure,
					// not evidence that the account ledger became uncertain.
					stream.mu.Lock()
					stream.err = err
					stream.mu.Unlock()
					continue
				}
				account.notifyCommitted()
			}
		}(symbol)
	}
	go func() {
		defer close(stream.done)
		defer borrow.Release()
		defer workers.Wait()
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case book, ok := <-updates:
				if !ok {
					for _, unit := range units {
						setFailure(unit.Instrument.ID, errors.New("execution: account depth subscription interrupted"))
					}
					// A public source outage does not invalidate settled account
					// state. Withdraw its quotes, retry subscription, and resume.
					for {
						select {
						case <-ctx.Done():
							return
						case <-time.After(250 * time.Millisecond):
						}
						var err *errs.Error
						updates, err = exchange.WatchOrderBooks(symbols, 0, map[string]any{banexg.ParamContext: ctx})
						if err == nil && updates != nil {
							break
						}
					}
					continue
				}
				if book == nil || wake[book.Symbol] == nil {
					continue
				}
				select {
				case wake[book.Symbol] <- struct{}{}:
				default:
				}
			}
		}
	}()
	return stream, nil
}

func accountBookQuote(book *banexg.OrderBook, received int64, ttl time.Duration) (VisibleQuote, error) {
	if book == nil || book.Bids == nil || book.Asks == nil || book.TimeStamp <= 0 || book.TimeStamp > received || received-book.TimeStamp >= ttl.Milliseconds() {
		return VisibleQuote{}, errors.New("execution: invalid or stale account depth timestamp")
	}
	bid, _ := book.Bids.Level(0)
	ask, _ := book.Asks.Level(0)
	b, err := decimalBoundary(bid)
	if err != nil {
		return VisibleQuote{}, err
	}
	a, err := decimalBoundary(ask)
	if err != nil || !b.IsPositive() || a.LessThan(b) {
		return VisibleQuote{}, errors.New("execution: invalid account depth spread")
	}
	return VisibleQuote{Bid: b, Ask: a, AtMS: book.TimeStamp, ReceivedMS: received, ValidUntilMS: book.TimeStamp + ttl.Milliseconds()}, nil
}
