package execution

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

type accountDepthExchange struct {
	banexg.BanExchange
	updates chan *banexg.OrderBook
	depth   int
	mu      sync.Mutex
	book    *banexg.OrderBook
	watches int
}

func (e *accountDepthExchange) WatchOrderBooks(_ []string, depth int, _ map[string]any) (chan *banexg.OrderBook, *errs.Error) {
	e.depth = depth
	e.watches++
	return e.updates, nil
}

func depthBook(price float64) *banexg.OrderBook {
	return &banexg.OrderBook{Symbol: "BTC/USDT:USDT", TimeStamp: time.Now().UnixMilli(), Bids: banexg.NewOdBookSide(true, 1, [][2]float64{{price, 100}}), Asks: banexg.NewOdBookSide(false, 1, [][2]float64{{price, 100}})}
}

func (e *accountDepthExchange) FetchOrderBook(_ string, _ int, _ map[string]any) (*banexg.OrderBook, *errs.Error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.book == nil {
		return depthBook(100), nil
	}
	return e.book, nil
}

func (e *accountDepthExchange) publish(price float64) {
	e.mu.Lock()
	e.book = depthBook(price)
	book := e.book
	e.mu.Unlock()
	e.updates <- book
}

func TestAccountBookQuoteRejectsInvalidDepth(t *testing.T) {
	now := time.Now().UnixMilli()
	for name, mutate := range map[string]func(*banexg.OrderBook){
		"stale":   func(b *banexg.OrderBook) { b.TimeStamp = now - 1000 },
		"future":  func(b *banexg.OrderBook) { b.TimeStamp = now + 1 },
		"missing": func(b *banexg.OrderBook) { b.Bids = nil },
		"empty":   func(b *banexg.OrderBook) { b.Asks = banexg.NewOdBookSide(false, 1, nil) },
		"crossed": func(b *banexg.OrderBook) { b.Asks = banexg.NewOdBookSide(false, 1, [][2]float64{{90, 100}}) },
		"nan":     func(b *banexg.OrderBook) { b.Asks = banexg.NewOdBookSide(false, 1, [][2]float64{{math.NaN(), 100}}) },
	} {
		t.Run(name, func(t *testing.T) {
			book := depthBook(100)
			book.TimeStamp = now
			mutate(book)
			if _, err := accountBookQuote(book, now, time.Second); err == nil {
				t.Fatal("invalid depth was accepted")
			}
		})
	}
	book := depthBook(100)
	book.TimeStamp = now
	quote, err := accountBookQuote(book, now, time.Second)
	if err != nil || !quote.Bid.Equal(intentPrice("100")) || quote.ValidUntilMS != now+1000 {
		t.Fatal(quote, err)
	}
}

func TestAccountMarketStreamExecutesEntryAndProtectionWithoutRobot(t *testing.T) {
	robot, service := bootstrapService(t)
	ctx := context.Background()
	fundStrategies(t, service.store)
	if err := robot.RegisterRiskPolicy("v1", domainRisk()); err != nil {
		t.Fatal(err)
	}
	if err := robot.Reconcile("market-start", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	definition := PendingDefinition{Order: *pendingTestOrder(service.store, "a"), Instrument: ledgerInstrument(), Specs: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("95")}}}
	definition.Order.Entry.Conditions.Stop = intentPrice("105")
	definition.Order.Entry.Conditions.CreatedBar = 0
	saveTestPendingBook(t, service.store, []PendingDefinition{definition})
	exchange := &accountDepthExchange{updates: make(chan *banexg.OrderBook, 4)}
	stream, err := StartBanexgMarketStream(ctx, service, exchange, []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument()}}, 5*time.Second, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if exchange.depth != 0 {
		t.Fatal("account depth must use the SDK default rather than an unsupported venue depth")
	}
	robot.Release()
	observer := service.Borrow()
	defer observer.Release()
	wake := make(chan struct{}, 8)
	unsubscribe, err := observer.SubscribeCommitted(func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	waitPosition := func(steps int64) {
		t.Helper()
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for {
			snapshot, err := observer.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var total int64
			for _, lot := range snapshot.Lots {
				total += lot.SignedSteps
			}
			if total == steps {
				return
			}
			select {
			case <-wake:
			case <-timer.C:
				t.Fatalf("market stream did not reach %d steps: %+v; stream: %v", steps, snapshot.Lots, stream.Err())
			}
		}
	}
	exchange.publish(110)
	waitPosition(10)
	exchange.publish(90)
	waitPosition(0)
	if err := observer.WithState(func(account *SharedAccount) error {
		book, err := account.store.LoadPendingBook(ctx)
		if err != nil {
			return err
		}
		if len(book) != 1 || book[0].Order.LastProtection != StopLossProtection || !book[0].Order.Canceled {
			t.Fatalf("missing committed protective exit: %+v", book)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestAccountMarketStreamClosedSourceRecoversWithoutLedgerFreeze(t *testing.T) {
	borrow, service := bootstrapService(t)
	if err := borrow.BootstrapCapital(context.Background(), map[StrategyID]decimal.Decimal{"a": intentPrice("1000")}, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	exchange := &accountDepthExchange{updates: make(chan *banexg.OrderBook)}
	stream, err := StartBanexgMarketStream(context.Background(), service, exchange, []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument()}}, 100*time.Millisecond, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	close(exchange.updates)
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-stream.done:
			t.Fatal("recoverable source outage stopped worker")
		case <-deadline:
			t.Fatal("source outage never reported")
		case <-time.After(time.Millisecond):
		}
		if stream.Err() != nil {
			break
		}
	}
	if err := borrow.WithState(func(account *SharedAccount) error {
		snapshot, err := account.store.Snapshot(context.Background())
		if snapshot.RiskFrozen {
			t.Fatal("public source outage froze authoritative account ledger")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// A REST refresh repairs quote availability even while WS reconnects.
	deadline = time.After(3 * time.Second)
	for {
		if _, err := borrow.VisibleQuote(context.Background(), "BTC", time.Now().UnixMilli()); err == nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("REST recovery never restored fresh quote")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestAccountMarketStreamValidatesBindingsBeforeSubscribing(t *testing.T) {
	_, service := bootstrapService(t)
	exchange := &accountDepthExchange{updates: make(chan *banexg.OrderBook, 1)}
	exchange.updates <- depthBook(110)
	sentinel := errors.New("unbound restored contributor")
	stream, err := StartBanexgMarketStream(context.Background(), service, exchange, []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument()}}, time.Second, func(*SharedAccountBorrow) error { return sentinel }, nil)
	if !errors.Is(err, sentinel) || stream != nil || exchange.watches != 0 {
		t.Fatal("invalid account started an execution source", stream, err, exchange.watches)
	}
}

func TestAccountMarketStreamDoesNotExecuteBeforePrivateSetup(t *testing.T) {
	_, service := bootstrapService(t)
	exchange := &accountDepthExchange{updates: make(chan *banexg.OrderBook, 1)}
	exchange.updates <- depthBook(110)
	sentinel := errors.New("private reports unavailable")
	stream, err := StartBanexgMarketStream(context.Background(), service, exchange, []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument()}}, time.Second, nil, func(*SharedAccountBorrow) error { return sentinel })
	if !errors.Is(err, sentinel) || stream != nil || exchange.watches != 1 || len(exchange.updates) != 1 {
		t.Fatal("market source consumed before private setup succeeded", stream, err)
	}
}

type isolatedDepthExchange struct {
	banexg.BanExchange
	updates chan *banexg.OrderBook
	mu      sync.Mutex
	blocked string
	failed  string
	started chan struct{}
}

func (e *isolatedDepthExchange) WatchOrderBooks(_ []string, _ int, _ map[string]any) (chan *banexg.OrderBook, *errs.Error) {
	return e.updates, nil
}

func (e *isolatedDepthExchange) FetchOrderBook(symbol string, _ int, params map[string]any) (*banexg.OrderBook, *errs.Error) {
	e.mu.Lock()
	blocked, failed := e.blocked == symbol, e.failed == symbol
	e.mu.Unlock()
	if blocked {
		select {
		case e.started <- struct{}{}:
		default:
		}
		ctx := params[banexg.ParamContext].(context.Context)
		<-ctx.Done()
		return nil, errs.New(errs.CodeTimeout, ctx.Err())
	}
	if failed {
		return nil, errs.NewMsg(errs.CodeNetFail, "transient snapshot failure")
	}
	book := depthBook(100)
	book.Symbol = symbol
	return book, nil
}

func TestAccountMarketStreamIsolatesSlowAndFailedSnapshots(t *testing.T) {
	borrow, service := bootstrapService(t)
	defer borrow.Release()
	btc := ledgerInstrument()
	eth := btc.Clone()
	eth.ID = "ETH"
	exchange := &isolatedDepthExchange{updates: make(chan *banexg.OrderBook, 8), started: make(chan struct{}, 1)}
	stream, err := StartBanexgMarketStream(context.Background(), service, exchange, []BanexgInstrument{
		{Symbol: "BTC/USDT:USDT", Instrument: btc},
		{Symbol: "ETH/USDT:USDT", Instrument: eth},
	}, 2*time.Second, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	initial, err := borrow.VisibleQuote(context.Background(), "ETH", time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	exchange.mu.Lock()
	exchange.blocked = "BTC/USDT:USDT"
	exchange.mu.Unlock()
	exchange.updates <- depthBook(100)
	select {
	case <-exchange.started:
	case <-time.After(time.Second):
		t.Fatal("slow symbol did not start refreshing")
	}
	// A second symbol's refresh must finish before BTC's two-second timeout.
	time.Sleep(2 * time.Millisecond)
	book := depthBook(100)
	book.Symbol = "ETH/USDT:USDT"
	exchange.updates <- book
	deadline := time.After(time.Second)
	for {
		q, err := borrow.VisibleQuote(context.Background(), "ETH", time.Now().UnixMilli())
		if err == nil && q.AtMS > initial.AtMS {
			break
		}
		select {
		case <-deadline:
			t.Fatal("slow BTC snapshot blocked fresh ETH quote")
		case <-time.After(time.Millisecond):
		}
	}
	exchange.mu.Lock()
	exchange.failed = "ETH/USDT:USDT"
	exchange.mu.Unlock()
	exchange.updates <- book
	deadline = time.After(time.Second)
	for {
		if _, err := borrow.VisibleQuote(context.Background(), "ETH", time.Now().UnixMilli()); err != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("failed snapshot left stale quote available")
		case <-time.After(time.Millisecond):
		}
	}
	exchange.mu.Lock()
	exchange.failed = ""
	exchange.mu.Unlock()
	exchange.updates <- book
	deadline = time.After(time.Second)
	for {
		if _, err := borrow.VisibleQuote(context.Background(), "ETH", time.Now().UnixMilli()); err == nil {
			return
		}
		select {
		case <-stream.done:
			t.Fatal("transient snapshot failure permanently stopped source")
		case <-deadline:
			t.Fatal("snapshot did not recover")
		case <-time.After(time.Millisecond):
		}
	}
}
