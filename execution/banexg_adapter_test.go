package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type fakeBanexgSession struct {
	banexg.BanExchange
	create       func(string, string, string, float64, float64, map[string]any) (*banexg.Order, *errs.Error)
	query        func(string, string, map[string]any) (*banexg.Order, *errs.Error)
	cancel       func(string, string, map[string]any) (*banexg.Order, *errs.Error)
	balances     *banexg.Balances
	positions    []*banexg.Position
	orders       []*banexg.Order
	book         *banexg.OrderBook
	creates      int
	snapshots    int
	fullSnapshot bool
	trades       chan *banexg.MyTrade
}

func (e *fakeBanexgSession) HasApi(string, string) bool { return true }
func (e *fakeBanexgSession) WatchMyTrades(map[string]any) (chan *banexg.MyTrade, *errs.Error) {
	return e.trades, nil
}
func (e *fakeBanexgSession) CreateOrder(symbol, kind, side string, amount, price float64, params map[string]any) (*banexg.Order, *errs.Error) {
	e.creates++
	return e.create(symbol, kind, side, amount, price, params)
}
func (e *fakeBanexgSession) FetchOrder(symbol, id string, params map[string]any) (*banexg.Order, *errs.Error) {
	return e.query(symbol, id, params)
}
func (e *fakeBanexgSession) CancelOrder(id, symbol string, params map[string]any) (*banexg.Order, *errs.Error) {
	return e.cancel(id, symbol, params)
}
func (e *fakeBanexgSession) FetchBalance(map[string]any) (*banexg.Balances, *errs.Error) {
	e.snapshots++
	return e.balances, nil
}
func (e *fakeBanexgSession) FetchPositions([]string, map[string]any) ([]*banexg.Position, *errs.Error) {
	return e.positions, nil
}
func (e *fakeBanexgSession) FetchOpenOrders(_ string, _ int64, _ int, params map[string]any) ([]*banexg.Order, *errs.Error) {
	e.fullSnapshot, _ = params[banexg.ParamFullSnapshot].(bool)
	return e.orders, nil
}
func (e *fakeBanexgSession) FetchOrderBook(string, int, map[string]any) (*banexg.OrderBook, *errs.Error) {
	return e.book, nil
}

type verifiedTestTransport struct {
	proof  BanexgExecutionProof
	invoke func(context.Context, func() error) error
	verify func(context.Context) error
}

func (p *verifiedTestTransport) Verify(ctx context.Context, _ banexg.BanExchange, _ AccountKey) (BanexgExecutionProof, error) {
	if p.verify != nil {
		return p.proof, p.verify(ctx)
	}
	return p.proof, nil
}
func (p *verifiedTestTransport) Invoke(ctx context.Context, call func() error) error {
	if p.invoke != nil {
		return p.invoke(ctx, call)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return call()
}

func banexgFixture(t *testing.T) (*Store, *AccountHandle, *fakeBanexgSession, *verifiedTestTransport, BanexgAdapterConfig) {
	s, _, h, _ := testStore(t)
	exchange := &fakeBanexgSession{balances: &banexg.Balances{Total: map[string]float64{"USDT": 1000}}}
	transport := &verifiedTestTransport{proof: BanexgExecutionProof{Account: s.key, EvidenceID: "verified-test-session", ContextBound: true, StableClientID: true, QueryClientID: true, CompleteCumulativeReports: true, CompleteAccountSnapshot: true, SettledCash: true, NetLinearPositions: true}}
	config := BanexgAdapterConfig{Account: s.key, Store: s, Transport: transport, Instruments: []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument()}}}
	return s, h, exchange, transport, config
}

func TestBanexgStartupFailsClosedWithoutTransportAndCapabilityProof(t *testing.T) {
	_, _, exchange, transport, config := banexgFixture(t)
	for _, scenario := range []string{"no-transport", "context", "snapshot", "fee", "account", "absence"} {
		t.Run(scenario, func(t *testing.T) {
			bad := config
			proof := transport.proof
			switch scenario {
			case "no-transport":
				bad.Transport = nil
			case "context":
				proof.ContextBound = false
			case "snapshot":
				proof.CompleteAccountSnapshot = false
			case "fee":
				proof.CompleteCumulativeReports = false
			case "account":
				proof.Account.Account = "wrong"
			case "absence":
				proof.AuthoritativeNotFound = true
			}
			if bad.Transport != nil {
				bad.Transport = &verifiedTestTransport{proof: proof}
			}
			if _, err := NewBanexgAdapter(context.Background(), exchange, bad); err == nil {
				t.Fatal("unproven startup admitted")
			}
		})
	}
	if exchange.creates != 0 || exchange.snapshots != 0 {
		t.Fatal("constructor performed trading/snapshot calls")
	}
}

func TestBanexgStartupRejectsCanceledAndTypedNilBindings(t *testing.T) {
	_, _, exchange, transport, config := banexgFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewBanexgAdapter(ctx, exchange, config); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled capability verification admitted startup", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	transport.verify = func(context.Context) error { cancel(); return nil }
	if _, err := NewBanexgAdapter(ctx, exchange, config); !errors.Is(err, context.Canceled) {
		t.Fatal("verification ignored cancellation and admitted startup", err)
	}
	transport.verify = nil
	var missingExchange *fakeBanexgSession
	if adapter, err := NewBanexgAdapter(context.Background(), missingExchange, config); err == nil || adapter != nil {
		t.Fatal("typed nil exchange admitted startup", adapter, err)
	}
	var missingTransport *verifiedTestTransport
	config.Transport = missingTransport
	if adapter, err := NewBanexgAdapter(context.Background(), exchange, config); err == nil || adapter != nil {
		t.Fatal("typed nil transport admitted startup", adapter, err)
	}
}

func TestBanexgStableSubmitCompleteQueryAndCancel(t *testing.T) {
	s, h, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	intent := planOrder(t, s, "sdk", 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
	if err := s.PrepareOrder(context.Background(), intent, 10); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.Order(context.Background(), intent.ID)
	exchange.create = func(symbol, kind, side string, amount, price float64, params map[string]any) (*banexg.Order, *errs.Error) {
		if symbol != "BTC/USDT:USDT" || kind != banexg.OdTypeMarket || side != banexg.OdSideBuy || amount != 1 || price != 0 || params[banexg.ParamClientOrderId] != stored.ClientID || params[banexg.ParamAccount] != s.key.Account || params[banexg.ParamRetry] != 0 {
			t.Fatal("lost frozen unified submission metadata", symbol, kind, side, amount, price, params)
		}
		return &banexg.Order{ID: "venue-sdk", ClientOrderID: stored.ClientID, Status: banexg.OdStatusOpen}, nil
	}
	owner := &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: adapter}
	if err := owner.Send(intent.ID, 11); err != nil {
		t.Fatal(err)
	}
	exchange.query = func(symbol, id string, params map[string]any) (*banexg.Order, *errs.Error) {
		if id != "venue-sdk" || params[banexg.ParamClientOrderId] != stored.ClientID {
			t.Fatal("query lost stable IDs")
		}
		return &banexg.Order{ID: id, ClientOrderID: stored.ClientID, Symbol: symbol, Side: banexg.OdSideBuy, Amount: 1, Filled: 0.2, Cost: 20, Fee: &banexg.Fee{Currency: "USDT", Cost: 0.02}, Status: banexg.OdStatusPartFilled, LastUpdateTimestamp: 12}, nil
	}
	if err := owner.Recover(intent.ID); err != nil {
		t.Fatal(err)
	}
	if err := owner.Recover(intent.ID); err != nil {
		t.Fatal(err)
	}
	current, err := s.Order(context.Background(), intent.ID)
	if err != nil || current.FilledSteps != 2 || !current.ReportedFee.Equal(intentPrice("0.02")) {
		t.Fatal(current, err)
	}
	exchange.cancel = func(id, symbol string, params map[string]any) (*banexg.Order, *errs.Error) {
		exchange.query = func(symbol, id string, _ map[string]any) (*banexg.Order, *errs.Error) {
			return &banexg.Order{ID: id, ClientOrderID: stored.ClientID, Symbol: symbol, Side: banexg.OdSideBuy, Amount: 1, Filled: 0.2, Cost: 20, Fee: &banexg.Fee{Currency: "USDT", Cost: 0.02}, Status: banexg.OdStatusCanceled, LastUpdateTimestamp: 13}, nil
		}
		return &banexg.Order{ID: id, Status: banexg.OdStatusCanceled}, nil
	}
	if err := owner.Cancel(intent.ID, 13); err != nil {
		t.Fatal(err)
	}
	current, err = s.Order(context.Background(), intent.ID)
	if err != nil || current.State != OrderCanceled || current.FilledSteps != 2 || exchange.creates != 1 {
		t.Fatal(current, err)
	}
}

func TestBanexgIncompleteQueryAndTransientNotFoundKeepUnknown(t *testing.T) {
	for _, scenario := range []string{"nil-order", "missing-fee", "foreign-fee", "transient"} {
		t.Run(scenario, func(t *testing.T) {
			s, h, exchange, _, config := banexgFixture(t)
			adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
			if err != nil {
				t.Fatal(err)
			}
			intent := planOrder(t, s, "unknown-sdk", 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
			if err := s.PrepareOrder(context.Background(), intent, 10); err != nil {
				t.Fatal(err)
			}
			stored, _ := s.Order(context.Background(), intent.ID)
			exchange.create = func(string, string, string, float64, float64, map[string]any) (*banexg.Order, *errs.Error) {
				return nil, errs.NewMsg(errs.CodeTimeout, "unknown send")
			}
			owner := &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: adapter}
			if err := owner.Send(intent.ID, 11); err == nil {
				t.Fatal("unknown transport accepted")
			}
			exchange.query = func(symbol, id string, params map[string]any) (*banexg.Order, *errs.Error) {
				if scenario == "nil-order" {
					return nil, nil
				}
				if scenario == "transient" {
					return nil, errs.NewMsg(errs.CodeDataNotFound, "history window incomplete")
				}
				raw := &banexg.Order{ID: "venue-unknown", ClientOrderID: stored.ClientID, Symbol: symbol, Side: banexg.OdSideBuy, Amount: 1, Filled: 0.2, Cost: 20, Status: banexg.OdStatusPartFilled}
				if scenario == "foreign-fee" {
					raw.Fee = &banexg.Fee{Currency: "BTC", Cost: 0.01}
				}
				return raw, nil
			}
			if err := owner.Recover(intent.ID); err == nil {
				t.Fatal("incomplete query accepted")
			}
			current, err := s.Order(context.Background(), intent.ID)
			if err != nil || current.State != OrderUnknown || exchange.creates != 1 {
				t.Fatal(current, err)
			}
		})
	}
}

func TestBanexgBoundedInvocationJoinsAndRejectsCanceledCalls(t *testing.T) {
	s, h, exchange, transport, config := banexgFixture(t)
	var active context.Context
	joined := false
	transport.invoke = func(ctx context.Context, call func() error) error {
		active = ctx
		err := call()
		joined = true
		return err
	}
	exchange.create = func(string, string, string, float64, float64, map[string]any) (*banexg.Order, *errs.Error) {
		<-active.Done()
		return nil, errs.New(errs.CodeTimeout, active.Err())
	}
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	intent := planOrder(t, s, "bounded-sdk", 1, Buy, EntryIntent, map[string]int64{"strategy": 1})
	if err := s.PrepareOrder(context.Background(), intent, 10); err != nil {
		t.Fatal(err)
	}
	owner := &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: adapter}
	owner.Timeout = 250 * time.Millisecond
	if err := owner.Send(intent.ID, 11); err == nil || !joined || exchange.creates != 1 {
		t.Fatal("context-bound submit did not join", err, joined, exchange.creates)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := adapter.Submit(ctx, intent, "another-client"); !errors.Is(err, context.Canceled) || exchange.creates != 1 {
		t.Fatal("canceled call entered SDK", err, exchange.creates)
	}
}

func TestBanexgCompleteSnapshotAndVisibleBidAsk(t *testing.T) {
	_, _, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	exchange.positions = []*banexg.Position{{Symbol: "BTC/USDT:USDT", Contracts: 0.3, Side: banexg.PosSideShort}}
	exchange.orders = []*banexg.Order{{ID: "unassigned-manual", ClientOrderID: "manual"}}
	snapshot, err := adapter.Snapshot(context.Background())
	if err != nil || snapshot.Cash != "1000" || snapshot.Positions["BTC"] != -3 || len(snapshot.OpenOrders) != 1 || snapshot.OpenOrders[0].Receipt.ExchangeID != "unassigned-manual" || !exchange.fullSnapshot {
		t.Fatal(snapshot, err)
	}
	exchange.book = &banexg.OrderBook{Symbol: "BTC/USDT:USDT", TimeStamp: time.Now().UnixMilli(), Bids: &banexg.OdBookSide{Price: []float64{99}}, Asks: &banexg.OdBookSide{Price: []float64{101}}}
	quote, err := adapter.Observe(context.Background(), "BTC")
	if err != nil || !quote.Bid.Equal(intentPrice("99")) || !quote.Ask.Equal(intentPrice("101")) || quote.ReceivedMS < quote.AtMS || quote.ValidUntilMS <= quote.ReceivedMS {
		t.Fatal(quote, err)
	}
	exchange.book.Asks = nil
	if _, err := adapter.Observe(context.Background(), "BTC"); err == nil {
		t.Fatal("scalar/incomplete quote substituted")
	}
	exchange.positions[0].Hedged = true
	if _, err := adapter.Snapshot(context.Background()); err == nil {
		t.Fatal("hedged snapshot accepted")
	}
}

func TestBanexgZeroFillAbsenceProofAndPrivateRecoveryHints(t *testing.T) {
	s, _, exchange, transport, config := banexgFixture(t)
	transport.proof.AuthoritativeNotFound = true
	config.Absence = func(err error) bool {
		var sdk *errs.Error
		return errors.As(err, &sdk) && sdk.Code == errs.CodeDataNotFound && sdk.Message() == "authoritative-absence"
	}
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	intent := planOrder(t, s, "stream-sdk", 1, Buy, EntryIntent, map[string]int64{"strategy": 1})
	if err := s.PrepareOrder(context.Background(), intent, 10); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.Order(context.Background(), intent.ID)
	exchange.query = func(symbol, id string, params map[string]any) (*banexg.Order, *errs.Error) {
		return &banexg.Order{ID: "stream-venue", ClientOrderID: stored.ClientID, Symbol: symbol, Side: banexg.OdSideBuy, Amount: 0.1, Status: banexg.OdStatusOpen}, nil
	}
	query, err := adapter.Query(context.Background(), stored.ClientID, "")
	if err != nil || !query.Complete || len(query.Receipt.Fills) != 0 {
		t.Fatal("zero-fill query fabricated invalid fill", query, err)
	}
	exchange.query = func(string, string, map[string]any) (*banexg.Order, *errs.Error) {
		return nil, errs.NewMsg(errs.CodeDataNotFound, "authoritative-absence")
	}
	query, err = adapter.Query(context.Background(), stored.ClientID, "")
	if err != nil || query.Found || !query.Authoritative {
		t.Fatal(query, err)
	}
	exchange.trades = make(chan *banexg.MyTrade, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports, err := adapter.Reports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exchange.trades <- &banexg.MyTrade{ClientID: stored.ClientID}
	exchange.trades <- &banexg.MyTrade{Trade: banexg.Trade{Order: "manual-stream-order"}}
	for index := 0; index < 2; index++ {
		select {
		case report := <-reports:
			if index == 0 && (report.Err != nil || report.OrderID != intent.ID) {
				t.Fatal(report)
			}
			if index == 1 && (report.Err == nil || report.UnassignedExchangeID != "manual-stream-order") {
				t.Fatal(report)
			}
		case <-time.After(time.Second):
			t.Fatal("stream normalization stalled")
		}
	}
	cancel()
	select {
	case _, ok := <-reports:
		if ok {
			t.Fatal("canceled private stream did not join")
		}
	case <-time.After(time.Second):
		t.Fatal("private stream did not stop")
	}
}

func TestBanexgMigrationSnapshotExactBasisAndNoFloatTruncation(t *testing.T) {
	_, _, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	exchange.positions = []*banexg.Position{{Symbol: "BTC/USDT:USDT", Side: banexg.PosSideLong, Contracts: 0.3, ContractSize: 1, EntryPrice: 100}}
	exchange.orders = []*banexg.Order{{ID: "old-order", ClientOrderID: "old-client", Symbol: "BTC/USDT:USDT", Side: banexg.OdSideSell, Amount: 0.2, Filled: 0.1, Cost: 10, Fee: &banexg.Fee{Currency: "USDT", Cost: 0.01}, Status: banexg.OdStatusPartFilled}}
	snapshot, err := adapter.MigrationSnapshot(context.Background())
	if err != nil || !snapshot.Complete || len(snapshot.Positions) != 1 || snapshot.Positions[0].SignedSteps != 3 || !snapshot.Positions[0].CostBasis.Equal(intentPrice("30")) || len(snapshot.OpenOrders) != 1 || snapshot.OpenOrders[0].FilledSteps != 1 || !snapshot.OpenOrders[0].Fee.Equal(intentPrice("0.01")) {
		t.Fatal(snapshot, err)
	}
	if _, err := floatBoundary(intentPrice("9007199254740993")); err == nil {
		t.Fatal("exact quantity silently rounded across float API")
	}
	if _, err := quantitySteps(0.15, ledgerInstrument()); err == nil {
		t.Fatal("off-step fill silently truncated")
	}
	exchange.positions[0].ContractSize = 2
	if _, err := adapter.MigrationSnapshot(context.Background()); err == nil {
		t.Fatal("wrong actual position units accepted")
	}
}

func TestBanexgOwnerStoreBindingAndFrozenLimitMetadata(t *testing.T) {
	s, _, exchange, _, config := banexgFixture(t)
	config.Store = nil
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Snapshot(context.Background()); err == nil || exchange.snapshots != 0 {
		t.Fatal("unbound adapter entered transport", err)
	}
	if err := adapter.BindStore(s); err != nil {
		t.Fatal(err)
	}
	if err := adapter.BindStore(s); err != nil {
		t.Fatal("same binding retry rejected", err)
	}
	if err := adapter.BindStore(&Store{key: s.key}); err == nil {
		t.Fatal("published adapter rebound another SQLiteview")
	}
	exchange.create = func(symbol, kind, side string, amount, price float64, params map[string]any) (*banexg.Order, *errs.Error) {
		if kind != banexg.OdTypeLimit || side != banexg.OdSideSell || price != 101 || params[banexg.ParamReduceOnly] != true {
			t.Fatal("frozen limit/reduce-only lost", kind, side, price, params)
		}
		return &banexg.Order{ID: "limit-venue", ClientOrderID: "limit-client"}, nil
	}
	_, err = adapter.Submit(context.Background(), OrderIntent{ID: "limit-order", Instrument: ledgerInstrument(), Side: Sell, Steps: 3, Limit: intentPrice("101"), ReduceOnly: true}, "limit-client")
	if err != nil {
		t.Fatal(err)
	}
}
