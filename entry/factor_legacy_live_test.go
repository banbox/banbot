package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

// Only the external SDK and registered source are fixtures. Startup, account
// policies, quote registration, TS restoration, and Trader dispatch are real.
type legacyEntryExchange struct {
	liveEntryExchange
	mu         sync.Mutex
	positions  map[string]float64
	orders     map[string]*banexg.Order
	prices     map[string]float64
	books      map[string]int
	cash       float64
	quoteDelay time.Duration
}

func (e *legacyEntryExchange) FetchBalance(map[string]any) (*banexg.Balances, *errs.Error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return &banexg.Balances{Total: map[string]float64{"USD": e.cash}}, nil
}
func (e *legacyEntryExchange) FetchPositions([]string, map[string]any) ([]*banexg.Position, *errs.Error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var rows []*banexg.Position
	for symbol, amount := range e.positions {
		if amount == 0 {
			continue
		}
		side := banexg.PosSideLong
		if amount < 0 {
			side = banexg.PosSideShort
			amount = -amount
		}
		rows = append(rows, &banexg.Position{Symbol: symbol, Contracts: amount, Side: side})
	}
	return rows, nil
}
func (e *legacyEntryExchange) FetchOrderBook(symbol string, _ int, _ map[string]any) (*banexg.OrderBook, *errs.Error) {
	if e.quoteDelay > 0 {
		time.Sleep(e.quoteDelay)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.books[symbol]++
	price := e.prices[symbol]
	return &banexg.OrderBook{Symbol: symbol, TimeStamp: time.Now().UnixMilli(), Bids: &banexg.OdBookSide{Price: []float64{price}}, Asks: &banexg.OdBookSide{Price: []float64{price}}}, nil
}
func (e *legacyEntryExchange) CreateOrder(symbol, kind, side string, amount, price float64, params map[string]any) (*banexg.Order, *errs.Error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	price = e.prices[symbol]
	id := fmt.Sprintf("fixture-%d", len(e.orders)+1)
	od := &banexg.Order{ID: id, ClientOrderID: params[banexg.ParamClientOrderId].(string), Symbol: symbol, Type: kind, Side: side, Amount: amount, Filled: amount, Cost: amount * price, Average: price, Status: banexg.OdStatusFilled, Timestamp: time.Now().UnixMilli(), Fee: &banexg.Fee{Currency: "USD"}}
	e.orders[id] = od
	delta := amount
	if side == banexg.OdSideSell {
		delta = -amount
	}
	// The seeded BTC cost basis is 100; software protection realizes its loss.
	if symbol == "BTC/USD" && delta < 0 {
		e.cash += (price - 100) * amount
	}
	e.positions[symbol] += delta
	return od, nil
}
func (e *legacyEntryExchange) FetchOrder(_ string, id string, _ map[string]any) (*banexg.Order, *errs.Error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.orders[id], nil
}

type legacyEntrySource struct {
	liveEntrySource
	run func(context.Context, []*strat.DataSub, data.DataSink) error
}

type legacyEntryTransport struct {
	liveEntryTransport
	block      atomic.Bool
	entered    chan struct{}
	returned   chan struct{}
	setupBlock bool
	calls      atomic.Int32
}

func (p *legacyEntryTransport) Invoke(ctx context.Context, call func() error) error {
	// The seeded terminal history query and startup account snapshot precede
	// private-stream setup. Block setup itself, not either recovery operation.
	if p.block.CompareAndSwap(true, false) || p.setupBlock && p.calls.Add(1) == 3 {
		close(p.entered)
		<-ctx.Done()
		close(p.returned)
		return ctx.Err()
	}
	return p.liveEntryTransport.Invoke(ctx, call)
}

func (s *legacyEntrySource) SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) error {
	return s.run(ctx, subs, sink)
}

func (s *legacyEntrySource) WarmupStart(_ context.Context, sub *orm.Subscription, anchor int64) (int64, error) {
	// The fixture exposes three explicitly dated irregular observations.
	history := []int64{anchor - 1000, anchor - 600, anchor - 400}
	if sub.WarmupNum <= 0 || sub.WarmupNum > len(history) {
		return 0, errors.New("fixture observation history unavailable")
	}
	return history[len(history)-sub.WarmupNum], nil
}

func (s *legacyEntrySource) FetchHistory(_ context.Context, sub *orm.Subscription, start, end int64) ([]*orm.DataRecord, error) {
	var rows []*orm.DataRecord
	for _, at := range []int64{end - 1000, end - 600, end - 400} {
		if at >= start {
			rows = append(rows, &orm.DataRecord{Sid: sub.ExSymbol.ID, TimeMS: at, EndMS: at, Closed: true, Values: map[string]any{"close": 100.0, "integer": int64(sub.ExSymbol.ID), "nullable": nil}})
		}
	}
	return rows, nil
}

func (s *legacyEntrySource) SubscribeManaged(ctx context.Context, subs []*orm.Subscription, sink data.DataSink) (data.LiveSourceSubscription, error) {
	ctx, cancel := context.WithCancel(ctx)
	h := &liveEntryHandle{cancel: cancel, done: make(chan struct{}), failures: make(chan error, 1)}
	go func() {
		defer close(h.done)
		if ready, ok := sink.(interface{ AwaitLiveReady(context.Context) error }); ok {
			if err := ready.AwaitLiveReady(ctx); err != nil {
				return
			}
		}
		if err := s.SubscribeLive(ctx, subs, sink); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			h.err = err
			h.failures <- err
		}
	}()
	return h, nil
}

func TestRegisteredFactorLiveRestoresLegacyOutsideUniverse(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Chunks = nil
	c.Mode = runner.Trade
	c.AccountID, c.StrategyID = "default", "cs"
	c.DecisionInterval, c.LatencyMS = 100, 1
	c.Execution.StorePath, c.Execution.SenderLeaseDir = filepath.Join(dir, "ledger.db"), filepath.Join(dir, "leases")
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Snapshot.Universe = factor.Universe{Version: "eth-only", Static: true, Tracked: []int32{2, 3}, Investable: []int32{2, 3}, Reference: []int32{2, 3}, Tradable: []int32{2, 3}, Evaluation: []int32{2, 3}}
	c.Snapshot.SIDMap = map[int32]string{2: "ETH/USD", 3: "ETH-PERP/USD"}
	basis := execution.Instrument{ID: "BTC/USD", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	c.Execution.Instruments = map[int32]execution.Instrument{}
	for sid, symbol := range c.Snapshot.SIDMap {
		unit := basis
		unit.ID = symbol
		c.Execution.Instruments[sid] = unit
	}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USD"}
	risk := execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(10000), MaxVirtualGross: decimal.NewFromInt(20000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}
	bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{basis.ID: basis}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000)}}, Risk: risk, IntentTTLMS: 60000, Quote: func(_ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000}, nil
	}}
	symbol := &orm.ExSymbol{ID: 1, Symbol: basis.ID, Exchange: "entrytest", Market: "linear"}
	seed := runtime.NewProcess()
	paper, err := runner.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := seed.NewRuntime(runtime.Options{Mode: core.RunModeBackTest, AccountOwnerKey: &key, SharedExecution: &biz.SharedExecutionOptions{StorePath: c.Execution.StorePath, SenderLeaseDir: c.Execution.SenderLeaseDir, Adapter: paper, AuthoritativeSnapshot: true}, SharedOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(seed.Close)
	rt.Clock.SetTimeMS(time.Now().UnixMilli())
	for _, event := range []execution.CashEvent{
		{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(1000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(1000)}}, AtMS: 100},
		{ID: "allocate", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-1000)}, {Strategy: "ts", Amount: decimal.NewFromInt(500)}, {Strategy: "cs", Amount: decimal.NewFromInt(500)}}, AtMS: 100},
	} {
		if err := rt.SharedExecution().CashEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt.SharedExecution().Reconcile("seed", 100); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: symbol, TimeFrame: "event", Account: "default", ExgStopLoss: true, CloseLong: true, CloseShort: true}
	job.BindRuntimeState(rt.Strategies, rt.Core, rt.Clock)
	job.BindRuntimeMarket(rt.Market.Prices, rt.Clock)
	rt.Market.Prices.SetBarPriceAt(rt.Clock.TimeMS(), basis.ID, 100)
	if err := job.OpenOrder(&strat.EnterReq{Tag: "seeded", Amount: 1, StopLoss: 95}); err != nil {
		t.Fatal(err)
	}
	entries, _, orderErr := biz.GetOdMgrWithState(rt.Trading, "default").ProcessOrders(job)
	if orderErr != nil || len(entries) != 1 || entries[0].Enter.Filled != 1 {
		t.Fatalf("seed real TS entry: %v %v", entries, orderErr)
	}
	if err := job.OpenOrder(&strat.EnterReq{Tag: "pending-feed", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 2}); err != nil {
		t.Fatal(err)
	}
	pending, _, pendingErr := biz.GetOdMgrWithState(rt.Trading, "default").ProcessOrders(job)
	if pendingErr != nil || len(pending) != 1 || pending[0].Enter.Filled != 0 {
		t.Fatal("seed pending StopBars", pending, pendingErr)
	}
	// Startup now queries terminal history as well as active orders. Carry the
	// paper-seeded venue receipt into the fake SDK's authoritative history;
	// an empty order map cannot prove recovery of this already-filled entry.
	seedEvents, err := rt.SharedExecution().Service().Store().EventsAfter(context.Background(), 0, 512)
	if err != nil {
		t.Fatal(err)
	}
	var seedID string
	for _, event := range seedEvents {
		if event.Kind != "OrderState" {
			continue
		}
		var state execution.OrderStateEvent
		if err := json.Unmarshal(event.Payload, &state); err != nil {
			t.Fatal(err)
		}
		if state.State == execution.OrderFilled {
			seedID = state.OrderID
		}
	}
	if seedID == "" {
		t.Fatal("seeded entry lacks its terminal execution event")
	}
	stored, err := rt.SharedExecution().Service().Store().Order(context.Background(), seedID)
	if err != nil || stored.State != execution.OrderFilled {
		t.Fatal("seeded entry history is not terminal", stored, err)
	}
	amount := stored.Intent.Instrument.QuantityStep.Mul(decimal.NewFromInt(stored.Intent.Steps)).InexactFloat64()
	filled := stored.Intent.Instrument.QuantityStep.Mul(decimal.NewFromInt(stored.FilledSteps)).InexactFloat64()
	seedReceipt := &banexg.Order{ID: stored.ExchangeID, ClientOrderID: stored.ClientID, Symbol: basis.ID, Type: banexg.OdTypeMarket, Side: string(stored.Intent.Side), Amount: amount, Filled: filled, Cost: stored.ReportedCost.InexactFloat64(), Average: 100, Status: banexg.OdStatusFilled, Timestamp: rt.Clock.TimeMS(), Fee: &banexg.Fee{Currency: "USD", Cost: stored.ReportedFee.InexactFloat64()}}
	seed.Close()
	exchange := &legacyEntryExchange{liveEntryExchange: liveEntryExchange{trades: make(chan *banexg.MyTrade)}, positions: map[string]float64{basis.ID: 1}, orders: map[string]*banexg.Order{seedReceipt.ID: seedReceipt}, prices: map[string]float64{basis.ID: 100, "ETH/USD": 100, "ETH-PERP/USD": 100}, books: map[string]int{}, cash: 1000}

	launchSerial := 0
	launch := func(t *testing.T, mutation string, exit bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		launchSerial++
		name := fmt.Sprintf("legacy_entry_%d_%d", time.Now().UnixNano(), launchSerial)
		current := c
		current.Prices = runner.PriceStream{Source: name, Frequency: "event", Field: "close"}
		current.Snapshot.Schemas = map[string]string{name: "schema-v1"}
		current.Snapshot.SourceVersions = map[string]string{name: "v1"}
		plan, err := factor.New().Add("signal", factor.Field(name, "integer", "event")).Compile()
		if err != nil {
			t.Fatal(err)
		}
		current.Plan = plan
		current.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"signal"}, Weights: map[string]float64{"signal": 1}}
		var output bytes.Buffer
		transport := &legacyEntryTransport{liveEntryTransport: liveEntryTransport{key: key}, entered: make(chan struct{}), returned: make(chan struct{})}
		if mutation == "blocked setup" {
			transport.setupBlock = true
			go func() {
				select {
				case <-transport.entered:
					cancel()
				case <-ctx.Done():
				}
			}()
		}
		var callbackErr error
		callbacks := 0
		warmupCallbacks := 0
		exitCommitted := make(chan struct{}, 1)
		completed := false
		feedExpired := false
		var waitingStatus int64
		liveJob := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: symbol, TimeFrame: "event", Account: "default", ExgStopLoss: true, CloseLong: true, CloseShort: true}
		liveJob.Strat.OnData = func(s *strat.StratJob, event strat.DataEvent) {
			if s.IsWarmUp {
				warmupCallbacks++
				if event.Raw("nullable") != nil || event.Raw("integer") != int64(symbol.ID) {
					callbackErr = errors.New("warmup changed custom field type/NULL")
				}
				return
			}
			callbacks++
			orders, ok := s.OpenOrdersSnapshot()
			var held, waiting *ormo.InOutOrder
			for _, od := range orders {
				if od.ID == entries[0].ID {
					held = od
				}
				if od.ID == pending[0].ID {
					waiting = od
				}
			}
			if !ok || held == nil || held.Enter.Filled != 1 || held.GetStopLoss() == nil || held.GetStopLoss().Price != 95 {
				callbackErr = fmt.Errorf("restored TS facade/protection: %v %v", ok, orders)
			}
			if waiting != nil {
				waitingStatus = waiting.Status
			}
			if waiting != nil && waiting.Status >= ormo.InOutStatusFullExit {
				feedExpired = true
			}
			if event.Raw("nullable") != nil {
				callbackErr = errors.New("NULL field changed")
			}
		}
		liveJob.Strat.OnOrderChange = func(_ *strat.StratJob, od *ormo.InOutOrder, _ int) {
			if od.Exit != nil && od.Exit.Filled == 1 {
				select {
				case exitCommitted <- struct{}{}:
				default:
				}
			}
		}
		binding := FactorLiveBinding{Account: key, Transport: transport, Symbols: map[int32]*orm.ExSymbol{1: symbol}, AccountInstruments: []execution.BanexgInstrument{{Symbol: basis.ID, Instrument: basis}}, Legacy: &FactorLegacyLiveBinding{Bridge: bridge, Jobs: []*strat.StratJob{liveJob}, Subscriptions: []*strat.DataSub{{Source: name, TimeFrame: "event", ExSymbol: symbol, Fields: []string{"close", "nullable"}, WarmupNum: 3}}}, VerifyFunding: func(context.Context, string) (string, error) { return "fixture-no-funding", nil }, Record: func(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
			return factor.VersionRecord{Series: *s, EventTime: s.EndMS, AvailableAt: s.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
		}}
		for sid, sym := range current.Snapshot.SIDMap {
			binding.Symbols[sid] = &orm.ExSymbol{ID: sid, Symbol: sym, Exchange: "entrytest", Market: "linear"}
		}
		switch mutation {
		case "missing mapping":
			binding.AccountInstruments = nil
		case "mismatched mapping":
			binding.AccountInstruments[0].Instrument.QuantityStep = decimal.RequireFromString("0.2")
		case "missing TS":
			binding.Legacy = nil
		case "mismatched TS":
			liveJob.Strat.Name = "undeclared"
		}
		if err := data.RegisterDataSourceFactory(name, func() data.DataSource {
			return &legacyEntrySource{liveEntrySource: liveEntrySource{name: name}, run: func(sourceCtx context.Context, subs []*strat.DataSub, sink data.DataSink) error {
				bySID := map[int32]*strat.DataSub{}
				for _, sub := range subs {
					bySID[sub.ExSymbol.ID] = sub
				}
				if len(bySID) != 3 || bySID[1] == nil || bySID[1].WarmupNum != 3 || !slices.Contains(bySID[1].Fields, "nullable") || !slices.Contains(bySID[2].Fields, "integer") || !slices.Contains(bySID[2].Fields, "close") {
					return fmt.Errorf("incomplete automatic subscription union: %v", subs)
				}
				grid := (time.Now().UnixMilli()/100 - 2) * 100
				emit := func(sid int32, stamp int64, closed bool) error {
					return sink.Emit(bySID[sid], []*orm.DataRecord{{Sid: sid, TimeMS: stamp, EndMS: stamp, Closed: closed, Values: map[string]any{"close": 100.0, "integer": int64(sid), "nullable": nil}}})
				}
				if mutation == "blocked quote" {
					transport.block.Store(true)
					go func() {
						select {
						case <-transport.entered:
							cancel()
						case <-ctx.Done():
						}
					}()
					// Cancel the entry while an admitted source callback waits in
					// verified quote IO. Only production startup cleanup stops it.
					err := emit(1, grid, false)
					if !errors.Is(err, context.Canceled) {
						return fmt.Errorf("blocked callback result: %w", err)
					}
					completed = true
					return err
				}
				if err := emit(1, grid, false); err != nil {
					return err
				}
				if callbackErr != nil {
					return callbackErr
				}
				if mutation == "feed expiry" {
					if feedExpired {
						return errors.New("pending StopBars expired before primary feed advanced")
					}
					if err := emit(1, grid+1, true); err != nil {
						return err
					}
					if feedExpired {
						return errors.New("pending StopBars expired after only one bar")
					}
					if err := emit(1, grid+2, true); err != nil {
						return err
					}
					if !feedExpired {
						return fmt.Errorf("actual entry SDK Bar0 did not expire StopBars: pending status=%d callbacks=%d", waitingStatus, callbacks)
					}
				}
				for _, sid := range []int32{2, 3} {
					if err := emit(sid, grid, true); err != nil {
						return err
					}
				}
				// Quotes must become observable after the completed decision's
				// explicit execution latency, not at its earlier grid timestamp.
				timer := time.NewTimer(time.Duration(current.LatencyMS+1) * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-sourceCtx.Done():
					return sourceCtx.Err()
				}
				for _, sid := range []int32{2, 3} {
					if err := emit(sid, time.Now().UnixMilli()-1, false); err != nil {
						return err
					}
				}
				exchange.mu.Lock()
				csFilled := exchange.positions["ETH/USD"] != 0 && exchange.positions["ETH-PERP/USD"] != 0
				btc := exchange.positions[basis.ID]
				books := exchange.books[basis.ID]
				exchange.mu.Unlock()
				if output.Len() == 0 || !csFilled || btc != 1 || books == 0 {
					return fmt.Errorf("CS publication/execution did not preserve marked outside TS: output=%d cs=%v btc=%v books=%d", output.Len(), csFilled, btc, books)
				}
				if exit {
					exchange.mu.Lock()
					exchange.prices[basis.ID] = 94
					exchange.mu.Unlock()
					// Software protection runs in feedFactorLegacy before OnData.
					liveJob.Strat.OnData = func(*strat.StratJob, strat.DataEvent) { callbacks++ }
					if err := emit(1, time.Now().UnixMilli()-1, false); err != nil {
						return err
					}
					exchange.mu.Lock()
					remaining := exchange.positions[basis.ID]
					csPreserved := exchange.positions["ETH/USD"] != 0 && exchange.positions["ETH-PERP/USD"] != 0
					var report *banexg.MyTrade
					for _, od := range exchange.orders {
						if od.Symbol == basis.ID && od.Side == banexg.OdSideSell {
							report = &banexg.MyTrade{Trade: banexg.Trade{Order: od.ID}, ClientID: od.ClientOrderID}
						}
					}
					exchange.mu.Unlock()
					if remaining != 0 || report == nil || !csPreserved {
						return fmt.Errorf("restored software stop: btc=%v report=%v cs=%v", remaining, report, csPreserved)
					}
					// SDK create acknowledgements carry identity only. A private
					// report must recover the authoritative fill before its callback.
					select {
					case exchange.trades <- report:
					case <-sourceCtx.Done():
						return sourceCtx.Err()
					}
					select {
					case <-exitCommitted:
					case <-sourceCtx.Done():
						return fmt.Errorf("software exit callback not committed: %w", sourceCtx.Err())
					}
				}
				completed = true
				cancel()
				<-sourceCtx.Done()
				return sourceCtx.Err()
			}}
		}); err != nil {
			t.Fatal(err)
		}
		if err := RegisterFactorLiveBinding(name, func(context.Context, banexg.BanExchange, *config.Snapshot, runner.Config) (FactorLiveBinding, error) {
			return binding, nil
		}); err != nil {
			t.Fatal(err)
		}
		defer func() { factorLiveBindings.Lock(); delete(factorLiveBindings.items, name); factorLiveBindings.Unlock() }()
		factory, err := factorLiveFactory(name)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			conn, err := listener.Accept()
			if err == nil {
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
			}
		}()
		cfg := &config.Config{Exchange: &config.ExchangeConfig{Name: "entrytest"}, MarketType: "linear", SpiderAddr: listener.Addr().String()}
		snapshot := config.NewSnapshotWithDirs(cfg, dir, "", nil)
		bound, err := factory(ctx, exchange, snapshot, current)
		if err != nil {
			t.Fatal(err)
		}
		sessionCtx, sessionCancel := context.WithCancel(context.Background())
		session := &explicitEntrySession{process: runtime.NewProcess(), ctx: sessionCtx, cancel: sessionCancel, exchange: exchange}
		err = session.runFactorLive(ctx, snapshot, current, bound, &output)
		session.close()
		if mutation != "" && mutation != "blocked quote" && mutation != "blocked setup" && mutation != "feed expiry" {
			expected := map[string]string{"missing mapping": "explicit account SDK mapping", "mismatched mapping": "descriptor mismatch", "missing TS": "no configured live job", "mismatched TS": "strategy is undeclared"}[mutation]
			if err == nil || !strings.Contains(err.Error(), expected) {
				t.Fatalf("invalid binding was admitted: %v", err)
			}
			if callbacks != 0 {
				t.Fatal("invalid binding dispatched TS")
			}
			t.Logf("fail closed (%s): %v", mutation, err)
			return
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("live startup/source regression: %v", err)
		}
		if mutation == "blocked quote" || mutation == "blocked setup" {
			if mutation == "blocked quote" && !completed || callbacks != 0 {
				t.Fatalf("blocked callback was not joined: completed=%v calls=%d", completed, callbacks)
			}
			select {
			case <-transport.returned:
			default:
				t.Fatal("blocked quote IO was not canceled and joined")
			}
			return
		}
		if warmupCallbacks != 3 {
			t.Fatalf("legacy startup history not advanced: warmup callbacks=%d", warmupCallbacks)
		}
		if callbackErr != nil || callbacks == 0 || !completed {
			t.Fatalf("restored callback/source: calls=%d completed=%v error=%v startup=%v", callbacks, completed, callbackErr, err)
		}
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Fatal("provider socket not joined")
		}
	}
	for _, mutation := range []string{"missing mapping", "mismatched mapping", "missing TS", "mismatched TS"} {
		t.Run(strings.ReplaceAll(mutation, " ", "_"), func(t *testing.T) { launch(t, mutation, false) })
	}
	t.Run("blocked_quote_callback_joins_on_cancel", func(t *testing.T) { launch(t, "blocked quote", false) })
	t.Run("blocked_private_setup_joins_on_request_cancel", func(t *testing.T) { launch(t, "blocked setup", false) })
	t.Run("restored_TS_and_CS", func(t *testing.T) { launch(t, "", false) })
	t.Run("actual_primary_feed_expires_SDK_zero_bar", func(t *testing.T) { launch(t, "feed expiry", false) })
	t.Run("repeat_startup_and_software_exit", func(t *testing.T) { launch(t, "", true) })
}
