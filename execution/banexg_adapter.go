package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

// BanexgExecutionTransport binds the entire synchronous SDK invocation to the
// owner context, including SDK semaphore/retry waits before HTTP dispatch.
// An HTTP client timeout alone is insufficient in banexg v0.2.64. Plain SDK
// sessions fail startup until the composition supplies this verified binding.
type BanexgExecutionTransport interface {
	Verify(context.Context, banexg.BanExchange, AccountKey) (BanexgExecutionProof, error)
	Invoke(context.Context, func() error) error // joins the real call before returning, including cancellation
}

type BanexgExecutionProof struct {
	Account                   AccountKey
	EvidenceID                string
	ContextBound              bool
	StableClientID            bool
	QueryClientID             bool
	AuthoritativeNotFound     bool
	CompleteCumulativeReports bool
	CompleteAccountSnapshot   bool
	SettledCash               bool // normalized Asset.Total is settled cash, excluding unrealized PnL
	NetLinearPositions        bool
	PostOnly                  bool // verified unified limit_maker rejection of marketable orders
}

type BanexgInstrument struct {
	Symbol     string
	Instrument Instrument
}

type BanexgAdapterConfig struct {
	Account     AccountKey
	Store       *Store
	Instruments []BanexgInstrument
	Transport   BanexgExecutionTransport
	// Absence must distinguish authoritative nonexistence from history-window,
	// pagination, backend and replication failures. Generic error codes do not.
	Absence  func(error) bool
	QuoteTTL time.Duration
}

type BanexgAdapter struct {
	exchange banexg.BanExchange
	config   BanexgAdapterConfig
	proof    BanexgExecutionProof
	byID     map[string]BanexgInstrument
	bySymbol map[string]Instrument
	store    atomic.Pointer[Store]
}

func missingBanexgBinding(value any) bool {
	return value == nil || reflect.ValueOf(value).Kind() == reflect.Pointer && reflect.ValueOf(value).IsNil()
}

// NewBanexgAdapter performs read-only capability verification. It never creates
// or cancels an order and never derives safety guarantees from venue names or
// HasApi alone. All actual calls remain synchronous inside the owner gate.
func NewBanexgAdapter(ctx context.Context, exchange banexg.BanExchange, config BanexgAdapterConfig) (*BanexgAdapter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if missingBanexgBinding(exchange) || missingBanexgBinding(config.Transport) || config.Store != nil && config.Store.Account() != config.Account || len(config.Instruments) == 0 {
		return nil, errors.New("execution: banexg requires an account store, units and verified context-bound transport")
	}
	if err := config.Account.Validate(); err != nil {
		return nil, err
	}
	if config.Store != nil && config.Store.Durability() != Durable {
		return nil, errors.New("execution: real venue adapter requires durable storage")
	}
	proof, err := config.Transport.Verify(ctx, exchange, config.Account)
	if err != nil || ctx.Err() != nil {
		return nil, errors.Join(err, ctx.Err())
	}
	if proof.Account != config.Account || !canonicalID(proof.EvidenceID) || !proof.ContextBound || !proof.StableClientID || !proof.QueryClientID || !proof.CompleteCumulativeReports || !proof.CompleteAccountSnapshot || !proof.SettledCash || !proof.NetLinearPositions {
		return nil, errors.New("execution: banexg execution/query/snapshot/transport capability is unproven")
	}
	if proof.AuthoritativeNotFound && config.Absence == nil {
		return nil, errors.New("execution: authoritative absence requires a proven classifier")
	}
	for _, api := range []string{banexg.ApiCreateOrder, banexg.ApiCancelOrder, banexg.ApiFetchOrder, banexg.ApiFetchOpenOrders, banexg.ApiFetchBalance, banexg.ApiFetchPositions, banexg.ApiFetchOrderBook} {
		if !exchange.HasApi(api, "") {
			return nil, fmt.Errorf("execution: banexg required API unavailable: %s", api)
		}
	}
	a := &BanexgAdapter{exchange: exchange, config: config, proof: proof, byID: make(map[string]BanexgInstrument), bySymbol: make(map[string]Instrument)}
	if config.Store != nil {
		a.store.Store(config.Store)
	}
	for _, entry := range config.Instruments {
		if err := entry.Instrument.Validate(); err != nil {
			return nil, err
		}
		if !canonicalID(entry.Symbol) || entry.Instrument.SettlementCurrency != config.Account.SettlementDomain {
			return nil, errors.New("execution: invalid banexg symbol/settlement mapping")
		}
		if _, ok := a.byID[entry.Instrument.ID]; ok {
			return nil, errors.New("execution: duplicate banexg instrument")
		}
		if _, ok := a.bySymbol[entry.Symbol]; ok {
			return nil, errors.New("execution: duplicate banexg symbol")
		}
		a.byID[entry.Instrument.ID], a.bySymbol[entry.Symbol] = entry, entry.Instrument
	}
	if a.config.QuoteTTL <= 0 {
		a.config.QuoteTTL = 2 * time.Second
	}
	return a, nil
}

// BindStore breaks the shared-service bootstrap cycle without opening a second
// database view. A published adapter cannot be rebound to another store/account.
func (a *BanexgAdapter) BindStore(store *Store) error {
	if store == nil || store.Account() != a.config.Account || store.Durability() != Durable {
		return errors.New("execution: adapter store/account mismatch")
	}
	if a.store.CompareAndSwap(nil, store) || a.store.Load() == store {
		return nil
	}
	return errors.New("execution: banexg adapter already bound to a different store")
}

func (a *BanexgAdapter) Capabilities() AdapterCapabilities {
	return AdapterCapabilities{PostOnly: a.proof.PostOnly, QueryClientID: a.proof.QueryClientID, AuthoritativeNotFound: a.proof.AuthoritativeNotFound, CumulativeReports: true}
}

func (a *BanexgAdapter) invoke(ctx context.Context, call func() error) error {
	if a.store.Load() == nil {
		return errors.New("execution: banexg adapter is not bound to its owner store")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := a.config.Transport.Invoke(ctx, call)
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return err
}

func (a *BanexgAdapter) params() map[string]any {
	return map[string]any{banexg.ParamAccount: a.config.Account.Account, banexg.ParamRetry: 0}
}

func floatBoundary(value decimal.Decimal) (float64, error) {
	n := value.InexactFloat64()
	if math.IsNaN(n) || math.IsInf(n, 0) || !decimal.NewFromFloat(n).Equal(value) {
		return 0, errors.New("execution: exact decimal cannot cross banexg float boundary")
	}
	return n, nil
}

func decimalBoundary(value float64) (decimal.Decimal, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return decimal.Zero, errors.New("execution: nonfinite banexg numeric value")
	}
	return decimal.NewFromFloat(value), nil
}

func quantitySteps(value float64, instrument Instrument) (int64, error) {
	quantity, err := decimalBoundary(value)
	if err != nil {
		return 0, err
	}
	steps, remainder := quantity.QuoRem(instrument.QuantityStep, 0)
	if !remainder.IsZero() || steps.IsNegative() || steps.GreaterThan(decimal.NewFromInt(1<<63-1)) {
		return 0, errors.New("execution: banexg quantity is outside exact step lattice")
	}
	return steps.IntPart(), nil
}

func (a *BanexgAdapter) Submit(ctx context.Context, intent OrderIntent, clientID string) (SubmitReceipt, error) {
	entry, ok := a.byID[intent.Instrument.ID]
	if !ok {
		return SubmitReceipt{}, errors.New("execution: unmapped banexg instrument")
	}
	expected, _ := payload(entry.Instrument)
	actual, _ := payload(intent.Instrument)
	if expected != actual || intent.Steps <= 0 || intent.Side != Buy && intent.Side != Sell || intent.Limit.IsNegative() || !canonicalID(clientID) {
		return SubmitReceipt{}, errors.New("execution: invalid banexg frozen order/units/client ID")
	}
	if intent.PostOnly && (!intent.Limit.IsPositive() || !a.proof.PostOnly) {
		return SubmitReceipt{}, errors.New("execution: post-only venue support or limit is unproven")
	}
	amount, err := floatBoundary(decimal.NewFromInt(intent.Steps).Mul(intent.Instrument.QuantityStep))
	if err != nil {
		return SubmitReceipt{}, err
	}
	price, err := floatBoundary(intent.Limit)
	if err != nil {
		return SubmitReceipt{}, err
	}
	typeName := banexg.OdTypeMarket
	if intent.Limit.IsPositive() {
		typeName = banexg.OdTypeLimit
	}
	if intent.PostOnly {
		typeName = banexg.OdTypeLimitMaker
	}
	params := a.params()
	params[banexg.ParamClientOrderId] = clientID
	params[banexg.ParamReduceOnly] = intent.ReduceOnly
	var raw *banexg.Order
	err = a.invoke(ctx, func() error {
		var sdkErr error
		order, e := a.exchange.CreateOrder(entry.Symbol, typeName, string(intent.Side), amount, price, params)
		raw = order
		if e != nil {
			sdkErr = e
		}
		return sdkErr
	})
	if err != nil {
		return SubmitReceipt{}, err
	}
	if raw == nil || !canonicalID(raw.ID) || raw.ClientOrderID != clientID {
		return SubmitReceipt{}, errors.New("execution: banexg acknowledgement does not prove stable client identity")
	}
	// Creation acknowledgements often omit authoritative cumulative fees. They
	// establish only identity; all fills come from a subsequent complete query.
	return SubmitReceipt{ExchangeID: raw.ID, Rejected: raw.Status == banexg.OdStatusRejected}, nil
}

func (a *BanexgAdapter) storedIdentity(ctx context.Context, clientID, exchangeID string) (StoredOrder, error) {
	var order StoredOrder
	store := a.store.Load()
	if store == nil {
		return order, errors.New("execution: banexg adapter store unbound")
	}
	err := store.commit(ctx, func(tx *storeTxn) error {
		var id string
		if err := tx.QueryRow(opFindVenueOrder, store.accountID, clientID, clientID, exchangeID, exchangeID).Scan(&id); err != nil {
			return err
		}
		var err error
		order, err = store.readOrder(ctx, tx, id)
		return err
	})
	if err != nil {
		return order, err
	}
	if clientID != "" && clientID != order.ClientID || exchangeID != "" && exchangeID != order.ExchangeID {
		return order, errors.New("execution: conflicting banexg order identity")
	}
	return order, nil
}

func (a *BanexgAdapter) Cancel(ctx context.Context, exchangeID string) (bool, error) {
	order, err := a.storedIdentity(ctx, "", exchangeID)
	if err != nil {
		return false, err
	}
	entry, ok := a.byID[order.Intent.Instrument.ID]
	if !ok {
		return false, errors.New("execution: unmapped banexg cancel instrument")
	}
	var raw *banexg.Order
	err = a.invoke(ctx, func() error {
		result, e := a.exchange.CancelOrder(exchangeID, entry.Symbol, a.params())
		raw = result
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if raw == nil || raw.ID != exchangeID {
		return false, errors.New("execution: banexg cancel identity is incomplete")
	}
	return raw.Status == banexg.OdStatusCanceled || raw.Status == banexg.OdStatusExpired, nil
}

func (a *BanexgAdapter) Query(ctx context.Context, clientID, exchangeID string) (QueryResult, error) {
	order, err := a.storedIdentity(ctx, clientID, exchangeID)
	if err != nil {
		return QueryResult{}, err
	}
	entry, ok := a.byID[order.Intent.Instrument.ID]
	if !ok {
		return QueryResult{}, errors.New("execution: unmapped banexg query instrument")
	}
	params := a.params()
	params[banexg.ParamClientOrderId] = clientID
	var raw *banexg.Order
	err = a.invoke(ctx, func() error {
		result, e := a.exchange.FetchOrder(entry.Symbol, exchangeID, params)
		raw = result
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		if a.proof.AuthoritativeNotFound && ctx.Err() == nil && a.config.Absence(err) {
			return QueryResult{Authoritative: true, Complete: true}, nil
		}
		return QueryResult{}, err
	}
	if raw == nil {
		return QueryResult{}, errors.New("execution: nil banexg query is inconclusive")
	}
	return a.orderSnapshot(raw, order)
}

func (a *BanexgAdapter) orderSnapshot(raw *banexg.Order, order StoredOrder) (QueryResult, error) {
	instrument := order.Intent.Instrument
	if raw.ID == "" || raw.ClientOrderID != order.ClientID || raw.Symbol != a.byID[instrument.ID].Symbol || raw.Side != string(order.Intent.Side) || order.ExchangeID != "" && raw.ID != order.ExchangeID {
		return QueryResult{}, errors.New("execution: banexg order snapshot identity mismatch")
	}
	amount, err := quantitySteps(raw.Amount, instrument)
	if err != nil || amount != order.Intent.Steps {
		return QueryResult{}, errors.New("execution: banexg order quantity mismatch")
	}
	filled, err := quantitySteps(raw.Filled, instrument)
	if err != nil || filled > amount {
		return QueryResult{}, errors.New("execution: banexg order fill quantity invalid")
	}
	cost, err := decimalBoundary(raw.Cost)
	if err != nil || cost.IsNegative() || filled > 0 && !cost.IsPositive() {
		return QueryResult{}, errors.New("execution: banexg cumulative cost incomplete")
	}
	fee := decimal.Zero
	if raw.Fee != nil {
		if raw.Fee.Currency != instrument.SettlementCurrency {
			return QueryResult{}, errors.New("execution: banexg fee lacks settlement currency normalization")
		}
		fee, err = decimalBoundary(raw.Fee.Cost)
		if err != nil {
			return QueryResult{}, err
		}
	} else if filled > 0 {
		return QueryResult{}, errors.New("execution: banexg cumulative fee incomplete")
	}
	switch raw.Status {
	case banexg.OdStatusOpen, banexg.OdStatusPartFilled, banexg.OdStatusFilled, banexg.OdStatusCanceled, banexg.OdStatusExpired, banexg.OdStatusRejected:
	default:
		return QueryResult{}, errors.New("execution: banexg order status incomplete")
	}
	if raw.Status == banexg.OdStatusFilled && filled != amount {
		return QueryResult{}, errors.New("execution: banexg terminal filled highwater incomplete")
	}
	price := order.Intent.Observation.Price
	if filled > 0 {
		price = cost.Div(instrument.Notional(filled, decimal.NewFromInt(1)))
	}
	if !price.IsPositive() {
		price = decimal.NewFromInt(1)
	} // zero-fill snapshot has no execution price
	atMS := raw.LastUpdateTimestamp
	if atMS == 0 {
		atMS = raw.Timestamp
	}
	if atMS < 0 {
		return QueryResult{}, errors.New("execution: invalid banexg update time")
	}
	// This external cumulative report identity predates framed internal IDs.
	// Preserve its dedup key across upgrades; its numeric tail has no '/' parts.
	eventID := "snapshot-" + legacyRebalanceID(raw.ID, "/", filled, "/", cost, "/", fee, "/", atMS)
	fill := FillReport{EventID: eventID, OrderID: order.Intent.ID, Steps: filled, Price: price, Cost: cost, Fee: fee, Cumulative: true, AuthoritativeSnapshot: true, AtMS: atMS}
	receipt := SubmitReceipt{ExchangeID: raw.ID, Rejected: raw.Status == banexg.OdStatusRejected}
	if filled > 0 {
		receipt.Fills = []FillReport{fill}
	} else if !fee.IsZero() || !cost.IsZero() {
		return QueryResult{}, errors.New("execution: banexg fee/cost without fills requires external reconciliation")
	}
	return QueryResult{Found: true, Authoritative: true, Complete: true, Canceled: raw.Status == banexg.OdStatusCanceled || raw.Status == banexg.OdStatusExpired, Receipt: receipt}, nil
}

type BanexgStreamReport struct {
	OrderID              string // recovery wake-up only; streamed partial fee reports never mutate books
	UnassignedExchangeID string
	Err                  error
}

// Reports translates private order/trade updates into recovery hints. The
// account owner queries authoritative cumulative snapshots before accounting.
// Unknown/manual identities are explicit unassigned events requiring a freeze.
func (a *BanexgAdapter) Reports(ctx context.Context) (<-chan BanexgStreamReport, error) {
	if !a.exchange.HasApi(banexg.ApiWatchMyTrades, "") {
		return nil, errors.New("execution: banexg private reports unsupported")
	}
	var input chan *banexg.MyTrade
	err := a.invoke(ctx, func() error {
		stream, e := a.exchange.WatchMyTrades(a.params())
		input = stream
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errors.New("execution: nil banexg private report stream")
	}
	output := make(chan BanexgStreamReport, 64)
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case trade, ok := <-input:
				if !ok {
					select {
					case output <- BanexgStreamReport{Err: errors.New("execution: banexg private stream closed; reconcile before admission")}:
					case <-ctx.Done():
					}
					return
				}
				report := BanexgStreamReport{}
				if trade == nil {
					report.Err = errors.New("execution: nil banexg private report")
				} else {
					stored, err := a.storedIdentity(ctx, trade.ClientID, trade.Order)
					if err != nil {
						report.UnassignedExchangeID = trade.Order
						report.Err = err
					} else {
						report.OrderID = stored.Intent.ID
					}
				}
				select {
				case output <- report:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output, nil
}

func (a *BanexgAdapter) fetchAccount(ctx context.Context) (*banexg.Balances, []*banexg.Position, []*banexg.Order, error) {
	var balances *banexg.Balances
	var positions []*banexg.Position
	var orders []*banexg.Order
	err := a.invoke(ctx, func() error {
		var e error
		b, be := a.exchange.FetchBalance(a.params())
		balances = b
		if be != nil {
			return be
		}
		p, pe := a.exchange.FetchPositions(nil, a.params())
		positions = p
		if pe != nil {
			return pe
		}
		params := a.params()
		params[banexg.ParamFullSnapshot] = true
		o, oe := a.exchange.FetchOpenOrders("", 0, 0, params)
		orders = o
		if oe != nil {
			e = oe
		}
		return e
	})
	return balances, positions, orders, err
}

func (a *BanexgAdapter) Snapshot(ctx context.Context) (VenueSnapshot, error) {
	balances, positions, orders, err := a.fetchAccount(ctx)
	if err != nil {
		return VenueSnapshot{}, err
	}
	if balances == nil {
		return VenueSnapshot{}, errors.New("execution: banexg balance snapshot absent")
	}
	cashValue, ok := balances.Total[a.config.Account.SettlementDomain]
	if !ok {
		return VenueSnapshot{}, errors.New("execution: banexg settled cash absent")
	}
	cash, err := decimalBoundary(cashValue)
	if err != nil {
		return VenueSnapshot{}, err
	}
	result := VenueSnapshot{Cash: cash.String(), Positions: make(map[string]int64)}
	for _, position := range positions {
		if position == nil {
			return VenueSnapshot{}, errors.New("execution: nil banexg position")
		}
		instrument, ok := a.bySymbol[position.Symbol]
		if !ok {
			return VenueSnapshot{}, errors.New("execution: unexplained banexg position instrument")
		}
		if position.Hedged {
			return VenueSnapshot{}, errors.New("execution: hedged position outside linear net contract")
		}
		steps, err := quantitySteps(position.Contracts, instrument)
		if err != nil {
			return VenueSnapshot{}, err
		}
		if position.Side == banexg.PosSideShort {
			steps = -steps
		} else if position.Side != banexg.PosSideLong && steps != 0 {
			return VenueSnapshot{}, errors.New("execution: unknown banexg position direction")
		}
		if _, ok := result.Positions[instrument.ID]; ok {
			return VenueSnapshot{}, errors.New("execution: duplicate banexg net position")
		}
		result.Positions[instrument.ID] = steps
	}
	for _, raw := range orders {
		if raw == nil || !canonicalID(raw.ID) {
			return VenueSnapshot{}, errors.New("execution: incomplete banexg open inventory")
		}
		stored, err := a.storedIdentity(ctx, raw.ClientOrderID, raw.ID)
		if errors.Is(err, sql.ErrNoRows) {
			result.OpenOrders = append(result.OpenOrders, QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: raw.ID}})
			continue
		}
		if err != nil {
			return VenueSnapshot{}, err
		}
		query, err := a.orderSnapshot(raw, stored)
		if err != nil {
			return VenueSnapshot{}, err
		}
		result.OpenOrders = append(result.OpenOrders, query)
	}
	return result, nil
}

// MigrationSnapshot exposes exact normalized actual basis and cumulative
// highwaters for the transactional importer. Unknown symbols and hedge modes
// fail closed; no legacy order is claimed by inspecting its symbol alone.
func (a *BanexgAdapter) MigrationSnapshot(ctx context.Context) (LegacyVenueSnapshot, error) {
	balances, positions, orders, err := a.fetchAccount(ctx)
	if err != nil {
		return LegacyVenueSnapshot{}, err
	}
	if balances == nil {
		return LegacyVenueSnapshot{}, errors.New("execution: migration balance absent")
	}
	value, ok := balances.Total[a.config.Account.SettlementDomain]
	if !ok {
		return LegacyVenueSnapshot{}, errors.New("execution: migration settled cash absent")
	}
	cash, err := decimalBoundary(value)
	if err != nil {
		return LegacyVenueSnapshot{}, err
	}
	result := LegacyVenueSnapshot{Complete: true, AccountCash: cash, AtMS: time.Now().UnixMilli()}
	seen := make(map[string]bool)
	for _, raw := range positions {
		if raw == nil {
			return LegacyVenueSnapshot{}, errors.New("execution: migration position absent")
		}
		instrument, ok := a.bySymbol[raw.Symbol]
		if !ok || raw.Hedged || seen[instrument.ID] {
			return LegacyVenueSnapshot{}, errors.New("execution: migration position identity/mode incomplete")
		}
		seen[instrument.ID] = true
		steps, err := quantitySteps(raw.Contracts, instrument)
		if err != nil {
			return LegacyVenueSnapshot{}, err
		}
		if steps == 0 {
			continue
		}
		contractSize, err := decimalBoundary(raw.ContractSize)
		if err != nil || !contractSize.Equal(instrument.ContractSize) {
			return LegacyVenueSnapshot{}, errors.New("execution: migration position contract units incomplete")
		}
		price, err := decimalBoundary(raw.EntryPrice)
		if err != nil || !price.IsPositive() {
			return LegacyVenueSnapshot{}, errors.New("execution: migration actual cost basis incomplete")
		}
		basis := instrument.Notional(steps, price)
		if raw.Side == banexg.PosSideShort {
			steps = -steps
		} else if raw.Side != banexg.PosSideLong {
			return LegacyVenueSnapshot{}, errors.New("execution: migration position direction incomplete")
		}
		result.Positions = append(result.Positions, VirtualLot{Instrument: instrument, SignedSteps: steps, CostBasis: basis})
	}
	for _, raw := range orders {
		if raw == nil || !canonicalID(raw.ID) || !canonicalID(raw.ClientOrderID) {
			return LegacyVenueSnapshot{}, errors.New("execution: migration open order identity incomplete")
		}
		instrument, ok := a.bySymbol[raw.Symbol]
		if !ok {
			return LegacyVenueSnapshot{}, errors.New("execution: migration unexplained order instrument")
		}
		steps, err := quantitySteps(raw.Amount, instrument)
		if err != nil {
			return LegacyVenueSnapshot{}, err
		}
		filled, err := quantitySteps(raw.Filled, instrument)
		if err != nil || filled >= steps {
			return LegacyVenueSnapshot{}, errors.New("execution: migration active order highwater invalid")
		}
		cost, err := decimalBoundary(raw.Cost)
		if err != nil || cost.IsNegative() || filled > 0 && !cost.IsPositive() {
			return LegacyVenueSnapshot{}, errors.New("execution: migration order cost incomplete")
		}
		fee := decimal.Zero
		if raw.Fee != nil {
			if raw.Fee.Currency != instrument.SettlementCurrency {
				return LegacyVenueSnapshot{}, errors.New("execution: migration fee currency incomplete")
			}
			fee, err = decimalBoundary(raw.Fee.Cost)
			if err != nil {
				return LegacyVenueSnapshot{}, err
			}
		} else if filled > 0 {
			return LegacyVenueSnapshot{}, errors.New("execution: migration cumulative fee absent")
		}
		if raw.Side != banexg.OdSideBuy && raw.Side != banexg.OdSideSell {
			return LegacyVenueSnapshot{}, errors.New("execution: migration order side incomplete")
		}
		if raw.Status != banexg.OdStatusOpen && raw.Status != banexg.OdStatusPartFilled {
			return LegacyVenueSnapshot{}, errors.New("execution: migration snapshot contains nonactive order")
		}
		result.OpenOrders = append(result.OpenOrders, LegacyVenueOrder{ExchangeID: raw.ID, ClientID: raw.ClientOrderID, Instrument: instrument.ID, Side: OrderSide(raw.Side), Steps: steps, FilledSteps: filled, Cost: cost, Fee: fee})
	}
	return result, nil
}

// Observe fetches a real bid/ask book. It never substitutes mark/last price or
// an archive/paper price. Callers serialize this read-only operation with the
// same owner network gate and stamp the decision after receipt.
func (a *BanexgAdapter) Observe(ctx context.Context, instrumentID string) (VisibleQuote, error) {
	entry, ok := a.byID[instrumentID]
	if !ok {
		return VisibleQuote{}, errors.New("execution: unmapped banexg quote instrument")
	}
	var book *banexg.OrderBook
	err := a.invoke(ctx, func() error {
		result, e := a.exchange.FetchOrderBook(entry.Symbol, 1, a.params())
		book = result
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return VisibleQuote{}, err
	}
	received := time.Now().UnixMilli()
	if book == nil || book.Symbol != entry.Symbol || book.Bids == nil || book.Asks == nil || len(book.Bids.Price) == 0 || len(book.Asks.Price) == 0 || book.TimeStamp <= 0 || book.TimeStamp > received || received-book.TimeStamp >= a.config.QuoteTTL.Milliseconds() {
		return VisibleQuote{}, errors.New("execution: incomplete banexg bid/ask quote")
	}
	bid, err := decimalBoundary(book.Bids.Price[0])
	if err != nil {
		return VisibleQuote{}, err
	}
	ask, err := decimalBoundary(book.Asks.Price[0])
	if err != nil {
		return VisibleQuote{}, err
	}
	if !bid.IsPositive() || ask.LessThan(bid) {
		return VisibleQuote{}, errors.New("execution: invalid banexg bid/ask spread")
	}
	return VisibleQuote{Bid: bid, Ask: ask, AtMS: book.TimeStamp, ReceivedMS: received, ValidUntilMS: book.TimeStamp + a.config.QuoteTTL.Milliseconds()}, nil
}
