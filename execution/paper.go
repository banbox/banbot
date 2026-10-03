package execution

import (
	"context"
	"errors"
	"sort"

	"github.com/shopspring/decimal"
	"strings"
	"sync"
)

type paperPosition struct {
	steps      int64
	average    decimal.Decimal
	instrument Instrument
}
type paperTotals struct {
	slippage, turnover decimal.Decimal
	fills              int
}

// PaperAdapter is a deterministic local venue simulator. Full market fills
// include directional slippage, tick rounding and fees. It has no network I/O;
// optional indexed history can spill settled receipts through its bound store.
// Ordinary limits reject when the modeled price exceeds their bound. Post-only
// orders rest at their limit until a later visible bid/ask touches it. The model
// assumes sufficient liquidity for a full fill; maker fees use the configured
// fee rate and no taker slippage is charged. Recovery is process-local only.
type paperOrder struct {
	intent OrderIntent
	client string
	result QueryResult
}

type PaperAdapter struct {
	mu              sync.Mutex
	cash, fee, slip decimal.Decimal
	positions       map[string]paperPosition
	fills           int
	lastFill        FillReport
	lastSourceAt    int64
	totals          map[StrategyID]paperTotals
	orders          map[string]*paperOrder
	pending         map[string]*paperOrder // quote scans depend on resting orders, not fill history
	quotes          map[string]VisibleQuote
	clients         map[string]string
	history         *Store
}
type PaperMetrics struct {
	Fills        int
	LastFill     FillReport
	LastSourceAt int64
}

func (a *PaperAdapter) Metrics() PaperMetrics {
	a.mu.Lock()
	defer a.mu.Unlock()
	return PaperMetrics{a.fills, a.lastFill, a.lastSourceAt}
}
func (a *PaperAdapter) StrategyCosts(id StrategyID) (float64, float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t := a.totals[id]
	return t.slippage.InexactFloat64(), t.turnover.InexactFloat64()
}

// StrategyFillCount counts a venue fill once for each strategy with a nonzero
// frozen allocation, even when that strategy contributes several lots.
func (a *PaperAdapter) StrategyFillCount(id StrategyID) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.totals[id].fills
}

func NewPaperAdapter(cash, fee, slip decimal.Decimal) (*PaperAdapter, error) {
	if !cash.IsPositive() || fee.IsNegative() || slip.IsNegative() || slip.GreaterThanOrEqual(decimal.NewFromInt(1)) {
		return nil, errors.New("execution: invalid paper cash/costs")
	}
	return &PaperAdapter{cash: cash, fee: fee, slip: slip, positions: map[string]paperPosition{}, totals: map[StrategyID]paperTotals{}, orders: map[string]*paperOrder{}, pending: map[string]*paperOrder{}, quotes: map[string]VisibleQuote{}, clients: map[string]string{}}, nil
}
func (a *PaperAdapter) Capabilities() AdapterCapabilities {
	return AdapterCapabilities{StableTradeID: true, PostOnly: true, QueryClientID: true, AuthoritativeNotFound: true}
}
func (a *PaperAdapter) Submit(ctx context.Context, o OrderIntent, client string) (SubmitReceipt, error) {
	if err := ctx.Err(); err != nil {
		return SubmitReceipt{}, err
	}
	if err := o.Instrument.Validate(); err != nil {
		return SubmitReceipt{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.archiveSettled(ctx); err != nil {
		return SubmitReceipt{}, err
	}
	if !canonicalID(client) || !canonicalID(o.ID) || o.Steps <= 0 || o.Side != Buy && o.Side != Sell || o.Limit.IsNegative() || !o.Observation.Price.IsPositive() {
		return SubmitReceipt{}, errors.New("execution: invalid paper order")
	}
	if o.SubmitAtMS < o.Observation.AtMS {
		return SubmitReceipt{}, errors.New("execution: paper submission clock precedes observable quote")
	}
	old := a.orders[a.clients[client]]
	if old == nil {
		var err error
		old, err = a.archivedOrder(ctx, "", client)
		if err != nil {
			return SubmitReceipt{}, err
		}
	}
	if old != nil {
		expected, _ := payload(old.intent)
		incoming, _ := payload(o)
		if incoming != expected {
			return SubmitReceipt{}, errors.New("execution: paper client identity reused")
		}
		return old.result.Receipt, nil
	}
	if _, exists := a.orders[o.ID]; exists {
		return SubmitReceipt{}, errors.New("execution: paper order identity reused")
	}
	if old, err := a.archivedOrder(ctx, o.ID, ""); err != nil {
		return SubmitReceipt{}, err
	} else if old != nil {
		return SubmitReceipt{}, errors.New("execution: paper order identity reused")
	}
	if o.PostOnly {
		q := o.Observation
		if latest, ok := a.quotes[o.Instrument.ID]; ok && latest.AtMS >= q.AtMS && latest.ReceivedMS <= o.SubmitAtMS {
			q.Bid, q.Ask, q.AtMS, q.ValidUntilMS = latest.Bid, latest.Ask, latest.AtMS, latest.ValidUntilMS
		}
		if !o.Limit.IsPositive() || !q.Bid.IsPositive() || q.Ask.LessThan(q.Bid) || q.AtMS > o.SubmitAtMS || q.ValidUntilMS <= o.SubmitAtMS {
			return SubmitReceipt{}, errors.New("execution: passive paper order requires a visible bid/ask and positive limit")
		}
		_, remainder := o.Limit.QuoRem(o.Instrument.PriceTick, 0)
		if !remainder.IsZero() {
			return SubmitReceipt{}, errors.New("execution: passive paper limit violates tick")
		}
		rejected := o.Side == Buy && o.Limit.GreaterThanOrEqual(q.Ask) || o.Side == Sell && o.Limit.LessThanOrEqual(q.Bid)
		result := QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "paper:" + o.ID, Rejected: rejected}}
		o.Allocations = append([]FillAllocation(nil), o.Allocations...)
		a.orders[o.ID] = &paperOrder{intent: o, client: client, result: result}
		if !rejected {
			a.pending[o.ID] = a.orders[o.ID]
		}
		a.clients[client] = o.ID
		return result.Receipt, nil
	}
	sign := int64(1)
	mult := decimal.NewFromInt(1).Add(a.slip)
	if o.Side == Sell {
		sign = -1
		mult = decimal.NewFromInt(1).Sub(a.slip)
	}
	price := o.Observation.Price.Mul(mult)
	ticks := price.Div(o.Instrument.PriceTick)
	if sign > 0 {
		ticks = ticks.Ceil()
	} else {
		ticks = ticks.Floor()
	}
	price = ticks.Mul(o.Instrument.PriceTick)
	if o.Limit.IsPositive() && (sign > 0 && price.GreaterThan(o.Limit) || sign < 0 && price.LessThan(o.Limit)) {
		return SubmitReceipt{Rejected: true}, nil
	}
	receipt, err := a.fillLocked(o, price, o.SubmitAtMS, o.Observation.AtMS, false)
	if err != nil {
		return receipt, err
	}
	a.orders[o.ID] = &paperOrder{intent: o, client: client, result: QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: receipt}}
	a.clients[client] = o.ID
	return receipt, nil
}

func (a *PaperAdapter) fillLocked(o OrderIntent, price decimal.Decimal, nowMS, sourceMS int64, maker bool) (SubmitReceipt, error) {
	sign := int64(1)
	if o.Side == Sell {
		sign = -1
	}
	p := a.positions[o.Instrument.ID]
	if o.ReduceOnly && (p.steps == 0 || (p.steps > 0) == (sign > 0) || o.Steps > absSteps(p.steps)) {
		return SubmitReceipt{ExchangeID: "paper:" + o.ID, Rejected: true}, nil
	}
	delta := sign * o.Steps
	cost := o.Instrument.Notional(o.Steps, price)
	fee := cost.Mul(a.fee).Round(o.Instrument.MoneyScale)
	realized := decimal.Zero
	if p.steps == 0 || (p.steps > 0) == (delta > 0) {
		old := o.Instrument.Notional(absSteps(p.steps), p.average)
		p.average = old.Add(cost).Div(o.Instrument.Notional(absSteps(p.steps)+o.Steps, decimal.NewFromInt(1)))
	} else {
		closed := min(absSteps(p.steps), o.Steps)
		realized = o.Instrument.Notional(closed, price.Sub(p.average))
		if p.steps < 0 {
			realized = realized.Neg()
		}
		if o.Steps > absSteps(p.steps) {
			p.average = price
		}
	}
	p.steps += delta
	p.instrument = o.Instrument
	a.positions[o.Instrument.ID] = p
	a.cash = a.cash.Add(realized).Sub(fee)
	allocated := make(map[StrategyID]bool)
	for _, allocation := range o.Allocations {
		t := a.totals[allocation.Strategy]
		if !maker {
			t.slippage = t.slippage.Add(o.Instrument.Notional(allocation.Steps, price.Sub(o.Observation.Price).Abs()))
		}
		t.turnover = t.turnover.Add(o.Instrument.Notional(allocation.Steps, o.Observation.Price))
		a.totals[allocation.Strategy] = t
		if allocation.Steps > 0 {
			allocated[allocation.Strategy] = true
		}
	}
	for strategy := range allocated {
		t := a.totals[strategy]
		t.fills++
		a.totals[strategy] = t
	}
	a.fills++
	a.lastSourceAt = sourceMS
	a.lastFill = FillReport{EventID: "paper-fill:" + o.ID, OrderID: o.ID, Steps: o.Steps, Fee: fee, Price: price, Cost: cost, AtMS: nowMS}
	return SubmitReceipt{ExchangeID: "paper:" + o.ID, Fills: []FillReport{a.lastFill}}, nil
}

// AdvanceQuote consumes only a subsequent visible top-of-book observation.
// Returned order IDs must be recovered by their account owner to persist fills.
func (a *PaperAdapter) AdvanceQuote(ctx context.Context, instrument string, q VisibleQuote, nowMS int64) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !q.Bid.IsPositive() || q.Ask.LessThan(q.Bid) || q.AtMS < 0 || q.AtMS > q.ReceivedMS || q.ReceivedMS > nowMS || q.ValidUntilMS <= nowMS {
		return nil, errors.New("execution: passive paper quote is not visible/valid")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if previous, ok := a.quotes[instrument]; ok && (previous.AtMS > q.AtMS || previous.ReceivedMS > q.ReceivedMS) {
		return nil, nil
	}
	a.quotes[instrument] = q
	var ids []string
	for id, order := range a.pending {
		o := order.intent
		if o.Instrument.ID != instrument || !o.PostOnly || order.result.Canceled || order.result.Receipt.Rejected || len(order.result.Receipt.Fills) > 0 {
			continue
		}
		if q.AtMS <= o.Observation.AtMS || q.ReceivedMS <= o.SubmitAtMS {
			continue
		}
		if o.Side == Buy && q.Ask.LessThanOrEqual(o.Limit) || o.Side == Sell && q.Bid.GreaterThanOrEqual(o.Limit) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		order := a.orders[id]
		receipt, err := a.fillLocked(order.intent, order.intent.Limit, nowMS, q.AtMS, true)
		if err != nil {
			return nil, err
		}
		order.result.Receipt = receipt
		delete(a.pending, id)
	}
	return ids, nil
}
func (a *PaperAdapter) Cancel(ctx context.Context, exchangeID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	order, ok := a.orders[strings.TrimPrefix(exchangeID, "paper:")]
	if !ok {
		var err error
		order, err = a.archivedOrder(ctx, strings.TrimPrefix(exchangeID, "paper:"), "")
		if err != nil {
			return false, err
		}
		if order != nil {
			return order.result.Receipt.ExchangeID == exchangeID, nil
		}
	}
	if !ok {
		return false, errors.New("execution: paper cancel order absent")
	}
	if len(order.result.Receipt.Fills) == 0 && !order.result.Receipt.Rejected {
		order.result.Canceled = true
		delete(a.pending, order.intent.ID)
	}
	return true, nil
}
func (a *PaperAdapter) Query(ctx context.Context, client, exchangeID string) (QueryResult, error) {
	if err := ctx.Err(); err != nil {
		return QueryResult{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	id := a.clients[client]
	if id == "" {
		id = strings.TrimPrefix(exchangeID, "paper:")
	}
	order, ok := a.orders[id]
	if !ok {
		var err error
		order, err = a.archivedOrder(ctx, id, client)
		if err != nil {
			return QueryResult{}, err
		}
		ok = order != nil
	}
	if !ok {
		return QueryResult{Authoritative: true, Complete: true}, nil
	}
	if client != "" && order.client != client || exchangeID != "" && order.result.Receipt.ExchangeID != exchangeID {
		return QueryResult{}, errors.New("execution: paper query identity mismatch")
	}
	result := order.result
	result.Receipt.Fills = append([]FillReport(nil), result.Receipt.Fills...)
	return result, nil
}
func (a *PaperAdapter) Snapshot(ctx context.Context) (VenueSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return VenueSnapshot{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	positions := map[string]int64{}
	for id, p := range a.positions {
		positions[id] = p.steps
	}
	var open []QueryResult
	for _, order := range a.pending {
		if !order.result.Canceled && !order.result.Receipt.Rejected && len(order.result.Receipt.Fills) == 0 {
			open = append(open, order.result)
		}
	}
	sort.Slice(open, func(i, j int) bool { return open[i].Receipt.ExchangeID < open[j].Receipt.ExchangeID })
	return VenueSnapshot{Cash: a.cash.String(), Positions: positions, OpenOrders: open}, nil
}
func (a *PaperAdapter) ApplyCash(amount decimal.Decimal) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cash = a.cash.Add(amount)
}
