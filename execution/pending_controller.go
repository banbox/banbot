package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/shopspring/decimal"
)

type PendingProtectionRequest struct {
	Order      *PendingOrder
	Instrument Instrument
	Specs      []PositionProtection
	Policy     OrderPolicy
}

type PendingProtectionTrigger struct {
	Strategy StrategyID
	Lot      VirtualLotID
	Kind     ProtectionKind
}

type PendingEntryRequest struct {
	Order      *PendingOrder
	Instrument Instrument
	Policy     OrderPolicy
}

func pendingLess(a, b *PendingOrder) bool {
	if a.Strategy != b.Strategy {
		return a.Strategy < b.Strategy
	}
	return a.Lot < b.Lot
}

func pendingLot(snapshot AccountSnapshot, order *PendingOrder, instrument string) VirtualLot {
	for _, lot := range snapshot.Lots {
		if lot.Strategy == order.Strategy && lot.ID == order.Lot && lot.Instrument.ID == instrument {
			return lot
		}
	}
	return VirtualLot{}
}

func pendingQuote(quotes map[string]VisibleQuote, instrument string, now int64) (VisibleQuote, decimal.Decimal, error) {
	q, ok := quotes[instrument]
	if !ok || !q.Bid.IsPositive() || q.Ask.LessThan(q.Bid) || q.AtMS <= 0 || q.AtMS > q.ReceivedMS || q.ReceivedMS > now || q.ValidUntilMS <= now {
		return q, decimal.Zero, fmt.Errorf("execution: pending order quote stale or unavailable: %s", instrument)
	}
	return q, q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2)), nil
}

func pendingEntryOrder(order StoredOrder, pending *PendingOrder, instrument string) bool {
	if order.Intent.Instrument.ID != instrument || order.State == OrderFilled || order.State == OrderCanceled || order.State == OrderRejected {
		return false
	}
	for _, allocation := range order.Intent.Allocations {
		if allocation.Kind == EntryIntent && allocation.Strategy == pending.Strategy && allocation.Lot == pending.Lot {
			return true
		}
	}
	return false
}

func (s *SharedAccount) cancelPendingOrder(ctx context.Context, id string, now int64) error {
	if err := s.executorFor(ctx).Cancel(id, now); err != nil {
		return err
	}
	final, err := s.store.Order(ctx, id)
	if err != nil {
		return err
	}
	if final.State != OrderCanceled && final.State != OrderFilled && final.State != OrderRejected {
		return fmt.Errorf("execution: pending reservation awaiting final cancellation: %s", id)
	}
	return nil
}

func (s *SharedAccount) cancelPendingReservations(ctx context.Context, snapshot AccountSnapshot, pending *PendingOrder, instrument string, kind IntentKind, now int64, limitsOnly bool) error {
	orders := append([]StoredOrder(nil), snapshot.Orders...)
	sort.Slice(orders, func(i, j int) bool { return orders[i].Intent.ID < orders[j].Intent.ID })
	for _, order := range orders {
		if order.Intent.Instrument.ID != instrument || limitsOnly && !order.Intent.Limit.IsPositive() || order.State == OrderFilled || order.State == OrderCanceled || order.State == OrderRejected {
			continue
		}
		for _, allocation := range order.Intent.Allocations {
			if allocation.Kind == kind && allocation.Strategy == pending.Strategy && allocation.Lot == pending.Lot {
				if err := s.cancelPendingOrder(ctx, order.Intent.ID, now); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

// cancelPendingEntries settles the final venue highwater before callers change
// virtual targets. A failed cancellation leaves the pending state retryable.
func (s *SharedAccount) cancelPendingEntries(ctx context.Context, snapshot AccountSnapshot, pending *PendingOrder, instrument string, now int64, limitsOnly bool) error {
	orders := append([]StoredOrder(nil), snapshot.Orders...)
	sort.Slice(orders, func(i, j int) bool { return orders[i].Intent.ID < orders[j].Intent.ID })
	for _, order := range orders {
		if pendingEntryOrder(order, pending, instrument) && (!limitsOnly || order.Intent.Limit.IsPositive()) {
			if err := s.cancelPendingOrder(ctx, order.Intent.ID, now); err != nil {
				return err
			}
		}
	}
	return nil
}

// AdvancePendingProtections runs inside WithState. The caller persists the
// pending checkpoints and translates trigger metadata into presentation tags.
func (s *SharedAccount) AdvancePendingProtections(ctx context.Context, requests []PendingProtectionRequest, quotes map[string]VisibleQuote, now int64) ([]PendingProtectionTrigger, error) {
	requests = append([]PendingProtectionRequest(nil), requests...)
	for _, request := range requests {
		if request.Order == nil {
			return nil, errors.New("execution: nil pending protection order")
		}
	}
	sort.SliceStable(requests, func(i, j int) bool { return pendingLess(requests[i].Order, requests[j].Order) })
	if err := s.prepareNativeProtections(ctx, requests, now); err != nil {
		return nil, err
	}
	var triggered []PendingProtectionTrigger
	for _, request := range requests {
		snapshot, err := s.store.Snapshot(ctx)
		if err != nil {
			return triggered, err
		}
		lot := pendingLot(snapshot, request.Order, request.Instrument.ID)
		if lot.SignedSteps == 0 {
			continue
		}
		q, price, err := pendingQuote(quotes, request.Instrument.ID, now)
		if err != nil {
			return triggered, err
		}
		kind, steps, err := request.Order.AdvanceProtection(s.AccountKey(), request.Instrument, request.Specs, lot.SignedSteps, lot.CostBasis, price, now, q.Bar)
		if err != nil {
			return triggered, err
		}
		if steps == 0 {
			continue
		}
		selected := request.Order.Protections[string(kind)]
		if err := s.cancelNativeForLot(ctx, request.Order, now); err != nil {
			return triggered, err
		}
		if err := s.cancelPendingEntries(ctx, snapshot, request.Order, request.Instrument.ID, now, false); err != nil {
			return triggered, err
		}
		final, err := s.store.Snapshot(ctx)
		if err != nil {
			return triggered, err
		}
		finalLot := pendingLot(final, request.Order, request.Instrument.ID)
		// Final native fills may refresh the book; retain this round's local latch.
		request.Order.Protections[string(kind)] = selected
		if err := request.Order.CommitProtection(kind, steps, absSteps(lot.SignedSteps), finalLot.SignedSteps); err != nil {
			return triggered, err
		}
		triggered = append(triggered, PendingProtectionTrigger{Strategy: request.Order.Strategy, Lot: request.Order.Lot, Kind: kind})
	}
	return triggered, nil
}

// AdvancePendingEntries accounts for every pending entry/exit and the carried
// factor targets, not just the orders being evaluated at this quote. Callers
// supply the complete pending set and invoke this inside WithState.
func (s *SharedAccount) AdvancePendingEntries(ctx context.Context, requests []PendingEntryRequest, quotes map[string]VisibleQuote, now int64) error {
	requests = append([]PendingEntryRequest(nil), requests...)
	var intents []EligibleIntent
	for _, request := range requests {
		if request.Order == nil {
			return errors.New("execution: nil pending entry order")
		}
		intents = append(intents, request.Order.Entry)
		if request.Order.Exit != nil {
			intents = append(intents, *request.Order.Exit)
		}
	}
	sort.SliceStable(requests, func(i, j int) bool { return pendingLess(requests[i].Order, requests[j].Order) })
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	plan, err := s.store.LatestPlan(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// Cancellation is a preflight: no pending target advances until every
	// affected venue reservation has an authoritative terminal snapshot.
	type exitUpdate struct {
		request PendingEntryRequest
		expired bool
	}
	var exits []exitUpdate
	for _, request := range requests {
		exclusive := ExclusiveOrderLot(snapshot, request.Instrument.ID, plan.Targets, intents)
		q, price, err := pendingQuote(quotes, request.Instrument.ID, now)
		if err != nil {
			return err
		}
		if !request.Order.Canceled && request.Order.Entry.State != Filled {
			preview := *request.Order
			lot := pendingLot(snapshot, request.Order, request.Instrument.ID)
			if err := preview.AdvanceEntry(price, now, q.Bar, lot.SignedSteps, request.Policy, exclusive); err != nil {
				return err
			}
			if preview.Entry.State != Filled {
				strict := preview.Entry
				available, err := strict.Evaluate(price, now, q.Bar)
				if err != nil {
					return err
				}
				delegated, err := preview.Entry.AvailableForSubmission(price, now, q.Bar)
				if err != nil {
					return err
				}
				// Ordinary executable limits remain live. Only a nonmarketable
				// reservation loses its sole-contributor delegation.
				if available == 0 && (!exclusive || delegated == 0) {
					if err := s.cancelPendingReservations(ctx, snapshot, request.Order, request.Instrument.ID, EntryIntent, now, preview.Entry.State != Expired); err != nil {
						return err
					}
				}
			}
		}
		if request.Order.Exit != nil && request.Order.Exit.State != Filled && request.Order.Exit.State != Canceled && request.Order.Exit.State != Expired {
			// Imported venue exits retain their original placement contract. Only
			// limits delegated by this controller participate in retreat policy.
			if len(request.Order.NativeExitOrders) > 0 && request.Order.Exit.VenueLimit == nil {
				preview := *request.Order.Exit
				if _, err := preview.Evaluate(price, now, q.Bar); err != nil {
					return err
				}
				if preview.State != Expired {
					continue
				}
			}
			if pendingLot(snapshot, request.Order, request.Instrument.ID).SignedSteps == request.Order.Desired {
				continue
			}
			preview := *request.Order.Exit
			if err := request.Policy.Admit(&preview, price, now, q.Bar, exclusive); err != nil {
				return err
			}
			strict := preview
			available, err := strict.Evaluate(price, now, q.Bar)
			if err != nil {
				return err
			}
			delegated, err := preview.AvailableForSubmission(price, now, q.Bar)
			if err != nil {
				return err
			}
			if available == 0 && (!exclusive || delegated == 0) {
				if err := s.cancelPendingReservations(ctx, snapshot, request.Order, request.Instrument.ID, ExitIntent, now, preview.State != Expired); err != nil {
					return err
				}
				exits = append(exits, exitUpdate{request: request, expired: preview.State == Expired})
			}
		}
	}
	snapshot, err = s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, update := range exits {
		order := update.request.Order
		held := pendingLot(snapshot, order, update.request.Instrument.ID).SignedSteps
		order.Exit.VenueLimit = nil
		order.NativeExitOrders = nil
		// Desired is the original absolute goal, not a fresh reduction.
		remaining := absSteps(held) - absSteps(order.Desired)
		remaining = max(int64(0), min(remaining, order.Exit.QuantitySteps))
		order.Exit.FilledSteps = order.Exit.QuantitySteps - remaining
		order.Exit.ReservedSteps = 0
		if update.expired {
			order.Exit.State = Expired
			order.Desired = held
		}
	}
	for _, request := range requests {
		if request.Order.Canceled || request.Order.Entry.State == Filled {
			continue
		}
		q, price, err := pendingQuote(quotes, request.Instrument.ID, now)
		if err != nil {
			return err
		}
		lot := pendingLot(snapshot, request.Order, request.Instrument.ID)
		exclusive := ExclusiveOrderLot(snapshot, request.Instrument.ID, plan.Targets, intents)
		if err := request.Order.AdvanceEntry(price, now, q.Bar, lot.SignedSteps, request.Policy, exclusive); err != nil {
			return err
		}
	}
	return nil
}

func (b *SharedAccountBorrow) AdvancePendingProtections(ctx context.Context, requests []PendingProtectionRequest, quotes map[string]VisibleQuote, now int64) ([]PendingProtectionTrigger, error) {
	var triggered []PendingProtectionTrigger
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, ctx)
		defer done()
		var err error
		triggered, err = s.AdvancePendingProtections(ctx, requests, quotes, now)
		return err
	})
	return triggered, err
}

func (b *SharedAccountBorrow) AdvancePendingEntries(ctx context.Context, requests []PendingEntryRequest, quotes map[string]VisibleQuote, now int64) error {
	return b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, ctx)
		defer done()
		return s.AdvancePendingEntries(ctx, requests, quotes, now)
	})
}
