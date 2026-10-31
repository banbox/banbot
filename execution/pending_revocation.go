package execution

import (
	"context"
	"errors"
)

// revokeConflictingReservations is shared by factor and pending revisions.
// A new contributor cannot inherit a sole lot's permission to rest a limit.
// Returned targets replace only canceled inherited reservations, not incoming
// explicit strategy targets; the pending book retains its local conditions.
func (s *SharedAccount) revokeConflictingReservations(ctx context.Context, updates []StrategyRebalance) ([]ExecutableTarget, *StrategyCheckpoint, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	var incoming []ExecutableTarget
	for _, update := range updates {
		for _, request := range update.Requests {
			if err := request.Instrument.Validate(); err != nil {
				return nil, nil, err
			}
			for _, intent := range request.IntentConstraints {
				if err := intent.Validate(); err != nil {
					return nil, nil, err
				}
			}
			for _, target := range request.Targets {
				if target.Strategy != update.Strategy || !canonicalID(string(target.Lot)) {
					return nil, nil, errors.New("execution: invalid incoming reservation contributor")
				}
				target.Instrument = request.Instrument.ID
				incoming = append(incoming, target)
			}
		}
	}
	definitions, err := s.store.LoadPendingBook(ctx)
	if err != nil {
		return nil, nil, err
	}
	var intents []EligibleIntent
	for _, d := range definitions {
		intents = append(intents, d.Order.Entry)
		if d.Order.Exit != nil {
			intents = append(intents, *d.Order.Exit)
		}
	}
	affected := make(map[VirtualIntentID]bool)
	for _, order := range snapshot.Orders {
		if !order.Intent.Limit.IsPositive() || ExclusiveOrderLot(snapshot, order.Intent.Instrument.ID, incoming, intents) {
			continue
		}
		delegated := false
		for _, allocation := range order.Intent.Allocations {
			intent, err := s.store.Intent(ctx, allocation.IntentID)
			if err != nil {
				return nil, nil, err
			}
			delegated = delegated || intent.VenueLimit != nil
		}
		if !delegated {
			continue
		}
		if err := s.executorFor(ctx).Cancel(order.Intent.ID, updates[0].DecisionMS); err != nil {
			return nil, nil, err
		}
		final, err := s.store.Order(ctx, order.Intent.ID)
		if err != nil {
			return nil, nil, err
		}
		if final.State != OrderCanceled && final.State != OrderFilled && final.State != OrderRejected {
			return nil, nil, errors.New("execution: reservation revocation awaiting final cancellation")
		}
		for _, a := range order.Intent.Allocations {
			// Map plan-scoped allocations back to persistent economic lots.
			for _, d := range definitions {
				if d.Order.Strategy == a.Strategy && d.Order.Lot == a.Lot {
					affected[d.Order.Entry.ID] = true
				}
			}
		}
	}
	if len(affected) == 0 {
		return nil, nil, nil
	}
	snapshot, err = s.store.Snapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	var overrides []ExecutableTarget
	for n := range definitions {
		d := &definitions[n]
		if !affected[d.Order.Entry.ID] {
			continue
		}
		held := pendingLot(snapshot, &d.Order, d.Instrument.ID).SignedSteps
		d.Order.Entry.VenueLimit = nil
		if d.Order.Exit != nil {
			d.Order.Exit.VenueLimit = nil
		}
		if !d.Order.Canceled {
			d.Order.Desired = held
		}
		overrides = append(overrides, ExecutableTarget{Instrument: d.Instrument.ID, Strategy: d.Order.Strategy, Lot: d.Order.Lot, SignedSteps: held})
	}
	checkpoint, err := PendingBookCheckpoint(definitions)
	return overrides, &checkpoint, err
}
