package execution

import (
	"context"
	"database/sql"
	"errors"
	"github.com/shopspring/decimal"
	"sort"
)

// CoordinatePending advances execution-owned state and returns strategy-scoped
// absolute targets. It runs inside the account serialization boundary; all
// cancellation final fills precede construction of replacement targets.
func (s *SharedAccount) CoordinatePending(ctx context.Context, definitions []PendingDefinition, quotes map[string]VisibleQuote, now int64) ([]InstrumentRebalance, error) {
	if err := s.expirePendingFeeds(ctx, definitions); err != nil {
		return nil, err
	}
	protections := make([]PendingProtectionRequest, 0, len(definitions))
	for n := range definitions {
		d := &definitions[n]
		protections = append(protections, PendingProtectionRequest{Order: &d.Order, Instrument: d.Instrument, Specs: d.Specs, Policy: d.Policy})
	}
	if err := s.prepareNativeProtections(ctx, protections, now); err != nil {
		return nil, err
	}
	requests := make(map[string]*InstrumentRebalance)
	var pending []PendingEntryRequest
	for n := range definitions {
		d := &definitions[n]
		requests[d.Instrument.ID] = &InstrumentRebalance{Instrument: d.Instrument, Quote: quotes[d.Instrument.ID]}
		pending = append(pending, PendingEntryRequest{Order: &d.Order, Instrument: d.Instrument, Policy: d.Policy})
	}
	if err := s.AdvancePendingEntries(ctx, pending, quotes, now); err != nil {
		return nil, err
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	latest, err := s.store.LatestPlan(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	for n := range definitions {
		definition := &definitions[n]
		od := &definition.Order
		instrument := definition.Instrument
		q := requests[instrument.ID].Quote
		var filled int64
		for _, lot := range snapshot.Lots {
			if lot.Strategy == od.Strategy && lot.ID == od.Lot {
				filled = lot.SignedSteps
			}
		}
		if od.Entry.State == Expired && !od.Canceled {
			if err := s.cancelPendingEntries(ctx, snapshot, od, instrument.ID, now, false); err != nil {
				return nil, err
			}
			var err error
			snapshot, err = s.store.Snapshot(ctx)
			if err != nil {
				return nil, err
			}
			filled = 0
			for _, lot := range snapshot.Lots {
				if lot.Strategy == od.Strategy && lot.ID == od.Lot {
					filled = lot.SignedSteps
				}
			}
			od.Entry.FilledSteps = filled
			if filled < 0 {
				od.Entry.FilledSteps = -filled
			}
			od.Canceled, od.Desired = true, filled
		}
		if od.ResumeEntrySteps > 0 && filled == od.Desired {
			remaining := filled
			if remaining < 0 {
				remaining = -remaining
			}
			od.Entry.QuantitySteps = remaining + od.ResumeEntrySteps
			od.Entry.FilledSteps = remaining
			od.Entry.State = PendingCondition
			od.Canceled = false
			od.ResumeEntrySteps = 0
			// The next observed round reevaluates the original pending conditions.
		}
		desired := od.Desired
		confirmedExit := false
		for _, order := range snapshot.Orders {
			if order.Intent.Conditional != nil || order.State != OrderAcknowledged && order.State != OrderPartial {
				continue
			}
			for _, allocation := range order.Intent.Allocations {
				if allocation.Kind == ExitIntent && allocation.Strategy == od.Strategy && allocation.Lot == od.Lot && allocation.Steps > order.AllocationFilled[allocation.ID] {
					confirmedExit = true
				}
			}
		}
		if len(od.NativeExitOrders) > 0 {
			canceledNative := false
			var reportedExit int64
			for _, id := range od.NativeExitOrders {
				order, err := s.store.Order(context.Background(), id)
				if err != nil {
					return nil, err
				}
				canceledNative = canceledNative || order.State == OrderCanceled || order.State == OrderRejected
				for _, allocation := range order.Intent.Allocations {
					if allocation.Kind == ExitIntent && allocation.Strategy == od.Strategy && allocation.Lot == od.Lot {
						reportedExit += order.AllocationFilled[allocation.ID]
					}
				}
			}
			od.Exit.FilledSteps = reportedExit
			if canceledNative && !confirmedExit {
				od.Desired, desired, od.Exit.State = filled, filled, Canceled
			}
			if !confirmedExit {
				od.NativeExitOrders = nil
			}
		}
		if od.Exit != nil && desired == filled && od.Exit.State != Canceled && od.Exit.State != Expired {
			od.Exit.State = Filled
		}
		exitReduction := filled > 0 && desired >= 0 && desired < filled || filled < 0 && desired <= 0 && desired > filled
		if od.Exit != nil && exitReduction && !confirmedExit {
			price := q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2))
			var intents []EligibleIntent
			for _, other := range definitions {
				intents = append(intents, other.Order.Entry)
				if other.Order.Exit != nil {
					intents = append(intents, *other.Order.Exit)
				}
			}
			exclusive := ExclusiveOrderLot(snapshot, instrument.ID, latest.Targets, intents)
			if err := definition.Policy.Admit(od.Exit, price, now, q.Bar, exclusive); err != nil {
				return nil, err
			}
			available, err := od.Exit.AvailableForSubmission(price, now, q.Bar)
			if err != nil {
				return nil, err
			}
			if available == 0 {
				desired = filled
			}
		}
		requests[instrument.ID].Targets = append(requests[instrument.ID].Targets, ExecutableTarget{Strategy: od.Strategy, Lot: od.Lot, SignedSteps: desired})
		filledAbs, desiredAbs := filled, desired
		if filledAbs < 0 {
			filledAbs = -filledAbs
		}
		if desiredAbs < 0 {
			desiredAbs = -desiredAbs
		}
		if desiredAbs > filledAbs {
			requests[instrument.ID].IntentConstraints = append(requests[instrument.ID].IntentConstraints, od.Entry)
		}
		if desiredAbs < filledAbs && od.Exit != nil {
			requests[instrument.ID].IntentConstraints = append(requests[instrument.ID].IntentConstraints, *od.Exit)
		}

	}
	if err := s.addNativeReservations(ctx, definitions, requests, snapshot, latest); err != nil {
		return nil, err
	}
	var list []InstrumentRebalance
	for _, request := range requests {
		list = append(list, *request)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Instrument.ID < list[j].Instrument.ID })
	return list, nil
}
