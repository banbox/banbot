package execution

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ProtectionPlacement exposes a requested policy separately from its effective
// placement. An uncertain venue attempt is never reclassified as local fallback.
type ProtectionPlacement struct {
	Requested ProtectionMode
	Effective string
	Reason    string
	OrderID   string
}

func pendingIdentity(strategy StrategyID, lot VirtualLotID) string {
	return rebalanceID(strategy, lot)
}

func liveConditional(order StoredOrder) bool {
	return order.Intent.Conditional != nil && !terminalOrder(string(order.State))
}

func copyNativeFill(target *PendingOrder, source PendingOrder) {
	if source.LastNativeFill == "" || source.LastNativeFill == target.LastNativeFill {
		return
	}
	explicitExit := target.Exit != nil && target.Exit.State != Filled && target.Exit.State != Canceled && target.Exit.State != Expired
	desired, protection := target.Desired, target.LastProtection
	target.Desired = source.Desired
	if explicitExit {
		// A final native fill reduces available inventory, not the caller's exit.
		if source.Desired > 0 {
			target.Desired = max(int64(0), min(desired, source.Desired))
		} else if source.Desired < 0 {
			target.Desired = min(int64(0), max(desired, source.Desired))
		}
	}
	target.Entry.State, target.Canceled = Canceled, true
	target.LastNativeFill, target.NativeFillPlanID = source.LastNativeFill, source.NativeFillPlanID
	target.LastNativeProtection = source.LastNativeProtection
	target.LastProtection = source.LastProtection
	if explicitExit {
		target.LastProtection = protection
	}
	target.NativeProtectionID, target.NativeProtectionKind = source.NativeProtectionID, source.NativeProtectionKind
	if target.Protections == nil {
		target.Protections = map[string]EligibleIntent{}
	}
	if target.ProtectionDone == nil {
		target.ProtectionDone = map[string]bool{}
	}
	kind := source.LastNativeProtection
	if kind == "" { // Older checkpoints used LastProtection for both roles.
		kind = source.LastProtection
	}
	key := string(kind)
	target.Protections[key] = source.Protections[key]
	target.ProtectionDone[key] = source.ProtectionDone[key]

}

func (s *SharedAccount) syncNativeFills(ctx context.Context, requests []PendingProtectionRequest) error {
	book, err := s.store.LoadPendingBook(ctx)
	if err != nil {
		return err
	}
	byLot := make(map[string]PendingOrder, len(book))
	for _, d := range book {
		byLot[pendingIdentity(d.Order.Strategy, d.Order.Lot)] = d.Order
	}
	for _, request := range requests {
		if request.Order != nil {
			copyNativeFill(request.Order, byLot[pendingIdentity(request.Order.Strategy, request.Order.Lot)])
		}
	}
	return nil
}

// prepareNativeProtections refreshes authoritative native fills before local
// siblings evaluate. A lost acknowledgement or inconclusive cancellation stops
// this round; it is not a capability fallback.
func (s *SharedAccount) prepareNativeProtections(ctx context.Context, requests []PendingProtectionRequest, now int64) error {
	if err := s.syncNativeFills(ctx, requests); err != nil {
		return err
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	byLot := make(map[string]PendingProtectionRequest, len(requests))
	for _, request := range requests {
		if request.Order == nil {
			return errors.New("execution: nil pending protection order")
		}
		request.Order.NativeProtectionID, request.Order.NativeProtectionKind = "", ""
		byLot[pendingIdentity(request.Order.Strategy, request.Order.Lot)] = request
	}
	sort.Slice(snapshot.Orders, func(i, j int) bool { return snapshot.Orders[i].Intent.ID < snapshot.Orders[j].Intent.ID })
	for _, order := range snapshot.Orders {
		if !liveConditional(order) || len(order.Intent.Allocations) != 1 {
			continue
		}
		allocation := order.Intent.Allocations[0]
		request, exists := byLot[pendingIdentity(allocation.Strategy, allocation.Lot)]
		if !exists {
			continue
		}
		if order.State != OrderPrepared {
			if err := s.executorFor(ctx).Recover(order.Intent.ID); err != nil {
				return err
			}
		}
		if err := s.syncNativeFills(ctx, requests); err != nil {
			return err
		}
		current, err := s.store.Order(ctx, order.Intent.ID)
		if err != nil {
			return err
		}
		if !liveConditional(current) {
			continue
		}
		definition := PendingDefinition{Order: *request.Order, Instrument: request.Instrument, Specs: request.Specs, Policy: request.Policy}
		hash, hashErr := ConditionalSourceHash(definition, current.Intent.Conditional.Kind)
		activeExit := request.Order.Exit != nil && request.Order.Exit.State != Filled && request.Order.Exit.State != Canceled && request.Order.Exit.State != Expired
		if current.State == OrderPrepared || !request.Policy.prefersNativeProtection() || activeExit || hashErr != nil || hash != current.Intent.Conditional.SourceHash {
			if err := s.cancelPendingOrder(ctx, current.Intent.ID, now); err != nil {
				return err
			}
			if err := s.syncNativeFills(ctx, requests); err != nil {
				return err
			}
			request.Order.NativeProtectionID, request.Order.NativeProtectionKind = "", ""
			continue
		}
		request.Order.NativeProtectionID, request.Order.NativeProtectionKind = current.Intent.ID, current.Intent.Conditional.Kind
		if request.Order.ProtectionPlacement == nil {
			request.Order.ProtectionPlacement = map[string]ProtectionPlacement{}
		}
		request.Order.ProtectionPlacement[string(current.Intent.Conditional.Kind)] = ProtectionPlacement{Requested: NativePreferredProtection, Effective: "native", Reason: "verified_conditional", OrderID: current.Intent.ID}
	}
	return nil
}

func (s *SharedAccount) cancelNativeForLot(ctx context.Context, order *PendingOrder, now int64) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, working := range snapshot.Orders {
		if !liveConditional(working) {
			continue
		}
		for _, allocation := range working.Intent.Allocations {
			if allocation.Strategy == order.Strategy && allocation.Lot == order.Lot {
				if err := s.cancelPendingOrder(ctx, working.Intent.ID, now); err != nil {
					return err
				}
				break
			}
		}
	}
	order.NativeProtectionID, order.NativeProtectionKind = "", ""
	if err := s.syncNativeFills(ctx, []PendingProtectionRequest{{Order: order}}); err != nil {
		return err
	}
	order.NativeProtectionID, order.NativeProtectionKind = "", ""
	return nil
}

// Native reservations are canceled before any new owner or changed target can
// share their instrument. Cancellation consumes its final fills first.
func (s *SharedAccount) revokeConditionalReservations(ctx context.Context, updates []StrategyRebalance) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	var incoming []ExecutableTarget
	for _, update := range updates {
		for _, request := range update.Requests {
			for _, target := range request.Targets {
				target.Instrument = request.Instrument.ID
				incoming = append(incoming, target)
			}
		}
	}
	book, err := s.store.LoadPendingBook(ctx)
	if err != nil {
		return err
	}
	var intents []EligibleIntent
	for _, d := range book {
		intents = append(intents, d.Order.Entry)
		if d.Order.Exit != nil {
			intents = append(intents, *d.Order.Exit)
		}
	}
	for _, order := range snapshot.Orders {
		if !liveConditional(order) {
			continue
		}
		cancel := !ExclusiveOrderLot(snapshot, order.Intent.Instrument.ID, incoming, intents)
		for _, allocation := range order.Intent.Allocations {
			var held int64
			for _, lot := range snapshot.Lots {
				if lot.Strategy == allocation.Strategy && lot.ID == allocation.Lot && lot.Instrument.ID == order.Intent.Instrument.ID {
					held = lot.SignedSteps
				}
			}
			for _, update := range updates {
				if update.Strategy != allocation.Strategy {
					continue
				}
				present := false
				for _, target := range incoming {
					if target.Strategy == allocation.Strategy && target.Lot == allocation.Lot && target.Instrument == order.Intent.Instrument.ID {
						present = true
						cancel = cancel || target.SignedSteps != held
					}
				}
				cancel = cancel || update.Mode == StrategyTargetsFull && !present
			}
		}
		if cancel {
			if err := s.cancelPendingOrder(ctx, order.Intent.ID, updates[0].DecisionMS); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SharedAccount) addNativeReservations(ctx context.Context, definitions []PendingDefinition, requests map[string]*InstrumentRebalance, snapshot AccountSnapshot, latest Plan) error {
	var intents []EligibleIntent
	for _, d := range definitions {
		intents = append(intents, d.Order.Entry)
		if d.Order.Exit != nil {
			intents = append(intents, *d.Order.Exit)
		}
	}
	for n := range definitions {
		d := &definitions[n]
		o := &d.Order
		if o.ProtectionPlacement == nil {
			o.ProtectionPlacement = map[string]ProtectionPlacement{}
		}
		lot := pendingLot(snapshot, o, d.Instrument.ID)
		exclusive := ExclusiveOrderLot(snapshot, d.Instrument.ID, latest.Targets, intents)
		selected := false
		for _, kind := range []ProtectionKind{StopLossProtection, TakeProfitProtection, TrailingProtection} {
			key := string(kind)
			trigger, exists := o.Protections[key]
			if !exists || o.ProtectionDone[key] {
				continue
			}
			status := ProtectionPlacement{Requested: SoftwareProtection, Effective: "local", Reason: "local_policy"}
			if d.Policy.prefersNativeProtection() {
				status.Requested = NativePreferredProtection
			}
			if o.NativeProtectionID != "" && o.NativeProtectionKind == kind {
				status.Effective, status.Reason, status.OrderID = "native", "verified_conditional", o.NativeProtectionID
				selected = true
				o.ProtectionPlacement[key] = status
				continue
			}
			reason := ""
			switch {
			case !d.Policy.prefersNativeProtection():
				reason = "local_policy"
			case !exclusive:
				reason = "shared_instrument"
			case lot.SignedSteps == 0:
				reason = "no_held_position"
			case o.Desired != lot.SignedSteps || o.ResumeEntrySteps > 0:
				reason = "position_transition"
			case !o.Canceled && o.Entry.State != Filled && o.Entry.QuantitySteps > absSteps(lot.SignedSteps):
				reason = "entry_growth"
			case o.Exit != nil && o.Exit.State != Filled && o.Exit.State != Canceled && o.Exit.State != Expired:
				reason = "explicit_exit"
			case selected || o.NativeProtectionID != "":
				reason = "another_native_protection"
			case trigger.Triggered:
				reason = "already_triggered"
			case kind == TrailingProtection:
				reason = "native_trailing_state_recovery_unproven"
			}
			condition := ConditionalOrder{Kind: kind, TriggerPrice: o.ProtectionLevels[key], Limit: trigger.Conditions.Limit, Reference: MidpointTrigger}
			if reason == "" {
				var err error
				condition.SourceHash, err = ConditionalSourceHash(*d, kind)
				if err != nil {
					return err
				}
				if err := NativeConditionalSupport(s.opts.Adapter, condition); err != nil {
					reason = "conditional_recovery_or_reference_unproven"
				}
			}
			if reason == "" {
				for _, working := range snapshot.Orders {
					if working.Intent.Instrument.ID == d.Instrument.ID && !terminalOrder(string(working.State)) {
						reason = "working_execution"
						break
					}
				}
			}
			if reason != "" {
				status.Reason = reason
				o.ProtectionPlacement[key] = status
				continue
			}
			steps := min(trigger.QuantitySteps-trigger.FilledSteps, absSteps(lot.SignedSteps))
			if steps <= 0 {
				status.Reason = "no_remaining_quantity"
				o.ProtectionPlacement[key] = status
				continue
			}
			intent := EligibleIntent{ID: VirtualIntentID(fmt.Sprintf("native/%s/%s/%s", o.Lot, kind, condition.SourceHash)), Account: o.Entry.Account, Strategy: o.Strategy, Lot: o.Lot, Instrument: d.Instrument.ID, Kind: ExitIntent, Side: trigger.Side, QuantitySteps: steps, State: Eligible}
			requests[d.Instrument.ID].ConditionalReservations = append(requests[d.Instrument.ID].ConditionalReservations, ConditionalReservation{Intent: intent, Condition: condition})
			status.Effective, status.Reason = "native", "verified_conditional"
			o.ProtectionPlacement[key] = status
			selected = true
		}
	}
	return nil
}

// applyNativeFillState runs in the same transaction as the trade ledger. The
// next unrelated strategy revision cannot carry the old pre-fill target.
func (s *Store) applyNativeFillState(ctx context.Context, tx *storeTxn, order StoredOrder, report FillReport, filled int64) error {
	if order.Intent.Conditional == nil {
		return nil
	}
	ctx = context.WithValue(ctx, storeTxContextKey{}, scopedStoreTx{s, tx})
	book, err := s.LoadPendingBook(ctx)
	if err != nil {
		return err
	}
	latest, err := s.LatestPlan(ctx)
	if err != nil {
		return err
	}
	allocation := order.Intent.Allocations[0]
	for n := range book {
		o := &book[n].Order
		if o.Strategy != allocation.Strategy || o.Lot != allocation.Lot || book[n].Instrument.ID != order.Intent.Instrument.ID {
			continue
		}
		var lot VirtualLot
		err = s.commit(ctx, func(tx *storeTxn) error {
			var e error
			lot, e = s.readLot(tx, o.Strategy, o.Lot, order.Intent.Instrument)
			return e
		})
		if err != nil {
			return err
		}
		explicitExit := o.Exit != nil && o.Exit.State != Filled && o.Exit.State != Canceled && o.Exit.State != Expired
		desired := lot.SignedSteps
		if explicitExit {
			// Preserve the absolute exit goal in durable state too: a process
			// restart between this fill and command admission loses local copies.
			if lot.SignedSteps > 0 {
				desired = max(int64(0), min(o.Desired, lot.SignedSteps))
			} else if lot.SignedSteps < 0 {
				desired = min(int64(0), max(o.Desired, lot.SignedSteps))
			}
		} else {
			o.LastProtection = order.Intent.Conditional.Kind
		}
		o.Desired, o.Canceled, o.Entry.State = desired, true, Canceled
		o.NativeFillPlanID, o.LastNativeFill = latest.ID, report.EventID
		o.LastNativeProtection = order.Intent.Conditional.Kind
		kind := string(order.Intent.Conditional.Kind)
		if o.ProtectionDone == nil {
			o.ProtectionDone = map[string]bool{}
		}
		o.ProtectionDone[kind] = filled == order.Intent.Steps
		trigger := o.Protections[kind]
		trigger.Triggered = true
		trigger.FilledSteps = filled
		o.Protections[kind] = trigger
		if filled < order.Intent.Steps {
			o.NativeProtectionID, o.NativeProtectionKind = order.Intent.ID, order.Intent.Conditional.Kind
		} else {
			o.NativeProtectionID, o.NativeProtectionKind = "", ""
		}
		checkpoint, e := PendingBookCheckpoint(book)
		if e != nil {
			return e
		}
		return s.saveAcceptedCheckpoint(ctx, checkpoint)
	}
	return errors.New("execution: native fill has no pending source")
}

func (s *SharedAccount) nativeFillTargets(ctx context.Context, plan Plan, targets []ExecutableTarget) ([]ExecutableTarget, error) {
	targets = append([]ExecutableTarget(nil), targets...)
	book, err := s.store.LoadPendingBook(ctx)
	if err != nil {
		return nil, err
	}
	for n := range targets {
		for _, d := range book {
			o := d.Order
			if o.LastNativeFill != "" && o.NativeFillPlanID == plan.ID && targets[n].Strategy == o.Strategy && targets[n].Lot == o.Lot && targets[n].Instrument == d.Instrument.ID {
				targets[n].SignedSteps = o.Desired
			}
		}
	}
	return targets, nil
}
