package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/shopspring/decimal"
)

type TriggerReference string

const (
	MidpointTrigger  TriggerReference = "midpoint"
	LastTradeTrigger TriggerReference = "last"
	MarkPriceTrigger TriggerReference = "mark"
)

// ConditionalOrder is a dormant, reduce-only venue reservation. Its accounting
// intent is deliberately separate from the executable intents crossed locally.
type ConditionalOrder struct {
	Kind            ProtectionKind
	TriggerPrice    decimal.Decimal
	Limit           decimal.Decimal
	TrailingPercent decimal.Decimal
	ActivationPrice decimal.Decimal
	TrailingAnchor  decimal.Decimal
	SourceHash      string
	Reference       TriggerReference
}

type ConditionalReservation struct {
	Intent    EligibleIntent
	Condition ConditionalOrder
}

// ConditionalCapabilities proves the complete lifecycle, not just creation.
// ReduceOnly includes atomic enforcement across simultaneously triggered siblings.
type ConditionalCapabilities struct {
	EvidenceID                string
	ContextBound              bool
	StableClientID            bool
	QueryClientID             bool
	CompleteSnapshots         bool
	CompleteCumulativeReports bool
	ReduceOnly                bool
	Limit                     bool
	SeedTrailingAnchor        bool
	Kinds                     map[ProtectionKind]bool
	References                []TriggerReference
}

type NativeConditionalAdapter interface {
	NativeConditionalCapabilities() ConditionalCapabilities
}

func (c ConditionalOrder) Validate() error {
	if !canonicalID(c.SourceHash) || c.Limit.IsNegative() || c.TriggerPrice.IsNegative() || c.TrailingPercent.IsNegative() || c.ActivationPrice.IsNegative() || c.TrailingAnchor.IsNegative() {
		return errors.New("execution: invalid conditional descriptor")
	}
	if c.Reference != MidpointTrigger && c.Reference != LastTradeTrigger && c.Reference != MarkPriceTrigger {
		return errors.New("execution: unknown conditional trigger reference")
	}
	if c.Kind == TrailingProtection {
		if !c.TrailingPercent.IsPositive() || c.TrailingPercent.GreaterThanOrEqual(decimal.NewFromInt(100)) || !c.TriggerPrice.IsZero() || !c.Limit.IsZero() {
			return errors.New("execution: invalid native trailing descriptor")
		}
	} else if c.Kind != StopLossProtection && c.Kind != TakeProfitProtection || !c.TriggerPrice.IsPositive() || !c.TrailingPercent.IsZero() || !c.ActivationPrice.IsZero() || !c.TrailingAnchor.IsZero() {
		return errors.New("execution: invalid native fixed protection descriptor")
	}
	return nil
}

func (c ConditionalCapabilities) Supports(condition ConditionalOrder) error {
	if err := condition.Validate(); err != nil {
		return err
	}
	if condition.Kind == TrailingProtection {
		return errors.New("execution: native trailing state recovery unproven")
	}
	if !canonicalID(c.EvidenceID) || !c.ContextBound || !c.StableClientID || !c.QueryClientID || !c.CompleteSnapshots || !c.CompleteCumulativeReports || !c.ReduceOnly || !c.Kinds[condition.Kind] {
		return errors.New("execution: native conditional recovery/capability unproven")
	}
	if condition.Limit.IsPositive() && !c.Limit {
		return errors.New("execution: native conditional limit capability unproven")
	}
	if condition.TrailingAnchor.IsPositive() && !c.SeedTrailingAnchor {
		return errors.New("execution: native trailing anchor recovery unproven")
	}
	for _, reference := range c.References {
		if reference == condition.Reference {
			return nil
		}
	}
	return errors.New("execution: native conditional trigger reference unproven")
}

func NativeConditionalSupport(adapter ExecutionAdapter, condition ConditionalOrder) error {
	capable, ok := adapter.(NativeConditionalAdapter)
	if !ok {
		return errors.New("execution: native conditional recovery/capability unproven")
	}
	return capable.NativeConditionalCapabilities().Supports(condition)
}

func (r ConditionalReservation) Validate() error {
	if err := r.Intent.Validate(); err != nil {
		return err
	}
	if err := r.Condition.Validate(); err != nil {
		return err
	}
	conditions, _ := payload(r.Intent.Conditions)
	empty, _ := payload(IntentConditions{})
	if r.Intent.Kind != ExitIntent || conditions != empty || r.Intent.VenueLimit != nil || r.Intent.Triggered || r.Intent.TrailingActive || !r.Intent.TrailingAnchor.IsZero() || r.Intent.FilledSteps != 0 || r.Intent.ReservedSteps != 0 || r.Intent.State != Eligible {
		return errors.New("execution: conditional accounting intent must be a fresh conditionless reduction")
	}
	return nil
}

// ConditionalSourceHash binds a reservation to its immutable request and fixed
// protection level, without coupling identity to mutable runtime/report state.
func ConditionalSourceHash(definition PendingDefinition, kind ProtectionKind) (string, error) {
	var spec *PositionProtection
	for n := range definition.Specs {
		if definition.Specs[n].Kind == kind {
			if spec != nil {
				return "", errors.New("execution: duplicate conditional protection source")
			}
			spec = &definition.Specs[n]
		}
	}
	if spec == nil {
		return "", errors.New("execution: conditional protection source missing")
	}
	body, err := payload(struct {
		Strategy      StrategyID
		Lot           VirtualLotID
		Entry         VirtualIntentID
		Instrument    Instrument
		Spec          PositionProtection
		Level         decimal.Decimal
		QuantitySteps int64
	}{definition.Order.Strategy, definition.Order.Lot, definition.Order.Entry.ID, definition.Instrument, *spec, definition.Order.ProtectionLevels[string(kind)], definition.Order.Protections[string(kind)].QuantitySteps})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(body))
	return hex.EncodeToString(hash[:]), nil
}

func conditionalReservationFor(plan Plan, order OrderIntent) (*ConditionalReservation, error) {
	if order.Conditional == nil {
		for _, allocation := range order.Allocations {
			for _, reservation := range plan.ConditionalIntents {
				if allocation.IntentID == reservation.Intent.ID {
					return nil, errors.New("execution: dormant reservation cannot become an ordinary order")
				}
			}
		}
		return nil, nil
	}
	if !order.ReduceOnly || order.PostOnly || len(order.RequiresFilled) != 0 || len(order.Allocations) != 1 {
		return nil, errors.New("execution: conditional order requires one independent reduce-only allocation")
	}
	for _, price := range []decimal.Decimal{order.Conditional.TriggerPrice, order.Conditional.Limit} {
		if price.IsPositive() {
			_, remainder := price.QuoRem(order.Instrument.PriceTick, 0)
			if !remainder.IsZero() {
				return nil, errors.New("execution: conditional price violates instrument tick")
			}
		}
	}
	allocation := order.Allocations[0]
	for n := range plan.ConditionalIntents {
		reservation := &plan.ConditionalIntents[n]
		if reservation.Intent.ID != allocation.IntentID {
			continue
		}
		expected, _ := payload(reservation.Condition)
		actual, _ := payload(*order.Conditional)
		intent := reservation.Intent
		if expected != actual || intent.Strategy != allocation.Strategy || intent.Lot != allocation.Lot || intent.Instrument != order.Instrument.ID || intent.Kind != ExitIntent || allocation.Kind != ExitIntent || intent.Side != order.Side || allocation.Side != order.Side || intent.QuantitySteps != order.Steps || allocation.Steps != order.Steps || !order.Limit.Equal(reservation.Condition.Limit) {
			return nil, errors.New("execution: conditional order differs from frozen reservation")
		}
		return reservation, nil
	}
	return nil, errors.New("execution: conditional order absent from frozen plan")
}

func (s *Store) validateConditionalSend(ctx context.Context, order OrderIntent, plan Plan) error {
	reservation, err := conditionalReservationFor(plan, order)
	if err != nil || reservation == nil {
		return err
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return err
	}
	if snapshot.RiskFrozen || !ExclusiveOrderLot(snapshot, order.Instrument.ID, plan.Targets, plan.ContributorConstraints) {
		return errors.New("execution: conditional protection no longer owns an exclusive instrument")
	}
	intent := reservation.Intent
	var held, actual int64
	for _, lot := range snapshot.Lots {
		if lot.Strategy == intent.Strategy && lot.ID == intent.Lot && lot.Instrument.ID == intent.Instrument {
			held = lot.SignedSteps
		}
	}
	for _, position := range snapshot.ActualPositions {
		if position.Instrument.ID == intent.Instrument {
			actual = position.SignedSteps
		}
	}
	if held == 0 || actual != held || order.Steps > absSteps(held) || order.Side == Buy && held > 0 || order.Side == Sell && held < 0 {
		return errors.New("execution: conditional reduction exceeds exclusive actual holdings")
	}
	targetFound := false
	for _, target := range plan.Targets {
		if target.Instrument == intent.Instrument && target.Strategy == intent.Strategy && target.Lot == intent.Lot {
			if target.SignedSteps != held {
				return errors.New("execution: conditional protection conflicts with a changed position target")
			}
			targetFound = true
		}
	}
	if !targetFound {
		return errors.New("execution: conditional protection requires an unchanged owner target")
	}
	for _, working := range snapshot.Orders {
		if working.Intent.ID == order.ID || terminalOrder(string(working.State)) || working.State == OrderPrepared {
			continue
		}
		if working.State == OrderUnknown || working.State == OrderSending || working.State == OrderCancelPending {
			return errors.New("execution: uncertain order blocks conditional protection")
		}
		if working.Intent.Instrument.ID == intent.Instrument && working.Intent.Conditional == nil {
			return errors.New("execution: working execution conflicts with conditional protection")
		}
		if working.Intent.Instrument.ID == intent.Instrument && working.Intent.Conditional != nil && working.Intent.Conditional.Kind == order.Conditional.Kind {
			return errors.New("execution: duplicate live conditional protection")
		}
	}
	definitions, err := s.LoadPendingBook(ctx)
	if err != nil {
		return err
	}
	var pendingIntents []EligibleIntent
	for _, definition := range definitions {
		pendingIntents = append(pendingIntents, definition.Order.Entry)
		if definition.Order.Exit != nil {
			pendingIntents = append(pendingIntents, *definition.Order.Exit)
		}
	}
	if !ExclusiveOrderLot(snapshot, order.Instrument.ID, plan.Targets, pendingIntents) {
		return errors.New("execution: pending contributor conflicts with conditional protection")
	}
	for _, definition := range definitions {
		pending := definition.Order
		if pending.Strategy != intent.Strategy || pending.Lot != intent.Lot || definition.Instrument.ID != intent.Instrument {
			continue
		}
		expectedInstrument, _ := payload(definition.Instrument)
		actualInstrument, _ := payload(order.Instrument)
		if expectedInstrument != actualInstrument {
			return errors.New("execution: conditional instrument differs from pending source")
		}
		if pending.ProtectionDone[string(order.Conditional.Kind)] || pending.Exit != nil && pending.Exit.State != Filled && pending.Exit.State != Canceled && pending.Exit.State != Expired {
			return errors.New("execution: conditional protection source already exited or replaced")
		}
		if !definition.Policy.prefersNativeProtection() {
			return errors.New("execution: conditional protection policy no longer permits venue placement")
		}
		trigger, exists := pending.Protections[string(order.Conditional.Kind)]
		if !exists || trigger.Triggered || intent.QuantitySteps != min(trigger.QuantitySteps, absSteps(held)) || !order.Conditional.TriggerPrice.Equal(pending.ProtectionLevels[string(order.Conditional.Kind)]) || !order.Conditional.Limit.Equal(trigger.Conditions.Limit) {
			return errors.New("execution: conditional descriptor differs from pending protection")
		}
		hash, err := ConditionalSourceHash(definition, order.Conditional.Kind)
		if err != nil || hash != order.Conditional.SourceHash {
			return errors.Join(errors.New("execution: conditional protection source changed"), err)
		}
		return nil
	}
	return errors.New("execution: conditional protection source is not registered")
}
