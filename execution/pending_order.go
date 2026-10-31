package execution

import (
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
)

// PendingOrder is the checkpointed execution state of one economic lot. The
// strategy facade may add request/projection metadata but does not own trigger
// algorithms. Field names retain compatibility with existing checkpoints.
type PendingOrder struct {
	Strategy             StrategyID
	Lot                  VirtualLotID
	Entry                EligibleIntent
	EntryFeedExpiry      *PendingFeedExpiry `json:",omitempty"`
	Exit                 *EligibleIntent
	ResumeEntrySteps     int64
	NativeExitOrders     []string
	Desired              int64
	Canceled             bool
	Protections          map[string]EligibleIntent
	ProtectionLevels     map[string]decimal.Decimal
	ProtectionDone       map[string]bool
	LastProtection       ProtectionKind
	NativeProtectionID   string
	NativeProtectionKind ProtectionKind
	NativeFillPlanID     string
	LastNativeFill       string
	LastNativeProtection ProtectionKind `json:",omitempty"`
	ProtectionPlacement  map[string]ProtectionPlacement
}

// AdvanceProtection selects at most one reduction. Selection is deterministic
// (SL, TP, trailing); the caller must cancel/reconcile the outstanding entry
// before CommitProtection uses the final held quantity.
func (o *PendingOrder) AdvanceProtection(account AccountKey, instrument Instrument, specs []PositionProtection,
	signed int64, basis, price decimal.Decimal, now, bar int64,
) (ProtectionKind, int64, error) {
	if o.Exit != nil && o.Exit.State != Canceled && o.Exit.State != Filled && o.Exit.State != Expired {
		return "", 0, nil
	}
	held := absSteps(signed)
	if held == 0 {
		return "", 0, nil
	}
	if err := instrument.Validate(); err != nil {
		return "", 0, err
	}
	if o.Protections == nil {
		o.Protections = make(map[string]EligibleIntent)
	}
	if o.ProtectionLevels == nil {
		o.ProtectionLevels = make(map[string]decimal.Decimal)
	}
	if o.ProtectionDone == nil {
		o.ProtectionDone = make(map[string]bool)
	}
	fillPrice := basis.Div(decimal.NewFromInt(held).Mul(instrument.QuantityStep).Mul(instrument.ContractSize))
	byKind := make(map[ProtectionKind]PositionProtection)
	for _, spec := range specs {
		byKind[spec.Kind] = spec
	}
	for _, kind := range []ProtectionKind{StopLossProtection, TakeProfitProtection, TrailingProtection} {
		key := string(kind)
		if o.NativeProtectionID != "" && o.NativeProtectionKind == kind {
			continue
		}
		if o.ProtectionDone[key] {
			continue
		}
		trigger, exists := o.Protections[key]
		if !exists {
			spec, exists := byKind[kind]
			if !exists {
				continue
			}
			side := Sell
			if signed < 0 {
				side = Buy
			}
			template := EligibleIntent{ID: VirtualIntentID(fmt.Sprintf("legacy-%s/%s", kind, o.Lot)), Account: account, Strategy: o.Strategy, Lot: o.Lot, Instrument: instrument.ID, Kind: ExitIntent, Side: side}
			var level decimal.Decimal
			var configured bool
			var err error
			trigger, level, configured, err = PreparePositionProtection(template, spec, held, fillPrice, bar)
			if err != nil {
				return "", 0, err
			}
			if !configured {
				continue
			}
			o.ProtectionLevels[key] = level
		}
		steps, err := EvaluatePositionProtection(kind, &trigger, o.ProtectionLevels[key], price, now, bar)
		if err != nil {
			return "", 0, err
		}
		o.Protections[key] = trigger
		if steps > 0 {
			return kind, min(steps, held), nil
		}
	}
	return "", 0, nil
}

func (o *PendingOrder) CommitProtection(kind ProtectionKind, steps, observedHeld, finalSigned int64) error {
	trigger, exists := o.Protections[string(kind)]
	if !exists || steps <= 0 || observedHeld <= 0 {
		return errors.New("execution: missing selected protection")
	}
	held := absSteps(finalSigned)
	if steps >= observedHeld {
		steps = held
	}
	steps = min(steps, held)
	if steps == 0 {
		o.Entry.State = Canceled
		o.Canceled = true
		o.Desired = finalSigned
		return nil
	}
	trigger.QuantitySteps = steps
	trigger.FilledSteps, trigger.ReservedSteps = 0, 0
	o.Protections[string(kind)] = trigger
	o.Exit = &trigger
	o.NativeExitOrders = nil
	o.Entry.State = Canceled
	o.Canceled = true
	o.Desired = held - steps
	if finalSigned < 0 {
		o.Desired = -o.Desired
	}
	o.ProtectionDone[string(kind)] = true
	o.LastProtection = kind
	return nil
}

func (o *PendingOrder) AdvanceEntry(price decimal.Decimal, now, bar, filled int64, policy OrderPolicy, exclusive bool) error {
	if o.Canceled || o.Entry.State == Filled {
		return nil
	}
	o.Entry.FilledSteps = absSteps(filled)
	if o.Entry.FilledSteps >= o.Entry.QuantitySteps {
		o.Entry.State, o.Desired = Filled, filled
		return nil
	}
	if err := policy.Admit(&o.Entry, price, now, bar, exclusive); err != nil {
		return err
	}
	available, err := o.Entry.AvailableForSubmission(price, now, bar)
	if err != nil {
		return err
	}
	if available > 0 {
		o.Desired = o.Entry.QuantitySteps
		if o.Entry.Side == Sell {
			o.Desired = -o.Desired
		}
	} else {
		// A limit that retreats before submission is no longer an executable
		// target. Confirmed live reservations are reconciled by the controller.
		o.Desired = filled
	}
	return nil
}
