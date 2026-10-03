package execution

import (
	"errors"
	"strings"

	"github.com/shopspring/decimal"
)

type StrategyID string
type VirtualLotID string
type VirtualIntentID string

type OrderSide string

const (
	Buy  OrderSide = "buy"
	Sell OrderSide = "sell"
)

type IntentKind string

const (
	EntryIntent IntentKind = "entry"
	ExitIntent  IntentKind = "exit"
)

type IntentState string

const (
	PendingCondition IntentState = "PendingCondition"
	Eligible         IntentState = "Eligible"
	Reserved         IntentState = "Reserved"
	Partial          IntentState = "Partial"
	Filled           IntentState = "Filled"
	Expired          IntentState = "Expired"
	Canceled         IntentState = "Canceled"
)

// IntentConditions use trade side: buy limits require price <= limit and buy
// stops require price >= stop; sell comparisons are reversed. TrailingPercent
// is a percentage (1 means 1%), not a fraction.
type IntentConditions struct {
	Limit           decimal.Decimal
	PostOnly        bool // requires a limit and verified passive venue execution
	Stop            decimal.Decimal
	ActivationPrice decimal.Decimal
	TrailingPercent decimal.Decimal
	CreatedBar      int64
	StopBars        int64
	ExpiresAtMS     int64 // exclusive; zero means no timestamp expiry
}

// EligibleIntent is a local execution contract, serialized by its account
// owner. Triggered/TrailingActive/TrailingAnchor are checkpointable trigger
// state. Local Cancel is not an acknowledgement of a submitted venue order.
type EligibleIntent struct {
	ID             VirtualIntentID
	Account        AccountKey
	Strategy       StrategyID
	Lot            VirtualLotID
	Instrument     string
	Kind           IntentKind
	Side           OrderSide
	QuantitySteps  int64
	Conditions     IntentConditions
	State          IntentState
	FilledSteps    int64
	ReservedSteps  int64
	Triggered      bool
	TrailingActive bool
	TrailingAnchor decimal.Decimal
}

func (i *EligibleIntent) Validate() error {
	if i == nil {
		return errors.New("execution: nil intent")
	}
	if err := i.Account.Validate(); err != nil {
		return err
	}
	for _, id := range []string{string(i.ID), string(i.Strategy), string(i.Lot), i.Instrument} {
		if id == "" || strings.TrimSpace(id) != id {
			return errors.New("execution: intent identities must be canonical")
		}
	}
	c := i.Conditions
	if (i.Side != Buy && i.Side != Sell) || (i.Kind != EntryIntent && i.Kind != ExitIntent) || i.QuantitySteps <= 0 ||
		i.FilledSteps < 0 || i.FilledSteps > i.QuantitySteps || i.ReservedSteps < 0 || i.ReservedSteps > i.QuantitySteps-i.FilledSteps ||
		c.Limit.IsNegative() || c.Stop.IsNegative() || c.ActivationPrice.IsNegative() || c.TrailingPercent.IsNegative() ||
		c.TrailingPercent.GreaterThanOrEqual(decimal.NewFromInt(100)) || c.CreatedBar < 0 || c.StopBars < 0 || c.ExpiresAtMS < 0 ||
		(c.ActivationPrice.IsPositive() && !c.TrailingPercent.IsPositive()) || (c.Stop.IsPositive() && c.TrailingPercent.IsPositive()) {
		return errors.New("execution: invalid intent quantities or conditions")
	}
	if c.PostOnly && !c.Limit.IsPositive() {
		return errors.New("execution: post-only intent requires a positive limit")
	}
	if i.State != "" && i.State != PendingCondition && i.State != Eligible && i.State != Reserved && i.State != Partial && i.State != Filled && i.State != Expired && i.State != Canceled {
		return errors.New("execution: invalid intent state")
	}
	if i.TrailingActive && !i.TrailingAnchor.IsPositive() {
		return errors.New("execution: invalid trailing checkpoint")
	}
	return nil
}

// Evaluate consumes a visible execution price, never an OHLC future range.
// Stop activation latches, but a stop-limit must still satisfy its limit on each
// evaluation. StopBars expires at the start of CreatedBar+StopBars.
func (i *EligibleIntent) Evaluate(price decimal.Decimal, nowMS, bar int64) (int64, error) {
	if err := i.Validate(); err != nil {
		return 0, err
	}
	if !price.IsPositive() || nowMS < 0 || bar < i.Conditions.CreatedBar {
		return 0, errors.New("execution: invalid execution observation")
	}
	if i.State == Filled || i.State == Canceled || i.State == Expired {
		return 0, nil
	}
	c := i.Conditions
	if (c.ExpiresAtMS > 0 && nowMS >= c.ExpiresAtMS) || (c.StopBars > 0 && bar-c.CreatedBar >= c.StopBars) {
		i.State = Expired
		i.ReservedSteps = 0
		return 0, nil
	}
	ready := true
	if c.Stop.IsPositive() && !i.Triggered {
		i.Triggered = (i.Side == Buy && price.GreaterThanOrEqual(c.Stop)) || (i.Side == Sell && price.LessThanOrEqual(c.Stop))
	}
	if c.TrailingPercent.IsPositive() && !i.Triggered {
		if !i.TrailingActive && (c.ActivationPrice.IsZero() || (i.Side == Sell && price.GreaterThanOrEqual(c.ActivationPrice)) || (i.Side == Buy && price.LessThanOrEqual(c.ActivationPrice))) {
			i.TrailingActive, i.TrailingAnchor = true, price
		}
		if i.TrailingActive {
			if (i.Side == Sell && price.GreaterThan(i.TrailingAnchor)) || (i.Side == Buy && price.LessThan(i.TrailingAnchor)) {
				i.TrailingAnchor = price
			}
			distance := i.TrailingAnchor.Mul(c.TrailingPercent).Shift(-2)
			i.Triggered = (i.Side == Sell && price.LessThanOrEqual(i.TrailingAnchor.Sub(distance))) || (i.Side == Buy && price.GreaterThanOrEqual(i.TrailingAnchor.Add(distance)))
		}
	}
	if c.Stop.IsPositive() || c.TrailingPercent.IsPositive() {
		ready = i.Triggered
	}
	if c.Limit.IsPositive() && !c.PostOnly {
		ready = ready && ((i.Side == Buy && price.LessThanOrEqual(c.Limit)) || (i.Side == Sell && price.GreaterThanOrEqual(c.Limit)))
	}
	if i.FilledSteps > 0 {
		i.State = Partial
	} else if i.ReservedSteps > 0 {
		i.State = Reserved
	} else if ready {
		i.State = Eligible
	} else {
		i.State = PendingCondition
	}
	if !ready {
		return 0, nil
	}
	return i.QuantitySteps - i.FilledSteps - i.ReservedSteps, nil
}

func (i *EligibleIntent) Reserve(steps int64, price decimal.Decimal, nowMS, bar int64) error {
	available, err := i.Evaluate(price, nowMS, bar)
	if err != nil {
		return err
	}
	if steps <= 0 || steps > available {
		return errors.New("execution: reservation exceeds eligible quantity")
	}
	i.ReservedSteps += steps
	i.State = Reserved
	if i.FilledSteps > 0 {
		i.State = Partial
	}
	return nil
}

func (i *EligibleIntent) ApplyFill(steps int64) error {
	if err := i.Validate(); err != nil {
		return err
	}
	if steps <= 0 || steps > i.ReservedSteps || (i.State != Reserved && i.State != Partial) {
		return errors.New("execution: fill exceeds reservation")
	}
	i.ReservedSteps -= steps
	i.FilledSteps += steps
	if i.FilledSteps == i.QuantitySteps {
		i.State = Filled
	} else {
		i.State = Partial
	}
	return nil
}

// Cancel ends only the local unfilled remainder, preserving all prior fills.
func (i *EligibleIntent) Cancel() error {
	if err := i.Validate(); err != nil {
		return err
	}
	if i.State != Filled && i.State != Expired {
		i.State = Canceled
		i.ReservedSteps = 0
	}
	return nil
}

type LotSelection struct {
	Account           AccountKey
	Strategy          StrategyID
	Lot               VirtualLotID
	Instrument        string
	FilledSteps       int64
	PendingEntrySteps int64
}

type ExitSelection struct {
	Account    AccountKey
	Strategy   StrategyID
	Lot        VirtualLotID
	Instrument string
	FilledOnly bool
	UnFillOnly bool
}

// Select returns independent filled reduction and pending-entry cancellation.
// The caller must apply price conditions before creating executable reductions.
func (s ExitSelection) Select(lot LotSelection) (reduce, cancel int64, err error) {
	if err = s.Account.Validate(); err != nil {
		return
	}
	if s.Strategy == "" || s.Lot == "" || s.Instrument == "" || s.Account != lot.Account || s.Strategy != lot.Strategy || s.Lot != lot.Lot || s.Instrument != lot.Instrument {
		return 0, 0, errors.New("execution: exit does not own selected lot")
	}
	if s.FilledOnly && s.UnFillOnly || lot.FilledSteps < 0 || lot.PendingEntrySteps < 0 {
		return 0, 0, errors.New("execution: invalid exit selection")
	}
	if !s.UnFillOnly {
		reduce = lot.FilledSteps
	}
	if !s.FilledOnly {
		cancel = lot.PendingEntrySteps
	}
	return
}
