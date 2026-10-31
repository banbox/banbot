package execution

import (
	"errors"

	"github.com/shopspring/decimal"
)

type OrderMode string
type ProtectionMode string

const (
	LocalFirst    OrderMode = "local"
	ExchangeFirst OrderMode = "exchange_first"
	SoftwareProtection ProtectionMode = "software"
	NativePreferredProtection ProtectionMode = "native_preferred"
)

// OrderPolicy separates venue submission from internal matching. NearLimitBPS
// is a relative distance, not permission to execute locally outside a limit.
type OrderPolicy struct {
	Mode         OrderMode       `yaml:"order_mode"`
	NearLimitBPS decimal.Decimal `yaml:"near_limit_bps"`
	ProtectionMode ProtectionMode `yaml:"protection_mode"`
}

func (p OrderPolicy) Validate() error {
	if p.Mode != "" && p.Mode != LocalFirst && p.Mode != ExchangeFirst {
		return errors.New("execution: order_mode must be local or exchange_first")
	}
	if p.NearLimitBPS.IsNegative() || p.NearLimitBPS.GreaterThan(decimal.NewFromInt(10000)) {
		return errors.New("execution: near_limit_bps must be between zero and 10000")
	}
	if p.ProtectionMode != "" && p.ProtectionMode != SoftwareProtection && p.ProtectionMode != NativePreferredProtection {
		return errors.New("execution: protection_mode must be software or native_preferred")
	}
	return nil
}

func (p OrderPolicy) prefersNativeProtection() bool {
	return p.ProtectionMode == NativePreferredProtection || p.ProtectionMode == "" && p.Mode == ExchangeFirst
}

func (p OrderPolicy) nearLimit() decimal.Decimal {
	if p.NearLimitBPS.IsZero() {
		return decimal.NewFromInt(10)
	}
	return p.NearLimitBPS
}

// Admit is evaluated with the complete account contributor set, including
// pending orders and carried factor targets. Nonmarketable orders may only be
// delegated when a single virtual lot owns the instrument.
func (p OrderPolicy) Admit(i *EligibleIntent, price decimal.Decimal, nowMS, bar int64, exclusive bool) error {
	if err := p.Validate(); err != nil {
		return err
	}
	i.VenueLimit = nil
	available, err := i.Evaluate(price, nowMS, bar)
	if err != nil {
		return err
	}
	if available > 0 {
		return nil
	}
	if !exclusive || !i.Conditions.Limit.IsPositive() {
		return nil
	}
	if p.Mode == ExchangeFirst || i.Conditions.Limit.Sub(price).Abs().Mul(decimal.NewFromInt(10000)).LessThanOrEqual(price.Mul(p.nearLimit())) {
		copy := p
		i.VenueLimit = &copy
	}
	return nil
}

// AvailableForSubmission never relaxes stops, trailing activation or expiry.
// Only a limit can rest at the venue ahead of its matching price. Internal
// crossing must continue using Evaluate instead.
func (i *EligibleIntent) AvailableForSubmission(price decimal.Decimal, nowMS, bar int64) (int64, error) {
	available, err := i.Evaluate(price, nowMS, bar)
	if err != nil || available > 0 || i.VenueLimit == nil || i.State == Expired || i.State == Canceled || i.State == Filled {
		return available, err
	}
	if err := i.VenueLimit.Validate(); err != nil {
		return 0, err
	}
	if !i.Conditions.Limit.IsPositive() {
		return 0, errors.New("execution: venue limit admission requires a limit")
	}
	if i.VenueLimit.Mode != ExchangeFirst && i.Conditions.Limit.Sub(price).Abs().Mul(decimal.NewFromInt(10000)).GreaterThan(price.Mul(i.VenueLimit.nearLimit())) {
		return 0, nil
	}
	copy := *i
	copy.Conditions.Limit = decimal.Zero
	copy.Conditions.PostOnly = false
	return copy.Evaluate(price, nowMS, bar)
}

// ExclusiveOrderLot deliberately uses lot, not only strategy identity: two
// independent protective orders of one strategy can conflict just as well.
func ExclusiveOrderLot(snapshot AccountSnapshot, instrument string, targets []ExecutableTarget, intents []EligibleIntent) bool {
	type ownerKey struct {
		strategy StrategyID
		lot      VirtualLotID
	}
	owners := make(map[ownerKey]bool)
	add := func(strategy StrategyID, lot VirtualLotID) { owners[ownerKey{strategy, lot}] = true }
	for _, lot := range snapshot.Lots {
		if lot.Instrument.ID == instrument && lot.SignedSteps != 0 {
			add(lot.Strategy, lot.ID)
		}
	}
	for _, target := range targets {
		if target.SignedSteps != 0 && (target.Instrument == "" || target.Instrument == instrument) {
			add(target.Strategy, target.Lot)
		}
	}
	for _, intent := range intents {
		if intent.Instrument == instrument && intent.State != Filled && intent.State != Canceled && intent.State != Expired {
			add(intent.Strategy, intent.Lot)
		}
	}
	for _, order := range snapshot.Orders {
		if order.Intent.Instrument.ID != instrument || order.State == OrderFilled || order.State == OrderCanceled || order.State == OrderRejected {
			continue
		}
		for _, allocation := range order.Intent.Allocations {
			add(allocation.Strategy, allocation.Lot)
		}
	}
	return len(owners) == 1
}
