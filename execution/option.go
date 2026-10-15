package execution

import (
	"errors"
	"github.com/shopspring/decimal"
)

// EuropeanOption is a research/paper cash-settled European payoff. Premium and
// strike are settlement currency per underlying unit; QuantityStep*ContractSize
// is underlying units per ledger step. No physical delivery or early exercise.
// UnderlyingStressMax declares the scenario contract 0<=fixing<=bound. This is
// conservative collateral within that contract, NOT a live solvency guarantee.
type EuropeanOption struct {
	UnderlyingID        string
	Right               string // call or put
	Strike              decimal.Decimal
	ExpiryMS            int64
	UnderlyingStressMax decimal.Decimal
}

func (i Instrument) IsOption() bool { return i.Valuation == "cash_european_option" }

// Clone owns the optional descriptor; decimal values themselves are immutable.
func (i Instrument) Clone() Instrument {
	if i.Option != nil {
		option := *i.Option
		i.Option = &option
	}
	return i
}
func (i Instrument) ValidMark(p decimal.Decimal) bool {
	return p.IsPositive() || i.IsOption() && p.IsZero()
}
func (i Instrument) CheckExecutionTime(atMS int64) error {
	if i.IsOption() && (i.Option == nil || atMS >= i.Option.ExpiryMS) {
		return errors.New("execution: option execution at or after expiry")
	}
	return nil
}
func (i Instrument) OptionPayoff(fixing decimal.Decimal) decimal.Decimal {
	payoff := fixing.Sub(i.Option.Strike)
	if i.Option.Right == "put" {
		payoff = payoff.Neg()
	}
	return decimal.Max(decimal.Zero, payoff)
}

// Collateral reserves premium for longs and the entire stress payoff for shorts,
// independently of premium and perpetual margin rate. Across option contracts
// no payoff netting is assumed. Netted identical account contracts are exact.
func (i Instrument) Collateral(steps int64, mark, perpetualRate decimal.Decimal) decimal.Decimal {
	if !i.IsOption() {
		return i.Notional(absSteps(steps), mark).Mul(perpetualRate)
	}
	if steps >= 0 {
		return i.Notional(steps, mark)
	}
	bound := i.Option.Strike
	if i.Option.Right == "call" {
		bound = decimal.Max(decimal.Zero, i.Option.UnderlyingStressMax.Sub(i.Option.Strike))
	}
	return i.Notional(absSteps(steps), bound)
}

func tradeCash(i Instrument, side OrderSide, cost, realized, fee decimal.Decimal) decimal.Decimal {
	if !i.IsOption() {
		return realized.Sub(fee)
	}
	if side == Buy {
		return cost.Neg().Sub(fee)
	}
	return cost.Sub(fee)
}

// RiskGross reports an option stress exposure (at least its premium), so a low
// premium cannot bypass a gross cap. Perpetual gross remains marked notional.
func (i Instrument) RiskGross(steps int64, mark decimal.Decimal) decimal.Decimal {
	if i.IsOption() {
		return decimal.Max(i.Notional(absSteps(steps), mark), i.Collateral(steps, mark, decimal.NewFromInt(1)))
	}
	return i.Notional(absSteps(steps), mark)
}
