package execution

import (
	"errors"

	"github.com/shopspring/decimal"
)

type ProtectionKind string

const (
	StopLossProtection   ProtectionKind = "stop_loss"
	TakeProfitProtection ProtectionKind = "take_profit"
	TrailingProtection   ProtectionKind = "trailing"
)

// PositionProtection describes a software reduction, not a venue-native order.
// Distance is measured from actual fill cost; Rate zero means the full position.
type PositionProtection struct {
	Kind            ProtectionKind
	Level           decimal.Decimal
	Distance        decimal.Decimal
	Limit           decimal.Decimal
	Rate            decimal.Decimal
	TrailingPercent decimal.Decimal
	ActivationPrice decimal.Decimal
}

// PreparePositionProtection fixes quantity and price conditions at first
// activation. Callers persist the returned intent, including its trigger state.
func PreparePositionProtection(template EligibleIntent, spec PositionProtection, heldSteps int64,
	fillPrice decimal.Decimal, bar int64,
) (EligibleIntent, decimal.Decimal, bool, error) {
	if spec.Kind != StopLossProtection && spec.Kind != TakeProfitProtection && spec.Kind != TrailingProtection {
		return EligibleIntent{}, decimal.Zero, false, errors.New("execution: unknown position protection")
	}
	if spec.Level.IsNegative() || spec.Distance.IsNegative() || spec.Limit.IsNegative() || spec.Rate.IsNegative() || spec.Rate.GreaterThan(decimal.NewFromInt(1)) || spec.TrailingPercent.IsNegative() || spec.TrailingPercent.GreaterThanOrEqual(decimal.NewFromInt(100)) || spec.ActivationPrice.IsNegative() || heldSteps < 0 || bar < 0 || !fillPrice.IsPositive() {
		return EligibleIntent{}, decimal.Zero, false, errors.New("execution: invalid position protection")
	}
	level := spec.Level
	conditions := IntentConditions{Limit: spec.Limit, CreatedBar: bar}
	if spec.Kind == TrailingProtection {
		if spec.TrailingPercent.IsZero() {
			return EligibleIntent{}, decimal.Zero, false, nil
		}
		conditions.TrailingPercent, conditions.ActivationPrice = spec.TrailingPercent, spec.ActivationPrice
	} else {
		if level.IsZero() && spec.Distance.IsPositive() {
			if (spec.Kind == StopLossProtection && template.Side == Sell) || (spec.Kind == TakeProfitProtection && template.Side == Buy) {
				level = fillPrice.Sub(spec.Distance)
			} else {
				level = fillPrice.Add(spec.Distance)
			}
			if !level.IsPositive() {
				return EligibleIntent{}, decimal.Zero, false, errors.New("execution: nonpositive relative protection level")
			}
		}
		if level.IsZero() {
			return EligibleIntent{}, decimal.Zero, false, nil
		}
		if spec.Kind == StopLossProtection {
			conditions.Stop = level
		}
	}
	rate := spec.Rate
	if rate.IsZero() {
		rate = decimal.NewFromInt(1)
	}
	steps, err := QuantitySteps(decimal.NewFromInt(heldSteps).Mul(rate), decimal.NewFromInt(1))
	if err != nil {
		return EligibleIntent{}, decimal.Zero, false, err
	}
	if steps == 0 {
		return EligibleIntent{}, decimal.Zero, false, nil
	}
	template.QuantitySteps, template.Conditions = steps, conditions
	if template.Kind != ExitIntent {
		return EligibleIntent{}, decimal.Zero, false, errors.New("execution: protection must reduce a position")
	}
	if err := template.Validate(); err != nil {
		return EligibleIntent{}, decimal.Zero, false, err
	}
	return template, level, true, nil
}

// EvaluatePositionProtection consumes one observable price. Take-profit
// activation latches before a separate limit can delay the actual reduction.
func EvaluatePositionProtection(kind ProtectionKind, intent *EligibleIntent, level, price decimal.Decimal,
	nowMS, bar int64,
) (int64, error) {
	if kind != StopLossProtection && kind != TakeProfitProtection && kind != TrailingProtection {
		return 0, errors.New("execution: unknown position protection")
	}
	if err := intent.Validate(); err != nil {
		return 0, err
	}
	if intent.Kind != ExitIntent || !price.IsPositive() || nowMS < 0 || bar < intent.Conditions.CreatedBar {
		return 0, errors.New("execution: invalid protection observation")
	}
	if kind == TakeProfitProtection && !intent.Triggered {
		if !level.IsPositive() {
			return 0, errors.New("execution: missing take-profit level")
		}
		intent.Triggered = intent.Side == Sell && price.GreaterThanOrEqual(level) || intent.Side == Buy && price.LessThanOrEqual(level)
		if !intent.Triggered {
			return 0, nil
		}
	}
	return intent.Evaluate(price, nowMS, bar)
}
