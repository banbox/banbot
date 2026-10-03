package execution

import (
	"errors"
	"github.com/shopspring/decimal"
)

// A coordinated plan reserves its complete account budget against bounded
// frozen marks. Every first send rechecks that reservation against current
// settled equity and strategy NAV, so fees/transfers cannot silently reuse an
// obsolete allowance. Expired marks require a new combined plan.
func validatePlanReservation(snapshot AccountSnapshot, plan Plan, risk PortfolioRisk, nowMS int64) error {
	if snapshot.RiskFrozen || plan.RiskValidUntilMS <= nowMS {
		return errors.New("execution: frozen risk reservation expired or account frozen")
	}
	instruments := make(map[string]Instrument)
	changed := make(map[string]bool)
	for _, instrument := range plan.Instruments {
		instruments[instrument.ID] = instrument
		if len(plan.RebalancedInstruments) == 0 {
			changed[instrument.ID] = true
		}
	}
	for _, id := range plan.RebalancedInstruments {
		changed[id] = true
	}
	if len(instruments) == 0 {
		return errors.New("execution: risk-reserved plan lacks instrument units")
	}
	nav := make(map[StrategyID]decimal.Decimal)
	for strategy, cash := range snapshot.SyntheticStrategyCash {
		nav[strategy] = cash
	}
	previousGross := make(map[StrategyID]decimal.Decimal)
	desiredNet := make(map[string]int64)
	currentNet := make(map[string]int64)
	for _, lot := range snapshot.Lots {
		mark, ok := risk.Marks[lot.Instrument.ID]
		if !ok || !mark.IsPositive() {
			return errors.New("execution: risk reservation lacks current lot mark")
		}
		amount := lot.Instrument.Notional(absSteps(lot.SignedSteps), mark)
		if changed[lot.Instrument.ID] {
			previousGross[lot.Strategy] = previousGross[lot.Strategy].Add(amount)
		}
		nav[lot.Strategy] = nav[lot.Strategy].Add(lot.Unrealized(mark))
		if !changed[lot.Instrument.ID] {
			instruments[lot.Instrument.ID] = lot.Instrument
		}
	}
	for _, position := range snapshot.ActualPositions {
		currentNet[position.Instrument.ID] = position.SignedSteps
		if _, ok := instruments[position.Instrument.ID]; !ok {
			instruments[position.Instrument.ID] = position.Instrument
		}
	}
	projectedLots, reservedPositions, projectionErr := reservedCarriedExposure(snapshot, plan.CarriedTargets, plan.Instruments)
	if projectionErr != nil {
		return projectionErr
	}
	desiredGross := make(map[StrategyID]decimal.Decimal)
	for _, lot := range projectedLots {
		if !changed[lot.Instrument.ID] {
			mark, ok := risk.Marks[lot.Instrument.ID]
			if !ok || !mark.IsPositive() {
				return errors.New("execution: projected lot mark missing")
			}
			desiredGross[lot.Strategy] = desiredGross[lot.Strategy].Add(lot.Instrument.Notional(absSteps(lot.SignedSteps), mark))
			previousGross[lot.Strategy] = previousGross[lot.Strategy].Add(lot.Instrument.Notional(absSteps(lot.SignedSteps), mark))
			instruments[lot.Instrument.ID] = lot.Instrument
		}
	}
	for _, target := range plan.Targets {
		// Untouched declarations preserve membership, while their actual pending
		// exposure above reserves the budget without initiating the old target.
		if !changed[target.Instrument] {
			continue
		}
		instrument, ok := instruments[target.Instrument]
		if !ok {
			return errors.New("execution: target instrument missing from risk reservation")
		}
		mark, ok := risk.Marks[target.Instrument]
		if !ok || !mark.IsPositive() {
			return errors.New("execution: target mark missing from risk reservation")
		}
		var err error
		desiredNet[target.Instrument], err = checkedSteps(desiredNet[target.Instrument], target.SignedSteps)
		if err != nil {
			return err
		}
		desiredGross[target.Strategy] = desiredGross[target.Strategy].Add(instrument.Notional(absSteps(target.SignedSteps), mark))
	}
	for _, order := range snapshot.Orders {
		if order.State == OrderUnknown || order.State == OrderSending || order.State == OrderCancelPending {
			return errors.New("execution: unresolved order invalidates budget admission")
		}
		if order.State != OrderAcknowledged && order.State != OrderPartial {
			continue
		}
		remaining := order.Intent.Steps - order.FilledSteps
		if order.Intent.Side == Sell {
			remaining = -remaining
		}
		var err error
		currentNet[order.Intent.Instrument.ID], err = checkedSteps(currentNet[order.Intent.Instrument.ID], remaining)
		if err != nil {
			return err
		}
	}
	for id, n := range currentNet {
		if !changed[id] {
			desiredNet[id] = n
		}
	}
	for _, position := range reservedPositions {
		if !changed[position.Instrument.ID] {
			desiredNet[position.Instrument.ID] = position.SignedSteps
			currentNet[position.Instrument.ID] = position.SignedSteps
		}
	}
	desiredMargin, currentMargin, totalGross, previousTotal := decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero
	for id, instrument := range instruments {
		mark, ok := risk.Marks[id]
		if !ok || !mark.IsPositive() {
			return errors.New("execution: account margin mark missing")
		}
		desiredMargin = desiredMargin.Add(instrument.Notional(absSteps(desiredNet[id]), mark).Mul(risk.MarginRate))
		currentMargin = currentMargin.Add(instrument.Notional(absSteps(currentNet[id]), mark).Mul(risk.MarginRate))
	}
	for strategy, amount := range desiredGross {
		limit, ok := risk.StrategyGrossLimits[strategy]
		if !ok || !limit.IsPositive() {
			return errors.New("execution: strategy reservation gross limit missing")
		}
		if amount.GreaterThan(previousGross[strategy]) && (amount.GreaterThan(limit) || amount.Mul(risk.MarginRate).GreaterThan(nav[strategy])) {
			return errors.New("execution: strategy NAV/gross reservation no longer valid")
		}
		totalGross = totalGross.Add(amount)
		previousTotal = previousTotal.Add(previousGross[strategy])
	}
	equity, err := snapshot.Equity(risk.Marks)
	if err != nil {
		return err
	}
	if desiredMargin.GreaterThan(currentMargin) && (desiredMargin.GreaterThan(risk.MaxAccountMargin) || desiredMargin.GreaterThan(equity)) || totalGross.GreaterThan(previousTotal) && totalGross.GreaterThan(risk.MaxVirtualGross) {
		return errors.New("execution: account equity/margin reservation no longer valid")
	}
	return nil
}
