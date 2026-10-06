package execution

import (
	"context"
	"errors"
	"sort"

	"github.com/shopspring/decimal"
)

// ExecutableTarget is an absolute virtual position whose conditions have
// already been evaluated. Pending stop/limit intents are not targets; carry
// them unchanged in the account-combined Plan until they become executable.
type ExecutableTarget struct {
	Strategy    StrategyID
	Lot         VirtualLotID
	SignedSteps int64
	Instrument  string
	// SourceSequence identifies the contributor's accepted revision even when
	// another strategy owns the later account-combined replacement plan.
	SourceSequence uint64 `json:",omitempty"`
}
type PortfolioRisk struct {
	MarginRate          decimal.Decimal
	MaxAccountMargin    decimal.Decimal
	MaxVirtualGross     decimal.Decimal
	StrategyGrossLimits map[StrategyID]decimal.Decimal
	Marks               map[string]decimal.Decimal
}
type VirtualDelta struct {
	Strategy      StrategyID
	Lot           VirtualLotID
	Side          OrderSide
	Steps         int64
	ReducingSteps int64
}
type ExecutionLeg struct {
	Side       OrderSide
	Steps      int64
	ReduceOnly bool
}
type Coordination struct {
	ActualSteps    int64
	ProjectedSteps int64
	DesiredSteps   int64
	VirtualGross   decimal.Decimal
	AccountMargin  decimal.Decimal
	Deltas         []VirtualDelta
	Legs           []ExecutionLeg // decrease precedes increase across a net reversal
}

// Coordinate computes against actual net plus confirmed remaining real orders
// and separately against strategy lots plus their frozen allocations. Unknown,
// Sending and CancelPending block all ordinary deltas, including decreases.
func Coordinate(snapshot AccountSnapshot, targets []ExecutableTarget, instrument Instrument, observation ExecutionObservation, risk PortfolioRisk) (Coordination, error) {
	var result Coordination
	if err := instrument.Validate(); err != nil {
		return result, err
	}
	if snapshot.RiskFrozen {
		return result, errors.New("execution: external account state freezes coordination")
	}
	if !observation.Price.IsPositive() || observation.AtMS < 0 || observation.ValidUntilMS <= observation.AtMS || !risk.MarginRate.IsPositive() || risk.MarginRate.GreaterThan(decimal.NewFromInt(1)) || !risk.MaxAccountMargin.IsPositive() || !risk.MaxVirtualGross.IsPositive() {
		return result, errors.New("execution: invalid coordination observation/risk limits")
	}
	marks := make(map[string]decimal.Decimal, len(risk.Marks)+1)
	for id, mark := range risk.Marks {
		marks[id] = mark
	}
	marks[instrument.ID] = observation.Price
	equity, err := snapshot.Equity(marks)
	if err != nil {
		return result, err
	}
	type lotKey struct {
		strategy StrategyID
		lot      VirtualLotID
	}
	projected := make(map[lotKey]int64)
	original := make(map[lotKey]int64)
	strategyNAV := make(map[StrategyID]decimal.Decimal)
	for id, cash := range snapshot.SyntheticStrategyCash {
		strategyNAV[id] = cash
	}
	otherGross := make(map[StrategyID]decimal.Decimal)
	for _, lot := range snapshot.Lots {
		mark, ok := marks[lot.Instrument.ID]
		if !ok || !mark.IsPositive() {
			return result, errors.New("execution: strategy valuation mark missing")
		}
		strategyNAV[lot.Strategy] = strategyNAV[lot.Strategy].Add(lot.Unrealized(mark))
		if lot.Instrument.ID == instrument.ID {
			key := lotKey{lot.Strategy, lot.ID}
			projected[key] = lot.SignedSteps
			original[key] = lot.SignedSteps
		} else {
			otherGross[lot.Strategy] = otherGross[lot.Strategy].Add(lot.Instrument.Notional(absSteps(lot.SignedSteps), mark))
		}
	}
	projectedLots, projectedPositions, err := confirmedExposure(snapshot)
	if err != nil {
		return result, err
	}
	otherGross = make(map[StrategyID]decimal.Decimal)
	for _, lot := range projectedLots {
		if lot.Instrument.ID != instrument.ID {
			mark, ok := marks[lot.Instrument.ID]
			if !ok || !mark.IsPositive() {
				return result, errors.New("execution: projected lot mark missing")
			}
			otherGross[lot.Strategy] = otherGross[lot.Strategy].Add(lot.Instrument.Notional(absSteps(lot.SignedSteps), mark))
		}
	}
	otherMargin := decimal.Zero
	for _, position := range projectedPositions {
		if position.Instrument.ID != instrument.ID {
			mark, ok := marks[position.Instrument.ID]
			if !ok || !mark.IsPositive() {
				return result, errors.New("execution: projected position mark missing")
			}
			otherMargin = otherMargin.Add(position.Instrument.Notional(absSteps(position.SignedSteps), mark).Mul(risk.MarginRate))
		}
	}
	for _, position := range snapshot.ActualPositions {
		if position.Instrument.ID == instrument.ID {
			result.ActualSteps = position.SignedSteps
		}
	}
	result.ProjectedSteps = result.ActualSteps
	for _, order := range snapshot.Orders {
		if order.State == OrderSending || order.State == OrderUnknown || order.State == OrderCancelPending {
			return result, errors.New("execution: unresolved real order blocks ordinary deltas")
		}
		if order.State != OrderAcknowledged && order.State != OrderPartial {
			continue
		}
		if order.Intent.Instrument.ID != instrument.ID {
			continue
		}
		remaining := order.Intent.Steps - order.FilledSteps
		if remaining < 0 {
			return result, errors.New("execution: real order highwater exceeds quantity")
		}
		delta := remaining
		if order.Intent.Side == Sell {
			delta = -delta
		}
		result.ProjectedSteps, err = checkedSteps(result.ProjectedSteps, delta)
		if err != nil {
			return result, err
		}
		// Allocation highwaters are included in the snapshot so projection never
		// assumes a real fill was evenly distributed across its strategies.
		for _, a := range order.Intent.Allocations {
			remaining := a.Steps - order.AllocationFilled[a.ID]
			if remaining < 0 {
				return result, errors.New("execution: allocation highwater exceeds quantity")
			}
			delta := remaining
			if a.Side == Sell {
				delta = -delta
			}
			key := lotKey{a.Strategy, a.Lot}
			projected[key], err = checkedSteps(projected[key], delta)
			if err != nil {
				return result, err
			}
		}
	}
	seen := make(map[lotKey]bool)
	targetGross := make(map[StrategyID]decimal.Decimal)
	for id, n := range otherGross {
		targetGross[id] = n
	}
	for _, target := range targets {
		key := lotKey{target.Strategy, target.Lot}
		if !canonicalID(string(target.Strategy)) || !canonicalID(string(target.Lot)) || seen[key] || target.SignedSteps == -1<<63 || target.Instrument != "" && target.Instrument != instrument.ID {
			return result, errors.New("execution: invalid/duplicate executable target")
		}
		seen[key] = true
		result.DesiredSteps, err = checkedSteps(result.DesiredSteps, target.SignedSteps)
		if err != nil {
			return result, err
		}
		gross := instrument.Notional(absSteps(target.SignedSteps), observation.Price)
		targetGross[target.Strategy] = targetGross[target.Strategy].Add(gross)
		delta, err := checkedSteps(target.SignedSteps, -projected[key])
		if err != nil {
			return result, err
		}
		if delta == 0 {
			continue
		}
		side := Buy
		if delta < 0 {
			side = Sell
		}
		reducing := int64(0)
		if projected[key] > 0 && delta < 0 || projected[key] < 0 && delta > 0 {
			reducing = min(absSteps(projected[key]), absSteps(delta))
		}
		result.Deltas = append(result.Deltas, VirtualDelta{Strategy: target.Strategy, Lot: target.Lot, Side: side, Steps: absSteps(delta), ReducingSteps: reducing})
	}
	for key, n := range projected {
		if n != 0 && !seen[key] {
			return result, errors.New("execution: account-combined plan omitted an active strategy lot")
		}
	}
	previousTotalGross := decimal.Zero
	for strategy, gross := range targetGross {
		limit, ok := risk.StrategyGrossLimits[strategy]
		if !ok || !limit.IsPositive() {
			return result, errors.New("execution: strategy gross budget exceeded")
		}
		result.VirtualGross = result.VirtualGross.Add(gross)
		previous := otherGross[strategy]
		for key, n := range original {
			if key.strategy == strategy {
				previous = previous.Add(instrument.Notional(absSteps(n), observation.Price))
			}
		}
		previousTotalGross = previousTotalGross.Add(previous)
		if gross.GreaterThan(previous) && gross.GreaterThan(limit) {
			return result, errors.New("execution: strategy gross budget exceeded")
		}
		if gross.GreaterThan(previous) && gross.Mul(risk.MarginRate).GreaterThan(strategyNAV[strategy]) {
			return result, errors.New("execution: strategy NAV margin budget exceeded")
		}
	}
	if result.VirtualGross.GreaterThan(previousTotalGross) && result.VirtualGross.GreaterThan(risk.MaxVirtualGross) {
		return result, errors.New("execution: virtual gross exposure limit exceeded")
	}
	result.AccountMargin = instrument.Notional(absSteps(result.DesiredSteps), observation.Price).Mul(risk.MarginRate).Add(otherMargin)
	currentMargin := instrument.Notional(absSteps(result.ProjectedSteps), observation.Price).Mul(risk.MarginRate).Add(otherMargin)
	if result.AccountMargin.GreaterThan(currentMargin) && (result.AccountMargin.GreaterThan(risk.MaxAccountMargin) || result.AccountMargin.GreaterThan(equity)) {
		return result, errors.New("execution: actual net margin allowance exceeded")
	}
	delta, err := checkedSteps(result.DesiredSteps, -result.ProjectedSteps)
	if err != nil {
		return result, err
	}
	if delta != 0 {
		side := Buy
		if delta < 0 {
			side = Sell
		}
		remaining := absSteps(delta)
		if result.ProjectedSteps > 0 && delta < 0 || result.ProjectedSteps < 0 && delta > 0 {
			reduce := min(absSteps(result.ProjectedSteps), remaining)
			result.Legs = append(result.Legs, ExecutionLeg{Side: side, Steps: reduce, ReduceOnly: true})
			remaining -= reduce
		}
		if remaining > 0 {
			result.Legs = append(result.Legs, ExecutionLeg{Side: side, Steps: remaining})
		}
	}
	sort.Slice(result.Deltas, func(a, b int) bool {
		if result.Deltas[a].Strategy != result.Deltas[b].Strategy {
			return result.Deltas[a].Strategy < result.Deltas[b].Strategy
		}
		return result.Deltas[a].Lot < result.Deltas[b].Lot
	})
	return result, nil
}

// SubmitEligible persists a combined plan and frozen eligible allocations under
// the owner, then performs its first durable send. Send rechecks plan validity
// after this local phase, so a concurrent replacement cannot emit an old plan.
func (e *OwnerExecutor) SubmitEligible(plan Plan, order OrderIntent, nowMS int64) error {
	if err := e.local(func(ctx context.Context) error {
		if order.PostOnly && !e.Adapter.Capabilities().PostOnly {
			return errors.New("execution: adapter does not prove post-only support")
		}
		if err := e.Store.SavePlan(ctx, plan); err != nil {
			return err
		}
		return e.Store.PrepareOrder(ctx, order, nowMS)
	}); err != nil {
		return err
	}
	return e.Send(order.ID, nowMS)
}
