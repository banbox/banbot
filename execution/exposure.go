package execution

import (
	"errors"
	"github.com/shopspring/decimal"
)

// confirmedExposure projects every instrument, including instruments omitted
// from a new rebalance, using only acknowledged remaining allocations.
func confirmedExposure(snapshot AccountSnapshot) ([]VirtualLot, []VirtualLot, error) {
	type key struct {
		instrument string
		strategy   StrategyID
		lot        VirtualLotID
	}
	lots := make(map[key]VirtualLot)
	positions := make(map[string]VirtualLot)
	for _, lot := range snapshot.Lots {
		lots[key{lot.Instrument.ID, lot.Strategy, lot.ID}] = lot
	}
	for _, position := range snapshot.ActualPositions {
		positions[position.Instrument.ID] = position
	}
	for _, order := range snapshot.Orders {
		if order.State == OrderUnknown || order.State == OrderSending || order.State == OrderCancelPending {
			return nil, nil, errors.New("execution: unresolved order blocks exposure projection")
		}
		if order.State != OrderAcknowledged && order.State != OrderPartial {
			continue
		}
		n := order.Intent.Steps - order.FilledSteps
		if n < 0 {
			return nil, nil, errors.New("execution: order highwater exceeds quantity")
		}
		if order.Intent.Side == Sell {
			n = -n
		}
		position := positions[order.Intent.Instrument.ID]
		position.Instrument = order.Intent.Instrument
		var err error
		position.SignedSteps, err = checkedSteps(position.SignedSteps, n)
		if err != nil {
			return nil, nil, err
		}
		positions[position.Instrument.ID] = position
		for _, allocation := range order.Intent.Allocations {
			n := allocation.Steps - order.AllocationFilled[allocation.ID]
			if n < 0 {
				return nil, nil, errors.New("execution: allocation highwater exceeds quantity")
			}
			if allocation.Side == Sell {
				n = -n
			}
			k := key{order.Intent.Instrument.ID, allocation.Strategy, allocation.Lot}
			lot := lots[k]
			lot.Instrument, lot.Strategy, lot.ID = order.Intent.Instrument, allocation.Strategy, allocation.Lot
			lot.SignedSteps, err = checkedSteps(lot.SignedSteps, n)
			if err != nil {
				return nil, nil, err
			}
			lots[k] = lot
		}
	}
	var virtual, actual []VirtualLot
	for _, lot := range lots {
		virtual = append(virtual, lot)
	}
	for _, position := range positions {
		actual = append(actual, position)
	}
	return virtual, actual, nil
}

// exposureReservation separates quantity projection from conservative risk.
// Both gross and collateral reserve the maximum across possible carried states.
type exposureReservation struct {
	VirtualLot
	alternatives []int64
}

func (r exposureReservation) riskGross(mark decimal.Decimal) decimal.Decimal {
	amount := r.Instrument.RiskGross(r.SignedSteps, mark)
	for _, n := range r.alternatives {
		amount = decimal.Max(amount, r.Instrument.RiskGross(n, mark))
	}
	return amount
}
func (r exposureReservation) collateral(mark, rate decimal.Decimal) decimal.Decimal {
	amount := r.Instrument.Collateral(r.SignedSteps, mark, rate)
	for _, n := range r.alternatives {
		amount = decimal.Max(amount, r.Instrument.Collateral(n, mark, rate))
	}
	return amount
}

// Untouched declarations reserve possible exposure without granting risk freed
// by reductions that this plan will never execute. Non-carried orders retain
// the existing confirmed remaining-allocation projection.
func reservedCarriedExposure(snapshot AccountSnapshot, targets []ExecutableTarget, instruments []Instrument) ([]exposureReservation, []exposureReservation, error) {
	lots, positions, err := confirmedExposure(snapshot)
	if err != nil {
		return nil, nil, err
	}
	type key struct {
		instrument string
		strategy   StrategyID
		lot        VirtualLotID
	}
	units := make(map[string]Instrument)
	for _, i := range instruments {
		units[i.ID] = i
	}
	virtual := make(map[key]exposureReservation)
	desired := make(map[key]int64)
	for _, lot := range lots {
		k := key{lot.Instrument.ID, lot.Strategy, lot.ID}
		virtual[k] = exposureReservation{VirtualLot: lot}
		desired[k] = lot.SignedSteps
	}
	carriedIDs := make(map[string]bool)
	for _, target := range targets {
		i, ok := units[target.Instrument]
		if !ok {
			return nil, nil, errors.New("execution: carried reservation units missing")
		}
		k := key{target.Instrument, target.Strategy, target.Lot}
		r := virtual[k]
		if r.Instrument.ID != "" {
			existing, _ := payload(r.Instrument)
			carried, _ := payload(i)
			if existing != carried {
				return nil, nil, errors.New("execution: carried instrument units/version changed")
			}
		}
		r.Instrument, r.Strategy, r.ID = i, target.Strategy, target.Lot
		r.alternatives = append(r.alternatives, target.SignedSteps)
		virtual[k] = r
		desired[k] = target.SignedSteps
		carriedIDs[target.Instrument] = true
	}
	for _, lot := range snapshot.Lots {
		if len(targets) > 0 {
			k := key{lot.Instrument.ID, lot.Strategy, lot.ID}
			r := virtual[k]
			r.alternatives = append(r.alternatives, lot.SignedSteps)
			virtual[k] = r
		}
	}
	actual := make(map[string]exposureReservation)
	for _, p := range positions {
		actual[p.Instrument.ID] = exposureReservation{VirtualLot: p}
	}
	for _, p := range snapshot.ActualPositions {
		if carriedIDs[p.Instrument.ID] {
			r := actual[p.Instrument.ID]
			r.alternatives = append(r.alternatives, p.SignedSteps)
			actual[p.Instrument.ID] = r
		}
	}
	net := make(map[string]int64)
	for k, n := range desired {
		if carriedIDs[k.instrument] {
			net[k.instrument], err = checkedSteps(net[k.instrument], n)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	for id, n := range net {
		r := actual[id]
		r.Instrument = units[id]
		r.alternatives = append(r.alternatives, n)
		actual[id] = r
	}
	var reservedLots, reservedPositions []exposureReservation
	for _, r := range virtual {
		reservedLots = append(reservedLots, r)
	}
	for _, r := range actual {
		reservedPositions = append(reservedPositions, r)
	}
	return reservedLots, reservedPositions, nil
}
