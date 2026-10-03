package execution

import "errors"

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

// Untouched target declarations reserve possible future exposure, without
// granting margin freed by a reduction that this plan will never execute.
func reservedCarriedExposure(snapshot AccountSnapshot, targets []ExecutableTarget, instruments []Instrument) ([]VirtualLot, []VirtualLot, error) {
	lots, positions, err := confirmedExposure(snapshot)
	if err != nil || len(targets) == 0 {
		return lots, positions, err
	}
	type key struct {
		instrument string
		strategy   StrategyID
		lot        VirtualLotID
	}
	units := make(map[string]Instrument)
	for _, instrument := range instruments {
		units[instrument.ID] = instrument
	}
	virtual := make(map[key]VirtualLot)
	desired := make(map[key]int64)
	for _, lot := range lots {
		k := key{lot.Instrument.ID, lot.Strategy, lot.ID}
		virtual[k] = lot
		desired[k] = lot.SignedSteps
	}
	for _, lot := range snapshot.Lots {
		k := key{lot.Instrument.ID, lot.Strategy, lot.ID}
		if absSteps(lot.SignedSteps) > absSteps(virtual[k].SignedSteps) {
			virtual[k] = lot
		}
	}
	carriedIDs := make(map[string]bool)
	for _, target := range targets {
		instrument, ok := units[target.Instrument]
		if !ok {
			return nil, nil, errors.New("execution: carried reservation units missing")
		}
		k := key{target.Instrument, target.Strategy, target.Lot}
		lot := virtual[k]
		if lot.Instrument.ID != "" {
			existing, _ := payload(lot.Instrument)
			carried, _ := payload(instrument)
			if existing != carried {
				return nil, nil, errors.New("execution: carried instrument units/version changed")
			}
		}
		lot.Instrument, lot.Strategy, lot.ID = instrument, target.Strategy, target.Lot
		if absSteps(target.SignedSteps) > absSteps(lot.SignedSteps) {
			lot.SignedSteps = target.SignedSteps
		}
		virtual[k] = lot
		desired[k] = target.SignedSteps
		carriedIDs[target.Instrument] = true
	}
	actual := make(map[string]VirtualLot)
	for _, position := range positions {
		actual[position.Instrument.ID] = position
	}
	for _, position := range snapshot.ActualPositions {
		if carriedIDs[position.Instrument.ID] && absSteps(position.SignedSteps) > absSteps(actual[position.Instrument.ID].SignedSteps) {
			actual[position.Instrument.ID] = position
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
		position := actual[id]
		position.Instrument = units[id]
		if absSteps(n) > absSteps(position.SignedSteps) {
			position.SignedSteps = n
		}
		actual[id] = position
	}
	lots, positions = nil, nil
	for _, lot := range virtual {
		lots = append(lots, lot)
	}
	for _, position := range actual {
		positions = append(positions, position)
	}
	return lots, positions, nil
}
