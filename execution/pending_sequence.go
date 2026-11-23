package execution

import "sort"

func sideIndex(side OrderSide) int {
	if side == Sell {
		return 1
	}
	return 0
}

// lotRef names one economic lot of a strategy.
type lotRef struct {
	Strategy StrategyID
	Lot      VirtualLotID
}

// sequenceOutcome reports what sequenceOpposite did. Held lists every lot whose
// target was reset to its projected size; HeldReductions is the subset that
// was reducing (a delayed exit); Executed lists the reducing lots that kept
// their target against a held opposite reduction.
type sequenceOutcome struct {
	Held, HeldReductions, Executed []lotRef
}

// sequenceOppositeLots resolves a same-strategy self-cross by sequencing the
// opposite sides across ticks. See sequenceOpposite; this form never delays a
// reduction (command admission and the entry-only rule).
func sequenceOppositeLots(snapshot AccountSnapshot, requests []InstrumentRebalance) ([]InstrumentRebalance, int) {
	out, outcome := sequenceOpposite(snapshot, requests, false, nil)
	return out, len(outcome.Held)
}

// sequenceOpposite is used only after preparation rejected the combined plan
// with RejectSelfCross, so every plan that was accepted before is untouched.
// Crossing two lots of one strategy is not an option: the internal match
// requires distinct strategies and would realise PnL at the frozen midpoint
// without a fill, and netting both legs into the residual real order would
// leave the crossed quantity without a fill source.
//
// Per instrument and strategy, using the same projected lot sizes and delta
// arithmetic as Coordinate:
//   - a lot whose delta reduces its projected position (including the reducing
//     part of a flip) is never held, so exits and protections are not delayed;
//   - when the reductions all point one way, every lot whose delta is a pure
//     increase on the opposite side is held;
//   - when there is no reduction but increases of both sides, the side of the
//     lowest lot ID is kept and the other side is held;
//   - reductions on both sides cannot be resolved without delaying an exit.
//     Only when reductions is true (the autonomous tick) the side of the
//     highest-priority reducing lot is kept and every lot of the other side is
//     held for this tick. Priority is the number of ticks a lot's reduction was
//     already held (priority(lot), larger first), then the lowest lot ID, so a
//     held exit is executed on the next tick and cannot be starved: a lot that
//     outranks it must already have been held at least as often, executes
//     first, and leaves; new conflicting lots start at priority zero. With
//     reductions false that strategy is left as is (the rejection stands).
//
// Holding sets the lot's target to its projected size and drops its contributor
// constraints: exactly the representation of a lot that is not executable this
// tick (od.Desired and the entry's lifetime in the pending book are not
// touched, so it is re-evaluated at the next tick). A held lot is never
// removed from the request. Returns the adjusted copy and what was held.
func sequenceOpposite(snapshot AccountSnapshot, requests []InstrumentRebalance, reductions bool, priority func(lotRef) int) ([]InstrumentRebalance, sequenceOutcome) {
	var outcome sequenceOutcome
	projectedLots, _, err := confirmedExposure(snapshot)
	if err != nil {
		return requests, outcome
	}
	type lotKey struct {
		instrument string
		strategy   StrategyID
		lot        VirtualLotID
	}
	projected := make(map[lotKey]int64, len(projectedLots))
	for _, lot := range projectedLots {
		projected[lotKey{lot.Instrument.ID, lot.Strategy, lot.ID}] = lot.SignedSteps
	}
	type candidate struct {
		index    int
		side     OrderSide
		reducing bool
		held     int64
	}
	out := make([]InstrumentRebalance, len(requests))
	for n, request := range requests {
		out[n] = request
		byStrategy := make(map[StrategyID][]candidate)
		for index, target := range request.Targets {
			held := projected[lotKey{request.Instrument.ID, target.Strategy, target.Lot}]
			delta := target.SignedSteps - held
			if delta == 0 {
				continue
			}
			c := candidate{index: index, side: Buy, held: held}
			if delta < 0 {
				c.side = Sell
			}
			c.reducing = held > 0 && delta < 0 || held < 0 && delta > 0
			byStrategy[target.Strategy] = append(byStrategy[target.Strategy], c)
		}
		var hold []candidate
		for _, list := range byStrategy {
			var reduceSides, allSides [2]bool
			for _, c := range list {
				allSides[sideIndex(c.side)] = true
				if c.reducing {
					reduceSides[sideIndex(c.side)] = true
				}
			}
			bothReduce := reduceSides[0] && reduceSides[1]
			if !allSides[0] || !allSides[1] || bothReduce && !reductions {
				continue
			}
			keep := Buy
			switch {
			case bothReduce:
				ref := func(c candidate) lotRef {
					return lotRef{request.Targets[c.index].Strategy, request.Targets[c.index].Lot}
				}
				var first *candidate
				for i := range list {
					c := &list[i]
					if !c.reducing {
						continue
					}
					if first == nil {
						first = c
						continue
					}
					cp, fp := 0, 0
					if priority != nil {
						cp, fp = priority(ref(*c)), priority(ref(*first))
					}
					if cp > fp || cp == fp && request.Targets[c.index].Lot < request.Targets[first.index].Lot {
						first = c
					}
				}
				keep = first.side
			case reduceSides[0]:
				keep = Buy
			case reduceSides[1]:
				keep = Sell
			default:
				first := list[0]
				for _, c := range list[1:] {
					if request.Targets[c.index].Lot < request.Targets[first.index].Lot {
						first = c
					}
				}
				keep = first.side
			}
			for _, c := range list {
				target := request.Targets[c.index]
				switch {
				case c.side != keep && (!c.reducing || bothReduce):
					hold = append(hold, c)
					if c.reducing {
						outcome.HeldReductions = append(outcome.HeldReductions, lotRef{target.Strategy, target.Lot})
					}
				case bothReduce && c.reducing:
					outcome.Executed = append(outcome.Executed, lotRef{target.Strategy, target.Lot})
				}
			}
		}
		if len(hold) == 0 {
			continue
		}
		sort.Slice(hold, func(i, j int) bool { return hold[i].index < hold[j].index })
		out[n].Targets = append([]ExecutableTarget(nil), request.Targets...)
		heldLots := make(map[lotRef]bool)
		for _, c := range hold {
			target := &out[n].Targets[c.index]
			target.SignedSteps = c.held
			heldLots[lotRef{target.Strategy, target.Lot}] = true
			outcome.Held = append(outcome.Held, lotRef{target.Strategy, target.Lot})
		}
		out[n].IntentConstraints = nil
		for _, constraint := range request.IntentConstraints {
			if heldLots[lotRef{constraint.Strategy, constraint.Lot}] {
				continue
			}
			out[n].IntentConstraints = append(out[n].IntentConstraints, constraint)
		}
	}
	return out, outcome
}
