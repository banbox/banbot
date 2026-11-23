package execution

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"sort"
)

// isolationBudget bounds the extra preparations one autonomous tick may spend
// excluding entries. Every preparation rolls back atomically on refusal (the
// whole plan, pending book and acceptance records are one local transaction),
// so a retry has no side effect beyond the idempotent reservation revocation
// that sequenceOpposite retries already accept.
const isolationBudget = 8

// isolatedCode returns the code of a deterministic pre-commit refusal and its
// attribution. Only a single Unwrap chain counts: a joined error is an
// uncertain outcome. self_cross is not isolatable (it is sequenced instead).
func isolatedCode(err error) (string, rejectionScope, bool) {
	for e := err; e != nil; e = errors.Unwrap(e) {
		switch v := e.(type) {
		case *RejectedError:
			if hubRejectCodes[v.Code] && v.Code != RejectSelfCross {
				return v.Code, v.Scope, true
			}
			return "", rejectionScope{}, false
		case *venueMinimumError:
			return "venue_minimum", rejectionScope{Instrument: v.Instrument}, true
		}
	}
	return "", rejectionScope{}, false
}

type isolationCandidate struct {
	ref        lotRef
	instrument string
	request    int
	target     int
	definition int
	held       int64
	excluded   bool
}

// isolationCandidates lists the lots whose delta only INCREASES their position
// (new entry or add) and whose pending entry is open with nothing resting at
// the venue: the only lots the owner may drop (and only if the owner admitted them). A reduction, a flip, a lot with
// a live entry order or a reservation is never a candidate. Order is the
// pending-book order (strategy, lot).
func isolationCandidates(snapshot AccountSnapshot, requests []InstrumentRebalance, definitions []PendingDefinition) []isolationCandidate {
	projectedLots, _, err := confirmedExposure(snapshot)
	if err != nil {
		return nil
	}
	type key struct {
		instrument string
		ref        lotRef
	}
	projected := make(map[key]int64, len(projectedLots))
	for _, lot := range projectedLots {
		projected[key{lot.Instrument.ID, lotRef{lot.Strategy, lot.ID}}] = lot.SignedSteps
	}
	byLot := make(map[key]int, len(definitions))
	for n, d := range definitions {
		byLot[key{d.Instrument.ID, lotRef{d.Order.Strategy, d.Order.Lot}}] = n
	}
	var out []isolationCandidate
	for ri, request := range requests {
		for ti, target := range request.Targets {
			k := key{request.Instrument.ID, lotRef{target.Strategy, target.Lot}}
			held := projected[k]
			delta := target.SignedSteps - held
			if delta == 0 || held > 0 && delta < 0 || held < 0 && delta > 0 {
				continue
			}
			di, ok := byLot[k]
			if !ok {
				continue
			}
			// Only an entry that passed owner admission (CommandRevision > 0) is
			// isolated: its refusal now is state dependent ("not now"). A book
			// entry the owner never admitted (seeded or written directly) keeps
			// today's fail-closed behaviour.
			od := &definitions[di].Order
			if definitions[di].CommandRevision == 0 || od.Canceled || od.Entry.ReservedSteps != 0 || od.Entry.State == Filled || od.Entry.State == Canceled || od.Entry.State == Expired {
				continue
			}
			live := false
			for _, order := range snapshot.Orders {
				if pendingEntryOrder(order, od, request.Instrument.ID) {
					live = true
					break
				}
			}
			if !live {
				out = append(out, isolationCandidate{ref: k.ref, instrument: k.instrument, request: ri, target: ti, definition: di, held: held})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ref.Strategy != out[j].ref.Strategy {
			return out[i].ref.Strategy < out[j].ref.Strategy
		}
		return out[i].ref.Lot < out[j].ref.Lot
	})
	return out
}

// pickIsolation chooses the next entry to exclude for a refusal. A refusal
// attributed to a lot excludes exactly that lot (or none when it is not an
// open increasing entry). Otherwise the candidates are narrowed by the
// attributed strategy and instrument and the LAST in book order is chosen, so
// older lots keep priority: a deterministic greedy exclusion. Returns -1 when
// nothing can be excluded.
func pickIsolation(candidates []isolationCandidate, scope rejectionScope, instrumentOnly bool) int {
	if scope.Lot != "" {
		for n, c := range candidates {
			if !c.excluded && c.ref == (lotRef{scope.Strategy, scope.Lot}) && (scope.Instrument == "" || c.instrument == scope.Instrument) {
				if scope.Kind != "" && scope.Kind != EntryIntent {
					return -1
				}
				return n
			}
		}
		return -1
	}
	for _, sameInstrument := range []bool{true, false} {
		if sameInstrument && scope.Instrument == "" || !sameInstrument && instrumentOnly && scope.Instrument != "" {
			continue
		}
		for n := len(candidates) - 1; n >= 0; n-- {
			c := candidates[n]
			if c.excluded || scope.Strategy != "" && c.ref.Strategy != scope.Strategy {
				continue
			}
			if sameInstrument && c.instrument != scope.Instrument {
				continue
			}
			// instrumentOnly (venue minimum): only an entry on that instrument can
			// change the refused leg. Strategy-level risk refusals may also be
			// relieved by another instrument of the strategy.
			return n
		}
	}
	return -1
}

type isolationRecord struct {
	Code       string
	Instrument string
	AtMS       int64
}

// isolateRejections handles a refused autonomous plan. Entries are excluded one
// at a time (terminal in the same commit as the plan that excludes them) until
// the plan prepares, at most isolationBudget times. Anything it cannot attribute
// to an excludable entry returns the refusal unchanged, i.e. today's fail ->
// freeze. definitions is mutated only for the excluded entries; prepare
// receives the adjusted requests and the audit records and commits on success.
func isolateRejections(snapshot AccountSnapshot, definitions []PendingDefinition, requests []InstrumentRebalance, now int64, first error,
	prepare func([]InstrumentRebalance, []StrategyCheckpoint) (PreparedRebalance, error)) (PreparedRebalance, error) {
	candidates := isolationCandidates(snapshot, requests, definitions)
	if len(candidates) == 0 {
		return PreparedRebalance{}, first
	}
	current := make([]InstrumentRebalance, len(requests))
	for n, r := range requests {
		current[n] = r
		current[n].Targets = append([]ExecutableTarget(nil), r.Targets...)
		current[n].IntentConstraints = append([]EligibleIntent(nil), r.IntentConstraints...)
	}
	var records []StrategyCheckpoint
	type excluded struct {
		ref        lotRef
		instrument string
		code       string
	}
	var done []excluded
	err := first
	for attempt := 0; attempt < isolationBudget; attempt++ {
		code, scope, ok := isolatedCode(err)
		if !ok {
			return PreparedRebalance{}, err
		}
		n := pickIsolation(candidates, scope, code == "venue_minimum")
		if n < 0 {
			return PreparedRebalance{}, err
		}
		c := &candidates[n]
		c.excluded = true
		request := &current[c.request]
		request.Targets[c.target].SignedSteps = c.held
		kept := request.IntentConstraints[:0]
		for _, constraint := range request.IntentConstraints {
			if constraint.Strategy != c.ref.Strategy || constraint.Lot != c.ref.Lot {
				kept = append(kept, constraint)
			}
		}
		request.IntentConstraints = kept
		od := &definitions[c.definition].Order
		od.Canceled, od.Desired = true, c.held
		od.Entry.State = Canceled
		od.Entry.FilledSteps = absSteps(c.held)
		od.Entry.ReservedSteps = 0
		od.Entry.VenueLimit = nil
		body, merr := json.Marshal(isolationRecord{Code: code, Instrument: c.instrument, AtMS: now})
		if merr != nil {
			return PreparedRebalance{}, merr
		}
		records = append(records, StrategyCheckpoint{
			Strategy: c.ref.Strategy, Name: "pending-isolated:" + string(c.ref.Lot), Payload: body,
			Events: []StrategyAcceptedEvent{{Strategy: c.ref.Strategy, Lot: c.ref.Lot, Kind: EntryIntent, CommandID: fmt.Sprintf("isolated-%s-%d", code, now), AtMS: now, Rejected: code}},
		})
		done = append(done, excluded{c.ref, c.instrument, code})
		prepared, perr := prepare(current, records)
		if perr == nil {
			for _, e := range done {
				log.Warn("execution: pending entry excluded by owner refusal", zap.String("strategy", string(e.ref.Strategy)), zap.String("lot", string(e.ref.Lot)), zap.String("instrument", e.instrument), zap.String("code", string(e.code)))
			}
			return prepared, nil
		}
		err = perr
	}
	return PreparedRebalance{}, err
}
