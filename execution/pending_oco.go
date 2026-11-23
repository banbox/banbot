package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"unicode"

	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

// OCO entry groups are enforced by the account owner.
//
// A group is scoped by (strategy, instrument, group key): the legacy
// EnterReq.OCOGroup contract is "account, symbol, strategy", and the account is
// implied by the owner. When several members of one group have an entry that
// is executable in the same tick (a gap through both legs of a breakout bracket,
// two limits crossed by one gap), at most one of them proceeds; the others are
// cancelled in the same commit as the plan (OCOSimultaneousReason). Once any
// member has a fill, its open unfilled siblings are cancelled on the next tick
// (OCOFilledSiblingReason), so OCO does not depend on a robot being online.
// The robot facade's post-fill cancel_entry remains as a harmless no-op.

const (
	// The closed set of OCO audit reasons carried by the acceptance event and
	// the pending-oco checkpoint of a cancelled sibling is
	// {OCOSimultaneousReason, OCOFilledSiblingReason}.
	// OCOSimultaneousReason: several members were executable in the same tick.
	OCOSimultaneousReason = "oco_simultaneous"
	// OCOFilledSiblingReason is the reason of an unfilled sibling cancelled
	// because another member of its group already has a fill.
	OCOFilledSiblingReason = "oco_filled_sibling"
	maxOCOGroupLen         = 128
)

// validOCOGroup is the admission rule of an OCO group key: canonical (non-empty,
// no surrounding space), bounded and free of control characters.
func validOCOGroup(group string) bool {
	if !canonicalID(group) || len(group) > maxOCOGroupLen {
		return false
	}
	for _, r := range group {
		if unicode.IsControl(r) || r == unicode.ReplacementChar {
			return false
		}
	}
	return true
}

type ocoRecord struct {
	Reason     string
	Group      string
	Instrument string
	Winner     VirtualLotID
	AtMS       int64
}

// nextOCOSeq returns the admission sequence of a new group member: one above
// every sequence already in the book, so it orders members by creation.
func nextOCOSeq(definitions []PendingDefinition) int64 {
	var top int64
	for _, d := range definitions {
		top = max(top, d.Order.OCOSeq)
	}
	return top + 1
}

func ocoEntryOpen(o *PendingOrder) bool {
	return !o.Canceled && o.Entry.State != Filled && o.Entry.State != Canceled && o.Entry.State != Expired
}

// resolvePendingOCO enforces OCO groups. (1) When a member of a group already
// has a fill, every remaining unfilled open sibling is cancelled with
// OCOFilledSiblingReason, independent of the robot. (2) Otherwise all but one
// executable member are cancelled with OCOSimultaneousReason.
// definitions is the active (quoted) part of the book and is mutated in place
// for the cancelled members. It returns the audit checkpoints to commit with
// the plan. Books without a group holding two open members return at once,
// without any store read, so plans without OCO are unchanged.
//
// Executable now is the owner's own evaluation of AdvancePendingEntries: the
// entry advanced on a copy (same policy and exclusivity) and then strictly
// evaluated (a resting venue limit is delegated, not executable). Tie-break,
// mirroring SharedOrderMgr.cancelSimultaneousOCOSiblings (lowest order ID among
// the executable): the lowest admission sequence (creation order), then the
// lot ID. One refinement the owner can see and the in-process manager cannot
// is that a member that already has a fill, a venue order or a reservation
// is never cancelled and takes precedence over unfilled members.
func (s *SharedAccount) resolvePendingOCO(ctx context.Context, definitions []PendingDefinition, quotes map[string]VisibleQuote, now int64) ([]StrategyCheckpoint, error) {
	type groupKey struct {
		strategy   StrategyID
		instrument string
		group      string
	}
	groups := make(map[groupKey][]int)
	open := make(map[groupKey]bool)
	for n := range definitions {
		o := &definitions[n].Order
		if o.OCOGroup != "" {
			k := groupKey{o.Strategy, definitions[n].Instrument.ID, o.OCOGroup}
			groups[k] = append(groups[k], n)
			open[k] = open[k] || ocoEntryOpen(o)
		}
	}
	multi := make([]groupKey, 0, len(groups))
	for k, members := range groups {
		if len(members) > 1 && open[k] {
			multi = append(multi, k)
		}
	}
	if len(multi) == 0 {
		return nil, nil
	}
	sort.Slice(multi, func(i, j int) bool {
		a, b := multi[i], multi[j]
		if a.strategy != b.strategy {
			return a.strategy < b.strategy
		}
		if a.instrument != b.instrument {
			return a.instrument < b.instrument
		}
		return a.group < b.group
	})
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	plan, err := s.store.LatestPlan(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var intents []EligibleIntent
	for _, d := range definitions {
		intents = append(intents, d.Order.Entry)
		if d.Order.Exit != nil {
			intents = append(intents, *d.Order.Exit)
		}
	}
	var records []StrategyCheckpoint
	cancel := func(n int, winner VirtualLotID, reason string) error {
		d := &definitions[n]
		o := &d.Order
		o.Canceled, o.Desired = true, 0
		o.Entry.State = Canceled
		o.Entry.FilledSteps, o.Entry.ReservedSteps = 0, 0
		o.Entry.VenueLimit = nil
		body, err := json.Marshal(ocoRecord{Reason: reason, Group: o.OCOGroup, Instrument: d.Instrument.ID, Winner: winner, AtMS: now})
		if err != nil {
			return err
		}
		records = append(records, StrategyCheckpoint{
			Strategy: o.Strategy, Name: "pending-oco:" + string(o.Lot), Payload: body,
			Events: []StrategyAcceptedEvent{{Strategy: o.Strategy, Lot: o.Lot, Kind: EntryIntent, CommandID: fmt.Sprintf("oco-%s-%d", reason, now), AtMS: now, Rejected: reason}},
		})
		log.Info("execution: OCO sibling cancelled by owner", zap.String("strategy", string(o.Strategy)), zap.String("lot", string(o.Lot)), zap.String("group", o.OCOGroup), zap.String("winner", string(winner)), zap.String("reason", reason))
		return nil
	}
	// A member is protected when it has a fill, a venue order, a reservation or
	// an exit: the rule never cancels it.
	isProtected := func(n int) bool {
		d := &definitions[n]
		lot := pendingLot(snapshot, &d.Order, d.Instrument.ID)
		live := lot.SignedSteps != 0 || d.Order.Entry.FilledSteps > 0 || d.Order.Entry.ReservedSteps > 0 || d.Order.Exit != nil
		for _, order := range snapshot.Orders {
			live = live || pendingEntryOrder(order, &d.Order, d.Instrument.ID)
		}
		return live
	}
	hasFill := func(n int) bool {
		d := &definitions[n]
		return pendingLot(snapshot, &d.Order, d.Instrument.ID).SignedSteps != 0 || d.Order.Entry.FilledSteps > 0
	}
	for _, k := range multi {
		var filled []int
		for _, n := range groups[k] {
			if hasFill(n) {
				filled = append(filled, n)
			}
		}
		if len(filled) > 0 {
			if len(filled) > 1 {
				log.Warn("execution: several OCO members have fills; keeping all", zap.String("strategy", string(k.strategy)), zap.String("instrument", k.instrument), zap.String("group", k.group))
			}
			winner := definitions[filled[0]].Order.Lot
			for _, n := range groups[k] {
				if ocoEntryOpen(&definitions[n].Order) && !isProtected(n) {
					if err := cancel(n, winner, OCOFilledSiblingReason); err != nil {
						return nil, err
					}
				}
			}
			continue
		}
		var executable, protected []int
		for _, n := range groups[k] {
			d := &definitions[n]
			if !ocoEntryOpen(&d.Order) {
				continue
			}
			q, price, err := pendingQuote(quotes, d.Instrument.ID, now)
			if err != nil {
				return nil, err
			}
			lot := pendingLot(snapshot, &d.Order, d.Instrument.ID)
			exclusive := ExclusiveOrderLot(snapshot, d.Instrument.ID, plan.Targets, intents)
			preview := d.Order
			if err := preview.AdvanceEntry(price, now, q.Bar, lot.SignedSteps, d.Policy, exclusive); err != nil {
				return nil, err
			}
			if preview.Entry.State == Filled {
				continue
			}
			strict := preview.Entry
			available, err := strict.Evaluate(price, now, q.Bar)
			if err != nil {
				return nil, err
			}
			if available == 0 {
				continue
			}
			executable = append(executable, n)
			if isProtected(n) {
				protected = append(protected, n)
			}
		}
		if len(executable) < 2 {
			continue
		}
		less := func(a, b int) bool {
			x, y := &definitions[a].Order, &definitions[b].Order
			if x.OCOSeq != y.OCOSeq {
				return x.OCOSeq < y.OCOSeq
			}
			return x.Lot < y.Lot
		}
		sort.Slice(executable, func(i, j int) bool { return less(executable[i], executable[j]) })
		sort.Slice(protected, func(i, j int) bool { return less(protected[i], protected[j]) })
		winner := executable[0]
		if len(protected) > 0 {
			winner = protected[0]
		}
		keep := make(map[int]bool, len(protected)+1)
		keep[winner] = true
		for _, n := range protected {
			keep[n] = true
		}
		for _, n := range executable {
			if !keep[n] {
				if err := cancel(n, definitions[winner].Order.Lot, OCOSimultaneousReason); err != nil {
					return nil, err
				}
			}
		}
	}
	return records, nil
}
