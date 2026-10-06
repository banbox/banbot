package backtest

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
)

type bookAcceptance struct {
	hash    [32]byte
	receipt execution.PolicyReceipt
}
type bookFillFact struct {
	sid              int32
	quantity         float64
	cursor, sequence uint64
	at               int64
}

func (b *Book) recordFirstFill(sid int32, qty float64, at int64) {
	old := b.quantities[sid]
	if qty == 0 {
		delete(b.firstFill, sid)
	} else if old == 0 || old > 0 && qty < 0 || old < 0 && qty > 0 {
		b.firstFill[sid] = at
	}
}

// ExecuteAllocation keeps absolute asset units independent of changing NAV and
// prices. Omitted Patch assets retain their actual absolute quantities.
func (b *Book) ExecuteAllocation(p *factor.PortfolioTarget, quotes map[int32]Quote, now int64, fee, slip float64) error {
	if p == nil || fee < 0 || slip < 0 || slip >= 1 || !finite(fee) || !finite(slip) {
		return errors.New("backtest: invalid allocation target/costs")
	}
	spec := p.Spec()
	if now < spec.ExecutableAt || now >= spec.ExpireAt {
		return errors.New("backtest: allocation outside execution window")
	}
	previous := b.previousAllocation
	if previous == nil && b.previous != nil {
		var err error
		previous, err = factor.PortfolioTargetFromWeights(b.previous)
		if err != nil {
			return err
		}
	}
	effective, err := p.EffectiveAllocations(previous)
	if err != nil {
		return err
	}
	targets := effective
	if spec.Mode == factor.Patch {
		targets = p.Allocations()
	}
	quantities := map[int32]float64{}
	for sid, a := range targets {
		q, ok := quotes[sid]
		if !ok || q.AtMS <= spec.DecisionTime || q.AtMS < spec.ExecutableAt || q.AtMS > now || q.AvailableAt > now || q.AvailableAt < q.AtMS || q.Price <= 0 || !finite(q.Price) {
			return errors.New("backtest: allocation requires a strictly later observable price")
		}
		value, err := strconv.ParseFloat(a.Value, 64)
		if err != nil || !finite(value) {
			return errors.New("backtest: unrepresentable allocation quantity")
		}
		qty := value
		switch a.Basis {
		case factor.NAVFraction:
			qty = value * spec.Budget.NAV / q.Price
		case factor.AbsoluteQuantity:
		default:
			return errors.New("backtest: unsupported allocation basis")
		}
		if !finite(qty) {
			return errors.New("backtest: unrepresentable executable quantity")
		}
		quantities[sid] = qty
	}
	next, err := factor.NewPortfolioTarget(spec, effective)
	if err != nil {
		return err
	}
	sids := make([]int32, 0, len(quantities))
	for sid := range quantities {
		sids = append(sids, sid)
	}
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	for _, sid := range sids {
		qty := quantities[sid]
		q := quotes[sid]
		delta := qty - b.quantities[sid]
		if delta != 0 {
			b.fillFacts = append(b.fillFacts, bookFillFact{sid: sid, quantity: delta, cursor: b.ledgerCursor + 1, sequence: spec.PlanSequence, at: q.AtMS})
		}
		price := q.Price
		if delta > 0 {
			price *= 1 + slip
		} else if delta < 0 {
			price *= 1 - slip
		}
		cost := math.Abs(delta*price) * fee
		b.cash -= delta*price + cost
		b.fees += cost
		b.slippage += math.Abs(delta * (price - q.Price))
		b.turnover += math.Abs(delta * q.Price)
		b.recordFirstFill(sid, qty, q.AtMS)
		b.quantities[sid] = qty
		b.marks[sid] = q
	}
	b.previousAllocation = next
	b.ledgerCursor++
	return nil
}

func (b *Book) PolicyEvidence(now int64) (factor.PortfolioEvidence, error) {
	e := factor.PortfolioEvidence{Positions: map[int32]factor.PositionEvidence{}, Marks: map[int32]string{}, StateVersion: b.stateVersion, LedgerCursor: b.ledgerCursor, State: append(json.RawMessage(nil), b.policyState...)}
	for sid, qty := range b.quantities {
		e.Positions[sid] = factor.PositionEvidence{Quantity: strconv.FormatFloat(qty, 'f', -1, 64), PendingQuantity: "0", FirstFillTime: b.firstFill[sid]}
	}
	for sid, q := range b.marks {
		if q.AvailableAt > now || q.AtMS > now {
			return e, errors.New("backtest: unavailable policy mark")
		}
		e.Marks[sid] = strconv.FormatFloat(q.Price, 'f', -1, 64)
	}
	for _, fill := range b.fillFacts {
		if fill.cursor <= b.policyLedgerCursor {
			continue
		}
		p := e.Positions[fill.sid]
		p.FillEvents = append(p.FillEvents, factor.PositionFillEvidence{Quantity: strconv.FormatFloat(fill.quantity, 'f', -1, 64), LedgerCursor: fill.cursor, PlanSequence: fill.sequence, AtMS: fill.at})
		e.Positions[fill.sid] = p
	}
	if b.previousAllocation != nil {
		e.PlanSequence = b.previousAllocation.Spec().PlanSequence
		e.Previous, _ = factor.NewPortfolioTarget(b.previousAllocation.Spec(), b.previousAllocation.Allocations())
	} else if b.previous != nil {
		e.PlanSequence = b.previous.Spec().PlanSequence
	}
	e.PlanSequence = max(e.PlanSequence, b.policySequence)
	return e, nil
}

func (b *Book) RestorePolicy() (json.RawMessage, uint64, uint64) {
	seq := uint64(0)
	if b.previousAllocation != nil {
		seq = b.previousAllocation.Spec().PlanSequence
	}
	seq = max(seq, b.policySequence)
	return append(json.RawMessage(nil), b.policyState...), b.stateVersion, seq
}

// AcceptProposal is the local simulation of the account owner's atomic state
// transaction. Invalid quotes and rejected evidence never advance exit steps.
func (b *Book) AcceptProposal(proposal factor.PortfolioProposal, version, cursor uint64, quotes map[int32]Quote, now int64, fee, slip float64) (execution.PolicyReceipt, error) {
	id := proposal.AcceptanceID
	if proposal.Target != nil {
		id = proposal.Target.ID()
	}
	if id == "" || !json.Valid(proposal.NextState) || len(proposal.NextState) > factor.MaxPortfolioStateBytes {
		return execution.PolicyReceipt{}, errors.New("backtest: invalid policy proposal")
	}
	body, err := json.Marshal(struct {
		Proposal        factor.PortfolioProposal
		Version, Cursor uint64
	}{proposal, version, cursor})
	if err != nil {
		return execution.PolicyReceipt{}, err
	}
	hash := sha256.Sum256(body)
	if accepted, ok := b.acceptedProposals[id]; ok {
		if accepted.hash != hash {
			return execution.PolicyReceipt{}, errors.New("backtest: proposal identity reused with different checkpoint")
		}
		return cloneBookReceipt(accepted.receipt), nil
	}
	if version != b.stateVersion || cursor != b.ledgerCursor {
		return execution.PolicyReceipt{}, execution.ErrPolicyEvidenceChanged
	}
	sequence := b.policySequence
	if b.previousAllocation != nil {
		sequence = max(sequence, b.previousAllocation.Spec().PlanSequence)
	}
	if proposal.PlanSequence > 0 && proposal.PlanSequence <= sequence {
		return execution.PolicyReceipt{}, errors.New("backtest: stale policy target sequence")
	}
	if proposal.Target == nil && proposal.ExpireAt > 0 && (now < proposal.DecisionTime || now >= proposal.ExpireAt) {
		return execution.PolicyReceipt{}, errors.New("backtest: state-only proposal outside acceptance window")
	}
	if proposal.Target != nil {
		if err := b.ExecuteAllocation(proposal.Target, quotes, now, fee, slip); err != nil {
			return execution.PolicyReceipt{}, err
		}
	}
	b.policyState = append(json.RawMessage(nil), proposal.NextState...)
	b.stateVersion++
	b.policySequence = max(b.policySequence, proposal.PlanSequence)
	b.policyLedgerCursor = cursor
	remaining := b.fillFacts[:0]
	for _, fill := range b.fillFacts {
		if fill.cursor > cursor {
			remaining = append(remaining, fill)
		}
	}
	b.fillFacts = remaining
	strategy := execution.StrategyID("")
	if proposal.Target != nil {
		strategy = execution.StrategyID(proposal.Target.Spec().StrategyID)
	}
	receipt := execution.PolicyReceipt{Accepted: true, PlanID: id, Versions: map[execution.StrategyID]uint64{strategy: b.stateVersion}}
	b.acceptedProposals[id] = bookAcceptance{hash: hash, receipt: cloneBookReceipt(receipt)}
	return receipt, nil
}
func cloneBookReceipt(receipt execution.PolicyReceipt) execution.PolicyReceipt {
	versions := map[execution.StrategyID]uint64{}
	for id, v := range receipt.Versions {
		versions[id] = v
	}
	receipt.Versions = versions
	return receipt
}
