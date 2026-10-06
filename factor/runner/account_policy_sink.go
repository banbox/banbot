package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/shopspring/decimal"
)

type policySinkCheckpoint struct {
	Version            int                            `json:"version"`
	State              json.RawMessage                `json:"state"`
	Target             *factor.PortfolioTarget        `json:"target,omitempty"`
	Sequence           uint64                         `json:"sequence"`
	SIDMap             map[int32]string               `json:"sid_map,omitempty"`
	Instruments        map[int32]execution.Instrument `json:"instruments,omitempty"`
	FundingInstruments map[int32]execution.Instrument `json:"funding_instruments,omitempty"`
}
type accountAcceptedProposal struct {
	Hash [32]byte
}

const maxAccountAcceptedPolicyCache = 64

func (s *AccountSink) CancelIncreasingPending(ctx context.Context, sids []int32, now int64) error {
	ids := make([]string, 0, len(sids))
	for _, sid := range sids {
		i, ok := s.Instruments[sid]
		if !ok {
			return errors.New("runner: cancellation instrument SID missing")
		}
		ids = append(ids, i.ID)
	}
	return s.Account.CancelPolicyEntriesContext(ctx, execution.StrategyID(s.StrategyID), ids, now)
}

func (s *AccountSink) PolicyEvidence(ctx context.Context, now int64) (factor.PortfolioEvidence, error) {
	e, err := s.Account.PolicyEvidenceContext(ctx, execution.StrategyID(s.StrategyID))
	if err != nil {
		return factor.PortfolioEvidence{}, err
	}
	result := factor.PortfolioEvidence{Positions: map[int32]factor.PositionEvidence{}, Marks: map[int32]string{}, StateVersion: e.State.Version, LedgerCursor: uint64(e.Snapshot.Checkpoint)}
	byID := map[string]int32{}
	for sid, i := range s.Instruments {
		byID[i.ID] = sid
	}
	for _, lot := range e.Snapshot.Lots {
		if lot.Strategy != execution.StrategyID(s.StrategyID) {
			continue
		}
		sid, ok := byID[lot.Instrument.ID]
		if !ok {
			return result, errors.New("runner: policy position lacks execution SID")
		}
		p := result.Positions[sid]
		qty := decimal.NewFromInt(lot.SignedSteps).Mul(lot.Instrument.ContractSize).Mul(lot.Instrument.QuantityStep)
		old, _ := decimal.NewFromString(p.Quantity)
		p.Quantity = old.Add(qty).String()
		p.Quantum = lot.Instrument.QuantityStep.Mul(lot.Instrument.ContractSize).String()
		at := e.FirstFillMS[lot.ID]
		if p.FirstFillTime == 0 || at != 0 && at < p.FirstFillTime {
			p.FirstFillTime = at
		}
		result.Positions[sid] = p
	}
	for _, order := range e.Snapshot.Orders {
		sid, known := byID[order.Intent.Instrument.ID]
		for _, a := range order.Intent.Allocations {
			if a.Strategy != execution.StrategyID(s.StrategyID) {
				continue
			}
			remaining := a.Steps - order.AllocationFilled[a.ID]
			if remaining <= 0 {
				continue
			}
			if !known {
				return result, errors.New("runner: policy order lacks execution SID")
			}
			p := result.Positions[sid]
			qty := decimal.NewFromInt(remaining).Mul(order.Intent.Instrument.ContractSize).Mul(order.Intent.Instrument.QuantityStep)
			if a.Side == execution.Sell {
				qty = qty.Neg()
			}
			pending, _ := decimal.NewFromString(p.PendingQuantity)
			p.PendingQuantity = pending.Add(qty).String()
			p.Quantum = order.Intent.Instrument.QuantityStep.Mul(order.Intent.Instrument.ContractSize).String()
			p.IncreasingPending = p.IncreasingPending || a.Kind == execution.EntryIntent
			p.PendingUnknown = p.PendingUnknown || order.State == execution.OrderUnknown || order.State == execution.OrderSending || order.State == execution.OrderCancelPending
			if p.Quantity == "" {
				p.Quantity = "0"
			}
			result.Positions[sid] = p
		}
	}
	for sid, p := range result.Positions {
		if p.PendingQuantity == "" {
			p.PendingQuantity = "0"
		}
		result.Positions[sid] = p
	}
	for _, intent := range e.PendingIntents {
		remaining := intent.QuantitySteps - intent.FilledSteps - intent.ReservedSteps
		if remaining <= 0 {
			continue
		}
		sid, ok := byID[intent.Instrument]
		if !ok {
			return result, errors.New("runner: pending policy intent lacks SID")
		}
		i := s.Instruments[sid]
		qty := decimal.NewFromInt(remaining).Mul(i.ContractSize).Mul(i.QuantityStep)
		if intent.Side == execution.Sell {
			qty = qty.Neg()
		}
		p := result.Positions[sid]
		pending, _ := decimal.NewFromString(p.PendingQuantity)
		p.PendingQuantity = pending.Add(qty).String()
		p.Quantum = i.ContractSize.Mul(i.QuantityStep).String()
		p.IncreasingPending = p.IncreasingPending || intent.Kind == execution.EntryIntent
		if p.Quantity == "" {
			p.Quantity = "0"
		}
		result.Positions[sid] = p
	}
	for _, fill := range e.Fills {
		sid, ok := byID[fill.Instrument.ID]
		if !ok {
			continue
		}
		p := result.Positions[sid]
		qty := decimal.NewFromInt(fill.QuantityDelta).Mul(fill.Instrument.QuantityStep).Mul(fill.Instrument.ContractSize)
		p.FillEvents = append(p.FillEvents, factor.PositionFillEvidence{Quantity: qty.String(), LedgerCursor: fill.Cursor, PlanSequence: fill.PlanSequence, AtMS: fill.AtMS})
		result.Positions[sid] = p
	}
	risk, err := s.Account.AccountRisk(ctx, now)
	if err != nil {
		return result, err
	}
	for id, mark := range risk.Marks {
		if sid, ok := byID[id]; ok {
			result.Marks[sid] = mark.String()
		}
	}
	if e.State.Version > 0 {
		var checkpoint policySinkCheckpoint
		if err := json.Unmarshal(e.State.Payload, &checkpoint); err != nil {
			return result, err
		}
		if checkpoint.Version != 1 {
			return result, errors.New("runner: unsupported policy checkpoint version")
		}
		result.State = append(json.RawMessage(nil), checkpoint.State...)
		result.PlanSequence = checkpoint.Sequence
		if checkpoint.Target != nil {
			result.Previous, err = factor.NewPortfolioTarget(checkpoint.Target.Spec(), checkpoint.Target.Allocations())
			if err != nil {
				return result, err
			}
		}
		s.previousAllocation = checkpoint.Target
	}
	return result, nil
}

func (s *AccountSink) RestorePolicy(ctx context.Context, name string) (json.RawMessage, uint64, uint64, error) {
	if name != "" && name != execution.PolicyCheckpointName {
		return nil, 0, 0, errors.New("runner: unsupported policy checkpoint name")
	}
	e, err := s.Account.PolicyEvidenceContext(ctx, execution.StrategyID(s.StrategyID))
	if err != nil {
		return nil, 0, 0, err
	}
	if e.State.Version == 0 {
		return nil, 0, 0, nil
	}
	var checkpoint policySinkCheckpoint
	if err := json.Unmarshal(e.State.Payload, &checkpoint); err != nil {
		return nil, 0, 0, err
	}
	if checkpoint.Version != 1 {
		return nil, 0, 0, errors.New("runner: unsupported policy checkpoint version")
	}
	s.previousAllocation = checkpoint.Target
	return append(json.RawMessage(nil), checkpoint.State...), e.State.Version, checkpoint.Sequence, nil
}

func (s *AccountSink) AcceptProposal(ctx context.Context, proposal factor.PortfolioProposal, stateVersion, ledgerCursor uint64, quotes map[int32]backtest.Quote, now int64) (execution.PolicyReceipt, error) {
	if !json.Valid(proposal.NextState) || ledgerCursor > math.MaxInt64 {
		return execution.PolicyReceipt{}, errors.New("runner: invalid policy proposal evidence")
	}
	id := proposal.AcceptanceID
	if proposal.Target != nil {
		id = proposal.Target.ID()
	}
	if id == "" {
		return execution.PolicyReceipt{}, errors.New("runner: missing proposal acceptance identity")
	}
	body, err := json.Marshal(struct {
		Proposal        factor.PortfolioProposal
		Version, Cursor uint64
	}{proposal, stateVersion, ledgerCursor})
	if err != nil {
		return execution.PolicyReceipt{}, err
	}
	hash := sha256.Sum256(body)
	if previous, ok := s.acceptedPolicy[id]; ok {
		if hash != previous.Hash {
			return execution.PolicyReceipt{}, errors.New("runner: proposal identity reused with changed state")
		}
	}
	proposalHash := hex.EncodeToString(hash[:])
	if receipt, found, err := s.Account.ResumePolicyContext(ctx, execution.StrategyID(s.StrategyID), id, proposalHash, now); err != nil || found {
		return receipt, err
	}
	request := execution.PolicyAcceptance{ID: id, ExpectedLedgerCursor: int64(ledgerCursor), DecisionMS: proposal.DecisionTime, ExpiresMS: proposal.ExpireAt}
	checkpoint := policySinkCheckpoint{Version: 1, State: proposal.NextState, Target: s.previousAllocation, SIDMap: s.PolicySIDMap, Instruments: s.Instruments, FundingInstruments: s.FundingInstruments}
	if checkpoint.Target != nil {
		checkpoint.Sequence = checkpoint.Target.Spec().PlanSequence
	}
	checkpoint.Sequence = max(checkpoint.Sequence, proposal.PlanSequence)
	if proposal.Target == nil && proposal.ExpireAt > 0 && (now < proposal.DecisionTime || now >= proposal.ExpireAt) {
		return execution.PolicyReceipt{}, errors.New("runner: state-only proposal outside acceptance window")
	}
	if proposal.Target != nil {
		p := proposal.Target
		sp := p.Spec()
		if sp.AccountID != s.AccountID || sp.StrategyID != s.StrategyID || sp.Budget.Currency != s.Currency || now < sp.ExecutableAt || now >= sp.ExpireAt {
			return execution.PolicyReceipt{}, errors.New("runner: allocation/account identity or execution window mismatch")
		}
		effective, err := p.EffectiveAllocations(s.previousAllocation)
		if err != nil {
			return execution.PolicyReceipt{}, err
		}
		declared := make([]int32, 0, len(s.Instruments))
		for sid := range s.Instruments {
			declared = append(declared, sid)
		}
		omissions := FullZeroOmissions(p, s.previousAllocation, declared)
		if len(omissions) > 0 {
			ownerEvidence, err := s.Account.PolicyEvidenceContext(ctx, execution.StrategyID(s.StrategyID))
			if err != nil {
				return execution.PolicyReceipt{}, err
			}
			if ownerEvidence.Snapshot.Checkpoint != int64(ledgerCursor) || ownerEvidence.State.Version != stateVersion {
				return execution.PolicyReceipt{}, execution.ErrPolicyEvidenceChanged
			}
			var previousCheckpoint policySinkCheckpoint
			if ownerEvidence.State.Version > 0 {
				if err := json.Unmarshal(ownerEvidence.State.Payload, &previousCheckpoint); err != nil {
					return execution.PolicyReceipt{}, err
				}
			}
			for sid := range omissions {
				lotID := execution.VirtualLotID(fmt.Sprintf("factor:%d", sid))
				instrumentID := previousCheckpoint.Instruments[sid].ID
				matches := func(lot execution.VirtualLotID, instrument string) bool {
					return lot == lotID || instrumentID != "" && instrument == instrumentID
				}
				for _, lot := range ownerEvidence.Snapshot.Lots {
					if lot.Strategy == execution.StrategyID(s.StrategyID) && lot.SignedSteps != 0 && matches(lot.ID, lot.Instrument.ID) {
						return execution.PolicyReceipt{}, errors.New("runner: removed zero SID still has a position; retain execution subscriptions")
					}
				}
				for _, order := range ownerEvidence.Snapshot.Orders {
					for _, a := range order.Intent.Allocations {
						if a.Strategy == execution.StrategyID(s.StrategyID) && a.Steps > order.AllocationFilled[a.ID] && matches(a.Lot, order.Intent.Instrument.ID) {
							return execution.PolicyReceipt{}, errors.New("runner: removed zero SID still has inflight orders; retain execution subscriptions")
						}
					}
				}
				for _, intent := range ownerEvidence.PendingIntents {
					if intent.QuantitySteps > intent.FilledSteps && matches(intent.Lot, intent.Instrument) {
						return execution.PolicyReceipt{}, errors.New("runner: removed zero SID still has unsettled intent; retain execution subscriptions")
					}
				}
				delete(effective, sid)
			}
		}
		targets := effective
		if sp.Mode == factor.Patch {
			targets = p.Allocations()
		}
		update := execution.StrategyRebalance{Strategy: execution.StrategyID(s.StrategyID), PlanID: id, DecisionMS: now, ExpiresMS: sp.ExpireAt, Mode: execution.StrategyTargetsFull}
		if sp.Mode == factor.Patch {
			update.Mode = execution.StrategyTargetsPatch
		}
		sids := make([]int32, 0, len(targets))
		for sid := range targets {
			sids = append(sids, sid)
		}
		sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
		for _, sid := range sids {
			i, ok := s.Instruments[sid]
			if !ok || i.SettlementCurrency != s.Currency {
				return execution.PolicyReceipt{}, errors.New("runner: allocation instrument/budget mismatch")
			}
			if err := i.Validate(); err != nil {
				return execution.PolicyReceipt{}, err
			}
			q, ok := quotes[sid]
			if !ok || q.AtMS < sp.ExecutableAt || q.AtMS <= sp.DecisionTime || q.AvailableAt > now || q.AvailableAt < q.AtMS || q.AtMS > now || q.Price <= 0 || math.IsNaN(q.Price) || math.IsInf(q.Price, 0) {
				return execution.PolicyReceipt{}, errors.New("runner: unavailable allocation executable quote")
			}
			price := decimal.NewFromFloat(q.Price)
			bid, ask := price, price
			if q.Bid > 0 && q.Ask >= q.Bid && !math.IsInf(q.Bid, 0) && !math.IsInf(q.Ask, 0) {
				bid, ask = decimal.NewFromFloat(q.Bid), decimal.NewFromFloat(q.Ask)
			} else if s.Paper == nil && s.VisibleQuote == nil {
				return execution.PolicyReceipt{}, errors.New("runner: live allocation requires bid/ask")
			}
			visible := execution.VisibleQuote{Bid: bid, Ask: ask, AtMS: q.AtMS, ReceivedMS: q.AvailableAt, ValidUntilMS: sp.ExpireAt, Bar: q.AtMS}
			if s.VisibleQuote != nil {
				if s.Clock == nil {
					return execution.PolicyReceipt{}, errors.New("runner: live allocation needs completion clock")
				}
				visible, err = s.VisibleQuote(ctx, i.ID, now)
				if err != nil {
					return execution.PolicyReceipt{}, err
				}
				now = max(now, s.Clock())
				visible.ValidUntilMS = min(visible.ValidUntilMS, sp.ExpireAt)
				if !visible.Bid.IsPositive() || visible.Ask.LessThan(visible.Bid) || visible.AtMS < sp.ExecutableAt || visible.AtMS > visible.ReceivedMS || visible.ReceivedMS > now || visible.ValidUntilMS <= now || now >= sp.ExpireAt {
					return execution.PolicyReceipt{}, errors.New("runner: stale allocation bid/ask")
				}
				price = visible.Bid.Add(visible.Ask).Mul(decimal.RequireFromString("0.5")).Div(i.PriceTick).Floor().Mul(i.PriceTick)
				if price.LessThan(visible.Bid) || price.GreaterThan(visible.Ask) {
					return execution.PolicyReceipt{}, errors.New("runner: bid/ask has no execution tick")
				}
				update.DecisionMS = now
			}
			a := targets[sid]
			qty, err := decimal.NewFromString(a.Value)
			if err != nil {
				return execution.PolicyReceipt{}, err
			}
			switch a.Basis {
			case factor.NAVFraction:
				qty = qty.Mul(decimal.NewFromFloat(sp.Budget.NAV)).Div(price)
			case factor.AbsoluteQuantity:
			default:
				return execution.PolicyReceipt{}, errors.New("runner: unsupported allocation units")
			}
			// Values are base asset units; quantity steps are contract units.
			steps, err := execution.QuantitySteps(qty.Div(i.ContractSize), i.QuantityStep)
			if err != nil {
				return execution.PolicyReceipt{}, err
			}
			update.Requests = append(update.Requests, execution.InstrumentRebalance{Instrument: i, Targets: []execution.ExecutableTarget{{Strategy: execution.StrategyID(s.StrategyID), Lot: execution.VirtualLotID(fmt.Sprintf("factor:%d", sid)), SignedSteps: steps, SourceSequence: sp.PlanSequence}}, Quote: visible})
		}
		request.Updates = []execution.StrategyRebalance{update}
		checkpoint.Target, err = factor.NewPortfolioTarget(sp, effective)
		if err != nil {
			return execution.PolicyReceipt{}, err
		}
		checkpoint.Sequence = sp.PlanSequence
	}
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return execution.PolicyReceipt{}, err
	}
	if len(payload) > execution.MaxPolicyStateBytes {
		return execution.PolicyReceipt{}, errors.New("runner: encoded policy checkpoint exceeds bounded owner storage")
	}
	request.Checkpoints = []execution.PolicyCheckpoint{{Strategy: execution.StrategyID(s.StrategyID), ExpectedVersion: stateVersion, Payload: payload, PlanSequence: checkpoint.Sequence, ProposalHash: proposalHash}}
	receipt, err := s.Account.AcceptPolicyBatchContext(ctx, request, now)
	if receipt.Accepted {
		s.previousAllocation = checkpoint.Target
		if s.acceptedPolicy == nil {
			s.acceptedPolicy = map[string]accountAcceptedProposal{}
			s.acceptedPolicyOrder = nil
		}
		if len(s.acceptedPolicyOrder) == maxAccountAcceptedPolicyCache {
			delete(s.acceptedPolicy, s.acceptedPolicyOrder[0])
			copy(s.acceptedPolicyOrder, s.acceptedPolicyOrder[1:])
			s.acceptedPolicyOrder = s.acceptedPolicyOrder[:len(s.acceptedPolicyOrder)-1]
		}
		s.acceptedPolicy[id] = accountAcceptedProposal{Hash: hash}
		s.acceptedPolicyOrder = append(s.acceptedPolicyOrder, id)
	}
	return receipt, err
}
