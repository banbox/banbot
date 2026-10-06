package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
)

// PolicySink atomically admits the complete proposal through the account owner.
// A receipt remains accepted even when sending the admitted plan fails.
type PolicySink interface {
	PolicyEvidence(context.Context, int64) (factor.PortfolioEvidence, error)
	AcceptProposal(context.Context, factor.PortfolioProposal, uint64, uint64, map[int32]backtest.Quote, int64) (execution.PolicyReceipt, error)
}

type AllocationOutput interface {
	DecisionAllocation(factor.Frame, *factor.PortfolioTarget, []factor.Diagnostic) error
	AllocationAccepted(*factor.PortfolioTarget, backtest.State, int64) error
}

type policyRun struct {
	policy   factor.PortfolioPolicy
	state    json.RawMessage
	version  uint64
	previous *factor.PortfolioTarget
	sequence uint64
}
type pendingProposal struct {
	proposal        factor.PortfolioProposal
	version, cursor uint64
	frame           factor.Frame
	universe        factor.Universe
	ideal           *factor.TargetPortfolio
	spec            factor.PortfolioSpec
	grid            int64
	noop            bool
	riskOnly        bool
}

func newPolicyRun(c Config) (*policyRun, error) {
	if c.Manifest.Portfolio.Policy == "" {
		return nil, nil
	}
	config, err := c.Manifest.Portfolio.PolicyConfig()
	if err != nil {
		return nil, err
	}
	policy, err := NewPortfolioPolicy(config)
	if err != nil {
		return nil, err
	}
	return &policyRun{policy: policy}, nil
}

func policyEvidence(ctx context.Context, sink Sink, book *backtest.Book, now int64) (factor.PortfolioEvidence, error) {
	if source, ok := sink.(PolicySink); ok {
		return source.PolicyEvidence(ctx, now)
	}
	if book != nil {
		return book.PolicyEvidence(now)
	}
	return factor.PortfolioEvidence{}, errors.New("runner: portfolio policy requires an explicit position evidence source")
}

func (p *policyRun) propose(ctx context.Context, c Config, frame factor.Frame, universe factor.Universe, ideal *factor.TargetPortfolio, spec factor.PortfolioSpec, grid int64, evidence factor.PortfolioEvidence, quotes map[int32]backtest.Quote, riskOnly bool) (*pendingProposal, error) {
	if evidence.StateVersion != p.version {
		p.state = append(json.RawMessage(nil), evidence.State...)
		p.version = evidence.StateVersion
		p.previous = evidence.Previous
		p.sequence = evidence.PlanSequence
	}
	marks := map[int32]float64{}
	for sid, q := range quotes {
		if q.Price > 0 {
			marks[sid] = q.Price
		}
	}
	for sid, value := range evidence.Marks {
		mark, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("runner: invalid policy mark SID %d: %w", sid, err)
		}
		marks[sid] = mark
	}
	names := maps.Clone(c.Snapshot.SIDMap)
	if names == nil {
		names = map[int32]string{}
	}
	for sid, i := range c.Execution.Instruments {
		// Configuration uses data symbols; execution IDs may be different.
		if names[sid] == "" {
			names[sid] = i.ID
		}
	}
	// Mapping identity is independent of ordinary Universe membership revisions.
	mappingVersion := c.PolicySIDMappingVersion
	if mappingVersion == "" {
		mappingVersion = "sid-v1"
	}
	context := factor.PortfolioContext{Frame: factor.CloneFrame(frame), Universe: factor.CloneUniverse(universe), Ideal: ideal, Spec: spec, GridTime: grid, BarMillis: c.DecisionInterval, ScoreName: "score", Positions: factor.ClonePositionEvidence(evidence.Positions), Marks: marks, AssetNames: names, SIDMappingVersion: mappingVersion, StateVersion: evidence.StateVersion, LedgerCursor: evidence.LedgerCursor, Previous: p.previous}
	if c.PortfolioBuilder != nil || c.Manifest.Portfolio.Builder != "" {
		context.PreserveIdealWeights = c.Manifest.Portfolio.Allocation == nil
	} else {
		context.Ideal = nil
	}
	if c.PolicyContext != nil {
		if err := c.PolicyContext(ctx, &context); err != nil {
			return nil, err
		}
	}
	context.RiskOnly = riskOnly
	if riskOnly {
		context.Ideal = nil
		context.Frame.Values = map[string]map[int32]factor.Numeric{}
	}
	proposal, err := p.policy.Propose(context, append(json.RawMessage(nil), p.state...))
	if err != nil {
		return nil, err
	}
	proposal.PlanSequence = spec.PlanSequence
	proposal.DecisionTime = spec.DecisionTime
	proposal.ExpireAt = spec.ExpireAt
	if len(proposal.NextState) > factor.MaxPortfolioStateBytes || !json.Valid(proposal.NextState) {
		return nil, errors.New("runner: policy returned invalid or oversized checkpoint")
	}
	noop := proposal.Target == nil && proposal.AcceptanceID == "" && bytes.Equal(proposal.NextState, p.state)
	if !noop && proposal.Target == nil && proposal.AcceptanceID == "" {
		return nil, errors.New("runner: state-only proposal requires acceptance identity")
	}
	if proposal.Target != nil {
		got := proposal.Target.Spec()
		if got.StrategyID != spec.StrategyID || got.AccountID != spec.AccountID || got.PlanSequence != spec.PlanSequence || got.PlanHash != spec.PlanHash || got.SnapshotID != spec.SnapshotID || got.Budget != spec.Budget || got.ExecutableAt != spec.ExecutableAt || got.ExpireAt != spec.ExpireAt {
			return nil, errors.New("runner: policy changed frozen target identity")
		}
	}
	return &pendingProposal{proposal: proposal, version: evidence.StateVersion, cursor: evidence.LedgerCursor, frame: factor.CloneFrame(frame), universe: factor.CloneUniverse(universe), ideal: ideal, spec: spec, grid: grid, noop: noop, riskOnly: riskOnly}, nil
}

type PolicyReconciler interface {
	CancelIncreasingPending(context.Context, []int32, int64) error
}

func (p *policyRun) refresh(ctx context.Context, c Config, pending *pendingProposal, sink Sink, book *backtest.Book, quotes map[int32]backtest.Quote, now int64) (*pendingProposal, error) {
	if len(pending.proposal.ReconcileSIDs) > 0 {
		if reconciler, ok := sink.(PolicyReconciler); ok {
			if err := reconciler.CancelIncreasingPending(ctx, pending.proposal.ReconcileSIDs, now); err != nil {
				return nil, err
			}
		}
	}
	evidence, err := policyEvidence(ctx, sink, book, now)
	if err != nil {
		return nil, err
	}
	if evidence.StateVersion == pending.version && evidence.LedgerCursor == pending.cursor && len(pending.proposal.ReconcileSIDs) == 0 {
		return pending, nil
	}
	// Recompute with the same visible factor frame and the current execution facts.
	// NAV and identity stay frozen for this proposal; changing market facts are
	// checked once again by the serialized owner during admission.
	return p.propose(ctx, c, pending.frame, pending.universe, pending.ideal, pending.spec, pending.grid, evidence, quotes, pending.riskOnly)
}

func policyMonitoringFrame(planHash string, grid, now int64, evidence factor.PortfolioEvidence) (factor.Frame, error) {
	id, err := factor.ContentHash(struct {
		Kind                       string
		GridTime, DecisionTime     int64
		StateVersion, LedgerCursor uint64
	}{"portfolio-risk-monitor-v1", grid, now, evidence.StateVersion, evidence.LedgerCursor})
	return factor.Frame{GridTime: grid, DecisionTime: now, SnapshotID: id, PlanHash: planHash, Values: map[string]map[int32]factor.Numeric{}}, err
}

func (p *policyRun) accepted(pending *pendingProposal) error {
	var next *factor.PortfolioTarget
	if target := pending.proposal.Target; target != nil {
		effective, err := target.EffectiveAllocations(p.previous)
		if err != nil {
			return err
		}
		next, err = factor.NewPortfolioTarget(target.Spec(), effective)
		if err != nil {
			return err
		}
	}
	p.state = append(json.RawMessage(nil), pending.proposal.NextState...)
	p.version = pending.version + 1
	if next != nil {
		p.previous = next
	}
	p.sequence = max(p.sequence, pending.proposal.PlanSequence)
	return nil
}

func proposalReady(p *pendingProposal, previous *factor.PortfolioTarget, quotes map[int32]backtest.Quote, now int64, declared []int32) (bool, error) {
	if p.noop {
		return false, nil
	}
	if p.proposal.Target == nil {
		return true, nil
	}
	sp := p.proposal.Target.Spec()
	if now < sp.ExecutableAt {
		return false, nil
	}
	allocations, err := p.proposal.Target.EffectiveAllocations(previous)
	if err != nil {
		return false, err
	}
	if sp.Mode == factor.Patch {
		allocations = p.proposal.Target.Allocations()
	}
	omitted := FullZeroOmissions(p.proposal.Target, previous, declared)
	for sid := range allocations {
		if omitted[sid] {
			continue
		}
		q, ok := quotes[sid]
		if !ok || q.AtMS < sp.ExecutableAt || q.AvailableAt > now {
			return false, nil
		}
	}
	return true, nil
}

func emitAllocationDecision(out Output, frame factor.Frame, target *factor.PortfolioTarget, diagnostics []factor.Diagnostic) error {
	if out == nil {
		return nil
	}
	if consumer, ok := out.(AllocationOutput); ok {
		return consumer.DecisionAllocation(factor.CloneFrame(frame), target, diagnostics)
	}
	var weight *factor.TargetPortfolio
	var err error
	if target != nil {
		weight, err = target.AsWeightPortfolio()
		if err != nil {
			return errors.New("runner: output does not support quantity allocation targets")
		}
	}
	return out.Decision(factor.CloneFrame(frame), weight, diagnostics)
}
func emitAllocationAccepted(out Output, target *factor.PortfolioTarget, state backtest.State, now int64) error {
	if out == nil || target == nil {
		return nil
	}
	if consumer, ok := out.(AllocationOutput); ok {
		return consumer.AllocationAccepted(target, state, now)
	}
	weight, err := target.AsWeightPortfolio()
	if err != nil {
		return errors.New("runner: output does not support quantity allocation targets")
	}
	return emitTargetAccepted(out, weight, state, now)
}

// Valuation weights are for research reports; execution consumes allocations.
func allocationWeights(target *factor.PortfolioTarget, quotes map[int32]backtest.Quote) map[int32]float64 {
	if target == nil {
		return nil
	}
	weights := map[int32]float64{}
	for sid, a := range target.Allocations() {
		value, _ := strconv.ParseFloat(a.Value, 64)
		if a.Basis == factor.AbsoluteQuantity {
			value = value * quotes[sid].Price / target.Spec().Budget.NAV
		}
		weights[sid] = value
	}
	return weights
}
