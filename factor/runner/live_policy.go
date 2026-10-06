package runner

import (
	"context"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"slices"
)

func (l *Live) proposePolicyRound(ctx context.Context, frame factor.Frame, universe factor.Universe, sequence uint64, nav float64, completed, decision int64, token factor.RoundToken, diagnostics []factor.Diagnostic) error {
	evidence, err := l.sink.(PolicySink).PolicyEvidence(ctx, completed)
	if err != nil {
		return err
	}
	sequence = max(sequence, evidence.PlanSequence+1)
	ideal, diags, err := l.engine.buildPortfolio(frame, universe, sequence, nav, completed+l.c.LatencyMS, decision+l.c.ExpiryMS)
	if err != nil {
		return err
	}
	sp := l.engine.portfolioSpec(frame, universe, sequence, nav, completed+l.c.LatencyMS, decision+l.c.ExpiryMS)
	l.mu.Lock()
	quotes := copyQuotes(l.quotes)
	l.mu.Unlock()
	pending, err := l.policy.propose(ctx, l.c, frame, universe, ideal, sp, decision, evidence, quotes, false)
	if err != nil {
		return err
	}
	diagnostics = append(diagnostics, diags...)
	diagnostics = append(diagnostics, pending.proposal.Reasons...)
	if l.clock() >= sp.ExpireAt {
		return factor.ErrRoundExpired
	}
	if err = emitAllocationDecision(l.out, frame, pending.proposal.Target, diagnostics); err != nil {
		return err
	}
	if _, err = l.barrier.Context(token); err != nil {
		return err
	}
	l.mu.Lock()
	if l.stopped || l.generation != token.Generation {
		l.mu.Unlock()
		return factor.ErrRoundStale
	}
	if l.clock() >= sp.ExpireAt {
		l.mu.Unlock()
		return factor.ErrRoundExpired
	}
	l.sequence = sequence
	l.lastDecision = decision
	l.policyPending = pending
	if pending.noop {
		l.policyPending = nil
	}
	l.updatePolicyScope(evidence, pending)
	l.mu.Unlock()
	return l.process(ctx)
}

func (l *Live) updatePolicyScope(evidence factor.PortfolioEvidence, pending *pendingProposal) {
	seen := map[int32]bool{}
	for sid, position := range evidence.Positions {
		if position.Quantity != "0" && position.Quantity != "" || position.PendingQuantity != "0" && position.PendingQuantity != "" || position.PendingUnknown {
			seen[sid] = true
		}
	}
	if pending != nil && pending.proposal.Target != nil {
		for sid, a := range pending.proposal.Target.Allocations() {
			if a.Value != "0" {
				seen[sid] = true
			}
		}
	}
	ids := make([]int32, 0, len(seen))
	for sid := range seen {
		ids = append(ids, sid)
	}
	slices.Sort(ids)
	l.policyScope.Store(&ids)
}

func (l *Live) monitorPolicy(ctx context.Context, universe factor.Universe, decision int64, token factor.RoundToken) error {
	now := l.clock()
	if now >= decision+l.c.ExpiryMS {
		return factor.ErrRoundExpired
	}
	evidence, err := l.sink.(PolicySink).PolicyEvidence(ctx, now)
	if err != nil {
		return err
	}
	nav, err := l.sink.(BudgetSource).StrategyNAV(ctx, now)
	if err != nil {
		return err
	}
	l.mu.Lock()
	sequence := max(l.sequence+1, evidence.PlanSequence+1)
	quotes := copyQuotes(l.quotes)
	l.mu.Unlock()
	frame, err := policyMonitoringFrame(l.engine.plan.Hash(), decision, now, evidence)
	if err != nil {
		return err
	}
	sp := l.engine.portfolioSpec(frame, universe, sequence, nav, now+l.c.LatencyMS, decision+l.c.ExpiryMS)
	pending, err := l.policy.propose(ctx, l.c, frame, universe, nil, sp, decision, evidence, quotes, true)
	if err != nil {
		return err
	}
	if err = emitAllocationDecision(l.out, frame, pending.proposal.Target, append(pending.proposal.Reasons, factor.Diagnostic{Code: "policy-monitor-incomplete", Detail: "only expiry and hard exits checked; no ordinary ranking step"})); err != nil {
		return err
	}
	if _, err = l.barrier.Context(token); err != nil {
		return err
	}
	l.mu.Lock()
	if l.stopped || l.generation != token.Generation {
		l.mu.Unlock()
		return factor.ErrRoundStale
	}
	l.sequence = sequence
	if !pending.noop {
		l.policyPending = pending
	}
	l.updatePolicyScope(evidence, pending)
	l.mu.Unlock()
	return l.process(ctx)
}

func (l *Live) executePolicy(ctx context.Context, now int64) error {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return factor.ErrRoundStale
	}
	pending := l.policyPending
	if pending == nil {
		l.mu.Unlock()
		return nil
	}
	if now >= pending.spec.ExpireAt {
		l.policyPending = nil
		l.mu.Unlock()
		return nil
	}
	quotes := copyQuotes(l.quotes)
	l.mu.Unlock()
	refreshed, refreshErr := l.policy.refresh(ctx, l.c, pending, l.sink, nil, quotes, now)
	if refreshErr != nil {
		return refreshErr
	}
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return factor.ErrRoundStale
	}
	if l.policyPending != pending {
		l.mu.Unlock()
		return factor.ErrRoundStale
	}
	l.policyPending = refreshed
	pending = refreshed
	ready, err := proposalReady(pending, l.policy.previous, quotes, now, l.ExecutionSIDs())
	l.mu.Unlock()
	if err != nil || !ready {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	receipt, err := l.sink.(PolicySink).AcceptProposal(ctx, pending.proposal, pending.version, pending.cursor, quotes, now)
	if receipt.Accepted {
		l.mu.Lock()
		if acceptErr := l.policy.accepted(pending); acceptErr != nil {
			l.mu.Unlock()
			return acceptErr
		}
		if l.policyPending == pending {
			l.policyPending = nil
		}
		stopped := l.stopped
		l.mu.Unlock()
		if stopped {
			return factor.ErrRoundStale
		}
		if l.out != nil && pending.proposal.Target != nil {
			source, ok := l.sink.(StateSource)
			if !ok {
				return errors.New("runner: policy output requires strategy state")
			}
			state, stateErr := source.StrategyState(ctx, now)
			if stateErr != nil {
				return stateErr
			}
			if outputErr := emitAllocationAccepted(l.out, pending.proposal.Target, state, now); outputErr != nil {
				return outputErr
			}
		}
	}
	if errors.Is(err, execution.ErrPolicyEvidenceChanged) {
		return nil
	}
	if err != nil {
		return err
	}
	return receipt.SendError
}
