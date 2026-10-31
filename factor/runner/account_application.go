package runner

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
)

// AccountQueries exposes committed execution facts without borrowing an owner,
// opening its store, or installing process-local quote/risk callbacks.
type AccountQueries interface {
	Snapshot(context.Context) (execution.AccountSnapshot, error)
	AccountRisk(context.Context, int64) (execution.PortfolioRisk, error)
	StrategyTotals(context.Context, execution.StrategyID) (execution.StrategyTotals, error)
	PolicyEvidenceContext(context.Context, execution.StrategyID) (execution.PolicyEvidence, error)
	TerminalSettled(context.Context, execution.Instrument) (bool, error)
	OptionSettled(context.Context, execution.Instrument) (bool, error)
}

type AccountCommands interface {
	RebalanceStrategyContext(context.Context, execution.StrategyRebalance, int64) error
	AcceptPolicyBatchContext(context.Context, execution.PolicyAcceptance, int64) (execution.PolicyReceipt, error)
	ResumePolicyContext(context.Context, execution.StrategyID, string, string, int64) (execution.PolicyReceipt, bool, error)
	CancelPolicyEntriesContext(context.Context, execution.StrategyID, []string, int64) error
}

type AccountApplication interface {
	AccountQueries
	AccountCommands
}

type TargetRecoveryApplication interface {
	ResumeStrategyTargetContext(context.Context, execution.StrategyID, string, string) (execution.HubTargetRecovery, error)
}

type accountTargetCheckpoint struct {
	Version int
	Spec    factor.PortfolioSpec
	Targets map[int32]float64
}

func (s *AccountSink) restoreTargetState(body json.RawMessage) error {
	if len(body) == 0 {
		return nil
	}
	var checkpoint accountTargetCheckpoint
	if err := json.Unmarshal(body, &checkpoint); err != nil {
		return err
	}
	if checkpoint.Version != 1 || checkpoint.Spec.AccountID != s.AccountID || checkpoint.Spec.StrategyID != s.StrategyID || checkpoint.Spec.Budget.Currency != s.Currency {
		return errors.New("runner: invalid execution target source checkpoint")
	}
	var err error
	s.previous, err = factor.NewTargetPortfolio(checkpoint.Spec, checkpoint.Targets)
	return err
}

var _ AccountApplication = (*execution.SharedAccountBorrow)(nil)

func (s *AccountSink) application() AccountApplication {
	if s.Application != nil {
		return s.Application
	}
	return s.Account
}

func (s *AccountSink) validateApplication(ctx context.Context) error {
	if s.Application == nil && s.Account == nil {
		return errors.New("runner: execution application is required")
	}
	if s.Application != nil {
		if s.Account != nil || s.Paper != nil || s.VisibleQuote == nil || s.Clock == nil {
			return errors.New("runner: remote execution forbids a local sender or paper adapter and requires owner quotes")
		}
		evidence, err := s.Application.PolicyEvidenceContext(ctx, execution.StrategyID(s.StrategyID))
		if err != nil {
			return err
		}
		if _, ok := evidence.Snapshot.SyntheticStrategyCash[execution.StrategyID(s.StrategyID)]; !ok {
			return errors.New("runner: execution contributor is not registered")
		}
	}
	return nil
}
