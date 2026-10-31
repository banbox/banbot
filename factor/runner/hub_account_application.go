package runner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/banbox/banbot/execution"
)

// HubAccountApplication owns one robot session's writer lease, not an account
// sender. WriterID must be unique to this session; reconnects reuse this object.
type HubAccountApplication struct {
	client   *execution.HubClient
	account  execution.AccountKey
	strategy execution.StrategyID
	epoch    uint64
	mu       sync.Mutex
	sequence uint64
	commands map[string]uint64
}

var _ AccountApplication = (*HubAccountApplication)(nil)

func (*HubAccountApplication) ExecutionOwnsFunding() bool { return true }

func NewHubAccountApplication(ctx context.Context, client *execution.HubClient, account execution.AccountKey, claim execution.StrategyWriterClaim) (*HubAccountApplication, error) {
	if client == nil {
		return nil, errors.New("runner: hub client is required")
	}
	lease, err := client.ClaimStrategyWriter(ctx, account, claim)
	if err != nil {
		return nil, err
	}
	return &HubAccountApplication{client: client, account: account, strategy: claim.Strategy, epoch: lease.Epoch, commands: map[string]uint64{}}, nil
}

// NewHubAccountSink binds a strategy to an already registered hub contributor.
// Account credentials, risk registration and data/private streams stay at hub.
func NewHubAccountSink(ctx context.Context, client *execution.HubClient, account execution.AccountKey, claim execution.StrategyWriterClaim, config Config) (*AccountSink, error) {
	if account.Account != config.AccountID || account.SettlementDomain != config.Manifest.Currency || string(claim.Strategy) != config.StrategyID || len(config.Execution.Instruments) == 0 {
		return nil, errors.New("runner: hub sink account, strategy, currency or instruments mismatch")
	}
	config, err := CloneConfig(config)
	if err != nil {
		return nil, err
	}
	for _, instrument := range config.Execution.Instruments {
		if err := instrument.Validate(); err != nil {
			return nil, err
		}
		if instrument.SettlementCurrency != account.SettlementDomain {
			return nil, errors.New("runner: hub sink instrument settlement mismatch")
		}
	}
	application, err := NewHubAccountApplication(ctx, client, account, claim)
	if err != nil {
		return nil, err
	}
	sink := &AccountSink{Application: application, AccountID: config.AccountID, StrategyID: config.StrategyID, Currency: config.Manifest.Currency, Instruments: config.Execution.Instruments, PolicySIDMap: config.Snapshot.SIDMap, AuthoritativeFunding: true, QuoteTTLMS: config.DecisionInterval + config.ExpiryMS, Clock: func() int64 { return time.Now().UnixMilli() }}
	sink.VisibleQuote = func(ctx context.Context, id string, _ int64) (execution.VisibleQuote, error) {
		return application.client.VisibleQuote(ctx, application.account, id)
	}
	if err := sink.validateApplication(ctx); err != nil {
		return nil, err
	}
	return sink, nil
}

func (a *HubAccountApplication) commandSequence(id string) (uint64, error) {
	if sequence, ok := a.commands[id]; ok {
		return sequence, nil
	}
	if a.sequence == math.MaxUint64 {
		return 0, errors.New("runner: hub writer sequence exhausted")
	}
	a.sequence++
	a.commands[id] = a.sequence
	return a.sequence, nil
}

func (a *HubAccountApplication) Snapshot(ctx context.Context) (execution.AccountSnapshot, error) {
	return a.client.Snapshot(ctx, a.account)
}

func (a *HubAccountApplication) AccountRisk(ctx context.Context, _ int64) (execution.PortfolioRisk, error) {
	return a.client.AccountRisk(ctx, a.account)
}

func (a *HubAccountApplication) StrategyTotals(ctx context.Context, strategy execution.StrategyID) (execution.StrategyTotals, error) {
	if strategy != a.strategy {
		return execution.StrategyTotals{}, errors.New("runner: hub sink contributor mismatch")
	}
	return a.client.StrategyTotals(ctx, a.account, strategy)
}

func (a *HubAccountApplication) PolicyEvidenceContext(ctx context.Context, strategy execution.StrategyID) (execution.PolicyEvidence, error) {
	if strategy != a.strategy {
		return execution.PolicyEvidence{}, errors.New("runner: hub sink contributor mismatch")
	}
	return a.client.PolicyEvidence(ctx, a.account, strategy)
}

func (a *HubAccountApplication) TerminalSettled(ctx context.Context, instrument execution.Instrument) (bool, error) {
	return a.client.InstrumentSettled(ctx, a.account, instrument, false)
}

func (a *HubAccountApplication) OptionSettled(ctx context.Context, instrument execution.Instrument) (bool, error) {
	return a.client.InstrumentSettled(ctx, a.account, instrument, true)
}

func (a *HubAccountApplication) RebalanceStrategyContext(ctx context.Context, request execution.StrategyRebalance, _ int64) error {
	if request.Strategy != a.strategy {
		return errors.New("runner: hub sink contributor mismatch")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	sequence, err := a.commandSequence("target:" + request.PlanID)
	if err != nil {
		return err
	}
	request.StrategyEpoch, request.StrategySequence = a.epoch, sequence
	return a.client.RebalanceStrategy(ctx, a.account, request, 0)
}

func (a *HubAccountApplication) AcceptPolicyBatchContext(ctx context.Context, request execution.PolicyAcceptance, _ int64) (execution.PolicyReceipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	sequence, err := a.commandSequence("policy:" + request.ID)
	if err != nil {
		return execution.PolicyReceipt{}, err
	}
	request.Updates = append([]execution.StrategyRebalance(nil), request.Updates...)
	request.Checkpoints = append([]execution.PolicyCheckpoint(nil), request.Checkpoints...)
	for n := range request.Updates {
		if request.Updates[n].Strategy != a.strategy {
			return execution.PolicyReceipt{}, errors.New("runner: hub sink contributor mismatch")
		}
		request.Updates[n].StrategyEpoch, request.Updates[n].StrategySequence = a.epoch, sequence
	}
	for n := range request.Checkpoints {
		if request.Checkpoints[n].Strategy != a.strategy {
			return execution.PolicyReceipt{}, errors.New("runner: hub sink contributor mismatch")
		}
		request.Checkpoints[n].StrategyEpoch, request.Checkpoints[n].StrategySequence = a.epoch, sequence
	}
	return a.client.AcceptPolicyBatch(ctx, a.account, request)
}

func (a *HubAccountApplication) ResumePolicyContext(ctx context.Context, strategy execution.StrategyID, id, proposalHash string, _ int64) (execution.PolicyReceipt, bool, error) {
	if strategy != a.strategy {
		return execution.PolicyReceipt{}, false, errors.New("runner: hub sink contributor mismatch")
	}
	return a.client.ResumePolicy(ctx, a.account, execution.HubPolicyResume{Strategy: strategy, ID: id, ProposalHash: proposalHash})
}

func (a *HubAccountApplication) ResumeStrategyTargetContext(ctx context.Context, strategy execution.StrategyID, id, proposalHash string) (execution.HubTargetRecovery, error) {
	if strategy != a.strategy {
		return execution.HubTargetRecovery{}, errors.New("runner: hub sink contributor mismatch")
	}
	return a.client.ResumeStrategyTarget(ctx, a.account, execution.HubTargetResume{Strategy: strategy, ID: id, ProposalHash: proposalHash})
}

func (a *HubAccountApplication) CancelPolicyEntriesContext(ctx context.Context, strategy execution.StrategyID, instruments []string, now int64) error {
	if strategy != a.strategy {
		return errors.New("runner: hub sink contributor mismatch")
	}
	instruments = append([]string(nil), instruments...)
	sort.Strings(instruments)
	body, err := json.Marshal(struct {
		Instruments []string
		AtMS        int64
	}{instruments, now})
	if err != nil {
		return err
	}
	id := fmt.Sprintf("factor-cancel:%x", sha256.Sum256(body))
	a.mu.Lock()
	defer a.mu.Unlock()
	sequence, err := a.commandSequence(id)
	if err != nil {
		return err
	}
	return a.client.CancelPolicyEntries(ctx, a.account, execution.HubPolicyEntriesCancel{ID: id, Strategy: strategy, StrategyEpoch: a.epoch, StrategySequence: sequence, Instruments: instruments})
}
