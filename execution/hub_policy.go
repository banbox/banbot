package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type hubPolicyRevision struct {
	Hash          [32]byte
	Request       PolicyAcceptance
	WriterRecords []StrategyCheckpoint
}

// AcceptPolicyBatch freezes owner-observed quotes with the strategy state,
// target and acceptance receipt. Retries replay that acceptance, not new quotes.
func (h *Hub) AcceptPolicyBatch(ctx context.Context, key AccountKey, request PolicyAcceptance) (PolicyReceipt, error) {
	var receipt PolicyReceipt
	// Clone nested slices as well: stripping untrusted quotes must not mutate a
	// local caller's proposal, which it may need to retry after a lost response.
	body, err := json.Marshal(request)
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return receipt, err
	}
	if !canonicalID(request.ID) || len(request.Checkpoints) == 0 {
		return receipt, errors.New("execution: invalid hub policy identity")
	}
	strategy := request.Checkpoints[0].Strategy
	for _, checkpoint := range request.Checkpoints {
		if !canonicalID(string(checkpoint.Strategy)) {
			return receipt, errors.New("execution: invalid hub policy contributor")
		}
		if checkpoint.Strategy < strategy {
			strategy = checkpoint.Strategy
		}
	}
	for n := range request.Updates {
		if request.Updates[n].StrategyEpoch == 0 || request.Updates[n].StrategySequence == 0 {
			return receipt, errors.New("execution: hub policy requires a claimed strategy writer and positive command sequence")
		}
		for m := range request.Updates[n].Requests {
			request.Updates[n].Requests[m].Quote = VisibleQuote{}
		}
	}
	body, err = json.Marshal(request)
	if err != nil {
		return receipt, err
	}
	hash := sha256.Sum256(body)
	err = h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.WithState(func(service *SharedAccount) error {
			ownerCtx, cancel := joinedOperationContext(account.Context(), ctx)
			defer cancel()
			name := "hub-policy:" + request.ID
			stored, err := service.store.StrategyCheckpoint(ownerCtx, strategy, name)
			var revision hubPolicyRevision
			var writerRecords []StrategyCheckpoint
			switch {
			case err == nil:
				if err := json.Unmarshal(stored, &revision); err != nil {
					return err
				}
				if revision.Hash != hash {
					return errors.New("execution: hub policy identity reused with a different command")
				}
			case errors.Is(err, sql.ErrNoRows):
				updates := map[StrategyID]StrategyRebalance{}
				for _, update := range request.Updates {
					updates[update.Strategy] = update
				}
				var stateUpdates []StrategyRebalance
				for _, checkpoint := range request.Checkpoints {
					if update, ok := updates[checkpoint.Strategy]; ok {
						if checkpoint.StrategyEpoch != 0 && checkpoint.StrategyEpoch != update.StrategyEpoch || checkpoint.StrategySequence != 0 && checkpoint.StrategySequence != update.StrategySequence {
							return errors.New("execution: hub policy state and target writer mismatch")
						}
						continue
					}
					if checkpoint.StrategyEpoch == 0 || checkpoint.StrategySequence == 0 {
						return errors.New("execution: hub policy state requires a claimed strategy writer and positive command sequence")
					}
					stateUpdates = append(stateUpdates, StrategyRebalance{Strategy: checkpoint.Strategy, StrategyEpoch: checkpoint.StrategyEpoch, StrategySequence: checkpoint.StrategySequence})
				}
				writerRecords, err = service.strategyOrderingCheckpoints(ownerCtx, stateUpdates)
				if err != nil {
					return err
				}
				now := time.Now().UnixMilli()
				if request.DecisionMS < 0 || request.DecisionMS > now || request.ExpiresMS <= now || request.ExpiresMS <= request.DecisionMS {
					return errors.New("execution: hub policy outside decision window")
				}
				for n := range request.Updates {
					update := &request.Updates[n]
					if update.DecisionMS < 0 || update.DecisionMS > now || update.ExpiresMS <= now || update.ExpiresMS <= update.DecisionMS {
						return errors.New("execution: hub policy target outside decision window")
					}
					for m := range update.Requests {
						input := &update.Requests[m]
						input.Quote, err = service.ReadQuote(ownerCtx, input.Instrument.ID, now)
						if err != nil {
							return err
						}
					}
					update.DecisionMS = now
				}
				revision = hubPolicyRevision{Hash: hash, Request: request, WriterRecords: writerRecords}
			default:
				return err
			}
			body, err := json.Marshal(revision)
			if err != nil {
				return err
			}
			record := StrategyCheckpoint{Strategy: strategy, Name: name, Payload: body}
			receipt, err = service.acceptPolicyWithRecords(ownerCtx, revision.Request, time.Now().UnixMilli(), append([]StrategyCheckpoint{record}, revision.WriterRecords...))
			return err
		})
	})
	return receipt, err
}

func (h *Hub) PolicyEvidence(ctx context.Context, key AccountKey, strategy StrategyID) (PolicyEvidence, error) {
	var evidence PolicyEvidence
	if !canonicalID(string(strategy)) {
		return evidence, errors.New("execution: invalid hub policy contributor")
	}
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		evidence, err = account.PolicyEvidenceContext(ctx, strategy)
		return err
	})
	return evidence, err
}

type HubPolicyResume struct {
	Strategy     StrategyID
	ID           string
	ProposalHash string
}

func (h *Hub) ResumePolicy(ctx context.Context, key AccountKey, request HubPolicyResume) (PolicyReceipt, bool, error) {
	var receipt PolicyReceipt
	var found bool
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		receipt, found, err = account.ResumePolicyContext(ctx, request.Strategy, request.ID, request.ProposalHash, time.Now().UnixMilli())
		return err
	})
	return receipt, found, err
}
