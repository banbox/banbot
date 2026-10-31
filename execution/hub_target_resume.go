package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const hubTargetState = "hub-target-state:v1"

type HubTargetResume struct {
	Strategy     StrategyID
	ID           string
	ProposalHash string
}

type HubTargetRecovery struct {
	Found bool
	State json.RawMessage `json:",omitempty"`
}

func hubTargetCheckpoint(strategy StrategyID, name string, body, state []byte) StrategyCheckpoint {
	checkpoint := StrategyCheckpoint{Strategy: strategy, Name: name, Payload: body}
	if len(state) > 0 {
		checkpoint.Records = []StrategyCheckpoint{{Strategy: strategy, Name: hubTargetState, Payload: state}}
	}
	return checkpoint
}

// ResumeStrategyTarget restores only a previously accepted immutable command.
// The latest source checkpoint is returned even when this command is new, so a
// restarted robot can build Patch/Full membership without guessing from fills.
func (h *Hub) ResumeStrategyTarget(ctx context.Context, key AccountKey, request HubTargetResume) (HubTargetRecovery, error) {
	var result HubTargetRecovery
	if !canonicalID(string(request.Strategy)) || !canonicalID(request.ID) || !validProposalHash(request.ProposalHash) {
		return result, errors.New("execution: invalid hub target resume identity")
	}
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.WithState(func(service *SharedAccount) error {
			ownerCtx, cancel := joinedOperationContext(account.Context(), ctx)
			defer cancel()
			name := "hub-target:" + request.ID
			stored, err := service.store.StrategyCheckpoint(ownerCtx, request.Strategy, name)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil {
				var revision hubTargetRevision
				if err := json.Unmarshal(stored, &revision); err != nil {
					return err
				}
				if revision.Request.Strategy != request.Strategy || revision.Request.PlanID != request.ID || revision.Request.ProposalHash != request.ProposalHash {
					return errors.New("execution: hub target resume proposal mismatch")
				}
				result.Found = true
				if err := service.store.requireNoAccountPolicyCancellation(ownerCtx, ""); err != nil {
					return err
				}
				checkpoint := hubTargetCheckpoint(request.Strategy, name, stored, revision.Request.ProposalState)
				prepared, err := service.PrepareStrategiesWithCheckpoints([]StrategyRebalance{revision.Request}, ownerCtx, []StrategyCheckpoint{checkpoint})
				if err != nil {
					return err
				}
				if err := service.SendPrepared(prepared, time.Now().UnixMilli(), ownerCtx); err != nil {
					return err
				}
			}
			result.State, err = service.store.StrategyCheckpoint(ownerCtx, request.Strategy, hubTargetState)
			if errors.Is(err, sql.ErrNoRows) {
				result.State = nil
				return nil
			}
			return err
		})
	})
	return result, err
}
