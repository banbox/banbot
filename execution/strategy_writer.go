package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
)

const strategyWriterCheckpoint = "strategy-writer:v1"

type StrategyWriterClaim struct {
	Strategy      StrategyID
	WriterID      string
	ExpectedEpoch uint64
}

type StrategyWriterLease struct {
	Epoch    uint64
	Revision uint64
}

type strategyWriterState struct {
	Epoch    uint64
	Sequence uint64
	Revision uint64
}

func (s *Store) strategyWriter(ctx context.Context, strategy StrategyID) (strategyWriterState, error) {
	var state strategyWriterState
	body, err := s.StrategyCheckpoint(ctx, strategy, strategyWriterCheckpoint)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err == nil {
		err = json.Unmarshal(body, &state)
	}
	return state, err
}

// ClaimStrategyWriter fences the preceding writer using an explicit CAS. A
// writer ID is one session's stable retry identity, never a reusable bot name.
func (b *SharedAccountBorrow) ClaimStrategyWriter(ctx context.Context, claim StrategyWriterClaim) (StrategyWriterLease, error) {
	var lease StrategyWriterLease
	err := b.WithState(func(s *SharedAccount) error {
		if !canonicalID(string(claim.Strategy)) || !canonicalID(claim.WriterID) {
			return errors.New("execution: invalid strategy writer identity")
		}
		ctx, done := joinedOperationContext(b.ctx, ctx)
		defer done()
		name := "strategy-writer-claim:" + claim.WriterID
		return s.store.atomically(ctx, func(ctx context.Context) error {
			body, err := s.store.StrategyCheckpoint(ctx, claim.Strategy, name)
			if err == nil {
				if err := json.Unmarshal(body, &lease); err != nil {
					return err
				}
				if lease.Epoch != claim.ExpectedEpoch+1 || claim.ExpectedEpoch == math.MaxUint64 {
					return errors.New("execution: strategy writer claim identity reused")
				}
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			state, err := s.store.strategyWriter(ctx, claim.Strategy)
			if err != nil {
				return err
			}
			if state.Epoch != claim.ExpectedEpoch || state.Epoch == math.MaxUint64 {
				return errors.New("execution: stale strategy writer claim")
			}
			state.Epoch++
			state.Sequence = 0
			lease = StrategyWriterLease{Epoch: state.Epoch, Revision: state.Revision}
			body, _ = json.Marshal(state)
			if err := s.store.SaveStrategyCheckpoint(ctx, claim.Strategy, strategyWriterCheckpoint, body); err != nil {
				return err
			}
			body, _ = json.Marshal(lease)
			return s.store.SaveStrategyCheckpoint(ctx, claim.Strategy, name, body)
		})
	})
	return lease, err
}

func (s *SharedAccount) strategyOrderingCheckpoints(ctx context.Context, updates []StrategyRebalance) ([]StrategyCheckpoint, error) {
	var checkpoints []StrategyCheckpoint
	for _, update := range updates {
		// Trusted in-process services also produce revisions (pending exits and
		// reconciliation). They are serialized by the owner, not remote writers.
		if update.StrategyEpoch == 0 && update.StrategySequence == 0 && update.ExpectedRevision == nil {
			continue
		}
		if err := s.store.requireNoPolicyCancellation(ctx, update.Strategy); err != nil {
			return nil, err
		}
		state, err := s.store.strategyWriter(ctx, update.Strategy)
		if err != nil {
			return nil, err
		}
		if update.StrategyEpoch == 0 || update.StrategyEpoch != state.Epoch || update.StrategySequence <= state.Sequence {
			return nil, errors.New("execution: stale strategy writer epoch or sequence")
		}
		if update.ExpectedRevision != nil && *update.ExpectedRevision != state.Revision {
			return nil, errors.New("execution: strategy target revision conflict")
		}
		if state.Revision == math.MaxUint64 {
			return nil, errors.New("execution: strategy target revision exhausted")
		}
		state.Sequence = update.StrategySequence
		state.Revision++
		body, _ := json.Marshal(state)
		checkpoints = append(checkpoints, StrategyCheckpoint{Strategy: update.Strategy, Name: strategyWriterCheckpoint, Payload: body})
	}
	return checkpoints, nil
}
