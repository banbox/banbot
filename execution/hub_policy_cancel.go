package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

var ErrPolicyCancellationRecoveryRequired = errors.New("execution: accepted policy cancellation requires authoritative recovery")

const policyCancellationActive = "policy-cancel-active:v1"

const policyCancellationAccount StrategyID = "execution-policy-cancellation"

type HubPolicyEntriesCancel struct {
	ID               string
	Strategy         StrategyID
	StrategyEpoch    uint64
	StrategySequence uint64
	Instruments      []string
}

type policyCancellationRecord struct {
	ID      string
	Hash    [32]byte
	Done    bool
	Request HubPolicyEntriesCancel
}

func (s *Store) requireNoAccountPolicyCancellation(ctx context.Context, executing string) error {
	body, err := s.StrategyCheckpoint(ctx, policyCancellationAccount, policyCancellationActive)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var record policyCancellationRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return err
	}
	if !record.Done && (executing == "" || executing != record.ID) {
		return ErrPolicyCancellationRecoveryRequired
	}
	return nil
}

func (s *Store) requireNoPolicyCancellation(ctx context.Context, strategy StrategyID) error {
	body, err := s.StrategyCheckpoint(ctx, strategy, policyCancellationActive)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var record policyCancellationRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return err
	}
	if !record.Done {
		return ErrPolicyCancellationRecoveryRequired
	}
	return nil
}

// CancelPolicyEntries persists admission before any venue cancellation. A
// completed retry is harmless; an interrupted multi-stage operation is fenced,
// never reinterpreted as permission to cancel a newer generation of orders.
func (h *Hub) CancelPolicyEntries(ctx context.Context, key AccountKey, request HubPolicyEntriesCancel) error {
	if !canonicalID(request.ID) || !canonicalID(string(request.Strategy)) || request.StrategyEpoch == 0 || request.StrategySequence == 0 || len(request.Instruments) == 0 {
		return errors.New("execution: policy cancellation requires command identity, writer and instruments")
	}
	request.Instruments = append([]string(nil), request.Instruments...)
	sort.Strings(request.Instruments)
	for n, id := range request.Instruments {
		if !canonicalID(id) || n > 0 && id == request.Instruments[n-1] {
			return errors.New("execution: policy cancellation instruments must be unique")
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(body)
	return h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.WithState(func(service *SharedAccount) error {
			ownerCtx, cancel := joinedOperationContext(account.Context(), ctx)
			defer cancel()
			name := "hub-policy-cancel:" + request.ID
			stored, err := service.store.StrategyCheckpoint(ownerCtx, request.Strategy, name)
			if err == nil {
				var record policyCancellationRecord
				if err := json.Unmarshal(stored, &record); err != nil {
					return err
				}
				if record.Hash != hash {
					return errors.New("execution: policy cancellation identity reused with different command")
				}
				if record.Done {
					return nil
				}
				return ErrPolicyCancellationRecoveryRequired
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if !service.ready {
				return errors.New("execution: shared account is not reconciled")
			}
			if err := service.store.requireNoAccountPolicyCancellation(ownerCtx, ""); err != nil {
				return err
			}
			if err := service.validateContributorInstruments(ownerCtx, request.Strategy, request.Instruments); err != nil {
				return err
			}
			for _, id := range request.Instruments {
				if _, exists := service.markets[id]; !exists {
					return errors.New("execution: policy cancellation instrument is not registered")
				}
			}
			checkpoints, err := service.strategyOrderingCheckpoints(ownerCtx, []StrategyRebalance{{Strategy: request.Strategy, StrategyEpoch: request.StrategyEpoch, StrategySequence: request.StrategySequence}})
			if err != nil {
				return err
			}
			record := policyCancellationRecord{ID: request.ID, Hash: hash, Request: request}
			save := func(ctx context.Context) error {
				body, err := json.Marshal(record)
				if err != nil {
					return err
				}
				if err := service.store.SaveStrategyCheckpoint(ctx, request.Strategy, name, body); err != nil {
					return err
				}
				if err := service.store.SaveStrategyCheckpoint(ctx, request.Strategy, policyCancellationActive, body); err != nil {
					return err
				}
				return service.store.SaveStrategyCheckpoint(ctx, policyCancellationAccount, policyCancellationActive, body)
			}
			if err := service.store.atomically(ownerCtx, func(ctx context.Context) error {
				for _, checkpoint := range checkpoints {
					if err := service.store.SaveStrategyCheckpoint(ctx, checkpoint.Strategy, checkpoint.Name, checkpoint.Payload); err != nil {
						return err
					}
				}
				return save(ctx)
			}); err != nil {
				return err
			}
			now := time.Now().UnixMilli()
			service.policyCancellationID = request.ID
			defer func() { service.policyCancellationID = "" }()
			err = service.cancelPolicyEntries(ownerCtx, request.Strategy, request.Instruments, now)
			if err == nil {
				record.Done = true
				err = service.store.atomically(ownerCtx, save)
			}
			if err != nil {
				service.ready = false
				freezeCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
				defer done()
				freezeErr := service.freezeFailure(freezeCtx, "policy-cancellation-incomplete/"+request.ID, now)
				return errors.Join(ErrPolicyCancellationRecoveryRequired, err, freezeErr)
			}
			return nil
		})
	})
}
