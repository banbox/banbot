package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/shopspring/decimal"
)

const hubContributorCheckpoint = "hub-contributor:v1"

// HubContributorRegistration adds one strategy to an existing account session.
// It cannot change account policy, credentials or the host's instrument universe.
// Capital is transferred from unassigned settled cash, never minted or borrowed
// from another strategy. Instrument declarations are immutable allowlists.
type HubContributorRegistration struct {
	Strategy      StrategyID
	PolicyVersion string
	Capital       decimal.Decimal
	GrossLimit    decimal.Decimal
	Instruments   []string
}

func normalizeContributor(request HubContributorRegistration) (HubContributorRegistration, error) {
	if !canonicalID(string(request.Strategy)) || !canonicalID(request.PolicyVersion) || !request.Capital.IsPositive() || !request.GrossLimit.IsPositive() || len(request.Instruments) == 0 {
		return request, errors.New("execution: invalid contributor registration")
	}
	request.Instruments = append([]string(nil), request.Instruments...)
	sort.Strings(request.Instruments)
	for n, id := range request.Instruments {
		if !canonicalID(id) || n > 0 && request.Instruments[n-1] == id {
			return request, errors.New("execution: invalid contributor instrument declaration")
		}
	}
	return request, nil
}

func (h *Hub) RegisterContributor(ctx context.Context, key AccountKey, request HubContributorRegistration) error {
	return h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.RegisterContributor(ctx, request, time.Now().UnixMilli())
	})
}

func (b *SharedAccountBorrow) RegisterContributor(caller context.Context, request HubContributorRegistration, now int64) error {
	return b.RegisterContributors(caller, []HubContributorRegistration{request}, now)
}

// RegisterContributors admits a robot's whole declaration atomically: a later
// invalid contributor must not leave earlier capital allocations behind.
func (b *SharedAccountBorrow) RegisterContributors(caller context.Context, requests []HubContributorRegistration, now int64) error {
	if now < 0 || len(requests) == 0 {
		return errors.New("execution: invalid contributor registration batch")
	}
	normalized := make([]HubContributorRegistration, len(requests))
	seen := map[StrategyID]bool{}
	for n, request := range requests {
		value, err := normalizeContributor(request)
		if err != nil {
			return err
		}
		if seen[value.Strategy] {
			return errors.New("execution: duplicate contributor in registration batch")
		}
		seen[value.Strategy] = true
		normalized[n] = value
	}
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, caller)
		defer done()
		if !s.ready {
			return errors.New("execution: contributor registration requires reconciled account")
		}
		return s.store.atomically(ctx, func(ctx context.Context) error {
			for _, request := range normalized {
				if err := s.registerContributor(ctx, request, now); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err == nil {
		b.service.notifyCommitted()
	}
	return err
}

func (s *SharedAccount) registerContributor(ctx context.Context, request HubContributorRegistration, now int64) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	prior, err := s.store.StrategyCheckpoint(ctx, request.Strategy, hubContributorCheckpoint)
	if err == nil {
		if string(prior) != string(body) {
			return errors.New("execution: immutable contributor registration mismatch")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	for _, id := range request.Instruments {
		if _, ok := s.markets[id]; !ok {
			return errors.New("execution: contributor instrument requires account host expansion")
		}
	}
	policy, err := s.store.AccountPolicy(ctx)
	if err != nil {
		return err
	}
	if policy.Version != request.PolicyVersion {
		return errors.New("execution: contributor account policy version mismatch")
	}
	if _, present := policy.StrategyGrossLimits[request.Strategy]; present {
		return errors.New("execution: contributor already allocated outside incremental registration")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if snapshot.RiskFrozen || snapshot.UnassignedCash.LessThan(request.Capital) || !snapshot.SyntheticStrategyCash[request.Strategy].IsZero() {
		return errors.New("execution: contributor requires unfrozen unassigned cash and a new strategy")
	}
	policy.StrategyGrossLimits[request.Strategy] = request.GrossLimit
	if err := s.store.RegisterAccountPolicy(ctx, policy); err != nil {
		return err
	}
	hash := sha256.Sum256(body)
	_, err = s.store.ApplyCashEvent(ctx, CashEvent{ID: fmt.Sprintf("hub-contributor:%x", hash), Kind: CapitalTransfer, Postings: []CashPosting{{Amount: request.Capital.Neg()}, {Strategy: request.Strategy, Amount: request.Capital}}, AtMS: now})
	if err != nil {
		return err
	}
	return s.store.SaveStrategyCheckpoint(ctx, request.Strategy, hubContributorCheckpoint, body)
}

func (s *SharedAccount) validateContributorInstruments(ctx context.Context, strategy StrategyID, instruments []string) error {
	body, err := s.store.StrategyCheckpoint(ctx, strategy, hubContributorCheckpoint)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // Account-bootstrap strategies retain their existing contract.
	}
	if err != nil {
		return err
	}
	var registration HubContributorRegistration
	if err := json.Unmarshal(body, &registration); err != nil {
		return err
	}
	for _, id := range instruments {
		n := sort.SearchStrings(registration.Instruments, id)
		if n == len(registration.Instruments) || registration.Instruments[n] != id {
			return errors.New("execution: instrument is outside contributor declaration")
		}
	}
	return nil
}

func (s *SharedAccount) validateRestoredContributor(ctx context.Context, strategy StrategyID) error {
	body, err := s.store.StrategyCheckpoint(ctx, strategy, hubContributorCheckpoint)
	if err != nil {
		return err
	}
	var request HubContributorRegistration
	if err := json.Unmarshal(body, &request); err != nil {
		return err
	}
	request, err = normalizeContributor(request)
	if err != nil || request.Strategy != strategy {
		return errors.New("execution: invalid restored hub contributor")
	}
	policy, err := s.store.AccountPolicy(ctx)
	if err != nil {
		return err
	}
	if policy.Version != request.PolicyVersion || !policy.StrategyGrossLimits[strategy].Equal(request.GrossLimit) {
		return errors.New("execution: restored hub contributor policy mismatch")
	}
	for _, id := range request.Instruments {
		if _, ok := s.markets[id]; !ok {
			return errors.New("execution: restored hub contributor instrument host is unavailable")
		}
	}
	return nil
}
