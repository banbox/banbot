package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/shopspring/decimal"
)

// AccountPolicy is an explicit, immutable policy registration. New strategies
// may be registered; existing account limits and strategy caps cannot change.
type AccountPolicy struct {
	Version                                       string
	Currency                                      string
	MarginRate, MaxAccountMargin, MaxVirtualGross decimal.Decimal
	StrategyGrossLimits                           map[StrategyID]decimal.Decimal
}

func (s *Store) AccountPolicy(ctx context.Context) (AccountPolicy, error) {
	var p AccountPolicy
	body, err := s.StrategyCheckpoint(ctx, "account-risk", "policy-v1")
	if err != nil {
		return p, err
	}
	err = json.Unmarshal(body, &p)
	return p, err
}
func (s *Store) RegisterAccountPolicy(ctx context.Context, p AccountPolicy) error {
	if p.Version == "" || p.Currency != s.key.SettlementDomain || !p.MarginRate.IsPositive() || !p.MaxAccountMargin.IsPositive() || !p.MaxVirtualGross.IsPositive() || len(p.StrategyGrossLimits) == 0 {
		return errors.New("execution: explicit account policy required")
	}
	for id, cap := range p.StrategyGrossLimits {
		if !canonicalID(string(id)) || !cap.IsPositive() {
			return errors.New("execution: invalid strategy policy")
		}
	}
	return s.atomically(ctx, func(ctx context.Context) error {
		old, err := s.AccountPolicy(ctx)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if old.Version != p.Version || old.Currency != p.Currency || !old.MarginRate.Equal(p.MarginRate) || !old.MaxAccountMargin.Equal(p.MaxAccountMargin) || !old.MaxVirtualGross.Equal(p.MaxVirtualGross) {
				return errors.New("execution: immutable account policy mismatch")
			}
			for id, cap := range p.StrategyGrossLimits {
				if prior, ok := old.StrategyGrossLimits[id]; ok && !prior.Equal(cap) {
					return errors.New("execution: immutable strategy policy mismatch")
				}
				old.StrategyGrossLimits[id] = cap
			}
			p = old
		}
		body, err := json.Marshal(p)
		if err != nil {
			return err
		}
		return s.commit(ctx, func(tx *storeTxn) error {
			_, err := tx.Exec(opPutStrategyCheckpoint, s.accountID, "account-risk", "policy-v1", string(body))
			return err
		})
	})
}
