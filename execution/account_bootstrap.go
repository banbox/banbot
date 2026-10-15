package execution

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/shopspring/decimal"
)

// BootstrapCapital imports settled cash and explicit strategy allocations in
// one transaction. Restart never repeats allocations or adopts manual exposure.
func (b *SharedAccountBorrow) BootstrapCapital(caller context.Context, capital map[StrategyID]decimal.Decimal, now int64) error {
	return b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			ctx, done := joinedOperationContext(ownerCtx, caller)
			defer done()
			ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			local, err := s.store.Snapshot(ctx)
			if err != nil || local.Checkpoint != 0 {
				return err
			}
			if !s.opts.AuthoritativeSnapshot {
				return errors.New("execution: capital bootstrap requires authoritative account inventory")
			}
			venue, err := s.opts.Adapter.Snapshot(ctx)
			if err != nil {
				return err
			}
			if len(venue.OpenOrders) != 0 || len(venue.Fills) != 0 {
				return errors.New("execution: capital bootstrap refuses existing venue orders or fills")
			}
			for _, steps := range venue.Positions {
				if steps != 0 {
					return errors.New("execution: capital bootstrap refuses existing venue positions")
				}
			}
			cash, err := decimal.NewFromString(venue.Cash)
			if err != nil || !cash.IsPositive() || len(capital) == 0 {
				return errors.New("execution: positive settled cash and explicit strategy capital required")
			}
			var ids []string
			total := decimal.Zero
			for id, amount := range capital {
				if !canonicalID(string(id)) || !amount.IsPositive() {
					return errors.New("execution: invalid strategy capital allocation")
				}
				ids = append(ids, string(id))
				total = total.Add(amount)
			}
			if total.GreaterThan(cash) {
				return errors.New("execution: strategy capital exceeds actual settled cash")
			}
			sort.Strings(ids)
			postings := []CashPosting{{Amount: cash.Sub(total)}}
			for _, id := range ids {
				postings = append(postings, CashPosting{Strategy: StrategyID(id), Amount: capital[StrategyID(id)]})
			}
			_, err = s.store.ApplyCashEvent(ctx, CashEvent{ID: "live-capital-bootstrap-v1", Kind: Reconciliation, AccountDelta: cash, Postings: postings, AtMS: now})
			return err
		})
	})
}
