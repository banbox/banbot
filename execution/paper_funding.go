package execution

import (
	"context"
	"errors"
	"sort"

	"github.com/shopspring/decimal"
)

// PaperFundingObservation identifies the source fact, independent of the lots
// held by whichever strategy happens to observe it next.
type PaperFundingObservation struct {
	ID                string
	Instrument        Instrument
	Mark, Rate        decimal.Decimal
	AtMS, AvailableAt int64
}

// ApplyPaperFunding freezes attribution and changes simulated venue cash once,
// under the same account gate used for fills. The durable source payload keeps
// duplicate validation valid after positions change or history becomes cold.
func (b *SharedAccountBorrow) ApplyPaperFunding(callerCtx context.Context, event PaperFundingObservation, now int64) (bool, error) {
	if err := event.Instrument.Validate(); err != nil {
		return false, err
	}
	if !canonicalID(event.ID) || event.Instrument.IsOption() || !event.Mark.IsPositive() || event.AtMS < 0 || event.AtMS != now || event.AvailableAt < event.AtMS || event.AvailableAt > now {
		return false, errors.New("execution: invalid/unavailable paper funding observation")
	}
	applied := false
	err := b.use(func(s *SharedAccount) error {
		paper, ok := s.opts.Adapter.(*PaperAdapter)
		if !ok || paper == nil {
			return errors.New("execution: paper funding requires a simulated account")
		}
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			ctx, done := joinedOperationContext(ctx, callerCtx)
			defer done()
			snap, err := s.store.Snapshot(ctx)
			if err != nil {
				return err
			}
			body, err := payload(event)
			if err != nil {
				return err
			}
			expectedInstrument, err := payload(event.Instrument)
			if err != nil {
				return err
			}
			delta := decimal.Zero
			err = s.store.commit(ctx, func(tx *storeTxn) error {
				fresh, err := s.store.recordEvent(tx, event.ID, "PaperFunding", body)
				if err != nil || !fresh {
					return err
				}
				virtual, actual := decimal.Zero, decimal.Zero
				byStrategy := map[StrategyID]decimal.Decimal{}
				for _, lot := range snap.ExternalPositions {
					if lot.Instrument.ID == event.Instrument.ID && lot.SignedSteps != 0 {
						return errors.New("execution: external funding exposure requires reconciliation")
					}
				}
				for _, lot := range snap.ActualPositions {
					if lot.Instrument.ID == event.Instrument.ID {
						actual = actual.Add(decimal.NewFromInt(lot.SignedSteps))
					}
				}
				for _, lot := range snap.Lots {
					if lot.Instrument.ID != event.Instrument.ID {
						continue
					}
					instrumentBody, err := payload(lot.Instrument)
					if err != nil {
						return err
					}
					if instrumentBody != expectedInstrument {
						return errors.New("execution: funding instrument revision mismatch")
					}
					virtual = virtual.Add(decimal.NewFromInt(lot.SignedSteps))
					amount := event.Instrument.Notional(lot.SignedSteps, event.Mark).Mul(event.Rate).Neg().Round(event.Instrument.MoneyScale)
					byStrategy[lot.Strategy] = byStrategy[lot.Strategy].Add(amount)
					delta = delta.Add(amount)
				}
				if !virtual.Equal(actual) {
					return errors.New("execution: unassigned funding exposure requires reconciliation")
				}
				if len(byStrategy) == 0 {
					byStrategy[""] = decimal.Zero
				}
				ids := make([]string, 0, len(byStrategy))
				for id := range byStrategy {
					ids = append(ids, string(id))
				}
				sort.Strings(ids)
				for _, id := range ids {
					strategy := StrategyID(id)
					amount := byStrategy[strategy]
					if err := s.store.addCash(tx, strategy, amount); err != nil {
						return err
					}
					if err := s.store.addStrategyTotals(tx, strategy, decimal.Zero, amount); err != nil {
						return err
					}
					if err := s.store.ledger(tx, LedgerEntry{EventID: event.ID, Kind: string(Funding), Strategy: strategy, CashDelta: amount, AtMS: event.AtMS}); err != nil {
						return err
					}
				}
				if err := s.store.commitAccountEvent(tx, event.ID, delta, nil); err != nil {
					return err
				}
				applied = true
				return nil
			})
			if err == nil && applied {
				paper.ApplyCash(delta)
			}
			return err
		})
	})
	if err == nil && applied {
		b.service.notifyCommitted()
	}
	return applied && err == nil, err
}
