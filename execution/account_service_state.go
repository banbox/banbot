package execution

import (
	"context"
	"errors"
)

// WithState is the serialized extension boundary for strategy checkpoint and
// projection adapters. Callers must not reenter the borrow from its callback.
// Networking remains behind OwnerExecutor and its durable-attempt gate.
func (b *SharedAccountBorrow) WithState(call func(*SharedAccount) error) error { return b.use(call) }
func (b *SharedAccountBorrow) Context() context.Context                        { return b.ctx }
func (b *SharedAccountBorrow) AccountKey() AccountKey                          { return b.service.owner.Token().Key }
func (s *SharedAccount) Store() *Store                                         { return s.store }
func (s *SharedAccount) AccountKey() AccountKey                                { return s.owner.Token().Key }
func (s *SharedAccount) Reconciled() bool                                      { return s.ready }
func (s *SharedAccount) SendTime(now int64) int64                              { return s.sendTime(now) }
func (s *SharedAccount) ExecutorFor(ctx context.Context) *OwnerExecutor        { return s.executorFor(ctx) }
func (s *SharedAccount) PrepareWithCheckpoint(request CombinedRebalance, ctx context.Context, checkpoint *StrategyCheckpoint) (PreparedRebalance, error) {
	return s.prepareRebalance(request, ctx, checkpoint)
}
func (s *SharedAccount) SendPrepared(prepared PreparedRebalance, now int64, ctx context.Context) error {
	return s.sendPreparedRebalance(prepared, now, ctx)
}
func (s *SharedAccount) ReadQuote(callerCtx context.Context, id string, now int64) (VisibleQuote, error) {
	var quote VisibleQuote
	err := s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
		ctx, cancel := joinedOperationContext(ctx, callerCtx)
		defer cancel()
		market, ok := s.markets[id]
		if !ok {
			return errors.New("execution: account quote provider missing")
		}
		var err error
		quote, err = market.quote(ctx, id, now)
		return err
	})
	return quote, err
}

// AdvancePaperQuote settles resting maker orders before processing this quote's
// strategy decisions. Recovery also picks up a prior simulated fill whose local
// persistence was interrupted; observing the same quote cannot lose that fill.
func (b *SharedAccountBorrow) AdvancePaperQuote(callerCtx context.Context, id string, quote VisibleQuote, nowMS int64) error {
	return b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		return s.AdvancePaperQuote(ctx, id, quote, nowMS)
	})
}

func (s *SharedAccount) AdvancePaperQuote(ctx context.Context, id string, quote VisibleQuote, nowMS int64) error {
	paper, ok := s.opts.Adapter.(*PaperAdapter)
	if !ok || paper == nil {
		// Live and wrapped adapters deliver their own venue reports.
		return nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	executor := s.executorFor(ctx)
	var completed []string
	err = executor.local(func(ctx context.Context) error {
		if _, err := paper.AdvanceQuote(ctx, id, quote, nowMS); err != nil {
			return err
		}
		for _, order := range snapshot.Orders {
			if order.Intent.Instrument.ID != id || !order.Intent.PostOnly || order.State == OrderPrepared {
				continue
			}
			result, err := paper.Query(ctx, order.ClientID, order.ExchangeID)
			if err != nil {
				return err
			}
			if result.Canceled || result.Receipt.Rejected || len(result.Receipt.Fills) > 0 {
				completed = append(completed, order.Intent.ID)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, orderID := range completed {
		if err := executor.Recover(orderID); err != nil {
			return err
		}
	}
	return nil
}
