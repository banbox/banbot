package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// CancelPolicyEntriesContext settles obsolete increasing orders before the
// policy freezes an absolute-quantity exit anchor. A shared real order is
// canceled at the owner, then untouched strategy targets are rebuilt together
// with this strategy's final confirmed holdings, including any late fills.
func (b *SharedAccountBorrow) CancelPolicyEntriesContext(callerCtx context.Context, strategy StrategyID, instruments []string, now int64) error {
	return b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		selected := map[string]bool{}
		for _, id := range instruments {
			selected[id] = true
		}
		snapshot, err := s.store.Snapshot(ctx)
		if err != nil {
			return err
		}
		var ids []string
		for _, order := range snapshot.Orders {
			if !selected[order.Intent.Instrument.ID] {
				continue
			}
			cancel := false
			for _, a := range order.Intent.Allocations {
				if a.Strategy == strategy && a.Kind == EntryIntent && a.Steps > order.AllocationFilled[a.ID] {
					cancel = true
				}
			}
			if !cancel {
				continue
			}
			ids = append(ids, order.Intent.ID)
		}
		for _, id := range ids {
			if err := s.executorFor(ctx).Cancel(id, now); err != nil {
				return err
			}
		}
		plan, err := s.store.LatestPlan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		err = s.executorFor(ctx).local(func(ctx context.Context) error {
			return s.store.atomically(ctx, func(ctx context.Context) error {
				for _, old := range plan.Intents {
					if old.Strategy != strategy || old.Kind != EntryIntent || !selected[old.Instrument] {
						continue
					}
					current, err := s.store.Intent(ctx, old.ID)
					if err != nil {
						return err
					}
					if current.State == Filled || current.State == Canceled || current.State == Expired {
						continue
					}
					if current.ReservedSteps > 0 {
						return errors.New("execution: increasing reservation still requires reconciliation")
					}
					if err := current.Cancel(); err != nil {
						return err
					}
					body, err := payload(current)
					if err != nil {
						return err
					}
					if err := s.store.commit(ctx, func(tx *storeTxn) error {
						_, err := tx.Exec(opUpdateIntentRuntime, body, s.store.accountID, string(current.ID))
						return err
					}); err != nil {
						return err
					}
				}
				return nil
			})
		})
		if err != nil {
			return err
		}
		final, err := s.store.Snapshot(ctx)
		if err != nil {
			return err
		}
		request := StrategyRebalance{Strategy: strategy, PlanID: "policy-reconcile:" + rebalanceID(strategy, selected, final.Checkpoint, now), DecisionMS: now, ExpiresMS: int64(^uint64(0) >> 1), Mode: StrategyTargetsPatch}
		instrumentIDs := make([]string, 0, len(selected))
		for id := range selected {
			instrumentIDs = append(instrumentIDs, id)
		}
		sort.Strings(instrumentIDs)
		for _, id := range instrumentIDs {
			market, ok := s.markets[id]
			if !ok {
				return errors.New("execution: policy reconciliation quote provider missing")
			}
			quote, err := s.ReadQuote(ctx, id, now)
			if err != nil {
				return err
			}
			request.ExpiresMS = min(request.ExpiresMS, quote.ValidUntilMS)
			input := InstrumentRebalance{Instrument: market.instrument, Quote: quote}
			lots := map[VirtualLotID]int64{}
			for _, target := range plan.Targets {
				if target.Strategy == strategy && target.Instrument == id {
					lots[target.Lot] = 0
				}
			}
			for _, lot := range final.Lots {
				if lot.Strategy == strategy && lot.Instrument.ID == id {
					lots[lot.ID] = lot.SignedSteps
				}
			}
			for lot, steps := range lots {
				input.Targets = append(input.Targets, ExecutableTarget{Strategy: strategy, Lot: lot, SignedSteps: steps})
			}
			sort.Slice(input.Targets, func(i, j int) bool { return input.Targets[i].Lot < input.Targets[j].Lot })
			request.Requests = append(request.Requests, input)
		}
		if request.ExpiresMS <= now {
			return fmt.Errorf("execution: policy reconciliation quote expired at %d", request.ExpiresMS)
		}
		prepared, err := s.prepareStrategy(request, ctx)
		if err != nil {
			return err
		}
		return s.sendPreparedRebalance(prepared, now, ctx)
	})
}
