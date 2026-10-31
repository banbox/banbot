package biz

import (
	"context"
	"database/sql"
	"errors"
	"math"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// restoreTakeovers adds presentation metadata only. Quantity, ownership and
// execution state were already committed atomically by the account owner.
func (m *SharedOrderMgr) restoreTakeovers(ctx context.Context, store *execution.Store, state *sharedTSCheckpoint, snapshot execution.AccountSnapshot) error {
	cfg := m.deps.ConfigView()
	if cfg == nil || cfg.TakeOverStrat == "" {
		return nil
	}
	name := cfg.TakeOverStrat
	binding, configured := m.config.Strategies[name]
	if !configured {
		return errors.New("biz: takeover strategy is not declared by the shared bridge")
	}
	records, err := store.AutoTakeoverRecords(ctx)
	if err != nil {
		return err
	}
	if err := state.loadPendingIndex(); err != nil {
		return err
	}
	changed := false
	for _, adopted := range records {
		request := adopted.Attribution
		if request.Strategy != binding.ID {
			continue
		}
		if err := state.loadLot(request.Strategy, request.Lot); err == nil {
			continue
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var symbol string
		for candidate, instrument := range m.config.Instruments {
			if instrument.ID == request.Instrument.ID {
				symbol = candidate
				break
			}
		}
		if symbol == "" {
			return errors.New("biz: adopted instrument is not declared by the shared bridge")
		}
		// Use the same runtime-owned timeframe assignment as the former live
		// takeover path. Without it, a single bound job is unambiguous.
		timeframe := ""
		if m.deps.Core != nil {
			timeframe, _ = m.deps.Core.StrategyTimeFrame(name, symbol)
		}
		m.mu.Lock()
		var job *strat.StratJob
		candidates := 0
		for identity, candidate := range m.jobs {
			if identity.Strategy == name && identity.Symbol == symbol && (timeframe == "" || identity.TimeFrame == timeframe) {
				job = candidate
				candidates++
			}
		}
		m.mu.Unlock()
		if candidates == 0 {
			continue // BindJobs restores this lot after jobs finish loading.
		}
		if candidates != 1 || job == nil || job.Symbol == nil {
			return errors.New("biz: takeover timeframe is ambiguous; configure the runtime strategy/pair timeframe")
		}
		timeframe = job.TimeFrame
		pending, ok := state.pendingOrders[sharedPendingKey{request.Strategy, request.Lot}]
		if !ok {
			return errors.New("biz: adopted lot is missing its execution checkpoint")
		}
		quantity := absSharedSteps(request.ExpectedSignedSteps)
		price := adopted.Position.CostBasis.Div(decimal.NewFromInt(quantity).Mul(request.Instrument.QuantityStep).Mul(request.Instrument.ContractSize))
		if state.Serial == math.MaxInt64 {
			return errors.New("biz: takeover projection order identifier overflow")
		}
		state.Serial++
		order := &sharedTSOrder{PendingOrder: pending, ID: state.Serial, StrategyName: name, Symbol: symbol, SID: job.Symbol.ID, TimeFrame: timeframe,
			CreatedMS: request.AtMS, InitPrice: price.InexactFloat64(),
			Request:    strat.EnterReq{StratName: name, Tag: core.EnterTagThird, Short: request.ExpectedSignedSteps < 0, Amount: decimal.NewFromInt(quantity).Mul(request.Instrument.QuantityStep).InexactFloat64(), OrderType: core.OrderTypeMarket},
			SourceInfo: map[string]any{"external_takeover": request.ID, "external_source_event": adopted.SourceEventID, "entry_time_is_adoption": true}}
		state.Orders[sharedOrderKey(binding.ID, order.ID)] = order
		changed = true
	}
	if !changed {
		return nil
	}
	checkpoint, err := splitSharedCheckpoint(*state, snapshot)
	if err != nil {
		return err
	}
	return store.SaveProjectionMetadata(ctx, checkpoint)
}

func absSharedSteps(steps int64) int64 {
	if steps < 0 {
		return -steps
	}
	return steps
}
