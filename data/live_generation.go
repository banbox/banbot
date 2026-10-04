package data

import (
	"context"
	"fmt"
	"reflect"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

// LiveProviderGenerationFactory creates an independently owned physical
// ingress. It must not reuse the previous provider's feeders or socket.
type LiveProviderGenerationFactory func(context.Context, *SubscriptionPlan) (*LiveProvider, error)

// SetGenerationFactory supports custom typed transports before installation.
func (p *LiveProvider) SetGenerationFactory(factory LiveProviderGenerationFactory) error {
	if p == nil || factory == nil || p.klineSubscriptionsSet {
		return fmt.Errorf("provider generation factory must be supplied before installation")
	}
	p.generationFactory = factory
	return nil
}

func (p *LiveProvider) NewGeneration(ctx context.Context, plan *SubscriptionPlan) (*LiveProvider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hasKlines := false
	for _, sub := range plan.Subscriptions() {
		hasKlines = hasKlines || sub.Source == orm.SeriesSourceKline
	}
	if !hasKlines {
		return NewLiveSourceProvider(plan.catalog), nil
	}
	if p == nil || p.generationFactory == nil {
		return nil, fmt.Errorf("independent kline provider generation factory is required")
	}
	next, err := p.generationFactory(ctx, plan)
	if err != nil {
		return nil, err
	}
	if next == nil || next == p || next.catalog != plan.catalog || next.klineSubscriptionsSet {
		return nil, fmt.Errorf("provider generation factory returned a reused or incompatible provider")
	}
	if next.generationFactory == nil {
		next.generationFactory = p.generationFactory
	}
	return next, nil
}

func isolateSubscriptionDeps(original *RuntimeDeps, ctx context.Context, plan *SubscriptionPlan) (*RuntimeDeps, error) {
	copyDeps := *original
	copyDeps.SubscriptionContext = ctx
	copyDeps.IsolatedSubscriptions = true
	copyDeps.Strategies = strat.NewState()
	exchange, market := original.identity()
	copyDeps.Market = com.NewMarketStateWithExchange(exchange, original.exchange())
	if original.Symbols == nil {
		return nil, fmt.Errorf("isolated provider requires explicit symbols")
	}
	copyDeps.Symbols = orm.NewSymbolStateWithAllocatorAndIdentity(original.Symbols.SIDAllocator(), exchange, market)
	if err := copyDeps.Symbols.BindStorage(original.storage()); err != nil {
		return nil, err
	}
	for _, symbol := range original.Symbols.GetExSymbolsByID(exchange, market) {
		copySymbol := *symbol
		if err := copyDeps.Symbols.CacheExSymbolChecked(&copySymbol); err != nil {
			return nil, err
		}
	}
	for _, sub := range plan.Subscriptions() {
		copySymbol := *sub.ExSymbol
		if err := copyDeps.Symbols.CacheExSymbolChecked(&copySymbol); err != nil {
			return nil, err
		}
	}
	return &copyDeps, nil
}

// configureSubscriptionIngress replaces captured strategy callbacks before
// creating/warming feeders. Values pass through the ordinary typed series map.
func (p *LiveProvider) configureSubscriptionIngress(ctx context.Context, plan *SubscriptionPlan, installation *SubscriptionInstallation) error {
	p.subscriptionSink = installation
	if p.deps != nil {
		isolated, err := isolateSubscriptionDeps(p.deps, ctx, plan)
		if err != nil {
			return err
		}
		p.deps, p.Provider.deps, p.symbols = isolated, isolated, isolated.Symbols
		p.wsSubs = strat.NewWsSubJobRegistryWithState(isolated.Strategies, isolated.Symbols)
		if p.SeriesWatcher != nil {
			p.SeriesWatcher.deps, p.SeriesWatcher.symbols = isolated, isolated.Symbols
		}
	}
	baseFactory := p.newFeeder
	if baseFactory == nil {
		return nil
	}
	subscriptions := make(map[orm.StreamKey]*orm.Subscription)
	for _, sub := range plan.Subscriptions() {
		copySub := sub
		subscriptions[sub.Key()] = &copySub
	}
	p.revisionStreams = len(subscriptions)
	p.newFeeder = func(pair string, tfs []string) (IDataFeeder, *errs.Error) {
		feeder, err := baseFactory(pair, tfs)
		if err != nil {
			return nil, err
		}
		series, ok := feeder.(interface {
			bindSubscriptionIngress(*RuntimeDeps, *orm.SymbolState, FnDataSeries)
		})
		if !ok {
			return nil, errs.NewMsg(core.ErrBadConfig, "typed subscription ingress requires a SeriesFeeder")
		}
		series.bindSubscriptionIngress(p.deps, p.symbols, func(row *orm.DataSeries) {
			if row == nil {
				installation.report(fmt.Errorf("kline provider emitted nil series"))
				return
			}
			copyRow := *row
			copyRow.Source = orm.NormalizeSeriesSource(copyRow.Source)
			row = &copyRow
			keySub := orm.Subscription{Source: row.Source, TimeFrame: row.TimeFrame, ExSymbol: row.ExSymbol}
			sub := subscriptions[keySub.Key()]
			if sub == nil || row.ExSymbol == nil || *row.ExSymbol != *sub.ExSymbol {
				installation.report(fmt.Errorf("kline provider emitted an undeclared or foreign series"))
				return
			}
			records := []*orm.DataRecord{{Sid: row.Sid, TimeMS: row.TimeMS, EndMS: row.EndMS, Closed: row.Closed, Values: row.Values}}
			var emitErr error
			if row.IsWarmUp {
				if typed, ok := installation.sink.(LiveSeriesSink); ok {
					emitErr = typed.WarmupSeries(sub, cloneStartupSeries([]*orm.DataSeries{row}))
				} else {
					emitErr = installation.Warmup(sub, records)
				}
			} else {
				emitErr = installation.emitRows(sub, records, []*orm.DataSeries{row}, nil)
			}
			if emitErr != nil {
				installation.report(emitErr)
			}
		})
		return feeder, nil
	}
	return nil
}

func cloneStartupSeries(rows []*orm.DataSeries) []*orm.DataSeries {
	if rows == nil {
		return nil
	}
	result := make([]*orm.DataSeries, len(rows))
	for i, row := range rows {
		if row != nil {
			copyRow := *row
			copyRow.Values = cloneStartupValue(reflect.ValueOf(row.Values)).Interface().(map[string]any)
			if row.Adj != nil {
				copyAdj := *row.Adj
				copyRow.Adj = &copyAdj
			}
			result[i] = &copyRow
		}
	}
	return result
}

func (f *SeriesFeeder) bindSubscriptionIngress(deps *RuntimeDeps, symbols *orm.SymbolState, callback FnDataSeries) {
	f.deps, f.symbols, f.CallBack = deps, symbols, callback
	if f.hour != nil {
		f.hour.deps, f.hour.symbols = deps, symbols
	}
	// Live series dispatch already flushes each completed decision grid.
	f.OnEnvEnd = func(*orm.DataSeries) {}
}

// The revision mapper consumes the same typed projection as the final callback.
// Enrich before revision admission, then reuse these rows through aggregation
// and dispatch so a late OHLCV-only packet needs only one storage read.
func (f *SeriesFeeder) enrichSubscriptionInput(timeframe string, rows []*orm.DataSeries) ([]*orm.DataSeries, *errs.Error) {
	return enrichStoredKlineFieldsWithRuntimeDepsAndReader(f.deps, f.ExSymbol, timeframe, rows, f.readKlineFields, f.subscriptionFields[timeframe])
}

func (f *SeriesFeeder) subscriptionRevisionView(row *orm.DataSeries) *orm.DataSeries {
	view := *applyAdjSeries(f.adj, []*orm.DataSeries{row})[0]
	if f.adj != nil {
		view.Adj = f.adj
	}
	view.IsWarmUp = false
	return &view
}

func (s *SubscriptionInstallation) Warmup(sub *orm.Subscription, rows []*orm.DataRecord) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if warm, ok := s.sink.(LiveWarmupSink); ok {
		return warm.Warmup(sub, cloneStartupRows(rows))
	}
	return nil
}

// StartSubscriptionLoop owns one reader even when existing entry callers also
// wait on LoopMain. Stop seals intake and Join waits the actual socket reader.
func (p *LiveProvider) StartSubscriptionLoop(installation *SubscriptionInstallation) {
	if p.SeriesWatcher == nil || p.SeriesWatcher.ClientIO == nil {
		return
	}
	installation.producers.Add(1)
	go func() {
		defer installation.producers.Done()
		err := p.LoopMain()
		if installation.ctx.Err() == nil {
			if err != nil {
				installation.report(err)
			} else {
				installation.report(fmt.Errorf("kline source reader exited before subscription shutdown"))
			}
		}
	}()
}

// PrepareSubscriptionPlan includes kline warmup and retains all live emissions
// until the generation owner commits and calls Activate.
func (p *LiveProvider) PrepareSubscriptionPlan(ctx context.Context, plan *SubscriptionPlan, sink DataSink) (*SubscriptionInstallation, error) {
	if p == nil || plan == nil || plan.catalog != p.catalog || p.klineSubscriptionsSet {
		return nil, fmt.Errorf("fresh provider and compatible subscription plan required")
	}
	installation, err := p.catalog.prepareLivePlan(ctx, plan, sink, func() { _ = p.Stop() }, p.Join)
	if err != nil {
		_ = p.Stop()
		p.Join()
		return nil, err
	}
	if err = p.configureSubscriptionIngress(ctx, plan, installation); err == nil {
		err = plan.warmupLive(installation)
	}
	if err == nil {
		var klines []Subscription
		for _, sub := range plan.Subscriptions() {
			if sub.Source == orm.SeriesSourceKline {
				klines = append(klines, sub)
			}
		}
		if warmErr := p.SetKlineSubscriptions(klines); warmErr != nil {
			err = warmErr
		}
	}
	if err == nil {
		err = installation.warmupReady(plan.options.AnchorMS)
	}
	if err == nil {
		err = installation.validateSources(plan)
	}
	if err != nil {
		installation.Stop()
		_ = installation.Join()
		return nil, err
	}
	p.StartSubscriptionLoop(installation)
	return installation, nil
}
