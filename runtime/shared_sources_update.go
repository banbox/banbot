package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
)

// FactorLiveSubscription owns dedicated runners and their source generations.
// Fixed legacy sessions retain their state while the single canonical physical
// ingress and independently warmed factor sessions swap at one boundary.
type FactorLiveSubscription struct {
	runtime    *Runtime
	provider   *data.LiveProvider
	ctx        context.Context
	cancel     context.CancelFunc
	errors     chan error
	boundary   sync.RWMutex
	updates    sync.Mutex
	lifetime   sync.Mutex // serializes Stop with generation publication
	current    atomic.Pointer[factorLiveGeneration]
	stopOnce   sync.Once
	retiredErr error // guarded by updates
	legacy     *legacyLiveIngress
}

type factorLiveGeneration struct {
	owner        *FactorLiveSubscription
	plan         *data.SubscriptionPlan
	sink         *factorsLiveSourceSink
	installation *data.SubscriptionInstallation
	provider     *data.LiveProvider
	ctx          context.Context
	cancel       context.CancelFunc
}

func (g *factorLiveGeneration) Emit(sub *orm.Subscription, rows []*orm.DataRecord) error {
	return g.EmitSeries(sub, sourceRecordSeries(sub, rows, false))
}
func (g *factorLiveGeneration) EmitSeries(sub *orm.Subscription, rows []*orm.DataSeries) error {
	g.owner.boundary.RLock()
	defer g.owner.boundary.RUnlock()
	if err := g.owner.ctx.Err(); err != nil {
		return err
	}
	if g.owner.current.Load() != g {
		return nil
	}
	if sub != nil && sub.Source == orm.SeriesSourceKline && sub.ExSymbol != nil && len(rows) > 0 {
		endMS := int64(0)
		for _, row := range rows {
			if row != nil {
				endMS = max(endMS, row.EndMS)
			}
		}
		if endMS > 0 {
			if market := g.owner.runtime.Market; market != nil && market.PairCopied != nil {
				previous := market.PairCopied.GetPairCopied(sub.ExSymbol.Symbol)
				step := rows[len(rows)-1].EndMS - rows[len(rows)-1].TimeMS
				market.PairCopied.SetPairMsAt(g.owner.runtime.Clock.TimeMS(), sub.ExSymbol.Symbol, max(previous[0], endMS), step)
			}
			g.owner.runtime.Core.AddTfPairHits(sub.TimeFrame, sub.ExSymbol.Symbol, len(rows))
		}
	}
	return g.sink.EmitSeries(sub, rows)
}
func (g *factorLiveGeneration) Warmup(sub *orm.Subscription, rows []*orm.DataRecord) error {
	return g.WarmupSeries(sub, sourceRecordSeries(sub, rows, true))
}
func (g *factorLiveGeneration) WarmupSeries(sub *orm.Subscription, rows []*orm.DataSeries) error {
	if !g.owner.runtime.EnterCallback() {
		return errors.New("runtime: shared source intake stopped")
	}
	defer g.owner.runtime.LeaveCallback()
	if err := g.sink.WarmupSeries(sub, rows); err != nil {
		return err
	}
	if g.owner.current.Load() == g && g.owner.legacy != nil {
		return g.owner.legacy.warmup(g, sub, rows)
	}
	return nil
}
func (g *factorLiveGeneration) WarmupReady(anchorMS int64) error { return g.sink.WarmupReady(anchorMS) }

func (g *factorLiveGeneration) SeriesRevision(sub *orm.Subscription, series *orm.DataSeries) (data.LiveSeriesRevision, error) {
	consumer := g.mappingConsumer(sub, series)
	copySeries := *series
	received := g.owner.runtime.Clock.TimeMS()
	record, err := consumer.mapper(&copySeries, received)
	if err == nil && (record.IngestedAt != received || record.EventTime > received || record.AvailableAt > received) {
		err = errors.New("runtime: correction invalid publication/reception time")
	}
	if err == nil {
		originalSeries := record.Series
		record, err = factor.CloneVersionRecord(record)
		// Archive clones omit mutable runtime metadata. Live reuse retains an
		// owned copy of the mapper's symbol/adjustment snapshot for dispatch.
		if err == nil {
			if originalSeries.ExSymbol != nil {
				copySymbol := *originalSeries.ExSymbol
				record.Series.ExSymbol = &copySymbol
			}
			if originalSeries.Adj != nil {
				copyAdj := *originalSeries.Adj
				if copyAdj.ExSymbol != nil {
					copySymbol := *copyAdj.ExSymbol
					copyAdj.ExSymbol = &copySymbol
				}
				record.Series.Adj = &copyAdj
			}
		}
	}
	if err == nil {
		reads := 1
		if g.owner.legacy != nil && g.owner.legacy.streams[sub.Key()] && consumer.engine.HasSID(series.Sid) && consumer.streams[sub.Source+"/"+sub.TimeFrame] {
			reads++
		}
		err = consumer.mappings.stage(series, record, received, reads)
	}
	return data.LiveSeriesRevision{Revision: record.Revision, EventTime: record.EventTime, SourceVersion: record.SourceVersion}, err
}

func (s *FactorLiveSubscription) Errors() <-chan error { return s.errors }

// Done belongs to the logical subscription lifetime, including replacements.
func (s *FactorLiveSubscription) Done() <-chan struct{} { return s.ctx.Done() }
func (s *FactorLiveSubscription) Plan() *data.SubscriptionPlan {
	if current := s.current.Load(); current != nil {
		return current.plan
	}
	return nil
}
func (s *FactorLiveSubscription) Degradations() []data.SubscriptionDegradation {
	if current := s.current.Load(); current != nil {
		return current.installation.Degradations()
	}
	return nil
}
func (s *FactorLiveSubscription) Stop() {
	s.stopOnce.Do(func() {
		s.lifetime.Lock()
		defer s.lifetime.Unlock()
		s.cancel()
		if current := s.current.Load(); current != nil {
			current.stop()
		}
	})
}
func (s *FactorLiveSubscription) Join() error {
	if s.ctx.Err() == nil {
		return errors.New("runtime: subscription Join requires Stop")
	}
	s.updates.Lock()
	defer s.updates.Unlock()
	if current := s.current.Load(); current != nil {
		return errors.Join(s.retiredErr, current.join())
	}
	return nil
}
func (g *factorLiveGeneration) stop() {
	g.cancel()
	g.DiscardPreparedSeries()
	if g.installation != nil {
		g.installation.Stop()
	} else if g.provider != nil {
		_ = g.provider.Stop()
	}
	for _, consumer := range g.sink.consumers {
		consumer.engine.Stop()
	}
}
func (g *factorLiveGeneration) join() error {
	var errs []error
	if g.installation != nil {
		errs = append(errs, g.installation.Join())
	} else if g.provider != nil {
		g.provider.Join()
	}
	for _, consumer := range g.sink.consumers {
		errs = append(errs, consumer.engine.Join(context.Background()))
	}
	return errors.Join(errs...)
}
func (s *FactorLiveSubscription) fail(g *factorLiveGeneration, err error) {
	if err != nil && s.current.Load() == g {
		s.runtime.failFactorLive(s.provider, s.cancel, s.errors, err)
	}
}
func (s *FactorLiveSubscription) monitor(g *factorLiveGeneration) {
	go func() {
		select {
		case <-g.ctx.Done():
		case err := <-g.installation.Errors():
			s.fail(g, err)
		}
	}()
}

func (s *FactorLiveSubscription) generation(plan *data.SubscriptionPlan, engines []*runner.Live, cfgs []runner.Config, mappers []func(*orm.DataSeries, int64) (factor.VersionRecord, error)) *factorLiveGeneration {
	ctx, cancel := context.WithCancel(s.ctx)
	g := &factorLiveGeneration{owner: s, plan: plan, ctx: ctx, cancel: cancel, sink: &factorsLiveSourceSink{runtime: s.runtime, skipLegacy: true}}
	g.sink.legacyEmit = func(sub *orm.Subscription, rows []*orm.DataSeries, received int64) error {
		if s.legacy != nil {
			return s.legacy.emit(g, sub, rows, received)
		}
		return nil
	}
	for i, engine := range engines {
		streams := map[string]bool{}
		for _, input := range engine.Inputs() {
			streams[input.Source+"/"+input.Frequency] = true
		}
		streams[cfgs[i].Prices.Source+"/"+cfgs[i].Prices.Frequency] = true
		if cfgs[i].Manifest.Costs.FundingPolicy == "required-stream" {
			streams[cfgs[i].FundingSource+"/event"] = true
		}
		g.sink.consumers = append(g.sink.consumers, &factorLiveSourceSink{runtime: s.runtime, engine: engine, cfg: cfgs[i], mapper: mappers[i], fail: func(err error) { s.fail(g, err) }, streams: streams})
		options := plan.Options()
		g.sink.consumers[len(g.sink.consumers)-1].mappings.limit = max(1, options.PrefetchRows, options.PageRows*len(plan.Streams()))
		g.sink.consumers[len(g.sink.consumers)-1].rememberRevision = func(row *orm.DataSeries, record factor.VersionRecord) {
			if g.provider != nil && row.Source == orm.SeriesSourceKline {
				g.provider.RememberSeriesRevision(row, data.LiveSeriesRevision{Revision: record.Revision, EventTime: record.EventTime, SourceVersion: record.SourceVersion, Warmup: row.IsWarmUp})
			}
		}
	}
	return g
}

func validateLiveMappers(engines []*runner.Live, cfgs []runner.Config, mappers []func(*orm.DataSeries, int64) (factor.VersionRecord, error)) error {
	if len(engines) == 0 || len(cfgs) != len(engines) || len(mappers) != len(engines) {
		return errors.New("runtime: aligned realtime source consumers required")
	}
	seen := map[string]bool{}
	for i, engine := range engines {
		if engine == nil || mappers[i] == nil || cfgs[i].StrategyID == "" || seen[cfgs[i].StrategyID] {
			return errors.New("runtime: unique live strategies, engines and mappers required")
		}
		seen[cfgs[i].StrategyID] = true
	}
	return nil
}

// InstallFactorsLive returns the lifetime owner so callers can update a plan.
// Runners supplied here must receive source observations through this owner.
func (r *Runtime) InstallFactorsLive(provider *data.LiveProvider, engines []*runner.Live, cfgs []runner.Config, mappers []func(*orm.DataSeries, int64) (factor.VersionRecord, error)) (*FactorLiveSubscription, error) {
	if provider == nil {
		return nil, errors.New("runtime: realtime provider required")
	}
	if err := validateLiveMappers(engines, cfgs, mappers); err != nil {
		return nil, err
	}
	plan, err := r.CompileFactorsLivePlan(engines, cfgs)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(r.Context())
	s := &FactorLiveSubscription{runtime: r, provider: provider, ctx: ctx, cancel: cancel, errors: make(chan error, 2)}
	s.legacy = newLegacyLiveIngress(r, plan.Options().PrefetchRows, plan.Options().PageRows)
	g := s.generation(plan, engines, cfgs, mappers)
	g.provider = provider
	s.current.Store(g)
	g.installation, err = provider.PrepareSubscriptionPlan(g.ctx, plan, g)
	if err == nil {
		err = g.installation.Activate()
	}
	if err != nil {
		cancel()
		g.stop()
		return nil, errors.Join(err, g.join())
	}
	r.OnClose(s.Stop)
	r.OnCloseWait(func() {
		if err := s.Join(); err != nil {
			s.fail(s.current.Load(), err)
		}
	})
	s.monitor(g)
	return s, nil
}

func replacementConsumers(previous, next *factorLiveGeneration) (map[string]*factorLiveSourceSink, error) {
	old := map[string]*factorLiveSourceSink{}
	for _, consumer := range previous.sink.consumers {
		old[consumer.cfg.StrategyID] = consumer
	}
	if len(old) != len(next.sink.consumers) {
		return nil, errors.New("runtime: subscription update must retain strategy ownership")
	}
	for _, consumer := range next.sink.consumers {
		for _, previousConsumer := range previous.sink.consumers {
			if consumer.engine.SharesComputation(previousConsumer.engine) {
				return nil, errors.New("runtime: replacement generation must own independent computation sessions")
			}
		}
		prior := old[consumer.cfg.StrategyID]
		if prior == nil || prior.engine == consumer.engine || prior.cfg.AccountID != consumer.cfg.AccountID || prior.cfg.Prices != consumer.cfg.Prices || prior.cfg.Manifest.Currency != consumer.cfg.Manifest.Currency || prior.cfg.FundingSource != consumer.cfg.FundingSource || prior.cfg.Manifest.Costs.FundingPolicy != consumer.cfg.Manifest.Costs.FundingPolicy {
			return nil, errors.New("runtime: update requires fresh runners and stable account, price and funding contracts")
		}
		for sid, symbol := range prior.cfg.Snapshot.SIDMap {
			if nextSymbol := consumer.cfg.Snapshot.SIDMap[sid]; nextSymbol != "" && symbol != nextSymbol {
				return nil, fmt.Errorf("runtime: update changes retained SID %d identity", sid)
			}
		}
		for _, sid := range prior.engine.FundingSIDs() {
			if !slices.Contains(consumer.engine.FundingSIDs(), sid) {
				continue
			}
			oldInstrument, oldExists := prior.engine.InstrumentForSID(sid)
			nextInstrument, nextExists := consumer.engine.InstrumentForSID(sid)
			if oldExists != nextExists || oldExists && !reflect.DeepEqual(oldInstrument, nextInstrument) {
				return nil, fmt.Errorf("runtime: update changes retained execution/funding SID %d units", sid)
			}
		}
	}
	return old, nil
}

func protectAccountSubscriptions(snapshot execution.AccountSnapshot, g *factorLiveGeneration) error {
	prices, funding := map[string]bool{}, map[string]bool{}
	strategyPrices, strategyFunding := map[execution.StrategyID]map[string]bool{}, map[execution.StrategyID]map[string]bool{}
	strategyNeedsFunding := map[execution.StrategyID]bool{}
	requiresFunding := false
	for _, consumer := range g.sink.consumers {
		strategy := execution.StrategyID(consumer.cfg.StrategyID)
		strategyPrices[strategy], strategyFunding[strategy] = map[string]bool{}, map[string]bool{}
		for _, sid := range consumer.engine.ExecutionSIDs() {
			if instrument, ok := consumer.engine.InstrumentForSID(sid); ok {
				prices[instrument.ID] = true
				strategyPrices[strategy][instrument.ID] = true
			} else {
				prices[consumer.cfg.Snapshot.SIDMap[sid]] = true
				strategyPrices[strategy][consumer.cfg.Snapshot.SIDMap[sid]] = true
			}
		}
		if consumer.cfg.Manifest.Costs.FundingPolicy == "required-stream" {
			strategyNeedsFunding[strategy] = true
			requiresFunding = true
			for _, sid := range consumer.engine.FundingSIDs() {
				if instrument, ok := consumer.engine.InstrumentForSID(sid); ok {
					funding[instrument.ID] = true
					strategyFunding[strategy][instrument.ID] = true
				} else if symbol := g.owner.runtime.Symbols.GetSymbolByID(sid); symbol != nil {
					funding[symbol.Symbol] = true
					strategyFunding[strategy][symbol.Symbol] = true
				}
			}
		}
	}
	checkStrategy := func(strategy execution.StrategyID, instrument string) error {
		if ownPrices, owned := strategyPrices[strategy]; owned && (!ownPrices[instrument] || strategyNeedsFunding[strategy] && !strategyFunding[strategy][instrument]) {
			return fmt.Errorf("runtime: update cannot remove strategy %s active instrument %s execution/funding membership", strategy, instrument)
		}
		return nil
	}
	protected := map[string]bool{}
	for _, lots := range [][]execution.VirtualLot{snapshot.Lots, snapshot.ActualPositions, snapshot.ExternalPositions} {
		for _, lot := range lots {
			if lot.SignedSteps != 0 {
				if err := checkStrategy(lot.Strategy, lot.Instrument.ID); err != nil {
					return err
				}
				protected[lot.Instrument.ID] = true
			}
		}
	}
	for _, order := range snapshot.Orders {
		if order.State != execution.OrderFilled && order.State != execution.OrderCanceled {
			for _, allocation := range order.Intent.Allocations {
				if allocation.Steps != 0 {
					if err := checkStrategy(allocation.Strategy, order.Intent.Instrument.ID); err != nil {
						return err
					}
				}
			}
			protected[order.Intent.Instrument.ID] = true
		}
	}
	for instrument := range protected {
		if !prices[instrument] || requiresFunding && !funding[instrument] {
			return fmt.Errorf("runtime: update cannot preserve active account instrument %s price/funding subscriptions", instrument)
		}
	}
	return nil
}

// Update prepares fresh runners while the old plan continues. It commits only
// at a complete source-callback boundary and a serialized account snapshot.
// Active execution/funding membership is retained by the account snapshot. The bool
// reports whether the generation committed, even if later activation/join fails.
// Kline corrections require retained typed mapper revision evidence. Corrections
// to derived/coarse buckets fail before cache mutation until the transport can
// supply reliable derived revision identities; equal warmed overlap is ignored.
func (s *FactorLiveSubscription) Update(ctx context.Context, engines []*runner.Live, cfgs []runner.Config, mappers []func(*orm.DataSeries, int64) (factor.VersionRecord, error)) (committed bool, err error) {
	s.updates.Lock()
	defer s.updates.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := s.ctx.Err(); err != nil {
		return false, err
	}
	if err := validateLiveMappers(engines, cfgs, mappers); err != nil {
		return false, err
	}
	previous := s.current.Load()
	if err := s.validateLegacy(); err != nil {
		return false, err
	}
	plan, err := s.runtime.CompileFactorsLivePlan(engines, cfgs)
	if err != nil {
		return false, err
	}
	next := s.generation(plan, engines, cfgs, mappers)
	priorConsumers, err := replacementConsumers(previous, next)
	if err != nil {
		next.cancel()
		return false, err
	}
	// Update owns compatible candidate runners from here, including rollback.
	defer func() {
		if !committed {
			next.stop()
			err = errors.Join(err, next.join())
		}
	}()
	if s.legacy != nil {
		s.legacy.pin()
		defer s.legacy.unpin()
	}
	stopCaller := context.AfterFunc(ctx, next.cancel)
	defer stopCaller()
	if s.runtime.SharedExecution() == nil {
		return false, errors.New("runtime: dynamic subscriptions require serialized shared account ownership")
	}
	// Every replacement producer must provide synchronous readiness and an
	// owned Stop/Join; a legacy asynchronous startup cannot prove a safe swap.
	options := plan.Options()
	options.RequireManagedLive = true
	plan, err = s.runtime.Catalog.CompileSubscriptionPlan(next.ctx, subscriptionPlanRequests(plan), options)
	if err != nil {
		return false, err
	}
	next.plan = plan
	next.provider, err = s.provider.NewGeneration(next.ctx, plan)
	if err != nil {
		return false, err
	}
	next.installation, err = next.provider.PrepareSubscriptionPlan(next.ctx, plan, next)
	if err != nil {
		return false, err
	}
	s.boundary.Lock()
	err = s.runtime.SharedExecution().AtSnapshot(next.ctx, func(snapshot execution.AccountSnapshot) error {
		if err := next.ctx.Err(); err != nil {
			return err
		}
		if err := protectAccountSubscriptions(snapshot, next); err != nil {
			return err
		}
		// A slow preparation crossing another decision grid must retry with
		// newer history rather than install stale indicator state.
		if err := next.sink.WarmupReady(s.runtime.Clock.TimeMS()); err != nil {
			return err
		}
		if err := next.plan.Validate(); err != nil {
			return err
		}
		if err := s.validateLegacy(); err != nil {
			return err
		}
		if s.legacy != nil {
			if err := s.legacy.overlapReady(); err != nil {
				return err
			}
		}
		// Detach before publication. A callback already running may still
		// cancel the candidate, so it cannot become the current generation.
		if !stopCaller() {
			return context.Canceled
		}
		s.lifetime.Lock()
		defer s.lifetime.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := next.ctx.Err(); err != nil {
			return err
		}
		for _, consumer := range next.sink.consumers {
			if err := consumer.engine.InheritAdmission(priorConsumers[consumer.cfg.StrategyID].engine); err != nil {
				return err
			}
		}
		return next.installation.CommitPrepared(func() error {
			s.current.Store(next)
			committed = true
			return nil
		})
	})
	s.boundary.Unlock()
	if err != nil {
		return false, err
	}
	// Candidate context now belongs to the subscription owner, not the caller.
	s.monitor(next)
	err = next.installation.Activate()
	if err != nil {
		next.stop()
		s.fail(next, err)
	}
	previous.stop()
	joinErr := previous.join()
	s.retiredErr = errors.Join(s.retiredErr, joinErr)
	return true, errors.Join(err, joinErr)
}

func subscriptionPlanRequests(plan *data.SubscriptionPlan) []data.SubscriptionRequest {
	var requests []data.SubscriptionRequest
	for _, stream := range plan.Streams() {
		for _, consumer := range stream.Consumers {
			sub := stream.Subscription
			sub.WarmupNum = consumer.WarmupNum
			requests = append(requests, data.SubscriptionRequest{Subscription: sub, Consumer: consumer.Name, Required: consumer.Required, MaxAgeMS: consumer.MaxAgeMS})
		}
	}
	return requests
}
