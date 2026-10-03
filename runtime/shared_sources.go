package runtime

import (
	"errors"
	"fmt"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"sort"
)

type factorLiveSourceSink struct {
	runtime          *Runtime
	engine           *runner.Live
	cfg              runner.Config
	mapper           func(*orm.DataSeries, int64) (factor.VersionRecord, error)
	fail             func(error)
	streams          map[string]bool
	warmupThrough    map[orm.StreamKey]int64
	rememberRevision func(*orm.DataSeries, factor.VersionRecord)
	mappings         preparedSeriesMappings
}

func (s *factorLiveSourceSink) observeAt(sub *strat.DataSub, rows []*orm.DataSeries, received int64) ([]int64, error) {
	if !s.streams[sub.Source+"/"+sub.TimeFrame] {
		return nil, nil
	}
	grids := map[int64]bool{}
	for _, raw := range rows {
		funding := sub.Source == s.cfg.FundingSource && s.engine.HasFundingSID(raw.Sid)
		if !s.engine.HasSID(raw.Sid) && !funding {
			continue
		}
		series := *raw
		record, err := s.mapRecord(&series, received)
		if err == nil {
			err = s.engine.Observe(s.runtime.Context(), record)
		}
		if err != nil {
			s.fail(err)
			return nil, err
		}
		if s.rememberRevision != nil {
			s.rememberRevision(raw, record)
		}
		if s.engine.HasSID(raw.Sid) && raw.Closed && raw.EndMS > s.warmupThrough[sub.Key()] && raw.EndMS%s.cfg.DecisionInterval == 0 && received < raw.EndMS+s.cfg.ExpiryMS {
			grids[raw.EndMS] = true
		}
	}
	ordered := make([]int64, 0, len(grids))
	for grid := range grids {
		ordered = append(ordered, grid)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered, nil
}
func (s *factorLiveSourceSink) flushAt(grids []int64, received int64) error {
	for _, grid := range grids {
		if err := s.engine.FlushAt(s.runtime.Context(), grid, received); err != nil {
			s.fail(err)
			return err
		}
	}
	return nil
}

// CompileFactorLivePlan collects factor, execution, funding and legacy needs
// into one normalized plan before installing any source or warming a feeder.
func (r *Runtime) factorLiveRequests(engine *runner.Live, cfg runner.Config) ([]data.SubscriptionRequest, error) {
	if r == nil || engine == nil || r.Catalog == nil || r.Symbols == nil || r.Clock == nil || cfg.DecisionInterval <= 0 {
		return nil, errors.New("runtime: complete factor realtime source composition required")
	}
	type scopedInput struct {
		input    factor.InputSpec
		sids     []int32
		consumer string
	}
	var inputs []scopedInput
	for _, input := range engine.Inputs() {
		inputs = append(inputs, scopedInput{input, engine.DataSIDs(), "factor:" + cfg.StrategyID})
	}
	inputs = append(inputs, scopedInput{factor.InputSpec{Source: cfg.Prices.Source, Frequency: cfg.Prices.Frequency, Fields: []string{cfg.Prices.Field}}, engine.ExecutionSIDs(), "execution-price:" + cfg.StrategyID})
	if cfg.Manifest.Costs.FundingPolicy == "required-stream" {
		inputs = append(inputs, scopedInput{factor.InputSpec{Source: cfg.FundingSource, Frequency: "event", Fields: []string{"rate", "mark", "account_amount", "settlement_id"}}, engine.FundingSIDs(), "execution-funding:" + cfg.StrategyID})
	}
	var requests []data.SubscriptionRequest
	for _, scope := range inputs {
		input := scope.input
		if input.Source != orm.SeriesSourceKline {
			source := r.Catalog.GetDataSource(input.Source)
			if source == nil || source.Info() == nil {
				return nil, fmt.Errorf("runtime: factor live source %s is not registered", input.Source)
			}
			if source.Info().TimeFrame != input.Frequency {
				return nil, fmt.Errorf("runtime: factor live source frequency mismatch: %s", input.Source)
			}
		}
		for _, sid := range scope.sids {
			symbol := r.Symbols.GetSymbolByID(sid)
			accountFunding := input.Source == cfg.FundingSource && engine.HasFundingSID(sid)
			if symbol == nil || !accountFunding && cfg.Snapshot.SIDMap[sid] != symbol.Symbol {
				return nil, fmt.Errorf("runtime: factor live SID %d identity mismatch", sid)
			}
			requests = append(requests, data.SubscriptionRequest{Subscription: data.Subscription{Source: input.Source, TimeFrame: input.Frequency, ExSymbol: symbol, WarmupNum: input.WarmupLength, Fields: input.Fields}, Consumer: scope.consumer, Required: true, MaxAgeMS: input.MaxAge})
		}
	}
	for _, legacy := range r.FactorLegacySubscriptions() {
		if legacy == nil || legacy.ExSymbol == nil {
			return nil, errors.New("runtime: legacy live subscription requires symbol")
		}
		symbol := r.Symbols.GetSymbolByID(legacy.ExSymbol.ID)
		if symbol == nil || *symbol != *legacy.ExSymbol {
			return nil, fmt.Errorf("runtime: legacy live SID %d identity mismatch", legacy.ExSymbol.ID)
		}
		requests = append(requests, data.SubscriptionRequest{Subscription: *legacy, Consumer: "legacy-time-series", Required: true})
	}
	return requests, nil
}

// SubscribeFactorLive installs the complete compiled kline/side-source plan.
// The runtime owns its stop and join phases and receives source failures.
func (r *Runtime) CompileFactorLivePlan(engine *runner.Live, cfg runner.Config) (*data.SubscriptionPlan, error) {
	return r.CompileFactorsLivePlan([]*runner.Live{engine}, []runner.Config{cfg})
}

func (r *Runtime) CompileFactorsLivePlan(engines []*runner.Live, cfgs []runner.Config) (*data.SubscriptionPlan, error) {
	if r == nil || len(engines) == 0 || len(engines) != len(cfgs) {
		return nil, errors.New("runtime: aligned factor realtime consumers required")
	}
	var requests []data.SubscriptionRequest
	strict := false
	for i, engine := range engines {
		items, err := r.factorLiveRequests(engine, cfgs[i])
		if err != nil {
			return nil, err
		}
		requests = append(requests, items...)
		strict = strict || cfgs[i].Mode == runner.Trade
	}
	namespace := r.ID
	if r.Storage != nil && r.Storage.Identity() != "" {
		namespace = r.Storage.Identity()
	}
	now := r.Clock.TimeMS()
	options := r.sourcePlanOptions
	if options.Namespace != "" {
		namespace += "/namespace/" + options.Namespace
	}
	options.Namespace = namespace
	options.AnchorMS, options.EndMS, options.RequireManagedLive = now, now, strict
	return r.Catalog.CompileSubscriptionPlan(r.Context(), requests, options)
}

func (r *Runtime) SubscribeFactorLive(provider *data.LiveProvider, engine *runner.Live, cfg runner.Config, mapper func(*orm.DataSeries, int64) (factor.VersionRecord, error)) (<-chan error, error) {
	return r.SubscribeFactorsLive(provider, []*runner.Live{engine}, []runner.Config{cfg}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){mapper})
}

type factorsLiveSourceSink struct {
	runtime    *Runtime
	consumers  []*factorLiveSourceSink
	skipLegacy bool
	legacyEmit func(*orm.Subscription, []*orm.DataSeries, int64) error
}

func (s *factorsLiveSourceSink) WarmupReady(anchorMS int64) error {
	for _, consumer := range s.consumers {
		if err := consumer.engine.ValidateWarmup(anchorMS); err != nil {
			return fmt.Errorf("runtime: factor %s warmup readiness: %w", consumer.cfg.StrategyID, err)
		}
	}
	return nil
}

func (s *factorsLiveSourceSink) Warmup(sub *strat.DataSub, rows []*orm.DataRecord) error {
	return s.WarmupSeries(sub, sourceRecordSeries(sub, rows, true))
}

func sourceRecordSeries(sub *orm.Subscription, rows []*orm.DataRecord, warm bool) []*orm.DataSeries {
	result := make([]*orm.DataSeries, len(rows))
	if sub == nil {
		return result
	}
	for i, raw := range rows {
		if raw != nil {
			result[i] = &orm.DataSeries{Source: sub.Source, Sid: raw.Sid, TimeMS: raw.TimeMS, EndMS: raw.EndMS, TimeFrame: sub.TimeFrame, Closed: raw.Closed, Values: raw.Values, ExSymbol: sub.ExSymbol, IsWarmUp: warm}
		}
	}
	return result
}

func (s *factorsLiveSourceSink) WarmupSeries(sub *strat.DataSub, rows []*orm.DataSeries) error {
	if sub == nil || sub.ExSymbol == nil {
		return errors.New("runtime: invalid shared warmup subscription")
	}
	if !s.runtime.EnterCallback() {
		return errors.New("runtime: shared source intake stopped")
	}
	defer s.runtime.LeaveCallback()
	received := s.runtime.Clock.TimeMS()
	for _, consumer := range s.consumers {
		if !consumer.engine.HasSID(sub.ExSymbol.ID) {
			continue
		}
		for _, input := range consumer.engine.Inputs() {
			if input.Source != sub.Source || input.Frequency != sub.TimeFrame || input.WarmupLength == 0 {
				continue
			}
			start := max(0, len(rows)-input.WarmupLength)
			for _, raw := range rows[start:] {
				if raw == nil || raw.Sid != sub.ExSymbol.ID {
					return errors.New("runtime: source emitted foreign or nil warmup row")
				}
				series := *raw
				series.IsWarmUp = true
				record, err := consumer.mapper(&series, received)
				if err == nil && (record.IngestedAt != received || record.AvailableAt > received || record.EventTime > received) {
					err = errors.New("runtime: factor warmup invalid publication/reception time")
				}
				if err == nil {
					err = consumer.engine.Warmup(s.runtime.Context(), record)
				}
				if err != nil {
					return err
				}
				if consumer.rememberRevision != nil {
					consumer.rememberRevision(raw, record)
				}
				if consumer.warmupThrough == nil {
					consumer.warmupThrough = make(map[orm.StreamKey]int64)
				}
				consumer.warmupThrough[sub.Key()] = max(consumer.warmupThrough[sub.Key()], raw.EndMS)
			}
		}
	}
	if !s.skipLegacy {
		for _, raw := range rows {
			if err := s.runtime.feedFactorLegacy(raw); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *factorsLiveSourceSink) Emit(sub *strat.DataSub, rows []*orm.DataRecord) error {
	return s.EmitSeries(sub, sourceRecordSeries(sub, rows, false))
}

func (s *factorsLiveSourceSink) EmitSeries(sub *strat.DataSub, rows []*orm.DataSeries) error {
	if sub == nil || sub.ExSymbol == nil {
		return errors.New("runtime: invalid shared live source subscription")
	}
	if !s.runtime.EnterCallback() {
		return errors.New("runtime: shared source intake stopped")
	}
	defer s.runtime.LeaveCallback()
	for _, raw := range rows {
		if raw == nil || raw.Sid != sub.ExSymbol.ID {
			return errors.New("runtime: source emitted foreign or nil row")
		}
	}
	grids := make([][]int64, len(s.consumers))
	received := s.runtime.Clock.TimeMS()
	for i, consumer := range s.consumers {
		var err error
		grids[i], err = consumer.observeAt(sub, rows, received)
		if err != nil {
			return err
		}
	}
	if s.legacyEmit != nil {
		if err := s.legacyEmit(sub, rows, received); err != nil {
			return err
		}
	} else if !s.skipLegacy {
		for _, raw := range rows {
			if err := s.runtime.feedFactorLegacy(raw); err != nil {
				return err
			}
		}
	}
	for i, consumer := range s.consumers {
		if err := consumer.flushAt(grids[i], received); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) SubscribeFactorsLive(provider *data.LiveProvider, engines []*runner.Live, cfgs []runner.Config, mappers []func(*orm.DataSeries, int64) (factor.VersionRecord, error)) (<-chan error, error) {
	installation, err := r.InstallFactorsLive(provider, engines, cfgs, mappers)
	if err != nil {
		return nil, err
	}
	return installation.Errors(), nil
}
