package runtime

import (
	"context"
	"errors"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

func (r *Runtime) failFactorLive(provider *data.LiveProvider, cancel context.CancelFunc, failures chan<- error, err error) {
	if err == nil {
		return
	}
	freezeErr := r.sharedExecution.FreezeFailure("factor-source-error", r.Clock.TimeMS())
	select {
	case failures <- errors.Join(err, freezeErr):
	default:
	}
	if cancel != nil {
		cancel()
	}
	if provider != nil {
		if stopErr := provider.Stop(); stopErr != nil {
			select {
			case failures <- errors.Join(err, freezeErr, stopErr):
			default:
			}
		}
	}
	r.Stop()
}

// NewFactorLiveProvider binds realtime source ingestion to this Runtime's
// clock, source catalog, symbol state and callback admission. Provider lifecycle
// is registered by data against the same Runtime; it is not an archive runner.
// The consumer must stamp actual publication/reception times and use a real
// visible bid/ask quote for live internal matching.
func (r *Runtime) NewFactorLiveProvider(consume data.FnDataSeries, end data.FuncEnvEnd) (*data.LiveProvider, *errs.Error) {
	if r == nil || !r.sharedMarketData || r.sharedExecution == nil || r.Exchange == nil || consume == nil || r.Core.BackTestMode {
		return nil, errs.NewMsg(core.ErrBadConfig, "factor live provider requires explicit shared realtime market-data composition")
	}
	return data.NewLiveProviderWithRuntimeDeps(r.DataDeps(), consume, end)
}

// BindFactorLive connects provider callbacks to the bounded Live engine. The
// record mapper supplies source revision/publication metadata explicitly; the
// receipt timestamp is captured from the Runtime realtime clock, never from an
// archive cursor. Failures close intake and are available to the entry layer.
func (r *Runtime) BindFactorLive(engine *runner.Live, record func(*orm.DataSeries, int64) (factor.VersionRecord, error), decisionInterval int64) (*data.LiveProvider, <-chan error, *errs.Error) {
	return r.BindFactorsLive([]*runner.Live{engine}, record, []int64{decisionInterval})
}

// BindFactorsLive delivers one provider observation to independent strategy
// consumers. Legacy jobs receive it once, while each factor keeps its own
// clock grid, budget, portfolio and callbacks.
func (r *Runtime) BindFactorsLive(engines []*runner.Live, record func(*orm.DataSeries, int64) (factor.VersionRecord, error), intervals []int64) (*data.LiveProvider, <-chan error, *errs.Error) {
	if len(engines) == 0 || len(engines) != len(intervals) || record == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "factor live engine and version mapper required")
	}
	for i, engine := range engines {
		if engine == nil || intervals[i] <= 0 {
			return nil, nil, errs.NewMsg(core.ErrBadConfig, "factor live engine and interval required")
		}
	}
	failures := make(chan error, 2)
	var provider *data.LiveProvider
	fail := func(err error) {
		r.failFactorLive(provider, nil, failures, err)
	}
	consume := r.factorsLiveConsumer(engines, record, intervals, fail)
	end := func(series *orm.DataSeries) {
		if series != nil && !series.IsWarmUp && series.Closed {
			cutoff := r.Clock.TimeMS()
			for i, engine := range engines {
				if series.EndMS%intervals[i] == 0 {
					fail(engine.FlushAt(r.Core.Context(), series.EndMS, cutoff))
				}
			}
		}
	}
	var err *errs.Error
	provider, err = r.NewFactorLiveProvider(consume, end)
	if err != nil {
		return nil, nil, err
	}
	for _, engine := range engines {
		r.OnClose(engine.Stop)
	}
	return provider, failures, nil
}

func (r *Runtime) factorLiveConsumer(engine *runner.Live, record func(*orm.DataSeries, int64) (factor.VersionRecord, error), decisionInterval int64, fail func(error)) data.FnDataSeries {
	return r.factorsLiveConsumer([]*runner.Live{engine}, record, []int64{decisionInterval}, fail)
}

func (r *Runtime) factorsLiveConsumer(engines []*runner.Live, record func(*orm.DataSeries, int64) (factor.VersionRecord, error), intervals []int64, fail func(error)) data.FnDataSeries {
	return func(series *orm.DataSeries) {
		if series == nil {
			return
		}
		if err := r.feedFactorLegacy(series); err != nil {
			fail(err)
			return
		}
		needed := false
		for _, engine := range engines {
			needed = needed || engine.HasSID(series.Sid)
		}
		if !needed {
			return
		}
		received := r.Clock.TimeMS()
		row, err := record(series, received)
		if err == nil && (row.IngestedAt != received || row.AvailableAt > received || row.EventTime > received) {
			err = errors.New("runtime: factor live record has invalid receipt/publication time")
		}
		if err != nil {
			fail(err)
			return
		}
		for _, engine := range engines {
			if !engine.HasSID(series.Sid) {
				continue
			}
			if series.IsWarmUp {
				err = engine.Warmup(r.Core.Context(), row)
			} else {
				err = engine.Observe(r.Core.Context(), row)
			}
			if err != nil {
				fail(err)
				return
			}
		}
		// OnEnvEnd denotes feeder adjustment boundaries, not every closed live
		// observation. Repeated incomplete attempts wait for the other SIDs.
		if !series.IsWarmUp && series.Closed && series.EndMS > 0 {
			for i, engine := range engines {
				if engine.HasSID(series.Sid) && series.EndMS%intervals[i] == 0 {
					if err = engine.FlushAt(r.Core.Context(), series.EndMS, received); err != nil {
						fail(err)
						return
					}
				}
			}
		}
		fail(err)
	}
}
