package runtime

import (
	"errors"
	"fmt"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
)

// InstallTimeSeriesLive runs the shared TS facade without a synthetic factor
// engine. The subscription installation owns bounded warmup and stop/join.
func (r *Runtime) InstallTimeSeriesLive(provider *data.LiveProvider) (*data.SubscriptionInstallation, error) {
	if r == nil || provider == nil || r.sharedOrderBridge == nil || r.factorLegacyTrader == nil || len(r.factorLegacySubs) == 0 {
		return nil, errors.New("runtime: bound shared time-series jobs and subscriptions required")
	}
	var requests []data.SubscriptionRequest
	for _, sub := range r.factorLegacySubs {
		if sub == nil || sub.ExSymbol == nil {
			return nil, errors.New("runtime: time-series live subscription requires symbol")
		}
		symbol := r.Symbols.GetSymbolByID(sub.ExSymbol.ID)
		if symbol == nil || *symbol != *sub.ExSymbol {
			return nil, fmt.Errorf("runtime: time-series live SID %d identity mismatch", sub.ExSymbol.ID)
		}
		requests = append(requests, data.SubscriptionRequest{Subscription: *sub, Consumer: "time-series", Required: true})
	}
	options := r.sourcePlanOptions
	namespace := r.ID
	if r.Storage != nil && r.Storage.Identity() != "" {
		namespace = r.Storage.Identity()
	}
	if options.Namespace != "" {
		namespace += "/namespace/" + options.Namespace
	}
	options.Namespace = namespace
	options.AnchorMS, options.EndMS, options.RequireManagedLive = r.Clock.TimeMS(), r.Clock.TimeMS(), true
	plan, err := r.Catalog.CompileSubscriptionPlan(r.Context(), requests, options)
	if err != nil {
		return nil, err
	}
	sink := &timeSeriesLiveSink{factorsLiveSourceSink: &factorsLiveSourceSink{runtime: r}}
	installation, err := provider.PrepareSubscriptionPlan(r.Context(), plan, sink)
	if err != nil {
		return nil, err
	}
	if err := installation.Activate(); err != nil {
		installation.Stop()
		return nil, errors.Join(err, installation.Join())
	}
	r.OnClose(installation.Stop)
	r.OnCloseWait(func() { _ = installation.Join() })
	return installation, nil
}

type timeSeriesLiveSink struct{ *factorsLiveSourceSink }

func (s *timeSeriesLiveSink) EnvironmentEnded(row *orm.DataSeries) error {
	r := s.runtime
	if !r.EnterCallback() {
		return errors.New("runtime: time-series environment-end intake stopped")
	}
	defer r.LeaveCallback()
	if row == nil || row.IsWarmUp {
		return nil
	}
	manager := biz.GetOdMgrWithState(r.Trading, r.defaultAccount)
	if manager == nil {
		return errors.New("runtime: time-series environment-end manager unavailable")
	}
	if err := manager.OnEnvEnd(row); err != nil {
		var freezeErr error
		if r.sharedExecution != nil {
			freezeErr = r.sharedExecution.FreezeFailure("time-series-environment-end-error", r.Clock.TimeMS())
		}
		r.Stop()
		return errors.Join(err, freezeErr)
	}
	return nil
}
