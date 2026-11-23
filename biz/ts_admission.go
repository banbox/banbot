package biz

import (
	"errors"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/utils"
)

// timeSeriesJobPolicies returns the run policies of the jobs bound to one
// symbol and timeframe, keyed by strategy name. The caller guards jobs.
func timeSeriesJobPolicies(jobs map[sharedJobIdentity]*strat.StratJob, symbol, tf string) map[string]*config.RunPolicyConfig {
	policies := make(map[string]*config.RunPolicyConfig)
	for key, job := range jobs {
		if key.Symbol == symbol && key.TimeFrame == tf && job.Strat != nil && job.Strat.Policy != nil {
			policies[key.Strategy] = job.Strat.Policy
		}
	}
	return policies
}

// admitTimeSeriesEntry applies the account and strategy open-order limits
// (max_open_orders, max_simul_open and per-policy MaxOpen/MaxSimulOpen) to one
// entry request of an execution-backed time-series manager. state supplies the
// known lots and the admission counters of the current bar round; an accepted
// request updates state.Admission, which the caller persists with the request.
// jobPolicies are the policies of the bound jobs for this symbol and timeframe.
func admitTimeSeriesEntry(deps RuntimeDeps, jobPolicies map[string]*config.RunPolicyConfig, state *sharedTSCheckpoint, snapshot execution.AccountSnapshot, exs *orm.ExSymbol, tf string, req *strat.EnterReq) (bool, error) {
	// Admission state is staged with the request checkpoint and serialized by
	// the account owner. Rejected or failed requests do not spend bar capacity.
	counts := make(map[string]int)
	held := make(map[[2]string]int64)
	for _, lot := range snapshot.Lots {
		held[[2]string{string(lot.Strategy), string(lot.ID)}] = lot.SignedSteps
	}
	openNum := 0
	for _, record := range state.Orders {
		pending := !record.Canceled && record.Entry.State != execution.Expired && record.Entry.State != execution.Canceled && record.Entry.QuantitySteps > record.Entry.FilledSteps
		if held[[2]string{string(record.Strategy), string(record.Lot)}] != 0 || pending {
			openNum++
			counts[record.StrategyName]++
		}
	}
	policies := make(map[string]*config.RunPolicyConfig)
	if deps.Config != nil && deps.Config.View() != nil {
		for _, policy := range deps.Config.View().RunPolicy {
			if policy != nil {
				policies[policy.Name] = policy
			}
		}
	}
	for name, policy := range jobPolicies {
		policies[name] = policy
	}
	previous := state.Admission
	gate := OrderMgr{Account: deps.DefaultAccount, clock: deps.Clock, runtimeCore: deps.Core, runtimeDeps: true, walletDeps: deps, runtimeCfg: makeRuntimeOrderConfig(deps, deps.DefaultAccount), BarMS: previous.BarMS, simulOpen: previous.SimulOpen, simulOpenSt: previous.Strategies}
	limits := sharedEntryLimits{MaxOpen: gate.maxOpenOrders(), MaxSimul: gate.maxSimulOpen()}
	if previous.AccountLimits != nil && *previous.AccountLimits != limits {
		return false, errors.New("biz: shared account TS admission limits changed; explicit policy update required")
	}
	policyLimits := previous.PolicyLimits
	if policyLimits == nil {
		policyLimits = make(map[string]sharedEntryLimits)
	}
	declared := policies[req.StratName]
	if declared == nil && deps.Strategies != nil {
		if strategy := deps.Strategies.Get(exs.Symbol, req.StratName); strategy != nil {
			declared = strategy.Policy
		}
	}
	if saved, exists := policyLimits[req.StratName]; exists {
		if declared != nil && saved != (sharedEntryLimits{MaxOpen: declared.MaxOpen, MaxSimul: declared.MaxSimulOpen}) {
			return false, errors.New("biz: shared strategy TS admission limits changed; explicit policy update required")
		}
		policies[req.StratName] = &config.RunPolicyConfig{MaxOpen: saved.MaxOpen, MaxSimulOpen: saved.MaxSimul}
	} else if declared != nil {
		policyLimits[req.StratName] = sharedEntryLimits{MaxOpen: declared.MaxOpen, MaxSimul: declared.MaxSimulOpen}
	}
	if gate.simulOpenSt == nil {
		gate.simulOpenSt = make(map[string]int)
	}
	barMS := deps.Clock.TimeMS()
	if secs, err := utils.TFToSecSafe(tf); err == nil && secs > 0 {
		if barMS >= 100000000000 {
			barMS = utils.AlignTfMSecs(barMS, int64(secs)*1000)
		} else {
			barMS = barMS / (int64(secs) * 1000) * (int64(secs) * 1000)
		}
	}
	// Short fixture clocks and irregular events still have a valid first round.
	barMS = max(int64(1), barMS)
	allowed, _ := gate.allowOrderEnterWithCounts(exs, tf, []*strat.EnterReq{req}, openNum, counts, policies, barMS)
	if len(allowed) == 0 {
		return false, nil
	}
	state.Admission = sharedEntryAdmission{BarMS: gate.BarMS, SimulOpen: gate.simulOpen, Strategies: gate.simulOpenSt, AccountLimits: &limits, PolicyLimits: policyLimits}
	return true, nil
}

// timeSeriesAdmissionPolicy converts max_open_orders / max_simul_open into the
// account admission policy registered with the account owner. The owner is the
// authority across robots; the robot-side check stays as the early filter.
func timeSeriesAdmissionPolicy(deps RuntimeDeps) execution.AdmissionPolicy {
	gate := OrderMgr{Account: deps.DefaultAccount, runtimeCore: deps.Core, runtimeDeps: true, walletDeps: deps, runtimeCfg: makeRuntimeOrderConfig(deps, deps.DefaultAccount)}
	return execution.AdmissionPolicy{MaxOpenOrders: max(0, gate.maxOpenOrders()), MaxSimulOpen: max(0, gate.maxSimulOpen())}
}
