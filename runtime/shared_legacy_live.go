package runtime

import (
	"errors"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func (r *Runtime) FactorLegacySubscriptions() []*strat.DataSub {
	return append([]*strat.DataSub(nil), r.factorLegacySubs...)
}

// BindFactorLegacyJobs restores the real TS facade and publishes configured
// jobs into the same Trader registry used by ordinary runtime callbacks.
func (r *Runtime) BindFactorLegacyJobs(jobs []*strat.StratJob, subs []*strat.DataSub) error {
	if r.sharedOrderBridge == nil || len(jobs) == 0 || len(subs) == 0 {
		return errors.New("runtime: explicit legacy bridge/jobs/subscriptions required")
	}
	for _, sub := range subs {
		if sub == nil || sub.ExSymbol == nil || sub.TimeFrame == "" {
			return errors.New("runtime: invalid legacy live subscription")
		}
		if sub.Source == "" {
			sub.Source = orm.SeriesSourceKline
		}
	}
	groups := map[string]map[string]*strat.StratJob{}
	infos := map[string]map[string]*strat.StratJob{}
	for _, job := range jobs {
		if job == nil || job.Strat == nil || job.Symbol == nil || job.Account != r.defaultAccount || job.TimeFrame == "" {
			return errors.New("runtime: incomplete legacy live job")
		}
		for _, sub := range subs {
			if sub.Source == orm.SeriesSourceKline && sub.ExSymbol.ID == job.Symbol.ID && sub.TimeFrame == job.TimeFrame && job.Strat.OnBar != nil && job.Env == nil {
				return errors.New("runtime: primary legacy kline job requires configured environment")
			}
		}
		if _, ok := r.sharedOrderBridge.Strategies[job.Strat.Name]; !ok {
			return errors.New("runtime: unknown legacy strategy binding")
		}
		symbol := r.Symbols.GetSymbolByID(job.Symbol.ID)
		if symbol == nil || symbol.Symbol != job.Symbol.Symbol {
			return errors.New("runtime: legacy job symbol not bound")
		}
		job.BindRuntimeState(r.Strategies, r.Core, r.Clock)
		job.BindRuntimeMarket(r.Market.Prices, r.Clock)
		key := job.Symbol.Symbol + "_" + job.TimeFrame
		if groups[key] == nil {
			groups[key] = map[string]*strat.StratJob{}
		}
		if groups[key][job.Strat.Name] != nil {
			return errors.New("runtime: duplicate legacy job")
		}
		groups[key][job.Strat.Name] = job
		if job.Env != nil {
			r.Strategies.SetEnv(key, job.Env)
		}
		if job.DataHub == nil {
			job.DataHub = strat.NewDataHub()
		}
		jobSubs := subs
		if job.Strat.OnDataSubs != nil {
			jobSubs = job.Strat.OnDataSubs(job)
		}
		job.DataHub.Configure(jobSubs)
		for _, sub := range jobSubs {
			if sub == nil || sub.ExSymbol == nil || sub.TimeFrame == "" {
				return errors.New("runtime: invalid legacy job subscription")
			}
			declared := false
			for _, live := range subs {
				if orm.NormalizeSeriesSource(live.Source) == orm.NormalizeSeriesSource(sub.Source) && live.ExSymbol.ID == sub.ExSymbol.ID && live.TimeFrame == sub.TimeFrame {
					declared = true
					break
				}
			}
			if !declared {
				return errors.New("runtime: legacy job subscription omitted from live binding")
			}
			key := strat.DataSubKey(sub.Source, sub.ExSymbol.ID, sub.TimeFrame)
			if infos[key] == nil {
				infos[key] = map[string]*strat.StratJob{}
			}
			// A reference stream can be shared by jobs of the same strategy at
			// different primary timeframes; retain each observer's own identity.
			observerKey := strat.DataSubKey(job.Strat.Name, job.Symbol.ID, job.TimeFrame)
			infos[key][observerKey] = job
		}
	}
	for key, jobs := range groups {
		r.Strategies.SetJobMap(r.defaultAccount, key, jobs)
	}
	for key, jobs := range infos {
		r.Strategies.SetInfoJobMap(r.defaultAccount, key, jobs)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(r.BizDeps(), nil, false)
	manager, ok := biz.GetOdMgrWithState(r.Trading, r.defaultAccount).(*biz.SharedOrderMgr)
	if !ok {
		return errors.New("runtime: legacy shared facade unavailable")
	}
	if err := manager.BindJobs(jobs); err != nil {
		return err
	}
	trader, err := biz.NewTraderWithRuntimeDeps(r.BizDeps())
	if err != nil {
		return err
	}
	r.factorLegacyTrader = &trader
	r.factorLegacySubs = append([]*strat.DataSub(nil), subs...)
	return nil
}

func (r *Runtime) feedFactorLegacy(series *orm.DataSeries) error {
	if r.factorLegacyTrader == nil || series == nil {
		return nil
	}
	matched := false
	for _, sub := range r.factorLegacySubs {
		if sub.Source == series.Source && sub.ExSymbol.ID == series.Sid && sub.TimeFrame == series.TimeFrame {
			matched = true
			break
		}
	}
	if !matched {
		return nil
	}
	if !series.IsWarmUp {
		manager := biz.GetOdMgrWithState(r.Trading, r.defaultAccount)
		if err := manager.UpdateByDataSeries(nil, series); err != nil {
			return err
		}
	}
	if err := r.factorLegacyTrader.FeedDataSeries(series); err != nil {
		return err
	}
	return nil
}
