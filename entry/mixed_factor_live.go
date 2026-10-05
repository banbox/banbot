package entry

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func mixedLiveAccountConfig(cfg *config.Config, account string, policies []*config.RunPolicyConfig) *config.Config {
	cfg = cfg.Clone()
	cfg.Accounts = map[string]*config.AccountConfig{account: cfg.Accounts[account]}
	cfg.RunPolicy = nil
	for _, p := range policies {
		cfg.RunPolicy = append(cfg.RunPolicy, p.Clone())
	}
	return cfg
}
func mixedLivePolicies(spec *config.RunSpec, account string) []*config.RunPolicyConfig {
	if spec == nil {
		return nil
	}
	u := spec.Config()
	var result []*config.RunPolicyConfig
	for _, p := range u.RunPolicy {
		if p.Engine == config.EngineTimeSeries && mixedLivePolicyAccount(u, p, account) {
			result = append(result, p.RunPolicyConfig.Clone())
		}
	}
	return result
}

func mixedLivePolicyAccount(u *config.UnifiedConfig, policy *config.PolicyV2, account string) bool {
	accounts, err := u.PolicyAccounts(policy)
	return err == nil && slices.Contains(accounts, account)
}
func (s *explicitEntrySession) prepareMixedLiveBinding(snapshot *config.Snapshot, c runner.Config, binding *FactorLiveBinding) (map[execution.StrategyID]decimal.Decimal, error) {
	capital := map[execution.StrategyID]decimal.Decimal{}
	binding.Symbols = maps.Clone(binding.Symbols)
	policies := mixedLivePolicies(s.runSpec, c.AccountID)
	if len(policies) == 0 {
		return capital, nil
	}
	if binding.Legacy != nil {
		if binding.Legacy.automatic {
			return binding.Legacy.capital, nil
		}
		if binding.Legacy.Bridge == nil {
			return nil, fmt.Errorf("mixed live: custom legacy bridge is missing")
		}
		u := s.runSpec.Config()
		for _, policy := range u.RunPolicy {
			if policy.Engine != config.EngineTimeSeries || !mixedLivePolicyAccount(u, policy, c.AccountID) {
				continue
			}
			id := policy.ID
			if id == "" {
				id = policy.RunPolicyConfig.ID()
			}
			bound, ok := binding.Legacy.Bridge.Strategies[policy.RunPolicyConfig.ID()]
			found := false
			for _, job := range binding.Legacy.Jobs {
				if job != nil && job.Strat != nil && job.Strat.Name == policy.RunPolicyConfig.ID() {
					found = true
				}
			}
			if !ok || bound.ID != execution.StrategyID(id) || !found {
				return nil, fmt.Errorf("mixed live: custom legacy binding omits configured TS policy %s", id)
			}
			nav := c.AccountInitialNAV
			if nav <= 0 {
				nav = c.InitialNAV
			}
			capital[bound.ID] = decimal.NewFromFloat(nav * policyCapitalWeight(u, policy, c.AccountID))
		}
		return capital, nil
	}
	e := c.Execution
	bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{}, Strategies: map[string]biz.SharedStrategyBinding{}, IntentTTLMS: c.ExpiryMS, Risk: execution.PortfolioRisk{MarginRate: e.MarginRate, MaxAccountMargin: e.MaxAccountMargin, MaxVirtualGross: e.MaxVirtualGross, StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{}}}
	for _, unit := range e.Instruments {
		bridge.Instruments[unit.ID] = unit
	}
	cfg := snapshot.View()
	pairs := slices.Clone(cfg.Pairs)
	for _, p := range policies {
		pairs = append(pairs, p.Pairs...)
	}
	sort.Strings(pairs)
	pairs = slices.Compact(pairs)
	if len(pairs) == 0 {
		for _, symbol := range binding.Symbols {
			pairs = append(pairs, symbol.Symbol)
		}
		sort.Strings(pairs)
	}
	for _, pair := range pairs {
		unit, ok := bridge.Instruments[pair]
		if !ok {
			var err error
			unit, err = mixedReplayInstrument(s.exchange, pair, c.Manifest.Currency)
			if err != nil {
				return nil, err
			}
			bridge.Instruments[pair] = unit
		}
		found := false
		for _, existing := range binding.AccountInstruments {
			if existing.Symbol == pair {
				found = true
				if !sameFactorInstrument(existing.Instrument, unit) {
					return nil, fmt.Errorf("mixed live: conflicting instrument %s", pair)
				}
				break
			}
		}
		if !found {
			binding.AccountInstruments = append(binding.AccountInstruments, execution.BanexgInstrument{Symbol: pair, Instrument: unit})
		}
	}
	u := s.runSpec.Config()
	for _, p := range u.RunPolicy {
		if p.Engine != config.EngineTimeSeries || !mixedLivePolicyAccount(u, p, c.AccountID) {
			continue
		}
		if _, ok := strat.GetStrategyFactory(p.Name); !ok {
			return nil, fmt.Errorf("mixed live: TS strategy %s is not registered", p.Name)
		}
		id := p.ID
		if id == "" {
			id = p.RunPolicyConfig.ID()
		}
		nav := c.AccountInitialNAV
		if nav <= 0 {
			nav = c.InitialNAV
		}
		amount := decimal.NewFromFloat(nav * policyCapitalWeight(u, p, c.AccountID))
		if !amount.IsPositive() {
			return nil, fmt.Errorf("mixed live: TS strategy %s requires positive capital", id)
		}
		stake := decimal.NewFromFloat(cfg.StakePct / 100)
		if stake.IsZero() && cfg.StakeAmount > 0 {
			stake = decimal.NewFromFloat(cfg.StakeAmount).Div(amount)
		}
		sid := execution.StrategyID(id)
		bridge.Strategies[p.RunPolicyConfig.ID()] = biz.SharedStrategyBinding{ID: sid, StakeNAVFraction: stake, MaxNotional: e.StrategyGrossLimit}
		bridge.Risk.StrategyGrossLimits[sid] = e.StrategyGrossLimit
		capital[sid] = amount
	}
	binding.Legacy = &FactorLegacyLiveBinding{Bridge: bridge, automatic: true, capital: capital}
	return capital, nil
}
func loadMixedLiveJobs(rt *runtimepkg.Runtime, binding *FactorLiveBinding) error {
	cfg := rt.BizDeps().Config.View()
	pairs := slices.Clone(cfg.Pairs)
	for _, p := range cfg.RunPolicy {
		pairs = append(pairs, p.Pairs...)
	}
	if len(pairs) == 0 {
		for _, symbol := range binding.Symbols {
			pairs = append(pairs, symbol.Symbol)
		}
	}
	sort.Strings(pairs)
	pairs = slices.Compact(pairs)
	scores := mixedTimeFrameScores(rt, pairs)
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	if _, _, err := strat.LoadStratJobsWithState(rt.Strategies, rt.Core, rt.Symbols, pairs, scores, rt.Orders); err != nil {
		return err
	}
	binding.Legacy.Jobs = rt.Strategies.CollectJobs()
	for _, job := range binding.Legacy.Jobs {
		binding.Symbols[job.Symbol.ID] = job.Symbol
		binding.Legacy.Subscriptions = append(binding.Legacy.Subscriptions, &strat.DataSub{Source: orm.SeriesSourceKline, ExSymbol: job.Symbol, TimeFrame: job.TimeFrame, WarmupNum: job.Strat.WarmupNum})
		binding.Legacy.Subscriptions = append(binding.Legacy.Subscriptions, strat.CollectDataSubsWithSymbolState(rt.Symbols, job)...)
	}
	for _, sub := range binding.Legacy.Subscriptions {
		if sub.ExSymbol != nil {
			binding.Symbols[sub.ExSymbol.ID] = sub.ExSymbol
		}
	}
	return rt.BindFactorLegacyJobs(binding.Legacy.Jobs, binding.Legacy.Subscriptions)
}
func (s *explicitEntrySession) runMixedLiveTSAccount(ctx context.Context, snapshot *config.Snapshot, account string, startup live.CryptoTraderStartupFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cfg := mixedLiveAccountConfig(snapshot.View(), account, mixedLivePolicies(s.runSpec, account))
	scoped := config.NewSnapshotWithDirs(cfg, snapshot.DataDir, snapshot.StrategyDir, snapshot.Location())
	rt, err := s.newStorageRuntimeContext(ctx, scoped, core.RunModeLive, 0)
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	trader, err := live.NewCryptoTraderWithRuntimeDeps(rt.BizDeps(), startup)
	if err != nil {
		return err
	}
	if err := trader.Run(); err != nil {
		return err
	}
	return nil
}
