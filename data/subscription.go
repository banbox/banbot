package data

import (
	"context"
	"fmt"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

// Subscription describes a data dependency without creating a strategy job.
// Fields retain their source types; SeriesFields selects only numeric views.
type Subscription = orm.Subscription

// NormalizeSubscriptions validates, unions fields and takes the maximum warmup
// for each source/SID/timeframe. It never changes the caller's subscriptions.
func (c *DataSourceCatalog) NormalizeSubscriptions(subs []Subscription) ([]Subscription, error) {
	seen := make(map[string]*orm.Subscription)
	for _, sub := range subs {
		normalized, err := validateBootstrapSubWithCatalog(c, &sub)
		if err != nil {
			return nil, err
		}
		if normalized.Source != orm.SeriesSourceKline {
			src := c.GetDataSource(normalized.Source)
			if src == nil {
				return nil, fmt.Errorf("data source %q is not registered", normalized.Source)
			}
			if normalized.TimeFrame != src.Info().TimeFrame {
				return nil, fmt.Errorf("sub timeframe %s does not match source timeframe %s", normalized.TimeFrame, src.Info().TimeFrame)
			}
		}
		if normalized.Frequency != orm.FrequencyEvent {
			if _, err := ThirdPartyWarmupStart([]*orm.Subscription{normalized}, 0); err != nil {
				return nil, err
			}
		}
		mergeDataSub(seen, normalized)
	}
	var result []Subscription
	for _, sub := range sortedDataSubs(seen) {
		result = append(result, Subscription(*sub))
	}
	return result, nil
}

func legacySubscriptions(subs []Subscription) []*orm.Subscription {
	result := make([]*orm.Subscription, 0, len(subs))
	for _, sub := range subs {
		cp := sub
		result = append(result, &cp)
	}
	return result
}

// EnsureSubscriptionsRange reuses the existing source/coverage path.
func (c *DataSourceCatalog) EnsureSubscriptionsRange(ctx context.Context, repo orm.SeriesRepo, subs []Subscription, startMS, endMS int64) *errs.Error {
	items, err := c.NormalizeSubscriptions(subs)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	return c.EnsureSeriesSubsRange(ctx, repo, legacySubscriptions(items), startMS, endMS)
}

// SetSubscriptions installs one static plan of klines and side sources without
// jobs, before replay. If initialization fails, discard the provider; a second
// installation is rejected so a partial plan cannot become a dynamic update.
func (p *HistProvider) SetSubscriptions(subs []Subscription) (result *errs.Error) {
	if p.genericSubscriptionsSet || p.replayStarted {
		return errs.NewMsg(core.ErrBadConfig, "generic subscriptions can only be installed once before replay")
	}
	items, err := p.catalog.NormalizeSubscriptions(subs)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	if p.compiledSubscriptions == nil {
		var timeRange *config.TimeTuple
		ctx := context.Background()
		if p.deps == nil {
			timeRange = config.TimeRange
		} else {
			timeRange = p.deps.timeRange()
			ctx = p.deps.context()
		}
		options := SubscriptionPlanOptions{PageRows: p.seriesBatchRows, PrefetchRows: p.seriesPrefetchBudget, PageBytes: p.seriesPageBytes}
		if timeRange != nil {
			options.AnchorMS, options.EndMS = timeRange.StartMS, timeRange.EndMS
		}
		requests := make([]SubscriptionRequest, 0, len(items))
		for _, sub := range items {
			requests = append(requests, SubscriptionRequest{Subscription: sub, Consumer: "historical", Required: true})
		}
		plan, err := p.catalog.CompileSubscriptionPlan(ctx, requests, options)
		if err != nil {
			return errs.New(core.ErrBadConfig, err)
		}
		p.compiledSubscriptions = plan
	}
	pageRows := p.compiledSubscriptions.options.PageRows
	if pageRows == 0 {
		pageRows = 20_000
	}
	if p.seriesPrefetchBudget > 0 && len(items) > 0 {
		if len(items) > p.seriesPrefetchBudget {
			return errs.NewMsg(core.ErrBadConfig, "series prefetch budget cannot hold one row per subscription")
		}
		pageRows = min(pageRows, p.seriesPrefetchBudget/len(items))
	}
	p.genericSubscriptionsSet = true
	defer func() { p.genericSubscriptionsReady = result == nil }()
	p.genericPageRows = pageRows
	p.genericKlineSubs = make(map[string][]Subscription)
	warms := make(map[string]map[string]int)
	for _, sub := range items {
		if sub.Source != orm.SeriesSourceKline {
			continue
		}
		pair := sub.ExSymbol.Symbol
		if pair == "" {
			return errs.NewMsg(core.ErrBadConfig, "kline subscription symbol is required")
		}
		p.genericKlineSubs[pair] = append(p.genericKlineSubs[pair], sub)
		if warms[pair] == nil {
			warms[pair] = make(map[string]int)
		}
		warms[pair][sub.TimeFrame] = sub.WarmupNum
	}
	for pair, subs := range p.genericKlineSubs {
		if hold, exists := p.getHolder(pair); exists {
			feeder, ok := hold.(*DBSeriesFeeder)
			if !ok {
				return errs.NewMsg(core.ErrBadConfig, "generic kline subscription requires DBSeriesFeeder")
			}
			configureKlineSubscriptions(feeder, subs, pageRows, p.seriesPageBytes)
		}
	}
	if len(warms) > 0 {
		if err := p.SubWarmPairs(warms, false); err != nil {
			return err
		}
	}
	return p.SetSeriesSubs(legacySubscriptions(items))
}

func configureKlineSubscriptions(feeder *DBSeriesFeeder, subs []Subscription, pageRows int, pageBytes ...int64) {
	if len(subs) == 0 {
		return
	}
	feeder.SeriesFeeder.setKlineSubscriptionFields(subs)
	fields := feeder.Feeder.subscriptionFields
	feeder.TfSeriesLoader.subscriptionFields = fields
	feeder.TfSeriesLoader.BatchRows = pageRows
	if len(pageBytes) > 0 {
		feeder.TfSeriesLoader.BatchBytes = pageBytes[0]
	}
	if feeder.hour != nil {
		feeder.hour.subscriptionFields = fields
		feeder.hour.BatchRows = pageRows
		feeder.hour.BatchBytes = feeder.TfSeriesLoader.BatchBytes
	}
}

func (f *SeriesFeeder) setKlineSubscriptionFields(subs []Subscription) {
	if f.subscriptionFields == nil {
		f.subscriptionFields = make(map[string][]string)
	}
	for _, sub := range subs {
		f.subscriptionFields[sub.TimeFrame] = orm.MergeSeriesFields(orm.DefaultKlineFields(), f.subscriptionFields[sub.TimeFrame], sub.Fields, sub.SeriesFields)
	}
	if f.hour != nil {
		f.hour.subscriptionFields = f.subscriptionFields
	}
}

// SetKlineSubscriptions installs typed kline projections before startup warmup.
// Existing feeder projections and timeframes are retained for other consumers.
// Side-source subscriptions are owned by the caller's source catalog runtime.
func (p *LiveProvider) SetKlineSubscriptions(subs []Subscription) *errs.Error {
	if p == nil || p.catalog == nil || p.klineSubscriptionsSet {
		return errs.NewMsg(core.ErrBadConfig, "live kline subscriptions require one startup plan and catalog")
	}
	items, err := p.catalog.NormalizeSubscriptions(subs)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	byPair := make(map[string][]Subscription)
	warms := make(map[string]map[string]int)
	for _, sub := range items {
		if sub.Source != orm.SeriesSourceKline {
			return errs.NewMsg(core.ErrBadConfig, "live kline installer cannot subscribe source %s", sub.Source)
		}
		pair := sub.ExSymbol.Symbol
		byPair[pair] = append(byPair[pair], sub)
		if warms[pair] == nil {
			warms[pair] = make(map[string]int)
		}
		warms[pair][sub.TimeFrame] = max(warms[pair][sub.TimeFrame], sub.WarmupNum)
	}
	if len(items) > 0 && p.newFeeder == nil {
		return errs.NewMsg(core.ErrBadConfig, "live kline feeder factory is unavailable")
	}
	if len(items) == 0 {
		p.klineSubscriptionsSet = true
		return nil
	}
	configure := func(feeder IDataFeeder, subs []Subscription) *errs.Error {
		projected, ok := feeder.(interface{ setKlineSubscriptionFields([]Subscription) })
		if !ok {
			return errs.NewMsg(core.ErrBadConfig, "live kline projection requires SeriesFeeder")
		}
		projected.setKlineSubscriptionFields(subs)
		return nil
	}
	for pair, subs := range byPair {
		if feeder, ok := p.getHolder(pair); ok {
			if err := configure(feeder, subs); err != nil {
				return err
			}
		}
	}
	newFeeder := p.newFeeder
	p.newFeeder = func(pair string, tfs []string) (IDataFeeder, *errs.Error) {
		feeder, err := newFeeder(pair, tfs)
		if err != nil {
			return nil, err
		}
		if err := configure(feeder, byPair[pair]); err != nil {
			return nil, err
		}
		return feeder, nil
	}
	p.klineSubscriptionsSet = true
	return p.SubWarmPairs(warms, false)
}
