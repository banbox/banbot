package data

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

// SetSeriesPageBytes limits decoded input pages; zero disables byte accounting.
// Retained warmup, aggregation and adapter transport buffers are outside it.
func (p *HistProvider) SetSeriesPageBytes(pageBytes int64) *errs.Error {
	if pageBytes < 0 {
		return errs.NewMsg(core.ErrBadConfig, "series page bytes must not be negative")
	}
	if len(p.series) > 0 || len(p.holderSnapshot()) > 0 || p.genericSubscriptionsSet {
		return errs.NewMsg(core.ErrBadConfig, "configure series page bytes before subscribing")
	}
	p.seriesPageBytes = pageBytes
	return nil
}

// SetSeriesPrefetch configures side-source pages before subscriptions are set.
// PrefetchBudget counts retained rows across all side-source streams, not bytes.
// Zero leaves the corresponding legacy limit unrestricted.
func (p *HistProvider) SetSeriesPrefetch(batchRows, prefetchBudget int) *errs.Error {
	if batchRows < 0 || prefetchBudget < 0 {
		return errs.NewMsg(core.ErrBadConfig, "series prefetch limits must not be negative")
	}
	if len(p.series) > 0 || len(p.holderSnapshot()) > 0 || p.genericSubscriptionsSet {
		return errs.NewMsg(core.ErrBadConfig, "configure series prefetch before subscribing")
	}
	p.seriesBatchRows, p.seriesPrefetchBudget = batchRows, prefetchBudget
	return nil
}

func (p *HistProvider) seriesPageRows(subs []*strat.DataSub) (int, *errs.Error) {
	rows := p.seriesBatchRows
	if rows == 0 {
		rows = 20_000
	}
	if p.genericPageRows > 0 {
		rows = min(rows, p.genericPageRows)
	}
	if p.seriesPrefetchBudget == 0 {
		return rows, nil
	}
	streams := make(map[string]bool)
	for _, sub := range subs {
		if sub != nil && sub.ExSymbol != nil && orm.NormalizeSeriesSource(sub.Source) != orm.SeriesSourceKline {
			streams[strat.DataSubKey(sub.Source, sub.ExSymbol.ID, sub.TimeFrame)] = true
		}
	}
	if len(streams) == 0 {
		return rows, nil
	}
	if len(streams) > p.seriesPrefetchBudget {
		return 0, errs.NewMsg(core.ErrBadConfig, "series prefetch budget %d cannot hold one row for each of %d streams", p.seriesPrefetchBudget, len(streams))
	}
	return min(rows, p.seriesPrefetchBudget/len(streams)), nil
}
