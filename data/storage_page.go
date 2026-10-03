package data

import (
	"context"
	"fmt"

	"github.com/banbox/banbot/orm"
)

// ReadSubscriptionPage retains arbitrary series repository projections.
// Kline pages require ReadSubscriptionPageWithRuntimeDeps so their exchange,
// adjustment and symbol ownership are explicit.
func (c *DataSourceCatalog) ReadSubscriptionPage(ctx context.Context, repo orm.SeriesRepo, storage *orm.Storage, sub orm.Subscription, start, end int64, limit int) ([]*orm.DataSeries, error) {
	if ctx == nil || c == nil || limit <= 0 {
		return nil, fmt.Errorf("storage page requires context, catalog and positive limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items, err := c.NormalizeSubscriptions([]Subscription{sub})
	if err != nil {
		return nil, err
	}
	sub = items[0]
	if sub.Source == orm.SeriesSourceKline {
		return nil, fmt.Errorf("kline page requires explicit runtime dependencies")
	}
	if repo == nil {
		return nil, fmt.Errorf("series page requires explicit repository")
	}
	source := c.GetDataSource(sub.Source)
	info, err := projectSeriesInfo(source.Info(), sub.Fields)
	if err != nil {
		return nil, err
	}
	rows, readErr := orm.NewSeriesStore(repo).Read(ctx, info, sub.ExSymbol, start, end, limit)
	if readErr != nil {
		return nil, readErr
	}
	return rows, nil
}

// ReadSubscriptionPageWithRuntimeDeps also retains the runtime's historical
// coverage, adjustment/symbol scope and exchange adapter on kline reads.
func (c *DataSourceCatalog) ReadSubscriptionPageWithRuntimeDeps(ctx context.Context, repo orm.SeriesRepo, deps *RuntimeDeps, sub orm.Subscription, start, end int64, limit int) ([]*orm.DataSeries, error) {
	if deps == nil {
		return nil, fmt.Errorf("runtime page dependencies are required")
	}
	if sub.Source != orm.SeriesSourceKline {
		return c.ReadSubscriptionPage(ctx, repo, deps.storage(), sub, start, end, limit)
	}
	if ctx == nil || limit <= 0 {
		return nil, fmt.Errorf("storage page requires context and positive limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items, err := c.NormalizeSubscriptions([]Subscription{sub})
	if err != nil {
		return nil, err
	}
	sub = items[0]
	storage := deps.storage()
	if storage == nil || deps.Clock == nil {
		return nil, fmt.Errorf("runtime page requires explicit storage and clock")
	}
	sess, conn, readErr := storage.Conn(ctx)
	if readErr != nil {
		return nil, readErr
	}
	defer conn.Release()
	if deps.Exchange != nil {
		sess = sess.WithExchange(deps.Exchange)
	}
	if deps.Symbols != nil {
		sess = sess.WithSeriesSymbolState(deps.Symbols)
	}
	sess = sess.WithKlineRuntimeOptions(deps.KlineOptions()).WithReadContext(ctx)
	rows, readErr := sess.QuerySeriesFields(sub.ExSymbol, sub.TimeFrame, orm.MergeSeriesFields(sub.Fields, sub.SeriesFields), start, end, limit, false)
	if readErr != nil {
		return nil, readErr
	}
	if err := orm.CheckDataSeriesBytes(ctx, rows); err != nil {
		return nil, err
	}
	return rows, nil
}
