package data

import (
	"context"
	"fmt"
	"math"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
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
	srcInfo, tfErr := resolveSourceSeriesInfo(source, sub.TimeFrame)
	if tfErr != nil {
		return nil, tfErr
	}
	info, err := projectSeriesInfo(srcInfo, sub.Fields)
	if err != nil {
		return nil, err
	}
	rows, readErr := readSeriesStoreOrResample(ctx, orm.NewSeriesStore(repo), source, info, sub.ExSymbol, start, end, limit)
	if readErr != nil {
		return nil, readErr
	}
	return rows, nil
}

func readSeriesStoreOrResample(ctx context.Context, store *orm.SeriesStore, source DataSource, info *orm.SeriesInfo, target *orm.ExSymbol, startMS, endMS int64, limit int) ([]*orm.DataSeries, *errs.Error) {
	rows, err := store.Read(ctx, info, target, startMS, endMS, limit)
	if err != nil || len(rows) > 0 || source == nil {
		return rows, err
	}
	multi, okMulti := source.(TimeFrameSeriesSource)
	resampler, okRes := source.(SubTimeFrameResampleSource)
	if !okMulti || !okRes {
		return nil, nil
	}
	toTFSecs, tfErr := utils2.TFToSecSafe(info.TimeFrame)
	if tfErr != nil || toTFSecs <= 0 {
		return nil, nil
	}
	toTFMS := int64(toTFSecs) * 1000
	_, offSecs := utils2.GetTfAlignOrigin(toTFSecs)
	alignOffMS := int64(offSecs) * 1000
	alignedStart := utils2.AlignTfMSecsOffset(startMS, toTFMS, alignOffMS)
	if alignedStart < startMS {
		alignedStart += toTFMS
	}
	if alignedStart >= endMS {
		return nil, nil
	}
	var subEndMS int64
	if endMS == math.MaxInt64 {
		subEndMS = math.MaxInt64
	} else {
		lastBucketStart := utils2.AlignTfMSecsOffset(endMS-1, toTFMS, alignOffMS)
		if math.MaxInt64-toTFMS < lastBucketStart {
			subEndMS = math.MaxInt64
		} else {
			subEndMS = lastBucketStart + toTFMS
		}
	}
	if limit > 0 && int64(limit) <= (math.MaxInt64-alignedStart)/toTFMS {
		if capEnd := alignedStart + int64(limit)*toTFMS; capEnd < subEndMS {
			subEndMS = capEnd
		}
	}

	rawReadCtx := orm.WithSeriesReadByteLimit(ctx, 0)
	for _, subTF := range resampler.SubTimeFrames(info.TimeFrame) {
		subSecs, sErr := utils2.TFToSecSafe(subTF)
		if sErr != nil || subSecs <= 0 {
			continue
		}
		baseSubInfo, ok := multi.SeriesInfoForTimeFrame(subTF)
		if !ok || baseSubInfo == nil {
			continue
		}
		projSubInfo := *baseSubInfo
		projSubInfo.Binding = baseSubInfo.Binding
		projSubInfo.Binding.Fields = append([]orm.SeriesField(nil), info.Binding.Fields...)
		subLimit := 0
		if limit > 0 {
			ratio := int(toTFMS / (int64(subSecs) * 1000))
			if ratio > 0 && limit <= math.MaxInt/ratio {
				subLimit = limit * ratio
			}
		}
		subSeries, subErr := store.Read(rawReadCtx, &projSubInfo, target, alignedStart, subEndMS, subLimit)
		if subErr != nil || len(subSeries) == 0 {
			continue
		}
		closedSubRecs := filterClosedBucketSubRecords(subSeries, startMS, endMS, toTFMS, alignOffMS)
		if len(closedSubRecs) == 0 {
			continue
		}
		aggRecs, aggErr := orm.ResampleSeriesRecords(info, target, closedSubRecs, toTFMS, 0)
		if aggErr != nil {
			return nil, errs.New(core.ErrDbReadFail, aggErr)
		}
		if limit > 0 && len(aggRecs) > limit {
			aggRecs = aggRecs[:limit]
		}
		out := orm.RecordsToSeries(info, target, aggRecs)
		if bErr := orm.CheckDataSeriesBytes(ctx, out); bErr != nil {
			return nil, errs.New(core.ErrDbReadFail, bErr)
		}
		return out, nil
	}
	return nil, nil
}

func filterClosedBucketSubRecords(subSeries []*orm.DataSeries, startMS, endMS, toTFMS, alignOffMS int64) []*orm.DataRecord {
	var out []*orm.DataRecord
	var bucket []*orm.DataRecord
	var bucketTime int64
	flush := func() {
		if len(bucket) == 0 {
			return
		}
		if bucketTime >= startMS && bucketTime < endMS && bucket[len(bucket)-1].EndMS == bucketTime+toTFMS {
			out = append(out, bucket...)
		}
		bucket = nil
	}
	for _, item := range subSeries {
		rec := orm.SeriesToRecord(item)
		if rec == nil {
			continue
		}
		bt := utils2.AlignTfMSecsOffset(rec.TimeMS, toTFMS, alignOffMS)
		if len(bucket) > 0 && bt != bucketTime {
			flush()
		}
		if len(bucket) == 0 {
			bucketTime = bt
		}
		bucket = append(bucket, rec)
	}
	flush()
	return out
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
