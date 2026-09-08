package orm

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5"
)

func KLinesToSeries(exs *ExSymbol, tf string, bars []*banexg.Kline, adj *AdjInfo, isWarmUp, closed bool) []*DataSeries {
	rows := make([]*DataSeries, 0, len(bars))
	for _, bar := range bars {
		if row := NewDataSeriesFromKline(exs, tf, bar, adj, isWarmUp, closed); row != nil {
			rows = append(rows, row)
		}
	}
	return rows
}

// SeriesToKLines projects OHLCV-shaped series into the legacy Kline boundary.
// It fails on the first invalid row and does not silently drop data.
func SeriesToKLines(rows []*DataSeries, exs *ExSymbol) ([]*banexg.Kline, error) {
	klines := make([]*banexg.Kline, len(rows))
	values := make([]banexg.Kline, len(rows))
	for i, row := range rows {
		_, fields, err := row.resolveOHLCV(exs)
		if err != nil {
			return nil, err
		}
		values[i] = fields.klineValue(row.TimeMS)
		klines[i] = &values[i]
	}
	return klines, nil
}

// seriesAlignOff resolves the aggregation boundary from the explicit symbol
// identity. The legacy GetAlignOff(sid, ...) path is intentionally reserved
// for callers that only have a SID and still use the package catalog.
func seriesAlignOff(exs *ExSymbol, tfMSecs int64) int64 {
	if exs == nil {
		return 0
	}
	return int64(exg.GetAlignOffForSymbol(exs.Exchange, exs.Market, exs.Symbol, int(tfMSecs/1000)) * 1000)
}

func (evt *DataSeries) BatchTimeMS() int64 {
	if evt == nil {
		return 0
	}
	return evt.TimeMS
}

// resampleOHLCVSeries preserves the K-line-specific aggregation contract while
// keeping the query and feeder paths in DataSeries form.
func resampleOHLCVSeries(state *SymbolState, exs *ExSymbol, tf string, rows, prev []*DataSeries, toTFMS int64,
	preFire float64, fromTFMS, offMS int64, isWarmUp bool) ([]*DataSeries, bool, error) {
	if len(rows) == 0 {
		return nil, false, nil
	}
	_, offset := utils2.GetTfAlignOrigin(int(toTFMS / 1000))
	alignOffMS := int64(offset * 1000)
	offsetMS := int64(float64(toTFMS)*preFire) + offMS
	if fromTFMS == 0 && len(rows) >= 2 {
		fromTFMS = rows[len(rows)-1].TimeMS - rows[len(rows)-2].TimeMS
	}
	aggNum, cacheNum := 0, 0
	if fromTFMS > 0 {
		aggNum = int(toTFMS / fromTFMS)
		cacheNum = len(rows)/max(aggNum, 1) + 3
	}
	result := make([]*DataSeries, 0, cacheNum+len(prev))
	var big *DataSeries
	var bucketRows []*DataSeries
	if len(prev) > 0 {
		result = append(result, prev[:len(prev)-1]...)
		var err error
		big, err = cloneOHLCVSeries(state, prev[len(prev)-1], exs, tf, toTFMS, isWarmUp)
		if err != nil {
			return nil, false, err
		}
		bucketRows = []*DataSeries{prev[len(prev)-1]}
	}
	aggCnt := 0
	for _, row := range rows {
		if row == nil {
			return nil, false, fmt.Errorf("series event is nil")
		}
		timeAlign := utils2.AlignTfMSecsOffset(row.TimeMS+offsetMS, toTFMS, alignOffMS)
		if big != nil && big.TimeMS == timeAlign {
			bucketRows = append(bucketRows, row)
			if err := mergeOHLCVSeriesWithRows(big, row, bucketRows); err != nil {
				return nil, false, err
			}
			big.Closed = row.Closed
			aggCnt++
			continue
		}
		if aggCnt > aggNum {
			aggNum = aggCnt
		}
		if keepOHLCVBucket(big, aggCnt, aggNum) {
			result = append(result, big)
		}
		var err error
		big, err = cloneOHLCVSeries(state, row, exs, tf, toTFMS, isWarmUp)
		if err != nil {
			return nil, false, err
		}
		big.TimeMS = timeAlign
		big.EndMS = timeAlign + toTFMS
		bucketRows = []*DataSeries{row}
		if err := mergeOHLCVExtraFields(big, row, bucketRows); err != nil {
			return nil, false, err
		}
		aggCnt = 1
	}
	if keepOHLCVBucket(big, aggCnt, aggNum) {
		result = append(result, big)
	}
	lastFinished := false
	if fromTFMS > 0 && len(result) > 0 {
		finishMS := utils2.AlignTfMSecsOffset(rows[len(rows)-1].TimeMS+fromTFMS+offsetMS, toTFMS, alignOffMS)
		lastFinished = finishMS > result[len(result)-1].TimeMS
	}
	return result, lastFinished, nil
}

func cloneOHLCVSeries(state *SymbolState, row *DataSeries, exs *ExSymbol, tf string, toTFMS int64, isWarmUp bool) (*DataSeries, error) {
	if row == nil {
		return nil, fmt.Errorf("series event is nil")
	}
	if err := validateOHLCVSeries(row); err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	cp := *row
	cp.Source = SeriesSourceKline
	cp.TimeFrame = tf
	cp.EndMS = cp.TimeMS + toTFMS
	cp.IsWarmUp = isWarmUp
	cp.ExSymbol = resolveSeriesExSymbol(state, row, exs)
	if cp.Sid == 0 && cp.ExSymbol != nil {
		cp.Sid = cp.ExSymbol.ID
	}
	cp.Values = make(map[string]any, len(row.Values))
	for key, val := range row.Values {
		cp.Values[key] = val
	}
	return &cp, nil
}

func mergeOHLCVSeries(dst, src *DataSeries) error {
	return mergeOHLCVSeriesWithRows(dst, src, []*DataSeries{dst, src})
}

func mergeOHLCVSeriesWithRows(dst, src *DataSeries, rows []*DataSeries) error {
	if dst == nil {
		return errs.NewMsg(core.ErrInvalidBars, "series row is nil")
	}
	if src == nil {
		return errs.NewMsg(core.ErrInvalidBars, "series row is nil")
	}
	if err := validateOHLCVSeries(src); err != nil {
		return errs.New(core.ErrInvalidBars, err)
	}
	if dst.Values == nil {
		dst.Values = make(map[string]any)
	}
	if err := mergeOHLCVExtraFields(dst, src, rows); err != nil {
		return err
	}
	srcVolume, err := src.VolumeValue()
	if err != nil {
		return err
	}
	if srcVolume <= 0 {
		return nil
	}
	dstVolume, valueErr := dst.VolumeValue()
	if valueErr != nil {
		return valueErr
	}
	srcOpen, err := src.OpenValue()
	if err != nil {
		return err
	}
	srcHigh, err := src.HighValue()
	if err != nil {
		return err
	}
	srcLow, err := src.LowValue()
	if err != nil {
		return err
	}
	srcClose, err := src.CloseValue()
	if err != nil {
		return err
	}
	if dstVolume == 0 {
		dst.Values["open"] = srcOpen
		dst.Values["high"] = srcHigh
		dst.Values["low"] = srcLow
	} else {
		dstHigh, valueErr := dst.HighValue()
		if valueErr != nil {
			return valueErr
		}
		dstLow, valueErr := dst.LowValue()
		if valueErr != nil {
			return valueErr
		}
		dst.Values["high"] = max(dstHigh, srcHigh)
		dst.Values["low"] = min(dstLow, srcLow)
	}
	dst.Values["close"] = srcClose
	dst.Values["volume"] = dstVolume + srcVolume
	dst.Values["quote"] = dst.QuoteValue() + src.QuoteValue()
	dst.Values["buy_volume"] = dst.BuyVolumeValue() + src.BuyVolumeValue()
	dst.Values["trade_num"] = dst.TradeNumValue() + src.TradeNumValue()
	return nil
}

func mergeOHLCVExtraFields(dst, src *DataSeries, rows []*DataSeries) error {
	seriesRows := make([]*DataSeries, 0, len(rows))
	dataRows := make([]*DataRecord, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		seriesRows = append(seriesRows, row)
		dataRows = append(dataRows, SeriesToRecord(row))
	}
	if len(seriesRows) == 0 {
		return nil
	}

	info := NewSeriesInfo(SeriesSourceKline, dst.TimeFrame, nil)
	exs := ResolveSeriesExSymbol(dst, src.ExSymbol)
	for _, field := range inferSeriesFields(seriesRows) {
		if isKlineReservedField(field.Name) {
			continue
		}
		fn, ok := GetAggRuleFunc(seriesAggRule(info, exs, field.Name))
		if !ok {
			fn, ok = GetAggRuleFunc("last")
		}
		if !ok {
			return fmt.Errorf("aggregation rule for series field %q is unavailable", field.Name)
		}
		value, err := fn(dataRows, field)
		if err != nil {
			return err
		}
		dst.Values[field.Name] = value
	}
	return nil
}

func keepOHLCVBucket(row *DataSeries, count, expected int) bool {
	if row == nil {
		return false
	}
	volume, err := row.VolumeValue()
	return err == nil && (volume > 0 || count*5 > expected)
}

func (q *Queries) QuerySeries(exs *ExSymbol, timeframe string, startMs, endMs int64, limit int, withUnFinish bool) ([]*DataSeries, *errs.Error) {
	return q.QuerySeriesFields(exs, timeframe, nil, startMs, endMs, limit, withUnFinish)
}

func (q *Queries) QuerySeriesFields(exs *ExSymbol, timeframe string, fields []string, startMs, endMs int64, limit int, withUnFinish bool) ([]*DataSeries, *errs.Error) {
	coverage := historicalCoverageForQuery(exs.Symbol)
	if err := validateHistoricalCoverageFields(coverage, fields); err != nil {
		return nil, err
	}
	if coverage == nil {
		return q.querySeriesFieldsRaw(exs, timeframe, fields, startMs, endMs, limit, withUnFinish)
	}
	_, rows, err := readHistoricalCoverageSeries(coverage, exs, timeframe, startMs, endMs, limit, withUnFinish,
		func(readStartMS, readEndMS int64, readLimit int, readWithUnFinish,
			reverse bool,
		) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			rows, readErr := q.querySeriesFieldsRawMode(exs, timeframe, fields, readStartMS, readEndMS,
				readLimit, readWithUnFinish, reverse)
			return nil, rows, readErr
		})
	return rows, err
}

func (q *Queries) querySeriesFieldsRaw(exs *ExSymbol, timeframe string, fields []string, startMs, endMs int64, limit int, withUnFinish bool) ([]*DataSeries, *errs.Error) {
	return q.querySeriesFieldsRawMode(exs, timeframe, fields, startMs, endMs, limit, withUnFinish,
		startMs == 0 && limit > 0)
}

func (q *Queries) querySeriesFieldsRawMode(exs *ExSymbol, timeframe string, fields []string,
	startMs, endMs int64, limit int, withUnFinish, revRead bool,
) ([]*DataSeries, *errs.Error) {
	fields = NormalizeSeriesFields(SeriesSourceKline, fields)
	tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
	boundedReverse := revRead && startMs > 0
	startMs, endMs = parseDownArgs(tfMSecs, startMs, endMs, limit, withUnFinish)
	maxEndMs := endMs
	finishEndMS := utils2.AlignTfMSecs(endMs, tfMSecs)
	unFinishMS := int64(0)
	if withUnFinish {
		curMs := btime.UTCStamp()
		unFinishMS = utils2.AlignTfMSecs(curMs, tfMSecs)
		if finishEndMS > unFinishMS {
			finishEndMS = unFinishMS
		}
	}
	coverage := historicalCoverageForQuery(exs.Symbol)
	consumerIntervals := historicalCoverageIntervals(coverage, exs.Symbol, timeframe, startMs, finishEndMS)
	listingPrefix, hasListingPrefix := legacyListingPrefixProof(
		coverage, exs, timeframe, startMs, consumerIntervals)
	physicalStart, physicalStop, physicalBound, coverageErr := historicalPhysicalCoverageBoundsWithListingPrefix(
		coverage, exs.Symbol, timeframe, startMs, finishEndMS, hasListingPrefix)
	if coverageErr != nil {
		return nil, coverageErr
	}
	if physicalBound {
		if physicalStop <= physicalStart {
			return nil, nil
		}
		startMs, finishEndMS = physicalStart, physicalStop
	}
	rows, subTF, err := q.querySeriesRows(exs, timeframe, fields, startMs, finishEndMS, limit,
		revRead, boundedReverse || physicalBound)
	if err != nil {
		return nil, err
	}
	if revRead {
		utils.ReverseArr(rows)
	}
	if hasListingPrefix && listingPrefix.minuteStartMS < listingPrefix.storageStartMS &&
		shouldRestoreHistoricalListingPrefix(timeframe, subTF, listingPrefix, rows) {
		queryStorageTF := subTF
		if queryStorageTF == "" {
			queryStorageTF = timeframe
		}
		if queryStorageTF != listingPrefix.storageTF {
			return nil, errs.NewMsg(core.ErrBadConfig,
				"historical listing prefix storage mismatch for %s %s: query=%s proof=%s",
				exs.Symbol, timeframe, queryStorageTF, listingPrefix.storageTF)
		}
		minuteRows, minuteSubTF, listingErr := q.querySeriesRows(exs, "1m", fields,
			listingPrefix.minuteStartMS, listingPrefix.storageStartMS, 0, false, true)
		if listingErr != nil {
			return nil, listingErr
		}
		if minuteSubTF != "" {
			return nil, errs.NewMsg(core.ErrBadConfig,
				"historical listing prefix for %s resolved 1m through %s", exs.Symbol, minuteSubTF)
		}
		rows, listingErr = prependHistoricalListingPrefix(exs, listingPrefix, minuteRows, rows)
		if listingErr != nil {
			return nil, listingErr
		}
	}
	if subTF != "" && len(rows) > 0 {
		fromTFMS := int64(utils2.TFToSecs(subTF) * 1000)
		var lastFinish bool
		offMS := seriesAlignOff(exs, tfMSecs)
		var err_ error
		rows, lastFinish, err_ = ResampleDataSeries(exs, timeframe, rows, nil, tfMSecs, 0, fromTFMS, offMS, false)
		if err_ != nil {
			return nil, errs.New(core.ErrInvalidBars, err_)
		}
		if !lastFinish && len(rows) > 0 {
			rows = rows[:len(rows)-1]
		}
	}
	if len(rows) > limit && limit > 0 {
		if revRead {
			rows = rows[len(rows)-limit:]
		} else {
			rows = rows[:limit]
		}
	}
	if len(rows) == 0 && maxEndMs-endMs > tfMSecs {
		return q.querySeriesFieldsRawMode(exs, timeframe, fields, endMs, maxEndMs, limit,
			withUnFinish, revRead)
	} else if withUnFinish && len(rows) > 0 && rows[len(rows)-1].TimeMS+tfMSecs == unFinishMS {
		unbar, _, _ := getUnFinish(q, exs.ID, timeframe, unFinishMS, unFinishMS+tfMSecs, "query")
		if unbar != nil {
			rows = append(rows, NewDataSeriesFromKline(exs, timeframe, unbar, nil, false, false))
		}
	}
	return rows, nil
}

func shouldRestoreHistoricalListingPrefix(timeframe, subTF string, prefix historicalListingPrefix,
	rows []*DataSeries,
) bool {
	if len(rows) == 0 || rows[0] == nil {
		return false
	}
	if subTF == "" {
		return timeframe == prefix.storageTF && rows[0].TimeMS == prefix.bucketStartMS
	}
	return subTF == prefix.storageTF &&
		(rows[0].TimeMS == prefix.bucketStartMS || rows[0].TimeMS == prefix.storageStartMS)
}

func (q *Queries) QuerySeriesBatch(exsMap map[int32]*ExSymbol, timeframe string, startMs, endMs int64, limit int, handle func(int32, []*DataSeries)) *errs.Error {
	return q.QuerySeriesBatchFields(exsMap, timeframe, nil, startMs, endMs, limit, handle)
}

func (q *Queries) QuerySeriesBatchFields(exsMap map[int32]*ExSymbol, timeframe string, fields []string, startMs, endMs int64, limit int, handle func(int32, []*DataSeries)) *errs.Error {
	if len(exsMap) == 0 {
		return nil
	}
	if config.StrictHistoricalReplay(config.HistoricalCoverage) {
		sids := make([]int, 0, len(exsMap))
		for sid := range exsMap {
			sids = append(sids, int(sid))
		}
		sort.Ints(sids)
		for _, value := range sids {
			sid := int32(value)
			exs := exsMap[sid]
			rows, err := q.QuerySeriesFields(exs, timeframe, fields, startMs, endMs, limit, false)
			if err != nil {
				return err
			}
			handle(sid, rows)
		}
		return nil
	}
	return q.querySeriesBatchFieldsRaw(exsMap, timeframe, fields, startMs, endMs, limit, handle)
}

func (q *Queries) querySeriesBatchFieldsRaw(exsMap map[int32]*ExSymbol, timeframe string, fields []string, startMs, endMs int64, limit int, handle func(int32, []*DataSeries)) *errs.Error {
	fields = NormalizeSeriesFields(SeriesSourceKline, fields)
	tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
	startMs, endMs = parseDownArgs(tfMSecs, startMs, endMs, limit, false)
	finishEndMS := utils2.AlignTfMSecs(endMs, tfMSecs)
	if core.LiveMode {
		curMs := btime.TimeMS()
		unFinishMS := utils2.AlignTfMSecs(curMs, tfMSecs)
		if finishEndMS > unFinishMS {
			finishEndMS = unFinishMS
		}
	}
	if !IsQuestDB {
		return q.querySeriesBatchPg(exsMap, timeframe, fields, startMs, finishEndMS, tfMSecs, handle)
	}
	sidTA := make([]string, 0, len(exsMap))
	for _, exs := range exsMap {
		sidTA = append(sidTA, fmt.Sprintf("%v", exs.ID))
	}
	sidText := strings.Join(sidTA, ", ")
	sql := fmt.Sprintf(`
select cast(ts as long)/1000,%s from $tbl
where ts >= cast(%v as timestamp) and ts < cast(%v as timestamp) and sid in (%v)
order by sid,ts`, klineSelectProjection(fields, true), startMs*1000, finishEndMS*1000, sidText)
	subTF, pgRows, err_ := queryHyper(q, timeframe, sql, 0)
	return handleSeriesBatchFields(exsMap, timeframe, fields, tfMSecs, subTF, pgRows, err_, handle)
}

func (q *Queries) InsertSeries(timeFrame string, exs *ExSymbol, rows []*DataSeries, aggBig bool) (int64, *errs.Error) {
	return q.InsertOHLCVSeriesAuto(timeFrame, exs, rows, aggBig)
}

func (q *Queries) UpdateSeries(exs *ExSymbol, timeFrame string, startMS, endMS int64, rows []*DataSeries, aggBig bool, skipHoles ...bool) *errs.Error {
	if _, err := normalizeOHLCVSeries(rows, exs.ID); err != nil {
		return err
	}
	return q.UpdateKRange(exs, timeFrame, startMS, endMS, aggBig, skipHoles...)
}

func AutoFetchSeries(exchange banexg.BanExchange, exs *ExSymbol, timeFrame string, startMS, endMS int64,
	limit int, withUnFinish bool, pBar *utils.PrgBar) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	sess, conn, err := Conn(nil)
	if err != nil {
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
		return nil, nil, err
	}
	defer conn.Release()
	return autoFetchSeries(timeFrame, startMS, endMS, limit, withUnFinish, pBar,
		func(downTF string, downStartMS, downEndMS int64) *errs.Error {
			_, downErr := sess.DownOHLCV2DBForRequestedTF(exchange, exs, downTF, timeFrame,
				downStartMS, downEndMS, pBar)
			return downErr
		}, func(readStartMS, readEndMS int64, readLimit int, readWithUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			return sess.GetSeries(exs, timeFrame, readStartMS, readEndMS, readLimit, readWithUnFinish)
		})
}

func autoFetchSeries(timeFrame string, startMS, endMS int64, limit int, withUnFinish bool, pBar *utils.PrgBar,
	download func(string, int64, int64) *errs.Error, read seriesFieldsReader,
) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	if allowImplicitKlineDownload() {
		downTF, err := GetDownTF(timeFrame)
		if err != nil {
			if pBar != nil {
				pBar.Add(core.StepTotal)
			}
			return nil, nil, err
		}
		tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
		downStartMS, downEndMS := parseDownArgs(tfMSecs, startMS, endMS, limit, withUnFinish)
		if err = download(downTF, downStartMS, downEndMS); err != nil {
			return nil, nil, err
		}
		return read(downStartMS, downEndMS, limit, withUnFinish)
	} else if pBar != nil {
		pBar.Add(core.StepTotal)
	}
	return read(startMS, endMS, limit, withUnFinish)
}

func GetSeries(exs *ExSymbol, timeFrame string, startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	retry, maxRetry := 0, 3
	for retry < maxRetry {
		sess, conn, err := Conn(nil)
		if err != nil {
			return nil, nil, err
		}
		adjs, rows, err := sess.GetSeries(exs, timeFrame, startMS, endMS, limit, withUnFinish)
		conn.Release()
		if err != nil && err.Code == core.ErrDbConnFail && retry < maxRetry+1 {
			retry += 1
			core.Sleep(time.Millisecond * 1000 * time.Duration(retry))
			continue
		}
		return adjs, rows, err
	}
	return nil, nil, errs.NewMsg(core.ErrDbReadFail, "max retry exceed")
}

func (q *Queries) GetSeries(exs *ExSymbol, timeFrame string, startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	return q.GetSeriesFields(exs, timeFrame, nil, startMS, endMS, limit, withUnFinish)
}

// GetPhysicalSeriesFields is used by the feeder's internal canonical physical
// timeframe loader. It accepts only the immutable PhysicalBars proof for the
// requested storage timeframe; ordinary strategy reads remain Bars-gated.
func (q *Queries) GetPhysicalSeriesFields(exs *ExSymbol, timeFrame string, fields []string,
	startMS, endMS int64, limit int, withUnFinish bool,
) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	return q.GetPhysicalSeriesFieldsForConsumer(exs, timeFrame, fields, timeFrame,
		startMS, endMS, limit, withUnFinish)
}

// GetPhysicalSeriesFieldsForConsumer permits an internal physical loader to
// use the extension tail only when its logical consumer is covered too.
func (q *Queries) GetPhysicalSeriesFieldsForConsumer(exs *ExSymbol, timeFrame string, fields []string,
	consumerTimeframe string, startMS, endMS int64, limit int, withUnFinish bool,
) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	coverage := historicalCoverageForQuery(exs.Symbol)
	if coverage == nil {
		return q.getSeriesFieldsRaw(exs, timeFrame, fields, startMS, endMS, limit, withUnFinish)
	}
	storageTF, err := PhysicalKlineStorageTimeframe(timeFrame)
	if err != nil {
		return nil, nil, err
	}
	if storageTF != timeFrame {
		return nil, nil, errs.NewMsg(core.ErrBadConfig,
			"physical series read requires canonical storage timeframe: %s via %s", timeFrame, storageTF)
	}
	if err = validateHistoricalCoverageFields(coverage, fields); err != nil {
		return nil, nil, err
	}
	intervals := historicalPhysicalCoverageIntervals(coverage, exs.Symbol, timeFrame, consumerTimeframe, startMS, endMS)
	// A derived consumer may legitimately begin with a partially listed bucket.
	// Keep the physical loader aligned with the ordinary read path by restoring
	// the separately proved listing prefix, but never apply it to a direct
	// physical-timeframe read.
	if consumerTimeframe != "" && consumerTimeframe != timeFrame {
		intervals = extendLegacyListingCoverage(coverage, exs, consumerTimeframe, startMS, intervals)
	}
	return readHistoricalCoverageIntervals(coverage, exs, timeFrame, startMS, endMS, limit, withUnFinish,
		intervals, func(readStartMS, readEndMS int64, readLimit int, readWithUnFinish, reverse bool) (
			[]*AdjInfo, []*DataSeries, *errs.Error,
		) {
			return q.getSeriesFieldsRawMode(exs, timeFrame, fields, readStartMS, readEndMS,
				readLimit, readWithUnFinish, reverse)
		})
}

func (q *Queries) GetSeriesFields(exs *ExSymbol, timeFrame string, fields []string, startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	coverage := historicalCoverageForQuery(exs.Symbol)
	if err := validateHistoricalCoverageFields(coverage, fields); err != nil {
		return nil, nil, err
	}
	if coverage == nil {
		return q.getSeriesFieldsRaw(exs, timeFrame, fields, startMS, endMS, limit, withUnFinish)
	}
	return readHistoricalCoverageSeries(coverage, exs, timeFrame, startMS, endMS, limit, withUnFinish,
		func(startMS, endMS int64, limit int, withUnFinish, reverse bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			return q.getSeriesFieldsRawMode(exs, timeFrame, fields, startMS, endMS, limit, withUnFinish, reverse)
		})
}

func (q *Queries) getSeriesFieldsRaw(exs *ExSymbol, timeFrame string, fields []string, startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	return q.getSeriesFieldsRawMode(exs, timeFrame, fields, startMS, endMS, limit, withUnFinish,
		startMS == 0 && limit > 0)
}

func (q *Queries) getSeriesFieldsRawMode(exs *ExSymbol, timeFrame string, fields []string,
	startMS, endMS int64, limit int, withUnFinish, reverse bool,
) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	// Combined is populated from banexg.Market.Combined at the symbol boundary.
	if exs.Combined {
		adjs, err := GetAdjs(exs.ID)
		if err != nil {
			return nil, nil, err
		}
		rows, err := q.getAdjSeriesFieldsMode(adjs, timeFrame, fields, startMS, endMS, limit,
			withUnFinish, reverse, q.querySeriesFieldsRawMode)
		if err != nil {
			return nil, nil, err
		}
		return adjs, bindSeriesTarget(rows, exs), nil
	}
	rows, err := q.querySeriesFieldsRawMode(exs, timeFrame, fields, startMS, endMS, limit,
		withUnFinish, reverse)
	return nil, rows, err
}

func bindSeriesTarget(rows []*DataSeries, exs *ExSymbol) []*DataSeries {
	if len(rows) == 0 || exs == nil {
		return rows
	}
	out := make([]*DataSeries, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		cp := *row
		cp.Sid = exs.ID
		cp.ExSymbol = exs
		out = append(out, &cp)
	}
	return out
}

func (q *Queries) GetAdjSeries(adjs []*AdjInfo, timeFrame string, startMS, endMS int64, limit int, withUnFinish bool) ([]*DataSeries, *errs.Error) {
	return q.GetAdjSeriesFields(adjs, timeFrame, nil, startMS, endMS, limit, withUnFinish)
}

func (q *Queries) GetAdjSeriesFields(adjs []*AdjInfo, timeFrame string, fields []string, startMS, endMS int64, limit int, withUnFinish bool) ([]*DataSeries, *errs.Error) {
	return q.getAdjSeriesFields(adjs, timeFrame, fields, startMS, endMS, limit, withUnFinish,
		q.QuerySeriesFields)
}

type querySeriesFieldsFunc func(*ExSymbol, string, []string, int64, int64, int, bool) ([]*DataSeries, *errs.Error)

type querySeriesFieldsModeFunc func(*ExSymbol, string, []string, int64, int64, int, bool,
	bool,
) ([]*DataSeries, *errs.Error)

func (q *Queries) getAdjSeriesFields(adjs []*AdjInfo, timeFrame string, fields []string, startMS, endMS int64,
	limit int, withUnFinish bool, read querySeriesFieldsFunc,
) ([]*DataSeries, *errs.Error) {
	return q.getAdjSeriesFieldsMode(adjs, timeFrame, fields, startMS, endMS, limit, withUnFinish,
		startMS == 0 && limit > 0,
		func(exs *ExSymbol, timeframe string, fields []string, startMS, endMS int64, limit int,
			withUnFinish, reverse bool,
		) ([]*DataSeries, *errs.Error) {
			if reverse {
				startMS = 0
			}
			return read(exs, timeframe, fields, startMS, endMS, limit, withUnFinish)
		})
}

func (q *Queries) getAdjSeriesFieldsMode(adjs []*AdjInfo, timeFrame string, fields []string,
	startMS, endMS int64, limit int, withUnFinish, revRead bool, read querySeriesFieldsModeFunc,
) ([]*DataSeries, *errs.Error) {
	if len(adjs) == 0 {
		return nil, nil
	}
	if endMS == 0 {
		endMS = btime.UTCStamp()
	}
	var result []*DataSeries
	if revRead {
		utils.ReverseArr(adjs)
		defer utils.ReverseArr(adjs)
	}
	for _, f := range adjs {
		if f.StartMS >= endMS || f.StopMS <= startMS {
			continue
		}
		start := max(f.StartMS, startMS)
		stop := min(f.StopMS, endMS)
		rows, err := read(f.ExSymbol, timeFrame, fields, start, stop, limit, withUnFinish, revRead)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			row.Adj = f
		}
		if revRead {
			result = append(rows, result...)
		} else {
			result = append(result, rows...)
		}
		withUnFinish = false
		if limit > 0 && len(result) >= limit {
			if len(result) > limit {
				if revRead {
					result = result[len(result)-limit:]
				} else {
					result = result[:limit]
				}
			}
			break
		}
	}
	return result, nil
}

func (q *Queries) querySeriesRows(exs *ExSymbol, timeframe string, fields []string, startMs, finishEndMS int64,
	limit int, revRead, boundedReverse bool,
) ([]*DataSeries, string, *errs.Error) {
	if !IsQuestDB {
		rows, subTF, err := q.querySeriesPg(exs, timeframe, fields, startMs, finishEndMS, limit,
			revRead, boundedReverse)
		if err != nil {
			return nil, "", NewDbErr(core.ErrDbReadFail, err)
		}
		return rows, subTF, nil
	}
	projection := klineSelectProjection(fields, false)
	var sql string
	if revRead {
		lowerBound := ""
		if boundedReverse {
			lowerBound = fmt.Sprintf(" and ts >= cast(%v as timestamp)", startMs*1000)
		}
		sql = fmt.Sprintf(`
select cast(ts as long)/1000,%s from $tbl
where sid=%d%s and ts < cast(%v as timestamp)
order by ts desc`, projection, exs.ID, lowerBound, finishEndMS*1000)
	} else {
		if limit == 0 {
			tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
			limit = int((finishEndMS-startMs)/tfMSecs) + 1
		}
		sql = fmt.Sprintf(`
select cast(ts as long)/1000,%s from $tbl
where sid=%d and ts >= cast(%v as timestamp) and ts < cast(%v as timestamp)
order by ts`, projection, exs.ID, startMs*1000, finishEndMS*1000)
	}
	subTF, pgRows, err_ := queryHyper(q, timeframe, sql, limit)
	rows, err_ := mapToSeriesFields(exs, timeframe, fields, pgRows, err_)
	if err_ != nil {
		return nil, "", NewDbErr(core.ErrDbReadFail, err_)
	}
	return rows, subTF, nil
}

func (q *Queries) querySeriesPg(exs *ExSymbol, timeframe string, fields []string, startMs, endMs int64,
	limit int, revRead, boundedReverse bool,
) ([]*DataSeries, string, error) {
	tblName, subTF, rate := resolveTablePg(timeframe)
	if limit > 0 && subTF != "" && rate > 1 {
		limit = rate * (limit + 1)
	}
	projection := klineSelectProjection(fields, false)
	var sql string
	if revRead {
		timeFilter := fmt.Sprintf("time < %d", endMs)
		if boundedReverse {
			timeFilter = buildPgTimeFilter(startMs, endMs)
		}
		sql = fmt.Sprintf(`SELECT time,%s FROM %s
	WHERE sid=%d AND %s
	ORDER BY time DESC`, projection, tblName, exs.ID, timeFilter)
	} else {
		sql = fmt.Sprintf(`SELECT time,%s FROM %s
	WHERE sid=%d AND %s
	ORDER BY time`, projection, tblName, exs.ID, buildPgTimeFilter(startMs, endMs))
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	pgRows, err := q.db.Query(context.Background(), sql)
	rows, err := mapToSeriesFields(exs, timeframe, fields, pgRows, err)
	return rows, subTF, err
}

func quoteSeriesFields(fields []string) []string {
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		out = append(out, quoteIdent(field))
	}
	return out
}

func klineSelectProjection(fields []string, includeSID bool) string {
	cols := quoteSeriesFields(NormalizeSeriesFields(SeriesSourceKline, fields))
	if includeSID {
		cols = append(cols, quoteIdent("sid"))
	}
	return strings.Join(cols, ",")
}

func (q *Queries) querySeriesBatchPg(exsMap map[int32]*ExSymbol, timeframe string, fields []string, startMs, finishEndMS, tfMSecs int64, handle func(int32, []*DataSeries)) *errs.Error {
	sidTA := make([]string, 0, len(exsMap))
	for _, exs := range exsMap {
		sidTA = append(sidTA, itoa(int64(exs.ID)))
	}
	tblName, subTF, _ := resolveTablePg(timeframe)
	sidText := strings.Join(sidTA, ",")
	sql := fmt.Sprintf(`SELECT time,%s FROM %s
WHERE %s AND sid IN (%s)
ORDER BY sid, time`, klineSelectProjection(fields, true), tblName, buildPgTimeFilter(startMs, finishEndMS), sidText)
	pgRows, err_ := q.db.Query(context.Background(), sql)
	return handleSeriesBatchFields(exsMap, timeframe, fields, tfMSecs, subTF, pgRows, err_, handle)
}

func handleSeriesBatch(exsMap map[int32]*ExSymbol, timeframe string, tfMSecs int64, subTF string, pgRows pgx.Rows, err_ error, handle func(int32, []*DataSeries)) *errs.Error {
	return handleSeriesBatchFields(exsMap, timeframe, DefaultKlineFields(), tfMSecs, subTF, pgRows, err_, handle)
}

func handleSeriesBatchFields(exsMap map[int32]*ExSymbol, timeframe string, fields []string, tfMSecs int64, subTF string, pgRows pgx.Rows, err_ error, handle func(int32, []*DataSeries)) *errs.Error {
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	defer pgRows.Close()
	fields = NormalizeSeriesFields(SeriesSourceKline, fields)
	grouped := make(map[int32][]*DataSeries, len(exsMap))
	var timeMS int64
	var sid int32
	values := make([]any, len(fields))
	targets := make([]any, 2+len(fields))
	targets[0] = &timeMS
	for i := range values {
		targets[i+1] = &values[i]
	}
	targets[len(targets)-1] = &sid
	for pgRows.Next() {
		timeMS = 0
		sid = 0
		clear(values)
		if err := pgRows.Scan(targets...); err != nil {
			return NewDbErr(core.ErrDbReadFail, err)
		}
		valueMap := make(map[string]any, len(fields))
		for i, field := range fields {
			valueMap[field] = values[i]
		}
		grouped[sid] = append(grouped[sid], &DataSeries{
			Source: SeriesSourceKline, Sid: sid, TimeMS: timeMS, EndMS: timeMS + tfMSecs,
			TimeFrame: timeframe, Closed: true, Values: valueMap, ExSymbol: exsMap[sid],
		})
	}
	if err := pgRows.Err(); err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	var seriesArr []*DataSeries
	fromTFMS := int64(0)
	if subTF != "" {
		fromTFMS = int64(utils2.TFToSecs(subTF) * 1000)
	}
	sids := make([]int32, 0, len(exsMap))
	for sid := range exsMap {
		sids = append(sids, sid)
	}
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	for _, sid := range sids {
		exs := exsMap[sid]
		seriesArr = grouped[sid]
		if fromTFMS > 0 {
			var lastDone bool
			var err error
			offMS := seriesAlignOff(exs, tfMSecs)
			seriesArr, lastDone, err = ResampleDataSeries(exs, timeframe, seriesArr, nil, tfMSecs, 0, fromTFMS, offMS, false)
			if err != nil {
				return errs.New(core.ErrInvalidBars, err)
			}
			if !lastDone && len(seriesArr) > 0 {
				seriesArr = seriesArr[:len(seriesArr)-1]
			}
		}
		handle(sid, seriesArr)
	}
	return nil
}

func mapToSeries(exs *ExSymbol, timeframe string, pgRows pgx.Rows, err_ error) ([]*DataSeries, error) {
	return mapToSeriesFields(exs, timeframe, DefaultKlineFields(), pgRows, err_)
}

func mapToSeriesFields(exs *ExSymbol, timeframe string, fields []string, pgRows pgx.Rows, err_ error) ([]*DataSeries, error) {
	if err_ != nil {
		return nil, err_
	}
	if pgRows == nil {
		return nil, nil
	}
	defer pgRows.Close()
	tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
	fields = NormalizeSeriesFields(SeriesSourceKline, fields)
	var out []*DataSeries
	var timeMS int64
	values := make([]any, len(fields))
	targets := make([]any, 1+len(fields))
	targets[0] = &timeMS
	for i := range values {
		targets[i+1] = &values[i]
	}
	for pgRows.Next() {
		timeMS = 0
		clear(values)
		if err := pgRows.Scan(targets...); err != nil {
			return nil, err
		}
		valueMap := make(map[string]any, len(fields))
		for i, field := range fields {
			valueMap[field] = values[i]
		}
		sid := int32(0)
		if exs != nil {
			sid = exs.ID
		}
		out = append(out, &DataSeries{
			Source: SeriesSourceKline, Sid: sid, TimeMS: timeMS, EndMS: timeMS + tfMSecs,
			TimeFrame: timeframe, Closed: true, Values: valueMap, ExSymbol: exs,
		})
	}
	return out, pgRows.Err()
}

func (q *Queries) InsertOHLCVSeriesAuto(timeFrame string, exs *ExSymbol, rows []*DataSeries, aggBig bool) (int64, *errs.Error) {
	values, err := normalizeOHLCVSeries(rows, exs.ID)
	if err != nil || len(values) == 0 {
		return 0, err
	}
	tblName := "kline_" + timeFrame
	unlock, lockErr := acquireQuestTableReadLock(context.Background(), tblName)
	if lockErr != nil {
		return 0, NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	startMS := values[0].TimeMS
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	lastMS := values[len(values)-1].TimeMS
	endMS := lastMS + tfMSecs
	insTs, err := AddInsJob(AddInsKlineParams{
		Sid:       exs.ID,
		Timeframe: timeFrame,
		StartMs:   startMS,
		StopMs:    endMS,
	})
	if err != nil || insTs.IsZero() {
		return 0, err
	}
	write := q
	var tx pgx.Tx
	if !IsQuestDB {
		var txErr error
		tx, write, txErr = q.begin(context.Background())
		if txErr != nil {
			return 0, NewDbErr(core.ErrDbExecFail, txErr)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
	}
	num, err := write.insertOHLCVRowsLocked(timeFrame, values)
	if err != nil {
		return num, err
	}
	if err = write.finalizeKlineInsert(exs, timeFrame, startMS, endMS, lastMS, insTs, aggBig); err != nil {
		return num, err
	}
	if tx != nil {
		if err_ := tx.Commit(context.Background()); err_ != nil {
			return num, NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	return num, nil
}

func (q *Queries) InsertOHLCVSeries(timeFrame string, sid int32, rows []*DataSeries) (int64, *errs.Error) {
	values, err := normalizeOHLCVSeries(rows, sid)
	if err != nil || len(values) == 0 {
		return 0, err
	}
	tblName := "kline_" + timeFrame
	unlock, lockErr := acquireQuestTableReadLock(context.Background(), tblName)
	if lockErr != nil {
		return 0, NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	return q.insertOHLCVRowsLocked(timeFrame, values)
}

func (q *Queries) insertOHLCVRows(timeFrame string, rows []*DataSeries) (int64, *errs.Error) {
	if !IsQuestDB {
		return q.insertOHLCVRowsLocked(timeFrame, rows)
	}
	tblName := "kline_" + timeFrame
	unlock, lockErr := acquireQuestTableReadLock(context.Background(), tblName)
	if lockErr != nil {
		return 0, NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	return q.insertOHLCVRowsLocked(timeFrame, rows)
}

// insertOHLCVRowsLocked writes rows while the caller holds the target table read lock.
func (q *Queries) insertOHLCVRowsLocked(timeFrame string, rows []*DataSeries) (int64, *errs.Error) {
	if !IsQuestDB {
		return q.insertOHLCVSeriesPg(timeFrame, rows)
	}
	tblName := "kline_" + timeFrame
	ctx := context.Background()
	fields := klineExtraFields(rows)
	cols := klineInsertColumns("ts", fields)
	colsPerRow := len(cols)
	const batchRows = 500
	var total int64
	for i := 0; i < len(rows); i += batchRows {
		j := min(len(rows), i+batchRows)
		var b strings.Builder
		b.WriteString("insert into ")
		b.WriteString(quoteIdent(tblName))
		b.WriteString(" (")
		b.WriteString(strings.Join(quoteSeriesFields(cols), ", "))
		b.WriteString(") values ")
		args := make([]any, 0, (j-i)*colsPerRow)
		for k := i; k < j; k++ {
			if k > i {
				b.WriteByte(',')
			}
			p := (k-i)*colsPerRow + 1
			b.WriteByte('(')
			for col := 0; col < colsPerRow; col++ {
				if col > 0 {
					b.WriteByte(',')
				}
				b.WriteString(fmt.Sprintf("$%d", p+col))
			}
			b.WriteByte(')')
			vals := rows[k]
			args = append(args, vals.Sid, time.UnixMilli(vals.TimeMS).UTC(), klineWriteValue(vals, "open"), klineWriteValue(vals, "high"), klineWriteValue(vals, "low"), klineWriteValue(vals, "close"), klineWriteValue(vals, "volume"), klineWriteValue(vals, "quote"), klineWriteValue(vals, "buy_volume"), klineWriteValue(vals, "trade_num"))
			for _, field := range fields {
				args = append(args, klineWriteValue(vals, field))
			}
		}
		_, err := q.db.Exec(ctx, b.String(), args...)
		if err != nil {
			return total, NewDbErr(core.ErrDbExecFail, err)
		}
		total += int64(j - i)
	}
	return total, nil
}

func (q *Queries) insertOHLCVSeriesPg(timeFrame string, rows []*DataSeries) (int64, *errs.Error) {
	tblName := "kline_" + timeFrame
	fields := klineExtraFields(rows)
	cols := klineInsertColumns("time", fields)
	newSrc := func() *iterForAddOHLCVSeriesPg { return &iterForAddOHLCVSeriesPg{rows: rows, fields: fields} }
	n, err := q.db.CopyFrom(context.Background(), pgx.Identifier{tblName}, cols, newSrc())
	if err != nil {
		tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
		startMS := rows[0].TimeMS
		endMS := rows[len(rows)-1].TimeMS + tfMSecs
		if delErr := delKLinesPg(q, timeFrame, rows[0].Sid, startMS, endMS); delErr != nil {
			return 0, delErr
		}
		n, err = q.db.CopyFrom(context.Background(), pgx.Identifier{tblName}, cols, newSrc())
		if err != nil {
			return 0, NewDbErr(core.ErrDbExecFail, err)
		}
	}
	return n, nil
}

func ohlcvSeriesValues(row *DataSeries, sid int32) (*DataSeries, *errs.Error) {
	if row == nil {
		return nil, errs.NewMsg(core.ErrInvalidBars, "series row is nil")
	}
	if err := validateOHLCVSeries(row); err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	item := *row
	item.Sid = sid
	item.Values = make(map[string]any, len(row.Values))
	for key, value := range row.Values {
		item.Values[key] = value
	}
	return &item, nil
}

func validateOHLCVSeries(row *DataSeries) error {
	if row == nil {
		return fmt.Errorf("series event is nil")
	}
	for _, field := range []string{"open", "high", "low", "close", "volume"} {
		if _, err := row.FloatValue(field); err != nil {
			return err
		}
	}
	return nil
}

func isKlineReservedField(field string) bool {
	switch field {
	case "sid", "ts", "time", "end_ms", "open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num":
		return true
	default:
		return false
	}
}

func klineWriteValue(row *DataSeries, field string) any {
	if value, ok := row.Values[field]; ok {
		return value
	}
	switch field {
	case "quote", "buy_volume":
		return float64(0)
	case "trade_num":
		return int64(0)
	default:
		return nil
	}
}

func klineExtraFields(rows []*DataSeries) []string {
	seen := make(map[string]bool)
	for _, row := range rows {
		for field := range row.Values {
			if strings.TrimSpace(field) != "" && !isKlineReservedField(field) {
				seen[field] = true
			}
		}
	}
	fields := make([]string, 0, len(seen))
	for field := range seen {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

func questKlineExtraColumns(columns []questTableColumn) []questTableColumn {
	result := make([]questTableColumn, 0, len(columns))
	for _, column := range columns {
		if strings.TrimSpace(column.Name) == "" || isKlineReservedField(column.Name) {
			continue
		}
		result = append(result, column)
	}
	return result
}

func buildQuestKlineAddColumnSQL(table string, column questTableColumn) (string, error) {
	if strings.TrimSpace(column.Name) == "" {
		return "", errors.New("questdb extension column has no name")
	}
	typeName := strings.TrimSpace(column.Type)
	if typeName == "" {
		return "", fmt.Errorf("questdb column %q has no type", column.Name)
	}
	for _, char := range typeName {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') && char != '_' && char != ' ' {
			return "", fmt.Errorf("questdb column %q has invalid type %q", column.Name, column.Type)
		}
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
		quoteIdent(table), quoteIdent(column.Name), typeName), nil
}

func ensureQuestKlineExtraColumns(ctx context.Context, q *Queries, table string, columns []questTableColumn) error {
	extraColumns := questKlineExtraColumns(columns)
	if len(extraColumns) == 0 {
		return nil
	}
	existing, err := queryQuestTableColumns(ctx, q, table)
	if err != nil {
		return err
	}
	existingByName := make(map[string]questTableColumn, len(existing))
	for _, column := range existing {
		existingByName[column.Name] = column
	}
	for _, column := range extraColumns {
		if current, ok := existingByName[column.Name]; ok {
			if !strings.EqualFold(strings.TrimSpace(current.Type), strings.TrimSpace(column.Type)) {
				return fmt.Errorf("questdb extension column %q type mismatch: target=%q source=%q", column.Name, current.Type, column.Type)
			}
			continue
		}
		statement, err := buildQuestKlineAddColumnSQL(table, column)
		if err != nil {
			return err
		}
		if _, err := q.db.Exec(ctx, statement); err != nil && !isQuestDuplicateColumnErr(err) {
			return err
		}
		existingByName[column.Name] = column
	}
	return nil
}

func questKlineDataFields(columns []questTableColumn) []string {
	return questKlineDataFieldsForTime(columns, "ts")
}

func questKlineDataFieldsForTime(columns []questTableColumn, timeColumn string) []string {
	fields := make([]string, 0, len(columns))
	for _, column := range columns {
		switch column.Name {
		case "sid", "ts", "time", "end_ms":
			continue
		default:
			if column.Name == timeColumn {
				continue
			}
			fields = append(fields, column.Name)
		}
	}
	return fields
}

func buildQuestKlineAggregateQuery(table string, columns []questTableColumn) (string, []string, error) {
	timeColumn, _, err := questRewriteSchema(columns)
	if err != nil {
		return "", nil, err
	}
	if !questRewriteHasColumn(columns, "sid") {
		return "", nil, fmt.Errorf("questdb kline table %q has no sid column", table)
	}
	fields := questKlineDataFieldsForTime(columns, timeColumn)
	if len(fields) == 0 {
		return "", nil, fmt.Errorf("questdb kline table %q has no data columns", table)
	}
	selectCols := append([]string{fmt.Sprintf("cast(%s as long)/1000", quoteIdent(timeColumn))}, quoteSeriesFields(fields)...)
	return fmt.Sprintf(`SELECT %s
FROM %s
WHERE %s = $1 AND %s >= $2 AND %s < $3
ORDER BY %s`, strings.Join(selectCols, ", "), quoteIdent(table), quoteIdent("sid"), quoteIdent(timeColumn), quoteIdent(timeColumn), quoteIdent(timeColumn)), fields, nil
}

func klineInsertColumns(timeColumn string, fields []string) []string {
	cols := []string{"sid", timeColumn, "open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num"}
	return append(cols, fields...)
}

func normalizeOHLCVSeries(rows []*DataSeries, sid int32) ([]*DataSeries, *errs.Error) {
	values := make([]*DataSeries, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		item, err := ohlcvSeriesValues(row, sid)
		if err != nil {
			return nil, err
		}
		values = append(values, item)
	}
	return values, nil
}

type iterForAddOHLCVSeriesPg struct {
	rows   []*DataSeries
	fields []string
	idx    int
}

func (r *iterForAddOHLCVSeriesPg) Next() bool {
	r.idx++
	return r.idx <= len(r.rows)
}

func (r *iterForAddOHLCVSeriesPg) Values() ([]interface{}, error) {
	row := r.rows[r.idx-1]
	values := []interface{}{row.Sid, row.TimeMS, klineWriteValue(row, "open"), klineWriteValue(row, "high"), klineWriteValue(row, "low"), klineWriteValue(row, "close"), klineWriteValue(row, "volume"), klineWriteValue(row, "quote"), klineWriteValue(row, "buy_volume"), klineWriteValue(row, "trade_num")}
	for _, field := range r.fields {
		values = append(values, klineWriteValue(row, field))
	}
	return values, nil
}

func (r *iterForAddOHLCVSeriesPg) Err() error {
	return nil
}
