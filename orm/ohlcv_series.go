package orm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
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
	klines := make([]*banexg.Kline, 0, len(rows))
	for _, row := range rows {
		_, fields, err := row.resolveOHLCV(exs)
		if err != nil {
			return nil, err
		}
		klines = append(klines, fields.kline(row.TimeMS))
	}
	return klines, nil
}

type seriesOHLCVRow struct {
	timeMS    int64
	open      float64
	high      float64
	low       float64
	close     float64
	volume    float64
	quote     float64
	buyVolume float64
	tradeNum  int64
}

type seriesSid struct {
	seriesOHLCVRow
	Sid int32
}

func (evt *DataSeries) BatchTimeMS() int64 {
	if evt == nil {
		return 0
	}
	return evt.TimeMS
}

// resampleOHLCVSeries preserves the K-line-specific aggregation contract while
// keeping the query and feeder paths in DataSeries form.
func resampleOHLCVSeries(exs *ExSymbol, tf string, rows, prev []*DataSeries, toTFMS int64,
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
	if len(prev) > 0 {
		result = append(result, prev[:len(prev)-1]...)
		var err error
		big, err = cloneOHLCVSeries(prev[len(prev)-1], exs, tf, toTFMS, isWarmUp)
		if err != nil {
			return nil, false, err
		}
	}
	aggCnt := 0
	for _, row := range rows {
		if row == nil {
			return nil, false, fmt.Errorf("series event is nil")
		}
		timeAlign := utils2.AlignTfMSecsOffset(row.TimeMS+offsetMS, toTFMS, alignOffMS)
		if big != nil && big.TimeMS == timeAlign {
			if err := mergeOHLCVSeries(big, row); err != nil {
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
		big, err = cloneOHLCVSeries(row, exs, tf, toTFMS, isWarmUp)
		if err != nil {
			return nil, false, err
		}
		big.TimeMS = timeAlign
		big.EndMS = timeAlign + toTFMS
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

func cloneOHLCVSeries(row *DataSeries, exs *ExSymbol, tf string, toTFMS int64, isWarmUp bool) (*DataSeries, error) {
	if row == nil {
		return nil, fmt.Errorf("series event is nil")
	}
	if _, err := ohlcvSeriesValues(row, 0); err != nil {
		return nil, err
	}
	cp := *row
	cp.Source = SeriesSourceKline
	cp.TimeFrame = tf
	cp.EndMS = cp.TimeMS + toTFMS
	cp.IsWarmUp = isWarmUp
	cp.ExSymbol = ResolveSeriesExSymbol(row, exs)
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
	srcValues, err := ohlcvSeriesValues(src, 0)
	if err != nil {
		return err
	}
	if srcValues.volume <= 0 {
		return nil
	}
	dstVolume, valueErr := dst.VolumeValue()
	if valueErr != nil {
		return valueErr
	}
	if dstVolume == 0 {
		dst.Values["open"] = srcValues.open
		dst.Values["high"] = srcValues.high
		dst.Values["low"] = srcValues.low
	} else {
		dstHigh, valueErr := dst.HighValue()
		if valueErr != nil {
			return valueErr
		}
		dstLow, valueErr := dst.LowValue()
		if valueErr != nil {
			return valueErr
		}
		dst.Values["high"] = max(dstHigh, srcValues.high)
		dst.Values["low"] = min(dstLow, srcValues.low)
	}
	dst.Values["close"] = srcValues.close
	dst.Values["volume"] = dstVolume + srcValues.volume
	dst.Values["quote"] = dst.QuoteValue() + srcValues.quote
	dst.Values["buy_volume"] = dst.BuyVolumeValue() + srcValues.buyVolume
	dst.Values["trade_num"] = dst.TradeNumValue() + srcValues.tradeNum
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
	tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
	revRead := startMs == 0 && limit > 0
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
	rows, subTF, err := q.querySeriesRows(exs, timeframe, startMs, finishEndMS, limit, revRead)
	if err != nil {
		return nil, err
	}
	if revRead {
		utils.ReverseArr(rows)
	}
	if subTF != "" && len(rows) > 0 {
		fromTFMS := int64(utils2.TFToSecs(subTF) * 1000)
		var lastFinish bool
		offMS := GetAlignOff(exs.ID, tfMSecs)
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
		return q.QuerySeries(exs, timeframe, endMs, maxEndMs, limit, withUnFinish)
	} else if withUnFinish && len(rows) > 0 && rows[len(rows)-1].TimeMS+tfMSecs == unFinishMS {
		unbar, _, _ := getUnFinish(q, exs.ID, timeframe, unFinishMS, unFinishMS+tfMSecs, "query")
		if unbar != nil {
			rows = append(rows, NewDataSeriesFromKline(exs, timeframe, unbar, nil, false, false))
		}
	}
	return rows, nil
}

func (q *Queries) QuerySeriesBatch(exsMap map[int32]*ExSymbol, timeframe string, startMs, endMs int64, limit int, handle func(int32, []*DataSeries)) *errs.Error {
	if len(exsMap) == 0 {
		return nil
	}
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
		return q.querySeriesBatchPg(exsMap, timeframe, startMs, finishEndMS, tfMSecs, handle)
	}
	sidTA := make([]string, 0, len(exsMap))
	for _, exs := range exsMap {
		sidTA = append(sidTA, fmt.Sprintf("%v", exs.ID))
	}
	sidText := strings.Join(sidTA, ", ")
	sql := fmt.Sprintf(`
select cast(ts as long)/1000,open,high,low,close,volume,quote,buy_volume,trade_num,sid from $tbl
where ts >= cast(%v as timestamp) and ts < cast(%v as timestamp) and sid in (%v)
order by sid,ts`, startMs*1000, finishEndMS*1000, sidText)
	subTF, pgRows, err_ := queryHyper(q, timeframe, sql, 0)
	return handleSeriesBatch(exsMap, timeframe, tfMSecs, subTF, pgRows, err_, handle)
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
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	startMS, endMS = parseDownArgs(tfMSecs, startMS, endMS, limit, withUnFinish)
	downTF, err := GetDownTF(timeFrame)
	if err != nil {
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
		return nil, nil, err
	}
	sess, conn, err := Conn(nil)
	if err != nil {
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
		return nil, nil, err
	}
	defer conn.Release()
	_, err = sess.DownOHLCV2DB(exchange, exs, downTF, startMS, endMS, pBar)
	if err != nil {
		return nil, nil, err
	}
	return sess.GetSeries(exs, timeFrame, startMS, endMS, limit, withUnFinish)
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
	if exs.Exchange == "china" && exs.Market != banexg.MarketSpot {
		parts := utils2.SplitParts(exs.Symbol)
		if len(parts) >= 2 && parts[1].Val == "888" {
			adjs, err := GetAdjs(exs.ID)
			if err != nil {
				return nil, nil, err
			}
			rows, err := q.GetAdjSeries(adjs, timeFrame, startMS, endMS, limit, withUnFinish)
			if err != nil {
				return nil, nil, err
			}
			return adjs, bindSeriesTarget(rows, exs), nil
		}
	}
	rows, err := q.QuerySeries(exs, timeFrame, startMS, endMS, limit, withUnFinish)
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
	if len(adjs) == 0 {
		return nil, nil
	}
	if endMS == 0 {
		endMS = btime.UTCStamp()
	}
	revRead := startMS == 0 && limit > 0
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
		if revRead {
			start = 0
		}
		rows, err := q.QuerySeries(f.ExSymbol, timeFrame, start, stop, limit, withUnFinish)
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

func (q *Queries) querySeriesRows(exs *ExSymbol, timeframe string, startMs, finishEndMS int64, limit int, revRead bool) ([]*DataSeries, string, *errs.Error) {
	if !IsQuestDB {
		rows, subTF, err := q.querySeriesPg(exs, timeframe, startMs, finishEndMS, limit, revRead)
		if err != nil {
			return nil, "", NewDbErr(core.ErrDbReadFail, err)
		}
		return rows, subTF, nil
	}
	var sql string
	if revRead {
		sql = fmt.Sprintf(`
select cast(ts as long)/1000,open,high,low,close,volume,quote,buy_volume,trade_num from $tbl
where sid=%d and ts < cast(%v as timestamp)
order by ts desc`, exs.ID, finishEndMS*1000)
	} else {
		if limit == 0 {
			tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
			limit = int((finishEndMS-startMs)/tfMSecs) + 1
		}
		sql = fmt.Sprintf(`
select cast(ts as long)/1000,open,high,low,close,volume,quote,buy_volume,trade_num from $tbl
where sid=%d and ts >= cast(%v as timestamp) and ts < cast(%v as timestamp)
order by ts`, exs.ID, startMs*1000, finishEndMS*1000)
	}
	subTF, pgRows, err_ := queryHyper(q, timeframe, sql, limit)
	rows, err_ := mapToSeries(exs, timeframe, pgRows, err_)
	if err_ != nil {
		return nil, "", NewDbErr(core.ErrDbReadFail, err_)
	}
	return rows, subTF, nil
}

func (q *Queries) querySeriesPg(exs *ExSymbol, timeframe string, startMs, endMs int64, limit int, revRead bool) ([]*DataSeries, string, error) {
	tblName, subTF, rate := resolveTablePg(timeframe)
	if limit > 0 && subTF != "" && rate > 1 {
		limit = rate * (limit + 1)
	}
	var sql string
	if revRead {
		sql = fmt.Sprintf(`SELECT time,open,high,low,close,volume,quote,buy_volume,trade_num FROM %s
	WHERE sid=%d AND time < %d
	ORDER BY time DESC`, tblName, exs.ID, endMs)
	} else {
		sql = fmt.Sprintf(`SELECT time,open,high,low,close,volume,quote,buy_volume,trade_num FROM %s
	WHERE sid=%d AND %s
	ORDER BY time`, tblName, exs.ID, buildPgTimeFilter(startMs, endMs))
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	pgRows, err := q.db.Query(context.Background(), sql)
	rows, err := mapToSeries(exs, timeframe, pgRows, err)
	return rows, subTF, err
}

func (q *Queries) querySeriesBatchPg(exsMap map[int32]*ExSymbol, timeframe string, startMs, finishEndMS, tfMSecs int64, handle func(int32, []*DataSeries)) *errs.Error {
	sidTA := make([]string, 0, len(exsMap))
	for _, exs := range exsMap {
		sidTA = append(sidTA, itoa(int64(exs.ID)))
	}
	tblName, subTF, _ := resolveTablePg(timeframe)
	sidText := strings.Join(sidTA, ",")
	sql := fmt.Sprintf(`SELECT time,open,high,low,close,volume,quote,buy_volume,trade_num,sid FROM %s
WHERE %s AND sid IN (%s)
ORDER BY sid, time`, tblName, buildPgTimeFilter(startMs, finishEndMS), sidText)
	pgRows, err_ := q.db.Query(context.Background(), sql)
	return handleSeriesBatch(exsMap, timeframe, tfMSecs, subTF, pgRows, err_, handle)
}

func handleSeriesBatch(exsMap map[int32]*ExSymbol, timeframe string, tfMSecs int64, subTF string, pgRows pgx.Rows, err_ error, handle func(int32, []*DataSeries)) *errs.Error {
	arrs, err_ := mapToItems(pgRows, err_, func() (*seriesSid, []any) {
		var i seriesSid
		return &i, []any{&i.timeMS, &i.open, &i.high, &i.low, &i.close, &i.volume, &i.quote, &i.buyVolume, &i.tradeNum, &i.Sid}
	})
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	initCap := max(len(arrs)/max(len(exsMap), 1), 16)
	var seriesArr []*DataSeries
	curSid := int32(0)
	fromTFMS := int64(0)
	if subTF != "" {
		fromTFMS = int64(utils2.TFToSecs(subTF) * 1000)
	}
	noFired := make(map[int32]bool)
	for _, exs := range exsMap {
		noFired[exs.ID] = true
	}
	callBack := func() *errs.Error {
		if fromTFMS > 0 {
			var lastDone bool
			var err error
			offMS := GetAlignOff(curSid, tfMSecs)
			seriesArr, lastDone, err = ResampleDataSeries(exsMap[curSid], timeframe, seriesArr, nil, tfMSecs, 0, fromTFMS, offMS, false)
			if err != nil {
				return errs.New(core.ErrInvalidBars, err)
			}
			if !lastDone && len(seriesArr) > 0 {
				seriesArr = seriesArr[:len(seriesArr)-1]
			}
		}
		if len(seriesArr) > 0 {
			delete(noFired, curSid)
			handle(curSid, seriesArr)
		}
		return nil
	}
	for _, row := range arrs {
		if row.Sid != curSid {
			if curSid > 0 && len(seriesArr) > 0 {
				if err := callBack(); err != nil {
					return err
				}
			}
			curSid = row.Sid
			seriesArr = make([]*DataSeries, 0, initCap)
		}
		seriesArr = append(seriesArr, row.toDataSeries(exsMap[row.Sid], timeframe, tfMSecs))
	}
	if curSid > 0 && len(seriesArr) > 0 {
		if err := callBack(); err != nil {
			return err
		}
	}
	for sid := range noFired {
		handle(sid, nil)
	}
	return nil
}

func mapToSeries(exs *ExSymbol, timeframe string, pgRows pgx.Rows, err_ error) ([]*DataSeries, error) {
	items, err := mapToItems(pgRows, err_, func() (*seriesOHLCVRow, []any) {
		i := &seriesOHLCVRow{}
		return i, []any{&i.timeMS, &i.open, &i.high, &i.low, &i.close, &i.volume, &i.quote, &i.buyVolume, &i.tradeNum}
	})
	if err != nil {
		return nil, err
	}
	tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
	out := make([]*DataSeries, 0, len(items))
	for _, item := range items {
		out = append(out, item.toDataSeries(exs, timeframe, tfMSecs))
	}
	return out, nil
}

func (r *seriesOHLCVRow) toDataSeries(exs *ExSymbol, timeframe string, tfMSecs int64) *DataSeries {
	sid := int32(0)
	if exs != nil {
		sid = exs.ID
	}
	return &DataSeries{
		Source:    SeriesSourceKline,
		Sid:       sid,
		TimeMS:    r.timeMS,
		EndMS:     r.timeMS + tfMSecs,
		TimeFrame: timeframe,
		Closed:    true,
		Values: map[string]any{
			"open":       r.open,
			"high":       r.high,
			"low":        r.low,
			"close":      r.close,
			"volume":     r.volume,
			"quote":      r.quote,
			"buy_volume": r.buyVolume,
			"trade_num":  r.tradeNum,
		},
		ExSymbol: exs,
	}
}

func (q *Queries) InsertOHLCVSeriesAuto(timeFrame string, exs *ExSymbol, rows []*DataSeries, aggBig bool) (int64, *errs.Error) {
	values, err := normalizeOHLCVSeries(rows, exs.ID)
	if err != nil || len(values) == 0 {
		return 0, err
	}
	startMS := values[0].timeMS
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	lastMS := values[len(values)-1].timeMS
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
	num, err := q.insertOHLCVRows(timeFrame, values)
	if err != nil {
		return num, err
	}
	return num, q.finalizeKlineInsert(exs, timeFrame, startMS, endMS, lastMS, insTs, aggBig)
}

func (q *Queries) InsertOHLCVSeries(timeFrame string, sid int32, rows []*DataSeries) (int64, *errs.Error) {
	values, err := normalizeOHLCVSeries(rows, sid)
	if err != nil || len(values) == 0 {
		return 0, err
	}
	return q.insertOHLCVRows(timeFrame, values)
}

func (q *Queries) insertOHLCVRows(timeFrame string, rows []ohlcvSeriesRow) (int64, *errs.Error) {
	if !IsQuestDB {
		return q.insertOHLCVSeriesPg(timeFrame, rows)
	}
	tblName := "kline_" + timeFrame
	ctx := context.Background()
	const colsPerRow = 10
	const batchRows = 500
	var total int64
	for i := 0; i < len(rows); i += batchRows {
		j := min(len(rows), i+batchRows)
		var b strings.Builder
		b.WriteString("insert into ")
		b.WriteString(tblName)
		b.WriteString(" (sid, ts, open, high, low, close, volume, quote, buy_volume, trade_num) values ")
		args := make([]any, 0, (j-i)*colsPerRow)
		for k := i; k < j; k++ {
			if k > i {
				b.WriteByte(',')
			}
			p := (k-i)*colsPerRow + 1
			b.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", p, p+1, p+2, p+3, p+4, p+5, p+6, p+7, p+8, p+9))
			vals := rows[k]
			args = append(args, vals.sid, time.UnixMilli(vals.timeMS).UTC(), vals.open, vals.high, vals.low, vals.close, vals.volume, vals.quote, vals.buyVolume, vals.tradeNum)
		}
		_, err := q.db.Exec(ctx, b.String(), args...)
		if err != nil {
			return total, NewDbErr(core.ErrDbExecFail, err)
		}
		total += int64(j - i)
	}
	return total, nil
}

func (q *Queries) insertOHLCVSeriesPg(timeFrame string, rows []ohlcvSeriesRow) (int64, *errs.Error) {
	tblName := "kline_" + timeFrame
	cols := []string{"sid", "time", "open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num"}
	newSrc := func() *iterForAddOHLCVSeriesPg { return &iterForAddOHLCVSeriesPg{rows: rows} }
	n, err := q.db.CopyFrom(context.Background(), pgx.Identifier{tblName}, cols, newSrc())
	if err != nil {
		tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
		startMS := rows[0].timeMS
		endMS := rows[len(rows)-1].timeMS + tfMSecs
		if delErr := delKLinesPg(q, timeFrame, rows[0].sid, startMS, endMS); delErr != nil {
			return 0, delErr
		}
		n, err = q.db.CopyFrom(context.Background(), pgx.Identifier{tblName}, cols, newSrc())
		if err != nil {
			return 0, NewDbErr(core.ErrDbExecFail, err)
		}
	}
	return n, nil
}

type ohlcvSeriesRow struct {
	sid    int32
	timeMS int64
	seriesOHLCVFields
}

func ohlcvSeriesValues(row *DataSeries, sid int32) (ohlcvSeriesRow, *errs.Error) {
	if row == nil {
		return ohlcvSeriesRow{}, errs.NewMsg(core.ErrInvalidBars, "series row is nil")
	}
	fields, err := row.readOHLCVFields()
	if err != nil {
		return ohlcvSeriesRow{}, errs.New(core.ErrInvalidBars, err)
	}
	return ohlcvSeriesRow{
		sid:               sid,
		timeMS:            row.TimeMS,
		seriesOHLCVFields: fields,
	}, nil
}

func normalizeOHLCVSeries(rows []*DataSeries, sid int32) ([]ohlcvSeriesRow, *errs.Error) {
	values := make([]ohlcvSeriesRow, 0, len(rows))
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
	rows []ohlcvSeriesRow
	idx  int
}

func (r *iterForAddOHLCVSeriesPg) Next() bool {
	r.idx++
	return r.idx <= len(r.rows)
}

func (r *iterForAddOHLCVSeriesPg) Values() ([]interface{}, error) {
	row := r.rows[r.idx-1]
	return []interface{}{row.sid, row.timeMS, row.open, row.high, row.low, row.close, row.volume, row.quote, row.buyVolume, row.tradeNum}, nil
}

func (r *iterForAddOHLCVSeriesPg) Err() error {
	return nil
}
