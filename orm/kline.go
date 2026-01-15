package orm

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

var (
	aggList = []*KlineAgg{
		// All use hypertables, and update dependent tables by themselves when inserting. Since continuous aggregation cannot be refreshed by sid, the performance is poor when refreshing after inserting historical data in batches by sid.
		//全部使用超表，自行在插入时更新依赖表。因连续聚合无法按sid刷新，在按sid批量插入历史数据后刷新时性能较差
		NewKlineAgg("1m", "kline_1m", "", "", "", "", "2 months", "12 months"),
		NewKlineAgg("5m", "kline_5m", "1m", "20m", "1m", "1m", "2 months", "12 months"),
		NewKlineAgg("15m", "kline_15m", "5m", "1h", "5m", "5m", "3 months", "16 months"),
		NewKlineAgg("1h", "kline_1h", "", "", "", "", "6 months", "3 years"),
		NewKlineAgg("1d", "kline_1d", "1h", "3d", "1h", "1h", "3 years", "20 years"),
	}
	aggMap  = make(map[string]*KlineAgg)
	downTfs = map[string]struct{}{"1m": {}, "15m": {}, "1h": {}, "1d": {}}
)

const (
	aggFields = `
  first(open, time) AS open,  
  max(high) AS high,
  min(low) AS low, 
  last(close, time) AS close,
  sum(volume) AS volume`
	klineInsConflict = `
ON CONFLICT (sid, time)
DO UPDATE SET 
open = EXCLUDED.open,
high = EXCLUDED.high,
low = EXCLUDED.low,
close = EXCLUDED.close,
volume = EXCLUDED.volume,
info = EXCLUDED.info`
)

func init() {
	for _, agg := range aggList {
		aggMap[agg.TimeFrame] = agg
	}
}

func aggCol(name, by string) string {
	if by == "first" || by == "last" {
		return fmt.Sprintf("%s(%s, time) AS %s", by, name, name)
	} else if by == "max" || by == "min" || by == "sum" {
		return fmt.Sprintf("%s(%s) AS %s", by, name, name)
	} else {
		panic("unknown agg by: " + by)
	}
}

func (q *Queries) QueryOHLCV(exs *ExSymbol, timeframe string, startMs, endMs int64, limit int, withUnFinish bool) ([]*banexg.Kline, *errs.Error) {
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
	var dctSql string
	if revRead {
		// No start time provided, quantity limit provided, search in reverse chronological order
		// 未提供开始时间，提供了数量限制，按时间倒序搜索
		dctSql = fmt.Sprintf(`
select time,open,high,low,close,volume,info from $tbl
where sid=%d and time < %v
order by time desc`, exs.ID, finishEndMS)
	} else {
		if limit == 0 {
			limit = int((finishEndMS-startMs)/tfMSecs) + 1
		}
		dctSql = fmt.Sprintf(`
select time,open,high,low,close,volume,info from $tbl
where sid=%d and time >= %v and time < %v
order by time`, exs.ID, startMs, finishEndMS)
	}
	subTF, rows, err_ := queryHyper(q, timeframe, dctSql, limit)
	klines, err_ := mapToKlines(rows, err_)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	if revRead {
		// If read in reverse order, reverse it again to make the time ascending
		// 倒序读取的，再次逆序，使时间升序
		utils.ReverseArr(klines)
	}
	if subTF != "" && len(klines) > 0 {
		fromTfMSecs := int64(utils2.TFToSecs(subTF) * 1000)
		var lastFinish bool
		offMS := GetAlignOff(exs.ID, tfMSecs)
		infoBy := exs.InfoBy()
		klines, lastFinish = utils.BuildOHLCV(klines, tfMSecs, 0, nil, fromTfMSecs, offMS, infoBy)
		if !lastFinish && len(klines) > 0 {
			klines = klines[:len(klines)-1]
		}
	}
	if len(klines) > limit {
		if revRead {
			klines = klines[len(klines)-limit:]
		} else {
			klines = klines[:limit]
		}
	}
	if len(klines) == 0 && maxEndMs-endMs > tfMSecs {
		return q.QueryOHLCV(exs, timeframe, endMs, maxEndMs, limit, withUnFinish)
	} else if withUnFinish && len(klines) > 0 && klines[len(klines)-1].Time+tfMSecs == unFinishMS {
		unbar, _, _ := getUnFinish(q, exs.ID, timeframe, unFinishMS, unFinishMS+tfMSecs, "query")
		if unbar != nil {
			klines = append(klines, unbar)
		}
	}
	return klines, nil
}

type KlineSid struct {
	banexg.Kline
	Sid int32
}

func (q *Queries) QueryOHLCVBatch(exsMap map[int32]*ExSymbol, timeframe string, startMs, endMs int64, limit int, handle func(int32, []*banexg.Kline)) *errs.Error {
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
	sidTA := make([]string, 0, len(exsMap))
	for _, exs := range exsMap {
		sidTA = append(sidTA, fmt.Sprintf("%v", exs.ID))
	}
	sidText := strings.Join(sidTA, ", ")
	dctSql := fmt.Sprintf(`
select time,open,high,low,close,volume,info,sid from $tbl
where time >= %v and time < %v and sid in (%v)
order by sid,time`, startMs, finishEndMS, sidText)
	subTF, rows, err_ := queryHyper(q, timeframe, dctSql, 0)
	arrs, err_ := mapToItems(rows, err_, func() (*KlineSid, []any) {
		var i KlineSid
		return &i, []any{&i.Time, &i.Open, &i.High, &i.Low, &i.Close, &i.Volume, &i.Info, &i.Sid}
	})
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	initCap := max(len(arrs)/len(exsMap), 16)
	var klineArr []*banexg.Kline
	curSid := int32(0)
	fromTfMSecs := int64(0)
	if subTF != "" {
		fromTfMSecs = int64(utils2.TFToSecs(subTF) * 1000)
	}
	noFired := make(map[int32]bool)
	for _, exs := range exsMap {
		noFired[exs.ID] = true
	}
	callBack := func() {
		if fromTfMSecs > 0 {
			var lastDone bool
			offMS := GetAlignOff(curSid, tfMSecs)
			infoBy := exsMap[curSid].InfoBy()
			klineArr, lastDone = utils.BuildOHLCV(klineArr, tfMSecs, 0, nil, fromTfMSecs, offMS, infoBy)
			if !lastDone && len(klineArr) > 0 {
				klineArr = klineArr[:len(klineArr)-1]
			}
		}
		if len(klineArr) > 0 {
			delete(noFired, curSid)
			handle(curSid, klineArr)
		}
	}
	// callBack for kline pairs
	for _, k := range arrs {
		if k.Sid != curSid {
			if curSid > 0 && len(klineArr) > 0 {
				callBack()
			}
			curSid = k.Sid
			klineArr = make([]*banexg.Kline, 0, initCap)
		}
		klineArr = append(klineArr, &banexg.Kline{Time: k.Time, Open: k.Open, High: k.High, Low: k.Low,
			Close: k.Close, Volume: k.Volume, Info: k.Info})
	}
	if curSid > 0 && len(klineArr) > 0 {
		callBack()
	}
	// callBack for no data sids
	for sid := range noFired {
		handle(sid, nil)
	}
	return nil
}

func (q *Queries) getKLineTimes(sid int32, timeframe string, startMs, endMs int64) ([]int64, *errs.Error) {
	tblName := "kline_" + timeframe
	dctSql := fmt.Sprintf(`
select time from %s
where sid=%d and time >= %v and time < %v
order by time`, tblName, sid, startMs, endMs)
	rows, err_ := q.db.Query(context.Background(), dctSql)
	res, err_ := mapToItems(rows, err_, func() (*int64, []any) {
		var t int64
		return &t, []any{&t}
	})
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	resList := make([]int64, len(res))
	for i, v := range res {
		resList[i] = *v
	}
	return resList, nil
}

func queryHyper(sess *Queries, timeFrame, sql string, limit int, args ...interface{}) (string, pgx.Rows, error) {
	agg, ok := aggMap[timeFrame]
	var subTF, table string
	var rate int
	if ok {
		table = agg.Table
	} else {
		// If there is no direct match for a timeframe, aggregate from the closest child timeframe
		// 时间帧没有直接符合的，从最接近的子timeframe聚合
		subTF, table, rate = getSubTf(timeFrame)
		if limit > 0 && rate > 1 {
			limit = rate * (limit + 1)
		}
	}
	if limit > 0 {
		sql += fmt.Sprintf(" limit %v", limit)
	}
	sql = strings.Replace(sql, "$tbl", table, 1)
	rows, err := sess.db.Query(context.Background(), sql, args...)
	return subTF, rows, err
}

func mapToKlines(rows pgx.Rows, err_ error) ([]*banexg.Kline, error) {
	return mapToItems(rows, err_, func() (*banexg.Kline, []any) {
		var i banexg.Kline
		return &i, []any{&i.Time, &i.Open, &i.High, &i.Low, &i.Close, &i.Volume, &i.Info}
	})
}

func getSubTf(timeFrame string) (string, string, int) {
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	for i := len(aggList) - 1; i >= 0; i-- {
		agg := aggList[i]
		if agg.MSecs >= tfMSecs {
			continue
		}
		if tfMSecs%agg.MSecs == 0 {
			return agg.TimeFrame, agg.Table, int(tfMSecs / agg.MSecs)
		}
	}
	return "", "", 0
}

func (q *Queries) PurgeKlineUn() *errs.Error {
	sql := "delete from kline_un"
	return q.Exec(sql)
}

/*
getUnFinish
Query the unfinished bars for a given period. The given period can be a preservation period of 1m, 5m, 15m, 1h, 1d; It can also be an aggregation period such as 4h, 3d
This method has two purposes: querying users for the latest data (possibly aggregation cycles); Calc updates the unfinished bar of the large cycle from the sub cycle (which cannot be an aggregation cycle)
The returned error indicates that the data does not exist
查询给定周期的未完成bar。给定周期可以是保存的周期1m,5m,15m,1h,1d；也可以是聚合周期如4h,3d
此方法两种用途：query用户查询最新数据（可能是聚合周期）；calc从子周期更新大周期的未完成bar（不可能是聚合周期）
返回的错误表示数据不存在
*/
func getUnFinish(sess *Queries, sid int32, timeFrame string, startMS, endMS int64, mode string) (*banexg.Kline, int64, error) {
	if mode != "calc" && mode != "query" {
		panic(fmt.Sprintf("`mode` of getUnFinish must be calc/query, current: %s", mode))
	}
	ctx := context.Background()
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	barEndMS := int64(0)
	fromTF := timeFrame
	var bigKlines = make([]*banexg.Kline, 0)
	if _, ok := aggMap[timeFrame]; mode == "calc" || !ok {
		// Collect data from completed sub cycles
		// 从已完成的子周期中归集数据
		fromTF, _, _ = getSubTf(timeFrame)
		aggFrom := "kline_" + fromTF
		sql := fmt.Sprintf(`select time,open,high,low,close,volume,info from %s
where sid=%d and time >= %v and time < %v`, aggFrom, sid, startMS, endMS)
		rows, err_ := sess.db.Query(ctx, sql)
		klines, err_ := mapToKlines(rows, err_)
		if err_ != nil {
			return nil, 0, err_
		}
		offMS := GetAlignOff(sid, tfMSecs)
		infoBy := GetSymbolByID(sid).InfoBy()
		bigKlines, _ = utils.BuildOHLCV(klines, tfMSecs, 0, nil, 0, offMS, infoBy)
		if len(klines) > 0 {
			barEndMS = klines[len(klines)-1].Time + int64(utils2.TFToSecs(fromTF)*1000)
		}
	}
	// Querying data from unfinished cycles/sub cycles
	// 从未完成的周期/子周期中查询数据
	sql := fmt.Sprintf(`SELECT start_ms,open,high,low,close,volume,info,stop_ms FROM kline_un
						where sid=%d and timeframe='%s' and start_ms >= %d
						limit 1`, sid, fromTF, startMS)
	row := sess.db.QueryRow(ctx, sql)
	var unbar = &banexg.Kline{}
	var unToMS = int64(0)
	err_ := row.Scan(&unbar.Time, &unbar.Open, &unbar.High, &unbar.Low, &unbar.Close, &unbar.Volume, &unbar.Info, &unToMS)
	if err_ != nil {
		return nil, 0, err_
	} else if unbar.Volume > 0 {
		bigKlines = append(bigKlines, unbar)
		barEndMS = max(barEndMS, unToMS)
	}
	if len(bigKlines) == 0 {
		return nil, barEndMS, nil
	}
	if len(bigKlines) == 1 {
		return bigKlines[0], barEndMS, nil
	} else {
		var res = bigKlines[0]
		for _, bar := range bigKlines[1:] {
			res.High = max(res.High, bar.High)
			res.Low = min(res.Low, bar.Low)
			res.Volume += bar.Volume
		}
		last := bigKlines[len(bigKlines)-1]
		res.Close = last.Close
		res.Info = last.Info
		return res, barEndMS, nil
	}
}

var alignOffs = make(map[int32]map[int64]int64)
var lockAlignOff deadlock.Mutex

func GetAlignOff(sid int32, toTfMSecs int64) int64 {
	lockAlignOff.Lock()
	defer lockAlignOff.Unlock()
	data, ok1 := alignOffs[sid]
	if ok1 {
		if resVal, ok2 := data[toTfMSecs]; ok2 {
			return resVal
		}
	} else {
		data = make(map[int64]int64)
		alignOffs[sid] = data
	}
	exs := GetSymbolByID(sid)
	offMS := int64(exg.GetAlignOff(exs.Exchange, int(toTfMSecs/1000)) * 1000)
	data[toTfMSecs] = offMS
	return offMS
}

/*
calcUnFinish
Calculate the unfinished bars of large cycles from sub cycles
从子周期计算大周期的未完成bar
*/
func calcUnFinish(sid int32, tfMSecs, fromTfMSecs int64, arr []*banexg.Kline) *banexg.Kline {
	offMS := GetAlignOff(sid, tfMSecs)
	infoBy := GetSymbolByID(sid).InfoBy()
	merged, _ := utils.BuildOHLCV(arr, tfMSecs, 0, nil, fromTfMSecs, offMS, infoBy)
	if len(merged) == 0 {
		return nil
	}
	out := merged[len(merged)-1]
	return out
}

func updateUnFinish(sess *Queries, agg *KlineAgg, sid int32, subTF string, startMS, endMS int64, klines []*banexg.Kline) *errs.Error {
	tfMSecs := int64(utils2.TFToSecs(agg.TimeFrame) * 1000)
	finished := endMS%tfMSecs == 0
	whereSql := fmt.Sprintf("where sid=%v and timeframe='%v';", sid, agg.TimeFrame)
	fromWhere := "from kline_un " + whereSql
	ctx := context.Background()
	if finished {
		_, err_ := sess.db.Exec(ctx, "delete "+fromWhere)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		return nil
	}
	if len(klines) == 0 {
		log.Warn("skip unFinish for empty", zap.Int64("s", startMS), zap.Int64("e", endMS))
		return nil
	}
	fromTfMSecs := int64(utils2.TFToSecs(subTF) * 1000)
	unFinish := calcUnFinish(sid, tfMSecs, fromTfMSecs, klines)
	if unFinish == nil {
		_, err_ := sess.db.Exec(ctx, "delete "+fromWhere)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		return nil
	}
	unFinish.Time = startMS
	barStartMS := utils2.AlignTfMSecs(startMS, tfMSecs)
	barEndMS := utils2.AlignTfMSecs(endMS, tfMSecs)
	if barStartMS == barEndMS {
		// When inserting the start and end timestamps of a sub cycle, corresponding to the current cycle and belonging to the same bar, fast updates are executed
		// 当子周期插入开始结束时间戳，对应到当前周期，属于同一个bar时，才执行快速更新
		sql := "select start_ms,open,high,low,close,volume,info,stop_ms " + fromWhere
		row := sess.db.QueryRow(ctx, sql)
		var unBar KlineUn
		err_ := row.Scan(&unBar.StartMs, &unBar.Open, &unBar.High, &unBar.Low, &unBar.Close, &unBar.Volume, &unBar.StopMs)
		if err_ == nil && unBar.StopMs == startMS {
			//When the start timestamp of this insertion matches exactly with the end timestamp of the unfinished bar, it is considered valid
			//当本次插入开始时间戳，和未完成bar结束时间戳完全匹配时，认为有效
			unBar.High = max(unBar.High, unFinish.High)
			unBar.Low = min(unBar.Low, unFinish.Low)
			unBar.Close = unFinish.Close
			unBar.Volume += unFinish.Volume
			unBar.StopMs = endMS
			updSql := fmt.Sprintf("update kline_un set high=%v,low=%v,close=%v,volume=%v,info=%v,stop_ms=%v %s",
				unBar.High, unBar.Low, unBar.Close, unBar.Volume, unBar.Info, unBar.StopMs, whereSql)
			_, err_ = sess.db.Exec(ctx, updSql)
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
			return nil
		}
	}
	//When rapid updates are unavailable, collect from sub cycles
	//当快速更新不可用时，从子周期归集
	return sess.SetUnfinish(sid, agg.TimeFrame, endMS, unFinish)
}

func (q *Queries) SetUnfinish(sid int32, tf string, endMS int64, bar *banexg.Kline) *errs.Error {
	whereSql := fmt.Sprintf("where sid=%v and timeframe='%v';", sid, tf)
	updSql := fmt.Sprintf("update kline_un set start_ms=%v,stop_ms=%v,open=%v,high=%v,low=%v,close=%v,volume=%v,info=%v %s",
		bar.Time, endMS, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, bar.Info, whereSql)
	res, err_ := q.db.Exec(context.Background(), updSql)
	var err *errs.Error
	if err_ != nil {
		err = NewDbErr(core.ErrDbExecFail, err_)
	} else if res.RowsAffected() == 0 {
		err = q.Exec(`insert into kline_un (sid, start_ms, stop_ms, open, high, low, close, volume, info, timeframe) 
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, sid, bar.Time, endMS, bar.Open, bar.High, bar.Low,
			bar.Close, bar.Volume, bar.Info, tf)
	}
	return err
}

// iterForAddKLines implements pgx.CopyFromSource.
type iterForAddKLines struct {
	rows                 []*KlineSid
	skippedFirstNextCall bool
}

func (r *iterForAddKLines) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	if !r.skippedFirstNextCall {
		r.skippedFirstNextCall = true
		return true
	}
	r.rows = r.rows[1:]
	return len(r.rows) > 0
}

func (r iterForAddKLines) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0].Sid,
		r.rows[0].Time,
		r.rows[0].Open,
		r.rows[0].High,
		r.rows[0].Low,
		r.rows[0].Close,
		r.rows[0].Volume,
		r.rows[0].Info,
	}, nil
}

func (r iterForAddKLines) Err() error {
	return nil
}

/*
InsertKLines
Only batch insert K-lines. To update associated information simultaneously, please use InsertKLinesAuto
只批量插入K线，如需同时更新关联信息，请使用InsertKLinesAuto
*/
func (q *Queries) InsertKLines(timeFrame string, sid int32, arr []*banexg.Kline, delOnDump bool) (int64, *errs.Error) {
	arrLen := len(arr)
	if arrLen == 0 {
		return 0, nil
	}
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	startMS, endMS := arr[0].Time, arr[arrLen-1].Time+tfMSecs
	log.Debug("insert klines", zap.String("tf", timeFrame), zap.Int32("sid", sid),
		zap.Int("num", arrLen), zap.Int64("start", startMS), zap.Int64("end", endMS))
	tblName := "kline_" + timeFrame
	var adds = make([]*KlineSid, arrLen)
	for i, v := range arr {
		adds[i] = &KlineSid{
			Kline: *v,
			Sid:   sid,
		}
	}
	ctx := context.Background()
	cols := []string{"sid", "time", "open", "high", "low", "close", "volume", "info"}
	num, err_ := q.db.CopyFrom(ctx, []string{tblName}, cols, &iterForAddKLines{rows: adds})
	if err_ != nil {
		var pgErr *pgconn.PgError
		if errors.As(err_, &pgErr) {
			if pgErr.Code == "23505" {
				if delOnDump {
					// 报错插入重复数据，删除重试
					err := q.DelKLines(sid, timeFrame, startMS, endMS)
					if err == nil {
						return q.InsertKLines(timeFrame, sid, arr, false)
					}
				}
				return 0, NewDbErr(core.ErrDbUniqueViolation, err_)
			}
		}
		return 0, NewDbErr(core.ErrDbExecFail, err_)
	}
	return num, nil
}

/*
InsertKLinesAuto
Insert K-line into the database and call updateKRange to update associated information
Before calling this method, it is necessary to determine whether it already exists in the database through GetKlineRange to avoid duplicate insertions
插入K线到数据库，同时调用UpdateKRange更新关联信息
调用此方法前必须通过GetKlineRange自行判断数据库中是否已存在，避免重复插入
*/
func (q *Queries) InsertKLinesAuto(timeFrame string, exs *ExSymbol, arr []*banexg.Kline, aggBig bool) (int64, *errs.Error) {
	if len(arr) == 0 {
		return 0, nil
	}
	startMS := arr[0].Time
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	endMS := arr[len(arr)-1].Time + tfMSecs
	insId, err := q.AddInsJob(AddInsKlineParams{
		Sid:       exs.ID,
		Timeframe: timeFrame,
		StartMs:   startMS,
		StopMs:    endMS,
	})
	if err != nil || insId == 0 {
		return 0, err
	}
	defer func() {
		if insId == 0 {
			return
		}
		if err_ := q.DelInsKline(context.Background(), insId); err_ != nil {
			log.Warn("DelInsKline fail", zap.Int32("id", insId), zap.Error(err_))
		}
	}()
	num, err := q.InsertKLines(timeFrame, exs.ID, arr, true)
	if err != nil {
		return num, err
	}
	err = q.UpdateKRange(exs, timeFrame, startMS, endMS, arr, aggBig)
	return num, err
}

/*
UpdateKRange
1. Update the effective range of the K-line
2. Search for holes and update Khole
3. Update continuous aggregation with larger cycles
1. 更新K线的有效区间
2. 搜索空洞，更新Khole
3. 更新更大周期的连续聚合
*/
func (q *Queries) UpdateKRange(exs *ExSymbol, timeFrame string, startMS, endMS int64, klines []*banexg.Kline, aggBig bool) *errs.Error {
	// Update the effective range of intervals
	// 更新有效区间范围
	err := q.updateKLineRange(exs.ID, timeFrame, startMS, endMS)
	if err != nil {
		return err
	}
	// Search for holes, update khole
	// 搜索空洞，更新khole
	err = q.updateKHoles(exs.ID, timeFrame, startMS, endMS, true)
	if err != nil {
		return err
	}
	if !aggBig {
		return nil
	}
	// Update a larger super table
	// 更新更大的超表
	return q.updateBigHyper(exs, timeFrame, startMS, endMS, klines)
}

/*
CalcKLineRange
Calculate the effective range of the specified period K-line within the specified range.
计算指定周期K线在指定范围内，有效区间。
*/
func (q *Queries) CalcKLineRange(sid int32, timeFrame string, start, end int64) (int64, int64, *errs.Error) {
	tblName := "kline_" + timeFrame
	sql := fmt.Sprintf("select min(time),max(time) from %s where sid=%v", tblName, sid)
	if start > 0 {
		sql += fmt.Sprintf(" and time>=%v", start)
	}
	if end > 0 {
		sql += fmt.Sprintf(" and time<%v", end)
	}
	ctx := context.Background()
	row := q.db.QueryRow(ctx, sql)
	var realStart, realEnd *int64
	err_ := row.Scan(&realStart, &realEnd)
	if err_ != nil {
		return 0, 0, NewDbErr(core.ErrDbReadFail, err_)
	}
	if realEnd == nil {
		realStart = new(int64)
		realEnd = new(int64)
	}
	if *realEnd > 0 {
		// 修正为实际结束时间
		*realEnd += int64(utils2.TFToSecs(timeFrame) * 1000)
	}
	return *realStart, *realEnd, nil
}

func (q *Queries) CalcKLineRanges(timeFrame string, sids map[int32]bool) (map[int32][2]int64, *errs.Error) {
	tblName := "kline_" + timeFrame
	if len(sids) > 0 {
		var b strings.Builder
		b.WriteString(" where sid in (")
		first := true
		for sid := range sids {
			if !first {
				b.WriteRune(',')
			}
			first = false
			b.WriteString(fmt.Sprintf("%v", sid))
		}
		b.WriteRune(')')
		tblName += b.String()
	}
	sql := fmt.Sprintf("select sid,min(time),max(time) from %s group by sid", tblName)
	ctx := context.Background()
	rows, err_ := q.db.Query(ctx, sql)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	res := make(map[int32][2]int64)
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	for rows.Next() {
		var sid int32
		var realStart, realEnd int64
		err_ = rows.Scan(&sid, &realStart, &realEnd)
		res[sid] = [2]int64{realStart, realEnd + tfMSecs}
		if err_ != nil {
			return res, NewDbErr(core.ErrDbReadFail, err_)
		}
	}
	err_ = rows.Err()
	if err_ != nil {
		return res, NewDbErr(core.ErrDbReadFail, err_)
	}
	return res, nil
}

func (q *Queries) updateKLineRange(sid int32, timeFrame string, startMS, endMS int64) *errs.Error {
	// 更新有效区间范围
	var err_ error
	ctx := context.Background()
	realStart, realEnd, err := q.CalcKLineRange(sid, timeFrame, 0, 0)
	if err != nil {
		if startMS > 0 && endMS > 0 {
			_, err_ = q.AddKInfo(ctx, AddKInfoParams{Sid: sid, Timeframe: timeFrame, Start: startMS, Stop: endMS})
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
			log.Debug("add kinfo", zap.Int32("sid", sid), zap.String("tf", timeFrame),
				zap.Int64("start", startMS), zap.Int64("end", endMS))
		}
		return nil
	}
	if realStart == 0 || realEnd == 0 {
		if startMS == 0 || endMS == 0 {
			return nil
		}
		realStart, realEnd = startMS, endMS
	} else if startMS > 0 && endMS > 0 {
		realStart = min(realStart, startMS)
		realEnd = max(realEnd, endMS)
	}
	oldStart, oldEnd := q.GetKlineRange(sid, timeFrame)
	if oldStart == 0 && oldEnd == 0 {
		_, err_ = q.AddKInfo(ctx, AddKInfoParams{Sid: sid, Timeframe: timeFrame, Start: realStart, Stop: realEnd})
	} else {
		err_ = q.SetKInfo(ctx, SetKInfoParams{Sid: sid, Timeframe: timeFrame, Start: realStart, Stop: realEnd})
	}
	if err_ != nil {
		var pgErr *pgconn.PgError
		if errors.As(err_, &pgErr) {
			if pgErr.Code == "23505" {
				err_ = q.SetKInfo(ctx, SetKInfoParams{Sid: sid, Timeframe: timeFrame, Start: realStart, Stop: realEnd})
			}
		}
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	log.Debug("set kinfo", zap.Int32("sid", sid), zap.String("tf", timeFrame),
		zap.Int64("start", startMS), zap.Int64("end", endMS))
	return nil
}

func (q *Queries) updateKHoles(sid int32, timeFrame string, startMS, endMS int64, isCont bool) *errs.Error {
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	barTimes, err := q.getKLineTimes(sid, timeFrame, startMS, endMS)
	if err != nil {
		return err
	}
	// Find the missing kholes from all bar times
	// 从所有bar时间中找到缺失的kholes
	holes := make([][2]int64, 0)
	if len(barTimes) == 0 {
		holes = append(holes, [2]int64{startMS, endMS})
	} else {
		if barTimes[0] > startMS {
			holes = append(holes, [2]int64{startMS, barTimes[0]})
		}
		prevTime := barTimes[0]
		for _, time := range barTimes[1:] {
			intv := time - prevTime
			if intv > tfMSecs {
				holes = append(holes, [2]int64{prevTime + tfMSecs, time})
			} else if intv < tfMSecs {
				log.Warn("invalid timeframe or kline", zap.Int32("sid", sid), zap.String("tf", timeFrame),
					zap.Int64("intv", intv/1000), zap.Int64("tfmsecs", tfMSecs/1000), zap.Int64("time", time))
			}
			prevTime = time
		}
		maxEnd := utils2.AlignTfMSecs(btime.UTCStamp(), tfMSecs) - tfMSecs
		if maxEnd-prevTime > tfMSecs*5 && endMS-prevTime > tfMSecs {
			holes = append(holes, [2]int64{prevTime + tfMSecs, min(endMS, maxEnd)})
		}
	}
	if len(holes) == 0 {
		return nil
	}
	// Check the statutory rest periods and filter out non trading periods
	// 检查法定休息时间段，过滤非交易时间段
	exs := GetSymbolByID(sid)
	if exs == nil {
		log.Warn("no ExSymbol found", zap.Int32("sid", sid))
		return nil
	}
	exchange, err := exg.GetWith(exs.Exchange, exs.Market, "")
	if err != nil {
		return err
	}
	// Due to the fact that some trading days in the historical data have not been entered, the filtering of K-line in the trading calendar is not applicable
	// 由于历史数据中部分交易日未录入，故不适用交易日历过滤K线
	susp, err := q.GetExSHoles(exchange, exs, startMS, endMS, true)
	if err != nil {
		return err
	}
	if len(susp) > 0 {
		// Filter out non trading time periods
		// 过滤掉非交易时间段
		hs := make([][2]int64, 0, len(holes))
		si, hi := 0, -1
		for hi+1 < len(holes) {
			hi += 1
			h := holes[hi]
			for si < len(susp) && susp[si][1] <= h[0] {
				si += 1
			}
			if si >= len(susp) {
				hs = append(hs, holes[hi:]...)
				break
			}
			s := susp[si]
			if s[0] > h[0] {
				// 空洞左侧是有效区间
				hs = append(hs, [2]int64{h[0], min(h[1], s[0])})
			}
			if h[1] > s[1] {
				// 右侧有可能溢出
				holes[hi][0] = s[1]
				hi -= 1
			}
		}
		holes = hs
	}
	// Filter out holes that are too small
	// 过滤太小的空洞
	if tfMSecs == 60000 {
		exInfo := exchange.Info()
		hs := make([][2]int64, 0, len(holes))
		wids := make(map[int]int)
		for _, h := range holes {
			num := int((h[1] - h[0]) / tfMSecs)
			if num <= exInfo.Min1mHole {
				continue
			}
			cnt, _ := wids[num]
			wids[num] = cnt + 1
			hs = append(hs, h)
		}
		//if len(wids) > 0 {
		//	var as = make([]string, 0, len(wids))
		//	for k, v := range wids {
		//		as = append(as, fmt.Sprintf("%v:%v", k, v))
		//	}
		//	log.Info("hole widths", zap.Int32("sid", sid), zap.String("d", strings.Join(as, " ")))
		//}
		holes = hs
	}
	// Query the recorded kholes and merge them
	// 查询已记录的khole，进行合并
	ctx := context.Background()
	resHoles, err_ := q.GetKHoles(ctx, GetKHolesParams{Sid: sid, Timeframe: timeFrame, Start: startMS, Stop: endMS})
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	for _, h := range holes {
		resHoles = append(resHoles, &KHole{Sid: sid, Timeframe: timeFrame, Start: h[0], Stop: h[1], NoData: isCont})
	}
	slices.SortFunc(resHoles, func(a, b *KHole) int {
		if a.Start != b.Start {
			return int((a.Start - b.Start) / 1000)
		}
		if a.NoData == b.NoData {
			return 0
		}
		if a.NoData {
			// 优先将NoData的放在前面
			return -1
		} else {
			return 1
		}
	})
	merged := make([]*KHole, 0)
	delIDs := make([]int64, 0)
	var prev *KHole
	for _, h := range resHoles {
		if h.Start >= h.Stop {
			if h.ID > 0 {
				delIDs = append(delIDs, h.ID)
			}
			continue
		}
		if prev == nil || prev.Stop < h.Start {
			//与前一个洞不连续，出现缺口
			merged = append(merged, h)
			prev = h
		} else {
			// 与前一个洞连续，可能重合
			if prev.NoData == h.NoData || prev.Stop >= h.Stop {
				// 当与前一个洞NoData一致，或者完全被前一个包含
				if h.Stop > prev.Stop {
					prev.Stop = h.Stop
				}
				if h.ID > 0 {
					delIDs = append(delIDs, h.ID)
				}
			} else {
				// NoData不一致(前true后false)，且当前有超出
				h.Start = prev.Stop
				merged = append(merged, h)
				prev = h
			}
		}
	}
	// Update or insert the merged kholes into the database
	// 将合并后的kholes更新或插入到数据库
	err = q.DelKHoleIDs(delIDs...)
	if err != nil {
		return err
	}
	var adds []AddKHolesParams
	for _, h := range merged {
		if h.ID == 0 {
			adds = append(adds, AddKHolesParams{Sid: h.Sid, Timeframe: h.Timeframe, Start: h.Start, Stop: h.Stop, NoData: h.NoData})
		} else {
			err_ = q.SetKHole(ctx, SetKHoleParams{ID: h.ID, Start: h.Start, Stop: h.Stop, NoData: h.NoData})
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
		}
	}
	if len(adds) > 0 {
		_, err_ = q.AddKHoles(ctx, adds)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	return nil
}

func (q *Queries) updateBigHyper(exs *ExSymbol, timeFrame string, startMS, endMS int64, klines []*banexg.Kline) *errs.Error {
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	aggTfs := map[string]bool{timeFrame: true}
	aggJobs := make([]*KlineAgg, 0)
	unFinishJobs := make([]*KlineAgg, 0)
	curMS := btime.TimeMS()
	for _, item := range aggList {
		if item.MSecs <= tfMSecs {
			//Skipping small dimensions; Skip irrelevant continuous aggregation
			//跳过过小维度；跳过无关的连续聚合
			continue
		}
		startAlignMS := utils2.AlignTfMSecs(startMS, item.MSecs)
		endAlignMS := utils2.AlignTfMSecs(endMS, item.MSecs)
		if _, ok := aggTfs[item.AggFrom]; ok && startAlignMS < endAlignMS {
			// startAlign < endAlign说明：插入的数据所属bar刚好完成
			aggTfs[item.TimeFrame] = true
			aggJobs = append(aggJobs, item)
		}
		unBarStartMs := utils2.AlignTfMSecs(curMS, item.MSecs)
		if endAlignMS >= unBarStartMs && endMS >= endAlignMS {
			// Only attempt to update when the data involves bars that have not been completed in the current cycle; Only pass in relevant bars to improve efficiency
			// 仅当数据涉及当前周期未完成bar时，才尝试更新；仅传入相关的bar，提高效率
			unFinishJobs = append(unFinishJobs, item)
		}
	}
	if len(unFinishJobs) > 0 {
		var err *errs.Error
		if len(klines) == 0 {
			// Take the first time after aligning the maximum cycle as the starting time
			// 取最大周期的对齐后第一个时间作为开始时间
			msecs := unFinishJobs[len(unFinishJobs)-1].MSecs
			startAlign := utils2.AlignTfMSecs(startMS, msecs)
			klines, err = q.QueryOHLCV(exs, timeFrame, startAlign, endMS, 0, true)
			if err != nil {
				return err
			}
		}
		for _, item := range unFinishJobs {
			err = updateUnFinish(q, item, exs.ID, timeFrame, startMS, endMS, klines)
			if err != nil {
				return err
			}
		}
	}
	if len(aggJobs) > 0 {
		for _, item := range aggJobs {
			err := q.refreshAgg(item, exs.ID, startMS, endMS, "", exs.InfoBy(), true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (q *Queries) refreshAgg(item *KlineAgg, sid int32, orgStartMS, orgEndMS int64, aggFrom, infoBy string, isCont bool) *errs.Error {
	tfMSecs := item.MSecs
	startMS := utils2.AlignTfMSecs(orgStartMS, tfMSecs)
	endMS := utils2.AlignTfMSecs(orgEndMS, tfMSecs)
	if startMS == endMS && endMS < orgStartMS {
		// 没有出现新的完成的bar数据，无需更新
		// 前2个相等，说明：插入的数据所属bar尚未完成。
		// start_ms < org_start_ms说明：插入的数据不是所属bar的第一个数据
		return nil
	}
	// It is possible that startMs happens to be the beginning of the next bar, and the previous one requires -1
	// 有可能startMs刚好是下一个bar的开始，前一个需要-1
	aggStart := startMS - tfMSecs
	oldStart, oldEnd := q.GetKlineRange(sid, item.TimeFrame)
	if oldStart > 0 && oldEnd > oldStart {
		// Avoid voids or data errors
		// 避免出现空洞或数据错误
		aggStart = min(aggStart, oldEnd)
		endMS = max(endMS, oldStart)
	}
	if aggFrom == "" {
		aggFrom = item.AggFrom
	}
	tblName := "kline_" + aggFrom
	infoCol := aggCol("info", infoBy)
	sql := fmt.Sprintf(`
select sid,"time"/%d*%d as atime,%s,%s
from %s where sid=%d and time>=%v and time<%v
GROUP BY sid, 2 
ORDER BY sid, 2`, tfMSecs, tfMSecs, aggFields, infoCol, tblName, sid, aggStart, endMS)
	finalSql := fmt.Sprintf(`
insert into %s (sid, time, open, high, low, close, volume, info)
%s %s`, item.Table, sql, klineInsConflict)
	_, err_ := q.db.Exec(context.Background(), finalSql)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	// Update the effective range of intervals
	// 更新有效区间范围
	err := q.updateKLineRange(sid, item.TimeFrame, startMS, endMS)
	if err != nil {
		return err
	}
	// Search for holes, update khole
	// 搜索空洞，更新khole
	err = q.updateKHoles(sid, item.TimeFrame, startMS, endMS, isCont)
	if err != nil {
		return err
	}
	return nil
}

func NewKlineAgg(TimeFrame, Table, AggFrom, AggStart, AggEnd, AggEvery, CpsBefore, Retention string) *KlineAgg {
	msecs := int64(utils2.TFToSecs(TimeFrame) * 1000)
	return &KlineAgg{TimeFrame, msecs, Table, AggFrom, AggStart, AggEnd, AggEvery, CpsBefore, Retention}
}

func (q *Queries) GetKlineNum(sid int32, timeFrame string, start, end int64) int {
	sql := fmt.Sprintf("select count(0) from kline_%s where sid=%v and time>=%v and time<%v",
		timeFrame, sid, start, end)
	row := q.db.QueryRow(context.Background(), sql)
	var num int
	_ = row.Scan(&num)
	return num
}

/*
GetDownTF
Retrieve the download time period corresponding to the specified period.
Only 1m and 1h allow downloading and writing to the super table. All other dimensions are aggregated from these two dimensions.

	获取指定周期对应的下载的时间周期。
	只有1m和1h允许下载并写入超表。其他维度都是由这两个维度聚合得到。
*/
func GetDownTF(timeFrame string) (string, *errs.Error) {
	secs := utils2.TFToSecs(timeFrame)
	if secs >= utils2.SecsDay {
		if secs%utils2.SecsDay > 0 {
			return "", errs.NewMsg(core.ErrInvalidTF, "invalid tf: %s", timeFrame)
		}
		return "1d", nil
	} else if secs >= utils2.SecsHour {
		if secs%utils2.SecsHour > 0 {
			return "", errs.NewMsg(core.ErrInvalidTF, "invalid tf: %s", timeFrame)
		}
		return "1h", nil
	} else if secs >= utils2.SecsMin*15 {
		if secs%(utils2.SecsMin*15) > 0 {
			return "", errs.NewMsg(core.ErrInvalidTF, "invalid tf: %s", timeFrame)
		}
		return "15m", nil
	} else if secs < utils2.SecsMin || secs%utils2.SecsMin > 0 {
		return "", errs.NewMsg(core.ErrInvalidTF, "invalid tf: %s", timeFrame)
	}
	return "1m", nil
}

func (q *Queries) DelKInfo(sid int32, timeFrame string) *errs.Error {
	sql := fmt.Sprintf("delete from kinfo where sid=%v and timeframe=$1", sid)
	return q.Exec(sql, timeFrame)
}

func (q *Queries) DelKLines(sid int32, timeFrame string, startMS, endMS int64) *errs.Error {
	sql := fmt.Sprintf("delete from kline_%s where sid=%v", timeFrame, sid)
	if startMS > 0 {
		sql += fmt.Sprintf(" and time >= %v", startMS)
	}
	if endMS > 0 {
		sql += fmt.Sprintf(" and time < %v", endMS)
	}
	return q.Exec(sql)
}

func (q *Queries) GetKlineRange(sid int32, timeFrame string) (int64, int64) {
	sql := fmt.Sprintf("select start,stop from kinfo where sid=%v and timeframe=$1 limit 1", sid)
	row := q.db.QueryRow(context.Background(), sql, timeFrame)
	var start, stop int64
	_ = row.Scan(&start, &stop)
	return start, stop
}

func (q *Queries) GetKlineRanges(sidList []int32, timeFrame string) map[int32][2]int64 {
	var texts = make([]string, len(sidList))
	for i, sid := range sidList {
		texts[i] = fmt.Sprintf("%v", sid)
	}
	sidText := strings.Join(texts, ", ")
	sql := fmt.Sprintf("select sid,start,stop from kinfo where timeframe=$1 and sid in (%v)", sidText)
	rows, err_ := q.db.Query(context.Background(), sql, timeFrame)
	if err_ != nil {
		return map[int32][2]int64{}
	}
	res := make(map[int32][2]int64)
	defer rows.Close()
	for rows.Next() {
		var start, stop int64
		var sid int32
		err_ = rows.Scan(&sid, &start, &stop)
		if err_ != nil {
			continue
		}
		res[sid] = [2]int64{start, stop}
	}
	return res
}

func (q *Queries) DelFactors(sid int32, startMS, endMS int64) *errs.Error {
	sql := fmt.Sprintf("delete from adj_factors where sid=%v or sub_id=%v", sid, sid)
	if startMS > 0 {
		sql += fmt.Sprintf(" and start_ms >= %v", startMS)
	}
	if endMS > 0 {
		sql += fmt.Sprintf(" and start_ms < %v", endMS)
	}
	return q.Exec(sql)
}

func (q *Queries) DelKLineUn(sid int32, timeFrame string) *errs.Error {
	sql := fmt.Sprintf("delete from kline_un where sid=%v and timeframe=$1", sid)
	return q.Exec(sql, timeFrame)
}

func (q *Queries) DelKHoles(sid int32, timeFrame string, startMS, endMS int64) *errs.Error {
	sql := fmt.Sprintf("delete from khole where sid=%v and timeframe=$1", sid)
	if startMS > 0 {
		sql += fmt.Sprintf(" and start >= %v", startMS)
	}
	if endMS > 0 {
		sql += fmt.Sprintf(" and stop <= %v", endMS)
	}
	return q.Exec(sql, timeFrame)
}

func (q *Queries) DelKHoleIDs(ids ...int64) *errs.Error {
	if len(ids) == 0 {
		return nil
	}
	var builder strings.Builder
	builder.WriteString("delete from khole where id in (")
	arr := make([]string, len(ids))
	for i, id := range ids {
		arr[i] = strconv.Itoa(int(id))
	}
	builder.WriteString(strings.Join(arr, ","))
	builder.WriteString(")")
	return q.Exec(builder.String())
}

func mapToItems[T any](rows pgx.Rows, err_ error, assign func() (T, []any)) ([]T, error) {
	if err_ != nil {
		return nil, err_
	}
	defer rows.Close()
	items := make([]T, 0)
	for rows.Next() {
		i, fields := assign()
		if err := rows.Scan(fields...); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

/*
FixKInfoZeros
修复kinfo表中start=0或stop=0的记录。通过查询实际K线数据范围来更新正确的start和stop值。
*/
func (q *Queries) FixKInfoZeros() *errs.Error {
	// 查询所有stop=0或start=0的记录
	sql := `SELECT sid, timeframe FROM kinfo WHERE stop = 0 or start = 0`
	rows, err_ := q.db.Query(context.Background(), sql)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()

	// 按TimeFrame分组记录sid
	tfGroups := make(map[string]map[int32]bool)
	for rows.Next() {
		var sid int32
		var timeframe string
		if err_ := rows.Scan(&sid, &timeframe); err_ != nil {
			return NewDbErr(core.ErrDbReadFail, err_)
		}
		if tfGroups[timeframe] == nil {
			tfGroups[timeframe] = make(map[int32]bool)
		}
		tfGroups[timeframe][sid] = true
	}
	if err_ := rows.Err(); err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(tfGroups) == 0 {
		return nil
	}

	// 记录处理结果
	var totalFixed int

	// 对每个TimeFrame分组进行处理
	for tf, sids := range tfGroups {
		log.Info("fixing kinfo zeros",
			zap.String("timeframe", tf),
			zap.Int("count", len(sids)))

		// 计算实际的K线范围
		ranges, err := q.CalcKLineRanges(tf, sids)
		if err != nil {
			return err
		}

		// 更新每个sid的范围
		for sid, r := range ranges {
			start, stop := r[0], r[1]
			if stop > 0 && start > 0 {
				err := q.updateKLineRange(sid, tf, start, stop)
				if err != nil {
					return err
				}
				totalFixed++
			}
		}
	}

	log.Info("fixed kinfo complete", zap.Int("total", totalFixed), zap.Int("timeframes", len(tfGroups)))
	return nil
}

/*
SyncKlineTFs
Check the data consistency of each kline table. If there is more low dimensional data than high dimensional data, aggregate and update to high dimensional data
检查各kline表的数据一致性，如果低维度数据比高维度多，则聚合更新到高维度
*/
func SyncKlineTFs(args *config.CmdArgs, pb *utils.StagedPrg) *errs.Error {
	log.Info("run kline data sync ...")
	pairs := make(map[string]bool)
	for _, p := range args.Pairs {
		pairs[p] = true
	}
	if len(pairs) == 0 && !args.Force {
		fmt.Println("KlineCorrect for all symbols would take a long time, input `y` to confirm (y/n):")
		reader := bufio.NewReader(os.Stdin)
		input, err_ := reader.ReadString('\n')
		if err_ != nil {
			return errs.New(errs.CodeRunTime, err_)
		}
		input = strings.TrimSpace(strings.ToLower(input))
		if input != "y" {
			return nil
		}
	}
	sess, conn, err := Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()
	err = sess.FixKInfoZeros()
	if err != nil {
		return err
	}
	if pb != nil {
		pb.SetProgress("fixKInfoZeros", 1)
	}
	// load all markets
	exsList := GetAllExSymbols()
	cache := map[string]map[string]bool{}
	sidMap := make(map[int32]string)
	for _, exs := range exsList {
		if len(pairs) > 0 {
			if _, ok := pairs[exs.Symbol]; !ok {
				continue
			}
			sidMap[exs.ID] = exs.InfoBy()
		}
		cc, _ := cache[exs.Exchange]
		if cc == nil {
			cc = make(map[string]bool)
			cache[exs.Exchange] = cc
		}
		if _, ok := cc[exs.Market]; !ok {
			exchange, err := exg.GetWith(exs.Exchange, exs.Market, "")
			if err != nil {
				return err
			}
			_, err = LoadMarkets(exchange, false)
			if err != nil {
				return err
			}
			cc[exs.Market] = true
		}
	}
	err = syncKlineInfos(sess, sidMap, func(done int, total int) {
		if pb != nil {
			pb.SetProgress("syncTfKinfo", float64(done)/float64(total))
		}
	})
	if err != nil {
		return err
	}
	log.Info("try filling KLine Holes ...")
	return tryFillHoles(sess, sidMap, func(done int, total int) {
		if pb != nil {
			pb.SetProgress("fillKHole", float64(done)/float64(total))
		}
	})
}

type KHoleExt struct {
	*KHole
	TfMSecs int64
}

func tryFillHoles(sess *Queries, sids map[int32]string, prg utils.PrgCB) *errs.Error {
	ctx := context.Background()
	sidList := utils2.KeysOfMap(sids)
	holes, err_ := sess.ListKHoles(ctx, sidList)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	rows := make([]*KHoleExt, len(holes))
	for i, h := range holes {
		rows[i] = &KHoleExt{
			KHole:   h,
			TfMSecs: int64(utils2.TFToSecs(h.Timeframe) * 1000),
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Sid != b.Sid {
			return a.Sid < b.Sid
		}
		if a.Start != b.Start {
			return a.Start < b.Start
		}
		return a.TfMSecs < b.TfMSecs
	})
	pBar := utils.NewPrgBar(len(rows), "FillHoles")
	if prg != nil {
		pBar.PrgCbs = append(pBar.PrgCbs, prg)
	}
	defer pBar.Close()
	// The kholes that need to be deleted have been filled in
	// 已填充需要删除的khole
	badIds := make([]int64, 0, len(rows)/10)
	curSid := int32(0)
	var exs *ExSymbol
	var editNum int
	var newHoles [][4]int64 // 需要新增的记录[]sid,tfMSecs,start,stop
	for _, row := range rows {
		pBar.Add(1)
		if row.Sid != curSid {
			curSid = row.Sid
			exs = GetSymbolByID(curSid)
		}
		if exs == nil {
			log.Warn("symbol id invalid", zap.Int32("sid", curSid))
			continue
		}
		exchange, err := exg.GetWith(exs.Exchange, exs.Market, "")
		if err != nil {
			return err
		}
		if !exchange.HasApi(banexg.ApiFetchOHLCV, exs.Market) {
			// Not supporting downloading K-line, skip
			// 不支持下载K线，跳过
			continue
		}
		start, stop := row.Start, row.Stop
		//There was originally a logic here to skip the large cycle KHole if the small cycle check has been filled, but it was cancelled because larger cycles cannot be collected from extra small cycles.
		//Each cycle should independently retrieve the actual K-line range to ensure that the range is correct
		// 这里本来有从小周期检查已填充则跳过大周期KHole的逻辑，但因较大周期无法从特小周期归集，故这里取消。
		// 每个周期应独立检索实际K线范围，确保范围正确
		updateKHole := func(newStart, newStop int64) bool {
			if newStart == 0 || newStop == 0 {
				return false
			}
			if newStart == start && newStop == stop {
				// This interval is completely filled and added to the deletion list
				// 此区间被完全填充，添加到删除列表
				badIds = append(badIds, row.ID)
				return true
			}
			if newStart == start {
				start = newStop
			} else if newStop == stop {
				stop = newStart
			} else {
				// Include, delete current, add two KHoles before and after
				// 被包含，删除当前，新增前后两个KHole
				badIds = append(badIds, row.ID)
				newHoles = append(newHoles, [4]int64{int64(row.Sid), row.TfMSecs, start, newStart},
					[4]int64{int64(row.Sid), row.TfMSecs, newStop, stop})
				return true
			}
			return false
		}
		// First check if it already exists
		// 先检查是否已存在
		oldStart, oldStop, err := sess.CalcKLineRange(exs.ID, row.Timeframe, start, stop)
		if err != nil {
			return err
		}
		if updateKHole(oldStart, oldStop) {
			continue
		}
		// Download K-lines and also collect higher cycle K-lines
		// 下载K线，同时也会归集更高周期K线
		saveNum, err := downOHLCV2DBRange(sess, exchange, exs, row.Timeframe, start, stop, 0, 0, 2, nil)
		if err != nil {
			if err.Code == errs.CodeNoMarketForPair {
				log.Info("skip down no market symbol", zap.Int32("sid", exs.ID),
					zap.String("symbol", exs.Symbol))
			} else {
				log.Warn("down ohlcv to fill fail", zap.Int32("sid", exs.ID),
					zap.String("symbol", exs.Symbol), zap.Error(err))
			}
			continue
		}
		// Query the actual updated interval
		// 查询实际更新的区间
		if saveNum == 0 {
			continue
		}
		resStart, resStop, err := sess.CalcKLineRange(exs.ID, row.Timeframe, start, stop)
		if err != nil {
			return err
		}
		if updateKHole(resStart, resStop) {
			continue
		}
		if start != row.Start || stop != row.Stop {
			// 此区间被更新
			editNum += 1
			err_ = sess.SetKHole(ctx, SetKHoleParams{ID: row.ID, Start: start, Stop: stop})
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
		}
	}
	// Delete filled IDs
	// 删除已填充的id
	if len(badIds) > 0 {
		err := sess.DelKHoleIDs(badIds...)
		if err != nil {
			return err
		}
	}
	// 新增kHoles
	if len(newHoles) > 0 {
		var items = make([]AddKHolesParams, len(newHoles))
		for i, h := range newHoles {
			items[i] = AddKHolesParams{
				Sid:       int32(h[0]),
				Timeframe: utils2.SecsToTF(int(h[1] / 1000)),
				Start:     h[2],
				Stop:      h[3],
				NoData:    true,
			}
		}
		_, err_ = sess.AddKHoles(ctx, items)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	log.Info(fmt.Sprintf("find kHoles %v, filled: %v, add: %v, edit: %v", len(holes),
		len(badIds), len(newHoles), editNum))
	return nil
}

type KInfoExt struct {
	KInfo
	TfMSecs int64
}

func syncKlineInfos(sess *Queries, sids map[int32]string, prg utils.PrgCB) *errs.Error {
	infos, err_ := sess.ListKInfos(context.Background())
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	if len(sids) > 0 {
		infoRes := make([]*KInfo, 0, len(sids)*6)
		for _, info := range infos {
			if _, ok := sids[info.Sid]; ok {
				infoRes = append(infoRes, info)
			}
		}
		infos = infoRes
	}
	// 显示进度条
	pgTotal := len(infos) + len(aggList)*10
	pBar := utils.NewPrgBar(pgTotal, "sync tf")
	if prg != nil {
		pBar.PrgCbs = append(pBar.PrgCbs, prg)
	}
	defer pBar.Close()
	// 加载计算的区间
	sidMap := make(map[int32]bool)
	for k := range sids {
		sidMap[k] = true
	}
	calcs := make(map[string]map[int32][2]int64)
	for _, agg := range aggList {
		ranges, err := sess.CalcKLineRanges(agg.TimeFrame, sidMap)
		if err != nil {
			return err
		}
		calcs[agg.TimeFrame] = ranges
		pBar.Add(10)
	}
	infoList := make([]*KInfoExt, 0, len(infos))
	for _, info := range infos {
		infoList = append(infoList, &KInfoExt{
			KInfo:   *info,
			TfMSecs: int64(utils2.TFToSecs(info.Timeframe) * 1000),
		})
	}
	slices.SortFunc(infoList, func(a, b *KInfoExt) int {
		return int(a.Sid - b.Sid)
	})
	var groups []map[string]*KInfoExt
	var curSid int32
	tfMap := make(map[string]*KInfoExt)
	for _, info := range infoList {
		if info.Sid != curSid {
			if len(tfMap) > 0 {
				groups = append(groups, tfMap)
			}
			tfMap = make(map[string]*KInfoExt)
			curSid = info.Sid
		}
		tfMap[info.Timeframe] = info
	}
	if len(tfMap) > 0 {
		groups = append(groups, tfMap)
	}
	return utils.ParallelRun(groups, 20, func(i int, m map[string]*KInfoExt) *errs.Error {
		sid := int32(0)
		for _, info := range m {
			sid = info.Sid
			break
		}
		sess2, conn, err := Conn(nil)
		if err != nil {
			return err
		}
		defer conn.Release()
		err = sess2.syncKlineSid(sid, sids[sid], m, calcs)
		pBar.Add(len(m))
		return err
	})
}

func (q *Queries) syncKlineSid(sid int32, infoBy string, tfMap map[string]*KInfoExt, calcs map[string]map[int32][2]int64) *errs.Error {
	var err *errs.Error
	var err_ error
	tfRanges := make(map[string][2]int64)
	ctx := context.Background()
	for _, agg := range aggList {
		var oldStart, oldEnd int64
		if info, ok := tfMap[agg.TimeFrame]; ok {
			oldStart, oldEnd = info.Start, info.Stop
		}
		if ranges, ok := calcs[agg.TimeFrame][sid]; ok {
			tfRanges[agg.TimeFrame] = ranges
			newStart, newEnd := ranges[0], ranges[1]
			err_ = nil
			if oldStart == 0 && oldEnd == 0 {
				_, err_ = q.AddKInfo(ctx, AddKInfoParams{
					Sid:       sid,
					Timeframe: agg.TimeFrame,
					Start:     newStart,
					Stop:      newEnd,
				})
			} else if newStart != oldStart || oldEnd != newEnd {
				err_ = q.SetKInfo(ctx, SetKInfoParams{
					Sid:       sid,
					Timeframe: agg.TimeFrame,
					Start:     newStart,
					Stop:      newEnd,
				})
			}
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
			// Update KHoles to avoid holes that are not recorded
			// 更新KHoles，避免有空洞但未记录
			err = q.updateKHoles(sid, agg.TimeFrame, newStart, newEnd, false)
			if err != nil {
				return err
			}
		} else if oldStart > 0 {
			// No data, but there are range records
			// 没有数据，但有范围记录
			err = q.DelKInfo(sid, agg.TimeFrame)
			if err != nil {
				return err
			}
		}
	}
	// Attempt to aggregate updates from subintervals
	// 尝试从子区间聚合更新
	for _, agg := range aggList[1:] {
		if agg.AggFrom == "" {
			continue
		}
		subRange, ok := tfRanges[agg.AggFrom]
		if !ok {
			continue
		}
		subStart, subEnd := subRange[0], subRange[1]
		var curStart, curEnd int64
		if curRange, ok := tfRanges[agg.TimeFrame]; ok {
			curStart, curEnd = curRange[0], curRange[1]
		}
		tfMSecs := int64(utils2.TFToSecs(agg.TimeFrame) * 1000)
		subAlignStart := utils2.AlignTfMSecs(subStart, tfMSecs)
		subAlignEnd := utils2.AlignTfMSecs(subEnd, tfMSecs)
		if subAlignStart < curStart {
			err = q.refreshAgg(agg, sid, subStart, min(subEnd, curStart), "", infoBy, false)
			if err != nil {
				return err
			}
		}
		if subAlignEnd > curEnd {
			err = q.refreshAgg(agg, sid, max(curEnd, subStart), subEnd, "", infoBy, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

/*
UpdatePendingIns
Update unfinished insertion tasks and call them when the robot starts,
更新未完成的插入任务，在机器人启动时调用，
*/
func (q *Queries) UpdatePendingIns() *errs.Error {
	if utils.HasBanConn() {
		lockVal, err := utils.GetNetLock("UpdatePendingIns", 10)
		if err != nil {
			return err
		}
		defer utils.DelNetLock("UpdatePendingIns", lockVal)
	}
	ctx := context.Background()
	items, err_ := q.GetAllInsKlines(ctx)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(items) == 0 {
		return nil
	}
	log.Info("Updating pending insert jobs", zap.Int("num", len(items)))
	for _, i := range items {
		if i.StartMs > 0 && i.StopMs > 0 {
			start, end, err := q.CalcKLineRange(i.Sid, i.Timeframe, i.StartMs, i.StopMs)
			if err != nil {
				return err
			}
			if start > 0 && end > 0 {
				exs := GetSymbolByID(i.Sid)
				err = q.UpdateKRange(exs, i.Timeframe, start, end, nil, true)
				if err != nil {
					return err
				}
			}
		}
		err_ = q.DelInsKline(ctx, i.ID)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	return nil
}

func (q *Queries) AddInsJob(add AddInsKlineParams) (int32, *errs.Error) {
	ctx := context.Background()
	ins, err_ := q.GetInsKline(ctx, add.Sid)
	if err_ != nil && !errors.Is(err_, pgx.ErrNoRows) {
		return 0, NewDbErr(core.ErrDbReadFail, err_)
	}
	if ins != nil && ins.ID > 0 {
		log.Warn("insert candles for symbol locked, skip", zap.Int32("sid", add.Sid), zap.String("tf", add.Timeframe))
		return 0, nil
	}
	tx, sess, err := q.NewTx(ctx)
	if err != nil {
		return 0, err
	}
	newId, err_ := sess.AddInsKline(ctx, add)
	if err_ != nil {
		_ = tx.Close(ctx, false)
		return 0, NewDbErr(core.ErrDbExecFail, err_)
	}
	err = tx.Close(ctx, true)
	if err != nil {
		return 0, err
	}
	return newId, nil
}

func GetKlineAggs() []*KlineAgg {
	return aggList
}

/*
CalcAdjFactors
Calculate and update all weighting factors
计算更新所有复权因子
*/
func CalcAdjFactors(args *config.CmdArgs) *errs.Error {
	if args.OutPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--out is required")
	}
	exInfo := exg.Default.Info()
	if exInfo.ID == "china" {
		return calcChinaAdjFactors(args)
	} else {
		return errs.NewMsg(errs.CodeParamInvalid, "exchange %s dont support adjust factors", exInfo.ID)
	}
}

func calcChinaAdjFactors(args *config.CmdArgs) *errs.Error {
	exchange := exg.Default
	_, err := LoadMarkets(exchange, false)
	if err != nil {
		return err
	}
	err = InitListDates()
	if err != nil {
		return err
	}
	sess, conn, err := Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()
	err = calcCnFutureFactors(sess, args)
	if err != nil {
		return err
	}
	// 对于股票计算复权因子?
	log.Info("calc china adj_factors complete")
	return nil
}

func calcCnFutureFactors(sess *Queries, args *config.CmdArgs) *errs.Error {
	err_ := utils.EnsureDir(args.OutPath, 0755)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	items := GetExSymbols("china", banexg.MarketLinear)
	exsList := utils.ValsOfMap(items)
	sort.Slice(exsList, func(i, j int) bool {
		return exsList[i].Symbol < exsList[j].Symbol
	})
	var allows = make(map[string]bool)
	for _, key := range args.Pairs {
		parts := utils2.SplitParts(key)
		allows[parts[0].Val] = true
	}
	var err *errs.Error
	// Save the daily trading volume of each contract for the current variety, used to find the main contract
	// 保存当前品种日线各个合约的成交量，用于寻找主力合约
	dateSidVols := make(map[int64]map[int32]*banexg.Kline)
	lastCode := ""
	var lastExs *ExSymbol
	// For all futures targets, obtain daily K in order and record it by time
	// 对所有期货标的，按顺序获取日K，并按时间记录
	var pBar = utils.NewPrgBar(len(exsList), "future")
	defer pBar.Close()
	dayMSecs := int64(utils2.TFToSecs("1d") * 1000)
	for _, exs := range exsList {
		pBar.Add(1)
		parts := utils2.SplitParts(exs.Symbol)
		if len(parts) > 1 && parts[1].Type == utils2.StrInt {
			p1Str := parts[1].Val
			p1num, _ := strconv.Atoi(p1Str[len(p1Str)-2:])
			if p1num == 0 || p1num > 12 {
				// 跳过000, 888, 999这些特殊后缀
				continue
			}
		}
		if _, ok := allows[parts[0].Val]; len(allows) > 0 && !ok {
			// 跳过未选中的
			continue
		}
		if lastCode != parts[0].Val {
			err = saveAdjFactors(dateSidVols, lastCode, lastExs, sess, args.OutPath)
			if err != nil {
				return err
			}
			dateSidVols = make(map[int64]map[int32]*banexg.Kline)
			lastCode = parts[0].Val
			lastExs = exs
		}
		klines, err := sess.QueryOHLCV(exs, "1d", 0, 0, 0, false)
		if err != nil {
			return err
		}
		for _, k := range klines {
			barTime := utils2.AlignTfMSecs(k.Time, dayMSecs)
			vols, _ := dateSidVols[barTime]
			if vols == nil {
				vols = make(map[int32]*banexg.Kline)
				dateSidVols[barTime] = vols
			}
			vols[exs.ID] = k
		}
	}
	return saveAdjFactors(dateSidVols, lastCode, lastExs, sess, args.OutPath)
}

func saveAdjFactors(data map[int64]map[int32]*banexg.Kline, pCode string, pExs *ExSymbol, sess *Queries, outDir string) *errs.Error {
	if pCode == "" {
		return nil
	}
	exs := &ExSymbol{
		Exchange: pExs.Exchange,
		Market:   pExs.Market,
		ExgReal:  pExs.ExgReal,
		Symbol:   pCode + "888", // 期货888结尾表示主力连续合约
		Combined: true,
	}
	err := EnsureSymbols([]*ExSymbol{exs})
	if err != nil {
		return err
	}
	// Delete the old main continuous contract compounding factor
	// 删除旧的主力连续合约复权因子
	ctx := context.Background()
	err_ := sess.DelAdjFactors(ctx, exs.ID)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	dates := utils.KeysOfMap(data)
	sort.Slice(dates, func(i, j int) bool {
		return dates[i] < dates[j]
	})
	// Daily search for the contract ID with the highest trading volume and calculate the compounding factor
	// 逐日寻找成交量最大的合约ID，并计算复权因子
	var adds []AddAdjFactorsParams
	var row *AddAdjFactorsParams
	// Choose the one with the largest position on the first day of listing
	// 上市首日选持仓量最大的
	vols, _ := data[dates[0]]
	vol, hold := findMaxVols(vols)
	adds = append(adds, AddAdjFactorsParams{
		Sid:     exs.ID,
		StartMs: dates[0],
		SubID:   hold.Sid,
		Factor:  1,
	})
	lastSid := hold.Sid
	var lines []string
	dateFmt := "2006-01-02"
	lines = writeAdjChg(lastSid, lastSid, 0, 5, data, dates, lines)
	for i, dateMS := range dates[1:] {
		if row != nil {
			row.StartMs = dateMS
			adds = append(adds, *row)
			row = nil
		}
		vols, _ = data[dateMS]
		vol, hold = findMaxVols(vols)
		// When the trading volume and position of the main force are not at their maximum, it is necessary to give up the main force
		// 当主力的成交量和持仓量都不为最大，需让出主力
		if vol.Sid != lastSid && hold.Sid != lastSid && len(vols) > 1 {
			tgt := hold
			if exs.ExgReal == "CFFEX" {
				tgt = vol
			}
			lines = writeAdjChg(lastSid, tgt.Sid, i+1, 5, data, dates, lines)
			lastK, _ := vols[lastSid]
			var factor float64
			if lastK != nil {
				factor = tgt.Price / lastK.Close
			} else {
				date := btime.ToDateStr(dateMS, dateFmt)
				it := GetSymbolByID(lastSid)
				log.Warn("last interrupted", zap.String("code", it.Symbol),
					zap.Int32("sid", lastSid), zap.String("date", date))
				factor = findPrevFactor(data, dates[1:], i, tgt.Sid, lastSid)
			}
			row = &AddAdjFactorsParams{
				Sid:    exs.ID,
				SubID:  tgt.Sid,
				Factor: factor,
			}
			lastSid = tgt.Sid
		}
	}
	outPath := filepath.Join(outDir, exs.Symbol+"_adjs.txt")
	_ = utils2.WriteFile(outPath, []byte(strings.Join(lines, "\n")))
	_, err_ = sess.AddAdjFactors(ctx, adds)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

func writeAdjChg(sid1, sid2 int32, hit, width int, data map[int64]map[int32]*banexg.Kline, dates []int64, lines []string) []string {
	symbol1 := GetSymbolByID(sid1).Symbol
	symbol2 := GetSymbolByID(sid2).Symbol
	dateFmt := "2006-01-02"
	lines = append(lines, symbol1+"  "+symbol2)
	start := max(hit-width, 0)
	stop := min(hit+width, len(dates))
	for start < stop {
		dateMs := dates[start]
		dateStr := btime.ToDateStr(dateMs, dateFmt)
		k1 := data[dateMs][sid1]
		k2 := data[dateMs][sid2]
		if k1 != nil || k2 != nil {
			var p1, v1, i1, p2, v2, i2 float64
			if k1 != nil {
				p1, v1, i1 = k1.Close, k1.Volume, k1.Info
			}
			if k2 != nil {
				p2, v2, i2 = k2.Close, k2.Volume, k2.Info
			}
			text := fmt.Sprintf("%v/%v\t%v/%v\t%v/%v", p1, p2, v1, v2, i1, i2)
			line := dateStr + "   " + text
			if start == hit {
				line += " *"
			}
			lines = append(lines, line)
		}
		start += 1
	}
	lines = append(lines, "")
	return lines
}

type PriceVol struct {
	Sid   int32
	Price float64
	Vol   float64
}

/*
Find the item with the highest trading volume and position
查找成交量和持仓量最大的项
*/
func findMaxVols(vols map[int32]*banexg.Kline) (*PriceVol, *PriceVol) {
	var vol, hold PriceVol
	for sid, k := range vols {
		if vol.Sid == 0 {
			vol.Sid = sid
			vol.Price = k.Close
			vol.Vol = k.Volume
			hold.Sid = sid
			hold.Price = k.Close
			hold.Vol = k.Info
		} else if k.Volume > vol.Vol {
			vol.Sid = sid
			vol.Price = k.Close
			vol.Vol = k.Volume
		}
		if k.Info > hold.Vol {
			hold.Sid = sid
			hold.Price = k.Close
			hold.Vol = k.Info
		}
	}
	return &vol, &hold
}

func findPrevFactor(data map[int64]map[int32]*banexg.Kline, dates []int64, i int, tgt, old int32) float64 {
	for i > 0 {
		i--
		vols := data[dates[i]]
		tgtK, _ := vols[tgt]
		oldK, _ := vols[old]
		if tgtK == nil || oldK == nil {
			continue
		}
		return tgtK.Close / oldK.Close
	}
	return 1
}
