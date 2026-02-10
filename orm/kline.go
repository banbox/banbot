package orm

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sasha-s/go-deadlock"

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
	"go.uber.org/zap"
)

var (
	aggList = []*KlineAgg{
		// Update dependent tables by ourselves when inserting (QuestDB).
		NewKlineAgg("1m", "kline_1m", "", "", "", "", "2 months", "12 months"),
		NewKlineAgg("5m", "kline_5m", "1m", "20m", "1m", "1m", "2 months", "12 months"),
		NewKlineAgg("15m", "kline_15m", "5m", "1h", "5m", "5m", "3 months", "16 months"),
		NewKlineAgg("1h", "kline_1h", "", "", "", "", "6 months", "3 years"),
		NewKlineAgg("1d", "kline_1d", "1h", "3d", "1h", "1h", "3 years", "20 years"),
	}
	aggMap          = make(map[string]*KlineAgg)
)

func init() {
	for _, agg := range aggList {
		aggMap[agg.TimeFrame] = agg
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
select cast(ts as long)/1000,open,high,low,close,volume,quote,buy_volume,trade_num from $tbl
where sid=%d and ts < cast(%v as timestamp)
order by ts desc`, exs.ID, finishEndMS*1000)
	} else {
		if limit == 0 {
			limit = int((finishEndMS-startMs)/tfMSecs) + 1
		}
		dctSql = fmt.Sprintf(`
select cast(ts as long)/1000,open,high,low,close,volume,quote,buy_volume,trade_num from $tbl
where sid=%d and ts >= cast(%v as timestamp) and ts < cast(%v as timestamp)
order by ts`, exs.ID, startMs*1000, finishEndMS*1000)
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
select cast(ts as long)/1000,open,high,low,close,volume,quote,buy_volume,trade_num,sid from $tbl
where ts >= cast(%v as timestamp) and ts < cast(%v as timestamp) and sid in (%v)
order by sid,ts`, startMs*1000, finishEndMS*1000, sidText)
	subTF, rows, err_ := queryHyper(q, timeframe, dctSql, 0)
	arrs, err_ := mapToItems(rows, err_, func() (*KlineSid, []any) {
		var i KlineSid
		return &i, []any{&i.Time, &i.Open, &i.High, &i.Low, &i.Close, &i.Volume, &i.Quote, &i.BuyVolume, &i.TradeNum, &i.Sid}
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
			Close: k.Close, Volume: k.Volume, Quote: k.Quote, BuyVolume: k.BuyVolume, TradeNum: k.TradeNum})
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
select cast(ts as long)/1000 from %s
where sid=%d and ts >= cast(%v as timestamp) and ts < cast(%v as timestamp)
order by ts`, tblName, sid, startMs*1000, endMs*1000)
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

func (q *Queries) getKLineTimeRange(sid int32, timeframe string) (int64, int64, *errs.Error) {
	tblName := "kline_" + timeframe
	sql := fmt.Sprintf("select min(cast(ts as long)/1000), max(cast(ts as long)/1000) from %s where sid=%d", tblName, sid)
	row := q.db.QueryRow(context.Background(), sql)
	var minTime, maxTime *int64
	if err := row.Scan(&minTime, &maxTime); err != nil {
		return 0, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	if minTime == nil || maxTime == nil {
		return 0, 0, nil
	}
	return *minTime, *maxTime, nil
}

func (q *Queries) updateKHoles(sid int32, timeFrame string, startMS, endMS int64, isCont bool) *errs.Error {
	_ = isCont
	if startMS <= 0 || endMS <= startMS {
		return nil
	}
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	barTimes, err := q.getKLineTimes(sid, timeFrame, startMS, endMS)
	if err != nil {
		return err
	}
	holes := make([]MSRange, 0)
	if len(barTimes) == 0 {
		holes = append(holes, MSRange{Start: startMS, Stop: endMS})
	} else {
		if barTimes[0] > startMS {
			holes = append(holes, MSRange{Start: startMS, Stop: barTimes[0]})
		}
		prevTime := barTimes[0]
		for _, t := range barTimes[1:] {
			intv := t - prevTime
			if intv > tfMSecs {
				holes = append(holes, MSRange{Start: prevTime + tfMSecs, Stop: t})
			} else if intv < tfMSecs {
				log.Warn("invalid timeframe or kline", zap.Int32("sid", sid), zap.String("tf", timeFrame),
					zap.Int64("intv", intv/1000), zap.Int64("tfmsecs", tfMSecs/1000), zap.Int64("time", t))
			}
			prevTime = t
		}
		maxEnd := utils2.AlignTfMSecs(btime.UTCStamp(), tfMSecs) - tfMSecs
		if maxEnd-prevTime > tfMSecs*5 && endMS-prevTime > tfMSecs {
			holes = append(holes, MSRange{Start: prevTime + tfMSecs, Stop: min(endMS, maxEnd)})
		}
	}
	if len(holes) == 0 {
		return nil
	}

	// Filter out non-trading time ranges.
	exs := GetSymbolByID(sid)
	if exs == nil {
		log.Warn("no ExSymbol found", zap.Int32("sid", sid))
		return nil
	}
	exchange, err := exg.GetWith(exs.Exchange, exs.Market, "")
	if err != nil {
		return err
	}
	susp, err := GetExSHoles(exchange, exs, startMS, endMS, true)
	if err != nil {
		return err
	}
	if len(susp) > 0 {
		filtered := make([]MSRange, 0, len(holes))
		si := 0
		for _, h := range holes {
			for si < len(susp) && susp[si][1] <= h.Start {
				si++
			}
			if si >= len(susp) {
				filtered = append(filtered, h)
				continue
			}
			cur := h
			for si < len(susp) {
				s := susp[si]
				if s[0] >= cur.Stop {
					break
				}
				if s[1] <= cur.Start {
					si++
					continue
				}
				// left part
				if s[0] > cur.Start {
					filtered = append(filtered, MSRange{Start: cur.Start, Stop: min(cur.Stop, s[0])})
				}
				// shrink to right remainder
				cur.Start = max(cur.Start, s[1])
				if cur.Start >= cur.Stop {
					break
				}
				si++
			}
			if cur.Start < cur.Stop {
				filtered = append(filtered, cur)
			}
		}
		holes = filtered
	}

	// Filter out tiny 1m holes.
	if tfMSecs == 60000 {
		exInfo := exchange.Info()
		hs := holes[:0]
		for _, h := range holes {
			num := int((h.Stop - h.Start) / tfMSecs)
			if num <= exInfo.Min1mHole {
				continue
			}
			hs = append(hs, h)
		}
		holes = hs
	}
	ctx := context.Background()
	if err := rewriteHoleRangesInWindow(ctx, sid, timeFrame, startMS, endMS, holes); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func rewriteHoleRangesInWindow(ctx context.Context, sid int32, timeFrame string, startMS, endMS int64, holes []MSRange) error {
	tbl := "kline_" + timeFrame
	if err := PubQ().UpdateSRanges(ctx, sid, tbl, timeFrame, startMS, endMS, true); err != nil {
		return err
	}
	for _, h := range holes {
		if h.Stop <= h.Start {
			continue
		}
		if err := PubQ().UpdateSRanges(ctx, sid, tbl, timeFrame, h.Start, h.Stop, false); err != nil {
			return err
		}
	}
	return nil
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
	items, err := mapToItems(rows, err_, func() (*banexg.Kline, []any) {
		var i banexg.Kline
		return &i, []any{&i.Time, &i.Open, &i.High, &i.Low, &i.Close, &i.Volume, &i.Quote, &i.BuyVolume, &i.TradeNum}
	})
	if err != nil {
		return nil, err
	}
	out := make([]*banexg.Kline, 0, len(items))
	for _, it := range items {
		out = append(out, it)
	}
	return out, nil
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
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	barEndMS := endMS
	if barEndMS <= startMS && tfMSecs > 0 {
		barEndMS = startMS + tfMSecs
	}

	nowMS := btime.UTCStamp()
	if barEndMS > 0 {
		nowMS = min(nowMS, barEndMS)
	}

	// 1) Read cached unfinish (kline_un) first. Only recompute when expired.
	cached, cachedStop, cachedExpire, err := queryUnfinish(sid, timeFrame, startMS)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, 0, err
	}
	if cached != nil && cachedExpire != nil && *cachedExpire > nowMS {
		return cached, cachedStop, nil
	}
	if tfMSecs <= 60000 || mode != "query" {
		if cached != nil {
			return cached, cachedStop, nil
		}
		return nil, 0, nil
	}

	// 2) Recompute from smaller timeframes (15m -> 5m -> 1m ...), then write back with expire_ms.
	bar, stopMS, err := calcUnfinishFromSubs(sess, sid, timeFrame, startMS, barEndMS, nowMS)
	if err != nil {
		return nil, 0, err
	}
	if bar == nil {
		return nil, stopMS, nil
	}
	bar.Time = startMS

	// Best-effort cache update.
	if stopMS <= 0 {
		stopMS = nowMS
	}
	if err2 := PubQ().SetUnfinish(sid, timeFrame, stopMS, bar); err2 != nil {
		log.Warn("set unfinish fail", zap.Int32("sid", sid), zap.String("tf", timeFrame), zap.String("err", err2.Short()))
	}
	return bar, stopMS, nil
}

func queryUnfinish(sid int32, timeFrame string, barStartMS int64) (*banexg.Kline, int64, *int64, error) {
	ctx := context.Background()
	db, err := BanPubConn(false)
	if err != nil {
		return nil, 0, nil, err
	}
	defer db.Close()
	row := db.QueryRowContext(ctx, `
select start_ms,open,high,low,close,volume,quote,buy_volume,trade_num,stop_ms,expire_ms
from kline_un
where sid=? and timeframe=? and start_ms >= ?
limit 1`,
		sid, timeFrame, barStartMS,
	)
	var (
		startMs                    int64
		open, high, low            float64
		closeP, vol, quote, buyVol float64
		tradeNum                   int64
		stopMs                     int64
		expireMsVal                int64
	)
	if err := row.Scan(&startMs, &open, &high, &low, &closeP, &vol, &quote, &buyVol, &tradeNum, &stopMs, &expireMsVal); err != nil {
		return nil, 0, nil, err
	}
	bar := &banexg.Kline{
		Time:      startMs,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     closeP,
		Volume:    vol,
		Quote:     quote,
		BuyVolume: buyVol,
		TradeNum:  tradeNum,
	}
	return bar, stopMs, &expireMsVal, nil
}

func unfinishChain(timeFrame string) []string {
	if utils2.TFToSecs(timeFrame) <= utils2.SecsMin {
		return nil
	}
	out := make([]string, 0, 4)
	cur := timeFrame
	for {
		sub, _, _ := getSubTf(cur)
		if sub == "" {
			return out
		}
		out = append(out, sub)
		if sub == "1m" {
			return out
		}
		cur = sub
	}
}

func queryKlinesRange(sess *Queries, sid int32, timeFrame string, startMS, endMS int64) ([]*banexg.Kline, error) {
	if startMS <= 0 || endMS <= startMS {
		return nil, nil
	}
	sql := fmt.Sprintf(`
select cast(ts as long)/1000,open,high,low,close,volume,quote,buy_volume,trade_num from $tbl
where sid=%d and ts >= cast(%v as timestamp) and ts < cast(%v as timestamp)
order by ts`, sid, startMS*1000, endMS*1000)
	subTF, rows, err_ := queryHyper(sess, timeFrame, sql, 0)
	klines, err := mapToKlines(rows, err_)
	if err != nil {
		return nil, err
	}
	if subTF == "" || len(klines) == 0 {
		return klines, nil
	}
	// Safety: if queryHyper fell back to a smaller table, aggregate to requested timeframe.
	toTfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	fromTfMSecs := int64(utils2.TFToSecs(subTF) * 1000)
	offMS := GetAlignOff(sid, toTfMSecs)
	infoBy := GetSymbolByID(sid).InfoBy()
	var lastFinish bool
	klines, lastFinish = utils.BuildOHLCV(klines, toTfMSecs, 0, nil, fromTfMSecs, offMS, infoBy)
	if !lastFinish && len(klines) > 0 {
		klines = klines[:len(klines)-1]
	}
	return klines, nil
}

func calcUnfinishFromSubs(sess *Queries, sid int32, timeFrame string, startMS, endMS, nowMS int64) (*banexg.Kline, int64, error) {
	chain := unfinishChain(timeFrame)
	if len(chain) == 0 {
		return nil, 0, nil
	}

	infoBy := GetSymbolByID(sid).InfoBy()
	parts := make([]*banexg.Kline, 0, 32)
	curStart := startMS
	lastToMS := int64(0)

	for _, tf := range chain {
		tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
		winEnd := utils2.AlignTfMSecs(nowMS, tfMSecs)
		if winEnd <= curStart {
			continue
		}
		klines, err := queryKlinesRange(sess, sid, tf, curStart, winEnd)
		if err != nil {
			return nil, 0, err
		}
		if len(klines) == 0 {
			continue
		}
		parts = append(parts, klines...)
		curStart = klines[len(klines)-1].Time + tfMSecs
		lastToMS = max(lastToMS, curStart)
		if curStart >= nowMS {
			break
		}
	}

	// Include the unfinished bar of the smallest timeframe (typically 1m) if present.
	smallTF := chain[len(chain)-1]
	smallMSecs := int64(utils2.TFToSecs(smallTF) * 1000)
	unStart := utils2.AlignTfMSecs(nowMS, smallMSecs)
	if unStart >= curStart && unStart < endMS {
		unbar, unTo, _, err := queryUnfinish(sid, smallTF, unStart)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, 0, err
		}
		if unbar != nil && unbar.Volume > 0 && unTo > unStart {
			parts = append(parts, unbar)
			lastToMS = max(lastToMS, unTo)
		}
	}

	if len(parts) == 0 {
		return nil, lastToMS, nil
	}

	res := &banexg.Kline{
		Time:   startMS,
		Open:   parts[0].Open,
		High:   parts[0].High,
		Low:    parts[0].Low,
		Close:  parts[len(parts)-1].Close,
		Volume: 0,
		Quote:  0,
	}
	var infoSum float64
	for _, p := range parts {
		res.High = max(res.High, p.High)
		res.Low = min(res.Low, p.Low)
		res.Volume += p.Volume
		res.Quote += p.Quote
		res.TradeNum += p.TradeNum
		infoSum += p.BuyVolume
	}
	if infoBy == "sum" {
		res.BuyVolume = infoSum
	} else {
		res.BuyVolume = parts[len(parts)-1].BuyVolume
	}
	return res, lastToMS, nil
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

func (q *PubQueries) SetUnfinish(sid int32, tf string, endMS int64, bar *banexg.Kline) *errs.Error {
	expireMS := utils2.AlignTfMSecs(btime.UTCStamp(), 60000) + 60000
	db, err := BanPubConn(true)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err_ := db.ExecContext(context.Background(), `
insert into kline_un (sid, timeframe, start_ms, stop_ms, expire_ms, open, high, low, close, volume, quote, buy_volume, trade_num)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(sid, timeframe) do update set
  start_ms=excluded.start_ms,
  stop_ms=excluded.stop_ms,
  expire_ms=excluded.expire_ms,
  open=excluded.open,
  high=excluded.high,
  low=excluded.low,
  close=excluded.close,
  volume=excluded.volume,
  quote=excluded.quote,
  buy_volume=excluded.buy_volume,
  trade_num=excluded.trade_num`,
		sid, tf, bar.Time, endMS, expireMS, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, bar.Quote, bar.BuyVolume, bar.TradeNum,
	)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
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
		r.rows[0].Quote,
		r.rows[0].BuyVolume,
		r.rows[0].TradeNum,
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
func (q *Queries) InsertKLines(timeFrame string, sid int32, arr []*banexg.Kline) (int64, *errs.Error) {
	arrLen := len(arr)
	if arrLen == 0 {
		return 0, nil
	}
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	startMS, endMS := arr[0].Time, arr[arrLen-1].Time+tfMSecs
	log.Debug("insert klines", zap.String("tf", timeFrame), zap.Int32("sid", sid),
		zap.Int("num", arrLen), zap.Int64("start", startMS), zap.Int64("end", endMS))
	tblName := "kline_" + timeFrame
	ctx := context.Background()
	const colsPerRow = 10
	const batchRows = 500
	var total int64
	for i := 0; i < arrLen; i += batchRows {
		j := min(arrLen, i+batchRows)
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
			kk := arr[k]
			args = append(args,
				sid,
				time.UnixMilli(kk.Time).UTC(),
				kk.Open,
				kk.High,
				kk.Low,
				kk.Close,
				kk.Volume,
				kk.Quote,
				kk.BuyVolume,
				kk.TradeNum,
			)
		}
		_, err := q.db.Exec(ctx, b.String(), args...)
		if err != nil {
			return total, NewDbErr(core.ErrDbExecFail, err)
		}
		total += int64(j - i)
	}
	return total, nil
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
	insId, err := AddInsJob(AddInsKlineParams{
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
		if err_ := PubQ().DelInsKline(context.Background(), insId); err_ != nil {
			log.Warn("DelInsKline fail", zap.Int64("id", insId), zap.Error(err_))
		}
	}()
	num, err := q.InsertKLines(timeFrame, exs.ID, arr)
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
func (q *Queries) UpdateKRange(exs *ExSymbol, timeFrame string, startMS, endMS int64, klines []*banexg.Kline, aggBig bool, skipHoles ...bool) *errs.Error {
	// Record data ranges in sranges (non-contiguous allowed).
	if err := updateKLineRange(exs.ID, timeFrame, startMS, endMS); err != nil {
		return err
	}
	// Search for holes and update sranges (has_data=false).
	// Skip when caller handles holes separately (e.g. downOHLCV2DBRange),
	// to avoid QuestDB WAL lag causing freshly inserted bars to appear missing.
	if len(skipHoles) == 0 || !skipHoles[0] {
		if err := q.updateKHoles(exs.ID, timeFrame, startMS, endMS, true); err != nil {
			return err
		}
	}
	if !aggBig {
		return nil
	}
	// Update a larger super table
	// 更新更大的超表
	return q.updateBigHyper(exs, timeFrame, startMS, endMS)
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
	sql := fmt.Sprintf("select sid,min(cast(ts as long)/1000),max(cast(ts as long)/1000) from %s group by sid", tblName)
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

func updateKLineRange(sid int32, timeFrame string, startMS, endMS int64) *errs.Error {
	// QuestDB + sranges: record the data range (non-contiguous allowed).
	if startMS <= 0 || endMS <= startMS {
		return nil
	}
	if err := PubQ().UpdateSRanges(context.Background(), sid, "kline_"+timeFrame, timeFrame, startMS, endMS, true); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func (q *Queries) updateBigHyper(exs *ExSymbol, timeFrame string, startMS, endMS int64) *errs.Error {
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	aggTfs := map[string]bool{timeFrame: true}
	aggJobs := make([]*KlineAgg, 0)
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
	oldStart, oldEnd := PubQ().GetKlineRange(sid, item.TimeFrame)
	if oldStart > 0 && oldEnd > oldStart {
		// Avoid voids or data errors
		// 避免出现空洞或数据错误
		aggStart = min(aggStart, oldEnd)
		endMS = max(endMS, oldStart)
	}
	if aggFrom == "" {
		aggFrom = item.AggFrom
	}
	if aggFrom == "" {
		return nil
	}
	fromTbl := "kline_" + aggFrom
	ctx := context.Background()
	rows, err_ := q.db.Query(ctx, fmt.Sprintf(`
select cast(ts as long)/1000,open,high,low,close,volume,quote,buy_volume,trade_num
from %s
where sid=$1 and ts >= $2 and ts < $3
order by ts`, fromTbl), sid, time.UnixMilli(aggStart).UTC(), time.UnixMilli(endMS).UTC())
	src, err_ := mapToKlines(rows, err_)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(src) == 0 {
		return nil
	}
	fromTfMSecs := int64(utils2.TFToSecs(aggFrom) * 1000)
	offMS := GetAlignOff(sid, tfMSecs)
	aggBars, lastFinish := utils.BuildOHLCV(src, tfMSecs, 0, nil, fromTfMSecs, offMS, infoBy)
	if !lastFinish && len(aggBars) > 0 {
		aggBars = aggBars[:len(aggBars)-1]
	}
	if len(aggBars) == 0 {
		return nil
	}
	// Keep only complete bars within [aggStart, endMS).
	cut := aggBars[:0]
	for _, b := range aggBars {
		if b.Time < aggStart {
			continue
		}
		if b.Time+tfMSecs > endMS {
			break
		}
		cut = append(cut, b)
	}
	aggBars = cut
	if len(aggBars) == 0 {
		return nil
	}
	_, err := q.InsertKLines(item.TimeFrame, sid, aggBars)
	if err != nil {
		return err
	}
	// Use actual inserted bar range for srange update
	// 使用实际插入的bar范围更新srange
	saveStart := aggBars[0].Time
	saveEnd := aggBars[len(aggBars)-1].Time + tfMSecs
	// Update the effective range of intervals
	// 更新有效区间范围
	err = updateKLineRange(sid, item.TimeFrame, saveStart, saveEnd)
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
	sql := fmt.Sprintf("select count(0) from kline_%s where sid=%v and ts>=cast(%v as timestamp) and ts<cast(%v as timestamp)",
		timeFrame, sid, start*1000, end*1000)
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

/*
DelKLines
通过重写表的方式删除不在sranges和exsymbol中的K线数据。
先计算涵盖的K线数量占比，不足50%时才执行删除，否则跳过。
*/
func (q *Queries) DelKLines(timeFrame string) *errs.Error {
	tblName := "kline_" + timeFrame
	ctx := context.Background()
	// 1. 获取sranges中有数据的sid集合
	db, err := BanPubConn(false)
	if err != nil {
		return err
	}
	rows, err_ := db.QueryContext(ctx, `select distinct sid from sranges where tbl=? and has_data=1`, tblName)
	if err_ != nil {
		db.Close()
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	srangeSids := make(map[int32]bool)
	for rows.Next() {
		var sid int32
		if err_ = rows.Scan(&sid); err_ != nil {
			rows.Close()
			db.Close()
			return NewDbErr(core.ErrDbReadFail, err_)
		}
		srangeSids[sid] = true
	}
	rows.Close()
	// 2. 获取exsymbol中所有sid集合
	rows, err_ = db.QueryContext(ctx, `select id from exsymbol`)
	if err_ != nil {
		db.Close()
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	exsSids := make(map[int32]bool)
	for rows.Next() {
		var sid int32
		if err_ = rows.Scan(&sid); err_ != nil {
			rows.Close()
			db.Close()
			return NewDbErr(core.ErrDbReadFail, err_)
		}
		exsSids[sid] = true
	}
	rows.Close()
	db.Close()
	// 3. 取交集：同时在sranges和exsymbol中的sid
	var validSids []string
	for sid := range srangeSids {
		if exsSids[sid] {
			validSids = append(validSids, strconv.Itoa(int(sid)))
		}
	}
	if len(validSids) == 0 {
		log.Info("DelKLines: no valid sids, skip", zap.String("tf", timeFrame))
		return nil
	}
	sidIn := strings.Join(validSids, ",")
	// 4. 统计总数据量和涵盖的数据量
	row := q.db.QueryRow(ctx, fmt.Sprintf("select count(0) from %s", tblName))
	var totalCount int64
	if err_ = row.Scan(&totalCount); err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if totalCount == 0 {
		return nil
	}
	row = q.db.QueryRow(ctx, fmt.Sprintf("select count(0) from %s where sid in (%s)", tblName, sidIn))
	var coveredCount int64
	if err_ = row.Scan(&coveredCount); err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	ratio := float64(coveredCount) / float64(totalCount)
	if ratio >= 0.5 {
		log.Info("DelKLines: covered ratio >= 50%, skip rewrite",
			zap.String("tf", timeFrame), zap.Int64("total", totalCount),
			zap.Int64("covered", coveredCount), zap.String("ratio", fmt.Sprintf("%.1f%%", ratio*100)))
		return nil
	}
	// 5. 估算时间并输出日志
	// QuestDB大约每秒处理100万行复制
	estSecs := float64(coveredCount) / 1_000_000
	if estSecs < 1 {
		estSecs = 1
	}
	log.Info("DelKLines: rewriting table, please wait...",
		zap.String("tf", timeFrame), zap.Int64("total", totalCount),
		zap.Int64("keep", coveredCount), zap.Int64("remove", totalCount-coveredCount),
		zap.String("ratio", fmt.Sprintf("%.1f%%", ratio*100)),
		zap.Int("est_secs", int(estSecs)))
	// 6. 重写表：创建新表 -> 复制数据 -> 删除旧表 -> 重命名
	partMap := map[string]string{
		"1m": "week", "5m": "month", "15m": "month", "1h": "year", "1d": "year",
	}
	partBy, ok := partMap[timeFrame]
	if !ok {
		return errs.NewMsg(core.ErrInvalidTF, "unsupported tf for rewrite: %s", timeFrame)
	}
	tmpTbl := tblName + "_new"
	createSQL := fmt.Sprintf(`create table %s as (
select sid,ts,open,high,low,close,volume,quote,buy_volume,trade_num
from %s where sid in (%s)
) timestamp(ts) partition by %s dedup upsert keys(sid, ts)`, tmpTbl, tblName, sidIn, partBy)
	if _, err_ = q.db.Exec(ctx, createSQL); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	if _, err_ = q.db.Exec(ctx, fmt.Sprintf("drop table %s", tblName)); err_ != nil {
		// 尝试清理临时表
		_, _ = q.db.Exec(ctx, fmt.Sprintf("drop table %s", tmpTbl))
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	if _, err_ = q.db.Exec(ctx, fmt.Sprintf("rename table %s to %s", tmpTbl, tblName)); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	log.Info("DelKLines: table rewrite done",
		zap.String("tf", timeFrame), zap.Int64("kept", coveredCount))
	return nil
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
	ctx := context.Background()
	db, err := BanPubConn(false)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err_ := db.QueryContext(ctx, `select sid,tbl,timeframe from sranges where has_data=1 and (stop_ms=0 or start_ms=0)`)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()

	tfGroups := make(map[string]map[int32]bool)
	for rows.Next() {
		var sid int32
		var tbl, tf string
		if err_ := rows.Scan(&sid, &tbl, &tf); err_ != nil {
			return NewDbErr(core.ErrDbReadFail, err_)
		}
		if tbl != "kline_"+tf {
			continue
		}
		if tfGroups[tf] == nil {
			tfGroups[tf] = make(map[int32]bool)
		}
		tfGroups[tf][sid] = true
	}
	if err_ := rows.Err(); err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(tfGroups) == 0 {
		return nil
	}

	var totalFixed int
	for tf, sids := range tfGroups {
		log.Info("fixing sranges zeros", zap.String("timeframe", tf), zap.Int("count", len(sids)))
		ranges, err := q.CalcKLineRanges(tf, sids)
		if err != nil {
			return err
		}
		for sid, r := range ranges {
			start, stop := r[0], r[1]
			if start <= 0 || stop <= start {
				continue
			}
			{
				dbw, err := BanPubConn(true)
				if err != nil {
					return err
				}
				_, err_ := dbw.ExecContext(ctx, `delete from sranges where sid=? and tbl=? and timeframe=? and has_data=1 and (stop_ms=0 or start_ms=0)`,
					sid, "kline_"+tf, tf)
				dbw.Close()
				if err_ != nil {
					return NewDbErr(core.ErrDbExecFail, err_)
				}
			}
			if err := updateKLineRange(sid, tf, start, stop); err != nil {
				return err
			}
			totalFixed++
		}
	}
	log.Info("fixed sranges zeros complete", zap.Int("total", totalFixed), zap.Int("timeframes", len(tfGroups)))
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
			pb.SetProgress("syncTfRanges", float64(done)/float64(total))
		}
	})
	return err
}

func syncKlineInfos(sess *Queries, sids map[int32]string, prg utils.PrgCB) *errs.Error {
	// Build sid filter for GetKlineRanges (sranges-based, avoids soft-deleted data in QuestDB).
	sidFilter := make([]int32, 0, len(sids))
	for sid := range sids {
		sidFilter = append(sidFilter, sid)
	}
	calcs := make(map[string]map[int32][2]int64)
	for _, agg := range aggList {
		calcs[agg.TimeFrame] = PubQ().GetKlineRanges(sidFilter, agg.TimeFrame)
	}
	// Decide which sids to process.
	sidList := make([]int32, 0)
	if len(sids) > 0 {
		for sid := range sids {
			sidList = append(sidList, sid)
		}
	} else {
		seen := make(map[int32]bool)
		for _, m := range calcs {
			for sid := range m {
				if seen[sid] {
					continue
				}
				seen[sid] = true
				// Skip sids not in exsymbol to avoid reviving soft-deleted data
				if GetSymbolByID(sid) == nil {
					continue
				}
				sidList = append(sidList, sid)
			}
		}
	}
	sort.Slice(sidList, func(i, j int) bool { return sidList[i] < sidList[j] })

	pgTotal := max(1, len(sidList)*len(aggList))
	pBar := utils.NewPrgBar(pgTotal, "sync tf")
	if prg != nil {
		pBar.PrgCbs = append(pBar.PrgCbs, prg)
	}
	defer pBar.Close()

	return utils.ParallelRun(sidList, 20, func(_ int, sid int32) *errs.Error {
		sess2, conn, err := Conn(nil)
		if err != nil {
			return err
		}
		defer conn.Release()
		infoBy := sids[sid]
		if infoBy == "" {
			if exs := GetSymbolByID(sid); exs != nil {
				infoBy = exs.InfoBy()
			}
		}
		err = sess2.syncKlineSid(sid, infoBy, calcs)
		pBar.Add(len(aggList))
		return err
	})
}

func (q *Queries) syncKlineSid(sid int32, infoBy string, calcs map[string]map[int32][2]int64) *errs.Error {
	ctx := context.Background()
	tfRanges := make(map[string][2]int64)
	for _, agg := range aggList {
		rg, ok := calcs[agg.TimeFrame][sid]
		if !ok || rg[0] == 0 || rg[1] == 0 {
			if err := PubQ().DelKInfo(sid, agg.TimeFrame); err != nil {
				return err
			}
			continue
		}
		newStart, newEnd := rg[0], rg[1]
		tfRanges[agg.TimeFrame] = rg
		if err := PubQ().UpdateSRanges(ctx, sid, "kline_"+agg.TimeFrame, agg.TimeFrame, newStart, newEnd, true); err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
		if err := q.updateKHoles(sid, agg.TimeFrame, newStart, newEnd, false); err != nil {
			return err
		}
	}
	// Attempt to aggregate updates from subintervals.
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
		if curStart == 0 || curEnd == 0 {
			if err := q.refreshAgg(agg, sid, subStart, subEnd, "", infoBy, false); err != nil {
				return err
			}
			continue
		}
		tfMSecs := int64(utils2.TFToSecs(agg.TimeFrame) * 1000)
		subAlignStart := utils2.AlignTfMSecs(subStart, tfMSecs)
		subAlignEnd := utils2.AlignTfMSecs(subEnd, tfMSecs)
		if subAlignStart < curStart {
			if err := q.refreshAgg(agg, sid, subStart, min(subEnd, curStart), "", infoBy, false); err != nil {
				return err
			}
		}
		if subAlignEnd > curEnd {
			if err := q.refreshAgg(agg, sid, max(curEnd, subStart), subEnd, "", infoBy, false); err != nil {
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
	items, err_ := PubQ().GetAllInsKlines(ctx)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(items) == 0 {
		return nil
	}
	log.Info("Updating pending insert jobs", zap.Int("num", len(items)))
	for _, i := range items {
		if i.StartMs > 0 && i.StopMs > 0 {
			start, end := PubQ().GetKlineRange(i.Sid, i.Timeframe)
			if start > 0 && end > 0 {
				exs := GetSymbolByID(i.Sid)
				if exs != nil {
					if err := q.UpdateKRange(exs, i.Timeframe, start, end, nil, true); err != nil {
						return err
					}
				}
			}
		}
		err_ = PubQ().DelInsKline(ctx, i.ID)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	return nil
}

func AddInsJob(add AddInsKlineParams) (int64, *errs.Error) {
	ctx := context.Background()
	newId, err_ := PubQ().AddInsKline(ctx, add)
	if err_ != nil {
		return 0, NewDbErr(core.ErrDbExecFail, err_)
	}
	if newId == 0 {
		log.Warn("insert candles for symbol locked, skip", zap.Int32("sid", add.Sid), zap.String("tf", add.Timeframe))
		return 0, nil
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
			err = saveAdjFactors(dateSidVols, lastCode, lastExs, args.OutPath)
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
	return saveAdjFactors(dateSidVols, lastCode, lastExs, args.OutPath)
}

func saveAdjFactors(data map[int64]map[int32]*banexg.Kline, pCode string, pExs *ExSymbol, outDir string) *errs.Error {
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
	err_ := PubQ().DelAdjFactors(ctx, exs.ID)
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
	_, err_ = PubQ().AddAdjFactors(ctx, adds)
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
				p1, v1, i1 = k1.Close, k1.Volume, k1.BuyVolume
			}
			if k2 != nil {
				p2, v2, i2 = k2.Close, k2.Volume, k2.BuyVolume
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
			hold.Vol = k.BuyVolume
		} else if k.Volume > vol.Vol {
			vol.Sid = sid
			vol.Price = k.Close
			vol.Vol = k.Volume
		}
		if k.BuyVolume > hold.Vol {
			hold.Sid = sid
			hold.Price = k.Close
			hold.Vol = k.BuyVolume
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
