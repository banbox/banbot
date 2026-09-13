package orm

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	aggMap = make(map[string]*KlineAgg)
)

func init() {
	for _, agg := range aggList {
		aggMap[agg.TimeFrame] = agg
	}
}

// QueryOHLCV queries K-lines using the legacy banexg.Kline representation.
//
// Deprecated: use QuerySeries, which returns DataSeries values, instead.
func (q *Queries) QueryOHLCV(exs *ExSymbol, timeframe string, startMs, endMs int64, limit int, withUnFinish bool) ([]*banexg.Kline, *errs.Error) {
	rows, err := q.QuerySeries(exs, timeframe, startMs, endMs, limit, withUnFinish)
	if err != nil {
		return nil, err
	}
	klines, projectErr := SeriesToKLines(rows, exs)
	if projectErr != nil {
		return nil, errs.New(core.ErrInvalidBars, projectErr)
	}
	return klines, nil
}

type KlineSid struct {
	banexg.Kline
	Sid int32
}

// KlineExchangeFactory creates an adapter for a stored exchange/market
// identity. The caller owns the adapters it returns.
type KlineExchangeFactory func(context.Context, string, string) (banexg.BanExchange, *errs.Error)

// KlineSyncDeps supplies the runtime-owned resources required to synchronize
// stored kline timeframes. Symbols must contain every stored identity that can
// be corrected; this operation is deliberately not limited to configured pairs.
type KlineSyncDeps struct {
	Context         context.Context
	Queries         *Queries
	Symbols         []*ExSymbol
	ExchangeFactory KlineExchangeFactory
	ConfirmAll      func(context.Context) (bool, error)
	Logger          *zap.Logger
}

// QueryOHLCVBatch queries K-lines in batches using the legacy banexg.Kline representation.
//
// Deprecated: use QuerySeriesBatch, which returns DataSeries values, instead.
func (q *Queries) QueryOHLCVBatch(exsMap map[int32]*ExSymbol, timeframe string, startMs, endMs int64, limit int, handle func(int32, []*banexg.Kline)) *errs.Error {
	var projectErr *errs.Error
	err := q.QuerySeriesBatch(exsMap, timeframe, startMs, endMs, limit, func(sid int32, rows []*DataSeries) {
		if projectErr != nil {
			return
		}
		klines, err := SeriesToKLines(rows, exsMap[sid])
		if err != nil {
			projectErr = errs.New(core.ErrInvalidBars, err)
			return
		}
		handle(sid, klines)
	})
	if err != nil {
		return err
	}
	return projectErr
}

func (q *Queries) getKLineTimes(sid int32, timeframe string, startMs, endMs int64) ([]int64, *errs.Error) {
	if !q.isQuestDB() {
		return q.getKLineTimesPg(sid, timeframe, startMs, endMs)
	}
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
	if !q.isQuestDB() {
		return q.getKLineTimeRangePg(sid, timeframe)
	}
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
	options, optionsErr := q.requireKlineRuntimeOptions()
	if optionsErr != nil {
		return optionsErr
	}
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	barTimes, err := q.getKLineTimes(sid, timeFrame, startMS, endMS)
	if err != nil {
		return err
	}
	holes := make([]MSRange, 0)
	if len(barTimes) == 0 {
		// **QuestDB WAL 延迟防护**：防止 WAL 延迟导致读取结果为空。
		// 若 `srangesCache` 已覆盖该区间，此处空值即为「假阴性」。
		// 严禁写入 `has_data=false`，否则其新时间戳会使 `LATEST BY` 永久覆盖旧的正确记录，导致回测时重复下载。
		if q.isQuestDB() {
			tbl := "kline_" + timeFrame
			cachedTrue := q.cachedSRanges(sid, tbl, timeFrame)
			if len(cachedTrue) > 0 {
				uncovered := subtractMSRanges(MSRange{Start: startMS, Stop: endMS}, cachedTrue)
				if len(uncovered) == 0 {
					// Fully covered by in-process has_data=true cache: skip hole detection.
					return nil
				}
			}
		}
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
		maxEnd := utils2.AlignTfMSecs(options.nowMS(), tfMSecs) - tfMSecs
		if maxEnd-prevTime > tfMSecs*5 && endMS-prevTime > tfMSecs {
			holes = append(holes, MSRange{Start: prevTime + tfMSecs, Stop: min(endMS, maxEnd)})
		}
	}
	if len(holes) == 0 {
		// No holes found – mark the entire window as has_data=true in one atomic call.
		ctx := context.Background()
		tbl := "kline_" + timeFrame
		if err := q.UpdateSRangesWithHoles(ctx, sid, tbl, timeFrame, startMS, endMS, nil); err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
		return nil
	}

	// Filter out non-trading time ranges.
	exs := q.symbolByID(sid)
	if exs == nil {
		log.Warn("no ExSymbol found", zap.Int32("sid", sid))
		return nil
	}
	exchange := q.exchange
	if exchange == nil {
		if q.usesExplicitExchange() {
			return errs.NewMsg(core.ErrExgNotInit, "explicit query requires an exchange adapter")
		}
		var err *errs.Error
		exchange, err = exg.GetWith(exs.Exchange, exs.Market, "")
		if err != nil {
			return err
		}
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
	if err := q.rewriteHoleRangesInWindow(ctx, sid, timeFrame, startMS, endMS, holes); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func (q *Queries) rewriteHoleRangesInWindow(ctx context.Context, sid int32, timeFrame string, startMS, endMS int64, holes []MSRange) error {
	tbl := "kline_" + timeFrame
	// Use a single read+write cycle so QuestDB WAL commit lag between two consecutive
	// UpdateSRanges calls cannot lose the has_data=true regions.
	return q.UpdateSRangesWithHoles(ctx, sid, tbl, timeFrame, startMS, endMS, holes)
}

func exactKlineHoles(barTimes []int64, tfMSecs, startMS, endMS int64) []MSRange {
	holes := make([]MSRange, 0)
	cur := startMS
	for _, barTime := range barTimes {
		if barTime < cur {
			continue
		}
		if barTime >= endMS {
			break
		}
		if barTime > cur {
			holes = append(holes, MSRange{Start: cur, Stop: barTime})
		}
		cur = barTime + tfMSecs
	}
	if cur < endMS {
		holes = append(holes, MSRange{Start: cur, Stop: endMS})
	}
	return holes
}

func (q *Queries) repairKlineRangeFromPhysical(sid int32, timeframe string, startMS, endMS int64) *errs.Error {
	if startMS <= 0 || endMS <= startMS {
		return nil
	}
	tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
	barTimes, err := q.getKLineTimes(sid, timeframe, startMS, endMS)
	if err != nil {
		return err
	}
	if err := q.rewriteHoleRangesInWindow(context.Background(), sid, timeframe, startMS, endMS,
		exactKlineHoles(barTimes, tfMSecs, startMS, endMS)); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func (q *Queries) reconcileKlineRangeFromPhysical(sid int32, timeframe string, startMS, endMS int64) (bool, *errs.Error) {
	if startMS <= 0 || endMS <= startMS {
		return false, nil
	}
	tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
	barTimes, err := q.getKLineTimes(sid, timeframe, startMS, endMS)
	if err != nil {
		return false, err
	}
	if len(barTimes) == 0 {
		return false, nil
	}
	if err := q.rewriteHoleRangesInWindow(context.Background(), sid, timeframe, startMS, endMS,
		exactKlineHoles(barTimes, tfMSecs, startMS, endMS)); err != nil {
		return false, NewDbErr(core.ErrDbExecFail, err)
	}
	return true, nil
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

	nowMS := sess.klineRuntimeOptions().nowMS()
	if barEndMS > 0 {
		nowMS = min(nowMS, barEndMS)
	}

	// 1) Read cached unfinish (kline_un) first. Only recompute when expired.
	cached, cachedStop, cachedExpire, err := sess.queryUnfinish(sid, timeFrame, startMS)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
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
	if err2 := sess.SetUnfinish(sid, timeFrame, stopMS, bar); err2 != nil {
		log.Warn("set unfinish fail", zap.Int32("sid", sid), zap.String("tf", timeFrame), zap.String("err", err2.Short()))
	}
	return bar, stopMS, nil
}

func queryUnfinish(sid int32, timeFrame string, barStartMS int64) (*banexg.Kline, int64, *int64, error) {
	queries, conn, err := Conn(nil)
	if err != nil {
		return nil, 0, nil, err
	}
	defer conn.Release()
	return queries.queryUnfinish(sid, timeFrame, barStartMS)
}

func (q *Queries) queryUnfinish(sid int32, timeFrame string, barStartMS int64) (*banexg.Kline, int64, *int64, error) {
	if !q.isQuestDB() {
		return q.queryUnfinishPg(sid, timeFrame, barStartMS)
	}
	unlock := q.LockCompactTableRead("kline_un_q")
	defer unlock()
	ctx := context.Background()
	row := q.db.QueryRow(ctx, `SELECT cast(ts as long)/1000, open, high, low, close, volume, quote, buy_volume, trade_num, stop_ms, expire_ms
FROM kline_un_q
LATEST BY sid, timeframe
WHERE sid = $1 AND timeframe = $2 AND coalesce(is_deleted, false) = false
  AND cast(ts as long)/1000 >= $3`,
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
	if !sess.isQuestDB() {
		return sess.queryOHLCVPg(sid, timeFrame, startMS, endMS, 0, false)
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
	exs := sess.symbolByID(sid)
	offMS := sess.alignOff(exs, toTfMSecs)
	if offMS == 0 && exs == nil && sess.usesLegacySymbolCatalog() {
		offMS = GetAlignOff(sid, toTfMSecs)
	}
	var lastFinish bool
	klines, lastFinish = utils.BuildOHLCV(klines, toTfMSecs, 0, nil, fromTfMSecs, offMS)
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
		unbar, unTo, _, err := sess.queryUnfinish(sid, smallTF, unStart)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
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
	for _, p := range parts {
		res.High = max(res.High, p.High)
		res.Low = min(res.Low, p.Low)
		res.Volume += p.Volume
		res.Quote += p.Quote
		res.TradeNum += p.TradeNum
		res.BuyVolume += p.BuyVolume
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
	offMS := int64(exg.GetAlignOffForSymbol(exs.Exchange, exs.Market, exs.Symbol, int(toTfMSecs/1000)) * 1000)
	data[toTfMSecs] = offMS
	return offMS
}

func (q *Queries) SetUnfinish(sid int32, tf string, endMS int64, bar *banexg.Kline) *errs.Error {
	options, optionsErr := q.requireKlineRuntimeOptions()
	if optionsErr != nil {
		return optionsErr
	}
	if !q.isQuestDB() {
		return q.setUnfinishPg(sid, tf, endMS, bar, options)
	}
	unlock := q.LockCompactTableRead("kline_un_q")
	defer unlock()
	expireMS := utils2.AlignTfMSecs(options.nowMS(), 60000) + 60000
	ts := time.UnixMilli(bar.Time).UTC()
	ctx := context.Background()
	_, err := q.db.Exec(ctx, `INSERT INTO kline_un_q
(sid, timeframe, ts, stop_ms, expire_ms, open, high, low, close, volume, quote, buy_volume, trade_num, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, false)`,
		sid, tf, ts, endMS, expireMS,
		bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, bar.Quote, bar.BuyVolume, bar.TradeNum,
	)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
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
	if _, err := q.requireKlineRuntimeOptions(); err != nil {
		return 0, err
	}
	if !q.isQuestDB() {
		return insertKLinesPg(q, timeFrame, sid, arr)
	}
	tblName := "kline_" + timeFrame
	ctx := context.Background()
	unlock, lockErr := q.tableReadLock(ctx, tblName)
	if lockErr != nil {
		return 0, NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	return q.insertKLinesLocked(timeFrame, sid, arr)
}

// insertKLinesLocked writes rows while the caller holds the target table read lock.
func (q *Queries) insertKLinesLocked(timeFrame string, sid int32, arr []*banexg.Kline) (int64, *errs.Error) {
	arrLen := len(arr)
	if arrLen == 0 {
		return 0, nil
	}
	if !q.isQuestDB() {
		return insertKLinesPg(q, timeFrame, sid, arr)
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
	if _, err := q.requireKlineRuntimeOptions(); err != nil {
		return 0, err
	}
	tblName := "kline_" + timeFrame
	unlock, lockErr := q.tableReadLock(context.Background(), tblName)
	if lockErr != nil {
		return 0, NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	startMS := arr[0].Time
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	endMS := arr[len(arr)-1].Time + tfMSecs
	insTs, addErr := q.AddInsKline(context.Background(), AddInsKlineParams{
		Sid:       exs.ID,
		Timeframe: timeFrame,
		StartMs:   startMS,
		StopMs:    endMS,
	})
	if addErr != nil {
		return 0, NewDbErr(core.ErrDbExecFail, addErr)
	}
	if insTs.IsZero() {
		return 0, nil
	}
	write := q
	var tx pgx.Tx
	if !q.isQuestDB() {
		var txErr error
		tx, write, txErr = q.begin(context.Background())
		if txErr != nil {
			return 0, NewDbErr(core.ErrDbExecFail, txErr)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
	}
	num, err := write.insertKLinesLocked(timeFrame, exs.ID, arr)
	if err != nil {
		return num, err
	}
	if err = write.finalizeKlineInsert(exs, timeFrame, startMS, endMS, arr[len(arr)-1].Time, insTs, aggBig); err != nil {
		return num, err
	}
	if tx != nil {
		if err_ := tx.Commit(context.Background()); err_ != nil {
			return num, NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	return num, nil
}

func (q *Queries) finalizeKlineInsert(exs *ExSymbol, timeFrame string, startMS, endMS, lastMS int64,
	insTs time.Time, aggBig bool) (outErr *errs.Error) {
	defer func() {
		if outErr != nil {
			_ = q.releaseInsertOwnership(exs.ID, timeFrame, insTs)
		}
	}()
	ctx := context.Background()
	if q.isQuestDB() {
		if err := waitForQuestKlineTimestampVisible(ctx, q, exs.ID, timeFrame, lastMS); err != nil {
			return err
		}
	}
	if err := q.UpdateKRange(exs, timeFrame, startMS, endMS, aggBig); err != nil {
		return err
	}
	if q.isQuestDB() {
		if err := waitForQuestKlineCoverageVisible(ctx, q, exs.ID, timeFrame, startMS, endMS); err != nil {
			return err
		}
	}
	if err := q.DelInsKline(ctx, exs.ID, timeFrame, insTs); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
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
func (q *Queries) UpdateKRange(exs *ExSymbol, timeFrame string, startMS, endMS int64, aggBig bool, skipHoles ...bool) *errs.Error {
	if _, err := q.requireKlineRuntimeOptions(); err != nil {
		return err
	}
	// Record data ranges in sranges (non-contiguous allowed).
	if err := q.updateKLineRange(exs.ID, timeFrame, startMS, endMS); err != nil {
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
	if !q.isQuestDB() {
		return q.calcKLineRangesPg(timeFrame, sids)
	}
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

func (q *Queries) updateKLineRange(sid int32, timeFrame string, startMS, endMS int64) *errs.Error {
	// QuestDB + sranges: record the data range (non-contiguous allowed).
	if startMS <= 0 || endMS <= startMS {
		return nil
	}
	ctx := context.Background()
	if err := q.UpdateSRanges(ctx, sid, "kline_"+timeFrame, timeFrame, startMS, endMS, true); err != nil {
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
			err := q.refreshAgg(item, exs.ID, startMS, endMS, "", true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (q *Queries) refreshAgg(item *KlineAgg, sid int32, orgStartMS, orgEndMS int64, aggFrom string, isCont bool) *errs.Error {
	tfMSecs := item.MSecs
	startMS := utils2.AlignTfMSecs(orgStartMS, tfMSecs)
	endMS := utils2.AlignTfMSecs(orgEndMS, tfMSecs)
	var delistMS int64
	var exs *ExSymbol
	if !q.isQuestDB() {
		var delistErr *errs.Error
		delistMS, delistErr = q.getDelistMSPg(sid)
		if delistErr != nil {
			return delistErr
		}
	} else if exs = q.symbolByID(sid); exs != nil {
		delistMS = exs.DelistMs
	}
	endMS = aggregateEndForTerminalDelist(orgEndMS, endMS, delistMS, tfMSecs)
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
	if aggFrom == "" {
		return nil
	}
	if !q.isQuestDB() {
		saveStart, saveEnd, err := q.refreshAggPg(item, sid, aggStart, endMS, aggFrom, delistMS)
		if err != nil || saveStart == 0 || saveEnd <= saveStart {
			return err
		}
		return q.repairKlineRangeFromPhysical(sid, item.TimeFrame, saveStart, saveEnd)
	}
	fromTbl := "kline_" + aggFrom
	ctx := context.Background()
	var sourceColumns []questTableColumn
	var src []*DataSeries
	readErr := func() error {
		unlock, lockErr := q.tableReadLock(ctx, fromTbl)
		if lockErr != nil {
			return lockErr
		}
		defer unlock()
		var err error
		sourceColumns, err = queryQuestTableColumns(ctx, q, fromTbl)
		if err != nil {
			return err
		}
		queryText, fields, err := buildQuestKlineAggregateQuery(fromTbl, sourceColumns)
		if err != nil {
			return err
		}
		rows, err := q.db.Query(ctx, queryText, sid, time.UnixMilli(aggStart).UTC(), time.UnixMilli(endMS).UTC())
		src, err = mapToSeriesFields(exs, aggFrom, fields, rows, err)
		return err
	}()
	err_ := readErr
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(src) == 0 {
		return nil
	}
	fromTfMSecs := int64(utils2.TFToSecs(aggFrom) * 1000)
	offMS := q.alignOff(exs, tfMSecs)
	aggBars, lastFinish, err := ResampleDataSeries(exs, item.TimeFrame, src, nil, tfMSecs, 0, fromTfMSecs, offMS, false)
	if err != nil {
		return errs.New(core.ErrInvalidBars, err)
	}
	if !lastFinish && len(aggBars) > 0 {
		aggBars = aggBars[:len(aggBars)-1]
	}
	if len(aggBars) == 0 {
		return nil
	}
	// Keep only complete bars within [aggStart, endMS).
	cut := aggBars[:0]
	for _, b := range aggBars {
		if b.TimeMS < aggStart {
			continue
		}
		if b.TimeMS+tfMSecs > endMS {
			break
		}
		cut = append(cut, b)
	}
	aggBars = cut
	if len(aggBars) == 0 {
		return nil
	}
	values, normalizeErr := normalizeOHLCVSeries(aggBars, sid)
	if normalizeErr != nil {
		return normalizeErr
	}
	targetTbl := "kline_" + item.TimeFrame
	unlock, lockErr := q.tableReadLock(ctx, targetTbl)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	if err := ensureQuestKlineExtraColumns(ctx, q, targetTbl, sourceColumns); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	if _, insertErr := q.insertOHLCVRowsLocked(item.TimeFrame, values); insertErr != nil {
		return insertErr
	}
	// Use actual inserted bar range for srange update
	// 使用实际插入的bar范围更新srange
	saveStart := aggBars[0].TimeMS
	saveEnd := aggBars[len(aggBars)-1].TimeMS + tfMSecs
	if err := waitForQuestKlineTimestampVisible(ctx, q, sid, item.TimeFrame, aggBars[len(aggBars)-1].TimeMS); err != nil {
		return err
	}
	// Update the effective range of intervals
	// 更新有效区间范围
	if rangeErr := q.updateKLineRange(sid, item.TimeFrame, saveStart, saveEnd); rangeErr != nil {
		return rangeErr
	}
	if err := waitForQuestKlineCoverageVisible(ctx, q, sid, item.TimeFrame, saveStart, saveEnd); err != nil {
		return err
	}
	return nil
}

func aggregateEndForTerminalDelist(orgEndMS, alignedEndMS, delistMS, timeframeMS int64) int64 {
	if orgEndMS >= delistMS && allowPartialTerminalAggregate(delistMS, alignedEndMS, timeframeMS) {
		return alignedEndMS + timeframeMS
	}
	return alignedEndMS
}

func NewKlineAgg(TimeFrame, Table, AggFrom, AggStart, AggEnd, AggEvery, CpsBefore, Retention string) *KlineAgg {
	msecs := int64(utils2.TFToSecs(TimeFrame) * 1000)
	return &KlineAgg{TimeFrame, msecs, Table, AggFrom, AggStart, AggEnd, AggEvery, CpsBefore, Retention}
}

func (q *Queries) GetKlineNum(sid int32, timeFrame string, start, end int64) int {
	if !q.isQuestDB() {
		return q.getKLineNumPg(sid, timeFrame, start, end)
	}
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
通过重写表的方式删除指定sid的K线数据（QuestDB不支持对WAL表直接DELETE）。
delSids为本次需要删除的sid集合；validSids = 表中已有sids - delSids。

调用此函数前，DelKData已经通过DelKInfo对delSids的sranges做了soft-delete。
由于kline表的任何读取路径都必须先查sranges确认数据范围，sranges已删除意味着
这些"幽灵行"永远不会被读到。因此：
- 若删除比例 < 50%（删除量偏少），跳过重写，sranges层面的删除已足够保证正确性；
- 若删除比例 >= 50%（删除量较多），执行重写以回收磁盘空间。
*/
func (q *Queries) DelKLines(timeFrame string, delSids map[int32]bool) *errs.Error {
	if !q.isQuestDB() {
		return q.delKLinesPgBySid(timeFrame, delSids)
	}
	tblName := "kline_" + timeFrame
	ctx := context.Background()

	partMap := map[string]string{
		"1m": "week", "5m": "month", "15m": "month", "1h": "year", "1d": "year",
	}
	partBy, ok := partMap[timeFrame]
	if !ok {
		return errs.NewMsg(core.ErrInvalidTF, "unsupported tf for rewrite: %s", timeFrame)
	}
	unlock, acquired, lockErr := q.tableWriteLock(ctx, tblName)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	if !acquired {
		return errs.NewMsg(core.ErrRunTime, "kline table %s is in use by another process", tblName)
	}
	defer unlock()
	sourceTxn, err_ := waitForCompactWalAppliedAtRoot(ctx, q.db, tblName, q.processLockRoot())
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}

	// 1. 一次扫描同时获取所有 sid 及其行数，避免两次全表扫描。
	//    不依赖 sranges，避免 WAL 延迟导致刚删除的 sid 仍被查到。
	sidRows, err_ := q.db.Query(ctx, fmt.Sprintf("SELECT %s, count(*) FROM %s GROUP BY %s", quoteIdent("sid"), quoteIdent(tblName), quoteIdent("sid")))
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	var validSids []string
	keepSidCounts := make(map[int32]int64)
	var totalCount, keepCount int64
	for sidRows.Next() {
		var sid int32
		var cnt int64
		if err_ = sidRows.Scan(&sid, &cnt); err_ != nil {
			sidRows.Close()
			return NewDbErr(core.ErrDbReadFail, err_)
		}
		totalCount += cnt
		if !delSids[sid] {
			validSids = append(validSids, strconv.Itoa(int(sid)))
			keepSidCounts[sid] = cnt
			keepCount += cnt
		}
	}
	sidRows.Close()
	if err_ = sidRows.Err(); err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if err_ = verifyQuestRewriteSourceStable(ctx, q, tblName, sourceTxn); err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}

	if totalCount == 0 {
		return nil
	}

	// 2. 当 validSids 为空时，全部数据需要清除，执行 DROP+RECREATE（QuestDB WAL表不支持 TRUNCATE）。
	if len(validSids) == 0 {
		log.Info("DelKLines: no valid sids, truncating table",
			zap.String("tf", timeFrame), zap.Int64("total", totalCount))
		tmpTbl := compactTempTableName(tblName)
		backupTbl := compactBackupTableName(tblName)
		expected, snapshotErr := captureQuestRewriteTableSnapshot(ctx, q, tblName, "sid", "1=0")
		if snapshotErr != nil {
			return NewDbErr(core.ErrDbReadFail, snapshotErr)
		}
		if err_ := verifyQuestRewriteSourceStable(ctx, q, tblName, sourceTxn); err_ != nil {
			return NewDbErr(core.ErrDbReadFail, err_)
		}
		createSQL, buildErr := buildQuestKlineRewriteSQLChecked(tmpTbl, tblName, "1=0", partBy, expected.Columns)
		if buildErr != nil {
			return NewDbErr(core.ErrDbReadFail, buildErr)
		}
		if _, err_ = q.db.Exec(ctx, createSQL); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTbl, fmt.Errorf("create kline rewrite table: %w", err_)))
		}
		if _, err_ = waitCompactVisibleCount(ctx, q.db, tmpTbl, questRewriteSnapshotRowCount(expected.Counts)); err_ != nil {
			return NewDbErr(core.ErrDbReadFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTbl, err_))
		}
		if verifyErr := verifyQuestRewriteTableSnapshot(ctx, q, tmpTbl, "sid", "", expected); verifyErr != nil {
			cause := fmt.Errorf("verify kline rewrite table: %s", verifyErr.Short())
			return NewDbErr(core.ErrDbReadFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTbl, cause))
		}
		if replaceErr := replaceVerifiedQuestTable(ctx, q, tblName, tmpTbl, backupTbl, "sid", "1=0", expected); replaceErr != nil {
			return replaceErr
		}
		log.Info("DelKLines: table truncated", zap.String("tf", timeFrame))
		return nil
	}

	// 3. 决定是否需要物理重写：
	//    删除比例 < 50% 时，重写性价比低；sranges soft-delete已足够屏蔽这些幽灵行。
	deleteCount := totalCount - keepCount
	deleteRatio := float64(deleteCount) / float64(totalCount)
	if deleteRatio < 0.5 {
		log.Info("DelKLines: delete ratio < 50%, skip rewrite (sranges already cleaned)",
			zap.String("tf", timeFrame), zap.Int64("total", totalCount),
			zap.Int64("delete", deleteCount), zap.Int64("keep", keepCount),
			zap.String("deleteRatio", fmt.Sprintf("%.1f%%", deleteRatio*100)))
		return nil
	}

	// 4. 估算重写耗时并输出日志（QuestDB大约每秒处理100万行复制）
	estSecs := float64(keepCount) / 1_000_000
	if estSecs < 1 {
		estSecs = 1
	}
	log.Info("DelKLines: rewriting table, please wait...",
		zap.String("tf", timeFrame), zap.Int64("total", totalCount),
		zap.Int64("keep", keepCount), zap.Int64("remove", deleteCount),
		zap.String("deleteRatio", fmt.Sprintf("%.1f%%", deleteRatio*100)),
		zap.Int("est_secs", int(estSecs)))

	// 5. 重写表：创建并验证新表 -> 备份旧表 -> 激活并复验新表 -> 删除备份
	sidIn := strings.Join(validSids, ",")
	tmpTbl := compactTempTableName(tblName)
	backupTbl := compactBackupTableName(tblName)
	predicate := fmt.Sprintf("%s IN (%s)", quoteIdent("sid"), sidIn)
	expected, snapshotErr := captureQuestRewriteTableSnapshot(ctx, q, tblName, "sid", predicate)
	if snapshotErr != nil {
		return NewDbErr(core.ErrDbReadFail, snapshotErr)
	}
	if !equalQuestRewriteSnapshot(expected.Counts, keepSidCounts) {
		return errs.NewMsg(core.ErrDbReadFail, "kline rewrite source changed before CTAS: table=%s got=%v want=%v", tblName, expected.Counts, keepSidCounts)
	}
	if err_ := verifyQuestRewriteSourceStable(ctx, q, tblName, sourceTxn); err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	createSQL, buildErr := buildQuestKlineRewriteSQLChecked(tmpTbl, tblName, predicate, partBy, expected.Columns)
	if buildErr != nil {
		return NewDbErr(core.ErrDbReadFail, buildErr)
	}
	if _, err_ = q.db.Exec(ctx, createSQL); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTbl, fmt.Errorf("create kline rewrite table: %w", err_)))
	}
	if _, err_ = waitCompactVisibleCount(ctx, q.db, tmpTbl, questRewriteSnapshotRowCount(expected.Counts)); err_ != nil {
		return NewDbErr(core.ErrDbReadFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTbl, err_))
	}
	if verifyErr := verifyQuestRewriteTableSnapshot(ctx, q, tmpTbl, "sid", "", expected); verifyErr != nil {
		cause := fmt.Errorf("verify kline rewrite table: %s", verifyErr.Short())
		return NewDbErr(core.ErrDbReadFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTbl, cause))
	}
	if replaceErr := replaceVerifiedQuestTable(ctx, q, tblName, tmpTbl, backupTbl, "sid", predicate, expected); replaceErr != nil {
		return replaceErr
	}
	log.Info("DelKLines: table rewrite done",
		zap.String("tf", timeFrame), zap.Int64("kept", keepCount))
	return nil
}

func buildQuestKlineRewriteSQLChecked(tmpTable, sourceTable, predicate, partitionBy string, columns []questTableColumn) (string, error) {
	return buildQuestRewriteSQLChecked(tmpTable, sourceTable, predicate, partitionBy, "ts", columns)
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
	return q.FixKInfoZerosWithContext(context.Background())
}

// FixKInfoZerosWithContext repairs invalid kline range metadata using the
// caller's cancellation scope.
func (q *Queries) FixKInfoZerosWithContext(ctx context.Context) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	unlock := func() {}
	locked := false
	if q.isQuestDB() {
		unlock = q.LockCompactTableRead("sranges_q")
		locked = true
		defer func() {
			if locked {
				unlock()
			}
		}()
	}
	var rows pgx.Rows
	var err_ error
	if q.isQuestDB() {
		rows, err_ = q.db.Query(ctx, `SELECT sid, tbl, timeframe
FROM sranges_q
LATEST BY sid, tbl, timeframe, start_ms
WHERE has_data = true AND coalesce(is_deleted, false) = false AND (stop_ms = 0 OR start_ms = 0)`)
	} else {
		rows, err_ = q.db.Query(ctx, `SELECT sid, tbl, timeframe
FROM sranges
WHERE has_data = true AND (stop_ms = 0 OR start_ms = 0)`)
	}
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
	if locked {
		unlock()
		locked = false
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
			if err := q.DelKInfo(sid, tf); err != nil {
				return err
			}
			if err := q.updateKLineRange(sid, tf, start, stop); err != nil {
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
	sidMap := make(map[int32]bool)
	for _, exs := range exsList {
		if len(pairs) > 0 {
			if _, ok := pairs[exs.Symbol]; !ok {
				continue
			}
			sidMap[exs.ID] = true
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

// SyncKlineTFsWithDeps synchronizes kline timeframe metadata using only the
// supplied runtime resources. It never opens a legacy connection, reads the
// package symbol catalog, or creates an exchange through the global facade.
func SyncKlineTFsWithDeps(args *config.CmdArgs, deps KlineSyncDeps, pb *utils.StagedPrg) *errs.Error {
	if args == nil {
		return errs.NewMsg(core.ErrBadConfig, "kline sync arguments are required")
	}
	ctx := deps.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return errs.New(errs.CodeRunTime, err)
	}
	pairs := make(map[string]bool, len(args.Pairs))
	for _, pair := range args.Pairs {
		pairs[pair] = true
	}
	if len(pairs) == 0 && !args.Force {
		if deps.ConfirmAll == nil {
			return errs.NewMsg(core.ErrBadConfig, "kline sync confirmation is required for all symbols")
		}
		confirmed, err := deps.ConfirmAll(ctx)
		if err != nil {
			return errs.New(errs.CodeRunTime, err)
		}
		if !confirmed {
			return nil
		}
	}
	symbols := deps.Symbols
	if len(pairs) > 0 {
		symbols = make([]*ExSymbol, 0, len(deps.Symbols))
		for _, item := range deps.Symbols {
			if item != nil && pairs[item.Symbol] {
				symbols = append(symbols, item)
			}
		}
		if len(symbols) == 0 {
			return nil
		}
	}
	if deps.Queries == nil {
		return errs.NewMsg(core.ErrBadConfig, "kline sync queries are required")
	}
	if deps.ExchangeFactory == nil {
		return errs.NewMsg(core.ErrBadConfig, "kline sync exchange factory is required")
	}
	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	logger.Info("run kline data sync")

	exchanges, err := prepareKlineSyncExchanges(ctx, symbols, deps.ExchangeFactory)
	if err != nil {
		return err
	}
	if err := deps.Queries.FixKInfoZerosWithContext(ctx); err != nil {
		return err
	}
	if pb != nil {
		pb.SetProgress("fixKInfoZeros", 1)
	}
	sidMap := make(map[int32]bool, len(symbols))
	if len(pairs) > 0 {
		for _, item := range symbols {
			sidMap[item.ID] = true
		}
	}
	return syncKlineInfosWithExchanges(ctx, deps.Queries, sidMap, func(done int, total int) {
		if pb != nil {
			pb.SetProgress("syncTfRanges", float64(done)/float64(total))
		}
	}, exchanges)
}

func prepareKlineSyncExchanges(ctx context.Context, symbols []*ExSymbol, factory KlineExchangeFactory) (map[string]banexg.BanExchange, *errs.Error) {
	exchanges := make(map[string]banexg.BanExchange)
	for _, item := range symbols {
		if item == nil {
			continue
		}
		key := item.Exchange + ":" + item.Market
		if _, ok := exchanges[key]; ok {
			continue
		}
		exchange, err := factory(ctx, item.Exchange, item.Market)
		if err != nil {
			return nil, err
		}
		if exchange == nil {
			return nil, errs.NewMsg(core.ErrExgNotInit, "exchange factory returned nil for %s", key)
		}
		exchanges[key] = exchange
	}
	return exchanges, nil
}

func syncKlineInfos(sess *Queries, sids map[int32]bool, prg utils.PrgCB) *errs.Error {
	return syncKlineInfosWithExchanges(context.Background(), sess, sids, prg, nil)
}

func syncKlineInfosWithExchanges(ctx context.Context, sess *Queries, sids map[int32]bool, prg utils.PrgCB, exchanges map[string]banexg.BanExchange) *errs.Error {
	// Build sid filter for GetKlineRanges (sranges-based, avoids soft-deleted data in QuestDB).
	sidFilter := make([]int32, 0, len(sids))
	for sid := range sids {
		sidFilter = append(sidFilter, sid)
	}
	calcs := make(map[string]map[int32][2]int64)
	for _, agg := range aggList {
		calcs[agg.TimeFrame] = sess.GetKlineRanges(sidFilter, agg.TimeFrame)
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
				if sess.symbolByID(sid) == nil {
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
		var sess2 *Queries
		var conn *pgxpool.Conn
		var err *errs.Error
		if sess.storage != nil {
			sess2, conn, err = sess.storage.Conn(ctx)
		} else {
			sess2, conn, err = Conn(nil)
		}
		if err != nil {
			return err
		}
		defer conn.Release()
		if len(exchanges) > 0 {
			exs := sess.symbolByID(sid)
			if exs == nil {
				return errs.NewMsg(core.ErrInvalidSymbol, "kline sync symbol %d is not in the supplied catalog", sid)
			}
			exchange := exchanges[exs.Exchange+":"+exs.Market]
			if exchange == nil {
				return errs.NewMsg(core.ErrExgNotInit, "kline sync exchange %s:%s is not initialized", exs.Exchange, exs.Market)
			}
			sess2 = sess2.WithExchange(exchange)
		} else if sess.exchange != nil {
			sess2 = sess2.WithExchange(sess.exchange)
		}
		if sess.symbols != nil {
			sess2 = sess2.WithSeriesSymbolState(sess.symbols)
		}
		if sess.options != nil {
			sess2 = sess2.WithKlineRuntimeOptions(*sess.options)
		}
		err = sess2.syncKlineSid(sid, calcs)
		pBar.Add(len(aggList))
		return err
	})
}

func (q *Queries) syncKlineSid(sid int32, calcs map[string]map[int32][2]int64) *errs.Error {
	var delistMS int64
	if !q.isQuestDB() {
		var delistErr *errs.Error
		delistMS, delistErr = q.getDelistMSPg(sid)
		if delistErr != nil {
			return delistErr
		}
	} else if exs := q.symbolByID(sid); exs != nil {
		delistMS = exs.DelistMs
	}
	tfRanges := make(map[string][2]int64)
	for _, agg := range aggList {
		rg, ok := calcs[agg.TimeFrame][sid]
		if !ok || rg[0] == 0 || rg[1] == 0 {
			// sranges may be corrupted; fall back to querying the actual kline table.
			minT, maxT, err := q.getKLineTimeRange(sid, agg.TimeFrame)
			if err != nil {
				return err
			}
			if minT == 0 || maxT == 0 {
				if err := q.DelKInfo(sid, agg.TimeFrame); err != nil {
					return err
				}
				continue
			}
			// maxT is the last bar's open time; stop_ms convention requires adding one tfMSecs.
			tfMSecs := int64(utils2.TFToSecs(agg.TimeFrame) * 1000)
			rg = [2]int64{minT, maxT + tfMSecs}
		}
		newStart, newEnd := rg[0], rg[1]
		tfRanges[agg.TimeFrame] = rg
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
			if err := q.refreshAgg(agg, sid, subStart, subEnd, "", false); err != nil {
				return err
			}
			continue
		}
		tfMSecs := int64(utils2.TFToSecs(agg.TimeFrame) * 1000)
		subAlignStart := utils2.AlignTfMSecs(subStart, tfMSecs)
		subAlignEnd := utils2.AlignTfMSecs(subEnd, tfMSecs)
		subAggregateEnd := aggregateEndForTerminalDelist(subEnd, subAlignEnd, delistMS, tfMSecs)
		if subAlignStart < curStart {
			if err := q.refreshAgg(agg, sid, subStart, min(subEnd, curStart), "", false); err != nil {
				return err
			}
		}
		if subAggregateEnd > curEnd {
			if err := q.refreshAgg(agg, sid, max(curEnd, subStart), subEnd, "", false); err != nil {
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
		active, lockErr := klineInsertFileLockActive(q.insertLockRoot(), i.Sid, i.Timeframe)
		if lockErr != nil {
			log.Warn("check pending insert owner fail; keep job", zap.Int32("sid", i.Sid),
				zap.String("tf", i.Timeframe), zap.Error(lockErr))
			continue
		}
		if active {
			log.Debug("pending insert still owned by active process", zap.Int32("sid", i.Sid),
				zap.String("tf", i.Timeframe))
			continue
		}
		if i.StartMs > 0 && i.StopMs > 0 {
			start, end := i.StartMs, i.StopMs
			if q.isQuestDB() {
				var waitErr *errs.Error
				start, end, waitErr = waitForQuestKlineRangeVisible(ctx, q, i.Sid, i.Timeframe, start, end)
				if waitErr != nil {
					if waitErr.Code != core.ErrTimeout {
						return waitErr
					}
					// QuestDB WAL visibility can lag without indicating a
					// database failure. Keep the pending marker and let the
					// next recovery pass retry it.
					log.Warn("pending insert rows still not visible; keep job for next recovery",
						zap.Int32("sid", i.Sid), zap.String("tf", i.Timeframe),
						zap.Int64("job_start", i.StartMs), zap.Int64("job_stop", i.StopMs),
						zap.Error(waitErr))
					continue
				}
			}
			if start > 0 && end > start {
				exs := q.symbolByID(i.Sid)
				if exs == nil {
					log.Warn("pending insert symbol is unavailable; keep job", zap.Int32("sid", i.Sid))
					continue
				}
				if q.isQuestDB() {
					if err := q.UpdateKRange(exs, i.Timeframe, start, end, true); err != nil {
						return err
					}
				} else {
					if err := q.repairKlineRangeFromPhysical(i.Sid, i.Timeframe, start, end); err != nil {
						return err
					}
					if err := q.updateBigHyper(exs, i.Timeframe, start, end); err != nil {
						return err
					}
				}
			} else if q.isQuestDB() {
				log.Warn("pending insert rows still not visible; keep job for next recovery",
					zap.Int32("sid", i.Sid),
					zap.String("tf", i.Timeframe),
					zap.Int64("job_start", i.StartMs),
					zap.Int64("job_stop", i.StopMs))
				continue
			}
		}
		err_ = q.DelInsKline(ctx, i.Sid, i.Timeframe, i.Ts)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	return nil
}

func AddInsJob(add AddInsKlineParams) (time.Time, *errs.Error) {
	ctx := context.Background()
	sess, conn, err2 := Conn(ctx)
	if err2 != nil {
		return time.Time{}, err2
	}
	defer conn.Release()
	ts, err_ := sess.AddInsKline(ctx, add)
	if err_ != nil {
		return time.Time{}, NewDbErr(core.ErrDbExecFail, err_)
	}
	if ts.IsZero() {
		log.Debug("insert candles already claimed, skip", zap.Int32("sid", add.Sid), zap.String("tf", add.Timeframe))
		return time.Time{}, nil
	}
	return ts, nil
}

func GetKlineAggs() []*KlineAgg {
	return aggList
}

/*
CalcAdjFactors calculates adjustment factors through an adapter-owned
capability. The released banexg version has no such capability, so callers
must provide the calculation explicitly until their adapter supplies it.
*/
type AdjFactorCalculator func(*config.CmdArgs) *errs.Error

type adjFactorCapability interface {
	CalcAdjFactors(*config.CmdArgs) *errs.Error
}

func CalcAdjFactors(args *config.CmdArgs, calculators ...AdjFactorCalculator) *errs.Error {
	return CalcAdjFactorsWithExchange(args, exg.Default, calculators...)
}

// CalcAdjFactorsWithExchange runs an adapter-provided calculator without
// consulting the process-wide default exchange.
func CalcAdjFactorsWithExchange(args *config.CmdArgs, exchange banexg.BanExchange, calculators ...AdjFactorCalculator) *errs.Error {
	if args == nil {
		return errs.NewMsg(errs.CodeParamRequired, "command args are required")
	}
	if args.OutPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--out is required")
	}
	if len(calculators) > 1 {
		return errs.NewMsg(errs.CodeParamInvalid, "only one adjustment-factor calculator is allowed")
	}
	if len(calculators) == 1 {
		if calculators[0] == nil {
			return errs.NewMsg(errs.CodeParamInvalid, "adjustment-factor calculator is nil")
		}
		return calculators[0](args)
	}
	if exchange == nil {
		return errs.NewMsg(core.ErrExgNotInit, "exchange is required")
	}
	if capability, ok := exchange.(adjFactorCapability); ok && capability != nil {
		return capability.CalcAdjFactors(args)
	}
	if wrapper, ok := exchange.(*exg.BotExchange); ok && wrapper != nil && wrapper.BanExchange != nil {
		if capability, ok := wrapper.BanExchange.(adjFactorCapability); ok && capability != nil {
			return capability.CalcAdjFactors(args)
		}
	}
	exchangeID := "unknown"
	if info := exchange.Info(); info != nil && info.ID != "" {
		exchangeID = info.ID
	}
	return errs.NewMsg(errs.CodeNotImplement,
		"exchange %s does not provide adjustment-factor calculation; pass an explicit calculator or use an adapter with CalcAdjFactors capability",
		exchangeID)
}
