package data

import (
	"context"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

type periodSta struct {
	stamps map[int32]int64
	lock   deadlock.Mutex
	msecs  int64
}

func newPeriodSta(tf string) *periodSta {
	msecs := int64(utils2.TFToSecs(tf) * 1000)
	return &periodSta{
		stamps: make(map[int32]int64),
		msecs:  msecs,
	}
}

// 返回指定周期对齐时间戳，此周期已入库的最新bar时间戳
func (p *periodSta) alignAndLast(sess *orm.Queries, sid int32, tf string, curMS int64) (int64, int64) {
	p.lock.Lock()
	// 上一个小时对齐时间戳
	lastMS, _ := p.stamps[sid]
	if lastMS == 0 {
		_, kinfoEnd := sess.GetKlineRange(sid, tf)
		if kinfoEnd > 0 {
			lastMS = kinfoEnd - p.msecs
		}
	}
	hourAlign := utils2.AlignTfMSecs(curMS, p.msecs)
	if lastMS == 0 {
		lastMS = hourAlign - p.msecs
	}
	if hourAlign > lastMS {
		p.stamps[sid] = hourAlign
	}
	p.lock.Unlock()
	return hourAlign, lastMS
}

func (p *periodSta) reset(sid int32, timeMS int64) {
	p.lock.Lock()
	p.stamps[sid] = timeMS
	p.lock.Unlock()
}

func trySaveSeries(job *SaveSeries, tfSecs int, mntSta *periodSta, hourSta *periodSta) *errs.Error {
	return trySaveSeriesWithDeps(nil, nil, job, tfSecs, mntSta, hourSta)
}

func trySaveSeriesWithSymbolState(symbols *orm.SymbolState, job *SaveSeries, tfSecs int,
	mntSta *periodSta, hourSta *periodSta) *errs.Error {
	return trySaveSeriesWithDeps(nil, symbols, job, tfSecs, mntSta, hourSta)
}

func trySaveSeriesWithRuntimeDeps(deps *RuntimeDeps, job *SaveSeries, tfSecs int,
	mntSta *periodSta, hourSta *periodSta) *errs.Error {
	if deps == nil {
		return trySaveSeries(job, tfSecs, mntSta, hourSta)
	}
	return trySaveSeriesWithDeps(deps, deps.Symbols, job, tfSecs, mntSta, hourSta)
}

func trySaveSeriesWithDeps(deps *RuntimeDeps, symbols *orm.SymbolState, job *SaveSeries, tfSecs int,
	mntSta *periodSta, hourSta *periodSta) *errs.Error {
	if job == nil || len(job.Rows) == 0 {
		return errs.NewMsg(core.ErrInvalidBars, "series save job has no rows")
	}
	for i, row := range job.Rows {
		if row == nil {
			return errs.NewMsg(core.ErrInvalidBars, "series save job row %d is nil", i)
		}
	}
	if mntSta == nil {
		return errs.NewMsg(core.ErrRunTime, "series save minute state is required")
	}
	sid := job.Sid
	if sid == 0 {
		for _, row := range job.Rows {
			if row.Sid != 0 {
				sid = row.Sid
				break
			}
			if row.ExSymbol != nil && row.ExSymbol.ID != 0 {
				sid = row.ExSymbol.ID
				break
			}
		}
	}
	if sid == 0 {
		return errs.NewMsg(core.ErrInvalidSymbol, "series save symbol id is required")
	}
	if deps != nil {
		if err := deps.context().Err(); err != nil {
			return errs.New(errs.CodeCancel, err)
		}
	}
	exs := resolveSaveSeriesSymbolWithDeps(deps, symbols, sid, job.Rows)
	if exs == nil {
		return errs.NewMsg(core.ErrInvalidSymbol, "symbol id %d not found", sid)
	}
	ctx := context.Background()
	if deps != nil {
		ctx = deps.context()
	}
	sess, conn, err := orm.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	addRows := job.Rows
	endMS := addRows[len(addRows)-1].EndMS
	savedNewBars := false
	if tfSecs < 60 {
		// 最小保存1m级别k线
		mntAlign, prevMS := mntSta.alignAndLast(sess, sid, "1m", endMS)
		if mntAlign <= prevMS {
			// 未出现新的1m完成k线
			return nil
		}
		var newEndMS int64
		newEndMS, _, err = ensureSeriesToWithDeps(deps, symbols, sess, sid, "1m", prevMS, mntAlign)
		if err != nil {
			mntSta.reset(sid, newEndMS)
			log.Error("down kline 1m fail", zap.Int32("sid", sid), zap.Error(err))
			return err
		} else if newEndMS < mntAlign {
			log.Warn("down kline 1m insufficient", zap.Int32("sid", sid),
				zap.Int64("exp", mntAlign), zap.Int64("end", newEndMS))
		}
		savedNewBars = newEndMS > prevMS
	} else {
		// 1m级别，可直接入库
		expEndMS := addRows[0].TimeMS
		var nextMS int64
		mntAlign, prevMS := mntSta.alignAndLast(sess, sid, "1m", expEndMS)
		if mntAlign > prevMS {
			// 有缺口，需要先下载缺失部分
			nextMS, _, err = ensureSeriesToWithDeps(deps, symbols, sess, sid, job.TimeFrame, prevMS, expEndMS)
			if nextMS < expEndMS {
				log.Warn("fetch lack 1m bad", zap.Int32("sid", sid), zap.Int64("end", endMS),
					zap.Int64("expEnd", expEndMS))
			}
		}
		if nextMS > expEndMS {
			// 待插入的k线头部有冗余
			var cutIdx = 0
			for i, row := range addRows {
				if row.TimeMS < nextMS {
					cutIdx = i + 1
				} else {
					break
				}
			}
			addRows = addRows[cutIdx:]
		}
		if err == nil && len(addRows) > 0 {
			_, err = sess.InsertSeries(job.TimeFrame, exs, addRows, true)
			if err == nil {
				savedNewBars = true
				mntSta.reset(sid, endMS)
			}
		}
	}
	if err == nil && savedNewBars {
		if hourSta == nil {
			return errs.NewMsg(core.ErrRunTime, "series save hour state is required")
		}
		// 下载1h及以上周期K线数据
		hourAlign, lastMS := hourSta.alignAndLast(sess, sid, "1h", endMS)
		if hourAlign > lastMS {
			_, _, err = ensureSeriesToWithDeps(deps, symbols, sess, sid, "1h", lastMS, hourAlign)
		}
	}
	if err != nil {
		log.Error("consumeSeriesWriteQ: fail", zap.Int32("sid", sid), zap.Error(err))
		return err
	}
	log.Debug("save series ok", zap.Int32("sid", sid), zap.Int("num", len(job.Rows)))
	return nil
}

func resolveSaveSeriesSymbol(symbols *orm.SymbolState, sid int32, rows []*orm.DataSeries) *orm.ExSymbol {
	if symbols != nil {
		return symbols.GetSymbolByID(sid)
	}
	for _, row := range rows {
		if row == nil || row.ExSymbol == nil {
			continue
		}
		if row.Sid == sid || row.ExSymbol.ID == sid {
			return row.ExSymbol
		}
	}
	return orm.GetSymbolByID(sid)
}

func resolveSaveSeriesSymbolWithDeps(deps *RuntimeDeps, symbols *orm.SymbolState, sid int32, rows []*orm.DataSeries) *orm.ExSymbol {
	if deps != nil && symbols == nil {
		return nil
	}
	return resolveSaveSeriesSymbol(symbols, sid, rows)
}

func ensureSeriesTo(sess *orm.Queries, sid int32, tf string, oldEndMS, toEndMS int64) (int64, *orm.DataSeries, *errs.Error) {
	return ensureSeriesToWithDeps(nil, nil, sess, sid, tf, oldEndMS, toEndMS)
}

func ensureSeriesToWithSymbolState(sess *orm.Queries, symbols *orm.SymbolState, sid int32, tf string,
	oldEndMS, toEndMS int64) (int64, *orm.DataSeries, *errs.Error) {
	return ensureSeriesToWithDeps(nil, symbols, sess, sid, tf, oldEndMS, toEndMS)
}

func ensureSeriesToWithRuntimeDeps(deps *RuntimeDeps, sess *orm.Queries, sid int32, tf string,
	oldEndMS, toEndMS int64) (int64, *orm.DataSeries, *errs.Error) {
	if deps == nil {
		return ensureSeriesTo(sess, sid, tf, oldEndMS, toEndMS)
	}
	return ensureSeriesToWithDeps(deps, deps.Symbols, sess, sid, tf, oldEndMS, toEndMS)
}

func ensureSeriesToWithDeps(deps *RuntimeDeps, symbols *orm.SymbolState, sess *orm.Queries, sid int32, tf string,
	oldEndMS, toEndMS int64) (int64, *orm.DataSeries, *errs.Error) {
	if deps != nil {
		if err := deps.context().Err(); err != nil {
			return oldEndMS, nil, errs.New(errs.CodeCancel, err)
		}
		if symbols == nil {
			return oldEndMS, nil, errs.NewMsg(core.ErrInvalidSymbol, "symbol id %d not found in runtime symbol state", sid)
		}
	}
	var exs *orm.ExSymbol
	if symbols != nil {
		exs = symbols.GetSymbolByID(sid)
		if exs == nil {
			return oldEndMS, nil, errs.NewMsg(core.ErrInvalidSymbol, "symbol id %d not found", sid)
		}
	}
	if oldEndMS == 0 {
		_, oldEndMS = sess.GetKlineRange(sid, tf)
	}

	var err *errs.Error
	if oldEndMS == 0 || toEndMS <= oldEndMS {
		// The new coin has no historical data, or the current bar and the inserted data are continuous, and the subsequent new bar can be directly inserted
		// 新的币无历史数据、或当前bar和已插入数据连续，直接插入后续新bar即可
		return oldEndMS, nil, nil
	}
	if exs == nil {
		exs = orm.GetSymbolByID(sid)
	}
	if exs == nil {
		return oldEndMS, nil, errs.NewMsg(core.ErrInvalidSymbol, "symbol id %d not found", sid)
	}
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	tryCount := 0
	var exchange banexg.BanExchange
	if deps == nil {
		exchange, err = exg.GetWith(exs.Exchange, exs.Market, "")
	} else {
		exchange = deps.exchange()
		if exchange == nil {
			err = errs.NewMsg(core.ErrBadConfig, "runtime exchange is required")
		}
	}
	if err != nil {
		return oldEndMS, nil, err
	}
	var newEndMS = oldEndMS
	var saveNum int
	if tf == "1h" && deps == nil {
		orm.DebugDownKLine = true
		defer func() {
			orm.DebugDownKLine = false
		}()
	}
	var last *orm.DataSeries
	for tryCount <= 5 {
		if deps != nil {
			if err := deps.context().Err(); err != nil {
				return newEndMS, last, errs.New(errs.CodeCancel, err)
			}
		}
		tryCount += 1
		saveNum, err = sess.DownOHLCV2DB(exchange, exs, tf, oldEndMS, toEndMS, nil)
		if err != nil {
			_, oldEndMS = sess.GetKlineRange(sid, tf)
			return oldEndMS, nil, err
		}
		saveRows, err := sess.QuerySeries(exs, tf, 0, 0, 1, false)
		if err != nil {
			_, oldEndMS = sess.GetKlineRange(sid, tf)
			return oldEndMS, nil, err
		}
		var lastMS = int64(0)
		if len(saveRows) > 0 {
			last = saveRows[len(saveRows)-1]
			lastMS = last.TimeMS
			newEndMS = lastMS + tfMSecs
		}
		if newEndMS >= toEndMS {
			break
		} else {
			//If the latest bar is not obtained, wait for 2s to try again
			//如果未成功获取最新的bar，等待2s重试
			log.Warn("ensureSeriesTo not complete, wait 2s, Your system time may be inaccurate, "+
				"you may need delete ban_ntp.json in Temp directory and retry",
				zap.String("pair", exs.Symbol), zap.Int("ins", saveNum),
				zap.Int64("last", lastMS), zap.Int64("newEnd", newEndMS))
			if deps == nil {
				core.Sleep(time.Second * 2)
			} else if !deps.sleep(time.Second * 2) {
				if cancelErr := deps.context().Err(); cancelErr != nil {
					return newEndMS, last, errs.New(errs.CodeCancel, cancelErr)
				}
			}
		}
	}
	return newEndMS, last, nil
}

func DownEmitHourKlines(dp *LiveProvider, endsMap map[int32]int64) {
	DownEmitHourKlinesWithSymbolState(dp, nil, endsMap)
}

// DownEmitHourKlinesWithSymbolState uses the provider runtime's symbol index
// for both symbol lookup and historical gap filling.
func DownEmitHourKlinesWithSymbolState(dp *LiveProvider, symbols *orm.SymbolState, endsMap map[int32]int64) {
	var deps *RuntimeDeps
	if dp != nil {
		deps = dp.deps
		if symbols == nil {
			symbols = dp.symbols
		}
	}
	downEmitHourKlines(deps, symbols, dp, endsMap)
}

// DownEmitHourKlinesWithRuntimeDeps keeps the periodic live repair on one
// runtime's clock, symbols, exchange, and identity.
func DownEmitHourKlinesWithRuntimeDeps(deps *RuntimeDeps, dp *LiveProvider, endsMap map[int32]int64) {
	if deps == nil {
		DownEmitHourKlines(dp, endsMap)
		return
	}
	downEmitHourKlines(deps, deps.Symbols, dp, endsMap)
}

func downEmitHourKlines(deps *RuntimeDeps, symbols *orm.SymbolState, dp *LiveProvider, endsMap map[int32]int64) {
	if dp == nil {
		log.Error("down emit hour klines requires live provider")
		return
	}
	ctx := context.Background()
	if deps != nil {
		ctx = deps.context()
		if err := ctx.Err(); err != nil {
			log.Warn("skip down emit hour klines after runtime cancellation", zap.Error(err))
			return
		}
	}
	sess, conn, err := orm.Conn(ctx)
	if err != nil {
		log.Error("get kline Conn fail", zap.Error(err))
		return
	}
	defer conn.Release()
	var curMS int64
	if deps == nil {
		curMS = btime.UTCStamp()
	} else {
		curMS = deps.utcStamp()
	}
	curEndMS := utils2.AlignTfMSecs(curMS, 3600000)
	for exsID, lastEnd := range endsMap {
		var exs *orm.ExSymbol
		if deps != nil && symbols == nil {
			continue
		}
		if symbols == nil {
			exs = orm.GetSymbolByID(exsID)
		} else {
			exs = symbols.GetSymbolByID(exsID)
		}
		if exs == nil {
			continue
		}
		newEnd, last, err := ensureSeriesToWithDeps(deps, symbols, sess, exs.ID, "1h", lastEnd, curEndMS)
		if err != nil {
			log.Error("ensureSeriesTo 1h fail", zap.Int32("sid", exs.ID), zap.Error(err))
			continue
		}
		endsMap[exsID] = newEnd
		if last == nil {
			continue
		}
		var exchangeName, market string
		if deps == nil {
			exchangeName, market = core.ExgName, core.Market
		} else {
			exchangeName, market = deps.identity()
		}
		dp.OnDataMsg(&SeriesMsg{
			NotifySeries: NotifySeries{
				TFSecs:   3600,
				Interval: 3600,
				Rows:     []*orm.DataSeries{last},
			},
			ExgName: exchangeName,
			Market:  market,
			Pair:    exs.Symbol,
		})
	}
}

func gapRecoverySymbol(symbols *orm.SymbolState, exs *orm.ExSymbol) (*orm.ExSymbol, *errs.Error) {
	if exs == nil {
		return nil, errs.NewMsg(core.ErrInvalidSymbol, "symbol is required for gap recovery")
	}
	if symbols == nil {
		return exs, nil
	}
	resolved := symbols.GetExSymbol2(exs.Exchange, exs.Market, exs.Symbol)
	if resolved == nil {
		return nil, errs.NewMsg(core.ErrInvalidSymbol, "%s not found in runtime symbol state", exs.Symbol)
	}
	return resolved, nil
}

func (j *PairTFCache) fillLacksWithSymbolState(symbols *orm.SymbolState, exs *orm.ExSymbol, subTfSecs int, startMS, endMS int64) ([]*orm.DataSeries, *errs.Error) {
	return j.fillLacksWithDeps(nil, symbols, exs, subTfSecs, startMS, endMS)
}

func (j *PairTFCache) fillLacksWithRuntimeDeps(deps *RuntimeDeps, exs *orm.ExSymbol, subTfSecs int, startMS, endMS int64) ([]*orm.DataSeries, *errs.Error) {
	if deps == nil {
		return j.fillLacksWithSymbolState(nil, exs, subTfSecs, startMS, endMS)
	}
	return j.fillLacksWithDeps(deps, deps.Symbols, exs, subTfSecs, startMS, endMS)
}

func (j *PairTFCache) fillLacksWithDeps(deps *RuntimeDeps, symbols *orm.SymbolState, exs *orm.ExSymbol,
	subTfSecs int, startMS, endMS int64) ([]*orm.DataSeries, *errs.Error) {
	if deps != nil {
		if err := deps.context().Err(); err != nil {
			return nil, errs.New(errs.CodeCancel, err)
		}
		if symbols == nil {
			return nil, errs.NewMsg(core.ErrInvalidSymbol, "runtime symbol state is required for gap recovery")
		}
	}
	var err *errs.Error
	if symbols != nil {
		exs, err = gapRecoverySymbol(symbols, exs)
		if err != nil {
			return nil, err
		}
	}
	if j.SubNextMS == 0 || j.SubNextMS >= startMS {
		j.SubNextMS = endMS
		return nil, nil
	}
	if symbols == nil {
		exs, err = gapRecoverySymbol(nil, exs)
		if err != nil {
			return nil, err
		}
	}
	// 这里NextMS < startMS，出现了bar缺失，查询更新。
	fetchTF := utils2.SecsToTF(subTfSecs)
	tfMSecs := int64(j.TFSecs * 1000)
	bigStartMS := utils2.AlignTfMSecs(j.SubNextMS, tfMSecs)
	var preRows []*orm.DataSeries
	if deps == nil {
		_, preRows, err = autoFetchOhlcv(exs, fetchTF, bigStartMS, startMS)
	} else {
		_, preRows, err = autoFetchOhlcvWithExchange(deps.exchange(), exs, fetchTF, bigStartMS, startMS)
	}
	if err != nil {
		return nil, err
	}
	var doneBars []*orm.DataSeries
	j.WaitBar = nil
	if len(preRows) > 0 {
		fromTFMS := int64(subTfSecs * 1000)
		var oldBars []*orm.DataSeries
		if deps == nil {
			oldBars, _, err = buildAggSeriesWithSymbolState(symbols, exs, j.TimeFrame, preRows,
				tfMSecs, 0, nil, fromTFMS, j.AlignOffMS, false)
		} else {
			oldBars, _, err = buildAggSeriesWithRuntimeDeps(deps, exs, j.TimeFrame, preRows,
				tfMSecs, 0, nil, fromTFMS, j.AlignOffMS, false)
		}
		if err != nil {
			return nil, err
		}
		if len(oldBars) > 0 {
			j.WaitBar = oldBars[len(oldBars)-1]
			doneBars = oldBars[:len(oldBars)-1]
		}
	}
	j.SubNextMS = endMS
	return doneBars, nil
}

func autoFetchOhlcv(exs *orm.ExSymbol, tf string, startMS, endMS int64) ([]*orm.AdjInfo, []*orm.DataSeries, *errs.Error) {
	exchange, err := exg.GetWith(exs.Exchange, exs.Market, "")
	if err != nil {
		return nil, nil, err
	}
	return autoFetchOhlcvWithExchange(exchange, exs, tf, startMS, endMS)
}

func autoFetchOhlcvWithExchange(exchange banexg.BanExchange, exs *orm.ExSymbol, tf string, startMS, endMS int64) ([]*orm.AdjInfo, []*orm.DataSeries, *errs.Error) {
	if exs == nil {
		return nil, nil, errs.NewMsg(core.ErrInvalidSymbol, "symbol is required for OHLCV recovery")
	}
	if exchange == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "runtime exchange is required")
	}
	if !exchange.HasApi(banexg.ApiFetchOHLCV, exs.Market) {
		// Downloading K lines is currently not allowed, skip
		// 当前不允许下载K线，跳过
		return nil, nil, nil
	}
	return orm.AutoFetchSeries(exchange, exs, tf, startMS, endMS, 0, false, nil)
}
