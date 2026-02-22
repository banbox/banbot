package orm

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

var (
	adjMap         = map[int32][]*AdjInfo{} // Cache of the target's weighting factor. 标的的复权因子缓存
	amLock         = deadlock.Mutex{}
	DebugDownKLine = false
)

/*
FetchApiOHLCV
Download the K-line data of the trading pair according to the given time period.
If you need to download from the end to the beginning, you should make startMS>endMS
按给定时间段下载交易对的K线数据。
如果需要从后往前下载，应该使startMS>endMS
*/
func FetchApiOHLCV(ctx context.Context, exchange banexg.BanExchange, pair, timeFrame string, startMS, endMS int64, out chan []*banexg.Kline) *errs.Error {
	if core.NetDisable {
		return nil
	}
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	if startMS < 1000000000000 {
		panic(fmt.Sprintf("startMS should be milli seconds, cur: %v", startMS))
	}
	if startMS > endMS {
		startMS, endMS = endMS, startMS
	}
	// 交易所返回的最后一个可能是未完成bar，需要过滤掉
	endMS = utils2.AlignTfMSecs(min(endMS, btime.UTCStamp()), tfMSecs)
	fetchNum := int((endMS - startMS) / tfMSecs)
	if fetchNum == 0 {
		return nil
	}
	batchSize := int64(min(core.KBatchSize, fetchNum+5))
	if batchSize <= 0 {
		batchSize = 1
	}
	curSince := startMS
	for curSince < endMS {
		leftNum := (endMS - curSince) / tfMSecs
		if leftNum <= 0 {
			break
		}
		curSize := int(min(batchSize, leftNum))
		data, err := exchange.FetchOHLCV(pair, timeFrame, curSince, curSize, map[string]interface{}{
			banexg.ParamDebug: DebugDownKLine,
		})
		if err != nil {
			return err
		}
		retSize := len(data)
		log.Debug("fetch kline", zap.String("pair", pair), zap.String("tf", timeFrame), zap.Int("curSize", curSize), zap.Int64("since", curSince),
			zap.Int("rawSize", retSize), zap.Int("saveSize", len(data)))
		if len(data) > 0 {
			select {
			case <-ctx.Done():
				return nil
			case out <- data:
			}
		}
		curSince = nextFetchSince(curSince, curSize, tfMSecs, data)
	}
	return nil
}

func nextFetchSince(curSince int64, curSize int, tfMSecs int64, clean []*banexg.Kline) int64 {
	if len(clean) > 0 {
		nextSince := clean[len(clean)-1].Time + tfMSecs
		if nextSince <= curSince {
			nextSince = curSince + tfMSecs
		}
		return nextSince
	}
	if curSize <= 0 {
		return curSince + tfMSecs
	}
	return curSince + int64(curSize)*tfMSecs
}

/*
DownOHLCV2DB
Download K-line to database. This method should be called in a transaction, otherwise there will be errors in querying and updating related data.
下载K线到数据库，应在事务中调用此方法，否则查询更新相关数据会有错误
*/
func (q *Queries) DownOHLCV2DB(exchange banexg.BanExchange, exs *ExSymbol, timeFrame string, startMS, endMS int64,
	pBar *utils.PrgBar) (int, *errs.Error) {
	return q.downOHLCV2DB(exchange, exs, timeFrame, startMS, endMS, 2, pBar)
}

func (q *Queries) downOHLCV2DB(exchange banexg.BanExchange, exs *ExSymbol, timeFrame string, startMS, endMS int64,
	retry int, pBar *utils.PrgBar) (int, *errs.Error) {
	startMS = exs.GetValidStart(startMS)
	return downOHLCV2DBRange(q, exchange, exs, timeFrame, startMS, endMS, retry, pBar)
}

/*
downOHLCV2DBRange
This function will be used for multi-threaded downloads. A database session can only be used for one thread, so Queries cannot be passed in.
stepCB is used to update the progress. The total value is fixed at 1000 to prevent the internal download interval from being larger than the passed interval.
此函数会用于多线程下载，一个数据库会话只能用于一个线程，所以不能传入Queries
stepCB 用于更新进度，总值固定1000，避免内部下载区间大于传入区间
*/
func downOHLCV2DBRange(sess *Queries, exchange banexg.BanExchange, exs *ExSymbol, timeFrame string, startMS, endMS int64,
	retry int, pBar *utils.PrgBar) (int, *errs.Error) {
	_ = retry
	if startMS >= endMS || exs.Combined || exs.DelistMs > 0 || core.NetDisable {
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
		return 0, nil
	}
	var err *errs.Error
	if sess == nil {
		var conn *pgxpool.Conn
		sess, conn, err = Conn(nil)
		if err != nil {
			if pBar != nil {
				pBar.Add(core.StepTotal)
			}
			return 0, err
		}
		defer conn.Release()
	}
	tfSecs := utils2.TFToSecs(timeFrame)
	tfMSecs := int64(tfSecs * 1000)

	covered, err_ := PubQ().getCoveredRanges(context.Background(), exs.ID, "kline_"+timeFrame, timeFrame, startMS, endMS)
	if err_ != nil {
		return 0, NewDbErr(core.ErrDbReadFail, err_)
	}
	missing := subtractMSRanges(MSRange{Start: startMS, Stop: endMS}, covered)
	if len(missing) == 0 {
		// If metadata says covered but no actual bars exist in the target range,
		// force a full-range redownload to self-heal stale sranges records.
		probe, probeErr := sess.QueryOHLCV(exs, timeFrame, startMS, endMS, 1, false)
		if probeErr != nil {
			return 0, probeErr
		}
		if len(probe) == 0 {
			log.Warn("sranges covered but no kline rows, force redownload",
				zap.Int32("sid", exs.ID),
				zap.String("symbol", exs.Symbol),
				zap.String("tf", timeFrame),
				zap.Int64("start", startMS),
				zap.Int64("end", endMS),
			)
			missing = []MSRange{{Start: startMS, Stop: endMS}}
		}
	}
	if len(missing) == 0 {
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
		return 0, nil
	}

	var (
		totalNum int
		curStart int64
		curEnd   int64
	)
	chanDown := make(chan *core.DownRange, len(missing))
	for _, r := range missing {
		chanDown <- &core.DownRange{Start: r.Start, End: r.Stop}
		totalNum += int((r.Stop-r.Start)/1000) / tfSecs
		if curStart == 0 || r.Start < curStart {
			curStart = r.Start
		}
		if r.Stop > curEnd {
			curEnd = r.Stop
		}
	}
	close(chanDown)
	insId, err := AddInsJob(AddInsKlineParams{
		Sid:       exs.ID,
		Timeframe: timeFrame,
		StartMs:   curStart,
		StopMs:    curEnd,
	})
	if err != nil || insId == 0 {
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
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

	if pBar == nil && totalNum > 10000 {
		pBar = utils.NewPrgBar(core.StepTotal, exs.Symbol)
		defer pBar.Close()
	}
	var bar *utils.PrgBarJob
	if pBar != nil {
		bar = pBar.NewJob(totalNum)
		defer bar.Done()
	}

	chanKline := make(chan []*banexg.Kline, 1000)
	var wg sync.WaitGroup
	wg.Add(2)
	var (
		outErr   *errs.Error
		outErrMu sync.Mutex
	)
	setOutErr := func(err *errs.Error) {
		if err == nil {
			return
		}
		outErrMu.Lock()
		if outErr == nil {
			outErr = err
		}
		outErrMu.Unlock()
	}
	saveNum := 0
	succDown := make([]MSRange, 0, len(missing))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// downloader goroutine
	go func() {
		defer func() {
			wg.Done()
			close(chanKline)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case job, ok := <-chanDown:
				if !ok {
					return
				}
				start, stop := job.Start, job.End
				if job.Reverse {
					start, stop = job.End, job.Start
				}
				err := FetchApiOHLCV(ctx, exchange, exs.Symbol, timeFrame, start, stop, chanKline)
				if err != nil {
					setOutErr(err)
					cancel()
					return
				}
				if ctx.Err() != nil {
					return
				}
				succDown = append(succDown, MSRange{Start: min(start, stop), Stop: max(start, stop)})
			}
		}
	}()
	// saver goroutine (db session is single-threaded)
	realStart, realEnd := int64(0), int64(0)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case batch, ok := <-chanKline:
				if !ok {
					return
				}
				num, err := sess.InsertKLines(timeFrame, exs.ID, batch)
				if err != nil {
					setOutErr(err)
					cancel()
					return
				}
				curNum := int(num)
				if bar != nil {
					bar.Add(curNum)
				}
				saveNum += curNum
				if realStart == 0 || batch[0].Time < realStart {
					realStart = batch[0].Time
				}
				last := batch[len(batch)-1].Time
				if last > realEnd {
					realEnd = last
				}
			}
		}
	}()

	wg.Wait()

	// Unfinished bar (best-effort).
	curMS := btime.UTCStamp()
	curAlignMS := utils2.AlignTfMSecs(curMS, tfMSecs)
	if endMS > curAlignMS {
		data, fetchErr := exchange.FetchOHLCV(exs.Symbol, timeFrame, curAlignMS, 1, nil)
		if fetchErr != nil {
			log.Warn("fetch unfinish bar fail", zap.Error(fetchErr))
		}
		if len(data) > 0 {
			if err := PubQ().SetUnfinish(exs.ID, timeFrame, curMS, data[0]); err != nil {
				log.Warn("set unfinish fail", zap.Int32("sid", exs.ID), zap.String("tf", timeFrame), zap.Error(err))
			}
		}
	}

	if saveNum > 0 {
		updErr := sess.UpdateKRange(exs, timeFrame, realStart, realEnd+tfMSecs, nil, true, true)
		if updErr != nil {
			if outErr == nil {
				outErr = updErr
			} else {
				log.Warn("UpdateKRange fail", zap.Int32("sid", exs.ID), zap.String("tf", timeFrame),
					zap.String("err", updErr.Short()))
			}
		}
	}
	return saveNum, outErr
}

/*
AutoFetchOHLCV

	Get K-line data for a given trading pair, a given time dimension, and a given range.
	Try to read from local first, download from the exchange if it doesn't exist, and then return.
	获取给定交易对，给定时间维度，给定范围的K线数据。
	先尝试从本地读取，不存在时从交易所下载，然后返回。
*/
func AutoFetchOHLCV(exchange banexg.BanExchange, exs *ExSymbol, timeFrame string, startMS, endMS int64,
	limit int, withUnFinish bool, pBar *utils.PrgBar) ([]*AdjInfo, []*banexg.Kline, *errs.Error) {
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
		// DownOHLCV2DB 内部已处理stepCB，这里无需处理
		return nil, nil, err
	}
	return sess.GetOHLCV(exs, timeFrame, startMS, endMS, limit, withUnFinish)
}

/*
GetOHLCV
Get the variety K-line, if you need to rebalance, it will be automatically reweighted
获取品种K线，如需复权自动前复权
*/
func GetOHLCV(exs *ExSymbol, timeFrame string, startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*banexg.Kline, *errs.Error) {
	retry, maxRetry := 0, 3
	for retry < maxRetry {
		sess, conn, err := Conn(nil)
		if err != nil {
			return nil, nil, err
		}
		adjs, klines, err := sess.GetOHLCV(exs, timeFrame, startMS, endMS, limit, withUnFinish)
		conn.Release()
		if err != nil && err.Code == core.ErrDbConnFail && retry < maxRetry+1 {
			// 连接被断开，等待一会，重试
			retry += 1
			core.Sleep(time.Millisecond * 1000 * time.Duration(retry))
			continue
		}
		return adjs, klines, err
	}
	return nil, nil, errs.NewMsg(core.ErrDbReadFail, "max retry exceed")
}

/*
GetOHLCV
Obtain the variety K-line, return the unweighted K-line and the weighting factor, and the caller can call ApplyAdj to re-weight
获取品种K线，返回未复权K线和复权因子，调用方可调用ApplyAdj进行复权
*/
func (q *Queries) GetOHLCV(exs *ExSymbol, timeFrame string, startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*banexg.Kline, *errs.Error) {
	if exs.Exchange == "china" && exs.Market != banexg.MarketSpot {
		// China's non stock market may include futures, options, funds
		// 国内非股票，可能是：期货、期权、基金、、、
		parts := utils2.SplitParts(exs.Symbol)
		if len(parts) >= 2 {
			p2val := parts[1].Val
			if p2val == "888" {
				// Futures 888 is the main continuous contract, while 000 is the index contract
				// 期货888是主力连续合约，000是指数合约
				adjs, err := GetAdjs(exs.ID)
				if err != nil {
					return nil, nil, err
				}
				klines, err := q.GetAdjOHLCV(adjs, timeFrame, startMS, endMS, limit, withUnFinish)
				return adjs, klines, err
			}
		}
	}
	klines, err := q.QueryOHLCV(exs, timeFrame, startMS, endMS, limit, withUnFinish)
	return nil, klines, err
}

func GetAdjs(sid int32) ([]*AdjInfo, *errs.Error) {
	amLock.Lock()
	cache, hasOld := adjMap[sid]
	amLock.Unlock()
	if hasOld {
		return cache, nil
	}
	ctx := context.Background()
	rows, err_ := PubQ().GetAdjFactors(ctx, sid)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	// FACS has recorded the deadline in ascending order of time, from back to front
	// facs已按时间升序，从后往前，记录截止时间
	adjs := make([]*AdjInfo, 0, len(rows))
	curEnd := btime.UTCStamp()
	for i := len(rows) - 1; i >= 0; i-- {
		f := rows[i]
		curSid := f.SubID
		if curSid == 0 {
			curSid = sid
		}
		adjs = append(adjs, &AdjInfo{
			ExSymbol: GetSymbolByID(curSid),
			Factor:   f.Factor,
			StartMS:  f.StartMs,
			StopMS:   curEnd,
		})
		curEnd = f.StartMs
	}
	utils.ReverseArr(adjs)
	amLock.Lock()
	adjMap[sid] = adjs
	amLock.Unlock()
	return adjs, nil
}

/*
GetAdjOHLCV
Obtain K-line and weighted information (returns K-line that has not been weighted yet, needs to call ApplyAdj for weighted)
获取K线和复权信息（返回的是尚未复权的K线，需调用ApplyAdj复权）
*/
func (q *Queries) GetAdjOHLCV(adjs []*AdjInfo, timeFrame string, startMS, endMS int64, limit int, withUnFinish bool) ([]*banexg.Kline, *errs.Error) {
	if len(adjs) == 0 {
		return nil, nil
	}
	if endMS == 0 {
		endMS = btime.UTCStamp()
	}
	revRead := startMS == 0 && limit > 0
	var result []*banexg.Kline
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
			// Read in reverse order, from back to front, starting from 0
			// 逆序读取，从后往前，开始置为0
			start = 0
		}
		klines, err := q.QueryOHLCV(f.ExSymbol, timeFrame, start, stop, limit, withUnFinish)
		if err != nil {
			return nil, err
		}
		if revRead {
			result = append(klines, result...)
		} else {
			result = append(result, klines...)
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

/*
ApplyAdj Calculate the K-line after adjustment 计算复权后K线
adjs Must be in ascending order 必须已升序
cutEnd Maximum end time of interception 截取的最大结束时间
adj Type of adjustment of Rights 复权类型
limit 返回数量
*/
func ApplyAdj(adjs []*AdjInfo, klines []*banexg.Kline, adj int, cutEnd int64, limit int) []*banexg.Kline {
	// When adjs is empty, it should not be returned directly, as klines may need to be trimmed
	// adjs为空时不应直接返回，因klines可能需要裁剪
	if len(klines) == 0 {
		return klines
	}
	doCutKlineEnd := true
	if cutEnd == 0 {
		cutEnd = klines[len(klines)-1].Time + 1000
		doCutKlineEnd = false
	}
	// Ignore tail out of range adjs
	// 忽略尾部超出范围的adjs
	match := false
	for i := len(adjs) - 1; i >= 0; i-- {
		if adjs[i].StartMS < cutEnd {
			adjs = adjs[:i+1]
			match = true
			break
		}
	}
	if !match {
		adjs = nil
	}
	if doCutKlineEnd {
		// Ignore K-lines with tails outside the range
		// 忽略尾部超出范围的K线
		match = false
		for i := len(klines) - 1; i >= 0; i-- {
			if klines[i].Time <= cutEnd {
				klines = klines[:i+1]
				match = true
				break
			}
		}
		if !match {
			return klines
		}
	}
	if limit > 0 && len(klines) > limit {
		klines = klines[len(klines)-limit:]
	}
	// Filter irrelevant items before adfs
	// 过滤adjs前面的无关项
	if len(adjs) > 0 {
		startMS := klines[0].Time
		match = false
		for i := len(adjs) - 1; i >= 0; i-- {
			if adjs[i].StartMS <= startMS {
				adjs = adjs[i:]
				match = true
				break
			}
		}
		if !match {
			return klines
		}
	} else {
		return klines
	}
	// factor(i) = newClose(i-1) / oldClose(i-1)
	if adj == core.AdjBehind {
		// Post weighted, from front to back, multiply the weighted factors cumulatively as factors for the new date; New data divided by factor
		// 后复权，从前往后，复权因子累乘，作为新日期的因子；新数据除以因子
		lastFac := float64(1)
		for _, f := range adjs {
			f.CumFactor = lastFac * f.Factor
			lastFac = f.CumFactor
		}
	} else if adj == core.AdjFront {
		// Forward weighted, from back to front, the cumulative multiplication of weighted factors serves as the factor for the old date; Multiply old data by factor
		// 前复权，从后往前，复权因子累乘，作为旧日期的因子；旧数据乘以因子
		lastFac := float64(1)
		for i := len(adjs) - 1; i >= 0; i-- {
			f := adjs[i]
			f.CumFactor = lastFac
			lastFac *= f.Factor
		}
	}
	result := make([]*banexg.Kline, 0, len(klines))
	cache := make([]*banexg.Kline, 0, len(klines)/3)
	var item = adjs[0]
	var ai = 1
	saveBatch := func() {
		if len(cache) == 0 {
			return
		}
		cache = item.Apply(cache, adj)
		result = append(result, cache...)
		cache = make([]*banexg.Kline, 0, len(klines)/3)
	}
	for i, k := range klines {
		if k.Time >= item.StopMS {
			saveBatch()
			if ai+1 < len(adjs) {
				ai += 1
				item = adjs[ai]
			} else {
				item = nil
				cache = klines[i:]
				break
			}
		}
		cache = append(cache, k)
	}
	saveBatch()
	return result
}

/*
BulkDownOHLCV
Batch simultaneous download of K-line
批量同时下载K线
*/
func BulkDownOHLCV(exchange banexg.BanExchange, exsList map[int32]*ExSymbol, timeFrame string, startMS, endMS int64, limit int, prg utils.PrgCB) *errs.Error {
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	startMS, endMS = parseDownArgs(tfMSecs, startMS, endMS, limit, false)
	downTF, err := GetDownTF(timeFrame)
	if err != nil {
		return err
	}
	barNum := int((endMS - startMS) / tfMSecs)
	startText := btime.ToDateStr(startMS, "")
	endText := btime.ToDateStr(endMS, "")
	var pBar *utils.PrgBar
	if barNum*len(exsList) > 99000 || len(exsList) > 10 || prg != nil {
		log.Info(fmt.Sprintf("bulk down %s %d pairs %s-%s, len:%d\n", timeFrame, len(exsList), startText, endText, barNum))
		pBar = utils.NewPrgBar(len(exsList)*core.StepTotal, "BulkDown")
		defer pBar.Close()
		if prg != nil {
			pBar.PrgCbs = append(pBar.PrgCbs, prg)
		}
	}
	sidList := utils.KeysOfMap(exsList)
	return utils.ParallelRun(sidList, core.ConcurNum, func(_ int, i int32) *errs.Error {
		exs, _ := exsList[i]
		if exs.DelistMs > 0 {
			return nil
		}
		_, dlErr := downOHLCV2DBRange(nil, exchange, exs, downTF, startMS, endMS, 2, pBar)
		return dlErr
	})
}

/*
FastBulkOHLCV
Quickly obtain K-lines in bulk. Download all the required currencies first, then perform batch queries and group returns.
Suitable for situations where there are multiple currencies, the required start and end times are consistent, and most of them have already been downloaded.
For combination varieties, return the unweighted candlestick and the weighting factor, and call ApplyAdj for weighting as needed
快速批量获取K线。先下载所有需要的币种，然后批量查询再分组返回。

	适用于币种较多，且需要的开始结束时间一致，且大部分已下载的情况。
	对于组合品种，返回未复权的K线，和复权因子，自行根据需要调用ApplyAdj复权
*/
func FastBulkOHLCV(exchange banexg.BanExchange, symbols []string, timeFrame string,
	startMS, endMS int64, limit int, handler func(string, string, []*banexg.Kline, []*AdjInfo)) *errs.Error {
	var exsMap, err = MapExSymbols(exchange, symbols)
	if len(exsMap) == 0 {
		return err
	}
	if err != nil {
		log.Error("resolve pairs fail", zap.String("err", err.Short()))
	}
	sess, conn, err := Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()
	err = EnsureListDates(sess, exchange, exsMap, nil)
	if err != nil {
		return err
	}
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	exInfo := exchange.Info()
	if exchange.HasApi(banexg.ApiFetchOHLCV, exInfo.MarketType) {
		retErr := BulkDownOHLCV(exchange, exsMap, timeFrame, startMS, endMS, limit, nil)
		if retErr != nil {
			return retErr
		}
	}
	if handler == nil {
		return nil
	}
	sugStartMS, sugEndMS := parseDownArgs(tfMSecs, startMS, endMS, limit, false)
	itemNum := (sugEndMS - sugStartMS) / tfMSecs
	leftArr := make([]int32, 0, len(exsMap))
	if itemNum < int64(core.KBatchSize) {
		rawMap := make(map[int32]*ExSymbol)
		for sid, exs := range exsMap {
			if exs.Combined {
				leftArr = append(leftArr, sid)
			} else {
				rawMap[sid] = exs
			}
		}
		if len(rawMap) > 0 {
			bulkHandler := func(sid int32, klines []*banexg.Kline) {
				exs, ok := exsMap[sid]
				if !ok {
					return
				}
				handler(exs.Symbol, timeFrame, klines, nil)
			}
			err = sess.QueryOHLCVBatch(rawMap, timeFrame, startMS, endMS, limit, bulkHandler)
			if err != nil {
				return err
			}
		}
	} else {
		leftArr = utils.KeysOfMap(exsMap)
	}
	// 单个数量过多，逐个查询
	for _, sid := range leftArr {
		exs := exsMap[sid]
		adjs, kline, err := sess.GetOHLCV(exs, timeFrame, startMS, endMS, limit, false)
		if err != nil {
			return err
		}
		handler(exs.Symbol, timeFrame, kline, adjs)
	}
	return nil
}

func MapExSymbols(exchange banexg.BanExchange, symbols []string) (map[int32]*ExSymbol, *errs.Error) {
	var exsMap = make(map[int32]*ExSymbol)
	var fails = make(map[string]*errs.Error)
	for _, pair := range symbols {
		exs, err := GetExSymbol(exchange, pair)
		if err != nil {
			fails[pair] = err
		} else {
			exsMap[exs.ID] = exs
		}
	}
	var lastCode int
	var codeMap = make(map[string][]string)
	for pair, err := range fails {
		lastCode = err.Code
		code := err.CodeName()
		codeMap[code] = append(codeMap[code], pair)
	}
	var b strings.Builder
	for code, names := range codeMap {
		b.WriteString(fmt.Sprintf("%s: %s\n", code, strings.Join(names, ", ")))
	}
	var err *errs.Error
	if lastCode != 0 {
		err = errs.NewMsg(lastCode, "fail pairs: \n%s", b.String())
	}
	return exsMap, err
}

func parseDownArgs(tfMSecs int64, startMS, endMS int64, limit int, withUnFinish bool) (int64, int64) {
	if startMS > 0 && startMS != core.MSMinStamp {
		fixStartMS := utils2.AlignTfMSecs(startMS, tfMSecs)
		if startMS > fixStartMS {
			startMS = fixStartMS + tfMSecs
		}
		if limit > 0 && endMS == 0 {
			endMS = startMS + tfMSecs*int64(limit)
		}
	}
	if endMS == 0 {
		endMS = btime.TimeMS()
	}
	alignEndMS := utils2.AlignTfMSecs(endMS, tfMSecs)
	if withUnFinish && endMS%tfMSecs > 0 {
		alignEndMS += tfMSecs
	}
	endMS = alignEndMS
	if startMS == 0 && limit > 0 {
		startMS = endMS - tfMSecs*int64(limit)
	}
	return startMS, endMS
}

func buildCalendarsSQL(fields string, startMS, stopMS int64) (string, []any) {
	var b strings.Builder
	b.WriteString("select ")
	b.WriteString(fields)
	b.WriteString(" from calendars where name=? ")
	args := make([]any, 0, 3)
	args = append(args, nil)
	if startMS > 0 {
		b.WriteString("and stop_ms > ? ")
		args = append(args, startMS)
	}
	if stopMS > 0 {
		b.WriteString("and start_ms < ? ")
		args = append(args, stopMS)
	}
	b.WriteString("order by start_ms")
	return b.String(), args
}

func (q *PubQueries) GetCalendars(name string, startMS, stopMS int64) ([][2]int64, *errs.Error) {
	db, err2 := BanPubConn(false)
	if err2 != nil {
		return nil, err2
	}
	defer db.Close()
	sqlText, args := buildCalendarsSQL("start_ms,stop_ms", startMS, stopMS)
	args[0] = name
	rows, err_ := db.QueryContext(context.Background(), sqlText, args...)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	result := make([][2]int64, 0)
	for rows.Next() {
		var start, stop int64
		if err := rows.Scan(&start, &stop); err != nil {
			return result, NewDbErr(core.ErrDbReadFail, err)
		}
		result = append(result, [2]int64{start, stop})
	}
	return result, nil
}

func (q *PubQueries) SetCalendars(name string, items [][2]int64) *errs.Error {
	if len(items) == 0 {
		return nil
	}
	startMS, stopMS := items[0][0], items[len(items)-1][1]
	db, err2 := BanPubConn(true)
	if err2 != nil {
		return err2
	}
	defer db.Close()

	ctx := context.Background()
	// 使用 IMMEDIATE 模式，在事务开始时就获取写锁，避免后续锁升级冲突
	tx, err_ := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err_ != nil {
		return NewDbErr(core.ErrDbConnFail, err_)
	}
	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()

	sqlText, args := buildCalendarsSQL("id,start_ms,stop_ms", startMS, stopMS)
	args[0] = name
	rows, err_ := tx.QueryContext(ctx, sqlText, args...)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()

	olds := make([]*Calendar, 0)
	for rows.Next() {
		var cal = &Calendar{}
		if err := rows.Scan(&cal.ID, &cal.StartMs, &cal.StopMs); err != nil {
			return NewDbErr(core.ErrDbReadFail, err)
		}
		olds = append(olds, cal)
	}
	if err := rows.Err(); err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}

	if len(olds) > 0 {
		items[0][0] = olds[0].StartMs
		items[len(items)-1][1] = olds[len(olds)-1].StopMs
		ids := make([]string, len(olds))
		for i, o := range olds {
			ids[i] = strconv.Itoa(int(o.ID))
		}
		delSQL := fmt.Sprintf("delete from calendars where id in (%s)", strings.Join(ids, ","))
		if _, err_ := tx.ExecContext(ctx, delSQL); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}

	stmt, err_ := tx.PrepareContext(ctx, `insert into calendars (name,start_ms,stop_ms) values (?,?,?)`)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	defer stmt.Close()
	for _, tu := range items {
		if _, err := stmt.ExecContext(ctx, name, tu[0], tu[1]); err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	commit = true
	return nil
}

/*
GetExSHoles
Retrieve all non trading time ranges for the specified Sid within a certain time period.
For the 365 * 24 coin circle, it will not stop and return empty
获取指定Sid在某个时间段内，所有非交易时间范围。
对于币圈365*24不休，返回空
*/
func GetExSHoles(exchange banexg.BanExchange, exs *ExSymbol, start, stop int64, full bool) ([][2]int64, *errs.Error) {
	exInfo := exchange.Info()
	if exInfo.FullDay && exInfo.NoHoliday {
		// 365天全年无休，且24小时可交易，不存在休息时间段
		return nil, nil
	}
	mar, err := exchange.GetMarket(exs.Symbol)
	if err != nil {
		return nil, err
	}
	var dtList [][2]int64
	if full {
		// 不使用交易日过滤
		dayMSecs := int64(utils2.TFToSecs("1d") * 1000)
		curTime := utils2.AlignTfMSecs(start, dayMSecs)
		for curTime < stop {
			curEnd := curTime + dayMSecs
			dtList = append(dtList, [2]int64{curTime, curEnd})
			curTime = curEnd
		}
	} else {
		// 获取交易日
		dtList, err = PubQ().GetCalendars(mar.ExgReal, start, stop)
		if err != nil {
			return nil, err
		}
		if len(dtList) == 0 {
			// 给定时间段没有可交易日。整个作为hole
			return [][2]int64{{start, stop}}, nil
		}
	}
	times := mar.GetTradeTimes()
	if len(times) == 0 {
		if !exInfo.FullDay {
			log.Warn("day_ranges/night_ranges invalid", zap.String("id", mar.ID))
		}
		times = [][2]int64{{0, 24 * 60 * 60000}}
	}
	res := make([][2]int64, 0)
	lastStop := int64(0)
	if times[0][0] > 0 {
		lastStop = dtList[0][0]
	}
	for _, dt := range dtList {
		for _, rg := range times {
			if lastStop > 0 {
				res = append(res, [2]int64{lastStop, dt[0] + rg[0]})
			}
			lastStop = dt[0] + rg[1]
		}
	}
	validStop := dtList[len(dtList)-1][0] + times[len(times)-1][1]
	if validStop < stop {
		res = append(res, [2]int64{validStop, stop})
	}
	return res, nil
}

func (q *Queries) DelKData(exsList []*ExSymbol, tfList []string, startMS, endMS int64) *errs.Error {
	pq := PubQ()
	for _, tf := range tfList {
		for _, exs := range exsList {
			if err := pq.DelKInfo(exs.ID, tf); err != nil {
				return err
			}
			if endMS == 0 {
				if err := pq.DelKLineUn(exs.ID, tf); err != nil {
					return err
				}
			}
		}
	}
	for _, tf := range tfList {
		err := q.DelKLines(tf)
		if err != nil {
			return err
		}
	}
	for _, exs := range exsList {
		if err := pq.DelFactors(exs.ID, startMS, endMS); err != nil {
			return err
		}
	}
	return nil
}
