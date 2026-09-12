package data

import (
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

type SeriesWatcher struct {
	*utils.ClientIO
	deps      *RuntimeDeps
	symbols   *orm.SymbolState
	jobs      map[string]map[string]*PairTFCache
	jobLock   sync.RWMutex
	initMsgs  []*utils.IOMsg
	OnDataMsg func(msg *SeriesMsg) // 收到爬虫K线消息
	OnTrades  func(exgName, market, pair string, trades []*banexg.Trade)
	OnDepth   func(dep *banexg.OrderBook)
}

type WatchJob struct {
	Symbol    string
	TimeFrame string
	Since     int64
}

func (j *PairTFCache) getFinishes(rows []*orm.DataSeries, lastFinish bool) []*orm.DataSeries {
	if len(rows) == 0 {
		return rows
	}
	j.WaitBar = nil
	if !lastFinish {
		j.WaitBar = rows[len(rows)-1]
		rows = rows[:len(rows)-1]
	}
	return rows
}

func NewSeriesWatcher(addr string) (*SeriesWatcher, *errs.Error) {
	return newSeriesWatcher(nil, nil, addr)
}

func NewSeriesWatcherWithSymbolState(symbols *orm.SymbolState, addr string) (*SeriesWatcher, *errs.Error) {
	return newSeriesWatcher(nil, symbols, addr)
}

// NewSeriesWatcherWithRuntimeDeps binds websocket callbacks and mutable
// market state to one runtime. A nil dependency set preserves legacy globals.
func NewSeriesWatcherWithRuntimeDeps(deps *RuntimeDeps, addr string) (*SeriesWatcher, *errs.Error) {
	if deps == nil {
		return NewSeriesWatcher(addr)
	}
	if addr == "" {
		addr = deps.spiderAddr()
	}
	return newSeriesWatcher(deps, deps.Symbols, addr)
}

func newSeriesWatcher(deps *RuntimeDeps, symbols *orm.SymbolState, addr string) (*SeriesWatcher, *errs.Error) {
	var client *utils.ClientIO
	var err *errs.Error
	if deps == nil {
		client, err = utils.NewClientIO(addr, "")
	} else {
		client, err = utils.NewClientIOWithState(deps.Core, addr, "")
	}
	if err != nil {
		return nil, err
	}
	res := &SeriesWatcher{
		ClientIO: client,
		deps:     deps,
		symbols:  symbols,
		jobs:     make(map[string]map[string]*PairTFCache),
	}
	res.Listens[core.WsSubKLine] = res.onSpiderSeries
	res.Listens["ohlcv"] = res.onSpiderSeries
	res.Listens["price"] = res.onPriceUpdate
	res.Listens[core.WsSubTrade] = res.onTrades
	res.Listens[core.WsSubDepth] = res.onBook
	res.ReInitConn = func() {
		if len(res.initMsgs) == 0 {
			return
		}
		for _, msg := range res.initMsgs {
			err = res.WriteMsg(msg)
			if err != nil {
				msgText, _ := utils2.MarshalString(msg)
				log.Error("re init conn fail", zap.String("msg", msgText))
				return
			}
		}
	}
	if deps == nil {
		go res.LoopPing(10)
	} else {
		go res.LoopPingContext(deps.context(), 10)
	}
	return res, nil
}

// RunForever delegates socket lifecycle ownership to BanConn.
func (w *SeriesWatcher) RunForever() *errs.Error {
	if w == nil || w.ClientIO == nil {
		return nil
	}
	return w.ClientIO.RunForever()
}

// Stop delegates the non-blocking stop phase to BanConn. Call Join after it.
func (w *SeriesWatcher) Stop() *errs.Error {
	if w == nil || w.ClientIO == nil {
		return nil
	}
	return w.ClientIO.Stop()
}

func (w *SeriesWatcher) Close() *errs.Error {
	return w.Stop()
}

func (w *SeriesWatcher) Join() {
	if w == nil || w.ClientIO == nil {
		return
	}
	w.ClientIO.Join()
}

func (w *SeriesWatcher) identity() (string, string) {
	if w != nil && w.deps != nil {
		return w.deps.identity()
	}
	return core.ExgName, core.Market
}

func (w *SeriesWatcher) nowMS() int64 {
	if w != nil && w.deps != nil {
		return w.deps.timeMS()
	}
	return btime.TimeMS()
}

func (w *SeriesWatcher) setPairMS(pair string, barMS, waitMS int64) {
	if w != nil && w.deps != nil {
		if state := w.deps.pairCopiedState(); state != nil {
			state.SetPairMsAt(w.nowMS(), pair, barMS, waitMS)
		}
		return
	}
	com.SetPairMs(pair, barMS, waitMS)
}

func (w *SeriesWatcher) delPairCopied(pair string) {
	if w != nil && w.deps != nil {
		if state := w.deps.pairCopiedState(); state != nil {
			state.DelPairCopieds(pair)
		}
		return
	}
	com.DelPairCopieds(pair)
}

func (w *SeriesWatcher) setPrices(data map[string]float64) {
	if w != nil && w.deps != nil {
		if state := w.deps.priceState(); state != nil {
			state.SetPricesAt(w.nowMS(), data, "")
		}
		return
	}
	com.SetPrices(data, "")
}

func (w *SeriesWatcher) setPrice(pair string, ask, bid float64) {
	if w != nil && w.deps != nil {
		if state := w.deps.priceState(); state != nil {
			state.SetPriceAt(w.nowMS(), pair, ask, bid)
		}
		return
	}
	com.SetPrice(pair, ask, bid)
}

func (w *SeriesWatcher) getOrderBook(pair string) (*banexg.OrderBook, bool) {
	if w != nil && w.deps != nil {
		if w.deps.Core == nil {
			return nil, false
		}
		return w.deps.Core.GetOdBook(pair)
	}
	return core.GetOdBook(pair)
}

func (w *SeriesWatcher) setOrderBook(pair string, book *banexg.OrderBook) {
	if w != nil && w.deps != nil {
		if w.deps.Core != nil {
			w.deps.Core.SetOdBook(pair, book)
		}
		return
	}
	core.SetOdBook(pair, book)
}

func (w *SeriesWatcher) addTfPairHits(timeFrame, pair string, count int) {
	if w != nil && w.deps != nil {
		if w.deps.Core != nil {
			w.deps.Core.AddTfPairHits(timeFrame, pair, count)
		}
		return
	}
	core.AddLegacyTfPairHits(timeFrame, pair, count)
}

func (w *SeriesWatcher) beginCallback() bool {
	if w == nil || w.deps == nil || w.deps.Callbacks == nil {
		return true
	}
	return w.deps.Callbacks.EnterCallback()
}

func (w *SeriesWatcher) endCallback() {
	if w == nil || w.deps == nil || w.deps.Callbacks == nil {
		return
	}
	w.deps.Callbacks.LeaveCallback()
}

func (w *SeriesWatcher) getPrefix(exgName, marketType, jobType string) string {
	if jobType == "price" {
		// price不按品种订阅
		return fmt.Sprintf("%s_%s_%s", jobType, exgName, marketType)
	}
	return fmt.Sprintf("%s_%s_%s_", jobType, exgName, marketType)
}

func (w *SeriesWatcher) GetJob(msgType, symbol string) *PairTFCache {
	w.jobLock.RLock()
	pairMap, ok := w.jobs[msgType]
	if ok {
		job, _ := pairMap[symbol]
		w.jobLock.RUnlock()
		return job
	}
	w.jobLock.RUnlock()
	return nil
}

func (w *SeriesWatcher) GetJobs(msgType string) map[string]*PairTFCache {
	w.jobLock.RLock()
	pairMap, _ := w.jobs[msgType]
	var res map[string]*PairTFCache
	if len(pairMap) > 0 {
		res = maps.Clone(pairMap)
	}
	w.jobLock.RUnlock()
	return res
}

// job=nil means delete
func (w *SeriesWatcher) setJob(msgType, symbol string, job *PairTFCache) {
	w.jobLock.Lock()
	pairMap, ok := w.jobs[msgType]
	if job != nil {
		if !ok {
			pairMap = make(map[string]*PairTFCache)
			w.jobs[msgType] = pairMap
		}
		pairMap[symbol] = job
	} else if len(pairMap) > 0 {
		delete(pairMap, symbol)
	}
	w.jobLock.Unlock()
}

/*
WatchJobs
Subscribe data from crawlers.
从爬虫订阅数据。ohlcv/uohlcv/trade/depth
*/
func (w *SeriesWatcher) WatchJobs(exgName, marketType, jobType string, jobs ...WatchJob) *errs.Error {
	if w.deps != nil {
		runtimeExg, runtimeMarket := w.identity()
		if exgName == "" {
			exgName = runtimeExg
		}
		if marketType == "" {
			marketType = runtimeMarket
		}
		if (runtimeExg != "" && exgName != runtimeExg) || (runtimeMarket != "" && marketType != runtimeMarket) {
			return errs.NewMsg(core.ErrBadConfig, "watch identity %s:%s does not match runtime %s:%s",
				exgName, marketType, runtimeExg, runtimeMarket)
		}
	}
	prefix := w.getPrefix(exgName, marketType, jobType)
	tags := make([]string, 0, len(jobs))
	pairs := make([]string, 0, len(jobs))
	minTfSecs := 300
	resolved := make([]*orm.ExSymbol, len(jobs))
	if w.symbols != nil {
		for i, job := range jobs {
			resolved[i] = w.symbols.GetExSymbol2(exgName, marketType, job.Symbol)
			if resolved[i] == nil {
				return errs.NewMsg(core.ErrInvalidSymbol, "%s:%s:%s not found in runtime symbol state",
					exgName, marketType, job.Symbol)
			}
		}
	} else if w.deps != nil {
		return errs.NewMsg(core.ErrInvalidSymbol, "runtime symbol state is required")
	}
	var exchange banexg.BanExchange
	var err *errs.Error
	if w.deps == nil {
		exchange, err = exg.GetWith(exgName, marketType, "")
	} else {
		exchange = w.deps.exchange()
		if exchange == nil {
			err = errs.NewMsg(core.ErrBadConfig, "runtime exchange is required")
		}
	}
	if err != nil {
		return err
	}
	for i, j := range jobs {
		job := w.GetJob(jobType, j.Symbol)
		if job != nil {
			continue
		}
		tfSecs := utils2.TFToSecs(j.TimeFrame)
		minTfSecs = min(minTfSecs, tfSecs)
		if strings.HasSuffix(prefix, "_") {
			tags = append(tags, prefix+j.Symbol)
		}
		pairs = append(pairs, j.Symbol)
		alignOffMs := int64(exg.GetAlignOffForExchange(exchange, j.Symbol, tfSecs) * 1000)
		var exs *orm.ExSymbol
		if w.symbols == nil {
			exs = orm.GetExSymbol2(exgName, marketType, j.Symbol)
		} else {
			exs = resolved[i]
		}
		w.setJob(jobType, j.Symbol, &PairTFCache{TimeFrame: j.TimeFrame, TFSecs: tfSecs, exSymbol: exs, SubNextMS: j.Since,
			AlignOffMS: alignOffMs})
		if j.Since > 0 {
			// 尽早启动延迟监听，避免spider始终未发送k线
			tfMSecs := int64(tfSecs * 1000)
			alignBarMs := utils2.AlignTfMSecsOffset(w.nowMS(), tfMSecs, alignOffMs)
			w.setPairMS(j.Symbol, alignBarMs, tfMSecs)
		}
	}
	if !strings.HasSuffix(prefix, "_") {
		tags = append(tags, prefix)
	}
	err = w.SendMsg("subscribe", tags)
	if err != nil {
		return err
	}
	if minTfSecs < 60 && banexg.IsContract(marketType) && jobType == "ohlcv" {
		//The contract market does not support OHLCV below 1M, and WS is used to listen to transaction aggregation
		//合约市场不支持1m以下的ohlcv，使用ws监听交易归集
		jobType = core.WsSubTrade
	}
	args := append([]string{exgName, marketType, jobType}, pairs...)
	return w.SendMsg("watch_pairs", args)
}

func (w *SeriesWatcher) SendMsg(action string, data interface{}) *errs.Error {
	msg := &utils.IOMsg{Action: action, Data: data}
	err := w.WriteMsg(msg)
	if err != nil {
		return err
	}
	w.initMsgs = append(w.initMsgs, msg)
	return nil
}

func (w *SeriesWatcher) UnWatchJobs(exgName, marketType, jobType string, pairs []string) *errs.Error {
	if w.deps != nil {
		runtimeExg, runtimeMarket := w.identity()
		if exgName == "" {
			exgName = runtimeExg
		}
		if marketType == "" {
			marketType = runtimeMarket
		}
		if (runtimeExg != "" && exgName != runtimeExg) || (runtimeMarket != "" && marketType != runtimeMarket) {
			return errs.NewMsg(core.ErrBadConfig, "watch identity %s:%s does not match runtime %s:%s",
				exgName, marketType, runtimeExg, runtimeMarket)
		}
	}
	prefix := w.getPrefix(exgName, marketType, jobType)
	tags := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		if strings.HasSuffix(prefix, "_") {
			tags = append(tags, prefix+pair)
		}
		w.setJob(jobType, pair, nil)
		w.delPairCopied(pair)
	}
	if len(tags) == 0 {
		return nil
	}
	return w.WriteMsg(&utils.IOMsg{Action: "unsubscribe", Data: tags})
}

func (w *SeriesWatcher) onSpiderSeries(raw *utils.IOMsgRaw) {
	if raw == nil || !w.beginCallback() {
		return
	}
	defer w.endCallback()
	key := raw.Action
	data := raw.Data
	if w.OnDataMsg == nil {
		log.Debug("spider series skipped", zap.String("key", key))
		return
	}
	parts := strings.Split(key, "_")
	if len(parts) < 4 {
		log.Debug("spider series invalid key", zap.String("key", key))
		return
	}
	msgType, exgName, market, pair := parts[0], parts[1], parts[2], strings.Join(parts[3:], "_")
	if w.deps != nil {
		runtimeExg, runtimeMarket := w.identity()
		if exgName != runtimeExg || market != runtimeMarket {
			return
		}
	}
	job := w.GetJob(msgType, pair)
	if job == nil {
		// 未监听，忽略
		log.Debug("spider series ignored", zap.String("key", key))
		return
	}
	var series NotifySeries
	err_ := utils2.Unmarshal(data, &series, utils2.JsonNumDefault)
	if err_ != nil {
		log.Debug("onSpiderSeries spider series decode fail", zap.String("key", key))
		return
	}
	if len(series.Rows) == 0 {
		log.Debug("spider series empty", zap.String("key", key))
		return
	}
	// 更新收到的时间戳
	lastRowMS := series.Rows[len(series.Rows)-1].TimeMS
	tfMSecs := int64(series.TFSecs * 1000)
	nextBarMS := lastRowMS + tfMSecs
	w.setPairMS(pair, nextBarMS, tfMSecs)
	var msg = &SeriesMsg{
		NotifySeries: series,
		ExgName:      exgName,
		Market:       market,
		Pair:         pair,
	}
	logFields := []zap.Field{zap.String("key", key), zap.Int("num", len(series.Rows)),
		zap.Int64("nextMS", nextBarMS)}
	if msgType == core.WsSubKLine {
		log.Debug("spider uohlcv", logFields...)
		w.OnDataMsg(msg)
		return
	}
	log.Debug("spider ohlcv", logFields...)
	// 记录收到的bar数量
	timeFrame := utils2.SecsToTF(series.TFSecs)
	w.addTfPairHits(timeFrame, pair, len(series.Rows))
	// 检测并填充缺失的K线
	var olds []*orm.DataSeries
	var err *errs.Error
	if w.deps == nil {
		olds, err = job.fillLacksWithSymbolState(w.symbols, job.exSymbol, series.TFSecs, series.Rows[0].TimeMS, nextBarMS)
	} else {
		olds, err = job.fillLacksWithRuntimeDeps(w.deps, job.exSymbol, series.TFSecs, series.Rows[0].TimeMS, nextBarMS)
	}
	if err != nil {
		log.Error("fillLacks fail", zap.String("pair", pair), zap.Error(err))
		return
	}
	// 归集更新指定的周期
	var finishes []*orm.DataSeries
	if series.TFSecs < job.TFSecs {
		//和旧的bar_row合并更新，判断是否有完成的bar
		curRows := series.Rows
		if job.WaitBar != nil {
			olds = append(olds, job.WaitBar)
		}
		jobMSecs := int64(job.TFSecs * 1000)
		var aggRows []*orm.DataSeries
		var lastDone bool
		if w.deps == nil {
			aggRows, lastDone, err = buildAggSeriesWithSymbolState(w.symbols, job.exSymbol, utils2.SecsToTF(job.TFSecs),
				curRows, jobMSecs, 0, olds, tfMSecs, job.AlignOffMS, false)
		} else {
			aggRows, lastDone, err = buildAggSeriesWithRuntimeDeps(w.deps, job.exSymbol, utils2.SecsToTF(job.TFSecs),
				curRows, jobMSecs, 0, olds, tfMSecs, job.AlignOffMS, false)
		}
		if err != nil {
			log.Error("build series fail", zap.String("pair", pair), zap.Error(err))
			return
		}
		finishes = job.getFinishes(aggRows, lastDone)
	} else {
		finishes = series.Rows
	}
	if len(finishes) > 0 {
		msg.TFSecs = job.TFSecs
		msg.Interval = job.TFSecs
		msg.Rows = finishes
		w.OnDataMsg(msg)
	}
}

func (w *SeriesWatcher) onPriceUpdate(raw *utils.IOMsgRaw) {
	if raw == nil || !w.beginCallback() {
		return
	}
	defer w.endCallback()
	key, data := raw.Action, raw.Data
	parts := strings.Split(key, "_")
	if len(parts) < 3 {
		return
	}
	exgName, market := parts[1], parts[2]
	runtimeExg, runtimeMarket := w.identity()
	if exgName != runtimeExg || market != runtimeMarket {
		return
	}
	var msg map[string]float64
	err := utils2.Unmarshal(data, &msg, utils2.JsonNumDefault)
	if err != nil {
		log.Warn("onPriceUpdate receive invalid msg", zap.String("raw", string(data)), zap.Error(err))
		return
	}
	w.setPrices(msg)
}

func (w *SeriesWatcher) onTrades(msg *utils.IOMsgRaw) {
	if msg == nil || !w.beginCallback() {
		return
	}
	defer w.endCallback()
	if w.OnTrades == nil {
		return
	}
	key, data := msg.Action, msg.Data
	parts := strings.Split(key, "_")
	if len(parts) < 4 {
		return
	}
	exgName, market, pair := parts[1], parts[2], strings.Join(parts[3:], "_")
	if w.deps != nil {
		runtimeExg, runtimeMarket := w.identity()
		if exgName != runtimeExg || market != runtimeMarket {
			return
		}
	}
	var trades []*banexg.Trade
	err := utils2.Unmarshal(data, &trades, utils2.JsonNumDefault)
	if err != nil {
		log.Error("onTrades receive invalid data", zap.String("raw", string(data)),
			zap.Error(err))
		return
	}
	if len(trades) == 0 {
		return
	}
	last := trades[len(trades)-1]
	if _, ok := w.getOrderBook(pair); !ok {
		w.setPrice(pair, last.Price, last.Price)
	}
	w.OnTrades(exgName, market, pair, trades)
}

func (w *SeriesWatcher) onBook(msg *utils.IOMsgRaw) {
	if msg == nil || !w.beginCallback() {
		return
	}
	defer w.endCallback()
	key, data := msg.Action, msg.Data
	parts := strings.Split(key, "_")
	if len(parts) < 4 {
		return
	}
	msgType, exgName, market, pair := parts[0], parts[1], parts[2], parts[3]
	if runtimeExg, runtimeMarket := w.identity(); exgName != runtimeExg || market != runtimeMarket {
		return
	}
	job := w.GetJob(msgType, pair)
	if job == nil {
		// 未监听，忽略
		return
	}
	var book banexg.OrderBook
	err := utils2.Unmarshal(data, &book, utils2.JsonNumDefault)
	if err != nil {
		log.Error("onBook receive invalid data", zap.String("raw", string(data)),
			zap.Error(err))
		return
	}
	if book.Symbol == "" {
		return
	}
	if len(book.Asks.Price) == 0 || len(book.Bids.Price) == 0 {
		return
	}
	w.setPrice(pair, book.Asks.Price[0], book.Bids.Price[0])
	w.setOrderBook(pair, &book)
	if w.OnDepth != nil {
		w.OnDepth(&book)
	}
}
