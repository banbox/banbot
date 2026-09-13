package data

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

type NotifySeries struct {
	TFSecs   int
	Interval int // 推送更新间隔, <= TFSecs
	Rows     []*orm.DataSeries
}

type SeriesMsg struct {
	NotifySeries
	ExgName string // The name of the exchange 交易所名称
	Market  string // market 市场
	Pair    string // symbol  币种
}

/** *******************************  Spider 爬虫部分   ****************************
 */
const klineParallelNum = 6

type SaveSeries struct {
	Deps      *RuntimeDeps
	Sid       int32
	TimeFrame string
	Rows      []*orm.DataSeries
	MsgAction string
	ReceiveAt int64
}

func (s *LiveSpider) consumeSeriesWriteQ(workNum int) {
	defer s.workers.Done()
	var saves sync.WaitGroup
	defer saves.Wait()
	guard := make(chan struct{}, workNum)
	defer close(guard)
	setOne := func() bool {
		logged := false
		for {
			select {
			case guard <- struct{}{}:
				return true
			case <-s.ctx.Done():
				return false
			case <-time.After(20 * time.Second):
				if !logged {
					log.Error("wait save in spider timeout")
					logged = true
				}
			}
		}
	}
	mntSta := newPeriodSta("1m")
	hourSta := newPeriodSta("1h")
	ema := core.NewEMA(0.1)
	lastDelayWarn := int64(0)
	for {
		var save *SaveSeries
		select {
		case <-s.ctx.Done():
			return
		case save = <-s.writeQ:
		}
		if save == nil {
			continue
		}
		if !setOne() {
			return
		}
		deps := save.Deps
		if deps == nil {
			deps = s.deps
		}
		nowMS := deps.utcStamp()
		waitMS := float64(nowMS - save.ReceiveAt)
		waitMa := ema.Update(waitMS)
		if waitMa > 1000 && len(s.writeQ) > 10 && nowMS > lastDelayWarn+60000 {
			lastDelayWarn = nowMS
			log.Warn("series save task consume slowly", zap.Int("waitMS", int(waitMa)), zap.Int("waitNum", len(s.writeQ)))
		}
		saves.Add(1)
		go func(job *SaveSeries) {
			defer saves.Done()
			start := time.Now()
			var saveCost, totalCost time.Duration
			tfSecs := utils2.TFToSecs(job.TimeFrame)
			defer func() {
				if totalCost > time.Millisecond*50 {
					jobDeps := job.Deps
					if jobDeps == nil {
						jobDeps = s.deps
					}
					waitCost := jobDeps.utcStamp() - job.ReceiveAt
					barEnd := job.Rows[len(job.Rows)-1].EndMS
					barEndStr := btime.ToDateStr(barEnd, core.DefaultDateFmt)
					log.Info("save series", zap.Int32("sid", job.Sid), zap.Int64("waitMS", waitCost),
						zap.Duration("saveDb", saveCost), zap.Duration("send", totalCost-saveCost),
						zap.Int("num", len(job.Rows)), zap.String("barEnd", barEndStr))
				}
				<-guard
			}()
			// Remove from sidMap when job is consumed
			s.sidMu.Lock()
			delete(s.sidMap, job.Sid)
			s.sidMu.Unlock()
			deps := job.Deps
			if deps == nil {
				deps = s.deps
			}
			if err := trySaveSeriesWithRuntimeDeps(deps, job, tfSecs, mntSta, hourSta); err != nil {
				log.Error("save series fail", zap.Int32("sid", job.Sid), zap.Error(err))
				return
			}
			saveCost = time.Since(start)
			// After the series is written to the database, notify robots to avoid repeated insertion.
			// 写入时序数据到数据库后，才发消息通知机器人，避免重复插入。
			err := s.Broadcast(&utils.IOMsg{
				Action: job.MsgAction,
				Data: NotifySeries{
					TFSecs:   tfSecs,
					Interval: tfSecs,
					Rows:     job.Rows,
				},
			})
			totalCost = time.Since(start)
			if err != nil {
				log.Error("broadCast kline fail", zap.String("action", job.MsgAction), zap.Error(err))
			}
		}(save)
	}
}

type FetchJob struct {
	PairTFCache
	Pair      string
	CheckSecs int
	Since     int64
	NextRun   int64
}

type Miner struct {
	spider       *LiveSpider
	deps         *RuntimeDeps
	cleanup      func()
	cleanupOnce  sync.Once
	ExgName      string
	Market       string
	exchange     banexg.BanExchange
	Fetchs       map[string]*FetchJob
	KLineApis    *PairSubs
	KLines       *PairSubs
	Trades       *PairSubs
	Depths       *PairSubs
	IsWatchPrice bool
	IsLoopKline  bool
	klineStates  map[string]*KLineState
	klineLasts   map[string]int64 // ws订阅k线的上次时间戳
	retryWaits   *btime.RetryWaits
	lockBarState deadlock.Mutex
	lockBarLasts deadlock.Mutex
}

type PairSubs struct {
	pairs  map[string]bool
	m      deadlock.Mutex
	Status int // 0 not subscribed, 1 subscribing, 2 subscribed
}

func NewPairSubs() *PairSubs {
	return &PairSubs{
		pairs: make(map[string]bool),
	}
}

// GetNewSubs get pairs need to be subscribed
func (s *PairSubs) GetNewSubs(pairs []string) []string {
	s.m.Lock()
	defer s.m.Unlock()
	if s.Status == 0 {
		// 未订阅，返回全部品种
		for _, pair := range pairs {
			s.pairs[pair] = true
		}
		pairs = utils.KeysOfMap(s.pairs)
		if len(pairs) > 0 {
			s.Status = 1
		}
		return pairs
	} else {
		// 正在订阅，返回新的尚未订阅品种
		result := make([]string, 0, len(pairs))
		for _, pair := range pairs {
			if !s.pairs[pair] {
				s.pairs[pair] = true
				result = append(result, pair)
			}
		}
		return result
	}
}

func (s *PairSubs) Set(pairs ...string) []string {
	s.m.Lock()
	res := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if _, ok := s.pairs[p]; !ok {
			s.pairs[p] = true
			res = append(res, p)
		}
	}
	s.m.Unlock()
	return res
}

func (s *PairSubs) Remove(pairs ...string) []string {
	s.m.Lock()
	res := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if _, ok := s.pairs[p]; ok {
			delete(s.pairs, p)
			res = append(res, p)
		}
	}
	s.m.Unlock()
	return res
}

func (s *PairSubs) Len() int {
	s.m.Lock()
	l := len(s.pairs)
	s.m.Unlock()
	return l
}

func (s *PairSubs) Keys() []string {
	s.m.Lock()
	keys := utils.KeysOfMap(s.pairs)
	s.m.Unlock()
	return keys
}

func (s *PairSubs) KeyMap() map[string]bool {
	s.m.Lock()
	res := maps.Clone(s.pairs)
	s.m.Unlock()
	return res
}

func (s *PairSubs) status() int {
	s.m.Lock()
	defer s.m.Unlock()
	return s.Status
}

func (s *PairSubs) setStatus(status int) {
	s.m.Lock()
	s.Status = status
	s.m.Unlock()
}

// readSpiderBatch waits for one adapter update, then drains currently queued
// updates without blocking. Unlike ReadChanBatch it also observes shutdown and
// closed adapter channels, so Spider.Join can prove watcher completion.
func readSpiderBatch[T comparable](ctx context.Context, updates <-chan T) ([]T, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case first, ok := <-updates:
		if !ok {
			return nil, false
		}
		items := []T{first}
		for {
			select {
			case item, ok := <-updates:
				if !ok {
					return items, true
				}
				items = append(items, item)
			default:
				return items, true
			}
		}
	}
}

type LiveSpider struct {
	*utils.ServerIO
	deps        *RuntimeDeps
	miners      map[string]*Miner
	minersMu    sync.RWMutex
	sidMap      map[int32]*SaveSeries
	sidMu       sync.Mutex
	writeQ      chan *SaveSeries
	ctx         context.Context
	cancel      context.CancelFunc
	workers     sync.WaitGroup
	minerWork   sync.WaitGroup
	lifecycleMu sync.Mutex
	stopped     bool
	newRuntime  SpiderRuntimeFactory
}

// NewLiveSpider creates one isolated live-ingestion owner. Every miner is
// created through the supplied typed runtime factory; it never falls back to
// package-global exchange, ORM, configuration, or clock state.
func NewLiveSpider(server *utils.ServerIO, deps *RuntimeDeps, newRuntime SpiderRuntimeFactory) *LiveSpider {
	ctx := context.Background()
	if deps != nil {
		ctx = deps.context()
	}
	return newLiveSpiderWithContext(server, deps, ctx, newRuntime)
}

func newLiveSpiderWithContext(server *utils.ServerIO, deps *RuntimeDeps, ctx context.Context,
	newRuntime SpiderRuntimeFactory) *LiveSpider {
	ctx, cancel := context.WithCancel(ctx)
	return &LiveSpider{
		ServerIO: server, deps: deps, miners: make(map[string]*Miner),
		sidMap: make(map[int32]*SaveSeries), writeQ: make(chan *SaveSeries, 9999),
		ctx: ctx, cancel: cancel,
		newRuntime: newRuntime,
	}
}

func (s *LiveSpider) Stop() {
	if s == nil {
		return
	}
	s.lifecycleMu.Lock()
	s.stopped = true
	if s.cancel != nil {
		s.cancel()
	}
	s.lifecycleMu.Unlock()
	if s.ServerIO != nil {
		s.ServerIO.Stop()
	}
}

func (m *Miner) close() {
	if m != nil && m.cleanup != nil {
		m.cleanupOnce.Do(m.cleanup)
	}
}

func (s *LiveSpider) Join() {
	if s == nil {
		return
	}
	s.minerWork.Wait()
	s.workers.Wait()
	if s.ServerIO != nil {
		s.ServerIO.Join()
	}
	s.minersMu.RLock()
	miners := make([]*Miner, 0, len(s.miners))
	for _, miner := range s.miners {
		miners = append(miners, miner)
	}
	s.minersMu.RUnlock()
	for _, miner := range miners {
		miner.close()
	}
}

type SpiderStartupFunc func(ctx context.Context, spider *LiveSpider) error

// SpiderExchangeFactory remains the entry-owned adapter constructor used while
// composing child runtimes. LiveSpider itself accepts only SpiderRuntimeFactory
// and never turns this exchange-only factory into runtime state.
type SpiderExchangeFactory func(exchange, market string) (banexg.BanExchange, *errs.Error)

// SpiderRuntimeFactory creates the complete identity owner for one miner. A
// child runtime owns an adapter, symbol state, and storage for its market.
type SpiderRuntimeFactory func(context.Context, string, string) (*RuntimeDeps, func(), *errs.Error)

// spiderStorage prepares the owned storage before network ingestion starts.
// It is deliberately typed and narrow so lifecycle tests can use a fake
// storage without restoring an ORM/global-state startup facade.
type spiderStorage interface {
	Prepare(context.Context, *RuntimeDeps) *errs.Error
}

type runtimeSpiderStorage struct{}

func (runtimeSpiderStorage) Prepare(ctx context.Context, deps *RuntimeDeps) *errs.Error {
	if err := orm.EnsureTimescaleCompressionWithStorage(ctx, deps.Storage); err != nil {
		return err
	}
	return purgeSpiderKlineUn(deps)
}

// monitorSubscriptions periodically checks all miners for failed subscriptions and restarts them
func (s *LiveSpider) monitorSubscriptions() {
	defer s.workers.Done()
	log.Info("Starting subscription monitor")
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(time.Second):
		}

		// Check all miners for failed subscriptions
		s.minersMu.RLock()
		for key, miner := range s.miners {
			curMS := miner.deps.utcStamp()
			if miner.KLineApis.Len() > 0 {
				miner.startLoopKLines()
			}

			// Check KLine subscriptions
			klineNum := miner.KLines.Len()
			if klineNum > 0 && miner.KLines.status() == 0 && curMS > miner.retryWaits.NextRetry("watchKLines") {
				log.Info("Recovering KLine subscription",
					zap.String("miner", key),
					zap.Int("pairs", klineNum))
				miner.watchKLines(nil)
			}

			// Check Trade subscriptions
			tradeNum := miner.Trades.Len()
			if tradeNum > 0 && miner.Trades.status() == 0 && curMS > miner.retryWaits.NextRetry("watchTrades") {
				log.Info("Recovering Trade subscription",
					zap.String("miner", key),
					zap.Int("pairs", tradeNum))
				miner.watchTrades(nil)
			}

			// Check OrderBook subscriptions
			bookNum := miner.Depths.Len()
			if bookNum > 0 && miner.Depths.status() == 0 && curMS > miner.retryWaits.NextRetry("watchOdBooks") {
				log.Info("Recovering OrderBook subscription",
					zap.String("miner", key),
					zap.Int("pairs", bookNum))
				miner.watchOdBooks(nil)
			}

			// Check Price subscriptions for contract markets
			//if miner.exchange.IsContract(miner.Market) && !miner.IsWatchPrice && curMS > retryWaits.NextRetry("watchPrices") {
			//	log.Info("Recovering Price subscription", zap.String("miner", key))
			//	miner.watchPrices()
			//}
		}
		s.minersMu.RUnlock()
	}
}

func newMiner(spider *LiveSpider, exgName, market string) (*Miner, *errs.Error) {
	if spider == nil || spider.newRuntime == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "spider runtime factory is required")
	}
	deps, cleanup, err := spider.newRuntime(spider.ctx, exgName, market)
	if err != nil {
		return nil, err
	}
	if cleanup == nil {
		cleanup = func() {}
	}
	if deps == nil || deps.Exchange == nil || deps.Symbols == nil || deps.Storage == nil {
		cleanup()
		return nil, errs.NewMsg(core.ErrBadConfig, "spider miner runtime dependencies are incomplete")
	}
	name, runtimeMarket, identityErr := deps.ResolveIdentity()
	if identityErr != nil || name != exgName || runtimeMarket != market {
		cleanup()
		if identityErr != nil {
			return nil, errs.New(core.ErrBadConfig, fmt.Errorf("spider miner runtime identity: %w", identityErr))
		}
		return nil, errs.NewMsg(core.ErrBadConfig, "spider miner runtime identity %s/%s does not match request %s/%s",
			name, runtimeMarket, exgName, market)
	}
	return &Miner{
		spider:      spider,
		deps:        deps,
		cleanup:     cleanup,
		ExgName:     exgName,
		Market:      market,
		exchange:    deps.Exchange,
		Fetchs:      map[string]*FetchJob{},
		KLineApis:   NewPairSubs(),
		KLines:      NewPairSubs(),
		Trades:      NewPairSubs(),
		Depths:      NewPairSubs(),
		klineStates: map[string]*KLineState{},
		klineLasts:  make(map[string]int64),
		retryWaits:  btime.NewRetryWaits(0, nil),
	}, nil
}

func (m *Miner) init() {
	var symbols *orm.SymbolState
	var snapshot *config.Snapshot
	var runtimeCore *core.State
	if m.deps != nil {
		symbols, snapshot, runtimeCore = m.deps.Symbols, m.deps.Config, m.deps.Core
	}
	_, err := orm.LoadMarketsWithRuntime(symbols, m.exchange, false, snapshot, runtimeCore)
	if err != nil {
		log.Error("load markets for miner fail", zap.String("exg", m.ExgName), zap.Error(err))
	}
}

func (m *Miner) SubPairs(jobType string, pairs ...string) *errs.Error {
	valids, _ := m.exchange.CheckSymbols(pairs...)
	if len(valids) == 0 {
		if len(pairs) > 0 {
			return nil
		}
		// If the incoming is empty, take all the underlying of the current exchange + market
		// 传入为空，取当前交易所+市场的所有标的
		markets := m.exchange.GetCurMarkets()
		valids = make([]string, 0, len(markets))
		for _, mar := range markets {
			valids = append(valids, mar.Symbol)
		}
	}
	ensures := make([]*orm.ExSymbol, 0, len(valids))
	for _, p := range valids {
		ensures = append(ensures, &orm.ExSymbol{
			Exchange: m.ExgName,
			Market:   m.Market,
			Symbol:   p,
		})
	}
	var err *errs.Error
	if m.deps != nil && m.deps.Symbols != nil {
		err = m.deps.Symbols.EnsureSymbols(ensures, m.ExgName)
	} else {
		err = orm.EnsureSymbols(ensures, m.ExgName)
	}
	if err != nil {
		return err
	}
	if jobType == core.WsSubDepth {
		m.watchOdBooks(valids)
	} else if jobType == "ohlcv" {
		m.loopKLines(valids)
	} else if jobType == core.WsSubKLine {
		m.watchKLines(valids)
	} else if jobType == "price" {
		m.watchPrices()
	} else if jobType == core.WsSubTrade {
		m.watchTrades(valids)
	} else {
		log.Error("unknown sub type", zap.String("val", jobType))
	}
	return nil
}

func (m *Miner) UnSubPairs(jobType string, pairs ...string) *errs.Error {
	if jobType == core.WsSubDepth {
		removes := m.Depths.Remove(pairs...)
		if len(removes) > 0 {
			log.Info("UnSubPairs Depth", zap.Strings("pairs", removes))
			return m.exchange.UnWatchOrderBooks(removes, nil)
		}
		return nil
	} else if jobType == "ohlcv" {
		m.KLineApis.Remove(pairs...)
		m.lockBarState.Lock()
		for _, p := range pairs {
			delete(m.klineStates, p)
		}
		m.lockBarState.Unlock()
		return nil
	} else if jobType == core.WsSubKLine {
		timeFrame := "1m"
		items := m.KLines.Remove(pairs...)
		jobs := make([][2]string, 0, len(items))
		for _, p := range items {
			jobs = append(jobs, [2]string{p, timeFrame})
		}
		if len(jobs) > 0 {
			log.Info("UnSubPairs "+jobType, zap.Strings("pairs", items))
			return m.exchange.UnWatchOHLCVs(jobs, nil)
		}
		return nil
	} else if jobType == "price" {
		log.Info("UnSubPairs all pairs price", zap.Strings("pairs", pairs))
		return m.exchange.UnWatchMarkPrices(nil, nil)
	} else if jobType == core.WsSubTrade {
		items := m.Trades.Remove(pairs...)
		if len(items) > 0 {
			log.Info("UnSubPairs trades", zap.Strings("pairs", items))
			return m.exchange.UnWatchTrades(items, nil)
		}
		return nil
	} else {
		log.Error("unknown unsub type", zap.String("val", jobType))
	}
	return nil
}

func (m *Miner) watchTrades(pairs []string) {
	if m.spider.ctx.Err() != nil {
		return
	}
	pairs = m.Trades.GetNewSubs(pairs)
	if len(pairs) == 0 {
		return
	}
	out, err := m.exchange.WatchTrades(pairs, nil)
	if err != nil {
		m.Trades.setStatus(0)
		m.retryWaits.SetFail("watchTrades")
		log.Error("watch trades fail", zap.String("exg", m.ExgName), zap.Error(err))
		return
	}
	m.retryWaits.Reset("watchTrades")
	if m.Trades.status() == 2 {
		return
	}
	m.Trades.setStatus(2)
	log.Info("start watch trades", zap.String("exg", m.ExgName), zap.Int("num", m.Trades.Len()))
	prefix := fmt.Sprintf("%s_%s_%s_", core.WsSubTrade, m.ExgName, m.Market)

	m.spider.workers.Add(1)
	go func() {
		defer m.spider.workers.Done()
		defer func() {
			m.Trades.setStatus(0)
			m.retryWaits.SetFail("watchTrades")
			log.Info("watch trades stopped", zap.String("exg", m.ExgName))
		}()
		for {
			batch, ok := readSpiderBatch(m.spider.ctx, out)
			if !ok {
				return
			}
			pairTrades := make(map[string][]*banexg.Trade)
			for _, t := range batch {
				items, _ := pairTrades[t.Symbol]
				pairTrades[t.Symbol] = append(items, t)
			}
			for pair, items := range pairTrades {
				err = m.spider.Broadcast(&utils.IOMsg{
					Action: prefix + pair,
					Data:   items,
				})
				if err != nil {
					log.Error("broadCast trade fail", zap.String("key", prefix), zap.Error(err))
				}
			}
		}
	}()
}

func (m *Miner) watchPrices() {
	if m.spider.ctx.Err() != nil || m.IsWatchPrice || !m.exchange.IsContract(m.Market) {
		return
	}
	out, err := m.exchange.WatchMarkPrices(nil, map[string]interface{}{
		banexg.ParamInterval: "1s",
	})
	if err != nil {
		m.IsWatchPrice = false
		m.retryWaits.SetFail("watchPrices")
		log.Error("watch prices fail", zap.String("exg", m.ExgName), zap.Error(err))
		return
	}
	m.retryWaits.Reset("watchPrices")
	m.IsWatchPrice = true
	log.Info("start watch prices", zap.String("exg", m.ExgName))
	prefix := fmt.Sprintf("price_%s_%s", m.ExgName, m.Market)

	m.spider.workers.Add(1)
	go func() {
		defer m.spider.workers.Done()
		defer func() {
			m.IsWatchPrice = false
			m.retryWaits.SetFail("watchPrices")
			log.Info("watch prices stopped", zap.String("exg", m.ExgName))
		}()
		for {
			select {
			case <-m.spider.ctx.Done():
				return
			case item, ok := <-out:
				if !ok {
					return
				}
				err = m.spider.Broadcast(&utils.IOMsg{
					Action: prefix,
					Data:   item,
				})
				if err != nil {
					log.Error("broadCast price fail", zap.String("key", prefix), zap.Error(err))
				}
			}
		}
	}()
}

func (m *Miner) watchOdBooks(pairs []string) {
	if m.spider.ctx.Err() != nil {
		return
	}
	pairs = m.Depths.GetNewSubs(pairs)
	if len(pairs) == 0 {
		return
	}
	out, err := m.exchange.WatchOrderBooks(pairs, 0, nil)
	if err != nil {
		m.Depths.setStatus(0)
		m.retryWaits.SetFail("watchOdBooks")
		log.Error("watch odBook fail", zap.String("exg", m.ExgName), zap.Error(err))
		return
	}
	m.retryWaits.Reset("watchOdBooks")
	if m.Depths.status() == 2 {
		return
	}
	m.Depths.setStatus(2)
	log.Info("start watch odBooks", zap.String("exg", m.ExgName), zap.Int("num", m.Depths.Len()))
	prefix := fmt.Sprintf("%s_%s_%s_", core.WsSubDepth, m.ExgName, m.Market)

	m.spider.workers.Add(1)
	go func() {
		defer m.spider.workers.Done()
		defer func() {
			m.Depths.setStatus(0)
			m.retryWaits.SetFail("watchOdBooks")
			log.Info("watch odBook stopped", zap.String("exg", m.ExgName))
		}()
		for {
			batch, ok := readSpiderBatch(m.spider.ctx, out)
			if !ok {
				return
			}
			pairBook := make(map[string]*banexg.OrderBook)
			for _, dep := range batch {
				pairBook[dep.Symbol] = dep
			}
			for pair, dep := range pairBook {
				err = m.spider.Broadcast(&utils.IOMsg{
					Action: prefix + pair,
					Data:   dep,
				})
				if err != nil {
					log.Error("broadCast odBook fail", zap.String("market", prefix), zap.Error(err))
				}
			}
		}
	}()
}

type KLineState struct {
	Sid      int32
	ExpectMS int64 // next bar start time
	PrevBar  *banexg.Kline
}

/*
这里将订阅此市场的最小周期(1s/1m)；1h/1d等大周期已在writeQ消费端判断并fetch
*/
func (m *Miner) watchKLines(pairs []string) {
	if m.spider.ctx.Err() != nil {
		return
	}
	pairs = m.KLines.GetNewSubs(pairs)
	if len(pairs) == 0 {
		return
	}
	jobs := make([][2]string, 0, len(pairs))
	timeFrame := "1m"
	tfSecs := utils2.TFToSecs(timeFrame)
	curTimeMS := m.deps.timeMS()
	for _, p := range pairs {
		jobs = append(jobs, [2]string{p, timeFrame})
		m.lockBarLasts.Lock()
		if _, ok := m.klineLasts[p]; !ok {
			m.klineLasts[p] = curTimeMS
		}
		m.lockBarLasts.Unlock()
	}
	out, err := m.exchange.WatchOHLCVs(jobs, nil)
	if err != nil {
		m.KLines.setStatus(0)
		m.retryWaits.SetFail("watchKLines")
		log.Error("watch kline fail", zap.String("exg", m.ExgName),
			zap.Strings("pairs", pairs), zap.Error(err))
		return
	}
	m.retryWaits.Reset("watchKLines")
	if m.KLines.status() == 2 {
		return
	}
	m.KLines.setStatus(2)
	log.Info("start watch kline", zap.String("exg", m.ExgName), zap.Int("num", m.KLines.Len()))
	unPrefix := fmt.Sprintf("uohlcv_%s_%s_", m.ExgName, m.Market)
	intvMa := core.NewEMA(0.1)
	// 5s统计更新一次K线全品种平均间隔到intvMa
	ns := core.NewNumSet(5000, func(stamp int64, data map[string]float64) {
		var sum float64
		for _, v := range data {
			sum += v
		}
		intvMa.Update(sum / float64(len(data)))
	})

	// The candlestick is received, sent to the robot, and saved to the database
	// 收到K线，发送到机器人，保存到数据库
	handleSubKLines := func(pair string, arr []*banexg.Kline) {
		m.lockBarLasts.Lock()
		lastNotify, ok := m.klineLasts[pair]
		m.lockBarLasts.Unlock()
		if !ok {
			code := fmt.Sprintf("%s.%s.%s", m.ExgName, m.Market, pair)
			log.Warn("no pair lasts: " + code)
			return
		}
		// Send uohlcv subscription messages
		// 发送uohlcv订阅消息
		exs, symbolErr := m.deps.Symbols.GetExSymbol(m.exchange, pair)
		if symbolErr != nil {
			log.Warn("kline symbol not found", zap.String("pair", pair), zap.Error(symbolErr))
			return
		}
		rows := orm.KLinesToSeries(exs, "1m", arr, nil, false, false)
		err_ := m.spider.Broadcast(&utils.IOMsg{
			Action: unPrefix + pair,
			Data: NotifySeries{
				TFSecs:   tfSecs,
				Interval: 1,
				Rows:     rows,
			},
		})
		curTS := m.deps.utcStamp()
		var intvMS float64
		if curTS > lastNotify+900 {
			// 1s最多记录一次
			if lastNotify > 0 {
				intvMS = float64(curTS - lastNotify)
				ns.Update(curTS, pair, intvMS)
			}
			m.lockBarLasts.Lock()
			m.klineLasts[pair] = curTS
			m.lockBarLasts.Unlock()
		}
		if intvMa.Age > 3 && intvMS > intvMa.Val*5 {
			// 间隔超过平均间隔的5倍，认为有缺失（也有可能是交易所数据无变化未推送）
			log.Warn("ohlcv interval too big, may lost data", zap.String("k", pair),
				zap.Float64("intv", intvMS), zap.Float64("avgIntv", intvMa.Val))
		}
		if err_ != nil {
			log.Error("broadCast kline fail", zap.String("pair", pair), zap.Error(err_))
		}
	}

	// 处理ws推送的K线数据
	pricePrefix := fmt.Sprintf("price_%s_%s", m.ExgName, m.Market)
	m.spider.workers.Add(1)
	go func() {
		defer m.spider.workers.Done()
		defer func() {
			m.KLines.setStatus(0)
			m.retryWaits.SetFail("watchKLines")
			log.Info("watch kline stopped", zap.String("exg", m.ExgName))
		}()
		for {
			klines, ok := readSpiderBatch(m.spider.ctx, out)
			if !ok {
				return
			}
			cache := map[string][]*banexg.Kline{}
			prices := map[string]float64{}
			for _, val := range klines {
				prices[val.Symbol] = val.Close
				arr, _ := cache[val.Symbol]
				if len(arr) > 0 {
					last := arr[len(arr)-1]
					if last.Time == val.Time && val.Volume > last.Volume {
						arr[len(arr)-1] = &val.Kline
					}
				} else {
					cache[val.Symbol] = append(arr, &val.Kline)
				}
			}
			// 使用k线的最新价格（MarkPrice偏差略大）
			err = m.spider.Broadcast(&utils.IOMsg{
				Action: pricePrefix,
				Data:   prices,
			})
			if err != nil {
				log.Error("broadCast price fail", zap.String("key", pricePrefix), zap.Error(err))
			}
			for key, arr := range cache {
				handleSubKLines(key, arr)
			}
		}
	}()
}

func (m *Miner) loopKLines(pairs []string) {
	newPairs := m.KLineApis.GetNewSubs(pairs)
	if len(newPairs) > 0 {
		startMs := utils2.AlignTfMSecs(m.deps.utcStamp(), 60000)
		for _, p := range newPairs {
			m.lockBarState.Lock()
			_, ok := m.klineStates[p]
			m.lockBarState.Unlock()
			if !ok {
				exs, err := m.deps.Symbols.GetExSymbol(m.exchange, p)
				if err != nil {
					code := fmt.Sprintf("%s.%s.%s", m.ExgName, m.Market, p)
					log.Error("invalid symbol", zap.String("pair", code), zap.Error(err))
					continue
				}
				state := &KLineState{
					Sid:      exs.ID,
					ExpectMS: startMs,
				}
				m.lockBarState.Lock()
				m.klineStates[p] = state
				m.lockBarState.Unlock()
			}
		}
	}
	m.startLoopKLines()
}

func (m *Miner) startLoopKLines() {
	if m.spider.ctx.Err() != nil {
		return
	}
	m.lockBarState.Lock()
	if m.IsLoopKline {
		m.lockBarState.Unlock()
		return
	}
	m.IsLoopKline = true
	m.lockBarState.Unlock()
	mntMSecs := int64(60000)
	curTF := "1m"
	prefix := fmt.Sprintf("ohlcv_%s_%s_", m.ExgName, m.Market)
	m.spider.workers.Add(1)
	go func() {
		defer m.spider.workers.Done()
		// Run once for newly subscribed pairs, then keep the schedule instance-local.
		// A process-wide cron entry would survive this Spider and leak callbacks into
		// the next runtime.
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			if !m.fetchLoopKlines(mntMSecs, curTF, prefix) {
				return
			}
			select {
			case <-m.spider.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// fetchLoopKlines fetches one completed-bar batch. It returns false only when
// the owning Spider was stopped.
func (m *Miner) fetchLoopKlines(mntMSecs int64, curTF, prefix string) bool {
	select {
	case <-m.spider.ctx.Done():
		return false
	default:
	}
	pairs := m.KLineApis.KeyMap()
	if len(pairs) == 0 {
		return true
	}
	curTimeMS := m.deps.utcStamp()
	var pairLock sync.Mutex
	startMS := utils2.AlignTfMSecs(curTimeMS, mntMSecs)
	retry := 0
	delay := time.Duration(20)
	initNum := len(pairs)
	barNum := 0
	for len(pairs) > 0 && retry < 3 {
		if retry > 0 {
			log.Info(fmt.Sprintf("retry %d fetch kline for %d/%d pairs", retry, len(pairs), initNum))
		}
		pairArr := utils.KeysOfMap(pairs)
		_ = utils.ParallelRun(pairArr, klineParallelNum, func(i int, p string) *errs.Error {
			m.lockBarState.Lock()
			sta, _ := m.klineStates[p]
			m.lockBarState.Unlock()
			if sta == nil {
				log.Warn("no sta to writeQ", zap.String("pair", p))
				return nil
			}
			log.Debug("try fetch kline", zap.Int32("sid", sta.Sid), zap.String("pair", p),
				zap.Int64("time", sta.ExpectMS))
			bars, err := m.exchange.FetchOHLCV(p, curTF, sta.ExpectMS, 0, nil)
			if err != nil {
				code := fmt.Sprintf("%s.%s.%s", m.ExgName, m.Market, p)
				log.Error("FetchOHLCV fail", zap.String("exg", code), zap.Error(err))
				return nil
			}
			log.Debug("fetch kline done", zap.Int32("sid", sta.Sid), zap.Int("num", len(bars)))
			if len(bars) > 0 {
				last := bars[len(bars)-1]
				if last.Time >= startMS {
					bars = bars[:len(bars)-1]
				}
				if len(bars) > 0 {
					pairLock.Lock()
					barNum += len(bars)
					pairLock.Unlock()
					sta.ExpectMS = bars[len(bars)-1].Time + mntMSecs
					exs := m.deps.Symbols.GetSymbolByID(sta.Sid)
					if exs == nil {
						return errs.NewMsg(core.ErrInvalidSymbol, "symbol id %d is not owned by spider miner", sta.Sid)
					}
					rows := orm.KLinesToSeries(exs, curTF, bars, nil, false, true)
					// There are completed k-lines, written to the database, and only then the message is broadcast
					// 有已完成的k线，写入到数据库，然后才广播消息
					m.spider.sidMu.Lock()
					existingJob, exists := m.spider.sidMap[sta.Sid]
					if exists {
						existingJob.Rows = append(existingJob.Rows, rows...)
						m.spider.sidMu.Unlock()
						log.Debug("kline appended to existing job", zap.Int32("sid", sta.Sid), zap.String("pair", p),
							zap.Int("num", len(bars)), zap.Int("total", len(existingJob.Rows)))
					} else {
						// Create new job and add to both sidMap and writeQ
						newJob := &SaveSeries{
							Deps:      m.deps,
							Sid:       sta.Sid,
							TimeFrame: curTF,
							Rows:      rows,
							MsgAction: prefix + p,
							ReceiveAt: m.deps.utcStamp(),
						}
						m.spider.sidMap[sta.Sid] = newJob
						m.spider.sidMu.Unlock()
						start := time.Now()
						select {
						case <-m.spider.ctx.Done():
							return nil
						case m.spider.writeQ <- newJob:
						}
						log.Debug("kline to writeQ", zap.Int32("sid", sta.Sid), zap.String("pair", p),
							zap.Int64("time", last.Time), zap.Int("num", len(bars)),
							zap.Duration("waitQ", time.Since(start)))
					}
				}
			} else {
				log.Info("no bars to writeQ", zap.Int32("sid", sta.Sid), zap.String("pair", p))
			}
			pairLock.Lock()
			delete(pairs, p)
			pairLock.Unlock()
			return nil
		})
		if len(pairs) == 0 {
			break
		}
		retry += 1
		if !m.deps.sleep(time.Millisecond * delay) {
			return false
		}
		delay *= 2
	}
	fails := utils.KeysOfMap(pairs)
	log.Info(fmt.Sprintf("fetched kline %d/%d pairs, retried: %d, total kline: %d at %d, fails: %v",
		initNum-len(pairs), initNum, retry, barNum, curTimeMS, fails))
	return true
}

// RunLiveSpiderWithRuntimeDeps is the explicit composition-root entry point.
// The caller supplies all runtime state and a typed per-market exchange
// factory; no package-global spider state is used.
func RunLiveSpiderWithRuntimeDeps(ctx context.Context, addr string, deps *RuntimeDeps,
	factory SpiderRuntimeFactory, startup SpiderStartupFunc) *errs.Error {
	if deps == nil || deps.Core == nil || deps.Clock == nil || deps.Config == nil || deps.Symbols == nil || deps.Storage == nil || factory == nil {
		return errs.NewMsg(core.ErrBadConfig, "live spider requires core, clock, config, symbols, storage, and exchange factory")
	}
	if ctx == nil {
		ctx = deps.context()
	}
	server := utils.NewServerIO(addr, "")
	spider, err := prepareLiveSpider(ctx, server, deps, factory, startup, runtimeSpiderStorage{})
	if err != nil {
		return err
	}
	defer func() {
		spider.Stop()
		spider.Join()
	}()
	return spider.RunForever(0, 0)
}

func prepareLiveSpider(ctx context.Context, server *utils.ServerIO, deps *RuntimeDeps,
	factory SpiderRuntimeFactory, startup SpiderStartupFunc, storage spiderStorage) (*LiveSpider, *errs.Error) {
	if server == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "spider server is nil")
	}
	if storage == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "spider storage is required")
	}
	if err := storage.Prepare(ctx, deps); err != nil {
		return nil, err
	}
	spider := newLiveSpiderWithContext(server, deps, ctx, factory)
	server.InitConn = makeInitConn(spider)
	if startup != nil {
		if err := startup(ctx, spider); err != nil {
			spider.Stop()
			spider.Join()
			return nil, errs.New(core.ErrRunTime, fmt.Errorf("spider startup: %w", err))
		}
	}
	spider.workers.Add(2)
	go spider.consumeSeriesWriteQ(5)
	go spider.monitorSubscriptions()
	return spider, nil
}

func purgeSpiderKlineUn(deps *RuntimeDeps) *errs.Error {
	sess, conn, err := deps.conn()
	if err != nil {
		return err
	}
	defer conn.Release()
	return sess.PurgeKlineUn()
}

func (s *LiveSpider) getMiner(exgName, market string) *Miner {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	if s.stopped || s.ctx.Err() != nil {
		s.lifecycleMu.Unlock()
		return nil
	}
	s.minerWork.Add(1)
	s.lifecycleMu.Unlock()
	defer s.minerWork.Done()
	key := fmt.Sprintf("%s:%s", exgName, market)
	s.minersMu.RLock()
	miner, ok := s.miners[key]
	s.minersMu.RUnlock()
	if ok {
		return miner
	}
	var err *errs.Error
	miner, err = newMiner(s, exgName, market)
	if err != nil {
		log.Error("create spider miner fail", zap.String("e", exgName), zap.String("m", market), zap.Error(err))
		return nil
	}
	if s.ctx.Err() != nil {
		miner.close()
		return nil
	}
	miner.init()
	s.minersMu.Lock()
	defer s.minersMu.Unlock()
	if s.ctx.Err() != nil {
		miner.close()
		return nil
	}
	if current := s.miners[key]; current != nil {
		miner.close()
		return current
	}
	s.miners[key] = miner
	log.Info("start miner for", zap.String("e", exgName), zap.String("m", market))
	return miner
}

func makeInitConn(s *LiveSpider) func(*utils.BanConn) {
	return func(c *utils.BanConn) {
		handlePairs := func(data []byte, name string) (*Miner, []string) {
			arr := make([]string, 0, 8)
			err := utils2.Unmarshal(data, &arr, utils2.JsonNumDefault)
			if err != nil {
				log.Warn("receive invalid pairs", zap.String("n", name),
					zap.String("in", string(data)), zap.Error(err))
				return nil, nil
			}
			if len(arr) < 4 {
				log.Error(name+" receive invalid", zap.Strings("msg", arr))
				return nil, nil
			}
			miner := s.getMiner(arr[0], arr[1])
			return miner, arr[2:]
		}
		c.Listens["watch_pairs"] = func(msg *utils.IOMsgRaw) {
			miner, arr := handlePairs(msg.Data, "watch_pairs")
			if miner == nil || len(arr) == 0 {
				return
			}
			err := miner.SubPairs(arr[0], arr[1:]...)
			if err != nil {
				log.Error("spider.sub_pairs fail", zap.Error(err))
			}
		}
		c.Listens["unwatch_pairs"] = func(msg *utils.IOMsgRaw) {
			miner, arr := handlePairs(msg.Data, "unwatch_pairs")
			if miner == nil || len(arr) == 0 {
				return
			}
			err := miner.UnSubPairs(arr[0], arr[1:]...)
			if err != nil {
				log.Error("spider.unsub_pairs fail", zap.Error(err))
			}
		}
	}
}
