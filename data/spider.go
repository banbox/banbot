package data

import (
	"fmt"
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
	"github.com/banbox/bntp"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
	"maps"
	"sync"
	"time"
)

var (
	Spider     *LiveSpider
	retryWaits = btime.NewRetryWaits(0, nil)
)

type NotifyKLines struct {
	TFSecs   int
	Interval int // 推送更新间隔, <= TFSecs
	Arr      []*banexg.Kline
}

type KLineMsg struct {
	NotifyKLines
	ExgName string // The name of the exchange 交易所名称
	Market  string // market 市场
	Pair    string // symbol  币种
}

/** *******************************  Spider 爬虫部分   ****************************
 */
var (
	sidMap           = make(map[int32]*SaveKline)
	sidLock          = sync.Mutex{}
	writeQ           = make(chan *SaveKline, 9999)
	KlineParallelNum = 6 // 抓取K线时的同时并发数
)

type SaveKline struct {
	Sid       int32
	TimeFrame string
	Arr       []*banexg.Kline
	MsgAction string
	ReceiveAt int64
}

func consumeWriteQ(workNum int) {
	guard := make(chan struct{}, workNum)
	defer close(guard)
	setOne := func() {
		logged := false
		for {
			select {
			case guard <- struct{}{}:
				return
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
	for save := range writeQ {
		setOne()
		waitMS := float64(btime.UTCStamp() - save.ReceiveAt)
		waitMa := ema.Update(waitMS)
		if waitMa > 1000 && len(writeQ) > 10 && btime.UTCStamp() > lastDelayWarn+60000 {
			lastDelayWarn = btime.UTCStamp()
			log.Warn("kline save task consume slowly", zap.Int("waitMS", int(waitMa)), zap.Int("waitNum", len(writeQ)))
		}
		go func(job *SaveKline) {
			start := time.Now()
			var saveCost, totalCost time.Duration
			tfSecs := utils2.TFToSecs(job.TimeFrame)
			defer func() {
				if totalCost > time.Millisecond*50 {
					waitCost := btime.UTCStamp() - job.ReceiveAt
					barEnd := job.Arr[len(job.Arr)-1].Time + int64(tfSecs*1000)
					barEndStr := btime.ToDateStr(barEnd, core.DefaultDateFmt)
					log.Info("save kline", zap.Int32("sid", job.Sid), zap.Int64("waitMS", waitCost),
						zap.Duration("saveDb", saveCost), zap.Duration("send", totalCost-saveCost),
						zap.Int("num", len(job.Arr)), zap.String("barEnd", barEndStr))
				}
				<-guard
			}()
			// Remove from sidMap when job is consumed
			sidLock.Lock()
			delete(sidMap, job.Sid)
			sidLock.Unlock()
			trySaveKlines(job, tfSecs, mntSta, hourSta)
			saveCost = time.Since(start)
			// After the K-line is written to the database, a message will be sent to notify the robot to avoid repeated insertion of K-line
			// 写入K线到数据库后，才发消息通知机器人，避免重复插入K线
			err := Spider.Broadcast(&utils.IOMsg{
				Action: job.MsgAction,
				Data: NotifyKLines{
					TFSecs:   tfSecs,
					Interval: tfSecs,
					Arr:      job.Arr,
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
	klineLasts   map[string]int64 //ws订阅k线的上次时间戳
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
	if s.Status == 0 {
		// 未订阅，返回全部品种
		if len(pairs) > 0 {
			s.Set(pairs...)
		}
		pairs = s.Keys()
		if len(pairs) > 0 {
			s.Status = 1
		}
		return pairs
	} else {
		// 正在订阅，返回新的尚未订阅品种
		return s.Set(pairs...)
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

type LiveSpider struct {
	*utils.ServerIO
	miners map[string]*Miner
}

// monitorSubscriptions periodically checks all miners for failed subscriptions and restarts them
func (s *LiveSpider) monitorSubscriptions() {
	log.Info("Starting subscription monitor")
	for {
		core.Sleep(time.Second * 1)

		// Check all miners for failed subscriptions
		for key, miner := range s.miners {
			curMS := btime.UTCStamp()
			if miner.KLineApis.Len() > 0 && !miner.IsLoopKline {
				miner.startLoopKLines()
			}

			// Check KLine subscriptions
			klineNum := miner.KLines.Len()
			if klineNum > 0 && miner.KLines.Status == 0 && curMS > retryWaits.NextRetry("watchKLines") {
				log.Info("Recovering KLine subscription",
					zap.String("miner", key),
					zap.Int("pairs", klineNum))
				miner.watchKLines(nil)
			}

			// Check Trade subscriptions
			tradeNum := miner.Trades.Len()
			if tradeNum > 0 && miner.Trades.Status == 0 && curMS > retryWaits.NextRetry("watchTrades") {
				log.Info("Recovering Trade subscription",
					zap.String("miner", key),
					zap.Int("pairs", tradeNum))
				miner.watchTrades(nil)
			}

			// Check OrderBook subscriptions
			bookNum := miner.Depths.Len()
			if bookNum > 0 && miner.Depths.Status == 0 && curMS > retryWaits.NextRetry("watchOdBooks") {
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
	}
}

func newMiner(spider *LiveSpider, exgName, market string) (*Miner, *errs.Error) {
	exchange, err := exg.GetWith(exgName, market, "")
	if err != nil {
		return nil, err
	}
	return &Miner{
		spider:      spider,
		ExgName:     exgName,
		Market:      market,
		exchange:    exchange,
		Fetchs:      map[string]*FetchJob{},
		KLineApis:   NewPairSubs(),
		KLines:      NewPairSubs(),
		Trades:      NewPairSubs(),
		Depths:      NewPairSubs(),
		klineStates: map[string]*KLineState{},
		klineLasts:  make(map[string]int64),
	}, nil
}

func (m *Miner) init() {
	_, err := orm.LoadMarkets(m.exchange, false)
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
	for _, p := range pairs {
		ensures = append(ensures, &orm.ExSymbol{
			Exchange: m.ExgName,
			Market:   m.Market,
			Symbol:   p,
		})
	}
	err := orm.EnsureSymbols(ensures, m.ExgName)
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
		for _, p := range pairs {
			delete(m.klineStates, p)
		}
		return nil
	} else if jobType == core.WsSubKLine {
		timeFrame := "1m"
		items := m.KLines.Remove(pairs...)
		jobs := make([][2]string, len(items))
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
	pairs = m.Trades.GetNewSubs(pairs)
	if len(pairs) == 0 {
		return
	}
	out, err := m.exchange.WatchTrades(pairs, nil)
	if err != nil {
		m.Trades.Status = 0
		retryWaits.SetFail("watchTrades")
		log.Error("watch trades fail", zap.String("exg", m.ExgName), zap.Error(err))
		return
	}
	retryWaits.Reset("watchTrades")
	if m.Trades.Status == 2 {
		return
	}
	m.Trades.Status = 2
	log.Info("start watch trades", zap.String("exg", m.ExgName), zap.Int("num", m.Trades.Len()))
	prefix := fmt.Sprintf("%s_%s_%s_", core.WsSubTrade, m.ExgName, m.Market)

	go func() {
		defer func() {
			m.Trades.Status = 0
			retryWaits.SetFail("watchTrades")
			log.Info("watch trades stopped", zap.String("exg", m.ExgName))
		}()
		for {
			batch := utils.ReadChanBatch(out, false)
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
	if m.IsWatchPrice || !m.exchange.IsContract(m.Market) {
		return
	}
	out, err := m.exchange.WatchMarkPrices(nil, map[string]interface{}{
		banexg.ParamInterval: "1s",
	})
	if err != nil {
		m.IsWatchPrice = false
		retryWaits.SetFail("watchPrices")
		log.Error("watch prices fail", zap.String("exg", m.ExgName), zap.Error(err))
		return
	}
	retryWaits.Reset("watchPrices")
	m.IsWatchPrice = true
	log.Info("start watch prices", zap.String("exg", m.ExgName))
	prefix := fmt.Sprintf("price_%s_%s", m.ExgName, m.Market)

	go func() {
		defer func() {
			m.IsWatchPrice = false
			retryWaits.SetFail("watchPrices")
			log.Info("watch prices stopped", zap.String("exg", m.ExgName))
		}()
		for item := range out {
			err = m.spider.Broadcast(&utils.IOMsg{
				Action: prefix,
				Data:   item,
			})
			if err != nil {
				log.Error("broadCast price fail", zap.String("key", prefix), zap.Error(err))
			}
		}
	}()
}

func (m *Miner) watchOdBooks(pairs []string) {
	pairs = m.Depths.GetNewSubs(pairs)
	if len(pairs) == 0 {
		return
	}
	out, err := m.exchange.WatchOrderBooks(pairs, 0, nil)
	if err != nil {
		m.Depths.Status = 0
		retryWaits.SetFail("watchOdBooks")
		log.Error("watch odBook fail", zap.String("exg", m.ExgName), zap.Error(err))
		return
	}
	retryWaits.Reset("watchOdBooks")
	if m.Depths.Status == 2 {
		return
	}
	m.Depths.Status = 2
	log.Info("start watch odBooks", zap.String("exg", m.ExgName), zap.Int("num", m.Depths.Len()))
	prefix := fmt.Sprintf("%s_%s_%s_", core.WsSubDepth, m.ExgName, m.Market)

	go func() {
		defer func() {
			m.Depths.Status = 0
			retryWaits.SetFail("watchOdBooks")
			log.Info("watch odBook stopped", zap.String("exg", m.ExgName))
		}()
		for {
			batch := utils.ReadChanBatch(out, false)
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
	pairs = m.KLines.GetNewSubs(pairs)
	if len(pairs) == 0 {
		return
	}
	jobs := make([][2]string, 0, len(pairs))
	timeFrame := "1m"
	tfSecs := utils2.TFToSecs(timeFrame)
	curTimeMS := btime.TimeMS()
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
		m.KLines.Status = 0
		retryWaits.SetFail("watchKLines")
		log.Error("watch kline fail", zap.String("exg", m.ExgName),
			zap.Strings("pairs", pairs), zap.Error(err))
		return
	}
	retryWaits.Reset("watchKLines")
	if m.KLines.Status == 2 {
		return
	}
	m.KLines.Status = 2
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
		err_ := m.spider.Broadcast(&utils.IOMsg{
			Action: unPrefix + pair,
			Data: NotifyKLines{
				TFSecs:   tfSecs,
				Interval: 1,
				Arr:      arr,
			},
		})
		curTS := btime.UTCStamp()
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
	go func() {
		defer func() {
			m.KLines.Status = 0
			retryWaits.SetFail("watchKLines")
			log.Info("watch kline stopped", zap.String("exg", m.ExgName))
		}()
		for {
			klines := utils.ReadChanBatch(out, false)
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
		startMs := utils2.AlignTfMSecs(bntp.UTCStamp(), 60000)
		for _, p := range newPairs {
			m.lockBarState.Lock()
			state, ok := m.klineStates[p]
			m.lockBarState.Unlock()
			if !ok {
				exs, err := orm.GetExSymbol(m.exchange, p)
				if err != nil {
					code := fmt.Sprintf("%s.%s.%s", m.ExgName, m.Market, p)
					log.Error("invalid symbol", zap.String("pair", code), zap.Error(err))
					continue
				}
				state = &KLineState{
					Sid:      exs.ID,
					ExpectMS: startMs,
				}
				m.lockBarState.Lock()
				m.klineStates[p] = state
				m.lockBarState.Unlock()
			}
		}
	}
	if !m.IsLoopKline {
		m.startLoopKLines()
	}
}

func (m *Miner) startLoopKLines() {
	if m.IsLoopKline {
		return
	}
	m.IsLoopKline = true
	mntMSecs := int64(60000)
	curTF := "1m"
	prefix := fmt.Sprintf("ohlcv_%s_%s_", m.ExgName, m.Market)
	// 刷新K线
	com.Cron().AddFunc("0 * * * * *", func() {
		pairs := m.KLineApis.KeyMap()
		if len(pairs) == 0 {
			return
		}
		curTimeMS := btime.UTCStamp()
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
			_ = utils.ParallelRun(pairArr, KlineParallelNum, func(i int, p string) *errs.Error {
				m.lockBarState.Lock()
				sta, _ := m.klineStates[p]
				m.lockBarState.Unlock()
				if sta == nil {
					log.Warn("no sta to writeQ", zap.String("pair", p))
					return nil
				}
				bars, err := m.exchange.FetchOHLCV(p, curTF, sta.ExpectMS, 0, nil)
				if err != nil {
					code := fmt.Sprintf("%s.%s.%s", m.ExgName, m.Market, p)
					log.Error("FetchOHLCV fail", zap.String("exg", code), zap.Error(err))
					return nil
				}
				if len(bars) > 0 {
					last := bars[len(bars)-1]
					if last.Time >= startMS {
						bars = bars[:len(bars)-1]
					}
					if len(bars) > 0 {
						barNum += len(bars)
						sta.ExpectMS = bars[len(bars)-1].Time + mntMSecs
						// There are completed k-lines, written to the database, and only then the message is broadcast
						// 有已完成的k线，写入到数据库，然后才广播消息
						sidLock.Lock()
						existingJob, exists := sidMap[sta.Sid]
						if exists {
							// Append to existing job's Arr
							existingJob.Arr = append(existingJob.Arr, bars...)
							sidLock.Unlock()
							log.Debug("kline appended to existing job", zap.Int32("sid", sta.Sid), zap.String("pair", p),
								zap.Int("num", len(bars)), zap.Int("total", len(existingJob.Arr)))
						} else {
							// Create new job and add to both sidMap and writeQ
							newJob := &SaveKline{
								Sid:       sta.Sid,
								TimeFrame: curTF,
								Arr:       bars,
								MsgAction: prefix + p,
								ReceiveAt: btime.UTCStamp(),
							}
							sidMap[sta.Sid] = newJob
							sidLock.Unlock()
							writeQ <- newJob
							log.Debug("kline to writeQ", zap.Int32("sid", sta.Sid), zap.String("pair", p),
								zap.Int64("time", last.Time), zap.Int("num", len(bars)))
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
			core.Sleep(time.Millisecond * delay)
			delay *= 2
		}
		fails := utils.KeysOfMap(pairs)
		log.Info(fmt.Sprintf("fetched kline %d/%d pairs, retried: %d, total kline: %d at %d, fails: %v",
			initNum-len(pairs), initNum, retry, barNum, curTimeMS, fails))
	})
}

func RunSpider(addr string) *errs.Error {
	server := utils.NewBanServer(addr, "")
	Spider = &LiveSpider{
		ServerIO: server,
		miners:   map[string]*Miner{},
	}
	server.InitConn = makeInitConn(Spider)
	go consumeWriteQ(5)
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	err = sess.PurgeKlineUn()
	if err != nil {
		conn.Release()
		return err
	}
	conn.Release()

	// Start the subscription monitor goroutine
	go Spider.monitorSubscriptions()

	com.Cron().Start()
	return Spider.RunForever(30, 600)
}

func (s *LiveSpider) getMiner(exgName, market string) *Miner {
	key := fmt.Sprintf("%s:%s", exgName, market)
	miner, ok := s.miners[key]
	var err *errs.Error
	if !ok {
		miner, err = newMiner(s, exgName, market)
		if err != nil {
			panic(err)
		}
		miner.init()
		//err = miner.SubPairs("price")
		//if err != nil {
		//	log.Error("sub prices fail", zap.Error(err))
		//}
		s.miners[key] = miner
		log.Info("start miner for", zap.String("e", exgName), zap.String("m", market))
	}
	return miner
}

func makeInitConn(s *LiveSpider) func(*utils.BanConn) {
	return func(c *utils.BanConn) {
		handlePairs := func(data []byte, name string) (*Miner, []string) {
			var arr = make([]string, 0, 8)
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
			err := miner.SubPairs(arr[0], arr[1:]...)
			if err != nil {
				log.Error("spider.sub_pairs fail", zap.Error(err))
			}
		}
		c.Listens["unwatch_pairs"] = func(msg *utils.IOMsgRaw) {
			miner, arr := handlePairs(msg.Data, "unwatch_pairs")
			err := miner.UnSubPairs(arr[0], arr[1:]...)
			if err != nil {
				log.Error("spider.unsub_pairs fail", zap.Error(err))
			}
		}
	}
}
