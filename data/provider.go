package data

import (
	"fmt"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/strat"
	"github.com/sasha-s/go-deadlock"
	"maps"
	"math"
	"sort"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type IProvider interface {
	LoopMain() *errs.Error
	SubWarmPairs(items map[string]map[string]int, delOther bool) *errs.Error
	UnSubPairs(pairs ...string) *errs.Error
	SetDirty()
}

type Provider[T IKlineFeeder] struct {
	holders   map[string]T
	newFeeder func(pair string, tfs []string) (T, *errs.Error)
	dirtyVers chan int
	dirtyLast int
	showLog   bool
}

func (p *Provider[IKlineFeeder]) UnSubPairs(pairs ...string) []string {
	var removed []string
	for _, pair := range pairs {
		if _, ok := p.holders[pair]; ok {
			delete(p.holders, pair)
			removed = append(removed, pair)
		}
	}
	return removed
}

func (p *Provider[IKlineFeeder]) SetDirty() {
	p.dirtyLast += 1
	p.dirtyVers <- p.dirtyLast
}

type WarmJob struct {
	hold    IKlineFeeder
	timeMS  int64
	tfWarms map[string]int
}

/*
SubWarmPairs
Add new trading pair subscription from data provider.

items: pair[timeFrame]warmNum
Return the trading pairs with the smallest period change (new/old pairs new period), warm-up tasks
从数据提供者添加新的交易对订阅。

	items: pair[timeFrame]warmNum
	返回最小周期变化的交易对(新增/旧对新周期)、预热任务
*/
func (p *Provider[IKlineFeeder]) SubWarmPairs(items map[string]map[string]int, delOther bool, pBar *utils.StagedPrg) ([]IKlineFeeder, map[string]int64, []string, *errs.Error) {
	var newHolds []IKlineFeeder
	var warmJobs []*WarmJob
	var oldSince = make(map[string]int64)
	var err *errs.Error
	for pair, tfWarms := range items {
		hold, ok := p.holders[pair]
		if !ok {
			hold, err = p.newFeeder(pair, utils.KeysOfMap(tfWarms))
			if err != nil {
				return nil, nil, nil, err
			}
			p.holders[pair] = hold
			newHolds = append(newHolds, hold)
			warmJobs = append(warmJobs, &WarmJob{hold: hold, tfWarms: tfWarms})
		} else {
			oldMinTf := hold.getStates()[0].TimeFrame
			newTfs := hold.SubTfs(utils.KeysOfMap(tfWarms), delOther)
			curMinTf := hold.getStates()[0].TimeFrame
			if oldMinTf != curMinTf {
				newHolds = append(newHolds, hold)
			} else {
				since, _ := oldSince[pair]
				oldSince[pair] = max(since, hold.getStates()[0].SubNextMS)
			}
			if len(newTfs) > 0 {
				warmJobs = append(warmJobs, &WarmJob{
					hold:    hold,
					tfWarms: utils.CutMap(tfWarms, newTfs...),
				})
			}
		}
	}
	var delPairs []string
	if delOther {
		for pair := range p.holders {
			if _, ok := items[pair]; !ok {
				delete(p.holders, pair)
				delPairs = append(delPairs, pair)
			}
		}
	}
	// 加载数据预热
	sinceMap, err := p.warmJobs(warmJobs, pBar)
	for key, since := range oldSince {
		sinceMap[key] = since
	}
	return newHolds, sinceMap, delPairs, err
}

func (p *Provider[IKlineFeeder]) warmJobs(warmJobs []*WarmJob, pb *utils.StagedPrg) (map[string]int64, *errs.Error) {
	sinceMap := make(map[string]int64)
	lockMap := deadlock.Mutex{}
	jobNum := 0
	// 预热所需的必要数据
	for _, job := range warmJobs {
		jobNum += len(job.tfWarms)
	}
	var pBar *utils.PrgBar
	if p.showLog {
		log.Info(fmt.Sprintf("warmup for %d pairs, %v jobs", len(warmJobs), jobNum))
		pBar = utils.NewPrgBar(jobNum*core.StepTotal, "warmup")
		defer pBar.Close()
		if pb != nil {
			pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
				pb.SetProgress("warmJobs", float64(done)/float64(total))
			})
		}
	}
	skipWarms := make(map[string][2]int)
	startTime := btime.TimeMS()
	// 这里不可使用并行预热，因预热过程会读写btime等全局变量，可能导致指标计算时repeat append on Series panic
	for _, job := range warmJobs {
		hold := job.hold
		if job.timeMS == 0 {
			job.timeMS = startTime
		}
		since, skips, err := hold.WarmTfs(job.timeMS, job.tfWarms, pBar)
		lockMap.Lock()
		sinceMap[hold.getSymbol()] = since
		for k, v := range skips {
			skipWarms[k] = v
		}
		lockMap.Unlock()
		if err != nil {
			return sinceMap, err
		}
	}
	if len(skipWarms) > 0 {
		log.Warn("warm lacks", zap.String("items", StrWarmLacks(skipWarms)))
	}
	return sinceMap, nil
}

type HistProvider struct {
	Provider[IHistKlineFeeder]
	getEnd    FnGetInt64
	maxTfSecs int
	pBar      *utils.StagedPrg

	wsLoader *WsDataLoader
	trades   map[string]*TradeFeeder
}

func NewHistProvider(callBack FnPairKline, envEnd FuncEnvEnd, getEnd FnGetInt64, showLog bool, pBar *utils.StagedPrg) *HistProvider {
	p := &HistProvider{
		Provider: Provider[IHistKlineFeeder]{
			holders: make(map[string]IHistKlineFeeder),
			newFeeder: func(pair string, tfs []string) (IHistKlineFeeder, *errs.Error) {
				exs, err := orm.GetExSymbolCur(pair)
				if err != nil {
					return nil, err
				}
				feeder, err := NewDBKlineFeeder(exs, callBack, showLog)
				if err != nil {
					return nil, err
				}
				feeder.OnEnvEnd = envEnd
				feeder.SubTfs(tfs, false)
				return feeder, nil
			},
			dirtyVers: make(chan int, 5),
			showLog:   showLog,
		},
		getEnd: getEnd,
		pBar:   pBar,
		trades: make(map[string]*TradeFeeder),
	}

	return p
}

func (p *HistProvider) downIfNeed() *errs.Error {
	exchange := exg.Default
	if !exchange.HasApi(banexg.ApiFetchOHLCV, core.Market) {
		return nil
	}
	var err *errs.Error
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()
	var pBar *utils.PrgBar
	if p.showLog {
		pBar = utils.NewPrgBar(len(p.holders)*core.StepTotal, "DownHist")
		defer pBar.Close()
		pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
			p.pBar.SetProgress("downKline", float64(done)/float64(total))
		})
	}
	for _, h := range p.holders {
		err = h.DownIfNeed(sess, exchange, pBar)
		if err != nil {
			log.Error("download ohlcv fail", zap.String("pair", h.getSymbol()), zap.Error(err))
			return err
		}
	}
	return nil
}

func (p *HistProvider) SubWarmPairs(items map[string]map[string]int, delOther bool) *errs.Error {
	newHolds, sinceMap, _, err := p.Provider.SubWarmPairs(items, delOther, p.pBar)
	if err != nil {
		return err
	}
	maxSince := int64(0)
	holders := make(map[string]IHistKlineFeeder)
	defSince := btime.TimeMS()
	for pair, since := range sinceMap {
		hold, ok := p.holders[pair]
		if !ok {
			continue
		}
		if since == 0 {
			since = defSince
		}
		holders[pair] = hold
		if hold.getNextMS() == 0 || hold.getStates()[0].SubNextMS != since {
			// Ignore here the targets that still exist after refreshing the trading pairs.
			// 这里忽略刷新交易对后，仍然存在的标的
			hold.SetSeek(since)
		}
		maxSince = max(maxSince, since)
	}
	// handle symbols whose minimal timeframe changed but do not require warming up
	// 处理最小周期变化，但无需预热的品种
	for _, hold := range newHolds {
		staArr := hold.getStates()
		last := staArr[len(staArr)-1]
		if last.TFSecs > p.maxTfSecs {
			p.maxTfSecs = last.TFSecs
		}
		pair := hold.getSymbol()
		if _, ok := holders[pair]; ok {
			continue
		}
		holders[pair] = hold
		sta := staArr[0]
		hold.SetSeek(sta.SubNextMS)
	}
	// 初始化高频数据订阅
	pairJobs, _ := strat.WsSubJobs[core.WsSubTrade]
	if len(pairJobs) > 0 {
		if p.wsLoader == nil {
			p.wsLoader, err = NewWsDataLoader()
			if err != nil {
				return err
			}
		}
		curMS := btime.TimeMS()
		tradeMap := maps.Clone(p.trades)
		for pair := range pairJobs {
			delete(tradeMap, pair)
			if _, ok := p.trades[pair]; ok {
				continue
			}
			exs, err := orm.GetExSymbolCur(pair)
			if err != nil {
				return err
			}
			feed := NewTradeFeeder(exs, p.wsLoader)
			feed.SetSeek(curMS)
			p.trades[pair] = feed
		}
		// 删除不再使用的
		for pair := range tradeMap {
			delete(p.trades, pair)
		}
	}
	// Delete items that are not warmed up
	// 删除未预热的项
	p.holders = holders
	btime.CurTimeMS = maxSince
	if p.getEnd != nil {
		// 结束时间推迟3个bar，以便触发下次品种刷新
		endMs := p.getEnd() + int64(p.maxTfSecs*1000*3)
		endMs = min(endMs, config.TimeRange.EndMS)
		for _, h := range holders {
			h.SetEndMS(endMs)
		}
		for _, h := range p.trades {
			h.SetEndMS(endMs)
		}
	}
	// Check whether the data needs to be downloaded during the backtest. If so, it will be downloaded automatically.
	// 检查回测期间数据是否需要下载，如需要自动下载
	err = p.downIfNeed()
	if err != nil {
		return err
	}
	return err
}

func (p *HistProvider) UnSubPairs(pairs ...string) *errs.Error {
	_ = p.Provider.UnSubPairs(pairs...)
	return nil
}

func (p *HistProvider) LoopMain() *errs.Error {
	if len(p.holders) == 0 {
		return errs.NewMsg(core.ErrBadConfig, "no pairs to run")
	}
	makeFeeders := func() []IHistFeeder {
		var feeders []IHistFeeder

		for _, klineFeeder := range p.holders {
			feeders = append(feeders, klineFeeder)
		}
		for _, tf := range p.trades {
			feeders = append(feeders, tf)
		}

		return feeders
	}
	totalMS := (config.TimeRange.EndMS - config.TimeRange.StartMS) / 1000
	var pBar = utils.NewPrgBar(int(totalMS), "RunHist")
	if p.pBar != nil {
		pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
			p.pBar.SetProgress("runBT", float64(done)/float64(total))
		})
	}
	defer pBar.Close()
	pBar.Last = config.TimeRange.StartMS
	if p.showLog {
		log.Info("run data loop for backtest..")
	}
	coreStop := core.StopAll
	core.StopAll = func() {
		p.Terminate()
		coreStop()
	}
	err := RunHistFeeders(makeFeeders, p.dirtyVers, pBar)
	core.StopAll = coreStop
	if p.pBar != nil {
		p.pBar.SetProgress("runBT", 1)
	}
	return err
}

func (p *HistProvider) Terminate() {
	p.dirtyVers <- -1
}

/*
RunHistFeeders run hist feeders for historical data

versions: When an integer greater than the previous value is received, makeFeeders will be called to re-acquire and continue running; when a negative number is received, exit immediately

pBar: optional, used to display a progress bar
*/
func RunHistFeeders(makeFeeders func() []IHistFeeder, versions chan int, pBar *utils.PrgBar) *errs.Error {
	var hold IHistFeeder
	var lastBarMs int64
	var oldVer int
	var holds []IHistFeeder
	var firstInit = true
	for {
		var ver = 0
		select {
		case ver = <-versions:
			if ver < 0 {
				return nil
			}
		default:
			ver = 0
		}
		if ver > oldVer || firstInit {
			holds = makeFeeders()
			holds = SortFeeders(holds, nil, false)
			oldVer = max(oldVer, ver)
			firstInit = false
		} else {
			holds = SortFeeders(holds, hold, true)
		}
		hold = holds[0]
		batch := hold.GetBatch()
		if batch == nil {
			break
		}
		hold.CallNext()
		holds = holds[1:]
		batchTime := batch.TimeMS()
		if batchTime > lastBarMs {
			// 更新进度条
			if pBar != nil {
				curMS := btime.TimeMS()
				if pBar.Last == 0 {
					pBar.Last = curMS
				} else if curMS > pBar.Last {
					pBarAdd := (curMS - pBar.Last) / 1000
					if pBarAdd > 0 {
						pBar.Add(int(pBarAdd))
						pBar.Last = curMS
					}
				}
			}
			lastBarMs = batchTime
		}
		// 这里不要使用多个goroutine加速，反而更慢，且导致多次回测结果略微差异
		err := hold.RunBatch(batch)
		if err != nil {
			return err
		}
	}
	return nil
}

func SortFeeders(holds []IHistFeeder, hold IHistFeeder, insert bool) []IHistFeeder {
	if insert {
		// 插入排序，说明holds已有序，二分查找位置，最快排序
		vb := hold.getNextMS()
		bSymbol := hold.getSymbol()
		index := sort.Search(len(holds), func(i int) bool {
			va := holds[i].getNextMS()
			if va != vb || va == math.MaxInt64 || vb == math.MaxInt64 {
				return va > vb
			}
			return holds[i].getSymbol() > bSymbol
		})
		holds = append(holds, hold)
		copy(holds[index+1:], holds[index:])
		holds[index] = hold
		return holds
	}
	sort.Slice(holds, func(i, j int) bool {
		a, b := holds[i], holds[j]
		va, vb := a.getNextMS(), b.getNextMS()
		if va != vb || va == math.MaxInt64 || vb == math.MaxInt64 {
			return va < vb
		}
		return a.getSymbol() < b.getSymbol()
	})
	return holds
}

type LiveProvider struct {
	Provider[IKlineFeeder]
	*KLineWatcher
	OnMinKlines func(msg *KLineMsg, bars []*banexg.Kline) *errs.Error
}

func NewLiveProvider(callBack FnPairKline, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error) {
	watcher, err := NewKlineWatcher(config.SpiderAddr)
	if err != nil {
		return nil, err
	}
	provider := &LiveProvider{
		Provider: Provider[IKlineFeeder]{
			holders: make(map[string]IKlineFeeder),
			newFeeder: func(pair string, tfs []string) (IKlineFeeder, *errs.Error) {
				exs, err := orm.GetExSymbol(exg.Default, pair)
				if err != nil {
					return nil, err
				}
				feeder, err := NewKlineFeeder(exs, callBack, true)
				if err != nil {
					return nil, err
				}
				feeder.SubTfs(tfs, false)
				feeder.OnEnvEnd = envEnd
				return feeder, nil
			},
			dirtyVers: make(chan int, 5),
		},
		KLineWatcher: watcher,
	}
	watcher.OnKLineMsg = makeOnKlineMsg(provider)
	watcher.OnTrades = makeOnTrade(provider)
	watcher.OnDepth = makeOnDepth(provider)
	// 立刻订阅实时价格
	//err = watcher.SendMsg("subscribe", []string{
	//	fmt.Sprintf("price_%s_%s", core.ExgName, core.Market),
	//})
	//if err != nil {
	//	return nil, err
	//}
	return provider, nil
}

func (p *LiveProvider) SubWarmPairs(items map[string]map[string]int, delOther bool) *errs.Error {
	newHolds, sinceMap, delPairs, err := p.Provider.SubWarmPairs(items, delOther, nil)
	if err != nil {
		return err
	}
	if len(newHolds) > 0 {
		var jobs []WatchJob
		var down1mPairs = make(map[int32]*orm.ExSymbol)
		var minSince = btime.UTCStamp()
		for _, h := range newHolds {
			sta := h.getStates()[0]
			symbol := h.getSymbol()
			if since, ok := sinceMap[symbol]; ok {
				jobs = append(jobs, WatchJob{
					Symbol:    symbol,
					TimeFrame: sta.TimeFrame,
					Since:     since,
				})
				minSince = min(minSince, since)
			}
			if sta.TFSecs >= 3600 {
				exs, err := orm.GetExSymbolCur(symbol)
				if err != nil {
					return err
				}
				down1mPairs[exs.ID] = exs
			}
		}
		if len(down1mPairs) > 0 {
			// 对1h及以上大周期，也需要对1m的K线数据提前下载到最新，避免spider下载耗时过久
			exchange, err := exg.GetWith(core.ExgName, core.Market, "")
			if err != nil {
				return err
			}
			err = orm.BulkDownOHLCV(exchange, down1mPairs, "1m", minSince, btime.UTCStamp(), 0, nil)
			if err != nil {
				return err
			}
		}
		err = p.WatchJobs(core.ExgName, core.Market, "ohlcv", jobs...)
		if err != nil {
			return err
		}
		for msgType, pairMap := range strat.WsSubJobs {
			jobs = make([]WatchJob, 0, len(pairMap))
			for pair := range pairMap {
				jobs = append(jobs, WatchJob{Symbol: pair, TimeFrame: "1m"})
			}
			err = p.WatchJobs(core.ExgName, core.Market, msgType, jobs...)
			if err != nil {
				return err
			}
		}
	}
	if len(delPairs) > 0 {
		err = p.UnWatchJobs(core.ExgName, core.Market, "ohlcv", delPairs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *LiveProvider) UnSubPairs(pairs ...string) *errs.Error {
	removed := p.Provider.UnSubPairs(pairs...)
	if len(removed) == 0 {
		return nil
	}
	return p.UnWatchJobs(core.ExgName, core.Market, "ohlcv", pairs)
}

func (p *LiveProvider) LoopMain() *errs.Error {
	return p.RunForever()
}

func makeOnKlineMsg(p *LiveProvider) func(msg *KLineMsg) {
	return func(msg *KLineMsg) {
		if msg.ExgName != core.ExgName || msg.Market != core.Market {
			return
		}
		if msg.Interval < msg.TFSecs {
			fireWsKlines(msg)
		}
		hold, ok := p.holders[msg.Pair]
		if !ok {
			return
		}
		tfMSecs := int64(msg.TFSecs * 1000)
		handleNewBars := func(bars_ []*banexg.Kline) {
			go func(bars []*banexg.Kline) {
				_, err := hold.onNewBars(tfMSecs, bars)
				if err != nil {
					log.Error("onNewBars fail", zap.String("p", msg.Pair), zap.Error(err))
				}
				if p.OnMinKlines != nil {
					err = p.OnMinKlines(msg, bars)
					if err != nil {
						log.Error("OnMinKlines fail", zap.String("p", msg.Pair), zap.Error(err))
					}
				}
			}(bars_)
		}
		// The weighting factor has been calculated during the start-up or market break, and the weighting is automatically carried out internally
		// 已在启动或休市期间计算复权因子，内部会自动进行复权
		if msg.Interval >= msg.TFSecs {
			handleNewBars(msg.Arr)
			return
		}
		// The frequency of updates is lower than the bar cycle, and what is received may not be completed
		// 更新频率低于bar周期，收到的可能未完成
		lastIdx := len(msg.Arr) - 1
		doneArr, lastBar := msg.Arr[:lastIdx], msg.Arr[lastIdx]
		waitBar := hold.getWaitBar()
		if waitBar != nil && waitBar.Time < lastBar.Time {
			doneArr = append([]*banexg.Kline{waitBar}, doneArr...)
			hold.setWaitBar(nil)
		}
		if len(doneArr) > 0 {
			handleNewBars(doneArr)
			return
		}
		if msg.Interval <= 5 && hold.getStates()[0].TFSecs >= 60 {
			// The update is fast, and the cycle required is relatively long, so it is required to be considered complete when the next bar occurs (follow the above logic)
			// 更新很快，需要的周期相对较长，则要求出现下一个bar时认为完成（走上面逻辑）
			hold.setWaitBar(lastBar)
			return
		}
		// The frequency of updates is relatively low, or the proportion of the required cycle is large, and the approximate completion is considered complete
		// 更新频率相对不高，或占需要的周期比率较大，近似完成认为完成
		endLackSecs := int((lastBar.Time + tfMSecs - btime.TimeMS()) / 1000)
		if endLackSecs*2 < msg.Interval {
			// The missing time is less than half of the update interval and is considered complete.
			// 缺少的时间不足更新间隔的一半，认为完成。
			handleNewBars([]*banexg.Kline{lastBar})
		} else {
			hold.setWaitBar(lastBar)
		}
	}
}

func makeOnTrade(p *LiveProvider) func(exgName, market, pair string, trades []*banexg.Trade) {
	return func(exgName, market, pair string, trades []*banexg.Trade) {
		pairMap, _ := strat.WsSubJobs[core.WsSubTrade]
		if len(pairMap) == 0 || len(trades) == 0 {
			return
		}
		jobMap, _ := pairMap[pair]
		for job := range jobMap {
			num1, num2 := strat.GetJobInOutNum(job)
			job.Strat.OnWsTrades(job, pair, trades)
			strat.CheckJobInOutNum(job, "OnWsTrades", num1, num2)
		}
	}
}

func makeOnDepth(p *LiveProvider) func(dep *banexg.OrderBook) {
	return func(dep *banexg.OrderBook) {
		pairMap, _ := strat.WsSubJobs[core.WsSubDepth]
		if len(pairMap) == 0 {
			return
		}
		jobMap, _ := pairMap[dep.Symbol]
		for job := range jobMap {
			num1, num2 := strat.GetJobInOutNum(job)
			job.Strat.OnWsDepth(job, dep)
			strat.CheckJobInOutNum(job, "OnWsDepth", num1, num2)
		}
	}
}

func fireWsKlines(msg *KLineMsg) {
	if len(msg.Arr) == 0 {
		return
	}
	last := msg.Arr[len(msg.Arr)-1]
	if _, ok := core.GetOdBook(msg.Pair); !ok {
		com.SetPrice(msg.Pair, last.Close, last.Close)
	}
	pairMap, _ := strat.WsSubJobs[core.WsSubKLine]
	if len(pairMap) == 0 {
		return
	}
	jobMap, _ := pairMap[msg.Pair]
	for job := range jobMap {
		num1, num2 := strat.GetJobInOutNum(job)
		job.Strat.OnWsKline(job, msg.Pair, last)
		strat.CheckJobInOutNum(job, "OnWsKline", num1, num2)
	}
}
