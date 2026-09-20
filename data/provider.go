package data

import (
	"cmp"
	"container/heap"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/strat"
	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type IProvider interface {
	LoopMain() *errs.Error
	SubWarmPairs(items map[string]map[string]int, delOther bool) *errs.Error
	UnSubPairs(pairs ...string) *errs.Error
	SetDirty()
}

func resolveExSymbolCur(state *orm.SymbolState, pair string) (*orm.ExSymbol, *errs.Error) {
	if state == nil {
		return orm.GetExSymbolCur(pair)
	}
	return state.GetExSymbolCur(pair)
}

func resolveExSymbol(state *orm.SymbolState, exchange banexg.BanExchange, pair string) (*orm.ExSymbol, *errs.Error) {
	if state == nil {
		return orm.GetExSymbol(exchange, pair)
	}
	return state.GetExSymbol(exchange, pair)
}

func getExSymbol2(state *orm.SymbolState, exchange, market, pair string) *orm.ExSymbol {
	if state == nil {
		return orm.GetExSymbol2(exchange, market, pair)
	}
	return state.GetExSymbol2(exchange, market, pair)
}

func resolveExSymbolCurWithRuntimeDeps(deps *RuntimeDeps, state *orm.SymbolState, pair string) (*orm.ExSymbol, *errs.Error) {
	if deps != nil {
		if state == nil {
			return nil, errs.NewMsg(core.ErrInvalidSymbol, "%s not found in runtime symbol state", pair)
		}
		return state.GetExSymbolCur(pair)
	}
	return resolveExSymbolCur(state, pair)
}

func resolveExSymbolWithRuntimeDeps(deps *RuntimeDeps, state *orm.SymbolState, exchange banexg.BanExchange, pair string) (*orm.ExSymbol, *errs.Error) {
	if deps != nil {
		if state == nil {
			return nil, errs.NewMsg(core.ErrInvalidSymbol, "%s not found in runtime symbol state", pair)
		}
		return state.GetExSymbol(exchange, pair)
	}
	return resolveExSymbol(state, exchange, pair)
}

func getExSymbol2WithRuntimeDeps(deps *RuntimeDeps, state *orm.SymbolState, exchange, market, pair string) *orm.ExSymbol {
	if deps != nil && state == nil {
		return nil
	}
	return getExSymbol2(state, exchange, market, pair)
}

type Provider[T IDataFeeder] struct {
	holders   map[string]T
	holdersMu sync.RWMutex
	opMu      sync.Mutex
	newFeeder func(pair string, tfs []string) (T, *errs.Error)
	dirtyVers chan int
	dirtyLast int
	showLog   bool
	deps      *RuntimeDeps
	wsSubs    *strat.WsSubJobRegistry
}

func (p *Provider[T]) wsRegistry() *strat.WsSubJobRegistry {
	if p != nil && p.wsSubs != nil {
		return p.wsSubs
	}
	if p != nil && p.deps != nil {
		return nil
	}
	return strat.LegacyWsSubJobRegistry()
}

func (p *Provider[T]) getHolder(pair string) (T, bool) {
	p.holdersMu.RLock()
	hold, ok := p.holders[pair]
	p.holdersMu.RUnlock()
	return hold, ok
}

func (p *Provider[T]) holderSnapshot() map[string]T {
	p.holdersMu.RLock()
	holders := maps.Clone(p.holders)
	p.holdersMu.RUnlock()
	return holders
}

func (p *Provider[T]) setHolder(pair string, hold T) {
	p.holdersMu.Lock()
	p.holders[pair] = hold
	p.holdersMu.Unlock()
}

func (p *Provider[T]) deleteHolder(pair string) {
	p.holdersMu.Lock()
	delete(p.holders, pair)
	p.holdersMu.Unlock()
}

func (p *Provider[T]) replaceHolders(holders map[string]T) {
	p.holdersMu.Lock()
	p.holders = holders
	p.holdersMu.Unlock()
}

func (p *Provider[IDataFeeder]) UnSubPairs(pairs ...string) []string {
	p.opMu.Lock()
	defer p.opMu.Unlock()
	return p.unSubPairs(pairs...)
}

func (p *Provider[IDataFeeder]) unSubPairs(pairs ...string) []string {
	var removed []string
	for _, pair := range pairs {
		if _, ok := p.getHolder(pair); ok {
			p.deleteHolder(pair)
			removed = append(removed, pair)
		}
	}
	return removed
}

func (p *Provider[IDataFeeder]) SetDirty() {
	p.dirtyLast += 1
	p.dirtyVers <- p.dirtyLast
}

type WarmJob struct {
	hold    IDataFeeder
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
func (p *Provider[IDataFeeder]) SubWarmPairs(items map[string]map[string]int, delOther bool, pBar *utils.StagedPrg) ([]IDataFeeder, map[string]int64, []string, *errs.Error) {
	p.opMu.Lock()
	defer p.opMu.Unlock()
	return p.subWarmPairs(items, delOther, pBar)
}

func (p *Provider[IDataFeeder]) subWarmPairs(items map[string]map[string]int, delOther bool, pBar *utils.StagedPrg) ([]IDataFeeder, map[string]int64, []string, *errs.Error) {
	registry := p.wsRegistry()
	if registry == nil && p != nil && p.deps != nil {
		return nil, nil, nil, errs.NewMsg(core.ErrBadConfig, "explicit data provider websocket registry is required")
	}
	registry.Refresh()
	var newHolds []IDataFeeder
	var warmJobs []*WarmJob
	var oldSince = make(map[string]int64)
	var err *errs.Error
	for _, pair := range slices.Sorted(maps.Keys(items)) {
		tfWarms := items[pair]
		hold, ok := p.getHolder(pair)
		if !ok {
			hold, err = p.newFeeder(pair, sortedTimeframes(tfWarms))
			if err != nil {
				return nil, nil, nil, err
			}
			p.setHolder(pair, hold)
			newHolds = append(newHolds, hold)
			warmJobs = append(warmJobs, &WarmJob{hold: hold, tfWarms: tfWarms})
		} else {
			oldMinTf := hold.getStates()[0].TimeFrame
			newTfs := hold.SubTfs(sortedTimeframes(tfWarms), delOther)
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
		for _, pair := range slices.Sorted(maps.Keys(p.holderSnapshot())) {
			if _, ok := items[pair]; !ok {
				p.deleteHolder(pair)
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

func sortedTimeframes(items map[string]int) []string {
	timeframes := slices.Sorted(maps.Keys(items))
	slices.SortFunc(timeframes, func(a, b string) int {
		if order := cmp.Compare(utils2.TFToSecs(a), utils2.TFToSecs(b)); order != 0 {
			return order
		}
		return cmp.Compare(a, b)
	})
	return timeframes
}

func (p *Provider[IDataFeeder]) warmJobs(warmJobs []*WarmJob, pb *utils.StagedPrg) (map[string]int64, *errs.Error) {
	sinceMap := make(map[string]int64)
	lockMap := deadlock.Mutex{}
	jobNum := 0
	// 预热所需的必要数据
	for _, job := range warmJobs {
		jobNum += len(job.tfWarms)
	}
	var pBar *utils.PrgBar
	if p.showLog {
		p.deps.logger().Info(fmt.Sprintf("warmup for %d pairs, %v jobs", len(warmJobs), jobNum))
		pBar = utils.NewPrgBar(jobNum*core.StepTotal, "warmup")
		defer pBar.Close()
		if pb != nil {
			pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
				pb.SetProgress("warmJobs", float64(done)/float64(total))
			})
		}
	}
	skipWarms := make(map[string][2]int)
	var startTime int64
	if p.deps == nil {
		startTime = btime.TimeMS()
	} else {
		startTime = p.deps.timeMS()
	}
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
		p.deps.logger().Warn("warm lacks", zap.String("items", StrWarmLacks(skipWarms)))
	}
	return sinceMap, nil
}

type HistProvider struct {
	Provider[IHistDataFeeder]
	deps          *RuntimeDeps
	catalog       *DataSourceCatalog
	symbols       *orm.SymbolState
	getEnd        FnGetInt64
	maxTfSecs     int
	pBar          *utils.StagedPrg
	allowDownload bool
	series        map[string]*HistSeriesFeeder
	seriesCB      FnDataSeries
	seriesRepo    orm.SeriesRepo

	wsLoader *WsDataLoader
	trades   map[string]*TradeFeeder
}

func (p *HistProvider) DataSourceCatalog() *DataSourceCatalog {
	if p == nil {
		return nil
	}
	return p.catalog
}

func NewHistProvider(callBack FnDataSeries, envEnd FuncEnvEnd, getEnd FnGetInt64, showLog bool, pBar *utils.StagedPrg) *HistProvider {
	return newHistProvider(nil, nil, callBack, envEnd, getEnd, showLog, pBar)
}

func NewHistProviderWithCatalog(catalog *DataSourceCatalog, symbols *orm.SymbolState, callBack FnDataSeries, envEnd FuncEnvEnd, getEnd FnGetInt64, showLog bool, pBar *utils.StagedPrg) *HistProvider {
	return newHistProviderWithCatalog(nil, symbols, catalog, callBack, envEnd, getEnd, showLog, pBar)
}

// NewHistProviderWithSymbolState binds symbol resolution and subscription
// bookkeeping to one runtime. A nil state preserves the legacy facade.
func NewHistProviderWithSymbolState(symbols *orm.SymbolState, callBack FnDataSeries, envEnd FuncEnvEnd, getEnd FnGetInt64, showLog bool, pBar *utils.StagedPrg) *HistProvider {
	return newHistProvider(nil, symbols, callBack, envEnd, getEnd, showLog, pBar)
}

// NewHistProviderWithRuntimeDeps binds all runtime-owned data dependencies to
// the provider and every feeder it creates.
func NewHistProviderWithRuntimeDeps(deps *RuntimeDeps, callBack FnDataSeries, envEnd FuncEnvEnd, getEnd FnGetInt64, showLog bool, pBar *utils.StagedPrg) (*HistProvider, *errs.Error) {
	if deps == nil || deps.Strategies == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "historical provider requires explicit strategy state")
	}
	return newHistProviderWithCatalog(deps, deps.Symbols, deps.Catalog, callBack, envEnd, getEnd, showLog, pBar), nil
}

func newHistProvider(deps *RuntimeDeps, symbols *orm.SymbolState, callBack FnDataSeries, envEnd FuncEnvEnd,
	getEnd FnGetInt64, showLog bool, pBar *utils.StagedPrg) *HistProvider {
	catalog := legacyDataSourceCatalog
	if deps != nil {
		catalog = deps.Catalog
	}
	return newHistProviderWithCatalog(deps, symbols, catalog, callBack, envEnd, getEnd, showLog, pBar)
}

func newHistProviderWithCatalog(deps *RuntimeDeps, symbols *orm.SymbolState, catalog *DataSourceCatalog, callBack FnDataSeries, envEnd FuncEnvEnd,
	getEnd FnGetInt64, showLog bool, pBar *utils.StagedPrg) *HistProvider {
	var wsSubs *strat.WsSubJobRegistry
	if deps != nil {
		wsSubs = strat.NewWsSubJobRegistryWithState(deps.Strategies, symbols)
	} else {
		wsSubs = strat.NewWsSubJobRegistry(symbols)
	}
	if deps == nil && symbols == nil {
		wsSubs = strat.LegacyWsSubJobRegistry()
	}
	var seriesRepo orm.SeriesRepo
	if deps != nil {
		seriesRepo = orm.NewSeriesRepo(deps.storage())
	} else {
		seriesRepo = orm.DefaultSeriesRepo()
	}
	p := &HistProvider{
		Provider: Provider[IHistDataFeeder]{
			holders: make(map[string]IHistDataFeeder),
			newFeeder: func(pair string, tfs []string) (IHistDataFeeder, *errs.Error) {
				exs, err := resolveExSymbolCurWithRuntimeDeps(deps, symbols, pair)
				if err != nil {
					return nil, err
				}
				var feeder *DBSeriesFeeder
				if deps == nil {
					feeder, err = NewDBSeriesFeederWithSymbolState(symbols, exs, callBack, showLog)
				} else {
					feeder, err = NewDBSeriesFeederWithRuntimeDeps(deps, exs, callBack, showLog)
				}
				if err != nil {
					return nil, err
				}
				feeder.OnEnvEnd = envEnd
				feeder.SubTfs(tfs, false)
				return feeder, nil
			},
			dirtyVers: make(chan int, 5),
			showLog:   showLog,
			deps:      deps,
			wsSubs:    wsSubs,
		},
		deps:          deps,
		catalog:       catalog,
		getEnd:        getEnd,
		symbols:       symbols,
		pBar:          pBar,
		allowDownload: true,
		trades:        make(map[string]*TradeFeeder),
		series:        make(map[string]*HistSeriesFeeder),
		seriesCB:      callBack,
		seriesRepo:    seriesRepo,
	}

	return p
}

func (p *HistProvider) SetAllowDownload(allow bool) {
	p.allowDownload = allow
}

func (p *HistProvider) SetSeriesSubs(subs []*strat.DataSub) *errs.Error {
	if len(subs) == 0 {
		p.series = make(map[string]*HistSeriesFeeder)
		return nil
	}
	var timeRange *config.TimeTuple
	if p.deps == nil {
		timeRange = config.TimeRange
	} else {
		timeRange = p.deps.timeRange()
	}
	if timeRange == nil {
		return errs.NewMsg(core.ErrBadConfig, "time range is required for historical series subscriptions")
	}
	items := make(map[string]*HistSeriesFeeder)
	var err error
	for _, sub := range subs {
		if sub == nil || sub.ExSymbol == nil || orm.NormalizeSeriesSource(sub.Source) == orm.SeriesSourceKline {
			continue
		}
		src := p.catalog.GetDataSource(sub.Source)
		if src == nil {
			return errs.NewMsg(core.ErrBadConfig, "data source %q is not registered", sub.Source)
		}
		info := src.Info()
		if info == nil {
			return errs.NewMsg(core.ErrBadConfig, "data source %q has no series info", sub.Source)
		}
		if sub.TimeFrame != info.TimeFrame {
			return errs.NewMsg(core.ErrBadConfig, "sub timeframe %s does not match source timeframe %s", sub.TimeFrame, info.TimeFrame)
		}
		key := strat.DataSubKey(info.Name, sub.ExSymbol.ID, info.TimeFrame)
		var feeder *HistSeriesFeeder
		if p.deps == nil {
			feeder, err = NewHistSeriesFeeder(p.seriesRepo, info, sub, p.seriesCB, timeRange.StartMS)
		} else {
			feeder, err = NewHistSeriesFeederWithRuntimeDeps(p.deps, p.seriesRepo, info, sub, p.seriesCB, timeRange.StartMS)
		}
		if err != nil {
			return errs.New(core.ErrBadConfig, err)
		}
		if old := p.series[key]; old != nil && old.sameProjection(feeder) {
			old.SetEndMS(timeRange.EndMS)
			items[key] = old
			continue
		}
		startMS, err_ := ThirdPartyWarmupStart([]*strat.DataSub{sub}, timeRange.StartMS)
		if err_ != nil {
			return errs.New(core.ErrBadConfig, err_)
		}
		var curMS int64
		if p.deps == nil {
			curMS = btime.TimeMS()
		} else {
			curMS = p.deps.timeMS()
		}
		if curMS > timeRange.StartMS {
			startMS, err_ = ThirdPartyWarmupStart([]*strat.DataSub{sub}, curMS)
			if err_ != nil {
				return errs.New(core.ErrBadConfig, err_)
			}
			feeder.warmEndMS = curMS
			feeder.SetEndMS(curMS)
			feeder.SetSeek(startMS)
			err := drainHistSeriesFeeder(feeder)
			if p.deps == nil {
				btime.CurTimeMS = curMS
			} else {
				p.deps.setTimeMS(curMS)
			}
			if err != nil {
				return err
			}
			feeder.warmEndMS = timeRange.StartMS
			startMS = curMS
		}
		feeder.SetEndMS(timeRange.EndMS)
		feeder.SetSeek(startMS)
		if feeder.loadErr != nil {
			return feeder.loadErr
		}
		items[key] = feeder
	}
	p.series = items
	return nil
}

func drainHistSeriesFeeder(feeder *HistSeriesFeeder) *errs.Error {
	for {
		batch := feeder.GetBatch()
		if batch == nil {
			return nil
		}
		feeder.CallNext()
		if err := feeder.RunBatch(batch); err != nil {
			return err
		}
	}
}

func (p *HistProvider) downIfNeed() *errs.Error {
	if !p.allowDownload {
		return nil
	}
	var exchange banexg.BanExchange
	var market string
	if p.deps == nil {
		exchange = exg.Default
		market = core.Market
	} else {
		exchange = p.deps.exchange()
		_, market = p.deps.identity()
		if exchange == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime exchange is required")
		}
	}
	if !exchange.HasApi(banexg.ApiFetchOHLCV, market) {
		return nil
	}
	var err *errs.Error
	var sess *orm.Queries
	var conn *pgxpool.Conn
	if p.deps == nil {
		sess, conn, err = orm.Conn(nil)
	} else {
		sess, conn, err = p.deps.conn()
	}
	if err != nil {
		return err
	}
	defer conn.Release()
	var pBar *utils.PrgBar
	holders := p.holderSnapshot()
	if p.showLog {
		pBar = utils.NewPrgBar(len(holders)*core.StepTotal, "DownHist")
		defer pBar.Close()
		if p.pBar != nil {
			pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
				p.pBar.SetProgress("downKline", float64(done)/float64(total))
			})
		}
	}
	for _, h := range holders {
		err = h.DownIfNeed(sess, exchange, pBar)
		if err != nil {
			p.deps.logger().Error("download ohlcv fail", zap.String("pair", h.getSymbol()), zap.Error(err))
			return err
		}
	}
	return nil
}

func (p *HistProvider) SubWarmPairs(items map[string]map[string]int, delOther bool) *errs.Error {
	p.opMu.Lock()
	defer p.opMu.Unlock()
	newHolds, sinceMap, delPairs, err := p.subWarmPairs(items, delOther, p.pBar)
	if err != nil {
		return err
	}
	registry := p.wsRegistry()
	maxSince := int64(0)
	holders := make(map[string]IHistDataFeeder)
	var defSince int64
	if p.deps == nil {
		defSince = btime.TimeMS()
	} else {
		defSince = p.deps.timeMS()
	}
	needSeek := make(map[string]int64)
	for pair, since := range sinceMap {
		hold, ok := p.getHolder(pair)
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
			needSeek[pair] = since
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
		needSeek[pair] = sta.SubNextMS
	}
	// 初始化高频数据订阅
	pairJobs := registry.Pairs(core.WsSubTrade)
	if len(pairJobs) > 0 {
		if p.wsLoader == nil {
			if p.deps == nil {
				p.wsLoader, err = NewWsDataLoader()
			} else {
				p.wsLoader, err = NewWsDataLoaderWithRuntimeDeps(p.deps)
			}
			if err != nil {
				return err
			}
		}
		var curMS int64
		if p.deps == nil {
			curMS = btime.TimeMS()
		} else {
			curMS = p.deps.timeMS()
		}
		tradeMap := maps.Clone(p.trades)
		for _, pair := range pairJobs {
			delete(tradeMap, pair)
			if _, ok := p.trades[pair]; ok {
				continue
			}
			exs, err := resolveExSymbolCurWithRuntimeDeps(p.deps, p.symbols, pair)
			if err != nil {
				return err
			}
			var feed *TradeFeeder
			feed = newTradeFeeder(p.deps, registry, exs, p.wsLoader)
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
	p.replaceHolders(holders)
	if p.deps == nil {
		btime.CurTimeMS = maxSince
	} else {
		p.deps.setTimeMS(maxSince)
	}
	if p.getEnd != nil {
		// 结束时间推迟3个bar，以便触发下次品种刷新
		endMs := p.getEnd() + int64(p.maxTfSecs*1000*3)
		var timeRange *config.TimeTuple
		if p.deps == nil {
			timeRange = config.TimeRange
		} else {
			timeRange = p.deps.timeRange()
		}
		if timeRange == nil {
			return errs.NewMsg(core.ErrBadConfig, "time range is required for historical provider")
		}
		endMs = min(endMs, timeRange.EndMS)
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
	// After data is ensured in DB, initialize/refresh loaders.
	for pair, since := range needSeek {
		hold, ok := p.getHolder(pair)
		if !ok {
			continue
		}
		hold.SetSeek(since)
	}
	if len(newHolds) > 0 || len(delPairs) > 0 {
		p.SetDirty()
	}
	return err
}

func (p *HistProvider) UnSubPairs(pairs ...string) *errs.Error {
	p.opMu.Lock()
	defer p.opMu.Unlock()
	_ = p.unSubPairs(pairs...)
	return nil
}

func (p *HistProvider) LoopMain() *errs.Error {
	if len(p.holderSnapshot()) == 0 && len(p.series) == 0 && len(p.trades) == 0 {
		return errs.NewMsg(core.ErrBadConfig, "no pairs to run")
	}
	var timeRange *config.TimeTuple
	if p.deps == nil {
		timeRange = config.TimeRange
	} else {
		timeRange = p.deps.timeRange()
	}
	if timeRange == nil {
		return errs.NewMsg(core.ErrBadConfig, "time range is required for historical provider")
	}
	totalMS := (timeRange.EndMS - timeRange.StartMS) / 1000
	var pBar = utils.NewPrgBar(int(totalMS), "RunHist")
	if p.pBar != nil {
		pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
			p.pBar.SetProgress("runBT", float64(done)/float64(total))
		})
	}
	defer pBar.Close()
	pBar.Last = timeRange.StartMS
	if p.showLog {
		p.deps.logger().Info("run data loop for backtest..")
	}
	err := runHistFeedersWithRuntimeDeps(p.deps, p.makeFeeders, p.dirtyVers, pBar)
	if p.pBar != nil {
		p.pBar.SetProgress("runBT", 1)
	}
	return err
}

func (p *HistProvider) makeFeeders() []IHistFeeder {
	holders := p.holderSnapshot()
	feeders := make([]IHistFeeder, 0, len(holders)+len(p.trades)+len(p.series))
	for _, key := range slices.Sorted(maps.Keys(holders)) {
		feeders = append(feeders, holders[key])
	}
	for _, key := range slices.Sorted(maps.Keys(p.trades)) {
		feeders = append(feeders, p.trades[key])
	}
	for _, key := range slices.Sorted(maps.Keys(p.series)) {
		feeders = append(feeders, p.series[key])
	}
	return feeders
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
	return runHistFeedersWithRuntimeDeps(nil, makeFeeders, versions, pBar)
}

// RunHistFeedersWithRuntimeDeps replays historical feeders using one explicit
// runtime's clock and cancellation state.
func RunHistFeedersWithRuntimeDeps(deps *RuntimeDeps, makeFeeders func() []IHistFeeder, versions chan int, pBar *utils.PrgBar) *errs.Error {
	if deps == nil {
		return errs.NewMsg(core.ErrBadConfig, "historical feeder replay requires explicit runtime dependencies")
	}
	return runHistFeedersWithRuntimeDeps(deps, makeFeeders, versions, pBar)
}

func runHistFeedersWithRuntimeDeps(deps *RuntimeDeps, makeFeeders func() []IHistFeeder, versions chan int, pBar *utils.PrgBar) *errs.Error {
	var lastBarMs int64
	var oldVer int
	var holds histFeederHeap
	var nextOrder uint64
	var firstInit = true
	for {
		if deps != nil && deps.Core != nil {
			select {
			case <-deps.Core.Done():
				return nil
			default:
			}
		}
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
			holds, nextOrder = initHistFeederHeap(makeFeeders, nextOrder)
			oldVer = max(oldVer, ver)
			firstInit = false
		}
		if holds.Len() == 0 {
			break
		}
		item := heap.Pop(&holds).(histFeederHeapItem)
		hold := item.feeder
		batch := hold.GetBatch()
		if batch == nil {
			break
		}
		hold.CallNext()
		batchTime := batch.TimeMS()
		lastBarMs = updateHistReplayProgress(pBar, lastBarMs, batchTime, deps)
		// 这里不要使用多个goroutine加速，反而更慢，且导致多次回测结果略微差异
		err := hold.RunBatch(batch)
		if err != nil {
			return err
		}
		item.order = nextOrder
		nextOrder++
		heap.Push(&holds, item)
	}
	return nil
}

func initHistFeederHeap(makeFeeders func() []IHistFeeder, nextOrder uint64) (histFeederHeap, uint64) {
	feeders := makeFeeders()
	holds := make(histFeederHeap, len(feeders))
	for i, feeder := range feeders {
		holds[i] = histFeederHeapItem{feeder: feeder, order: nextOrder}
		nextOrder++
	}
	heap.Init(&holds)
	return holds, nextOrder
}

func updateHistReplayProgress(pBar *utils.PrgBar, lastBarMs, batchTime int64, deps *RuntimeDeps) int64 {
	if batchTime <= lastBarMs {
		return lastBarMs
	}
	if pBar != nil {
		curMS := btime.TimeMS()
		if deps != nil {
			curMS = deps.timeMS()
		}
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
	return batchTime
}

type histFeederHeapItem struct {
	feeder IHistFeeder
	order  uint64
}

type histFeederHeap []histFeederHeapItem

func (h histFeederHeap) Len() int {
	return len(h)
}

func (h histFeederHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	aMS, bMS := a.feeder.getNextMS(), b.feeder.getNextMS()
	if aMS != bMS {
		return aMS < bMS
	}
	if aMS != math.MaxInt64 {
		aSymbol, bSymbol := a.feeder.getSymbol(), b.feeder.getSymbol()
		if aSymbol != bSymbol {
			return aSymbol < bSymbol
		}
	}
	return a.order < b.order
}

func (h histFeederHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *histFeederHeap) Push(value any) {
	*h = append(*h, value.(histFeederHeapItem))
}

func (h *histFeederHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = histFeederHeapItem{}
	*h = old[:last]
	return item
}

type LiveProvider struct {
	Provider[IDataFeeder]
	deps    *RuntimeDeps
	catalog *DataSourceCatalog
	symbols *orm.SymbolState
	*SeriesWatcher
	OnDataSeries  func(msg *SeriesMsg, rows []*orm.DataSeries) *errs.Error
	handlerLock   sync.Mutex
	handlerWait   sync.WaitGroup
	handlerStop   bool
	lifecycleOnce sync.Once
}

func (p *LiveProvider) DataSourceCatalog() *DataSourceCatalog {
	if p == nil {
		return nil
	}
	return p.catalog
}

func NewLiveProvider(callBack FnDataSeries, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error) {
	return newLiveProvider(nil, nil, callBack, envEnd)
}

func NewLiveProviderWithCatalog(catalog *DataSourceCatalog, symbols *orm.SymbolState, callBack FnDataSeries, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error) {
	return newLiveProviderWithCatalog(nil, symbols, catalog, callBack, envEnd)
}

// NewLiveProviderWithSymbolState binds symbol resolution and subscription
// bookkeeping to one runtime. A nil state preserves the legacy facade.
func NewLiveProviderWithSymbolState(symbols *orm.SymbolState, callBack FnDataSeries, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error) {
	return newLiveProvider(nil, symbols, callBack, envEnd)
}

// NewLiveProviderWithRuntimeDeps binds the provider, watcher, and all live
// feeders to one runtime.
func NewLiveProviderWithRuntimeDeps(deps *RuntimeDeps, callBack FnDataSeries, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error) {
	if deps == nil || deps.Strategies == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "live provider requires explicit strategy state")
	}
	return newLiveProviderWithCatalog(deps, deps.Symbols, deps.Catalog, callBack, envEnd)
}

func newLiveProvider(deps *RuntimeDeps, symbols *orm.SymbolState, callBack FnDataSeries, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error) {
	catalog := legacyDataSourceCatalog
	if deps != nil {
		catalog = deps.Catalog
	}
	return newLiveProviderWithCatalog(deps, symbols, catalog, callBack, envEnd)
}

func newLiveProviderWithCatalog(deps *RuntimeDeps, symbols *orm.SymbolState, catalog *DataSourceCatalog, callBack FnDataSeries, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error) {
	var wsSubs *strat.WsSubJobRegistry
	if deps != nil {
		wsSubs = strat.NewWsSubJobRegistryWithState(deps.Strategies, symbols)
	} else {
		wsSubs = strat.NewWsSubJobRegistry(symbols)
	}
	if deps == nil && symbols == nil {
		wsSubs = strat.LegacyWsSubJobRegistry()
	}
	var addr string
	if deps == nil {
		addr = config.SpiderAddr
	} else {
		addr = deps.spiderAddr()
	}
	var watcher *SeriesWatcher
	var err *errs.Error
	if deps == nil {
		watcher, err = NewSeriesWatcherWithSymbolState(symbols, addr)
	} else {
		watcher, err = NewSeriesWatcherWithRuntimeDeps(deps, addr)
	}
	if err != nil {
		return nil, err
	}
	provider := &LiveProvider{
		Provider: Provider[IDataFeeder]{
			holders: make(map[string]IDataFeeder),
			newFeeder: func(pair string, tfs []string) (IDataFeeder, *errs.Error) {
				var exchange banexg.BanExchange
				if deps == nil {
					exchange = exg.Default
				} else {
					exchange = deps.exchange()
					if exchange == nil {
						return nil, errs.NewMsg(core.ErrBadConfig, "runtime exchange is required")
					}
				}
				exs, err := resolveExSymbolWithRuntimeDeps(deps, symbols, exchange, pair)
				if err != nil {
					return nil, err
				}
				var feeder *SeriesFeeder
				if deps == nil {
					feeder, err = NewSeriesFeederWithSymbolState(symbols, exs, callBack, true)
				} else {
					feeder, err = NewSeriesFeederWithRuntimeDeps(deps, exs, callBack, true)
				}
				if err != nil {
					return nil, err
				}
				feeder.SubTfs(tfs, false)
				feeder.OnEnvEnd = envEnd
				return feeder, nil
			},
			dirtyVers: make(chan int, 5),
			deps:      deps,
			wsSubs:    wsSubs,
		},
		deps:          deps,
		catalog:       catalog,
		symbols:       symbols,
		SeriesWatcher: watcher,
	}
	watcher.OnDataMsg = makeOnSeriesMsg(provider)
	watcher.OnTrades = makeOnTrade(provider)
	watcher.OnDepth = makeOnDepth(provider)
	provider.registerLifecycle()
	// 立刻订阅实时价格
	//err = watcher.SendMsg("subscribe", []string{
	//	fmt.Sprintf("price_%s_%s", core.ExgName, core.Market),
	//})
	//if err != nil {
	//	return nil, err
	//}
	return provider, nil
}

// registerLifecycle gives a directly-created provider the same stop -> join
// ownership as the composition-root trader. Existing trader lifecycle wiring
// may also register these callbacks; Stop and Join are intentionally
// idempotent, and the once guard prevents duplicate registration on retries.
func (p *LiveProvider) registerLifecycle() {
	if p == nil || p.deps == nil || p.deps.Callbacks == nil {
		return
	}
	lifecycle, ok := p.deps.Callbacks.(LifecycleRegistrar)
	if !ok {
		return
	}
	p.lifecycleOnce.Do(func() {
		lifecycle.OnClose(func() {
			_ = p.Stop()
		})
		lifecycle.OnCloseWait(p.Join)
	})
}

func (p *LiveProvider) SubWarmPairs(items map[string]map[string]int, delOther bool) *errs.Error {
	p.opMu.Lock()
	defer p.opMu.Unlock()
	newHolds, sinceMap, delPairs, err := p.subWarmPairs(items, delOther, nil)
	if err != nil {
		return err
	}
	if len(newHolds) > 0 {
		var jobs []WatchJob
		var minSince int64
		if p.deps == nil {
			minSince = btime.UTCStamp()
		} else {
			minSince = p.deps.timeMS()
		}
		for _, h := range newHolds {
			sta := h.getStates()[0]
			symbol := h.getSymbol()
			since, ok := sinceMap[symbol]
			if ok {
				minSince = min(minSince, since)
			}
			if sta.TFSecs >= 3600 {
				exs, err := resolveExSymbolCurWithRuntimeDeps(p.deps, p.symbols, symbol)
				if err != nil {
					return err
				}
				if p.deps != nil {
					if p.symbols == nil {
						return errs.NewMsg(core.ErrInvalidSymbol, "%s not found in runtime symbol state", symbol)
					}
					p.symbols.AddHourSymbol(exs)
				} else if p.symbols == nil {
					orm.AddHourSymbol(exs)
				} else {
					p.symbols.AddHourSymbol(exs)
				}
			} else {
				if ok {
					jobs = append(jobs, WatchJob{
						Symbol:    symbol,
						TimeFrame: sta.TimeFrame,
						Since:     since,
					})
				}
				if p.deps != nil {
					if p.symbols == nil {
						return errs.NewMsg(core.ErrInvalidSymbol, "%s not found in runtime symbol state", symbol)
					}
					p.symbols.Sub1mSymbol(symbol)
				} else if p.symbols == nil {
					orm.Sub1mSymbol(symbol)
				} else {
					p.symbols.Sub1mSymbol(symbol)
				}
			}
		}
		if len(jobs) > 0 {
			var exchangeName, market string
			if p.deps == nil {
				exchangeName, market = core.ExgName, core.Market
			} else {
				exchangeName, market = p.deps.identity()
			}
			err = p.WatchJobs(exchangeName, market, "ohlcv", jobs...)
			if err != nil {
				return err
			}
		}
		for _, msgType := range p.wsRegistry().Types() {
			pairs := p.wsRegistry().Pairs(msgType)
			jobs = make([]WatchJob, 0, len(pairs))
			for _, pair := range pairs {
				jobs = append(jobs, WatchJob{Symbol: pair, TimeFrame: "1m"})
			}
			var exchangeName, market string
			if p.deps == nil {
				exchangeName, market = core.ExgName, core.Market
			} else {
				exchangeName, market = p.deps.identity()
			}
			err = p.WatchJobs(exchangeName, market, msgType, jobs...)
			if err != nil {
				return err
			}
		}
	}
	if len(delPairs) > 0 {
		var exchangeName, market string
		if p.deps == nil {
			exchangeName, market = core.ExgName, core.Market
		} else {
			exchangeName, market = p.deps.identity()
		}
		err = p.UnWatchJobs(exchangeName, market, "ohlcv", delPairs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *LiveProvider) UnSubPairs(pairs ...string) *errs.Error {
	p.opMu.Lock()
	defer p.opMu.Unlock()
	removed := p.unSubPairs(pairs...)
	if len(removed) == 0 {
		return nil
	}
	var exchangeName, market string
	if p.deps == nil {
		exchangeName, market = core.ExgName, core.Market
	} else {
		exchangeName, market = p.deps.identity()
	}
	return p.UnWatchJobs(exchangeName, market, "ohlcv", pairs)
}

func (p *LiveProvider) LoopMain() *errs.Error {
	defer func() {
		_ = p.Close()
		p.Join()
	}()
	return p.RunForever()
}

func (p *LiveProvider) beginHandler() bool {
	p.handlerLock.Lock()
	defer p.handlerLock.Unlock()
	if p.handlerStop {
		return false
	}
	if p.deps != nil && p.deps.Callbacks != nil && !p.deps.Callbacks.EnterCallback() {
		return false
	}
	p.handlerWait.Add(1)
	return true
}

func (p *LiveProvider) leaveHandler() {
	p.handlerWait.Done()
	if p.deps != nil && p.deps.Callbacks != nil {
		p.deps.Callbacks.LeaveCallback()
	}
}

func (p *LiveProvider) joinHandlers() {
	p.handlerWait.Wait()
}

func (p *LiveProvider) runHandler(hold IDataFeeder, tfMSecs int64, msg *SeriesMsg, rows []*orm.DataSeries) {
	_, err := hold.onNewData(tfMSecs, rows)
	if err != nil {
		p.deps.logger().Error("onNewData fail", zap.String("p", msg.Pair), zap.Error(err))
		return
	}
	if p.OnDataSeries != nil {
		err = p.OnDataSeries(msg, rows)
		if err != nil {
			p.deps.logger().Error("OnDataSeries fail", zap.String("p", msg.Pair), zap.Error(err))
		}
	}
}

// Close stops intake and the socket. Join is a separate phase so a handler
// can safely close its own provider.
func (p *LiveProvider) Close() *errs.Error {
	if p == nil {
		return nil
	}
	p.handlerLock.Lock()
	p.handlerStop = true
	p.handlerLock.Unlock()
	if p.SeriesWatcher == nil || p.SeriesWatcher.ClientIO == nil {
		return nil
	}
	return p.SeriesWatcher.ClientIO.Stop()
}

// Stop is the non-blocking stop phase of the provider lifecycle. Call Join
// from the owner after Stop when all accepted callbacks must have completed.
func (p *LiveProvider) Stop() *errs.Error {
	return p.Close()
}

// Join waits for the socket listeners and provider callbacks admitted before
// Close or Stop. The stop phase must run first to prevent new admissions.
func (p *LiveProvider) Join() {
	if p == nil {
		return
	}
	p.handlerLock.Lock()
	stopped := p.handlerStop
	p.handlerLock.Unlock()
	if !stopped {
		return
	}
	if p.SeriesWatcher != nil && p.SeriesWatcher.ClientIO != nil {
		p.SeriesWatcher.ClientIO.Join()
	}
	p.joinHandlers()
}

func makeOnSeriesMsg(p *LiveProvider) func(msg *SeriesMsg) {
	return func(msg *SeriesMsg) {
		if !p.beginHandler() {
			return
		}
		admitted := true
		defer func() {
			if admitted {
				p.leaveHandler()
			}
		}()
		var exchangeName, market string
		if p.deps == nil {
			exchangeName, market = core.ExgName, core.Market
		} else {
			exchangeName, market = p.deps.identity()
		}
		if msg.ExgName != exchangeName || msg.Market != market {
			return
		}
		if len(msg.Rows) == 0 {
			return
		}
		if msg.Interval < msg.TFSecs {
			fireWsSeries(p, msg)
		}
		hold, ok := p.getHolder(msg.Pair)
		if !ok {
			return
		}
		tfMSecs := int64(msg.TFSecs * 1000)
		exs := getExSymbol2WithRuntimeDeps(p.deps, p.symbols, msg.ExgName, msg.Market, msg.Pair)
		handleNewRows := func(rows []*orm.DataSeries) {
			// Transfer the admission token explicitly to the asynchronous callback.
			// The callback is admitted before Stop seals the provider.
			admitted = false
			go func() {
				defer p.leaveHandler()
				p.runHandler(hold, tfMSecs, msg, rows)
			}()
		}
		// The weighting factor has been calculated during the start-up or market break, and the weighting is automatically carried out internally
		// 已在启动或休市期间计算复权因子，内部会自动进行复权
		if msg.Interval >= msg.TFSecs {
			handleNewRows(msg.Rows)
			return
		}
		// The frequency of updates is lower than the bar cycle, and what is received may not be completed
		// 更新频率低于bar周期，收到的可能未完成
		lastIdx := len(msg.Rows) - 1
		doneRows, lastRow := msg.Rows[:lastIdx], msg.Rows[lastIdx]
		waitData := hold.getWaitData()
		if waitData != nil && waitData.TimeMS < lastRow.TimeMS {
			doneRows = append([]*orm.DataSeries{waitData}, doneRows...)
			hold.setWaitData(nil)
		}
		if len(doneRows) > 0 {
			handleNewRows(doneRows)
			return
		}
		if msg.Interval <= 5 && hold.getStates()[0].TFSecs >= 60 {
			// The update is fast, and the cycle required is relatively long, so it is required to be considered complete when the next bar occurs (follow the above logic)
			// 更新很快，需要的周期相对较长，则要求出现下一个bar时认为完成（走上面逻辑）
			hold.setWaitData(lastRow.CloneWithExSymbol(exs))
			return
		}
		// The frequency of updates is relatively low, or the proportion of the required cycle is large, and the approximate completion is considered complete
		// 更新频率相对不高，或占需要的周期比率较大，近似完成认为完成
		var nowMS int64
		if p.deps == nil {
			nowMS = btime.TimeMS()
		} else {
			nowMS = p.deps.timeMS()
		}
		endLackSecs := int((lastRow.TimeMS + tfMSecs - nowMS) / 1000)
		if endLackSecs*2 < msg.Interval {
			// The missing time is less than half of the update interval and is considered complete.
			// 缺少的时间不足更新间隔的一半，认为完成。
			handleNewRows([]*orm.DataSeries{lastRow})
		} else {
			hold.setWaitData(lastRow.CloneWithExSymbol(exs))
		}
	}
}

type klineFieldReader func(exs *orm.ExSymbol, tf string, fields []string, startMS, endMS int64) ([]*orm.DataSeries, *errs.Error)

func queryStoredKlineFields(exs *orm.ExSymbol, tf string, fields []string, startMS, endMS int64) ([]*orm.DataSeries, *errs.Error) {
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	return sess.QuerySeriesFields(exs, tf, fields, startMS, endMS, 0, false)
}

func enrichStoredKlineFields(symbols *orm.SymbolState, exs *orm.ExSymbol, tf string, rows []*orm.DataSeries) ([]*orm.DataSeries, *errs.Error) {
	return enrichStoredKlineFieldsWithReader(symbols, exs, tf, rows, nil)
}

func enrichStoredKlineFieldsWithRuntimeDeps(deps *RuntimeDeps, exs *orm.ExSymbol, tf string, rows []*orm.DataSeries) ([]*orm.DataSeries, *errs.Error) {
	return enrichStoredKlineFieldsWithRuntimeDepsAndReader(deps, exs, tf, rows, nil)
}

func enrichStoredKlineFieldsWithRuntimeDepsAndReader(deps *RuntimeDeps, exs *orm.ExSymbol, tf string,
	rows []*orm.DataSeries, reader klineFieldReader,
) ([]*orm.DataSeries, *errs.Error) {
	if deps == nil {
		return enrichStoredKlineFieldsWithReader(nil, exs, tf, rows, reader)
	}
	var symbols *orm.SymbolState
	var strategies *strat.State
	if deps != nil {
		symbols = deps.Symbols
		strategies = deps.Strategies
	}
	if strategies != nil && symbols == nil {
		symbols = strategies.Symbols
	}
	if reader == nil && deps != nil {
		reader = func(exs *orm.ExSymbol, tf string, fields []string, startMS, endMS int64) ([]*orm.DataSeries, *errs.Error) {
			sess, conn, connErr := deps.conn()
			if connErr != nil {
				return nil, connErr
			}
			defer conn.Release()
			if symbols != nil {
				sess = sess.WithSeriesSymbolState(symbols)
			}
			return sess.QuerySeriesFields(exs, tf, fields, startMS, endMS, 0, false)
		}
	}
	return enrichStoredKlineFieldsWithState(strategies, symbols, exs, tf, rows, reader)
}

func enrichStoredKlineFieldsWithReader(symbols *orm.SymbolState, exs *orm.ExSymbol, tf string,
	rows []*orm.DataSeries, reader klineFieldReader,
) ([]*orm.DataSeries, *errs.Error) {
	fields := strat.CollectKlineSubFieldsWithSymbolState(symbols, exs.ID, tf)
	return enrichKlineFieldRows(exs, tf, fields, rows, reader)
}

func enrichStoredKlineFieldsWithState(strategies *strat.State, symbols *orm.SymbolState, exs *orm.ExSymbol, tf string,
	rows []*orm.DataSeries, reader klineFieldReader,
) ([]*orm.DataSeries, *errs.Error) {
	if exs == nil || len(rows) == 0 {
		return rows, nil
	}
	var fields []string
	if strategies != nil {
		fields = strategies.CollectKlineSubFields(symbols, exs.ID, tf)
	} else {
		// Explicit providers do not fall back to the process-wide strategy
		// registry when no strategy state was supplied.
		fields = orm.NormalizeSeriesFields(orm.SeriesSourceKline, nil)
	}
	return enrichKlineFieldRows(exs, tf, fields, rows, reader)
}

func enrichKlineFieldRows(exs *orm.ExSymbol, tf string, fields []string, rows []*orm.DataSeries,
	reader klineFieldReader,
) ([]*orm.DataSeries, *errs.Error) {
	var extraFields []string
	for _, field := range fields {
		if !isDefaultKlineField(field) {
			extraFields = append(extraFields, field)
		}
	}
	if len(extraFields) == 0 || len(rows) == 0 {
		return rows, nil
	}
	if reader == nil {
		reader = queryStoredKlineFields
	}

	needsRead := make(map[int64]bool)
	for _, row := range rows {
		if row == nil {
			return nil, errs.NewMsg(core.ErrInvalidBars, "nil kline row cannot be enriched: pair=%s timeframe=%s", exs.Symbol, tf)
		}
		for _, field := range extraFields {
			if _, ok := row.Values[field]; !ok {
				needsRead[row.TimeMS] = true
				break
			}
		}
	}
	if len(needsRead) == 0 {
		return rows, nil
	}

	var startMS, endMS int64
	var hasRange bool
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	for _, row := range rows {
		if !needsRead[row.TimeMS] {
			continue
		}
		if !hasRange || row.TimeMS < startMS {
			startMS = row.TimeMS
		}
		rowEndMS := row.EndMS
		if rowEndMS <= row.TimeMS {
			rowEndMS = row.TimeMS + tfMSecs
		}
		if !hasRange || rowEndMS > endMS {
			endMS = rowEndMS
		}
		hasRange = true
	}
	if !hasRange || endMS <= startMS {
		return nil, errs.NewMsg(core.ErrInvalidBars,
			"kline row has no valid time range for enrichment: pair=%s timeframe=%s", exs.Symbol, tf)
	}

	stored, err := reader(exs, tf, fields, startMS, endMS)
	if err != nil {
		return nil, err
	}
	byTime := make(map[int64]*orm.DataSeries, len(stored))
	for _, row := range stored {
		if row != nil {
			byTime[row.TimeMS] = row
		}
	}
	for _, row := range rows {
		if !needsRead[row.TimeMS] {
			continue
		}
		storedRow, ok := byTime[row.TimeMS]
		if !ok {
			return nil, errs.NewMsg(core.ErrDbReadFail,
				"stored kline row missing for enrichment: pair=%s timeframe=%s time_ms=%d",
				exs.Symbol, tf, row.TimeMS)
		}
		for _, field := range extraFields {
			if _, present := row.Values[field]; present {
				continue
			}
			if _, present := storedRow.Values[field]; !present {
				return nil, errs.NewMsg(core.ErrDbReadFail,
					"stored kline field missing for enrichment: pair=%s timeframe=%s time_ms=%d field=%s",
					exs.Symbol, tf, row.TimeMS, field)
			}
		}
	}
	return mergeKlineFieldRows(rows, byTime), nil
}

func mergeKlineFieldRows(rows []*orm.DataSeries, byTime map[int64]*orm.DataSeries) []*orm.DataSeries {
	out := make([]*orm.DataSeries, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			out = append(out, row)
			continue
		}
		full := byTime[row.TimeMS]
		if full == nil {
			out = append(out, row)
			continue
		}
		cp := *row
		cp.Values = make(map[string]any, len(row.Values)+len(full.Values))
		for key, val := range row.Values {
			cp.Values[key] = val
		}
		for key, val := range full.Values {
			if !isDefaultKlineField(key) {
				if _, exists := cp.Values[key]; exists {
					continue
				}
				cp.Values[key] = val
			}
		}
		out = append(out, &cp)
	}
	return out
}

func hasExtraKlineFields(fields []string) bool {
	for _, field := range fields {
		if !isDefaultKlineField(field) {
			return true
		}
	}
	return false
}

func isDefaultKlineField(field string) bool {
	switch field {
	case "open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num":
		return true
	default:
		return false
	}
}

func makeOnTrade(p *LiveProvider) func(exgName, market, pair string, trades []*banexg.Trade) {
	return func(exgName, market, pair string, trades []*banexg.Trade) {
		if !p.beginHandler() {
			return
		}
		defer p.leaveHandler()
		if len(trades) == 0 {
			return
		}
		p.wsRegistry().ForEach(core.WsSubTrade, pair, func(job *strat.StratJob) {
			num1, num2 := strat.GetJobInOutNum(job)
			job.Strat.OnWsTrades(job, pair, trades)
			strat.CheckJobInOutNum(job, "OnWsTrades", num1, num2)
		})
	}
}

func makeOnDepth(p *LiveProvider) func(dep *banexg.OrderBook) {
	return func(dep *banexg.OrderBook) {
		if !p.beginHandler() {
			return
		}
		defer p.leaveHandler()
		p.wsRegistry().ForEach(core.WsSubDepth, dep.Symbol, func(job *strat.StratJob) {
			num1, num2 := strat.GetJobInOutNum(job)
			job.Strat.OnWsDepth(job, dep)
			strat.CheckJobInOutNum(job, "OnWsDepth", num1, num2)
		})
	}
}

func fireWsSeries(p *LiveProvider, msg *SeriesMsg) {
	if len(msg.Rows) == 0 {
		return
	}
	last := msg.Rows[len(msg.Rows)-1]
	view, err := last.OHLCV(getExSymbol2WithRuntimeDeps(p.deps, p.symbols, msg.ExgName, msg.Market, msg.Pair))
	if err != nil {
		log.Error("ws series missing OHLCV", zap.String("p", msg.Pair), zap.Error(err))
		return
	}
	if p.deps == nil {
		if _, ok := core.GetOdBook(msg.Pair); !ok {
			com.SetPrice(msg.Pair, view.Close, view.Close)
		}
	} else {
		hasBook := false
		if p.deps.Core != nil {
			_, hasBook = p.deps.Core.GetOdBook(msg.Pair)
		}
		if !hasBook {
			if prices := p.deps.priceState(); prices != nil {
				prices.SetPriceAt(p.deps.timeMS(), msg.Pair, view.Close, view.Close)
			}
		}
	}
	p.wsRegistry().ForEach(core.WsSubKLine, msg.Pair, func(job *strat.StratJob) {
		num1, num2 := strat.GetJobInOutNum(job)
		if job.Strat.OnWsData != nil {
			job.Strat.OnWsData(job, last)
			strat.CheckJobInOutNum(job, "OnWsData", num1, num2)
		} else {
			job.Strat.OnWsKline(job, msg.Pair, view.Bar())
			strat.CheckJobInOutNum(job, "OnWsKline", num1, num2)
		}
	})
}
