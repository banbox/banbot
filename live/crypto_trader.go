package live

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/web"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type CryptoTrader struct {
	biz.Trader
	dp            *data.LiveProvider
	startup       CryptoTraderStartupFunc
	initFn        func() *errs.Error
	startJobsFn   func()
	loopMainFn    func() *errs.Error
	seriesRuntime *data.SeriesRuntime
	nowMSFn       func() int64
	collectJobsFn func() []*strat.StratJob
}

type CryptoTraderStartupFunc func(ctx context.Context, trader *CryptoTrader) error

func NewCryptoTrader() *CryptoTrader {
	return &CryptoTrader{}
}

func NewCryptoTraderWith(startup CryptoTraderStartupFunc) *CryptoTrader {
	return &CryptoTrader{startup: startup}
}

func (t *CryptoTrader) Init() *errs.Error {
	config.LoadPerfs(config.GetDataDir())
	dp, err := data.NewLiveProvider(t.FeedDataSeries, t.OnEnvEnd)
	if err != nil {
		return err
	}
	t.dp = dp
	strat.SetPairUpdateHooks(strat.PairUpdateHooks{
		SubWarmPairs: t.dp.SubWarmPairs,
		ExitOrders: func(acc string, orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
			return biz.GetOdMgr(acc).ExitAndFill(orders, req)
		},
	})
	err = ormo.InitTask(true, config.GetDataDir())
	if err != nil {
		return err
	}
	// Trading pair initialization
	// 交易对初始化
	err = orm.InitListDates()
	if err != nil {
		return err
	}
	// 初始化 Telegram 订单管理器
	biz.InitTelegramOrderManager()
	err = web.StartApi()
	if err != nil {
		return err
	}
	if core.EnvReal {
		CheckLiveAccounts()
	}
	// Order Manager initialization
	// 订单管理器初始化
	err = t.initOdMgr()
	if err != nil {
		return err
	}
	err = opt.RefreshPairJobs(dp, true, true, nil)
	lastRefreshMS = btime.TimeMS()
	// add exit callback
	core.ExitCalls = append(core.ExitCalls, exitCleanUp)
	strat.WsSubUnWatch = func(m map[string][]string) {
		for msgType, pairs := range m {
			err2 := dp.UnWatchJobs(core.ExgName, core.Market, msgType, pairs)
			if err2 != nil {
				log.Error("UnWatchJobs fail", zap.String("type", msgType), zap.Error(err2))
			}
		}
	}
	return err
}

func (t *CryptoTrader) initOdMgr() *errs.Error {
	if !core.EnvReal {
		if !biz.RestoreDryRunWalletSnapshot(config.DefAcc) {
			biz.InitFakeWallets()
		}
		biz.InitLocalLiveOrderMgr(t.orderCB, true)
		t.dp.OnDataSeries = biz.CallLocalLiveOdMgrsData
		return nil
	}
	biz.InitLiveOrderMgr(t.orderCB)
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		odMgr := biz.GetLiveOdMgr(account)
		oldList, newList, delList, err := odMgr.SyncExgOrders()
		if err != nil {
			return err
		}
		openOds, lock := ormo.GetOpenODs(account)
		lock.Lock()
		msg := fmt.Sprintf("orders: %d restored, %d deleted, %d added, %d opened", len(oldList), len(delList), len(newList), len(openOds))
		lock.Unlock()
		rpc.SendMsg(map[string]interface{}{
			"type":    rpc.MsgTypeStatus,
			"account": account,
			"status":  msg,
		})
	}
	return nil
}

func (t *CryptoTrader) Run() *errs.Error {
	return t.runWithDeps()
}

func (t *CryptoTrader) bootstrapThirdPartySources(ctx context.Context) error {
	_, err := t.thirdPartyRuntime().SyncLive(ctx, t.runtimeJobs(), t.currentTimeMS())
	return err
}

func (t *CryptoTrader) runtimeJobs() []*strat.StratJob {
	if t.collectJobsFn != nil {
		return t.collectJobsFn()
	}
	seen := make(map[*strat.StratJob]bool)
	var jobs []*strat.StratJob
	strat.LockJobsRead()
	defer strat.UnlockJobsRead()
	accounts := make([]string, 0, len(strat.AccJobs))
	for acc := range strat.AccJobs {
		accounts = append(accounts, acc)
	}
	sort.Strings(accounts)
	for _, acc := range accounts {
		jobMap := strat.AccJobs[acc]
		envKeys := make([]string, 0, len(jobMap))
		for envKey := range jobMap {
			envKeys = append(envKeys, envKey)
		}
		sort.Strings(envKeys)
		for _, envKey := range envKeys {
			stgMap := jobMap[envKey]
			stgNames := make([]string, 0, len(stgMap))
			for stgName := range stgMap {
				stgNames = append(stgNames, stgName)
			}
			sort.Strings(stgNames)
			for _, stgName := range stgNames {
				job := stgMap[stgName]
				if job == nil || seen[job] {
					continue
				}
				seen[job] = true
				jobs = append(jobs, job)
			}
		}
	}
	return jobs
}

func (t *CryptoTrader) currentTimeMS() int64 {
	if t.nowMSFn != nil {
		return t.nowMSFn()
	}
	return btime.TimeMS()
}

func (t *CryptoTrader) thirdPartyRuntime() *data.SeriesRuntime {
	if t.seriesRuntime == nil {
		t.seriesRuntime = data.NewSeriesRuntime(t)
	}
	if t.seriesRuntime.Sink == nil {
		t.seriesRuntime.Sink = t
	}
	return t.seriesRuntime
}

func (t *CryptoTrader) runWithDeps() *errs.Error {
	initFn := t.initFn
	if initFn == nil {
		initFn = t.Init
	}
	err := initFn()
	if err != nil {
		return err
	}
	if t.startup != nil {
		if errRun := t.startup(context.Background(), t); errRun != nil {
			return errs.New(core.ErrRunTime, errRun)
		}
	}
	if errRun := t.bootstrapThirdPartySources(context.Background()); errRun != nil {
		return errs.New(core.ErrRunTime, errRun)
	}
	startJobsFn := t.startJobsFn
	if startJobsFn == nil {
		startJobsFn = t.startJobs
	}
	startJobsFn()
	loopMainFn := t.loopMainFn
	if loopMainFn == nil {
		loopMainFn = t.dp.LoopMain
	}
	err = loopMainFn()
	if err != nil {
		return err
	}
	// clean CallBacks already to core.ExitCalls
	return nil
}

func (t *CryptoTrader) FeedDataSeries(evt *orm.DataSeries) {
	view, errView := evt.OHLCV(evt.ExSymbol)
	if errView != nil {
		if err := t.Trader.FeedDataSeries(evt); err != nil {
			log.Error("handle data series fail", zap.Int32("sid", evt.Sid), zap.Error(err))
		}
		return
	}
	if view.IsWarmUp {
		t.handleWarmupSeries(view)
	} else {
		t.handleLiveSeries(view)
	}
	if errRun := t.Trader.FeedDataSeries(evt); errRun != nil {
		log.Error("handle data series fail", zap.String("pair", view.Symbol()), zap.Error(errRun))
		return
	}
}

func (t *CryptoTrader) Emit(sub *strat.DataSub, rows []*orm.DataRecord) error {
	if t == nil {
		return fmt.Errorf("crypto trader is required")
	}
	if t.dp == nil {
		return fmt.Errorf("live provider is required")
	}
	if sub == nil {
		return fmt.Errorf("data sub is required")
	}
	if sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return fmt.Errorf("data sub exsymbol is required")
	}
	if sub.TimeFrame == "" {
		return fmt.Errorf("data sub timeframe is required")
	}
	if len(rows) == 0 {
		return nil
	}
	msg := &data.SeriesMsg{
		ExgName: core.ExgName,
		Market:  core.Market,
		Pair:    sub.ExSymbol.Symbol,
		NotifySeries: data.NotifySeries{
			TFSecs:   utils.TFToSecs(sub.TimeFrame),
			Interval: utils.TFToSecs(sub.TimeFrame),
		},
	}
	seriesRows := make([]*orm.DataSeries, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		evt := &orm.DataSeries{
			Source:    orm.NormalizeSeriesSource(sub.Source),
			Sid:       sub.ExSymbol.ID,
			TimeMS:    row.TimeMS,
			EndMS:     row.EndMS,
			TimeFrame: sub.TimeFrame,
			Closed:    row.Closed,
			Values:    cloneSeriesValues(row.Values),
			ExSymbol:  sub.ExSymbol,
		}
		seriesRows = append(seriesRows, evt)
	}
	if len(seriesRows) == 0 {
		return nil
	}
	for _, evt := range seriesRows {
		t.FeedDataSeries(evt)
	}
	if t.dp.OnDataSeries != nil {
		if err := t.dp.OnDataSeries(msg, seriesRows); err != nil {
			return err
		}
	}
	return nil
}

func (t *CryptoTrader) handleWarmupSeries(view *orm.SeriesOHLCV) {
	if view == nil {
		return
	}
	tfMSecs := int64(utils.TFToSecs(view.TimeFrame) * 1000)
	barEndMS := view.Time + tfMSecs
	if barEndMS > strat.LastBatchMS {
		// Enter the next timeframe and trigger the batch entry callback
		// 进入下一个时间帧，触发批量入场回调
		execMS := barEndMS + core.DelayBatchMS + 1
		waitNum := biz.TryFireBatches(execMS, view.IsWarmUp)
		if waitNum > 0 {
			log.Warn(fmt.Sprintf("batch job exec fail, wait: %v", waitNum))
		}
		strat.LastBatchMS = barEndMS
	}
}

func (t *CryptoTrader) handleLiveSeries(view *orm.SeriesOHLCV) {
	if view == nil {
		return
	}
	delayExecBatch()
	envKey := strings.Join([]string{view.Symbol(), view.TimeFrame}, "_")
	if bar := view.Bar(); bar != nil {
		orm.AddDumpRow(orm.DumpKline, envKey, *bar)
	}
}

func cloneSeriesValues(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	cp := make(map[string]any, len(values))
	for key, val := range values {
		cp[key] = val
	}
	return cp
}

func delayExecBatch() {
	time.AfterFunc(time.Millisecond*core.DelayBatchMS, func() {
		waitNum := biz.TryFireBatches(btime.UTCStamp(), false)
		if waitNum > 0 {
			// There are TF cycles that have not yet been completed, and they are postponed for a few seconds to trigger again
			// 有尚未完成的tf周期，推迟几秒再次触发
			delayExecBatch()
		} else {
			orm.FlushDumps()
		}
	})
}

func (t *CryptoTrader) orderCB(od *ormo.InOutOrder, isEnter bool) {
	sendOrderMsg(od, isEnter)
}

func (t *CryptoTrader) startJobs() {
	if core.EnvReal {
		// Listen to account order flow, process user orders, and consume order queues
		// 监听账户订单流、处理用户下单、消费订单队列
		biz.StartLiveOdMgr()
	}
	t.markUnWarm()
	// Refresh trading pairs regularly
	// 定期刷新交易对
	CronRefreshPairs(t.dp, func() error {
		return t.bootstrapThirdPartySources(context.Background())
	})
	// 定时加载1h及以上周期K线
	FetchHourKlines(t.dp)
	// Refresh the market regularly
	// 定时刷新市场行情
	CronLoadMarkets()
	// Check every 5 minutes to see if the global stop loss is triggered
	// 每5分钟检查是否触发全局止损
	CronFatalLossCheck()
	// Regularly check the candlestick timeout, updated every minute
	// 定期检查K线超时，每分钟更新
	CronKlineDelays(t.dp)
	// The timer output is executed every 5 minutes: 01:30 06:30 11:30
	// 定时输出收到K线情况，每5分钟执行：01:30  06:30  11:30
	CronKlineSummary()
	// 每分钟定时输出策略Outputs信息到BanDataDir/logs/[name]_[strat].log
	CronDumpStratOutputs()
	// 实盘中定期回测对比
	CronBacktestInLive()
	if core.EnvReal {
		// Check if the limit order submission is triggered at 15th secs of every minute
		// 每分钟第15s检查是否触发限价单提交
		CronCheckTriggerOds()
		// Regularly update balance and synchronize exchange positions with local orders
		// 定期更新余额，同步交易所持仓到本地订单
		StartLoopBalancePositions()
		// 定期保存实盘钱包快照
		biz.StartLiveWalletSnapshots()
	}
	com.Cron().Start()
}

func (t *CryptoTrader) markUnWarm() {
	strat.LockJobsRead()
	for _, accMap := range strat.AccJobs {
		for _, jobMap := range accMap {
			for _, job := range jobMap {
				job.IsWarmUp = false
			}
		}
	}
	strat.UnlockJobsRead()
}

func exitCleanUp() {
	orm.FlushDumps()
	orm.CloseDump()
	err := biz.CleanUpOdMgr()
	if err != nil {
		log.Error("clean odMgr fail", zap.Error(err))
	}
	strat.ExitStratJobs()
	com.Cron().Stop()
	err = exg.Default.Close()
	if err != nil {
		log.Error("close exg fail", zap.Error(err))
	}
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		openOds, lock := ormo.GetOpenODs(account)
		lock.Lock()
		openNum := len(openOds)
		lock.Unlock()
		msg := fmt.Sprintf("bot stop, %d orders opened", openNum)
		rpc.SendMsg(map[string]interface{}{
			"type":    rpc.MsgTypeStatus,
			"account": account,
			"status":  msg,
		})
	}
	rpc.CleanUp()
}
