package live

import (
	"fmt"
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
	dp *data.LiveProvider
}

func NewCryptoTrader() *CryptoTrader {
	return &CryptoTrader{}
}

func (t *CryptoTrader) Init() *errs.Error {
	config.LoadPerfs(config.GetDataDir())
	dp, err := data.NewLiveProvider(t.FeedKLine, t.OnEnvEnd)
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
		t.dp.OnMinKlines = biz.CallLocalLiveOdMgrsKline
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
	err := t.Init()
	if err != nil {
		return err
	}
	t.startJobs()
	err = t.dp.LoopMain()
	if err != nil {
		return err
	}
	// clean CallBacks already to core.ExitCalls
	return nil
}

func (t *CryptoTrader) FeedKLine(bar *orm.InfoKline) {
	if bar.IsWarmUp {
		tfMSecs := int64(utils.TFToSecs(bar.TimeFrame) * 1000)
		barEndMS := bar.Time + tfMSecs
		if barEndMS > strat.LastBatchMS {
			// Enter the next timeframe and trigger the batch entry callback
			// 进入下一个时间帧，触发批量入场回调
			execMS := barEndMS + core.DelayBatchMS + 1
			waitNum := biz.TryFireBatches(execMS, bar.IsWarmUp)
			if waitNum > 0 {
				log.Warn(fmt.Sprintf("batch job exec fail, wait: %v", waitNum))
			}
			strat.LastBatchMS = barEndMS
		}
	} else {
		delayExecBatch()
		envKey := strings.Join([]string{bar.Symbol, bar.TimeFrame}, "_")
		orm.AddDumpRow(orm.DumpKline, envKey, bar.Kline)
	}
	err := t.Trader.FeedKline(bar)
	if err != nil {
		log.Error("handle bar fail", zap.String("pair", bar.Symbol), zap.Error(err))
		return
	}
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
	CronRefreshPairs(t.dp)
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
