package biz

import (
	"fmt"
	"github.com/banbox/banbot/com"
	"math"
	"strings"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
	"go.uber.org/zap"
)

type Trader struct {
}

func (t *Trader) OnEnvJobs(bar *orm.InfoKline) (*ta.BarEnv, *errs.Error) {
	envKey := strings.Join([]string{bar.Symbol, bar.TimeFrame}, "_")
	env, ok := strat.Envs[envKey]
	if !ok {
		// 额外订阅1h没有对应的env，无需处理
		return nil, nil
	}
	if core.LiveMode {
		if env.TimeStop > bar.Time {
			// This bar has expired, ignore it, the crawler may push the processed expired bar when starting
			// 此bar已过期，忽略，启动时爬虫可能会推已处理的过期bar
			return nil, nil
		} else if env.TimeStop > 0 && env.TimeStop < bar.Time {
			lackNum := int(math.Round(float64(bar.Time-env.TimeStop) / float64(env.TFMSecs)))
			if lackNum > 0 {
				log.Warn("taEnv bar lack", zap.Int("num", lackNum), zap.String("env", envKey))
			}
		}
	}
	// Update BarEnv status
	// 更新BarEnv状态
	err := env.OnBar(bar.Time, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, bar.Info)
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	return env, nil
}

func (t *Trader) FeedKline(bar *orm.InfoKline) *errs.Error {
	tfSecs := utils2.TFToSecs(bar.TimeFrame)
	com.SetBarPrice(bar.Symbol, bar.Close)
	// If it exceeds 1 minute and half of the period, the bar is considered delayed and orders cannot be placed.
	// 超过1分钟且周期的一半，认为bar延迟，不可下单
	delaySecs := int((btime.TimeMS()-bar.Time)/1000) - tfSecs
	barExpired := delaySecs >= max(60, tfSecs/2)
	if barExpired {
		if core.LiveMode && !bar.IsWarmUp {
			log.Warn(fmt.Sprintf("%s/%s delay %v s, open order disabled for this K-line", bar.Symbol, bar.TimeFrame, delaySecs))
		} else {
			barExpired = false
		}
	}
	// Update indicator environment
	// 更新指标环境
	env, err := t.OnEnvJobs(bar)
	if err != nil {
		log.Error(fmt.Sprintf("%s/%s OnEnvJobs fail", bar.Symbol, bar.TimeFrame), zap.Error(err))
		return err
	} else if env == nil {
		return nil
	}
	for account := range config.Accounts {
		curErr := t.onAccountKline(account, env, bar, barExpired)
		if curErr != nil {
			if err != nil {
				log.Error("onAccountKline fail", zap.String("account", account), zap.Error(curErr))
			} else {
				err = curErr
			}
		}
	}
	return err
}

func (t *Trader) onAccountKline(account string, env *ta.BarEnv, bar *orm.InfoKline, barExpired bool) *errs.Error {
	envKey := strings.Join([]string{bar.Symbol, bar.TimeFrame}, "_")
	// Get strategy jobs 获取交易任务
	jobs, _ := strat.GetJobs(account)[envKey]
	// jobs which subscript info timeframes  辅助订阅的任务
	infoJobs, _ := strat.GetInfoJobs(account)[envKey]
	if len(jobs) == 0 && len(infoJobs) == 0 {
		return nil
	}
	openOds, lock := ormo.GetOpenODs(account)
	// Update orders in non-production mode 更新非生产模式的订单
	lock.Lock()
	allOrders := utils2.ValsOfMap(openOds)
	lock.Unlock()
	odMgr := GetOdMgr(account)
	var err *errs.Error
	isWarmup := bar.IsWarmUp
	if !isWarmup && len(allOrders) > 0 {
		// The order status may be modified here
		// 这里可能修改订单状态
		err = odMgr.UpdateByBar(allOrders, bar)
		if err != nil {
			return err
		}
	}
	// retrieve the current open orders after UpdateByBar, filter for closed orders
	// 要在UpdateByBar后检索当前开放订单，过滤已平仓订单
	var curOrders []*ormo.InOutOrder
	for _, od := range allOrders {
		if od.Status < ormo.InOutStatusFullExit && od.Symbol == bar.Symbol && od.Timeframe == bar.TimeFrame {
			curOrders = append(curOrders, od)
		}
	}
	if core.LiveMode && !isWarmup {
		// Live mode is saved to the database. Non-real-time mode, orders are temporarily saved in memory, no database required
		// 实时模式保存到数据库。非实时模式，订单临时保存到内存，无需数据库
		numStr := fmt.Sprintf("%d/%d", len(curOrders), len(openOds))
		log.Info("onAccountKline", zap.String("acc", account), zap.String("pair", bar.Symbol),
			zap.String("tf", bar.TimeFrame), zap.String("odNum", numStr))
	}
	var saveJobs []*strat.StratJob
	for _, job := range jobs {
		job.IsWarmUp = isWarmup
		job.InitBar(curOrders)
		job.Strat.OnBar(job)
		isBatch := job.Strat.BatchInOut && job.Strat.OnBatchJobs != nil
		if !barExpired {
			if isBatch {
				AddBatchJob(account, bar.TimeFrame, job, nil)
			}
		} else {
			entryNum := len(job.Entrys)
			if core.LiveMode && !isWarmup && entryNum > 0 {
				log.Info("skip open orders by bar expired", zap.String("acc", account),
					zap.String("pair", bar.Symbol), zap.String("tf", bar.TimeFrame),
					zap.Int("num", entryNum))
				strat.AddAccFailOpens(account, strat.FailOpenBarTooLate, entryNum)
				job.Entrys = nil
			}
		}
		if !isWarmup {
			err = strat.CheckCustomExits(job)
			if err != nil {
				return err
			}
			if len(job.Entrys) > 0 || len(job.Exits) > 0 {
				saveJobs = append(saveJobs, job)
			}
		}
	}
	if len(saveJobs) > 0 {
		sess, conn, err := ormo.Conn(orm.DbTrades, true)
		if err != nil {
			log.Error("get db sess fail", zap.Error(err))
			return err
		}
		for _, job := range saveJobs {
			_, _, err = odMgr.ProcessOrders(sess, job)
			if err != nil {
				conn.Close()
				return err
			}
		}
		conn.Close()
	}
	// invoke OnInfoBar
	// 更新辅助订阅数据
	// 此处不应允许开平仓或更新止盈止损等，否则订单的TimeFrame会出现歧义
	for _, job := range infoJobs {
		job.IsWarmUp = isWarmup
		num1, num2 := strat.GetJobInOutNum(job)
		job.Strat.OnInfoBar(job, env, bar.Symbol, bar.TimeFrame)
		strat.CheckJobInOutNum(job, "OnInfoBar", num1, num2)
		if job.Strat.BatchInfo && job.Strat.OnBatchInfos != nil {
			AddBatchJob(account, bar.TimeFrame, job, env)
		}
	}
	if env.VNum > 1000 && !isWarmup {
		// Series过多，检查是否有内存泄露
		keyAt := "first_hit_at"
		keyNum := "first_hit_vnum"
		if cacheVal, ok := env.Data[keyAt]; ok {
			firstAt, _ := cacheVal.(int)
			firstNum, _ := env.Data[keyNum].(int)
			if env.BarNum-firstAt > 10 {
				if env.VNum-firstNum > 0 {
					// 相比第一次有Series异常新增
					addNum := env.VNum - firstNum
					addFor := env.BarNum - firstAt
					errMsg := "series too many (total %v), new add %v in %v bars, try replace `NewSeries` with `Series.To`"
					core.BotRunning = false
					return errs.NewMsg(errs.CodeRunTime, errMsg, env.VNum, addNum, addFor)
				}
			}
		} else {
			env.Data[keyAt] = env.BarNum
			env.Data[keyNum] = env.VNum
		}
	}
	return nil
}

func (t *Trader) OnEnvEnd(bar *banexg.PairTFKline, adj *orm.AdjInfo) {
	mgrs := GetAllOdMgr()
	for acc, mgr := range mgrs {
		err := mgr.OnEnvEnd(bar, adj)
		if err != nil {
			log.Warn("close orders on env end fail", zap.String("acc", acc), zap.Error(err))
		}
	}
	envKey := strings.Join([]string{bar.Symbol, bar.TimeFrame}, "_")
	env, ok := strat.Envs[envKey]
	if ok {
		env.Reset()
	}
}
