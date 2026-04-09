package biz

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/banbox/banbot/com"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
	"go.uber.org/zap"
)

type Trader struct {
}

func (t *Trader) OnEnvSeries(evt *orm.DataSeries) (*ta.BarEnv, *errs.Error) {
	if evt == nil {
		return nil, nil
	}
	symbol := evt.Symbol()
	if symbol == "" {
		return nil, errs.NewMsg(core.ErrInvalidSymbol, "series sid %d not found", evt.Sid)
	}
	envKey := strings.Join([]string{symbol, evt.TimeFrame}, "_")
	env, ok := strat.Envs[envKey]
	if !ok {
		// 额外订阅1h没有对应的env，无需处理
		return nil, nil
	}
	if core.LiveMode {
		if env.TimeStop > evt.TimeMS {
			// This bar has expired, ignore it, the crawler may push the processed expired bar when starting
			return nil, nil
		} else if env.TimeStop > 0 && env.TimeStop < evt.TimeMS {
			lackNum := int(math.Round(float64(evt.TimeMS-env.TimeStop) / float64(env.TFMSecs)))
			if lackNum > 0 {
				log.Warn("taEnv bar lack", zap.Int("num", lackNum), zap.String("env", envKey))
			}
		}
	}
	openVal, err := evt.OpenValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	highVal, err := evt.HighValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	lowVal, err := evt.LowValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	closeVal, err := evt.CloseValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	volumeVal, err := evt.VolumeValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	err = env.OnBar(evt.TimeMS, openVal, highVal, lowVal, closeVal, volumeVal, evt.QuoteValue(), evt.BuyVolumeValue(), evt.TradeNumValue())
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	return env, nil
}

func (t *Trader) FeedDataSeries(evt *orm.DataSeries) *errs.Error {
	if evt == nil {
		return nil
	}
	exs := evt.EnsureExSymbol()
	if exs == nil && orm.NormalizeSeriesSource(evt.Source) == orm.SeriesSourceKline {
		return errs.NewMsg(core.ErrInvalidSymbol, "series sid %d not found", evt.Sid)
	}
	if !evt.HasOHLCV() {
		return t.feedDataOnlySeries(evt)
	}
	return t.feedClosedSeries(evt)
}

func (t *Trader) FeedSeries(evt *orm.DataSeries) *errs.Error {
	return t.FeedDataSeries(evt)
}

func (t *Trader) feedDataOnlySeries(evt *orm.DataSeries) *errs.Error {
	if evt == nil {
		return nil
	}
	subKey := strat.DataSubKey(evt.Source, evt.Sid, evt.TimeFrame)
	dispatched := false
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		dispatched = true
		strat.LockJobsRead()
		jobMap, _ := strat.GetInfoJobs(account)[subKey]
		strat.UnlockJobsRead()
		deliverDataOnlySeries(jobMap, evt)
	}
	if !dispatched {
		strat.LockJobsRead()
		jobMap, _ := strat.GetInfoJobs(config.DefAcc)[subKey]
		strat.UnlockJobsRead()
		deliverDataOnlySeries(jobMap, evt)
	}
	return nil
}

func deliverDataOnlySeries(jobMap map[string]*strat.StratJob, evt *orm.DataSeries) {
	for _, job := range jobMap {
		if job.DataHub == nil {
			job.DataHub = strat.NewDataHub()
		}
		job.DataHub.Set(evt)
		job.IsWarmUp = evt.IsWarmUp
		if job.Strat.OnData == nil {
			continue
		}
		num1, num2 := strat.GetJobInOutNum(job)
		job.Strat.OnData(job, evt)
		strat.CheckJobInOutNum(job, "OnData", num1, num2)
	}
}

func (t *Trader) feedClosedSeries(evt *orm.DataSeries) *errs.Error {
	symbol := evt.Symbol()
	if symbol == "" {
		return errs.NewMsg(core.ErrInvalidSymbol, "series sid %d not found", evt.Sid)
	}
	closeVal, closeErr := evt.CloseValue()
	if closeErr != nil {
		return errs.New(core.ErrInvalidBars, closeErr)
	}
	core.LockOdMatch.RLock()
	_, odMatch := core.OrderMatchTfs[evt.TimeFrame]
	core.LockOdMatch.RUnlock()
	accOrders := make(map[string][]*ormo.InOutOrder)
	com.SetBarPrice(symbol, closeVal)
	if odMatch && !evt.IsWarmUp {
		for account, cfg := range config.Accounts {
			if cfg.NoTrade {
				continue
			}
			openOds, lock := ormo.GetOpenODs(account)
			lock.Lock()
			allOrders := utils2.ValsOfMap(openOds)
			lock.Unlock()
			odMgr := GetOdMgr(account)
			if len(allOrders) > 0 {
				if updateErr := odMgr.UpdateByDataSeries(allOrders, evt); updateErr != nil {
					return updateErr
				}
				newOpens := make([]*ormo.InOutOrder, 0, len(allOrders))
				for _, od := range allOrders {
					if od.Status < ormo.InOutStatusFullExit {
						newOpens = append(newOpens, od)
					}
				}
				accOrders[account] = newOpens
			}
		}
	}
	env, errEnv := t.OnEnvSeries(evt)
	if errEnv != nil {
		log.Error(fmt.Sprintf("%s/%s OnEnvSeries fail", symbol, evt.TimeFrame), zap.Error(errEnv))
		return errEnv
	} else if env == nil {
		return nil
	}
	tfSecs := utils2.TFToSecs(evt.TimeFrame)
	delaySecs := int((btime.TimeMS()-evt.TimeMS)/1000) - tfSecs
	barExpired := delaySecs >= max(60, tfSecs/2)
	if barExpired {
		if core.LiveMode && !evt.IsWarmUp {
			log.Warn(fmt.Sprintf("%s/%s delay %v s, open order disabled for this data series", symbol, evt.TimeFrame, delaySecs))
		} else {
			barExpired = false
		}
	}
	var wg sync.WaitGroup
	var runErr *errs.Error
	var accOdArr = make([]string, 0, len(config.Accounts))
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		allOrders, _ := accOrders[account]
		if !odMatch {
			openOds, lock := ormo.GetOpenODs(account)
			lock.Lock()
			allOrders = utils2.ValsOfMap(openOds)
			lock.Unlock()
		}
		var curOrders []*ormo.InOutOrder
		for _, od := range allOrders {
			if od.Status < ormo.InOutStatusFullExit && od.Symbol == symbol && od.Timeframe == evt.TimeFrame {
				curOrders = append(curOrders, od)
			}
		}
		if core.LiveMode && !evt.IsWarmUp {
			numStr := fmt.Sprintf("%s: %d/%d", account, len(curOrders), len(allOrders))
			accOdArr = append(accOdArr, numStr)
		}
		if !core.ParallelOnBar {
			curErr := t.onAccountDataSeries(account, env, evt, allOrders, barExpired)
			if curErr != nil {
				if runErr != nil {
					log.Error("onAccountDataSeries fail", zap.String("account", account), zap.Error(curErr))
				} else {
					runErr = curErr
				}
			}
		} else {
			wg.Add(1)
			go func(acc string, ods []*ormo.InOutOrder) {
				defer wg.Done()
				if curErr := t.onAccountDataSeries(acc, env, evt, ods, barExpired); curErr != nil {
					if runErr != nil {
						log.Error("onAccountDataSeries fail", zap.String("account", acc), zap.Error(curErr))
					} else {
						runErr = curErr
					}
				}
			}(account, allOrders)
		}
	}
	if core.ParallelOnBar {
		wg.Wait()
	}
	if core.LiveMode && len(accOdArr) > 0 {
		log.Info("OnSeries", zap.String("pair", symbol), zap.String("tf", evt.TimeFrame),
			zap.Strings("accOdNums", accOdArr))
	}
	return runErr
}

func (t *Trader) onAccountDataSeries(account string, env *ta.BarEnv, evt *orm.DataSeries, curOrders []*ormo.InOutOrder, barExpired bool) *errs.Error {
	symbol := evt.Symbol()
	envKey := strings.Join([]string{symbol, evt.TimeFrame}, "_")
	infoKey := strat.DataSubKey(evt.Source, evt.Sid, evt.TimeFrame)
	strat.LockJobsRead()
	jobs, _ := strat.GetJobs(account)[envKey]
	infoJobs, _ := strat.GetInfoJobs(account)[infoKey]
	strat.UnlockJobsRead()
	if len(jobs) == 0 && len(infoJobs) == 0 {
		return nil
	}
	odMgr := GetOdMgr(account)
	var err *errs.Error
	isWarmup := evt.IsWarmUp
	var wg sync.WaitGroup
	for _, job := range jobs {
		if job.DataHub == nil {
			job.DataHub = strat.NewDataHub()
		}
		job.DataHub.Set(evt)
		job.IsWarmUp = isWarmup
		job.InitBar(curOrders)
		if !core.ParallelOnBar {
			err = t.onAccountDataSeriesJob(odMgr, job, evt, barExpired)
			if err != nil {
				return err
			}
		} else {
			wg.Add(1)
			go func(j *strat.StratJob) {
				defer wg.Done()
				if errCur := t.onAccountDataSeriesJob(odMgr, j, evt, barExpired); errCur != nil {
					if errCur != nil {
						err = errCur
					}
				}
			}(job)
		}
	}
	if core.ParallelOnBar {
		wg.Wait()
		if err != nil {
			return err
		}
	}
	for _, job := range infoJobs {
		if job.DataHub == nil {
			job.DataHub = strat.NewDataHub()
		}
		job.DataHub.Set(evt)
		job.IsWarmUp = isWarmup
		num1, num2 := strat.GetJobInOutNum(job)
		if job.Strat.OnData != nil {
			job.Strat.OnData(job, evt)
			strat.CheckJobInOutNum(job, "OnData", num1, num2)
		} else if job.Strat.OnInfoBar != nil {
			job.Strat.OnInfoBar(job, env, symbol, evt.TimeFrame)
			strat.CheckJobInOutNum(job, "OnInfoBar", num1, num2)
		}
		if job.Strat.BatchInfo && job.Strat.OnBatchInfos != nil {
			AddBatchJob(account, evt.TimeFrame, job, env)
		}
	}
	if env.VNum > 1000 && !isWarmup {
		keyAt := "first_hit_at"
		keyNum := "first_hit_vnum"
		if cacheVal, ok := env.Data.Load(keyAt); ok {
			firstAt, _ := cacheVal.(int)
			firstNumVal, _ := env.Data.Load(keyNum)
			firstNum, _ := firstNumVal.(int)
			if env.BarNum-firstAt > 10 {
				if env.VNum-firstNum > 0 {
					addNum := env.VNum - firstNum
					addFor := env.BarNum - firstAt
					errMsg := "series too many (total %v), new add %v in %v bars, try replace `NewSeries` with `Series.To`"
					core.BotRunning = false
					return errs.NewMsg(errs.CodeRunTime, errMsg, env.VNum, addNum, addFor)
				}
			}
		} else {
			env.Data.Store(keyAt, env.BarNum)
			env.Data.Store(keyNum, env.VNum)
		}
	}
	return nil
}

func (t *Trader) onAccountDataSeriesJob(odMgr IOrderMgr, job *strat.StratJob, evt *orm.DataSeries, barExpired bool) *errs.Error {
	account := job.Account
	if job.Strat.OnData != nil {
		job.Strat.OnData(job, evt)
	} else {
		job.Strat.OnBar(job)
	}
	isWarmup := job.IsWarmUp
	isBatch := job.Strat.BatchInOut && job.Strat.OnBatchJobs != nil
	if !barExpired {
		if isBatch {
			AddBatchJob(account, evt.TimeFrame, job, nil)
		}
	} else {
		entryNum := len(job.Entrys)
		if core.LiveMode && !isWarmup && entryNum > 0 {
			log.Info("skip open orders by bar expired", zap.String("acc", account),
				zap.String("pair", evt.Symbol()), zap.String("tf", evt.TimeFrame),
				zap.Int("num", entryNum))
			strat.AddAccFailOpens(account, strat.FailOpenBarTooLate, entryNum)
			job.Entrys = nil
		}
	}
	if !isWarmup {
		err := strat.CheckCustomExits(job)
		if err != nil {
			return err
		}
		_, _, err = odMgr.ProcessOrders(job)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Trader) OnEnvEnd(evt *orm.DataSeries) {
	mgrs := GetAllOdMgr()
	for acc, mgr := range mgrs {
		err := mgr.OnEnvEnd(evt)
		if err != nil {
			log.Warn("close orders on env end fail", zap.String("acc", acc), zap.Error(err))
		}
	}
	if evt == nil {
		return
	}
	envKey := strings.Join([]string{evt.Symbol(), evt.TimeFrame}, "_")
	env, ok := strat.Envs[envKey]
	if ok {
		env.Reset()
	}
}
