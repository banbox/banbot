package live

import (
	"fmt"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

var (
	lastNotifyDelay = int64(0)
	lastRefreshMS   = int64(0)
)

func CronRefreshPairs(dp data.IProvider) {
	if config.PairMgr.Cron != "" {
		_, err_ := core.Cron.Add(config.PairMgr.Cron, func() {
			curMS := btime.TimeMS()
			if curMS-lastRefreshMS < config.MinPairCronGapMS {
				return
			}
			lastRefreshMS = curMS
			err := opt.RefreshPairJobs(dp, true, false, nil)
			if err != nil {
				log.Error("RefreshPairJobs fail", zap.Error(err))
			}
		})
		if err_ != nil {
			log.Error("add RefreshPairList fail", zap.Error(err_))
		}
	}
}

func CronLoadMarkets() {
	// 2小时更新一次市场行情
	_, err := core.Cron.Add("30 3 */2 * * *", func() {
		_, _ = orm.LoadMarkets(exg.Default, true)
	})
	if err != nil {
		log.Error("add CronLoadMarkets fail", zap.Error(err))
	}
}

func CronFatalLossCheck() {
	checkIntvs := utils.KeysOfMap(config.FatalStop)
	if len(checkIntvs) == 0 {
		return
	}
	minIntv := slices.Min(checkIntvs)
	if minIntv < 1 {
		log.Error("fatal_stop invalid, min is 1, skip", zap.Int("current", minIntv))
		return
	}
	cronStr := fmt.Sprintf("35 */%v * * * *", min(5, minIntv))
	maxIntv := slices.Max(checkIntvs)
	_, err := core.Cron.Add(cronStr, biz.MakeCheckFatalStop(maxIntv))
	if err != nil {
		log.Error("add CronFatalLossCheck fail", zap.Error(err))
	}
}

func CronKlineDelays() {
	logDelay := func(msgText string) {
		curMS := btime.TimeMS()
		log.Warn(msgText)
		if curMS-lastNotifyDelay > 600000 {
			// Delay reminders are sent every 10 minutes
			// 10 分钟发送一次延迟提醒
			lastNotifyDelay = curMS
			rpc.SendMsg(map[string]interface{}{
				"type":   rpc.MsgTypeException,
				"status": msgText,
			})
		}
	}
	stuckCount := 0
	_, err_ := core.Cron.Add("30 * * * * *", func() {
		curMS := btime.TimeMS()
		delaySecs := int((curMS - core.LastCopiedMs) / 1000)
		if delaySecs > 120 {
			// It should be received every minute, alert if haven't been received for more than 2 minutes
			// 应该每分钟都能收到，超过2分钟未收到爬虫推送报警
			logDelay("Listen to the spider kline timeout!")
			stuckCount += 1
			if stuckCount > config.CloseOnStuck {
				// 超时未收到K线，全部平仓
				for account := range config.Accounts {
					openOds, lock := ormo.GetOpenODs(account)
					lock.Lock()
					var odList = utils.ValsOfMap(openOds)
					lock.Unlock()
					if len(odList) > 0 {
						closeNum, failNum, err := biz.CloseAccOrders(account, odList, &strat.ExitReq{
							Tag:   core.ExitTagDataStuck,
							Force: true,
						})
						if err != nil {
							log.Error("close orders on stuck fail", zap.String("acc", account),
								zap.Int("success", closeNum), zap.Int("fail", failNum), zap.Error(err))
						} else {
							log.Warn(fmt.Sprintf("close orders on stuck: %s, %d closed, %d failed",
								account, closeNum, failNum))
						}
					}
				}
				// 防止频繁检查
				stuckCount = 0
			}
			return
		}
		stuckCount = 0
		var fails = make(map[string][]string)
		copied := com.GetPairCopieds()
		for pair, wait := range copied {
			if wait[0]+wait[1]*2 > curMS {
				continue
			}
			timeoutMin := strconv.Itoa(int((curMS-wait[0])/60000)) + "mins"
			arr, _ := fails[timeoutMin]
			fails[timeoutMin] = append(arr, pair)
		}
		if len(fails) > 0 {
			failText := core.GroupByPairQuotes(fails, false)
			logDelay("Listen to the spider kline timeout:" + failText)
		}
	})
	if err_ != nil {
		log.Error("add Monitor Klines fail", zap.Error(err_))
	}
}

func CronKlineSummary() {
	_, err_ := core.Cron.Add("30 1-59/10 * * * *", func() {
		core.TfPairHitsLock.Lock()
		var pairGroups = make(map[string][]string)
		for tf, tfMap := range core.TfPairHits {
			hitMap := make(map[int][]string)
			for pair, num := range tfMap {
				arr, _ := hitMap[num]
				hitMap[num] = append(arr, pair)
			}
			for num, arr := range hitMap {
				arrLen := len(arr)
				pairGroups[fmt.Sprintf("%s_%v: %v", tf, num, arrLen)] = arr
			}
			core.TfPairHits[tf] = make(map[string]int)
		}
		if len(pairGroups) > 0 {
			staText := core.GroupByPairQuotes(pairGroups, true)
			log.Info(fmt.Sprintf("receive bars in 10 mins:\n%s", staText))
		}
		core.TfPairHitsLock.Unlock()
	})
	if err_ != nil {
		log.Error("add Receive Klines Summary fail", zap.Error(err_))
	}
}

func CronDumpStratOutputs() {
	_, err_ := core.Cron.Add("31 * * * * *", func() {
		groups := make(map[string][]string)
		for _, items := range strat.PairStrats {
			for _, stgy := range items {
				if len(stgy.Outputs) == 0 {
					continue
				}
				rows, _ := groups[stgy.Name]
				groups[stgy.Name] = append(rows, stgy.Outputs...)
				stgy.Outputs = nil
			}
		}
		for name, lines := range groups {
			name = strings.ReplaceAll(name, ":", "_")
			fname := fmt.Sprintf("%s_%s.log", config.Name, name)
			outPath := filepath.Join(config.GetLogsDir(), fname)
			file, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Error("create strategy output file fail", zap.String("name", name), zap.Error(err))
				continue
			}
			_, err = file.WriteString(strings.Join(lines, "\n"))
			if err != nil {
				log.Error("write strategy output fail", zap.String("name", name), zap.Error(err))
			}
			_, _ = file.WriteString("\n")
			err = file.Close()
			if err != nil {
				log.Error("close strategy output fail", zap.String("name", name), zap.Error(err))
			}
		}
	})
	if err_ != nil {
		log.Error("add CronDumpStratOutputs fail", zap.Error(err_))
	}
}

func CronCheckTriggerOds() {
	// Check every minute 15 seconds to see if the limit order submission is triggered
	// 在每分钟的15s检查是否触发限价单提交
	_, err_ := core.Cron.Add("15,45 * * * * *", biz.VerifyTriggerOds)
	if err_ != nil {
		log.Error("add VerifyTriggerOds fail", zap.Error(err_))
	}
}

func CronBacktestInLive() {
	if config.BTInLive != nil && config.BTInLive.Cron != "" {
		_, err := core.Cron.Add(config.BTInLive.Cron, opt.BacktestToCompare)
		if err != nil {
			log.Error("add CronBacktestInLive fail", zap.Error(err))
		}
	}
}

func StartLoopBalancePositions() {
	for account := range config.Accounts {
		updateAccBalance(account)
	}
	go func() {
		ticker := time.NewTicker(time.Duration(config.AccountPullSecs) * time.Second)
		core.ExitCalls = append(core.ExitCalls, ticker.Stop)
		for {
			select {
			case <-ticker.C:
				updateBalancePos()
			}
		}
	}()
}

func updateBalancePos() {
	for account := range config.Accounts {
		odList, lock := ormo.GetOpenODs(account)
		lock.Lock()
		odNum := len(odList)
		lock.Unlock()
		if odNum == 0 {
			continue
		}
		if core.Market == banexg.MarketLinear || core.Market == banexg.MarketInverse {
			// 定期同步仓位检查不匹配订单，现货不支持
			odMgr := biz.GetLiveOdMgr(account)
			_, err := odMgr.SyncLocalOrders()
			if err != nil {
				log.Error("SyncLocalOrders fail", zap.String("acc", account), zap.Error(err))
			}
		}
		updateAccBalance(account)
	}
}

func updateAccBalance(account string) {
	wallet := biz.GetWallets(account)
	rsp, err := exg.Default.FetchBalance(map[string]interface{}{
		banexg.ParamAccount: account,
	})
	if err != nil {
		log.Error("UpdateBalance fail", zap.String("acc", account), zap.Error(err))
	} else {
		biz.UpdateWalletByBalances(wallet, rsp)
	}
}

func sendOrderMsg(od *ormo.InOutOrder, isEnter bool) {
	msgType := rpc.MsgTypeExit
	subOd := od.Exit
	action := "Close Long"
	if od.Short {
		action = "Close Short"
	}
	if isEnter {
		msgType = rpc.MsgTypeEntry
		subOd = od.Enter
		action = "Open Long"
		if od.Short {
			action = "Open Short"
		}
	}
	filled, price := subOd.Filled, subOd.Average
	account := ormo.GetTaskAcc(od.TaskID)
	if subOd.Status != ormo.OdStatusClosed || filled == 0 {
		log.Info("skip send rpc msg", zap.String("acc", account), zap.String("key", od.Key()),
			zap.Int64("status", subOd.Status), zap.Float64("filled", filled))
		return
	}
	rpc.SendMsg(map[string]interface{}{
		"type":          msgType,
		"account":       account,
		"action":        action,
		"enter_tag":     od.EnterTag,
		"exit_tag":      od.ExitTag,
		"side":          subOd.Side,
		"short":         od.Short,
		"leverage":      od.Leverage,
		"amount":        filled,
		"price":         price,
		"value":         filled * price,
		"cost":          filled * price / od.Leverage,
		"strategy":      od.Strategy,
		"pair":          od.Symbol,
		"timeframe":     od.Timeframe,
		"profit":        od.Profit,
		"profit_rate":   od.ProfitRate,
		"max_pft_rate":  od.MaxPftRate,
		"max_draw_down": od.MaxDrawDown,
	})
}
