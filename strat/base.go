package strat

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/banbox/banbot/com"

	utils2 "github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	ta "github.com/banbox/banta"
	"go.uber.org/zap"
)

/*
******************************  Membership methods of TradeStrat 成员方法  ***********************************
 */

func (s *TradeStrat) GetStakeAmount(j *StratJob) float64 {
	var amount float64
	account := ""
	if j != nil {
		account = j.Account
	}
	if cfg := s.runtimeConfigFor(j); cfg != nil {
		accounts, accountsMu := s.runtimeAccountsFor(j)
		amount = runtimeStakeAmount(cfg, accounts, accountsMu, account)
	} else {
		amount = config.GetStakeAmount(account)
	}
	// 乘以策略倍率
	if s.StakeRate > 0 {
		amount *= s.StakeRate
	}
	// 乘以此任务的开单倍率
	key := ""
	if j != nil && j.Strat != nil && j.Symbol != nil {
		key = core.KeyStratPairTf(j.Strat.Name, j.Symbol.Symbol, j.TimeFrame)
	}
	var pref core.JobPerf
	var hasPref bool
	if j != nil && j.runtimeCore != nil {
		pref, hasPref = j.runtimeCore.JobPerfValue(key)
	} else {
		pref, hasPref = core.LegacyJobPerfValue(key)
	}
	if hasPref {
		amount = pref.GetAmount(amount)
	}
	return amount
}

func (s *TradeStrat) runtimeConfigFor(j *StratJob) *config.Config {
	if j != nil && j.strategyState != nil && j.strategyState != legacyState && j.strategyState.Config != nil {
		return j.strategyState.Config
	}
	if s != nil && s.runtimeConfig != nil {
		return s.runtimeConfig
	}
	return nil
}

func (s *TradeStrat) runtimeAccountsFor(j *StratJob) (map[string]*config.AccountConfig, *sync.RWMutex) {
	if j != nil && j.strategyState != nil && j.strategyState != legacyState {
		if j.strategyState.runtimeAccounts != nil {
			return j.strategyState.runtimeAccounts, j.strategyState.runtimeAccountsMu
		}
	}
	if s != nil {
		return s.runtimeAccounts, s.runtimeAccountsMu
	}
	return nil, nil
}

func runtimeStakeAmount(cfg *config.Config, accounts map[string]*config.AccountConfig, accountsMu *sync.RWMutex, account string) float64 {
	if cfg == nil {
		return 0
	}
	if accountsMu != nil {
		accountsMu.RLock()
		defer accountsMu.RUnlock()
	}
	amount := cfg.StakeAmount
	if accounts == nil {
		accounts = cfg.Accounts
	}
	if acc, ok := accounts[account]; ok && acc != nil {
		if acc.StakePctAmt > 0 {
			amount = acc.StakePctAmt
		}
		if acc.StakeRate > 0 {
			amount *= acc.StakeRate
		}
		if acc.MaxStakeAmt > 0 && acc.MaxStakeAmt < amount {
			amount = acc.MaxStakeAmt
		}
	} else if cfg.MaxStakeAmt > 0 && cfg.MaxStakeAmt < amount {
		amount = cfg.MaxStakeAmt
	}
	return amount
}

func (s *StratJob) runtimeLive() bool {
	if s != nil && s.runtimeCore != nil {
		return s.runtimeCore.LiveMode
	}
	return core.LiveMode
}

func (s *StratJob) runtimeBacktest() bool {
	if s != nil && s.runtimeCore != nil {
		return s.runtimeCore.BackTestMode
	}
	return core.BackTestMode
}

func (s *StratJob) runtimeTimeMS() int64 {
	if s != nil && s.runtimeClock != nil {
		return s.runtimeClock.TimeMS()
	}
	return btime.TimeMS()
}

func (s *StratJob) addFailOpen(tag string) {
	if s != nil && s.strategyState != nil && s.strategyState != legacyState {
		s.strategyState.AddAccFailOpen(s.Account, tag)
		return
	}
	AddAccFailOpen(s.Account, tag)
}

func (s *StratJob) currentPrice(side string) float64 {
	if s != nil && s.runtimePrices != nil {
		price := s.runtimePrices.GetPriceSafeExpAt(s.runtimeTimeMS(), s.Symbol.Symbol, side, com.PriceExpireMS)
		if price >= 0 {
			return price
		}
	}
	if s != nil && s.Env != nil {
		return s.Env.Close.Get(0)
	}
	return -1
}

func (s *TradeStrat) WriteOutput(line string, date bool) {
	if date {
		nowMS := btime.TimeMS()
		if s != nil && s.runtimeClock != nil {
			nowMS = s.runtimeClock.TimeMS()
		}
		prefix := btime.ToDateStr(nowMS, core.DefaultDateFmt)
		line = prefix + " " + line
	}
	if state := s.outputStateOwner(); state != nil {
		state.mu.Lock()
		s.Outputs = append(s.Outputs, line)
		state.mu.Unlock()
	}
}

/*
Select the time period to trade from several candidate time periods. This method is called by the system
从若干候选时间周期中选择要交易的时间周期。此方法由系统调用
*/
func (s *TradeStrat) pickTimeFrame(symbol string, tfScores map[string]float64) string {
	if len(tfScores) == 0 {
		return ""
	}
	// 过滤当前需要的时间周期
	useTfs := make(map[string]bool)
	tfList := s.RunTimeFrames
	if len(s.Policy.RunTimeframes) > 0 {
		tfList = s.Policy.RunTimeframes
	}
	if len(tfList) == 0 {
		if s.runtimeConfig != nil {
			tfList = s.runtimeConfig.RunTimeframes
		} else if !s.runtimeExplicit {
			tfList = config.RunTimeframes
		}
	}
	for _, tf := range tfList {
		useTfs[tf] = true
	}
	curScores := make([]*core.TfScore, 0, len(tfScores))
	for tf, score := range tfScores {
		if _, ok := useTfs[tf]; ok {
			curScores = append(curScores, &core.TfScore{TF: tf, Score: score})
		}
	}
	slices.SortFunc(curScores, func(a, b *core.TfScore) int {
		if order := cmp.Compare(a.Score, b.Score); order != 0 {
			return order
		}
		return cmp.Compare(a.TF, b.TF)
	})
	if s.PickTimeFrame != nil {
		return s.PickTimeFrame(symbol, curScores)
	}
	for _, tfs := range curScores {
		if tfs.Score >= s.MinTfScore {
			return tfs.TF
		}
	}
	return ""
}

func (s *TradeStrat) orderBarMax() int {
	if s.OdBarMax > 0 {
		return s.OdBarMax
	}
	if s.runtimeConfig != nil && s.runtimeConfig.OrderBarMax > 0 {
		return s.runtimeConfig.OrderBarMax
	}
	if s.runtimeExplicit {
		return 500
	}
	return config.OrderBarMax
}

/*
*****************************  StratJob的成员方法   ****************************************
 */
func (s *StratJob) CanOpen(short bool) bool {
	disable := false
	state := s.executionState()
	state.mu.Lock()
	if short {
		disable = s.MaxOpenShort < 0 || s.MaxOpenShort > 0 && len(s.ShortOrders) >= s.MaxOpenShort
		if s.runtimeMarket() == banexg.MarketSpot {
			disable = true
		}
	} else {
		disable = s.MaxOpenLong < 0 || s.MaxOpenLong > 0 && len(s.LongOrders) >= s.MaxOpenLong
	}
	state.mu.Unlock()
	return !disable
}

// runtimeMarket keeps explicit jobs independent from the process-wide market.
// Symbol identity wins over the shared BarEnv because a legacy env cache may
// be reused while a job still owns its runtime-local ExSymbol.
func (s *StratJob) runtimeMarket() string {
	if s == nil {
		return core.Market
	}
	if s.Symbol != nil && s.Symbol.Market != "" {
		return s.Symbol.Market
	}
	if s.Env != nil && s.Env.MarketType != "" {
		return s.Env.MarketType
	}
	if s.symbols != nil {
		return ""
	}
	return core.Market
}

func (s *StratJob) OpenOrder(req *EnterReq) *errs.Error {
	if s.recordInspectEffect("OpenOrder") {
		return errs.NewMsg(core.ErrRunTime, "OpenOrder is forbidden during strategy inspection")
	}
	doLog := !s.IsWarmUpState() && req != nil && req.Log
	var q *EnterReq
	if doLog {
		q = req.Clone()
	}
	err := s.openOrder(req)
	if err != nil && q != nil {
		fields := append(q.GetZapFields(s), zap.String("err", err.Short()))
		log.Warn("OpenOrder fail", fields...)
	}
	return err
}

func (s *StratJob) openOrder(req *EnterReq) *errs.Error {
	if req == nil {
		return errs.NewMsg(errs.CodeParamRequired, "req cannot be nil")
	}
	if req.Tag == "" {
		return errs.NewMsg(errs.CodeParamRequired, "tag is Required")
	}
	req.StratName = s.Strat.Name
	isLiveMode := s.runtimeLive()
	symbol := s.Symbol.Symbol
	var dirType = core.OdDirtLong
	if req.Short {
		dirType = core.OdDirtShort
	}
	if !s.CanOpen(req.Short) {
		if isLiveMode {
			log.Warn("open order disabled",
				zap.String("strategy", s.Strat.Name),
				zap.String("pair", symbol),
				zap.String("tag", req.Tag),
				zap.Int("dir", dirType))
		}
		s.addFailOpen(FailOpenBadDirtOrLimit)
		return errs.NewMsg(errs.CodeParamInvalid, "open order disabled")
	}

	if math.IsNaN(req.Limit+req.Amount+req.Leverage+req.CostRate+req.LegalCost) ||
		math.IsNaN(req.StopLoss+req.StopLossVal+req.StopLossLimit+req.StopLossRate) ||
		math.IsNaN(req.TakeProfit+req.TakeProfitVal+req.TakeProfitLimit+req.TakeProfitRate) {
		s.addFailOpen(FailOpenNanNum)
		return errs.NewMsg(errs.CodeParamInvalid, "nan in EnterReq")
	}
	if req.CallbackPct > 0 && (req.CallbackPct < 0.1 || req.CallbackPct > 10) {
		return errs.NewMsg(errs.CodeParamInvalid, "CallbackPct should be in [0.1, 10]")
	}
	// 检查价格是否有效
	dirFlag := 1.0
	if req.Short {
		dirFlag = -1.0
	}
	odSide := banexg.OdSideBuy
	if req.Short {
		odSide = banexg.OdSideSell
	}
	var curPrice float64
	isWarmUp := s.IsWarmUpState()
	if isWarmUp {
		curPrice = s.Env.Close.Get(0)
	} else {
		if s.runtimeCore != nil {
			curPrice = s.currentPrice(odSide)
		} else {
			_ = com.EnsureLatestPrice(symbol)
			curPrice = com.GetPriceSafe(symbol, odSide)
			if curPrice < 0 {
				curPrice = s.Env.Close.Get(0)
			}
		}
	}
	enterPrice := curPrice
	isLimit := core.IsLimitOrder(req.OrderType)
	if isLimit && req.Limit == 0 {
		req.Limit = enterPrice
	}
	if req.Limit > 0 {
		if req.Stop > 0 {
			return errs.NewMsg(errs.CodeParamInvalid, "Stop/Limit can not be used together")
		}
		if (req.Limit-enterPrice)*dirFlag < 0 {
			enterPrice = req.Limit
		}
	}
	if req.Stop > 0 {
		legacyIntrabar := false
		if !s.Strat.runtimeExplicit {
			legacyIntrabar = config.Data.BTLegacyIntrabar
		}
		if s.Strat.runtimeConfig != nil {
			legacyIntrabar = s.Strat.runtimeConfig.BTLegacyIntrabar
		}
		enterPrice = normalizeEntryStop(req, curPrice, isLimit, s.runtimeBacktest(), legacyIntrabar)
	}
	if req.Amount == 0 && req.LegalCost == 0 {
		if req.CostRate == 0 {
			req.CostRate = 1
		}
		req.LegalCost = s.Strat.GetStakeAmount(s) * req.CostRate
		avgVol := s.avgVolume(5) // 最近5个蜡烛成交量
		reqAmt := req.LegalCost / enterPrice
		openVolRate := 1.0
		lowCostAction := ""
		if !s.Strat.runtimeExplicit {
			openVolRate = config.OpenVolRate
			lowCostAction = config.LowCostAction
		}
		if s.Strat.runtimeConfig != nil {
			openVolRate = s.Strat.runtimeConfig.OpenVolRate
			if openVolRate == 0 {
				openVolRate = 1
			}
			lowCostAction = s.Strat.runtimeConfig.LowCostAction
		}
		if avgVol > 0 && reqAmt/avgVol > openVolRate {
			req.LegalCost = avgVol * openVolRate * enterPrice
			if isLiveMode {
				log.Info(fmt.Sprintf("%v open amt rate: %.1f > open_vol_rate(%.1f), cut to cost: %.1f",
					symbol, reqAmt/avgVol, openVolRate, req.LegalCost))
			}
		}
		minCost := core.MinStakeAmount
		if req.LegalCost < minCost {
			rate := req.LegalCost / minCost
			if lowCostAction == core.LowCostKeepBig && rate > 0.4 || lowCostAction == core.LowCostKeepAll {
				req.LegalCost = minCost * 1.1
			} else {
				s.addFailOpen(FailOpenCostTooLess)
				return errs.NewMsg(errs.CodeParamInvalid, "legal cost must >= %.2f", minCost)
			}
		}
	}
	// 检查止损
	curSLPrice := s.LongSLPrice
	if req.Short {
		curSLPrice = s.ShortSLPrice
	}
	if curSLPrice == 0 && s.Strat.StopLoss > 0 {
		curSLPrice = enterPrice * (1 - s.Strat.StopLoss*dirFlag)
	}
	if req.StopLossLimit > 0 && req.StopLoss == 0 {
		req.StopLoss = req.StopLossLimit
	}
	if req.StopLossVal > 0 {
		curSLPrice = enterPrice - req.StopLossVal*dirFlag
	} else if req.StopLoss != 0 {
		curSLPrice = req.StopLoss
	}
	if curSLPrice < 0 {
		curSLPrice = enterPrice * 0.001
	}
	req.StopLossVal = 0
	req.StopLoss = 0
	if curSLPrice > 0 {
		if s.ExgStopLoss {
			if (curSLPrice-enterPrice)*dirFlag >= 0 {
				rel := "<"
				if req.Short {
					rel = ">"
				}
				s.addFailOpen(FailOpenBadStopLoss)
				return errs.NewMsg(errs.CodeParamInvalid, "%s stopLoss %f must %s %f for %v order",
					symbol, curSLPrice, rel, enterPrice, dirType)
			}
			req.StopLoss = curSLPrice
		} else if isLiveMode {
			log.Warn("stopLoss disabled",
				zap.String("strategy", s.Strat.Name),
				zap.String("pair", symbol))
		}
	}
	// 检查止盈
	curTPPrice := s.LongTPPrice
	if req.Short {
		curTPPrice = s.ShortTPPrice
	}
	if req.TakeProfitLimit > 0 && req.TakeProfit == 0 {
		req.TakeProfit = req.TakeProfitLimit
	}
	if req.TakeProfitVal > 0 {
		curTPPrice = enterPrice + req.TakeProfitVal*dirFlag
	} else {
		curTPPrice = req.TakeProfit
	}
	if curTPPrice < 0 {
		curTPPrice = enterPrice * 0.001
	}
	req.TakeProfitVal = 0
	req.TakeProfit = 0
	if curTPPrice > 0 {
		if s.ExgTakeProfit {
			if (curTPPrice-enterPrice)*dirFlag <= 0 {
				rel := ">"
				if req.Short {
					rel = "<"
				}
				s.addFailOpen(FailOpenBadTakeProfit)
				return errs.NewMsg(errs.CodeParamInvalid, "%s takeProfit %f must %s %f for %v order",
					symbol, curTPPrice, rel, enterPrice, dirType)
			}
			req.TakeProfit = curTPPrice
		} else if isLiveMode {
			log.Warn("takeProfit disabled", zap.String("stagy", s.Strat.Name), zap.String("pair", symbol))
		}
	}
	if req.Limit > 0 && req.OrderType == 0 {
		req.OrderType = core.OrderTypeLimit
	}
	if req.Limit > 0 {
		// 是限价入场单
		if req.StopBars == 0 {
			req.StopBars = s.Strat.StopEnterBars
		}
		if req.OrderType == core.OrderTypeLimitMaker {
			if req.Short {
				if req.Limit < curPrice {
					// 做空卖出，价格更低，立刻成交，maker不满足退出
					return errs.NewMsg(errs.CodeParamInvalid, "[short] Limit %f must >= market price %f", req.Limit, curPrice)
				}
			} else if req.Limit > curPrice {
				// 做多买入，价格更高，立刻成交，maker不满足退出
				return errs.NewMsg(errs.CodeParamInvalid, "[long] Limit %f must <= market price %f", req.Limit, curPrice)
			}
		}
	}
	if s.enqueueEntry(req) {
		if isLiveMode {
			log.Info("OpenOrder", req.GetZapFields(s)...)
		}
	}
	return nil
}

func normalizeEntryStop(req *EnterReq, curPrice float64, isLimit, backtest, legacyIntrabar bool) float64 {
	stopPrice := req.Stop
	if backtest && legacyIntrabar {
		return curPrice
	}
	stopActsAsLimit := req.Stop < curPrice
	if req.Short {
		stopActsAsLimit = req.Stop > curPrice
	}
	if stopActsAsLimit {
		isLimit = true
	}
	if isLimit {
		req.Limit = req.Stop
		req.Stop = 0
	}
	return stopPrice
}

func (s *StratJob) CloseOrders(req *ExitReq) *errs.Error {
	if s.recordInspectEffect("CloseOrders") {
		return errs.NewMsg(core.ErrRunTime, "CloseOrders is forbidden during strategy inspection")
	}
	doLog := !s.IsWarmUpState() && req != nil && req.Log
	var q *ExitReq
	if doLog {
		q = req.Clone()
	}
	err := s.closeOrders(req)
	if err != nil && q != nil {
		fields := append(q.GetZapFields(s), zap.String("err", err.Short()))
		log.Warn("CloseOrders fail", fields...)
	}
	return err
}

func (s *StratJob) closeOrders(req *ExitReq) *errs.Error {
	if s.GetOrderNum(float64(req.Dirt)) == 0 {
		return nil
	}
	if req.Tag == "" {
		return errs.NewMsg(errs.CodeParamRequired, "tag is required")
	}
	if req.StratName == "" {
		req.StratName = s.Strat.Name
	}
	dirtBoth := req.Dirt == core.OdDirtBoth
	if !s.CloseShort && (dirtBoth || req.Dirt == core.OdDirtShort) || !s.CloseLong && (dirtBoth || req.Dirt == core.OdDirtLong) {
		log.Warn("close order disabled",
			zap.String("strategy", s.Strat.Name),
			zap.String("pair", s.Symbol.Symbol),
			zap.String("tag", req.Tag),
			zap.Int("dirt", req.Dirt))
		return errs.NewMsg(errs.CodeParamInvalid, "close order disabled")
	}
	if req.ExitRate > 1 {
		return errs.NewMsg(errs.CodeParamInvalid, "ExitRate shoud in (0, 1], current: %f", req.ExitRate)
	} else if req.ExitRate == 0 {
		req.ExitRate = 1
	}
	if req.Limit > 0 && req.OrderType == 0 {
		req.OrderType = core.OrderTypeLimit
	}
	if req.Limit > 0 {
		if !core.IsLimitOrder(req.OrderType) {
			return errs.NewMsg(errs.CodeParamInvalid, "`ExitReq.Limit` is invalid for Market order")
		}
		snapshot := s.ExecutionSnapshot()
		hasLong := len(snapshot.LongOrders) > 0
		hasShort := len(snapshot.ShortOrders) > 0
		if req.Dirt == core.OdDirtBoth {
			if hasLong && hasShort {
				return errs.NewMsg(errs.CodeParamInvalid, "`ExitReq.Dirt` is required with long & short positions")
			} else if hasLong {
				req.Dirt = core.OdDirtLong
			} else if hasShort {
				req.Dirt = core.OdDirtShort
			}
		}
		if req.Limit > 0 && req.OrderType == core.OrderTypeLimitMaker {
			odSide := ""
			if req.Dirt == core.OdDirtLong {
				odSide = banexg.OdSideSell
			} else if req.Dirt == core.OdDirtShort {
				odSide = banexg.OdSideBuy
			}
			var curPrice float64
			if snapshot.IsWarmUp {
				curPrice = s.Env.Close.Get(0)
			} else {
				if s.runtimeCore != nil {
					curPrice = s.currentPrice(odSide)
				} else {
					_ = com.EnsureLatestPrice(s.Symbol.Symbol)
					curPrice = com.GetPriceSafe(s.Symbol.Symbol, odSide)
					if curPrice < 0 {
						curPrice = s.Env.Close.Get(0)
					}
				}
			}
			sl := &ormo.ExitTrigger{
				Price: req.Limit,
				Limit: req.Limit,
				Tag:   req.Tag,
			}
			if req.Dirt == core.OdDirtShort {
				if req.Limit > curPrice {
					// 平空买入，价格更高，立刻成交，maker改为限价止损
					for _, od := range snapshot.ShortOrders {
						err := od.SetStopLoss(sl)
						if err != nil {
							return err
						}
					}
					return nil
				}
			} else if req.Limit < curPrice {
				// 平多卖出，价格更低，立刻成交，maker改为限价止损
				for _, od := range snapshot.LongOrders {
					err := od.SetStopLoss(sl)
					if err != nil {
						return err
					}
				}
				return nil
			}
		}
	}
	if s.enqueueExit(req) {
		if s.runtimeLive() {
			log.Info("CloseOrders", req.GetZapFields(s)...)
		}
	}
	return nil
}

/*
avgVolume
Calculate the average trading volume of the latest num candlesticks
计算最近num个K线的平均成交量
*/
func (s *StratJob) avgVolume(num int) float64 {
	arr := s.Env.Volume.Range(0, num)
	if len(arr) == 0 {
		return 0
	}
	sumVal := float64(0)
	for _, val := range arr {
		sumVal += val
	}
	return sumVal / float64(len(arr))
}

/*
Calculate the current order and the drawdown distance from maximum profit
Return: Withdrawal ratio after profit, entry price, maximum profit margin
计算当前订单，距离最大盈利的回撤
返回：盈利后回撤比例，入场价格，最大利润率
*/
func (s *StratJob) getMaxTp(od *ormo.InOutOrder) (float64, float64, float64) {
	entPrice := od.Enter.Average
	if entPrice == 0 {
		entPrice = od.InitPrice
	}
	exmPrice, ok := s.TPMaxs[od.ID]
	if !ok {
		exmPrice = od.InitPrice
	}
	var cmp func(float64, float64) float64
	var price float64
	if od.Short {
		price = s.Env.Low.Get(0)
		cmp = func(f float64, f2 float64) float64 {
			return min(f, f2)
		}
	} else {
		price = s.Env.High.Get(0)
		cmp = func(f float64, f2 float64) float64 {
			return max(f, f2)
		}
	}
	exmPrice = cmp(exmPrice, price)
	s.TPMaxs[od.ID] = exmPrice
	maxTPVal := math.Abs(exmPrice - entPrice)
	maxChg := maxTPVal / entPrice
	if utils.EqualNearly(maxTPVal, 0.0) {
		return 0, entPrice, maxChg
	}
	backVal := math.Abs(exmPrice - s.Env.Close.Get(0))
	return backVal / maxTPVal, entPrice, maxChg
}

func CalcDrawDownExitRate(maxChg float64) float64 {
	var rate float64
	switch {
	case maxChg > 0.1:
		rate = 0.15
	case maxChg > 0.04:
		rate = 0.17
	case maxChg > 0.025:
		rate = 0.25
	case maxChg > 0.015:
		rate = 0.37
	case maxChg > 0.007:
		rate = 0.5
	default:
		rate = 0
	}
	return rate
}

func (s *StratJob) getDrawDownExitPrice(od *ormo.InOutOrder) float64 {
	_, entPrice, exmChg := s.getMaxTp(od)
	var stopRate = float64(-1)
	if s.Strat.GetDrawDownExitRate != nil {
		stopRate = s.Strat.GetDrawDownExitRate(s, od, exmChg)
	}
	if stopRate < 0 {
		// 如果策略返回负数，则表示使用默认算法
		stopRate = CalcDrawDownExitRate(exmChg)
	}
	if utils.EqualNearly(stopRate, 0) {
		return 0
	}
	odDirt := 1.0
	if od.Short {
		odDirt = -1
	}
	return entPrice * (1 + exmChg*(1-stopRate)*odDirt)
}

/*
drawDownExit
Check whether the tracking profit check has reached the drawdown threshold. If it exceeds the threshold, exit. This method is called by the system
按跟踪止盈检查是否达到回撤阈值，超出则退出，此方法由系统调用
*/
func (s *StratJob) drawDownExit(od *ormo.InOutOrder) *ExitReq {
	spVal := s.getDrawDownExitPrice(od)
	if spVal == 0 {
		return nil
	}
	curPrice := s.Env.Close.Get(0)
	odDirt := 1.0
	if od.Short {
		odDirt = -1
	}
	if (spVal-curPrice)*odDirt >= 0 {
		return &ExitReq{Tag: core.ExitTagDrawDown, OrderID: od.ID}
	}
	_ = od.SetStopLoss(&ormo.ExitTrigger{
		Price: spVal,
		Tag:   core.ExitTagDrawDown,
	})
	return nil
}

/*
customExit
Check if the order needs to be exited, this method is called by the system
检查订单是否需要退出，此方法由系统调用
*/
func (s *StratJob) customExit(od *ormo.InOutOrder) (*ExitReq, *errs.Error) {
	var req *ExitReq
	if s.Strat.OnCheckExit != nil {
		req = s.Strat.OnCheckExit(s, od)
		if req != nil {
			req.OrderID = od.ID
			if od.Short {
				req.Dirt = core.OdDirtShort
			} else {
				req.Dirt = core.OdDirtLong
			}
		}
	}
	if req == nil && s.Strat.DrawDownExit && od.Status >= ormo.InOutStatusFullEnter {
		// 只对已完全入场的订单启用回撤平仓
		req = s.drawDownExit(od)
	}
	var err *errs.Error
	if req != nil {
		err = s.CloseOrders(req)
	}
	return req, err
}

/*
Position
Retrieve the position size and return a multiple based on the benchmark amount.
获取仓位大小，返回基于基准金额的倍数。
side long/short/空
enterTag 入场标签，可为空
*/
func (s *StratJob) Position(dirt float64, enterTag string) float64 {
	var totalCost float64
	orders := s.GetOrders(dirt)
	for _, od := range orders {
		if enterTag != "" && od.EnterTag != enterTag {
			continue
		}
		totalCost += od.HoldCost()
	}
	return totalCost / s.Strat.GetStakeAmount(s)
}

func (s *StratJob) PositionAvgPrice(dirt float64, enterTag string) float64 {
	var totalCost, totalAmount float64
	if dirt == core.OdDirtBoth {
		panic("`dirt` for PositionAvgPrice must be core.OdDirtLong/OdDirtShort")
	}
	orders := s.GetOrders(dirt)
	for _, od := range orders {
		if enterTag != "" && od.EnterTag != enterTag {
			continue
		}
		totalCost += od.HoldCost()
		totalAmount += od.HoldAmount()
	}
	if totalAmount > 0 {
		return totalCost / totalAmount
	}
	return 0
}

func (s *StratJob) GetOrders(dirt float64) []*ormo.InOutOrder {
	snapshot := s.ExecutionSnapshot()
	if dirt < 0 {
		return snapshot.ShortOrders
	} else if dirt > 0 {
		return snapshot.LongOrders
	} else {
		res := make([]*ormo.InOutOrder, 0, len(snapshot.ShortOrders)+len(snapshot.LongOrders))
		res = append(res, snapshot.ShortOrders...)
		res = append(res, snapshot.LongOrders...)
		return res
	}
}

func (s *StratJob) GetOrderNum(dirt float64) int {
	snapshot := s.ExecutionSnapshot()
	if dirt > 0 {
		return len(snapshot.LongOrders)
	} else if dirt < 0 {
		return len(snapshot.ShortOrders)
	} else {
		return len(snapshot.LongOrders) + len(snapshot.ShortOrders)
	}
}

func (s *StratJob) GetAvgCostPrice(odList ...*ormo.InOutOrder) float64 {
	if len(odList) == 0 {
		return 0
	}
	totalCost := float64(0)
	totalAmt := float64(0)
	for _, od := range odList {
		totalCost += od.EnterCost()
		totalAmt += od.Enter.Filled
	}
	if totalAmt == 0 {
		return 0
	}
	return totalCost / totalAmt
}

func (s *StratJob) GetTmpEnv(stamp int64, o, h, l, c, v, quote, buyVolume float64, tradeNum int64) *ta.BarEnv {
	envKey := strings.Join([]string{s.Symbol.Symbol, s.TimeFrame}, "_")
	var e *ta.BarEnv
	tmpEnvs := TmpEnvs
	if s.strategyState != nil && s.strategyState != legacyState {
		state := s.strategyState
		state.ensureMaps()
		state.tmpEnvLock.Lock()
		tmpEnvs = state.TmpEnvs
		e, ok := tmpEnvs[envKey]
		if !ok {
			e = s.Env.Clone()
			tmpEnvs[envKey] = e
		}
		state.tmpEnvLock.Unlock()
	} else {
		lockTmpEnv.Lock()
		e, ok := tmpEnvs[envKey]
		if !ok {
			e = s.Env.Clone()
			tmpEnvs[envKey] = e
		}
		lockTmpEnv.Unlock()
	}
	if e.TimeStop < stamp {
		// update on new data
		barMs := utils2.AlignTfMSecs(stamp, e.TFMSecs)
		if barMs > e.TimeStart && barMs == s.Env.TimeStop {
			// enter new kline
			e = s.Env.Clone()
			if s.strategyState != nil && s.strategyState != legacyState {
				state := s.strategyState
				state.tmpEnvLock.Lock()
				state.TmpEnvs[envKey] = e
				state.tmpEnvLock.Unlock()
			} else {
				lockTmpEnv.Lock()
				TmpEnvs[envKey] = e
				lockTmpEnv.Unlock()
			}
			e.OnBar2(barMs, stamp, o, o, o, o, 0, 0, 0, 0)
		} else {
			e.ResetTo(s.Env)
		}
		h1, l1 := e.High.Get(0), e.Low.Get(0)
		if h1 < h {
			e.High.Data[len(e.High.Data)-1] = h
		}
		if l1 > l {
			e.Low.Data[len(e.Low.Data)-1] = l
		}
		e.Close.Data[len(e.Close.Data)-1] = c
		e.Volume.Data[len(e.Volume.Data)-1] = v
		e.Quote.Data[len(e.Quote.Data)-1] = quote
		e.BuyVolume.Data[len(e.BuyVolume.Data)-1] = buyVolume
		e.TradeNum.Data[len(e.TradeNum.Data)-1] = float64(tradeNum)
		e.TimeStop = stamp
	}
	return e
}

func (s *StratJob) setAllExitTrigger(dirt float64, key string, args *ormo.ExitTrigger) *errs.Error {
	if s.GetOrderNum(dirt) == 0 {
		return nil
	}
	snapshot := s.ExecutionSnapshot()
	if dirt == 0 && len(snapshot.LongOrders) > 0 && len(snapshot.ShortOrders) > 0 {
		panic(fmt.Sprintf("%v SetAll%s.dirt should be 1/-1 when both long/short orders exists!", s.Strat.Name, key))
	}
	odList := s.GetOrders(dirt)
	if args == nil {
		// 取消所有止盈或止损
		for _, od := range odList {
			_ = od.SetExitTrigger(key, nil, 0)
		}
		return nil
	}
	var entOds = make([]*ormo.InOutOrder, 0, len(odList))
	var position float64
	setAll := args.Rate <= 0 || args.Rate >= 1
	for _, od := range odList {
		if od.Status >= ormo.InOutStatusPartEnter && od.Status <= ormo.InOutStatusPartExit {
			if setAll {
				err := od.SetExitTrigger(key, args, 0)
				if err != nil {
					return err
				}
			} else {
				entOds = append(entOds, od)
				position += od.HoldAmount()
			}
		}
	}
	if setAll || len(entOds) == 0 {
		return nil
	}
	setPos := position * args.Rate
	for _, od := range entOds {
		size := od.HoldAmount()
		if size < core.AmtDust {
			continue
		}
		if setPos < core.AmtDust {
			_ = od.SetExitTrigger(key, nil, 0)
		} else {
			if setPos >= size+core.AmtDust {
				err := od.SetExitTrigger(key, &ormo.ExitTrigger{
					Price: args.Price,
					Limit: args.Limit,
					Tag:   args.Tag,
				}, 0)
				if err != nil {
					return err
				}
				setPos -= size
			} else {
				err := od.SetExitTrigger(key, &ormo.ExitTrigger{
					Price: args.Price,
					Limit: args.Limit,
					Rate:  setPos / size,
					Tag:   args.Tag,
				}, 0)
				if err != nil {
					return err
				}
				setPos = 0
			}
		}
	}
	return nil
}

func (s *StratJob) SetAllStopLoss(dirt float64, args *ormo.ExitTrigger) *errs.Error {
	if s.recordInspectEffect("SetAllStopLoss") {
		return errs.NewMsg(core.ErrRunTime, "SetAllStopLoss is forbidden during strategy inspection")
	}
	return s.setAllExitTrigger(dirt, ormo.OdInfoStopLoss, args)
}

func (s *StratJob) SetAllTakeProfit(dirt float64, args *ormo.ExitTrigger) *errs.Error {
	if s.recordInspectEffect("SetAllTakeProfit") {
		return errs.NewMsg(core.ErrRunTime, "SetAllTakeProfit is forbidden during strategy inspection")
	}
	return s.setAllExitTrigger(dirt, ormo.OdInfoTakeProfit, args)
}

func (s *StratJob) recordInspectEffect(name string) bool {
	if s.inspectEffect == nil {
		return false
	}
	s.inspectEffect(name)
	return true
}
