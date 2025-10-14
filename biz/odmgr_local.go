package biz

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"maps"
	"strings"
)

type LocalOrderMgr struct {
	OrderMgr
	showLog  bool
	zeroAmts map[string]int
}

type FnOdCb = func(od *ormo.InOutOrder, isEnter bool)

func InitLocalOrderMgr(callBack FnOdCb, showLog bool) {
	for account := range config.Accounts {
		_, ok := accOdMgrs[account]
		if !ok {
			odMgr := &LocalOrderMgr{
				OrderMgr: OrderMgr{
					callBack: callBack,
					Account:  account,
				},
				showLog:  showLog,
				zeroAmts: make(map[string]int),
			}
			odMgr.afterEnter = makeLocalAfterEnter(odMgr)
			accOdMgrs[account] = odMgr
		}
	}
}

func (o *LocalOrderMgr) ProcessOrders(sess *ormo.Queries, job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	return o.OrderMgr.ProcessOrders(sess, job)
}

func (o *LocalOrderMgr) UpdateByBar(allOpens []*ormo.InOutOrder, bar *orm.InfoKline) *errs.Error {
	if len(allOpens) == 0 || core.EnvReal || core.LiveMode {
		return nil
	}
	// Simulate order entry and exit, which are usually executed at the beginning of the bar
	// 模拟订单入场出场，入场出场一般在bar开始时执行
	var curOrders []*ormo.InOutOrder
	var curMap = make(map[int64]bool)
	for _, od := range allOpens {
		if od.Symbol == bar.Symbol {
			curOrders = append(curOrders, od)
			curMap[od.ID] = true
			if od.Exit != nil {
				curMap[-od.ID] = true
			}
		}
	}
	if len(curOrders) == 0 && !core.CheckWallets {
		return nil
	}
	curOrders, err := o.fillPendingOrdersAll(curOrders, curMap, bar)
	if err != nil {
		return err
	}
	return o.updateProfitAndWallets(allOpens, curOrders, bar)
}

func (o *LocalOrderMgr) updateProfitAndWallets(allOpens, curOrders []*ormo.InOutOrder, bar *orm.InfoKline) *errs.Error {
	// Update all orders to profit at the end of the bar
	// 更新所有订单在bar结束时利润
	err := o.OrderMgr.UpdateByBar(curOrders, bar)
	if err != nil {
		return err
	}
	if core.IsContract && core.CheckWallets {
		// Update all order margins and wallet status of this pricing currency for the contract
		// 为合约更新此定价币的所有订单保证金和钱包情况
		_, _, code, _ := core.SplitSymbol(bar.Symbol)
		var orders []*ormo.InOutOrder
		for _, od := range allOpens {
			_, _, odSettle, _ := core.SplitSymbol(od.Symbol)
			if odSettle == code && od.Status < ormo.InOutStatusFullExit {
				orders = append(orders, od)
			}
		}
		wallets := GetWallets(o.Account)
		err = wallets.UpdateOds(orders, code)
	}
	return err
}

func (o *LocalOrderMgr) fillPendingOrdersAll(orders []*ormo.InOutOrder, curMap map[int64]bool, bar *orm.InfoKline) ([]*ormo.InOutOrder, *errs.Error) {
	_, err := o.fillPendingOrders(orders, bar)
	if err != nil {
		return orders, err
	}
	// 在订单事件回调中可能触发新订单入场
	checkCount := 0
	for core.NewNumInSim > 0 {
		openOds, lock := ormo.GetOpenODs(o.Account)
		var newOds []*ormo.InOutOrder
		lock.Lock()
		for _, od := range openOds {
			if !(bar == nil || od.Symbol == bar.Symbol) {
				continue
			}
			if _, ok := curMap[od.ID]; !ok {
				newOds = append(newOds, od)
				orders = append(orders, od)
				curMap[od.ID] = true
			}
			if od.Exit != nil {
				if _, ok := curMap[-od.ID]; !ok {
					newOds = append(newOds, od)
					curMap[-od.ID] = true
				}
			}
		}
		lock.Unlock()
		if len(newOds) > 0 {
			_, err = o.fillPendingOrders(newOds, bar)
			if err != nil {
				return orders, err
			}
			checkCount += 1
			if checkCount > 30 {
				return orders, errs.NewMsg(errs.CodeRunTime, "OpenOrder in OnOrderChange callstack exceed 30 times")
			}
		} else {
			break
		}
	}
	return orders, nil
}

/*
fillPendingOrders
Fills orders waiting for exchange response. Cannot be used for real trading; can be used for backtesting, simulated real trading, etc.
填充等待交易所响应的订单。不可用于实盘；可用于回测、模拟实盘等。
*/
func (o *LocalOrderMgr) fillPendingOrders(orders []*ormo.InOutOrder, bar *orm.InfoKline) (int, *errs.Error) {
	core.SimOrderMatch = true
	core.NewNumInSim = 0
	defer func() {
		core.SimOrderMatch = false
	}()
	affectNum := 0
	for _, od := range orders {
		if bar != nil && bar.TimeFrame != od.Timeframe {
			continue
		}
		var exOrder *ormo.ExOrder
		if od.ExitTag != "" && od.Exit != nil && od.Exit.Status < ormo.OdStatusClosed {
			exOrder = od.Exit
		} else if od.Enter.Status < ormo.OdStatusClosed {
			exOrder = od.Enter
		} else {
			if od.ExitTag == "" && bar != nil {
				// 已入场完成，尚未出现出场信号，检查是否触发止损The entry has been completed, but the exit signal has not yet appeared. Check whether the stop loss is triggered.
				err := o.tryFillTriggers(od, &bar.Kline)
				if err != nil {
					return 0, err
				}
			}
			continue
		}
		odType := config.OrderType
		if exOrder.OrderType != "" {
			odType = exOrder.OrderType
		}
		price := exOrder.Price
		odTFSecs := utils.TFToSecs(od.Timeframe)
		fillMS := exOrder.CreateAt + int64(config.BTNetCost*1000)
		barStartMS := utils.AlignTfMSecs(fillMS, int64(odTFSecs*1000))
		odIsBuy := exOrder.Side == banexg.OdSideBuy
		var minRate float64
		var fillBarRate float64
		var isStopEnter bool
		if exOrder.Enter && od.Stop > 0 && bar != nil {
			// 使用触发价格，enterOrder中已判断有效性
			trigPrice := od.Stop
			if trigPrice < bar.Low || trigPrice > bar.High {
				// 不处于bar的范围，无法触发
				continue
			}
			price = trigPrice
			od.Stop = 0
			if strings.Contains(odType, "limit") && exOrder.Price > 0 && (exOrder.Price > trigPrice) == od.Short {
				// 触发价满足，有额外限价单
				price = exOrder.Price
			}
			minRate = float64((exOrder.CreateAt-barStartMS)/1000) / float64(odTFSecs)
			minRate = simMarketRate(&bar.Kline, trigPrice, odIsBuy, true, minRate)
			fillBarRate = minRate
			fillMS = bar.Time + int64(float64(odTFSecs)*minRate)*1000
			isStopEnter = true
		}
		if bar == nil {
			price = com.GetPriceSafeExp(od.Symbol, "", com.Day10MSecs)
			if price < 0 {
				// here return error would cause backtest interrupt
				return 0, nil
			}
		} else if strings.Contains(odType, "limit") && exOrder.Price > 0 {
			if odIsBuy {
				if price < bar.Low {
					continue
				} else if price > bar.Open {
					// 买价高于市价，以市价成交
					// If the purchase price is higher than the market price, the transaction will be completed at the market price.
					price = bar.Open
				}
			} else {
				if price > bar.High {
					continue
				} else if price < bar.Open {
					// If the selling price is lower than the market price, the transaction will be done at the market price.
					// 卖价低于市价，以市价成交
					price = bar.Open
				}
			}
			if minRate == 0 {
				minRate = float64((exOrder.CreateAt-barStartMS)/1000) / float64(odTFSecs)
			}
			fillBarRate = simMarketRate(&bar.Kline, exOrder.Price, odIsBuy, false, minRate)
			fillMS = bar.Time + int64(float64(odTFSecs)*fillBarRate)*1000
		} else if !isStopEnter {
			// 按网络延迟，模拟成交价格，和开盘价接近According to the network delay, the simulated transaction price is close to the opening price
			fillBarRate = float64((fillMS-barStartMS)/1000) / float64(odTFSecs)
			price = simMarketPrice(&bar.Kline, fillBarRate)
		}
		var err *errs.Error
		if exOrder.Enter {
			err = o.fillPendingEnter(od, price, fillMS)
			if err == nil && bar != nil {
				// 入场后可能立刻触发止损/止盈
				endBar := cutKlineFromRate(&bar.Kline, int64(odTFSecs*1000), fillBarRate)
				err = o.tryFillTriggers(od, endBar)
			}
		} else {
			err = o.fillPendingExit(od, price, fillMS)
		}
		if err != nil {
			return 0, err
		}
		affectNum += 1
	}
	// Forced liquidation of limit entry orders that have not been executed within a timeout period
	// 强制平仓超时未成交的限价入场单
	curMS := btime.TimeMS()
	for _, od := range orders {
		if od.Status > ormo.InOutStatusInit || od.Enter.Price == 0 ||
			!strings.Contains(od.Enter.OrderType, banexg.OdTypeLimit) {
			// Skip entered and non-limit orders
			// 跳过已入场的以及非限价单
			continue
		}
		stopAfter := od.GetInfoInt64(ormo.OdInfoStopAfter)
		if stopAfter > 0 && stopAfter <= curMS {
			err := od.LocalExit(stopAfter, core.ExitTagEntExp, od.InitPrice, "reach StopEnterBars", "")
			strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
			if err != nil {
				log.Error("local exit for StopEnterBars fail", zap.String("key", od.Key()), zap.Error(err))
			}
		}
	}
	return affectNum, nil
}

func (o *LocalOrderMgr) fillPendingEnter(od *ormo.InOutOrder, price float64, fillMS int64) *errs.Error {
	wallets := GetWallets(o.Account)
	_, err := wallets.EnterOd(od)
	if err != nil {
		if err.Code == core.ErrLowFunds {
			err = od.LocalExit(fillMS, core.ExitTagForceExit, od.InitPrice, err.Error(), "")
			strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
			o.onLowFunds()
			return err
		}
		return err
	}
	exchange := exg.Default
	market, err := exchange.GetMarket(od.Symbol)
	if err != nil {
		return err
	}
	entPrice, err := exchange.PrecPrice(market, price)
	if err != nil {
		return err
	}
	exOrder := od.Enter
	if exOrder.Amount == 0 {
		if od.Short && !core.IsContract {
			// Spot short order, quantity must be given
			// 现货空单，必须给定数量
			return errs.NewMsg(core.ErrInvalidCost, "EnterAmount is required")
		}
		entAmount := od.QuoteCost / entPrice
		exOrder.Amount, err = exchange.PrecAmount(market, entAmount)
		if err != nil || exOrder.Amount == 0 {
			if err != nil {
				if o.showLog {
					log.Warn("prec enter amount fail", zap.String("symbol", od.Symbol),
						zap.Float64("amt", entAmount), zap.Error(err))
				}
			} else {
				num, _ := o.zeroAmts[od.Symbol]
				o.zeroAmts[od.Symbol] = num + 1
			}
			err = od.LocalExit(fillMS, core.ExitTagFatalErr, od.InitPrice, err.Error(), "")
			_, quote, _, _ := core.SplitSymbol(od.Symbol)
			wallets.Cancel(od.Key(), quote, 0, true)
			strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
			return err
		}
	}
	if exOrder.Price == 0 {
		exOrder.Price = entPrice
	}
	updateTime := fillMS
	exOrder.UpdateAt = updateTime
	if exOrder.CreateAt == 0 {
		exOrder.CreateAt = updateTime
	}
	exOrder.Filled = exOrder.Amount
	exOrder.Average = entPrice
	exOrder.Status = ormo.OdStatusClosed
	err = od.UpdateFee(entPrice, true)
	if err != nil {
		return err
	}
	wallets.ConfirmOdEnter(od, entPrice)
	od.Status = ormo.InOutStatusFullEnter
	od.DirtyEnter = true
	od.DirtyMain = true
	if core.LiveMode {
		err = od.Save(nil)
		if err != nil {
			log.Error("save order fail", zap.String("acc", o.Account),
				zap.String("key", od.Key()), zap.Error(err))
		}
	}
	o.callBack(od, true)
	strat.FireOdChange(o.Account, od, strat.OdChgEnterFill)
	return nil
}

func (o *LocalOrderMgr) fillPendingExit(od *ormo.InOutOrder, price float64, fillMS int64) *errs.Error {
	wallets := GetWallets(o.Account)
	exOrder := od.Exit
	wallets.ExitOd(od, exOrder.Amount)
	if exOrder.Filled == 0 {
		od.ExitAt = fillMS
	}
	exOrder.UpdateAt = fillMS
	exOrder.CreateAt = fillMS
	exOrder.Status = ormo.OdStatusClosed
	exOrder.Price = price
	exOrder.Filled = exOrder.Amount
	exOrder.Average = price
	err := od.UpdateFee(price, false)
	if err != nil {
		return err
	}
	od.Status = ormo.InOutStatusFullExit
	od.DirtyMain = true
	od.DirtyExit = true
	_ = o.finishOrder(od, nil)
	wallets.ConfirmOdExit(od, price)
	if core.LiveMode {
		err = od.Save(nil)
		if err != nil {
			log.Error("save order fail", zap.String("acc", o.Account),
				zap.String("key", od.Key()), zap.Error(err))
		}
	}
	o.callBack(od, false)
	strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
	return nil
}

func (o *LocalOrderMgr) tryFillTriggers(od *ormo.InOutOrder, bar *banexg.Kline) *errs.Error {
	sl := od.GetStopLoss()
	tp := od.GetTakeProfit()
	if sl == nil && tp == nil {
		return nil
	}
	if sl != nil && !sl.Hit {
		// 空单止损，最高价超过止损价触发
		// Short order stop loss, triggered when the highest price exceeds the stop loss price
		// 多单止损，最低价跌破止损价触发
		// Stop loss for long orders, triggered when the lowest price falls below the stop loss price
		sl.Hit = od.Short && bar.High >= sl.Price || !od.Short && bar.Low <= sl.Price
	}
	if tp != nil && !tp.Hit {
		// 空单止盈，最低价跌破止盈价触发
		// Short order stop profit, the lowest price falls below the stop profit price to trigger
		// 多单止盈，最高价突破止盈价触发
		// Long order stop profit, the highest price breaks through the stop profit price to trigger
		tp.Hit = od.Short && bar.Low <= tp.Price || !od.Short && bar.High >= tp.Price
	}
	if (sl == nil || !sl.Hit) && (tp == nil || !tp.Hit) {
		// 止损和止盈都未触发
		return nil
	}
	od.DirtyInfo = true
	tfSecs := float64(utils.TFToSecs(od.Timeframe))
	var fillPrice, trigPrice, amtRate float64
	var exitTag string
	if sl != nil && sl.Hit {
		// Trigger stop loss and calculate execution price
		// 触发止损，计算执行价格
		trigPrice = sl.Price
		amtRate = sl.Rate
		fillPrice = getExcPrice(od, bar, sl.Price, sl.Limit, 0, tfSecs)
		if sl.Tag != "" {
			exitTag = sl.Tag
		} else {
			exitTag = core.ExitTagStopLoss
			od.UpdateProfits(fillPrice)
			if od.ProfitRate >= 0 {
				exitTag = core.ExitTagSLTake
			}
		}
	} else if tp != nil && tp.Hit {
		// Trigger take profit and calculate execution price
		// 触发止盈，计算执行价格
		trigPrice = tp.Price
		amtRate = tp.Rate
		fillPrice = getExcPrice(od, bar, tp.Price, tp.Limit, 0, tfSecs)
		if fillPrice == 0 && tp.Limit > 0 {
			// 设置了限价止盈，强制使用止盈价出场
			fillPrice = tp.Limit
		}
		if tp.Tag != "" {
			exitTag = tp.Tag
		} else {
			exitTag = core.ExitTagTakeProfit
		}
	} else {
		return nil
	}
	if fillPrice < 0 {
		return nil
	}
	curMS := btime.TimeMS()
	// The time when the simulation is triggered
	// 模拟触发时的时间
	var rate = float64(0) // 限价单触发不考虑网络延迟
	odType := banexg.OdTypeMarket
	if fillPrice > 0 {
		odType = banexg.OdTypeLimit
		rate += simMarketRate(bar, fillPrice, od.Short, true, 0)
	} else {
		// Stop time + network delay
		// 触发时间+网络延迟
		rate += simMarketRate(bar, trigPrice, od.Short, true, 0)
		// Stop loss at market price and sell immediately
		// 市价止损，立刻卖出
		fillPrice = simMarketPrice(bar, rate)
	}
	if amtRate > 0 && amtRate <= 0.99 {
		// Partial withdrawal
		// 部分退出
		part := o.CutOrder(od, amtRate, 0)
		if sl != nil && sl.Hit {
			_ = od.SetStopLoss(nil)
		} else {
			_ = od.SetTakeProfit(nil)
		}
		err := od.Save(nil)
		if err != nil {
			log.Error("save cutPart parent order fail", zap.String("key", od.Key()), zap.Error(err))
		}
		od = part
	}
	cutSecs := tfSecs * (1 - rate)
	exitAt := curMS - int64(cutSecs*1000)
	err := od.LocalExit(exitAt, exitTag, fillPrice, "", odType)
	wallets := GetWallets(o.Account)
	wallets.ExitOd(od, od.Exit.Amount)
	_ = o.finishOrder(od, nil)
	wallets.ConfirmOdExit(od, od.Exit.Price)
	o.callBack(od, false)
	strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
	return err
}

func (o *LocalOrderMgr) onLowFunds() {
	// If the balance is insufficient and there are no orders entered, the backtest will be terminated early.
	// 如果余额不足，且没有入场的订单，则提前终止回测
	openNum := ormo.OpenNum(o.Account, ormo.InOutStatusPartEnter)
	if openNum > 0 {
		return
	}
	wallets := GetWallets(o.Account)
	value := wallets.TotalLegal(nil, false)
	if value < core.MinStakeAmount {
		log.Warn("wallet low funds, no open orders, stop backTest..")
		core.StopAll()
		core.BotRunning = false
	}
}

func (o *LocalOrderMgr) OnEnvEnd(bar *banexg.PairTFKline, adj *orm.AdjInfo) *errs.Error {
	err := o.exitAndFill(nil, &strat.ExitReq{
		Tag:  core.ExitTagEnvEnd,
		Dirt: core.OdDirtBoth,
	}, &orm.InfoKline{PairTFKline: bar, Adj: adj}, true)
	return err
}

func (o *LocalOrderMgr) exitAndFill(sess *ormo.Queries, req *strat.ExitReq, bar *orm.InfoKline, noEnter bool) *errs.Error {
	pairs := ""
	if bar != nil {
		pairs = bar.Symbol
	}
	orders, err := o.ExitOpenOrders(sess, pairs, req)
	if err != nil {
		return err
	}
	if len(orders) > 0 {
		odMap := make(map[int64]bool)
		for _, od := range orders {
			odMap[od.ID] = true
			if od.Exit != nil {
				odMap[-od.ID] = true
			}
		}
		backUntil := int64(0)
		if noEnter {
			backUntil, _ = core.NoEnterUntil[o.Account]
			core.NoEnterUntil[o.Account] = btime.TimeMS() + 72*3600*1000
		}
		_, err = o.fillPendingOrdersAll(orders, odMap, bar)
		if noEnter {
			core.NoEnterUntil[o.Account] = backUntil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *LocalOrderMgr) ExitAndFill(sess *ormo.Queries, orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
	for _, od := range orders {
		_, err := o.exitOrder(sess, od, req)
		if err != nil {
			return err
		}
	}
	timeMS := btime.TimeMS()
	for _, od := range orders {
		price := com.GetPriceExp(od.Symbol, "", com.Day10MSecs)
		err := o.fillPendingExit(od, price, timeMS)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *LocalOrderMgr) CleanUp() *errs.Error {
	exitReq := &strat.ExitReq{
		Tag:   core.ExitTagBotStop,
		Dirt:  core.OdDirtBoth,
		Force: true,
	}
	openOds, lock := ormo.GetOpenODs(o.Account)
	lock.Lock()
	oldOpens := maps.Clone(openOds)
	lock.Unlock()
	err := o.exitAndFill(nil, exitReq, nil, false)
	if err != nil {
		return err
	}
	lock.Lock()
	// 检查已平仓订单，将平仓时间大于当前时间的，置为BotStop退出
	for oid := range openOds {
		delete(oldOpens, oid)
	}
	curMS := btime.UTCStamp()
	for _, od := range oldOpens {
		if od.ExitTag != "" && od.ExitAt > curMS && od.ExitTag != core.ExitTagBotStop {
			od.ExitTag = core.ExitTagBotStop
			// 回测无需持久化
		}
	}
	openOdList := utils.ValsOfMap(openOds)
	lock.Unlock()
	if len(openOdList) > 0 {
		exitOds := make([]*ormo.InOutOrder, 0, len(openOdList))
		odMap := make(map[int64]bool)
		var iod *ormo.InOutOrder
		for _, od := range openOdList {
			iod, err = o.exitOrder(nil, od, exitReq)
			if err != nil {
				break
			}
			exitOds = append(exitOds, iod)
			odMap[iod.ID] = true
			if iod.Exit != nil {
				odMap[-iod.ID] = true
			}
		}
		if err == nil {
			core.NoEnterUntil[o.Account] = btime.TimeMS() + 72*3600*1000
			_, err = o.fillPendingOrdersAll(exitOds, odMap, nil)
		}
	}
	if err != nil {
		return err
	}
	if len(o.zeroAmts) > 0 {
		log.Warn("prec amount to zero", zap.Any("times", o.zeroAmts))
	}
	// Reset Unrealized P&L
	// 重置未实现盈亏
	wallets := GetWallets(o.Account)
	for _, item := range wallets.Items {
		item.lock.Lock()
		item.UnrealizedPOL = 0
		item.UsedUPol = 0
		item.lock.Unlock()
	}
	// Filter unfilled orders
	// 过滤未入场订单
	var validOds = make([]*ormo.InOutOrder, 0, len(ormo.HistODs))
	for _, od := range ormo.HistODs {
		if od.Enter == nil || od.Enter.Filled == 0 {
			continue
		}
		validOds = append(validOds, od)
	}
	ormo.HistODs = validOds
	return nil
}

func simPriceByRate(bar *banexg.Kline, rate float64) (float64, float64, float64) {
	var (
		a, b, c, pa, totalLen float64
		aEndRate, bEndRate    float64
		start, end, posRate   float64
	)

	openP := bar.Open
	highP := bar.High
	lowP := bar.Low
	closeP := bar.Close

	if rate == 0 {
		return openP, highP, lowP
	}
	if rate >= 0.999 {
		return closeP, highP, lowP
	}
	newHigh, newLow := highP, lowP

	if openP <= closeP {
		// close > open, generally first moves down to the lower shadow line, then rises to the highest point, and finally retreats slightly to form the upper shadow line.
		// 阳线  一般是先下调走出下影线，然后上升到最高点，最后略微回撤，出现上影线
		pa = (openP - lowP) * 0.3 // a向下前的小幅向上回调，模拟震荡
		a = openP + pa - lowP
		b = highP - lowP
		c = (highP - closeP) * 1.3 // 多加些，模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return closeP, highP, lowP
		}
		paEndRate := pa / totalLen
		aEndRate = (pa + a) / totalLen
		bEndRate = (pa + a + b) / totalLen
		if rate <= paEndRate {
			start, end, posRate = openP, openP+pa, rate/paEndRate
		} else if rate <= aEndRate {
			start, end, posRate = openP+pa, lowP, (rate-paEndRate)/(aEndRate-paEndRate)
		} else if rate <= bEndRate {
			start, end, posRate = lowP, highP, (rate-aEndRate)/(bEndRate-aEndRate)
			newLow = closeP
		} else {
			start, end, posRate = highP, closeP, (rate-bEndRate)/(1-bEndRate)
			newHigh, newLow = closeP, closeP
		}
	} else {
		// close < open. generally rises first and goes out of the upper shadow line, then drops to the lowest point, and finally pulls back slightly to form a lower shadow line.
		// 阴线  一般是先上升走出上影线，然后下降到最低点，最后略微回调，出现下影线
		pa = (highP - openP) * 0.3 // a向上前的小幅向下回调，模拟震荡
		a = highP - (openP - pa)
		b = highP - lowP
		c = (closeP - lowP) * 1.3 // 模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return closeP, highP, lowP
		}
		paEndRate := pa / totalLen
		aEndRate = (pa + a) / totalLen
		bEndRate = (pa + a + b) / totalLen
		if rate <= paEndRate {
			start, end, posRate = openP, openP-pa, rate/paEndRate
		} else if rate <= aEndRate {
			start, end, posRate = openP-pa, highP, (rate-paEndRate)/(aEndRate-paEndRate)
		} else if rate <= bEndRate {
			start, end, posRate = highP, lowP, (rate-aEndRate)/(bEndRate-aEndRate)
			highP = closeP
		} else {
			start, end, posRate = lowP, closeP, (rate-bEndRate)/(1-bEndRate)
			newHigh, newLow = closeP, closeP
		}
	}

	newOpen := start*(1-posRate) + end*posRate
	newHigh = max(newOpen, newHigh)
	newLow = min(newOpen, newLow)
	return newOpen, newHigh, newLow
}

func simMarketPrice(bar *banexg.Kline, rate float64) float64 {
	start, _, _ := simPriceByRate(bar, rate)
	return start
}

func cutKlineFromRate(bar *banexg.Kline, tfMSecs int64, rate float64) *banexg.Kline {
	start, high, low := simPriceByRate(bar, rate)
	return &banexg.Kline{
		Time: bar.Time + int64(float64(tfMSecs)*rate),
		Open: start, High: high, Low: low, Close: bar.Close, Volume: bar.Volume * (1 - rate), Info: bar.Info,
	}
}

func simMarketRate(bar *banexg.Kline, price float64, isBuy, isTrigger bool, minRate float64) float64 {
	if isTrigger {
		// For the order that triggers the price, it is not a pending order. If it is judged that it is not within the bar range, it is considered to be completed immediately.
		// 对于触发价格的订单，不是挂单，判断如果未在bar范围内，则认为立刻成交
		if price < bar.Low || price > bar.High {
			return minRate
		}
	} else {
		// Non-trigger mode, directly compare with the opening price
		// 非触发模式，直接和开盘价对比
		if isBuy && price >= bar.Open || !isBuy && price <= bar.Open {
			// 开盘立刻成交。
			return minRate
		}
	}

	var (
		a, b, c, pa, totalLen float64
	)

	openP := bar.Open
	highP := bar.High
	lowP := bar.Low
	closeP := bar.Close

	if openP <= closeP {
		// close > open. generally first moves down to the lower shadow line, then rises to the highest point, and finally retreats slightly to form the upper shadow line.
		// 阳线  一般是先下调走出下影线，然后上升到最高点，最后略微回撤，出现上影线
		pa = (openP - lowP) * 0.3  // a向下前的小幅向上回调，模拟震荡
		a = openP + pa - lowP      // open~low. 开盘~最低
		b = highP - lowP           // low~high. 最低~最高
		c = (highP - closeP) * 1.3 // high~close. 最高~收盘，模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return 0.5
		}
		if isTrigger {
			// Trigger price, no need to consider buying and selling direction, direct comparison
			// 触发价格，无需考虑买卖方向，直接比较
			if price >= openP && price <= openP+pa {
				// a向下前小幅向上回调时触发
				rate := (price - openP) / totalLen
				if rate >= minRate {
					return rate
				}
			} else if price < openP {
				// The trigger bid price is lower than the opening price, and it is triggered when the opening price is the lowest
				// 触发买价低于开盘，在开盘~最低时触发
				rate := (pa + openP + pa - price) / totalLen
				if rate >= minRate {
					return rate
				}
			}
			// Otherwise, it will be triggered from the lowest to the highest
			// 否则在最低~最高中触发
			rate := (pa + a + price - lowP) / totalLen
			if rate >= minRate {
				return rate
			} else {
				// Triggered during the highest to closing time
				// 在最高~收盘中触发
				return (pa + a + b + highP - price) / totalLen
			}
		} else {
			if isBuy {
				// Buy order, triggered at opening ~ lowest price
				// 买单，在开盘~最低时触发
				rate := (pa + openP + pa - price) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Trigger at minimum to maximum
					// 在最低~最高时触发
					return (pa + a + price - lowP) / totalLen
				}
			} else {
				// Sell order, triggered between the lowest and highest levels
				// 卖单，在最低~最高中触发
				rate := (pa + a + price - lowP) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Triggered during the highest to closing time
					// 在最高~收盘中触发
					return (pa + a + b + highP - price) / totalLen
				}
			}
		}
	} else {
		// close < open. generally rises first and goes out of the upper shadow line, then drops to the lowest point, and finally pulls back slightly to form a lower shadow line.
		// 阴线  一般是先上升走出上影线，然后下降到最低点，最后略微回调，出现下影线
		pa = (highP - openP) * 0.3 // a向上前的小幅回调向下，模拟震荡
		a = highP - (openP - pa)   // 开盘~最高
		b = highP - lowP           // 最高~最低
		c = (closeP - lowP) * 1.3  // 最低~收盘，模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return 0.5
		}
		if isTrigger {
			// Trigger price, no need to consider buying and selling direction, direct comparison
			// 触发价格，无需考虑买卖方向，直接比较
			if price < openP {
				if price >= openP-pa {
					// pa: 先小幅下降回调
					rate := (openP - price) / totalLen
					if rate >= minRate {
						return rate
					}
				}
				// If the trigger price is lower than the opening price, it must be triggered between the highest and lowest prices.
				// 触发价低于开盘，必然在最高~最低中触发
				rate := (pa + a + highP - price) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Triggered at the lowest price ~ closing price
					// 在最低~收盘中触发
					return (pa + a + b + price - lowP) / totalLen
				}
			} else {
				// The trigger price is higher than the opening price, and is triggered between the opening price and the highest price.
				// 触发价高于开盘，在开盘~最高中触发
				rate := (pa + price - openP + pa) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Trigger between highest and lowest
					// 在最高~最低中触发
					return (pa + a + highP - price) / totalLen
				}
			}
		} else {
			if isBuy {
				if price >= openP-pa {
					// 在向上前的小幅回调中触发
					rate := (openP - price) / totalLen
					if rate >= minRate {
						return rate
					}
				}
				// Buy orders must be triggered between the highest and lowest prices.
				// 买单，必然在最高~最低中触发
				rate := (pa + a + highP - price) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Triggered at the lowest price ~ closing price
					// 在最低~收盘中触发
					return (pa + a + b + price - lowP) / totalLen
				}
			} else {
				// Sell order, triggered from the opening to the highest price
				// 卖单，在开盘~最高中触发
				rate := (pa + price - openP + pa) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Trigger between highest and lowest
					// 在最高~最低中触发
					return (pa + a + highP - price) / totalLen
				}
			}
		}
	}
}

/*
计算平仓成交价格，0市价，-1不平仓，>0指定价格
Calculate the transaction price for closing the position, 0 market price, -1 for not closing the position, >0 specified price
*/
func getExcPrice(od *ormo.InOutOrder, bar *banexg.Kline, trigPrice, limit, afterRate, tfSecs float64) float64 {
	if limit > 0 {
		if od.Short && limit < bar.Low || !od.Short && limit > bar.High {
			// 空单，平仓限价低于bar最低，不触发
			// 多单，平仓限价高于bar最高，不触发
			return -1
		}
		if od.Short && limit <= trigPrice || !od.Short && limit >= trigPrice {
			// 简单起见，指定了Limit限价出场，则默认限价单成交，不考虑时间
			return limit
			// 空单，平仓限价低于触发价，可能是限价单
			// 多单，平仓限价高于触发价，可能是限价单
			//trigRate := simMarketRate(bar, trigPrice, od.Short, true, afterRate)
			//rate := simMarketRate(bar, limit, od.Short, true, afterRate)
			//if (rate-trigRate)*tfSecs > 30 {
			//	// 触发后，限价单超过30s成交，认为限价单
			//	return limit
			//}
		}
	}
	return 0
}

func makeLocalAfterEnter(o *LocalOrderMgr) FuncHandleIOrder {
	return func(order *ormo.InOutOrder) *errs.Error {
		// 伪时间增加1，避免同时多个订单下单key相同导致钱包扣除错误
		btime.CurTimeMS += 1
		return nil
	}
}
