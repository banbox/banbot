package biz

import (
	"maps"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

type LocalOrderMgr struct {
	OrderMgr
	showLog      bool
	zeroAmts     map[string]int
	stopBacktest func()
	simMu        sync.Mutex
}

type FnOdCb = func(od *ormo.InOutOrder, isEnter bool)

func InitLocalOrderMgr(callBack FnOdCb, showLog bool, stops ...func()) {
	InitLocalOrderMgrWithPriceState(callBack, showLog, nil, nil, stops...)
}

// InitLocalOrderMgrWithPriceState binds order matching to a Runtime-owned
// price and clock state. A nil price state keeps the legacy facade behavior.
func InitLocalOrderMgrWithPriceState(callBack FnOdCb, showLog bool, prices *com.PriceState, clock *btime.ClockState, stops ...func()) {
	initLocalOrderMgr(nil, callBack, showLog, prices, clock, stops...)
}

// InitLocalOrderMgrWithRuntimeDeps binds order matching and wallet symbol
// accounting to one explicit Runtime. The legacy initializer remains the
// compatibility path for callers that still use package state.
func InitLocalOrderMgrWithRuntimeDeps(deps RuntimeDeps, callBack FnOdCb, showLog bool, stops ...func()) {
	initLocalOrderMgr(&deps, callBack, showLog, nil, nil, stops...)
}

func initLocalOrderMgr(deps *RuntimeDeps, callBack FnOdCb, showLog bool, prices *com.PriceState, clock *btime.ClockState, stops ...func()) {
	var stopBacktest func()
	if deps == nil {
		stopBacktest = core.StopAll
	}
	if deps != nil {
		// Complete the explicit dependency set before creating any manager. A
		// runtime manager must never fall back to the process-wide order state.
		if deps.Orders == nil {
			deps.Orders = ormo.NewOrderState()
		}
		if deps.Trading == nil {
			deps.Trading = NewTradingState()
		}
	}
	if deps != nil && deps.Core != nil {
		stopBacktest = deps.Core.StopAll
	}
	if len(stops) > 0 {
		stopBacktest = stops[0]
	}
	var managers map[string]IOrderMgr
	if deps != nil {
		managers = deps.Trading.OrderManagers
	} else {
		managers = accOdMgrs
	}
	accounts := executionAccountConfigs(deps)
	for account, cfg := range accounts {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		_, ok := managers[account]
		if !ok {
			odMgr := &LocalOrderMgr{
				OrderMgr: OrderMgr{
					callBack: callBack,
					prices:   prices,
					clock:    clock,
					Account:  account,
				},
				showLog:      showLog,
				zeroAmts:     make(map[string]int),
				stopBacktest: stopBacktest,
			}
			if deps != nil {
				odMgr.bindRuntimeDeps(*deps)
			}
			odMgr.afterEnter = makeLocalAfterEnter(odMgr)
			managers[account] = odMgr
		} else if odMgr, ok := managers[account].(*LocalOrderMgr); ok {
			if deps != nil {
				odMgr.bindRuntimeDeps(*deps)
			} else {
				odMgr.prices = prices
				odMgr.clock = clock
			}
			odMgr.callBack = callBack
			odMgr.stopBacktest = stopBacktest
		}
	}
}

func (o *LocalOrderMgr) ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	if o.isBacktest() {
		o.simMu.Lock()
		defer o.simMu.Unlock()
	}
	return o.OrderMgr.ProcessOrders(job)
}

func (o *LocalOrderMgr) UpdateByDataSeries(allOpens []*ormo.InOutOrder, evt *orm.DataSeries) *errs.Error {
	if len(allOpens) == 0 || o.isEnvReal() || o.isLive() {
		return nil
	}
	if evt == nil {
		return nil
	}
	symbol := evt.Symbol()
	// Simulate order entry and exit, which are usually executed at the beginning of the bar
	// 模拟订单入场出场，入场出场一般在bar开始时执行
	var curOrders []*ormo.InOutOrder
	var curMap = make(map[int64]bool)
	for _, od := range allOpens {
		if od.Symbol == symbol {
			curOrders = append(curOrders, od)
			curMap[od.ID] = true
			if od.Exit != nil {
				curMap[-od.ID] = true
			}
		}
	}
	if len(curOrders) == 0 && !o.checkWallets() {
		return nil
	}
	curOrders, err := o.fillPendingOrdersAll(curOrders, curMap, evt)
	if err != nil {
		return err
	}
	return o.updateProfitAndWallets(allOpens, curOrders, evt)
}

func (o *LocalOrderMgr) updateProfitAndWallets(allOpens, curOrders []*ormo.InOutOrder, evt *orm.DataSeries) *errs.Error {
	// Update all orders to profit at the end of the bar
	// 更新所有订单在bar结束时利润
	err := o.OrderMgr.UpdateByDataSeries(curOrders, evt)
	if err != nil {
		return err
	}
	if o.isContract() && o.checkWallets() {
		// Update all order margins and wallet status of this pricing currency for the contract
		// 为合约更新此定价币的所有订单保证金和钱包情况
		parts, parseErr := o.priceSymbolParts(evt.Symbol())
		if parseErr != nil {
			return parseErr
		}
		code := parts[2]
		var orders []*ormo.InOutOrder
		for _, od := range allOpens {
			odParts, parseErr := o.priceSymbolParts(od.Symbol)
			if parseErr != nil {
				return parseErr
			}
			odSettle := odParts[2]
			if odSettle == code && od.Status < ormo.InOutStatusFullExit {
				orders = append(orders, od)
			}
		}
		wallets := o.walletsForOrder()
		err = wallets.UpdateOds(orders, code)
	}
	return err
}

func (o *LocalOrderMgr) fillPendingOrdersAll(orders []*ormo.InOutOrder, curMap map[int64]bool, evt *orm.DataSeries) ([]*ormo.InOutOrder, *errs.Error) {
	_, err := o.fillPendingOrders(orders, evt)
	if err != nil {
		return orders, err
	}
	// 在订单事件回调中可能触发新订单入场
	checkCount := 0
	for o.newOrdersInSim() > 0 {
		openOds, lock := o.openOrders()
		var newOds []*ormo.InOutOrder
		lock.Lock()
		for _, od := range openOds {
			if !(evt == nil || od.Symbol == evt.Symbol()) {
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
			// openOds is a map, so its iteration order is not a historical
			// order that frozen replay can preserve.
			o.sortMapOrdersForBacktest(newOds)
			_, err = o.fillPendingOrders(newOds, evt)
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

func (o *LocalOrderMgr) newOrdersInSim() int {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			return o.runtimeCore.NewNumInSim
		}
		return 0
	}
	return core.NewNumInSim
}

func (o *LocalOrderMgr) setSimOrderMatch(enabled bool) {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			o.runtimeCore.SimOrderMatch = enabled
		}
		return
	}
	core.SimOrderMatch = enabled
}

func (o *LocalOrderMgr) resetSimOrderCount() {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			o.runtimeCore.NewNumInSim = 0
		}
		return
	}
	core.NewNumInSim = 0
}

func (o *LocalOrderMgr) sortMapOrdersForBacktest(orders []*ormo.InOutOrder) {
	if !o.isBacktest() || len(orders) < 2 {
		return
	}
	sortOrdersByID(orders)
}

// sortOrdersForBacktest enforces deterministic order iteration only in backtests.
func sortOrdersForBacktest(orders []*ormo.InOutOrder) {
	if !core.BackTestMode || preserveFrozenReplayExecutionOrder() || len(orders) < 2 {
		return
	}
	sortOrdersByID(orders)
}

func sortMapOrdersForBacktest(orders []*ormo.InOutOrder) {
	if !core.BackTestMode || len(orders) < 2 {
		return
	}
	sortOrdersByID(orders)
}

func sortOrdersByID(orders []*ormo.InOutOrder) {
	sort.Slice(orders, func(i, j int) bool {
		return orders[i].ID < orders[j].ID
	})
}

/*
fillPendingOrders
Fills orders waiting for exchange response. Cannot be used for real trading; can be used for backtesting, simulated real trading, etc.
填充等待交易所响应的订单。不可用于实盘；可用于回测、模拟实盘等。
*/
func (o *LocalOrderMgr) fillPendingOrders(orders []*ormo.InOutOrder, evt *orm.DataSeries) (int, *errs.Error) {
	orders = executionOrderView(orders, o.executionDeps())
	o.setSimOrderMatch(true)
	o.resetSimOrderCount()
	defer func() {
		o.setSimOrderMatch(false)
	}()
	affectNum := 0
	bar := seriesOHLCVCompat(evt)
	for _, od := range orders {
		matchTf := o.refineTimeFrame(od.Strategy, od.Timeframe)
		if evt != nil && evt.TimeFrame != matchTf {
			continue
		}
		exOrder := getPendingSub(od)
		if exOrder == nil {
			if od.ExitTag == "" && evt != nil {
				// 已入场完成，尚未出现出场信号，检查是否触发止损The entry has been completed, but the exit signal has not yet appeared. Check whether the stop loss is triggered.
				err := o.tryFillTriggers(od, bar, matchTf, 0)
				if err != nil {
					return 0, err
				}
			}
			continue
		}
		odType := o.orderType()
		if exOrder.OrderType != "" {
			odType = exOrder.OrderType
		}
		price := exOrder.Price
		odTFSecs := utils.TFToSecs(matchTf)
		fillMS := exOrder.CreateAt + int64(o.backtestNetCost()*1000)
		barStartMS := utils.AlignTfMSecs(fillMS, int64(odTFSecs*1000))
		odIsBuy := exOrder.Side == banexg.OdSideBuy
		var minRate float64
		var fillBarRate float64
		var isStopEnter bool
		if exOrder.Enter && od.Stop > 0 && evt != nil {
			// 使用触发价格，enterOrder中已判断有效性
			trigPrice := od.Stop
			lowVal, _ := evt.LowValue()
			highVal, _ := evt.HighValue()
			if !stopEntryTriggeredWith(odIsBuy, trigPrice, lowVal, highVal, o.legacyIntrabarEnabled()) {
				// The bar has not crossed the stop in the order direction.
				continue
			}
			price = trigPrice
			od.Stop = 0
			if strings.Contains(odType, "limit") && exOrder.Price > 0 && (exOrder.Price > trigPrice) == od.Short {
				// 触发价满足，有额外限价单
				price = exOrder.Price
			}
			minRate = float64((exOrder.CreateAt-barStartMS)/1000) / float64(odTFSecs)
			minRate = o.simMarketRate(bar, trigPrice, odIsBuy, true, minRate)
			fillBarRate = minRate
			fillMS = evt.TimeMS + int64(float64(odTFSecs)*minRate)*1000
			isStopEnter = true
		}
		if evt == nil {
			if o.isBacktest() {
				price = o.lastBarPrice(od.Symbol)
			} else {
				price = o.priceSafeExp(od.Symbol, "", com.Day10MSecs)
			}
			if price < 0 {
				continue
			}
		} else if strings.Contains(odType, "limit") && exOrder.Price > 0 {
			lowVal, _ := evt.LowValue()
			highVal, _ := evt.HighValue()
			openVal, _ := evt.OpenValue()
			if odIsBuy {
				if price < lowVal {
					continue
				} else if price > openVal {
					// 买价高于市价，以市价成交
					// If the purchase price is higher than the market price, the transaction will be completed at the market price.
					price = openVal
				}
			} else {
				if price > highVal {
					continue
				} else if price < openVal {
					// If the selling price is lower than the market price, the transaction will be done at the market price.
					// 卖价低于市价，以市价成交
					price = openVal
				}
			}
			if minRate == 0 {
				minRate = float64((exOrder.CreateAt-barStartMS)/1000) / float64(odTFSecs)
			}
			fillBarRate = o.simMarketRate(bar, exOrder.Price, odIsBuy, false, minRate)
			fillMS = evt.TimeMS + int64(float64(odTFSecs)*fillBarRate)*1000
		} else if !isStopEnter {
			// 按网络延迟，模拟成交价格，和开盘价接近According to the network delay, the simulated transaction price is close to the opening price
			fillBarRate = float64((fillMS-barStartMS)/1000) / float64(odTFSecs)
			price = o.simMarketPrice(bar, fillBarRate)
		}
		var err *errs.Error
		if exOrder.Enter {
			err = o.fillPendingEnter(od, price, fillMS)
			if err == nil && evt != nil {
				// 入场后可能立刻触发止损/止盈
				if o.legacyIntrabarEnabled() {
					err = o.tryFillTriggers(od, bar, matchTf, fillBarRate)
				} else {
					endBar := o.cutSeriesFromRate(bar, int64(odTFSecs*1000), fillBarRate)
					err = o.tryFillTriggers(od, endBar, matchTf, 0)
				}
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
	curMS := o.priceNow()
	for _, od := range orders {
		if od.Status > ormo.InOutStatusInit || od.Enter.Price == 0 ||
			!strings.Contains(od.Enter.OrderType, banexg.OdTypeLimit) {
			// Skip entered and non-limit orders
			// 跳过已入场的以及非限价单
			continue
		}
		stopAfter := od.GetInfoInt64(ormo.OdInfoStopAfter)
		if stopAfter > 0 && stopAfter <= curMS {
			err := o.localExit(od, stopAfter, core.ExitTagEntExp, od.InitPrice, "reach StopEnterBars", "")
			o.fireOdChange(od, strat.OdChgExitFill)
			if err != nil {
				log.Error("local exit for StopEnterBars fail", zap.String("key", od.Key()), zap.Error(err))
			}
		}
	}
	return affectNum, nil
}

func stopEntryTriggered(isBuy bool, trigger, low, high float64) bool {
	return stopEntryTriggeredWith(isBuy, trigger, low, high, legacyIntrabarEnabled())
}

func stopEntryTriggeredWith(isBuy bool, trigger, low, high float64, legacyIntrabar bool) bool {
	if legacyIntrabar {
		if isBuy {
			return trigger <= high
		}
		return trigger >= low
	}
	return trigger >= low && trigger <= high
}

func (o *LocalOrderMgr) simMarketPrice(bar *orm.SeriesOHLCV, rate float64) float64 {
	return simMarketPriceWithLegacy(bar, rate, o.legacyIntrabarEnabled())
}

func (o *LocalOrderMgr) simMarketRate(bar *orm.SeriesOHLCV, price float64, isBuy, isTrigger bool, minRate float64) float64 {
	return simMarketRateWithLegacy(bar, price, isBuy, isTrigger, minRate, o.legacyIntrabarEnabled())
}

func (o *LocalOrderMgr) cutSeriesFromRate(bar *orm.SeriesOHLCV, tfMSecs int64, rate float64) *orm.SeriesOHLCV {
	return cutSeriesFromRateWithLegacy(bar, tfMSecs, rate, o.legacyIntrabarEnabled())
}

func (o *LocalOrderMgr) fillPendingEnter(od *ormo.InOutOrder, price float64, fillMS int64) *errs.Error {
	wallets := o.walletsForOrder()
	_, err := wallets.EnterOd(od)
	if err != nil {
		if err.Code == core.ErrLowFunds {
			err = o.localExit(od, fillMS, core.ExitTagForceExit, od.InitPrice, err.Error(), "")
			o.fireOdChange(od, strat.OdChgExitFill)
			o.onLowFunds()
			return err
		}
		return err
	}
	exchange := o.exchangeClient()
	if exchange == nil {
		return errs.NewMsg(core.ErrExgNotInit, "exchange is required to fill %s", od.Symbol)
	}
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
		if od.Short && !o.isContract() {
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
			err = o.localExit(od, fillMS, core.ExitTagFatalErr, od.InitPrice, err.Error(), "")
			_, quote, _, _ := core.SplitSymbol(od.Symbol)
			wallets.Cancel(od.Key(), quote, 0, true)
			o.fireOdChange(od, strat.OdChgExitFill)
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
	err = o.updateOrderFee(od, entPrice, true)
	if err != nil {
		return err
	}
	wallets.ConfirmOdEnter(od, entPrice)
	od.Status = ormo.InOutStatusFullEnter
	od.DirtyEnter = true
	od.DirtyMain = true
	err = od.UpdateTrailing(entPrice)
	if err != nil {
		return err
	}
	if o.isLive() {
		err = od.Save()
		if err != nil {
			log.Error("save order fail", zap.String("acc", o.Account),
				zap.String("key", od.Key()), zap.Error(err))
		}
	}
	o.callBack(od, true)
	o.fireOdChange(od, strat.OdChgEnterFill)
	return nil
}

func (o *LocalOrderMgr) fillPendingExit(od *ormo.InOutOrder, price float64, fillMS int64) *errs.Error {
	wallets := o.walletsForOrder()
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
	err := o.updateOrderFee(od, price, false)
	if err != nil {
		return err
	}
	od.Status = ormo.InOutStatusFullExit
	od.DirtyMain = true
	od.DirtyExit = true
	_ = o.finishOrder(od)
	wallets.ConfirmOdExit(od, price)
	if o.isLive() {
		err = od.Save()
		if err != nil {
			log.Error("save order fail", zap.String("acc", o.Account),
				zap.String("key", od.Key()), zap.Error(err))
		}
	}
	o.callBack(od, false)
	o.fireOdChange(od, strat.OdChgExitFill)
	return nil
}

func (o *LocalOrderMgr) tryFillTriggers(od *ormo.InOutOrder, bar *orm.SeriesOHLCV, tf string,
	afterRate float64,
) *errs.Error {
	if bar == nil {
		return nil
	}
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
	hitSL := sl != nil && sl.Hit
	hitTP := tp != nil && tp.Hit
	if !hitSL && !hitTP {
		// 止损和止盈都未触发
		return nil
	}
	od.DirtyInfo = true
	tfSecs := float64(utils.TFToSecs(tf))
	var fillPrice, trigPrice, amtRate float64
	var exitTag string
	if hitSL {
		// Trigger stop loss and calculate execution price
		// 触发止损，计算执行价格
		trigPrice = sl.Price
		amtRate = sl.Rate
		fillPrice = getExcPrice(od, bar, sl.Price, sl.Limit, afterRate, tfSecs)
		if sl.Tag != "" {
			exitTag = sl.Tag
		} else {
			exitTag = core.ExitTagStopLoss
			od.UpdateProfits(fillPrice)
			if od.ProfitRate >= 0 {
				exitTag = core.ExitTagSLTake
			}
		}
	} else if hitTP {
		// Trigger take profit and calculate execution price
		// 触发止盈，计算执行价格
		trigPrice = tp.Price
		amtRate = tp.Rate
		fillPrice = getExcPrice(od, bar, tp.Price, tp.Limit, afterRate, tfSecs)
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
	curMS := o.priceNow()
	// The time when the simulation is triggered
	// 模拟触发时的时间
	var rate = float64(0) // 限价单触发不考虑网络延迟
	odType := banexg.OdTypeMarket
	if fillPrice > 0 {
		odType = banexg.OdTypeLimit
		rate += o.simMarketRate(bar, fillPrice, od.Short, true, afterRate)
	} else {
		// Stop time + network delay
		// 触发时间+网络延迟
		rate += o.simMarketRate(bar, trigPrice, od.Short, true, afterRate)
		// Stop loss at market price and sell immediately
		// 市价止损，立刻卖出
		fillPrice = o.simMarketPrice(bar, rate)
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
		err := od.Save()
		if err != nil {
			log.Error("save cutPart parent order fail", zap.String("key", od.Key()), zap.Error(err))
		}
		od = part
	}
	if hitSL && hitTP {
		od.SetInfo(ormo.OdInfoSLTP, "yes")
	}
	cutSecs := tfSecs * (1 - rate)
	exitAt := curMS - int64(cutSecs*1000)
	err := o.localExit(od, exitAt, exitTag, fillPrice, "", odType)
	wallets := o.walletsForOrder()
	wallets.ExitOd(od, od.Exit.Amount)
	_ = o.finishOrder(od)
	wallets.ConfirmOdExit(od, od.Exit.Price)
	o.callBack(od, false)
	o.fireOdChange(od, strat.OdChgExitFill)
	return err
}

func (o *LocalOrderMgr) onLowFunds() {
	// If the balance is insufficient and there are no orders entered, the backtest will be terminated early.
	// 如果余额不足，且没有入场的订单，则提前终止回测
	var openNum int
	if o.runtimeDeps {
		openNum = o.orderState().OpenNum(o.Account, ormo.InOutStatusPartEnter)
	} else {
		openNum = ormo.OpenNum(o.Account, ormo.InOutStatusPartEnter)
	}
	if openNum > 0 {
		return
	}
	wallets := o.walletsForOrder()
	value := wallets.TotalLegal(nil, false)
	if value < core.MinStakeAmount {
		log.Warn("wallet low funds, no open orders, stop backTest..")
		if o.stopBacktest != nil {
			o.stopBacktest()
		} else if stopAll := o.stopAll(); stopAll != nil {
			stopAll()
		}
		o.setBotRunning(false)
	}
}

func (o *LocalOrderMgr) OnEnvEnd(evt *orm.DataSeries) *errs.Error {
	err := o.exitAndFill(&strat.ExitReq{
		Tag:  core.ExitTagEnvEnd,
		Dirt: core.OdDirtBoth,
	}, evt, true)
	return err
}

// cleanUpAt applies the same complete cleanup path used at the end of a
// backtest, but at an explicit historical cutoff.
func (o *LocalOrderMgr) cleanUpAt(atMS int64) *errs.Error {
	if atMS <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "historical cleanup cutoff is invalid: %d", atMS)
	}
	oldMS := o.priceNow()
	noEnterUntil := o.noEnterUntil()
	oldNoEnter, hadNoEnter := noEnterUntil[o.Account]
	if o.clock != nil {
		o.clock.SetTimeMS(atMS)
	} else {
		btime.SetTimeMS(atMS)
	}
	defer func() {
		if o.clock != nil {
			o.clock.SetTimeMS(oldMS)
		} else {
			btime.SetTimeMS(oldMS)
		}
		if hadNoEnter {
			noEnterUntil[o.Account] = oldNoEnter
		} else {
			delete(noEnterUntil, o.Account)
		}
	}()
	return o.CleanUp()
}

// CloseBacktestOrdersAt closes positions that are still open at a historical
// baseline before an extended backtest continues into its new tail.
func CloseBacktestOrdersAt(account string, atMS int64) *errs.Error {
	return closeBacktestOrdersAt(nil, account, atMS)
}

// CloseBacktestOrdersAtWithState closes typed-runtime positions at a
// historical boundary without consulting the legacy order-manager registry.
func CloseBacktestOrdersAtWithState(state *TradingState, account string, atMS int64) *errs.Error {
	return closeBacktestOrdersAt(state, account, atMS)
}

func closeBacktestOrdersAt(state *TradingState, account string, atMS int64) *errs.Error {
	mgr, ok := getBatchOrderManager(state, account).(*LocalOrderMgr)
	if !ok || mgr == nil {
		return errs.NewMsg(core.ErrRunTime, "backtest order manager is not local")
	}
	return mgr.cleanUpAt(atMS)
}

func (o *LocalOrderMgr) exitAndFill(req *strat.ExitReq, evt *orm.DataSeries, noEnter bool) *errs.Error {
	pairs := ""
	if evt != nil {
		pairs = evt.Symbol()
	}
	orders, err := o.ExitOpenOrders(pairs, req)
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
			noEnterUntil := o.noEnterUntil()
			backUntil, _ = noEnterUntil[o.Account]
			noEnterUntil[o.Account] = o.priceNow() + 72*3600*1000
		}
		_, err = o.fillPendingOrdersAll(orders, odMap, evt)
		if noEnter {
			o.noEnterUntil()[o.Account] = backUntil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *LocalOrderMgr) ExitAndFill(orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
	orders = executionOrderView(orders, o.executionDeps())
	for _, od := range orders {
		_, err := o.exitOrder(od, req)
		if err != nil {
			return err
		}
	}
	timeMS := o.priceNow()
	for _, od := range orders {
		var price float64
		if o.isBacktest() {
			price = o.lastBarPrice(od.Symbol)
		} else {
			price = o.priceExp(od.Symbol, "", com.Day10MSecs)
		}
		if price < 0 {
			return errs.NewMsg(core.ErrRunTime, "no historical price for %s", od.Symbol)
		}
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
	openOds, lock := o.openOrders()
	lock.Lock()
	oldOpens := maps.Clone(openOds)
	lock.Unlock()
	err := o.exitAndFill(exitReq, nil, false)
	if err != nil {
		return err
	}
	lock.Lock()
	// 检查已平仓订单，将平仓时间大于当前时间的，置为BotStop退出
	for oid := range openOds {
		delete(oldOpens, oid)
	}
	curMS := o.priceNow()
	for _, od := range oldOpens {
		if od.ExitTag != "" && od.ExitAt > curMS && od.ExitTag != core.ExitTagBotStop {
			od.ExitTag = core.ExitTagBotStop
			// 回测无需持久化
		}
	}
	openOdList := executionOpenOrders(openOds, o.executionDeps())
	lock.Unlock()
	if len(openOdList) > 0 {
		exitOds := make([]*ormo.InOutOrder, 0, len(openOdList))
		odMap := make(map[int64]bool)
		var iod *ormo.InOutOrder
		for _, od := range openOdList {
			iod, err = o.exitOrder(od, exitReq)
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
			o.noEnterUntil()[o.Account] = o.priceNow() + 72*3600*1000
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
	wallets := o.walletsForOrder()
	for _, item := range wallets.Items {
		item.lock.Lock()
		item.UnrealizedPOL = 0
		item.UsedUPol = 0
		item.lock.Unlock()
	}
	lock.Lock()
	openNum := len(openOds)
	lock.Unlock()
	frozenNum := 0
	frozenTotal := float64(0)
	for _, item := range wallets.Items {
		item.lock.Lock()
		for _, amount := range item.Frozens {
			if math.Abs(amount) > core.AmtDust {
				frozenNum++
				frozenTotal += math.Abs(amount)
			}
		}
		item.lock.Unlock()
	}
	if openNum > 0 || frozenNum > 0 {
		return errs.NewMsg(core.ErrRunTime,
			"cleanup incomplete: %d open orders, %d frozen wallet entries (%.8f total)",
			openNum, frozenNum, frozenTotal)
	}
	// Filter unfilled orders
	// 过滤未入场订单
	state := o.orderState()
	if o.runtimeDeps {
		if state == nil {
			return errs.NewMsg(core.ErrRunTime, "runtime order state is required for cleanup")
		}
	} else {
		state = ormo.LegacyState()
	}
	state.FilterUnfilledHistoricalOrders()
	return nil
}

func getPendingSub(od *ormo.InOutOrder) *ormo.ExOrder {
	if od.ExitTag != "" && od.Exit != nil && od.Exit.Status < ormo.OdStatusClosed {
		return od.Exit
	} else if od.Enter != nil && od.Enter.Status < ormo.OdStatusClosed {
		return od.Enter
	}
	return nil
}

func simPriceByRate(bar *orm.SeriesOHLCV, rate float64) (float64, float64, float64) {
	return simPriceByRateWithLegacy(bar, rate, legacyIntrabarEnabled())
}

func simPriceByRateWithLegacy(bar *orm.SeriesOHLCV, rate float64, legacyIntrabar bool) (float64, float64, float64) {
	var (
		a, b, c, pa, totalLen float64
		aEndRate, bEndRate    float64
		start, end, posRate   float64
	)

	openP := bar.Open
	highP := bar.High
	lowP := bar.Low
	closeP := bar.Close
	preMoveFactor, closeLegFactor := 0.3, 1.3
	if legacyIntrabar {
		preMoveFactor, closeLegFactor = 0, 1
	}

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
		pa = (openP - lowP) * preMoveFactor // a向下前的小幅向上回调，模拟震荡
		a = openP + pa - lowP
		b = highP - lowP
		c = (highP - closeP) * closeLegFactor // 多加些，模拟震荡
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
		pa = (highP - openP) * preMoveFactor // a向上前的小幅向下回调，模拟震荡
		a = highP - (openP - pa)
		b = highP - lowP
		c = (closeP - lowP) * closeLegFactor // 模拟震荡
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
			newHigh = closeP
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

func simMarketPrice(bar *orm.SeriesOHLCV, rate float64) float64 {
	return simMarketPriceWithLegacy(bar, rate, legacyIntrabarEnabled())
}

func simMarketPriceWithLegacy(bar *orm.SeriesOHLCV, rate float64, legacyIntrabar bool) float64 {
	start, _, _ := simPriceByRateWithLegacy(bar, rate, legacyIntrabar)
	return start
}

func cutSeriesFromRate(bar *orm.SeriesOHLCV, tfMSecs int64, rate float64) *orm.SeriesOHLCV {
	return cutSeriesFromRateWithLegacy(bar, tfMSecs, rate, legacyIntrabarEnabled())
}

func cutSeriesFromRateWithLegacy(bar *orm.SeriesOHLCV, tfMSecs int64, rate float64, legacyIntrabar bool) *orm.SeriesOHLCV {
	start, high, low := simPriceByRateWithLegacy(bar, rate, legacyIntrabar)
	return &orm.SeriesOHLCV{
		Sid:       bar.Sid,
		ExSymbol:  bar.ExSymbol,
		Source:    bar.Source,
		Time:      bar.Time + int64(float64(tfMSecs)*rate),
		EndMS:     bar.EndMS,
		TimeFrame: bar.TimeFrame,
		Open:      start,
		High:      high,
		Low:       low,
		Close:     bar.Close,
		Volume:    bar.Volume * (1 - rate),
		Quote:     bar.Quote,
		BuyVolume: bar.BuyVolume,
		TradeNum:  bar.TradeNum,
		Adj:       bar.Adj,
		IsWarmUp:  bar.IsWarmUp,
		Closed:    bar.Closed,
	}
}

func simMarketRate(bar *orm.SeriesOHLCV, price float64, isBuy, isTrigger bool, minRate float64) float64 {
	return simMarketRateWithLegacy(bar, price, isBuy, isTrigger, minRate, legacyIntrabarEnabled())
}

func simMarketRateWithLegacy(bar *orm.SeriesOHLCV, price float64, isBuy, isTrigger bool, minRate float64, legacyIntrabar bool) float64 {
	if bar == nil {
		return minRate
	}
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
	preMoveFactor, closeLegFactor := 0.3, 1.3
	if legacyIntrabar {
		preMoveFactor, closeLegFactor = 0, 1
	}

	if openP <= closeP {
		// close > open. generally first moves down to the lower shadow line, then rises to the highest point, and finally retreats slightly to form the upper shadow line.
		// 阳线  一般是先下调走出下影线，然后上升到最高点，最后略微回撤，出现上影线
		pa = (openP - lowP) * preMoveFactor   // a向下前的小幅向上回调，模拟震荡
		a = openP + pa - lowP                 // open~low. 开盘~最低
		b = highP - lowP                      // low~high. 最低~最高
		c = (highP - closeP) * closeLegFactor // high~close. 最高~收盘，模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return 0.5
		}
		if isTrigger {
			// Trigger price, no need to consider buying and selling direction, direct comparison
			// 触发价格，无需考虑买卖方向，直接比较
			if !legacyIntrabar && price >= openP && price <= openP+pa {
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
		pa = (highP - openP) * preMoveFactor // a向上前的小幅回调向下，模拟震荡
		a = highP - (openP - pa)             // 开盘~最高
		b = highP - lowP                     // 最高~最低
		c = (closeP - lowP) * closeLegFactor // 最低~收盘，模拟震荡
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

func legacyIntrabarEnabled() bool {
	return core.BackTestMode && config.Data.BTLegacyIntrabar
}

/*
计算平仓成交价格，0市价，-1不平仓，>0指定价格
Calculate the transaction price for closing the position, 0 market price, -1 for not closing the position, >0 specified price
*/
func getExcPrice(od *ormo.InOutOrder, bar *orm.SeriesOHLCV, trigPrice, limit, afterRate, tfSecs float64) float64 {
	if bar == nil {
		return -1
	}
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
		if !o.isBacktest() {
			return nil
		}
		if o.clock != nil {
			o.clock.AdvanceMS(1)
		} else {
			btime.AdvanceTimeMS(1)
		}
		return nil
	}
}
