package biz

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

type FuncApplyMyTrade = func(od *ormo.InOutOrder, subOd *ormo.ExOrder, trade *banexg.MyTrade) *errs.Error
type FuncHandleMyOrder = func(trade *banexg.Order) bool

type LiveOrderMgr struct {
	OrderMgr
	queue            chan *OdQItem
	doneKeys         map[string]int64            // Completed Orders 已完成的订单：symbol+orderId
	exgIdMap         map[string]*ormo.InOutOrder // symbol+orderId: InOutOrder
	doneTrades       map[string]int64            // Processed trades 已处理的交易：symbol+tradeId
	lockDoneKeys     deadlock.Mutex
	lockExgIdMap     deadlock.Mutex
	lockDoneTrades   deadlock.Mutex
	isWatchMyTrade   bool                       // Is the account transaction flow being monitored? 是否正在监听账户交易流
	isTrialUnMatches bool                       // Is monitoring unmatched transactions? 是否正在监听未匹配交易
	isConsumeOrderQ  bool                       // Is it consuming from the order queue? 是否正在从订单队列消费
	isWatchAccConfig bool                       // Is the leverage ratio being monitored? 是否正在监听杠杆倍数变化
	unMatchTrades    map[string]*banexg.MyTrade // Transactions received from ws that have no matching orders 从ws收到的暂无匹配的订单的交易
	lockUnMatches    deadlock.Mutex             // Prevent concurrent reading and writing of unMatchTrades 防止并发读写unMatchTrades
	exitByMyOrder    FuncHandleMyOrder          // Try to use the transaction results of other end operations to update the current order status 尝试使用其他端操作的交易结果，更新当前订单状态
	traceExgOrder    FuncHandleMyOrder
}

type OdQItem struct {
	Order  *ormo.InOutOrder
	Action string
}

const (
	AmtDust = 1e-8
)

var (
	pairVolMap     = map[string]*PairValItem{}
	volPrices      = map[string]*VolPrice{}
	lockPairVolMap deadlock.Mutex
	lockVolPrices  deadlock.Mutex
)

type PairValItem struct {
	AvgVol   float64
	LastVol  float64
	ExpireMS int64
}

func InitLiveOrderMgr(callBack func(od *ormo.InOutOrder, isEnter bool)) {
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		mgr, ok := accLiveOdMgrs[account]
		if !ok {
			odMgr := newLiveOrderMgr(account, callBack)
			accLiveOdMgrs[account] = odMgr
			accOdMgrs[account] = odMgr
		} else {
			mgr.callBack = callBack
		}
	}
	if ormo.OdEditListener == nil {
		ormo.OdEditListener = func(od *ormo.InOutOrder, action string) {
			odMgr := GetOdMgr(ormo.GetTaskAcc(od.TaskID))
			if odMgr != nil {
				odMgr.EditOrder(od, action)
			}
		}
	}
}

func newLiveOrderMgr(account string, callBack func(od *ormo.InOutOrder, isEnter bool)) *LiveOrderMgr {
	res := &LiveOrderMgr{
		OrderMgr: OrderMgr{
			callBack: callBack,
			Account:  account,
		},
		queue:         make(chan *OdQItem, 1000),
		doneKeys:      map[string]int64{},
		exgIdMap:      map[string]*ormo.InOutOrder{},
		doneTrades:    map[string]int64{},
		unMatchTrades: map[string]*banexg.MyTrade{},
	}
	res.afterEnter = makeAfterEnter(res)
	res.afterExit = makeAfterExit(res)
	res.exitByMyOrder = exitByMyOrder(res)
	res.traceExgOrder = traceExgOrder(res)
	if exg.AfterCreateOrder == nil {
		exg.AfterCreateOrder = logPutOrder
	}
	return res
}

/*
SyncLocalOrders 将交易所仓位和本地仓位对比，关闭本地多余仓位对应订单

定期执行，用于解决币安偶发止损成交但状态为expired导致本地订单未更新问题。
*/
func (o *LiveOrderMgr) SyncLocalOrders() ([]*ormo.InOutOrder, *errs.Error) {
	// 获取交易所所有持仓
	posList, err := exg.Default.FetchAccountPositions(nil, map[string]interface{}{
		banexg.ParamAccount:     o.Account,
		banexg.ParamSettleCoins: config.StakeCurrency,
	})
	if err != nil {
		return nil, err
	}

	// 将持仓按symbol分组,并区分多空方向
	posMap := make(map[string]map[bool]*banexg.Position)
	for _, pos := range posList {
		if pos.Contracts <= AmtDust {
			continue
		}
		if _, ok := posMap[pos.Symbol]; !ok {
			posMap[pos.Symbol] = make(map[bool]*banexg.Position)
		}
		isShort := pos.Side == banexg.PosSideShort
		posMap[pos.Symbol][isShort] = pos
	}

	// 按symbol分组本地订单
	openOds, lock := ormo.GetOpenODs(o.Account)
	lock.Lock()
	// 过滤重复订单
	var duplicateOdNum = 0
	var checkKeys = make(map[string]bool)
	odMap := make(map[string]map[bool][]*ormo.InOutOrder)
	for _, od := range openOds {
		key := od.Key()
		if _, ok := checkKeys[key]; !ok {
			checkKeys[key] = true
			dirtMap, ok := odMap[od.Symbol]
			if !ok {
				dirtMap = make(map[bool][]*ormo.InOutOrder)
				odMap[od.Symbol] = dirtMap
			}
			dirtMap[od.Short] = append(dirtMap[od.Short], od)
		} else {
			duplicateOdNum += 1
		}
	}
	lock.Unlock()
	if duplicateOdNum > 0 {
		log.Info("found duplicate orders in global Open orders",
			zap.String("acc", o.Account), zap.Int("num", duplicateOdNum))
	}

	// 对每个symbol的多空方向进行检查
	var curMS = btime.UTCStamp()
	var closedList []*ormo.InOutOrder
	for symbol, sideOds := range odMap {
		pos, hasPair := posMap[symbol]
		for isShort, curOds := range sideOds {
			var posAmt float64
			if hasPair {
				if p, ok := pos[isShort]; ok {
					posAmt = p.Contracts
				}
			}

			// 计算本地订单总量
			var localAmt float64
			var maxEntMS int64
			for _, od := range curOds {
				localAmt += od.HoldAmount()
				maxEntMS = max(maxEntMS, od.RealEnterMS())
			}
			if curMS-maxEntMS < 20000 {
				// 最新订单不足20秒，可能状态未更新完成，跳过
				continue
			}

			// 如果本地订单量大于实际持仓量,需要关闭部分订单
			if localAmt > posAmt*1.02 {
				// 按数量降序排序
				sort.Slice(curOds, func(i, j int) bool {
					amtI := curOds[i].HoldAmount()
					amtJ := curOds[j].HoldAmount()
					return amtI > amtJ
				})
				overAmt := localAmt - posAmt
				dustAmt := overAmt * 0.001

				// 逐个关闭订单直到数量匹配
				var closeOds []string
				for _, od := range curOds {
					if overAmt < dustAmt {
						break
					}
					odAmt := od.HoldAmount()
					if odAmt < AmtDust {
						continue
					}

					part := od
					if odAmt > overAmt*1.01 {
						part = od.CutPart(overAmt, 0)
					}
					closeAmt := part.HoldAmount()
					err = part.LocalExit(0, core.ExitTagNoMatch, 0, "SyncLocalOrders", "")
					if err != nil {
						log.Error("force exit order fail", zap.String("acc", o.Account),
							zap.String("key", part.Key()), zap.Error(err))
						continue
					}

					overAmt -= closeAmt
					closeOds = append(closeOds, part.Key())
					closedList = append(closedList, part)
					strat.FireOdChange(o.Account, part, strat.OdChgExitFill)
				}
				log.Warn("close extra local open orders", zap.String("acc", o.Account), zap.String("pair", symbol),
					zap.Float64("ExtraTotal", localAmt-posAmt), zap.Float64("ExtraLeft", overAmt),
					zap.Float64("localAmt", localAmt), zap.Float64("exgAmt", posAmt),
					zap.Int("odNum", len(closeOds)), zap.Strings("orders", closeOds))
			}
		}
	}
	return closedList, nil
}

/*
SyncExgOrders
Synchronize the latest local orders of the exchange

First, use fetch_account_positions to fetch the positions of all coins in the exchange.
If there are no open orders locally:
If the exchange has no positions: Ignore
If the exchange has all positions: Treat it as a new order opened by the user and create a new order tracking
If there are open orders locally:
Get the last time of the local order as the start time, and query all subsequent orders through the fetch_orders interface.
Determine the current status of open orders from the exchange order records: closed, partially closed, unclosed
For redundant positions, treat them as new orders opened by the user and create new order tracking.
将交易所最新状态本地订单进行同步，机器人启动时初始化

	先通过fetch_account_positions抓取交易所所有币的仓位情况。
	如果本地没有未平仓订单：
	    如果交易所没有持仓：忽略
	    如果交易所有持仓：视为用户开的新订单，创建新订单跟踪
	如果本地有未平仓订单：
	     获取本地订单的最后时间作为起始时间，通过fetch_orders接口查询此后所有订单。
	     从交易所订单记录来确定未平仓订单的当前状态：已平仓、部分平仓、未平仓
	     对于冗余的仓位，视为用户开的新订单，创建新订单跟踪。
*/
func (o *LiveOrderMgr) SyncExgOrders() ([]*ormo.InOutOrder, []*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	EnsurePricesLoaded()
	exchange := exg.Default
	task := ormo.GetTask(o.Account)
	// Get the exchange order
	// 获取交易所挂单
	exOdList, err := exchange.FetchOpenOrders("", task.CreateAt, 1000, map[string]interface{}{
		banexg.ParamAccount:     o.Account,
		banexg.ParamSettleCoins: config.StakeCurrency,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	exgOdMap := make(map[string]*banexg.Order)
	for _, od := range exOdList {
		exgOdMap[od.ID] = od
	}
	orders, pairLastTfs, err := loadOpenOrders(task.ID, o.Account)
	if err != nil {
		return nil, nil, nil, err
	}
	openOds, lock := ormo.GetOpenODs(o.Account)
	var lastOrderMS int64
	var openPairs = map[string]struct{}{}
	var delOds, saveOds []*ormo.InOutOrder
	for _, od := range orders {
		if od.Status >= ormo.InOutStatusFullExit {
			continue
		}
		if od.Enter == nil {
			delOds = append(delOds, od)
			continue
		}
		if od.Enter.OrderID != "" {
			lastOrderMS = max(lastOrderMS, od.RealEnterMS(), od.RealExitMS())
		}
		err = o.restoreInOutOrder(od, exgOdMap)
		if err != nil {
			log.Error("restoreInOutOrder fail", zap.String("acc", o.Account), zap.String("key", od.Key()), zap.Error(err))
		}
		if od.Status < ormo.InOutStatusFullExit {
			lock.Lock()
			openOds[od.ID] = od
			lock.Unlock()
			openPairs[od.Symbol] = struct{}{}
		}
		saveOds = append(saveOds, od)
	}
	if len(delOds) > 0 || len(saveOds) > 0 {
		delSaveOrders(o.Account, delOds, saveOds)
	}
	if !banexg.IsContract(core.Market) {
		// 非合约市场，无法获取仓位，直接返回
		lock.Lock()
		oldList := utils2.ValsOfMap(openOds)
		lock.Unlock()
		return oldList, nil, nil, nil
	}
	// Get exchange positions
	// 获取交易所仓位
	posList, err := exchange.FetchAccountPositions(nil, map[string]interface{}{
		banexg.ParamAccount:     o.Account,
		banexg.ParamSettleCoins: config.StakeCurrency,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	posMap := utils.ArrToMap(posList, func(v *banexg.Position) string {
		openPairs[v.Symbol] = struct{}{}
		return v.Symbol
	})
	lock.Lock()
	var resOdList = make([]*ormo.InOutOrder, 0, len(openOds))
	lock.Unlock()
	for pair := range openPairs {
		curOds := make(map[int64]*ormo.InOutOrder)
		lock.Lock()
		for _, od := range openOds {
			if od.Symbol == pair {
				curOds[od.ID] = od
			}
		}
		lock.Unlock()
		curPos, _ := posMap[pair]
		if len(curPos) == 0 && len(curOds) == 0 {
			continue
		}
		var longPos, shortPos *banexg.Position
		for _, pos := range curPos {
			if pos.Side == banexg.PosSideLong {
				longPos = pos
			} else {
				shortPos = pos
			}
		}
		prevTF, _ := pairLastTfs[pair]
		err = o.syncPairOrders(pair, prevTF, longPos, shortPos, lastOrderMS, curOds)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, od := range curOds {
			if od.Status >= ormo.InOutStatusFullExit {
				continue
			}
			resOdList = append(resOdList, od)
		}
	}
	var oldList = make([]*ormo.InOutOrder, 0, 4)
	var newList = make([]*ormo.InOutOrder, 0, 4)
	var delList = make([]*ormo.InOutOrder, 0, 4)
	resMap := utils.ArrToMap(resOdList, func(od *ormo.InOutOrder) int64 {
		return od.ID
	})
	lock.Lock()
	for key, od := range openOds {
		_, newHas := resMap[key]
		if !newHas {
			delList = append(delList, od)
		}
	}
	for key, od := range resMap {
		_, oldHas := openOds[key]
		if oldHas {
			oldList = append(oldList, od...)
		} else {
			newList = append(newList, od...)
			openOds[key] = od[0]
		}
	}
	lock.Unlock()
	if len(oldList) > 0 {
		log.Info(fmt.Sprintf("%s: Restore %v open orders", o.Account, len(oldList)))
	}
	if len(newList) > 0 {
		log.Info(fmt.Sprintf("%s: Started tracking %v users' orders", o.Account, len(newList)))
	}
	err = ormo.SaveDirtyODs(orm.DbTrades, o.Account)
	if err != nil {
		log.Error("SaveDirtyODs fail", zap.String("acc", o.Account), zap.Error(err))
	}
	return oldList, newList, delList, nil
}

func loadOpenOrders(taskID int64, account string) ([]*ormo.InOutOrder, map[string]string, *errs.Error) {
	sess, conn, err := ormo.Conn(orm.DbTrades, true)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	// Loading orders from the database
	// 从数据库加载未平仓订单
	orders, err := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID: taskID,
		Status: 1,
		Limit:  10000,
	})
	if err != nil {
		return nil, nil, err
	}
	if len(orders) >= 10000 {
		log.Warn("local open orders may be too many to load", zap.String("acc", account),
			zap.Int("num", len(orders)))
	}
	// Query the most recent usage time period of a task
	// 查询任务的最近使用时间周期
	var pairLastTfs = make(map[string]string)
	if config.TakeOverStrat != "" {
		pairLastTfs, err = sess.GetHistOrderTfs(taskID, config.TakeOverStrat)
		if err != nil {
			return nil, nil, err
		}
	}
	return orders, pairLastTfs, nil
}

func delSaveOrders(account string, delOds, saves []*ormo.InOutOrder) {
	sess, conn, err := ormo.Conn(orm.DbTrades, true)
	if err != nil {
		log.Warn("get conn fail", zap.Error(err))
		return
	}
	defer conn.Close()
	for _, od := range delOds {
		fields := []zap.Field{zap.String("acc", account), zap.String("od", od.Key())}
		err = sess.DelOrder(od)
		if err != nil {
			fields = append(fields, zap.Error(err))
			log.Error("del order fail", fields...)
		} else {
			log.Warn("del no enter order", fields...)
		}
	}
	for _, od := range saves {
		err = od.Save()
		if err != nil {
			log.Error("save order in SyncExgOrders fail", zap.String("acc", account), zap.String("key", od.Key()), zap.Error(err))
		}
	}
}

/*
restoreInOutOrder
Restore order status
恢复订单状态
*/
func (o *LiveOrderMgr) restoreInOutOrder(od *ormo.InOutOrder, exgOdMap map[string]*banexg.Order) *errs.Error {
	tryOd := od.Enter
	if od.Exit != nil {
		tryOd = od.Exit
	}
	if tryOd == nil {
		return errs.NewMsg(errs.CodeRunTime, "Enter part of %s is nil", od.Key())
	}
	var err *errs.Error
	if tryOd.Enter && tryOd.OrderID == "" && tryOd.Status == ormo.OdStatusInit {
		// The order has not been submitted to the exchange and is an entry order
		// 订单未提交到交易所，且是入场订单
		if isFarEnter(od) {
			ormo.AddTriggerOd(o.Account, od)
		} else {
			err = od.LocalExit(0, core.ExitTagForceExit, od.InitPrice, "Restart and cancel orders that haven't been filled", "")
			strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
			return err
		}
	} else if tryOd.OrderID != "" && tryOd.Status != ormo.OdStatusClosed {
		// Submitted to the exchange, not yet completed
		// 已提交到交易所，尚未完成
		exOd, ok := exgOdMap[tryOd.OrderID]
		if !ok {
			// The order has been cancelled or completed. Check the exchange order
			// 订单已取消或已成交，查询交易所订单
			exOd, err = exg.Default.FetchOrder(od.Symbol, tryOd.OrderID, map[string]interface{}{
				banexg.ParamAccount: o.Account,
			})
			if err != nil && err.BizCode != -2013 {
				// 跳过错误 -2013 订单不存在
				return err
			}
		}
		if exOd != nil {
			err = o.updateOdByExgRes(od, tryOd.Enter, exOd)
			if err != nil {
				return err
			}
		}
	} else if !tryOd.Enter {
		// Close order. It is impossible that it has been submitted to the exchange but not yet completed. It belongs to the previous else if
		// 平仓订单，这里不可能是已提交到交易所尚未完成，属于上一个else if
		err = o.tryExitPendingEnter(od)
		if err != nil {
			return err
		}
		if od.Status >= ormo.InOutStatusFullExit {
			// 订单已退出
			return nil
		}
		if tryOd.Status == ormo.OdStatusClosed {
			od.Status = ormo.InOutStatusFullExit
			od.DirtyMain = true
			strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
			return nil
		} else if tryOd.Status > ormo.OdStatusInit {
			// You shouldn't go here.
			// 这里不应该走到
			log.Error("Exit Status Invalid", zap.String("acc", o.Account), zap.String("key", od.Key()),
				zap.Int64("sta", tryOd.Status), zap.String("orderId", tryOd.OrderID))
		} else {
			// Here OrderID must be empty, and the number of entry orders must be filled.
			// 这里OrderID一定为空，并且入场单数量一定有成交的。
			o.queue <- &OdQItem{
				Action: ormo.OdActionExit,
				Order:  od,
			}
		}
	}
	return nil
}

/*
For the specified currency, synchronize the exchange order status to the local machine. Executed when the robot is just started.
对指定币种，将交易所订单状态同步到本地。机器人刚启动时执行。
sinceMS是本地记录的已处理交易所最新订单时间戳，只获取此后的订单，同步状态到本地。
*/
func (o *LiveOrderMgr) syncPairOrders(pair, defTF string, longPos, shortPos *banexg.Position, sinceMS int64,
	openOds map[int64]*ormo.InOutOrder) *errs.Error {
	var exOrders []*banexg.Order
	var err *errs.Error
	var curMS = btime.UTCStamp()
	// Get exchange order history and try to restore the order status.
	// 从交易所获取订单记录，尝试恢复订单状态。
	// 这里必须指定sinceMS，避免获取过早的订单创建冗余本地记录
	monMSecs := int64(utils2.TFToSecs("1M") * 1000)
	minSince := curMS - monMSecs
	exOrders, err = exg.Default.FetchOrders(pair, max(sinceMS, minSince), 300, map[string]interface{}{
		banexg.ParamAccount:   o.Account,
		banexg.ParamUntil:     curMS,
		banexg.ParamLoopIntv:  int64(utils2.TFToSecs("7d") * 1000),
		banexg.ParamDirection: "endToStart",
	})
	if err != nil {
		return err
	}
	// 计算机器人仓位，其他端仓位
	var longPosAmt, shortPosAmt float64
	if longPos != nil {
		longPosAmt = longPos.Contracts
	}
	if shortPos != nil {
		shortPosAmt = shortPos.Contracts
	}
	if len(openOds) > 0 {
		exIdMap := make(map[string]*ormo.InOutOrder)
		for _, iod := range openOds {
			if iod.Enter != nil && iod.Enter.OrderID != "" {
				exIdMap[iod.Enter.OrderID] = iod
			}
			if iod.Exit != nil && iod.Exit.OrderID != "" {
				exIdMap[iod.Exit.OrderID] = iod
			}
		}
		for _, exod := range exOrders {
			if !banexg.IsOrderDone(exod.Status) {
				// Skip uncompleted orders
				// 跳过未完成订单
				continue
			}
			err = o.applyHisOrder(openOds, exIdMap, exod, defTF)
			if err != nil {
				return err
			}
		}
		// Check if the remaining open orders match the position. If not, close the corresponding orders.
		// 检查剩余的打开订单是否和仓位匹配，如不匹配强制关闭对应的订单
		for _, iod := range openOds {
			odAmt := iod.HoldAmount()
			if odAmt == 0 {
				if iod.Status == 0 && iod.Enter.OrderID == "" {
					// Not submitted to the exchange yet, cancel directly
					// 尚未提交到交易所，直接取消
					msg := "Cancel unsubmitted orders"
					err = iod.LocalExit(0, core.ExitTagCancel, iod.Enter.Price, msg, "")
					strat.FireOdChange(o.Account, iod, strat.OdChgExitFill)
					if err != nil {
						return err
					}
					delete(openOds, iod.ID)
				}
				continue
			}
			if odAmt*iod.InitPrice < 1 {
				// TODO: The quote value calculated here needs to be changed to the legal currency value later
				// TODO: 这里计算的quote价值，后续需要改为法币价值
				if iod.Status < ormo.InOutStatusFullExit {
					msg := "The order has no corresponding position"
					err = iod.LocalExit(0, core.ExitTagFatalErr, iod.InitPrice, msg, "")
					strat.FireOdChange(o.Account, iod, strat.OdChgExitFill)
					if err != nil {
						return err
					}
				}
				delete(openOds, iod.ID)
				continue
			}
			var fillAmt = float64(0)
			if iod.Short {
				fillAmt = min(shortPosAmt, odAmt)
				shortPosAmt -= odAmt
			} else {
				fillAmt = min(longPosAmt, odAmt)
				longPosAmt -= odAmt
			}
			if fillAmt < odAmt*0.01 {
				msg := fmt.Sprintf("no corresponding position in the exchange")
				err = iod.LocalExit(0, core.ExitTagFatalErr, iod.InitPrice, msg, "")
				strat.FireOdChange(o.Account, iod, strat.OdChgExitFill)
				if err != nil {
					return err
				}
				delete(openOds, iod.ID)
			} else if fillAmt < odAmt*0.99 {
				price := com.GetPrice(pair, "")
				holdCost := odAmt * price
				fillPct := math.Round(fillAmt * 100 / odAmt)
				log.Error("position not match", zap.String("acc", o.Account),
					zap.String("pair", pair), zap.String("key", iod.Key()),
					zap.Float64("holdCost", holdCost), zap.Float64("fillPct", fillPct))
			}
		}
	}
	if config.TakeOverStrat == "" {
		if longPosAmt > AmtDust || shortPosAmt > AmtDust {
			price := com.GetPrice(pair, "")
			longCost := math.Round(longPosAmt*price*100) / 100
			shortCost := math.Round(shortPosAmt*price*100) / 100
			if longCost > 1 {
				longOds := make([]string, 0, len(openOds)/2)
				for _, od := range openOds {
					if !od.Short {
						longOds = append(longOds, od.Key())
					}
				}
				log.Error("unknown long position", zap.String("acc", o.Account), zap.String("pair", pair),
					zap.Float64("longAmt", longPosAmt), zap.Strings("local", longOds))
			}
			if shortCost > 1 {
				shortOds := make([]string, 0, len(openOds)/2)
				for _, od := range openOds {
					if od.Short {
						shortOds = append(shortOds, od.Key())
					}
				}
				log.Error("unknown short position", zap.String("acc", o.Account), zap.String("pair", pair),
					zap.Float64("shortAmt", shortPosAmt), zap.Strings("local", shortOds))
			}
		}
		return nil
	}
	if longPos != nil && longPosAmt > AmtDust {
		longPos.Contracts = longPosAmt
		longOd, err := o.createOdFromPos(longPos, defTF)
		if err != nil {
			return err
		}
		delete(openOds, longOd.ID)
		err = longOd.Save()
		if err != nil {
			return err
		}
	}
	if shortPos != nil && shortPosAmt > AmtDust {
		shortPos.Contracts = shortPosAmt
		shortOd, err := o.createOdFromPos(shortPos, defTF)
		if err != nil {
			return err
		}
		delete(openOds, shortOd.ID)
		err = shortOd.Save()
		if err != nil {
			return err
		}
	}
	return nil
}

func getFeeNameCost(fee *banexg.Fee, pair, odType, side string, amount, price float64) (string, float64, float64) {
	isMaker := false
	if fee != nil {
		if fee.Cost > 0 {
			return fee.Currency, fee.Cost, fee.QuoteCost
		}
		isMaker = fee.IsMaker
	} else {
		isMaker = odType != banexg.OdTypeMarket
	}
	fee, err := exg.Default.CalculateFee(pair, odType, side, amount, price, isMaker, nil)
	if err != nil {
		log.Error("calc fee fail getFeeNameCost", zap.Error(err))
		return "", 0, 0
	}
	return fee.Currency, fee.Cost, fee.QuoteCost
}

func (o *LiveOrderMgr) applyHisOrder(ods map[int64]*ormo.InOutOrder, exIdMap map[string]*ormo.InOutOrder, od *banexg.Order, defTF string) *errs.Error {
	if od.Filled == 0 {
		return nil
	}
	isShort := od.PositionSide == banexg.PosSideShort
	isSell := od.Side == banexg.OdSideSell
	exs, err := orm.GetExSymbolCur(od.Symbol)
	if err != nil {
		return err
	}
	feeName, feeCost, feeQuote := getFeeNameCost(od.Fee, od.Symbol, od.Type, od.Side, od.Filled, od.Average)
	price, amount, odTime := od.Average, od.Filled, od.Timestamp
	defTF = config.GetTakeOverTF(od.Symbol, defTF)

	var inOut *ormo.InOutOrder
	var exOd *ormo.ExOrder
	if iod, ok := exIdMap[od.ID]; ok {
		// 通过订单id直接匹配
		if iod.Enter != nil && iod.Enter.OrderID == od.ID {
			exOd = iod.Enter
		} else if iod.Exit != nil && iod.Exit.OrderID == od.ID {
			exOd = iod.Exit
		}
		inOut = iod
	}
	if exOd == nil {
		// 通过ClientOrderID解析目标订单
		iodID := getClientOrderId(od.ClientOrderID)
		if iodID > 0 {
			if iod, ok := ods[iodID]; ok {
				if isShort == isSell {
					exOd = iod.Enter
				} else {
					exOd = iod.Exit
				}
				inOut = iod
			}
		}
	}
	if exOd != nil {
		if amount > exOd.Filled*1.01 {
			exOd.UpdateAt = odTime
			exOd.Filled = amount
			exOd.Amount = max(amount, exOd.Amount)
			exOd.Average = price
			exOd.FeeQuote = feeQuote
			exOd.Fee = feeCost
			exOd.FeeType = feeName
			if od.Type != "" {
				exOd.OrderType = od.Type
			}
			if od.Status == banexg.OdStatusFilled || amount >= exOd.Amount*0.99 {
				exOd.Status = ormo.OdStatusClosed
				exOd.Price = price
				if exOd.Enter {
					inOut.Status = ormo.InOutStatusFullEnter
					inOut.DirtyEnter = true
				} else {
					inOut.Status = ormo.InOutStatusFullExit
					inOut.DirtyExit = true
				}
			}
			inOut.DirtyMain = true
			inOut.UpdateProfits(price)
			err = inOut.Save()
			if err != nil {
				return err
			}
		}
		return nil
	}
	if isShort == isSell {
		// Open long or short 开多或开空
		if defTF == "" {
			log.Warn("take over job not found", zap.String("acc", o.Account),
				zap.String("pair", od.Symbol), zap.String("strat", config.TakeOverStrat))
			return nil
		}
		tag := "[LONG]"
		if isShort {
			tag = "[SHORT]"
		}
		log.Info(fmt.Sprintf("%s %s: price:%.5f, amount: %.5f, %v, fee: %.5f, %v id:%v",
			o.Account, tag, price, amount, od.Type, feeCost, odTime, od.ID))
		iod := o.createInOutOd(exs, isShort, price, amount, od.Type, feeCost, feeQuote, feeName, odTime, ormo.OdStatusClosed,
			od.ID, defTF)
		err = iod.Save()
		if err != nil {
			return err
		}
		ods[iod.ID] = iod
	} else {
		// Close long or short 平多或平空
		var part *ormo.InOutOrder
		var feeLeft float64
		for _, iod := range ods {
			if iod.Short != isShort || iod.RealEnterMS() > odTime {
				continue
			}
			amount, feeLeft, part = o.tryFillExit(iod, amount, price, odTime, od.ID, od.Type, feeName, feeCost, feeQuote)
			feeCost *= feeLeft
			feeQuote *= feeLeft
			err = part.Save()
			if err != nil {
				return err
			}
			tag := "Close Long"
			if isShort {
				tag = "Close Short"
			}
			log.Info(fmt.Sprintf("%s %v: price:%.5f, amount: %.5f, %v, %v id: %v",
				o.Account, tag, price, part.Exit.Filled, od.Type, odTime, od.ID))
			if iod.Status < ormo.InOutStatusFullExit {
				err = iod.Save()
				if err != nil {
					return err
				}
			}
			if amount <= AmtDust {
				break
			}
		}
		if !od.ReduceOnly && amount > AmtDust {
			// Remaining quantity, create opposite order 剩余数量，创建相反订单
			if defTF == "" {
				log.Warn("take over job not found", zap.String("acc", o.Account),
					zap.String("pair", od.Symbol), zap.String("stagy", config.TakeOverStrat))
				return nil
			}
			tag := "[long]"
			if isShort {
				tag = "[short]"
			}
			log.Info(fmt.Sprintf("%s %v: price:%.5f, amount: %.5f, %v, fee: %.5f %v id: %v",
				o.Account, tag, price, amount, od.Type, feeCost, odTime, od.ID))
			iod := o.createInOutOd(exs, isShort, price, amount, od.Type, feeCost, feeQuote, feeName, odTime, ormo.OdStatusClosed,
				od.ID, defTF)
			err = iod.Save()
			if err != nil {
				return err
			}
			ods[iod.ID] = iod
		}
	}
	return nil
}

func (o *LiveOrderMgr) createInOutOd(exs *orm.ExSymbol, short bool, average, filled float64, odType string,
	feeCost float64, feeQuote float64, feeName string, enterAt int64, entStatus int, entOdId string, defTF string) *ormo.InOutOrder {
	notional := average * filled
	leverage, _ := exg.GetLeverage(exs.Symbol, notional, o.Account)
	if leverage == 0 {
		leverage = config.GetAccLeverage(o.Account)
	}
	status := ormo.InOutStatusPartEnter
	if entStatus == ormo.OdStatusClosed {
		status = ormo.InOutStatusFullEnter
	}
	stgVer, _ := strat.Versions[config.TakeOverStrat]
	entSide := banexg.OdSideBuy
	if short {
		entSide = banexg.OdSideSell
	}
	taskId := ormo.GetTaskID(o.Account)
	od := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			TaskID:    taskId,
			Symbol:    exs.Symbol,
			Sid:       int64(exs.ID),
			Timeframe: defTF,
			Short:     short,
			Status:    int64(status),
			EnterTag:  core.EnterTagThird,
			InitPrice: average,
			QuoteCost: notional * leverage,
			Leverage:  leverage,
			EnterAt:   enterAt,
			Strategy:  config.TakeOverStrat,
			StgVer:    int64(stgVer),
		},
		Enter: &ormo.ExOrder{
			TaskID:    taskId,
			Symbol:    exs.Symbol,
			Enter:     true,
			OrderType: odType,
			OrderID:   entOdId,
			Side:      entSide,
			CreateAt:  enterAt,
			Price:     average,
			Average:   average,
			Amount:    filled,
			Filled:    filled,
			Status:    int64(entStatus),
			Fee:       feeCost,
			FeeQuote:  feeQuote,
			FeeType:   feeName,
			UpdateAt:  enterAt,
		},
		DirtyMain:  true,
		DirtyEnter: true,
	}
	if status >= ormo.InOutStatusFullEnter {
		strat.FireOdChange(o.Account, od, strat.OdChgEnterFill)
	} else {
		strat.FireOdChange(o.Account, od, strat.OdChgEnter)
	}
	return od
}

func (o *LiveOrderMgr) createOdFromPos(pos *banexg.Position, defTF string) (*ormo.InOutOrder, *errs.Error) {
	if defTF == "" {
		return nil, errs.NewMsg(core.ErrBadConfig, "take over job not found, %s %s", pos.Symbol, config.TakeOverStrat)
	}
	exs, err := orm.GetExSymbolCur(pos.Symbol)
	if err != nil {
		return nil, err
	}
	average, filled, entOdType := pos.EntryPrice, pos.Contracts, config.OrderType
	isShort := pos.Side == banexg.PosSideShort
	// There is no handling fee for position information. The handling fee is inferred directly from the current robot order type, which may be different from the actual handling fee.
	//持仓信息没有手续费，直接从当前机器人订单类型推断手续费，可能和实际的手续费不同
	feeName, feeCost, feeQuote := getFeeNameCost(nil, pos.Symbol, "", pos.Side, pos.Contracts, pos.EntryPrice)
	tag := "LONG"
	if isShort {
		tag = "SHORT"
	}
	log.Info(fmt.Sprintf("%s [Pos]%v: price:%.5f, amount:%.5f, fee: %.5f", o.Account, tag, average, filled, feeCost))
	enterAt := btime.TimeMS()
	entStatus := ormo.OdStatusClosed
	iod := o.createInOutOd(exs, isShort, average, filled, entOdType, feeCost, feeQuote, feeName, enterAt, entStatus, "", defTF)
	return iod, nil
}

/*
tryFillExit
Try to close a position, used to update the closing status of the robot's order from a third-party transaction
尝试平仓，用于从第三方交易中更新机器人订单的平仓状态
*/
func (o *LiveOrderMgr) tryFillExit(iod *ormo.InOutOrder, filled, price float64, odTime int64, orderID, odType,
	feeName string, feeCost float64, feeQuote float64) (float64, float64, *ormo.InOutOrder) {
	orgFeeCost := feeCost
	if iod.Enter.Filled == 0 {
		err := iod.LocalExit(0, core.ExitTagForceExit, iod.InitPrice, "not entered", "")
		strat.FireOdChange(o.Account, iod, strat.OdChgExitFill)
		if err != nil {
			log.Error("local exit no enter order fail", zap.String("acc", o.Account),
				zap.String("key", iod.Key()), zap.Error(err))
		}
		return filled, feeCost / orgFeeCost, iod
	}
	var avaAmount float64
	// Should a small order be split?
	var doCut = false // 是否应该分割一个小订单
	if iod.Exit != nil && iod.Exit.Amount > 0 {
		avaAmount = iod.Exit.Amount - iod.Exit.Filled
		doCut = avaAmount/iod.Exit.Amount < 0.99
	} else {
		avaAmount = iod.Enter.Filled
	}
	if !doCut && filled < avaAmount*0.99 {
		doCut = true
	}
	var part = iod
	fillAmt := min(avaAmount, filled)
	curPartRate := fillAmt / filled
	filled -= fillAmt
	if doCut {
		part = iod.CutPart(fillAmt, 0)
	}
	curFeeCost := feeCost * curPartRate
	curFeeQuote := feeQuote * curPartRate
	feeCost -= curFeeCost
	if part.Exit == nil {
		exitSide := banexg.OdSideSell
		if part.Short {
			exitSide = banexg.OdSideBuy
		}
		taskId := ormo.GetTaskID(o.Account)
		part.Exit = &ormo.ExOrder{
			TaskID:    taskId,
			InoutID:   part.ID,
			Symbol:    part.Symbol,
			Enter:     false,
			OrderType: odType,
			OrderID:   orderID,
			Side:      exitSide,
			CreateAt:  odTime,
			Price:     price,
			Average:   price,
			Amount:    part.Enter.Amount,
			Filled:    fillAmt,
			Status:    ormo.OdStatusClosed,
			Fee:       curFeeCost,
			FeeQuote:  curFeeQuote,
			FeeType:   feeName,
			UpdateAt:  odTime,
		}
	} else {
		part.Exit.Filled = fillAmt
		part.Exit.OrderType = odType
		part.Exit.OrderID = orderID
		part.Exit.Price = price
		part.Exit.Average = price
		part.Exit.Status = ormo.OdStatusClosed
		part.Exit.Fee = curFeeCost
		part.Exit.FeeQuote = curFeeQuote
		part.Exit.FeeType = feeName
		part.Exit.UpdateAt = odTime
	}
	part.DirtyExit = true
	part.ExitTag = core.ExitTagThird
	part.ExitAt = odTime
	part.Status = ormo.InOutStatusFullExit
	part.DirtyMain = true
	strat.FireOdChange(o.Account, part, strat.OdChgExitFill)
	return filled, feeCost / orgFeeCost, part
}

func (o *LiveOrderMgr) ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	if len(job.Entrys) == 0 && len(job.Exits) == 0 {
		return nil, nil, nil
	}
	log.Info("ProcessOrders", zap.String("acc", o.Account), zap.String("pair", job.Symbol.Symbol),
		zap.Any("enters", job.Entrys), zap.Any("exits", job.Exits))
	return o.OrderMgr.ProcessOrders(job)
}

func (o *LiveOrderMgr) UpdateByBar(allOpens []*ormo.InOutOrder, bar *orm.InfoKline) *errs.Error {
	if err := o.OrderMgr.UpdateByBar(allOpens, bar); err != nil {
		return err
	}
	// Enforce StopBars for submitted limit entry orders in live mode.
	curMS := btime.TimeMS()
	for _, od := range allOpens {
		if od.Status > ormo.InOutStatusInit || od.Enter == nil || od.Enter.Price == 0 {
			continue
		}
		if !strings.Contains(od.Enter.OrderType, banexg.OdTypeLimit) {
			continue
		}
		if od.Enter.OrderID == "" {
			// Trigger orders (not yet submitted) are handled by VerifyTriggerOds.
			continue
		}
		stopAfter := od.GetInfoInt64(ormo.OdInfoStopAfter)
		if stopAfter > 0 && stopAfter <= curMS {
			cancelTimeoutEnter(o, od)
		}
	}
	return nil
}

func (o *LiveOrderMgr) EditOrder(od *ormo.InOutOrder, action string) {
	if isFarEnter(od) {
		ormo.AddTriggerOd(o.Account, od)
	} else {
		o.queue <- &OdQItem{
			Order:  od,
			Action: action,
		}
	}
}

func makeAfterEnter(o *LiveOrderMgr) FuncHandleIOrder {
	return func(order *ormo.InOutOrder) *errs.Error {
		fields := []zap.Field{zap.String("acc", o.Account), zap.String("key", order.Key())}
		if isFarEnter(order) {
			// Limit orders that are difficult to execute for a long time will not be submitted to the exchange to prevent funds from being occupied.
			// 长时间难以成交的限价单，先不提交到交易所，防止资金占用
			ormo.AddTriggerOd(o.Account, order)
			log.Info("NEW Enter trigger", fields...)
			return nil
		}
		log.Info("NEW Enter", fields...)
		o.queue <- &OdQItem{
			Order:  order,
			Action: ormo.OdActionEnter,
		}
		return nil
	}
}

func makeAfterExit(o *LiveOrderMgr) FuncHandleIOrder {
	return func(order *ormo.InOutOrder) *errs.Error {
		fields := []zap.Field{zap.String("acc", o.Account), zap.String("key", order.Key())}
		action := ormo.OdActionExit
		if order.Exit != nil {
			fields = append(fields, zap.String("tag", order.ExitTag))
			log.Info("Exit Order", fields...)
		} else {
			tp := order.GetTakeProfit()
			if tp != nil {
				action = ormo.OdActionTakeProfit
				log.Info("Set Order TakeProfit", fields...)
			} else {
				log.Error("afterExit: unknown order status", fields...)
				return nil
			}
		}
		o.queue <- &OdQItem{Order: order, Action: action}
		return nil
	}
}

func (o *LiveOrderMgr) ConsumeOrderQueue() {
	if o.isConsumeOrderQ {
		return
	}
	o.isConsumeOrderQ = true
	go func() {
		defer func() {
			o.isConsumeOrderQ = false
		}()
		for {
			var item *OdQItem
			select {
			case <-core.Ctx.Done():
				return
			case item = <-o.queue:
				break
			}
			o.handleOrderQueue(item.Order, item.Action)
		}
	}()
}

func (o *LiveOrderMgr) handleOrderQueue(od *ormo.InOutOrder, action string) {
	err := com.EnsureLatestPrice(od.Symbol)
	if err != nil {
		log.Error("ensureLatestPrice fail", zap.String("od", od.Key()), zap.Error(err))
		if action == ormo.OdActionEnter {
			return
		}
	}
	lock := od.Lock()
	defer lock.Unlock()
	switch action {
	case ormo.OdActionEnter:
		err = o.execOrderEnter(od)
	case ormo.OdActionExit:
		err = o.execOrderExit(od)
	case ormo.OdActionStopLoss, ormo.OdActionTakeProfit:
		o.editTriggerOd(od, action)
	case ormo.OdActionLimitEnter, ormo.OdActionLimitExit:
		err = o.editLimitOd(od, action)
	case ormo.OdActionTrailing:
		o.setTrailingStop(od)
	default:
		log.Error("unknown od action", zap.String("action", action), zap.String("key", od.Key()))
		return
	}
	if err != nil {
		log.Error("ConsumeOrderQueue error", zap.String("acc", o.Account),
			zap.String("action", action), zap.Error(err))
	}
	if od.IsDirty() {
		err = od.Save()
		if err != nil {
			log.Error("save od for exg status fail", zap.String("acc", o.Account),
				zap.String("key", od.Key()), zap.Error(err))
		}
	}
	logFields := []zap.Field{zap.String("acc", o.Account), zap.String("key", od.Key())}
	if action == ormo.OdActionEnter {
		if od.Enter.OrderID != "" && od.Status < ormo.InOutStatusFullExit {
			log.Info("Enter Order Submitted", logFields...)
		} else if od.Status >= ormo.InOutStatusFullExit {
			logFields = append(logFields, zap.String("exitTag", od.ExitTag))
			log.Info("Enter Order Closed", logFields...)
		}
	} else if action == ormo.OdActionExit {
		if od.Exit.OrderID != "" {
			logFields = append(logFields, zap.Int64("state", od.Status))
			log.Info("Exit Order Submitted", logFields...)
		} else if od.Status >= ormo.InOutStatusFullExit {
			logFields = append(logFields, zap.String("exitTag", od.ExitTag))
			log.Info("Exit Order Closed", logFields...)
		}
	}
}

func (o *LiveOrderMgr) WatchMyTrades() {
	if o.isWatchMyTrade {
		return
	}
	out, err := exg.Default.WatchMyTrades(map[string]interface{}{
		banexg.ParamAccount: o.Account,
	})
	if err != nil {
		log.Error("WatchMyTrades fail", zap.String("acc", o.Account), zap.Error(err))
		return
	}
	o.isWatchMyTrade = true
	go func() {
		defer func() {
			o.isWatchMyTrade = false
		}()
		for trade := range out {
			orm.AddDumpRow(orm.DumpWsMyTrade, trade.Symbol+trade.ID, trade)
			if trade.State == banexg.OdStatusOpen {
				continue
			}
			o.handleMyTrade(trade)
		}
	}()
}

func (o *LiveOrderMgr) handleMyTrade(trade *banexg.MyTrade) {
	if _, ok := core.PairsMap[trade.Symbol]; !ok {
		// 忽略不处理的交易对
		return
	}
	tradeKey := trade.Symbol + trade.ID
	if o.checkTradeDone(tradeKey) {
		// 交易已处理
		return
	}
	odKey := trade.Symbol + trade.Order
	if o.checkOrderDone(odKey) {
		// 订单已完成
		return
	}
	o.lockExgIdMap.Lock()
	iod, ok := o.exgIdMap[odKey]
	// OKX: When a trigger/algo order fires, it creates a new order with different ordId.
	// The original algo order is stored with key "algo:XXX", try matching by AlgoId.
	// OKX触发单触发后会生成新的ordId，原始algo订单以"algo:XXX"为key存储，需要用AlgoId匹配
	if !ok && trade.AlgoId != "" {
		algoKey := trade.Symbol + "algo:" + trade.AlgoId
		iod, ok = o.exgIdMap[algoKey]
		if ok {
			// Update exgIdMap to use the new ordId for subsequent trades
			// 更新exgIdMap使用新的ordId，以便后续交易能匹配
			o.exgIdMap[odKey] = iod
		}
	}
	o.lockExgIdMap.Unlock()
	if !ok {
		// Check whether the order is placed by a robot
		// 检查是否是机器人下单
		orderId := getClientOrderId(trade.ClientID)
		if orderId > 0 {
			openOds, lock := ormo.GetOpenODs(o.Account)
			lock.Lock()
			iod, _ = openOds[orderId]
			lock.Unlock()
		}
		if iod == nil {
			if orderId > 0 {
				// 可能订单已完成，从OpenODs中移除了
				log.Warn("no order found in OpenODs, skip handleMyTrade", zap.String("acc", o.Account),
					zap.String("clientID", trade.ClientID), zap.Int64("inoutId", orderId))
			} else {
				// No matching order, recorded in unMatchTrades
				// 没有匹配订单，记录到unMatchTrades
				o.lockUnMatches.Lock()
				o.unMatchTrades[tradeKey] = trade
				o.lockUnMatches.Unlock()
			}
			return
		}
	}
	if strings.Contains(trade.Type, banexg.OdTypeStop) || strings.Contains(trade.Type, banexg.OdTypeTakeProfit) {
		// Ignore stop loss / take profit trigger trades unless they are the tracked enter/exit order.
		// 忽略止损/止盈触发单，但允许处理已追踪的入场/出场订单
		enterID := ""
		exitID := ""
		if iod != nil && iod.Enter != nil {
			enterID = iod.Enter.OrderID
		}
		if iod != nil && iod.Exit != nil {
			exitID = iod.Exit.OrderID
		}
		if trade.Order == "" || (trade.Order != enterID && trade.Order != exitID) {
			return
		}
	}
	lock := iod.Lock()
	defer lock.Unlock()
	err := o.updateByMyTrade(iod, trade)
	if err != nil {
		log.Error("updateByMyTrade fail", zap.String("acc", o.Account), zap.String("key", iod.Key()),
			zap.String("trade", trade.ID), zap.Error(err))
	}
	subOd := iod.Exit
	if iod.Short == (trade.Side == banexg.OdSideSell) {
		subOd = iod.Enter
	}
	if subOd != nil {
		err = o.consumeUnMatches(iod, subOd)
		if err != nil {
			log.Error("consumeUnMatches for WatchMyTrades fail", zap.String("acc", o.Account),
				zap.String("key", iod.Key()), zap.Error(err))
		}
	} else {
		log.Error("handleMyTrade: subOd nil", zap.String("acc", o.Account), zap.String("key", iod.Key()),
			zap.String("trade", trade.ID), zap.String("side", trade.Side),
			zap.String("clientId", trade.ClientID), zap.String("order", trade.Order))
	}
	if iod.IsDirty() {
		if iod.Status == ormo.InOutStatusFullEnter {
			// Place stop loss and take profit orders only after full entry
			// 仅在完全入场后，下止损止盈单
			o.setTrailingStop(iod)
			o.editTriggerOd(iod, ormo.OdActionStopLoss)
			o.editTriggerOd(iod, ormo.OdActionTakeProfit)
		}
		err = iod.Save()
		if err != nil {
			log.Error("save od from myTrade fail", zap.String("acc", o.Account),
				zap.String("key", iod.Key()), zap.Error(err))
		}
	}
}

/*
Parse the order ClientID passed into the exchange
For Binance: botName_inOutId_randNum (underscore separated)
For OKX: {nameHash6}{orderId12}{rand4} (fixed-length alphanumeric)
解析传入交易所的订单ClientID
*/
func getClientOrderId(clientId string) int64 {
	if strings.Contains(clientId, "_") {
		// Binance format: underscore separated
		arr := strings.Split(clientId, "_")
		if len(arr) < 2 || arr[0] != config.Name {
			return 0
		}
		val, err := strconv.ParseInt(arr[1], 10, 64)
		if err != nil {
			return 0
		}
		return val
	}
	// OKX format: fixed-length {nameHash6}{orderId12}{rand4}
	if len(clientId) >= 18 {
		nameHash := utils.HashToAlphaNum(config.Name, 6)
		if strings.HasPrefix(clientId, nameHash) {
			orderIdStr := clientId[6:18]
			val, err := strconv.ParseInt(orderIdStr, 10, 64)
			if err != nil {
				return 0
			}
			return val
		}
	}
	return 0
}

func (o *LiveOrderMgr) TrialUnMatchesForever() {
	if !core.EnvReal || o.isTrialUnMatches {
		return
	}
	o.isTrialUnMatches = true
	go func() {
		defer func() {
			o.isTrialUnMatches = false
		}()
		for {
			if !core.Sleep(time.Second * 3) {
				return
			}
			curMS := btime.UTCStamp()
			// 清理doneTrades中过期1分钟以上的
			o.lockDoneTrades.Lock()
			for td, stamp := range o.doneTrades {
				if stamp+60000 < curMS {
					delete(o.doneTrades, td)
				}
			}
			o.lockDoneTrades.Unlock()
			// 清理doneKeys中过期1分钟以上的
			o.lockDoneKeys.Lock()
			for od, stamp := range o.doneKeys {
				if stamp+60000 < curMS {
					delete(o.doneKeys, od)
				}
			}
			o.lockDoneKeys.Unlock()
			// 清理exgIdMap中过期5分钟的
			o.lockExgIdMap.Lock()
			for k, iod := range o.exgIdMap {
				if iod.Exit != nil && iod.Exit.UpdateAt+300000 < curMS {
					delete(o.exgIdMap, k)
				}
			}
			o.lockExgIdMap.Unlock()
			var pairTrades = make(map[string][]*banexg.MyTrade)
			expireMS := curMS - 1000
			data := make(map[string]*banexg.MyTrade)
			o.lockUnMatches.Lock()
			for key, trade := range o.unMatchTrades {
				if trade.Timestamp >= expireMS {
					continue
				}
				data[key] = trade
				delete(o.unMatchTrades, key)
			}
			o.lockUnMatches.Unlock()
			for _, trade := range data {
				odKey := trade.Symbol + trade.Order
				o.lockExgIdMap.Lock()
				iod, ok := o.exgIdMap[odKey]
				o.lockExgIdMap.Unlock()
				if ok {
					if o.checkOrderDone(odKey) {
						// 订单已完成
						return
					}
					lock := iod.Lock()
					err := o.updateByMyTrade(iod, trade)
					lock.Unlock()
					if err != nil {
						log.Error("updateByMyTrade fail", zap.String("acc", o.Account), zap.String("key", iod.Key()),
							zap.String("trade", trade.ID), zap.Error(err))
					}
					continue
				}
				if getClientOrderId(trade.ClientID) == 0 {
					// Record non-robot orders to check if a third party closes or places an order
					// 记录非机器人订单，检查是否第三方平仓或下单
					odTrades, _ := pairTrades[odKey]
					pairTrades[odKey] = append(odTrades, trade)
				}
			}
			unHandleNum := 0
			allowTakeOver := config.TakeOverStrat != ""
			// Traverse third-party orders to check whether they are closed or tracked
			// 遍历第三方订单，检查是否平仓或跟踪
			for _, trades := range pairTrades {
				exOd, err := banexg.MergeMyTrades(trades)
				if err != nil {
					log.Error("MergeMyTrades fail", zap.String("acc", o.Account), zap.Int("num", len(trades)), zap.Error(err))
					continue
				}
				if o.exitByMyOrder(exOd) {
					continue
				} else if allowTakeOver && o.traceExgOrder(exOd) {
					continue
				}
				unHandleNum += 1
			}
			if unHandleNum > 0 {
				log.Warn(fmt.Sprintf("expired unmatch orders %s: %v", o.Account, unHandleNum))
			}
			err := ormo.SaveDirtyODs(orm.DbTrades, o.Account)
			if err != nil {
				log.Error("SaveDirtyODs fail", zap.String("acc", o.Account), zap.Error(err))
			}
		}
	}()
}

func (o *LiveOrderMgr) updateByMyTrade(od *ormo.InOutOrder, trade *banexg.MyTrade) *errs.Error {
	if trade.State == banexg.OdStatusOpen {
		return nil
	}
	odId := getClientOrderId(trade.ClientID)
	if odId > 0 && odId != od.ID {
		log.Error("update order with wrong", zap.String("acc", o.Account), zap.String("trade", trade.ID),
			zap.String("order", trade.Order), zap.String("client", trade.ClientID),
			zap.String("for", od.Key()), zap.String("side", trade.Side))
		return nil
	}
	o.lockDoneTrades.Lock()
	o.doneTrades[trade.Symbol+trade.ID] = trade.Timestamp
	o.lockDoneTrades.Unlock()
	sl := od.GetStopLoss()
	tp := od.GetTakeProfit()
	isSell := trade.Side == banexg.OdSideSell
	isEnter := od.Short == isSell
	subOd := od.Exit
	dirtTag := "enter"
	var isStopLoss, isTakeProfit bool
	if sl != nil && sl.OrderId == trade.Order {
		isStopLoss = true
	} else if tp != nil && tp.OrderId == trade.Order {
		isTakeProfit = true
	}
	if isEnter {
		subOd = od.Enter
		dirtTag = "exit"
	} else if subOd == nil {
		// Exit order. This is mostly caused by stop loss or take profit. No exit sub-order has been created yet.
		// 退出订单，这里多半是止损止盈导致的退出，尚未创建退出子订单
		if isStopLoss {
			od.SetExit(0, core.ExitTagStopLoss, banexg.OdTypeMarket, 0)
		} else if isTakeProfit {
			od.SetExit(0, core.ExitTagTakeProfit, banexg.OdTypeTakeProfit, 0)
		} else {
			trialID := od.GetInfoString(ormo.OdInfoTrailingID)
			if trialID == trade.Order {
				od.SetExit(0, core.ExitTagTrailingStop, banexg.OdTypeMarket, 0)
			} else {
				// TODO: 检查是否是用户主动平仓，用户可能一次性平仓多个，需要更新相关订单状态
				log.Error(fmt.Sprintf("%s %s subOd %s nil, trade state: %s", o.Account, od.Key(), dirtTag, trade.State))
				return nil
			}
		}
		subOd = od.Exit
		subOd.UpdateAt = trade.Timestamp
	}
	if trade.Timestamp < subOd.UpdateAt {
		// 收到的订单更新不一定按服务器端顺序。故早于已处理的时间戳的跳过
		return nil
	}
	if subOd.Status == ormo.OdStatusClosed {
		log.Debug(fmt.Sprintf("%s %s %s complete, skip trade: %v", o.Account, od.Key(), dirtTag, trade.ID))
		return nil
	}
	if subOd.Enter {
		od.DirtyEnter = true
	} else {
		od.DirtyExit = true
	}
	subOd.UpdateAt = trade.Timestamp
	if subOd.Amount == 0 {
		subOd.Amount = trade.Amount
	}
	state := trade.State
	if state == banexg.OdStatusFilled || state == banexg.OdStatusPartFilled {
		odStatus := ormo.OdStatusPartOK
		if subOd.Filled == 0 {
			// don't update EnterAt, it would affect Key of order
			if !subOd.Enter {
				od.ExitAt = trade.Timestamp
			}
			od.DirtyMain = true
		}
		subOd.OrderType = trade.Type
		subOd.Filled = trade.Filled
		subOd.Average = trade.Average
		if state == banexg.OdStatusFilled {
			odStatus = ormo.OdStatusClosed
			subOd.Price = trade.Average
			if subOd.Enter {
				od.Status = ormo.InOutStatusFullEnter
			} else {
				od.Status = ormo.InOutStatusFullExit
			}
			od.DirtyMain = true
		}
		subOd.Status = int64(odStatus)
		if trade.Fee != nil {
			subOd.FeeType = trade.Fee.Currency
			subOd.Fee = trade.Fee.Cost
			subOd.FeeQuote = trade.Fee.QuoteCost
		}
	} else if banexg.IsOrderDone(state) {
		subOd.Status = ormo.OdStatusClosed
		if subOd.Enter {
			if subOd.Filled == 0 {
				od.Status = ormo.InOutStatusFullExit
			} else {
				od.Status = ormo.InOutStatusFullEnter
			}
			od.DirtyMain = true
		}
	} else {
		log.Error(fmt.Sprintf("unknown bnb order status %s: %s", o.Account, state))
	}
	if od.Status == ormo.InOutStatusFullExit {
		// May be triggered by stop loss or take profit, delete and set to completed
		// 可能由止盈止损触发，删除置为已完成
		if isStopLoss {
			sl.OrderId = ""
			od.DirtyInfo = true
		} else if isTakeProfit {
			tp.OrderId = ""
			od.DirtyInfo = true
		}
		err := o.finishOrder(od)
		if err != nil {
			return err
		}
		cancelTriggerOds(od, o.Account)
		o.callBack(od, subOd.Enter)
		strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
	} else {
		o.callBack(od, subOd.Enter)
		strat.FireOdChange(o.Account, od, strat.OdChgEnterFill)
	}
	return nil
}

func (o *LiveOrderMgr) execOrderEnter(od *ormo.InOutOrder) *errs.Error {
	if od.ExitTag != "" {
		// 订单已取消，不提交到交易所
		return nil
	}
	odKey := od.Key()

	var err *errs.Error
	if od.Enter.Amount == 0 {
		if od.QuoteCost == 0 {
			wallets := GetWallets(o.Account)
			_, err = wallets.EnterOd(od)
			if err != nil {
				if err.Code == core.ErrLowFunds || err.Code == core.ErrInvalidCost {
					o.forceDelOd(od, err)
					return nil
				} else {
					msg := err.Short()
					err = od.LocalExit(0, core.ExitTagFatalErr, od.InitPrice, msg, "")
					strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
					if err != nil {
						log.Error("local exit order fail", zap.String("acc", o.Account), zap.String("key", odKey), zap.Error(err))
					}
					return errs.NewMsg(core.ErrRunTime, "%s, %s", msg, odKey)
				}
			}
		}
		err = com.EnsureLatestPrice(od.Symbol)
		if err != nil {
			return err
		}
		realPrice := com.GetPriceSafe(od.Symbol, od.Enter.Side)
		if realPrice < 0 {
			msg := "no valid price"
			err = od.LocalExit(0, core.ExitTagFatalErr, od.InitPrice, msg, "")
			strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
			return errs.NewMsg(errs.CodeParamInvalid, "no valid price")
		}
		// The market price should be used to calculate the quantity here, because the input price may be very different from the market price
		// 这里应使用市价计算数量，因传入价格可能和市价相差很大
		od.Enter.Amount, err = exg.PrecAmount(exg.Default, od.Symbol, od.QuoteCost/realPrice)
		if err != nil {
			o.forceDelOd(od, err)
			return nil
		} else if od.Enter.Amount == 0 {
			o.forceDelOd(od, errs.NewMsg(core.ErrRunTime, "amount too small"))
			return nil
		}
	}
	err = o.submitExgOrder(od, true)
	if err != nil {
		msg := "submit order fail, local exit"
		log.Error(msg, zap.String("acc", o.Account), zap.String("key", odKey), zap.Error(err))
		err = od.LocalExit(0, core.ExitTagFatalErr, od.InitPrice, err.Short(), "")
		strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
		if err != nil {
			log.Error("local exit order fail", zap.String("acc", o.Account), zap.String("key", odKey), zap.Error(err))
		}
	}
	return nil
}

func (o *LiveOrderMgr) forceDelOd(od *ormo.InOutOrder, err *errs.Error) {
	odKey := od.Key()
	if err != nil {
		log.Error("del order", zap.String("acc", o.Account), zap.String("key", odKey), zap.Error(err))
	}
	sess, conn, err := ormo.Conn(orm.DbTrades, true)
	if err != nil {
		log.Error("get db sess fail", zap.String("acc", o.Account), zap.Error(err))
		return
	}
	defer conn.Close()
	err = sess.DelOrder(od)
	if err != nil {
		log.Error("del order fail", zap.String("acc", o.Account), zap.String("key", odKey), zap.Error(err))
	}
}

func (o *LiveOrderMgr) tryExitPendingEnter(od *ormo.InOutOrder) *errs.Error {
	if od.Enter.Status == ormo.OdStatusClosed {
		return nil
	}
	// May not have entered yet, or may not have fully entered
	// 可能尚未入场，或未完全入场
	if od.Enter.OrderID != "" {
		order, err := exg.Default.CancelOrder(od.Enter.OrderID, od.Symbol, map[string]interface{}{
			banexg.ParamAccount: o.Account,
		})
		if err != nil {
			log.Error("cancel order fail", zap.String("acc", o.Account),
				zap.String("key", od.Key()), zap.String("err", err.Short()))
		} else {
			err = o.updateOdByExgRes(od, true, order)
			if err != nil {
				log.Error("apply cancel res fail", zap.String("acc", o.Account), zap.String("key", od.Key()), zap.Error(err))
			}
		}
	}
	if od.Enter.Filled == 0 {
		od.Status = ormo.InOutStatusFullExit
		if od.Enter.Status < ormo.OdStatusClosed {
			od.Enter.Status = ormo.OdStatusClosed
			od.DirtyEnter = true
		}
		od.SetExit(0, core.ExitTagForceExit, "", od.Enter.Price)
		od.Exit.Status = ormo.OdStatusClosed
		od.DirtyMain = true
		od.DirtyExit = true
		err := o.finishOrder(od)
		if err != nil {
			return err
		}
		cancelTriggerOds(od, o.Account)
		strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
		o.forceDelOd(od, nil)
		return nil
	} else if od.Enter.Status < ormo.OdStatusClosed {
		od.Enter.Status = ormo.OdStatusClosed
		od.DirtyEnter = true
	}
	if od.Enter.Filled > 0 {
		o.callBack(od, true)
	}
	return nil
}

func (o *LiveOrderMgr) execOrderExit(od *ormo.InOutOrder) *errs.Error {
	err := o.tryExitPendingEnter(od)
	if err != nil {
		return err
	}
	if od.Status >= ormo.InOutStatusFullExit {
		return nil
	}
	return o.submitExgOrder(od, false)
}

func (o *LiveOrderMgr) submitExgOrder(od *ormo.InOutOrder, isEnter bool) *errs.Error {
	subOd := od.Exit
	if isEnter {
		subOd = od.Enter
	}
	setDirty := func() {
		if isEnter {
			od.DirtyEnter = true
		} else {
			od.DirtyExit = true
		}
	}
	var err *errs.Error
	exchange := exg.Default
	leverage, maxLeverage := exg.GetLeverage(od.Symbol, od.QuoteCost, o.Account)
	if isEnter && od.Leverage > 0 && od.Leverage != leverage {
		newLeverage := od.Leverage
		if maxLeverage >= 1 {
			newLeverage = min(maxLeverage, od.Leverage)
		} else {
			log.Warn("get symbol leverage fail", zap.String("pair", od.Symbol),
				zap.Float64("cur", leverage), zap.Float64("max", maxLeverage))
		}
		if newLeverage != leverage {
			_, err = exchange.SetLeverage(newLeverage, od.Symbol, map[string]interface{}{
				banexg.ParamAccount: o.Account,
			})
			if err != nil {
				return err
			}
			// The leverage of this currency is relatively small, so the corresponding amount is reduced
			// 此币种杠杆比较小，对应缩小金额
			rate := newLeverage / od.Leverage
			od.Leverage = newLeverage
			subOd.Amount *= rate
			od.QuoteCost *= rate
			od.DirtyMain = true
			setDirty()
		}
	}
	if subOd.OrderType == "" {
		subOd.OrderType = config.OrderType
		setDirty()
	}
	if subOd.Price == 0 && subOd.OrderType != banexg.OdTypeMarket {
		// calculate the price when it is not a market order
		// 非市价单时，计算价格
		buyPrice, sellPrice := o.getLimitPrice(od.Symbol, config.LimitVolSecs)
		price := sellPrice
		if subOd.Side == banexg.OdSideBuy {
			price = buyPrice
		}
		subOd.Price, err = exg.PrecPrice(exchange, od.Symbol, price)
		if err != nil {
			return err
		}
		setDirty()
	}
	if subOd.Amount == 0 {
		if isEnter {
			return errs.NewMsg(core.ErrRunTime, "amount is required for %s", od.Key())
		}
		subOd.Amount = od.Enter.Filled
		if subOd.Amount == 0 {
			// No amount, direct local exit.
			// 没有入场，直接本地退出。
			od.Status = ormo.InOutStatusFullExit
			subOd.Price = od.Enter.Price
			od.DirtyExit = true
			od.DirtyMain = true
			err = o.finishOrder(od)
			if err != nil {
				return err
			}
			cancelTriggerOds(od, o.Account)
			strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
			return nil
		}
	}
	side, amount, price := subOd.Side, subOd.Amount, subOd.Price
	params := map[string]interface{}{
		banexg.ParamAccount:       o.Account,
		banexg.ParamClientOrderId: od.ClientId(true),
	}
	if core.IsContract {
		params[banexg.ParamPositionSide] = "LONG"
		if od.Short {
			params[banexg.ParamPositionSide] = "SHORT"
		}
	}
	if isEnter && od.Stop > 0 {
		// 设置触发价入场价格
		curPrice := com.GetPriceSafe(od.Symbol, side)
		if curPrice <= 0 {
			// 价格缓存过期，从订单簿获取
			book, err := exg.GetOdBook(od.Symbol)
			if err == nil && book != nil {
				if side == banexg.OdSideBuy && len(book.Asks.Price) > 0 {
					curPrice = book.Asks.Price[0]
				} else if len(book.Bids.Price) > 0 {
					curPrice = book.Bids.Price[0]
				}
			}
		}
		if curPrice > 0 {
			if (od.Stop >= curPrice) == (side == banexg.OdSideBuy) {
				params[banexg.ParamStopLossPrice] = od.Stop
			} else {
				params[banexg.ParamTakeProfitPrice] = od.Stop
			}
		} else {
			// 无法获取价格，根据方向默认设置
			if side == banexg.OdSideBuy {
				params[banexg.ParamStopLossPrice] = od.Stop
			} else {
				params[banexg.ParamTakeProfitPrice] = od.Stop
			}
		}
	}
	retryNum := od.GetInfoInt64(ormo.OdInfoRetry)
	if retryNum > 0 {
		params[banexg.ParamRetry] = int(retryNum)
	}
	res, err := exchange.CreateOrder(od.Symbol, subOd.OrderType, side, amount, price, params)
	if err != nil {
		if !isEnter && err.BizCode == -2022 {
			msg := "ReduceOnly Order is rejected."
			log.Error("close exg pos fail", zap.String("acc", o.Account), zap.String("key", od.Key()), zap.Error(err))
			err = od.LocalExit(btime.UTCStamp(), core.ExitTagNoMatch, price, msg, banexg.OdTypeMarket)
			if err != nil {
				return err
			}
			cancelTriggerOds(od, o.Account)
			o.callBack(od, isEnter)
			return nil
		} else {
			return err
		}
	}
	err = o.updateOdByExgRes(od, isEnter, res)
	if err != nil {
		return err
	}
	if isEnter {
		if od.Status == ormo.InOutStatusFullEnter {
			// Place stop loss and take profit orders only after full entry
			// 仅在完全入场后，下止损止盈单
			o.setTrailingStop(od)
			o.editTriggerOd(od, ormo.OdActionStopLoss)
			o.editTriggerOd(od, ormo.OdActionTakeProfit)
		}
	} else {
		// Close a position and cancel associated orders
		// 平仓，取消关联订单
		cancelTriggerOds(od, o.Account)
	}
	return nil
}

func (o *LiveOrderMgr) updateOdByExgRes(od *ormo.InOutOrder, isEnter bool, res *banexg.Order) *errs.Error {
	subOd := od.Exit
	if isEnter {
		subOd = od.Enter
		od.DirtyEnter = true
	} else {
		od.DirtyExit = true
	}
	if subOd.OrderID != "" && subOd.OrderID != res.ID {
		// If you modify the order price, order_id will change
		// 如修改订单价格，order_id会变化
		o.lockDoneKeys.Lock()
		o.doneKeys[od.Symbol+subOd.OrderID] = res.LastTradeTimestamp
		o.lockDoneKeys.Unlock()
	}
	subOd.OrderID = res.ID
	idKey := od.Symbol + subOd.OrderID
	o.lockExgIdMap.Lock()
	o.exgIdMap[idKey] = od
	o.lockExgIdMap.Unlock()
	if o.hasNewTrades(res) && subOd.UpdateAt <= res.Timestamp {
		subOd.UpdateAt = res.Timestamp
		if subOd.Amount == 0 {
			subOd.Amount = res.Amount
		}
		if res.Filled > 0 {
			fillPrice := subOd.Price
			if res.Average > 0 {
				fillPrice = res.Average
			} else if res.Price > 0 {
				fillPrice = res.Price
			}
			subOd.Average = fillPrice
			if subOd.Filled == 0 {
				// don't update EnterAt, it would affect Key of Order
				if !isEnter {
					od.ExitAt = res.Timestamp
				}
				od.DirtyMain = true
			}
			subOd.Filled = res.Filled
			if res.Fee != nil && res.Fee.Cost > 0 {
				subOd.Fee = res.Fee.Cost
				subOd.FeeQuote = res.Fee.QuoteCost
				subOd.FeeType = res.Fee.Currency
			}
			subOd.Status = ormo.OdStatusPartOK
		}
		if banexg.IsOrderDone(res.Status) {
			subOd.Status = ormo.OdStatusClosed
			if subOd.Filled > 0 && subOd.Average > 0 {
				subOd.Price = subOd.Average
			}
			if res.Filled == 0 {
				if isEnter {
					// Entry order, 0 trades, closed; overall status: fully exited
					// 入场订单，0成交，被关闭；整体状态为：完全退出
					od.Status = ormo.InOutStatusFullExit
				} else {
					// Exit order, 0 transactions, closed, overall status: entered
					// 出场订单，0成交，被关闭，整体状态为：已入场
					od.Status = ormo.InOutStatusFullEnter
				}
			} else {
				if isEnter {
					od.Status = ormo.InOutStatusFullEnter
				} else {
					od.Status = ormo.InOutStatusFullExit
				}
			}
			od.DirtyMain = true
		}
		if od.Status == ormo.InOutStatusFullExit {
			err := o.finishOrder(od)
			o.callBack(od, false)
			strat.FireOdChange(o.Account, od, strat.OdChgExitFill)
			if err != nil {
				return err
			}
		} else {
			o.callBack(od, true)
			strat.FireOdChange(o.Account, od, strat.OdChgEnterFill)
		}
	}
	return o.consumeUnMatches(od, subOd)
}

func (o *LiveOrderMgr) hasNewTrades(res *banexg.Order) bool {
	if res == nil {
		return false
	}
	if banexg.IsOrderDone(res.Status) || res.Filled > 0 {
		return true
	}
	if len(res.Trades) == 0 {
		return false
	}
	for _, trade := range res.Trades {
		key := res.Symbol + trade.ID
		if !o.checkTradeDone(key) {
			o.lockDoneTrades.Lock()
			o.doneTrades[key] = trade.Timestamp
			o.lockDoneTrades.Unlock()
			return true
		}
	}
	return false
}

/*
从缓存的unMatchTrades中查找和subOd.OrderID匹配的交易，用于更新此订单
*/
func (o *LiveOrderMgr) consumeUnMatches(od *ormo.InOutOrder, subOd *ormo.ExOrder) *errs.Error {
	data := make(map[string]*banexg.MyTrade)
	o.lockUnMatches.Lock()
	for key, trade := range o.unMatchTrades {
		if trade.Symbol != od.Symbol || trade.Order != subOd.OrderID {
			continue
		}
		delete(o.unMatchTrades, key)
		data[key] = trade
	}
	o.lockUnMatches.Unlock()
	if subOd.Status == ormo.OdStatusClosed {
		return nil
	}
	for key, trade := range data {
		ok := o.checkTradeDone(key)
		if ok || trade.Timestamp < subOd.UpdateAt {
			continue
		}
		odKey := trade.Symbol + trade.Order
		if o.checkOrderDone(odKey) {
			continue
		}
		err := o.updateByMyTrade(od, trade)
		if err != nil {
			return err
		}
	}
	return nil
}

type VolPrice struct {
	BuyPrice  float64
	SellPrice float64
	ExpireMS  int64
}

/*
getLimitPrice
Get the approximate limit order price for a specified number of seconds
获取等待指定秒数的大概限价单价格
*/
func (o *LiveOrderMgr) getLimitPrice(pair string, waitSecs int) (float64, float64) {
	key := fmt.Sprintf("%s_%s", pair, strconv.Itoa(waitSecs))
	lockVolPrices.Lock()
	cache, ok := volPrices[key]
	lockVolPrices.Unlock()
	if ok && cache.ExpireMS > btime.TimeMS() {
		return cache.BuyPrice, cache.SellPrice
	}
	// Invalid or expired, need to be recalculated
	// 无效或过期，需要重新计算
	avgVol, lastVol, err := getPairMinsVol(pair, 5)
	if err != nil {
		log.Error("getPairMinsVol fail for getLimitPrice", zap.String("acc", o.Account), zap.String("pair", pair), zap.Error(err))
	}
	secsFlt := float64(waitSecs)
	// 5-minute trading volume per second * waiting seconds * 2: The final multiplication by 2 here is to prevent the trading volume from being too low
	// 5分钟每秒成交量*等待秒数*2：这里最后乘2是以防成交量过低
	depth := min(avgVol/30*secsFlt, lastVol/60*secsFlt)
	book, err := exg.GetOdBook(pair)
	var buyPrice, sellPrice float64
	if err != nil {
		buyPrice, sellPrice = 0, 0
		log.Error("get odBook fail", zap.String("acc", o.Account), zap.String("pair", pair), zap.Error(err))
	} else {
		buyPrice, _, _ = book.AvgPrice(banexg.OdSideBuy, depth)
		sellPrice, _, _ = book.AvgPrice(banexg.OdSideSell, depth)
	}
	// The longest price cache is 3 seconds, and the shortest is 1/10 of the incoming price.
	// 价格缓存最长3s，最短传入的1/10
	expMS := min(3000, int64(waitSecs)*100)
	lockVolPrices.Lock()
	volPrices[key] = &VolPrice{
		BuyPrice:  buyPrice,
		SellPrice: sellPrice,
		ExpireMS:  btime.TimeMS() + expMS,
	}
	lockVolPrices.Unlock()
	return buyPrice, sellPrice
}

/*
getPairMinsVol
Get the average volume per minute and the volume of the last minute over a period of time. This function has a cache and is updated every minute.
获取一段时间内，每分钟平均成交量，以及最后一分钟成交量
此函数有缓存，每分钟更新
*/
func getPairMinsVol(pair string, num int) (float64, float64, *errs.Error) {
	cacheKey := fmt.Sprintf("%s_%v", pair, num)
	lockPairVolMap.Lock()
	cache, ok := pairVolMap[cacheKey]
	lockPairVolMap.Unlock()
	curMs := btime.TimeMS()
	if ok && cache.ExpireMS > curMs {
		return cache.AvgVol, cache.LastVol, nil
	}
	calc := func() (float64, float64, *errs.Error) {
		exs, err := orm.GetExSymbolCur(pair)
		if err != nil {
			return 0, 0, err
		}
		_, bars, err := orm.AutoFetchOHLCV(exg.Default, exs, "1m", 0, 0, num, false, nil)
		if err != nil {
			return 0, 0, err
		} else if len(bars) == 0 {
			return 0, 0, nil
		}
		sumVol := float64(0)
		for _, bar := range bars {
			sumVol += bar.Volume
		}
		lastMinVol := bars[len(bars)-1].Volume
		return sumVol / float64(len(bars)), lastMinVol, nil
	}
	avg, last, err := calc()
	expireMS := utils2.AlignTfMSecs(curMs+60000, 60000)
	lockPairVolMap.Lock()
	pairVolMap[cacheKey] = &PairValItem{AvgVol: avg, LastVol: last, ExpireMS: expireMS}
	lockPairVolMap.Unlock()
	return avg, last, err
}

func getEnterTriggerTarget(od *ormo.InOutOrder) (string, float64, bool) {
	if od == nil || od.Enter == nil {
		return "", 0, false
	}
	isLimit := od.Enter.Price > 0 && strings.Contains(od.Enter.OrderType, banexg.OdTypeLimit)
	if !isLimit && od.Stop <= 0 {
		// 跳过非限价单、非触发单
		return "", 0, false
	}
	if od.Stop > 0 {
		// 检查触发价格是否耗时较长
		side := banexg.OdSideSell
		if od.Short {
			// 触发价格方向与订单入场方向相反
			side = banexg.OdSideBuy
		}
		return side, od.Stop, od.Stop > 0
	}
	return od.Enter.Side, od.Enter.Price, od.Enter.Price > 0
}

func isFarEnter(od *ormo.InOutOrder) bool {
	if od.Status > ormo.InOutStatusPartEnter {
		// 跳过已完全入场
		return false
	}
	side, price, ok := getEnterTriggerTarget(od)
	if !ok {
		return false
	}
	stopAfter := od.GetInfoInt64(ormo.OdInfoStopAfter)
	if stopAfter == 0 || stopAfter <= btime.TimeMS() {
		return false
	}
	return isFarLimitTrigger(od.Symbol, side, price)
}

/*
Determine whether an order is a limit order that is difficult to execute for a long time
判断一个订单是否是长时间难以成交的限价单
*/
func isFarLimitTrigger(pair, side string, price float64) bool {
	secs, rate, err := getSecsByLimit(pair, side, price)
	if err != nil {
		log.Error("getSecsByLimit for isFarLimitTrigger fail", zap.String("pair", pair),
			zap.String("side", side), zap.Float64("price", price), zap.Error(err))
		return false
	}
	if secs < config.PutLimitSecs && rate >= 0.8 {
		return false
	}
	return true
}

var verifyTriggersLock deadlock.Mutex

/*
VerifyTriggerOds
Check if there is a triggerable limit order. If so, submit it to the exchange and it should be called every minute.
Only for real trading
检查是否有可触发的限价单，如有，提交到交易所，应被每分钟调用
仅实盘使用
*/
func VerifyTriggerOds() {
	if !verifyTriggersLock.TryLock() {
		return
	}
	defer verifyTriggersLock.Unlock()
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		verifyAccountTriggerOds(account)
	}
}

func getSubmittedEnterOdsByPair(account string) map[string][]*ormo.InOutOrder {
	openOds, lock := ormo.GetOpenODs(account)
	lock.Lock()
	list := make([]*ormo.InOutOrder, 0, len(openOds))
	for _, od := range openOds {
		list = append(list, od)
	}
	lock.Unlock()
	submitOds := make(map[string][]*ormo.InOutOrder)
	for _, od := range list {
		if od == nil || od.Exit != nil {
			continue
		}
		if od.Status > ormo.InOutStatusPartEnter {
			continue
		}
		if od.Enter == nil || od.Enter.OrderID == "" {
			continue
		}
		if od.Enter.Filled > 0 {
			// Skip partial filled to avoid overbuy when re-submit
			// 部分成交跳过，避免重新提交时超量
			continue
		}
		_, price, ok := getEnterTriggerTarget(od)
		if !ok || price <= 0 {
			continue
		}
		submitOds[od.Symbol] = append(submitOds[od.Symbol], od)
	}
	return submitOds
}

func verifyAccountTriggerOds(account string) {
	triggerOds, lock := ormo.GetTriggerODs(account)
	var resOds []*ormo.InOutOrder
	var copyTriggers = make(map[string]map[int64]*ormo.InOutOrder)
	lock.Lock()
	for key, val := range triggerOds {
		copyTriggers[key] = val
	}
	lock.Unlock()
	var zeros []string
	var fails []string
	odMgr := GetLiveOdMgr(account)
	var saves []*ormo.InOutOrder
	submitOds := getSubmittedEnterOdsByPair(account)
	pairs := make(map[string]struct{})
	for pair := range copyTriggers {
		pairs[pair] = struct{}{}
	}
	for pair := range submitOds {
		pairs[pair] = struct{}{}
	}
	for pair := range pairs {
		ods := copyTriggers[pair]
		var secsVol float64
		var book *banexg.OrderBook
		// Calculate the past 50 minutes, average volume, and last minute volume
		// 计算过去50分钟，平均成交量，以及最后一分钟成交量
		avgVol, lastVol, err := getPairMinsVol(pair, 50)
		if err == nil {
			secsVol = max(avgVol, lastVol) / 60
			if secsVol > 0 {
				book, err = exg.GetOdBook(pair)
			} else {
				zeros = append(zeros, pair)
			}
		}
		if err != nil {
			fails = append(fails, pair)
			log.Error("VerifyTriggerOds fail", zap.String("pair", pair), zap.Error(err))
		}
		calcWait := func(side string, price float64) (int, float64) {
			waitVol, rate := book.SumVolTo(side, price)
			waitSecs := int(math.Round(waitVol / secsVol))
			return waitSecs, rate
		}
		var leftOds = make(map[int64]*ormo.InOutOrder)
		if len(ods) > 0 {
			if book == nil {
				for _, od := range ods {
					leftOds[od.ID] = od
				}
			} else {
				var viewKeys = make(map[string]bool)
				for _, od := range ods {
					odKey := od.Key()
					if _, ok := viewKeys[odKey]; ok {
						log.Error("duplicate order in triggers", zap.String("acc", account), zap.String("key", odKey))
						continue
					}
					viewKeys[odKey] = true
					if od.Status >= ormo.InOutStatusFullExit {
						continue
					}
					subOd := od.Enter
					if od.Exit != nil {
						subOd = od.Exit
					}
					// Calculate the amount to be purchased and the price ratio to reach the specified price
					// 计算到指定价格，需要吃进的量，以及价格比例
					waitSecs, rate := calcWait(subOd.Side, subOd.Price)
					if waitSecs < config.PutLimitSecs && rate >= 0.8 {
						resOds = append(resOds, od)
					} else {
						stopAfter := od.GetInfoInt64(ormo.OdInfoStopAfter)
						if stopAfter > 0 && stopAfter <= btime.TimeMS() {
							cancelTimeoutEnter(odMgr, od)
							saves = append(saves, od)
						} else {
							leftOds[od.ID] = od
						}
					}
				}
			}
		}
		if len(ods) > 0 {
			lock.Lock()
			triggerOds[pair] = leftOds
			lock.Unlock()
		}
		subList := submitOds[pair]
		if len(subList) == 0 || book == nil || secsVol == 0 {
			continue
		}
		for _, od := range subList {
			stopAfter := od.GetInfoInt64(ormo.OdInfoStopAfter)
			if stopAfter > 0 && stopAfter <= btime.TimeMS() {
				cancelTimeoutEnter(odMgr, od)
				saves = append(saves, od)
				continue
			}
			side, price, ok := getEnterTriggerTarget(od)
			if !ok || price <= 0 {
				continue
			}
			waitSecs, _ := calcWait(side, price)
			if waitSecs > config.PutLimitSecs*5 {
				// Far from execution, rollback to local trigger to avoid fund occupation
				// 距离成交过远，撤回到本地触发，避免资金占用
				if cancelEnterToLocalTrigger(odMgr, od, waitSecs) {
					saves = append(saves, od)
				}
			}
		}
	}
	if len(zeros)+len(fails) > 0 {
		log.Warn("calc vols for triggers fail", zap.Strings("zeros", zeros), zap.Strings("fail", fails))
	}
	if len(resOds) > 0 {
		log.Info("put trigger to exchange", zap.Int("num", len(resOds)))
	}
	for _, od := range resOds {
		if od.Status >= ormo.InOutStatusFullExit {
			continue
		}
		tag := ormo.OdActionEnter
		if od.Exit != nil {
			tag = ormo.OdActionExit
		}
		odMgr.queue <- &OdQItem{
			Order:  od,
			Action: tag,
		}
	}
	if len(saves) > 0 {
		saveIOrders(saves)
	}
}

/*
getSecsByLimit
Based on the target price, calculate the approximate waiting time for the transaction.
根据目标价格，计算大概成交需要等待的时长。
*/
func getSecsByLimit(pair, side string, price float64) (int, float64, *errs.Error) {
	avgVol, lastVol, err := getPairMinsVol(pair, 50)
	if err != nil {
		return 0, 1, err
	}
	secsVol := max(avgVol, lastVol) / 60
	if secsVol == 0 {
		return 0, 1, nil
	}
	book, err := exg.GetOdBook(pair)
	if err != nil {
		return 0, 1, err
	}
	waitVol, rate := book.SumVolTo(side, price)
	return int(math.Round(waitVol / secsVol)), rate, nil
}

func saveIOrders(saveOds []*ormo.InOutOrder) {
	// There are orders that need to be saved
	// 有需要保存的订单
	for _, od := range saveOds {
		err := od.Save()
		if err != nil {
			log.Error("save od fail", zap.String("key", od.Key()), zap.Error(err))
		}
	}
}

func cancelEnterToLocalTrigger(odMgr *LiveOrderMgr, od *ormo.InOutOrder, waitSecs int) bool {
	lock := od.Lock()
	defer lock.Unlock()
	if od.Enter == nil || od.Enter.OrderID == "" {
		return false
	}
	if od.Enter.Filled > 0 || od.Status > ormo.InOutStatusPartEnter {
		return false
	}
	orderId := od.Enter.OrderID
	res, err := exg.Default.CancelOrder(orderId, od.Symbol, map[string]interface{}{
		banexg.ParamAccount: odMgr.Account,
	})
	if err != nil {
		log.Error("cancel enter order fail", zap.String("acc", odMgr.Account),
			zap.String("key", od.Key()), zap.Error(err))
		return false
	}
	if res != nil {
		odMgr.lockDoneKeys.Lock()
		odMgr.doneKeys[od.Symbol+orderId] = res.Timestamp
		odMgr.lockDoneKeys.Unlock()
	}
	odMgr.lockExgIdMap.Lock()
	delete(odMgr.exgIdMap, od.Symbol+orderId)
	odMgr.lockExgIdMap.Unlock()
	od.Enter.OrderID = ""
	od.Enter.Status = ormo.OdStatusInit
	od.Enter.UpdateAt = btime.UTCStamp()
	od.DirtyEnter = true
	if od.Status != ormo.InOutStatusInit && od.Enter.Filled == 0 {
		od.Status = ormo.InOutStatusInit
		od.DirtyMain = true
	}
	ormo.AddTriggerOd(odMgr.Account, od)
	log.Info("rollback enter to local trigger", zap.String("acc", odMgr.Account),
		zap.String("key", od.Key()), zap.String("orderId", orderId), zap.Int("waitSecs", waitSecs))
	return true
}

func cancelTimeoutEnter(odMgr *LiveOrderMgr, od *ormo.InOutOrder) {
	lock := od.Lock()
	defer lock.Unlock()
	if od.Enter.OrderID != "" {
		res, err := exg.Default.CancelOrder(od.Enter.OrderID, od.Symbol, map[string]interface{}{
			banexg.ParamAccount: odMgr.Account,
		})
		if err != nil {
			log.Error("cancel old limit enters fail", zap.String("key", od.Key()), zap.Error(err))
		} else {
			err = odMgr.updateOdByExgRes(od, true, res)
			if err != nil {
				log.Error("apply cancel res fail", zap.String("key", od.Key()), zap.Error(err))
			}
		}
	}
	if od.Enter.Filled == 0 {
		// Not yet filled, exit directly
		// 尚未入场，直接退出
		err := od.LocalExit(0, core.ExitTagForceExit, od.InitPrice, "reach StopEnterBars", "")
		strat.FireOdChange(odMgr.Account, od, strat.OdChgExitFill)
		if err != nil {
			log.Error("local exit for StopEnterBars fail", zap.String("key", od.Key()), zap.Error(err))
		}
	} else {
		// Partial filled, set to fully admitted
		// 部分入场，置为已完全入场
		od.Enter.Status = ormo.OdStatusClosed
		od.Status = ormo.InOutStatusFullEnter
		od.DirtyMain = true
		od.DirtyEnter = true
		odMgr.callBack(od, true)
		strat.FireOdChange(odMgr.Account, od, strat.OdChgEnterFill)
	}
}

func (o *LiveOrderMgr) editLimitOd(od *ormo.InOutOrder, action string) *errs.Error {
	subOd := od.Enter
	if action == ormo.OdActionLimitExit {
		subOd = od.Exit
	}
	exchange := exg.Default
	args := map[string]interface{}{
		banexg.ParamAccount: o.Account,
	}
	if core.Market != banexg.MarketLinear && core.Market != banexg.MarketInverse {
		// Spot, Margin, Options. Cancel the old order first, then create a new order
		// 现货，保证金，期权。先取消旧订单，再创建新订单
		_, err := exchange.CancelOrder(subOd.OrderID, od.Symbol, args)
		if err != nil {
			return err
		}
		subOd.OrderID = ""
	}
	if subOd.OrderID == "" {
		if subOd.Enter {
			return o.execOrderEnter(od)
		} else {
			return o.execOrderExit(od)
		}
	}
	// Only U-based & coin-based, modify order
	// 只有U本位 & 币本位，修改订单
	res, err := exchange.EditOrder(od.Symbol, subOd.OrderID, subOd.Side, subOd.Amount, subOd.Price, args)
	if err != nil {
		return err
	}
	return o.updateOdByExgRes(od, subOd.Enter, res)
}

func (o *LiveOrderMgr) setTrailingStop(od *ormo.InOutOrder) {
	callRate := od.GetInfoFloat64(ormo.OdInfoCallbackPct)
	if callRate <= 0 {
		// 取消跟踪止损
		oldID := od.GetInfoString(ormo.OdInfoTrailingID)
		if oldID != "" {
			_, err := exg.Default.CancelOrder(oldID, od.Symbol, map[string]interface{}{
				banexg.ParamAccount: o.Account,
			})
			if err != nil {
				log.Error("cancel old trigger fail", zap.String("key", od.Key()), zap.Error(err))
			}
			od.SetInfo(ormo.OdInfoCallbackPct, 0.0)
			od.SetInfo(ormo.OdInfoActivePrice, nil)
			od.SetInfo(ormo.OdInfoTrailingID, "")
			od.SetInfo(ormo.OdInfoTrailingBest, nil)
		}
		return
	}
	params := map[string]interface{}{
		banexg.ParamAccount:       o.Account,
		banexg.ParamClientOrderId: od.ClientId(true),
	}
	if core.IsContract {
		params[banexg.ParamPositionSide] = "LONG"
		if od.Short {
			params[banexg.ParamPositionSide] = "SHORT"
		}
	}
	// 追踪止损
	odType := banexg.OdTypeTrailingStopMarket
	activePrice := od.GetInfoFloat64(ormo.OdInfoActivePrice)
	if activePrice > 0 {
		params[banexg.ParamActivationPrice] = activePrice
	}
	params[banexg.ParamCallbackRate] = callRate
	retryNum := od.GetInfoInt64(ormo.OdInfoRetry)
	if retryNum > 0 {
		params[banexg.ParamRetry] = int(retryNum)
	}
	side := banexg.OdSideSell
	if od.Short {
		side = banexg.OdSideBuy
	}
	amt := od.Enter.Amount
	log.Debug("set trailing trigger", zap.String("acc", o.Account), zap.String("key", od.Key()),
		zap.Float64("callbackRate", callRate), zap.Float64("qty", amt),
		zap.Float64("activePrice", activePrice))
	res, err := exg.Default.CreateOrder(od.Symbol, odType, side, amt, 0, params)
	if err != nil {
		log.Error("put trigger fail", zap.String("key", od.Key()), zap.Error(err))
	} else {
		oldID := od.GetInfoString(ormo.OdInfoTrailingID)
		if oldID != "" {
			_, err = exg.Default.CancelOrder(oldID, od.Symbol, map[string]interface{}{
				banexg.ParamAccount: o.Account,
			})
			if err != nil {
				log.Error("cancel old trigger fail", zap.String("key", od.Key()), zap.Error(err))
			}
		}
		od.SetInfo(ormo.OdInfoTrailingID, res.ID)
	}
	err = od.Save()
	if err != nil {
		log.Error("save order fail", zap.String("key", od.Key()), zap.Error(err))
	}
}

func (o *LiveOrderMgr) editTriggerOd(od *ormo.InOutOrder, prefix string) {
	if od.Status >= ormo.InOutStatusFullExit {
		return
	}
	tg := od.GetExitTrigger(prefix)
	if tg == nil || tg.Old != nil && tg.Old.Equal(tg.ExitTrigger) {
		return
	}
	tg.SaveOld()
	od.DirtyInfo = true
	if tg.Price <= 0 {
		// Stop loss/take profit is not set, or needs to be cancelled
		// 未设置止损/止盈，或需要撤销
		if tg.OrderId != "" {
			_, err := exg.Default.CancelOrder(tg.OrderId, od.Symbol, map[string]interface{}{
				banexg.ParamAccount: o.Account,
			})
			if err != nil {
				log.Error("cancel old trigger fail", zap.String("key", od.Key()), zap.Error(err))
			}
			tg.OrderId = ""
			_ = od.SetExitTrigger(prefix, nil, 0)
		}
		return
	}
	params := map[string]interface{}{
		banexg.ParamAccount:       o.Account,
		banexg.ParamClientOrderId: od.ClientId(true),
	}
	if core.IsContract {
		params[banexg.ParamPositionSide] = "LONG"
		if od.Short {
			params[banexg.ParamPositionSide] = "SHORT"
		}
	}
	var odType = banexg.OdTypeMarket
	var price = tg.Price
	if tg.Limit > 0 {
		odType = banexg.OdTypeLimit
		price = tg.Limit
	}
	// 这里不应设置ClosePosition仓位止盈止损，否则多策略或多个订单止盈止损会互相覆盖
	// 双向持仓无需设置ReduceOnly
	if prefix == ormo.OdActionStopLoss {
		if tg.Tag == core.ExitTagTrailingStop {
			return
		}
		params[banexg.ParamStopLossPrice] = tg.Price
	} else if prefix == ormo.OdActionTakeProfit {
		params[banexg.ParamTakeProfitPrice] = tg.Price
	} else {
		log.Error("invalid trigger ", zap.String("prefix", prefix))
		return
	}
	side := banexg.OdSideSell
	if od.Short {
		side = banexg.OdSideBuy
	}
	amt := od.Enter.Amount
	if tg.Rate > 0 && tg.Rate < 1 {
		amt *= tg.Rate
	}
	retryNum := od.GetInfoInt64(ormo.OdInfoRetry)
	if retryNum > 0 {
		params[banexg.ParamRetry] = int(retryNum)
	}
	log.Debug("set trigger", zap.String("acc", o.Account), zap.String("key", od.Key()),
		zap.Float64("amt", od.Enter.Amount), zap.Float64("qmt", amt),
		zap.Float64("price", od.Enter.Average))
	res, err := exg.Default.CreateOrder(od.Symbol, odType, side, amt, price, params)
	if err != nil {
		if err.BizCode == -2021 {
			// Stop loss and stop profit are executed immediately, and the position is closed at the market price
			// 止损止盈立刻成交，则市价平仓
			log.Warn("Order would immediately trigger, exit", zap.String("key", od.Key()))
			exitTag := prefix
			if tg.Tag != "" {
				exitTag = tg.Tag
			}
			od.SetExit(0, exitTag, banexg.OdTypeMarket, 0)
			err = o.execOrderExit(od)
			if err != nil {
				log.Error("exit order by trigger fail", zap.String("key", od.Key()), zap.Error(err))
			}
			err = od.Save()
			if err != nil {
				log.Error("save order by trigger fail", zap.String("key", od.Key()), zap.Error(err))
			}
		} else {
			// When the update of stop loss and stop profit fails, the old stop profit and stop loss will not be cancelled
			// 更新止损止盈失败时，不取消旧的止盈止损
			log.Error("put trigger order fail", zap.String("key", od.Key()), zap.Error(err))
		}
		return
	}
	orderId := tg.OrderId
	if res != nil {
		tg.OrderId = res.ID
		od.DirtyInfo = true
	}
	if orderId != "" && (res == nil || res.Status == "open") {
		res, err = exg.Default.CancelOrder(orderId, od.Symbol, map[string]interface{}{
			banexg.ParamAccount: o.Account,
		})
		if err != nil {
			log.Error("cancel old trigger fail", zap.String("key", od.Key()), zap.Error(err))
		} else if res != nil {
			o.lockDoneKeys.Lock()
			o.doneKeys[od.Symbol+orderId] = res.Timestamp
			o.lockDoneKeys.Unlock()
		}
	}
}

func (o *LiveOrderMgr) checkTradeDone(k string) bool {
	o.lockDoneTrades.Lock()
	_, ok := o.doneTrades[k]
	o.lockDoneTrades.Unlock()
	return ok
}

func (o *LiveOrderMgr) checkOrderDone(k string) bool {
	o.lockDoneKeys.Lock()
	_, ok := o.doneKeys[k]
	o.lockDoneKeys.Unlock()
	return ok
}

/*
cancelTriggerOds
Cancel the associated order of the order. When the order is closed, the associated stop loss order and take profit order will not be automatically exited, and this method needs to be called to exit
取消订单的关联订单。订单在平仓时，关联的止损单止盈单不会自动退出，需要调用此方法退出
*/
func cancelTriggerOds(od *ormo.InOutOrder, account string) {
	sl := od.GetStopLoss()
	tp := od.GetTakeProfit()
	if sl == nil && tp == nil {
		return
	}
	odKey := od.Key()
	if account == "" {
		account = ormo.GetTaskAcc(od.TaskID)
		if account == "" {
			log.Error("cancel trigger order fail, unknown account", zap.String("key", odKey))
			return
		}
	}
	args := map[string]interface{}{
		banexg.ParamAccount: account,
	}
	var logFields []zap.Field
	if sl != nil && sl.OrderId != "" {
		_, err := exg.Default.CancelOrder(sl.OrderId, od.Symbol, args)
		if err != nil {
			log.Warn("cancel stopLoss fail", zap.String("key", odKey), zap.String("err", err.Short()))
		} else {
			logFields = append(logFields, zap.String("sl", sl.OrderId))
		}
		sl.OrderId = ""
		od.DirtyInfo = true
	}
	if tp != nil && tp.OrderId != "" {
		_, err := exg.Default.CancelOrder(tp.OrderId, od.Symbol, args)
		if err != nil {
			log.Warn("cancel takeProfit fail", zap.String("key", odKey), zap.String("err", err.Short()))
		} else {
			logFields = append(logFields, zap.String("tp", tp.OrderId))
		}
		tp.OrderId = ""
		od.DirtyInfo = true
	}
	if len(logFields) > 0 {
		logFields = append(logFields, zap.String("key", odKey))
		log.Info("cancel order triggers", logFields...)
	}
}

/*
finishOrder
sess 可为nil
When the transaction is in progress, it will be saved to the database internally.
实盘时，内部会保存到数据库
*/
func (o *LiveOrderMgr) finishOrder(od *ormo.InOutOrder) *errs.Error {
	curMS := btime.UTCStamp()
	if od.Enter != nil && od.Enter.OrderID != "" {
		o.lockDoneKeys.Lock()
		o.doneKeys[od.Symbol+od.Enter.OrderID] = curMS
		o.lockDoneKeys.Unlock()
	}
	if od.Exit != nil && od.Exit.OrderID != "" {
		o.lockDoneKeys.Lock()
		o.doneKeys[od.Symbol+od.Exit.OrderID] = curMS
		o.lockDoneKeys.Unlock()
	}
	log.Info("Finish Order", zap.String("acc", o.Account), zap.String("key", od.Key()),
		zap.String("tag", od.ExitTag))
	return o.OrderMgr.finishOrder(od)
}

func (o *LiveOrderMgr) WatchLeverages() {
	if !core.IsContract || o.isWatchAccConfig {
		return
	}
	if !exg.Default.HasApi(banexg.ApiWatchAccountConfig, core.Market) {
		return
	}
	out, err := exg.Default.WatchAccountConfig(map[string]interface{}{
		banexg.ParamAccount: o.Account,
	})
	if err != nil {
		log.Error("WatchLeverages error", zap.Error(err))
		return
	}
	o.isWatchAccConfig = true
	go func() {
		defer func() {
			o.isWatchAccConfig = false
		}()
		for range out {
			continue
		}
	}()
}

/*
CheckFatalStop
Check if the global stop loss is triggered. This method should be called regularly via cron
检查是否触发全局止损，此方法应通过cron定期调用
*/
func MakeCheckFatalStop(maxIntv int) func() {
	return func() {
		for account, cfg := range config.Accounts {
			if cfg.NoTrade {
				continue
			}
			checkAccFatalStop(account, maxIntv)
		}
	}
}

func checkAccFatalStop(account string, maxIntv int) {
	stopUntil, _ := core.NoEnterUntil[account]
	if stopUntil >= btime.TimeMS() {
		return
	}
	sess, conn, err := ormo.Conn(orm.DbTrades, false)
	if err != nil {
		log.Error("get db sess fail", zap.Error(err))
		return
	}
	defer conn.Close()
	minTimeMS := btime.TimeMS() - int64(maxIntv)*60000
	taskId := ormo.GetTaskID(account)
	orders, err := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID:     taskId,
		Status:     2,
		CloseAfter: minTimeMS,
	})
	if err != nil {
		log.Error("get cur his orders fail", zap.Error(err))
		return
	}
	wallets := GetWallets(account)
	for backMins, rate := range config.FatalStop {
		lossRate := calcFatalLoss(wallets, orders, backMins)
		if lossRate >= rate {
			lossPct := int(lossRate * 100)
			core.NoEnterUntil[account] = btime.TimeMS() + int64(config.FatalStopHours)*3600*1000
			log.Error(fmt.Sprintf("%v: Loss of %v%% in %v minutes, prohibition of placing orders for %v hours!", account,
				lossPct, backMins, config.FatalStopHours))
			break
		}
	}
}

/*
calcFatalLoss
Calculate the percentage of account balance loss in the last n minutes at the system level
计算系统级别最近n分钟内，账户余额损失百分比
*/
func calcFatalLoss(wallets *BanWallets, orders []*ormo.InOutOrder, backMins int) float64 {
	minTimeMS := btime.TimeMS() - int64(backMins)*60000
	minTimeMS = min(minTimeMS, core.StartAt)
	sumProfit := float64(0)
	for i := len(orders) - 1; i >= 0; i-- {
		od := orders[i]
		if od.RealEnterMS() < minTimeMS {
			break
		}
		sumProfit += od.Profit
	}
	if sumProfit >= 0 {
		return 0
	}
	lossVal := math.Abs(sumProfit)
	totalLegal := wallets.TotalLegal(nil, false)
	return lossVal / (lossVal + totalLegal)
}

func (o *LiveOrderMgr) OnEnvEnd(bar *banexg.PairTFKline, adj *orm.AdjInfo) *errs.Error {
	_, err := o.ExitOpenOrders(bar.Symbol, &strat.ExitReq{
		Tag:  core.ExitTagEnvEnd,
		Dirt: core.OdDirtBoth,
	})
	return err
}

func (o *LiveOrderMgr) ExitAndFill(orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
	for _, od := range orders {
		_, err := o.exitOrder(od, req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *LiveOrderMgr) CleanUp() *errs.Error {
	return nil
}

func StartLiveOdMgr() {
	if !core.EnvReal {
		panic("StartLiveOdMgr for FakeEnv is forbidden:" + core.RunEnv)
	}
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		odMgr := GetLiveOdMgr(account)
		// Monitor account order flow 监听账户订单流
		odMgr.WatchMyTrades()
		// Track user orders 跟踪用户下单
		odMgr.TrialUnMatchesForever()
		// Consumption order queue 消费订单队列
		odMgr.ConsumeOrderQueue()
		// Monitor leverage changes 监听杠杆倍数变化
		odMgr.WatchLeverages()
	}
}

func logPutOrder(arg *exg.PutOrderRes) *errs.Error {
	if arg.Err != nil {
		return arg.Err
	}
	orm.AddDumpRow(orm.DumpApiOrder, arg.Symbol+arg.Order.ID, arg)
	return nil
}
