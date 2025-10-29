package biz

import (
	"fmt"
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
	"math"
	"slices"
	"strings"
)

var (
	accOdMgrs     = make(map[string]IOrderMgr)
	accLiveOdMgrs = make(map[string]*LiveOrderMgr)
)

type IOrderMgr interface {
	ProcessOrders(sess *ormo.Queries, job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error)
	EditOrder(od *ormo.InOutOrder, action string)
	RelayOrders(sess *ormo.Queries, orders []*ormo.InOutOrder) *errs.Error
	EnterOrder(sess *ormo.Queries, exs *orm.ExSymbol, tf string, req *strat.EnterReq) (*ormo.InOutOrder, *errs.Error)
	ExitOpenOrders(sess *ormo.Queries, pairs string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error)
	ExitOrder(sess *ormo.Queries, od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error)
	UpdateByBar(allOpens []*ormo.InOutOrder, bar *orm.InfoKline) *errs.Error
	ExitAndFill(sess *ormo.Queries, orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error
	OnEnvEnd(bar *banexg.PairTFKline, adj *orm.AdjInfo) *errs.Error
	CleanUp() *errs.Error
}

type IOrderMgrLive interface {
	IOrderMgr
	SyncExgOrders() ([]*ormo.InOutOrder, []*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error)
	WatchMyTrades()
	TrialUnMatchesForever()
	ConsumeOrderQueue()
}

type FuncHandleIOrder = func(order *ormo.InOutOrder) *errs.Error

type OrderMgr struct {
	callBack    func(order *ormo.InOutOrder, isEnter bool)
	afterEnter  FuncHandleIOrder
	afterExit   FuncHandleIOrder
	Account     string
	BarMS       int64
	simulOpen   int // Simultaneously open number in the current bar
	simulOpenSt map[string]int
}

func GetOdMgr(account string) IOrderMgr {
	if !core.EnvReal {
		account = config.DefAcc
	}
	val, _ := accOdMgrs[account]
	return val
}

func GetAllOdMgr() map[string]IOrderMgr {
	var result = make(map[string]IOrderMgr)
	if core.EnvReal {
		for acc, mgr := range accLiveOdMgrs {
			result[acc] = mgr
		}
	} else {
		for acc, mgr := range accOdMgrs {
			result[acc] = mgr
		}
	}
	return result
}

func GetLiveOdMgr(account string) *LiveOrderMgr {
	if !core.EnvReal {
		panic("call GetLiveOdMgr in FakeEnv is forbidden: " + core.RunEnv)
	}
	val, _ := accLiveOdMgrs[account]
	return val
}

func CleanUpOdMgr() *errs.Error {
	var err *errs.Error
	for account := range config.Accounts {
		var curErr *errs.Error
		if core.EnvReal {
			if mgr, ok := accLiveOdMgrs[account]; ok {
				curErr = mgr.CleanUp()
			}
		} else {
			if mgr, ok := accOdMgrs[account]; ok {
				curErr = mgr.CleanUp()
			}
		}
		if curErr != nil {
			if err != nil {
				log.Error("clean odMgr fail", zap.String("acc", account), zap.Error(curErr))
			} else {
				err = curErr
			}
		}
	}
	return err
}

func (o *OrderMgr) allowOrderEnter(exs *orm.ExSymbol, tf string, enters []*strat.EnterReq) ([]*strat.EnterReq, map[string]int) {
	curMS := btime.TimeMS()
	rawNum := len(enters)
	if banUntil, ok := core.BanPairsUntil[exs.Symbol]; ok {
		if curMS < banUntil {
			return nil, map[string]int{"BanPair": rawNum}
		} else {
			delete(core.BanPairsUntil, exs.Symbol)
		}
	}
	if core.RunMode == core.RunModeOther {
		// Does not involve order mode, prohibit opening orders
		// 不涉及订单模式，禁止开单
		return nil, map[string]int{"NoOrderMode": rawNum}
	}
	pairZapField := zap.String("pair", exs.Symbol)
	stopUntil, _ := core.NoEnterUntil[o.Account]
	if curMS < stopUntil {
		if core.LiveMode {
			log.Warn("any enter forbid", pairZapField)
		}
		strat.AddAccFailOpens(o.Account, strat.FailOpenNoEntry, len(enters))
		return nil, map[string]int{"AccNoEntry": rawNum}
	}
	tfMSecs := int64(utils.TFToSecs(tf) * 1000)
	barStopMS := utils.AlignTfMSecs(curMS, tfMSecs)
	if o.BarMS < barStopMS {
		o.BarMS = barStopMS
		o.simulOpen = 0
		o.simulOpenSt = make(map[string]int)
	}
	maxOpenNum := config.MaxOpenOrders
	acc, _ := config.Accounts[o.Account]
	if acc != nil && acc.MaxOpenOrders > 0 {
		maxOpenNum = acc.MaxOpenOrders
	}
	orgNum := len(enters)
	enters = checkOrderNum(enters, orgNum, maxOpenNum, "max_open_orders")
	if len(enters) > 0 && config.MaxSimulOpen > 0 {
		enters = checkOrderNum(enters, o.simulOpen, config.MaxSimulOpen, "max_simul_open")
	}
	if orgNum > len(enters) {
		strat.AddAccFailOpens(o.Account, strat.FailOpenNumLimit, orgNum-len(enters))
	}
	if len(enters) == 0 {
		return nil, map[string]int{"OpenTooMuch": rawNum}
	}
	numCut := rawNum - len(enters)
	tagMap := map[string]int{}
	if numCut > 0 {
		tagMap["OpenTooMuch"] = numCut
	}
	// Check whether the maximum number of orders opened by the strategy is exceeded
	// 检查是否超出策略最大开单数量
	openOds, lock := ormo.GetOpenODs(o.Account)
	lock.Lock()
	stratOdNum := make(map[string]int)
	for _, od := range openOds {
		num, _ := stratOdNum[od.Strategy]
		stratOdNum[od.Strategy] = num + 1
	}
	lock.Unlock()
	skipNum := 0
	res := make([]*strat.EnterReq, 0, len(enters))
	for _, req := range enters {
		num, _ := stratOdNum[req.StratName]
		simulNum, _ := o.simulOpenSt[req.StratName]
		pol := strat.Get(exs.Symbol, req.StratName).Policy
		if pol != nil {
			if pol.MaxOpen > 0 && num >= pol.MaxOpen {
				skipNum += 1
				continue
			}
			if pol.MaxSimulOpen > 0 && simulNum >= pol.MaxSimulOpen {
				skipNum += 1
				continue
			}
		}
		stratOdNum[req.StratName] = num + 1
		o.simulOpenSt[req.StratName] = simulNum + 1
		o.simulOpen += 1
		res = append(res, req)
	}
	if skipNum > 0 {
		strat.AddAccFailOpens(o.Account, strat.FailOpenNumLimitPol, skipNum)
	}
	numCut = rawNum - len(enters)
	if numCut > 0 {
		tagMap["OpenTooMuch"] = numCut
	}
	return res, tagMap
}

func checkOrderNum(enters []*strat.EnterReq, oldNum, maxNum int, tag string) []*strat.EnterReq {
	cutNum := oldNum + len(enters) - maxNum
	if maxNum > 0 && cutNum > 0 {
		if maxNum > oldNum {
			enters = enters[:maxNum-oldNum]
			if core.LiveMode {
				log.Warn("cut enters by", zap.String("tag", tag),
					zap.Int("left", len(enters)), zap.Int("cut", cutNum))
			}
		} else {
			enters = nil
			if core.LiveMode {
				log.Warn("skip enters by", zap.String("tag", tag), zap.Int("cut", cutNum))
			}
		}
	}
	return enters
}

/*
ProcessOrders
Execute order entry and exit requests
Create pending orders, the returned orders are not actually entered or exited;
Backtest: the caller executes the entry/exit order according to the next bar and updates the status
Live trading: monitor the exchange to return the order status to update the entry and exit
执行订单入场出场请求
创建待执行订单，返回的订单实际并未入场或出场；
回测：调用方根据下一个bar执行入场/出场订单，更新状态
实盘：监听交易所返回订单状态更新入场出场
*/
func (o *OrderMgr) ProcessOrders(sess *ormo.Queries, job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	enters, exits := job.Entrys, job.Exits
	if len(enters) == 0 && len(exits) == 0 {
		return nil, nil, nil
	}
	job.Entrys = nil
	job.Exits = nil
	exs := job.Symbol
	var entOrders, extOrders []*ormo.InOutOrder
	if len(enters) > 0 {
		rawNum := len(enters)
		var reasons map[string]int
		enters, reasons = o.allowOrderEnter(exs, job.TimeFrame, enters)
		if core.LiveMode && len(enters) < rawNum {
			log.Info("skip enters by allowOrderEnter", zap.Any("tags", reasons))
		}
		for _, ent := range enters {
			iorder, err := o.enterOrder(sess, exs, job.TimeFrame, ent, false)
			if err != nil {
				return entOrders, extOrders, err
			}
			entOrders = append(entOrders, iorder)
		}
	}
	if len(exits) > 0 {
		for _, exit := range exits {
			iorders, err := o.ExitOpenOrders(sess, exs.Symbol, exit)
			if err != nil {
				return entOrders, extOrders, err
			}
			extOrders = append(extOrders, iorders...)
		}
	}
	if job.Strat.OnOrderChange != nil && (len(entOrders) > 0 || len(extOrders) > 0) {
		for _, od := range entOrders {
			job.Strat.OnOrderChange(job, od, strat.OdChgEnter)
		}
		for _, od := range extOrders {
			job.Strat.OnOrderChange(job, od, strat.OdChgExit)
		}
		if len(job.Entrys) > 0 || len(job.Exits) > 0 {
			ents, exts, err := o.ProcessOrders(sess, job)
			if err != nil {
				return entOrders, extOrders, err
			}
			entOrders = append(entOrders, ents...)
			extOrders = append(extOrders, exts...)
		}
	}
	return entOrders, extOrders, nil
}

func (o *LocalOrderMgr) EditOrder(od *ormo.InOutOrder, action string) {

}

func (o *OrderMgr) RelayOrders(sess *ormo.Queries, orders []*ormo.InOutOrder) *errs.Error {
	symbolMap := orm.GetExSymbolMap(core.ExgName, core.Market)
	taskId := ormo.GetTaskID(o.Account)
	for _, odr := range orders {
		exs, ok := symbolMap[odr.Symbol]
		if !ok {
			return errs.NewMsg(errs.CodeNoMarketForPair, "%s not found", odr.Symbol)
		}
		price := com.GetPriceExp(odr.Symbol, odr.Enter.Side, com.Day10MSecs)
		curTime := btime.TimeMS()
		od := &ormo.InOutOrder{
			IOrder: &ormo.IOrder{
				TaskID:    taskId,
				Symbol:    odr.Symbol,
				Sid:       int64(exs.ID),
				Timeframe: odr.Timeframe,
				Short:     odr.Short,
				Status:    odr.Status,
				EnterTag:  odr.EnterTag,
				InitPrice: odr.InitPrice,
				Stop:      odr.Stop,
				QuoteCost: odr.QuoteCost,
				ExitTag:   odr.ExitTag,
				Leverage:  odr.Leverage,
				EnterAt:   odr.EnterAt,
				ExitAt:    odr.ExitAt,
				Strategy:  odr.Strategy,
				StgVer:    odr.StgVer,
				Info:      odr.IOrder.Info,
				// ignore: MaxPftRate,MaxDrawDown,Profit,ProfitRate
			},
			Enter: &ormo.ExOrder{
				TaskID:    taskId,
				Symbol:    odr.Symbol,
				Enter:     true,
				OrderType: odr.Enter.OrderType,
				//OrderID:   odr.Enter.OrderID,
				Side:     odr.Enter.Side,
				CreateAt: curTime,
				UpdateAt: curTime,
				Price:    price,
				Amount:   odr.Enter.Amount,
				Status:   ormo.OdStatusInit,
			},
			Info:       make(map[string]interface{}),
			DirtyMain:  true,
			DirtyEnter: true,
		}
		if odr.Exit != nil && odr.Exit.Filled > 0 {
			od.Enter.Amount -= odr.Exit.Filled
			od.QuoteCost = od.Enter.Price * od.Enter.Amount
		}
		if len(odr.Info) > 0 {
			maps.Copy(od.Info, odr.Info)
		}
		err := od.Save(sess)
		if err == nil {
			if o.afterEnter != nil {
				err = o.afterEnter(od)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *OrderMgr) EnterOrder(sess *ormo.Queries, exs *orm.ExSymbol, tf string, req *strat.EnterReq) (*ormo.InOutOrder, *errs.Error) {
	return o.enterOrder(sess, exs, tf, req, true)
}

func (o *OrderMgr) enterOrder(sess *ormo.Queries, exs *orm.ExSymbol, tf string, req *strat.EnterReq, doCheck bool) (*ormo.InOutOrder, *errs.Error) {
	isSpot := core.Market == banexg.MarketSpot
	if req.Short && isSpot {
		return nil, errs.NewMsg(core.ErrRunTime, "short oder is invalid for spot")
	}
	if doCheck {
		enters, reasons := o.allowOrderEnter(exs, tf, []*strat.EnterReq{req})
		if len(enters) == 0 {
			log.Warn("skip enter by allowOrderEnter", zap.Any("reasons", reasons))
			return nil, nil
		}
	}
	if req.Leverage == 0 {
		req.Leverage = 1
		if !isSpot {
			exchange := exg.Default
			exInfo := exchange.Info()
			if exInfo.FixedLvg {
				req.Leverage, _ = exchange.GetLeverage(exs.Symbol, 0, o.Account)
			} else {
				req.Leverage = config.GetAccLeverage(o.Account)
			}
		}
	}
	stgVer, _ := strat.Versions[req.StratName]
	odSide := banexg.OdSideBuy
	if req.Short {
		odSide = banexg.OdSideSell
	}
	price := com.GetPriceSafe(exs.Symbol, odSide)
	if price < 0 {
		return nil, errs.NewMsg(errs.CodeRunTime, "no valid price: %v", exs.Symbol)
	}
	enterPrice := price
	if !req.Short && req.Stop > price || req.Short && req.Stop < price && req.Stop > 0 {
		enterPrice = req.Stop
	} else if !req.Short && req.Limit > 0 && req.Limit < price || req.Short && req.Limit > price {
		enterPrice = req.Limit
	}
	curTimeMS := btime.TimeMS()
	taskId := ormo.GetTaskID(o.Account)
	od := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			TaskID:    taskId,
			Symbol:    exs.Symbol,
			Sid:       int64(exs.ID),
			Timeframe: tf,
			Short:     req.Short,
			Status:    ormo.InOutStatusInit,
			EnterTag:  req.Tag,
			InitPrice: enterPrice,
			Stop:      req.Stop,
			Leverage:  req.Leverage,
			EnterAt:   curTimeMS,
			Strategy:  req.StratName,
			StgVer:    int64(stgVer),
		},
		Enter: &ormo.ExOrder{
			TaskID:    taskId,
			Symbol:    exs.Symbol,
			Enter:     true,
			OrderType: core.OrderTypeEnums[req.OrderType],
			Side:      odSide,
			Price:     req.Limit,
			Amount:    req.Amount,
			Status:    ormo.OdStatusInit,
			CreateAt:  curTimeMS,
			UpdateAt:  curTimeMS,
		},
		Info:       map[string]interface{}{},
		DirtyMain:  true,
		DirtyEnter: true,
	}
	if od.Enter.OrderType == "" {
		od.Enter.OrderType = config.OrderType
	}
	if req.Limit > 0 {
		od.InitPrice = req.Limit
		if req.StopBars == 0 {
			req.StopBars = config.StopEnterBars
		}
		if req.StopBars > 0 {
			stopAfter := btime.TimeMS() + int64(req.StopBars*utils.TFToSecs(od.Timeframe))*1000
			od.SetInfo(ormo.OdInfoStopAfter, stopAfter)
		}
	}
	od.SetInfo(ormo.OdInfoLegalCost, req.LegalCost)
	if req.StopLoss > 0 {
		err := od.SetExitTrigger(ormo.OdInfoStopLoss, &ormo.ExitTrigger{
			Price: req.StopLoss,
			Limit: req.StopLossLimit,
			Rate:  req.StopLossRate,
			Tag:   req.StopLossTag,
		}, enterPrice)
		if err != nil {
			return od, err
		}
	}
	if req.TakeProfit > 0 {
		err := od.SetExitTrigger(ormo.OdInfoTakeProfit, &ormo.ExitTrigger{
			Price: req.TakeProfit,
			Limit: req.TakeProfitLimit,
			Rate:  req.TakeProfitRate,
			Tag:   req.TakeProfitTag,
		}, enterPrice)
		if err != nil {
			return od, err
		}
	}
	if req.ClientID != "" {
		od.SetInfo(ormo.OdInfoClientID, req.ClientID)
	}
	if len(req.Infos) > 0 {
		for k, v := range req.Infos {
			od.SetInfo(k, v)
		}
	}
	err := od.Save(sess)
	if err != nil {
		return od, err
	}
	if o.afterEnter != nil {
		err = o.afterEnter(od)
	}
	return od, err
}

func (o *OrderMgr) ExitOpenOrders(sess *ormo.Queries, pairs string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error) {
	// Filter matching orders 筛选匹配的订单
	var matches []*ormo.InOutOrder
	openOds, lock := ormo.GetOpenODs(o.Account)
	if req.OrderID > 0 {
		// Specify the exact order ID to exit 精确指定退出的订单ID
		lock.Lock()
		od, ok := openOds[req.OrderID]
		lock.Unlock()
		if !ok {
			return nil, errs.NewMsg(errs.CodeParamInvalid, "req orderId not found: %d", req.OrderID)
		}
		matches = append(matches, od)
	} else {
		parts := strings.Split(pairs, ",")
		pairMap := make(map[string]bool)
		for _, p := range parts {
			if p == "" {
				continue
			}
			pairMap[p] = true
		}
		dirtBoth := req.Dirt == core.OdDirtBoth
		isShort := req.Dirt == core.OdDirtShort
		lock.Lock()
		for _, od := range openOds {
			if req.StratName != "" && od.Strategy != req.StratName {
				continue
			}
			if len(pairMap) > 0 {
				if _, ok := pairMap[od.Symbol]; !ok {
					continue
				}
			}
			if !dirtBoth && isShort != od.Short {
				continue
			}
			if req.EnterTag != "" && od.EnterTag != req.EnterTag {
				continue
			}
			if od.ExitTag != "" || (od.Exit != nil && od.Exit.Amount > 0) {
				// Order Exited 订单已退出
				continue
			}
			if req.UnFillOnly && od.Enter.Filled >= od.Enter.Amount {
				continue
			}
			if req.FilledOnly && od.Enter.Filled < core.AmtDust {
				continue
			}
			matches = append(matches, od)
		}
		lock.Unlock()
	}
	if len(matches) == 0 {
		if core.LiveMode {
			fields := req.GetZapFields(nil, zap.String("acc", o.Account), zap.String("pair", pairs),
				zap.Int("all", len(openOds)))
			log.Warn("no match orders to exit", fields...)
		}
		return nil, nil
	}
	var exitAmount float64
	useRate := req.ExitRate > 0 && req.ExitRate < 1
	if useRate || req.Amount <= 0 {
		// Calculate the amount to withdraw 计算要退出的数量
		allAmount := float64(0)
		for _, od := range matches {
			allAmount += od.Enter.Amount
			if od.Exit != nil {
				allAmount -= od.Exit.Amount
			}
		}
		exitAmount = allAmount
		if useRate {
			exitAmount = allAmount * req.ExitRate
		}
	} else {
		exitAmount = req.Amount
	}
	isTakeProfit := false
	if req.Limit > 0 && core.IsLimitOrder(req.OrderType) {
		symbol := matches[0].Symbol
		for _, od := range matches[1:] {
			if od.Symbol != symbol {
				return nil, errs.NewMsg(errs.CodeParamInvalid, "ExitReq.Limit invalid for multi pairs")
			}
		}
		odSide := ""
		if req.Dirt == core.OdDirtLong {
			odSide = banexg.OdSideSell
		} else if req.Dirt == core.OdDirtShort {
			odSide = banexg.OdSideBuy
		}
		price := com.GetPriceExp(symbol, odSide, com.Day10MSecs)
		if price > 0 && (req.Limit-price)*float64(req.Dirt) > 0 {
			isTakeProfit = true
		}
	}
	slices.SortFunc(matches, func(a, b *ormo.InOutOrder) int {
		fillA := a.Enter.Filled * a.InitPrice
		fillB := b.Enter.Filled * b.InitPrice
		fillChg := int(math.Round((fillA - fillB) * 100))
		// For profit taking or filled only, descending order by filled amount.
		// 对于止盈或退出已入场的，优先按已入场金额降序
		if (isTakeProfit || req.FilledOnly) && fillChg != 0 {
			// 止盈单，优先按入场金额倒序
			return -fillChg
		}
		costA := a.Enter.Amount * a.InitPrice
		unfillA := costA - fillA
		costB := b.Enter.Amount * b.InitPrice
		unfillB := costB - fillB
		// First, in descending order by unsold amount. 首先按未成交金额倒序
		res := int(math.Round((unfillB - unfillA) * 100))
		if res != 0 {
			return res
		}
		// Secondly, in ascending order by deposit amount 其次按已入场金额升序
		if fillChg != 0 {
			return res
		}
		// Last entry time ascending 最后按入场时间升序
		return int((a.RealEnterMS() - b.RealEnterMS()) / 1000)
	})
	var result []*ormo.InOutOrder
	var part *ormo.InOutOrder
	var err *errs.Error
	for i, od := range matches {
		if !req.Force && !od.CanClose() {
			continue
		}
		dust := od.Enter.Amount * 0.01
		if exitAmount < dust {
			if isTakeProfit {
				// reset TakeProfit for remaining orders
				// 剩余订单重置TakeProfit
				for _, odr := range matches[i:] {
					err = odr.SetTakeProfit(nil)
					if err != nil {
						return result, err
					}
					_, err = o.postOrderExit(sess, odr)
					if err != nil {
						return result, err
					}
				}
			}
			break
		}
		if req.FilledOnly && od.Enter.Filled < od.Enter.Amount {
			// Only exit the entered orders, the current order is partially entered and divided into sub-orders
			// 只退出已入场的订单，当前订单部分入场，切分成子订单
			cutAmt := min(exitAmount, od.Enter.Filled)
			part = od.CutPart(cutAmt, 0)
			err = od.Save(sess)
			if err != nil {
				return result, err
			}
			od = part
		}
		q := req.Clone()
		q.ExitRate = min(1, exitAmount/od.Enter.Amount)
		if isTakeProfit && od.Status >= ormo.InOutStatusPartEnter {
			err = od.SetTakeProfit(&ormo.ExitTrigger{
				Price: q.Limit,
				Limit: q.Limit,
				Rate:  q.ExitRate,
				Tag:   q.Tag,
			})
			if err != nil {
				return result, err
			}
			part, err = o.postOrderExit(sess, od)
		} else {
			part, err = o.exitOrder(sess, od, q)
		}
		if err != nil {
			return result, err
		}
		if part != nil {
			exitAmount -= part.Enter.Amount * q.ExitRate
			result = append(result, part)
		}
	}
	return result, nil
}

func (o *OrderMgr) ExitOrder(sess *ormo.Queries, od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	if od.ExitTag != "" || (od.Exit != nil && od.Exit.Amount > 0) {
		// Exit一旦有值，表示全部退出
		return nil, nil
	}
	if req.Dirt != 0 && (req.Dirt < 0) != od.Short {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "`ExitReq.Dirt` mismatch with Order")
	}
	if req.Limit > 0 && core.IsLimitOrder(req.OrderType) {
		odSide := ""
		if req.Dirt == core.OdDirtLong {
			odSide = banexg.OdSideSell
		} else if req.Dirt == core.OdDirtShort {
			odSide = banexg.OdSideBuy
		}
		price := com.GetPriceExp(od.Symbol, odSide, com.Day10MSecs)
		if price > 0 && (req.Limit-price)*float64(req.Dirt) > 0 {
			// It is a valid limit order, set to take profit
			// 是有效的限价出场单，设置到止盈中
			_ = od.SetTakeProfit(&ormo.ExitTrigger{
				Price: req.Limit,
				Rate:  req.ExitRate,
				Tag:   req.Tag,
			})
			return o.postOrderExit(sess, od)
		}
	}
	return o.exitOrder(sess, od, req)
}

func (o *OrderMgr) exitOrder(sess *ormo.Queries, od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	// It has been confirmed externally that it is not a limit price stop profit
	// 外部已确认不是限价止盈
	odType := core.OrderTypeEnums[req.OrderType]
	if odType == "" {
		odType = config.OrderType
	}
	if req.ExitRate < 0.99 && req.ExitRate > 0 {
		// The portion to be exited is less than 99%, so a small order is split out for exit.
		// 要退出的部分不足99%，分割出一个小订单，用于退出。
		part := o.CutOrder(od, req.ExitRate, 0)
		req.ExitRate = 1
		err := od.Save(sess)
		if err != nil {
			log.Error("save cutPart parent order fail", zap.String("key", od.Key()), zap.Error(err))
		}
		return o.exitOrder(sess, part, req)
	}
	od.SetExit(0, req.Tag, odType, req.Limit)
	return o.postOrderExit(sess, od)
}

func (o *OrderMgr) postOrderExit(sess *ormo.Queries, od *ormo.InOutOrder) (*ormo.InOutOrder, *errs.Error) {
	err := od.Save(sess)
	if err != nil {
		return od, err
	}
	if o.afterExit != nil {
		err = o.afterExit(od)
	}
	return od, err
}

/*
UpdateByBar
Use the price to update the profit of the order, etc. It may trigger a margin call
使用价格更新订单的利润等。可能会触发爆仓
*/
func (o *OrderMgr) UpdateByBar(allOpens []*ormo.InOutOrder, bar *orm.InfoKline) *errs.Error {
	for _, od := range allOpens {
		if od.Symbol != bar.Symbol || od.Timeframe != bar.TimeFrame || od.Status >= ormo.InOutStatusFullExit {
			continue
		}
		od.UpdateProfits(bar.Close)
	}
	return nil
}

func (o *OrderMgr) CutOrder(od *ormo.InOutOrder, enterRate, exitRate float64) *ormo.InOutOrder {
	part := od.CutPart(od.Enter.Amount*enterRate, od.Enter.Amount*exitRate)
	// Here the key of part is the same as the original one, so part is used as src_key
	// 这里part的key和原始的一样，所以part作为src_key
	tgtKey, srcKey := od.Key(), part.Key()
	base, quote, _, _ := core.SplitSymbol(od.Symbol)
	wallets := GetWallets(o.Account)
	wallets.CutPart(srcKey, tgtKey, base, 1-enterRate)
	wallets.CutPart(srcKey, tgtKey, quote, 1-enterRate)
	return part
}

/*
finishOrder
sess 可为nil
It will be saved internally to the database during the actual trading.
实盘时内部会保存到数据库。
*/
func (o *OrderMgr) finishOrder(od *ormo.InOutOrder, sess *ormo.Queries) *errs.Error {
	od.UpdateProfits(0)
	err := od.Save(sess)
	cfg := strat.GetStratPerf(od.Symbol, od.Strategy)
	if cfg != nil && cfg.Enable && o.Account == config.DefAcc {
		err2 := strat.CalcJobScores(od.Symbol, od.Timeframe, od.Strategy)
		if err2 != nil {
			log.Error("calc job performance fail", zap.Error(err2),
				zap.Strings("job", []string{od.Symbol, od.Timeframe, od.Strategy}))
		}
	}
	return err
}

func (o *OrderMgr) CleanUp() *errs.Error {
	return nil
}

func CloseAccOrders(acc string, odList []*ormo.InOutOrder, req *strat.ExitReq) (int, int, *errs.Error) {
	var odMgr IOrderMgr
	if core.EnvReal {
		odMgr = GetLiveOdMgr(acc)
	} else {
		odMgr = GetOdMgr(acc)
	}

	sess, conn, err := ormo.Conn(orm.DbTrades, true)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	closeNum, failNum := 0, 0
	var errMsg strings.Builder
	for _, od := range odList {
		r := req.Clone()
		r.StratName = od.Strategy
		r.OrderID = od.ID
		_, err2 := odMgr.ExitOrder(sess, od, r)
		if err2 != nil {
			failNum += 1
			errMsg.WriteString(fmt.Sprintf("Order %v: %v\n", od.ID, err2.Short()))
		} else {
			closeNum += 1
		}
	}
	if failNum > 0 {
		return closeNum, failNum, errs.NewMsg(errs.CodeRunTime, errMsg.String())
	}
	return closeNum, failNum, nil
}
