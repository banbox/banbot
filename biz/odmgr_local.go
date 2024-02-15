package biz

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strategy"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

const (
	netCost = 3
)

type LocalOrderMgr struct {
	OrderMgr
}

func NewLocalOrderMgr(callBack func(od *orm.InOutOrder, isEnter bool)) *LocalOrderMgr {
	return &LocalOrderMgr{
		OrderMgr{
			wallet:   Wallets,
			data:     data.Main,
			callBack: callBack,
		},
	}
}

func (o *LocalOrderMgr) UpdateByBar(allOpens []*orm.InOutOrder, bar *banexg.PairTFKline) *errs.Error {
	err := o.OrderMgr.UpdateByBar(allOpens, bar)
	if err != nil {
		return err
	}
	if len(allOpens) == 0 || core.ProdMode() {
		return nil
	}
	if banexg.IsContract(core.Market) {
		// 为合约更新此定价币的所有订单保证金和钱包情况
		_, _, code, _ := utils2.SplitSymbol(bar.Symbol)
		var orders []*orm.InOutOrder
		for _, od := range allOpens {
			_, _, odSettle, _ := utils2.SplitSymbol(od.Symbol)
			if odSettle == code {
				orders = append(orders, od)
			}
		}
		err = Wallets.UpdateOds(orders)
		if err != nil {
			return err
		}
	}
	var orders []*orm.InOutOrder
	for _, od := range allOpens {
		if od.Symbol == bar.Symbol {
			orders = append(orders, od)
		}
	}
	_, err = o.fillPendingOrders(orders, bar)
	return err
}

/*
fillPendingOrders
填充等待交易所响应的订单。不可用于实盘；可用于回测、模拟实盘等。
*/
func (o *LocalOrderMgr) fillPendingOrders(orders []*orm.InOutOrder, bar *banexg.PairTFKline) (int, *errs.Error) {
	affectNum := 0
	for _, od := range orders {
		if bar != nil && bar.TimeFrame != od.Timeframe {
			continue
		}
		var exOrder *orm.ExOrder
		if od.ExitTag != "" && od.Exit != nil && od.Exit.Status < orm.OdStatusClosed {
			exOrder = od.Exit
		} else if od.Enter.Status < orm.OdStatusClosed {
			exOrder = od.Enter
		} else {
			if od.ExitTag != "" {
				// 已入场完成，尚未出现出场信号，检查是否触发止损
				o.tryFillTriggers(od, &bar.Kline)
			}
			continue
		}
		odType := config.OrderType
		if exOrder.OrderType != "" {
			odType = exOrder.OrderType
		}
		price := exOrder.Price
		if bar == nil {
			price = core.GetPrice(od.Symbol)
		} else if odType == banexg.OdTypeLimit && exOrder.Price > 0 {
			if exOrder.Side == banexg.OdSideBuy {
				if price < bar.Low {
					continue
				} else if price > bar.Open {
					// 买价高于市价，以市价成交
					price = bar.Open
				}
			} else if exOrder.Side == banexg.OdSideSell {
				if price > bar.High {
					continue
				} else if price < bar.Open {
					// 卖价低于市价，以市价成交
					price = bar.Open
				}
			}
		} else {
			// 按网络延迟，模拟成交价格，和开盘价接近
			rate := float64(netCost) / float64(utils2.TFToSecs(od.Timeframe))
			price = o.simMarketPrice(&bar.Kline, rate)
		}
		var err *errs.Error
		if exOrder.Enter {
			err = o.fillPendingEnter(od, price)
		} else {
			err = o.fillPendingExit(od, price)
		}
		if err != nil {
			return 0, err
		}
		affectNum += 1
	}
	return affectNum, nil
}

func (o *LocalOrderMgr) fillPendingEnter(od *orm.InOutOrder, price float64) *errs.Error {
	_, err := Wallets.EnterOd(od)
	if err != nil {
		if err.Code == core.ErrLowFunds {
			od.LocalExit(core.ExitTagForceExit, 0, err.Error())
			o.onLowFunds()
			return nil
		}
		return err
	}
	exchange, err := exg.Get()
	if err != nil {
		return err
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
		if od.Short && !banexg.IsContract(core.Market) {
			// 现货空单，必须给定数量
			return errs.NewMsg(core.ErrInvalidCost, "EnterAmount is required")
		}
		entAmount := od.QuoteCost / entPrice
		exOrder.Amount, err = exchange.PrecAmount(market, entAmount)
		if err != nil {
			log.Warn("prec enter amount fail", zap.Float64("amt", entAmount), zap.Error(err))
			od.LocalExit(core.ExitTagFatalErr, 0, err.Error())
			_, quote, _, _ := utils2.SplitSymbol(od.Symbol)
			Wallets.Cancel(od.Key(), quote, 0, true)
			return nil
		}
	}
	maker := core.IsMaker(od.Symbol, exOrder.Side, entPrice)
	fee, err := exchange.CalculateFee(od.Symbol, exOrder.OrderType, exOrder.Side, exOrder.Amount, entPrice, maker, nil)
	if err != nil {
		return err
	}
	exOrder.Fee = fee.Cost
	exOrder.FeeType = fee.Currency
	if exOrder.Price == 0 {
		exOrder.Price = entPrice
	}
	Wallets.ConfirmOdEnter(od, entPrice)
	updateTime := btime.TimeMS() + int64(netCost)*1000
	exOrder.UpdateAt = updateTime
	if exOrder.CreateAt == 0 {
		exOrder.CreateAt = updateTime
	}
	exOrder.Filled = exOrder.Amount
	exOrder.Average = entPrice
	exOrder.Status = orm.OdStatusClosed
	od.Status = orm.InOutStatusFullEnter
	od.DirtyEnter = true
	od.DirtyMain = true
	o.callBack(od, true)
	return nil
}

func (o *LocalOrderMgr) fillPendingExit(od *orm.InOutOrder, price float64) *errs.Error {
	exOrder := od.Exit
	Wallets.ExitOd(od, exOrder.Amount)
	exchange, err := exg.Get()
	if err != nil {
		return err
	}
	maker := core.IsMaker(od.Symbol, exOrder.Side, price)
	fee, err := exchange.CalculateFee(od.Symbol, exOrder.OrderType, exOrder.Side, exOrder.Amount, price, maker, nil)
	if err != nil {
		return err
	}
	exOrder.Fee = fee.Cost
	exOrder.FeeType = fee.Currency
	updateTime := btime.TimeMS() + int64(netCost)*1000
	exOrder.UpdateAt = updateTime
	exOrder.CreateAt = updateTime
	exOrder.Status = orm.OdStatusClosed
	exOrder.Price = price
	exOrder.Filled = exOrder.Amount
	exOrder.Average = price
	od.Status = orm.InOutStatusFullExit
	od.DirtyMain = true
	od.DirtyExit = true
	_ = o.finishOrder(od, nil)
	Wallets.ConfirmOdExit(od, price)
	o.callBack(od, false)
	return nil
}

func (o *LocalOrderMgr) simMarketPrice(bar *banexg.Kline, rate float64) float64 {
	var (
		a, b, c, totalLen   float64
		aEndRate, bEndRate  float64
		start, end, posRate float64
	)

	openP := bar.Open
	highP := bar.High
	lowP := bar.Low
	closeP := bar.Close

	if openP <= closeP {
		// 阳线  一般是先下调走出下影线，然后上升到最高点，最后略微回撤，出现上影线
		a = openP - lowP
		b = highP - lowP
		c = highP - closeP
		totalLen = a + b + c
		if totalLen == 0 {
			return closeP
		}
		aEndRate = a / totalLen
		bEndRate = (a + b) / totalLen
		if rate <= aEndRate {
			start, end, posRate = openP, lowP, rate/aEndRate
		} else if rate <= bEndRate {
			start, end, posRate = lowP, highP, (rate-aEndRate)/(bEndRate-aEndRate)
		} else {
			start, end, posRate = highP, closeP, (rate-bEndRate)/(1-bEndRate)
		}
	} else {
		// 阴线  一般是先上升走出上影线，然后下降到最低点，最后略微回调，出现下影线
		a = highP - openP
		b = highP - lowP
		c = closeP - lowP
		totalLen = a + b + c
		if totalLen == 0 {
			return closeP
		}
		aEndRate = a / totalLen
		bEndRate = (a + b) / totalLen
		if rate <= aEndRate {
			start, end, posRate = openP, highP, rate/aEndRate
		} else if rate <= bEndRate {
			start, end, posRate = highP, lowP, (rate-aEndRate)/(bEndRate-aEndRate)
		} else {
			start, end, posRate = lowP, closeP, (rate-bEndRate)/(1-bEndRate)
		}
	}

	return start*(1-posRate) + end*posRate
}

func (o *LocalOrderMgr) tryFillTriggers(od *orm.InOutOrder, bar *banexg.Kline) {
	slPrice := od.GetInfoFloat64(orm.KeyStopLossPrice)
	tpPrice := od.GetInfoFloat64(orm.KeyTakeProfitPrice)
	if slPrice == 0 && tpPrice == 0 {
		return
	}
	if slPrice > 0 && (od.Short && bar.High >= slPrice || !od.Short && bar.Low <= slPrice) {
		od.LocalExit(core.ExitTagStopLoss, slPrice, "")
	} else if tpPrice > 0 && (od.Short && bar.Low <= tpPrice || !od.Short && bar.High >= tpPrice) {
		od.LocalExit(core.ExitTagTakeProfit, tpPrice, "")
	} else {
		return
	}
	Wallets.ExitOd(od, od.Exit.Amount)
	Wallets.ConfirmOdExit(od, od.Exit.Price)
}

func (o *LocalOrderMgr) onLowFunds() {
	panic("onLowFunds not implement")
}

func (o *LocalOrderMgr) CleanUp() *errs.Error {
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()
	orders, err := o.ExitOpenOrders(sess, "", &strategy.ExitReq{
		Tag:  core.ExitTagBotStop,
		Dirt: core.OdDirtBoth,
	})
	if err != nil {
		return err
	}
	if len(orders) > 0 {
		_, err = o.fillPendingOrders(orders, nil)
		if err != nil {
			return err
		}
	}
	return sess.DumpOrdersToDb()
}