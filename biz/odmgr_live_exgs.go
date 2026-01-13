package biz

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

func exitByMyOrder(o *LiveOrderMgr) FuncHandleMyOrder {
	return func(od *banexg.Order) bool {
		if od.Filled == 0 {
			return false
		}
		isShort := od.PositionSide == banexg.PosSideShort
		var openOds []*ormo.InOutOrder
		accOpenOds, lock := ormo.GetOpenODs(o.Account)
		lock.Lock()
		for _, iod := range accOpenOds {
			if iod.Short != isShort || iod.Symbol != od.Symbol || iod.Enter.Side == od.Side {
				continue
			}
			openOds = append(openOds, iod)
		}
		lock.Unlock()
		if len(openOds) == 0 {
			// There are no orders that can be closed in the same direction or opposite direction.
			// 没有同方向，相反操作的可平仓订单
			return false
		}
		filled := od.Filled
		feeName, feeCost, feeQuote := "", float64(0), float64(0)
		if od.Fee != nil {
			feeName = od.Fee.Currency
			feeCost = od.Fee.Cost
			feeQuote = od.Fee.QuoteCost
		}
		var err *errs.Error
		var part *ormo.InOutOrder
		var doneParts []*ormo.InOutOrder
		var feeLeft float64
		for _, iod := range openOds {
			lock2 := iod.Lock()
			filled, feeLeft, part = o.tryFillExit(iod, filled, od.Average, od.Timestamp, od.ID, od.Type, feeName, feeCost, feeQuote)
			lock2.Unlock()
			feeCost *= feeLeft
			feeQuote *= feeLeft
			if part.Status == ormo.InOutStatusFullExit {
				doneParts = append(doneParts, part)
			}
			if filled < AmtDust {
				break
			}
		}
		// 检查是否有剩余数量，创建相反订单 Check if there is a remaining quantity and create an opposite order
		createInv := !od.ReduceOnly && filled > AmtDust && config.TakeOverStrat != ""
		if len(doneParts) == 0 && !createInv {
			return true
		}
		for _, part = range doneParts {
			lock2 := part.Lock()
			err = o.finishOrder(part)
			lock2.Unlock()
			if err != nil {
				log.Error("finish order fail", zap.String("key", part.Key()), zap.Error(err))
			}
			log.Info("exit order by third", zap.String("acc", o.Account),
				zap.String("key", part.Key()), zap.String("id", od.ID))
			o.callBack(part, false)
		}
		if createInv {
			iod := o.makeInOutOd(od.Symbol, isShort, od.Average, filled, od.Type, feeCost, feeQuote, feeName,
				od.Timestamp, ormo.OdStatusClosed, od.ID)
			if iod != nil {
				o.callBack(iod, true)
			}
		}
		return true
	}
}

func (o *LiveOrderMgr) makeInOutOd(pair string, short bool, average, filled float64, odType string,
	feeCost float64, feeQuote float64, feeName string, enterAt int64, entStatus int, entOdId string) *ormo.InOutOrder {
	exs, err := orm.GetExSymbolCur(pair)
	if err != nil {
		log.Error("get exSymbol fail", zap.Error(err))
		return nil
	}
	defTF := config.GetTakeOverTF(pair, "")
	if defTF != "" {
		log.Error("no strat job found for trade", zap.String("pair", pair),
			zap.String("id", entOdId))
		return nil
	}
	iod := o.createInOutOd(exs, short, average, filled, odType, feeCost, feeQuote, feeName, enterAt,
		entStatus, entOdId, defTF)
	err = iod.Save()
	if err != nil {
		log.Error("save third order fail", zap.String("key", iod.Key()), zap.Error(err))
		return nil
	}
	openOds, lock := ormo.GetOpenODs(o.Account)
	lock.Lock()
	openOds[iod.ID] = iod
	lock.Unlock()
	return iod
}

func traceExgOrder(o *LiveOrderMgr) FuncHandleMyOrder {
	return func(od *banexg.Order) bool {
		if od.ReduceOnly || od.Status != banexg.OdStatusFilled {
			// 忽略只减仓订单  只对完全入场的尝试跟踪Ignore Reduction Only Orders Only track attempts for full entry
			return false
		}
		isShort := od.PositionSide == banexg.PosSideShort
		if core.IsContract {
			if !isShort && od.Side == banexg.OdSideSell || isShort && od.Side == banexg.OdSideBuy {
				// Ignore closed orders 忽略平仓的订单
				return false
			}
		} else if od.Side == banexg.OdSideSell {
			// 现货市场卖出即平仓，忽略平仓
			return false
		}
		feeName, feeCost, feeQuote := getFeeNameCost(od.Fee, od.Symbol, od.Type, od.Side, od.Amount, od.Average)
		iod := o.makeInOutOd(od.Symbol, isShort, od.Average, od.Filled, od.Type, feeCost, feeQuote, feeName,
			od.Timestamp, ormo.OdStatusClosed, od.ID)
		if iod != nil {
			o.callBack(iod, true)
		}
		return true
	}
}
