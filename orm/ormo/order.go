package ormo

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/banbox/banbot/com"

	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

const (
	iOrderFields  = "id, task_id, symbol, sid, timeframe, short, status, enter_tag, init_price, quote_cost, exit_tag, leverage, enter_at, exit_at, strategy, stg_ver, max_pft_rate, max_draw_down, profit_rate, profit, info"
	exOrderFields = "id, task_id, inout_id, symbol, enter, order_type, order_id, side, create_at, price, average, quantity, filled, status, fee, fee_quote, fee_type, update_at"
)

type InOutOrder struct {
	*IOrder
	Enter      *ExOrder               `json:"enter"`
	Exit       *ExOrder               `json:"exit"`
	Info       map[string]interface{} `json:"info"`
	DirtyMain  bool                   `json:"-"` // IOrder has unsaved temporary changes 有未保存的临时修改
	DirtyEnter bool                   `json:"-"` // Enter has unsaved temporary changes 有未保存的临时修改
	DirtyExit  bool                   `json:"-"` // Exit has unsaved temporary changes 有未保存的临时修改
	DirtyInfo  bool                   `json:"-"` // Info has unsaved temporary changes 有未保存的临时修改
	idKey      string                 // Key to distinguish orders 区分订单的key
}

type InOutEdit struct {
	Order  *InOutOrder
	Action string
}

func (i *InOutOrder) SetInfo(key string, val interface{}) {
	i.loadInfo()
	if val == nil {
		delete(i.Info, key)
	} else {
		oldVal, _ := i.Info[key]
		if val == oldVal {
			// 无需修改
			return
		}
		i.Info[key] = val
	}
	i.DirtyInfo = true
}

func (i *InOutOrder) loadInfo() {
	if i.Info != nil {
		return
	}
	i.Info = decodeIOrderInfo(i.IOrder.Info)
}

func (i *InOutOrder) GetInfoFloat64(key string) float64 {
	i.loadInfo()
	val, ok := i.Info[key]
	if !ok {
		return 0
	}
	return utils.ConvertFloat64(val)
}

func (i *InOutOrder) GetInfoInt64(key string) int64 {
	i.loadInfo()
	val, ok := i.Info[key]
	if !ok {
		return 0
	}
	return utils.ConvertInt64(val)
}

func (i *InOutOrder) GetInfoString(key string) string {
	i.loadInfo()
	return utils2.GetMapVal(i.Info, key, "")
}

func (i *InOutOrder) EnterCost() float64 {
	if i.Enter.Filled == 0 {
		return 0
	}
	var price float64
	if i.Enter.Average > 0 {
		price = i.Enter.Average
	} else if i.Enter.Price > 0 {
		price = i.Enter.Price
	} else {
		price = i.InitPrice
	}
	return utils.NanInfTo(i.Enter.Filled*price, 0)
}

func (i *InOutOrder) HoldCost() float64 {
	holdCost := i.EnterCost()
	if i.Exit != nil && i.Exit.Filled > 0 {
		holdCost -= i.Exit.Filled * i.Exit.Average
	}
	return holdCost
}

func (i *InOutOrder) HoldAmount() float64 {
	entAmt := i.Enter.Filled
	if entAmt == 0 {
		return 0
	}
	if i.Exit != nil {
		entAmt -= i.Exit.Filled
	}
	return entAmt
}

func (i *InOutOrder) key(stamp int64) string {
	side := "long"
	if i.Short {
		side = "short"
	}
	enterAt := strconv.FormatInt(stamp, 10)
	return strings.Join([]string{i.Symbol, i.Strategy, side, i.EnterTag, enterAt}, "|")
}

func (i *InOutOrder) Key() string {
	if i.idKey != "" {
		return i.idKey
	}
	i.idKey = i.key(i.EnterAt)
	return i.idKey
}

/*
KeyAlign 开单时间戳按时间周期对齐，方便回测和实盘订单对比
*/
func (i *InOutOrder) KeyAlign() string {
	tfMSecs := int64(utils2.TFToSecs(i.Timeframe) * 1000)
	timeMS := int64(math.Round(float64(i.EnterAt)/float64(tfMSecs))) * tfMSecs
	return i.key(timeMS)
}

// SetEnterLimit set limit price for enter
func (i *InOutOrder) SetEnterLimit(price float64) *errs.Error {
	if i.Status >= InOutStatusFullEnter || i.Enter == nil || i.Enter.Status >= OdStatusClosed {
		return errs.NewMsg(errs.CodeRunTime, "cannot set entry limit for entered order: %s", i.Key())
	}
	if i.Enter.Price != price {
		i.Enter.Price = price
		i.DirtyEnter = true
		stopBars := i.GetInfoInt64(OdInfoStopBars)
		if stopBars > 0 {
			stopAfter := btime.TimeMS() + stopBars*int64(utils2.TFToSecs(i.Timeframe))*1000
			i.SetInfo(OdInfoStopAfter, stopAfter)
		}
		fireOdEdit(i, OdActionLimitEnter)
	}
	return nil
}

/*
CalcProfit
Return profit (before deducting commission)
返回利润（未扣除手续费）
*/
func (i *InOutOrder) CalcProfit(price float64) float64 {
	if i.Status == InOutStatusInit || i.Enter == nil || i.Enter.Average == 0 || i.Enter.Filled == 0 {
		return math.MaxFloat64
	}
	if price == 0 {
		if i.Exit != nil {
			if i.Exit.Average > 0 {
				price = i.Exit.Average
			} else if i.Exit.Price > 0 {
				price = i.Exit.Price
			}
		}
		if price == 0 && i.Enter.Price > 0 {
			price = i.Enter.Price
		}
	}
	profitVal := i.Enter.Filled * (price - i.Enter.Average)
	if i.Short {
		profitVal = 0 - profitVal
	}
	return profitVal
}

/*
UpdateProfits
update Profit&ProfitRate (without leverage)
因显示订单的时候突出的是amount和cost，是名义价值，没有突出订单保证金，带杠杆的利润率会让人误以为是名义价值基础上的利润率。为避免误解计算利润率不带杠杆，
需要杠杆的话，乘以Leverage就行
*/
func (i *InOutOrder) UpdateProfits(price float64) {
	profitVal := i.CalcProfit(price)
	if profitVal == math.MaxFloat64 {
		return
	}
	enterFee, exitFee := float64(0), float64(0)
	if i.Enter != nil && !math.IsNaN(i.Enter.FeeQuote) && !math.IsInf(i.Enter.FeeQuote, 0) {
		enterFee = i.Enter.FeeQuote
	}
	if i.Exit != nil && !math.IsNaN(i.Exit.FeeQuote) && !math.IsInf(i.Exit.FeeQuote, 0) {
		exitFee = i.Exit.FeeQuote
	}
	i.Profit = profitVal - enterFee - exitFee
	entPrice := i.InitPrice
	if i.Enter.Average > 0 {
		entPrice = i.Enter.Average
	} else if i.Enter.Price > 0 {
		entPrice = i.Enter.Price
	}
	entQuoteVal := entPrice * i.Enter.Filled
	i.ProfitRate = i.Profit / entQuoteVal
	if i.ProfitRate > i.MaxPftRate {
		i.MaxPftRate = i.ProfitRate
	} else {
		i.MaxDrawDown = min(i.MaxDrawDown, i.Profit, 0)
	}
	i.DirtyMain = true
}

/*
UpdateFee
Calculates commission for entry/exit orders. Must be called after Filled is assigned a value, otherwise the calculation is empty
为入场/出场订单计算手续费，必须在Filled赋值后调用，否则计算为空
*/
func (i *InOutOrder) UpdateFee(price float64, forEnter bool) *errs.Error {
	exchange := exg.Default
	exOrder := i.Enter
	if !forEnter {
		exOrder = i.Exit
	}
	//  dry-run 不用com.IsMaker最新价格判断是否限价单，因也是bar，会错取取close
	var maker = strings.Contains(exOrder.OrderType, "limit")
	if exOrder.OrderType == banexg.OdTypeLimit {
		if maker {
			exOrder.OrderType = banexg.OdTypeLimitMaker
		} else {
			exOrder.OrderType = "limit_taker"
		}
	}
	fee, err := exchange.CalculateFee(i.Symbol, exOrder.OrderType, exOrder.Side, exOrder.Filled, price, maker, nil)
	if err != nil {
		return err
	}
	exOrder.Fee = fee.Cost
	exOrder.FeeQuote = fee.QuoteCost
	exOrder.FeeType = fee.Currency
	if forEnter {
		i.DirtyEnter = true
	} else {
		i.DirtyExit = true
	}
	return nil
}

func (i *InOutOrder) CanClose() bool {
	if i.ExitTag != "" {
		return false
	}
	if i.Timeframe == "ws" {
		return true
	}
	tfMSecs := int64(utils2.TFToSecs(i.Timeframe) * 1000)
	return float64(btime.TimeMS()-i.RealEnterMS()) > float64(tfMSecs)*0.9
}

func (i *InOutOrder) SetExit(exitAt int64, tag, orderType string, limit float64) {
	if exitAt == 0 {
		exitAt = btime.TimeMS()
	}
	if i.ExitAt == 0 {
		if tag == "" {
			tag = core.ExitTagUnknown
		}
		i.ExitTag = tag
		i.ExitAt = exitAt
		i.DirtyMain = true
	}
	if i.Exit == nil {
		odSide := banexg.OdSideSell
		if i.Short {
			odSide = banexg.OdSideBuy
		}
		core.NewNumInSim += 1
		i.Exit = &ExOrder{
			TaskID:    i.TaskID,
			InoutID:   i.ID,
			Symbol:    i.Symbol,
			Enter:     false,
			OrderType: orderType,
			Side:      odSide,
			CreateAt:  exitAt,
			UpdateAt:  exitAt,
			Price:     limit,
			Quantity:  i.Enter.Filled,
			Status:    OdStatusInit,
		}
		i.DirtyExit = true
	} else {
		if orderType != "" {
			i.Exit.OrderType = orderType
			i.DirtyExit = true
		}
		if limit > 0 {
			i.Exit.Price = limit
			i.DirtyExit = true
		}
	}
}

/*
LocalExit
Forcefully exiting the order locally takes effect immediately, without waiting for the next bar. This does not involve wallet updates, the wallet needs to be updated on its own.
When calling this function on a real drive, it will be saved to the database
在本地强制退出订单，立刻生效，无需等到下一个bar。这里不涉及钱包更新，钱包需要自行更新。
实盘时调用此函数会持久化存储
*/
func (i *InOutOrder) LocalExit(exitAt int64, tag string, price float64, msg, odType string) *errs.Error {
	if price == 0 {
		newPrice := com.GetPriceExp(i.Symbol, "", com.Day10MSecs)
		if newPrice > 0 {
			price = newPrice
		} else if i.Enter.Average > 0 {
			price = i.Enter.Average
		} else if i.Enter.Price > 0 {
			price = i.Enter.Price
		} else {
			price = i.InitPrice
		}
	}
	if i.Enter.Status < OdStatusClosed {
		i.Enter.Status = OdStatusClosed
		err := i.UpdateFee(price, true)
		if err != nil {
			return err
		}
		i.DirtyEnter = true
	}
	if odType == "" {
		odType = banexg.OdTypeMarket
	}
	i.SetExit(exitAt, tag, odType, price)
	i.Exit.Status = OdStatusClosed
	i.Exit.Filled = i.Enter.Filled
	i.Exit.Average = i.Exit.Price
	i.Status = InOutStatusFullExit
	err := i.UpdateFee(price, false)
	if err != nil {
		return err
	}
	i.UpdateProfits(price)
	i.DirtyMain = true
	i.DirtyExit = true
	if msg != "" {
		i.SetInfo(KeyStatusMsg, msg)
	}
	return i.Save()
}

/*
CutPart
Split a small InOutOrder from the current order to solve the problem of one buy and multiple sell
从当前订单分割出一个小的InOutOrder，解决一次买入，多次卖出问题
*/
func (i *InOutOrder) CutPart(enterAmt, exitAmt float64) *InOutOrder {
	enterRate := enterAmt / i.Enter.Quantity
	exitRate := float64(0)
	if i.Exit != nil && exitAmt > 0 {
		exitRate = exitAmt / i.Exit.Quantity
	}
	part := &InOutOrder{
		IOrder: &IOrder{
			TaskID:    i.TaskID,
			Symbol:    i.Symbol,
			Sid:       i.Sid,
			Timeframe: i.Timeframe,
			Short:     i.Short,
			Status:    i.Status,
			EnterTag:  i.EnterTag,
			InitPrice: i.InitPrice,
			QuoteCost: i.QuoteCost * enterRate,
			Leverage:  i.Leverage,
			EnterAt:   i.EnterAt,
			Strategy:  i.Strategy,
			StgVer:    i.StgVer,
			Info:      i.IOrder.Info,
		},
		Info:       make(map[string]interface{}),
		DirtyMain:  true,
		DirtyEnter: true,
		DirtyInfo:  true,
		idKey:      i.idKey,
	}
	for key, val := range i.Info {
		part.Info[key] = val
	}
	// The enter.at of the original order needs to be+1 to prevent conflicts with sub orders that have been split.
	// 原来订单的enter_at需要+1，防止和拆分的子订单冲突。
	i.EnterAt += 1
	i.idKey = ""
	i.QuoteCost -= part.QuoteCost
	i.DirtyMain = true
	i.DirtyEnter = true
	partEnter := i.Enter.CutPart(enterRate, true)
	partEnter.InoutID = part.ID
	part.Enter = partEnter
	if i.Enter.Status == OdStatusInit && i.Status > InOutStatusInit {
		i.Status = InOutStatusInit
	}
	if exitRate == 0 && i.Exit != nil && i.Exit.Quantity > i.Enter.Quantity {
		exitRate = (i.Exit.Quantity - i.Enter.Quantity) / i.Exit.Quantity
	}
	if exitRate > 0 && i.Exit != nil {
		i.DirtyExit = true
		part.DirtyExit = true
		part.ExitAt = i.ExitAt
		part.ExitTag = i.ExitTag
		partExit := i.Exit.CutPart(exitRate, true)
		partExit.InoutID = part.ID
		part.Exit = partExit
		if part.Status < InOutStatusFullEnter {
			part.Status = InOutStatusFullEnter
		}
	} else if part.Status > InOutStatusFullEnter {
		// No Exit, the status should not be Exit
		// 无Exit，状态不应该为退出
		part.Status = InOutStatusFullEnter
	}
	return part
}

func (i *InOutOrder) IsDirty() bool {
	return i.DirtyExit || i.DirtyMain || i.DirtyEnter || i.DirtyInfo
}

func (i *InOutOrder) Save() *errs.Error {
	if i.ID == 0 && core.SimOrderMatch {
		core.NewNumInSim += 1
	}
	if i.Status == InOutStatusFullExit && (i.Enter == nil || i.Enter.Filled == 0) && (i.Exit == nil || i.Exit.Filled == 0) {
		i.Status = InOutStatusDelete
	}
	if core.LiveMode {
		openOds, lock := GetOpenODs(GetTaskAcc(i.TaskID))
		lock.Lock()
		if i.Status < InOutStatusFullExit {
			openOds[i.ID] = i
		} else {
			delete(openOds, i.ID)
			mLockOds.Lock()
			delete(lockOds, i.Key())
			mLockOds.Unlock()
		}
		lock.Unlock()
		oldId := i.ID
		err := i.saveToDb()
		if i.ID != oldId && i.Status < InOutStatusFullExit {
			lock.Lock()
			delete(openOds, oldId)
			openOds[i.ID] = i
			lock.Unlock()
		}
		if err != nil {
			return err
		}
	} else {
		i.saveToMem()
	}
	return nil
}

func (i *InOutOrder) saveToMem() {
	if i.ID == 0 {
		if i.Status == InOutStatusDelete {
			return
		}
		i.ID = FakeOdId
		FakeOdId += 1
	}
	openOds, lock := GetOpenODs(GetTaskAcc(i.TaskID))
	lock.Lock()
	if i.Status < InOutStatusFullExit {
		openOds[i.ID] = i
	} else {
		if _, ok := openOds[i.ID]; ok {
			delete(openOds, i.ID)
		}
		if i.Status == InOutStatusFullExit && i.Enter != nil && i.Enter.Filled > core.AmtDust {
			if _, ok := doneODs[i.ID]; !ok {
				doneODs[i.ID] = true
				// 切分的订单不会出现在openOds中
				HistODs = append(HistODs, i)
			}
		}
	}
	lock.Unlock()
}

func (i *InOutOrder) saveToDb() *errs.Error {
	if i.Status == InOutStatusDelete && i.ID == 0 {
		return nil
	}
	var err *errs.Error
	_, err = i.GetInfoText()
	if err != nil {
		return err
	}
	var sess *Queries
	var conn *orm.TrackedDB
	sess, conn, err = Conn(orm.DbTrades, true)
	if err != nil {
		return err
	}
	defer conn.Close()
	if i.Status == InOutStatusDelete {
		return sess.DelOrder(i)
	}
	i.NanInfTo(0)
	if i.ID == 0 {
		err = i.IOrder.saveAdd(sess)
		if err != nil {
			return err
		}
		i.DirtyMain = false
		i.Enter.InoutID = i.ID
		err = i.Enter.saveAdd(sess)
		if err != nil {
			return err
		}
		i.DirtyEnter = false
		if i.Exit != nil && i.Exit.Symbol != "" {
			i.Exit.InoutID = i.ID
			err = i.Exit.saveAdd(sess)
			if err != nil {
				return err
			}
		}
		i.DirtyExit = false
	} else {
		if i.DirtyMain {
			err = i.IOrder.saveUpdate(sess)
			if err != nil {
				return err
			}
			i.DirtyMain = false
		}
		if i.DirtyEnter {
			err = i.Enter.saveUpdate(sess)
			if err != nil {
				return err
			}
			i.DirtyEnter = false
		}
		if i.DirtyExit && i.Exit != nil && i.Exit.Symbol != "" {
			if i.Exit.ID == 0 {
				err = i.Exit.saveAdd(sess)
			} else {
				err = i.Exit.saveUpdate(sess)
			}
			if err != nil {
				return err
			}
			i.DirtyExit = false
		}
	}
	return nil
}

func (i *InOutOrder) GetInfoText() (string, *errs.Error) {
	if !i.DirtyInfo {
		return i.IOrder.Info, nil
	}
	i.DirtyInfo = false
	if i.Info != nil && len(i.Info) > 0 {
		infoText, err_ := utils2.MarshalString(i.Info)
		if err_ != nil {
			return "", errs.New(errs.CodeUnmarshalFail, err_)
		}
		i.IOrder.Info = infoText
		i.DirtyMain = true
		return infoText, nil
	}
	return "", nil
}

func (i *InOutOrder) SetExitTrigger(key string, args *ExitTrigger, price float64) *errs.Error {
	if key != OdInfoTakeProfit && key != OdInfoStopLoss {
		return errs.NewMsg(errs.CodeParamInvalid, "invalid key: %v", key)
	}
	var empty *TriggerState
	tg := utils2.GetMapVal(i.Info, key, empty)
	if args == nil || args.Price == 0 {
		if tg != nil && tg.OrderId != "" {
			tg.ExitTrigger = &ExitTrigger{}
			i.SetInfo(key, tg)
		} else {
			i.SetInfo(key, nil)
		}
		return nil
	} else if tg == nil {
		tg = &TriggerState{}
	}
	var isStopLoss = key == OdInfoStopLoss
	var side string
	if i.Enter.Side == banexg.OdSideBuy {
		side = banexg.OdSideSell
	} else {
		side = banexg.OdSideBuy
	}
	if price <= 0 {
		if i.Status == InOutStatusInit && i.Enter != nil && strings.Contains(i.Enter.OrderType, "limit") {
			price = i.Enter.Price
		} else {
			price = com.GetPriceExp(i.Symbol, side, com.Day10MSecs)
		}
	}
	if isStopLoss == (side == banexg.OdSideSell) {
		// 触发价低于最新价：平多止损、平空止盈
		if args.Price > price {
			return errs.NewMsg(errs.CodeParamInvalid, "%v price must <= latest price for %v", key, side)
		}
	} else {
		// 触发价高于最新价：平多止盈、平空止损
		if args.Price < price {
			return errs.NewMsg(errs.CodeParamInvalid, "%v price must >= latest price for %v", key, side)
		}
	}
	var rangeVal float64
	if args.Limit != 0 {
		rangeVal = math.Abs(i.InitPrice - args.Limit)
	} else {
		rangeVal = math.Abs(i.InitPrice - args.Price)
	}
	var changed = true
	if tg.ExitTrigger != nil {
		old := tg.ExitTrigger
		changed = old.Price != args.Price || old.Limit != args.Limit || old.Rate != args.Rate
	}
	tg.Range = rangeVal
	tg.ExitTrigger = args
	i.SetInfo(key, tg)
	if changed {
		fireOdEdit(i, key)
	}
	return nil
}

func (i *InOutOrder) SetStopLoss(args *ExitTrigger) *errs.Error {
	return i.SetExitTrigger(OdInfoStopLoss, args, 0)
}

func (i *InOutOrder) SetTakeProfit(args *ExitTrigger) *errs.Error {
	return i.SetExitTrigger(OdInfoTakeProfit, args, 0)
}

func (i *InOutOrder) GetExitTrigger(key string) *TriggerState {
	i.loadInfo()
	var empty *TriggerState
	return utils2.GetMapVal(i.Info, key, empty)
}

func (i *InOutOrder) GetStopLoss() *TriggerState {
	return i.GetExitTrigger(OdInfoStopLoss)
}

func (i *InOutOrder) GetTakeProfit() *TriggerState {
	return i.GetExitTrigger(OdInfoTakeProfit)
}

// SetTrailingStop 设置跟踪止损
// callbackPct回撤百分比；0取消，有效值[0.1, 10]
// activationPrice：0立即生效，-1不修改，>0具体激活价格
func (i *InOutOrder) SetTrailingStop(callbackPct, activationPrice float64) *errs.Error {
	callPct := i.GetInfoFloat64(OdInfoCallbackPct)
	if callPct == callbackPct {
		return nil
	}
	if callbackPct > 0 && (callbackPct < 0.1 || callbackPct > 10) {
		return errs.NewMsg(errs.CodeParamInvalid, "callbackPct should be in [0.1, 10]")
	}
	i.SetInfo(OdInfoCallbackPct, callbackPct)
	if activationPrice >= 0 {
		i.SetInfo(OdInfoActivePrice, activationPrice)
	}
	fireOdEdit(i, OdActionTrailing)
	return nil
}

// UpdateTrailing for local order
func (i *InOutOrder) UpdateTrailing(price float64) *errs.Error {
	callPct := i.GetInfoFloat64(OdInfoCallbackPct)
	if callPct > 0 {
		// 有跟踪止损
		activePrice := i.GetInfoFloat64(OdInfoActivePrice)
		if activePrice > 0 {
			if i.Short && activePrice < price || !i.Short && activePrice > price {
				return nil
			}
			i.SetInfo(OdInfoActivePrice, 0)
		}
		trailBest := i.GetInfoFloat64(OdInfoTrailingBest)
		if trailBest <= 0 {
			trailBest = price
		} else if i.Short {
			if trailBest <= price {
				return nil
			}
			trailBest = price
		} else {
			if trailBest >= price {
				return nil
			}
			trailBest = price
		}
		i.SetInfo(OdInfoTrailingBest, trailBest)
		if i.Short {
			callPct = -callPct
		}
		slPrice := trailBest * (1 - callPct/100)
		sl := i.GetStopLoss()
		var err *errs.Error
		if sl == nil {
			err = i.SetExitTrigger(OdInfoStopLoss, &ExitTrigger{
				Price: slPrice,
				Tag:   core.ExitTagTrailingStop,
			}, price)
			if err != nil {
				return err
			}
		} else if i.Short {
			if sl.Price <= 0 || slPrice < sl.Price {
				sl.Price = slPrice
				if sl.Limit != 0 {
					sl.Limit = min(slPrice, sl.Limit)
				}
			}
		} else if sl.Price <= 0 || slPrice > sl.Price {
			sl.Price = slPrice
			if sl.Limit != 0 {
				sl.Limit = max(slPrice, sl.Limit)
			}
		}
	}
	return nil
}

/*
ClientId
Generate the exchange's ClientOrderId
生成交易所的ClientOrderId
*/
func (i *InOutOrder) ClientId(random bool) string {
	client := i.GetInfoString(OdInfoClientID)
	if random {
		return fmt.Sprintf("%s_%v_%v_%v", config.Name, i.ID, rand.Intn(1000), client)
	}
	return fmt.Sprintf("%s_%v_%v", config.Name, i.ID, client)
}

func fireOdEdit(od *InOutOrder, action string) {
	if OdEditListener != nil && core.EnvReal && od.Status > InOutStatusInit && od.ID > 0 {
		OdEditListener(od, action)
	}
}

/*
Return to modify the lock of the current order. A successful return indicates that the lock has been obtained
返回修改当前订单的锁，返回成功表示已获取锁
*/
func (i *InOutOrder) Lock() *deadlock.Mutex {
	odKey := i.Key()
	mLockOds.Lock()
	lock, ok := lockOds[odKey]
	if !ok {
		lock = &deadlock.Mutex{}
		lockOds[odKey] = lock
	}
	mLockOds.Unlock()
	var got = int32(0)
	if core.LiveMode {
		// Real time mode with added deadlock detection
		// 实时模式，增加死锁检测
		stack := errs.CallStack(3, 20)
		time.AfterFunc(time.Second*5, func() {
			if atomic.LoadInt32(&got) == 1 {
				return
			}
			log.Error("order DeadLock found", zap.String("key", odKey), zap.String("stack", stack))
		})
	}
	lock.Lock()
	atomic.StoreInt32(&got, 1)
	return lock
}

func (i *InOutOrder) NanInfTo(v float64) {
	if i == nil {
		return
	}
	i.IOrder.NanInfTo(v)
	i.Enter.NanInfTo(v)
	i.Exit.NanInfTo(v)
}

func (i *InOutOrder) RealEnterMS() int64 {
	if i.Enter != nil {
		if i.Enter.UpdateAt > 0 {
			return i.Enter.UpdateAt
		} else if i.Enter.CreateAt > 0 {
			return i.Enter.CreateAt
		}
	}
	return i.EnterAt
}

func (i *InOutOrder) RealExitMS() int64 {
	if i.Exit != nil {
		if i.Exit.UpdateAt > 0 {
			return i.Exit.UpdateAt
		} else if i.Exit.CreateAt > 0 {
			return i.Exit.CreateAt
		}
	}
	return i.ExitAt
}

func (i *InOutOrder) Clone() *InOutOrder {
	if i == nil {
		return nil
	}
	clone := &InOutOrder{
		IOrder:     i.IOrder.Clone(),
		Enter:      i.Enter.Clone(),
		Exit:       i.Exit.Clone(),
		Info:       make(map[string]interface{}),
		DirtyMain:  i.DirtyMain,
		DirtyEnter: i.DirtyEnter,
		DirtyExit:  i.DirtyExit,
		DirtyInfo:  i.DirtyInfo,
		idKey:      i.idKey,
	}
	if i.Info != nil {
		for k, v := range i.Info {
			clone.Info[k] = v
		}
	}
	return clone
}

func (i *IOrder) saveAdd(sess *Queries) *errs.Error {
	newID, err_ := sess.AddIOrder(context.Background(), AddIOrderParams{
		TaskID:      i.TaskID,
		Symbol:      i.Symbol,
		Sid:         i.Sid,
		Timeframe:   i.Timeframe,
		Short:       i.Short,
		Status:      i.Status,
		EnterTag:    i.EnterTag,
		InitPrice:   i.InitPrice,
		QuoteCost:   i.QuoteCost,
		ExitTag:     i.ExitTag,
		Leverage:    i.Leverage,
		EnterAt:     i.EnterAt,
		ExitAt:      i.ExitAt,
		Strategy:    i.Strategy,
		StgVer:      i.StgVer,
		MaxPftRate:  i.MaxPftRate,
		MaxDrawDown: i.MaxDrawDown,
		ProfitRate:  i.ProfitRate,
		Profit:      i.Profit,
		Info:        i.Info,
	})
	if err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	i.ID = newID
	return nil
}

func (i *IOrder) saveUpdate(sess *Queries) *errs.Error {
	err_ := sess.SetIOrder(context.Background(), SetIOrderParams{
		TaskID:      i.TaskID,
		Symbol:      i.Symbol,
		Sid:         i.Sid,
		Timeframe:   i.Timeframe,
		Short:       i.Short,
		Status:      i.Status,
		EnterTag:    i.EnterTag,
		InitPrice:   i.InitPrice,
		QuoteCost:   i.QuoteCost,
		ExitTag:     i.ExitTag,
		Leverage:    i.Leverage,
		EnterAt:     i.EnterAt,
		ExitAt:      i.ExitAt,
		Strategy:    i.Strategy,
		StgVer:      i.StgVer,
		MaxPftRate:  i.MaxPftRate,
		MaxDrawDown: i.MaxDrawDown,
		ProfitRate:  i.ProfitRate,
		Profit:      i.Profit,
		Info:        i.Info,
		ID:          i.ID,
	})
	if err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	return nil
}

func (i *IOrder) NanInfTo(v float64) {
	if i == nil {
		return
	}
	i.Profit = utils.NanInfTo(i.Profit, v)
	i.ProfitRate = utils.NanInfTo(i.ProfitRate, v)
	i.MaxPftRate = utils.NanInfTo(i.MaxPftRate, v)
	i.MaxDrawDown = utils.NanInfTo(i.MaxDrawDown, v)
}

func (i *IOrder) Clone() *IOrder {
	if i == nil {
		return nil
	}
	return &IOrder{
		ID:          i.ID,
		TaskID:      i.TaskID,
		Symbol:      i.Symbol,
		Sid:         i.Sid,
		Timeframe:   i.Timeframe,
		Short:       i.Short,
		Status:      i.Status,
		EnterTag:    i.EnterTag,
		InitPrice:   i.InitPrice,
		QuoteCost:   i.QuoteCost,
		ExitTag:     i.ExitTag,
		Leverage:    i.Leverage,
		EnterAt:     i.EnterAt,
		ExitAt:      i.ExitAt,
		Strategy:    i.Strategy,
		StgVer:      i.StgVer,
		MaxPftRate:  i.MaxPftRate,
		MaxDrawDown: i.MaxDrawDown,
		ProfitRate:  i.ProfitRate,
		Profit:      i.Profit,
		Info:        i.Info,
	}
}

func (i *ExOrder) saveAdd(sess *Queries) *errs.Error {
	var err_ error
	i.ID, err_ = sess.AddExOrder(context.Background(), AddExOrderParams{
		TaskID:    i.TaskID,
		InoutID:   i.InoutID,
		Symbol:    i.Symbol,
		Enter:     i.Enter,
		OrderType: i.OrderType,
		OrderID:   i.OrderID,
		Side:      i.Side,
		CreateAt:  i.CreateAt,
		Price:     i.Price,
		Average:   i.Average,
		Quantity:  i.Quantity,
		Filled:    i.Filled,
		Status:    i.Status,
		Fee:       i.Fee,
		FeeQuote:  i.FeeQuote,
		FeeType:   i.FeeType,
		UpdateAt:  i.UpdateAt,
	})
	if err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	return nil
}

func (i *ExOrder) saveUpdate(sess *Queries) *errs.Error {
	err_ := sess.SetExOrder(context.Background(), SetExOrderParams{
		TaskID:    i.TaskID,
		InoutID:   i.InoutID,
		Symbol:    i.Symbol,
		Enter:     i.Enter,
		OrderType: i.OrderType,
		OrderID:   i.OrderID,
		Side:      i.Side,
		CreateAt:  i.CreateAt,
		Price:     i.Price,
		Average:   i.Average,
		Quantity:  i.Quantity,
		Filled:    i.Filled,
		Status:    i.Status,
		Fee:       i.Fee,
		FeeQuote:  i.FeeQuote,
		FeeType:   i.FeeType,
		UpdateAt:  i.UpdateAt,
		ID:        i.ID,
	})
	if err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	return nil
}

func (i *ExOrder) CutPart(rate float64, fill bool) *ExOrder {
	part := &ExOrder{
		TaskID:    i.TaskID,
		Symbol:    i.Symbol,
		Enter:     i.Enter,
		OrderType: i.OrderType,
		OrderID:   i.OrderID,
		Side:      i.Side,
		CreateAt:  i.CreateAt,
		Price:     i.Price,
		Average:   i.Average,
		Quantity:  i.Quantity * rate,
		Fee:       i.Fee,
		FeeQuote:  i.FeeQuote,
		FeeType:   i.FeeType,
		UpdateAt:  i.UpdateAt,
	}
	i.Quantity -= part.Quantity
	if fill && i.Filled > 0 {
		if i.Filled <= part.Quantity {
			part.Filled = i.Filled
			i.Filled = 0
		} else {
			part.Filled = part.Quantity
			i.Filled -= part.Filled
		}
	} else if i.Filled > i.Quantity {
		part.Filled = i.Filled - i.Quantity
		i.Filled = i.Quantity
	}
	if part.Filled >= part.Quantity {
		part.Status = OdStatusClosed
	} else if part.Filled > 0 {
		part.Status = OdStatusPartOK
	} else {
		part.Status = OdStatusInit
	}
	if i.Filled >= i.Quantity {
		i.Status = OdStatusClosed
	}
	return part
}

func (i *ExOrder) NanInfTo(v float64) {
	if i == nil {
		return
	}
	i.Price = utils.NanInfTo(i.Price, v)
	i.Fee = utils.NanInfTo(i.Fee, v)
	i.FeeQuote = utils.NanInfTo(i.FeeQuote, v)
}

func (i *ExOrder) Clone() *ExOrder {
	if i == nil {
		return nil
	}
	return &ExOrder{
		ID:        i.ID,
		TaskID:    i.TaskID,
		InoutID:   i.InoutID,
		Symbol:    i.Symbol,
		Enter:     i.Enter,
		OrderType: i.OrderType,
		OrderID:   i.OrderID,
		Side:      i.Side,
		CreateAt:  i.CreateAt,
		Price:     i.Price,
		Average:   i.Average,
		Quantity:  i.Quantity,
		Filled:    i.Filled,
		Status:    i.Status,
		Fee:       i.Fee,
		FeeQuote:  i.FeeQuote,
		FeeType:   i.FeeType,
		UpdateAt:  i.UpdateAt,
	}
}

func (q *Queries) getIOrders(sql string, args []interface{}) ([]*IOrder, *errs.Error) {
	rows, err_ := q.db.QueryContext(context.Background(), sql, args...)
	if err_ != nil {
		return nil, errs.New(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	var res = make([]*IOrder, 0, 4)
	for rows.Next() {
		var iod IOrder
		err_ = rows.Scan(
			&iod.ID,
			&iod.TaskID,
			&iod.Symbol,
			&iod.Sid,
			&iod.Timeframe,
			&iod.Short,
			&iod.Status,
			&iod.EnterTag,
			&iod.InitPrice,
			&iod.QuoteCost,
			&iod.ExitTag,
			&iod.Leverage,
			&iod.EnterAt,
			&iod.ExitAt,
			&iod.Strategy,
			&iod.StgVer,
			&iod.MaxPftRate,
			&iod.MaxDrawDown,
			&iod.ProfitRate,
			&iod.Profit,
			&iod.Info,
		)
		if err_ != nil {
			return nil, errs.New(core.ErrDbReadFail, err_)
		}
		res = append(res, &iod)
	}
	return res, nil
}

func (q *Queries) getExOrders(sql string, args []interface{}) ([]*ExOrder, *errs.Error) {
	rows, err_ := q.db.QueryContext(context.Background(), sql, args...)
	if err_ != nil {
		return nil, errs.New(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	var res = make([]*ExOrder, 0, 4)
	for rows.Next() {
		var iod ExOrder
		err_ = rows.Scan(
			&iod.ID,
			&iod.TaskID,
			&iod.InoutID,
			&iod.Symbol,
			&iod.Enter,
			&iod.OrderType,
			&iod.OrderID,
			&iod.Side,
			&iod.CreateAt,
			&iod.Price,
			&iod.Average,
			&iod.Quantity,
			&iod.Filled,
			&iod.Status,
			&iod.Fee,
			&iod.FeeQuote,
			&iod.FeeType,
			&iod.UpdateAt,
		)
		if err_ != nil {
			return nil, errs.New(core.ErrDbReadFail, err_)
		}
		res = append(res, &iod)
	}
	return res, nil
}

type GetHistOrdersArgs struct {
	Strategy    string
	Pairs       []string
	TimeFrame   string
	TaskID      int64 // 0 represents all,>0 represents specified task 0表示所有，>0表示指定任务
	Dirt        int   // core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth
	EnterTag    string
	ExitTag     string
	CloseAfter  int64 // Start timestamp 开始时间戳
	CloseBefore int64 // End timestamp 结束时间戳
	Limit       int
	AfterID     int  // position means after; negative means before
	IDAsc       bool // sort by ID asc(default: desc)
}

type GetOrdersArgs struct {
	Strategy    string
	Pairs       []string
	TimeFrame   string
	TaskID      int64 // 0 represents all,>0 represents specified task 0表示所有，>0表示指定任务
	Dirt        int   // core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth
	EnterTag    string
	ExitTag     string
	CloseAfter  int64 // Start timestamp 开始时间戳
	CloseBefore int64 // End timestamp 结束时间戳
	Limit       int
	AfterID     int // position means after; negative means before

	Status  int // 0 represents all, 1 represents open interest, 2 represents historical orders 0表示所有，1表示未平仓，2表示历史订单
	OrderBy string
}

func (q *Queries) GetOrders(args GetOrdersArgs) ([]*InOutOrder, *errs.Error) {
	if args.TaskID < 0 {
		return nil, nil
	}
	var b strings.Builder
	b.WriteString("select ")
	b.WriteString(iOrderFields)
	b.WriteString(" from iorder where 1=1 ")
	sqlParams := make([]interface{}, 0, 2)
	if args.TaskID > 0 {
		b.WriteString(fmt.Sprintf("and task_id=$%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.TaskID)
	}
	if args.AfterID > 0 {
		b.WriteString(fmt.Sprintf(" and id > $%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.AfterID)
		args.OrderBy = "id asc"
	} else if args.AfterID < 0 {
		b.WriteString(fmt.Sprintf(" and id < $%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, -args.AfterID)
		args.OrderBy = "id desc"
	}
	if args.Status >= 1 {
		rel := "<"
		if args.Status == 2 {
			rel = "="
		}
		b.WriteString(fmt.Sprintf("and status%v$%v ", rel, len(sqlParams)+1))
		sqlParams = append(sqlParams, InOutStatusFullExit)
	}
	if args.Strategy != "" {
		b.WriteString(fmt.Sprintf("and strategy=$%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.Strategy)
	}
	if len(args.Pairs) == 1 {
		b.WriteString(fmt.Sprintf("and symbol=$%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.Pairs[0])
	} else if len(args.Pairs) > 1 {
		b.WriteString("and symbol in(")
		for i, pair := range args.Pairs {
			isLast := i == len(args.Pairs)-1
			b.WriteString(fmt.Sprintf("$%v", len(sqlParams)+1))
			if !isLast {
				b.WriteString(",")
			}
			sqlParams = append(sqlParams, pair)
		}
		b.WriteString(") ")
	}
	if args.TimeFrame != "" {
		b.WriteString(fmt.Sprintf("and timeframe=$%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.TimeFrame)
	}
	if args.Dirt != 0 {
		b.WriteString(fmt.Sprintf("and short=$%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.Dirt == core.OdDirtShort)
	}
	if args.CloseAfter > 0 {
		b.WriteString(fmt.Sprintf("and exit_at >= $%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.CloseAfter)
	}
	if args.CloseBefore > 0 {
		b.WriteString(fmt.Sprintf("and exit_at < $%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.CloseBefore)
	}
	if args.EnterTag != "" {
		b.WriteString(fmt.Sprintf("and enter_tag=$%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.EnterTag)
	}
	if args.ExitTag != "" {
		b.WriteString(fmt.Sprintf("and exit_tag=$%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.ExitTag)
	}
	if args.OrderBy == "" {
		args.OrderBy = "id desc"
	}
	b.WriteString("order by " + args.OrderBy)
	if args.Limit > 0 {
		b.WriteString(fmt.Sprintf(" limit $%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.Limit)
	}
	iorders, err := q.getIOrders(b.String(), sqlParams)
	if err != nil || len(iorders) == 0 {
		return nil, err
	}
	var itemMap = make(map[int64]*InOutOrder)
	b = strings.Builder{}
	b.WriteString("select ")
	b.WriteString(exOrderFields)
	b.WriteString(" from exorder where 1=1 ")
	sqlParams = make([]interface{}, 0, 2)
	if args.TaskID > 0 {
		b.WriteString(fmt.Sprintf("and task_id=$%v ", len(sqlParams)+1))
		sqlParams = append(sqlParams, args.TaskID)
	}
	b.WriteString("and inout_id in (")
	isFirst := true
	res := make([]*InOutOrder, 0, len(iorders))
	for _, od := range iorders {
		iod := &InOutOrder{
			IOrder: od,
		}
		iod.loadInfo()
		res = append(res, iod)
		itemMap[od.ID] = iod
		if !isFirst {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("$%v", len(sqlParams)+1))
		sqlParams = append(sqlParams, od.ID)
		isFirst = false
	}
	b.WriteString(")")
	exOrders, err := q.getExOrders(b.String(), sqlParams)
	if err != nil {
		return nil, err
	}
	for _, od := range exOrders {
		iod, ok := itemMap[od.InoutID]
		if !ok {
			continue
		}
		if od.Enter {
			iod.Enter = od
		} else {
			iod.Exit = od
		}
	}
	result := make([]*InOutOrder, 0, len(iorders))
	for _, od := range res {
		if od.Enter != nil {
			result = append(result, od)
		}
	}
	return result, nil
}

// deleteExOrderById deletes an exorder record by ID
func (q *Queries) deleteExOrderById(ctx context.Context, exId int64) *errs.Error {
	if exId == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from exorder where id=%v", exId)
	_, err_ := q.db.ExecContext(ctx, sql)
	if err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	return nil
}

// deleteIOrderById deletes an iorder record by ID
func (q *Queries) deleteIOrderById(ctx context.Context, iorderId int64) *errs.Error {
	if iorderId == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from iorder where id=%v", iorderId)
	_, err_ := q.db.ExecContext(ctx, sql)
	if err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	return nil
}

func (q *Queries) DelOrder(od *InOutOrder) *errs.Error {
	if od == nil || od.ID == 0 {
		return nil
	}
	openOds, lock := GetOpenODs(GetTaskAcc(od.TaskID))
	lock.Lock()
	delete(openOds, od.ID)
	lock.Unlock()
	// The setting has been completed to prevent incorrect usage elsewhere
	// 设置已完成，防止其他地方错误使用
	od.Status = InOutStatusDelete
	ctx := context.Background()
	err := q.deleteIOrderById(ctx, od.ID)
	if err != nil {
		return err
	}
	if od.Enter != nil && od.Enter.ID > 0 {
		err = q.deleteExOrderById(ctx, od.Enter.ID)
		if err != nil {
			return err
		}
	}
	if od.Exit != nil && od.Exit.ID > 0 {
		return q.deleteExOrderById(ctx, od.Exit.ID)
	}
	return nil
}

/*
GetHistOrderTfs
Retrieve the specified task and the latest usage time period of the specified policy
获取指定任务，指定策略的最新使用的时间周期
*/
func (q *Queries) GetHistOrderTfs(taskId int64, stagy string) (map[string]string, *errs.Error) {
	ctx := context.Background()
	sqlStr := fmt.Sprintf("select DISTINCT symbol,timeframe from iorder where task_id=%v and strategy=? ORDER BY symbol, enter_at DESC", taskId)
	rows, err_ := q.db.QueryContext(ctx, sqlStr, stagy)
	if err_ != nil {
		return nil, errs.New(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	var result = make(map[string]string)
	for rows.Next() {
		var symbol, timeFrame string
		err_ = rows.Scan(&symbol, &timeFrame)
		if err_ != nil {
			return result, errs.New(core.ErrDbReadFail, err_)
		}
		result[symbol] = timeFrame
	}
	return result, nil
}

func (s *TriggerState) SaveOld() {
	if s.Old == nil {
		s.Old = s.ExitTrigger.Clone()
	} else {
		s.Old.Price = s.Price
		s.Old.Limit = s.Limit
		s.Old.Rate = s.Rate
		if s.Tag != "" {
			s.Old.Tag = s.Tag
		}
	}
}

func (s *TriggerState) Clone() *TriggerState {
	if s == nil {
		return nil
	}
	return &TriggerState{
		ExitTrigger: &ExitTrigger{
			Price: s.Price,
			Limit: s.Limit,
			Rate:  s.Rate,
			Tag:   s.Tag,
		},
		Range:   s.Rate,
		Hit:     s.Hit,
		OrderId: s.OrderId,
	}
}

func (t *ExitTrigger) Equal(o *ExitTrigger) bool {
	if t == nil || o == nil {
		return (t != nil) == (o != nil)
	}
	if t.Price != o.Price || t.Limit != o.Limit || t.Rate != o.Limit {
		return false
	}
	return true
}

func (t *ExitTrigger) Clone() *ExitTrigger {
	if t == nil {
		return nil
	}
	return &ExitTrigger{
		Price: t.Price,
		Limit: t.Limit,
		Rate:  t.Rate,
		Tag:   t.Tag,
	}
}

/*
LegalDoneProfits
Calculate the fiat value of realized profits
计算已实现利润的法币价值
*/
func LegalDoneProfits(off int) float64 {
	var total = float64(0)
	var skips []string
	for i := off; i < len(HistODs); i++ {
		od := HistODs[i]
		_, quote, _, _ := core.SplitSymbol(od.Symbol)
		price := com.GetPriceSafe(quote, "")
		if price == -1 {
			skips = append(skips, quote)
			continue
		}
		total += price * od.Profit
	}
	if len(skips) > 0 {
		log.Info("skip items in LegalDoneProfits", zap.Int("num", len(skips)),
			zap.String("items", strings.Join(skips, ",")))
	}
	return total
}

func CalcTimeRange(odList []*InOutOrder) (int64, int64) {
	if len(odList) == 0 {
		return 0, 0
	}
	startMS := odList[0].RealEnterMS()
	endMS := odList[0].RealExitMS()
	for _, od := range odList {
		curEnter := od.RealEnterMS()
		curExit := od.RealExitMS()
		if curEnter > 0 {
			startMS = min(startMS, curEnter)
		}
		endMS = max(endMS, curExit)
	}
	return startMS, endMS
}

/*
CalcUnitReturns 计算单位每日回报金额

odList 订单列表，可无序
closes 对应tfMSecs的每个单位收盘价，可为空
startMS 区间开始时间，13位时间戳
endMS 区间结束时间，13位时间戳
tfMSecs 单位的毫秒间隔
*/
func CalcUnitReturns(odList []*InOutOrder, closes []float64, startMS, endMS, tfMSecs int64) ([]float64, int, int) {
	arrLen := int((endMS - startMS) / tfMSecs)
	returns := make([]float64, arrLen)
	dayOff, dayEnd := 0, 0
	for i, od := range odList {
		entryMS := od.RealEnterMS()
		entryAlignMS := utils2.AlignTfMSecs(entryMS, tfMSecs)
		offset := int((entryAlignMS - startMS) / tfMSecs)
		dirt := float64(1)
		if od.Short {
			dirt = -1
		}
		if i == 0 {
			dayOff = offset
		}
		entryPrice := od.Enter.Average
		exitPrice := od.Exit.Average
		exitMS := od.RealExitMS()
		if exitMS <= entryMS {
			continue
		}
		holdNumF := float64((exitMS-entryAlignMS)/tfMSecs + 1)
		holdNum := int(math.Ceil(holdNumF))
		priceStep := (exitPrice - entryPrice) / holdNumF
		posPrice := entryPrice + priceStep // 模拟每个单位的收盘价
		enterCost := od.EnterCost()

		rets := make([]float64, holdNum)
		idx := 0
		if od.Enter != nil {
			rets[idx] -= od.Enter.FeeQuote
		}

		// 计算在每个时间单位上的回报
		openPrice := entryPrice
		for entryAlignMS < exitMS {
			closePrice := exitPrice
			if exitMS > entryAlignMS+tfMSecs {
				if offset+idx < len(closes) {
					closePrice = closes[offset+idx]
				} else {
					closePrice = posPrice
				}
			}
			ret := (closePrice - openPrice) / entryPrice * dirt * enterCost
			if idx < len(rets) {
				rets[idx] += ret
			}
			idx += 1
			entryAlignMS += tfMSecs
			openPrice = closePrice
			posPrice += priceStep
		}
		if idx-1 < len(rets) && od.Exit != nil {
			rets[idx-1] -= od.Exit.FeeQuote
		}
		// 累加利润
		for j, v := range rets {
			if offset+j >= len(returns) {
				break
			}
			returns[offset+j] += v
		}
		dayEnd = offset + len(rets)
	}
	return returns, dayOff, dayEnd
}
