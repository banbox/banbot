package biz

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

var (
	accWallets = make(map[string]*BanWallets)
)

type ItemWallet struct {
	Coin          string             // Coin code, not pair 币代码，非交易对
	Available     float64            // Available balance 可用余额
	Pendings      map[string]float64 // Lock the amount when buying or selling. The key can be the order id. 买入卖出时锁定金额，键可以是订单id
	Frozens       map[string]float64 // Long-term frozen amount for short orders, etc. The key can be the order ID. 空单等长期冻结金额，键可以是订单id
	UnrealizedPOL float64            // The public unrealized profit and loss of this currency, used by the contract, can be deducted from the margin of other orders. Recalculated every bar. 此币的公共未实现盈亏，合约用到，可抵扣其他订单保证金占用。每个bar重新计算
	UsedUPol      float64            // Used unrealized profit and loss (used as margin for other orders). 已占用的未实现盈亏（用作其他订单的保证金）
	Withdraw      float64            // Cash withdrawal from balance will not be used for trading. 从余额提现的，不会用于交易
	lock          deadlock.Mutex
}

type BanWallets struct {
	Items   map[string]*ItemWallet
	Account string
	IsWatch bool
}

/*
InitFakeWallets
Initialize a wallet object from a configuration file
从配置文件初始化一个钱包对象
*/
func InitFakeWallets(symbols ...string) {
	updates := make(map[string]float64)
	if len(symbols) == 0 {
		updates = config.WalletAmounts
	} else {
		for _, s := range symbols {
			amount, ok := config.WalletAmounts[s]
			if ok {
				updates[s] = amount
			}
		}
	}
	wallets := GetWallets(config.DefAcc)
	wallets.SetWallets(updates)
	wallets.TryUpdateStakePctAmt()
}

func GetWallets(account string) *BanWallets {
	if !core.EnvReal {
		account = config.DefAcc
	}
	val, ok := accWallets[account]
	if !ok {
		val = &BanWallets{
			Items:   map[string]*ItemWallet{},
			Account: account,
		}
		accWallets[account] = val
	}
	return val
}

// Total: Available+Pendings+Frozens+[UnrealizedPOL]
func (iw *ItemWallet) Total(withUpol bool) float64 {
	sumVal := iw.Used()
	iw.lock.Lock()
	sumVal += iw.Available
	if withUpol {
		sumVal += iw.UnrealizedPOL
	}
	iw.lock.Unlock()
	return sumVal
}

func (iw *ItemWallet) Used() float64 {
	iw.lock.Lock()
	sumVal := float64(0)
	if allVal, ok := iw.Pendings["*"]; ok {
		sumVal += allVal
	} else {
		for _, v := range iw.Pendings {
			sumVal += v
		}
	}
	if allVal, ok := iw.Frozens["*"]; ok {
		sumVal += allVal
	} else {
		for _, v := range iw.Frozens {
			sumVal += v
		}
	}
	iw.lock.Unlock()
	return sumVal
}

/*
FiatValue Get the fiat currency value of this wallet 获取此钱包的法币价值
*/
func (iw *ItemWallet) FiatValue(withUpol bool) float64 {
	return iw.Total(withUpol) * com.GetPriceExp(iw.Coin, "", com.Day10MSecs) // 10天内价格均可
}

/*
SetMargin
Set margin occupancy. Take priority from unrealized pol used upol. When insufficient, it will be taken from the balance. Released to balance when exceeded
设置保证金占用。优先从unrealized_pol-used_upol中取。不足时从余额中取。

	超出时释放到余额
*/
func (iw *ItemWallet) SetMargin(odKey string, amount float64) *errs.Error {
	// Extract the old margin occupation value
	// 提取旧保证金占用值
	iw.lock.Lock()
	oldAmt, exists := iw.Frozens[odKey]
	if !exists {
		oldAmt = 0
	}

	avaUpol := iw.UnrealizedPOL - iw.UsedUPol
	upolCost := 0.0

	if avaUpol > 0 {
		// Priority use of unavailable unrealized profit and loss balance
		// 优先使用可用的未实现盈亏余额
		iw.UsedUPol += amount
		if iw.UsedUPol <= iw.UnrealizedPOL {
			// Not achieved enough profit and loss, no need to freeze
			// 未实现盈亏足够，无需冻结
			if _, exists := iw.Frozens[odKey]; exists {
				delete(iw.Frozens, odKey)
			}
			upolCost = amount
			amount = 0
		} else {
			//Not achieved insufficient profit and loss, the update needs to be occupied
			//未实现盈亏不足，更新还需占用的
			newAmount := iw.UsedUPol - iw.UnrealizedPOL
			upolCost = amount - newAmount
			amount = newAmount
			iw.UsedUPol = iw.UnrealizedPOL
		}
	}

	addVal := amount - oldAmt
	if addVal <= 0 {
		//The existing margin exceeds the required value, released to the balance
		//已有保证金超过要求值，释放到余额
		iw.Available -= addVal
	} else {
		//There is insufficient security deposit and it will be deducted from the balance.
		//已有保证金不足，从余额中扣除
		if iw.Available < addVal {
			//Insufficient balance
			//余额不足
			iw.UsedUPol -= upolCost
			iw.lock.Unlock()
			return errs.NewMsg(core.ErrLowFunds, "%s Insufficient, frozen require: %s, %s",
				iw.Coin, fmt.Sprintf("%.5f", addVal), odKey)
		}
		iw.Available -= addVal
	}
	iw.Frozens[odKey] = amount
	iw.lock.Unlock()

	return nil
}

/*
SetFrozen
Set the frozen amount to a fixed value. Can be synchronized from balance or pending.

Any shortage is taken from the other side, and excess is added to the other side.
设置冻结金额为固定值。可从余额或pending中同步。

	不足则从另一侧取用，超出则添加到另一侧。
*/
func (iw *ItemWallet) SetFrozen(odKey string, amount float64, withAvailable bool) *errs.Error {
	iw.lock.Lock()
	oldAmt, exists := iw.Frozens[odKey]
	if !exists {
		oldAmt = 0
	}

	addVal := amount - oldAmt
	if withAvailable {
		if addVal > 0 && iw.Available < addVal {
			iw.lock.Unlock()
			return errs.NewMsg(core.ErrLowFunds, "%s Insufficient, frozen require: %s, %s",
				iw.Coin, fmt.Sprintf("%.5f", addVal), odKey)
		}
		iw.Available -= addVal
	} else {
		pendVal, exists := iw.Pendings[odKey]
		if !exists {
			pendVal = 0
		}
		if addVal > 0 && pendVal < addVal {
			iw.lock.Unlock()
			return errs.NewMsg(core.ErrLowFunds, "%s Insufficient, frozen require: %s, %s",
				iw.Coin, fmt.Sprintf("%.5f", addVal), odKey)
		}
		iw.Pendings[odKey] = pendVal - addVal
	}
	iw.Frozens[odKey] = amount
	iw.lock.Unlock()

	return nil
}

func (iw *ItemWallet) Reset() {
	iw.lock.Lock()
	iw.Available = 0
	iw.UnrealizedPOL = 0
	iw.UsedUPol = 0
	iw.Frozens = make(map[string]float64)
	iw.Pendings = make(map[string]float64)
	iw.lock.Unlock()
}

func (w *BanWallets) SetWallets(data map[string]float64) {
	for k, v := range data {
		item, ok := w.Items[k]
		if !ok {
			item = &ItemWallet{
				Coin:     k,
				Pendings: make(map[string]float64),
				Frozens:  make(map[string]float64),
			}
			w.Items[k] = item
		} else {
			item.Reset()
		}
		item.lock.Lock()
		item.Available = v
		item.lock.Unlock()
	}
}

func (w *BanWallets) DumpAvas() map[string]float64 {
	res := make(map[string]float64)
	for k, v := range w.Items {
		res[k] = v.Available
	}
	return res
}

func (w *BanWallets) Get(code string) *ItemWallet {
	wallet, ok := w.Items[code]
	if !ok {
		wallet = &ItemWallet{
			Coin:     code,
			Pendings: make(map[string]float64),
			Frozens:  make(map[string]float64),
		}
		w.Items[code] = wallet
	}
	return wallet
}

/*
CostAva
Deducted from the available balance of a certain coin and added to pending, used only for backtesting
从某个币的可用余额中扣除，添加到pending中，仅用于回测
negative : Whether to allow negative balances (used for short orders) 是否允许负数余额（空单用到）
minRate: Minimum order opening ratio 最小开单倍率
return: Actual amount deducted, errs.Error
*/
func (w *BanWallets) CostAva(odKey string, symbol string, amount float64, negative bool, minRate float64) (float64, *errs.Error) {
	wallet := w.Get(symbol)
	wallet.lock.Lock()
	srcAmount := wallet.Available
	if minRate == 0 {
		minRate = config.MinOpenRate
	}
	var realCost float64
	if srcAmount >= amount || negative {
		//If the balance is sufficient, or negative amounts are allowed, they will be deducted directly.
		//余额充足，或允许负数，直接扣除
		realCost = amount
	} else if srcAmount/amount > minRate {
		//The difference is within the approximate allowable range, minus the actual value
		//差额在近似允许范围内，扣除实际值
		realCost = srcAmount
	} else {
		wallet.lock.Unlock()
		return 0, errs.NewMsg(core.ErrLowFunds, "wallet %s balance %.5f < %.5f", symbol, srcAmount, amount)
	}
	log.Debug("CostAva wallet", zap.String("key", odKey), zap.String("coin", symbol),
		zap.Float64("ava", wallet.Available), zap.Float64("cost", realCost))
	wallet.Available -= realCost
	wallet.Pendings[odKey] = realCost
	wallet.lock.Unlock()
	return realCost, nil
}

/*
CostFrozen
This method is not used for contracts
Deduct from frozen, if not enough, deduct the remainder from available
After deduction, add to pending

	此方法不用于合约
	从frozen中扣除，如果不够，从available扣除剩余部分
	扣除后，添加到pending中
*/
func (w *BanWallets) CostFrozen(odKey string, symbol string, amount float64) float64 {
	wallet, ok := w.Items[symbol]
	if !ok {
		return 0
	}

	wallet.lock.Lock()
	frozenAmt, ok := wallet.Frozens[odKey]
	if ok {
		delete(wallet.Frozens, odKey)
	} else {
		frozenAmt = 0
	}

	log.Debug("CostFrozen wallet", zap.String("key", odKey), zap.String("coin", symbol),
		zap.Float64("ava", wallet.Available), zap.Float64("add", frozenAmt-amount))
	// Return the remaining portion of the freeze to available, either positive or negative.
	// 将冻结的剩余部分归还到available，正负都有可能
	wallet.Available += frozenAmt - amount

	realCost := amount
	if wallet.Available < 0 {
		realCost += wallet.Available
		wallet.Available = 0
	}

	if wallet.Pendings == nil {
		wallet.Pendings = make(map[string]float64)
	}
	wallet.Pendings[odKey] = realCost
	wallet.lock.Unlock()

	return realCost
}

/*
ConfirmPending
Confirm deduction from src and add to tgt's balance
从src中确认扣除，添加到tgt的余额中
*/
func (w *BanWallets) ConfirmPending(odKey string, srcKey string, srcAmount float64, tgtKey string, tgtAmount float64, toFrozen bool) bool {
	src, srcExists := w.Items[srcKey]
	if !srcExists {
		return false
	}

	tgt := w.Get(tgtKey)

	src.lock.Lock()
	pendingAmt, ok := src.Pendings[odKey]
	if !ok {
		src.lock.Unlock()
		return false
	}

	leftPending := pendingAmt - srcAmount
	delete(src.Pendings, odKey)

	log.Debug("ConfirmPending wallet", zap.String("key", odKey), zap.String("from", srcKey),
		zap.Float64("ava", src.Available), zap.Float64("leftPend", leftPending))
	// The remaining pending amount is returned to available (positive or negative)
	src.Available += leftPending // 剩余的 pending 金额归还到 available（正负都可能）
	src.lock.Unlock()

	tgt.lock.Lock()
	if toFrozen {
		tgt.Frozens[odKey] = tgtAmount
	} else {
		tgt.Available += tgtAmount
	}
	tgt.lock.Unlock()
	return true
}

/*
Cancel
Unlock the quantity of currency (frozens/pendings) and add it to available again
取消对币种的数量锁定(frozens/pendings)，重新加到available上
*/
func (w *BanWallets) Cancel(odKey string, symbol string, addAmount float64, fromPending bool) {
	wallet, ok := w.Items[symbol]
	if !ok {
		return
	}

	var srcMap map[string]float64
	if fromPending {
		srcMap = wallet.Pendings
	} else {
		srcMap = wallet.Frozens
	}

	wallet.lock.Lock()
	srcAmount, exists := srcMap[odKey]
	if exists {
		delete(srcMap, odKey)
	}
	srcAmount += addAmount

	tag := "frozen"
	if fromPending {
		tag = "pending"
	}
	wallet.Available += srcAmount
	wallet.lock.Unlock()
	log.Debug("cancel to ava", zap.String("tag", tag), zap.Float64("srcAmt", srcAmount),
		zap.String("od", odKey), zap.String("coin", symbol), zap.Float64("ava", wallet.Available))
}

/*
EnterOd
Both real offer and simulation are executed, which can prevent excessive consumption during real offer.

If the balance is insufficient, an exception will be issued
Need to call confirm_od_enter to confirm. You can also call cancel to cancel
实盘和模拟都执行，实盘时可防止过度消费

	如果余额不足，会发出异常
	需要调用confirm_od_enter确认。也可调用cancel取消
*/
func (w *BanWallets) EnterOd(od *ormo.InOutOrder) (float64, *errs.Error) {
	odKey := od.Key()
	exs := orm.GetSymbolByID(int32(od.Sid))
	if exs == nil {
		panic(fmt.Sprintf("EnterOd invalid sid of order: %v", od.Sid))
	}
	var legalCost float64

	curPrice := com.GetPriceSafe(od.Symbol, od.Enter.Side)
	if curPrice < 0 {
		return 0, errs.NewMsg(errs.CodeRunTime, "no valid price: %v", od.Symbol)
	}
	if od.Enter.Quantity != 0 {
		price := od.Enter.Average
		if price == 0 {
			price = curPrice
		}
		legalCost = od.Enter.Quantity * price
	} else {
		legalCost = od.GetInfoFloat64(ormo.OdInfoLegalCost)
	}

	isFuture := banexg.IsContract(exs.Market)
	var quoteCost, quoteMargin float64
	var err *errs.Error

	baseCode, quoteCode, _, _ := core.SplitSymbol(exs.Symbol)
	if isFuture || !od.Short {
		// Futures contract, spot long order lock quote
		// 期货合约，现货多单锁定quote
		if legalCost < core.MinStakeAmount {
			log.Warn(fmt.Sprintf("cost should >= %v, cur: %.2f, order: %v", core.MinStakeAmount, legalCost, od.Key()))
		}

		if isFuture {
			//Futures contract, nominal value = margin * leverage
			//期货合约，名义价值=保证金*杠杆
			legalCost /= od.Leverage
		}

		quoteCost = w.GetAmountByLegal(quoteCode, legalCost)
		quoteCost, err = w.CostAva(odKey, quoteCode, quoteCost, false, 0)

		if err != nil {
			return 0, err
		}

		// Calculate nominal quantity
		// 计算名义数量
		quoteMargin = quoteCost
		if isFuture {
			quoteMargin *= od.Leverage
		}

		od.QuoteCost, err = exg.PrecCost(nil, od.Symbol, quoteMargin)
		if err != nil {
			return 0, err
		}
	} else {
		// Spot short order, lock base, allow amount to be negative
		// 现货空单，锁定base，允许金额为负
		baseCost := w.GetAmountByLegal(baseCode, legalCost)
		baseCost, err = w.CostAva(odKey, baseCode, baseCost, true, 0)
		if err != nil {
			return 0, err
		}
		od.Enter.Quantity = baseCost
	}

	return legalCost, nil
}

func (w *BanWallets) ConfirmOdEnter(od *ormo.InOutOrder, enterPrice float64) {
	if core.EnvReal {
		return
	}
	exs := orm.GetSymbolByID(int32(od.Sid))
	if exs == nil {
		panic(fmt.Sprintf("EnterOd invalid sid of order: %v", od.Sid))
	}
	subOd := od.Enter
	quoteAmount := enterPrice * subOd.Quantity
	curFee := subOd.FeeQuote

	baseCode, quoteCode, _, _ := core.SplitSymbol(exs.Symbol)
	if core.IsContract {
		// Futures contracts only lock the fixed currency and do not involve the increase of base currency.
		// 期货合约，只锁定定价币，不涉及base币的增加
		quoteAmount /= od.Leverage
		gotAmt := quoteAmount - curFee
		w.ConfirmPending(od.Key(), quoteCode, quoteAmount, quoteCode, gotAmt, true)
	} else if od.Short {
		// Sold in stock, handling fee deducted
		// 现货卖，手续费扣U
		gotAmt := quoteAmount - curFee
		w.ConfirmPending(od.Key(), baseCode, subOd.Quantity, quoteCode, gotAmt, true)
	} else {
		// Buy in spot, handling fee will be deducted
		// 现货买，手续费扣币
		baseAmt := subOd.Quantity - curFee/enterPrice
		w.ConfirmPending(od.Key(), quoteCode, quoteAmount, baseCode, baseAmt, false)
	}
}
func (w *BanWallets) ExitOd(od *ormo.InOutOrder, baseAmount float64) {
	if core.EnvReal {
		return
	}
	exs := orm.GetSymbolByID(int32(od.Sid))
	if exs == nil {
		panic(fmt.Sprintf("EnterOd invalid sid of order: %v", od.Sid))
	}
	if banexg.IsContract(exs.Market) {
		// 期货合约，不涉及base币的变化。退出订单时，对锁定的定价币平仓释放
		return
	}
	baseCode, quoteCode, _, _ := core.SplitSymbol(exs.Symbol)
	if od.Short {
		// Spot short orders are sold from the frozen part of the quote, calculated to the available part of the quote, and canceled from the pending unfilled part of the base.
		// 现货空单，从quote的frozen卖，计算到quote的available，从base的pending未成交部分取消
		w.Cancel(od.Key(), baseCode, 0, true)
		// There is no upfront deduction here, the price may be None
		// 这里不用预先扣除，价格可能为None
	} else {
		// For spot multiple orders, sell from the available value of the base, calculate the available value of the quote, and cancel it from the pending unfilled part of the quote.
		// 现货多单，从base的available卖，计算到quote的available，从quote的pending未成交部分取消
		wallet := w.Get(baseCode)

		if wallet.Available > 0 && wallet.Available < baseAmount || math.Abs(wallet.Available/baseAmount-1) <= 0.01 {
			baseAmount = wallet.Available
			// Cancel the pending unfulfilled part of the quote
			// 取消quote的pending未成交部分
			w.Cancel(od.Key(), quoteCode, 0, true)
		}

		if baseAmount > 0 {
			_, err := w.CostAva(od.Key(), baseCode, baseAmount, false, 0.01)
			if err != nil {
				log.Error("exit order fail", zap.String("od", od.Key()))
			}
		}
	}
}

func (w *BanWallets) ConfirmOdExit(od *ormo.InOutOrder, exitPrice float64) {
	if core.EnvReal {
		return
	}
	exs := orm.GetSymbolByID(int32(od.Sid))
	if exs == nil {
		panic(fmt.Sprintf("EnterOd invalid sid of order: %v", od.Sid))
	}
	subOd := od.Exit
	curFee := subOd.FeeQuote
	baseCode, quoteCode, _, _ := core.SplitSymbol(exs.Symbol)
	odKey := od.Key()
	if banexg.IsContract(exs.Market) {
		//Futures contracts do not involve changes in base currency. When exiting the order, the locked pricing currency will be closed and released.
		//Here profit deducts the entry and exit handling fees. The entry handling fee has been deducted previously, so the entry handling fee needs to be added here.
		//期货合约不涉及base币的变化。退出订单时，对锁定的定价币平仓释放
		//这里profit扣除了入场和出场手续费，前面入场手续费已扣过了，所以这里需要加入场手续费
		w.Cancel(odKey, quoteCode, od.Profit+od.Enter.FeeQuote, false)
	} else if od.Short {
		//For short orders, priority is given to buying from the frozen price of the quote. If it is not converted to base, it will be converted to the available price of the quote.
		//空单，优先从quote的frozen买，不兑换为base，再换算为quote的avaiable
		orgAmount := od.Enter.Filled
		if orgAmount > 0 {
			//Execute quote purchase and neutralize base's debt
			//执行quote买入，中和base的欠债
			w.CostFrozen(odKey, quoteCode, orgAmount*exitPrice)
		}
		if exitPrice < od.Enter.Price {
			//For a short order, the exit price is lower than the entry price, there is a profit, and the frozen profit is made available.
			//空单，出场价低于入场价，有利润，将冻结的利润置为available
			w.Cancel(odKey, quoteCode, 0, false)
		}
		quoteAmount := exitPrice*orgAmount + curFee
		w.ConfirmPending(odKey, quoteCode, quoteAmount, baseCode, orgAmount, false)
	} else {
		//For long orders, sell from the base's availability and exchange it for the quote's availability.
		//多单，从base的avaiable卖，兑换为quote的available
		quoteAmount := exitPrice*subOd.Quantity - curFee
		w.ConfirmPending(odKey, baseCode, subOd.Quantity, quoteCode, quoteAmount, false)
	}
}
func (w *BanWallets) CutPart(srcKey string, tgtKey string, symbol string, rate float64) {
	item, exists := w.Items[symbol]
	if !exists {
		return
	}

	item.lock.Lock()
	if value, ok := item.Pendings[srcKey]; ok {
		cutAmt := value * rate
		item.Pendings[tgtKey] += cutAmt
		item.Pendings[srcKey] -= cutAmt
	}

	if value, ok := item.Frozens[srcKey]; ok {
		cutAmt := value * rate
		item.Frozens[tgtKey] += cutAmt
		item.Frozens[srcKey] -= cutAmt
	}
	item.lock.Unlock()
}

/*
UpdateOds
Update order. Currently only for futures contract orders, the margin ratio of contract orders needs to be updated.

Incoming orders must be for the same fixed currency.
Margin ratio: (position notional value * maintenance margin ratio - maintenance margin quick calculation) / (wallet balance + unrealized profit and loss)
Wallet balance = initial net transfer balance (including initial margin) + realized profit and loss + net funding fee - handling fee
更新订单。目前只针对期货合约订单，需要更新合约订单的保证金比率。

	传入的订单必然都是同一个定价币的订单
	保证金比率： (仓位名义价值 * 维持保证金率 - 维持保证金速算数) / (钱包余额 + 未实现盈亏)
	钱包余额 = 初始净划入余额（含初始保证金） + 已实现盈亏 + 净资金费用 - 手续费
*/
func (w *BanWallets) UpdateOds(odList []*ormo.InOutOrder, currency string) *errs.Error {
	if len(odList) == 0 {
		for _, item := range w.Items {
			item.lock.Lock()
			item.UnrealizedPOL = 0
			item.UsedUPol = 0
			item.lock.Unlock()
		}
		return nil
	}
	// All orders are for the same pricing coin, get the wallet of this coin in advance
	// 所有订单都是同一个定价币，提前获取此币的钱包
	wallet := w.Get(currency)

	// Calculate whether to liquidate your position
	// 计算是否爆仓
	var totProfit float64
	for _, od := range odList {
		totProfit += od.Profit
	}
	wallet.lock.Lock()
	wallet.UnrealizedPOL = totProfit
	wallet.UsedUPol = 0
	wallet.lock.Unlock()
	if totProfit < 0 {
		marginRatio := math.Abs(totProfit) / wallet.Total(false)
		if marginRatio > 0.99 {
			// The total loss exceeds the total assets and the position is liquidated.
			// 总亏损超过总资产，爆仓
			wallet.Reset()
			return errs.NewMsg(core.ErrLiquidation, "Account Wallet Liquidation")
		}
	}

	exchange := exg.Default
	for _, od := range odList {
		if od.Enter == nil || od.Enter.Filled == 0 {
			continue
		}
		curPrice := com.GetPriceSafe(od.Symbol, "")
		if curPrice == -1 {
			continue
		}
		// Calculate nominal value
		// 计算名义价值
		quoteValue := od.Enter.Filled * curPrice
		// Calculate current required margin
		// 计算当前所需保证金
		curMargin := quoteValue / od.Leverage
		// Determine whether the price trend and order opening direction are the same
		// 判断价格走势和开单方向是否相同
		odDirt := 1.0
		if od.Short {
			odDirt = -1.0
		}
		odKey := od.Key()
		isGood := (curPrice - od.Enter.Average) * odDirt
		if isGood < 0 && od.Profit < 0 {
			// Generally speaking, when isGood < 0, od.Profit should be < 0, but sometimes the price is updated and the order profit has not been updated, causing od.Profit > 0.
			// If the price trend is different and a loss occurs, determine whether to automatically replenish the margin.
			// Calculate maintenance margin
			// 一般来说isGood < 0时应该od.Profit < 0，但有时候价格更新了，订单利润尚未更新导致od.Profit > 0
			// 价格走势不同，产生亏损，判断是否自动补充保证金
			// 计算维持保证金
			minMargin, err := exchange.CalcMaintMargin(od.Symbol, quoteValue) // 要求的最低保证金
			if err != nil {
				return err
			}
			if math.Abs(od.Profit) >= (curMargin-minMargin)*config.MarginAddRate {
				// When the loss reaches the initial margin ratio, increase the margin for this order to avoid forced liquidation.
				// 当亏损达到初始保证金比例时，为此订单增加保证金避免强平
				lossPct := config.MarginAddRate * 100
				log.Debug("loss addMargin", zap.Float64("lossPct", lossPct),
					zap.String("od", odKey), zap.Float64("profit", od.Profit),
					zap.Float64("margin", curMargin))
				curMargin -= od.Profit
			}
		}
		// The price trend is as expected. Margin required increases
		// 价格走势和预期相同。所需保证金增长
		err := wallet.SetMargin(odKey, curMargin)
		if err != nil {
			log.Debug("cash lack, add margin fail", zap.String("od", odKey), zap.Error(err))
		}
	}
	return nil
}

func (w *BanWallets) GetAmountByLegal(symbol string, legalCost float64) float64 {
	return legalCost / com.GetPrice(symbol, "")
}

func (w *BanWallets) calcLegal(itemAmt func(item *ItemWallet) float64, symbols []string) ([]float64, []string, []float64) {
	var data map[string]*ItemWallet
	if len(symbols) > 0 {
		data = make(map[string]*ItemWallet)
		for _, sym := range symbols {
			if item, ok := w.Items[sym]; ok {
				data[sym] = item
			}
		}
	} else {
		data = w.Items
	}

	amounts := make([]float64, 0)
	coins := make([]string, 0)
	prices := make([]float64, 0)
	var skips []string

	for key, item := range data {
		var price = com.GetPriceSafe(key, "")
		if price == -1 {
			skips = append(skips, key)
			continue
		}
		amounts = append(amounts, itemAmt(item)*price)
		coins = append(coins, key)
		prices = append(prices, price)
	}
	if len(skips) > 0 {
		log.Info("skip pairs in wallet.calcLegal", zap.Int("num", len(skips)),
			zap.String("pairs", strings.Join(skips, ",")))
	}

	return amounts, coins, prices
}

func (w *BanWallets) calculateTotalLegal(valueExtractor func(*ItemWallet) float64, symbols []string) float64 {
	amounts, _, _ := w.calcLegal(valueExtractor, symbols)
	total := 0.0
	for _, amt := range amounts {
		total += amt
	}
	return total
}

func (w *BanWallets) AvaLegal(symbols []string) float64 {
	return w.calculateTotalLegal(func(x *ItemWallet) float64 { return x.Available }, symbols)
}

func (w *BanWallets) TotalLegal(symbols []string, withUPol bool) float64 {
	return w.calculateTotalLegal(func(x *ItemWallet) float64 { return x.Total(withUPol) }, symbols)
}

func (w *BanWallets) UnrealizedPOLLegal(symbols []string) float64 {
	return w.calculateTotalLegal(func(x *ItemWallet) float64 { return x.UnrealizedPOL }, symbols)
}

func (w *BanWallets) GetWithdrawLegal(symbols []string) float64 {
	return w.calculateTotalLegal(func(x *ItemWallet) float64 { return x.Withdraw }, symbols)
}

/*
WithdrawLegal
Withdraw cash from the balance, thereby prohibiting a portion of the money from being billed.
从余额提现，从而禁止一部分钱开单。
*/
func (w *BanWallets) WithdrawLegal(amount float64, symbols []string) {
	amounts, coins, prices := w.calcLegal(func(x *ItemWallet) float64 { return x.Available }, symbols)
	total := 0.0
	for _, amt := range amounts {
		total += amt
	}

	drawAmts := make([]float64, len(amounts))
	for i, amt := range amounts {
		drawAmts[i] = (amt / total) * amount / prices[i]
	}

	for i, drawAmt := range drawAmts {
		item := w.Items[coins[i]]
		item.lock.Lock()
		drawAmt = min(drawAmt, item.Available)
		item.Withdraw += drawAmt
		item.Available -= drawAmt
		item.lock.Unlock()
	}
}

/*
FiatValue
Returns the value of the given currency against fiat currency. Returns all currencies if empty
返回给定币种的对法币价值。为空时返回所有币种
*/
func (w *BanWallets) FiatValue(withUpol bool, symbols ...string) float64 {
	if len(symbols) == 0 {
		for symbol := range w.Items {
			symbols = append(symbols, symbol)
		}
	}

	var totalVal float64
	for _, symbol := range symbols {
		item, exists := w.Items[symbol]
		if !exists {
			continue
		}
		totalVal += item.FiatValue(withUpol)
	}

	return totalVal
}

/*
TryUpdateStakePctAmt
Update single billing amount
Backtesting mode should call this method when the order is closed
更新单笔开单金额
回测模式应在订单平仓时调用此方法
*/
func (w *BanWallets) TryUpdateStakePctAmt() {
	if config.StakePct > 0 {
		acc, ok := config.Accounts[w.Account]
		if ok {
			legalValue := w.TotalLegal(nil, true)
			if banexg.IsContract(core.Market) && config.Leverage > 1 {
				// 对于合约市场，百分比开单应基于带杠杆的名义资产价值
				legalValue *= config.Leverage
			}
			// Round to the nearest tenth place
			// 四舍五入到十位
			pctAmt := math.Round(legalValue*config.StakePct/1000) * 10
			if acc.StakePctAmt == 0 {
				acc.StakePctAmt = pctAmt
			} else if math.Abs(pctAmt/acc.StakePctAmt-1) >= 0.2 {
				// Update only if total assets change by more than 20%
				// 总资产变化超过20%才更新
				date := btime.ToDateStr(btime.TimeMS(), core.DefaultDateFmt)
				log.Debug("stake amount changed by stake_pct", zap.String("d", date),
					zap.Float64("old", acc.StakePctAmt), zap.Float64("new", pctAmt))
				acc.StakePctAmt = pctAmt
			}
		}
	}
}

func EnsurePricesLoaded() {
	_, err := getBookTickers()
	if err != nil {
		log.Error("load ticker prices fail", zap.Error(err))
	}
}

func UpdateWalletByBalances(wallets *BanWallets, item *banexg.Balances) {
	EnsurePricesLoaded()
	var items []*banexg.Asset
	var skips []string
	for coin, it := range item.Assets {
		if it.Total == 0 {
			continue
		}
		record, ok := wallets.Items[coin]
		if ok {
			record.lock.Lock()
			record.Available = it.Free
			record.UnrealizedPOL = it.UPol
			record.lock.Unlock()
		} else {
			record = &ItemWallet{
				Coin:          coin,
				Available:     it.Free,
				UnrealizedPOL: it.UPol,
				Pendings:      make(map[string]float64),
				Frozens:       make(map[string]float64),
			}
			wallets.Items[coin] = record
		}
		record.lock.Lock()
		if core.IsContract {
			record.Pendings["*"] = it.Used
			record.Frozens["*"] = 0
		} else {
			record.Pendings["*"] = 0
			record.Frozens["*"] = it.Used
		}
		record.lock.Unlock()
		coinPrice := com.GetPriceSafe(coin, "")
		if coinPrice == -1 {
			skips = append(skips, coin)
			continue
		}
		items = append(items, &banexg.Asset{
			Code:  coin,
			Free:  it.Free,
			Used:  it.Used,
			UPol:  it.UPol,
			Total: it.Total * coinPrice,
		})
	}
	// Update single billing amount
	// 更新单笔开单金额
	wallets.TryUpdateStakePctAmt()
	slices.SortFunc(items, func(a, b *banexg.Asset) int {
		return -int((a.Total - b.Total) * 100)
	})
	var msgList []string
	for _, it := range items {
		msgList = append(msgList, fmt.Sprintf("%s: %.5f/%.5f/%.5f", it.Code, it.Free, it.Used, it.UPol))
	}
	if len(skips) > 0 {
		log.Info(fmt.Sprintf("update balance skips: %s", strings.Join(skips, "  ")))
	}
	if len(msgList) > 0 {
		log.Debug(fmt.Sprintf("update balances %s: %s", wallets.Account, strings.Join(msgList, "  ")))
	}
}

/*
WatchLiveBalances
币安推送的余额经常不够及时导致不准确，推荐定期主动拉取更新
*/
func WatchLiveBalances() {
	for account := range config.Accounts {
		wallets := GetWallets(account)
		if wallets.IsWatch {
			continue
		}
		out, err := exg.Default.WatchBalance(map[string]interface{}{
			banexg.ParamAccount: account,
		})
		if err != nil {
			log.Error("watch balance err", zap.Error(err))
			return
		}
		wallets.IsWatch = true
		go func() {
			defer func() {
				wallets.IsWatch = false
			}()
			for item := range out {
				UpdateWalletByBalances(wallets, item)
			}
		}()
	}
}
