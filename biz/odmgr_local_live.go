package biz

import (
	"strings"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

type LocalLiveOrderMgr struct {
	LocalOrderMgr
}

func InitLocalLiveOrderMgr(callBack FnOdCb, showLog bool) {
	initLocalLiveOrderMgr(nil, callBack, showLog)
}

// InitLocalLiveOrderMgrWithRuntimeDeps binds dry-run fills to one Runtime's
// prices, clock, and exchange. The legacy initializer remains package-scoped.
func InitLocalLiveOrderMgrWithRuntimeDeps(deps RuntimeDeps, callBack FnOdCb, showLog bool) {
	initLocalLiveOrderMgr(&deps, callBack, showLog)
}

func initLocalLiveOrderMgr(deps *RuntimeDeps, callBack FnOdCb, showLog bool) {
	managers := accOdMgrs
	if deps != nil && deps.Trading != nil {
		deps.Trading.ensure()
		managers = deps.Trading.OrderManagers
	}
	accounts := executionAccountConfigs(deps)
	for account, cfg := range accounts {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		_, ok := managers[account]
		if !ok || deps != nil {
			odMgr := &LocalLiveOrderMgr{
				LocalOrderMgr: LocalOrderMgr{
					OrderMgr: OrderMgr{
						callBack: callBack,
						Account:  account,
					},
					showLog:  showLog,
					zeroAmts: make(map[string]int),
				},
			}
			if deps != nil {
				odMgr.bindRuntimeDeps(*deps)
			}
			odMgr.afterEnter = makeAfterEnterLocalLive(odMgr)
			odMgr.afterExit = makeAfterExitLocalLive(odMgr)
			managers[account] = odMgr
		}
	}
}

func getAskBidPrice(symbol string) (float64, float64, *errs.Error) {
	tickerMap, err := com.GetBookTickers()
	if err != nil {
		return 0, 0, err
	}
	if tick, ok := tickerMap[symbol]; ok {
		return tick.Ask, tick.Bid, nil
	}
	return 0, 0, errs.NewMsg(core.ErrInvalidSymbol, "symbol %s not found in %v tickers", symbol, len(tickerMap))
}

func (o *LocalLiveOrderMgr) getAskBidPrice(symbol string) (float64, float64, *errs.Error) {
	if !o.runtimeDeps {
		return getAskBidPrice(symbol)
	}
	if err := o.ensureLatestPrice(symbol); err != nil {
		return 0, 0, err
	}
	ask := o.prices.GetPriceSafeExpAt(o.priceNow(), symbol, banexg.OdSideSell, com.PriceExpireMS)
	bid := o.prices.GetPriceSafeExpAt(o.priceNow(), symbol, banexg.OdSideBuy, com.PriceExpireMS)
	if ask <= 0 || bid <= 0 {
		return 0, 0, errs.NewMsg(core.ErrInvalidSymbol, "symbol %s has no valid ask/bid price", symbol)
	}
	return ask, bid, nil
}

func makeAfterEnterLocalLive(o *LocalLiveOrderMgr) FuncHandleIOrder {
	return func(order *ormo.InOutOrder) *errs.Error {
		return tryFillLocalLiveOrder(o, order, true)
	}
}

func makeAfterExitLocalLive(o *LocalLiveOrderMgr) FuncHandleIOrder {
	return func(order *ormo.InOutOrder) *errs.Error {
		return tryFillLocalLiveOrder(o, order, false)
	}
}

func tryFillLocalLiveOrder(o *LocalLiveOrderMgr, od *ormo.InOutOrder, isEnter bool) *errs.Error {
	exOd := od.Enter
	if !isEnter {
		exOd = od.Exit
	}
	if exOd == nil {
		return errs.NewMsg(core.ErrRunTime, "ExOrder is required for tryFillLocalLiveOrder: %v %v", od.Key(), isEnter)
	}
	odType := o.orderType()
	if exOd.OrderType != "" {
		odType = exOd.OrderType
	}
	if !strings.Contains(odType, banexg.OdTypeMarket) || od.Stop > 0 {
		// 限价单或触发价格，按推送价格处理
		return nil
	}
	// 市价单立刻撮合成交
	ask, bid, err := o.getAskBidPrice(od.Symbol)
	if err != nil {
		return err
	}
	fillPrice := ask
	if od.Short == isEnter {
		// 做空入场，做多离场，都是吃买单
		fillPrice = bid
	}
	tag := "exit"
	if isEnter {
		tag = "enter"
	}
	log.Info("try fill market "+tag, zap.String("od", od.Key()), zap.Float64("price", fillPrice))
	if isEnter {
		err = o.fillPendingEnter(od, fillPrice, o.priceNow())
	} else {
		err = o.fillPendingExit(od, fillPrice, o.priceNow())
	}
	if err != nil {
		return err
	}
	if od.IsDirty() {
		err = od.Save()
		if err != nil {
			log.Error("save order fail", zap.String("acc", o.Account),
				zap.String("key", od.Key()), zap.Error(err))
		}
	}
	return nil
}

func CallLocalLiveOdMgrsData(msg *data.SeriesMsg, rows []*orm.DataSeries) *errs.Error {
	return callLocalLiveOdMgrsData(nil, msg, rows)
}

// CallLocalLiveOdMgrsDataWithRuntime dispatches dry-run fills through one
// runtime's local managers and order registry.
func CallLocalLiveOdMgrsDataWithRuntime(deps RuntimeDeps, msg *data.SeriesMsg, rows []*orm.DataSeries) *errs.Error {
	return callLocalLiveOdMgrsData(&deps, msg, rows)
}

func callLocalLiveOdMgrsData(deps *RuntimeDeps, msg *data.SeriesMsg, rows []*orm.DataSeries) *errs.Error {
	if len(rows) == 0 {
		return nil
	}
	managers := accOdMgrs
	var orderState *ormo.OrderState
	if deps != nil && deps.Trading != nil {
		deps.Trading.ensure()
		managers = deps.Trading.OrderManagers
		orderState = deps.Orders
	}
	for account, mgr := range managers {
		liveMgr, ok := mgr.(*LocalLiveOrderMgr)
		if !ok {
			continue
		}
		openOds, lock := getLocalLiveOpenOrders(orderState, account)
		lock.Lock()
		curOdMap := make(map[int64]int64)
		var curOds = make([]*ormo.InOutOrder, 0, len(openOds))
		for _, od := range openOds {
			if od.Symbol == msg.Pair {
				curOds = append(curOds, od)
				curOdMap[od.ID] = od.Status
			}
		}
		allOpens := utils.ValsOfMap(openOds)
		lock.Unlock()
		if len(curOds) == 0 {
			continue
		}
		var lastEvt *orm.DataSeries
		for _, row := range rows {
			lastEvt = row.CloneWithExSymbol(&orm.ExSymbol{Symbol: msg.Pair})
			barEndMS := row.EndMS
			allodOds := make([]*ormo.InOutOrder, 0, len(curOds))
			for _, od := range curOds {
				exod := getPendingSub(od)
				if exod == nil {
					exod = od.Enter
				}
				if exod != nil && exod.CreateAt >= barEndMS {
					// 过滤晚于此k线的订单
					continue
				}
				allodOds = append(allodOds, od)
			}
			_, err := liveMgr.fillPendingOrders(allodOds, lastEvt)
			if err != nil {
				return err
			}
		}
		err := liveMgr.updateProfitAndWallets(allOpens, curOds, lastEvt)
		if err != nil {
			return err
		}
		if liveMgr.runEnv() == core.RunEnvDryRun {
			var err *errs.Error
			if liveMgr.runtimeDeps {
				err = saveWalletSnapshotWithRuntimeDeps(&liveMgr.walletDeps, account, liveMgr.priceNow(), false)
			} else {
				err = SaveDryRunWalletSnapshot(account, liveMgr.priceNow(), false)
			}
			if err != nil {
				log.Warn("save dry_run wallet snapshot fail", zap.Error(err))
			}
		}
		for _, od := range curOds {
			oldStatus := curOdMap[od.ID]
			if od.Status != oldStatus {
				statusText, _ := ormo.InOutStatusMap[od.Status]
				log.Info("order status change", zap.String("od", od.Key()), zap.String("status", statusText))
			}
		}
	}
	return nil
}

func getLocalLiveOpenOrders(state *ormo.OrderState, account string) (map[int64]*ormo.InOutOrder, *deadlock.Mutex) {
	if state != nil {
		return state.GetOpenODs(account)
	}
	return ormo.GetOpenODs(account)
}

func CallLocalLiveOdMgrsSeries(msg *data.SeriesMsg, rows []*orm.DataSeries) *errs.Error {
	return CallLocalLiveOdMgrsData(msg, rows)
}

func (o *LocalLiveOrderMgr) CleanUp() *errs.Error {
	var snapshotErr *errs.Error
	if o.runtimeDeps {
		snapshotErr = saveWalletSnapshotWithRuntimeDeps(&o.walletDeps, o.Account, 0, true)
	} else {
		snapshotErr = SaveDryRunWalletSnapshot(o.Account, 0, true)
	}
	if snapshotErr != nil {
		log.Warn("save dry_run wallet state fail", zap.Error(snapshotErr))
	}
	openOds, lock := o.openOrders()
	lock.Lock()
	var needSaveOds []*ormo.InOutOrder
	for _, od := range openOds {
		if od.IsDirty() {
			needSaveOds = append(needSaveOds, od)
		}
	}
	lock.Unlock()

	// 保存所有需要保存的订单到数据库
	if len(needSaveOds) > 0 {
		for _, od := range needSaveOds {
			err := od.Save()
			if err != nil {
				log.Warn("save order fail", zap.String("acc", o.Account),
					zap.String("key", od.Key()), zap.Error(err))
				return err
			}
		}
	}
	log.Info("cleanUp for LocalLiveOrderMgr", zap.Int("num", len(needSaveOds)))

	if len(o.zeroAmts) > 0 {
		log.Warn("prec amount to zero", zap.Any("times", o.zeroAmts))
	}
	return nil
}
