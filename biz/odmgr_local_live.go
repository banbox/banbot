package biz

import (
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"strings"
	"time"
)

type LocalLiveOrderMgr struct {
	LocalOrderMgr
}

func InitLocalLiveOrderMgr(callBack FnOdCb, showLog bool) {
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		_, ok := accOdMgrs[account]
		if !ok {
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
			odMgr.afterEnter = makeAfterEnterLocalLive(odMgr)
			odMgr.afterExit = makeAfterExitLocalLive(odMgr)
			accOdMgrs[account] = odMgr
		}
	}
}

func getBookTickers() (map[string]*banexg.Ticker, *errs.Error) {
	key := fmt.Sprintf("%s_%s_bookTicker", core.ExgName, core.Market)
	cacheVal, exist := core.Cache.Get(key)
	var tickerMap map[string]*banexg.Ticker
	if !exist {
		tickers, err := exg.Default.FetchTickers(nil, map[string]interface{}{
			banexg.ParamMethod: "bookTicker",
		})
		if err != nil {
			return nil, err
		}
		tickerMap = make(map[string]*banexg.Ticker)
		for _, t := range tickers {
			tickerMap[t.Symbol] = t
			com.SetPrice(t.Symbol, t.Ask, t.Bid)
		}
		log.Info("load book prices", zap.String("mkt", core.Market), zap.Int("num", len(tickerMap)))
		core.Cache.SetWithTTL(key, tickerMap, 0, time.Millisecond*1500)
	} else {
		tickerMap = cacheVal.(map[string]*banexg.Ticker)
	}
	return tickerMap, nil
}

func getAskBidPrice(symbol string) (float64, float64, *errs.Error) {
	tickerMap, err := getBookTickers()
	if err != nil {
		return 0, 0, err
	}
	if tick, ok := tickerMap[symbol]; ok {
		return tick.Ask, tick.Bid, nil
	}
	return 0, 0, errs.NewMsg(core.ErrInvalidSymbol, "symbol %s not found in %v tickers", symbol, len(tickerMap))
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
	odType := config.OrderType
	if exOd.OrderType != "" {
		odType = exOd.OrderType
	}
	if !strings.Contains(odType, banexg.OdTypeMarket) || od.Stop > 0 {
		// 限价单或触发价格，按推送价格处理
		return nil
	}
	// 市价单立刻撮合成交
	ask, bid, err := getAskBidPrice(od.Symbol)
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
		err = o.fillPendingEnter(od, fillPrice, btime.UTCStamp())
	} else {
		err = o.fillPendingExit(od, fillPrice, btime.UTCStamp())
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

func CallLocalLiveOdMgrsKline(msg *data.KLineMsg, bars []*banexg.Kline) *errs.Error {
	if len(bars) == 0 {
		return nil
	}
	for account, mgr := range accOdMgrs {
		liveMgr, ok := mgr.(*LocalLiveOrderMgr)
		if !ok {
			continue
		}
		openOds, lock := ormo.GetOpenODs(account)
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
		var lastK *orm.InfoKline
		timeFrame := utils.SecsToTF(msg.TFSecs)
		for _, k := range bars {
			lastK = &orm.InfoKline{
				PairTFKline: &banexg.PairTFKline{
					Kline:     *k,
					Symbol:    msg.Pair,
					TimeFrame: timeFrame,
				},
			}
			barEndMS := k.Time + int64(msg.TFSecs*1000)
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
			_, err := liveMgr.fillPendingOrders(allodOds, lastK)
			if err != nil {
				return err
			}
		}
		err := liveMgr.updateProfitAndWallets(allOpens, curOds, lastK)
		if err != nil {
			return err
		}
		if core.RunEnv == core.RunEnvDryRun {
			if err := SaveDryRunWalletSnapshot(account, btime.TimeMS(), false); err != nil {
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

func (o *LocalLiveOrderMgr) CleanUp() *errs.Error {
	if err := SaveDryRunWalletSnapshot(o.Account, 0, true); err != nil {
		log.Warn("save dry_run wallet state fail", zap.Error(err))
	}
	openOds, lock := ormo.GetOpenODs(o.Account)
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
