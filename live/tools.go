package live

import (
	"flag"
	"fmt"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

func RunTradeClose(args []string) error {
	parser := flag.NewFlagSet("", flag.ExitOnError)
	var accountStr, pairStr, stratStr string
	var isExg bool
	var configs config.ArrString
	parser.Var(&configs, "config", "config path to use, Multiple -config options may be used")
	parser.StringVar(&accountStr, "account", "", "accounts, comma separated, empty means all")
	parser.StringVar(&pairStr, "pair", "", "pairs, comma separated, empty means all")
	parser.StringVar(&stratStr, "strat", "", "strats, comma separated, empty means all")
	parser.BoolVar(&isExg, "exg", false, "close exchange position directly")
	err_ := parser.Parse(args)
	if err_ != nil {
		return err_
	}
	core.SetRunMode(core.RunModeLive)
	err := config.LoadConfig(&config.CmdArgs{
		Configs:  configs,
		LogLevel: "info",
	})
	if err != nil {
		return err
	}

	// 解析命令行参数
	var accMap = utils.SplitToMap(accountStr, ",")
	var pairMap = utils.SplitToMap(pairStr, ",")
	var stratMap = utils.SplitToMap(stratStr, ",")

	// 初始化订单管理器
	err = biz.SetupComsExg(&config.CmdArgs{LogLevel: "info"})
	if err != nil {
		return err
	}

	// 查找符合要求的订单并平仓
	err = cancelPendingOrders(accMap, pairMap)
	if err != nil {
		log.Error("cancelPendingOrders fail", zap.Error(err))
	}
	if isExg {
		return closeOrdersByPos(accMap, pairMap)
	} else {
		return closeOrdersByLocal(accMap, pairMap, stratMap)
	}
}

func closeOrdersByPos(accMap map[string]bool, pairMap map[string]bool) error {
	exchange := exg.Default
	odType := banexg.OdTypeMarket
	closeNum := 0
	for account := range config.Accounts {
		if len(accMap) > 0 {
			if _, accOk := accMap[account]; !accOk {
				continue
			}
		}
		posList, err := exchange.FetchAccountPositions(nil, map[string]interface{}{
			banexg.ParamAccount: account,
		})
		if err != nil {
			return err
		}
		log.Info("fetch account pos", zap.String("acc", account), zap.Int("num", len(posList)))
		exitPos := make([]*banexg.Position, 0, len(posList))
		for _, pos := range posList {
			if _, ok := pairMap[pos.Symbol]; !ok && len(pairMap) > 0 {
				continue
			}
			exitPos = append(exitPos, pos)
		}
		if len(exitPos) > 0 {
			for _, pos := range exitPos {
				isShort := pos.Side == banexg.PosSideShort
				exitSide := banexg.OdSideSell
				params := map[string]interface{}{
					banexg.ParamAccount:       account,
					banexg.ParamClientOrderId: fmt.Sprintf("bancli_%v", rand.Intn(1000)),
				}
				if core.IsContract {
					params[banexg.ParamPositionSide] = "LONG"
					if isShort {
						params[banexg.ParamPositionSide] = "SHORT"
						exitSide = banexg.OdSideBuy
					}
				}
				closeNum += 1
				res, err := exchange.CreateOrder(pos.Symbol, odType, exitSide, pos.Contracts, 0, params)
				if err != nil {
					return err
				}
				if res.Status == "filled" {
					log.Info("close pos ok", zap.String("acc", account), zap.String("pair", pos.Symbol),
						zap.String("side", pos.Side), zap.Float64("price", res.Average),
						zap.Float64("amount", res.Filled))
				} else {
					log.Warn("close fail", zap.String("acc", account), zap.String("pair", pos.Symbol),
						zap.String("side", pos.Side), zap.String("status", res.Status))
				}
			}
		}
	}
	log.Info("try close exchange positions", zap.Int("num", closeNum))
	return nil
}

// cancelPendingOrders 取消所有账户在交易所的未成交挂单
func cancelPendingOrders(accMap map[string]bool, pairMap map[string]bool) *errs.Error {
	exchange := exg.Default
	cancelNum := 0
	for account := range config.Accounts {
		if len(accMap) > 0 {
			if _, accOk := accMap[account]; !accOk {
				continue
			}
		}
		// 获取账户的所有未成交挂单
		openOrders, err := exchange.FetchOpenOrders("", 0, 1000, map[string]interface{}{
			banexg.ParamAccount: account,
		})
		if err != nil {
			return err
		}
		log.Info("fetch account open orders", zap.String("acc", account), zap.Int("num", len(openOrders)))

		// 筛选需要取消的订单
		cancelOrders := make([]*banexg.Order, 0, len(openOrders))
		for _, order := range openOrders {
			if _, ok := pairMap[order.Symbol]; !ok && len(pairMap) > 0 {
				continue
			}
			// 只取消未成交或部分成交的订单
			if order.Status == "open" || order.Status == "partially_filled" {
				cancelOrders = append(cancelOrders, order)
			}
		}

		// 取消订单
		if len(cancelOrders) > 0 {
			for _, order := range cancelOrders {
				cancelNum += 1
				res, err := exchange.CancelOrder(order.ID, order.Symbol, map[string]interface{}{
					banexg.ParamAccount: account,
				})
				if err != nil {
					log.Error("cancel order fail", zap.String("acc", account),
						zap.String("pair", order.Symbol), zap.String("orderId", order.ID),
						zap.Error(err))
					continue
				}
				if res.Status == "canceled" || res.Status == "cancelled" {
					log.Info("cancel order ok", zap.String("acc", account),
						zap.String("pair", order.Symbol), zap.String("orderId", order.ID),
						zap.String("side", order.Side), zap.Float64("amount", order.Amount))
				} else {
					log.Warn("cancel order unexpected status", zap.String("acc", account),
						zap.String("pair", order.Symbol), zap.String("orderId", order.ID),
						zap.String("status", res.Status))
				}
			}
		}
	}
	log.Info("try cancel pending orders", zap.Int("num", cancelNum))
	return nil
}

func closeOrdersByLocal(accMap map[string]bool, pairMap map[string]bool, stratMap map[string]bool) error {
	err := ormo.InitTask(true, config.GetDataDir())
	if err != nil {
		return err
	}
	biz.InitLiveOrderMgr(sendOrderMsg)
	biz.StartLiveOdMgr()
	checkAccs := make(map[string][]*ormo.InOutOrder)
	closeNum := 0
	for account := range config.Accounts {
		if len(accMap) > 0 {
			if _, accOk := accMap[account]; !accOk {
				continue
			}
		}
		odMgr := biz.GetLiveOdMgr(account)
		oldList, newList, delList, err := odMgr.SyncExgOrders()
		if err != nil {
			return err
		}
		openOds, lock := ormo.GetOpenODs(account)
		var exitOds []*ormo.InOutOrder
		lock.Lock()
		msg := fmt.Sprintf("orders: %d restored, %d deleted, %d added, %d opened", len(oldList), len(delList), len(newList), len(openOds))
		log.Info(msg)
		for _, od := range openOds {
			if _, ok := pairMap[od.Symbol]; !ok && len(pairMap) > 0 {
				continue
			}
			if _, ok := stratMap[od.Strategy]; !ok && len(stratMap) > 0 {
				continue
			}
			exitOds = append(exitOds, od)
		}
		lock.Unlock()
		if len(exitOds) > 0 {
			checkAccs[account] = exitOds
			log.Info("try exit orders", zap.String("acc", account), zap.Int("num", len(exitOds)))
			exit_, _, err := biz.CloseAccOrders(account, exitOds, &strat.ExitReq{Tag: core.ExitTagCli})
			if err != nil {
				return err
			}
			closeNum += exit_
		}
	}
	log.Info("closed orders", zap.Int("num", closeNum))
	if len(checkAccs) == 0 {
		log.Info("no match open orders to close")
		return nil
	}
	// 5s 超时
	timeout := btime.TimeMS() + 5000
	for {
		core.Sleep(time.Second)
		for account, odList := range checkAccs {
			openOds := make([]*ormo.InOutOrder, 0)
			for _, od := range odList {
				if od.Status >= ormo.InOutStatusFullExit {
					continue
				}
				openOds = append(openOds, od)
			}
			if len(openOds) == 0 {
				delete(checkAccs, account)
			} else {
				log.Info("still open", zap.String("acc", account), zap.Int("num", len(openOds)))
			}
		}
		if len(checkAccs) == 0 || btime.TimeMS() > timeout {
			break
		}
	}
	return nil
}
