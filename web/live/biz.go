package live

import (
	"context"
	"errors"
	"fmt"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banexg/binance"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banbot/web/base"
	"github.com/banbox/banexg"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/gofiber/fiber/v2"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

func regApiBiz(api fiber.Router) {
	api.Get("/version", getVersion)
	api.Get("/balance", getBalance)
	api.Post("/refresh_wallet", postRefreshWallet)
	api.Get("/today_num", getTodayNum)
	api.Get("/statistics", getStatistics)
	api.Get("/incomes", getIncomes)
	api.Get("/task_pairs", getTaskPairs)
	api.Get("/exs_map", getExsMap)
	api.Get("/orders", getOrders)
	api.Post("/calc_profits", postCalcProfits)
	api.Post("/exit_order", postExitOrder)
	api.Post("/close_exg_pos", postCloseExgPos)
	api.Post("/delay_entry", postDelayEntry)
	api.Get("/config", getConfig)
	api.Get("/stg_jobs", getStratJobs)
	api.Get("/performance", getPerformance)
	api.Post("/start_down_trade", postStartDownTrade)
	api.Get("/get_down_trade", getDownTrade)
	api.Get("/group_sta", getGroupSta)
	api.Get("/log", getLog)
	api.Get("/bot_info", getBotInfo)
}

type FnAccCB = func(acc string) error
type FnAccDbCB = func(acc string, sess *orm.Queries) error

func wrapAccount(c *fiber.Ctx, cb FnAccCB) error {
	account := c.Get("X-Account")
	if account == "" {
		return fiber.NewError(fiber.StatusBadRequest, "header `X-Account` missing")
	}
	return cb(account)
}

func wrapAccDb(c *fiber.Ctx, cb FnAccDbCB) error {
	return wrapAccount(c, func(acc string) error {
		ctx := context.Background()
		sess, conn, err := orm.Conn(ctx)
		if err != nil {
			return err
		}
		err_ := cb(acc, sess)
		conn.Release()
		return err_
	})
}

func getVersion(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"version": core.Version,
	})
}

func getBalance(c *fiber.Ctx) error {
	return wrapAccount(c, func(account string) error {
		wallet := biz.GetWallets(account)
		return c.JSON(fiber.Map{
			"items": walletItems(wallet),
			"total": wallet.FiatValue(true),
		})
	})
}

func walletItems(wallet *biz.BanWallets) []map[string]interface{} {
	items := make([]map[string]interface{}, 0)
	for coin, item := range wallet.Items {
		total := item.Total(true)
		items = append(items, map[string]interface{}{
			"symbol":     coin,
			"total":      total,
			"upol":       item.UnrealizedPOL,
			"free":       item.Available,
			"used":       item.Used(),
			"total_fiat": total * core.GetPrice(coin, ""),
		})
	}
	return items
}

func postRefreshWallet(c *fiber.Ctx) error {
	return wrapAccount(c, func(account string) error {
		wallet := biz.GetWallets(account)
		if core.EnvReal {
			rsp, err := exg.Default.FetchBalance(map[string]interface{}{
				banexg.ParamAccount: account,
			})
			if err != nil {
				return err
			}
			biz.UpdateWalletByBalances(wallet, rsp)
			rsp.Info = nil
			log.Info("RefreshWallet", zap.String("acc", account), zap.Any("rsp", rsp))
		}
		return c.JSON(fiber.Map{
			"items": walletItems(wallet),
			"total": wallet.FiatValue(true),
		})
	})
}

func getTodayNum(c *fiber.Ctx) error {
	return wrapAccount(c, func(acc string) error {
		openOds, lock := ormo.GetOpenODs(acc)
		lock.Lock()
		dayOpenNum := len(openOds)
		dayOpenPft := float64(0)
		for _, od := range openOds {
			dayOpenPft += od.Profit
		}
		lock.Unlock()

		// 获取今日完成的订单
		tfMSecs := int64(utils2.TFToSecs("1d") * 1000)
		nowMS := btime.UTCStamp()
		todayStartMS := utils2.AlignTfMSecs(nowMS, tfMSecs)
		taskId := ormo.GetTaskID(acc)
		dayDoneNum := 0
		dayDonePft := float64(0)
		if taskId > 0 {
			sess, conn, err := ormo.Conn(orm.DbTrades, false)
			if err != nil {
				return err
			}
			defer conn.Close()
			orders, err := sess.GetOrders(ormo.GetOrdersArgs{
				TaskID:      taskId,
				Status:      2, // 已完成状态
				CloseAfter:  todayStartMS,
				CloseBefore: nowMS,
			})
			if err != nil {
				return err
			}
			for _, od := range orders {
				dayDonePft += od.Profit
			}
			dayDoneNum = len(orders)
		}
		return c.JSON(fiber.Map{
			"running":    taskId > 0,
			"dayDoneNum": dayDoneNum,
			"dayDonePft": dayDonePft,
			"dayOpenNum": dayOpenNum,
			"dayOpenPft": dayOpenPft,
		})
	})
}

func getStatistics(c *fiber.Ctx) error {
	return wrapAccount(c, func(acc string) error {
		sess, conn, err := ormo.Conn(orm.DbTrades, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		taskId := ormo.GetTaskID(acc)
		orders, err := sess.GetOrders(ormo.GetOrdersArgs{
			TaskID: taskId,
		})
		if err != nil {
			return err
		}
		wallets := biz.GetWallets(acc)
		var totalDuration int64 // All order holding seconds 所有订单持仓秒数
		var profitSum, profitRateSum, totalCost float64
		var doneProfitSum, doneProfitRateSum, doneTotalCost float64
		var curMS = btime.UTCStamp()
		var odNum, winNum, lossNum, doneNum int
		var winValue, lossValue float64
		var bestPair string
		var bestRate float64
		var curDay int64
		var dayProfitSum float64
		var dayProfits []float64 // Daily Profit 每日利润
		var dayMSecs = int64(utils2.TFToSecs("1d") * 1000)
		for _, od := range orders {
			if od.Status < ormo.InOutStatusPartEnter || od.Status > ormo.InOutStatusFullExit {
				continue
			}
			odNum += 1
			durat := od.RealExitMS() - od.RealEnterMS()
			if durat < 0 {
				durat = curMS - od.RealEnterMS()
			}
			totalDuration += durat / 1000
			profitSum += od.Profit
			profitRateSum += od.ProfitRate
			totalCost += od.EnterCost()
			if od.Status == ormo.InOutStatusFullExit {
				doneNum += 1
				if od.ProfitRate > bestRate {
					bestRate = od.ProfitRate
					bestPair = od.Symbol
				}
				if od.Profit > 0 {
					winNum += 1
					winValue += od.Profit
				} else {
					lossNum += 1
					lossValue -= od.Profit
				}
				doneProfitSum += od.Profit
				doneProfitRateSum += od.ProfitRate
				doneTotalCost += od.EnterCost()
				curDayMS := utils2.AlignTfMSecs(od.RealEnterMS(), dayMSecs)
				if curDay == 0 || curDay == curDayMS {
					dayProfitSum += od.Profit
				} else {
					dayProfits = append(dayProfits, dayProfitSum)
					curDay = curDayMS
					dayProfitSum = 0
				}
			}
		}
		if dayProfitSum > 0 {
			dayProfits = append(dayProfits, dayProfitSum)
		}
		doneProfitMean := doneProfitSum / float64(max(1, doneNum))
		profitMean := profitSum / float64(max(1, odNum))
		profitFactor := winValue / max(1e-6, math.Abs(lossValue))
		winRate := float64(winNum) / float64(max(1, doneNum))

		firstEntMs, lastEntMs := int64(0), int64(0)
		if odNum > 0 {
			firstEntMs = orders[0].RealEnterMS()
			lastEntMs = orders[len(orders)-1].RealEnterMS()
		}

		expProfit, expRatio := utils.CalcExpectancy(dayProfits)
		initBalance := wallets.TotalLegal(nil, true) - profitSum
		ddPct, ddVal, _, _, _, _ := utils.CalcMaxDrawDown(dayProfits, initBalance)

		jobs := strat.GetJobs(acc)
		pairs := make(map[string]bool)
		tfMap := make(map[string]bool)
		for key := range jobs {
			arr := strings.Split(key, "_")
			pairs[arr[0]] = true
			tfMap[arr[1]] = true
		}
		return c.JSON(fiber.Map{
			"doneProfitMean":    doneProfitMean,
			"doneProfitPctMean": doneProfitRateSum / float64(max(1, doneNum)) * 100,
			"doneProfitSum":     doneProfitSum,
			"doneProfitPctSum":  doneProfitSum / max(1e-6, doneTotalCost) * 100,
			"allProfitMean":     profitMean,
			"allProfitPctMean":  profitRateSum / float64(max(1, odNum)) * 100,
			"allProfitSum":      profitSum,
			"allProfitPctSum":   profitSum / max(1e-6, totalCost) * 100,
			"orderNum":          odNum,
			"doneOrderNum":      doneNum,
			"firstOdTs":         firstEntMs / 1000,
			"lastOdTs":          lastEntMs / 1000,
			"avgDuration":       totalDuration / int64(max(1, odNum)),
			"bestPair":          bestPair,
			"bestProfitPct":     bestRate,
			"winNum":            winNum,
			"lossNum":           lossNum,
			"profitFactor":      profitFactor,
			"winRate":           winRate,
			"expectancy":        expProfit,
			"expectancyRatio":   expRatio,
			"maxDrawdownPct":    ddPct * 100,
			"maxDrawdownVal":    ddVal,
			"totalCost":         totalCost,
			"botStartMs":        core.StartAt,
			"runTfs":            utils.KeysOfMap(tfMap),
			"exchange":          core.ExgName,
			"market":            core.Market,
			"pairs":             utils.KeysOfMap(pairs),
		})
	})
}

func getOrders(c *fiber.Ctx) error {
	type OrderArgs struct {
		StartMs   int64  `query:"startMs"`
		StopMs    int64  `query:"stopMs"`
		Limit     int    `query:"limit"`
		AfterID   int    `query:"afterId"`
		Symbols   string `query:"symbols"`
		Status    string `query:"status"`
		Dirt      string `query:"dirt"`
		Strategy  string `query:"strategy"`
		TimeFrame string `query:"timeFrame"`
		Source    string `query:"source" validate:"required"`
		EnterTag  string `query:"enterTag"`
		ExitTag   string `query:"exitTag"`
	}
	var data = new(OrderArgs)
	if err := base.VerifyArg(c, data, base.ArgQuery); err != nil {
		return err
	}
	type OdWrap struct {
		*ormo.InOutOrder
		CurPrice float64 `json:"curPrice"`
	}
	getBotOrders := func(acc string) error {
		sess, conn, err := ormo.Conn(orm.DbTrades, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		taskId := ormo.GetTaskID(acc)
		var symbols []string
		if data.Symbols != "" {
			symbols = strings.Split(data.Symbols, ",")
		}
		var status = 0
		if data.Status == "open" || data.Status == "wait" {
			status = 1
		} else if data.Status == "his" {
			status = 2
		}
		var odDirt = 0
		if data.Dirt == "long" {
			odDirt = core.OdDirtLong
		} else if data.Dirt == "short" {
			odDirt = core.OdDirtShort
		}
		orders, err := sess.GetOrders(ormo.GetOrdersArgs{
			TaskID:      taskId,
			Strategy:    data.Strategy,
			Pairs:       symbols,
			TimeFrame:   data.TimeFrame,
			Status:      status,
			Dirt:        odDirt,
			CloseAfter:  data.StartMs,
			CloseBefore: data.StopMs,
			Limit:       data.Limit,
			AfterID:     data.AfterID,
			EnterTag:    data.EnterTag,
			ExitTag:     data.ExitTag,
		})
		if err != nil {
			return err
		}
		odList := make([]*OdWrap, 0, len(orders))
		minStatus, maxStatus := int64(ormo.InOutStatusInit), int64(ormo.InOutStatusDelete)
		if data.Status == "open" {
			minStatus = ormo.InOutStatusPartEnter
			maxStatus = ormo.InOutStatusPartExit
		} else if data.Status == "wait" {
			minStatus = ormo.InOutStatusInit
			maxStatus = ormo.InOutStatusInit
		}
		for _, od := range orders {
			if od.Status < minStatus || od.Status > maxStatus {
				continue
			}
			price := float64(0)
			if od.ExitTag != "" && od.Exit != nil && od.Exit.Price > 0 {
				price = od.Exit.Price
			} else {
				price = core.GetPriceSafe(od.Symbol, "")
				if price > 0 {
					od.UpdateProfits(price)
				}
			}
			od.NanInfTo(0)
			odList = append(odList, &OdWrap{
				InOutOrder: od,
				CurPrice:   price,
			})
		}
		sort.Slice(odList, func(i, j int) bool {
			return odList[i].RealEnterMS() > odList[j].RealEnterMS()
		})
		return c.JSON(fiber.Map{
			"data": odList,
		})
	}
	getExgOrders := func(acc string) error {
		orders, err := exg.Default.FetchOrders(data.Symbols, data.StartMs, data.Limit, map[string]interface{}{
			banexg.ParamAccount: acc,
		})
		if err != nil {
			return err
		}
		if data.Dirt != "" {
			filtered := make([]*banexg.Order, 0, len(orders))
			for _, od := range orders {
				if od.PositionSide != data.Dirt {
					continue
				}
				filtered = append(filtered, od)
			}
			orders = filtered
		}
		sort.Slice(orders, func(i, j int) bool {
			return orders[i].Timestamp > orders[j].Timestamp
		})
		return c.JSON(fiber.Map{
			"data": orders,
		})
	}
	getExgPositions := func(acc string) error {
		var symbols []string
		if data.Symbols != "" {
			symbols = strings.Split(data.Symbols, ",")
		}
		posList, err := exg.Default.FetchPositions(symbols, map[string]interface{}{
			banexg.ParamAccount: acc,
		})
		if err != nil {
			return err
		}
		return c.JSON(fiber.Map{
			"data": posList,
		})
	}
	return wrapAccount(c, func(acc string) error {
		if data.Source == "bot" {
			return getBotOrders(acc)
		} else if data.Source == "exchange" {
			return getExgOrders(acc)
		} else if data.Source == "position" {
			return getExgPositions(acc)
		} else {
			return fiber.NewError(fiber.StatusBadRequest, "invalid source")
		}
	})
}

func postCalcProfits(c *fiber.Ctx) error {
	return wrapAccount(c, func(acc string) error {
		openOds, lock := ormo.GetOpenODs(acc)
		if len(openOds) == 0 {
			return nil
		}
		lock.Lock()
		defer lock.Unlock()
		items, err := exg.Default.FetchLastPrices(nil, map[string]interface{}{
			banexg.ParamMarket:  core.Market,
			banexg.ParamAccount: acc,
		})
		if err != nil {
			return err
		}
		prices := make(map[string]float64)
		for _, it := range items {
			prices[it.Symbol] = it.Price
		}
		core.SetPrices(prices, "")
		fails := make(map[string]bool)
		for _, od := range openOds {
			if price, ok := prices[od.Symbol]; ok {
				od.UpdateProfits(price)
			} else {
				fails[od.Symbol] = true
			}
		}
		if len(fails) > 0 {
			failStr := utils.MapToStr(fails, false, 0)
			log.Warn("fetch latest prices fail", zap.String("for", failStr))
		}
		return nil
	})
}

func postExitOrder(c *fiber.Ctx) error {
	type ForceExitArgs struct {
		OrderID string `json:"orderId" validate:"required"`
	}
	var data = new(ForceExitArgs)
	if err := base.VerifyArg(c, data, base.ArgBody); err != nil {
		return err
	}

	return wrapAccount(c, func(acc string) error {
		openOds, lock := ormo.GetOpenODs(acc)
		lock.Lock()

		var targetOrders []*ormo.InOutOrder
		if data.OrderID == "all" {
			targetOrders = utils2.ValsOfMap(openOds)
		} else {
			orderID, err := strconv.ParseInt(data.OrderID, 10, 64)
			if err != nil {
				lock.Unlock()
				return fiber.NewError(fiber.StatusBadRequest, "invalid order id")
			}
			for _, od := range openOds {
				if od.ID == orderID {
					targetOrders = append(targetOrders, od)
					break
				}
			}
			if len(targetOrders) == 0 {
				lock.Unlock()
				return fiber.NewError(fiber.StatusNotFound, "order not found")
			}
		}
		lock.Unlock()

		closeNum, failNum, err := biz.CloseAccOrders(acc, targetOrders, &strat.ExitReq{
			Tag:   core.ExitTagUserExit,
			Force: true,
		})
		var errMsg string
		if err != nil {
			errMsg = err.Short()
		}

		return c.JSON(fiber.Map{
			"closeNum": closeNum,
			"failNum":  failNum,
			"errMsg":   errMsg,
		})
	})
}

func postCloseExgPos(c *fiber.Ctx) error {
	type CloseArgs struct {
		Symbol    string  `json:"symbol" validate:"required"`
		Side      string  `json:"side"`
		Amount    float64 `json:"amount"`
		OrderType string  `json:"orderType"`
		Price     float64 `json:"price"`
	}
	var data = new(CloseArgs)
	if err := base.VerifyArg(c, data, base.ArgBody); err != nil {
		return err
	}
	return wrapAccount(c, func(acc string) error {
		var reqs []*CloseArgs
		if data.Symbol == "all" {
			posList, err := exg.Default.FetchPositions(nil, map[string]interface{}{
				banexg.ParamAccount: acc,
			})
			if err != nil {
				return err
			}
			for _, p := range posList {
				reqs = append(reqs, &CloseArgs{
					Symbol:    p.Symbol,
					Side:      p.Side,
					Amount:    p.Contracts,
					OrderType: banexg.OdTypeMarket,
				})
			}
		} else {
			reqs = append(reqs, data)
		}
		closeNum, doneNum := 0, 0
		for _, q := range reqs {
			side := "sell"
			if q.Side == "short" {
				side = "buy"
			}
			params := map[string]interface{}{
				banexg.ParamAccount:       acc,
				banexg.ParamClientOrderId: fmt.Sprintf("bandash_%v", rand.Intn(1000)),
			}
			if banexg.IsContract(core.Market) {
				params[banexg.ParamPositionSide] = strings.ToUpper(q.Side)
			}
			res, err := exg.Default.CreateOrder(q.Symbol, q.OrderType, side, q.Amount, q.Price, params)
			if err != nil {
				return err
			}
			if res.ID != "" {
				closeNum += 1
				if res.Filled == res.Amount {
					doneNum += 1
				}
			}
		}
		return c.JSON(fiber.Map{
			"closeNum": closeNum,
			"doneNum":  doneNum,
		})
	})
}

func getIncomes(c *fiber.Ctx) error {
	type CloseArgs struct {
		InType    string `query:"intype" validate:"required"`
		Symbol    string `query:"symbol"`
		StartTime int64  `query:"startTime"`
		Limit     int    `query:"limit"`
	}
	var data = new(CloseArgs)
	if err := base.VerifyArg(c, data, base.ArgQuery); err != nil {
		return err
	}
	return wrapAccount(c, func(acc string) error {
		items, err := exg.Default.FetchIncomeHistory(data.InType, data.Symbol, data.StartTime, data.Limit, map[string]interface{}{
			banexg.ParamAccount: acc,
		})
		if err != nil {
			return err
		}
		return c.JSON(fiber.Map{"data": items})
	})
}

func postDelayEntry(c *fiber.Ctx) error {
	type DelayArgs struct {
		Secs float64 `json:"secs"`
	}
	var data = new(DelayArgs)
	if err := base.VerifyArg(c, data, base.ArgBody); err != nil {
		return err
	}
	return wrapAccount(c, func(acc string) error {
		untilMS := btime.UTCStamp() + int64(data.Secs*1000)
		core.NoEnterUntil[acc] = untilMS
		return c.JSON(fiber.Map{
			"allowTradeAt": untilMS,
		})
	})
}

func getConfig(c *fiber.Ctx) error {
	// 因在线更新配置有很多限制，大多数配置无法即刻生效，故暂不提供在线修改
	data, err := config.DumpYaml(true)
	if err != nil {
		return err
	}
	return c.SendString(string(data))
}

func getStratJobs(c *fiber.Ctx) error {
	type JobItem struct {
		Pair      string  `json:"pair"`
		Strategy  string  `json:"strategy"`
		TF        string  `json:"tf"`
		Price     float64 `json:"price"`
		OdNum     int     `json:"odNum"`
		LastBarMS int64   `json:"lastBarMS"`
	}
	return wrapAccount(c, func(acc string) error {
		jobs := strat.GetJobs(acc)
		items := make([]*JobItem, 0, len(jobs))
		openOds, lock := ormo.GetOpenODs(acc)
		lock.Lock()
		defer lock.Unlock()
		for pairTF, jobMap := range jobs {
			arr := strings.Split(pairTF, "_")
			price := core.GetPriceSafe(arr[0], "")
			for stgName, job := range jobMap {
				var odNum = 0
				for _, od := range openOds {
					if od.Symbol == arr[0] && od.Timeframe == arr[1] && od.Strategy == stgName {
						odNum += 1
					}
				}
				item := &JobItem{
					Pair:      arr[0],
					TF:        arr[1],
					Strategy:  stgName,
					Price:     price,
					OdNum:     odNum,
					LastBarMS: job.LastBarMS,
				}
				items = append(items, item)
			}
		}
		return c.JSON(fiber.Map{
			"jobs":   items,
			"strats": strat.Versions,
		})
	})
}

func getTaskPairs(c *fiber.Ctx) error {
	type PairArgs struct {
		Start int64 `query:"start"`
		Stop  int64 `query:"stop"`
	}
	var data = new(PairArgs)
	if err_ := base.VerifyArg(c, data, base.ArgQuery); err_ != nil {
		return err_
	}
	return wrapAccount(c, func(acc string) error {
		sess, conn, err := ormo.Conn(orm.DbTrades, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx := context.Background()
		taskId := ormo.GetTaskID(acc)
		if data.Stop == 0 {
			data.Stop = math.MaxInt64
		}
		pairs, err_ := sess.GetTaskPairs(ctx, ormo.GetTaskPairsParams{
			TaskID:    taskId,
			EnterAt:   data.Start,
			EnterAt_2: data.Stop,
		})
		if err_ != nil {
			return err_
		}
		return c.JSON(fiber.Map{"pairs": pairs})
	})
}

func getExsMap(c *fiber.Ctx) error {
	exsMap := orm.GetExSymbolMap(core.ExgName, core.Market)
	return c.JSON(fiber.Map{
		"data": exsMap,
	})
}

type GroupItem struct {
	Key       string             `json:"key"`
	HoldHours float64            `json:"holdHours"`
	TotalCost float64            `json:"totalCost"`
	ProfitSum float64            `json:"profitSum"`
	ProfitPct float64            `json:"profitPct"`
	CloseNum  int                `json:"closeNum"`
	WinNum    int                `json:"winNum"`
	Orders    []*ormo.InOutOrder `json:"-"`
}

func getPerformance(c *fiber.Ctx) error {
	type PerfArgs struct {
		GroupBy   string   `query:"groupBy"`
		Pairs     []string `query:"pairs"`
		StartSecs int64    `query:"startSecs"`
		StopSecs  int64    `query:"stopSecs"`
		Limit     int      `query:"limit"`
	}
	var data = new(PerfArgs)
	if err_ := base.VerifyArg(c, data, base.ArgQuery); err_ != nil {
		return err_
	}
	return wrapAccount(c, func(acc string) error {
		sess, conn, err := ormo.Conn(orm.DbTrades, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		taskId := ormo.GetTaskID(acc)
		orders, err := sess.GetOrders(ormo.GetOrdersArgs{
			TaskID:      taskId,
			Pairs:       data.Pairs,
			Status:      2,
			CloseAfter:  data.StartSecs * 1000,
			CloseBefore: data.StopSecs * 1000,
		})
		if err != nil {
			return err
		}
		var odKey func(od *ormo.InOutOrder) string
		if data.GroupBy == "symbol" {
			odKey = func(od *ormo.InOutOrder) string {
				return od.Symbol
			}
		} else if data.GroupBy == "month" {
			tfMSecs := int64(utils2.TFToSecs("1M") * 1000)
			odKey = func(od *ormo.InOutOrder) string {
				dateMS := utils2.AlignTfMSecs(od.RealEnterMS(), tfMSecs)
				return btime.ToDateStrLoc(dateMS, "2006-01")
			}
		} else if data.GroupBy == "week" {
			tfMSecs := int64(utils2.TFToSecs("1w") * 1000)
			odKey = func(od *ormo.InOutOrder) string {
				dateMS := utils2.AlignTfMSecs(od.RealEnterMS(), tfMSecs)
				return btime.ToDateStrLoc(dateMS, "2006-01-02")
			}
		} else if data.GroupBy == "day" {
			tfMSecs := int64(utils2.TFToSecs("1d") * 1000)
			odKey = func(od *ormo.InOutOrder) string {
				dateMS := utils2.AlignTfMSecs(od.RealEnterMS(), tfMSecs)
				return btime.ToDateStrLoc(dateMS, "2006-01-02")
			}
		} else {
			return c.JSON(fiber.Map{"code": 400, "msg": "unsupport group type: " + data.GroupBy})
		}
		res := groupOrders(orders, odKey)
		enterTags := groupOrders(orders, func(od *ormo.InOutOrder) string {
			return od.EnterTag
		})
		exitTags := groupOrders(orders, func(od *ormo.InOutOrder) string {
			return od.ExitTag
		})
		return c.JSON(fiber.Map{"items": res, "enters": enterTags, "exits": exitTags})
	})
}

func groupOrders(orders []*ormo.InOutOrder, odKey func(od *ormo.InOutOrder) string) []*GroupItem {
	var itemMap = map[string]*GroupItem{}
	hourMSecs := float64(utils2.TFToSecs("1h") * 1000)
	for _, od := range orders {
		key := odKey(od)
		gp, ok := itemMap[key]
		if !ok {
			gp = &GroupItem{Key: key}
			itemMap[key] = gp
		}
		holdHours := float64(od.RealExitMS()-od.RealEnterMS()) / hourMSecs
		gp.CloseNum += 1
		gp.ProfitSum += od.Profit
		gp.TotalCost += od.EnterCost()
		gp.HoldHours += holdHours
		gp.Orders = append(gp.Orders, od)
		if od.Profit > 0 {
			gp.WinNum += 1
		}
	}
	for _, gp := range itemMap {
		if gp.TotalCost > 0 {
			gp.ProfitPct = gp.ProfitSum / gp.TotalCost
		}
		gp.HoldHours /= float64(gp.CloseNum)
	}
	var res = make([]*GroupItem, 0, len(itemMap))
	for _, v := range itemMap {
		res = append(res, v)
	}
	slices.SortFunc(res, func(a, b *GroupItem) int {
		if a.Key <= b.Key {
			return -1
		}
		return 1
	})
	return res
}

type GroupSta struct {
	*GroupItem
	Nums    []int `json:"nums"`
	MinTime int64 `json:"minTime"`
	MaxTime int64 `json:"maxTime"`
}

func getGroupSta(c *fiber.Ctx) error {
	type GroupStaArgs struct {
		Symbol    string `query:"symbol"`
		Strategy  string `query:"strategy"`
		EnterTag  string `query:"enterTag"`
		ExitTag   string `query:"exitTag"`
		GroupBy   string `query:"groupBy"`
		StartTime string `query:"startTime"`
		EndTime   string `query:"endTime"`
	}
	var data = new(GroupStaArgs)
	if err_ := base.VerifyArg(c, data, base.ArgQuery); err_ != nil {
		return err_
	}
	var err_ error
	startMS, endMS := int64(0), int64(0)
	if data.StartTime != "" {
		startMS, err_ = btime.ParseTimeMS(data.StartTime)
		if err_ != nil {
			return err_
		}
	}
	if data.EndTime != "" {
		endMS, err_ = btime.ParseTimeMS(data.EndTime)
		if err_ != nil {
			return err_
		}
	}
	return wrapAccount(c, func(acc string) error {
		sess, conn, err := ormo.Conn(orm.DbTrades, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		taskId := ormo.GetTaskID(acc)
		var symbols []string
		if data.Symbol != "" {
			symbols = strings.Split(data.Symbol, ",")
		}
		orders, err := sess.GetOrders(ormo.GetOrdersArgs{
			TaskID:      taskId,
			Strategy:    data.Strategy,
			Pairs:       symbols,
			CloseAfter:  startMS,
			CloseBefore: endMS,
			EnterTag:    data.EnterTag,
			ExitTag:     data.ExitTag,
		})
		if err != nil {
			return err
		}
		groups := groupOrders(orders, func(od *ormo.InOutOrder) string {
			if data.GroupBy == "strategy" {
				return od.Strategy
			} else if data.GroupBy == "enterTag" {
				return fmt.Sprintf("%v:%v", od.Strategy, od.EnterTag)
			} else if data.GroupBy == "exitTag" {
				return fmt.Sprintf("%v:%v", od.Strategy, od.ExitTag)
			}
			return od.Symbol
		})
		staList := make([]*GroupSta, 0, len(groups))
		for _, g := range groups {
			odNums, minTime, maxTime := opt.SampleOdNums(g.Orders, 300)
			staList = append(staList, &GroupSta{
				GroupItem: g,
				Nums:      odNums,
				MinTime:   minTime,
				MaxTime:   maxTime,
			})
		}
		return c.JSON(fiber.Map{
			"data": staList,
		})
	})
}

func postStartDownTrade(c *fiber.Ctx) error {
	type DownArgs struct {
		StartTime string `json:"startTime" validate:"required"`
		EndTime   string `json:"endTime" validate:"required"`
		Source    string `json:"source" validate:"required"`
	}
	var data = new(DownArgs)
	if err_ := base.VerifyArg(c, data, base.ArgBody); err_ != nil {
		return err_
	}
	startMS, err_ := btime.ParseTimeMS(data.StartTime)
	if err_ != nil {
		return err_
	}
	endMS, err_ := btime.ParseTimeMS(data.EndTime)
	if err_ != nil {
		return err_
	}
	if exg.Default.Info().ID != "binance" {
		return errors.New("exchange not support")
	}
	method := binance.MethodFapiPrivateGetOrderAsyn
	if data.Source == "income" {
		method = binance.MethodFapiPrivateGetIncomeAsyn
	} else if data.Source == "trade" {
		method = binance.MethodFapiPrivateGetTradeAsyn
	}
	return wrapAccount(c, func(acc string) error {
		rsp, err := exg.Default.Call(method, map[string]interface{}{
			banexg.ParamAccount: acc,
			"startTime":         startMS,
			"endTime":           endMS,
			"timestamp":         btime.UTCStamp(),
		})
		if err != nil {
			return err
		}
		return c.Type("json").SendString(rsp.Content)
	})
}

func getDownTrade(c *fiber.Ctx) error {
	type DownArgs struct {
		ID     string `query:"id" validate:"required"`
		Source string `query:"source" validate:"required"`
	}
	var data = new(DownArgs)
	if err_ := base.VerifyArg(c, data, base.ArgQuery); err_ != nil {
		return err_
	}
	if exg.Default.Info().ID != "binance" {
		return errors.New("exchange not support")
	}
	method := binance.MethodFapiPrivateGetOrderAsynId
	if data.Source == "income" {
		method = binance.MethodFapiPrivateGetIncomeAsynId
	} else if data.Source == "trade" {
		method = binance.MethodFapiPrivateGetTradeAsynId
	}
	return wrapAccount(c, func(acc string) error {
		rsp, err := exg.Default.Call(method, map[string]interface{}{
			banexg.ParamAccount: acc,
			"downloadId":        data.ID,
			"timestamp":         btime.UTCStamp(),
		})
		if err != nil {
			return err
		}
		return c.Type("json").SendString(rsp.Content)
	})
}

func getLog(c *fiber.Ctx) error {
	type LogArgs struct {
		End   int64 `query:"end"`   // 结束位置坐标，0表示末尾
		Limit int64 `query:"limit"` // 读取字节数大小
	}
	var args = new(LogArgs)
	if err_ := base.VerifyArg(c, args, base.ArgQuery); err_ != nil {
		return err_
	}
	if core.LogFile == "" {
		return c.JSON(fiber.Map{"code": 400, "msg": "no log file"})
	}
	data, pos, err := utils.ReadFileTail(core.LogFile, args.Limit, args.End)
	if err != nil {
		return err
	}
	return c.JSON(fiber.Map{
		"data":  string(data),
		"start": pos,
	})
}

func getBotInfo(c *fiber.Ctx) error {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		return err
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return err
	}
	return wrapAccount(c, func(acc string) error {
		stopUntil, _ := core.NoEnterUntil[acc]
		return c.JSON(fiber.Map{
			"cpuPct":       percent[0],
			"ramPct":       v.UsedPercent,
			"lastProcess":  core.LastCopiedMs,
			"env":          core.RunEnv,
			"allowTradeAt": stopUntil,
		})
	})
}
