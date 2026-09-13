package live

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/opt"
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
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/gofiber/fiber/v2"
	"github.com/sasha-s/go-deadlock"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

func regApiBiz(api fiber.Router) {
	newAPIHandlers(nil).regApiBiz(api)
}

// apiHandlers owns the concrete runtime dependencies used by one API server.
// A nil deps value preserves the legacy process-global entrypoint only.
type apiHandlers struct {
	deps   *biz.RuntimeDeps
	remote *biz.RemoteCommandService
}

func newAPIHandlers(deps *biz.RuntimeDeps) *apiHandlers {
	h := &apiHandlers{deps: deps}
	if deps == nil {
		h.remote = biz.NewRemoteCommandService()
	} else {
		h.remote = biz.NewRemoteCommandServiceWithRuntimeDeps(*deps)
	}
	return h
}

func (h *apiHandlers) regApiBiz(api fiber.Router) {
	api.Get("/version", h.getVersion)
	api.Get("/balance", h.getBalance)
	api.Post("/refresh_wallet", h.postRefreshWallet)
	api.Get("/today_num", h.getTodayNum)
	api.Get("/statistics", h.getStatistics)
	api.Get("/incomes", h.getIncomes)
	api.Get("/task_pairs", h.getTaskPairs)
	api.Get("/exs_map", h.getExsMap)
	api.Get("/orders", h.getOrders)
	api.Post("/calc_profits", h.postCalcProfits)
	api.Post("/exit_order", h.postExitOrder)
	api.Post("/close_exg_pos", h.postCloseExgPos)
	api.Post("/delay_entry", h.postDelayEntry)
	api.Get("/config", h.getConfig)
	api.Get("/stg_jobs", h.getStratJobs)
	api.Get("/performance", h.getPerformance)
	api.Post("/start_down_trade", h.postStartDownTrade)
	api.Get("/get_down_trade", h.getDownTrade)
	api.Get("/group_sta", h.getGroupSta)
	api.Get("/log", h.getLog)
	api.Get("/bot_info", h.getBotInfo)
}

func (h *apiHandlers) runtime() bool { return h != nil && h.deps != nil }

func (h *apiHandlers) wallet(account string) *biz.BanWallets {
	if h.runtime() {
		if h.deps.Trading == nil {
			return nil
		}
		return h.deps.Trading.Wallet(account)
	}
	return biz.GetWallets(account)
}

func (h *apiHandlers) openOrders(account string) (map[int64]*ormo.InOutOrder, *deadlock.Mutex) {
	if h.runtime() {
		if h.deps.Orders == nil {
			return nil, nil
		}
		return h.deps.Orders.GetOpenODs(account)
	}
	return ormo.GetOpenODs(account)
}

func (h *apiHandlers) taskID(account string) int64 {
	if h.runtime() {
		if h.deps.Orders == nil {
			return -1
		}
		return h.deps.Orders.GetTaskID(account)
	}
	return ormo.GetTaskID(account)
}

func (h *apiHandlers) orderConn(write bool) (*ormo.Queries, *orm.TrackedDB, *errs.Error) {
	if h.runtime() {
		if h.deps.Orders == nil {
			return nil, nil, errs.NewMsg(core.ErrBadConfig, "runtime orders are required")
		}
		return h.deps.Orders.Conn(write)
	}
	return ormo.Conn(orm.DbTrades, write)
}

func (h *apiHandlers) exchange() banexg.BanExchange {
	if h.runtime() {
		return h.deps.Exchange
	}
	return exg.Default
}

func (h *apiHandlers) market() string {
	if h.runtime() && h.deps.Core != nil {
		return h.deps.Core.Market
	}
	return core.Market
}

func (h *apiHandlers) exchangeName() string {
	if h.runtime() && h.deps.Core != nil {
		return h.deps.Core.ExgName
	}
	return core.ExgName
}

func (h *apiHandlers) startAt() int64 {
	if h.runtime() && h.deps.Core != nil {
		return h.deps.Core.StartAt
	}
	return core.StartAt
}

func (h *apiHandlers) noEnterUntil(account string) int64 {
	if h.runtime() {
		if h.deps.Core == nil {
			return 0
		}
		until, _ := h.deps.Core.NoEnterUntilFor(account)
		return until
	}
	until, _ := core.LegacyNoEnterUntilFor(account)
	return until
}

func (h *apiHandlers) runEnv() string {
	if h.runtime() && h.deps.Core != nil {
		return h.deps.Core.RunEnv
	}
	return core.RunEnv
}

func (h *apiHandlers) lastProcessMS() int64 {
	if h.runtime() {
		if h.deps.Market != nil && h.deps.Market.PairCopied != nil {
			return h.deps.Market.PairCopied.LastCopiedMs()
		}
		return 0
	}
	return core.LastCopiedMs
}

func (h *apiHandlers) nowMS() int64 {
	if h.runtime() && h.deps.Clock != nil {
		return h.deps.Clock.TimeMS()
	}
	return btime.UTCStamp()
}

func (h *apiHandlers) price(symbol string) float64 {
	if h.runtime() {
		if h.deps.Market == nil || h.deps.Market.Prices == nil {
			return 0
		}
		return h.deps.Market.Prices.GetPriceSafeExpAt(h.nowMS(), symbol, "", com.PriceExpireMS)
	}
	return com.GetPriceSafe(symbol, "")
}

func (h *apiHandlers) jobs(account string) map[string]map[string]*strat.StratJob {
	if h.runtime() {
		if h.deps.Strategies == nil {
			return nil
		}
		return h.deps.Strategies.JobMapsView(account)
	}
	return strat.GetJobs(account)
}

func (h *apiHandlers) strategyVersions() map[string]int {
	if h.runtime() {
		if h.deps.Strategies == nil {
			return nil
		}
		return h.deps.Strategies.VersionsSnapshot()
	}
	return strat.LegacyVersionsSnapshot()
}

func (h *apiHandlers) configView() *config.Config {
	if h.runtime() {
		return h.deps.ConfigView()
	}
	return &config.Data
}

func (h *apiHandlers) symbolMap() map[string]*orm.ExSymbol {
	if h.runtime() {
		if h.deps.Symbols == nil || h.deps.Core == nil {
			return nil
		}
		return h.deps.Symbols.GetExSymbolMap(h.deps.Core.ExgName, h.deps.Core.Market)
	}
	return orm.GetExSymbolMap(core.ExgName, core.Market)
}

type FnAccCB = func(acc string) error

func wrapAccount(c *fiber.Ctx, cb FnAccCB) error {
	account := c.Get("X-Account")
	if account == "" {
		return fiber.NewError(fiber.StatusBadRequest, "header `X-Account` missing")
	}
	if roles, ok := c.Locals("accounts").(map[string]string); ok {
		if _, allowed := roles[account]; !allowed {
			return fiber.NewError(fiber.StatusForbidden, "account unauthorized")
		}
	}
	return cb(account)
}

func (h *apiHandlers) getVersion(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"version": core.Version,
	})
}

func (h *apiHandlers) getBalance(c *fiber.Ctx) error {
	return wrapAccount(c, func(account string) error {
		wallet := h.wallet(account)
		if wallet == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime trading state is required")
		}
		return c.JSON(fiber.Map{
			"items": h.walletItems(wallet),
			"total": wallet.FiatValue(true),
		})
	})
}

func (h *apiHandlers) walletItems(wallet *biz.BanWallets) []map[string]interface{} {
	items := make([]map[string]interface{}, 0)
	for coin, item := range wallet.Items {
		total := item.Total(true)
		totalFiat := float64(0)
		if total > 0 {
			price := h.price(coin)
			if price > 0 {
				totalFiat = total * price
			}
		}
		items = append(items, map[string]interface{}{
			"symbol":     coin,
			"total":      total,
			"upol":       item.UnrealizedPOL,
			"free":       item.Available,
			"used":       item.Used(),
			"total_fiat": totalFiat,
		})
	}
	return items
}

func (h *apiHandlers) postRefreshWallet(c *fiber.Ctx) error {
	return wrapAccount(c, func(account string) error {
		wallet := h.wallet(account)
		if wallet == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime trading state is required")
		}
		envReal := core.EnvReal
		if h.runtime() {
			if h.deps.Core == nil {
				return errs.NewMsg(core.ErrBadConfig, "runtime core is required")
			}
			envReal = h.deps.Core.EnvReal
		}
		if envReal {
			exchange := h.exchange()
			if exchange == nil {
				return errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
			}
			rsp, err := exchange.FetchBalance(map[string]interface{}{
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
			"items": h.walletItems(wallet),
			"total": wallet.FiatValue(true),
		})
	})
}

func (h *apiHandlers) getTodayNum(c *fiber.Ctx) error {
	return wrapAccount(c, func(acc string) error {
		openOds, lock := h.openOrders(acc)
		if lock == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime orders are required")
		}
		lock.Lock()
		dayOpenNum := len(openOds)
		dayOpenPft := float64(0)
		for _, od := range openOds {
			dayOpenPft += od.Profit
		}
		lock.Unlock()

		// 获取今日完成的订单
		tfMSecs := int64(utils2.TFToSecs("1d") * 1000)
		nowMS := h.nowMS()
		todayStartMS := utils2.AlignTfMSecs(nowMS, tfMSecs)
		taskId := h.taskID(acc)
		dayDoneNum := 0
		dayDonePft := float64(0)
		if taskId > 0 {
			sess, conn, err := h.orderConn(false)
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

func (h *apiHandlers) getStatistics(c *fiber.Ctx) error {
	return wrapAccount(c, func(acc string) error {
		sess, conn, err := h.orderConn(false)
		if err != nil {
			return err
		}
		defer conn.Close()
		taskId := h.taskID(acc)
		orders, err := sess.GetOrders(ormo.GetOrdersArgs{
			TaskID: taskId,
		})
		if err != nil {
			return err
		}
		wallets := h.wallet(acc)
		if wallets == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime trading state is required")
		}
		var totalDuration int64 // All order holding seconds 所有订单持仓秒数
		var profitSum, profitRateSum, totalCost float64
		var doneProfitSum, doneProfitRateSum, doneTotalCost float64
		var curMS = h.nowMS()
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

		jobs := h.jobs(acc)
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
			"botStartMs":        h.startAt(),
			"runTfs":            utils.KeysOfMap(tfMap),
			"exchange":          h.exchangeName(),
			"market":            h.market(),
			"pairs":             utils.KeysOfMap(pairs),
		})
	})
}

func (h *apiHandlers) getOrders(c *fiber.Ctx) error {
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
		sess, conn, err := h.orderConn(false)
		if err != nil {
			return err
		}
		defer conn.Close()
		taskId := h.taskID(acc)
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
				price = h.price(od.Symbol)
				if price > 0 {
					od.UpdateProfits(price)
					err = od.UpdateTrailing(price)
					if err != nil {
						return err
					}
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
		exchange := h.exchange()
		if exchange == nil {
			return errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
		}
		orders, err := exchange.FetchOrders(data.Symbols, data.StartMs, data.Limit, map[string]interface{}{
			banexg.ParamAccount: acc,
		})
		if err != nil {
			return err
		}
		algoOrders, err := exchange.FetchOrders(data.Symbols, data.StartMs, data.Limit, map[string]interface{}{
			banexg.ParamAccount:   acc,
			banexg.ParamAlgoOrder: true,
		})
		if err != nil {
			log.Error("fetch algo orders fail", zap.Error(err))
		} else {
			orders = append(orders, algoOrders...)
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
		exchange := h.exchange()
		if exchange == nil {
			return errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
		}
		posList, err := exchange.FetchPositions(symbols, map[string]interface{}{
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

func (h *apiHandlers) postCalcProfits(c *fiber.Ctx) error {
	return wrapAccount(c, func(acc string) error {
		openOds, lock := h.openOrders(acc)
		if lock == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime orders are required")
		}
		if len(openOds) == 0 {
			return nil
		}
		lock.Lock()
		defer lock.Unlock()
		exchange := h.exchange()
		if exchange == nil {
			return errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
		}
		items, err := exchange.FetchLastPrices(nil, map[string]interface{}{
			banexg.ParamMarket:  h.market(),
			banexg.ParamAccount: acc,
		})
		if err != nil {
			return err
		}
		prices := make(map[string]float64)
		for _, it := range items {
			prices[it.Symbol] = it.Price
		}
		if h.runtime() {
			if h.deps.Market == nil || h.deps.Market.Prices == nil {
				return errs.NewMsg(core.ErrBadConfig, "runtime prices are required")
			}
			h.deps.Market.Prices.SetPricesAt(h.nowMS(), prices, "")
		} else {
			com.SetPrices(prices, "")
		}
		fails := make(map[string]bool)
		for _, od := range openOds {
			if price, ok := prices[od.Symbol]; ok {
				od.UpdateProfits(price)
				err = od.UpdateTrailing(price)
				if err != nil {
					return err
				}
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

func (h *apiHandlers) postExitOrder(c *fiber.Ctx) error {
	type ForceExitArgs struct {
		OrderID string `json:"orderId" validate:"required"`
	}
	var data = new(ForceExitArgs)
	if err := base.VerifyArg(c, data, base.ArgBody); err != nil {
		return err
	}

	return wrapAccount(c, func(acc string) error {
		var orderID int64
		all := data.OrderID == "all"
		if data.OrderID == "all" {
			orderID = 0
		} else {
			parsed, err := strconv.ParseInt(data.OrderID, 10, 64)
			if err != nil {
				return fiber.NewError(fiber.StatusBadRequest, "invalid order id")
			}
			orderID = parsed
		}
		res, err := h.remote.CloseOrders(biz.RemoteCommand{
			Source:      biz.RemoteSourceWeb,
			Actor:       c.IP(),
			Account:     acc,
			Idempotency: c.Get("Idempotency-Key"),
			OrderID:     orderID,
			All:         all,
			Confirmed:   true,
			ExitTag:     core.ExitTagUserExit,
		})
		var errMsg string
		if err != nil {
			errMsg = err.Short()
			res = &biz.RemoteCommandResult{}
		}

		return c.JSON(fiber.Map{
			"closeNum":   res.CloseNum,
			"failNum":    res.FailNum,
			"idempotent": res.Idempotent,
			"errMsg":     errMsg,
		})
	})
}

func (h *apiHandlers) postCloseExgPos(c *fiber.Ctx) error {
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
		doneNum := 0
		res, err := h.remote.Run(biz.RemoteCommand{
			Source:      biz.RemoteSourceWeb,
			Actor:       c.IP(),
			Account:     acc,
			Action:      biz.RemoteActionCloseOrder,
			Idempotency: c.Get("Idempotency-Key"),
			All:         data.Symbol == "all",
			Confirmed:   true,
			ExitTag:     core.ExitTagUserExit,
			ExecClose: func() (int, int, *errs.Error) {
				var reqs []*CloseArgs
				if data.Symbol == "all" {
					exchange := h.exchange()
					if exchange == nil {
						return 0, 0, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
					}
					posList, err := exchange.FetchPositions(nil, map[string]interface{}{
						banexg.ParamAccount: acc,
					})
					if err != nil {
						return 0, 0, err
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
				closeNum := 0
				for _, q := range reqs {
					side := "sell"
					if q.Side == "short" {
						side = "buy"
					}
					params := map[string]interface{}{
						banexg.ParamAccount:       acc,
						banexg.ParamClientOrderId: fmt.Sprintf("bandash_%v", rand.Intn(1000)),
					}
					if banexg.IsContract(h.market()) {
						params[banexg.ParamPositionSide] = strings.ToUpper(q.Side)
					}
					res, err := h.exchange().CreateOrder(q.Symbol, q.OrderType, side, q.Amount, q.Price, params)
					if err != nil {
						return closeNum, 0, err
					}
					if res.ID != "" {
						closeNum += 1
						if res.Filled == res.Amount {
							doneNum += 1
						}
					}
				}
				return closeNum, 0, nil
			},
		})
		if err != nil {
			if res == nil {
				res = &biz.RemoteCommandResult{}
			}
			return err
		}
		return c.JSON(fiber.Map{
			"closeNum":   res.CloseNum,
			"doneNum":    doneNum,
			"idempotent": res.Idempotent,
		})
	})
}

func (h *apiHandlers) getIncomes(c *fiber.Ctx) error {
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
		exchange := h.exchange()
		if exchange == nil {
			return errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
		}
		items, err := exchange.FetchIncomeHistory(data.InType, data.Symbol, data.StartTime, data.Limit, map[string]interface{}{
			banexg.ParamAccount: acc,
		})
		if err != nil {
			return err
		}
		return c.JSON(fiber.Map{"data": items})
	})
}

func (h *apiHandlers) postDelayEntry(c *fiber.Ctx) error {
	type DelayArgs struct {
		Secs float64 `json:"secs"`
	}
	var data = new(DelayArgs)
	if err := base.VerifyArg(c, data, base.ArgBody); err != nil {
		return err
	}
	return wrapAccount(c, func(acc string) error {
		untilMS := h.nowMS() + int64(data.Secs*1000)
		res, err := h.remote.Run(biz.RemoteCommand{
			Source:      biz.RemoteSourceWeb,
			Actor:       c.IP(),
			Account:     acc,
			Action:      biz.RemoteActionTradingSwitch,
			Idempotency: c.Get("Idempotency-Key"),
			UntilMS:     untilMS,
		})
		if err != nil {
			return err
		}
		return c.JSON(fiber.Map{
			"allowTradeAt": res.UntilMS,
			"idempotent":   res.Idempotent,
		})
	})
}

func (h *apiHandlers) getConfig(c *fiber.Ctx) error {
	// 因在线更新配置有很多限制，大多数配置无法即刻生效，故暂不提供在线修改
	cfg := h.configView()
	if cfg == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime configuration is required")
	}
	data, err := cfg.Desensitize().DumpYaml()
	if err != nil {
		return err
	}
	return c.SendString(string(data))
}

func (h *apiHandlers) getStratJobs(c *fiber.Ctx) error {
	type JobItem struct {
		Pair      string  `json:"pair"`
		Strategy  string  `json:"strategy"`
		TF        string  `json:"tf"`
		Price     float64 `json:"price"`
		OdNum     int     `json:"odNum"`
		LastBarMS int64   `json:"lastBarMS"`
	}
	return wrapAccount(c, func(acc string) error {
		jobs := h.jobs(acc)
		items := make([]*JobItem, 0, len(jobs))
		openOds, lock := h.openOrders(acc)
		if lock == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime orders are required")
		}
		lock.Lock()
		defer lock.Unlock()
		for pairTF, jobMap := range jobs {
			arr := strings.Split(pairTF, "_")
			price := h.price(arr[0])
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
			"strats": h.strategyVersions(),
		})
	})
}

func (h *apiHandlers) getTaskPairs(c *fiber.Ctx) error {
	type PairArgs struct {
		Start int64 `query:"start"`
		Stop  int64 `query:"stop"`
	}
	var data = new(PairArgs)
	if err_ := base.VerifyArg(c, data, base.ArgQuery); err_ != nil {
		return err_
	}
	return wrapAccount(c, func(acc string) error {
		sess, conn, err := h.orderConn(false)
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx := context.Background()
		taskId := h.taskID(acc)
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

func (h *apiHandlers) getExsMap(c *fiber.Ctx) error {
	exsMap := h.symbolMap()
	if exsMap == nil && h.runtime() {
		return errs.NewMsg(core.ErrBadConfig, "runtime symbols and core are required")
	}
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

func (h *apiHandlers) getPerformance(c *fiber.Ctx) error {
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
		sess, conn, err := h.orderConn(false)
		if err != nil {
			return err
		}
		defer conn.Close()
		taskId := h.taskID(acc)
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

func (h *apiHandlers) getGroupSta(c *fiber.Ctx) error {
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
		sess, conn, err := h.orderConn(false)
		if err != nil {
			return err
		}
		defer conn.Close()
		taskId := h.taskID(acc)
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

func (h *apiHandlers) postStartDownTrade(c *fiber.Ctx) error {
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
	return wrapAccount(c, func(acc string) error {
		exchange := h.exchange()
		if exchange == nil {
			return errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
		}
		rsp, err := exg.StartAccountDownload(exchange, data.Source, acc, startMS, endMS, h.nowMS())
		if err != nil {
			return err
		}
		return c.Type("json").SendString(rsp.Content)
	})
}

func (h *apiHandlers) getDownTrade(c *fiber.Ctx) error {
	type DownArgs struct {
		ID     string `query:"id" validate:"required"`
		Source string `query:"source" validate:"required"`
	}
	var data = new(DownArgs)
	if err_ := base.VerifyArg(c, data, base.ArgQuery); err_ != nil {
		return err_
	}
	return wrapAccount(c, func(acc string) error {
		exchange := h.exchange()
		if exchange == nil {
			return errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
		}
		rsp, err := exg.GetAccountDownload(exchange, data.Source, acc, data.ID, h.nowMS())
		if err != nil {
			return err
		}
		return c.Type("json").SendString(rsp.Content)
	})
}

func (h *apiHandlers) getLog(c *fiber.Ctx) error {
	type LogArgs struct {
		End   int64 `query:"end"`   // 结束位置坐标，0表示末尾
		Limit int64 `query:"limit"` // 读取字节数大小
	}
	var args = new(LogArgs)
	if err_ := base.VerifyArg(c, args, base.ArgQuery); err_ != nil {
		return err_
	}
	logFile := log.LogFilePath()
	if logFile == "" {
		return c.JSON(fiber.Map{"code": 400, "msg": "no log file"})
	}
	data, pos, err := utils.ReadFileTail(logFile, args.Limit, args.End)
	if err != nil {
		return err
	}
	return c.JSON(fiber.Map{
		"data":  string(data),
		"start": pos,
	})
}

func (h *apiHandlers) getBotInfo(c *fiber.Ctx) error {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		return err
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return err
	}
	return wrapAccount(c, func(acc string) error {
		stopUntil := h.noEnterUntil(acc)
		return c.JSON(fiber.Map{
			"cpuPct":       percent[0],
			"ramPct":       v.UsedPercent,
			"lastProcess":  h.lastProcessMS(),
			"env":          h.runEnv(),
			"allowTradeAt": stopUntil,
		})
	})
}
