package opt

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/utils"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// CompareOrdersOptions describes one comparison without relying on process
// configuration or the legacy runtime gate.
type CompareOrdersOptions struct {
	Backtest   string
	BotName    string
	Account    string
	AmountRate float64
	SkipUnhit  bool
}

// CompareOrdersDeps are the owned dependencies of one comparison command.
// Identity is read from the stored SID in the backtest file by the entry
// boundary before its runtime is constructed.
type CompareOrdersDeps struct {
	Runtime  biz.RuntimeDeps
	Identity *orm.ExSymbol
	Logger   *zap.Logger
	dateLoc  func(int64, string) string
	legacy   bool
}

type legacyCompareOrdersOptions struct {
	configs config.ArrString
	CompareOrdersOptions
}

/*
CompareExgBTOrders
Compare the exchange export order records with the backtest order records.
对比交易所导出订单记录和回测订单记录。
*/
func CompareExgBTOrders(args []string) error {
	command := NewCompareExgBTOrdersCommand()
	command.SetArgs(args)
	return command.Execute()
}

func NewCompareExgBTOrdersCommand() *cobra.Command {
	options := &legacyCompareOrdersOptions{CompareOrdersOptions: CompareOrdersOptions{AmountRate: 0.1, SkipUnhit: true}}
	command := &cobra.Command{
		Use: "cmp-orders", Aliases: []string{"cmp_orders"},
		Short: "compare exchange orders with a backtest", Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error { return compareExgBTOrdersLegacy(options) },
	}
	command.Flags().StringArrayVar((*[]string)(&options.configs), "config", nil, "config path; may be repeated")
	command.Flags().StringVar(&options.BotName, "bot-name", "", "bot name used for live trading")
	command.Flags().StringVar(&options.Account, "account", "", "account whose API key will fetch orders")
	command.Flags().StringVar(&options.Backtest, "bt-path", "", "backtest order file")
	command.Flags().Float64Var(&options.AmountRate, "amt-rate", 0.1, "amount difference threshold from 0 to 1")
	command.Flags().BoolVar(&options.SkipUnhit, "skip-unhit", true, "skip backtest pairs with no exchange orders")
	return command
}

func compareExgBTOrdersLegacy(options *legacyCompareOrdersOptions) error {
	if options == nil {
		return errs.NewMsg(core.ErrBadConfig, "comparison options are required")
	}
	core.SetRunMode(core.RunModeLive)
	if err := biz.SetupComs(&config.CmdArgs{Configs: options.configs}); err != nil {
		return err
	}
	sid, err := ReadBacktestOrderSID(options.Backtest)
	if err != nil {
		return err
	}
	identity := orm.GetSymbolByID(sid)
	if identity == nil {
		return errs.NewMsg(core.ErrInvalidSymbol, "backtest symbol SID %d not found", sid)
	}
	exchange, err := exg.GetWith(identity.Exchange, identity.Market, "")
	if err != nil {
		return err
	}
	if _, err = orm.LoadMarkets(exchange, false); err != nil {
		return err
	}
	return compareExgBTOrders(options.CompareOrdersOptions, CompareOrdersDeps{
		Identity: identity, Logger: log.L(), legacy: true,
	})
}

// ReadBacktestOrderSID returns the stored symbol identity of the first
// backtest order. The caller must resolve this SID through its own storage.
func ReadBacktestOrderSID(path string) (int32, *errs.Error) {
	orders, _, _, _, err := readBackTestOrders(path)
	if err != nil {
		return 0, errs.New(errs.CodeIOReadFail, err)
	}
	if len(orders) == 0 || orders[0] == nil || orders[0].IOrder == nil || orders[0].Sid == 0 {
		return 0, errs.NewMsg(errs.CodeParamInvalid, "no backtest order symbol SID found")
	}
	return int32(orders[0].Sid), nil
}

// CompareExgBTOrdersWithRuntimeDeps compares downloaded exchange orders with
// a backtest using only one explicit runtime and its resolved identity.
func CompareExgBTOrdersWithRuntimeDeps(options CompareOrdersOptions, deps CompareOrdersDeps) error {
	deps.dateLoc = NewReportDeps(deps.Runtime).dateStrLoc
	return compareExgBTOrders(options, deps)
}

func compareExgBTOrders(options CompareOrdersOptions, deps CompareOrdersDeps) error {
	if options.Account == "" || options.Backtest == "" || options.BotName == "" {
		return errors.New("`exg-path/account` `bt-path` bot-name is required")
	}
	if deps.Identity == nil || deps.Identity.Exchange == "" || deps.Identity.Market == "" {
		return errs.NewMsg(core.ErrBadConfig, "stored backtest symbol identity is required")
	}
	if !deps.legacy {
		if deps.Runtime.Config == nil || deps.Runtime.Config.DataDir == "" || deps.Runtime.Exchange == nil || deps.Runtime.Core == nil {
			return errs.NewMsg(core.ErrBadConfig, "comparison runtime dependencies are required")
		}
		if deps.Runtime.Core.ExgName != deps.Identity.Exchange || deps.Runtime.Core.Market != deps.Identity.Market {
			return errs.NewMsg(core.ErrBadConfig, "comparison runtime identity does not match stored backtest symbol")
		}
	}
	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	dateLoc := deps.dateLoc
	if dateLoc == nil {
		dateLoc = btime.ToDateStrLoc
	}
	btOrders, pairNums, startMS, endMS, err_ := readBackTestOrders(options.Backtest)
	if err_ != nil {
		return err_
	}
	if len(btOrders) == 0 {
		return errors.New("no batcktest orders found")
	}
	dataDir := config.GetDataDir()
	if !deps.legacy {
		dataDir = deps.Runtime.Config.DataDir
	}
	outDir := filepath.Join(dataDir, "exgOrders")
	var exgOrders []*banexg.Order
	var err *errs.Error
	if deps.legacy {
		exgOrders, err = loadExgOrders(options.Account, deps.Identity.Exchange, deps.Identity.Market, startMS, endMS, pairNums)
	} else {
		exgOrders, err = loadExgOrdersWithRuntimeDeps(deps.Runtime, options.Account, deps.Identity.Exchange, deps.Identity.Market, startMS, endMS, pairNums)
	}
	if err != nil {
		return err
	}
	if len(exgOrders) == 0 {
		return errors.New("no exchange orders to compare")
	}
	logger.Info("loaded exchange orders", zap.Int("num", len(exgOrders)))
	pairExgOds := buildExgOrders(exgOrders, options.BotName)
	exgOdList := make([]*ormo.InOutOrder, 0)
	for _, odList := range pairExgOds {
		exgOdList = append(exgOdList, odList...)
	}
	outPath := fmt.Sprintf("%s/cmp_orders.csv", outDir)
	file, err_ := os.Create(outPath)
	if err_ != nil {
		return err_
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	heads := []string{"tag", "symbol", "timeFrame", "dirt", "entAt", "exitAt", "entPrice", "exitPrice", "Amount",
		"Fee", "Profit", "entDelay", "exitDelay", "priceDiff %", "amtDiff %", "feeDiff %",
		"profitDiff %", "profitDf", "reason"}
	if err_ = writer.Write(heads); err_ != nil {
		return err_
	}
	for _, iod := range btOrders {
		tfMSecs := int64(utils2.TFToSecs(iod.Timeframe) * 1000)
		tfMSecsFlt := float64(tfMSecs)
		entFixMS := utils2.AlignTfMSecs(iod.RealEnterMS(), tfMSecs)
		exgOds, _ := pairExgOds[iod.Symbol]
		if options.SkipUnhit && len(exgOds) == 0 {
			continue
		}
		dirt := "long"
		if iod.Short {
			dirt = "short"
		}
		// Find out if there are matching exchange orders
		// 查找是否有匹配的交易所订单
		var matches []*ormo.InOutOrder
		for _, exod := range exgOds {
			if exod.Short == iod.Short && math.Abs(float64(exod.RealEnterMS()-entFixMS)) < tfMSecsFlt {
				amtRate2 := exod.Enter.Filled / iod.Enter.Filled
				if math.Abs(amtRate2-1) <= options.AmountRate {
					matches = append(matches, exod)
				}
			}
		}
		var matOd *ormo.InOutOrder
		if len(matches) > 1 {
			slices.SortFunc(matches, func(a, b *ormo.InOutOrder) int {
				diffA := math.Abs(float64(a.RealExitMS()-iod.RealExitMS()) / 1000)
				diffB := math.Abs(float64(b.RealExitMS()-iod.RealExitMS()) / 1000)
				return int(diffA - diffB)
			})
		}
		if len(matches) > 0 {
			matOd = matches[0]
			unMatches := make([]*ormo.InOutOrder, 0, len(exgOds))
			for _, exod := range exgOds {
				if exod == matOd {
					continue
				}
				unMatches = append(unMatches, exod)
			}
			pairExgOds[iod.Symbol] = unMatches
		}
		if matOd == nil {
			// There is no corresponding record for backtesting orders
			// 回测订单没有对应记录
			entMSStr := dateLoc(iod.RealEnterMS(), "")
			exitMSStr := dateLoc(iod.RealExitMS(), "")
			entPriceStr := strconv.FormatFloat(iod.Enter.Price, 'f', 6, 64)
			amtStr := strconv.FormatFloat(iod.Enter.Filled+iod.Exit.Filled, 'f', 6, 64)
			feeStr := strconv.FormatFloat(iod.Enter.FeeQuote+iod.Exit.FeeQuote, 'f', 6, 64)
			exitPriceStr := strconv.FormatFloat(iod.Exit.Price, 'f', 6, 64)
			profitStr := strconv.FormatFloat(iod.Profit, 'f', 6, 64)
			err_ = writer.Write([]string{"bt", iod.Symbol, iod.Timeframe, dirt, entMSStr, exitMSStr, entPriceStr,
				exitPriceStr, amtStr, feeStr, profitStr, "0", "0", "", "", "", "", "", ""})
			if err_ != nil {
				logger.Error("writer csv fail", zap.Error(err_))
			}
		} else {
			// 有匹配记录
			if matOd.Exit == nil {
				matOd.Exit = &ormo.ExOrder{}
			}
			entMSStr := dateLoc(matOd.RealEnterMS(), "")
			exitMSStr := dateLoc(matOd.RealExitMS(), "")
			entPriceStr := strconv.FormatFloat(matOd.Enter.Average, 'f', 6, 64)
			amtStr := strconv.FormatFloat(matOd.Enter.Filled+matOd.Exit.Filled, 'f', 6, 64)
			feeStr := strconv.FormatFloat(matOd.Enter.FeeQuote+matOd.Exit.FeeQuote, 'f', 6, 64)
			profitStr := strconv.FormatFloat(matOd.Profit, 'f', 6, 64)
			exitPriceStr := strconv.FormatFloat(matOd.Exit.Average, 'f', 6, 64)
			entDelay := matOd.RealEnterMS() - iod.RealEnterMS()
			exitDelay := matOd.RealExitMS() - iod.RealExitMS()
			entDelayStr := strconv.FormatInt(entDelay/1000, 10)
			exitDelayStr := strconv.FormatInt(exitDelay/1000, 10)
			priceDf := (matOd.Enter.Average - iod.Enter.Average) - (matOd.Exit.Average - iod.Exit.Average)
			priceDiff := strconv.FormatFloat(priceDf*100/iod.Enter.Average, 'f', 1, 64)
			amtDf := (matOd.Enter.Filled - iod.Enter.Filled) - (matOd.Exit.Filled - iod.Exit.Filled)
			amountDiff := strconv.FormatFloat(amtDf*100/iod.Enter.Filled, 'f', 1, 64)
			feeDf := (matOd.Enter.FeeQuote - iod.Enter.FeeQuote) + (matOd.Exit.FeeQuote - iod.Exit.FeeQuote)
			feeDiff := strconv.FormatFloat(feeDf*50/iod.Enter.FeeQuote, 'f', 1, 64)
			profitDf := matOd.Profit - iod.Profit
			profitDfPct := profitDf * 100 / iod.Profit
			profitDiff := strconv.FormatFloat(profitDfPct, 'f', 1, 64)
			profitDfStr := strconv.FormatFloat(profitDf, 'f', 6, 64)
			reason := "OK"
			if math.Abs(float64(entDelay)) < tfMSecsFlt && math.Abs(float64(exitDelay)) < tfMSecsFlt {
				// The time of entry and exit is matched
				// 入场和出场的时间匹配
				if math.Abs(profitDfPct) < 20 {
					reason = "OK"
				} else {
					reason = "Slop"
				}
			} else {
				reason = "Wrong"
			}
			err_ = writer.Write([]string{"same", iod.Symbol, iod.Timeframe, dirt, entMSStr, exitMSStr, entPriceStr,
				exitPriceStr, amtStr, feeStr, profitStr, entDelayStr, exitDelayStr, priceDiff, amountDiff,
				feeDiff, profitDiff, profitDfStr, reason})
			if err_ != nil {
				logger.Error("writer csv fail", zap.Error(err_))
			}
		}
	}
	// append unmatch exchange orders
	for _, odList := range pairExgOds {
		for _, iod := range odList {
			dirt := "long"
			if iod.Short {
				dirt = "short"
			}
			if iod.Exit == nil {
				iod.Exit = &ormo.ExOrder{}
			}
			entMSStr := dateLoc(iod.RealEnterMS(), "")
			exitMSStr := dateLoc(iod.RealExitMS(), "")
			entPriceStr := strconv.FormatFloat(iod.Enter.Average, 'f', 6, 64)
			amtStr := strconv.FormatFloat(iod.Enter.Filled+iod.Exit.Filled, 'f', 6, 64)
			feeStr := strconv.FormatFloat(iod.Enter.FeeQuote+iod.Exit.FeeQuote, 'f', 6, 64)
			profitStr := strconv.FormatFloat(iod.Profit, 'f', 6, 64)
			exitPriceStr := strconv.FormatFloat(iod.Exit.Average, 'f', 6, 64)
			err_ = writer.Write([]string{"exg", iod.Symbol, iod.Timeframe, dirt, entMSStr, exitMSStr, entPriceStr,
				exitPriceStr, amtStr, feeStr, profitStr, "0", "0", "", "", "", "", "", ""})
			if err_ != nil {
				logger.Error("writer csv fail", zap.Error(err_))
			}
		}
	}
	logger.Info("dump compare result", zap.String("at", outPath))
	// write raw exchange orders
	outPath = fmt.Sprintf("%s/exg_orders_raw.csv", outDir)
	rows := make([][]string, 0, len(exgOrders)+1)
	rows = append(rows, []string{"symbol", "orderId", "dateTime", "status", "type", "timeInForce", "pos", "side",
		"price", "average", "amount", "filled", "cost", "reduceOnly", "fee"})
	for _, od := range exgOrders {
		price := strconv.FormatFloat(od.Price, 'f', -1, 64)
		average := strconv.FormatFloat(od.Average, 'f', -1, 64)
		amount := strconv.FormatFloat(od.Amount, 'f', -1, 64)
		filled := strconv.FormatFloat(od.Filled, 'f', -1, 64)
		cost := strconv.FormatFloat(od.Cost, 'f', -1, 64)
		reduceOnly := strconv.FormatBool(od.ReduceOnly)
		feeStr := ""
		if od.Fee != nil {
			feeStr = fmt.Sprintf("%s: %.2f", od.Fee.Currency, od.Fee.Cost)
		}
		rows = append(rows, []string{
			od.Symbol, od.ClientOrderID, od.Datetime, od.Status, od.Type,
			od.TimeInForce, od.PositionSide, od.Side,
			price, average, amount, filled, cost, reduceOnly, feeStr,
		})
	}
	err = utils.WriteCsvFile(outPath, rows, false)
	if err != nil {
		return err
	}
	logger.Info("dump exchange raw orders", zap.String("at", outPath))
	outPath = fmt.Sprintf("%s/exg_orders.csv", outDir)
	logger.Info("dump exchange orders", zap.String("at", outPath))
	return DumpOrdersCSV(exgOdList, outPath)
}

func loadExgOrdersWithRuntimeDeps(deps biz.RuntimeDeps, account, exgName, market string, startMS, endMS int64, pairNums map[string]int) ([]*banexg.Order, *errs.Error) {
	save, err := biz.NewExgOrderSetWithRuntimeDeps(deps, account, exgName, market)
	if err != nil {
		return nil, err
	}
	pairs := utils.KeysOfMap(pairNums)
	err = save.Download(startMS, endMS, pairs, true)
	if err != nil {
		return nil, err
	}
	var pairOrders map[string][]*banexg.Order
	pairOrders, err = save.Get(startMS, endMS, pairs, "")
	if err != nil {
		return nil, err
	}
	var exgOrders []*banexg.Order
	for _, odList := range pairOrders {
		exgOrders = append(exgOrders, odList...)
	}
	sort.Slice(exgOrders, func(i, j int) bool {
		return exgOrders[i].Timestamp < exgOrders[j].Timestamp
	})
	return exgOrders, nil
}

func loadExgOrders(account, exgName, market string, startMS, endMS int64, pairNums map[string]int) ([]*banexg.Order, *errs.Error) {
	save, err := biz.GetExgOrderSet(account, exgName, market)
	if err != nil {
		return nil, err
	}
	pairs := utils.KeysOfMap(pairNums)
	if err = save.Download(startMS, endMS, pairs, true); err != nil {
		return nil, err
	}
	pairOrders, err := save.Get(startMS, endMS, pairs, "")
	if err != nil {
		return nil, err
	}
	var orders []*banexg.Order
	for _, items := range pairOrders {
		orders = append(orders, items...)
	}
	sort.Slice(orders, func(i, j int) bool { return orders[i].Timestamp < orders[j].Timestamp })
	return orders, nil
}

func readBackTestOrders(path string) ([]*ormo.InOutOrder, map[string]int, int64, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	if info.IsDir() {
		path = filepath.Join(path, "orders.gob")
	} else if !strings.HasSuffix(path, ".gob") {
		return nil, nil, 0, 0, errors.New("orders.gob path is required")
	}
	orders, err2 := ormo.LoadOrdersGob(path)
	if err2 != nil {
		return nil, nil, 0, 0, err2
	}
	var startMS, endMS int64
	var startTFSecs int
	var maxTfSecs int
	var pairNums = make(map[string]int)
	for _, od := range orders {
		tfSecs := utils2.TFToSecs(od.Timeframe)
		if tfSecs > maxTfSecs {
			maxTfSecs = tfSecs
		}
		curEntMS := od.RealEnterMS()
		if startMS == 0 || curEntMS < startMS {
			startMS = curEntMS
			startTFSecs = tfSecs
		}
		if od.Exit != nil && od.Exit.UpdateAt > endMS {
			endMS = od.Exit.UpdateAt
		}
		num, _ := pairNums[od.Symbol]
		pairNums[od.Symbol] = num + 1
	}
	// 初始时间向前移动半个周期，防止部分订单未记录
	startMS -= int64(startTFSecs * 500)
	// Move the end time back by 2 bars to prevent the exchange order section from being filtered
	// 将结束时间，往后推移2个bar，防止交易所订单部分被过滤
	endMS += int64(maxTfSecs*1000) * 2
	return orders, pairNums, startMS, endMS, nil
}

/*
handleExitOrders 处理平仓订单，返回更新后的订单列表和已使用的平仓数量
remainFilled: 剩余需要平仓的数量
od: 交易所订单
odList: 当前持仓订单列表
excludeOd: 需要排除的订单（已经通过ClientOrderID匹配的订单）
返回值: 更新后的订单列表, 已使用的平仓数量
*/
func handleExitOrders(remainFilled float64, od *banexg.Order, odList []*ormo.InOutOrder, excludeOd *ormo.InOutOrder) ([]*ormo.InOutOrder, float64) {
	newList := make([]*ormo.InOutOrder, 0, len(odList))
	usedAmount := float64(0)
	initAmt := remainFilled
	for _, iod := range odList {
		if iod == excludeOd || iod.Exit != nil || iod.Enter.Side == od.Side {
			newList = append(newList, iod)
			continue
		}
		part := iod
		exitAmount := iod.Enter.Amount
		var rate float64
		if remainFilled < iod.Enter.Amount*0.99 {
			exitAmount = remainFilled
			part = iod.CutPart(remainFilled, remainFilled)
			rate = remainFilled / od.Filled
		} else {
			rate = iod.Enter.Amount / od.Filled
		}
		curFee := od.Fee.Cost * rate
		curFeeQuote := od.Fee.QuoteCost * rate
		part.ExitAt = od.LastUpdateTimestamp
		part.Exit = &ormo.ExOrder{
			Symbol:   part.Symbol,
			CreateAt: od.LastUpdateTimestamp,
			UpdateAt: od.LastUpdateTimestamp,
			Price:    od.Price,
			Average:  od.Average,
			Amount:   part.Enter.Amount,
			Filled:   part.Enter.Filled,
			Fee:      curFee,
			FeeQuote: curFeeQuote,
		}
		part.Status = ormo.InOutStatusFullExit
		part.UpdateProfits(od.Average)
		remainFilled -= exitAmount
		usedAmount += exitAmount
		newList = append(newList, part)
		if part != iod {
			newList = append(newList, iod)
		}
		if remainFilled <= initAmt*0.01 {
			break
		}
	}
	return newList, usedAmount
}

/*
buildExgOrders
Construct an InOutOrder from an exchange order for comparison; It is not used for real/backtesting
从交易所订单构建InOutOrder用于对比；非实盘/回测时使用
*/
func buildExgOrders(ods []*banexg.Order, clientPrefix string) map[string][]*ormo.InOutOrder {
	// 按ClientOrderID分组订单
	orderMap := make(map[string][]*banexg.Order)
	for _, od := range ods {
		if od.Filled == 0 {
			continue
		}
		orderMap[od.ClientOrderID] = append(orderMap[od.ClientOrderID], od)
	}
	jobMap := make(map[string][]*ormo.InOutOrder)
	var temps []*banexg.Order

	for _, orders := range orderMap {
		if len(orders) != 2 {
			// 不是配对订单，加入temps
			temps = append(temps, orders...)
			continue
		}

		// 确保orders[0]是较早的订单
		if orders[0].LastTradeTimestamp > orders[1].LastTradeTimestamp {
			orders[0], orders[1] = orders[1], orders[0]
		}

		// 检查是否是一买一卖
		if orders[0].Side == orders[1].Side {
			temps = append(temps, orders...)
			continue
		}
		ent, exit := orders[0], orders[1]
		if !strings.HasPrefix(ent.ClientOrderID, clientPrefix) || !strings.HasPrefix(exit.ClientOrderID, clientPrefix) {
			temps = append(temps, orders...)
			continue
		}

		// 创建InOutOrder
		iod := &ormo.InOutOrder{
			IOrder: &ormo.IOrder{
				Symbol:  ent.Symbol,
				Short:   ent.Side == banexg.OdSideSell,
				EnterAt: ent.LastTradeTimestamp,
				ExitAt:  exit.LastTradeTimestamp,
				Status:  ormo.InOutStatusFullExit,
			},
			Enter: &ormo.ExOrder{
				Enter:    true,
				Symbol:   ent.Symbol,
				Side:     ent.Side,
				CreateAt: ent.LastTradeTimestamp,
				UpdateAt: ent.LastTradeTimestamp,
				Price:    ent.Price,
				Average:  ent.Average,
				Amount:   ent.Filled,
				Filled:   ent.Filled,
				Fee:      ent.Fee.Cost,
				FeeQuote: ent.Fee.QuoteCost,
				OrderID:  ent.ClientOrderID,
			},
			Exit: &ormo.ExOrder{
				Symbol:   exit.Symbol,
				CreateAt: exit.LastTradeTimestamp,
				UpdateAt: exit.LastTradeTimestamp,
				Price:    exit.Price,
				Average:  exit.Average,
				Amount:   exit.Filled, // 使用入场订单的数量
				Filled:   exit.Filled,
				Fee:      exit.Fee.Cost,
				FeeQuote: exit.Fee.QuoteCost,
			},
		}

		// 将InOutOrder添加到结果map
		jobMap[ent.Symbol] = append(jobMap[ent.Symbol], iod)

		inoutRate := exit.Filled / ent.Filled
		if inoutRate > 1.01 {
			// 平仓数量未耗尽，同时平仓其他订单
			remainOrder := *exit
			remainOrder.Filled = exit.Filled - ent.Filled
			remainOrder.Fee.Cost = exit.Fee.Cost * (remainOrder.Filled / exit.Filled)
			temps = append(temps, &remainOrder)
			iod.Exit.Amount = ent.Filled
			iod.Exit.Filled = ent.Filled
			iod.Exit.Fee = exit.Fee.Cost * (ent.Fee.Cost / exit.Fee.Cost)
			iod.Exit.FeeQuote = exit.Fee.QuoteCost * (ent.Fee.QuoteCost / exit.Fee.QuoteCost)
		} else if inoutRate < 0.99 {
			// 未完全平仓
			remainOrder := *ent
			remainOrder.Filled = ent.Filled - exit.Filled
			remainOrder.Fee.Cost = ent.Fee.Cost * (remainOrder.Filled / ent.Filled)
			temps = append(temps, &remainOrder)
			iod.Enter.Amount = exit.Filled
			iod.Enter.Filled = exit.Filled
			iod.Enter.Fee = ent.Fee.Cost * (exit.Fee.Cost / ent.Fee.Cost)
			iod.Enter.FeeQuote = ent.Fee.QuoteCost * (exit.Fee.QuoteCost / ent.Fee.QuoteCost)
		}
		iod.UpdateProfits(exit.Average)
	}

	// 处理未配对订单
	if len(temps) > 0 {
		// 按时间排序temps
		sort.Slice(temps, func(i, j int) bool {
			return temps[i].LastTradeTimestamp < temps[j].LastTradeTimestamp
		})

		// 对每个未配对订单执行原来的逻辑
		for _, od := range temps {
			odList, _ := jobMap[od.Symbol]
			var newList []*ormo.InOutOrder
			remainFilled := od.Filled

			// 尝试平仓现有持仓
			var usedAmount float64
			newList, usedAmount = handleExitOrders(remainFilled, od, odList, nil)
			remainFilled -= usedAmount

			jobMap[od.Symbol] = newList
			if remainFilled <= od.Filled*0.01 || !strings.HasPrefix(od.ClientOrderID, clientPrefix) {
				continue
			}

			// 创建新的入场订单
			iod := &ormo.InOutOrder{
				IOrder: &ormo.IOrder{
					Symbol:  od.Symbol,
					Short:   od.Side == banexg.OdSideSell,
					EnterAt: od.LastTradeTimestamp,
					Status:  ormo.InOutStatusFullEnter,
				},
				Enter: &ormo.ExOrder{
					Enter:    true,
					Symbol:   od.Symbol,
					Side:     od.Side,
					CreateAt: od.LastTradeTimestamp,
					UpdateAt: od.LastTradeTimestamp,
					Price:    od.Price,
					Average:  od.Average,
					Amount:   remainFilled,
					Filled:   remainFilled,
					Fee:      od.Fee.Cost * (remainFilled / od.Filled),
					FeeQuote: od.Fee.QuoteCost * (remainFilled / od.Filled),
					OrderID:  od.ClientOrderID,
				},
			}
			jobMap[od.Symbol] = append(newList, iod)
		}
	}

	return jobMap
}

type AssetData struct {
	Title    string     `json:"title"`
	Labels   []string   `json:"labels"`
	Datasets []*ChartDs `json:"datasets"`
	Times    []int64    // 存储解析后的时间戳
}

/*
MergeAssetsHtml 合并多个assets.html文件的曲线到一个html文件中

files assets.html文件路径Map，键是路径，值是代表此文件的字符串ID
outPath 输出文件路径
lines 需要提取的曲线名称列表，默认为["Real", "Available"]
*/
func MergeAssetsHtml(outPath string, files map[string]string, tags []string, useRate bool) *errs.Error {
	if len(files) <= 1 {
		return errs.NewMsg(errs.CodeParamRequired, "at least 2 files need to merge")
	}
	if len(tags) == 0 {
		tags = []string{"Real", "Available"}
	}
	linesMap := make(map[string]bool)
	for _, line := range tags {
		linesMap[line] = true
	}

	// 读取所有文件的数据
	var allData []*AssetData
	var minTime, maxTime int64 = math.MaxInt64, 0
	for file, prefix := range files {
		data, err := readAssetHtml(file, prefix, useRate)
		if err != nil {
			return err
		}
		allData = append(allData, data)
		// 更新整体时间范围
		if len(data.Times) > 0 {
			minTime = min(minTime, data.Times[0])
			maxTime = max(maxTime, data.Times[len(data.Times)-1])
		}
	}

	// 确定采样数量和间隔
	maxSamples := 0
	for _, data := range allData {
		maxSamples = max(maxSamples, len(data.Labels))
	}
	if maxSamples < 2 || minTime == math.MaxInt64 {
		return errs.NewMsg(errs.CodeInvalidData, "assets data requires at least two samples")
	}

	// 生成最终的时间戳和标签
	interval := (maxTime - minTime) / int64(maxSamples-1)
	dateLay := core.DefaultDateFmt
	if interval >= utils2.SecsDay*1000 {
		dateLay = core.DateFmt
	}
	var labels = make([]string, 0, maxSamples+3)
	var finalTimes = make([]int64, 0, maxSamples+3)
	for i := 0; i < maxSamples; i++ {
		t := minTime + interval*int64(i)
		finalTimes = append(finalTimes, t)
		labels = append(labels, btime.ToDateStr(t, dateLay))
	}
	finalTimes = append(finalTimes, maxTime)
	labels = append(labels, btime.ToDateStr(maxTime, dateLay))

	// 合并数据集
	var datasets []*ChartDs
	for _, data := range allData {
		for _, ds := range data.Datasets {
			if _, ok := linesMap[ds.Label]; !ok {
				continue
			}
			values := make([]float64, 0, len(finalTimes))
			curVal := math.NaN()
			idx := 0
			nextTime, nextVal := data.Times[0], ds.Data[0]
			for _, curTime := range finalTimes {
				for curTime >= nextTime {
					curVal = nextVal
					idx += 1
					if idx < len(data.Times) {
						nextTime, nextVal = data.Times[idx], ds.Data[idx]
					} else {
						nextTime = math.MaxInt64
					}
				}
				values = append(values, curVal)
			}

			// 添加到最终数据集
			label := fmt.Sprintf("%s_%s", data.Title, ds.Label)
			datasets = append(datasets, &ChartDs{
				Label: label,
				Data:  values,
			})
		}
	}

	// 生成最终的图表
	title := "Merged Assets Comparison"
	return DumpChart(outPath, title, labels, 5, nil, datasets)
}

// readAssetHtml 读取assets.html文件并解析数据
func readAssetHtml(file, prefix string, useRate bool) (*AssetData, *errs.Error) {
	content, err := os.ReadFile(file)
	if err != nil {
		return nil, errs.New(errs.CodeIOReadFail, err)
	}

	// 提取JSON数据
	text := string(content)
	start := strings.Index(text, "chartData = ")
	keyLen := len("chartData = ")
	if start < 0 {
		start = strings.Index(text, "chartData=")
		keyLen = len("chartData=")
	}
	if start < 0 {
		return nil, errs.New(errs.CodeInvalidData, fmt.Errorf("invalid html format in file %s", file))
	}
	start += keyLen
	end := strings.Index(text[start:], "\n")
	if end < 0 {
		end = strings.Index(text[start:], ";")
	}
	if end < 0 {
		end = strings.Index(text[start:], "</script>")
	}
	if end < 0 {
		return nil, errs.New(errs.CodeInvalidData, fmt.Errorf("invalid html format in file %s", file))
	}
	jsonStr := text[start : start+end]

	var data AssetData
	if err = utils2.UnmarshalString(jsonStr, &data, utils2.JsonNumDefault); err != nil {
		return nil, errs.New(errs.CodeUnmarshalFail, err)
	}

	// 解析时间标签为时间戳
	data.Times = make([]int64, len(data.Labels))
	for i, label := range data.Labels {
		data.Times[i], err = btime.ParseTimeMS(label)
		if err != nil {
			return nil, errs.New(errs.CodeRunTime, err)
		}
	}
	if prefix == "" {
		prefix = filepath.Base(filepath.Dir(file))
	}
	data.Title = prefix
	if useRate {
		for _, ds := range data.Datasets {
			initVal := ds.Data[0]
			if initVal != 0 {
				resArr := make([]float64, 0, len(ds.Data))
				for _, curVal := range ds.Data {
					resArr = append(resArr, curVal/initVal)
				}
			}
		}
	}

	return &data, nil
}

type FacArgs struct {
	Pairs        []*PairStat
	AvgOrderCost float64 // Avg Enter Cost for all orders
	TimeFrame    string
	TFMSecs      int64
	StartMS      int64
	EndMS        int64
	MinBack      string // 最小回顾历史周期
	MaxBack      string // 最大回顾历史周期
	Interval     string // 重新轮动的间隔
	MinBackMS    int64
	MaxBackMS    int64
	IntervalMS   int64
}

type FactorFunc func(FacArgs) ([]string, error)

var (
	factorRegistry   = make(map[string]FactorFunc)
	factorRegistryMu sync.RWMutex
)

// RegisterFactor registers a process-wide factor definition.
func RegisterFactor(name string, factor FactorFunc) {
	if name == "" {
		panic("factor name must not be empty")
	}
	if factor == nil {
		panic("factor function must not be nil")
	}
	factorRegistryMu.Lock()
	factorRegistry[name] = factor
	factorRegistryMu.Unlock()
}

// GetFactor returns a registered factor definition.
func GetFactor(name string) (FactorFunc, bool) {
	factorRegistryMu.RLock()
	factor, ok := factorRegistry[name]
	factorRegistryMu.RUnlock()
	return factor, ok
}

// UnregisterFactor removes a process-wide factor definition.
func UnregisterFactor(name string) {
	factorRegistryMu.Lock()
	delete(factorRegistry, name)
	factorRegistryMu.Unlock()
}

// SnapshotFactors returns a detached view of the factor registry.
func SnapshotFactors() map[string]FactorFunc {
	factorRegistryMu.RLock()
	defer factorRegistryMu.RUnlock()
	result := make(map[string]FactorFunc, len(factorRegistry))
	for name, factor := range factorRegistry {
		result[name] = factor
	}
	return result
}

// BtFactorsWithRuntimeDeps evaluates rolling factors with state owned by one
// runtime. It does not initialize, reset, or restore process-wide state.
func BtFactorsWithRuntimeDeps(args []string, deps biz.RuntimeDeps) error {
	command, options := newBtFactorsCommand()
	command.SetArgs(args)
	if err := command.ParseFlags(args); err != nil {
		return err
	}
	reportDeps := NewReportDeps(deps)
	if err := reportDeps.validateResult(); err != nil {
		return err
	}
	if options.output != "" {
		options.output = reportDeps.Config.ParsePath(options.output)
		if err := utils.EnsureDir(options.output, 0755); err != nil {
			return err
		}
	}
	return btFactorsWithReportDeps(options, reportDeps)
}

// BtFactors preserves the public serial tool facade. It initializes the
// package-level compatibility runtime once, then calls the same typed core as
// the explicit RuntimeDeps entrypoint.
func BtFactors(args []string) error {
	command := NewBtFactorsCommand()
	command.SetArgs(args)
	return command.Execute()
}

type btFactorsOptions struct {
	configs  config.ArrString
	factor   string
	input    string
	output   string
	minBack  string
	maxBack  string
	interval string
	download bool
}

// NewBtFactorsCommand preserves the standalone compatibility command.
func NewBtFactorsCommand() *cobra.Command {
	command, options := newBtFactorsCommand()
	command.RunE = func(_ *cobra.Command, _ []string) error {
		return btFactorsLegacy(options)
	}
	return command
}

// NewBtFactorsCommandWithRun builds the command for an explicit composition
// root that owns runtime construction.
func NewBtFactorsCommandWithRun(run func([]string) error) *cobra.Command {
	command, options := newBtFactorsCommand()
	command.RunE = func(_ *cobra.Command, _ []string) error {
		if run == nil {
			return errs.NewMsg(errs.CodeParamRequired, "factor runtime callback is required")
		}
		return run(options.args())
	}
	return command
}

func btFactorsLegacy(options *btFactorsOptions) error {
	var logFile string
	if options.output != "" {
		options.output = config.ParsePath(options.output)
		if err := utils.EnsureDir(options.output, 0755); err != nil {
			return err
		}
		logFile = filepath.Join(options.output, "out.log")
	}
	core.SetRunMode(core.RunModeBackTest)
	if err := biz.SetupComsExg(&config.CmdArgs{Configs: options.configs, Logfile: logFile}); err != nil {
		return err
	}
	deps := legacyReplayReportDeps()
	deps.Exchange = exg.Default
	return btFactorsWithReportDeps(options, deps)
}

func newBtFactorsCommand() (*cobra.Command, *btFactorsOptions) {
	options := &btFactorsOptions{}
	command := &cobra.Command{
		Use:         "bt-factor",
		Aliases:     []string{"bt_factor"},
		Short:       "backtest factors with orders",
		Args:        cobra.NoArgs,
		Annotations: map[string]string{},
	}
	command.Flags().StringArrayVar((*[]string)(&options.configs), "config", nil, "config path; may be repeated")
	command.Flags().StringVar(&options.factor, "factor", "", "factor to test")
	command.Flags().StringVar(&options.input, "in", "", "orders file containing all pairs")
	command.Flags().StringVar(&options.output, "out", "", "output directory")
	command.Flags().StringVar(&options.minBack, "min-back", "1y", "minimum lookback period")
	command.Flags().StringVar(&options.maxBack, "max-back", "2y", "maximum lookback period")
	command.Flags().StringVar(&options.interval, "interval", "4M", "interval between refreshes")
	command.Flags().BoolVar(&options.download, "down", false, "download missing klines")
	return command, options
}

func (o *btFactorsOptions) args() []string {
	args := make([]string, 0, len(o.configs)+12)
	for _, path := range o.configs {
		args = append(args, "--config", path)
	}
	args = append(args, "--factor", o.factor, "--in", o.input, "--out", o.output,
		"--min-back", o.minBack, "--max-back", o.maxBack, "--interval", o.interval)
	if o.download {
		args = append(args, "--down")
	}
	return args
}

func btFactorsWithReportDeps(options *btFactorsOptions, deps *ReportDeps) error {
	if deps == nil {
		return errs.NewMsg(core.ErrBadConfig, "factor report dependencies are required")
	}
	var err error
	cfg := deps.configView()
	if cfg == nil || deps.Config == nil {
		return errs.NewMsg(core.ErrBadConfig, "factor runtime config is required")
	}
	parsePath := deps.Config.ParsePath
	exchange := deps.Exchange
	dateStr := deps.dateStr
	if cfg.TimeRange == nil {
		return errs.NewMsg(core.ErrBadConfig, "factor time range is required")
	}
	startMs := cfg.TimeRange.StartMS
	endMS := cfg.TimeRange.EndMS
	if len(cfg.StakeCurrency) == 0 {
		return errors.New("`stake_currency` in yml is required")
	}
	var tfSecs int
	var orders []*ormo.InOutOrder
	if options.input == "" {
		return errors.New("--in is required")
	} else {
		options.input = parsePath(options.input)
	}
	facFunc, ok := GetFactor(options.factor)
	if !ok || facFunc == nil {
		return errors.New("--factor is invalid")
	}
	orders, err = ormo.LoadOrdersGob(options.input)
	if err != nil {
		return err
	}
	var exsMap = make(map[int32]*orm.ExSymbol)
	var pairMap = make(map[string]int32)
	var totalCost float64
	var validOdNum int
	if len(orders) > 0 {
		startMs = orders[0].RealEnterMS()
		endMS = orders[0].RealExitMS()
		for _, od := range orders {
			curStart := od.RealEnterMS()
			if curStart < startMs && curStart > 0 {
				startMs = curStart
			}
			curEnd := od.RealExitMS()
			if curEnd > endMS {
				endMS = curEnd
			}
			tfSecs = max(tfSecs, utils2.TFToSecs(od.Timeframe))
			curCost := od.EnterCost()
			if curCost > 0 {
				totalCost += curCost
				validOdNum += 1
			}
			if _, ok = pairMap[od.Symbol]; !ok {
				var exs *orm.ExSymbol
				exs, err = deps.symbol(od.Symbol)
				if err != nil {
					return err
				}
				exsMap[exs.ID] = exs
				pairMap[od.Symbol] = exs.ID
			}
		}
	} else {
		return errors.New("orders is empty")
	}
	if validOdNum == 0 {
		return errors.New("no valid orders")
	}
	if tfSecs == 0 {
		tfSecs = utils2.TFToSecs("1d")
	}
	minBackMSecs := int64(utils2.TFToSecs(options.minBack) * 1000)
	maxBackMSecs := int64(utils2.TFToSecs(options.maxBack) * 1000)
	intvMSecs := int64(utils2.TFToSecs(options.interval) * 1000)
	dayMSecs := int64(utils2.TFToSecs("1d") * 1000)
	if intvMSecs < dayMSecs {
		return errors.New("interval must >= 1d")
	}
	if minBackMSecs > maxBackMSecs {
		return errors.New("min-back must <= max-back")
	}
	if minBackMSecs < intvMSecs {
		return errors.New("min-back must >= interval")
	}
	// 按interval对齐开始截止时间
	totalRangeMs := endMS - startMs
	dayNum := totalRangeMs / dayMSecs
	tfMSecs := int64(tfSecs * 1000)
	if dayNum >= 90 {
		tfMSecs = dayMSecs
	}
	if totalRangeMs < minBackMSecs+intvMSecs {
		return errors.New("order time range must > min-back + interval")
	}
	tf := utils2.SecsToTF(int(tfMSecs / 1000))
	// 下载K线
	if options.download {
		prgTotal := 10000
		pBar := utils.NewPrgBar(prgTotal, "BulkDown")
		progress := func(done int, total int) {
			newProgress := int64(prgTotal) * int64(done) / int64(total)
			add := newProgress - pBar.Last
			if add > 0 {
				pBar.Last = newProgress
				pBar.Add(int(add))
			}
		}
		if deps.legacy {
			err = orm.BulkDownOHLCV(exchange, exsMap, tf, startMs, endMS, 0, progress)
		} else {
			options := orm.NewKlineRuntimeOptions(deps.Core, cfg, deps.reportNowMS(), deps.Storage)
			err = orm.BulkDownOHLCVWithOptions(exchange, exsMap, tf, startMs, endMS, 0, progress, options)
		}
		if err != nil {
			return err
		}
	}
	// 滚动测试因子
	avgCost := totalCost / float64(validOdNum)
	rangeStart := startMs
	rangeEnd := startMs + minBackMSecs
	var testOrders []*ormo.InOutOrder
	for rangeEnd+intvMSecs/5 < endMS {
		pairOrders, err := cutOrdersInRange(orders, rangeStart, rangeEnd, deps)
		if err != nil {
			return err
		}
		pairInfos, err := calcPairStats(pairOrders, rangeStart, rangeEnd, tf, deps)
		if err != nil {
			return err
		}
		pairs, err_ := facFunc(FacArgs{
			Pairs:        pairInfos,
			AvgOrderCost: avgCost,
			TimeFrame:    tf,
			TFMSecs:      tfMSecs,
			StartMS:      rangeStart,
			EndMS:        rangeEnd,
			MinBack:      options.minBack,
			MinBackMS:    minBackMSecs,
			MaxBack:      options.maxBack,
			MaxBackMS:    maxBackMSecs,
			Interval:     options.interval,
			IntervalMS:   intvMSecs,
		})
		if err_ != nil {
			return err_
		}
		startDate := dateStr(rangeStart, core.DefaultDateFmt)
		endDate := dateStr(rangeEnd, core.DefaultDateFmt)
		rangeStr := fmt.Sprintf("%s-%s", startDate, endDate)
		deps.logger().Info("select pairs", zap.String("range", rangeStr), zap.Strings("arr", pairs))
		// 使用选中品种，交易interval时间段
		pairOrders, err = cutOrdersInRange(orders, rangeEnd, rangeEnd+intvMSecs, deps)
		if err != nil {
			return err
		}
		for _, s := range pairs {
			if v, ok := pairOrders[s]; ok {
				testOrders = append(testOrders, v...)
			}
		}
		rangeEnd += intvMSecs
		if rangeEnd-rangeStart > maxBackMSecs {
			rangeStart = rangeEnd - maxBackMSecs
		}
	}
	_, err = calcBtResultWithDeps(testOrders, cfg.WalletAmounts, options.output, deps)
	if err != nil {
		return err
	}
	return nil
}

// CutOrdersInRangeWithRuntimeDeps performs the same replay-window clipping
// with an explicit symbol catalog and storage owner.
func CutOrdersInRangeWithRuntimeDeps(orders []*ormo.InOutOrder, startMS, endMS int64, deps biz.RuntimeDeps) (map[string][]*ormo.InOutOrder, *errs.Error) {
	return cutOrdersInRange(orders, startMS, endMS, NewReportDeps(deps))
}

// CutOrdersInRange preserves the package-level serial report tool.
func CutOrdersInRange(orders []*ormo.InOutOrder, startMS, endMS int64) (map[string][]*ormo.InOutOrder, *errs.Error) {
	return cutOrdersInRange(orders, startMS, endMS, legacyReportDeps())
}

func cutOrdersInRange(orders []*ormo.InOutOrder, startMS, endMS int64, deps *ReportDeps) (map[string][]*ormo.InOutOrder, *errs.Error) {
	if err := deps.validateSeries(); err != nil {
		return nil, err
	}
	pairOrders := make(map[string][]*ormo.InOutOrder)
	cloneIds := make(map[int64]bool)
	for _, od := range orders {
		enterMS := od.RealEnterMS()
		exitMS := od.RealExitMS()
		if enterMS >= endMS || exitMS > 0 && exitMS <= startMS {
			continue
		}
		var curOd *ormo.InOutOrder
		if enterMS >= startMS && exitMS <= endMS {
			curOd = od
		} else {
			curOd = od.Clone()
			cloneIds[curOd.ID] = true
		}
		items, _ := pairOrders[od.Symbol]
		pairOrders[od.Symbol] = append(items, curOd)
	}
	for pair, items := range pairOrders {
		tfMap := make(map[string]int)
		minTF, minTfSecs := "", 0
		for _, od := range items {
			if _, ok := tfMap[od.Timeframe]; !ok {
				secs := utils2.TFToSecs(od.Timeframe)
				tfMap[od.Timeframe] = secs
				if minTF == "" || secs < minTfSecs {
					minTfSecs = secs
					minTF = od.Timeframe
				}
			}
		}
		tfMSecs := int64(minTfSecs * 1000)
		var exs *orm.ExSymbol
		var err *errs.Error
		exs, err = deps.symbol(pair)
		if err != nil {
			return nil, err
		}
		var rows []*orm.DataSeries
		queries, release, queryErr := deps.queries()
		if queryErr != nil {
			return nil, queryErr
		}
		_, rows, err = queries.GetSeries(exs, minTF, startMS, endMS, 1, false)
		release()
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			continue
		}
		priceOpen, err_ := rows[0].OpenValue()
		if err_ != nil {
			return nil, errs.New(core.ErrInvalidBars, err_)
		}
		openMS := rows[0].TimeMS
		queries, release, queryErr = deps.queries()
		if queryErr != nil {
			return nil, queryErr
		}
		_, rows, err = queries.GetSeries(exs, minTF, 0, endMS, 1, false)
		release()
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, errs.NewMsg(errs.CodeRunTime, "no kline before %v -%v", pair, endMS)
		}
		last := rows[len(rows)-1]
		priceClose, err_ := last.CloseValue()
		if err_ != nil {
			return nil, errs.New(core.ErrInvalidBars, err_)
		}
		closeMS := last.TimeMS + tfMSecs
		for _, od := range items {
			if _, ok := cloneIds[od.ID]; !ok {
				continue
			}
			// 订单持仓超过时间区间，使用首尾价格重新计算
			amtRate := float64(1)
			if od.RealEnterMS() < openMS {
				od.InitPrice = priceOpen
				if od.Enter != nil {
					curAmt := od.EnterCost() / priceOpen
					amtRate = curAmt / od.Enter.Filled
					od.Enter.Filled = curAmt
					od.Enter.Amount = curAmt
					od.Enter.CreateAt = openMS
					od.Enter.UpdateAt = openMS
					od.Enter.Price = priceOpen
					od.Enter.Average = priceOpen
				}
			}
			if od.Exit != nil {
				if amtRate != 1 {
					od.Exit.Amount *= amtRate
					od.Exit.Filled *= amtRate
				}
				if od.RealExitMS() > closeMS {
					od.Exit.CreateAt = closeMS
					od.Exit.UpdateAt = closeMS
					od.Exit.Price = priceClose
					od.Exit.Average = priceClose
				}
			}
			od.UpdateProfits(priceClose)
		}
	}
	return pairOrders, nil
}

// BuildBtResultWithRuntimeDeps builds a report from an explicit runtime. The
// path does not install or restore process-wide config, wallet, order, price,
// or core state.
func BuildBtResultWithRuntimeDeps(args *config.CmdArgs, deps biz.RuntimeDeps) *errs.Error {
	if args == nil {
		return errs.NewMsg(errs.CodeParamRequired, "build backtest result args are required")
	}
	reportDeps := NewReportDeps(deps)
	if err := reportDeps.validateResult(); err != nil {
		return err
	}
	cfg := reportDeps.configView()
	parsePath := reportDeps.Config.ParsePath
	if args.InPath == "" {
		return errs.NewMsg(errs.CodeRunTime, "-in for orders.gob is required")
	}
	outDir := parsePath(args.OutPath)
	if outDir != "" {
		if err := utils.EnsureDir(outDir, 0755); err != nil {
			return errs.New(errs.CodeIOWriteFail, err)
		}
		args.Logfile = filepath.Join(outDir, "out.log")
	}
	orders, err := ormo.LoadOrdersGob(parsePath(args.InPath))
	if err != nil {
		return err
	}
	_, err = calcBtResultWithDeps(orders, cfg.WalletAmounts, outDir, reportDeps)
	return err
}

// BuildBtResult preserves the public serial report tool. It initializes the
// compatibility runtime once and then calls the same typed report core.
func BuildBtResult(args *config.CmdArgs) *errs.Error {
	if args == nil {
		return errs.NewMsg(errs.CodeParamRequired, "build backtest result args are required")
	}
	core.SetRunMode(core.RunModeBackTest)
	if args.InPath == "" {
		return errs.NewMsg(errs.CodeRunTime, "-in for orders.gob is required")
	}
	outDir := config.ParsePath(args.OutPath)
	if outDir != "" {
		if err := utils.EnsureDir(outDir, 0755); err != nil {
			return errs.New(errs.CodeIOWriteFail, err)
		}
		args.Logfile = filepath.Join(outDir, "out.log")
	}
	if err := biz.SetupComsExg(args); err != nil {
		return err
	}
	orders, err := ormo.LoadOrdersGob(config.ParsePath(args.InPath))
	if err != nil {
		return err
	}
	_, err = calcBtResultWithDeps(orders, config.WalletAmounts, outDir, legacyReplayReportDeps())
	return err
}

var odNextMS = make(map[string]int64)
var odNextLock sync.Mutex

type backtestCompareRuntime struct {
	cfg            *config.Config
	dataDir        func() string
	name           string
	lang           string
	market         string
	startAt        int64
	exchange       banexg.BanExchange
	nowMS          func() int64
	nextMS         map[string]int64
	nextLock       *sync.Mutex
	defaultAccount string
	orders         *ormo.OrderState
	explicit       bool
	dateStr        func(int64, string) string
}

func legacyBacktestCompareRuntime() *backtestCompareRuntime {
	return &backtestCompareRuntime{
		cfg:            &config.Data,
		dataDir:        config.GetDataDir,
		name:           config.Name,
		lang:           config.ShowLangCode,
		market:         core.Market,
		startAt:        core.StartAt,
		exchange:       exg.Default,
		nowMS:          btime.UTCStamp,
		nextMS:         odNextMS,
		nextLock:       &odNextLock,
		defaultAccount: config.DefAcc,
		explicit:       false,
		dateStr:        btime.ToDateStr,
	}
}

func runtimeBacktestCompareRuntime(deps biz.RuntimeDeps) (*backtestCompareRuntime, bool) {
	if deps.Config == nil || deps.Core == nil || deps.Clock == nil || deps.Exchange == nil {
		log.Error("runtime backtest comparison requires config, core, clock, and exchange")
		return nil, false
	}
	cfg := deps.Config.View()
	if cfg == nil || cfg.BTInLive == nil {
		return nil, false
	}
	if deps.Config.DataDir == "" {
		log.Error("runtime backtest comparison requires a data directory")
		return nil, false
	}
	lang := cfg.ShowLangCode
	if lang == "" {
		lang = "en-US"
	}
	reportDeps := NewReportDeps(deps)
	return &backtestCompareRuntime{
		cfg:            cfg,
		dataDir:        func() string { return deps.Config.DataDir },
		name:           cfg.Name,
		lang:           lang,
		market:         deps.Core.Market,
		startAt:        deps.Core.StartAt,
		exchange:       deps.Exchange,
		nowMS:          deps.Clock.TimeMS,
		nextMS:         make(map[string]int64),
		nextLock:       &sync.Mutex{},
		defaultAccount: firstRuntimeAccount(cfg.Accounts),
		orders:         deps.Orders,
		explicit:       true,
		dateStr:        reportDeps.dateStr,
	}, true
}

func firstRuntimeAccount(accounts map[string]*config.AccountConfig) string {
	names := make([]string, 0, len(accounts))
	for account, cfg := range accounts {
		if cfg != nil {
			names = append(names, account)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// BacktestToCompare 实盘时定期回测对比持仓
func BacktestToCompare() {
	backtestToCompare(legacyBacktestCompareRuntime())
}

// BacktestToCompareWithRuntime runs the live comparison using one explicit
// runtime. The child backtest process uses the normal CLI entry; the parent
// only reads the supplied runtime state.
func BacktestToCompareWithRuntime(deps biz.RuntimeDeps) {
	runtime, ok := runtimeBacktestCompareRuntime(deps)
	if !ok {
		return
	}
	backtestToCompare(runtime)
}

func backtestToCompare(runtime *backtestCompareRuntime) {
	if runtime == nil || runtime.cfg == nil || runtime.exchange == nil || runtime.nowMS == nil ||
		runtime.nextMS == nil || runtime.nextLock == nil || runtime.dateStr == nil {
		return
	}
	cfg := runtime.cfg.Clone()
	if cfg == nil || cfg.BTInLive == nil {
		return
	}
	runArgs := make([]string, 0, 4)
	runArgs = append(runArgs, "backtest")
	btCfg := cfg.BTInLive
	account := runtime.defaultAccount
	if btCfg.Acount != "" && len(cfg.Accounts) > 0 {
		accCfg, _ := cfg.Accounts[btCfg.Acount]
		cfg.Accounts = make(map[string]*config.AccountConfig)
		if accCfg != nil {
			cfg.Accounts[btCfg.Acount] = accCfg
			account = btCfg.Acount
		}
	}
	if !banexg.IsContract(runtime.market) {
		return
	}
	posList, err2 := runtime.exchange.FetchAccountPositions(nil, map[string]interface{}{
		banexg.ParamAccount:     account,
		banexg.ParamSettleCoins: cfg.StakeCurrency,
	})
	if err2 != nil {
		log.Error("FetchAccountPositions fail", zap.Error(err2))
		return
	}
	runtime.nextLock.Lock()
	defer runtime.nextLock.Unlock()
	curMS := runtime.nowMS()
	var liveOpens map[int64]*ormo.InOutOrder
	var lock interface {
		Lock()
		Unlock()
	}
	if runtime.explicit {
		if runtime.orders == nil {
			log.Error("runtime backtest comparison requires order state")
			return
		}
		liveOpens, lock = runtime.orders.GetOpenODs(account)
	} else {
		liveOpens, lock = ormo.GetOpenODs(account)
	}
	minStartMS := curMS
	liveOpenQtys := make(map[int64]*ormo.InOutOrder)
	lock.Lock()
	openNum := len(liveOpens)
	for _, od := range liveOpens {
		holdQty := od.HoldAmount()
		if holdQty > 0 {
			liveOpenQtys[od.ID] = od
			minStartMS = min(minStartMS, od.RealEnterMS())
		}
	}
	lock.Unlock()
	if openNum == 0 && len(posList) == 0 {
		// 没有持仓中订单，没有仓位，跳过回测
		return
	}
	cfgData, err2 := cfg.DumpYaml()
	if err2 != nil {
		log.Error("dump config fail in BacktestToCompare", zap.Error(err2))
		return
	}
	// Reserve a fresh directory for every comparison run. A live runtime may
	// trigger another comparison before the previous child process has fully
	// finished, and two runtimes can legitimately share the same name. The
	// allocator uses an atomic mkdir so neither case can overwrite another
	// run's reports.
	dataDir := runtime.dataDir()
	if dataDir == "" {
		log.Error("runtime backtest comparison requires a data directory")
		return
	}
	basePath := filepath.Join(dataDir, "backtest", "bt_in_live_"+runtime.name)
	outPath, err := config.AllocateOutputDir(basePath)
	if err != nil {
		log.Error("create backtest dir fail", zap.Error(err))
		return
	}
	keepOutput := false
	defer func() {
		if !keepOutput {
			_ = os.RemoveAll(outPath)
		}
	}()
	cfgPath := filepath.Join(outPath, "config.yml")
	err = utils2.WriteFile(cfgPath, cfgData)
	if err != nil {
		log.Error("write backtest config.yml fail", zap.Error(err))
		return
	}
	runArgs = append(runArgs, "-config", cfgPath, "-out", outPath)
	exePath, err := os.Executable()
	if err != nil {
		log.Error("get Executable fail", zap.Error(err))
		return
	}
	startMS := min(minStartMS, max(runtime.startAt, curMS-86400000*30))
	startStr := strconv.FormatInt(startMS, 10)
	endStr := strconv.FormatInt(curMS, 10)
	runArgs = append(runArgs, "-timeend", endStr, "-timestart", startStr)
	cmd := exec.Command(exePath, runArgs...)
	err = cmd.Run()
	if err != nil {
		log.Error("BacktestToCompare run fail", zap.Error(err))
		return
	}
	// 读取回测后订单记录
	outGobPath := filepath.Join(outPath, "orders.gob")
	log.Info("bt_in_live done", zap.String("at", outGobPath))
	btOds, err2 := ormo.LoadOrdersGob(outGobPath)
	if err2 != nil {
		log.Error("load bt orders fail", zap.Error(err2))
		return
	}
	// 解析回测所有订单和未平仓订单
	btOpens := make(map[string]*ormo.InOutOrder)
	btAll := make(map[string]*ormo.InOutOrder)
	localAmts := make(map[string]float64)
	for _, od := range btOds {
		keyAlign := od.KeyAlign()
		if od.ExitTag == core.ExitTagBotStop {
			btOpens[keyAlign] = od
			key := od.Symbol + "_long"
			if od.Short {
				key = od.Symbol + "_short"
			}
			cum, _ := localAmts[key]
			localAmts[key] = cum + od.Exit.Filled
		}
		btAll[keyAlign] = od
	}
	// 将实盘未平仓订单和回测订单对比
	matchOpens := make([]string, 0, len(btOpens))
	btMore := make(map[string]int64)
	liveMore := make(map[string]int64)
	liveAmts := make(map[string]float64)
	dupNexts := maps.Clone(runtime.nextMS)
	for _, od := range liveOpenQtys {
		holdQty := od.HoldAmount()
		odKey := od.KeyAlign()
		tfMSecs := int64(utils2.TFToSecs(od.Timeframe) * 1000)
		odNext, _ := runtime.nextMS[odKey]
		runtime.nextMS[odKey] = utils2.AlignTfMSecs(curMS, tfMSecs) + tfMSecs
		btOd, _ := btOpens[odKey]
		delete(dupNexts, odKey)
		if btOd != nil || curMS < odNext || curMS-od.RealEnterMS() < tfMSecs {
			// 已匹配到，或尚未到下次可检查时间，或刚开仓，认为匹配
			matchOpens = append(matchOpens, odKey)
			delete(btOpens, odKey)
		} else {
			btOd, _ = btAll[odKey]
			if btOd != nil {
				liveMore[odKey] = btOd.ExitAt
			} else {
				liveMore[odKey] = 0
			}
		}
		key := od.Symbol + "_long"
		if od.Short {
			key = od.Symbol + "_short"
		}
		cum, _ := liveAmts[key]
		liveAmts[key] = cum + holdQty
	}
	// 清理已完成订单的key
	for key := range dupNexts {
		delete(runtime.nextMS, key)
	}
	for _, od := range btOpens {
		btMore[od.KeyAlign()] = od.RealEnterMS()
	}
	// 和交易所仓位对比
	exgMatch, exgDiff := compareLocalWithExg(posList, localAmts, liveAmts)
	// 发送对比邮件报告
	sendPosCompareReport(runtime, matchOpens, btMore, liveMore, exgMatch, exgDiff, outPath)
	keepOutput = true
}

func compareLocalWithExg(posList []*banexg.Position, localAmts, liveAmts map[string]float64) (map[string]float64, map[string][3]float64) {
	matchSizes := make(map[string]float64)
	diffSizes := make(map[string][3]float64)
	for _, p := range posList {
		key := p.Symbol + "_" + p.Side
		localAmt, _ := localAmts[key]
		liveAmt, _ := liveAmts[key]
		diffRate := math.Abs(p.Contracts-localAmt) / max(p.Contracts, localAmt)
		if diffRate < 0.05 {
			matchSizes[key] = p.Contracts
		} else {
			diffSizes[key] = [3]float64{p.Contracts, localAmt, liveAmt}
		}
	}
	return matchSizes, diffSizes
}

func sendPosCompareReport(runtime *backtestCompareRuntime, matchOpens []string, btMore, liveMore map[string]int64, exgMatch map[string]float64, exgDiff map[string][3]float64, btDir string) {
	langMsg := func(code, defaultValue string) string {
		return config.GetLangMsgBy(runtime.lang, code, defaultValue)
	}
	title := runtime.name + " " + langMsg("backtest_regular", "定期回测")
	liveBadOpen := langMsg("live_bad_open", "实盘误开")
	liveNoOpen := langMsg("live_no_open", "实盘未开")
	liveBadPos := langMsg("live_bad_pos", "仓位不符")
	allMatch := true
	var attFileData []byte
	var mail = &utils.EmailTask{}
	if len(btMore) == 0 && len(liveMore) == 0 && len(exgDiff) == 0 {
		title += langMsg("normal", "正常")
	} else {
		const maxFileSize = 30 * 1024 * 1024 // 30MB
		var err error
		attFileData, err = ZipBacktestResult(btDir, false, true, maxFileSize)
		if err != nil {
			log.Warn("zip backtest fail", zap.Error(err))
		} else {
			mail.AttachFileBy("result.zip", attFileData, "")
		}
		allMatch = false
		title += langMsg("abnormal", "异常")
		title += fmt.Sprintf(", %s: %d %s: %d",
			liveBadOpen, len(liveMore), liveNoOpen, len(btMore),
		)
		if len(exgDiff) > 0 {
			title += fmt.Sprintf(", %s: %d/%d", liveBadPos, len(exgDiff), len(exgMatch)+len(exgDiff))
		}
	}
	var b strings.Builder
	b.WriteString(liveBadOpen + ":\n")
	for key, stamp := range liveMore {
		b.WriteString(fmt.Sprintf("\t%s should close at %s\n", key, runtime.dateStr(stamp, core.DefaultDateFmt)))
	}
	b.WriteString("\n" + liveNoOpen + ":\n")
	for key, stamp := range btMore {
		b.WriteString(fmt.Sprintf("\t%s should open at %s\n", key, runtime.dateStr(stamp, core.DefaultDateFmt)))
	}
	liveOpenMatch := langMsg("live_open_match", "开仓匹配")
	b.WriteString("\n" + liveOpenMatch + ":\n")
	for _, key := range matchOpens {
		b.WriteString(fmt.Sprintf("\t%s\n", key))
	}
	b.WriteString("\n\n" + liveBadPos + ":\n")
	for key, amts := range exgDiff {
		b.WriteString(fmt.Sprintf("\t%s in exg: %.6f but in local: %.6f, and in live: %.6f\n",
			key, amts[0], amts[1], amts[2]))
	}
	livePosMatch := langMsg("live_pos_match", "交易所持仓匹配")
	b.WriteString("\n" + livePosMatch + ":\n")
	for key, amt := range exgMatch {
		b.WriteString(fmt.Sprintf("\t%s with amt: %.6f\n", key, amt))
	}
	if runtime.cfg != nil && runtime.cfg.BTInLive != nil && len(runtime.cfg.BTInLive.MailTo) > 0 {
		mail.Subject = title
		mail.Body = b.String()
		mail.To = runtime.cfg.BTInLive.MailTo
		err := utils.SendEmail(mail)
		if err != nil {
			log.Error("send mail fail", zap.Strings("to", mail.To), zap.Error(err))
		}
	} else if !allMatch {
		log.Error(title, zap.String("detail", b.String()))
	} else {
		log.Info(title)
	}
}

// collectGoFiles 递归收集指定目录及其子目录下所有的.go文件
func collectGoFiles(rootPath string) ([]string, error) {
	var goFiles []string
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 跳过目录，只处理文件
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".go") {
			goFiles = append(goFiles, path)
		}
		return nil
	})
	return goFiles, err
}

// ZipBacktestResult 压缩回测结果为zip数据
// maxFileSize: 最大允许的文件大小(字节)，超过此大小的文件不添加到zip中，0表示不限制
func ZipBacktestResult(resultPath string, cleanIfSuccess, withOrders bool, maxFileSize int64) ([]byte, error) {
	if !utils.Exists(resultPath) {
		return nil, fmt.Errorf("backtest result not exist: %s", resultPath)
	}

	names := []string{"detail.json", "config.yml", "out.log"}
	if withOrders {
		names = append(names, "orders.gob")
	}
	var fileList []string
	for _, name := range names {
		fileList = append(fileList, filepath.Join(resultPath, name))
	}
	// 递归收集所有.go文件
	goFiles, err := collectGoFiles(resultPath)
	if err != nil {
		log.Warn("Failed to find go files", zap.Error(err))
	} else {
		for _, goFile := range goFiles {
			fileList = append(fileList, goFile)
		}
	}

	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	successCount := 0
	for _, filePath := range fileList {
		// 计算相对于resultPath的相对路径
		fileName, err := filepath.Rel(resultPath, filePath)
		if err != nil {
			// 如果无法计算相对路径，使用文件名
			fileName = filepath.Base(filePath)
		}

		// 跳过不存在的文件
		if !utils.Exists(filePath) {
			log.Debug("File not found, skipping", zap.String("file", fileName))
			continue
		}

		// 检查文件大小
		if maxFileSize > 0 {
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				log.Warn("Failed to stat file", zap.String("file", fileName), zap.Error(err))
				continue
			}
			if fileInfo.Size() > maxFileSize {
				log.Warn("File too large, skipping", zap.String("file", fileName),
					zap.Int64("size", fileInfo.Size()), zap.Int64("maxSize", maxFileSize))
				continue
			}
		}

		// 读取文件内容
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			log.Warn("Failed to read file", zap.String("file", fileName), zap.Error(err))
			continue
		}

		// 创建ZIP文件中的条目
		zipPath := strings.ReplaceAll(fileName, "\\", "/")

		w, err := zipWriter.Create(zipPath)
		if err != nil {
			log.Warn("Failed to create zip entry", zap.String("file", fileName), zap.Error(err))
			continue
		}

		// 写入文件内容
		if _, err = w.Write(fileData); err != nil {
			log.Warn("Failed to write to zip", zap.String("file", fileName), zap.Error(err))
			continue
		}
		successCount += 1
	}

	if successCount == 0 {
		return nil, fmt.Errorf("no valid backtest files to upload")
	}

	// 关闭ZIP writer
	if err = zipWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close zip writer: %w", err)
	}

	if cleanIfSuccess {
		if err = os.RemoveAll(resultPath); err != nil {
			log.Warn("Failed to delete local backtest files", zap.String("path", resultPath), zap.Error(err))
		}
	}
	return buf.Bytes(), nil
}
