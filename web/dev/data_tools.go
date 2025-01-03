package dev

import (
	"fmt"
	"github.com/banbox/banbot/btime"
	"sync"

	"github.com/banbox/banbot/utils"
	utils2 "github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/web/base"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// DataToolsManager 数据工具任务管理器
type DataToolsManager struct {
	running    bool
	runningMux sync.Mutex
}

type FnDataTool = func(args *DataToolsArgs, pBar *utils.StagedPrg) *errs.Error

type DataToolsArgs struct {
	Action   string `json:"action" validate:"required"`
	Folder   string `json:"folder"`
	Exchange string `json:"exchange" validate:"required"`
	ExgReal  string `json:"exgReal"`
	Market   string `json:"market" validate:"required"`
	Exg      banexg.BanExchange
	Pairs    []string `json:"pairs"`
	Periods  []string `json:"periods"`
	StartMs  int64    `json:"startMs"`
	EndMs    int64    `json:"endMs"`
	Force    bool     `json:"force"`
}

var (
	dataToolsMgr = &DataToolsManager{}
	validActions = map[string]bool{
		"download": true,
		"export":   true,
		"purge":    true,
		"correct":  true,
	}
)

// StartTask 开始一个任务
func (m *DataToolsManager) StartTask() error {
	m.runningMux.Lock()
	defer m.runningMux.Unlock()

	if m.running {
		return fmt.Errorf("another task is running, please wait")
	}
	m.running = true
	return nil
}

// EndTask 结束任务
func (m *DataToolsManager) EndTask() {
	m.runningMux.Lock()
	defer m.runningMux.Unlock()
	m.running = false
}

// RunDataTools 执行数据工具任务
func RunDataTools(args *DataToolsArgs) *errs.Error {
	switch args.Action {
	case "download":
		return runDataTask(runDownloadData, args, []string{"downKline"}, []float64{1})
	case "export":
		return runDataTask(runExportData, args, []string{"export"}, []float64{1})
	case "purge":
		return runDataTask(runPurgeData, args, []string{"purge"}, []float64{1})
	case "correct":
		return runDataTask(runCorrectData, args, []string{"fixKInfoZeros", "syncTfKinfo", "fillKHole"},
			[]float64{1, 5, 5})
	default:
		return errs.NewMsg(errs.CodeParamInvalid, "invalid action")
	}
}

func runDataTask(fn FnDataTool, args *DataToolsArgs, tasks []string, weights []float64) *errs.Error {
	pBar := utils.NewStagedPrg(tasks, weights)
	pBar.AddTrigger("", func(task string, progress float64) {
		BroadcastWS("", map[string]interface{}{
			"type":     "heavyPrg",
			"name":     task,
			"progress": progress,
		})
	})
	return fn(args, pBar)
}

// runDownloadData 下载数据
func runDownloadData(args *DataToolsArgs, pBar *utils.StagedPrg) *errs.Error {
	exsMap := make(map[int32]*orm.ExSymbol)
	exchange, err := exg.GetWith(args.Exchange, args.Market, "")
	if err != nil {
		return err
	}
	err = orm.InitExg(exchange)
	if err != nil {
		return err
	}
	for _, pair := range args.Pairs {
		exs, err := orm.GetExSymbolCur(pair)
		if err != nil {
			return err
		}
		exsMap[exs.ID] = exs
	}
	log.Info("start download data",
		zap.String("exchange", args.Exchange),
		zap.String("market", args.Market),
		zap.Int("symbolNum", len(exsMap)),
		zap.Strings("timeframes", args.Periods),
		zap.Int64("start", args.StartMs),
		zap.Int64("end", args.EndMs))

	startMs, endMs := args.StartMs, args.EndMs
	for i, tf := range args.Periods {
		prgBase := float64(i) / float64(len(args.Periods))
		err = orm.BulkDownOHLCV(args.Exg, exsMap, tf, startMs, endMs, 0, func(done int, total int) {
			pBar.SetProgress("downKline", prgBase+float64(done)/float64(total))
		})
		if err != nil {
			return err
		}
	}

	log.Info("download data completed")
	return nil
}

// runExportData 导出数据
func runExportData(args *DataToolsArgs, pBar *utils.StagedPrg) *errs.Error {
	log.Info("start export data",
		zap.String("exchange", args.Exchange),
		zap.String("market", args.Market),
		zap.String("folder", args.Folder),
		zap.Strings("pairs", args.Pairs),
		zap.Strings("timeframes", args.Periods),
		zap.Int64("start", args.StartMs),
		zap.Int64("end", args.EndMs))

	backExg, backMarket := core.ExgName, core.Market
	core.ExgName, core.Market = args.Exchange, args.Market
	prg := func(done int, total int) {
		pBar.SetProgress("export", float64(done)/float64(total))
	}
	err := biz.ExportKlines(&config.CmdArgs{
		OutPath:    args.Folder,
		ExgReal:    args.ExgReal,
		Pairs:      args.Pairs,
		TimeFrames: args.Periods,
		TimeRange:  fmt.Sprintf("%d-%d", args.StartMs, args.EndMs),
		AdjType:    "none",
	}, prg)
	core.ExgName, core.Market = backExg, backMarket
	if err != nil {
		log.Error("export data failed", zap.Error(err))
		return err
	}

	log.Info("export data completed")
	return nil
}

// runPurgeData 清理数据
func runPurgeData(args *DataToolsArgs, pb *utils.StagedPrg) *errs.Error {
	log.Info("start purge data",
		zap.String("exchange", args.Exchange),
		zap.String("market", args.Market),
		zap.Strings("pairs", args.Pairs),
		zap.Strings("timeframes", args.Periods),
		zap.Int64("start", args.StartMs),
		zap.Int64("end", args.EndMs))

	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()

	pBar := utils.NewPrgBar(len(args.Pairs), "Purge")
	pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
		pb.SetProgress("purge", float64(done)/float64(total))
	})
	defer pBar.Close()
	exsMap := make(map[string]bool)
	for _, pair := range args.Pairs {
		if _, ok := exsMap[pair]; ok {
			pBar.Add(1)
			continue
		}
		exsMap[pair] = true
		exs, err := orm.GetExSymbolCur(pair)
		if err != nil {
			return err
		}
		err = sess.DelKData(exs, args.Periods, args.StartMs, args.EndMs)
		if err != nil {
			return err
		}
		pBar.Add(1)
	}

	log.Info("purge data completed")
	return nil
}

// runCorrectData 修正数据
func runCorrectData(args *DataToolsArgs, pb *utils.StagedPrg) *errs.Error {
	log.Info("start correct data",
		zap.String("exchange", args.Exchange),
		zap.String("market", args.Market),
		zap.Strings("pairs", args.Pairs))

	err := orm.SyncKlineTFs(&config.CmdArgs{
		Pairs: args.Pairs,
		Force: true,
	}, pb)
	if err != nil {
		log.Error("correct data failed", zap.Error(err))
		return err
	}

	log.Info("correct data completed")
	return nil
}

// handleDataTools 处理数据工具请求
func handleDataTools(c *fiber.Ctx) error {
	var args = new(DataToolsArgs)
	if err := base.VerifyArg(c, args, base.ArgBody); err != nil {
		return err
	}

	if !validActions[args.Action] {
		return c.Status(400).JSON(fiber.Map{
			"msg": "invalid action",
		})
	}

	// 验证必填参数
	if args.StartMs > 0 && args.EndMs == 0 {
		args.EndMs = btime.UTCStamp()
	}
	var errMsg string
	if args.Action != "correct" {
		if args.StartMs == 0 {
			errMsg = "startTime is required"
		} else if len(args.Periods) == 0 {
			errMsg = "periods is required"
		}
	}
	if args.Action == "export" && args.Folder == "" {
		errMsg = "folder is required"
	}
	if errMsg != "" {
		return c.Status(400).JSON(fiber.Map{
			"msg": errMsg,
		})
	}

	exchange, err2 := exg.GetWith(args.Exchange, args.Market, banexg.MarketSwap)
	if err2 != nil {
		return err2
	}

	err2 = orm.InitExg(exchange)
	if err2 != nil {
		return err2
	}
	args.Exg = exchange

	if len(args.Pairs) == 0 {
		exsMap := orm.GetExSymbols(args.Exchange, args.Market)
		for _, exs := range exsMap {
			args.Pairs = append(args.Pairs, exs.Symbol)
		}
	}

	if !args.Force {
		msgTpl := "\nExchange: %s\nExgReal: %s\nMarket: %s\nPairs: %v\nPeriods: %v\nStartMs: %d\nEndMs: %d\n"
		msg := fmt.Sprintf(msgTpl, args.Exchange, args.ExgReal, args.Market, len(args.Pairs),
			args.Periods, args.StartMs, args.EndMs)
		if args.Action == "download" {
			barNum := 0
			for _, tf := range args.Periods {
				tfMSec := int64(utils2.TFToSecs(tf) * 1000)
				singleNum := int((args.EndMs - args.StartMs) / tfMSec)
				barNum += singleNum * len(args.Pairs)
			}
			totalMins := barNum/core.ConcurNum/core.DownKNumMin + 1
			msg += fmt.Sprintf("Cost Time: %d Hours %d Minutes", totalMins/60, totalMins%60)
		}
		return c.JSON(fiber.Map{
			"code": 401,
			"msg":  msg,
		})
	}

	// 尝试启动任务
	if err := dataToolsMgr.StartTask(); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"msg": err.Error(),
		})
	}

	if args.Action == "export" {
		args.Folder = config.ParsePath(args.Folder)
	}

	// 在goroutine中执行任务
	go func() {
		defer dataToolsMgr.EndTask()
		err := RunDataTools(args)
		if err != nil {
			log.Error("data tools task failed",
				zap.String("action", args.Action),
				zap.Error(err))
		}
	}()

	return c.JSON(fiber.Map{
		"code": 200,
		"msg":  "task started",
	})
}
