package dev

import (
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"os"

	"github.com/banbox/banbot/btime"

	"github.com/banbox/banbot/utils"
	utils2 "github.com/banbox/banexg/utils"

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
	runningMux deadlock.Mutex
}

type FnDataTool = func(args *DataToolsArgs, pBar *utils.StagedPrg) *errs.Error

type DataToolsArgs struct {
	Action      string `json:"action" validate:"required"`
	Folder      string `json:"folder"`
	Exchange    string `json:"exchange"`
	ExgReal     string `json:"exgReal"`
	Market      string `json:"market"`
	Exg         banexg.BanExchange
	Pairs       []string `json:"pairs"`
	Periods     []string `json:"periods"`
	StartMs     int64    `json:"startMs"`
	EndMs       int64    `json:"endMs"`
	Force       bool     `json:"force"`
	Concurrency int      `json:"concurrency"`
	Config      string   `json:"config"`
}

var (
	dataToolsMgr = &DataToolsManager{}
	validActions = map[string]bool{
		"download": true,
		"export":   true,
		"import":   true,
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
	m.running = false
	m.runningMux.Unlock()
}

// RunDataTools 执行数据工具任务
func RunDataTools(args *DataToolsArgs) *errs.Error {
	switch args.Action {
	case "download":
		return runDataTask(runDownloadData, args, []string{"downKline"}, []float64{1})
	case "export":
		return runDataTask(runExportData, args, []string{"holes", "kline"}, []float64{1, 5})
	case "import":
		return runDataTask(runImportData, args, []string{"kline", "range"}, []float64{3, 1})
	case "purge":
		return runDataTask(runPurgeData, args, []string{"purge"}, []float64{1})
	case "correct":
		return runDataTask(runCorrectData, args, []string{"syncTFs"}, []float64{1})
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
		exs, err := orm.GetExSymbol(exchange, pair)
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
		zap.String("folder", args.Folder),
		zap.Int("concurrency", args.Concurrency))

	// 创建临时配置文件
	tmpFile, err := os.CreateTemp("", "export_config_*.yml")
	if err != nil {
		return errs.New(errs.CodeIOWriteFail, err)
	}
	defer os.Remove(tmpFile.Name())

	// 写入配置内容
	if _, err := tmpFile.WriteString(args.Config); err != nil {
		tmpFile.Close()
		return errs.New(errs.CodeIOWriteFail, err)
	}
	tmpFile.Close()

	// 调用 orm.ExportKData
	err2 := orm.ExportKData(tmpFile.Name(), args.Folder, args.Concurrency, pBar)
	if err2 != nil {
		return err2
	}

	log.Info("export data completed")
	return nil
}

// runImportData 导入数据
func runImportData(args *DataToolsArgs, pBar *utils.StagedPrg) *errs.Error {
	log.Info("start import data",
		zap.String("folder", args.Folder),
		zap.Int("concurrency", args.Concurrency))

	// 调用 orm.ImportData
	err2 := orm.ImportData(args.Folder, args.Concurrency, pBar)
	if err2 != nil {
		return err2
	}

	log.Info("import data completed")
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
		exs := orm.GetExSymbol2(args.Exchange, args.Market, pair)
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
	mustMarket := false
	if args.Action == "export" || args.Action == "import" {
		if args.Folder == "" {
			errMsg = "folder is required"
		}
	} else {
		mustMarket = true
		if args.Exchange == "" || args.Market == "" {
			errMsg = "exchange & market is required"
		}
		if args.Action != "correct" {
			if args.StartMs == 0 {
				errMsg = "startTime is required"
			} else if len(args.Periods) == 0 {
				errMsg = "periods is required"
			}
		}
	}
	if errMsg != "" {
		return c.Status(400).JSON(fiber.Map{
			"msg": errMsg,
		})
	}

	if mustMarket {
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
		} else if !mustMarket {
			msg = fmt.Sprintf("\nFolder: %s", args.Folder)
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
