package entry

import (
	_ "embed"
	"fmt"
	"path/filepath"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

func runBackTestEntry(args *config.CmdArgs) *errs.Error {
	return runExplicitBackTest(args)
}

func runLegacyEntrySession(run func() *errs.Error) *errs.Error {
	return opt.WithLegacySession(func(opt.LegacySession) *errs.Error { return run() })
}

func RunBackTest(args *config.CmdArgs) *errs.Error {
	return runExplicitBackTest(args)
}

func executeBackTest(outDir string, run func() *errs.Error) (string, *errs.Error) {
	if err := run(); err != nil {
		return "", err
	}
	return outDir, nil
}

func RunTrade(args *config.CmdArgs) *errs.Error {
	return RunTradeWith(args, nil)
}

func RunTradeWith(args *config.CmdArgs, startup live.CryptoTraderStartupFunc) *errs.Error {
	return runExplicitTrade(args, startup)
}

func runTradeEntry(args *config.CmdArgs) *errs.Error {
	return runExplicitTrade(args, nil)
}

func runtimeRunnerDeps(rt *runtime.Runtime) biz.RuntimeDeps {
	return biz.RuntimeDeps{
		Core:           rt.Core,
		Clock:          rt.Clock,
		Market:         rt.Market,
		Batch:          rt.Batch,
		Strategies:     rt.Strategies,
		Orders:         rt.Orders,
		Trading:        rt.Trading,
		Config:         rt.Config,
		Symbols:        rt.Symbols,
		Storage:        rt.Storage,
		Exchange:       rt.Exchange,
		Dump:           rt.Dump,
		Scheduler:      rt.Scheduler(),
		Notifications:  rt.Notifications,
		DefaultAccount: rt.Config.DefaultAccount(),
	}
}

func runtimeRunnerDataDeps(rt *runtime.Runtime) *data.RuntimeDeps {
	return &data.RuntimeDeps{
		Core:         rt.Core,
		Clock:        rt.Clock,
		Config:       rt.Config,
		Market:       rt.Market,
		Symbols:      rt.Symbols,
		Storage:      rt.Storage,
		Strategies:   rt.Strategies,
		Catalog:      rt.Catalog,
		Callbacks:    rt,
		Exchange:     rt.Exchange,
		Dump:         rt.Dump,
		ExchangeName: rt.Core.ExgName,
		MarketType:   rt.Core.Market,
	}
}

func RunDownData(args *config.CmdArgs) *errs.Error {
	return runLegacyEntrySession(func() *errs.Error { return runDownData(args) })
}

func runDownData(args *config.CmdArgs) *errs.Error {
	core.SetRunMode(core.RunModeData)
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	pairs, err := goods.RefreshPairList(btime.TimeMS())
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		log.Warn("no pairs to download")
		return nil
	}
	log.Info("start down kline for pairs", zap.Int("num", len(pairs)), zap.Strings("tfs", args.TimeFrames))
	exsMap := make(map[int32]*orm.ExSymbol)
	for _, pair := range pairs {
		exs, err := orm.GetExSymbolCur(pair)
		if err != nil {
			return err
		}
		exsMap[exs.ID] = exs
	}
	startMs, endMs := config.TimeRange.StartMS, config.TimeRange.EndMS
	for _, tf := range args.TimeFrames {
		err = orm.BulkDownOHLCV(exg.Default, exsMap, tf, startMs, endMs, 0, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func RunRepairKlineRanges(args *config.CmdArgs) *errs.Error {
	return runLegacyEntrySession(func() *errs.Error { return runRepairKlineRanges(args) })
}

func runRepairKlineRanges(args *config.CmdArgs) *errs.Error {
	core.SetRunMode(core.RunModeData)
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	pairs, dynamic := config.GetStaticPairs()
	if dynamic || len(pairs) == 0 {
		return errs.NewMsg(errs.CodeParamInvalid, "kline repair-ranges requires explicit pairs")
	}
	exsMap := make(map[int32]*orm.ExSymbol, len(pairs))
	for _, pair := range pairs {
		exs, getErr := orm.GetExSymbolCur(pair)
		if getErr != nil {
			return getErr
		}
		exsMap[exs.ID] = exs
	}
	return orm.RepairKlineRanges(exsMap, args.TimeFrames, config.TimeRange.StartMS, config.TimeRange.EndMS)
}

func runExportData(args *config.CmdArgs) *errs.Error {
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	return biz.ExportKlines(args, nil)
}

func runPurgeData(args *config.CmdArgs) *errs.Error {
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	return biz.PurgeKlines(args)
}

func RunKlineCorrect(args *config.CmdArgs) *errs.Error {
	return runLegacyEntrySession(func() *errs.Error { return runKlineCorrect(args) })
}

func runKlineCorrect(args *config.CmdArgs) *errs.Error {
	err := biz.SetupComs(args)
	if err != nil {
		return err
	}
	return orm.SyncKlineTFs(args, nil)
}

func RunKlineAdjFactors(args *config.CmdArgs) *errs.Error {
	return runLegacyEntrySession(func() *errs.Error { return runKlineAdjFactors(args) })
}

func runKlineAdjFactors(args *config.CmdArgs) *errs.Error {
	err := biz.SetupComs(args)
	if err != nil {
		return err
	}
	return orm.CalcAdjFactors(args)
}

func RunVerifyData(args *config.CmdArgs) *errs.Error {
	return runLegacyEntrySession(func() *errs.Error { return runVerifyData(args) })
}

func runVerifyData(args *config.CmdArgs) *errs.Error {
	err := biz.SetupComs(args)
	if err != nil {
		return err
	}
	vArgs, err := orm.ParseVerifyArgs(args)
	if err != nil {
		return err
	}
	results, err := orm.VerifyDataRanges(vArgs)
	if err != nil {
		return err
	}
	orm.PrintVerifyResults(results)
	return nil
}

func RunSpider(args *config.CmdArgs) *errs.Error {
	return RunSpiderWith(args, nil)
}

func runSpider(args *config.CmdArgs) *errs.Error {
	return runSpiderWith(args, nil)
}

func RunSpiderWith(args *config.CmdArgs, startup data.SpiderStartupFunc) *errs.Error {
	return runLegacyEntrySession(func() *errs.Error { return runSpiderWith(args, startup) })
}

func runSpiderWith(args *config.CmdArgs, startup data.SpiderStartupFunc) *errs.Error {
	core.SetRunMode(core.RunModeLive)
	args.AutoCompact = true
	if args.Logfile == "" {
		args.Logfile = filepath.Join(config.GetLogsDir(), "spider.log")
	}
	err := biz.SetupComs(args)
	if err != nil {
		return err
	}
	return data.RunSpiderWithSession(config.SpiderAddr, startup)
}

func LoadKLinesToDB(args *config.CmdArgs) *errs.Error {
	return runLegacyEntrySession(func() *errs.Error { return loadKLinesToDB(args) })
}

func loadKLinesToDB(args *config.CmdArgs) *errs.Error {
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	if args.InPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--in is required")
	}
	names, err := data.FindPathNames(args.InPath, ".zip")
	if err != nil {
		return err
	}
	dirPath := names[0]
	names = names[1:]
	totalNum := len(names) * core.StepTotal
	pBar := utils.NewPrgBar(totalNum, "load1m")
	zArgs := []string{core.ExgName, core.Market, core.ContractType}
	for _, name := range names {
		fileInPath := filepath.Join(dirPath, name)
		err = data.ReadZipCSVs(fileInPath, pBar, biz.LoadZipSeries, zArgs)
		if err != nil {
			return err
		}
	}
	pBar.Close()
	return nil
}

func AggKlineBigs(args *config.CmdArgs) *errs.Error {
	return runLegacyEntrySession(func() *errs.Error { return aggKlineBigs(args) })
}

func aggKlineBigs(args *config.CmdArgs) *errs.Error {
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	return biz.AggBigKlines(args)
}

func runInit(args *config.CmdArgs) *errs.Error {
	args.Init()
	errs.PrintErr = utils.PrintErr
	dataDir := config.GetDataDir()
	fmt.Printf("BanDataDir=%s\n", dataDir)
	err := biz.InitDataDir()
	if err != nil {
		return err
	}
	err = config.LoadConfig(args)
	if err != nil {
		return err
	}
	log.Info("init done")
	return nil
}

func runDataExport(args *config.CmdArgs) *errs.Error {
	if len(args.Configs) == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "-config is required")
	}
	cfgPath := args.Configs[len(args.Configs)-1]
	args.Configs = args.Configs[:len(args.Configs)-1]
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	if args.OutPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "-out is required")
	}
	cfgPath = config.ParsePath(cfgPath)
	return orm.ExportKData(cfgPath, args.OutPath, args.Concur, nil)
}

func runDataImport(args *config.CmdArgs) *errs.Error {
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	if args.InPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "-in is required")
	}
	return orm.ImportData(args.InPath, args.Concur, nil)
}
