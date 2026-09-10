package entry

import (
	_ "embed"
	"fmt"
	"os"
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

func runLegacyRunnerSession(run func(*runtime.Process) *errs.Error) *errs.Error {
	process := runtime.NewProcess()
	// These entrypoints still install process-wide facades. Serialize their full
	// lifecycle; explicit runtimes inside the session do not make it concurrent.
	return runLegacyEntrySession(func() *errs.Error {
		defer process.Close()
		return run(process)
	})
}

func runBackTestEntry(args *config.CmdArgs) *errs.Error {
	return runExplicitBackTest(args)
}

func runLegacyEntrySession(run func() *errs.Error) *errs.Error {
	return opt.WithLegacySession(func(opt.LegacySession) *errs.Error { return run() })
}

func RunBackTest(args *config.CmdArgs) *errs.Error {
	return runExplicitBackTest(args)
}

func runBackTestSession(process *runtime.Process, args *config.CmdArgs, session opt.LegacySession) *errs.Error {
	core.SetRunMode(core.RunModeBackTest)
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	if args.OutPath == "" {
		hash, err := config.Data.HashCode()
		if err != nil {
			panic(err)
		}
		args.OutPath = fmt.Sprintf("$backtest/%s", hash)
	}
	if args.Separate && len(config.RunPolicy) > 1 {
		log.Info("run backtest separately for policies", zap.Int("num", len(config.RunPolicy)))
		policyList := config.RunPolicy
		for i, item := range policyList {
			log.Info("start backtest", zap.Int("id", i+1), zap.String("name", item.Name))
			err = config.SetRunPolicy(true, item)
			if err != nil {
				return err
			}
			outDir, err := runBackTest(process, fmt.Sprintf("%s%d", args.OutPath, i+1), "", session)
			if err != nil {
				return err
			}
			err_ := utils.CopyDir(outDir, fmt.Sprintf("%s_%d", outDir, i+1))
			if err_ != nil {
				return errs.New(errs.CodeIOWriteFail, err_)
			}
		}
	} else {
		_, err = runBackTest(process, args.OutPath, args.PrgOut, session)
		return err
	}
	return nil
}

func runBackTest(process *runtime.Process, outDir string, prgOut string, session opt.LegacySession) (string, *errs.Error) {
	core.BotRunning = true
	biz.ResetVars()
	startAt := int64(0)
	if config.TimeRange != nil {
		startAt = config.TimeRange.StartMS
	}
	rt, err := newEntryRuntime(process, core.RunModeBackTest, startAt)
	if err != nil {
		return "", err
	}
	defer func() {
		rt.Close()
		rt.Join()
	}()
	b, err := opt.NewBackTestWithRuntimeDataDeps(session, runtimeRunnerDeps(rt), rt.Symbols, false, outDir, runtimeRunnerDataDeps(rt))
	if err != nil {
		return "", err
	}
	if prgOut != "" {
		lastSave := btime.UTCStamp()
		b.PBar.AddTrigger("", func(task string, rate float64) {
			curTime := btime.UTCStamp()
			if curTime-lastSave < 200 && rate < 1 {
				return
			}
			lastSave = curTime
			fmt.Printf("%s: %v\n", prgOut, rate)
		})
	}
	return executeBackTest(b.OutDir, b.Run)
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

func runTradeSession(process *runtime.Process, args *config.CmdArgs, startup live.CryptoTraderStartupFunc) *errs.Error {
	core.SetRunMode(core.RunModeLive)
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	if args.OutPath != "" {
		file, err_ := os.OpenFile(args.OutPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err_ != nil {
			log.Error("open live dump file fail", zap.Error(err_))
		} else {
			orm.SetDump(file)
		}
	}
	core.BotRunning = true
	core.StartAt = btime.UTCStamp()
	rt, err := newEntryRuntime(process, core.RunModeLive, core.StartAt)
	if err != nil {
		return err
	}
	defer func() {
		rt.Close()
		rt.Join()
	}()
	t := live.NewCryptoTraderWithRuntimeDataDeps(rt, runtimeRunnerDeps(rt), rt.Symbols, startup, runtimeRunnerDataDeps(rt))
	return t.Run()
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

func newEntryRuntime(process *runtime.Process, mode string, startAt int64) (*runtime.Runtime, *errs.Error) {
	rt, err := process.NewRuntime(runtime.Options{
		Context:      core.Ctx,
		Config:       &config.Data,
		DataDir:      config.GetDataDirSafe(),
		StrategyDir:  config.GetStratDir(),
		Mode:         mode,
		Env:          core.RunEnv,
		StartAt:      startAt,
		Exchange:     exg.Default,
		Storage:      orm.CurrentStorage(),
		ExchangeName: core.ExgName,
		Market:       core.Market,
		ContractType: core.ContractType,
		Pairs:        config.Pairs,
	})
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	// SetupComsExg initializes the legacy catalog before the runtime is built.
	// Seed only the current exchange/market; subsequent provider lookups stay on
	// the runtime state and do not expose other identities to event processing.
	for _, item := range orm.GetExSymbols(core.ExgName, core.Market) {
		if cacheErr := rt.Symbols.CacheExSymbolChecked(item); cacheErr != nil {
			rt.Close()
			rt.Join()
			return nil, errs.New(errs.CodeRunTime, cacheErr)
		}
	}
	return rt, nil
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
