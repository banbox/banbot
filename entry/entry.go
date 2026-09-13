package entry

import (
	"bufio"
	"context"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
)

func runBackTestEntry(args *config.CmdArgs) *errs.Error {
	return runExplicitBackTest(args)
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

func RunDownData(args *config.CmdArgs) *errs.Error {
	return runExplicitDownData(args)
}

func RunRepairKlineRanges(args *config.CmdArgs) *errs.Error {
	return runExplicitRepairKlineRanges(args)
}

func runExportData(args *config.CmdArgs) *errs.Error {
	return runKlineMaintenance(args, func(rt *runtime.Runtime, snap *config.Snapshot, q *orm.Queries) *errs.Error {
		return biz.ExportKlinesWithRuntimeDeps(args, &biz.KlineMaintenanceDeps{
			Context: rt.Context(), Queries: q, Symbols: rt.Symbols, Config: snap.View(), Exchange: rt.Exchange,
			Logger: rt.Core.Log(), Location: snap.Location(),
		}, nil)
	})
}

func runPurgeData(args *config.CmdArgs) *errs.Error {
	return runKlineMaintenance(args, func(rt *runtime.Runtime, snap *config.Snapshot, q *orm.Queries) *errs.Error {
		return biz.PurgeKlinesWithRuntimeDeps(args, &biz.KlineMaintenanceDeps{
			Context: rt.Context(), Queries: q, Symbols: rt.Symbols, Config: snap.View(), Exchange: rt.Exchange,
			Logger: rt.Core.Log(), Location: snap.Location(),
		})
	})
}

func runExportAdjFactors(args *config.CmdArgs) *errs.Error {
	return runKlineMaintenance(args, func(rt *runtime.Runtime, snap *config.Snapshot, q *orm.Queries) *errs.Error {
		return biz.ExportAdjFactorsWithRuntimeDeps(args, &biz.KlineMaintenanceDeps{
			Context: rt.Context(), Queries: q, Symbols: rt.Symbols, Config: snap.View(), Exchange: rt.Exchange,
			Logger: rt.Core.Log(), Location: snap.Location(),
		})
	})
}

func runKlineMaintenance(args *config.CmdArgs, run func(*runtime.Runtime, *config.Snapshot, *orm.Queries) *errs.Error) *errs.Error {
	s, snap, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer s.close()
	rt, err := s.newRuntime(snap, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	q, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	q = q.WithSeriesSymbolState(rt.Symbols).WithExchange(rt.Exchange).WithKlineRuntimeOptions(orm.NewKlineRuntimeOptions(rt.Core, snap.View(), rt.Clock.TimeMS(), rt.Storage))
	if snap.View().Exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange config is required")
	}
	if err := q.LoadExgSymbols(snap.View().Exchange.Name); err != nil {
		return err
	}
	return run(rt, snap, q)
}

func RunKlineCorrect(args *config.CmdArgs) *errs.Error {
	return runKlineCorrect(args)
}

func runKlineCorrect(args *config.CmdArgs) *errs.Error {
	if args == nil {
		return errs.NewMsg(core.ErrBadConfig, "kline sync arguments are required")
	}
	syncArgs := args
	if len(args.Pairs) == 0 && !args.Force {
		confirmed, confirmErr := confirmKlineCorrect(context.Background())
		if confirmErr != nil {
			return errs.New(errs.CodeRunTime, confirmErr)
		}
		if !confirmed {
			return nil
		}
		copyArgs := *args
		copyArgs.Force = true
		syncArgs = &copyArgs
	}
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	q, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	allSymbols, err := loadStoredKlineSyncSymbols(rt.Context(), q)
	if err != nil {
		return err
	}
	symbols := orm.NewSymbolStateWithAllocator(rt.Symbols.SIDAllocator())
	if err := symbols.SetExSymbols(allSymbols); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	q = q.WithSeriesSymbolState(symbols).WithKlineRuntimeOptions(
		orm.NewKlineRuntimeOptions(rt.Core, snapshot.View(), rt.Clock.TimeMS(), rt.Storage),
	)
	factory, closeFactory := session.spiderExchangeFactory(snapshot, rt.Exchange)
	defer closeFactory()
	return orm.SyncKlineTFsWithDeps(syncArgs, orm.KlineSyncDeps{
		Context: rt.Context(), Queries: q, Symbols: allSymbols, Logger: rt.Core.Log(), ConfirmAll: confirmKlineCorrect,
		ExchangeFactory: func(_ context.Context, exchange, market string) (banexg.BanExchange, *errs.Error) {
			return factory(exchange, market)
		},
	}, nil)
}

func loadStoredKlineSyncSymbols(ctx context.Context, q *orm.Queries) ([]*orm.ExSymbol, *errs.Error) {
	exchanges, err := q.ListExchanges(ctx)
	if err != nil {
		return nil, errs.New(core.ErrDbReadFail, err)
	}
	items := make([]*orm.ExSymbol, 0)
	for _, exchange := range exchanges {
		symbols, err := q.ListSymbols(ctx, exchange)
		if err != nil {
			return nil, errs.New(core.ErrDbReadFail, err)
		}
		items = append(items, symbols...)
	}
	return items, nil
}

func confirmKlineCorrect(_ context.Context) (bool, error) {
	fmt.Println("KlineCorrect for all symbols would take a long time, input `y` to confirm (y/n):")
	input, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(strings.ToLower(input)) == "y", nil
}

func RunKlineAdjFactors(args *config.CmdArgs) *errs.Error {
	return runKlineAdjFactors(args)
}

func runKlineAdjFactors(args *config.CmdArgs) *errs.Error {
	return runKlineMaintenance(args, func(rt *runtime.Runtime, _ *config.Snapshot, _ *orm.Queries) *errs.Error {
		return orm.CalcAdjFactorsWithExchange(args, rt.Exchange)
	})
}

func RunVerifyData(args *config.CmdArgs) *errs.Error {
	return runExplicitVerifyData(args)
}

func RunSpider(args *config.CmdArgs) *errs.Error {
	return RunSpiderWith(args, nil)
}

func runSpider(args *config.CmdArgs) *errs.Error {
	return runSpiderWith(args, nil)
}

func RunSpiderWith(args *config.CmdArgs, startup data.SpiderStartupFunc) *errs.Error {
	return runSpiderWith(args, startup)
}

func runSpiderWith(args *config.CmdArgs, startup data.SpiderStartupFunc) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newRuntime(snapshot, core.RunModeLive, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	factory, closeFactory := session.spiderExchangeFactory(snapshot, rt.Exchange)
	defer closeFactory()
	runtimeFactory := data.SpiderRuntimeFactory(func(ctx context.Context, name, market string) (*data.RuntimeDeps, func(), *errs.Error) {
		if name == rt.Core.ExgName && market == rt.Core.Market {
			return rt.DataDeps(), func() {}, nil
		}
		return newDevChildRuntime(session, snapshot, ctx, name, market, factory)
	})
	addr := snapshot.View().SpiderAddr
	if addr == "" {
		addr = "127.0.0.1:6789"
	}
	return data.RunLiveSpiderWithRuntimeDeps(rt.Context(), addr, rt.DataDeps(), runtimeFactory, startup)
}

func LoadKLinesToDB(args *config.CmdArgs) *errs.Error {
	return loadKLinesToDB(args)
}

func loadKLinesToDB(args *config.CmdArgs) *errs.Error {
	if args == nil || args.InPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--in is required")
	}
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	cfg := snapshot.View()
	if cfg.Exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange config is required")
	}
	sess, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	sess = sess.WithSeriesSymbolState(rt.Symbols).WithExchange(rt.Exchange).
		WithKlineRuntimeOptions(orm.NewKlineRuntimeOptions(rt.Core, cfg, rt.Clock.TimeMS(), rt.Storage))
	names, err := data.FindPathNames(args.InPath, ".zip")
	if err != nil {
		return err
	}
	dirPath := names[0]
	names = names[1:]
	totalNum := len(names) * core.StepTotal
	pBar := utils.NewPrgBar(totalNum, "load1m")
	deps := &biz.KlineLoadRuntimeDeps{
		Context: rt.Context(), Queries: sess, Symbols: rt.Symbols, Exchange: rt.Exchange,
		ExchangeName: cfg.Exchange.Name, Market: cfg.MarketType,
	}
	for _, name := range names {
		fileInPath := filepath.Join(dirPath, name)
		err = data.ReadZipCSVs(fileInPath, pBar, biz.LoadZipSeriesWithRuntimeDeps(deps), nil)
		if err != nil {
			return err
		}
	}
	pBar.Close()
	return nil
}

func AggKlineBigs(args *config.CmdArgs) *errs.Error {
	return aggKlineBigs(args)
}

func aggKlineBigs(args *config.CmdArgs) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	cfg := snapshot.View()
	if cfg.Exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange config is required")
	}
	sess, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	sess = sess.WithSeriesSymbolState(rt.Symbols).WithExchange(rt.Exchange).
		WithKlineRuntimeOptions(orm.NewKlineRuntimeOptions(rt.Core, cfg, rt.Clock.TimeMS(), rt.Storage))
	if err := sess.LoadExgSymbols(cfg.Exchange.Name); err != nil {
		return err
	}
	var startMS, endMS int64
	if cfg.TimeRange != nil {
		startMS, endMS = cfg.TimeRange.StartMS, cfg.TimeRange.EndMS
	}
	return biz.AggBigKlinesWithRuntimeDeps(args, &biz.KlineAggRuntimeDeps{
		Context: rt.Context(), Queries: sess, Symbols: rt.Symbols, Exchange: rt.Exchange,
		ExchangeName: cfg.Exchange.Name, Market: cfg.MarketType, StartMS: startMS, EndMS: endMS,
	})
}

func runInit(args *config.CmdArgs) *errs.Error {
	if args == nil {
		return errs.NewMsg(errs.CodeParamRequired, "command arguments are required")
	}
	dataDir := args.DataDir
	if dataDir == "" {
		dataDir = os.Getenv("BanDataDir")
	}
	if dataDir == "" {
		return errs.NewMsg(errs.CodeParamRequired, "-datadir is required")
	}
	args.DataDir = dataDir
	fmt.Printf("BanDataDir=%s\n", dataDir)
	err := biz.InitDataDirAt(dataDir)
	if err != nil {
		return err
	}
	_, err = config.LoadRuntimeSnapshot(args)
	if err != nil {
		return err
	}
	log.Info("init done")
	return nil
}

func runDataExport(args *config.CmdArgs) *errs.Error {
	return runExplicitDataExport(args)
}

func runDataImport(args *config.CmdArgs) *errs.Error {
	return runExplicitDataImport(args)
}
