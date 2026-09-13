package entry

import (
	"context"
	"os"
	"path/filepath"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banbot/web/dev"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newDevWebServer(args *dev.CmdArgs) (*dev.DevServer, func(), error) {
	if args == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "web arguments are required")
	}
	configArgs := &config.CmdArgs{DataDir: args.DataDir, LogLevel: args.LogLevel, TimeZone: args.TimeZone,
		Configs: args.Configs, ConfigData: args.ConfigData, Logfile: args.LogFile, AutoCompact: true}
	session, snapshot, err := openExplicitEntrySession(configArgs)
	if err != nil {
		return nil, nil, err
	}
	if err := persistDockerWebConfig(snapshot, args); err != nil {
		session.close()
		return nil, nil, err
	}
	rt, err := session.newRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		session.close()
		return nil, nil, err
	}
	queries, conn, err := runtimeWebQueries(rt, snapshot)
	if err != nil {
		rt.Close()
		rt.Join()
		session.close()
		return nil, nil, err
	}
	if snapshot.View().Exchange == nil {
		conn.Release()
		rt.Close()
		rt.Join()
		session.close()
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "exchange config is required")
	}
	if err := queries.LoadExgSymbols(snapshot.View().Exchange.Name); err != nil {
		conn.Release()
		rt.Close()
		rt.Join()
		session.close()
		return nil, nil, err
	}
	conn.Release()
	exchangeFor, closeExchanges := session.spiderExchangeFactory(snapshot, rt.Exchange)
	runtimeFor := dev.RuntimeFactory(func(ctx context.Context, name, market string) (*data.RuntimeDeps, func(), *errs.Error) {
		if name == rt.Core.ExgName && market == rt.Core.Market {
			return rt.BizDeps().DataDeps(), func() {}, nil
		}
		return newDevChildRuntime(session, snapshot, ctx, name, market, exchangeFor)
	})
	server, err := dev.NewDevServer(dev.DevDeps{
		Data:        rt.BizDeps().DataDeps(),
		ConfigPaths: configPathsForSnapshot(snapshot, args.Configs),
		RuntimeFor:  runtimeFor,
		Maintenance: devMaintenanceRunner(exchangeFor),
	})
	if err != nil {
		closeExchanges()
		rt.Close()
		rt.Join()
		session.close()
		return nil, nil, err
	}
	cleanup := func() {
		server.Stop()
		server.Join()
		closeExchanges()
		rt.Close()
		rt.Join()
		session.close()
	}
	return server, cleanup, nil
}

func configPathsForSnapshot(snapshot *config.Snapshot, paths []string) []string {
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		result = append(result, snapshot.ParsePath(path))
	}
	return result
}

func runtimeWebQueries(rt *runtime.Runtime, snapshot *config.Snapshot) (*orm.Queries, *pgxpool.Conn, *errs.Error) {
	q, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return nil, nil, err
	}
	return q.WithSeriesSymbolState(rt.Symbols).WithExchange(rt.Exchange).WithKlineRuntimeOptions(orm.NewKlineRuntimeOptions(rt.Core, snapshot.View(), rt.Clock.TimeMS(), rt.Storage)), conn, nil
}

func newDevChildRuntime(session *explicitEntrySession, parent *config.Snapshot, ctx context.Context, name, market string, exchangeFor data.SpiderExchangeFactory) (*data.RuntimeDeps, func(), *errs.Error) {
	cfg := parent.View().Clone()
	if cfg.Exchange == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "exchange config is required")
	}
	normalizeDevChildConfig(cfg, name, market)
	snapshot := config.NewSnapshotWithDirs(cfg, parent.DataDir, parent.StrategyDir)
	exchange, err := exchangeFor(name, market)
	if err != nil {
		return nil, nil, err
	}
	catalog, catalogErr := data.RuntimeCatalogFromRegisteredSources()
	if catalogErr != nil {
		return nil, nil, errs.New(errs.CodeRunTime, catalogErr)
	}
	child, createErr := session.process.NewRuntime(runtime.Options{Context: ctx, Logger: session.logger, Config: cfg, DataDir: snapshot.DataDir, StrategyDir: snapshot.StrategyDir, Mode: core.RunModeData, Env: cfg.Env, StartAt: btime.UTCStamp(), DisplayLocation: snapshot.Location(), NetDisable: session.netDisable, Exchange: exchange, Storage: session.storage, ExchangeName: name, Market: market, ContractType: cfg.ContractType, Pairs: cfg.Pairs, Catalog: catalog})
	if createErr != nil {
		return nil, nil, errs.New(errs.CodeRunTime, createErr)
	}
	q, conn, queryErr := runtimeWebQueries(child, snapshot)
	if queryErr != nil {
		child.Close()
		child.Join()
		return nil, nil, queryErr
	}
	if queryErr = q.LoadExgSymbols(name); queryErr != nil {
		conn.Release()
		child.Close()
		child.Join()
		return nil, nil, queryErr
	}
	conn.Release()
	return child.BizDeps().DataDeps(), func() { child.Close(); child.Join() }, nil
}

func normalizeDevChildConfig(cfg *config.Config, name, market string) {
	cfg.Exchange.Name, cfg.MarketType = name, market
	if !banexg.IsContract(market) {
		cfg.ContractType = ""
	} else if cfg.ContractType == "" {
		cfg.ContractType = banexg.MarketSwap
	}
}

func devMaintenanceRunner(exchangeFor data.SpiderExchangeFactory) dev.DataToolsRunner {
	return func(ctx context.Context, deps *data.RuntimeDeps, args *dev.DataToolsArgs, pb *utils.StagedPrg) *errs.Error {
		q, conn, err := runtimeWebQueriesFor(ctx, deps)
		if err != nil {
			return err
		}
		defer conn.Release()
		cmd := &config.CmdArgs{Pairs: args.Pairs, TimeFrames: args.Periods, ExgReal: args.ExgReal, OutPath: args.Folder, InPath: args.Folder, Force: true}
		switch args.Action {
		case "download":
			exs := make(map[int32]*orm.ExSymbol)
			for _, pair := range args.Pairs {
				item, itemErr := deps.Symbols.GetExSymbol(args.Exg, pair)
				if itemErr != nil {
					return itemErr
				}
				exs[item.ID] = item
			}
			for i, tf := range args.Periods {
				base := float64(i) / float64(len(args.Periods))
				if err := orm.BulkDownOHLCVWithOptions(args.Exg, exs, tf, args.StartMs, args.EndMs, 0, func(done, total int) { pb.SetProgress("downKline", base+float64(done)/float64(total)) }, klineOptionsFor(deps)); err != nil {
					return err
				}
			}
			return nil
		case "export":
			file, fileErr := os.CreateTemp("", "banbot_web_export_*.yml")
			if fileErr != nil {
				return errs.New(errs.CodeIOWriteFail, fileErr)
			}
			defer os.Remove(file.Name())
			if _, fileErr = file.WriteString(args.Config); fileErr != nil {
				_ = file.Close()
				return errs.New(errs.CodeIOWriteFail, fileErr)
			}
			if fileErr = file.Close(); fileErr != nil {
				return errs.New(errs.CodeIOWriteFail, fileErr)
			}
			return orm.ExportKDataWithDeps(file.Name(), args.Folder, args.Concurrency, transferDepsFor(ctx, deps), pb)
		case "import":
			return orm.ImportDataWithDeps(args.Folder, args.Concurrency, transferDepsFor(ctx, deps), pb)
		case "purge":
			exs := make([]*orm.ExSymbol, 0, len(args.Pairs))
			for _, pair := range args.Pairs {
				item, itemErr := deps.Symbols.GetExSymbol(args.Exg, pair)
				if itemErr != nil {
					return itemErr
				}
				exs = append(exs, item)
			}
			return q.DelKData(exs, args.Periods, args.StartMs, args.EndMs)
		case "correct":
			all := make([]*orm.ExSymbol, 0)
			exchanges, listErr := q.ListExchanges(ctx)
			if listErr != nil {
				return errs.New(errs.CodeRunTime, listErr)
			}
			for _, name := range exchanges {
				items, listErr := q.ListSymbols(ctx, name)
				if listErr != nil {
					return errs.New(errs.CodeRunTime, listErr)
				}
				all = append(all, items...)
			}
			return orm.SyncKlineTFsWithDeps(cmd, orm.KlineSyncDeps{Context: ctx, Queries: q, Symbols: all, Logger: deps.Core.Log(), ConfirmAll: func(context.Context) (bool, error) { return true, nil }, ExchangeFactory: func(_ context.Context, name, market string) (banexg.BanExchange, *errs.Error) {
				return exchangeFor(name, market)
			}}, pb)
		}
		return errs.NewMsg(errs.CodeParamInvalid, "invalid data tool action")
	}
}

func runtimeWebQueriesFor(ctx context.Context, deps *data.RuntimeDeps) (*orm.Queries, *pgxpool.Conn, *errs.Error) {
	if deps == nil || deps.Storage == nil || deps.Symbols == nil || deps.Core == nil || deps.Clock == nil || deps.Config == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "complete web maintenance dependencies are required")
	}
	q, conn, err := deps.Storage.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return q.WithSeriesSymbolState(deps.Symbols).WithExchange(deps.Exchange).WithKlineRuntimeOptions(klineOptionsFor(deps)), conn, nil
}

func klineOptionsFor(deps *data.RuntimeDeps) orm.KlineRuntimeOptions {
	return orm.NewKlineRuntimeOptions(deps.Core, deps.Config.View(), deps.Clock.TimeMS(), deps.Storage)
}

func transferDepsFor(ctx context.Context, deps *data.RuntimeDeps) orm.KDataTransferDeps {
	options := klineOptionsFor(deps)
	return orm.KDataTransferDeps{
		Context:      ctx,
		Storage:      deps.Storage,
		Symbols:      deps.Symbols,
		Logger:       deps.Core.Log(),
		KlineOptions: &options,
	}
}

func persistDockerWebConfig(snapshot *config.Snapshot, args *dev.CmdArgs) *errs.Error {
	if !utils.IsDocker() || (len(args.Configs) == 0 && args.ConfigData == "") {
		return nil
	}
	paths := make([]string, 0, len(args.Configs)+2)
	localPath := filepath.Join(snapshot.DataDir, "config.local.yml")
	if _, err := os.Stat(localPath); err == nil {
		paths = append(paths, localPath)
	}
	if args.ConfigData != "" {
		file, err := os.CreateTemp("", "banbot_web_config_*.yml")
		if err != nil {
			return errs.New(errs.CodeIOWriteFail, err)
		}
		if _, err = file.WriteString(args.ConfigData); err != nil {
			_ = file.Close()
			_ = os.Remove(file.Name())
			return errs.New(errs.CodeIOWriteFail, err)
		}
		_ = file.Close()
		defer os.Remove(file.Name())
		paths = append(paths, file.Name())
	}
	for _, path := range args.Configs {
		paths = append(paths, snapshot.ParsePath(path))
	}
	content, err := config.MergeConfigPaths(paths)
	if err != nil {
		return errs.New(errs.CodeIOReadFail, err)
	}
	if err := os.WriteFile(localPath, []byte(content), 0644); err != nil {
		return errs.New(errs.CodeIOWriteFail, err)
	}
	return nil
}
