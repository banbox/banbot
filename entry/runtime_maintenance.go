package entry

import (
	"context"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

func runExplicitLoadCalendars(args *config.CmdArgs) *errs.Error {
	if args == nil || args.InPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--in is required")
	}
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newStorageRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	q, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	return biz.LoadCalendarsWithDeps(&config.CmdArgs{InPath: snapshot.ParsePath(args.InPath)}, q, rt.Core.Log())
}

func runExplicitDataExport(args *config.CmdArgs) *errs.Error {
	if args == nil || len(args.Configs) == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "-config is required")
	}
	if args.OutPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "-out is required")
	}
	configPath := args.Configs[len(args.Configs)-1]
	args.Configs = args.Configs[:len(args.Configs)-1]
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newStorageRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	q, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	symbols, err := loadDataTransferSymbols(rt.Context(), q, rt.Symbols.SIDAllocator())
	if err != nil {
		return err
	}
	options := orm.NewKlineRuntimeOptions(rt.Core, snapshot.View(), rt.Clock.TimeMS(), rt.Storage)
	return orm.ExportKDataWithDeps(snapshot.ParsePath(configPath), args.OutPath, args.Concur,
		orm.KDataTransferDeps{Context: rt.Context(), Storage: rt.Storage, Symbols: symbols, Logger: rt.Core.Log(), KlineOptions: &options}, nil)
}

func runExplicitDataImport(args *config.CmdArgs) *errs.Error {
	if args == nil || args.InPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "-in is required")
	}
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newStorageRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	q, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	symbols, err := loadDataTransferSymbols(rt.Context(), q, rt.Symbols.SIDAllocator())
	if err != nil {
		return err
	}
	options := orm.NewKlineRuntimeOptions(rt.Core, snapshot.View(), rt.Clock.TimeMS(), rt.Storage)
	return orm.ImportDataWithDeps(args.InPath, args.Concur,
		orm.KDataTransferDeps{Context: rt.Context(), Storage: rt.Storage, Symbols: symbols, Logger: rt.Core.Log(), KlineOptions: &options}, nil)
}

func loadDataTransferSymbols(ctx context.Context, q *orm.Queries, allocator *orm.SIDAllocator) (*orm.SymbolState, *errs.Error) {
	items, err := loadStoredKlineSyncSymbols(ctx, q)
	if err != nil {
		return nil, err
	}
	symbols := orm.NewSymbolStateWithAllocator(allocator)
	if err := symbols.SetExSymbols(items); err != nil {
		return nil, errs.New(core.ErrBadConfig, err)
	}
	return symbols, nil
}

func runExplicitRepairKlineRanges(args *config.CmdArgs) *errs.Error {
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
	pairs, dynamic := cfg.StaticPairs()
	if dynamic || len(pairs) == 0 {
		return errs.NewMsg(errs.CodeParamInvalid, "kline repair-ranges requires explicit pairs")
	}
	if err := orm.EnsureCurSymbolsWithRuntimeConfig(rt.Symbols, rt.Exchange, pairs, cfg, snapshot.DataDir, rt.Core); err != nil {
		return err
	}
	exsList := make(map[int32]*orm.ExSymbol, len(pairs))
	for _, pair := range pairs {
		exs, symbolErr := rt.Symbols.GetExSymbolCur(pair)
		if symbolErr != nil {
			return symbolErr
		}
		exsList[exs.ID] = exs
	}
	sess, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	var startMS, endMS int64
	if cfg.TimeRange != nil {
		startMS, endMS = cfg.TimeRange.StartMS, cfg.TimeRange.EndMS
	}
	return orm.RepairKlineRangesWithQueries(sess, exsList, args.TimeFrames, startMS, endMS)
}

func runExplicitVerifyData(args *config.CmdArgs) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newStorageRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()

	cfg := snapshot.View()
	sess, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	sess = sess.WithSeriesSymbolState(rt.Symbols)
	if cfg.Exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange config is required")
	}
	if err := sess.LoadExgSymbols(cfg.Exchange.Name); err != nil {
		return err
	}
	vArgs, err := orm.ParseVerifyArgsWithSymbolState(args, rt.Symbols, cfg.Exchange.Name, cfg.MarketType)
	if err != nil {
		return err
	}
	results, err := orm.VerifyDataRangesWithQueries(sess, rt.Symbols, vArgs)
	if err != nil {
		return err
	}
	orm.PrintVerifyResults(results)
	return nil
}
