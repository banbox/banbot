package entry

import (
	"context"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
)

func runExplicitDataServer(args *config.CmdArgs) *errs.Error {
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

	exchangeFor, closeExchanges := session.spiderExchangeFactory(snapshot, rt.Exchange)
	defer closeExchanges()
	runtimeFor := biz.DataServerRuntimeFactory(func(ctx context.Context, name, market string) (*data.RuntimeDeps, func(), *errs.Error) {
		if name == rt.Core.ExgName && market == rt.Core.Market {
			return rt.DataDeps(), func() {}, nil
		}
		return newDevChildRuntime(session, snapshot, ctx, name, market, exchangeFor)
	})
	server, err := biz.NewDataServer(runtimeFor, rt.Core.Log())
	if err != nil {
		return err
	}
	defer func() { server.Stop(); server.Join() }()
	return server.Serve(rt.Context(), ":6789")
}

func runExplicitCorrelation(args *config.CmdArgs) *errs.Error {
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
	q = q.WithSeriesSymbolState(rt.Symbols).WithExchange(rt.Exchange).
		WithKlineRuntimeOptions(orm.NewKlineRuntimeOptions(rt.Core, snapshot.View(), rt.Clock.TimeMS(), rt.Storage))
	deps := rt.BizDeps()
	return biz.CalcCorrelationWithRuntimeDeps(args, &biz.CorrelationRuntimeDeps{
		Queries: q, Runtime: &deps, Logger: rt.Core.Log(),
	})
}

// NewRuntimeKlineConsistencyCommand compares a recorded live dump with the
// configured runtime's local series. It deliberately exposes normal runtime
// config flags because its symbol identity and storage cannot be inferred from
// the dump alone.
func NewRuntimeKlineConsistencyCommand() *cobra.Command {
	args := &config.CmdArgs{}
	options := &runtimeCommandFlags{}
	command := &cobra.Command{
		Use:     "test-live-bars DUMP_FILE",
		Aliases: []string{"test_live_bars"},
		Short:   "compare live-trade klines with local data",
		Args:    cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, values []string) error {
			args.BTStrictSet = command.Flags().Changed("bt-strict")
			args.NetDisable, args.CPUProfile, args.MemProfile = options.netDisable, options.cpuProfile, options.memProfile
			return runExplicitKlineConsistency(args, values[0])
		},
	}
	bindCommonFlags(args, options, command.Flags(), true)
	return command
}

func runExplicitKlineConsistency(args *config.CmdArgs, dumpPath string) error {
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
	if snapshot.View().Exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange config is required")
	}
	q, conn, err := rt.Storage.Conn(rt.Context())
	if err != nil {
		return err
	}
	defer conn.Release()
	q = q.WithSeriesSymbolState(rt.Symbols).WithExchange(rt.Exchange).
		WithKlineRuntimeOptions(orm.NewKlineRuntimeOptions(rt.Core, snapshot.View(), rt.Clock.TimeMS(), rt.Storage))
	if err := q.LoadExgSymbols(snapshot.View().Exchange.Name); err != nil {
		return err
	}
	return biz.TestKLineConsistencyWithRuntimeDeps(snapshot.ParsePath(dumpPath), biz.KlineConsistencyDeps{
		Queries: q, Symbols: rt.Symbols, Logger: rt.Core.Log(),
	})
}
