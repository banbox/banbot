package entry

import (
	"context"
	"sort"
	"strings"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
)

type runtimeCompareOrdersOptions struct {
	backtest, botName, account string
	amountRate                 float64
	skipUnhit                  bool
}

// NewRuntimeCompareExgBTOrdersCommand creates the explicit-runtime order
// comparison CLI. Its exchange identity is taken from the first persisted
// backtest SID, never from the command's primary config by accident.
func NewRuntimeCompareExgBTOrdersCommand() *cobra.Command {
	options := &runtimeCompareOrdersOptions{}
	command := newRuntimeConfigCommand("cmp-orders", "compare exchange orders with a backtest", func(args *config.CmdArgs) *errs.Error {
		return runRuntimeCompareExgBTOrders(args, options)
	}, false)
	command.Aliases = []string{"cmp_orders"}
	command.Flags().StringVar(&options.botName, "bot-name", "", "bot name used for live trading")
	command.Flags().StringVar(&options.account, "account", "", "account whose API key will fetch orders")
	command.Flags().StringVar(&options.backtest, "bt-path", "", "backtest order file")
	command.Flags().Float64Var(&options.amountRate, "amt-rate", 0.1, "amount difference threshold from 0 to 1")
	command.Flags().BoolVar(&options.skipUnhit, "skip-unhit", true, "skip backtest pairs with no exchange orders")
	return command
}

func runRuntimeCompareExgBTOrders(args *config.CmdArgs, options *runtimeCompareOrdersOptions) *errs.Error {
	if options == nil || options.account == "" || options.backtest == "" || options.botName == "" {
		return errs.NewMsg(errs.CodeParamRequired, "account, bt-path, and bot-name are required")
	}
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	sid, err := opt.ReadBacktestOrderSID(snapshot.ParsePath(options.backtest))
	if err != nil {
		return err
	}
	identity, err := lookupStoredSymbolByID(session.ctx, session.storage, sid)
	if err != nil {
		return err
	}
	rt, err := newRuntimeForOrderIdentity(session, snapshot, core.RunModeLive, identity.Exchange, identity.Market)
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	if compareErr := opt.CompareExgBTOrdersWithRuntimeDeps(opt.CompareOrdersOptions{
		Backtest: snapshot.ParsePath(options.backtest), BotName: options.botName, Account: options.account,
		AmountRate: options.amountRate, SkipUnhit: options.skipUnhit,
	}, opt.CompareOrdersDeps{Runtime: rt.BizDeps(), Identity: identity, Logger: rt.Core.Log()}); compareErr != nil {
		return errs.New(errs.CodeRunTime, compareErr)
	}
	return nil
}

// newRuntimeForOrderIdentity clones a snapshot before selecting an exchange
// so commands with an explicit or persisted identity never reuse the primary
// runtime adapter.
func newRuntimeForOrderIdentity(session *explicitEntrySession, snapshot *config.Snapshot, mode, name, market string) (*runtime.Runtime, *errs.Error) {
	if session == nil || snapshot == nil || snapshot.View() == nil || name == "" || market == "" {
		return nil, errs.NewMsg(core.ErrBadConfig, "order runtime identity is required")
	}
	child, err := snapshotForOrderIdentity(snapshot, name, market)
	if err != nil {
		return nil, err
	}
	return session.newRuntime(child, mode, btime.UTCStamp())
}

func snapshotForOrderIdentity(snapshot *config.Snapshot, name, market string) (*config.Snapshot, *errs.Error) {
	if snapshot == nil || snapshot.View() == nil || snapshot.View().Exchange == nil || name == "" || market == "" {
		return nil, errs.NewMsg(core.ErrBadConfig, "order runtime identity is required")
	}
	child := snapshot.Clone()
	normalizeDevChildConfig(child.View(), name, market)
	return child, nil
}

func lookupStoredSymbolByID(ctx context.Context, storage *orm.Storage, sid int32) (*orm.ExSymbol, *errs.Error) {
	if storage == nil || sid <= 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "stored symbol SID is required")
	}
	q, conn, err := storage.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	exchanges, listErr := q.ListExchanges(ctx)
	if listErr != nil {
		return nil, errs.New(core.ErrDbReadFail, listErr)
	}
	for _, exchange := range exchanges {
		items, listErr := q.ListSymbols(ctx, exchange)
		if listErr != nil {
			return nil, errs.New(core.ErrDbReadFail, listErr)
		}
		for _, item := range items {
			if item != nil && item.ID == sid {
				return item, nil
			}
		}
	}
	return nil, errs.NewMsg(errs.CodeParamInvalid, "stored backtest symbol SID %d was not found", sid)
}

type runtimeDownOrdersOptions struct {
	account, exchange, market, pairs, start, end string
	force                                        bool
}

func NewRuntimeDownExgOrdersCommand() *cobra.Command {
	options := &runtimeDownOrdersOptions{}
	command := newRuntimeConfigCommand("down-order", "download exchange orders for an account", func(args *config.CmdArgs) *errs.Error {
		return runRuntimeDownExgOrders(args, options)
	}, false)
	command.Aliases = []string{"down_order"}
	command.Flags().StringVar(&options.account, "account", "", "account whose API key will fetch orders")
	command.Flags().StringVar(&options.exchange, "exchange", "", "exchange identifier")
	command.Flags().StringVar(&options.market, "market", "", "market: spot, linear, inverse, or option")
	command.Flags().StringVar(&options.start, "timestart", "", "start time in a supported time format")
	command.Flags().StringVar(&options.end, "timeend", "", "end time in a supported time format")
	command.Flags().StringVar(&options.pairs, "pairs", "", "comma-separated symbols")
	command.Flags().BoolVar(&options.force, "force", false, "force checking from the order timestamp")
	return command
}

func runRuntimeDownExgOrders(args *config.CmdArgs, options *runtimeDownOrdersOptions) *errs.Error {
	if options == nil || options.account == "" || options.start == "" || options.end == "" {
		return errs.NewMsg(errs.CodeParamRequired, "account, timestart, and timeend are required")
	}
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	if options.exchange == "" {
		if snapshot.View().Exchange == nil {
			return errs.NewMsg(core.ErrBadConfig, "exchange config is required")
		}
		options.exchange = snapshot.View().Exchange.Name
	}
	if options.market == "" {
		options.market = snapshot.View().MarketType
	}
	if options.pairs == "" {
		options.pairs = strings.Join(snapshot.View().Pairs, ",")
	}
	if options.pairs == "" {
		return errs.NewMsg(errs.CodeParamRequired, "pairs is required")
	}
	rt, err := newRuntimeForOrderIdentity(session, snapshot, core.RunModeLive, options.exchange, options.market)
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	deps := rt.BizDeps()
	startMS, parseErr := btime.ParseTimeMS(options.start)
	if parseErr != nil {
		return errs.New(errs.CodeParamInvalid, parseErr)
	}
	endMS, parseErr := btime.ParseTimeMS(options.end)
	if parseErr != nil {
		return errs.New(errs.CodeParamInvalid, parseErr)
	}
	set, err := biz.NewExgOrderSetWithRuntimeDeps(deps, options.account, options.exchange, options.market)
	if err != nil {
		return err
	}
	return set.Download(startMS, endMS, strings.Split(options.pairs, ","), options.force)
}

type runtimeTradeCloseOptions struct {
	accounts   string
	pairs      string
	strategies string
	exchange   bool
}

// NewRuntimeTradeCloseCommand creates the explicit-runtime close-order CLI.
func NewRuntimeTradeCloseCommand() *cobra.Command {
	options := &runtimeTradeCloseOptions{}
	command := newRuntimeConfigCommand("close-order", "close orders by account, pair, or strategy", func(args *config.CmdArgs) *errs.Error {
		return runRuntimeTradeClose(args, options)
	}, false)
	command.Aliases = []string{"close_order"}
	command.Flags().StringVar(&options.accounts, "account", "", "comma-separated accounts; empty means all")
	command.Flags().StringVar(&options.pairs, "pair", "", "comma-separated pairs; empty means all")
	command.Flags().StringVar(&options.strategies, "strat", "", "comma-separated strategies; empty means all")
	command.Flags().BoolVar(&options.exchange, "exg", false, "close exchange positions directly")
	return command
}

func runRuntimeTradeClose(args *config.CmdArgs, options *runtimeTradeCloseOptions) *errs.Error {
	if options == nil {
		return errs.NewMsg(core.ErrBadConfig, "close-order options are required")
	}
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
	deps := rt.BizDeps()
	if !options.exchange {
		accounts := make([]string, 0, len(deps.AccountConfigs()))
		for account, accountCfg := range deps.AccountConfigs() {
			if accountCfg != nil && !accountCfg.NoTrade {
				accounts = append(accounts, account)
			}
		}
		sort.Strings(accounts)
		if initErr := ormo.InitLiveTasksWithState(deps.Orders, accounts, snapshot.View().Name, deps.Core.EnvReal); initErr != nil {
			return initErr
		}
	}
	_, err = live.CloseOrdersWithRuntimeDeps(deps, live.TradeCloseRequest{
		Accounts: []string{options.accounts}, Pairs: []string{options.pairs}, Strategies: []string{options.strategies},
		Exchange: options.exchange, Confirmed: true,
	})
	return err
}
