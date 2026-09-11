package opt

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	banutils "github.com/banbox/banexg/utils"
	"github.com/banbox/cron/v3"
	"go.uber.org/zap"
)

const (
	ShowNum                       = 600
	runtimeMinPairCronGapMS int64 = 30 * 60 * 1000
)

type BackTestLite struct {
	biz.Trader
	*BTResult
	dp              *data.HistProvider
	symbols         *orm.SymbolState
	isOpt           bool // whether is hyper optimization
	runErr          *errs.Error
	stoppedEarly    bool
	legacyClockSync bool
}

type BackTest struct {
	*BackTestLite
	lastDumpMs           int64 // The last time the backtest status was saved 上一次保存回测状态的时间
	PBar                 *utils.StagedPrg
	afterBacktest        func(*BackTest)
	dataPrep             bool
	dataPrepErr          *errs.Error
	nextRefresh          int64 // The time of the next refresh of the trading pair 下一次刷新交易对的时间
	schedule             cron.Schedule
	seriesRuntime        *data.SeriesRuntime
	loopMainFn           func() *errs.Error
	historicalCloseMS    []int64
	historicalCloseIndex int
	outputOwned          bool
}

func (b *BackTest) SetAfterBacktest(callback func(*BackTest)) {
	if b != nil {
		b.afterBacktest = callback
	}
}

func (b *BackTest) runAfterBacktestCallback() {
	if b == nil {
		return
	}
	if b.afterBacktest != nil {
		b.afterBacktest(b)
	} else if (b.BackTestLite == nil || b.BackTestLite.RuntimeDependencies() == nil) && AfterBacktest != nil {
		AfterBacktest(b)
	}
}

func (b *BackTestLite) runConfig() *config.Config {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			return deps.ConfigView()
		}
	}
	return &config.Data
}

func (b *BackTestLite) runSnapshot() *config.Snapshot {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			return deps.Config
		}
	}
	return nil
}

func (b *BackTestLite) runTimeRange() *config.TimeTuple {
	if b != nil && b.RuntimeDependencies() != nil {
		if cfg := b.runConfig(); cfg != nil {
			return cfg.TimeRange
		}
		return nil
	}
	return config.TimeRange
}

func (b *BackTestLite) strictBacktest() bool {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			return deps.StrictBacktest()
		}
	}
	return config.StrictBacktest()
}

func (b *BackTestLite) defaultAccount() string {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			if deps.DefaultAccount != "" {
				return deps.DefaultAccount
			}
			if deps.Config != nil {
				return deps.Config.DefaultAccount()
			}
			if accounts := deps.AccountConfigs(); len(accounts) == 1 {
				for account := range accounts {
					return account
				}
			}
			return "default"
		}
	}
	return config.DefAcc
}

func (b *BackTestLite) accountConfigs() map[string]*config.AccountConfig {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			return deps.AccountConfigs()
		}
	}
	return config.Accounts
}

func runtimeConfigForDeps(deps *biz.RuntimeDeps) *config.Config {
	if deps != nil {
		return deps.ConfigView()
	}
	return &config.Data
}

func runtimeTimeRangeForDeps(deps *biz.RuntimeDeps) *config.TimeTuple {
	if deps != nil {
		if cfg := deps.ConfigView(); cfg != nil {
			return cfg.TimeRange
		}
		return nil
	}
	return config.TimeRange
}

func runtimeCronSchedule(exp string, deps *biz.RuntimeDeps) (cron.Schedule, error) {
	if deps == nil {
		return utils.NewCronScheduler(exp)
	}
	location := time.UTC
	if deps.Config != nil {
		location = deps.Config.Location()
	}
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return parser.Parse(exp, location)
}

/*
NewBackTestLite 创建一个临时内部回测，仅用于寻找回测未平仓订单来接力
Create a temporary internal backtest, solely for the purpose of finding backtest open orders to relay.
*/
func NewBackTestLite(isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) *BackTestLite {
	return newBackTestLite(biz.NewTrader(strat.NewBatchState()), nil, isOpt, onBar, getEnd, pBar, nil)
}

// NewBackTestLiteWithBatchState lets a composition root make the batch queue
// part of the same runtime as the backtest. The session must remain active for
// the returned runner's lifetime because setup still uses legacy globals.
func NewBackTestLiteWithBatchState(session LegacySession, batchState *strat.BatchState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) *BackTestLite {
	session.require()
	lite := newBackTestLite(biz.NewTrader(batchState), nil, isOpt, onBar, getEnd, pBar, nil)
	lite.legacyClockSync = true
	return lite
}

// NewBackTestLiteWithBatchAndSymbolState binds both mutable runtime state
// objects used by a backtest. A nil symbol state preserves the legacy facade;
// the session must remain active for the returned runner's lifetime.
func NewBackTestLiteWithBatchAndSymbolState(session LegacySession, batchState *strat.BatchState, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) *BackTestLite {
	session.require()
	if batchState == nil {
		batchState = strat.NewBatchState()
	}
	lite := newBackTestLite(biz.NewTrader(batchState), symbols, isOpt, onBar, getEnd, pBar, nil)
	lite.legacyClockSync = true
	return lite
}

// NewBackTestLiteWithRuntimeDeps binds the typed runtime state used by the
// runner hot path. Strategy jobs, wallets, and orders remain legacy globals;
// the session must remain active for the returned runner's lifetime.
func NewBackTestLiteWithRuntimeDeps(session LegacySession, deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) *BackTestLite {
	session.require()
	lite := NewBackTestLiteWithRuntimeDataDepsOwned(deps, symbols, isOpt, onBar, getEnd, pBar,
		legacyDataRuntimeDeps(&deps, symbols))
	lite.legacyClockSync = true
	return lite
}

// NewBackTestLiteWithRuntimeDataDeps is the explicit composition-root entry
// point. The older constructor remains compatible by snapshotting the legacy
// config/exchange facade at its boundary; the session must remain active for
// the returned runner's lifetime.
func NewBackTestLiteWithRuntimeDataDeps(session LegacySession, deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg, dataDeps *data.RuntimeDeps) *BackTestLite {
	session.require()
	lite := NewBackTestLiteWithRuntimeDataDepsOwned(deps, symbols, isOpt, onBar, getEnd, pBar, dataDeps)
	lite.legacyClockSync = true
	return lite
}

// NewBackTestLiteWithRuntimeDataDepsOwned constructs a backtest over explicit
// runtime state. It does not require or touch the legacy session gate.
func NewBackTestLiteWithRuntimeDataDepsOwned(deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg, dataDeps *data.RuntimeDeps) *BackTestLite {
	resolvedSymbols, symbolsErr := resolveBacktestSymbols(deps.Symbols, symbols, dataDepsSymbolState(dataDeps))
	if symbolsErr == nil {
		symbols = resolvedSymbols
		if deps.Symbols == nil {
			deps.Symbols = symbols
		}
	} else if symbols == nil {
		// Keep the constructed runner usable enough to report the identity
		// error through its normal run-error path without selecting a foreign
		// catalog. The caller still receives a deterministic failure from the
		// first loop operation.
		if deps.Symbols != nil {
			symbols = deps.Symbols
		} else {
			symbols = dataDepsSymbolState(dataDeps)
		}
		deps.Symbols = symbols
	}
	trader := biz.NewTraderWithRuntimeDeps(deps)
	lite := newBackTestLite(trader, symbols, isOpt, onBar, getEnd, pBar, dataDeps)
	if symbolsErr != nil {
		lite.setRunError(symbolsErr)
	}
	return lite
}

func legacyDataRuntimeDeps(deps *biz.RuntimeDeps, symbols *orm.SymbolState) *data.RuntimeDeps {
	if deps != nil {
		return bindDataRuntimeDeps(deps, symbols, &data.RuntimeDeps{
			Config:   deps.Config,
			Exchange: deps.Exchange,
			Symbols:  deps.Symbols,
		})
	}
	return bindDataRuntimeDeps(deps, symbols, &data.RuntimeDeps{
		Config:   config.NewSnapshot(&config.Data),
		Exchange: exg.Default,
	})
}

func bindDataRuntimeDeps(traderDeps *biz.RuntimeDeps, symbols *orm.SymbolState, supplied *data.RuntimeDeps) *data.RuntimeDeps {
	bound := &data.RuntimeDeps{}
	if supplied != nil {
		*bound = *supplied
	}
	if traderDeps != nil {
		if bound.Core == nil {
			bound.Core = traderDeps.Core
		}
		if bound.Clock == nil {
			bound.Clock = traderDeps.Clock
		}
		if bound.Market == nil {
			bound.Market = traderDeps.Market
		}
		if bound.Config == nil {
			bound.Config = traderDeps.Config
		}
		if bound.Symbols == nil {
			bound.Symbols = traderDeps.Symbols
		}
		if bound.Storage == nil {
			bound.Storage = traderDeps.Storage
		}
		if bound.Strategies == nil {
			bound.Strategies = traderDeps.Strategies
		}
		if bound.Exchange == nil {
			bound.Exchange = traderDeps.Exchange
		}
	}
	if supplied != nil && supplied.Catalog != nil {
		bound.Catalog = supplied.Catalog
	}
	if bound.Symbols == nil {
		bound.Symbols = symbols
	}
	if bound.ExchangeName == "" && bound.Core != nil {
		bound.ExchangeName = bound.Core.ExgName
	}
	if bound.MarketType == "" && bound.Core != nil {
		bound.MarketType = bound.Core.Market
	}
	if bound.Exchange != nil && (bound.ExchangeName == "" || bound.MarketType == "") {
		if info := bound.Exchange.Info(); info != nil {
			if bound.ExchangeName == "" {
				bound.ExchangeName = info.ID
			}
			if bound.MarketType == "" {
				bound.MarketType = info.MarketType
			}
		}
	}
	return bound
}

func newBackTestLite(trader biz.Trader, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg, dataDeps *data.RuntimeDeps) *BackTestLite {
	var runtimeDepsErr *errs.Error
	if deps := trader.RuntimeDependencies(); deps != nil {
		traderSymbols := deps.Symbols
		if _, symbolsErr := resolveBacktestSymbols(traderSymbols, symbols, dataDepsSymbolState(dataDeps)); symbolsErr != nil {
			runtimeDepsErr = symbolsErr
		}
		dataDeps = bindDataRuntimeDeps(deps, symbols, dataDeps)
		symbols = dataDeps.Symbols
		if deps.Symbols == nil {
			deps.Symbols = symbols
		}
		if runtimeDepsErr == nil {
			runtimeDepsErr = validateDataRuntimeDeps(deps, symbols, dataDeps)
		}
	}
	b := &BackTestLite{
		Trader:   trader,
		BTResult: NewBTResult(),
		symbols:  symbols,
		isOpt:    isOpt,
	}
	b.BTResult.runtimeDeps = b.RuntimeDependencies()
	b.BTResult.reportDeps = reportDepsFromRuntime(b.RuntimeDependencies())
	if runtimeDepsErr != nil {
		b.runErr = runtimeDepsErr
	}
	var wallets *biz.BanWallets
	if deps := b.RuntimeDependencies(); deps != nil {
		wallets = biz.InitFakeWalletsWithRuntimeDeps(*deps)
	} else {
		biz.InitFakeWallets()
		wallets = biz.GetWallets(config.DefAcc)
	}
	b.TotalInvest = wallets.TotalLegal(nil, false)
	if onBar == nil {
		onBar = func(evt *orm.DataSeries) {
			b.FeedDataSeries(evt)
		}
	}
	if b.RuntimeDependencies() == nil {
		b.dp = data.NewHistProviderWithSymbolState(symbols, onBar, b.OnEnvEnd, getEnd, !isOpt, pBar)
	} else {
		b.dp = data.NewHistProviderWithRuntimeDeps(dataDeps, onBar, b.OnEnvEnd, getEnd, !isOpt, pBar)
	}
	b.dp.SetAllowDownload(allowBacktestKlineDownloadForRuntime(isOpt, b.RuntimeDependencies()))
	if state := b.runtimeCore(); state != nil {
		state.OnExit(b.dp.Terminate)
	}
	pairHooks := strat.PairUpdateHooks{
		SubWarmPairs: b.dp.SubWarmPairs,
		SymbolState:  symbols,
		Core:         b.runtimeCore(),
		StrategyState: func() *strat.State {
			if deps := b.RuntimeDependencies(); deps != nil {
				return deps.Strategies
			}
			return nil
		}(),
		LookupSymbol: func(pair string) (*orm.ExSymbol, *errs.Error) {
			if b.RuntimeDependencies() != nil && symbols == nil {
				return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required to resolve %s", pair)
			}
			if symbols == nil {
				return orm.GetExSymbolCur(pair)
			}
			return symbols.GetExSymbolCur(pair)
		},
		ExitOrders: func(acc string, orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
			if deps := b.RuntimeDependencies(); deps != nil {
				return biz.GetOdMgrWithState(deps.Trading, acc).ExitAndFill(orders, req)
			}
			return biz.GetOdMgr(acc).ExitAndFill(orders, req)
		},
	}
	if deps := b.RuntimeDependencies(); deps != nil && deps.Strategies != nil {
		deps.Strategies.SetPairUpdateHooks(pairHooks)
	} else {
		strat.SetPairUpdateHooks(pairHooks)
	}
	stopBacktest := core.StopAll
	if state := b.runtimeCore(); state != nil {
		stopBacktest = state.Stop
	}
	if isOpt {
		stopBacktest = b.dp.Terminate
	}
	var prices *com.PriceState
	var clock *btime.ClockState
	if deps := b.RuntimeDependencies(); deps != nil {
		if deps.Market != nil {
			prices = deps.Market.Prices
		}
		clock = deps.Clock
	}
	if deps := b.RuntimeDependencies(); deps != nil {
		biz.InitLocalOrderMgrWithRuntimeDeps(*deps, b.orderCB, !isOpt, stopBacktest)
	} else {
		biz.InitLocalOrderMgrWithPriceState(b.orderCB, !isOpt, prices, clock, stopBacktest)
	}
	return b
}

func allowBacktestKlineDownload(isOpt bool) bool {
	return !isOpt && !config.Data.BTNoKlineDownload
}

func allowBacktestKlineDownloadForRuntime(isOpt bool, deps *biz.RuntimeDeps) bool {
	if isOpt {
		return false
	}
	if deps == nil {
		return allowBacktestKlineDownload(false)
	}
	cfg := deps.ConfigView()
	return cfg == nil || !cfg.BTNoKlineDownload
}

func (b *BackTestLite) FeedDataSeries(evt *orm.DataSeries) bool {
	if b == nil || evt == nil {
		return false
	}
	if deps := b.RuntimeDependencies(); deps != nil && deps.Core != nil {
		select {
		case <-deps.Core.Done():
			return false
		default:
		}
	}
	if b.RuntimeDependencies() != nil {
		b.SetTimeMS(seriesEndMS(evt))
	}
	if orm.NormalizeSeriesSource(evt.Source) != orm.SeriesSourceKline || !evt.HasOHLCV() {
		if err := b.Trader.FeedDataSeries(evt); err != nil {
			log.Error("FeedDataSeries fail", zap.Int32("sid", evt.Sid), zap.Error(err))
			b.setRunError(err)
			return false
		}
		return true
	}
	b.BarNum += 1
	curTime := b.TimeMS()
	batchState := b.batchStateForRun()
	lastBatchMS := batchState.LastBatchMS()
	if curTime > lastBatchMS {
		// Enter the next timeframe and trigger the batch entry callback
		// 进入下一个时间帧，触发批量入场回调
		b.SetTimeMS(lastBatchMS)
		waitNum := biz.TryFireBatchesWithRuntimeDeps(b.RuntimeDependencies(), batchState, curTime, evt.IsWarmUp)
		if waitNum > 0 {
			log.Warn(fmt.Sprintf("batch job exec fail, wait: %v", waitNum))
		}
		batchState.SetLastBatchMS(curTime)
		b.SetTimeMS(curTime)
	}
	if curTime > b.lastTime {
		b.lastTime = curTime
		b.TimeNum += 1
		if !evt.IsWarmUp {
			b.setCheckWallets(true)
		}
	}
	if errRun := b.Trader.FeedDataSeries(evt); errRun != nil {
		if errRun.Code == core.ErrLiquidation {
			b.onLiquidation(evt.Symbol())
		} else {
			log.Error("FeedDataSeries fail", zap.String("p", evt.Symbol()), zap.Error(errRun))
			b.setRunError(errRun)
		}
		return false
	}
	if !b.botRunning() {
		b.stoppedEarly = true
		b.dp.Terminate()
		return false
	}
	return true
}

// SetTimeMS keeps the legacy feeder facade aligned with the explicit clock
// while the remaining historical provider code still reads btime.CurTimeMS.
// Entry runners hold the legacy gate, so this compatibility write is safe and
// adds no lookup or synchronization to the typed state path.
func (b *BackTestLite) SetTimeMS(timeMS int64) {
	b.Trader.SetTimeMS(timeMS)
	if b.legacyClockSync {
		btime.CurTimeMS = timeMS
	}
}

func seriesEndMS(evt *orm.DataSeries) int64 {
	if evt == nil {
		return 0
	}
	if evt.EndMS > evt.TimeMS {
		return evt.EndMS
	}
	if evt.TimeFrame != "" {
		return evt.TimeMS + int64(banutils.TFToSecs(evt.TimeFrame))*1000
	}
	return evt.TimeMS
}

func (b *BackTestLite) runtimeCore() *core.State {
	if b == nil {
		return nil
	}
	if deps := b.RuntimeDependencies(); deps != nil {
		return deps.Core
	}
	return nil
}

func (b *BackTestLite) setCheckWallets(check bool) {
	if state := b.runtimeCore(); state != nil {
		state.CheckWallets = check
		return
	}
	core.CheckWallets = check
}

func (b *BackTestLite) checkWallets() bool {
	if state := b.runtimeCore(); state != nil {
		return state.CheckWallets
	}
	return core.CheckWallets
}

func (b *BackTestLite) botRunning() bool {
	if state := b.runtimeCore(); state != nil {
		return state.BotRunning
	}
	return core.BotRunning
}

func (b *BackTestLite) batchStateForRun() *strat.BatchState {
	return b.Trader.BatchState()
}

func (b *BackTestLite) setRunError(err *errs.Error) {
	if err == nil || b.runErr != nil {
		return
	}
	b.runErr = err
	if b.dp != nil {
		b.dp.Terminate()
	}
}

func (b *BackTestLite) resolveLoopError(err *errs.Error) *errs.Error {
	if err != nil {
		return err
	}
	return b.runErr
}

func (b *BackTestLite) onLiquidation(symbol string) {
	date := btime.ToDateStr(b.TimeMS(), "")
	chargeOnBomb := false
	if cfg := b.runConfig(); cfg != nil {
		chargeOnBomb = cfg.ChargeOnBomb
	}
	if chargeOnBomb {
		account := b.defaultAccount()
		var wallets *biz.BanWallets
		if deps := b.RuntimeDependencies(); deps != nil {
			wallets = biz.InitFakeWalletsWithRuntimeDeps(*deps, symbol)
		} else {
			wallets = biz.GetWallets(account)
		}
		oldVal := wallets.TotalLegal(nil, false)
		if b.RuntimeDependencies() == nil {
			biz.InitFakeWallets(symbol)
		}
		newVal := wallets.TotalLegal(nil, false)
		b.TotalInvest += newVal - oldVal
		log.Warn(fmt.Sprintf("wallet %s BOMB at %s, reset wallet and continue..", symbol, date))
	} else {
		log.Warn(fmt.Sprintf("wallet %s BOMB at %s, exit", symbol, date))
		if !b.isOpt {
			if state := b.runtimeCore(); state != nil {
				state.Stop()
			} else if core.StopAll != nil {
				core.StopAll()
			}
		}
		b.dp.Terminate()
	}
}

func (b *BackTestLite) orderCB(order *ormo.InOutOrder, isEnter bool) {
	account := b.defaultAccount()
	var deps *biz.RuntimeDeps
	if b != nil {
		deps = b.RuntimeDependencies()
	}
	var wallets *biz.BanWallets
	if deps != nil {
		if deps.DefaultAccount != "" {
			account = deps.DefaultAccount
		}
		if deps.Trading != nil {
			wallets = deps.Trading.Wallet(account)
		}
	}
	if isEnter {
		openNum := 0
		if deps != nil && deps.Orders != nil {
			openNum = deps.Orders.OpenNum(account, ormo.InOutStatusPartEnter)
		} else {
			openNum = ormo.OpenNum(account, ormo.InOutStatusPartEnter)
		}
		if openNum > b.MaxOpenOrders {
			b.MaxOpenOrders = openNum
		}
	} else {
		if wallets == nil {
			wallets = biz.GetWallets(account)
		}
		// 更新单笔开单金额
		wallets.TryUpdateStakePctAmt()
		cfg := b.runConfig()
		if cfg != nil && cfg.DrawBalanceOver > 0 {
			quoteLegal := wallets.AvaLegal(cfg.StakeCurrency)
			if quoteLegal > cfg.DrawBalanceOver {
				wallets.WithdrawLegal(quoteLegal-cfg.DrawBalanceOver, cfg.StakeCurrency)
			}
		}
	}
}

func NewBackTest(isOpt bool, outDir string) (*BackTest, *errs.Error) {
	return newBackTest(biz.NewTrader(strat.NewBatchState()), nil, isOpt, outDir, nil)
}

// NewBackTestWithBatchState binds the backtest to a composition root's batch
// queue. A nil state creates a private state; the session must remain active
// for the returned runner's lifetime.
func NewBackTestWithBatchState(session LegacySession, batchState *strat.BatchState, isOpt bool, outDir string) (*BackTest, *errs.Error) {
	session.require()
	return newBackTest(biz.NewTrader(batchState), nil, isOpt, outDir, nil)
}

// NewBackTestWithBatchAndSymbolState binds a backtest to the composition
// root's batch queue and symbol indexes. A nil symbol state preserves legacy
// package-level symbol APIs for existing callers; the session must remain
// active for the returned runner's lifetime.
func NewBackTestWithBatchAndSymbolState(session LegacySession, batchState *strat.BatchState, symbols *orm.SymbolState, isOpt bool, outDir string) (*BackTest, *errs.Error) {
	session.require()
	return newBackTest(biz.NewTrader(batchState), symbols, isOpt, outDir, nil)
}

// NewBackTestWithRuntimeDeps constructs a backtest over explicit runtime state
// while retaining the legacy strategy, order, and wallet globals.
func NewBackTestWithRuntimeDeps(session LegacySession, deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, outDir string) (*BackTest, *errs.Error) {
	session.require()
	backtest, err := newBackTestWithRuntimeDataDepsCompat(deps, symbols, isOpt, outDir,
		legacyDataRuntimeDeps(&deps, symbols))
	if backtest != nil {
		backtest.legacyClockSync = true
	}
	return backtest, err
}

// NewBackTestWithRuntimeDataDeps is the explicit composition-root entry point
// for a backtest. dataDeps carries runtime-owned data dependencies, but
// strategy, order, wallet, and other legacy facades still require the session
// to remain active for the returned runner's lifetime.
func NewBackTestWithRuntimeDataDeps(session LegacySession, deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, outDir string, dataDeps *data.RuntimeDeps) (*BackTest, *errs.Error) {
	session.require()
	bt, err := newBackTestWithRuntimeDataDepsCompat(deps, symbols, isOpt, outDir, dataDeps)
	if bt != nil {
		bt.legacyClockSync = true
	}
	return bt, err
}

// NewBackTestWithRuntimeDataDepsOwned constructs a backtest over explicit
// runtime state. The returned runner does not depend on a legacy session.
func NewBackTestWithRuntimeDataDepsOwned(deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, outDir string, dataDeps *data.RuntimeDeps) (*BackTest, *errs.Error) {
	resolvedSymbols, symbolsErr := resolveBacktestSymbols(deps.Symbols, symbols, dataDepsSymbolState(dataDeps))
	if symbolsErr != nil {
		return nil, symbolsErr
	}
	symbols = resolvedSymbols
	if deps.Symbols == nil {
		deps.Symbols = symbols
	}
	if err := validateBacktestRuntimeDeps(&deps, symbols); err != nil {
		return nil, err
	}
	trader := biz.NewTraderWithRuntimeDeps(deps)
	return newBackTest(trader, symbols, isOpt, outDir, dataDeps)
}

func newBackTestWithRuntimeDataDepsCompat(deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool,
	outDir string, dataDeps *data.RuntimeDeps) (*BackTest, *errs.Error) {
	resolvedSymbols, symbolsErr := resolveBacktestSymbols(deps.Symbols, symbols, dataDepsSymbolState(dataDeps))
	if symbolsErr != nil {
		return nil, symbolsErr
	}
	symbols = resolvedSymbols
	if deps.Symbols == nil {
		deps.Symbols = symbols
	}
	trader := biz.NewTraderWithRuntimeDeps(deps)
	if err := validateDataRuntimeDeps(trader.RuntimeDependencies(), symbols, dataDeps); err != nil {
		return nil, err
	}
	return newBackTest(trader, symbols, isOpt, outDir, dataDeps)
}

func dataDepsSymbolState(dataDeps *data.RuntimeDeps) *orm.SymbolState {
	if dataDeps == nil {
		return nil
	}
	return dataDeps.Symbols
}

// resolveBacktestSymbols keeps the symbol catalog identity consistent across
// the constructor's three possible owners. A later dependency set must not
// silently replace a catalog that was already supplied by the caller.
func resolveBacktestSymbols(depsSymbols, explicitSymbols, suppliedSymbols *orm.SymbolState) (*orm.SymbolState, *errs.Error) {
	resolved := explicitSymbols
	if depsSymbols != nil {
		if resolved != nil && resolved != depsSymbols {
			return nil, errs.NewMsg(core.ErrRunTime, "backtest runtime symbols do not match dependencies")
		}
		resolved = depsSymbols
	}
	if suppliedSymbols != nil {
		if resolved != nil && resolved != suppliedSymbols {
			return nil, errs.NewMsg(core.ErrRunTime, "data runtime symbols do not match backtest runtime")
		}
		resolved = suppliedSymbols
	}
	return resolved, nil
}

// validateDataRuntimeDeps rejects a supplied data dependency set that points
// at a different runtime than the trader. Provider-only extensions such as a
// data source catalog and callback tracker may vary; mutable state identities
// must remain shared with the composition root.
func validateDataRuntimeDeps(deps *biz.RuntimeDeps, symbols *orm.SymbolState, supplied *data.RuntimeDeps) *errs.Error {
	if deps == nil || supplied == nil {
		return nil
	}
	if supplied.Core != nil && supplied.Core != deps.Core {
		return errs.NewMsg(core.ErrRunTime, "data runtime core does not match trader runtime")
	}
	if supplied.Clock != nil && supplied.Clock != deps.Clock {
		return errs.NewMsg(core.ErrRunTime, "data runtime clock does not match trader runtime")
	}
	if supplied.Config != nil && supplied.Config != deps.Config {
		return errs.NewMsg(core.ErrRunTime, "data runtime config does not match trader runtime")
	}
	if supplied.Market != nil && supplied.Market != deps.Market {
		return errs.NewMsg(core.ErrRunTime, "data runtime market does not match trader runtime")
	}
	if supplied.Symbols != nil && supplied.Symbols != symbols {
		return errs.NewMsg(core.ErrRunTime, "data runtime symbols do not match trader runtime")
	}
	if supplied.Storage != nil && supplied.Storage != deps.Storage {
		return errs.NewMsg(core.ErrRunTime, "data runtime storage does not match trader runtime")
	}
	if supplied.Strategies != nil && supplied.Strategies != deps.Strategies {
		return errs.NewMsg(core.ErrRunTime, "data runtime strategies do not match trader runtime")
	}
	if supplied.Exchange != nil && supplied.Exchange != deps.Exchange {
		return errs.NewMsg(core.ErrRunTime, "data runtime exchange does not match trader runtime")
	}
	if supplied.Dump != nil && supplied.Dump != deps.Dump {
		return errs.NewMsg(core.ErrRunTime, "data runtime dump does not match trader runtime")
	}
	if supplied.ExchangeName != "" && deps.Core != nil && supplied.ExchangeName != deps.Core.ExgName {
		return errs.NewMsg(core.ErrRunTime, "data runtime exchange name does not match trader runtime")
	}
	if supplied.MarketType != "" && deps.Core != nil && supplied.MarketType != deps.Core.Market {
		return errs.NewMsg(core.ErrRunTime, "data runtime market type does not match trader runtime")
	}
	return nil
}

func newBackTest(trader biz.Trader, symbols *orm.SymbolState, isOpt bool, outDir string, dataDeps *data.RuntimeDeps) (*BackTest, *errs.Error) {
	stages := []string{"init", "listMs", "loadPairs", "tfScores", "loadJobs", "warmJobs", "downKline", "runBT"}
	stgWeis := []float64{1, 1, 2, 2, 1, 2, 10, 10}
	b := &BackTest{
		PBar: utils.NewStagedPrg(stages, stgWeis),
	}
	getEnd := func() int64 {
		if b.nextRefresh > 0 {
			return b.nextRefresh
		}
		runRange := b.runTimeRange()
		if runRange == nil {
			return 0
		}
		return runRange.EndMS
	}
	b.BackTestLite = newBackTestLite(trader, symbols, isOpt, b.FeedDataSeries, getEnd, b.PBar, dataDeps)
	if outDir == "" && !isOpt {
		cfg := b.runConfig()
		if cfg == nil {
			return nil, errs.NewMsg(core.ErrBadConfig, "backtest runtime config is required")
		}
		hash, err := cfg.HashCode()
		if err != nil {
			return nil, err
		}
		dataDir := ""
		if snapshot := b.runSnapshot(); snapshot != nil {
			dataDir = snapshot.DataDir
		} else {
			dataDir = config.GetDataDir()
		}
		baseDir := filepath.Join(dataDir, "backtest", hash)
		allocatedDir, outputErr := config.AllocateOutputDir(baseDir)
		if outputErr != nil {
			return nil, errs.New(core.ErrIOWriteFail, outputErr)
		}
		outDir = allocatedDir
		b.outputOwned = true
	}
	if snapshot := b.runSnapshot(); snapshot != nil {
		b.OutDir = snapshot.ParsePath(outDir)
	} else {
		b.OutDir = config.ParsePath(outDir)
		config.LoadPerfs(config.GetDataDir())
	}
	return b, nil
}

func (b *BackTest) Init() *errs.Error {
	cfg := b.runConfig()
	runRange := b.runTimeRange()
	if cfg == nil || runRange == nil {
		return errs.NewMsg(core.ErrBadConfig, "backtest runtime config and time range are required")
	}
	if err := validateBacktestRuntimeDeps(b.RuntimeDependencies(), b.symbols); err != nil {
		return err
	}
	b.SetTimeMS(runRange.StartMS)
	b.historicalCloseMS = historicalCloseBoundaries(cfg.HistoricalCoverage, runRange)
	b.historicalCloseIndex = 0
	b.MinReal = math.MaxFloat64
	dataDir := ""
	if snapshot := b.runSnapshot(); snapshot != nil {
		dataDir = snapshot.DataDir
	} else {
		dataDir = config.GetDataDirSafe()
	}
	log.Info("backtest config summary",
		zap.Bool("questdb", b.runtimeQuestDB()),
		zap.String("data_dir", dataDir),
		zap.String("timerange", cfg.TimeRangeRaw),
		zap.String("time_start", cfg.TimeStart),
		zap.String("time_end", cfg.TimeEnd),
		zap.Int("pair_count", len(cfg.Pairs)),
		zap.Int("run_policy_count", len(cfg.RunPolicy)),
		zap.Int("pair_filter_count", len(cfg.PairFilters)),
		zap.Int("run_tf_count", len(cfg.RunTimeframes)),
		zap.Float64("stake_amount", cfg.StakeAmount),
		zap.Float64("bt_net_cost", cfg.BTNetCost),
		zap.Int("order_bar_max", cfg.OrderBarMax),
		zap.String("out_dir", b.OutDir))
	if b.OutDir != "" {
		err_ := os.MkdirAll(b.OutDir, 0755)
		if err_ != nil {
			return errs.New(core.ErrIOWriteFail, err_)
		}
	}
	var err *errs.Error
	if deps := b.RuntimeDependencies(); deps != nil {
		startAt, endAt := int64(0), int64(0)
		if deps.Config != nil {
			if cfg := deps.Config.View(); cfg != nil && cfg.TimeRange != nil {
				startAt, endAt = cfg.TimeRange.StartMS, cfg.TimeRange.EndMS
			}
		}
		if deps.Core != nil {
			if startAt == 0 {
				startAt = deps.Core.StartAt
			}
			err = ormo.InitTaskWithState(deps.Orders, deps.DefaultAccount, deps.Core.RunMode, startAt, endAt, !b.isOpt)
		} else {
			return errs.NewMsg(core.ErrRunTime, "runtime core is required for backtest initialization")
		}
	} else {
		err = ormo.InitTask(!b.isOpt, b.OutDir)
	}
	if err != nil {
		return err
	}
	err = b.initTaskOut()
	if err != nil {
		return err
	}
	b.PBar.SetProgress("init", 1)
	if deps := b.RuntimeDependencies(); deps != nil {
		if b.symbols == nil || deps.Exchange == nil {
			return errs.NewMsg(core.ErrRunTime, "runtime symbols and exchange are required for listing dates")
		}
		err = orm.InitListDatesWithExchange(b.symbols, deps.Exchange)
	} else {
		err = orm.InitListDatesWithState(b.symbols)
	}
	if err != nil {
		return err
	}
	b.PBar.SetProgress("listMs", 1)
	// 交易对初始化
	err = refreshPairJobsWithRuntimeDeps(b.dp, b.symbols, b.RuntimeDependencies(), !b.isOpt, true, b.PBar)
	if err != nil {
		return err
	}
	_, err = b.syncThirdPartySeriesRange()
	if err != nil {
		return err
	}
	return nil
}

func (b *BackTest) syncThirdPartySeriesRange() (*data.SeriesPlan, *errs.Error) {
	jobs, collectErr := collectBacktestJobsForBacktest(b)
	if collectErr != nil {
		return nil, collectErr
	}
	seriesRuntime := b.thirdPartyRuntime()
	var plan *data.SeriesPlan
	var err error
	var runtimeDeps *biz.RuntimeDeps
	if b.BackTestLite != nil {
		runtimeDeps = b.BackTestLite.RuntimeDependencies()
	}
	if runtimeDeps == nil || b.legacyClockSync &&
		(seriesRuntime.Catalog == nil || seriesRuntime.Catalog == data.LegacyDataSourceCatalog()) {
		plan, err = backtestBootstrapPlan(jobs, b.runTimeRange())
	} else {
		plan, err = backtestBootstrapPlanWithCatalog(seriesRuntime.Catalog, jobs, b.runTimeRange())
	}
	if err != nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "%v", err)
	}
	if err := seriesRuntime.Ensure(b.backtestSeriesContext(), plan); err != nil {
		return plan, errs.New(core.ErrRunTime, err)
	}
	if b.BackTestLite != nil && b.dp != nil {
		if err := b.dp.SetSeriesSubs(plan.Subs); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func (b *BackTest) ensureThirdPartySeriesRange() (*data.SeriesPlan, *errs.Error) {
	return b.syncThirdPartySeriesRange()
}

func (b *BackTest) thirdPartyRuntime() *data.SeriesRuntime {
	if b.seriesRuntime == nil {
		var catalog *data.DataSourceCatalog
		if b.dp != nil {
			catalog = b.dp.DataSourceCatalog()
		}
		b.seriesRuntime = data.NewSeriesRuntimeWithCatalog(catalog, nil)
		if deps := b.RuntimeDependencies(); deps != nil {
			b.seriesRuntime.Repo = orm.NewSeriesRepo(deps.Storage)
		}
	}
	return b.seriesRuntime
}

func (b *BackTest) runtimeQuestDB() bool {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			return deps.Storage != nil && deps.Storage.IsQuestDB()
		}
	}
	return orm.IsQuestDB
}

func (b *BackTest) backtestSeriesContext() context.Context {
	if b != nil && b.BackTestLite != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			if deps.Core != nil && deps.Core.Context() != nil {
				return deps.Core.Context()
			}
			return context.Background()
		}
	}
	if core.Ctx != nil {
		return core.Ctx
	}
	return context.Background()
}

func collectBacktestJobs() []*strat.StratJob {
	return collectBacktestJobsFromMap(strat.GetJobs(config.DefAcc))
}

func collectBacktestJobsForBacktest(b *BackTest) ([]*strat.StratJob, *errs.Error) {
	if b != nil && b.BackTestLite != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			if deps.Strategies == nil || strat.IsLegacyState(deps.Strategies) {
				return nil, errs.NewMsg(core.ErrRunTime, "runtime strategy state is required for backtest series")
			}
			return collectBacktestJobsFromMap(deps.Strategies.JobMaps(deps.DefaultAccount)), nil
		}
	}
	return collectBacktestJobs(), nil
}

func collectBacktestJobsFromMap(jobsByEnv map[string]map[string]*strat.StratJob) []*strat.StratJob {
	seen := make(map[*strat.StratJob]bool)
	var jobs []*strat.StratJob
	envKeys := make([]string, 0, len(jobsByEnv))
	for envKey := range jobsByEnv {
		envKeys = append(envKeys, envKey)
	}
	sort.Strings(envKeys)
	for _, envKey := range envKeys {
		stgMap := jobsByEnv[envKey]
		stgNames := make([]string, 0, len(stgMap))
		for stgName := range stgMap {
			stgNames = append(stgNames, stgName)
		}
		sort.Strings(stgNames)
		for _, stgName := range stgNames {
			job := stgMap[stgName]
			if job == nil || seen[job] {
				continue
			}
			seen[job] = true
			jobs = append(jobs, job)
		}
	}
	return jobs
}

func backtestBootstrapPlan(jobs []*strat.StratJob, tr *config.TimeTuple) (*data.SeriesPlan, error) {
	if tr == nil {
		return nil, fmt.Errorf("bootstrap collect phase=collect: time range is required")
	}
	if tr.StartMS >= tr.EndMS {
		return nil, fmt.Errorf("bootstrap collect phase=collect: invalid time range")
	}
	return data.NewSeriesPlan(jobs, tr.StartMS, tr.EndMS)
}

func backtestBootstrapPlanWithCatalog(catalog *data.DataSourceCatalog, jobs []*strat.StratJob, tr *config.TimeTuple) (*data.SeriesPlan, error) {
	if tr == nil {
		return nil, fmt.Errorf("bootstrap collect phase=collect: time range is required")
	}
	if tr.StartMS >= tr.EndMS {
		return nil, fmt.Errorf("bootstrap collect phase=collect: invalid time range")
	}
	if catalog == nil || catalog == data.LegacyDataSourceCatalog() {
		return nil, fmt.Errorf("bootstrap collect phase=collect: explicit data source catalog is required")
	}
	return data.NewSeriesPlanWithCatalog(catalog, jobs, tr.StartMS, tr.EndMS)
}

func (b *BackTest) FeedDataSeries(evt *orm.DataSeries) {
	if orm.NormalizeSeriesSource(evt.Source) != orm.SeriesSourceKline || !evt.HasOHLCV() {
		_ = b.BackTestLite.FeedDataSeries(evt)
		return
	}
	if b.shouldCloseHistoricalBoundary(evt.TimeMS) {
		if err := b.closeHistoricalBoundaries(evt.TimeMS); err != nil {
			b.setRunError(err)
			return
		}
	}
	curTime := b.TimeMS()
	ok := b.BackTestLite.FeedDataSeries(evt)
	if !evt.IsWarmUp && b.checkWallets() {
		b.setCheckWallets(false)
		account := b.defaultAccount()
		odNum := 0
		if deps := b.RuntimeDependencies(); deps != nil && deps.Orders != nil {
			odNum = deps.Orders.OpenNum(account, ormo.InOutStatusPartEnter)
		} else {
			odNum = ormo.OpenNum(account, ormo.InOutStatusPartEnter)
		}
		b.logState(evt.TimeMS, curTime, odNum)
	}
	if ok && b.nextRefresh > 0 && evt.TimeMS >= b.nextRefresh {
		// 刷新交易对
		refreshMs := evt.TimeMS // 这里bar.Time 可能远大于b.nextRefresh，所以应当用bar.Time
		b.nextRefresh = b.schedule.Next(time.UnixMilli(evt.TimeMS)).UnixMilli()
		b.SetTimeMS(refreshMs)
		err := refreshPairJobsWithRuntimeDeps(b.dp, b.symbols, b.RuntimeDependencies(), !b.isOpt, false, nil)
		b.SetTimeMS(curTime)
		dateStr := btime.ToDateStr(refreshMs, "")
		if err != nil {
			log.Error("RefreshPairJobs", zap.String("date", dateStr), zap.Error(err))
			if b.dataPrep {
				b.dataPrepErr = err
			}
			b.setRunError(err)
			return
		} else {
			if _, err := b.syncThirdPartySeriesRange(); err != nil {
				log.Error("ensure third-party series after pair refresh", zap.String("date", dateStr), zap.Error(err))
				if b.dataPrep {
					b.dataPrepErr = err
				}
				b.setRunError(err)
				return
			}
			log.Info("refreshed pairs at", zap.String("date", dateStr))
		}
		b.dp.SetDirty()
	}
}

func historicalCloseBoundaries(coverage *config.HistoricalCoverageConfig, runRange *config.TimeTuple) []int64 {
	if coverage == nil || runRange == nil {
		return nil
	}
	result := make([]int64, 0, 2)
	if endMS := coverage.BaselineEndMS; endMS > runRange.StartMS && runRange.EndMS > endMS &&
		(len(result) == 0 || result[len(result)-1] != endMS) {
		result = append(result, endMS)
	}
	if endMS := coverage.HistoricalResultEndMS; endMS > runRange.StartMS && runRange.EndMS > endMS &&
		(len(result) == 0 || result[len(result)-1] != endMS) {
		result = append(result, endMS)
	}
	return result
}

func (b *BackTest) shouldCloseHistoricalBoundary(eventMS int64) bool {
	return b.historicalCloseIndex < len(b.historicalCloseMS) &&
		eventMS >= b.historicalCloseMS[b.historicalCloseIndex]
}

func (b *BackTest) closeHistoricalBoundaries(eventMS int64) *errs.Error {
	for b.shouldCloseHistoricalBoundary(eventMS) {
		account := b.defaultAccount()
		if deps := b.RuntimeDependencies(); deps != nil {
			if err := biz.CloseBacktestOrdersAtWithState(deps.Trading, account, b.historicalCloseMS[b.historicalCloseIndex]); err != nil {
				return err
			}
		} else if err := biz.CloseBacktestOrdersAt(account, b.historicalCloseMS[b.historicalCloseIndex]); err != nil {
			return err
		}
		b.historicalCloseIndex++
	}
	return nil
}

func (b *BackTest) Run() *errs.Error {
	completed := false
	defer func() {
		if !completed && b != nil && b.outputOwned && b.OutDir != "" {
			_ = os.RemoveAll(b.OutDir)
		}
	}()
	err := b.initRefreshCron()
	if err != nil {
		log.Error("init pair cron fail", zap.Error(err))
		return err
	}
	err = b.Init()
	if err != nil {
		log.Error("backtest init fail", zap.Error(err))
		return err
	}
	if !b.isOpt {
		b.cronDumpBtStatus()
		b.schedulerForRun().Start()
	}
	btStart := btime.UTCTime()
	loopMainFn := b.loopMainFn
	if loopMainFn == nil {
		loopMainFn = b.dp.LoopMain
	}
	err = b.resolveLoopError(loopMainFn())
	if !b.isOpt {
		b.schedulerForRun().Stop()
	}
	if err != nil {
		log.Error("backtest loop fail", zap.Error(err))
		return err
	}
	// Some feeders finish without emitting a bar at or after a historical
	// cutoff (for example when every series ends at the old baseline). Ensure
	// those positions are closed before the final cleanup uses the new end.
	if err := b.closeHistoricalBoundaries(math.MaxInt64); err != nil {
		log.Error("close historical boundaries fail", zap.Error(err))
		return err
	}
	btCost := btime.UTCTime() - btStart
	account := b.defaultAccount()
	var wallets *biz.BanWallets
	var odMgr biz.IOrderMgr
	if deps := b.RuntimeDependencies(); deps != nil {
		if deps.DefaultAccount != "" {
			account = deps.DefaultAccount
		}
		odMgr = biz.GetOdMgrWithState(deps.Trading, account)
		if deps.Trading != nil {
			wallets = deps.Trading.Wallet(account)
		}
	} else {
		odMgr = biz.GetOdMgr(account)
		wallets = biz.GetWallets(account)
	}
	if odMgr == nil {
		return errs.NewMsg(core.ErrRunTime, "backtest order manager is not initialized")
	}
	err = odMgr.CleanUp()
	if deps := b.RuntimeDependencies(); deps != nil && deps.Strategies != nil {
		strat.ExitStratJobsWithState(deps.Strategies)
	} else {
		strat.ExitStratJobs()
	}
	if err != nil {
		log.Error("backtest clean orders fail", zap.Error(err))
		return err
	}
	if b.dataPrep {
		completed = true
		return nil
	}
	if wallets == nil {
		wallets = biz.GetWallets(account)
	}
	b.logPlot(wallets, b.TimeMS(), -1, -1)
	b.normalizeBacktestResultRange()
	b.Collect()
	b.runAfterBacktestCallback()
	if !b.isOpt {
		log.Info(fmt.Sprintf("Complete! cost: %.1fs, avg: %.1f bar/s", btCost, float64(b.BarNum)/btCost))
		var failOpens string
		if deps := b.RuntimeDependencies(); deps != nil && deps.Strategies != nil {
			failOpens = strat.DumpAccFailOpensWithState(deps.Strategies)
		} else {
			failOpens = strat.DumpAccFailOpens()
		}
		if failOpens != "" {
			log.Info("fail open tag nums:\n" + failOpens)
		}
		b.printBtResult(true)
	}
	completed = true
	return nil
}

// normalizeBacktestResultRange keeps the reported window tied to the
// immutable backtest request. A run with no non-warmup events can otherwise
// report its first appended-tail event as the start of the whole backtest.
func normalizeBacktestResultRange(result *BTResult, stoppedEarly bool) {
	normalizeBacktestResultRangeForTimeRange(result, stoppedEarly, config.TimeRange)
}

func (b *BackTest) normalizeBacktestResultRange() {
	if b == nil {
		return
	}
	normalizeBacktestResultRangeForTimeRange(b.BTResult, b.stoppedEarly, b.runTimeRange())
}

func normalizeBacktestResultRangeForTimeRange(result *BTResult, stoppedEarly bool, runRange *config.TimeTuple) {
	if stoppedEarly {
		return
	}
	if result == nil || runRange == nil || runRange.StartMS <= 0 ||
		runRange.EndMS <= runRange.StartMS {
		return
	}
	result.StartMS = runRange.StartMS
	result.EndMS = runRange.EndMS
}

func (b *BackTest) resolveLoopError(err *errs.Error) *errs.Error {
	if err != nil {
		return err
	}
	if b.dataPrepErr != nil {
		return b.dataPrepErr
	}
	if b.BackTestLite != nil {
		return b.BackTestLite.resolveLoopError(nil)
	}
	return nil
}

func (b *BackTest) initTaskOut() *errs.Error {
	if b.OutDir != "" {
		if b.RuntimeDependencies() == nil {
			logFile := b.OutDir + "/out.log"
			config.Args.Logfile = logFile
			if utils.Exists(logFile) {
				err_ := os.Remove(logFile)
				if err_ != nil {
					log.Warn("delete old log fail", zap.Error(err_))
				}
			}
			config.Args.SetLog(!b.isOpt)
		}
	}
	account := b.defaultAccount()
	_, ok := b.accountConfigs()[account]
	if !ok {
		if b.RuntimeDependencies() != nil {
			return errs.NewMsg(core.ErrBadConfig, "default account %q is invalid", account)
		}
		panic("default Account invalid!")
	}
	cfg := b.runConfig()
	if cfg != nil && cfg.StakePct > 0 {
		log.Warn("stake_amt may result in inconsistent order amounts with each backtest!")
	}
	return nil
}

func (b *BackTest) cronDumpBtStatus() {
	if b.strictBacktest() {
		return
	}
	b.lastDumpMs = btime.UTCStamp()
	_, err_ := b.schedulerForRun().AddFunc("30 * * * * *", func() {
		curTime := btime.UTCStamp()
		if curTime-b.lastDumpMs < 300000 {
			// 5分钟保存一次回测状态
			return
		}
		b.lastDumpMs = curTime
		log.Info("dump backTest status to files...")
		b.Collect()
		b.printBtResult(false)
	})
	if err_ != nil {
		log.Error("add Dump BackTest Status fail", zap.Error(err_))
	}
}

func (b *BackTest) schedulerForRun() com.Scheduler {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil && deps.Scheduler != nil {
			return deps.Scheduler
		}
		if deps := b.RuntimeDependencies(); deps != nil {
			location := time.UTC
			lang := ""
			if deps.Config != nil {
				location = deps.Config.Location()
				if cfg := deps.Config.View(); cfg != nil {
					lang = cfg.NTPLangCode
				}
			}
			return com.NewSchedulerWithConfig(location, lang)
		}
	}
	return com.Cron()
}

func (b *BackTest) initRefreshCron() *errs.Error {
	cfg := b.runConfig()
	runRange := b.runTimeRange()
	if cfg == nil || cfg.PairMgr == nil || cfg.PairMgr.Cron == "" {
		return nil
	}
	if runRange == nil {
		return errs.NewMsg(core.ErrBadConfig, "backtest time range is required for pair refresh")
	}
	if cfg.PairMgr.Cron != "" {
		var err_ error
		b.schedule, err_ = runtimeCronSchedule(cfg.PairMgr.Cron, b.RuntimeDependencies())
		if err_ != nil {
			return errs.New(core.ErrBadConfig, err_)
		}
		baseMS := runRange.StartMS
		for {
			baseTime := time.UnixMilli(baseMS)
			b.nextRefresh = b.schedule.Next(baseTime).UnixMilli()
			gap := runtimeMinPairCronGapMS
			if b.RuntimeDependencies() == nil {
				gap = int64(config.MinPairCronGapMS)
			}
			if b.nextRefresh-baseMS > gap {
				break
			}
			baseMS = b.nextRefresh
		}
	}
	return nil
}

func RefreshPairJobs(dp data.IProvider, showLog, isFirst bool, pBar *utils.StagedPrg) *errs.Error {
	return RefreshPairJobsWithSymbolState(dp, nil, showLog, isFirst, pBar)
}

// RefreshPairJobsWithSymbolState keeps the pair refresh's symbol subscription
// set aligned with the provider's runtime-owned symbol state.
func RefreshPairJobsWithSymbolState(dp data.IProvider, symbols *orm.SymbolState, showLog, isFirst bool, pBar *utils.StagedPrg) *errs.Error {
	return refreshPairJobsWithRuntimeDeps(dp, symbols, nil, showLog, isFirst, pBar)
}

// refreshPairJobsWithRuntimeDeps uses typed clock/core state when supplied.
// Job registries, order managers, and relay snapshots remain legacy globals.
func refreshPairJobsWithRuntimeDeps(dp data.IProvider, symbols *orm.SymbolState, deps *biz.RuntimeDeps, showLog, isFirst bool, pBar *utils.StagedPrg) *errs.Error {
	if deps != nil {
		if symbols == nil {
			symbols = deps.Symbols
		}
		if deps.Symbols == nil {
			deps.Symbols = symbols
		}
		if err := validateBacktestRuntimeDeps(deps, symbols); err != nil {
			return err
		}
	}
	if dp == nil {
		return errs.NewMsg(core.ErrRunTime, "backtest data provider is required for pair refresh")
	}
	cfg := runtimeConfigForDeps(deps)
	runRange := runtimeTimeRangeForDeps(deps)
	curTime := int64(0)
	envReal := false
	if deps != nil {
		curTime = deps.Clock.TimeMS()
		envReal = deps.Core.EnvReal
	} else {
		curTime = btime.TimeMS()
		envReal = core.EnvReal
	}
	if isFirst {
		if cfg != nil && cfg.PairMgr != nil && cfg.PairMgr.Cron != "" {
			schedule, err_ := runtimeCronSchedule(cfg.PairMgr.Cron, deps)
			if err_ != nil {
				return errs.New(errs.CodeRunTime, err_)
			}
			curTime = utils.CronAlign(schedule, btime.ToTime(curTime)).UnixMilli()
		} else if !envReal && cfg != nil && cfg.PairMgr != nil && cfg.PairMgr.UseLatest && runRange != nil {
			// 回测时配置use_latest=true，且cron为空，则使用最新时间刷新交易品种
			curTime = min(curTime, runRange.EndMS)
		}
	}
	var pairs []string
	var pairTfScores map[string]map[string]float64
	var err *errs.Error
	if deps != nil {
		pairs, pairTfScores, err = biz.RefreshPairsWithRuntimeDeps(deps, showLog, curTime, pBar)
	} else {
		pairs, pairTfScores, err = biz.RefreshPairsWithSymbolState(symbols, showLog, curTime, pBar)
	}
	if err != nil {
		return err
	}
	if deps != nil {
		syncRuntimePairsWithConfig(deps.Core, pairs, cfg)
	}
	// store the currently running jobs and mark them as prohibited from running
	// 获取旧的已运行一段时间的任务（在刷新任务前运行），标记为禁止运行
	var forbidJobs map[string]map[string]bool
	if deps != nil {
		if deps.Strategies != nil {
			forbidJobs = deps.Strategies.JobKeysAll()
		}
	} else {
		forbidJobs = strat.GetJobKeys()
	}
	// 刷新交易任务
	warms, err := biz.RefreshJobsWithRuntimeDeps(deps, symbols, pairs, pairTfScores, showLog, pBar)
	if err != nil {
		return err
	}
	if isFirst {
		// 监听订单状态变化，触发策略的OnOrderChange
		biz.InitOdSubsWithRuntimeDeps(deps)
	}
	// relay the simulate open position orders for new symbols at this time
	// 接力入场新品种的截止此时模拟持仓订单
	err = relayUnFinishOrdersWithDeps(pairTfScores, forbidJobs, isFirst, symbols, deps)
	if err != nil {
		return err
	}
	if symbols == nil {
		orm.ResetSubSymbol()
	} else {
		symbols.ResetSubSymbol()
	}
	// warm up for new symbols
	return dp.SubWarmPairs(warms, true)
}

func validateBacktestRuntimeDeps(deps *biz.RuntimeDeps, symbols *orm.SymbolState) *errs.Error {
	if deps == nil {
		return nil
	}
	missing := make([]string, 0, 8)
	if deps.Clock == nil {
		missing = append(missing, "clock")
	}
	if deps.Core == nil {
		missing = append(missing, "core")
	}
	if deps.Strategies == nil || strat.IsLegacyState(deps.Strategies) {
		missing = append(missing, "strategy state")
	}
	if deps.Orders == nil {
		missing = append(missing, "order state")
	}
	if deps.Trading == nil {
		missing = append(missing, "trading state")
	}
	if deps.Batch == nil {
		missing = append(missing, "batch state")
	}
	if deps.Market == nil || deps.Market.Prices == nil {
		missing = append(missing, "market state")
	}
	if symbols == nil {
		missing = append(missing, "symbol state")
	}
	if deps.Exchange == nil {
		missing = append(missing, "exchange")
	}
	if deps.Config == nil || deps.Config.View() == nil {
		missing = append(missing, "config")
	}
	if len(missing) > 0 {
		return errs.NewMsg(core.ErrRunTime, "explicit backtest runtime requires %s", strings.Join(missing, ", "))
	}
	return nil
}

func syncRuntimePairs(state *core.State, pairs []string) {
	legacyConfig := config.Data
	legacyConfig.RunPolicy = config.RunPolicy
	syncRuntimePairsWithConfig(state, pairs, &legacyConfig)
}

func syncRuntimePairsWithConfig(state *core.State, pairs []string, cfg *config.Config) {
	if state == nil {
		return
	}
	additionalAllowed := make([]string, 0)
	if cfg == nil {
		state.SetPairs(pairs, additionalAllowed)
		return
	}
	for _, policy := range cfg.RunPolicy {
		additionalAllowed = append(additionalAllowed, policy.Pairs...)
	}
	state.SetPairs(pairs, additionalAllowed)
}

// syncRuntimeOrderMatchState is the temporary boundary between the legacy
// strategy registry and the typed Trader. Strategy loading still builds its
// timeframe map in core's compatibility facade; copy the completed snapshot
// once per refresh so the hot path never consults that facade.
func syncRuntimeOrderMatchState(state *core.State) {
	if state == nil {
		return
	}
	core.LockOdMatch.RLock()
	state.LockOdMatch.Lock()
	state.OrderMatchTfs = make(map[string]bool, len(core.OrderMatchTfs))
	for tf, enabled := range core.OrderMatchTfs {
		state.OrderMatchTfs[tf] = enabled
	}
	state.LockOdMatch.Unlock()
	core.LockOdMatch.RUnlock()
}

/*
获取模拟回测的未完成订单，接力入场；
应在RefreshJobs之后再调用，否则入场订单可能被视为旧的平仓掉
*/
func withRelayBacktestState(run func() *errs.Error) *errs.Error {
	backUp := biz.BackupVars()
	timeRange := config.TimeRange.Clone()
	backTime := btime.CurTimeMS
	backPols := config.RunPolicy
	backRunMode := core.RunMode
	backRunEnv := core.RunEnv
	bakAccs := config.MergeAccounts()
	defer func() {
		config.RunPolicy = backPols
		config.ClearRefineMap()
		config.TimeRange = timeRange
		btime.CurTimeMS = backTime
		biz.RestoreVars(backUp)
		core.SetRunMode(backRunMode)
		core.SetRunEnv(backRunEnv)
		config.Accounts = bakAccs
	}()
	core.SetRunMode(core.RunModeBackTest)
	core.SetRunEnv(backRunEnv)
	return run()
}

func relayUnFinishOrders(pairTfScores map[string]map[string]float64, forbidJobs map[string]map[string]bool, isFirst bool, symbols *orm.SymbolState) *errs.Error {
	return relayUnFinishOrdersLegacy(pairTfScores, forbidJobs, isFirst, symbols)
}

func relayUnFinishOrdersWithDeps(pairTfScores map[string]map[string]float64, forbidJobs map[string]map[string]bool,
	isFirst bool, symbols *orm.SymbolState, deps *biz.RuntimeDeps) *errs.Error {
	if deps == nil {
		return relayUnFinishOrdersLegacy(pairTfScores, forbidJobs, isFirst, symbols)
	}
	cfg := deps.ConfigView()
	if cfg == nil || !cfg.RelaySimUnFinish {
		return nil
	}
	if deps.Core == nil || deps.Clock == nil || deps.Strategies == nil || deps.Orders == nil || deps.Trading == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime relay requires core, clock, strategy, order, and trading state")
	}
	simEndMS := deps.Clock.TimeMS()
	relayOpens := make(map[string]*ormo.InOutOrder)
	relayDones := make(map[string]*ormo.InOutOrder)
	for _, group := range strat.RelayPolicyGroupsWithState(deps.Strategies, symbols) {
		if group == nil || len(group.Policies) == 0 || group.StartMS >= simEndMS {
			continue
		}
		tempDeps, lite, cleanup, err := newRelayRuntime(*deps, symbols, group, simEndMS, forbidJobs, pairTfScores)
		if err != nil {
			return err
		}
		warms, _, loadErr := strat.LoadStratJobsWithState(tempDeps.Strategies, tempDeps.Core, symbols,
			tempDeps.Core.Pairs, pairTfScores, tempDeps.Orders)
		if loadErr != nil {
			cleanup()
			return loadErr
		}
		if len(warms) > 0 {
			if warmErr := lite.dp.SubWarmPairs(warms, true); warmErr != nil {
				cleanup()
				return warmErr
			}
			if runErr := lite.resolveLoopError(lite.dp.LoopMain()); runErr != nil {
				cleanup()
				return runErr
			}
		}
		collectRelayOrders(tempDeps.Orders, tempDeps.DefaultAccount, relayOpens, relayDones)
		cleanup()
	}
	return syncSimOrdersWithDeps(deps, isFirst, relayOpens, relayDones)
}

func newRelayRuntime(parent biz.RuntimeDeps, symbols *orm.SymbolState, group *strat.PolicyGroup,
	simEndMS int64, forbidJobs map[string]map[string]bool, pairTfScores map[string]map[string]float64,
) (biz.RuntimeDeps, *BackTestLite, func(), *errs.Error) {
	state, err := core.NewState(parent.Core.Context())
	if err != nil {
		return biz.RuntimeDeps{}, nil, func() {}, errs.New(core.ErrRunTime, err)
	}
	state.SetRunMode(core.RunModeBackTest)
	state.SetRunEnv(parent.Core.RunEnv)
	state.StartAt = group.StartMS
	state.ExgName, state.Market, state.ContractType = parent.Core.ExgName, parent.Core.Market, parent.Core.ContractType
	state.IsContract = parent.Core.IsContract
	state.NetDisable = parent.Core.NetDisable
	state.SimOrderMatch = parent.Core.SimOrderMatch
	state.ParallelOnBar = parent.Core.ParallelOnBar
	state.NumTaCache, state.ConcurNum = parent.Core.NumTaCache, parent.Core.ConcurNum
	state.SetPairs(parent.Core.Pairs, nil)
	clock := btime.NewClockState(true, parent.Config.Location())
	clock.SetTimeMS(group.StartMS)
	market := com.NewMarketStateWithExchange(state.ExgName, parent.Exchange)
	configSnapshot := parent.Config.Clone()
	runConfig := configSnapshot.View()
	runConfig.TimeRange = &config.TimeTuple{StartMS: group.StartMS, EndMS: simEndMS}
	runConfig.RunPolicy = make([]*config.RunPolicyConfig, len(group.Policies))
	for i, policy := range group.Policies {
		if policy != nil {
			runConfig.RunPolicy[i] = policy.Clone()
		}
	}
	strategyState := strat.NewStateWithRuntime(state, clock, runConfig, symbols, parent.Exchange)
	orderState := ormo.NewOrderState()
	tradingState := biz.NewTradingState()
	accounts := parent.AccountConfigs()
	defaultAccount := parent.DefaultAccount
	if defaultAccount == "" {
		defaultAccount = configSnapshot.DefaultAccount()
	}
	tempDeps := biz.RuntimeDeps{
		Core: state, Clock: clock, Market: market, Batch: strat.NewBatchState(),
		Strategies: strategyState, Orders: orderState, Trading: tradingState,
		Config: configSnapshot, Accounts: accounts, DefaultAccount: defaultAccount,
		Symbols: symbols, Storage: parent.Storage, Exchange: parent.Exchange,
		Scheduler: parent.Scheduler,
	}
	trader := biz.NewTraderWithRuntimeDeps(tempDeps)
	tempDeps = *trader.RuntimeDependencies()
	if err := ormo.InitTasksWithState(tempDeps.Orders, runtimeAccountNames(tempDeps.AccountConfigs()),
		core.RunModeBackTest, group.StartMS, simEndMS, false); err != nil {
		state.Close()
		return biz.RuntimeDeps{}, nil, func() {}, err
	}
	strategyState.ForbidJobs = forbidJobs
	dataDeps := &data.RuntimeDeps{
		Core: state, Clock: clock, Config: configSnapshot, Market: market, Symbols: symbols,
		Storage: parent.Storage, Strategies: strategyState, Exchange: parent.Exchange,
		ExchangeName: state.ExgName, MarketType: state.Market,
	}
	lite := newBackTestLite(trader, symbols, true, nil, nil, nil, dataDeps)
	cleanup := func() {
		if lite != nil && lite.dp != nil {
			lite.dp.Terminate()
		}
		state.Close()
	}
	return tempDeps, lite, cleanup, nil
}

func runtimeAccountNames(accounts map[string]*config.AccountConfig) []string {
	result := make([]string, 0, len(accounts))
	for account := range accounts {
		result = append(result, account)
	}
	sort.Strings(result)
	return result
}

func collectRelayOrders(state *ormo.OrderState, account string, opens, dones map[string]*ormo.InOutOrder) {
	if state == nil {
		return
	}
	openOrders, lock := state.GetOpenODs(account)
	lock.Lock()
	for _, order := range openOrders {
		if order == nil {
			continue
		}
		if order.Status >= ormo.InOutStatusPartEnter && order.ExitTag == "" {
			opens[order.KeyAlign()] = order
		} else if order.Status >= ormo.InOutStatusFullExit {
			dones[order.KeyAlign()] = order
		}
	}
	lock.Unlock()
	for _, order := range state.HistoricalOrders() {
		if order != nil {
			dones[order.KeyAlign()] = order
		}
	}
}

func syncSimOrdersWithDeps(deps *biz.RuntimeDeps, isFirst bool, relayOpens, relayDones map[string]*ormo.InOutOrder) *errs.Error {
	if deps == nil || deps.Orders == nil || deps.Trading == nil || deps.Strategies == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime relay state is incomplete")
	}
	accounts := deps.AccountConfigs()
	strict := deps.StrictBacktest()
	if isFirst {
		closeNums := make(map[string]int)
		for _, account := range runtimeAccountNames(accounts) {
			cfg := accounts[account]
			if cfg == nil || cfg.NoTrade {
				continue
			}
			manager := biz.GetOdMgrWithState(deps.Trading, account)
			if manager == nil {
				continue
			}
			openOrders, lock := deps.Orders.GetOpenODs(account)
			var exits []*ormo.InOutOrder
			lock.Lock()
			for _, order := range openOrders {
				if _, ok := relayDones[order.KeyAlign()]; ok {
					exits = append(exits, order)
				}
			}
			lock.Unlock()
			if len(exits) == 0 {
				continue
			}
			if err := manager.ExitAndFill(exits, &strat.ExitReq{Tag: core.ExitTagExitDelay}); err != nil {
				return err
			}
			closeNums[account] = len(exits)
		}
		if len(closeNums) > 0 {
			log.Info("closed delayed order", zap.Any("nums", closeNums))
		}
	}
	if len(relayOpens) == 0 {
		return nil
	}
	for _, account := range runtimeAccountNames(accounts) {
		cfg := accounts[account]
		if cfg == nil || cfg.NoTrade {
			continue
		}
		manager := biz.GetOdMgrWithState(deps.Trading, account)
		if manager == nil {
			continue
		}
		jobs := deps.Strategies.JobMaps(account)
		openOrders, lock := deps.Orders.GetOpenODs(account)
		current := make(map[string]*ormo.InOutOrder, len(openOrders))
		lock.Lock()
		for _, order := range openOrders {
			current[order.KeyAlign()] = order
		}
		lock.Unlock()
		allowed := make([]*ormo.InOutOrder, 0, len(relayOpens))
		for _, key := range sortedMapKeys(relayOpens, strict) {
			order := relayOpens[key]
			if _, exists := current[key]; exists {
				continue
			}
			if strategyJobs := jobs[fmt.Sprintf("%s_%s", order.Symbol, order.Timeframe)]; strategyJobs != nil {
				if job := strategyJobs[order.Strategy]; job != nil {
					job.AddOrderCount(1)
					allowed = append(allowed, order)
				}
			}
		}
		if len(allowed) > 0 {
			if err := manager.RelayOrders(allowed); err != nil {
				return err
			}
		}
	}
	return nil
}

func sortedMapKeys(items map[string]*ormo.InOutOrder, strict bool) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	if strict {
		sort.Strings(keys)
	}
	return keys
}

func relayUnFinishOrdersLegacy(pairTfScores map[string]map[string]float64, forbidJobs map[string]map[string]bool, isFirst bool, symbols *orm.SymbolState) *errs.Error {
	if !config.RelaySimUnFinish {
		return nil
	}
	simEndMs := btime.CurTimeMS
	if core.LiveMode {
		simEndMs = btime.TimeMS()
	}
	// pair_tf_stratID
	relayOpens := make(map[string]*ormo.InOutOrder)
	relayDones := make(map[string]*ormo.InOutOrder)
	err := withRelayBacktestState(func() *errs.Error {
		// Divide into multiple groups based on the subscription period according to the strategy
		// 按策略订阅周期划分为多个组
		groups := strat.RelayPolicyGroupsWithSymbolState(symbols)
		for _, gp := range groups {
			// Reset global variables, backtest for time range, and search for open orders
			// 重置全局变量，回测过去一段时间，查找未平仓订单
			biz.ResetVars()
			strat.ForbidJobs = forbidJobs
			btime.CurTimeMS = gp.StartMS
			config.TimeRange = &config.TimeTuple{
				StartMS: gp.StartMS,
				EndMS:   simEndMs,
			}
			err := ormo.InitTask(false, "")
			if err != nil {
				return err
			}
			lite := newBackTestLite(biz.NewTrader(strat.NewBatchState()), symbols, true, nil, nil, nil, nil)
			// set policy to run
			// 重新加载策略任务
			err = config.SetRunPolicy(false, gp.Policies...)
			if err != nil {
				return err
			}
			warms, _, err := strat.LoadStratJobsWithSymbolState(symbols, core.Pairs, pairTfScores)
			if err != nil {
				return err
			}
			if len(warms) == 0 {
				// 没有需要预回测的任务
				continue
			}
			err = lite.dp.SubWarmPairs(warms, true)
			if err != nil {
				return err
			}
			err = lite.resolveLoopError(lite.dp.LoopMain())
			if err != nil {
				return err
			}
			// Record the last unfinished orders
			// 记录最后的未完成订单
			odMap, lock := ormo.GetOpenODs(config.DefAcc)
			lock.Lock()
			for _, od := range odMap {
				if od.Status >= ormo.InOutStatusPartEnter && od.ExitTag == "" {
					relayOpens[od.KeyAlign()] = od
				} else if od.Status >= ormo.InOutStatusFullExit {
					relayDones[od.KeyAlign()] = od
				}
			}
			lock.Unlock()
			for _, od := range ormo.HistODs {
				relayDones[od.KeyAlign()] = od
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return syncSimOrders(isFirst, relayOpens, relayDones)
}

func syncSimOrders(isFirst bool, relayOpens, relayDones map[string]*ormo.InOutOrder) *errs.Error {
	if isFirst {
		// 如果是初次执行，检查打开的订单是否已在测试期间平仓，是则自动平仓
		// 主要针对实盘隔一段时间后重启有未平仓订单场景，需检查订单是否应在机器人停止期间平仓
		var err *errs.Error
		closeNums := make(map[string]int)
		for acc := range utils.MapKeys(config.Accounts, config.StrictBacktest()) {
			cfg := config.Accounts[acc]
			if cfg.NoTrade {
				continue
			}
			odMgr := biz.GetOdMgr(acc)
			odMap, lock := ormo.GetOpenODs(acc)
			var exitOds []*ormo.InOutOrder
			lock.Lock()
			for _, od := range odMap {
				if _, ok := relayDones[od.KeyAlign()]; ok {
					exitOds = append(exitOds, od)
				}
			}
			lock.Unlock()
			if len(exitOds) > 0 {
				err = odMgr.ExitAndFill(exitOds, &strat.ExitReq{Tag: core.ExitTagExitDelay})
				if err != nil {
					log.Error("close delayed order fail", zap.Int("num", len(exitOds)), zap.Error(err))
				} else {
					closeNums[acc] = len(exitOds)
				}
			}
		}
		if len(closeNums) > 0 {
			log.Info("closed delayed order", zap.Any("nums", closeNums))
		}
	}
	if len(relayOpens) == 0 {
		return nil
	}
	var err *errs.Error
	for acc := range utils.MapKeys(config.Accounts, config.StrictBacktest()) {
		cfg := config.Accounts[acc]
		if cfg.NoTrade {
			continue
		}
		odMgr := biz.GetOdMgr(acc)
		jobs := strat.GetJobs(acc)
		allowOds := make([]*ormo.InOutOrder, 0, len(relayOpens))
		odMap, lock := ormo.GetOpenODs(acc)
		curKeyMap := make(map[string]*ormo.InOutOrder)
		lock.Lock()
		for _, od := range odMap {
			curKeyMap[od.KeyAlign()] = od
		}
		lock.Unlock()
		for keyAlign := range utils.MapKeys(relayOpens, config.StrictBacktest()) {
			od := relayOpens[keyAlign]
			if _, ok := curKeyMap[keyAlign]; ok {
				// 此订单已存在，跳过
				continue
			}
			stgMap, ok := jobs[fmt.Sprintf("%s_%s", od.Symbol, od.Timeframe)]
			if ok {
				if job, ok := stgMap[od.Strategy]; ok {
					job.AddOrderCount(1)
					allowOds = append(allowOds, od)
					continue
				}
			}
			// 此账户未订阅此策略任务，忽略即可
		}
		if len(allowOds) > 0 {
			err = odMgr.RelayOrders(allowOds)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
