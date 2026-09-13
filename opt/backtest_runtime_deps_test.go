package opt

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
)

func completeBacktestDepsForTest(deps biz.RuntimeDeps) biz.RuntimeDeps {
	if deps.Core == nil {
		deps.Core = &core.State{}
	}
	if deps.Core.ExgName == "" {
		deps.Core.ExgName = "test"
	}
	if deps.Core.Market == "" {
		deps.Core.Market = "spot"
	}
	if deps.Clock == nil {
		deps.Clock = btime.NewClockState(deps.Core.BackTestMode, nil)
	}
	if deps.Market == nil {
		deps.Market = com.NewMarketState(deps.Core.ExgName)
	}
	if deps.Batch == nil {
		deps.Batch = strat.NewBatchState()
	}
	if deps.Strategies == nil {
		deps.Strategies = strat.NewState()
	}
	if deps.Orders == nil {
		deps.Orders = ormo.NewOrderState()
	}
	if deps.Trading == nil {
		deps.Trading = biz.NewTradingState()
	}
	if deps.Config == nil {
		deps.Config = config.NewSnapshot(&config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}})
	}
	if deps.Accounts == nil {
		deps.Accounts = config.CloneAccountConfigsForRuntime(deps.Config.View().Accounts)
	}
	if len(deps.Accounts) == 0 {
		deps.Accounts = map[string]*config.AccountConfig{"default": {}}
	}
	if deps.AccountsMu == nil {
		deps.AccountsMu = &sync.RWMutex{}
	}
	if deps.Symbols == nil {
		deps.Symbols = orm.NewSymbolStateWithIdentity(deps.Core.ExgName, deps.Core.Market)
	}
	if deps.DefaultAccount == "" {
		deps.DefaultAccount = "default"
	}
	if deps.Exchange == nil {
		deps.Exchange = newBacktestRuntimeExchangeStub(deps.Core.ExgName, deps.Core.Market)
	}
	if err := biz.BindRuntimeDeps(deps); err != nil {
		panic(err)
	}
	return deps
}

func newBacktestTraderForTest(t *testing.T, deps biz.RuntimeDeps) biz.Trader {
	t.Helper()
	trader, err := biz.NewTraderWithRuntimeDeps(completeBacktestDepsForTest(deps))
	if err != nil {
		t.Fatal(err)
	}
	return trader
}

type backtestRuntimeExchangeStub struct {
	banexg.BanExchange
	info *banexg.ExgInfo
}

type panicBacktestRuntimeExchange struct{ banexg.BanExchange }

func (*panicBacktestRuntimeExchange) Info() *banexg.ExgInfo {
	panic("metadata unavailable")
}

func (e *backtestRuntimeExchangeStub) Info() *banexg.ExgInfo {
	return e.info
}

func newBacktestRuntimeExchangeStub(id, market string) *backtestRuntimeExchangeStub {
	return &backtestRuntimeExchangeStub{info: &banexg.ExgInfo{ID: id, MarketType: market}}
}

func TestBacktestRuntimeAdapterInfoPanicFailsClosed(t *testing.T) {
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	state.SetRunMode(core.RunModeBackTest)
	state.ExgName = "runtime-exchange"
	state.Market = "spot"
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(100)
	configSnapshot := config.NewSnapshotWithDirs(&config.Config{
		TimeRange: &config.TimeTuple{StartMS: 100, EndMS: 200},
		Accounts:  map[string]*config.AccountConfig{config.DefAcc: {}},
	}, t.TempDir(), "")
	symbols := orm.NewSymbolStateWithIdentity(state.ExgName, state.Market)
	lite := newBackTestLiteForTest(t, biz.RuntimeDeps{
		Core: state, Clock: clock, Config: configSnapshot, Symbols: symbols,
		Exchange: &panicBacktestRuntimeExchange{},
	}, true, nil, nil, nil)
	if lite == nil || lite.runErr == nil {
		t.Fatalf("adapter metadata panic was not captured: %#v", lite)
	}
	if !strings.Contains(lite.runErr.Error(), "adapter Info panicked") {
		t.Fatalf("adapter metadata error = %v, want panic detail", lite.runErr)
	}
	if err := (&BackTest{BackTestLite: lite}).Init(); err == nil {
		t.Fatal("BackTest.Init accepted an invalid adapter identity")
	}
	if lite.dp != nil {
		t.Cleanup(lite.dp.Terminate)
	}
}

func TestBacktestSeriesContextUsesRuntimeCore(t *testing.T) {
	oldContext := core.Ctx
	legacyContext, cancelLegacy := context.WithCancel(context.Background())
	cancelLegacy()
	core.Ctx = legacyContext
	t.Cleanup(func() { core.Ctx = oldContext })

	runtimeContext := context.Background()
	state, err := core.NewState(runtimeContext)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	trader := newBacktestTraderForTest(t, biz.RuntimeDeps{Core: state})
	backtest := &BackTest{BackTestLite: &BackTestLite{Trader: trader}}
	got := backtest.backtestSeriesContext()
	select {
	case <-got.Done():
		t.Fatal("backtest series context inherited canceled legacy context")
	default:
	}
}

func TestSyncRuntimePairsUpdatesAdmissionState(t *testing.T) {
	cfg := &config.Config{RunPolicy: []*config.RunPolicyConfig{{Pairs: []string{"POLICY/USDT"}}}}

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.SetPairs([]string{"OLD/USDT"}, nil)
	state.SetPairBanUntil("OLD/USDT", 1)

	syncRuntimePairsWithConfig(state, []string{"NEW/USDT"}, cfg)

	if !slices.Equal(state.Pairs, []string{"NEW/USDT"}) || !state.PairEnabled("NEW/USDT") ||
		!state.PairEnabled("POLICY/USDT") || state.PairEnabled("OLD/USDT") {
		t.Fatalf("runtime pair state = %#v/%v", state.Pairs, state.AdmissionPairs())
	}
	if slices.Contains(state.BannedPairs(), "OLD/USDT") {
		t.Fatal("removed pair retained its runtime ban state")
	}
}
