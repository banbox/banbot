package opt

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
)

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
	lite := NewBackTestLiteWithRuntimeDataDepsOwned(biz.RuntimeDeps{
		Core: state, Clock: clock, Config: configSnapshot, Symbols: symbols,
		Exchange: &panicBacktestRuntimeExchange{},
	}, symbols, true, nil, nil, nil, nil)
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

func TestLegacyDataRuntimeDepsUsesExplicitRuntimeValues(t *testing.T) {
	oldData, oldDefault := config.Data, exg.Default
	t.Cleanup(func() { config.Data, exg.Default = oldData, oldDefault })
	config.Data = config.Config{TimeRange: &config.TimeTuple{StartMS: 1, EndMS: 2}}
	legacyExchange := newBacktestRuntimeExchangeStub("legacy", "spot")
	runtimeExchange := newBacktestRuntimeExchangeStub("runtime", "spot")
	exg.Default = legacyExchange

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.ExgName = "runtime-exchange"
	state.Market = "spot"
	symbols := orm.NewSymbolStateWithIdentity(state.ExgName, state.Market)
	runtimeConfig := config.NewSnapshotWithDirs(&config.Config{
		TimeRange: &config.TimeTuple{StartMS: 10, EndMS: 20},
	}, t.TempDir(), "")
	deps := &biz.RuntimeDeps{
		Core:     state,
		Config:   runtimeConfig,
		Exchange: runtimeExchange,
		Symbols:  symbols,
	}

	got := legacyDataRuntimeDeps(deps, nil)
	if got.Config != runtimeConfig {
		t.Fatalf("runtime config = %p, want explicit snapshot %p", got.Config, runtimeConfig)
	}
	if got.Exchange != runtimeExchange {
		t.Fatalf("runtime exchange = %p, want explicit exchange %p", got.Exchange, runtimeExchange)
	}
	if got.Symbols != symbols {
		t.Fatalf("runtime symbols = %p, want explicit symbols %p", got.Symbols, symbols)
	}
	if got.ExchangeName != state.ExgName || got.MarketType != state.Market {
		t.Fatalf("runtime identity = %s/%s, want %s/%s", got.ExchangeName, got.MarketType, state.ExgName, state.Market)
	}
}

func TestLegacyDataRuntimeDepsKeepsLegacyGlobalBranch(t *testing.T) {
	oldData, oldDefault := config.Data, exg.Default
	t.Cleanup(func() { config.Data, exg.Default = oldData, oldDefault })
	config.Data = config.Config{TimeRange: &config.TimeTuple{StartMS: 30, EndMS: 40}}
	legacyExchange := newBacktestRuntimeExchangeStub("legacy", "spot")
	exg.Default = legacyExchange

	got := legacyDataRuntimeDeps(nil, nil)
	if got.Config == nil || got.Config.View() == nil || got.Config.View().TimeRange.StartMS != 30 {
		t.Fatalf("legacy config was not snapshotted: %#v", got.Config)
	}
	if got.Exchange != legacyExchange {
		t.Fatalf("legacy exchange = %p, want global exchange %p", got.Exchange, legacyExchange)
	}
}

func TestBackTestRuntimeDataDepsUseConfigSnapshot(t *testing.T) {
	oldTimeRange := config.TimeRange
	config.TimeRange = nil
	t.Cleanup(func() { config.TimeRange = oldTimeRange })

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.SetRunMode(core.RunModeBackTest)
	state.ExgName = "runtime-exchange"
	state.Market = "spot"
	symbols := orm.NewSymbolStateWithIdentity(state.ExgName, state.Market)
	dataDeps := &data.RuntimeDeps{
		Config: config.NewSnapshotWithDirs(&config.Config{
			TimeRange: &config.TimeTuple{StartMS: 100, EndMS: 200},
		}, t.TempDir(), ""),
		Symbols:      symbols,
		ExchangeName: state.ExgName,
		MarketType:   state.Market,
	}
	WithLegacySession(func(session LegacySession) struct{} {
		lite := NewBackTestLiteWithRuntimeDataDeps(
			session, biz.RuntimeDeps{Core: state}, symbols, true, nil, nil, nil, dataDeps)
		t.Cleanup(lite.dp.Terminate)

		err = lite.dp.SetSeriesSubs([]*strat.DataSub{{
			Source:    "opt-runtime-missing-source",
			ExSymbol:  &orm.ExSymbol{ID: 1},
			TimeFrame: "1d",
		}})
		if err == nil || !strings.Contains(err.Error(), "data source") {
			t.Fatalf("provider did not use runtime config snapshot, err=%v", err)
		}
		return struct{}{}
	})
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
	trader := biz.NewTraderWithRuntimeDeps(biz.RuntimeDeps{Core: state})
	backtest := &BackTest{BackTestLite: &BackTestLite{Trader: trader}}
	got := backtest.backtestSeriesContext()
	select {
	case <-got.Done():
		t.Fatal("backtest series context inherited canceled legacy context")
	default:
	}
}

func TestRuntimeBacktestConstructorsRejectExpiredLegacySession(t *testing.T) {
	var expired LegacySession
	WithLegacySession(func(session LegacySession) struct{} {
		expired = session
		return struct{}{}
	})

	assertPanic := func(name string, run func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s accepted an expired legacy session", name)
			}
		}()
		run()
	}
	assertPanic("lite runtime constructor", func() {
		NewBackTestLiteWithRuntimeDeps(expired, biz.RuntimeDeps{}, nil, true, nil, nil, nil)
	})
	assertPanic("lite data runtime constructor", func() {
		NewBackTestLiteWithRuntimeDataDeps(expired, biz.RuntimeDeps{}, nil, true, nil, nil, nil, nil)
	})
	assertPanic("backtest runtime constructor", func() {
		_, _ = NewBackTestWithRuntimeDeps(expired, biz.RuntimeDeps{}, nil, true, "")
	})
}

func TestSyncRuntimePairsUpdatesAdmissionState(t *testing.T) {
	oldPolicies := config.RunPolicy
	config.RunPolicy = []*config.RunPolicyConfig{{Pairs: []string{"POLICY/USDT"}}}
	t.Cleanup(func() { config.RunPolicy = oldPolicies })

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.SetPairs([]string{"OLD/USDT"}, nil)
	state.BanPairsUntil["OLD/USDT"] = 1

	syncRuntimePairs(state, []string{"NEW/USDT"})

	if !slices.Equal(state.Pairs, []string{"NEW/USDT"}) || !state.PairEnabled("NEW/USDT") ||
		!state.PairEnabled("POLICY/USDT") || state.PairEnabled("OLD/USDT") {
		t.Fatalf("runtime pair state = %#v/%v", state.Pairs, state.PairsMap)
	}
	if _, ok := state.BanPairsUntil["OLD/USDT"]; ok {
		t.Fatal("removed pair retained its runtime ban state")
	}
}
