package opt

import (
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
)

func TestBackTestRuntimeErrorIsReturnedAfterLoop(t *testing.T) {
	runErr := errs.NewMsg(core.ErrRunTime, "series too many")
	bt := &BackTest{BackTestLite: &BackTestLite{runErr: runErr}}
	if got := bt.resolveLoopError(nil); got != runErr {
		t.Fatalf("resolveLoopError = %v, want runtime error", got)
	}
}

func TestBackTestLiteRuntimeErrorIsReturnedAfterProviderLoop(t *testing.T) {
	runErr := errs.NewMsg(core.ErrRunTime, "series too many")
	bt := &BackTestLite{runErr: runErr}
	if got := bt.resolveLoopError(nil); got != runErr {
		t.Fatalf("resolveLoopError = %v, want runtime error", got)
	}
}

func TestBackTestPreservesFirstRuntimeError(t *testing.T) {
	first := errs.NewMsg(core.ErrRunTime, "first")
	second := errs.NewMsg(core.ErrRunTime, "second")
	bt := &BackTestLite{}
	bt.setRunError(first)
	bt.setRunError(second)
	if bt.runErr != first {
		t.Fatalf("runErr = %v, want first error", bt.runErr)
	}
}

func TestBackTestAfterCallbackIsRuntimeOwned(t *testing.T) {
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	runtimeBacktest := &BackTest{BackTestLite: &BackTestLite{
		Trader: newBacktestTraderForTest(t, biz.RuntimeDeps{Core: state}),
	}}
	ownedCalls := 0
	runtimeBacktest.SetAfterBacktest(func(*BackTest) { ownedCalls++ })
	runtimeBacktest.runAfterBacktestCallback()
	if ownedCalls != 1 {
		t.Fatalf("runtime callback calls = %d, want 1", ownedCalls)
	}

	(&BackTest{}).runAfterBacktestCallback()
}

func TestNewBackTestLiteOwnsBatchState(t *testing.T) {
	first := newBackTestLiteForTest(t, biz.RuntimeDeps{}, true, nil, nil, nil)
	second := newBackTestLiteForTest(t, biz.RuntimeDeps{}, true, nil, nil, nil)
	first.dp.Terminate()
	second.dp.Terminate()
	firstState := first.Trader.BatchState()
	secondState := second.Trader.BatchState()
	if firstState == nil || secondState == nil || firstState == secondState {
		t.Fatal("backtests must own distinct batch states")
	}
	zero := &BackTestLite{}
	zeroState := zero.batchStateForRun()
	if zeroState == nil || zeroState != zero.Trader.BatchState() {
		t.Fatal("zero-value backtest did not lazily create a private batch state")
	}
}

func TestBackTestLiteCanUseCompositionRootBatchState(t *testing.T) {
	state := strat.NewBatchState()
	bt := newBackTestLiteForTest(t, biz.RuntimeDeps{Batch: state}, true, nil, nil, nil)
	if bt.dp != nil {
		bt.dp.Terminate()
	}
	if bt.Trader.BatchState() != state {
		t.Fatal("backtest replaced owner batch state")
	}
}

func TestHistoricalCloseBoundariesPreserveLegacyAndSequenceInTime(t *testing.T) {
	runRange := &config.TimeTuple{StartMS: 10, EndMS: 200}
	legacy := historicalCloseBoundaries(&config.HistoricalCoverageConfig{BaselineEndMS: 100}, runRange)
	if len(legacy) != 1 || legacy[0] != 100 {
		t.Fatalf("legacy close boundaries = %v, want [100]", legacy)
	}
	boundaries := historicalCloseBoundaries(&config.HistoricalCoverageConfig{
		BaselineEndMS: 100, HistoricalResultEndMS: 160,
	}, runRange)
	if len(boundaries) != 2 || boundaries[0] != 100 || boundaries[1] != 160 {
		t.Fatalf("historical close boundaries = %v, want [100 160]", boundaries)
	}
	equal := historicalCloseBoundaries(&config.HistoricalCoverageConfig{
		BaselineEndMS: 100, HistoricalResultEndMS: 100,
	}, runRange)
	if len(equal) != 1 || equal[0] != 100 {
		t.Fatalf("equal close boundaries = %v, want [100]", equal)
	}
	bt := &BackTest{historicalCloseMS: boundaries}
	if bt.shouldCloseHistoricalBoundary(99) || !bt.shouldCloseHistoricalBoundary(100) {
		t.Fatal("historical baseline boundary predicate is incorrect")
	}
	bt.historicalCloseIndex++
	if bt.shouldCloseHistoricalBoundary(159) || !bt.shouldCloseHistoricalBoundary(160) {
		t.Fatal("historical result boundary predicate is incorrect")
	}
}

func TestBackTestLiteOptimizationLiquidationDoesNotStopLiveRuntime(t *testing.T) {
	originalChargeOnBomb := config.ChargeOnBomb
	originalStopAll := core.StopAll
	t.Cleanup(func() {
		config.ChargeOnBomb = originalChargeOnBomb
		core.StopAll = originalStopAll
	})
	config.ChargeOnBomb = false
	liveStopCalls := 0
	core.StopAll = func() { liveStopCalls++ }
	bt := &BackTestLite{
		isOpt: true,
		dp:    data.NewHistProvider(nil, nil, nil, false, nil),
	}

	bt.onLiquidation("BTC/USDT")

	if liveStopCalls != 0 {
		t.Fatalf("live StopAll calls = %d, want 0", liveStopCalls)
	}
}

func newBackTestLiteForTest(t *testing.T, deps biz.RuntimeDeps, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pbar *utils.StagedPrg) *BackTestLite {
	deps = completeBacktestDepsForTest(deps)
	trader := newBacktestTraderForTest(t, deps)
	return newBackTestLite(trader, deps.Symbols, isOpt, onBar, getEnd, pbar, deps.DataDeps())
}

func TestBackTestLiteConstructorRejectsMissingDependencies(t *testing.T) {
	lite, err := NewBackTestLiteWithRuntimeDeps(biz.RuntimeDeps{}, true, nil, nil, nil)
	if lite != nil || err == nil {
		t.Fatalf("incomplete constructor returned %v, %v", lite, err)
	}
}

func TestBackTestInternalHelpersRejectLegacyTrader(t *testing.T) {
	lite := newBackTestLite(biz.Trader{}, nil, true, nil, nil, nil, nil)
	if lite.runErr == nil {
		t.Fatal("internal lite helper accepted a legacy trader")
	}
	if _, err := newBackTest(biz.Trader{}, nil, true, "", nil); err == nil {
		t.Fatal("internal backtest helper accepted a legacy trader")
	}
}
