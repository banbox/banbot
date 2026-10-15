package opt

import (
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg/errs"
)

func TestDeriveBacktestSnapshotDoesNotMutateSource(t *testing.T) {
	source := config.NewSnapshot(&config.Config{
		TimeRange: &config.TimeTuple{StartMS: 100, EndMS: 200},
		Pairs:     []string{"SOURCE/USDT"},
		RunPolicy: []*config.RunPolicyConfig{{Name: "source"}},
	})
	derived := deriveBacktestSnapshot(source, 300, 400, []string{"RUN/USDT"}, []*config.RunPolicyConfig{{Name: "run"}})
	if derived == source {
		t.Fatal("derived snapshot reused source")
	}
	if got := derived.View(); got.TimeRange.StartMS != 300 || got.TimeRange.EndMS != 400 ||
		len(got.Pairs) != 1 || got.Pairs[0] != "RUN/USDT" || len(got.RunPolicy) != 1 || got.RunPolicy[0].Name != "run" {
		t.Fatalf("derived config = %+v", got)
	}
	if got := source.View(); got.TimeRange.StartMS != 100 || got.TimeRange.EndMS != 200 ||
		len(got.Pairs) != 1 || got.Pairs[0] != "SOURCE/USDT" || len(got.RunPolicy) != 1 || got.RunPolicy[0].Name != "source" {
		t.Fatalf("source config mutated: %+v", got)
	}
}

func TestRollingResultHandoffRebindsCurrentOrderStateAfterCleanup(t *testing.T) {
	oldTrader := newBacktestTraderForTest(t, biz.RuntimeDeps{Orders: ormo.NewOrderState()})
	oldBT := &BackTest{BackTestLite: &BackTestLite{Trader: oldTrader, BTResult: NewBTResult()}}
	oldDeps := oldBT.RuntimeDependencies()
	oldBT.BTResult.runtimeDeps = oldDeps
	oldBT.BTResult.reportDeps = reportDepsFromRuntime(oldDeps)
	oldOrder := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{ID: 1, Symbol: "BTC/USDT", Timeframe: "1h", QuoteCost: 100, Leverage: 1, Profit: 5,
			EnterAt: 1_700_000_000_000, ExitAt: 1_700_003_600_000},
		Enter: &ormo.ExOrder{Filled: 1},
	}
	oldDeps.Orders.AddHistoricalOrder(oldOrder)

	previous, carried := detachRollingResult(oldBT)
	oldDeps.Orders.Reset() // Mirrors factory cleanup for the completed window.

	currentTrader := newBacktestTraderForTest(t, biz.RuntimeDeps{Orders: ormo.NewOrderState()})
	currentBT := &BackTest{BackTestLite: &BackTestLite{Trader: currentTrader, BTResult: NewBTResult()}}
	if err := restoreRollingResult(currentBT, previous, carried); err != nil {
		t.Fatal(err)
	}
	currentDeps := currentBT.RuntimeDependencies()
	if previous.reportRuntimeDeps().Orders != currentDeps.Orders || previous.runtimeDeps != currentDeps {
		t.Fatal("rolling result retained dependencies from the cleaned runtime")
	}
	if got := previous.historyOrders(); len(got) != 1 || got[0] != oldOrder {
		t.Fatalf("current runtime history = %#v, want carried order", got)
	}
	previous.Collect()
	if previous.OrderNum != 1 || previous.TotProfit != 5 {
		t.Fatalf("carried metrics = orders:%d profit:%v", previous.OrderNum, previous.TotProfit)
	}
}

func TestRunBTOnceUsesDistinctOwnedSnapshots(t *testing.T) {
	source := config.NewSnapshot(&config.Config{
		TimeRange: &config.TimeTuple{StartMS: 100, EndMS: 200},
		Pairs:     []string{"BTC/USDT"},
		RunPolicy: []*config.RunPolicyConfig{{Name: "source", Params: map[string]float64{"period": 10}}},
	})
	var received []*config.Snapshot
	factory := func(snapshot *config.Snapshot, isOpt bool, outDir string) (*BackTest, func(), *errs.Error) {
		if !isOpt || outDir != "" {
			t.Fatalf("factory inputs = isOpt:%v outDir:%q", isOpt, outDir)
		}
		received = append(received, snapshot)
		snapshot.View().RunPolicy[0].Params["period"] = 999
		return nil, nil, errs.NewMsg(errs.CodeRunTime, "stop after factory capture")
	}
	for _, period := range []float64{20, 30} {
		_, _, err := runBTOnce(source, factory, []*config.RunPolicyConfig{{Name: "trial", Params: map[string]float64{"period": period}}})
		if err == nil || err.Message() != "stop after factory capture" {
			t.Fatalf("trial error = %v", err)
		}
	}
	if len(received) != 2 || received[0] == received[1] {
		t.Fatalf("factory snapshots = %#v", received)
	}
	for i, snapshot := range received {
		if snapshot == source || snapshot.View().RunPolicy[0].Name != "trial" {
			t.Fatalf("factory trial %d did not receive derived policy snapshot", i)
		}
	}
	if got := source.View().RunPolicy[0].Params["period"]; got != 10 {
		t.Fatalf("trial mutation leaked into source snapshot: %v", got)
	}
}
