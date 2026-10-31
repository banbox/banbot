package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedEnvEndClosesSymbolAcrossTimeframesAndCancelsPending(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.job.TimeFrame = "1m"
	first := f.entry(t, &strat.EnterReq{Tag: "fast", Amount: 1})
	f.job.TimeFrame = "5m"
	second := f.entry(t, &strat.EnterReq{Tag: "slow", Amount: 1})
	pending := f.entry(t, &strat.EnterReq{Tag: "pending", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	// The legacy EnvEnd request retains the ordinary close-delay policy.
	f.rt.Clock.SetTimeMS(1_000_000)
	event := &orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: f.job.Symbol, TimeFrame: "1m", EndMS: 1_000_000,
		Values: map[string]any{"high": 1000.0, "low": 1.0, "close": 1.0}}
	if err := f.manager.OnEnvEnd(event); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil || len(snapshot.Lots) != 0 || len(snapshot.Orders) != 0 {
		t.Fatalf("environment ended with exposure or orders: %+v err=%v", snapshot, err)
	}
	if metrics := f.adapter.Metrics(); metrics.Fills != 3 || metrics.LastFill.Steps != 20 || metrics.LastFill.Price.String() != "100" {
		t.Fatalf("expected two entries and one net exit at visible price, pending limit must not fill: %+v", metrics)
	}
	rows, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	for _, id := range []int64{first.ID, second.ID} {
		order := rows[id]
		if order == nil || order.ExitTag != core.ExitTagEnvEnd {
			lock.Unlock()
			t.Fatalf("missing environment-end projection: order=%+v", order)
		}
	}
	if order := rows[pending.ID]; order == nil || order.Status != ormo.InOutStatusDelete || order.Enter.Filled != 0 {
		lock.Unlock()
		t.Fatalf("pending entry was not canceled without a fill: %+v", order)
	}
	lock.Unlock()
	before := f.adapter.Metrics().Fills
	if err := f.manager.OnEnvEnd(event); err != nil {
		t.Fatal(err)
	}
	if f.adapter.Metrics().Fills != before {
		t.Fatal("repeated environment end submitted another exit")
	}
}

func TestSharedEnvEndUnrelatedSymbolDoesNotEvaluateProtections(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "held", Amount: 1, StopLoss: 95})
	before, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.price = decimal.NewFromInt(94)
	if err := f.manager.OnEnvEnd(&orm.DataSeries{Source: "kline", Sid: 2, ExSymbol: &orm.ExSymbol{ID: 2, Symbol: "OTHER"}, TimeFrame: "1m"}); err != nil {
		t.Fatal(err)
	}
	after, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil || !reflect.DeepEqual(before.Lots, after.Lots) || f.adapter.Metrics().Fills != 1 {
		t.Fatalf("unrelated environment end changed exposure: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestSharedEnvEndUsesFinalCancelFill(t *testing.T) {
	var adapter *partialRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
		return &feedRevisionAdapter{adapter}
	})
	f.entry(t, &strat.EnterReq{Tag: "partial", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 100})
	adapter.cancelFillSteps = 2
	if err := f.manager.OnEnvEnd(&orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: f.job.Symbol, TimeFrame: "ws"}); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 0 || len(adapter.trace) != 2 || adapter.trace[1].Steps != 7 {
		t.Fatalf("environment exit missed concurrent cancel fill: steps=%d orders=%+v", f.steps(t), adapter.trace)
	}
}

func TestSharedEnvEndClosesShortAndPreservesCloseDelay(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.job.TimeFrame = "1m"
	f.entry(t, &strat.EnterReq{Tag: "short", Short: true, Amount: 1})
	event := &orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: f.job.Symbol, TimeFrame: "1m"}
	if err := f.manager.OnEnvEnd(event); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != -10 {
		t.Fatal("environment end changed the legacy close-delay policy")
	}
	f.rt.Clock.SetTimeMS(60_100)
	if err := f.manager.OnEnvEnd(event); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 0 || f.adapter.Metrics().Fills != 2 {
		t.Fatal("environment end did not close the short exposure")
	}
}

func TestSharedEnvEndNilAndUnresolvedSymbolAreSafe(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "held", Amount: 1})
	if err := f.manager.OnEnvEnd(nil); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.OnEnvEnd(&orm.DataSeries{Source: "kline"}); err == nil {
		t.Fatal("unresolved environment symbol must not become an all-account exit")
	}
	if f.steps(t) != 10 || f.adapter.Metrics().Fills != 1 {
		t.Fatal("invalid environment event changed exposure")
	}
}
