package runtime

import (
	"context"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

type sharedModeTraceAdapter struct {
	execution.ExecutionAdapter
	cancels, queries int
}

func (a *sharedModeTraceAdapter) Cancel(ctx context.Context, id string) (bool, error) {
	a.cancels++
	return a.ExecutionAdapter.Cancel(ctx, id)
}

func (a *sharedModeTraceAdapter) Query(ctx context.Context, client, id string) (execution.QueryResult, error) {
	a.queries++
	return a.ExecutionAdapter.Query(ctx, client, id)
}

func TestSharedIndependentPendingLotRevokesExchangeLimit(t *testing.T) {
	var trace *sharedModeTraceAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(adapter *runner.PaperAdapter) execution.ExecutionAdapter {
		trace = &sharedModeTraceAdapter{ExecutionAdapter: adapter}
		return trace
	})
	f.bridge.OrderPolicy.Mode = execution.ExchangeFirst
	first := f.entry(t, &strat.EnterReq{Tag: "first-limit", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	second := f.entry(t, &strat.EnterReq{Tag: "independent-limit", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 80})
	if trace.cancels != 1 || trace.queries < 1 {
		t.Fatalf("independent lot must revoke and query live reservation: cancels=%d queries=%d", trace.cancels, trace.queries)
	}
	snapshot, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Orders) != 0 || f.adapter.Metrics().Fills != 0 || first.Enter.Filled != 0 || second.Enter.Filled != 0 {
		t.Fatalf("conflicting limits must stay local without fills: orders=%+v fills=%+v", snapshot.Orders, f.adapter.Metrics())
	}
}

func TestSharedOrdinaryLimitSubmissionModes(t *testing.T) {
	for _, mode := range []execution.OrderMode{"", execution.ExchangeFirst} {
		name := string(mode)
		if name == "" {
			name = "default-local"
		}
		t.Run(name, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			f.bridge.OrderPolicy.Mode = mode
			od := f.entry(t, &strat.EnterReq{Tag: "distant", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
			snapshot, err := f.rt.SharedExecution().Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			wantOrders := 0
			if mode == execution.ExchangeFirst {
				wantOrders = 1
			}
			if len(snapshot.Orders) != wantOrders || od.Enter.Filled != 0 || f.adapter.Metrics().Fills != 0 {
				t.Fatalf("limit submission: orders=%d want=%d filled=%v venue=%+v", len(snapshot.Orders), wantOrders, od.Enter.Filled, f.adapter.Metrics())
			}
			f.observe(t, 90, 2)
			if f.steps(t) != 10 || od.Enter.Filled != 1 || f.adapter.Metrics().Fills != 1 {
				t.Fatalf("later visible quote did not hydrate facade fill: steps=%d filled=%v venue=%+v", f.steps(t), od.Enter.Filled, f.adapter.Metrics())
			}
		})
	}
}

func TestSharedAccountTickDrivesProtectionAndFacade(t *testing.T) {
	f := newSharedTriggerFixture(t)
	od := f.entry(t, &strat.EnterReq{Tag: "account-stop", Amount: 1, StopLoss: 95})
	f.price, f.bar = decimal.NewFromInt(94), 2
	f.rt.Clock.SetTimeMS(102)
	// No strategy update or facade market callback participates in this tick.
	if err := f.rt.SharedExecution().AdvancePendingExecution(context.Background(), 102); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 0 || od.Exit == nil || od.Exit.Filled != 1 {
		t.Fatalf("account-owned protection did not hydrate facade: steps=%d exit=%+v", f.steps(t), od.Exit)
	}
}

func TestSharedConfiguredLimitPriceCallbackIsOptional(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "quote-fallback"
		if enabled {
			name = "volume-price"
		}
		t.Run(name, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			calls := 0
			if enabled {
				f.bridge.DefaultLimitPrice = func(ctx context.Context, symbol string, side execution.OrderSide, seconds int) (float64, error) {
					calls++
					if ctx.Err() != nil || symbol != "BTC" || side != execution.Buy || seconds != 17 {
						t.Fatalf("volume price callback arguments: %s %s %d %v", symbol, side, seconds, ctx.Err())
					}
					return 90, nil
				}
			}
			deps := f.rt.BizDeps()
			deps.Config = config.NewSnapshot(&config.Config{OrderType: "limit", LimitVolSecs: 17})
			manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
			if err != nil {
				t.Fatal(err)
			}
			od, failure := manager.EnterOrder(f.job.Symbol, "ws", &strat.EnterReq{StratName: "legacy", Tag: name, Amount: 1})
			if failure != nil {
				t.Fatal(failure)
			}
			wantPrice, wantCalls := float64(100), 0
			if enabled {
				wantPrice, wantCalls = 90, 1
			}
			if od.Enter.Price != wantPrice || calls != wantCalls {
				t.Fatalf("configured limit: price=%v want=%v callback=%d want=%d", od.Enter.Price, wantPrice, calls, wantCalls)
			}
		})
	}
}
