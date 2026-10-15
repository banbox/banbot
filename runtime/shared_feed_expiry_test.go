package runtime

import (
	"context"
	"fmt"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"testing"
)

type feedRevisionAdapter struct{ *partialRuntimeAdapter }

func (a feedRevisionAdapter) Query(ctx context.Context, client, id string) (execution.QueryResult, error) {
	q, err := a.partialRuntimeAdapter.Query(ctx, client, id)
	for index := range q.Receipt.Fills {
		q.Receipt.Fills[index].EventID += fmt.Sprintf("/%d", q.Receipt.Fills[index].Steps)
	}
	return q, err
}

func TestSharedSDKZeroBarStopBarsUsesOwnFeedAndRestoresCountdown(t *testing.T) {
	var adapter *partialRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
		return &feedRevisionAdapter{adapter}
	})
	original := f.bridge.Quote
	f.bridge.Quote = func(id string, now int64) (execution.VisibleQuote, error) {
		q, err := original(id, now)
		q.Bar = 0
		return q, err
	}
	f.job.TimeFrame = "1m"
	od := f.entry(t, &strat.EnterReq{Tag: "primary", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 100, StopBars: 2, StopLoss: 95})
	f.job.TimeFrame = "5m"
	slow := f.entry(t, &strat.EnterReq{Tag: "slow", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 2})
	emit := func(tf string, end int64) {
		t.Helper()
		if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: tf, EndMS: end, TimeMS: end - 1, Closed: true, Values: map[string]any{"close": 100.0}}); err != nil {
			t.Fatal(err)
		}
	}
	emit("1m", 101)
	emit("1m", 101)
	if adapter.orders == nil || f.steps(t) != 5 {
		t.Fatal("first primary bar closed filled lot")
	}
	f.process.Close()
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	rt.Clock.SetTimeMS(102)
	if err := rt.SharedExecution().RecoverPersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := rt.SharedExecution().Reconcile("feed-restart", 102); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	f.manager = biz.GetOdMgrWithState(rt.Trading, "default")
	adapter.cancelFillSteps = 2
	emit("1m", 102)
	if got := f.steps(t); got != 7 {
		t.Fatal("expiry failed final fill attribution", got)
	}
	snapshot, err := rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Orders) != 0 {
		t.Fatal("expired entry remainder still live", snapshot.Orders)
	}
	rows, lock := rt.Orders.GetOpenODs("default")
	lock.Lock()
	primary, secondary := rows[od.ID], rows[slow.ID]
	lock.Unlock()
	if primary == nil || primary.Enter.Filled != .7 || primary.GetStopLoss() == nil || primary.GetStopLoss().Price != 95 {
		t.Fatal("expiry lost held lot/protection", primary)
	}
	if secondary == nil || secondary.Status >= ormo.InOutStatusFullExit {
		t.Fatal("1m feed expired 5m contributor", secondary)
	}
	emit("5m", 103)
	if f.steps(t) != 7 {
		t.Fatal("slow first bar affected held lot")
	}
}
