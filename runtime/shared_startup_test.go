package runtime

import (
	"context"
	"database/sql"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/strat"
	"testing"
)

func TestStartupRecoversOfflineFillBeforeRepeatReconciliation(t *testing.T) {
	for _, state := range []execution.RealOrderState{execution.OrderPartial, execution.OrderSending, execution.OrderUnknown, execution.OrderCancelPending} {
		t.Run(string(state), func(t *testing.T) {
			var adapter *partialRuntimeAdapter
			f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
				adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
				return adapter
			})
			f.entry(t, &strat.EnterReq{Tag: "restart", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 100})
			if err := f.rt.SharedExecution().Reconcile("repeat-startup", 100); err != nil {
				t.Fatal(err)
			}
			snapshot, _ := f.rt.SharedExecution().Snapshot(context.Background())
			id := snapshot.Orders[0].Intent.ID
			client := snapshot.Orders[0].ClientID
			exchange := snapshot.Orders[0].ExchangeID
			f.process.Close()
			db, err := sql.Open("sqlite", f.opts.StorePath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("UPDATE exec_order SET state=? WHERE id=?", string(state), id); err != nil {
				t.Fatal(err)
			}
			db.Close()
			adapter.complete = true
			if _, err := adapter.Query(context.Background(), client, exchange); err != nil {
				t.Fatal(err)
			} // venue completes while owner is offline
			p := NewProcess()
			defer p.Close()
			rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
			if err != nil {
				t.Fatal(err)
			}
			if err := rt.SharedExecution().Reconcile("repeat-startup", 101); err == nil {
				t.Fatal("offline fill unexpectedly reconciled without recovery")
			}
			if err := rt.SharedExecution().RecoverPersisted(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := rt.SharedExecution().Reconcile("repeat-startup", 101); err != nil {
				t.Fatal(err)
			}
			final, err := rt.SharedExecution().Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(final.Lots) != 1 || final.Lots[0].SignedSteps != 10 || final.Lots[0].Strategy != "ts" || len(final.Orders) != 0 {
				t.Fatal("recovery lost attributed offline fill or uncertainty", final)
			}
			if len(adapter.trace) != 1 {
				t.Fatal("startup resent durable order", adapter.trace)
			}
			if err := rt.SharedExecution().RecoverPersisted(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := rt.SharedExecution().Reconcile("repeat-startup", 101); err != nil {
				t.Fatal(err)
			}
			again, _ := rt.SharedExecution().Snapshot(context.Background())
			if again.Checkpoint != final.Checkpoint || !again.AccountSettledCash.Equal(final.AccountSettledCash) {
				t.Fatal("duplicate startup replayed accounting", final, again)
			}
		})
	}
}
