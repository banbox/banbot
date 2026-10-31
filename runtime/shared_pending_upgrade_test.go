package runtime

import (
	"context"
	"database/sql"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedPriorCheckpointSeedsAccountPendingProtectionOnRestart(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "prior-protected-lot", Amount: 1, StopLoss: 95})
	f.process.Close()
	db, err := sql.Open("sqlite", f.opts.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(`DELETE FROM exec_checkpoint WHERE kind='strategy' AND strategy=? AND name=?`, execution.PendingBookStrategy, execution.PendingBookName)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	removed, err := result.RowsAffected()
	if err != nil || removed != 1 {
		db.Close()
		t.Fatalf("prior-version fixture must remove only pending book: rows=%d err=%v", removed, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	p := NewProcess()
	t.Cleanup(p.Close)
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	rt.Clock.SetTimeMS(101)
	if err := rt.SharedExecution().Reconcile("prior-checkpoint-restart", 101); err != nil {
		t.Fatal(err)
	}
	before := f.adapter.Metrics().Fills
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	manager := biz.GetOdMgrWithState(rt.Trading, "default").(*biz.SharedOrderMgr)
	f.job = &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: "ws", Account: "default", CloseLong: true, CloseShort: true, ExgStopLoss: true, ExgTakeProfit: true}
	f.job.BindRuntimeState(rt.Strategies, rt.Core, rt.Clock)
	f.job.BindRuntimeMarket(rt.Market.Prices, rt.Clock)
	if err := manager.BindJobs([]*strat.StratJob{f.job}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SharedExecution().WithState(func(account *execution.SharedAccount) error {
		book, err := account.Store().LoadPendingBook(context.Background())
		if err == nil && len(book) != 1 {
			t.Fatalf("prior protected lot was not seeded: %+v", book)
		}
		if err == nil {
			found := false
			for _, spec := range book[0].Specs {
				found = found || spec.Kind == execution.StopLossProtection && spec.Level.Equal(decimal.NewFromInt(95))
			}
			if !found {
				t.Fatalf("seeded pending book lost stop protection: %+v", book[0].Specs)
			}
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	orders := f.job.ExecutionSnapshot().LongOrders
	if len(orders) != 1 || orders[0].Enter.Filled != 1 || f.adapter.Metrics().Fills != before {
		t.Fatalf("upgrade must restore without sending: orders=%+v fills=%+v", orders, f.adapter.Metrics())
	}
	open, lock := rt.BizDeps().Orders.GetOpenODs("default")
	lock.Lock()
	restored := open[orders[0].ID]
	lock.Unlock()
	if restored == nil {
		t.Fatal("restored job order missing from facade order state")
	}
	f.price, f.bar = decimal.NewFromInt(94), 2
	rt.Clock.SetTimeMS(102)
	if err := rt.SharedExecution().AdvancePendingExecution(context.Background(), 102); err != nil {
		t.Fatal(err)
	}
	if err := manager.LastError(); err != nil {
		t.Fatal("account tick facade projection failed", err)
	}
	if f.steps(t) != 0 || restored.Exit == nil || restored.Exit.Filled != 1 || f.adapter.Metrics().Fills != before+1 {
		t.Fatalf("account tick did not execute/project upgraded protection: steps=%d exit=%+v fills=%+v", f.steps(t), restored.Exit, f.adapter.Metrics())
	}
	if len(f.job.ExecutionSnapshot().LongOrders) != 0 {
		t.Fatal("account tick left closed protected lot in strategy projection")
	}
}
