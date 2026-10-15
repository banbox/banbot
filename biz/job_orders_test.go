package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type jobOrderProcessorStub struct {
	IOrderMgr
	jobs []*strat.StratJob
	err  *errs.Error
}

func (s *jobOrderProcessorStub) ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	s.jobs = append(s.jobs, job)
	return job.LongOrders, job.ShortOrders, s.err
}

func TestProcessJobOrdersRespectsRuntimeAccountOwnership(t *testing.T) {
	legacy := &jobOrderProcessorStub{}
	old := accOdMgrs
	accOdMgrs = map[string]IOrderMgr{config.DefAcc: legacy, "same": legacy}
	t.Cleanup(func() { accOdMgrs = old })
	for i := 0; i < 2; i++ {
		deps := completeTraderDepsForTest(RuntimeDeps{})
		manager := &jobOrderProcessorStub{err: errs.NewMsg(core.ErrBadConfig, "sentinel")}
		deps.Trading.SetOrderManager("same", manager)
		job := &strat.StratJob{Account: "same", LongOrders: []*ormo.InOutOrder{{}}}
		job.BindRuntimeState(deps.Strategies, deps.Core, deps.Clock)
		entered, _, err := ProcessJobOrders(job)
		if err != manager.err || len(entered) != 1 || len(manager.jobs) != 1 || manager.jobs[0] != job {
			t.Fatalf("runtime %d did not use its manager", i)
		}
	}
	if len(legacy.jobs) != 0 {
		t.Fatal("runtime job used the legacy account manager")
	}
	job := &strat.StratJob{Account: "same"}
	if _, _, err := ProcessJobOrders(job); err != nil || len(legacy.jobs) != 1 {
		t.Fatalf("legacy job did not use its legacy manager: %v", err)
	}
}

func TestProcessJobOrdersRejectsMissingRuntimeManager(t *testing.T) {
	legacy := &jobOrderProcessorStub{}
	old := accOdMgrs
	accOdMgrs = map[string]IOrderMgr{config.DefAcc: legacy, "same": legacy}
	t.Cleanup(func() { accOdMgrs = old })
	deps := completeTraderDepsForTest(RuntimeDeps{})
	for _, state := range []*strat.State{deps.Strategies, strat.NewState()} {
		job := &strat.StratJob{Account: "same"}
		job.BindRuntimeState(state, deps.Core, deps.Clock)
		if _, _, err := ProcessJobOrders(job); err == nil {
			t.Fatal("incomplete runtime accepted order processing")
		}
	}
	partial := &strat.StratJob{Account: "same"}
	partial.BindRuntimeMarket(deps.Market.Prices, deps.Clock)
	if _, _, err := ProcessJobOrders(partial); err == nil {
		t.Fatal("partially bound runtime used legacy manager")
	}
	strat.AddStratGroup("jobprocessor-test", map[string]strat.FuncMakeStrat{
		"explicit": func(*config.RunPolicyConfig) *strat.TradeStrat { return &strat.TradeStrat{} },
	})
	partial = &strat.StratJob{Account: "same", Strat: strat.NewState().NewStrategy(&config.RunPolicyConfig{Name: "jobprocessor-test:explicit"})}
	if _, _, err := ProcessJobOrders(partial); err == nil {
		t.Fatal("explicit strategy missing its job state used legacy manager")
	}
	if _, _, err := ProcessJobOrders(nil); err == nil {
		t.Fatal("nil job accepted")
	}
	if len(legacy.jobs) != 0 {
		t.Fatal("incomplete runtime fell back to the legacy account")
	}
}

func TestRuntimeOrderProcessorBindingRejectsDifferentRegistry(t *testing.T) {
	deps := completeTraderDepsForTest(RuntimeDeps{})
	deps.Strategies, deps.Orders, deps.Trading = strat.NewState(), ormo.NewOrderState(), nil
	if err := BindRuntimeDeps(deps); err != nil {
		t.Fatal(err)
	}
	manager := &jobOrderProcessorStub{}
	deps.Trading = NewTradingState()
	deps.Trading.SetOrderManager("same", manager)
	if err := BindRuntimeDeps(deps); err != nil {
		t.Fatalf("completing a previously missing registry failed: %v", err)
	}
	job := &strat.StratJob{Account: "same"}
	job.BindRuntimeState(deps.Strategies, deps.Core, deps.Clock)
	if _, _, err := ProcessJobOrders(job); err != nil || len(manager.jobs) != 1 {
		t.Fatalf("completed runtime did not process the job: %v", err)
	}
	changed := deps
	changed.Trading = NewTradingState()
	if _, err := NewTraderWithRuntimeDeps(changed); err == nil {
		t.Fatal("constructing a trader with another account registry succeeded")
	}
	if err := BindRuntimeDeps(changed); err == nil {
		t.Fatal("replacing the bound account registry succeeded")
	}
	changed = deps
	changed.DefaultAccount = "different"
	if _, err := NewTraderWithRuntimeDeps(changed); err == nil {
		t.Fatal("constructing a trader with another default account succeeded")
	}
	if err := BindRuntimeDeps(changed); err == nil {
		t.Fatal("changing the bound default account succeeded")
	}
	if _, _, err := ProcessJobOrders(job); err != nil || len(manager.jobs) != 2 {
		t.Fatalf("rejected rebind changed the original processor: %v", err)
	}
}
