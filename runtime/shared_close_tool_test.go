package runtime

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/strat"
)

func TestSharedCloseToolUsesAttributedExecutionOrders(t *testing.T) {
	f := newSharedTriggerFixture(t)
	filled := f.entry(t, &strat.EnterReq{Tag: "filled", Amount: 1})
	f.entry(t, &strat.EnterReq{Tag: "pending", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	f.rt.Core.LiveMode = true
	f.rt.Core.SetRunEnv(core.RunEnvProd)
	deps := f.rt.BizDeps()
	deps.Config = config.NewSnapshot(&config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}})
	deps.Accounts = deps.Config.View().Accounts
	for _, request := range []live.TradeCloseRequest{
		{Accounts: []string{"default"}, Confirmed: false},
		{Accounts: []string{"default"}, Exchange: true, Confirmed: true},
	} {
		if _, err := live.CloseOrdersWithRuntimeDeps(deps, request); err == nil {
			t.Fatal("unconfirmed or unattributed close was accepted")
		}
		if f.steps(t) != 10 {
			t.Fatal("rejected close changed the position")
		}
	}
	result, err := live.CloseOrdersWithRuntimeDeps(deps, live.TradeCloseRequest{Accounts: []string{"default"}, Pairs: []string{"BTC"}, Confirmed: true})
	if err != nil || result.Closed != 2 || result.Failed != 0 || f.steps(t) != 0 {
		t.Fatal("shared close did not cancel pending and close filled orders", result, err)
	}
	if filled.ExitTag != core.ExitTagCli {
		t.Fatal("CLI exit attribution was lost", filled.ExitTag)
	}
	if _, err := live.CloseOrdersWithRuntimeDeps(deps, live.TradeCloseRequest{Accounts: []string{"default"}, Confirmed: true}); err != nil {
		t.Fatal("repeated shared close should not resubmit closed orders", err)
	}
}
