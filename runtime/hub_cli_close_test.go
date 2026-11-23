package runtime

import (
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func hubCloseDeps(deps biz.RuntimeDeps) biz.RuntimeDeps {
	deps.Config = config.NewSnapshot(&config.Config{StakeCurrency: []string{"USDT"}})
	deps.Accounts = map[string]*config.AccountConfig{"default": {}}
	return deps
}

func checkHubCliClose(t *testing.T, deps biz.RuntimeDeps, manager *biz.HubOrderMgr, rt *Runtime, symbol *orm.ExSymbol, tf string) {
	t.Helper()
	deps = hubCloseDeps(deps)
	od, e := manager.EnterOrder(symbol, tf, &strat.EnterReq{CommandID: "cli-open", Tag: "cli"})
	if e != nil || od == nil || od.Enter.Filled != 1 {
		t.Fatal(od, e)
	}
	// Raw venue closing stays refused and nothing is closed by the refusal.
	if _, err := live.CloseOrdersWithRuntimeDeps(deps, live.TradeCloseRequest{Exchange: true, Confirmed: true}); err == nil {
		t.Fatal("raw exchange close accepted for an execution-backed account")
	}
	if _, err := live.CloseOrdersWithRuntimeDeps(deps, live.TradeCloseRequest{Accounts: []string{"other"}, Confirmed: true}); err == nil {
		t.Fatal("close accepted for an unknown account")
	}
	// A strategy filter that matches nothing closes nothing.
	if result, err := live.CloseOrdersWithRuntimeDeps(deps, live.TradeCloseRequest{Strategies: []string{"nobody"}, Confirmed: true}); err != nil || result.Closed != 0 {
		t.Fatalf("filtered close: %+v %v", result, err)
	}
	result, err := live.CloseOrdersWithRuntimeDeps(deps, live.TradeCloseRequest{Pairs: []string{"BTC"}, Strategies: []string{"legacy"}, Confirmed: true})
	if err != nil || result.Closed != 1 || result.Failed != 0 {
		t.Fatalf("cli close: %+v %v", result, err)
	}
	if err := manager.UpdateByDataSeries(nil, nil); err != nil {
		t.Fatal(err)
	}
	closed := hubOrderByID(rt, od.ID)
	if closed == nil || closed.Status != ormo.InOutStatusFullExit || closed.ExitTag != core.ExitTagCli || closed.Exit == nil || closed.Exit.Filled != 1 {
		t.Fatalf("strategy order not closed through the manager: %+v", closed)
	}
}

func TestCliCloseThroughRemoteHubOrderMgr(t *testing.T) {
	f := newHubTSFixture(t, nil)
	deps := f.rt.BizDeps()
	if deps.SharedExecution != nil || deps.HubOrderBridge == nil {
		t.Fatal("not a remote Hub runtime")
	}
	checkHubCliClose(t, deps, f.manager, f.rt, &orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws")
}

func TestCliCloseThroughLocalAccountHubOrderMgr(t *testing.T) {
	f := newEmbeddedHubFixture(t, nil)
	deps := f.rt.BizDeps()
	if deps.SharedExecution == nil || deps.HubOrderBridge == nil {
		t.Fatal("not an embedded Hub order manager runtime")
	}
	checkHubCliClose(t, deps, f.manager, f.rt, &orm.ExSymbol{ID: 1, Symbol: "BTC"}, "1m")
}

// A Hub runtime without an order manager for the account must fail closed, not
// fall back to raw venue calls.
func TestCliCloseHubWithoutManagerFailsClosed(t *testing.T) {
	f := newHubTSFixture(t, nil)
	deps := hubCloseDeps(f.rt.BizDeps())
	deps.Trading = biz.NewTradingState()
	if _, err := live.CloseOrdersWithRuntimeDeps(deps, live.TradeCloseRequest{Confirmed: true}); err == nil {
		t.Fatal("close without an execution order manager succeeded")
	}
}
