package runtime

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	weblive "github.com/banbox/banbot/web/live"
	"github.com/gofiber/fiber/v2"
	"github.com/shopspring/decimal"
)

// A factor-only Hub robot has no HubOrderBridge/time-series manager; its
// metrics and dashboard read the Hub ledger through a read-only binding.
func TestFactorOnlyHubRobotReadsLedgerReadOnly(t *testing.T) {
	f := newHubTSFixture(t, nil) // supplies the Hub; its manager plays "another writer"
	rt, err := f.owner.process.NewRuntime(Options{Mode: core.RunModeLive, Env: core.RunEnvDryRun})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { rt.Close(); rt.Join() }()
	deps := rt.BizDeps()
	if deps.HubOrderBridge != nil || deps.SharedExecution != nil {
		t.Fatal("factor-only runtime must not carry an order binding")
	}
	if _, m, backed, _ := biz.ExecutionAccountBalance(deps, "default"); backed || m.Valid {
		t.Fatal("unbound runtime reported a backed account")
	}
	release, err := biz.BindHubAccountReader(deps, f.client, f.owner.key, execution.PortfolioRisk{MarginRate: decimal.NewFromInt(1)})
	if err != nil {
		t.Fatal(err)
	}
	reader := biz.BoundHubAccountReader(deps)
	currency, m, backed, remote := biz.ExecutionAccountBalance(deps, "default")
	if !backed || !remote || currency != "USDT" || !m.Valid || m.Equity != 1000 || m.Available != 1000 {
		t.Fatalf("balance: %s %+v backed=%v remote=%v", currency, m, backed, remote)
	}
	// Strategy-facing metrics see the same ledger.
	job := &strat.StratJob{Account: "default", Strat: &strat.TradeStrat{Name: "factor"}}
	job.BindRuntimeState(rt.Strategies, rt.Core, rt.Clock)
	if eq, av := job.AccountEquity(), job.AvailableBalance(); eq != 1000 || av != 1000 {
		t.Fatalf("strategy-facing metrics: equity=%v available=%v", eq, av)
	}
	// No time-series order manager, writer or journal was created.
	if biz.GetOdMgrWithState(rt.Trading, "default") != nil {
		t.Fatal("factor-only runtime built an order manager")
	}

	// Repeated reads share one Hub round within the TTL.
	for i := 0; i < 200; i++ {
		biz.ExecutionAccountBalance(deps, "default")
	}
	if n := reader.RPCRounds(); n != 1 {
		t.Fatalf("Hub rounds under repeated reads = %d, want 1", n)
	}

	// Dashboard answers from the ledger.
	app := fiber.New()
	weblive.RegisterBotAPI(app.Group("/api/bot"), deps)
	for _, path := range []string{"/balance"} {
		req := httptest.NewRequest(http.MethodGet, "/api/bot"+path, strings.NewReader(""))
		req.Header.Set("X-Account", "default")
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		items, _ := body["items"].([]any)
		if resp.StatusCode != http.StatusOK || len(items) != 1 || body["total"].(float64) != 1000 || items[0].(map[string]any)["symbol"] != "USDT" {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, raw)
		}
	}

	// A position opened by the account's writer is visible (cache expires by
	// a fresh reader: bind a second runtime to avoid sleeping).
	if od, e := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "open", Tag: "x"}); e != nil || od == nil {
		t.Fatal(od, e)
	}
	rt2, err := f.owner.process.NewRuntime(Options{Mode: core.RunModeLive, Env: core.RunEnvDryRun})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { rt2.Close(); rt2.Join() }()
	deps2 := rt2.BizDeps()
	release2, err := biz.BindHubAccountReader(deps2, f.client, f.owner.key, execution.PortfolioRisk{MarginRate: decimal.NewFromInt(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, m2, _, _ := biz.ExecutionAccountBalance(deps2, "default"); !m2.Valid || m2.Equity <= 0 || m2.Available >= m2.Equity {
		t.Fatalf("open position margin not reflected: %+v", m2)
	}
	release2()
	release()
	if _, _, backed, _ := biz.ExecutionAccountBalance(deps, "default"); backed {
		t.Fatal("release did not unbind")
	}
}
