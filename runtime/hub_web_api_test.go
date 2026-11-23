package runtime

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	weblive "github.com/banbox/banbot/web/live"
	"github.com/gofiber/fiber/v2"
	"github.com/shopspring/decimal"
)

// The dashboard API of a Hub robot must answer from the execution ledger and
// its projections: the robot holds no wallet, no account credentials and keeps
// open orders only in memory.
func TestHubRobotDashboardAPIUsesExecutionLedger(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.rt.Orders.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
	biz.EnsureTimeSeriesOrderTask(f.rt.BizDeps())
	app := fiber.New()
	weblive.RegisterBotAPI(app.Group("/api/bot"), f.rt.BizDeps())

	call := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		request := httptest.NewRequest(method, "/api/bot"+path, strings.NewReader(body))
		request.Header.Set("X-Account", "default")
		if body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := app.Test(request, -1)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		var decoded map[string]any
		_ = json.Unmarshal(raw, &decoded)
		if decoded == nil {
			decoded = map[string]any{"raw": string(raw)}
		}
		return response.StatusCode, decoded
	}
	orders := func(status string) []any {
		t.Helper()
		code, body := call(http.MethodGet, "/orders?source=bot&status="+status, "")
		if code != http.StatusOK {
			t.Fatalf("orders %s: %d %v", status, code, body)
		}
		list, _ := body["data"].([]any)
		return list
	}

	// Balance comes from the ledger: one settlement-currency row, no wallet.
	code, balance := call(http.MethodGet, "/balance", "")
	items, _ := balance["items"].([]any)
	if code != http.StatusOK || len(items) != 1 || balance["total"].(float64) != 1000 {
		t.Fatalf("balance: %d %v", code, balance)
	}
	if row := items[0].(map[string]any); row["symbol"] != "USDT" || row["total"].(float64) != 1000 || row["free"].(float64) != 1000 {
		t.Fatalf("balance row: %v", row)
	}
	if code, body := call(http.MethodPost, "/refresh_wallet", ""); code != http.StatusOK || body["total"].(float64) != 1000 {
		t.Fatalf("refresh wallet: %d %v", code, body)
	}

	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "open", Tag: "remote"})
	if err != nil || od == nil || od.Enter.Filled != 1 {
		t.Fatal(od, err)
	}
	// The open order exists only as an in-memory projection, yet is listed.
	open := orders("open")
	if len(open) != 1 || open[0].(map[string]any)["enter_tag"] != "remote" || int64(open[0].(map[string]any)["id"].(float64)) != od.ID {
		t.Fatalf("open orders: %v", open)
	}
	if his := orders("his"); len(his) != 0 {
		t.Fatalf("history before close: %v", his)
	}
	// Margin of the open position is taken out of the available balance.
	if _, balance = call(http.MethodGet, "/balance", ""); balance["items"].([]any)[0].(map[string]any)["free"].(float64) >= 1000 {
		t.Fatalf("open position did not reduce available balance: %v", balance)
	}

	// Raw exchange actions are refused: the robot has no credentials and the
	// position belongs to the execution account.
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/close_exg_pos", `{"symbol":"all"}`},
		{http.MethodGet, "/orders?source=position", ""},
		{http.MethodGet, "/orders?source=exchange", ""},
		{http.MethodGet, "/incomes?intype=REALIZED_PNL", ""},
	} {
		if code, body := call(tc.method, tc.path, tc.body); code == http.StatusOK {
			t.Fatalf("%s %s must be refused on a Hub robot: %v", tc.method, tc.path, body)
		}
	}
	if f.owner.adapter.Metrics().Fills != 1 {
		t.Fatal("a refused endpoint reached the venue")
	}

	// Closing through the dashboard goes to the strategy order manager. The
	// exit fills 10 above the entry, so the closed order has a real profit.
	f.owner.price = decimal.NewFromInt(110)
	code, closed := call(http.MethodPost, "/exit_order", `{"orderId":"`+jsonInt(od.ID)+`"}`)
	if code != http.StatusOK || closed["closeNum"].(float64) != 1 || closed["errMsg"] != "" {
		t.Fatalf("exit order: %d %v", code, closed)
	}
	if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); err != nil {
		t.Fatal(err)
	}
	if f.owner.adapter.Metrics().Fills != 2 {
		t.Fatalf("dashboard close did not fill: %d fills", f.owner.adapter.Metrics().Fills)
	}
	if open = orders("open"); len(open) != 0 {
		t.Fatalf("closed order still listed open: %v", open)
	}
	his := orders("his")
	if len(his) != 1 || his[0].(map[string]any)["enter_tag"] != "remote" || his[0].(map[string]any)["exit_tag"] == "" {
		t.Fatalf("history after close: %v", his)
	}
	// History shows the realized result, also after the market moved away: a
	// closed order must never be re-marked at the current price.
	f.owner.price = decimal.NewFromInt(50)
	f.rt.Market.Prices.SetBarPriceAt(f.rt.Clock.TimeMS(), "BTC", 50)
	closedRow := orders("his")[0].(map[string]any)
	if closedRow["profit"].(float64) != 10 || closedRow["profit_rate"].(float64) != 0.1 || closedRow["curPrice"].(float64) != 110 {
		t.Fatalf("closed order was re-marked: profit=%v rate=%v curPrice=%v", closedRow["profit"], closedRow["profit_rate"], closedRow["curPrice"])
	}
	if code, body := call(http.MethodGet, "/statistics", ""); code != http.StatusOK {
		t.Fatalf("statistics: %d %v", code, body)
	}
	if code, body := call(http.MethodGet, "/bot_info", ""); code != http.StatusOK {
		t.Fatalf("bot info: %d %v", code, body)
	}
}

func jsonInt(v int64) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}
