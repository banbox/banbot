package live

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/gofiber/fiber/v2"

	"github.com/banbox/banbot/web/base"
)

// factorOnlyHubDeps is the runtime of a factor-only Hub robot: data mode, no
// order manager, no strategy jobs, no HubOrderBridge and no SharedExecution.
func factorOnlyHubDeps(t *testing.T) *apiHandlers {
	t.Helper()
	deps := testHandlerDeps(t)
	deps.Core.SetRunMode(core.RunModeData)
	deps.Orders.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
	// Create the trades database the way a started runtime has it.
	if _, conn, err := deps.Orders.Conn(true); err != nil {
		t.Fatal(err)
	} else {
		conn.Close()
	}
	return newAPIHandlers(deps)
}

func doReq(t *testing.T, app *fiber.App, method, url, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("X-Account", "acc")
	if body != "" {
		req.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	}
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data)
}

func TestFactorOnlyRuntimeRoutesNeverPanicOrFail5xx(t *testing.T) {
	h := factorOnlyHubDeps(t)
	app := fiber.New(fiber.Config{ErrorHandler: base.ErrHandler})
	RegisterBotAPI(app, *h.deps)

	getURLs := []string{
		"/version", "/balance", "/today_num", "/statistics", "/incomes?intype=x",
		"/task_pairs", "/exs_map", "/orders?source=bot", "/orders?source=exchange",
		"/orders?source=position", "/config", "/stg_jobs", "/performance?groupBy=day",
		"/get_down_trade?id=1&source=x", "/group_sta", "/bot_info",
	}
	for _, url := range getURLs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("GET %s panicked: %v", url, r)
				}
			}()
			code, body := doReq(t, app, http.MethodGet, url, "")
			if code >= 500 {
				t.Errorf("GET %s = %d %s", url, code, body)
			}
		}()
	}
	for _, url := range []string{"/balance", "/orders?source=exchange", "/orders?source=position", "/incomes?intype=x"} {
		code, body := doReq(t, app, http.MethodGet, url, "")
		if code != http.StatusBadRequest || !strings.Contains(body, "factor-only Hub robot") {
			t.Errorf("GET %s = %d %q, want 400 with the factor-only message", url, code, body)
		}
	}
	// Bot-order listings are empty rather than failing.
	if code, body := doReq(t, app, http.MethodGet, "/stg_jobs", ""); code != http.StatusOK || !strings.Contains(body, `"jobs":[]`) {
		t.Errorf("/stg_jobs = %d %s", code, body)
	}

	posts := map[string]string{
		"/exit_order":       `{"orderId":"all"}`,
		"/close_exg_pos":    `{"symbol":"all"}`,
		"/refresh_wallet":   ``,
		"/calc_profits":     ``,
		"/delay_entry":      `{"secs":1}`,
		"/start_down_trade": `{"startTime":"2024-01-01","endTime":"2024-01-02","source":"x"}`,
	}
	for url, body := range posts {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("POST %s panicked: %v", url, r)
				}
			}()
			code, text := doReq(t, app, http.MethodPost, url, body)
			if code >= 500 {
				t.Errorf("POST %s = %d %s", url, code, text)
			}
		}()
	}
	for _, url := range []string{"/exit_order", "/close_exg_pos"} {
		code, body := doReq(t, app, http.MethodPost, url, posts[url])
		if code != http.StatusBadRequest || body == "" {
			t.Errorf("POST %s = %d %q, want a clear 400", url, code, body)
		}
	}
}

func TestFactorOnlyRuntimeAllRegisteredGetRoutesSurvive(t *testing.T) {
	h := factorOnlyHubDeps(t)
	app := fiber.New(fiber.Config{ErrorHandler: base.ErrHandler})
	RegisterBotAPI(app, *h.deps)
	seen := 0
	for _, route := range app.Stack()[0] {
		if route.Method != http.MethodGet || strings.Contains(route.Path, ":") || route.Path == "/bot_info" || strings.HasPrefix(route.Path, "/kline/") || route.Path == "/" {
			continue
		}
		seen++
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("GET %s panicked: %v", route.Path, r)
				}
			}()
			if code, body := doReq(t, app, http.MethodGet, route.Path, ""); code >= 500 {
				t.Errorf("GET %s = %d %s", route.Path, code, body)
			}
		}()
	}
	if seen < 10 {
		t.Fatalf("only %d GET routes exercised", seen)
	}
}
