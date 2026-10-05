package live

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/gofiber/fiber/v2"
)

func TestRuntimeLogAPIUsesOwnedFile(t *testing.T) {
	first, second := testHandlerDeps(t), testHandlerDeps(t)
	for index, deps := range []*biz.RuntimeDeps{first, second} {
		content := fmt.Sprintf("runtime-log-%d", index)
		deps.Core.LogFile = filepath.Join(t.TempDir(), "owned.log")
		if err := os.WriteFile(deps.Core.LogFile, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		app := fiber.New()
		app.Get("/log", newAPIHandlers(deps).getLog)
		response, err := app.Test(httptest.NewRequest(http.MethodGet, "/log?end=-1&limit=20480", nil))
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			Data string `json:"data"`
		}
		err = json.NewDecoder(response.Body).Decode(&result)
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || result.Data != content {
			t.Fatalf("runtime log response = %+v, status %d, error %v", result, response.StatusCode, err)
		}
		deps.Core.LogFile = ""
		response, err = app.Test(httptest.NewRequest(http.MethodGet, "/log", nil))
		if err != nil {
			t.Fatal(err)
		}
		var missing struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		}
		err = json.NewDecoder(response.Body).Decode(&missing)
		response.Body.Close()
		if err != nil || missing.Code != 400 || missing.Msg != "no log file" {
			t.Fatalf("missing log response: %+v, %v", missing, err)
		}
	}
}

func TestRegApiBizLegacyRegistersRoutes(t *testing.T) {
	app := fiber.New()
	regApiBiz(app)

	for _, path := range []string{"/version", "/balance", "/kline/data_sources", "/kline/series"} {
		found := false
		for _, route := range app.Stack()[0] {
			if route.Path == path {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("GET route %s missing", path)
		}
	}
}

func TestAPIHandlersUseOwnedRuntimeState(t *testing.T) {
	first := testHandlerDeps(t)
	second := testHandlerDeps(t)
	firstHandler := newAPIHandlers(first)
	secondHandler := newAPIHandlers(second)

	firstWallet := firstHandler.wallet("acc")
	secondWallet := secondHandler.wallet("acc")
	if firstWallet == secondWallet {
		t.Fatal("handlers shared runtime wallet")
	}
	firstWallet.Account = "first-runtime"
	if secondWallet.Account == firstWallet.Account {
		t.Fatal("wallet mutation crossed runtime boundary")
	}

	firstOrders, firstLock := firstHandler.openOrders("acc")
	secondOrders, secondLock := secondHandler.openOrders("acc")
	if firstOrders == nil || firstLock == nil || secondOrders == nil || secondLock == nil {
		t.Fatal("runtime order state was not resolved")
	}
	firstOrders[1] = &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1}}
	if _, ok := secondOrders[1]; ok {
		t.Fatal("handlers shared runtime order state")
	}
}

func TestAPIHandlersStrategyVersionsAreSnapshot(t *testing.T) {
	deps := testHandlerDeps(t)
	deps.Strategies = strat.NewState()
	deps.Strategies.SetVersion("owned", 1)
	handler := newAPIHandlers(deps)

	versions := handler.strategyVersions()
	versions["caller"] = 1
	if _, ok := deps.Strategies.Version("caller"); ok {
		t.Fatal("strategy API returned the mutable version registry")
	}
}

func TestAPIHandlersRemoteSwitchUsesOwnedCore(t *testing.T) {
	deps := testHandlerDeps(t)
	handler := newAPIHandlers(deps)
	core.SetLegacyNoEnterUntil("acc", 0)
	t.Cleanup(func() { core.SetLegacyNoEnterUntil("acc", 0) })

	const untilMS = int64(123456)
	_, err := handler.remote.Run(biz.RemoteCommand{
		Source: "test", Actor: "test", Account: "acc",
		Action: biz.RemoteActionTradingSwitch, UntilMS: untilMS,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := deps.Core.NoEnterUntilFor("acc"); got != untilMS {
		t.Fatalf("runtime no-enter-until = %d, want %d", got, untilMS)
	}
	if got, _ := core.LegacyNoEnterUntilFor("acc"); got != 0 {
		t.Fatalf("legacy no-enter-until = %d, want unchanged", got)
	}
}

func TestAPIHandlersLoginUsesOwnedAccounts(t *testing.T) {
	deps := testHandlerDeps(t)
	deps.Config = config.NewSnapshot(&config.Config{
		Name:      "runtime-only",
		APIServer: &config.APIServerConfig{JWTSecretKey: "runtime-secret"},
		Accounts: map[string]*config.AccountConfig{
			"runtime-account": {APIServer: &config.AccPwdRole{Pwd: "runtime-password", Role: "admin"}},
		},
	})
	handler := newAPIHandlers(deps)
	users := handler.apiUsers()
	if len(users) != 1 || users[0].Username != "runtime-account" {
		t.Fatalf("runtime users = %#v, want only runtime account", users)
	}

	app := fiber.New()
	handler.regApiPub(app)
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"runtime-account","password":"runtime-password"}`))
	req.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d", resp.StatusCode)
	}
	var body struct {
		Name     string            `json:"name"`
		Accounts map[string]string `json:"accounts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Name != "runtime-only" || body.Accounts["runtime-account"] != "admin" || len(body.Accounts) != 1 {
		t.Fatalf("login response = %#v", body)
	}
}

func TestAPIHandlersAuthRejectsUnknownUserAndUnauthorizedAccount(t *testing.T) {
	deps := testHandlerDeps(t)
	deps.Config = config.NewSnapshot(&config.Config{
		APIServer: &config.APIServerConfig{JWTSecretKey: "runtime-secret"},
		Accounts: map[string]*config.AccountConfig{
			"allowed": {APIServer: &config.AccPwdRole{Pwd: "password", Role: "admin"}},
		},
	})
	handler := newAPIHandlers(deps)
	app := fiber.New()
	app.Get("/balance", handler.authMiddleware("runtime-secret"), handler.getBalance)

	validToken, err := CreateAuthToken("allowed", "runtime-secret", 1)
	if err != nil {
		t.Fatal(err)
	}
	unknownToken, err := CreateAuthToken("removed-user", "runtime-secret", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, token, account string
		status               int
	}{
		{name: "authorized", token: validToken, account: "allowed", status: http.StatusOK},
		{name: "unknown user", token: unknownToken, account: "allowed", status: http.StatusUnauthorized},
		{name: "unauthorized account", token: validToken, account: "other", status: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/balance", nil)
			req.Header.Set("X-Authorization", "Bearer "+test.token)
			req.Header.Set("X-Account", test.account)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != test.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, test.status)
			}
		})
	}
}

func TestStartAPIWithDepsRejectsMissingRuntimeState(t *testing.T) {
	deps := testHandlerDeps(t)
	deps.Config = config.NewSnapshot(&config.Config{APIServer: &config.APIServerConfig{Enable: true}})
	if server, err := startApiWithDeps(nil, deps); err == nil || server != nil {
		t.Fatalf("startApiWithDeps = %v, %v; want missing dependency error", server, err)
	}
}

func testHandlerDeps(t *testing.T) *biz.RuntimeDeps {
	t.Helper()
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	accounts := map[string]*config.AccountConfig{"acc": {}}
	return &biz.RuntimeDeps{
		Core:     state,
		Clock:    btime.NewClockState(true, nil),
		Orders:   ormo.NewOrderState(),
		Trading:  biz.NewTradingState(),
		Config:   config.NewSnapshot(&config.Config{Accounts: accounts}),
		Accounts: accounts,
	}
}
