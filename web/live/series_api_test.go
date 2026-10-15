package live

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/web/base"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

func TestDashboardSeriesAliasesAuthenticateAccountsAndUseRuntimeCatalog(t *testing.T) {
	const secret = "series-test-secret"
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user": "reader"}).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"first_metric", "second_metric"} {
		deps := testHandlerDeps(t)
		deps.Config = config.NewSnapshot(&config.Config{APIServer: &config.APIServerConfig{Users: []*config.UserConfig{{Username: "reader", AccRoles: map[string]string{"acc": "reader"}}}}})
		deps.Core.ExgName, deps.Core.Market = "runtime-api", "spot"
		deps.Symbols = orm.NewSymbolStateWithIdentity("runtime-api", "spot")
		deps.Storage = orm.NewStorage(nil, false, source)
		deps.Exchange = &apiExchangeStub{}
		deps.Catalog = data.NewDataSourceCatalog()
		info := &orm.SeriesInfo{Name: source, TimeFrame: "1d", Binding: orm.SeriesBinding{Table: source, TimeColumn: "time", EndColumn: "end_ms", Fields: []orm.SeriesField{{Name: "label", Type: "string"}, {Name: "flag", Type: "bool"}}}}
		if err := deps.Catalog.RegisterFuncDataSource(info, func(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) { return nil, nil }, nil); err != nil {
			t.Fatal(err)
		}
		h := newAPIHandlers(deps)
		app := fiber.New(fiber.Config{ErrorHandler: base.ErrHandler})
		h.regApiBiz(app.Group("/api/bot", h.authMiddleware(secret)))
		for _, test := range []struct {
			name, path, account, bearer string
			status                      int
			contains                    string
		}{
			{"catalog", "/api/bot/kline/data_sources", "acc", token, http.StatusOK, source},
			{"missing token", "/api/bot/kline/data_sources", "acc", "", http.StatusUnauthorized, "missing token"},
			{"invalid token", "/api/bot/kline/data_sources", "acc", "invalid", http.StatusUnauthorized, "invalid token"},
			{"missing account", "/api/bot/kline/data_sources", "", token, http.StatusBadRequest, "X-Account"},
			{"foreign account", "/api/bot/kline/data_sources", "foreign", token, http.StatusForbidden, "unauthorized"},
			{"series foreign account", "/api/bot/kline/series?source=" + source + "&sid=991&timeframe=1d", "foreign", token, http.StatusForbidden, "unauthorized"},
			{"series owned symbols", "/api/bot/kline/series?source=" + source + "&sid=991&timeframe=1d", "acc", token, http.StatusInternalServerError, "symbol sid 991 not found"},
		} {
			t.Run(source+"/"+test.name, func(t *testing.T) {
				request := httptest.NewRequest(http.MethodGet, test.path, nil)
				request.Header.Set("X-Account", test.account)
				if test.bearer != "" {
					request.Header.Set("X-Authorization", "Bearer "+test.bearer)
				}
				response, err := app.Test(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil || response.StatusCode != test.status || !strings.Contains(string(body), test.contains) {
					t.Fatalf("status %d, body %s, error %v; want %d / %q", response.StatusCode, body, err, test.status, test.contains)
				}
				other := "first_metric"
				if source == other {
					other = "second_metric"
				}
				if strings.Contains(string(body), other) {
					t.Fatalf("foreign runtime source leaked: %s", body)
				}
			})
		}
	}
}
