package live

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
)

type apiExchangeStub struct{ banexg.BanExchange }

func (*apiExchangeStub) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "runtime-api", MarketType: "spot"}
}

func TestEnabledRuntimeAPIStartsWithOwnedRoutesAndStops(t *testing.T) {
	deps := testHandlerDeps(t)
	dataDir := t.TempDir()
	uiDir := filepath.Join(dataDir, "uidist")
	if err := os.MkdirAll(uiDir, 0755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"index.html": "runtime UI", "version.txt": core.UIVersion} {
		if err := os.WriteFile(filepath.Join(uiDir, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reservation.Addr().(*net.TCPAddr).Port
	_ = reservation.Close()
	deps.Config = config.NewSnapshotWithDirs(&config.Config{APIServer: &config.APIServerConfig{Enable: true, BindIPAddr: "127.0.0.1", Port: port}}, dataDir, "")
	deps.Core.ExgName, deps.Core.Market = "runtime-api", "spot"
	deps.Symbols = orm.NewSymbolStateWithIdentity("runtime-api", "spot")
	if err := deps.Symbols.SetExSymbols([]*orm.ExSymbol{{ID: 10, Exchange: "runtime-api", Market: "spot", Symbol: "OWNED/USDT"}}); err != nil {
		t.Fatal(err)
	}
	deps.Storage = orm.NewStorage(nil, false, "runtime-api-test")
	deps.Catalog = data.NewDataSourceCatalog()
	deps.Exchange = &apiExchangeStub{}
	deps.Strategies = strat.NewState()
	deps.Market = com.NewMarketStateWithExchange("runtime-api", deps.Exchange)
	deps.Trading = biz.NewTradingState()
	server, runErr := StartApiWithRuntimeDeps(nil, *deps)
	if runErr != nil {
		t.Fatal(runErr)
	}
	if server == nil {
		t.Fatal("enabled runtime API did not start")
	}
	defer func() { server.Stop(); server.Join() }()
	client := &http.Client{Timeout: time.Second}
	url := "http://" + net.JoinHostPort("127.0.0.1", fmt.Sprint(port)) + "/api/kline/symbols"
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get(url)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "OWNED/USDT") {
				t.Fatalf("owned symbols response: %d %s", response.StatusCode, body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	server.Stop()
	joined := make(chan struct{})
	go func() { server.Join(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(3 * time.Second):
		t.Fatal("runtime API did not finish shutdown")
	}
}
