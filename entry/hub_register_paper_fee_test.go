package entry

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
)

// registerRobotHubAccount must put the configured taker fee into the paper
// account request of a pure time-series dry-run robot (otherwise fills are free).
func TestRegisterRobotHubAccountCarriesConfiguredTakerFeeForTimeSeriesDryRun(t *testing.T) {
	var mu sync.Mutex
	var captured *HubAccountConfig
	hub, err := execution.NewHub(func(_ context.Context, reg execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		// Plain decode: the hub's strict validation needs instruments and
		// capital, which are irrelevant to the fee under test.
		var decoded HubAccountConfig
		decodeErr := json.Unmarshal(reg.Configuration, &decoded)
		mu.Lock()
		if decodeErr == nil {
			captured = &decoded
		}
		mu.Unlock()
		// Stop after capturing: building a real account is not under test.
		return execution.HubAccountBinding{}, errors.New("capture only")
	})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "hub-fee-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	server, err := execution.ListenHubUnix(sock, hub)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	client := execution.NewHubClient(sock)
	t.Cleanup(func() { client.Close() })

	cfg := &config.Config{Name: "t", Env: core.RunEnvDryRun, MarketType: "linear", Exchange: &config.ExchangeConfig{
		Name:  "binance",
		Items: map[string]map[string]any{"binance": {"fees": map[string]any{"linear": map[string]any{"taker": 0.0005}}}},
	}}
	dir2 := t.TempDir()
	snapshot := config.NewSnapshotWithDirs(cfg, dir2, dir2)
	key := execution.AccountKey{VenueSessionIdentity: "binance/linear/dry_run", Account: "a", SettlementDomain: "USDT"}
	configs := []runner.Config{{AccountID: "a", InitialNAV: 1000}}
	configs[0].Manifest.Currency = "USDT"

	_, _ = registerRobotHubAccount(context.Background(), client, snapshot, configs, FactorLiveBinding{Account: key})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("hub never received a decodable registration")
	}
	if got, _ := captured.PaperFee.Float64(); got != 0.0005 {
		t.Fatalf("PaperFee = %s, want 0.0005", captured.PaperFee)
	}
	if got, _ := captured.PaperCash.Float64(); got != 1000 {
		t.Fatalf("PaperCash = %s, want 1000", captured.PaperCash)
	}
}
