package exg

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestNewForRuntimeUsesSnapshotExchangeAndEnvironmentCredentials(t *testing.T) {
	oldExchange, oldEnv := config.Exchange, core.RunEnv
	t.Cleanup(func() {
		config.Exchange, core.RunEnv = oldExchange, oldEnv
	})
	config.Exchange = &config.ExchangeConfig{Name: "legacy"}
	core.RunEnv = core.RunEnvProd

	snapshot := config.NewSnapshotWithDirs(&config.Config{
		Env:        core.RunEnvTest,
		Exchange:   &config.ExchangeConfig{Name: "binance"},
		MarketType: "spot",
		Accounts: map[string]*config.AccountConfig{
			"default": {Exchanges: map[string]*config.ExgApiSecrets{
				"legacy":  {Prod: &config.ApiSecretConfig{APIKey: "legacy-prod"}},
				"binance": {Prod: &config.ApiSecretConfig{APIKey: "runtime-prod"}, Test: &config.ApiSecretConfig{APIKey: "runtime-test"}},
			}},
		},
	}, "", "")
	exchange, err := NewForRuntime(snapshot, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exchange.Close() })

	account := exchange.GetExg().Accounts["default"]
	if account == nil || account.Creds == nil {
		t.Fatalf("runtime exchange account credentials missing: %#v", account)
	}
	if account.Creds.ApiKey != "runtime-test" {
		t.Fatalf("runtime credentials = %q, want runtime-test", account.Creds.ApiKey)
	}
}
