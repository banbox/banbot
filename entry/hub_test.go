package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
)

func TestHubCommandAndSpiderCompatibility(t *testing.T) {
	for _, name := range []string{"hub", "spider"} {
		root := NewRootCommand()
		command, _, err := root.Find([]string{name})
		if err != nil || command.Name() != "hub" || command.Flags().Lookup("execution-socket") == nil {
			t.Fatalf("%s does not select hub: %v", name, err)
		}
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs([]string{name, "--help"})
		if err := root.Execute(); err != nil || !strings.Contains(out.String(), "--execution-socket") {
			t.Fatal("hub help", out.String(), err)
		}
	}
}

func TestHubPublicSnapshotDoesNotInheritPrivateAccounts(t *testing.T) {
	cfg := &config.Config{Env: core.RunEnvProd, Exchange: &config.ExchangeConfig{Name: "example", Items: map[string]map[string]any{"example": {"api_key": "secret"}}}, Accounts: map[string]*config.AccountConfig{"private": {}}, RunPolicy: []*config.RunPolicyConfig{{Name: "robot"}}}
	original := config.NewSnapshotWithDirs(cfg, t.TempDir(), "strategies")
	public, err := publicHubSnapshot(original)
	if err != nil {
		t.Fatal(err)
	}
	if len(public.View().Accounts) != 0 || len(public.View().Exchange.Items) != 0 || len(public.View().RunPolicy) != 0 {
		t.Fatal("public miner inherited private configuration")
	}
	if original.View().Accounts["private"] == nil || len(original.View().Exchange.Items) == 0 || len(original.View().RunPolicy) == 0 {
		t.Fatal("public snapshot mutated robot configuration")
	}
	if public.DataDir != original.DataDir || public.StrategyDir != original.StrategyDir {
		t.Fatal("lost storage identity")
	}
	for _, env := range []string{"backtest", "test", ""} {
		cfg.Env = env
		if _, err := publicHubSnapshot(config.NewSnapshotWithDirs(cfg, "", "")); err == nil {
			t.Fatalf("hub accepted non-live environment %q", env)
		}
	}
}

func hubConfigFixture() (execution.HubAccountRegistration, HubAccountConfig) {
	d := decimal.RequireFromString
	c := HubAccountConfig{Exchange: "example", Instruments: []execution.BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: d("0.01"), PriceTick: d("0.1"), ContractSize: d("1"), MoneyScale: 8}}}, Capital: map[execution.StrategyID]decimal.Decimal{"s": d("100")}, PaperCash: d("1000"), Risk: execution.PortfolioRisk{MarginRate: d("0.1"), MaxAccountMargin: d("100"), MaxVirtualGross: d("1000"), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"s": d("500")}}}
	return execution.HubAccountRegistration{Account: execution.AccountKey{VenueSessionIdentity: "example/linear/dry_run", Account: "default", SettlementDomain: "USDT"}, Env: core.RunEnvDryRun}, c
}

func TestHubAccountConfigurationIsExplicitAndEnvironmentIsolated(t *testing.T) {
	for name, mutate := range map[string]func(*execution.HubAccountRegistration, *HubAccountConfig){
		"valid":             func(_ *execution.HubAccountRegistration, _ *HubAccountConfig) {},
		"wrong-environment": func(r *execution.HubAccountRegistration, _ *HubAccountConfig) { r.Env = "backtest" },
		"wrong-identity": func(r *execution.HubAccountRegistration, _ *HubAccountConfig) {
			r.Account.VenueSessionIdentity = "example/linear/prod"
		},
		"paper-with-credentials": func(_ *execution.HubAccountRegistration, c *HubAccountConfig) { c.Credential = &config.AccountConfig{} },
		"paper-with-takeover":    func(_ *execution.HubAccountRegistration, c *HubAccountConfig) { c.Takeover = "s" },
		"missing-capital":        func(_ *execution.HubAccountRegistration, c *HubAccountConfig) { c.Capital = nil },
		"missing-risk":           func(_ *execution.HubAccountRegistration, c *HubAccountConfig) { c.Risk.StrategyGrossLimits = nil },
		"duplicate-instrument": func(_ *execution.HubAccountRegistration, c *HubAccountConfig) {
			c.Instruments = append(c.Instruments, c.Instruments[0])
		},
		"production-without-credentials": func(r *execution.HubAccountRegistration, c *HubAccountConfig) {
			r.Env, r.Account.VenueSessionIdentity = core.RunEnvProd, "example/linear/prod"
			c.PaperCash = decimal.Zero
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := hubConfigFixture()
			mutate(&r, &c)
			r.Configuration, _ = json.Marshal(c)
			_, err := decodeHubAccountConfig(r)
			if (err == nil) != (name == "valid") {
				t.Fatalf("unexpected validation: %v", err)
			}
		})
	}
}

func TestHubAccountRejectsUnknownAndTrailingConfiguration(t *testing.T) {
	r, c := hubConfigFixture()
	body, _ := json.Marshal(c)
	for _, input := range []string{string(body) + " {}", strings.TrimSuffix(string(body), "}") + `,"StorePath":"/tmp/untrusted"}`, `null`} {
		r.Configuration = json.RawMessage(input)
		if _, err := decodeHubAccountConfig(r); err == nil {
			t.Fatal("accepted noncanonical or unknown configuration")
		}
	}
}

func TestHubProductionRequiresSharedLeaseDomainBeforeSDKCreation(t *testing.T) {
	r, c := hubConfigFixture()
	r.Env, r.Account.VenueSessionIdentity = core.RunEnvProd, "example/linear/prod"
	c.PaperCash = decimal.Zero
	c.Credential = &config.AccountConfig{Exchanges: map[string]*config.ExgApiSecrets{"example": {Prod: &config.ApiSecretConfig{APIKey: "request-key", APISecret: "request-secret"}}}}
	r.Configuration, _ = json.Marshal(c)
	_, err := newHubAccountFactory(t.TempDir(), "", true)(context.Background(), r)
	if err == nil || !strings.Contains(err.Error(), "sender lease directory") || strings.Contains(err.Error(), "request-secret") {
		t.Fatalf("missing cutover fence before SDK construction: %v", err)
	}
}

func TestHubInstrumentMetadataIncludesSettlementAndValuation(t *testing.T) {
	_, c := hubConfigFixture()
	declared := c.Instruments[0].Instrument
	expected := declared.Clone()
	expected.Version = "sdk-metadata-version"
	if !hubInstrumentMatches(declared, expected) {
		t.Fatal("declared version is not an SDK metadata version")
	}
	expected.SettlementCurrency = "USDC"
	if hubInstrumentMatches(declared, expected) {
		t.Fatal("cross-settlement declaration accepted")
	}
	expected = declared.Clone()
	expected.Valuation = "spot"
	if hubInstrumentMatches(declared, expected) {
		t.Fatal("incompatible valuation accepted")
	}
}
