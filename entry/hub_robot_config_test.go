package entry

import (
	"context"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type robotPublicExchange struct {
	banexg.BanExchange
	market   *banexg.Market
	currency *banexg.Currency
}

func (e *robotPublicExchange) GetMarket(string) (*banexg.Market, *errs.Error) { return e.market, nil }
func (e *robotPublicExchange) GetExg() *banexg.Exchange {
	exchange := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{}}
	exchange.CurrenciesByCode = map[string]*banexg.Currency{"USDT": e.currency}
	return exchange
}

func TestRobotPublicBindingHasNoPrivateExecutionAndPreservesFields(t *testing.T) {
	market := &banexg.Market{ID: "BTCUSDTPERP", Symbol: "BTC/USDT:USDT", Contract: true, Linear: true, Swap: true, Settle: "USDT", ContractSize: 1, Precision: &banexg.Precision{Amount: 0.001, ModeAmount: banexg.PrecModeTickSize, Price: 2, ModePrice: banexg.PrecModeDecimalPlace}, Limits: &banexg.MarketLimits{Amount: &banexg.LimitRange{Min: 0.001}, Cost: &banexg.LimitRange{Min: 5}}}
	currency := &banexg.Currency{Code: "USDT", Precision: 8, PrecMode: banexg.PrecModeDecimalPlace}
	exchange := &robotPublicExchange{market: market, currency: currency}
	unit, err := execution.InstrumentFromBanexgMarket("BTC", market, currency)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := config.NewSnapshotWithDirs(&config.Config{Env: "dry_run", MarketType: "linear", Exchange: &config.ExchangeConfig{Name: "fixture", Items: map[string]map[string]any{"private": {"secret": "must-not-leak"}}}, Accounts: map[string]*config.AccountConfig{"a": {}}}, t.TempDir(), "")
	public, err := robotPublicSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if public.View().Accounts != nil || public.View().Exchange.Items != nil || snapshot.View().Accounts == nil || snapshot.View().Exchange.Items == nil {
		t.Fatal("credentials retained or original snapshot mutated")
	}
	c := runner.Config{AccountID: "a", Manifest: runner.Config{}.Manifest, Snapshot: factor.SnapshotSpec{SIDMap: map[int32]string{1: market.Symbol}, SourceVersions: map[string]string{"custom": "v1"}}, Execution: runner.ExecutionConfig{Instruments: map[int32]execution.Instrument{1: unit}}}
	c.Manifest.Currency = "USDT"
	binding, err := robotPublicBinding(context.Background(), exchange, public, c)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Transport != nil || binding.VerifyFunding != nil || len(binding.Sources) != 0 || binding.Close != nil {
		t.Fatal("robot public binding installed private capabilities")
	}
	row := &orm.DataSeries{Source: "custom", TimeFrame: "event", Sid: 1, TimeMS: 100, Values: map[string]any{"integer": int64(9007199254740993), "null": nil, "custom": "kept"}}
	record, err := binding.Record(row, 101)
	if err != nil {
		t.Fatal(err)
	}
	if record.Series.Values["integer"] != int64(9007199254740993) || record.Series.Values["null"] != nil || record.Series.Values["custom"] != "kept" {
		t.Fatal("custom field types or NULL lost")
	}
	row.Values["custom"] = "changed"
	if record.Series.Values["custom"] != "kept" {
		t.Fatal("record aliases source map")
	}
}

func TestFactorDefaultHubSelectorProdAndDryRun(t *testing.T) {
	_, path := factorYAMLFixture(t)
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	u := spec.Config()
	configs := []runner.Config{{AccountID: "default"}, {AccountID: "second"}}
	for _, env := range []string{"prod", "dry_run"} {
		u.Root.Env = env
		for _, provider := range []string{"", "banexg", "hub"} {
			u.Execution["live_provider"] = provider
			hub, err := factorLiveUsesHub(u, configs, "")
			if err != nil || !hub {
				t.Fatalf("%s/%s default bypassed Hub: %v %v", env, provider, hub, err)
			}
		}
		u.Execution["live_provider"] = "embedded-test"
		hub, err := factorLiveUsesHub(u, configs, "")
		if err != nil || hub {
			t.Fatal("explicit embedding provider not preserved", hub, err)
		}
		u.Execution["live_provider"] = "hub"
		u.AccountExecution["second"] = map[string]any{"live_provider": "embedded-test"}
		if _, err := factorLiveUsesHub(u, configs, ""); err == nil {
			t.Fatal("mixed Hub/embedded declaration could fall back to local sender")
		}
		delete(u.AccountExecution, "second")
	}
}
