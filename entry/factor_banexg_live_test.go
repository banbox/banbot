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
	"github.com/shopspring/decimal"
)

type factorBindingExchange struct {
	banexg.BanExchange
	market *banexg.Market
	base   *banexg.Exchange
}

func (e *factorBindingExchange) GetMarket(string) (*banexg.Market, *errs.Error) { return e.market, nil }
func (e *factorBindingExchange) GetExg() *banexg.Exchange                       { return e.base }
func (e *factorBindingExchange) FetchFundingCash(context.Context, string, string, int64, int64) ([]banexg.FundingCash, *errs.Error) {
	return nil, nil
}

func TestDefaultFactorLiveBindingRejectsIncorrectEconomicUnitsAndRetainsFields(t *testing.T) {
	market := &banexg.Market{ID: "asset", Symbol: "asset/USD:USD", Contract: true, Linear: true, Swap: true, Settle: "USD", ContractSize: 1, Precision: &banexg.Precision{Amount: .1, ModeAmount: banexg.PrecModeTickSize, Price: 1, ModePrice: banexg.PrecModeTickSize}, Limits: &banexg.MarketLimits{Amount: &banexg.LimitRange{Min: .1}, Cost: &banexg.LimitRange{Min: 5}}}
	exchange := &factorBindingExchange{market: market, base: &banexg.Exchange{ExgInfo: &banexg.ExgInfo{CurrenciesByCode: banexg.CurrencyMap{"USD": {Code: "USD", Precision: 3, PrecMode: banexg.PrecModeDecimalPlace}}}}}
	unit, err := execution.InstrumentFromBanexgMarket("asset", market, exchange.base.CurrenciesByCode["USD"])
	if err != nil {
		t.Fatal(err)
	}
	c := runner.Config{AccountID: "account", FundingSource: "funding", Snapshot: factor.SnapshotSpec{SIDMap: map[int32]string{1: market.Symbol}, SourceVersions: map[string]string{"kline": "v1", "funding": "v1"}}, Execution: runner.ExecutionConfig{Instruments: map[int32]execution.Instrument{1: unit}}}
	c.Manifest.Currency, c.Manifest.Costs.FundingPolicy = "USD", "required-stream"
	snapshot := config.NewSnapshot(&config.Config{Env: "prod", MarketType: "linear", Exchange: &config.ExchangeConfig{Name: "venue"}})
	factory, err := factorLiveFactory("")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"multiplier", "step", "tick", "minimum"} {
		bad := unit
		switch kind {
		case "multiplier":
			bad.ContractSize = decimal.RequireFromString("0.1")
		case "step":
			bad.QuantityStep = decimal.RequireFromString("0.01")
		case "tick":
			bad.PriceTick = decimal.RequireFromString("0.1")
		case "minimum":
			bad.MinNotional = decimal.Zero
		}
		c.Execution.Instruments[1] = bad
		if _, err := factory(context.Background(), exchange, snapshot, c); err == nil {
			t.Fatal("unsafe economic unit accepted", kind)
		}
	}
	c.Execution.Instruments[1] = unit
	binding, err := factory(context.Background(), exchange, snapshot, c)
	if err != nil || len(binding.Sources) != 1 || !binding.BootstrapCapital {
		t.Fatal(binding, err)
	}
	values := map[string]any{"close": 100.0, "nullable": nil, "integer": int64(7), "text": "extra"}
	row, err := binding.Record(&orm.DataSeries{Source: "kline", Sid: 1, TimeMS: 100, EndMS: 200, TimeFrame: "1m", Closed: true, Values: values}, 201)
	if err != nil || row.EventTime != 200 || row.AvailableAt != 201 || row.Series.Values["integer"] != int64(7) || row.Series.Values["nullable"] != nil {
		t.Fatal("live mapper lost fields or visibility", row, err)
	}
	values["text"] = "changed"
	if row.Series.Values["text"] != "extra" {
		t.Fatal("live mapper retained mutable caller map")
	}
	snapshot = config.NewSnapshot(&config.Config{Env: "test", MarketType: "linear", Exchange: &config.ExchangeConfig{Name: "venue"}})
	if _, err := factory(context.Background(), exchange, snapshot, c); err == nil {
		t.Fatal("test environment admitted")
	}
}
