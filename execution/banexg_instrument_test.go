package execution

import (
	"math"
	"testing"

	"github.com/banbox/banexg"
)

func TestBanexgNormalizedInstrumentPreservesUnitsAndVersion(t *testing.T) {
	market := &banexg.Market{ID: "BTCUSDTPERP", Symbol: "BTC/USDT:USDT", Contract: true, Linear: true, Swap: true, Settle: "USDT", ContractSize: 2, Precision: &banexg.Precision{Amount: 0.001, ModeAmount: banexg.PrecModeTickSize, Price: 2, ModePrice: banexg.PrecModeDecimalPlace}, Limits: &banexg.MarketLimits{Amount: &banexg.LimitRange{Min: 0.0015}, Cost: &banexg.LimitRange{Min: 5}}}
	currency := &banexg.Currency{Code: "USDT", Precision: 8, PrecMode: banexg.PrecModeDecimalPlace}
	instrument, err := InstrumentFromBanexgMarket("BTC", market, currency)
	if err != nil || !instrument.QuantityStep.Equal(intentPrice("0.001")) || !instrument.PriceTick.Equal(intentPrice("0.01")) || !instrument.ContractSize.Equal(intentPrice("2")) || instrument.MinSteps != 2 || instrument.MoneyScale != 8 || !instrument.MinNotional.Equal(intentPrice("5")) {
		t.Fatal(instrument, err)
	}
	again, _ := InstrumentFromBanexgMarket("BTC", market, currency)
	if again.Version != instrument.Version {
		t.Fatal("identical normalized metadata changed version")
	}
	market.ContractSize = 3
	changed, _ := InstrumentFromBanexgMarket("BTC", market, currency)
	if changed.Version == instrument.Version {
		t.Fatal("unit change retained metadata version")
	}
	for _, failure := range []string{"significant", "missing-mode", "nonfinite", "spot", "inverse", "dated", "settlement", "money-tick", "missing-limits"} {
		t.Run(failure, func(t *testing.T) {
			bad, precision, limits, money := *market, *market.Precision, *market.Limits, *currency
			bad.Precision, bad.Limits = &precision, &limits
			switch failure {
			case "significant":
				precision.ModeAmount = banexg.PrecModeSignifDigits
			case "missing-mode":
				precision.ModePrice = 0
			case "nonfinite":
				precision.Amount = math.Inf(1)
			case "spot":
				bad.Spot = true
			case "inverse":
				bad.Inverse = true
			case "dated":
				bad.Expiry = 1
			case "settlement":
				money.Code = "USD"
			case "money-tick":
				money.Precision, money.PrecMode = 0.05, banexg.PrecModeTickSize
			case "missing-limits":
				bad.Limits = nil
			}
			if _, err := InstrumentFromBanexgMarket("BTC", &bad, &money); err == nil {
				t.Fatal("unsupported/incomplete normalized metadata guessed")
			}
		})
	}
}
