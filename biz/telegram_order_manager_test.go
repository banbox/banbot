package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/orm/ormo"
	"gopkg.in/yaml.v3"
)

func TestBuildTelegramOrderInfoCalculatesLivePnL(t *testing.T) {
	tests := []struct {
		name       string
		short      bool
		markPrice  float64
		wantProfit float64
		wantRate   float64
	}{
		{name: "long", markPrice: 110, wantProfit: 19, wantRate: 0.095},
		{name: "short", short: true, markPrice: 90, wantProfit: 19, wantRate: 0.095},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			order := &ormo.InOutOrder{
				IOrder: &ormo.IOrder{
					ID:       1,
					Symbol:   "BTC/USDT:USDT",
					Short:    test.short,
					Status:   ormo.InOutStatusFullEnter,
					EnterTag: "test",
				},
				Enter: &ormo.ExOrder{Average: 100, Filled: 2, FeeQuote: 1},
			}
			info := buildTelegramOrderInfo(order, "default", test.markPrice)
			if !info.ProfitValid {
				t.Fatal("calculated PnL should be valid")
			}
			if math.Abs(info.Profit-test.wantProfit) > 1e-12 {
				t.Fatalf("profit = %v, want %v", info.Profit, test.wantProfit)
			}
			if math.Abs(info.ProfitRate-test.wantRate) > 1e-12 {
				t.Fatalf("profit rate = %v, want %v", info.ProfitRate, test.wantRate)
			}
		})
	}
}

func TestBuildTelegramOrderInfoMarksMissingPriceInvalid(t *testing.T) {
	order := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{ID: 1, Symbol: "BTC/USDT:USDT", Status: ormo.InOutStatusFullEnter},
		Enter:  &ormo.ExOrder{Average: 100, Filled: 2},
	}
	if info := buildTelegramOrderInfo(order, "default", -1); info.ProfitValid {
		t.Fatal("PnL without a current price must remain unavailable")
	}
}

func TestEmbeddedConfigDeclaresDisplayLanguage(t *testing.T) {
	var cfg map[string]interface{}
	if err := yaml.Unmarshal(configData, &cfg); err != nil {
		t.Fatalf("parse embedded config: %v", err)
	}
	if got := cfg["show_lang_code"]; got != "en-US" {
		t.Fatalf("show_lang_code = %v, want en-US", got)
	}
}
