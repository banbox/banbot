package entry

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
)

// A pure time-series dry-run account has no factor cost model, so its paper
// fee must come from the exchange fee configuration instead of being zero.
func TestConfiguredTakerFeeForTimeSeriesDryRun(t *testing.T) {
	fees := func(v any) *config.Config {
		return &config.Config{MarketType: "linear", Exchange: &config.ExchangeConfig{Name: "binance", Items: map[string]map[string]any{"binance": {"fees": v}}}}
	}
	for name, tc := range map[string]struct {
		cfg  *config.Config
		want float64
		bad  bool
	}{
		"configured":        {fees(map[string]any{"linear": map[string]any{"taker": 0.0005, "maker": 0.0002}}), 0.0005, false},
		"other market only": {fees(map[string]any{"spot": map[string]any{"taker": 0.001}}), 0, false},
		"no fees section":   {&config.Config{MarketType: "linear", Exchange: &config.ExchangeConfig{Name: "binance"}}, 0, false},
		"no exchange":       {&config.Config{MarketType: "linear"}, 0, false},
		"negative":          {fees(map[string]any{"linear": map[string]any{"taker": -0.1}}), 0, true},
		"not a rate":        {fees(map[string]any{"linear": map[string]any{"taker": 1.5}}), 0, true},
		"malformed":         {fees("0.0005"), 0, true},
	} {
		got, err := configuredTakerFee(tc.cfg)
		if (err != nil) != tc.bad || got != tc.want {
			t.Fatalf("%s: fee=%v err=%v", name, got, err)
		}
	}
	if !pureTimeSeriesAccount([]runner.Config{{AccountID: "a"}}) {
		t.Fatal("an account without factor strategies is a time-series account")
	}
	if pureTimeSeriesAccount([]runner.Config{{AccountID: "a"}, {AccountID: "a", StrategyID: "factor"}}) {
		t.Fatal("a factor strategy brings its own cost model")
	}
	if pureTimeSeriesAccount(nil) {
		t.Fatal("no configuration is not a time-series account")
	}
}
