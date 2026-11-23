package entry

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
)

// The config loader fills min_open_rate/margin_add_rate/account_pull_secs with
// their defaults, so a non-zero value alone must not be reported as ignored.
func TestIgnoredExecutionConfigKeysLoaderDefaults(t *testing.T) {
	loaded := func(mutate func(*config.Config)) *config.Config {
		cfg := &config.Config{Exchange: &config.ExchangeConfig{Name: "binance"}, StakeCurrency: []string{"USDT"}, MarketType: "spot"}
		if mutate != nil {
			mutate(cfg)
		}
		if err := cfg.NormalizeRuntime(); err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	base := loaded(nil)
	if base.MinOpenRate != 0.5 || base.MarginAddRate != 0.66 || base.AccountPullSecs != 60 {
		t.Fatalf("loader defaults changed (%v %v %v): update ignoredExecutionConfigKeys", base.MinOpenRate, base.MarginAddRate, base.AccountPullSecs)
	}
	if base.PutLimitSecs != 180 || base.OdBookTtl != 500 {
		t.Fatalf("loader defaults changed (%v %v): update ignoredExecutionConfigKeys", base.PutLimitSecs, base.OdBookTtl)
	}
	if got := ignoredExecutionConfigKeys(base); len(got) != 0 {
		t.Fatalf("loader defaults reported as ignored: %v", got)
	}
	tests := []struct {
		name   string
		mutate func(*config.Config)
		want   []string
	}{
		{"user min_open_rate", func(c *config.Config) { c.MinOpenRate = 0.8 }, []string{"min_open_rate"}},
		{"user margin_add_rate", func(c *config.Config) { c.MarginAddRate = 0.3 }, []string{"margin_add_rate"}},
		{"user account_pull_secs", func(c *config.Config) { c.AccountPullSecs = 30 }, []string{"account_pull_secs"}},
		{"user put_limit_secs", func(c *config.Config) { c.PutLimitSecs = 60 }, []string{"put_limit_secs"}},
		{"user odbook_ttl", func(c *config.Config) { c.OdBookTtl = 1000 }, []string{"odbook_ttl"}},
		{"explicit default values", func(c *config.Config) { c.MinOpenRate, c.MarginAddRate, c.AccountPullSecs = 0.5, 0.66, 60 }, nil},
		{"all three changed", func(c *config.Config) { c.MinOpenRate, c.MarginAddRate, c.AccountPullSecs = 0.1, 0.2, 5 }, []string{"min_open_rate", "margin_add_rate", "account_pull_secs"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ignoredExecutionConfigKeys(loaded(tt.mutate))
			slices.Sort(got)
			want := slices.Clone(tt.want)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("keys = %v, want %v", got, want)
			}
		})
	}
}
