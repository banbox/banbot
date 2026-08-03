package opt

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestSectionPairsUsesCanonicalOrderInStrictBacktest(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	t.Cleanup(func() {
		core.BackTestMode = oldMode
		config.Data = oldData
	})
	core.BackTestMode = true
	config.Data.BTStrict = true

	pairs := map[string]bool{
		"SOL/USDT": true,
		"BTC/USDT": true,
		"ETH/USDT": true,
	}
	want := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	for range 32 {
		if got := sectionPairs(pairs); !slices.Equal(got, want) {
			t.Fatalf("section pairs = %v, want %v", got, want)
		}
	}
}
