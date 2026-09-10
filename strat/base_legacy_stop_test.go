package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestNormalizeEntryStopConvertsCrossedLegacyStopToLimit(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
	})
	core.BackTestMode = true
	config.Data.BTLegacyIntrabar = true
	config.Data.BTStrict = true

	for _, test := range []struct {
		name                string
		short               bool
		stop, wantPrice     float64
		wantStop, wantLimit float64
	}{
		{name: "long crossed", stop: 90, wantPrice: 90, wantLimit: 90},
		{name: "short crossed", short: true, stop: 110, wantPrice: 110, wantLimit: 110},
		{name: "long pending", stop: 110, wantPrice: 100, wantStop: 110},
		{name: "short pending", short: true, stop: 90, wantPrice: 100, wantStop: 90},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := &EnterReq{Short: test.short, Stop: test.stop}
			price := normalizeEntryStop(req, 100, false)
			if price != test.wantPrice || req.Stop != test.wantStop || req.Limit != test.wantLimit {
				t.Fatalf("price=%v stop=%v limit=%v, want price=%v stop=%v limit=%v",
					price, req.Stop, req.Limit, test.wantPrice, test.wantStop, test.wantLimit)
			}
		})
	}
}
