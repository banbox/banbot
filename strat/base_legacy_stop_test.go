package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func enableStrictHistoricalStopTest(t *testing.T) {
	t.Helper()
	originalMode, originalData, originalCoverage := core.BackTestMode, config.Data, config.HistoricalCoverage
	t.Cleanup(func() {
		core.BackTestMode, config.Data, config.HistoricalCoverage = originalMode, originalData, originalCoverage
	})
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.Data.BTLegacyIntrabar = true
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 2,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 0, StopMS: 2}}},
		},
	}
}

func TestNormalizeEntryStopPreservesLegacyBacktestStop(t *testing.T) {
	enableStrictHistoricalStopTest(t)
	for _, test := range []struct {
		name string
		req  *EnterReq
	}{
		{"long", &EnterReq{Stop: 90}},
		{"short", &EnterReq{Short: true, Stop: 110}},
	} {
		t.Run("legacy_"+test.name, func(t *testing.T) {
			stop := test.req.Stop
			if price := normalizeEntryStop(test.req, 100, false); price != 100 || test.req.Stop != stop || test.req.Limit != 0 {
				t.Fatalf("legacy stop changed: price=%v stop=%v limit=%v", price, test.req.Stop, test.req.Limit)
			}
		})
	}

	config.Data.BTLegacyIntrabar = false
	for _, test := range []struct {
		name string
		req  *EnterReq
	}{
		{"long", &EnterReq{Stop: 90}},
		{"short", &EnterReq{Short: true, Stop: 110}},
	} {
		t.Run("current_"+test.name, func(t *testing.T) {
			stop := test.req.Stop
			if price := normalizeEntryStop(test.req, 100, false); price != stop || test.req.Stop != 0 || test.req.Limit != stop {
				t.Fatalf("current stop normalization changed: price=%v stop=%v limit=%v", price, test.req.Stop, test.req.Limit)
			}
		})
	}
}

func TestNormalizeEntryStopRejectsIncompleteHistoricalReplay(t *testing.T) {
	enableStrictHistoricalStopTest(t)
	strictCoverage := config.HistoricalCoverage
	for _, test := range []struct {
		name   string
		change func()
	}{
		{name: "non-strict", change: func() { config.Data.BTStrict = false }},
		{name: "download-enabled", change: func() { config.Data.BTNoKlineDownload = false }},
		{name: "missing coverage", change: func() { config.HistoricalCoverage = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			core.BackTestMode = true
			config.Data = config.Config{BTLegacyIntrabar: true, BTStrict: true, BTNoKlineDownload: true}
			config.HistoricalCoverage = strictCoverage
			test.change()
			req := &EnterReq{Stop: 90}
			if price := normalizeEntryStop(req, 100, false); price != 90 || req.Stop != 0 || req.Limit != 90 {
				t.Fatalf("current stop semantics changed: price=%v stop=%v limit=%v", price, req.Stop, req.Limit)
			}
		})
	}
}
