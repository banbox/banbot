package orm

import (
	"testing"

	"github.com/banbox/banbot/config"
)

func TestStoredRetiredSymbolNeedsStoredDataInsideWindow(t *testing.T) {
	state := NewSymbolState()
	if err := state.SetExSymbols([]*ExSymbol{
		{ID: 1, Exchange: "binance", Market: "linear", Symbol: "MATIC/USDT:USDT", ListMs: 10, DelistMs: 99},
		{ID: 2, Exchange: "binance", Market: "linear", Symbol: "OLD/USDT:USDT", ListMs: 10, DelistMs: 1784737902136}, // synthetic marker
		{ID: 3, Exchange: "binance", Market: "linear", Symbol: "NODATA/USDT:USDT"},
	}); err != nil {
		t.Fatal(err)
	}
	ranges := map[int32]map[string][2]int64{
		1: {"1h": {100, 5000}},
		2: {"1d": {10, 900}, "1h": {10, 950}}, // ends before the window
	}
	old := klineRangeLookup
	defer func() { klineRangeLookup = old }()
	klineRangeLookup = func(_ *SymbolState, sid int32, tf string) (int64, int64, error) {
		r := ranges[sid][tf]
		return r[0], r[1], nil
	}
	cfg := &config.Config{TimeRange: &config.TimeTuple{StartMS: 1000, EndMS: 9000}}
	got, err := storedRetiredSymbol(state, "binance", "linear", "MATIC/USDT:USDT", cfg)
	if err != nil || got == nil || got.ID != 1 || got.ListMs != 10 || got.DelistMs != 99 {
		t.Fatalf("symbol with data inside the window not resolved: %+v %v", got, err)
	}
	if got == state.GetExSymbol2("binance", "linear", "MATIC/USDT:USDT") {
		t.Fatal("must return a copy of the stored identity")
	}
	if !RetiredSymbolReplayable(state, "binance", "linear", "MATIC/USDT:USDT", cfg) {
		t.Fatal("RetiredSymbolReplayable disagrees")
	}
	for name, c := range map[string]struct {
		symbol string
		cfg    *config.Config
	}{
		"data ends before the window (DelistMs is irrelevant)": {"OLD/USDT:USDT", cfg},
		"no stored data":                                       {"NODATA/USDT:USDT", cfg},
		"unknown":                                              {"XXX/USDT:USDT", cfg},
		"window before the data":                               {"MATIC/USDT:USDT", &config.Config{TimeRange: &config.TimeTuple{StartMS: 1, EndMS: 50}}},
		"no time range":                                        {"MATIC/USDT:USDT", &config.Config{}},
		"no config":                                            {"MATIC/USDT:USDT", nil},
	} {
		if got, err := storedRetiredSymbol(state, "binance", "linear", c.symbol, c.cfg); err != nil || got != nil {
			t.Fatalf("%s: got %+v %v, want not replayable", name, got, err)
		}
	}
}
