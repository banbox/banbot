package orm

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestStoredRetiredSymbolNeedsStoredDataInsideWindow(t *testing.T) {
	oldCatalog := catalogSymbolLookup
	defer func() { catalogSymbolLookup = oldCatalog }()
	catalogSymbolLookup = func(*SymbolState, string, string, string) (*ExSymbol, error) { return nil, nil } // the state is the whole catalog here
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
		"no stored data":         {"NODATA/USDT:USDT", cfg},
		"unknown":                {"XXX/USDT:USDT", cfg},
		"window before the data": {"MATIC/USDT:USDT", &config.Config{TimeRange: &config.TimeTuple{StartMS: 1, EndMS: 50}}},
		"no time range":          {"MATIC/USDT:USDT", &config.Config{}},
		"no config":              {"MATIC/USDT:USDT", nil},
	} {
		if got, err := storedRetiredSymbol(state, "binance", "linear", c.symbol, c.cfg); err != nil || got != nil {
			t.Fatalf("%s: got %+v %v, want not replayable", name, got, err)
		}
	}
}

// BtFeatureLevel 9: pool members without stored data inside the window are unavailable for it (skipped and reported),
// not a reason to fail the run; unknown symbols and an all-unavailable pool still fail; pools whose members all have
// data resolve exactly as before.
func TestResolveEnsureListSkipsUnavailableMembers(t *testing.T) {
	oldCatalog := catalogSymbolLookup
	defer func() { catalogSymbolLookup = oldCatalog }()
	catalogSymbolLookup = func(*SymbolState, string, string, string) (*ExSymbol, error) { return nil, nil } // the state is the whole catalog here
	state := NewSymbolState()
	if err := state.SetExSymbols([]*ExSymbol{
		{ID: 1, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT", ListMs: 10},
		{ID: 2, Exchange: "binance", Market: "linear", Symbol: "ALIVE/USDT:USDT", ListMs: 10, DelistMs: 1784737902136},
		{ID: 3, Exchange: "binance", Market: "linear", Symbol: "DEAD/USDT:USDT", ListMs: 10, DelistMs: 1784737902136},
		{ID: 4, Exchange: "binance", Market: "linear", Symbol: "LATE/USDT:USDT", ListMs: 20000},
	}); err != nil {
		t.Fatal(err)
	}
	ranges := map[int32][2]int64{2: {10, 8000}, 3: {10, 500}, 4: {20000, 30000}} // sid -> [start, stop) of every timeframe
	old := klineRangeLookup
	defer func() { klineRangeLookup = old }()
	klineRangeLookup = func(_ *SymbolState, sid int32, _ string) (int64, int64, error) {
		return ranges[sid][0], ranges[sid][1], nil
	}
	cfg := &config.Config{TimeRange: &config.TimeTuple{StartMS: 1000, EndMS: 9000}}
	markets := banexg.MarketMap{"BTC/USDT:USDT": &banexg.Market{}} // only BTC is in the current market table
	resolve := func(symbols []string, allow bool, skipped *[]SkippedPair, c *config.Config) ([]*ExSymbol, *errs.Error) {
		return resolveEnsureList(state, markets, "binance", "linear", symbols, c, allow, skipped)
	}
	// window after the delisting (DEAD) and before the listing (LATE): both skipped, ALIVE and BTC kept in order
	var skipped []SkippedPair
	got, err := resolve([]string{"BTC/USDT:USDT", "DEAD/USDT:USDT", "ALIVE/USDT:USDT", "LATE/USDT:USDT"}, true, &skipped, cfg)
	if err != nil || len(got) != 2 || got[0].Symbol != "BTC/USDT:USDT" || got[1].Symbol != "ALIVE/USDT:USDT" {
		t.Fatalf("got %+v %v", got, err)
	}
	if len(skipped) != 2 || skipped[0] != (SkippedPair{Symbol: "DEAD/USDT:USDT", Reason: "data_ends_before_window"}) || skipped[1] != (SkippedPair{Symbol: "LATE/USDT:USDT", Reason: "data_starts_after_window"}) {
		t.Fatalf("skipped %v", skipped)
	}
	// every member with data: nothing skipped, identical to the strict resolution
	skipped = nil
	all, err := resolve([]string{"BTC/USDT:USDT", "ALIVE/USDT:USDT"}, true, &skipped, cfg)
	strict, err2 := resolve([]string{"BTC/USDT:USDT", "ALIVE/USDT:USDT"}, true, nil, cfg)
	if err != nil || err2 != nil || len(skipped) != 0 || len(all) != len(strict) {
		t.Fatalf("%v %v %v", err, err2, skipped)
	}
	for i := range all {
		if *all[i] != *strict[i] {
			t.Fatalf("member %d differs: %+v vs %+v", i, all[i], strict[i])
		}
	}
	// every member unavailable
	skipped = nil
	if _, err := resolve([]string{"DEAD/USDT:USDT", "LATE/USDT:USDT"}, true, &skipped, cfg); err == nil || err.Code != core.ErrInvalidSymbol {
		t.Fatalf("all unavailable must fail: %v", err)
	}
	// symbols the catalog does not know keep failing
	if _, err := resolve([]string{"BTC/USDT:USDT", "XXX/USDT:USDT"}, true, &skipped, cfg); err == nil || err.Code != core.ErrInvalidSymbol {
		t.Fatalf("unknown symbol must fail: %v", err)
	}
	// the strict behaviour (no skipped collector, or retired symbols not allowed) is unchanged
	if _, err := resolve([]string{"DEAD/USDT:USDT"}, true, nil, cfg); err == nil || err.Code != core.ErrInvalidSymbol {
		t.Fatalf("strict: %v", err)
	}
	if _, err := resolve([]string{"ALIVE/USDT:USDT"}, false, &skipped, cfg); err == nil || err.Code != core.ErrInvalidSymbol {
		t.Fatalf("retired not allowed: %v", err)
	}
}

// The runtime state holds only the symbols registered so far (BTC) while the stored catalog knows the retired ones: the
// lookup must ask the catalog, not conclude "unknown" from a non-empty state.
func TestRetiredLookupAsksCatalogWhenStateIsPartial(t *testing.T) {
	state := NewSymbolState()
	if err := state.SetExSymbols([]*ExSymbol{{ID: 1, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT", ListMs: 10}}); err != nil {
		t.Fatal(err)
	}
	catalog := map[string]*ExSymbol{
		"EOS/USDT:USDT":   {ID: 104, Exchange: "binance", Market: "linear", Symbol: "EOS/USDT:USDT", ListMs: 10, DelistMs: 1784737902136},
		"MATIC/USDT:USDT": {ID: 105, Exchange: "binance", Market: "linear", Symbol: "MATIC/USDT:USDT", ListMs: 10},
	}
	oldCat, oldRange := catalogSymbolLookup, klineRangeLookup
	defer func() { catalogSymbolLookup, klineRangeLookup = oldCat, oldRange }()
	catalogSymbolLookup = func(_ *SymbolState, _, _, symbol string) (*ExSymbol, error) { return catalog[symbol], nil }
	ranges := map[int32][2]int64{104: {10, 5000}, 105: {10, 500}}
	klineRangeLookup = func(_ *SymbolState, sid int32, _ string) (int64, int64, error) {
		return ranges[sid][0], ranges[sid][1], nil
	}
	cfg := &config.Config{TimeRange: &config.TimeTuple{StartMS: 1000, EndMS: 9000}}
	if got, known, err := storedRetiredLookup(state, "binance", "linear", "EOS/USDT:USDT", cfg); err != nil || !known || got == nil || got.ID != 104 {
		t.Fatalf("partial window: %+v %v %v", got, known, err) // data to 5000 overlaps [1000,9000)
	}
	if got, known, err := storedRetiredLookup(state, "binance", "linear", "MATIC/USDT:USDT", cfg); err != nil || !known || got != nil {
		t.Fatalf("window after the data: %+v %v %v (known, no stored data)", got, known, err)
	}
	if got, known, err := storedRetiredLookup(state, "binance", "linear", "NOPE/USDT:USDT", cfg); err != nil || known || got != nil {
		t.Fatalf("unknown: %+v %v %v", got, known, err)
	}
	if state.SymbolCount() != 1 {
		t.Fatal("the lookup must not register anything in the state")
	}
}
