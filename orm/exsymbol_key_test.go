package orm

import "testing"

func TestGetExSymbol2PrefersCanonicalCacheEntry(t *testing.T) {
	oldKeyMap := keySymbolMap
	oldIDMap := idSymbolMap
	defer func() {
		keySymbolMap = oldKeyMap
		idSymbolMap = oldIDMap
	}()

	keySymbolMap = map[string]*ExSymbol{}
	idSymbolMap = map[int32]*ExSymbol{}
	fred := &ExSymbol{ID: 2, Exchange: "macro", Market: "macro", Symbol: "CPI_US", ExgReal: "fred"}
	base := &ExSymbol{ID: 1, Exchange: "macro", Market: "macro", Symbol: "CPI_US", ExgReal: ""}

	cacheExSymbol(fred)
	cacheExSymbol(base)

	if got := GetExSymbol2("macro", "macro", "CPI_US"); got != base {
		t.Fatalf("expected empty exg_real symbol to become canonical, got %+v", got)
	}
	if got := GetExSymbol2("macro", "macro", "CPI_US", "fred"); got != base {
		t.Fatalf("expected lookup with exg_real to resolve to canonical symbol, got %+v", got)
	}
}

func TestExSymbolAggRules(t *testing.T) {
	exs := &ExSymbol{Exchange: "binance", Market: "spot", AggRules: `{"rate":"avg","high":"max","bad":"wat"}`}
	if got := exs.AggRule("rate"); got != "avg" {
		t.Fatalf("expected avg, got %q", got)
	}
	if got := exs.AggRule("missing"); got != "last" {
		t.Fatalf("expected missing rule to default last, got %q", got)
	}
	if got := exs.AggRule("bad"); got != "last" {
		t.Fatalf("expected invalid rule to default last, got %q", got)
	}
}

func TestExSymbolCustomAggRule(t *testing.T) {
	if !RegisterAggRule("weighted_avg", func(rows []*DataRecord, field SeriesField) (any, error) {
		return nil, nil
	}) {
		t.Fatal("expected custom agg rule registration to succeed")
	}
	exs := &ExSymbol{AggRules: `{"rate":"weighted_avg","bad":"missing_custom"}`}
	if got := exs.AggRule("rate"); got != "weighted_avg" {
		t.Fatalf("expected registered custom rule, got %q", got)
	}
	if got := exs.AggRule("bad"); got != "last" {
		t.Fatalf("expected unregistered rule to default last, got %q", got)
	}
	if _, ok := GetAggRuleFunc("weighted_avg"); !ok {
		t.Fatal("expected registered custom function to be available")
	}
}

func TestExSymbolSetAggRulesKeepsCustomName(t *testing.T) {
	exs := &ExSymbol{}
	if err := exs.SetAggRules(map[string]string{"rate": "future_custom"}); err != nil {
		t.Fatalf("SetAggRules returned error: %v", err)
	}
	if !RegisterAggRule("future_custom", func(rows []*DataRecord, field SeriesField) (any, error) {
		return nil, nil
	}) {
		t.Fatal("expected custom agg rule registration to succeed")
	}
	if got := exs.AggRule("rate"); got != "future_custom" {
		t.Fatalf("expected custom rule name to survive SetAggRules, got %q", got)
	}
}
