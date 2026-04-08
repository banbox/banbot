package orm

import (
	"fmt"
	"testing"
	"time"
)

func TestGetExSymbol2UsesThreeFieldIdentity(t *testing.T) {
	oldKey := keySymbolMap
	oldID := idSymbolMap
	keySymbolMap = map[string]*ExSymbol{}
	idSymbolMap = map[int32]*ExSymbol{}
	t.Cleanup(func() {
		keySymbolMap = oldKey
		idSymbolMap = oldID
	})

	item := &ExSymbol{ID: 101, Exchange: "macro", Market: "macro", Symbol: "CPI_US", ExgReal: "fred"}
	cacheExSymbol(item)

	if got := GetExSymbol2("macro", "macro", "CPI_US"); got != item {
		t.Fatalf("expected 3-field lookup to return cached symbol, got %+v", got)
	}
	if got := GetExSymbol2("macro", "macro", "CPI_US", "wind"); got != item {
		t.Fatalf("expected exg_real to be ignored for identity lookup, got %+v", got)
	}
}

func TestEnsureSymbolsReusesIdentityAcrossExgReal(t *testing.T) {
	if err := initApp(); err != nil {
		t.Fatalf("initApp failed: %v", err)
	}
	exchange := fmt.Sprintf("macro_test_%d", time.Now().UnixNano())
	items := []*ExSymbol{
		{Exchange: exchange, Market: "macro", Symbol: "CPI_US", ExgReal: "fred"},
		{Exchange: exchange, Market: "macro", Symbol: "CPI_US", ExgReal: "wind"},
	}
	if err := EnsureSymbols(items); err != nil {
		t.Fatalf("EnsureSymbols failed: %v", err)
	}
	if items[0].ID == 0 || items[1].ID == 0 {
		t.Fatalf("expected non-zero ids after EnsureSymbols: %+v", items)
	}
	if items[0].ID != items[1].ID {
		t.Fatalf("expected same sid for same exchange/market/symbol, got %+v", items)
	}
	if got := GetExSymbol2(exchange, "macro", "CPI_US"); got == nil || got.ID != items[0].ID {
		t.Fatalf("failed to load symbol back from cache: %+v", got)
	}
}
