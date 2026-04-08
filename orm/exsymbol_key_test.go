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
