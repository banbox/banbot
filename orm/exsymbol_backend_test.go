package orm

import "testing"

func TestEnsureSymbolsIgnoresExgRealForIdentity(t *testing.T) {
	if err := initApp(); err != nil {
		t.Fatalf("initApp failed: %v", err)
	}
	items := []*ExSymbol{
		{Exchange: "macro_test", Market: "macro", Symbol: "CPI_US", ExgReal: "fred"},
		{Exchange: "macro_test", Market: "macro", Symbol: "CPI_US", ExgReal: "wind"},
	}
	if err := EnsureSymbols(items); err != nil {
		t.Fatalf("EnsureSymbols failed: %v", err)
	}
	if items[0].ID == 0 || items[1].ID == 0 {
		t.Fatalf("expected non-zero ids, got %+v", items)
	}
	if items[0].ID != items[1].ID {
		t.Fatalf("expected identical sids when only exg_real differs, got %+v", items)
	}
	got := GetExSymbol2("macro_test", "macro", "CPI_US")
	if got == nil || got.ID != items[0].ID {
		t.Fatalf("expected cache lookup by exchange/market/symbol to succeed, got %+v", got)
	}
}
