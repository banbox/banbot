package orm

import (
	"fmt"
	"testing"
	"time"
)

func TestEnsureSymbolsStoresCanonicalExSymbol(t *testing.T) {
	err := initApp()
	if err != nil {
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
		t.Fatalf("expected non-zero ids: %+v", items)
	}
	if items[0].ID != items[1].ID {
		t.Fatalf("expected identical ids for same logical source: %+v", items)
	}

	if got := GetExSymbol2(exchange, "macro", "CPI_US", "fred"); got == nil || got.ID != items[0].ID {
		t.Fatalf("lookup mismatch: %+v", got)
	}
}
