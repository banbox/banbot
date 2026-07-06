package orm

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestEnsureExSymbolRejectsIncompleteIdentity(t *testing.T) {
	tests := []struct {
		name     string
		exchange string
		market   string
		symbol   string
		wantMsg  string
	}{
		{name: "missing exchange", market: "macro", symbol: "CPI_US", wantMsg: "exchange is required"},
		{name: "missing market", exchange: "macro", symbol: "CPI_US", wantMsg: "market is required"},
		{name: "missing symbol", exchange: "macro", market: "macro", wantMsg: "symbol is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EnsureExSymbol(tt.exchange, tt.market, tt.symbol)
			if err == nil {
				t.Fatalf("expected error, got symbol %+v", got)
			}
			if got != nil {
				t.Fatalf("expected nil symbol on validation failure, got %+v", got)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("expected error %q, got %v", tt.wantMsg, err)
			}
		})
	}
}

func TestEnsureExSymbolReusesSyntheticIdentity(t *testing.T) {
	if err := initApp(); err != nil {
		t.Fatalf("initApp failed: %v", err)
	}
	exchange := fmt.Sprintf("macro_synth_%d", time.Now().UnixNano())

	first, err := EnsureExSymbol(exchange, "macro", "CPI_US", "fred")
	if err != nil {
		t.Fatalf("first EnsureExSymbol failed: %v", err)
	}
	second, err := EnsureExSymbol(exchange, "macro", "CPI_US", "wind")
	if err != nil {
		t.Fatalf("second EnsureExSymbol failed: %v", err)
	}
	if first == nil || second == nil {
		t.Fatalf("expected non-nil symbols, got first=%+v second=%+v", first, second)
	}
	if first.ID == 0 || second.ID == 0 {
		t.Fatalf("expected non-zero sid reuse, got first=%+v second=%+v", first, second)
	}
	if first.ID != second.ID {
		t.Fatalf("expected repeated synthetic identity resolution to reuse sid, got first=%+v second=%+v", first, second)
	}
	if first.Symbol != "CPI_US" || second.Symbol != "CPI_US" {
		t.Fatalf("expected helper to keep the synthetic symbol key stable, got first=%+v second=%+v", first, second)
	}
	cached := GetExSymbol2(exchange, "macro", "CPI_US")
	if cached == nil || cached.ID != first.ID {
		t.Fatalf("expected canonical cache entry after EnsureExSymbol, got %+v", cached)
	}
}
