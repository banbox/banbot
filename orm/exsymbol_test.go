package orm

import (
	"sort"
	"testing"

	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

func getExchange(name string, market string, t *testing.T) banexg.BanExchange {
	exchange, err := exg.GetWith(name, market, "")
	if err != nil {
		t.Error(err)
		return nil
	}
	markets, err := LoadMarkets(exchange, false)
	if err != nil {
		t.Error(err)
		return nil
	}
	log.Info("load exchange markets", zap.String("name", name), zap.String("mak", market),
		zap.Int("num", len(markets)))
	err = LoadAllExSymbols()
	if err != nil {
		t.Error(err)
		return nil
	}
	return exchange
}

func TestGetExSymbol(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	bnb := getExchange("binance", "linear", t)
	if bnb == nil {
		return
	}
	marketSymbols := bnb.GetCurMarkets()
	if len(marketSymbols) == 0 {
		t.Fatal("expected binance linear markets to be loaded")
	}
	keys := make([]string, 0, len(marketSymbols))
	for symbol := range marketSymbols {
		keys = append(keys, symbol)
	}
	sort.Strings(keys)
	var pick string
	for _, symbol := range keys {
		if exs := findExSymbol("binance", "linear", symbol); exs != nil {
			pick = symbol
			break
		}
	}
	if pick == "" {
		t.Fatal("expected at least one cached binance linear ExSymbol")
	}

	got, err := GetExSymbol(bnb, pick)
	if err != nil {
		t.Fatalf("GetExSymbol(%q) returned error: %v", pick, err)
	}
	if got == nil {
		t.Fatalf("GetExSymbol(%q) returned nil", pick)
	}
	if got.Exchange != "binance" {
		t.Fatalf("expected exchange binance, got %q", got.Exchange)
	}
	if got.Market != "linear" {
		t.Fatalf("expected market linear, got %q", got.Market)
	}
	if got.Symbol != pick {
		t.Fatalf("expected symbol %q, got %q", pick, got.Symbol)
	}
	if got.ID == 0 {
		t.Fatalf("expected persisted sid for %q, got 0", pick)
	}
}
