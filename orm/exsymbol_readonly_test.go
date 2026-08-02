package orm

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
)

func TestValidateCurSymbolsReadOnlyRejectsMissingDatabaseAuthority(t *testing.T) {
	oldKeys := keySymbolMap
	keySymbolMap = map[string]*ExSymbol{}
	t.Cleanup(func() { keySymbolMap = oldKeys })

	const symbol = "BTC/USDT:USDT"
	markets := banexg.MarketMap{symbol: {Symbol: symbol, Type: core.Market}}
	err := validateCurSymbolsReadOnly([]string{symbol}, markets, "binance", core.Market)
	if err == nil || !strings.Contains(err.Error(), "absent from database authority") {
		t.Fatalf("missing database symbol was accepted: %v", err)
	}

	keySymbolMap[exSymbolKey("binance", core.Market, symbol)] = &ExSymbol{
		ID: 1, Exchange: "binance", Market: core.Market, Symbol: symbol,
	}
	if err = validateCurSymbolsReadOnly([]string{symbol}, markets, "binance", core.Market); err != nil {
		t.Fatalf("existing database symbol was rejected: %v", err)
	}
}

func TestValidateListDatesReadOnlyRejectsRepairableMetadata(t *testing.T) {
	const symbol = "OLD/USDT:USDT"
	exs := &ExSymbol{ID: 1, Exchange: "binance", Market: core.Market, Symbol: symbol}
	markets := banexg.MarketMap{symbol: {Symbol: symbol, Created: 100, Expiry: 200}}
	if err := validateListDatesReadOnly(map[int32]*ExSymbol{1: exs}, markets); err == nil ||
		!strings.Contains(err.Error(), "incomplete list-date authority") {
		t.Fatalf("repairable list dates were accepted: %v", err)
	}
	if exs.ListMs != 0 || exs.DelistMs != 0 {
		t.Fatalf("read-only validation mutated symbol: %+v", exs)
	}

	exs.ListMs, exs.DelistMs = 100, 200
	if err := validateListDatesReadOnly(map[int32]*ExSymbol{1: exs}, markets); err != nil {
		t.Fatalf("complete list dates were rejected: %v", err)
	}
}
