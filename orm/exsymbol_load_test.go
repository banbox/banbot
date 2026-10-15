package orm

import "testing"

func TestCacheExgSymbolsForStateFiltersOtherMarketsAndFencesSID(t *testing.T) {
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "linear")
	items := []*ExSymbol{
		{ID: 17, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"},
		{ID: 41, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"},
	}

	if err := cacheExgSymbolsForState(state, items); err != nil {
		t.Fatal(err)
	}
	if got := state.GetExSymbol2("binance", "linear", "BTC/USDT:USDT"); got == nil || got.ID != 17 {
		t.Fatalf("linear symbol = %+v, want sid 17", got)
	}
	if got := state.GetExSymbol2("binance", "spot", "ETH/USDT"); got != nil {
		t.Fatalf("foreign market was cached: %+v", got)
	}
	if got := NewSymbolStateWithAllocator(allocator).NextSID(); got != 42 {
		t.Fatalf("SID after filtered catalog = %d, want 42", got)
	}
}
