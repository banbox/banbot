package orm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestExplicitSymbolStateAllocatorDoesNotReadLegacyConfig(t *testing.T) {
	previousDataDir, previousDatabase := config.DataDir, config.Database
	config.DataDir = t.TempDir()
	config.Database = &config.DatabaseConfig{Url: "postgres://legacy.example/legacy", DbType: "postgres"}
	t.Cleanup(func() { config.DataDir, config.Database = previousDataDir, previousDatabase })

	state := NewSymbolStateWithIdentity("runtime", "spot")
	allocator := state.sidAllocator()
	if allocator.legacyConfig {
		t.Fatal("explicit symbol state created a legacy-configured SID allocator")
	}
	if allocator.Namespace() != "" || allocator.sharedReservationRoot() != "" {
		t.Fatalf("explicit symbol state inherited global storage identity: namespace=%q root=%q",
			allocator.Namespace(), allocator.sharedReservationRoot())
	}
}

type symbolStateIdentityExchange struct {
	banexg.BanExchange
}

func (symbolStateIdentityExchange) Info() *banexg.ExgInfo {
	panic("identity-bound symbol state consulted exg.Default")
}

func (symbolStateIdentityExchange) GetMarket(string) (*banexg.Market, *errs.Error) {
	panic("identity-bound symbol state consulted exg.Default")
}

func TestSymbolStateInstancesAreIndependent(t *testing.T) {
	a := NewSymbolState()
	b := NewSymbolState()
	aSymbol := &ExSymbol{ID: 1, Exchange: "binance", Market: "spot", Symbol: "A/USDT"}
	bSymbol := &ExSymbol{ID: 2, Exchange: "binance", Market: "spot", Symbol: "B/USDT"}
	a.CacheExSymbol(aSymbol)
	b.CacheExSymbol(bSymbol)

	if got := a.GetSymbolByID(1); got == nil || got == aSymbol || got.Symbol != aSymbol.Symbol || a.GetSymbolByID(2) != nil {
		t.Fatalf("state A symbols leaked: %#v", a.GetExSymbols("", ""))
	}
	if got := b.GetSymbolByID(2); got == nil || got == bSymbol || got.Symbol != bSymbol.Symbol || b.GetSymbolByID(1) != nil {
		t.Fatalf("state B symbols leaked: %#v", b.GetExSymbols("", ""))
	}
	if a.MaxSID() != 1 || b.MaxSID() != 2 {
		t.Fatalf("max SID leaked: A=%d B=%d", a.MaxSID(), b.MaxSID())
	}
}

func TestSymbolStateIdentityIgnoresGlobalExchangeAndMarket(t *testing.T) {
	oldExchange, oldMarket, oldDefault := core.ExgName, core.Market, exg.Default
	t.Cleanup(func() {
		core.ExgName, core.Market, exg.Default = oldExchange, oldMarket, oldDefault
	})

	first := NewSymbolStateWithIdentity("binance", "spot")
	second := NewSymbolStateWithIdentity("okx", "linear")
	first.CacheExSymbol(&ExSymbol{ID: 1, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"})
	second.CacheExSymbol(&ExSymbol{ID: 2, Exchange: "okx", Market: "linear", Symbol: "BTC/USDT"})
	core.ExgName, core.Market = "global", "future"
	exg.Default = symbolStateIdentityExchange{}

	if got, err := first.GetExSymbolCur("BTC/USDT"); err != nil || got == nil || got.ID != 1 {
		t.Fatalf("first identity lookup = %+v, err=%v", got, err)
	}
	if got, err := second.GetExSymbolCur("BTC/USDT"); err != nil || got == nil || got.ID != 2 {
		t.Fatalf("second identity lookup = %+v, err=%v", got, err)
	}
}

func TestMapExSymbolsWithSymbolStateDoesNotUseLegacyCatalog(t *testing.T) {
	oldDefault := exg.Default
	t.Cleanup(func() { exg.Default = oldDefault })
	legacyRestore, err := InstallFrozenExSymbols([]*ExSymbol{{
		ID: 1, Exchange: "state-map", Market: "spot", Symbol: "BTC/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(legacyRestore)

	state := NewSymbolStateWithIdentity("state-map", "spot")
	state.CacheExSymbol(&ExSymbol{ID: 2, Exchange: "state-map", Market: "spot", Symbol: "BTC/USDT"})
	exchange := &localReadExchange{
		info:   &banexg.ExgInfo{ID: "state-map", MarketType: "spot"},
		market: &banexg.Market{Symbol: "BTC/USDT", Type: "spot"},
	}
	got, mapErr := MapExSymbolsWithSymbolState(state, exchange, []string{"BTC/USDT"})
	if mapErr != nil {
		t.Fatalf("state-aware map returned error: %v", mapErr)
	}
	if got[2] == nil || got[1] != nil {
		t.Fatalf("state-aware map = %#v, want only runtime SID 2", got)
	}
}

func TestNewSymbolStateGetExSymbolCurKeepsLegacyGlobalFallback(t *testing.T) {
	oldExchange, oldMarket, oldDefault := core.ExgName, core.Market, exg.Default
	t.Cleanup(func() {
		core.ExgName, core.Market, exg.Default = oldExchange, oldMarket, oldDefault
	})

	state := NewSymbolState()
	state.CacheExSymbol(&ExSymbol{ID: 3, Exchange: "legacy", Market: "spot", Symbol: "ETH/USDT"})
	core.ExgName, core.Market = "legacy", "spot"
	exg.Default = nil

	if got, err := state.GetExSymbolCur("ETH/USDT"); err != nil || got == nil || got.ID != 3 {
		t.Fatalf("legacy fallback lookup = %+v, err=%v", got, err)
	}
}

func TestSymbolStateSubscriptionSetsAreIndependent(t *testing.T) {
	a := NewSymbolState()
	b := NewSymbolState()
	aSymbol := &ExSymbol{ID: 1, Symbol: "A/USDT"}
	bSymbol := &ExSymbol{ID: 2, Symbol: "B/USDT"}
	a.AddHourSymbol(aSymbol)
	b.AddHourSymbol(bSymbol)
	a.Sub1mSymbol(aSymbol.Symbol)

	if got := a.GetHourOnlySymbols(); len(got) != 0 {
		t.Fatalf("state A hour-only symbols = %#v, want empty", got)
	}
	if got := b.GetHourOnlySymbols(); len(got) != 1 || got[2] == nil || got[2] == bSymbol || got[2].Symbol != bSymbol.Symbol {
		t.Fatalf("state B subscriptions leaked: %#v", got)
	}
}

func TestSymbolStateConcurrentReadWrite(t *testing.T) {
	state := NewSymbolState()
	const count = 200
	var writers sync.WaitGroup
	for i := 1; i <= count; i++ {
		i := i
		writers.Add(1)
		go func() {
			defer writers.Done()
			symbol := &ExSymbol{ID: int32(i), Exchange: "binance", Market: "spot", Symbol: fmt.Sprintf("PAIR-%d", i)}
			state.CacheExSymbol(symbol)
			state.AddHourSymbol(symbol)
			if i%2 == 0 {
				state.Sub1mSymbol(symbol.Symbol)
			}
			_ = state.GetSymbolByID(int32(i))
			_ = state.GetExSymbol2(symbol.Exchange, symbol.Market, symbol.Symbol)
			_ = state.GetHourOnlySymbols()
		}()
	}
	writers.Wait()

	if got := len(state.GetExSymbols("", "")); got != count {
		t.Fatalf("symbol count = %d, want %d", got, count)
	}
	if got := len(state.GetHourOnlySymbols()); got != count/2 {
		t.Fatalf("hour-only count = %d, want %d", got, count/2)
	}
}

func TestSymbolStateFieldUpdatesPublishSnapshots(t *testing.T) {
	state := NewSymbolState()
	input := &ExSymbol{
		ID: 1, Exchange: "binance", Market: "spot", Symbol: "A/USDT",
		ListMs: 10, DelistMs: 20, AggRules: "old",
	}
	state.CacheExSymbol(input)
	state.AddHourSymbol(input)

	oldByID := state.GetSymbolByID(input.ID)
	oldByKey := state.GetExSymbol2(input.Exchange, input.Market, input.Symbol)
	oldHour := state.GetHourOnlySymbols()[input.ID]
	state.updateAggRules(input.ID, "new", nil)
	state.updateListMS(input.ID, 30, 40, nil)

	if oldByID.AggRules != "old" || oldByID.ListMs != 10 || oldByID.DelistMs != 20 {
		t.Fatalf("old ID snapshot changed: %+v", oldByID)
	}
	if oldByKey.AggRules != "old" || oldByKey.ListMs != 10 || oldByKey.DelistMs != 20 {
		t.Fatalf("old key snapshot changed: %+v", oldByKey)
	}
	if oldHour.AggRules != "old" || oldHour.ListMs != 10 || oldHour.DelistMs != 20 {
		t.Fatalf("old subscription snapshot changed: %+v", oldHour)
	}
	if input.AggRules != "old" || input.ListMs != 10 || input.DelistMs != 20 {
		t.Fatalf("caller-owned input changed: %+v", input)
	}
	got := state.GetSymbolByID(input.ID)
	if got.AggRules != "new" || got.ListMs != 30 || got.DelistMs != 40 {
		t.Fatalf("new snapshot missing updates: %+v", got)
	}
	if hour := state.GetHourOnlySymbols()[input.ID]; hour == nil || hour.AggRules != "new" || hour.ListMs != 30 || hour.DelistMs != 40 {
		t.Fatalf("subscription snapshot missing updates: %+v", hour)
	}
}

func TestSymbolStateConcurrentFieldSnapshots(t *testing.T) {
	state := NewSymbolState()
	state.CacheExSymbol(&ExSymbol{
		ID: 1, Exchange: "binance", Market: "spot", Symbol: "A/USDT",
		ListMs: 1, DelistMs: 2, AggRules: "initial",
	})
	state.AddHourSymbol(state.GetSymbolByID(1))

	const iterations = 500
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				byID := state.GetSymbolByID(1)
				if byID != nil {
					_ = byID.AggRules
					_ = byID.ListMs
					_ = byID.DelistMs
				}
				byKey := state.GetExSymbol2("binance", "spot", "A/USDT")
				if byKey != nil {
					_ = byKey.AggRules
					_ = byKey.ListMs
					_ = byKey.DelistMs
				}
				for _, item := range state.GetExSymbolsByID("binance", "spot") {
					_ = item.AggRules
					_ = item.ListMs
					_ = item.DelistMs
				}
				for _, item := range state.GetHourOnlySymbols() {
					_ = item.AggRules
					_ = item.ListMs
					_ = item.DelistMs
				}
			}
		}()
	}
	for i := 0; i < 2; i++ {
		writer := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				state.updateAggRules(1, fmt.Sprintf("rule-%d-%d", writer, j), nil)
				state.updateListMS(1, int64(j+writer), int64(j+writer+1), nil)
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestSymbolStateGetExSymbolsByIDKeepsDuplicateIdentityRows(t *testing.T) {
	state := NewSymbolState()
	first := &ExSymbol{ID: 1, Exchange: "macro", Market: "macro", Symbol: "CPI_US", ExgReal: "fred"}
	second := &ExSymbol{ID: 2, Exchange: "macro", Market: "macro", Symbol: "CPI_US", ExgReal: "wind"}
	state.CacheExSymbol(first)
	state.CacheExSymbol(second)

	if got := state.GetExSymbols("macro", "macro"); len(got) != 1 {
		t.Fatalf("canonical symbols = %#v, want one identity", got)
	}
	got := state.GetExSymbolsByID("macro", "macro")
	if len(got) != 2 || got[1] == nil || got[1] == first || got[1].Symbol != first.Symbol || got[2] == nil || got[2] == second || got[2].Symbol != second.Symbol {
		t.Fatalf("symbols by ID = %#v, want both SID rows", got)
	}
}

func TestSymbolStateSetExSymbolsKeepsDuplicateExgRealRows(t *testing.T) {
	state := NewSymbolState()
	items := []*ExSymbol{
		{ID: 2, Exchange: "macro", Market: "macro", Symbol: "CPI_US", ExgReal: "fred"},
		{ID: 1, Exchange: "macro", Market: "macro", Symbol: "CPI_US"},
	}
	if err := state.SetExSymbols(items); err != nil {
		t.Fatalf("SetExSymbols returned error: %v", err)
	}
	if got := state.GetExSymbolsByID("macro", "macro"); len(got) != 2 || got[1] == nil || got[2] == nil {
		t.Fatalf("symbols by ID = %#v, want both ExgReal rows", got)
	}
	if got := state.GetExSymbol2("macro", "macro", "CPI_US"); got == nil || got.ExgReal != "" {
		t.Fatalf("canonical symbol = %+v, want generic row", got)
	}
}

func TestSymbolStateCanonicalReplacementKeepsMarketCount(t *testing.T) {
	state := NewSymbolState()
	state.CacheExSymbol(&ExSymbol{ID: 2, Exchange: "macro", Market: "macro", Symbol: "CPI_US", ExgReal: "fred"})
	state.CacheExSymbol(&ExSymbol{ID: 1, Exchange: "macro", Market: "macro", Symbol: "CPI_US"})

	if got := state.MarketCount("macro", "macro"); got != 1 {
		t.Fatalf("market count after canonical replacement = %d, want 1", got)
	}
	if got := state.GetExSymbol2("macro", "macro", "CPI_US"); got == nil || got.ID != 1 {
		t.Fatalf("canonical symbol = %+v, want generic SID 1", got)
	}
}

func TestSymbolStateSetExSymbolsClearsReplaceOnlyState(t *testing.T) {
	state := NewSymbolState()
	old := &ExSymbol{ID: 1, Exchange: "binance", Market: "spot", Symbol: "OLD/USDT"}
	state.CacheExSymbol(old)
	state.AddHourSymbol(old)
	state.tryListMu.Lock()
	state.tryListIDs[old.ID] = true
	state.tryListMu.Unlock()

	newItem := &ExSymbol{ID: 2, Exchange: "binance", Market: "spot", Symbol: "NEW/USDT"}
	if err := state.SetExSymbols([]*ExSymbol{newItem}); err != nil {
		t.Fatalf("SetExSymbols returned error: %v", err)
	}
	if got := state.GetSymbolByID(old.ID); got != nil {
		t.Fatalf("old symbol survived replacement: %+v", got)
	}
	if got := state.GetHourOnlySymbols(); len(got) != 0 {
		t.Fatalf("subscription state survived replacement: %#v", got)
	}
	state.tryListMu.Lock()
	tryListCount := len(state.tryListIDs)
	state.tryListMu.Unlock()
	if tryListCount != 0 {
		t.Fatalf("try-list state survived replacement: %d entries", tryListCount)
	}
}

func TestQueriesSymbolStatePropagation(t *testing.T) {
	explicit := NewSymbolState()
	legacy := New(nil)

	if got := New(nil).WithSymbolState(explicit).WithTx(nil).symbolState(); got != explicit {
		t.Fatal("WithTx did not preserve explicit symbol state")
	}
	if got := legacy.WithSymbolState(explicit).symbolState(); got != explicit {
		t.Fatal("WithSymbolState did not bind explicit symbol state")
	}
	if got := legacy.WithSymbolState(nil).symbolState(); got == nil {
		t.Fatal("WithSymbolState(nil) did not preserve legacy facade semantics")
	}

	frozen := NewSymbolState()
	previous := swapDefaultSymbolState(frozen)
	defer swapDefaultSymbolState(previous)
	if got := legacy.WithSymbolState(nil).symbolState(); got != frozen {
		t.Fatal("existing legacy Queries did not observe default state replacement")
	}
}
