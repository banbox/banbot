package orm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
)

func TestInstallFrozenExSymbolsIsMemoryOnlyAndRestorable(t *testing.T) {
	original := &ExSymbol{ID: 91, Exchange: "old", Market: "spot", Symbol: "OLD/USDT"}
	restoreOriginal, err := InstallFrozenExSymbols([]*ExSymbol{original})
	if err != nil {
		t.Fatal(err)
	}
	defer restoreOriginal()

	restoreFrozen, err := InstallFrozenExSymbols([]*ExSymbol{{ID: 7, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}})
	if err != nil {
		t.Fatal(err)
	}
	if GetSymbolByID(7) == nil || GetSymbolByID(91) != nil {
		t.Fatal("frozen symbol cache was not installed exclusively")
	}
	oldExchange, oldMarket, oldDefault := core.ExgName, core.Market, exg.Default
	core.ExgName, core.Market, exg.Default = "binance", "linear", nil
	defer func() { core.ExgName, core.Market, exg.Default = oldExchange, oldMarket, oldDefault }()
	if current, currentErr := GetExSymbolCur("BTC/USDT:USDT"); currentErr != nil || current.ID != 7 {
		t.Fatalf("GetExSymbolCur did not use the frozen cache: symbol=%+v err=%v", current, currentErr)
	}
	restoreFrozen()
	if GetSymbolByID(91) == nil || GetSymbolByID(7) != nil {
		t.Fatal("original symbol cache was not restored")
	}
}

func TestInstallFrozenExSymbolsConcurrentLegacyReads(t *testing.T) {
	base := NewSymbolState()
	previous := swapDefaultSymbolState(base)
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
	legacyQueries := New(nil)

	const iterations = 100
	var readers sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for j := 0; j < iterations; j++ {
				_ = GetExSymbol2("test", "spot", "PAIR")
				_ = GetExSymbols("", "")
				_ = GetAllExSymbols()
				_ = legacyQueries.WithSymbolState(nil).symbolState().SymbolCount()
			}
		}()
	}
	close(start)
	for i := 1; i <= iterations; i++ {
		restore, err := InstallFrozenExSymbols([]*ExSymbol{{
			ID:       int32(i),
			Exchange: "test",
			Market:   "spot",
			Symbol:   fmt.Sprintf("PAIR-%d", i),
		}})
		if err != nil {
			t.Fatal(err)
		}
		restore()
	}
	readers.Wait()
}
