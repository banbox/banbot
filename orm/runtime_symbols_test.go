package orm

import (
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
