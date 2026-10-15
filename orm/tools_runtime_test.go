package orm

import (
	"context"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
)

func TestImportDataWithDepsRequiresKlineOptionsBeforeFilesOrDatabase(t *testing.T) {
	err := ImportDataWithDeps(t.TempDir(), 1, KDataTransferDeps{
		Context: context.Background(), Storage: &Storage{}, Symbols: NewSymbolState(),
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "kline runtime options are required") {
		t.Fatalf("ImportDataWithDeps error = %v, want required kline options", err)
	}
}

func TestGenExportKlinesUsesExplicitAllIdentityCatalog(t *testing.T) {
	symbols := NewSymbolState()
	if err := symbols.SetExSymbols([]*ExSymbol{
		{ID: 1, Exchange: "first", Market: "spot", Symbol: "BTC/USDT"},
		{ID: 2, Exchange: "second", Market: "linear", Symbol: "BTC/USDT:USDT"},
	}); err != nil {
		t.Fatal(err)
	}
	jobs, _, err := genExportKlines(symbols, []*config.MarketTFSymbolsRange{{
		MarketSymbolsRange: &config.MarketSymbolsRange{MarketRange: &config.MarketRange{
			Exchange: "second", Market: "linear", TimeRange: "1700000000000-1700003600000",
		}, Symbols: []string{"BTC/USDT:USDT"}}, TimeFrames: []string{"1h"},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != 2 {
		t.Fatalf("export jobs = %#v, want only stored second:linear identity", jobs)
	}
}
