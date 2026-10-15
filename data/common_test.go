package data

import (
	"context"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

func TestFillLacksExplicitStateRejectsInvalidSymbolWithoutGap(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("binance", "spot")
	if err := state.SetExSymbols([]*orm.ExSymbol{{
		ID: 1, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
	}}); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name string
		exs  *orm.ExSymbol
	}{
		{name: "nil symbol"},
		{name: "foreign symbol", exs: &orm.ExSymbol{ID: 2, Exchange: "okx", Market: "spot", Symbol: "BTC/USDT"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := &PairTFCache{TimeFrame: "1m", TFSecs: 60}
			_, err := cache.fillLacksWithSymbolState(state, test.exs, 60, 1_000, 2_000)
			if err == nil || err.Code != core.ErrInvalidSymbol {
				t.Fatalf("fillLacksWithSymbolState error = %v, want invalid symbol", err)
			}
			if cache.SubNextMS != 0 {
				t.Fatalf("SubNextMS = %d, want unchanged after validation failure", cache.SubNextMS)
			}
		})
	}
}

func TestFillLacksNilStateKeepsNoGapBehavior(t *testing.T) {
	cache := &PairTFCache{TimeFrame: "1m", TFSecs: 60}
	rows, err := cache.fillLacksWithSymbolState(nil, nil, 60, 1_000, 2_000)
	if err != nil || rows != nil || cache.SubNextMS != 2_000 {
		t.Fatalf("legacy no-gap result rows=%v next=%d err=%v", rows, cache.SubNextMS, err)
	}
}

func TestTrySaveSeriesRuntimeDoesNotFallbackWithoutSymbolState(t *testing.T) {
	job := &SaveSeries{
		Sid: 7,
		Rows: []*orm.DataSeries{{
			Sid:      7,
			ExSymbol: &orm.ExSymbol{ID: 7, Symbol: "LEGACY/USDT"},
		}},
	}
	err := trySaveSeriesWithRuntimeDeps(&RuntimeDeps{}, job, 60, newPeriodSta("1m"), newPeriodSta("1h"))
	if err == nil || err.Code != core.ErrInvalidSymbol {
		t.Fatalf("runtime save without symbol state error = %v, want invalid symbol", err)
	}
}

func TestResolveSaveSeriesSymbolUsesExplicitRuntimeState(t *testing.T) {
	runtimeSymbols := orm.NewSymbolStateWithIdentity("runtime", "spot")
	explicit := &orm.ExSymbol{ID: 7, Exchange: "runtime", Market: "spot", Symbol: "RUNTIME/USDT"}
	if err := runtimeSymbols.SetExSymbols([]*orm.ExSymbol{explicit}); err != nil {
		t.Fatal(err)
	}
	rows := []*orm.DataSeries{{Sid: 7, ExSymbol: &orm.ExSymbol{ID: 7, Symbol: "LEGACY/USDT"}}}
	got := resolveSaveSeriesSymbolWithDeps(&RuntimeDeps{Symbols: runtimeSymbols}, runtimeSymbols, 7, rows)
	if got != explicit && (got == nil || got.Symbol != explicit.Symbol) {
		t.Fatalf("runtime save symbol = %+v, want %+v", got, explicit)
	}
}

func TestTrySaveSeriesRejectsNilRowsBeforeRuntimeLookup(t *testing.T) {
	symbols := orm.NewSymbolStateWithIdentity("runtime", "spot")
	explicit := &orm.ExSymbol{ID: 7, Exchange: "runtime", Market: "spot", Symbol: "RUNTIME/USDT"}
	if err := symbols.SetExSymbols([]*orm.ExSymbol{explicit}); err != nil {
		t.Fatal(err)
	}

	job := &SaveSeries{
		Sid: 7,
		Rows: []*orm.DataSeries{
			nil,
			{Sid: 7, TimeMS: 1, EndMS: 2, ExSymbol: explicit},
		},
	}
	if err := trySaveSeriesWithSymbolState(symbols, job, 60, newPeriodSta("1m"), newPeriodSta("1h")); err == nil || err.Code != core.ErrInvalidBars {
		t.Fatalf("nil row save error = %v, want invalid bars", err)
	}
}

func TestTrySaveSeriesStopsBeforeIOWhenRuntimeCanceled(t *testing.T) {
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.Stop()
	symbols := orm.NewSymbolStateWithIdentity("runtime", "spot")
	explicit := &orm.ExSymbol{ID: 7, Exchange: "runtime", Market: "spot", Symbol: "RUNTIME/USDT"}
	if err := symbols.SetExSymbols([]*orm.ExSymbol{explicit}); err != nil {
		t.Fatal(err)
	}

	job := &SaveSeries{Sid: 7, Rows: []*orm.DataSeries{{Sid: 7, TimeMS: 1, EndMS: 2, ExSymbol: explicit}}}
	got := trySaveSeriesWithRuntimeDeps(&RuntimeDeps{Core: state, Symbols: symbols}, job, 60,
		newPeriodSta("1m"), newPeriodSta("1h"))
	if got == nil || got.Code != errs.CodeCancel {
		t.Fatalf("canceled save error = %v, want cancellation", got)
	}
}

func TestSeriesRepairRequiresRuntimeSymbolsBeforeSessionAccess(t *testing.T) {
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, _, repairErr := ensureSeriesToWithRuntimeDeps(&RuntimeDeps{Core: state}, nil, 7, "1h", 0, 1)
	if repairErr == nil || repairErr.Code != core.ErrInvalidSymbol || got != 0 {
		t.Fatalf("missing runtime symbols repair = (end=%d, err=%v), want invalid symbol", got, repairErr)
	}
}

func TestCanceledRuntimeSkipsDataRepairIO(t *testing.T) {
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.Stop()
	deps := &RuntimeDeps{Core: state, Symbols: orm.NewSymbolStateWithIdentity("runtime", "spot")}
	cache := &PairTFCache{TimeFrame: "1m", TFSecs: 60, SubNextMS: 1}
	if _, got := cache.fillLacksWithRuntimeDeps(deps, &orm.ExSymbol{ID: 7, Symbol: "RUNTIME/USDT"}, 60, 2, 3); got == nil || got.Code != errs.CodeCancel {
		t.Fatalf("canceled gap repair error = %v, want cancellation", got)
	}
	downEmitHourKlines(deps, deps.Symbols, &LiveProvider{}, map[int32]int64{7: 1})
}
