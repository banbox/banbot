package orm

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestStorageCloseConcurrent(t *testing.T) {
	var closed atomic.Int32
	storage := &Storage{closeFn: func() { closed.Add(1) }}
	var calls sync.WaitGroup
	for index := 0; index < 32; index++ {
		calls.Add(1)
		go func() {
			defer calls.Done()
			storage.Close()
		}()
	}
	calls.Wait()
	if closed.Load() != 1 {
		t.Fatalf("pool closed %d times", closed.Load())
	}
}

func TestStorageQueryBackendAndCoordination(t *testing.T) {
	quest := NewStorage(nil, true, "database:first")
	postgres := NewStorage(nil, false, "database:second")
	if !quest.NewQueries(nil).isQuestDB() || postgres.NewQueries(nil).isQuestDB() {
		t.Fatal("query backend is not storage-bound")
	}
	if quest.ProcessLockRoot() == postgres.ProcessLockRoot() {
		t.Fatal("independent databases share coordination root")
	}
	if quest.ProcessLockRoot() != NewStorage(nil, true, quest.Identity()).ProcessLockRoot() {
		t.Fatal("same database does not share coordination root")
	}
	if quest.NewQueries(nil).WithTx(nil).Storage() != quest {
		t.Fatal("transaction loses storage binding")
	}
	exchange := &listDateExchange{}
	q := quest.NewQueries(nil).WithExchange(exchange).WithKlineRuntimeOptions(KlineRuntimeOptions{NowMS: 42, ClockValid: true})
	tx := q.WithTx(nil)
	if tx.exchange != exchange || tx.klineRuntimeOptions().NowMS != 42 {
		t.Fatal("transaction loses explicit query dependencies")
	}
}

func TestStorageRangeCacheIsolation(t *testing.T) {
	first := NewStorage(nil, true, t.Name()+":first").NewQueries(nil)
	second := NewStorage(nil, true, t.Name()+":second").NewQueries(nil)
	sibling := NewStorage(nil, true, first.Storage().Identity()).NewQueries(nil)
	key := first.srangesKey(42, "kline_1m", "1m")
	srangesCacheLock.Lock()
	srangesCache[key] = &srangesCacheEntry{spans: []srangeSpan{{StartMs: 100, StopMs: 200, HasData: true}}}
	srangesCacheLock.Unlock()
	t.Cleanup(func() { first.deleteCachedSRanges(42, "kline_1m", "1m") })
	if spans, ok := second.cachedSRangeSpans(42, "kline_1m", "1m"); ok || len(spans) != 0 {
		t.Fatal("another database observed uncommitted WAL coverage")
	}
	if spans, ok := sibling.cachedSRangeSpans(42, "kline_1m", "1m"); !ok || len(spans) != 1 {
		t.Fatal("same database lost read-after-write coverage")
	}
	second.deleteCachedSRanges(42, "kline_1m", "1m")
	if _, ok := first.cachedSRangeSpans(42, "kline_1m", "1m"); !ok {
		t.Fatal("another database invalidated coverage")
	}
}

func TestStorageBoundQueriesDoNotFallBackToLegacySymbols(t *testing.T) {
	previous := swapDefaultSymbolState(NewSymbolState())
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
	legacy := &ExSymbol{ID: 7, Exchange: "legacy", Market: "spot", Symbol: "LEGACY/USDT"}
	if err := loadDefaultSymbolState().CacheExSymbolChecked(legacy); err != nil {
		t.Fatal(err)
	}
	explicit := NewSymbolStateWithIdentity("runtime", "spot")
	bound := &ExSymbol{ID: 7, Exchange: "runtime", Market: "spot", Symbol: "BOUND/USDT"}
	if err := explicit.CacheExSymbolChecked(bound); err != nil {
		t.Fatal(err)
	}
	q := NewWithStorage(nil, NewStorage(nil, true, "storage:bound"))
	if got := q.symbolByID(7); got != nil {
		t.Fatalf("storage-bound query fell back to legacy symbol %q", got.Symbol)
	}
	if got := q.WithSeriesSymbolState(explicit).symbolByID(7); got == nil || got.ID != bound.ID || got.Symbol != bound.Symbol {
		t.Fatalf("explicit symbol state was not used: got=%+v", got)
	}
}

func TestExplicitStorageSymbolMutationsRequireState(t *testing.T) {
	q := NewWithStorage(nil, NewStorage(nil, true, "storage:requires-symbol-state"))
	if _, err := q.AddSymbols(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "explicit symbol state") {
		t.Fatalf("AddSymbols error = %v, want explicit symbol state error", err)
	}
	if err := q.SetListMS(context.Background(), SetListMSParams{ID: 1}); err == nil || !strings.Contains(err.Error(), "explicit symbol state") {
		t.Fatalf("SetListMS error = %v, want explicit symbol state error", err)
	}
	if err := q.SetAggRules(context.Background(), SetAggRulesParams{ID: 1}); err == nil || !strings.Contains(err.Error(), "explicit symbol state") {
		t.Fatalf("SetAggRules error = %v, want explicit symbol state error", err)
	}
	if err := q.LoadExgSymbols("binance"); err == nil || !strings.Contains(err.Error(), "explicit symbol state") {
		t.Fatalf("LoadExgSymbols error = %v, want explicit symbol state error", err)
	}
	if _, err := q.WithKlineRuntimeOptions(KlineRuntimeOptions{NowMS: 1, ClockValid: true}).GetAdjs(7); err == nil || !strings.Contains(err.Error(), "explicit symbol state") {
		t.Fatalf("GetAdjs error = %v, want explicit symbol state error", err)
	}
}

func TestGetAdjsUsesBoundStorageConnection(t *testing.T) {
	previous := swapDefaultSymbolState(NewSymbolState())
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
	if err := loadDefaultSymbolState().CacheExSymbolChecked(&ExSymbol{
		ID: 7, Exchange: "legacy", Market: "spot", Symbol: "LEGACY/USDT",
	}); err != nil {
		t.Fatal(err)
	}

	state := NewSymbolStateWithIdentity("runtime", banexg.MarketSpot)
	if err := state.CacheExSymbolChecked(&ExSymbol{
		ID: 7, Exchange: "runtime", Market: banexg.MarketSpot, Symbol: "BOUND/USDT",
	}); err != nil {
		t.Fatal(err)
	}
	storage := NewStorage(nil, true, "storage:get-adjs")
	q := NewWithStorage(nil, storage).WithSeriesSymbolState(state).WithKlineRuntimeOptions(KlineRuntimeOptions{
		NowMS: 123, ClockValid: true,
	})
	if _, err := q.GetAdjs(7); err == nil || !strings.Contains(err.Error(), "storage pool is not configured") {
		t.Fatalf("GetAdjs error = %v, want bound storage pool error", err)
	}
}

func TestExplicitGetAdjsRequiresKlineRuntimeOptions(t *testing.T) {
	state := NewSymbolStateWithIdentity("runtime", banexg.MarketSpot)
	if err := state.CacheExSymbolChecked(&ExSymbol{
		ID: 7, Exchange: "runtime", Market: banexg.MarketSpot, Symbol: "BOUND/USDT",
	}); err != nil {
		t.Fatal(err)
	}
	q := NewWithStorage(nil, NewStorage(nil, true, "storage:get-adjs-options")).WithSeriesSymbolState(state)
	if _, err := q.GetAdjs(7); err == nil || !strings.Contains(err.Error(), "K-line runtime options") {
		t.Fatalf("GetAdjs error = %v, want missing K-line runtime options error", err)
	}
}

func TestExplicitSeriesQueryRequiresKlineRuntimeOptions(t *testing.T) {
	state := NewSymbolStateWithIdentity("runtime", banexg.MarketSpot)
	exs := &ExSymbol{ID: 7, Exchange: "runtime", Market: banexg.MarketSpot, Symbol: "BOUND/USDT"}
	if err := state.CacheExSymbolChecked(exs); err != nil {
		t.Fatal(err)
	}
	q := NewWithStorage(nil, NewStorage(nil, true, "storage:series-options")).WithSeriesSymbolState(state)
	if _, err := q.QuerySeriesFields(exs, "1m", nil, 1, 2, 0, false); err == nil || !strings.Contains(err.Error(), "K-line runtime options") {
		t.Fatalf("QuerySeriesFields error = %v, want missing K-line runtime options error", err)
	}
}

func TestExplicitKlineRuntimeRequiresClockForQueryAndWrite(t *testing.T) {
	state := NewSymbolStateWithIdentity("runtime", banexg.MarketSpot)
	exs := &ExSymbol{ID: 7, Exchange: "runtime", Market: banexg.MarketSpot, Symbol: "BOUND/USDT"}
	if err := state.CacheExSymbolChecked(exs); err != nil {
		t.Fatal(err)
	}
	exchange := &listDateExchange{info: &banexg.ExgInfo{ID: "runtime", MarketType: banexg.MarketSpot}}
	q := NewWithStorage(nil, NewStorage(nil, true, "storage:missing-clock")).
		WithSeriesSymbolState(state).
		WithExchange(exchange).
		WithKlineRuntimeOptions(KlineRuntimeOptions{})
	if _, err := q.QuerySeriesFields(exs, "1m", nil, 1, 2, 0, false); err == nil ||
		err.Code != core.ErrBadConfig || !strings.Contains(err.Error(), "runtime clock") {
		t.Fatalf("QuerySeriesFields error = %v, want missing runtime clock error", err)
	}
	if err := q.SetUnfinish(exs.ID, "1m", 2, nil); err == nil ||
		err.Code != core.ErrBadConfig || !strings.Contains(err.Error(), "runtime clock") {
		t.Fatalf("SetUnfinish error = %v, want missing runtime clock error", err)
	}
	wantClockErr := func(name string, err *errs.Error) {
		t.Helper()
		if err == nil || err.Code != core.ErrBadConfig || !strings.Contains(err.Error(), "runtime clock") {
			t.Fatalf("%s error = %v, want missing runtime clock error", name, err)
		}
	}
	_, insertErr := q.InsertKLines("1m", exs.ID, []*banexg.Kline{{Time: 1}})
	wantClockErr("InsertKLines", insertErr)
	_, insertAutoErr := q.InsertKLinesAuto("1m", exs, []*banexg.Kline{{Time: 1}}, false)
	wantClockErr("InsertKLinesAuto", insertAutoErr)
	_, seriesErr := q.InsertOHLCVSeries("1m", exs.ID, []*DataSeries{{Sid: exs.ID, TimeMS: 1}})
	wantClockErr("InsertOHLCVSeries", seriesErr)
	_, seriesAutoErr := q.InsertOHLCVSeriesAuto("1m", exs, []*DataSeries{{Sid: exs.ID, TimeMS: 1}}, false)
	wantClockErr("InsertOHLCVSeriesAuto", seriesAutoErr)
	wantClockErr("UpdateKRange", q.UpdateKRange(exs, "1m", 1, 2, false))
}

func TestExplicitSeriesQueryRequiresExchangeAdapter(t *testing.T) {
	state := NewSymbolStateWithIdentity("runtime", banexg.MarketSpot)
	exs := &ExSymbol{ID: 7, Exchange: "runtime", Market: banexg.MarketSpot, Symbol: "BOUND/USDT"}
	if err := state.CacheExSymbolChecked(exs); err != nil {
		t.Fatal(err)
	}
	q := NewWithStorage(nil, NewStorage(nil, true, "storage:series-exchange")).
		WithSeriesSymbolState(state).
		WithKlineRuntimeOptions(KlineRuntimeOptions{NowMS: 123, ClockValid: true})
	if _, err := q.QuerySeriesFields(exs, "1m", nil, 1, 2, 0, false); err == nil || !strings.Contains(err.Error(), "exchange adapter") {
		t.Fatalf("QuerySeriesFields error = %v, want missing exchange adapter error", err)
	}
}

func TestExplicitQueryStateIsUsedByEnsureListDates(t *testing.T) {
	q := NewWithStorage(nil, NewStorage(nil, false, "storage:ensure-list-dates"))
	exchange := &listDateExchange{info: &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot}}
	if err := EnsureListDatesWithState(q, nil, exchange, nil, nil); err == nil || !strings.Contains(err.Error(), "explicit symbol state") {
		t.Fatalf("EnsureListDates error = %v, want explicit symbol state error", err)
	}

	state := NewSymbolStateWithIdentity("test", banexg.MarketSpot)
	q = q.WithSeriesSymbolState(state)
	if got, err := resolveQuerySymbolState(q, nil); err != nil || got != state {
		t.Fatalf("query-bound state = %p, err=%v; want %p", got, err, state)
	}
}

func TestExplicitSymbolStateConnDoesNotFallBackToLegacyStorage(t *testing.T) {
	state := NewSymbolStateWithAllocator(NewSIDAllocatorForStorage("explicit:"+t.Name(), t.TempDir()))
	_, _, err := state.Conn(context.Background())
	if err == nil || !strings.Contains(err.Error(), "explicit symbol state") {
		t.Fatalf("explicit symbol state connection error = %v, want fail-closed storage error", err)
	}
}

func TestStorageBoundCompactStateAndRewriteIntentAreIsolated(t *testing.T) {
	first := NewStorage(nil, true, t.Name()+":first")
	second := NewStorage(nil, true, t.Name()+":second")
	q := first.NewQueries(nil)
	q.MarkTableForCompact("sranges_q", 3)
	firstState := compactStateForRoot(first.ProcessLockRoot())
	secondState := compactStateForRoot(second.ProcessLockRoot())
	firstState.mu.Lock()
	firstPending := firstState.tables["sranges_q"].pendingRows
	firstState.mu.Unlock()
	secondState.mu.Lock()
	_, secondSeen := secondState.tables["sranges_q"]
	secondState.mu.Unlock()
	if firstPending != 3 || secondSeen {
		t.Fatalf("compact state crossed storage owners: first=%d secondSeen=%t", firstPending, secondSeen)
	}
	compactStatesMu.Lock()
	delete(compactStates, first.ProcessLockRoot())
	delete(compactStates, second.ProcessLockRoot())
	compactStatesMu.Unlock()

	firstRoot := t.TempDir()
	secondRoot := t.TempDir()
	intent := &questRewriteSwapIntent{Source: "source", Temp: "temp", Backup: "backup"}
	if err := saveQuestRewriteSwapIntentAtRoot(intent, firstRoot); err != nil {
		t.Fatal(err)
	}
	if got, err := questRewriteIntentStoreForRoot(secondRoot).Load("source"); err != nil || got != nil {
		t.Fatalf("rewrite intent leaked to another storage: intent=%+v err=%v", got, err)
	}
	if got, err := questRewriteIntentStoreForRoot(firstRoot).Load("source"); err != nil || got == nil {
		t.Fatalf("rewrite intent missing from owner root: intent=%+v err=%v", got, err)
	}
	if err := clearQuestRewriteSwapIntentAtRoot("source", firstRoot); err != nil {
		t.Fatal(err)
	}
}
