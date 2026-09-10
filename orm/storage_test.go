package orm

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
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
