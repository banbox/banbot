package orm

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestQueriesSetAggRulesUpdatesExplicitStateOnly(t *testing.T) {
	oldQuestDB := IsQuestDB
	defer func() { IsQuestDB = oldQuestDB }()

	for _, questDB := range []bool{false, true} {
		name := "postgres"
		if questDB {
			name = "questdb"
		}
		t.Run(name, func(t *testing.T) {
			IsQuestDB = questDB
			defaultState := NewSymbolState()
			previous := swapDefaultSymbolState(defaultState)
			defer swapDefaultSymbolState(previous)

			const sid = int32(7)
			explicitState := NewSymbolState()
			explicitState.CacheExSymbol(&ExSymbol{
				ID: sid, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT", AggRules: "old",
			})
			db := &setAggRulesDBStub{item: &ExSymbol{
				ID: sid, Exchange: "binance", ExgReal: "binance", Market: "spot", Symbol: "BTC/USDT", AggRules: "old",
			}}

			if err := New(db).WithSymbolState(explicitState).SetAggRules(context.Background(), SetAggRulesParams{
				ID: sid, AggRules: `{"price":"mid"}`,
			}); err != nil {
				t.Fatalf("SetAggRules returned error: %v", err)
			}
			if got := explicitState.GetSymbolByID(sid); got == nil || got.AggRules != `{"price":"mid"}` {
				t.Fatalf("explicit state was not updated: %+v", got)
			}
			if got := defaultState.GetSymbolByID(sid); got != nil {
				t.Fatalf("default state was polluted: %+v", got)
			}
		})
	}
}

func TestCatalogCheckedCachePropagatesSIDConflict(t *testing.T) {
	state := NewSymbolState()
	if err := state.CacheExSymbolChecked(&ExSymbol{
		ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.CacheExSymbolChecked(&ExSymbol{
		ID: 7, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT",
	}); err == nil || !strings.Contains(err.Error(), "already cached") {
		t.Fatalf("checked catalog conflict = %v, want propagated error", err)
	}
}

func TestConcurrentCatalogCacheReservationsAreExclusive(t *testing.T) {
	allocator := NewSIDAllocator()
	const workers = 16
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			state := NewSymbolStateWithAllocator(allocator)
			<-start
			results <- state.CacheExSymbolChecked(&ExSymbol{
				ID: int32(i + 1), Exchange: "test", Market: "spot", Symbol: "BTC/USDT",
			})
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent cache successes = %d, want 1", successes)
	}
	reserved := allocator.reservationSID(exSymbolKey("test", "spot", "BTC/USDT"))
	if reserved <= 0 {
		t.Fatalf("shared logical reservation was not published: %d", reserved)
	}

	conflict := NewSymbolStateWithAllocator(allocator)
	if err := conflict.CacheExSymbolChecked(&ExSymbol{
		ID: reserved, Exchange: "other", Market: "spot", Symbol: "ETH/USDT",
	}); err == nil {
		t.Fatal("cached SID was reused for another logical symbol")
	}
}

func TestUpdateSymbolBaseReservesSID(t *testing.T) {
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocator(allocator)
	base := &ExSymbol{ID: 7, Exchange: "test", Market: "spot", Symbol: "BTC/USDT"}
	if got := state.updateListMS(base.ID, 10, 20, base); got == nil {
		t.Fatal("base symbol was not cached")
	}
	other := NewSymbolStateWithAllocator(allocator)
	if err := other.CacheExSymbolChecked(&ExSymbol{
		ID: 8, Exchange: base.Exchange, Market: base.Market, Symbol: base.Symbol,
	}); err == nil {
		t.Fatal("base symbol SID reservation was bypassed")
	}
}

func TestQueriesSetListMSUpdatesExplicitStateOnly(t *testing.T) {
	oldQuestDB := IsQuestDB
	defer func() { IsQuestDB = oldQuestDB }()

	for _, questDB := range []bool{false, true} {
		name := "postgres"
		if questDB {
			name = "questdb"
		}
		t.Run(name, func(t *testing.T) {
			IsQuestDB = questDB
			defaultState := NewSymbolState()
			previous := swapDefaultSymbolState(defaultState)
			defer swapDefaultSymbolState(previous)

			const sid = int32(7)
			explicitState := NewSymbolState()
			explicitState.CacheExSymbol(&ExSymbol{
				ID: sid, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
				ListMs: 10, DelistMs: 20,
			})
			db := &setAggRulesDBStub{item: &ExSymbol{
				ID: sid, Exchange: "binance", ExgReal: "binance", Market: "spot", Symbol: "BTC/USDT",
				ListMs: 10, DelistMs: 20,
			}}

			if err := New(db).WithSymbolState(explicitState).SetListMS(context.Background(), SetListMSParams{
				ID: sid, ListMs: 30, DelistMs: 40,
			}); err != nil {
				t.Fatalf("SetListMS returned error: %v", err)
			}
			if got := explicitState.GetSymbolByID(sid); got == nil || got.ListMs != 30 || got.DelistMs != 40 {
				t.Fatalf("explicit state was not updated: %+v", got)
			}
			if got := defaultState.GetSymbolByID(sid); got != nil {
				t.Fatalf("default state was polluted: %+v", got)
			}
		})
	}
}

func TestAddSymbolsAllocatesUniqueSIDsAcrossStates(t *testing.T) {
	oldQuestDB := IsQuestDB
	oldDataDir := config.DataDir
	config.DataDir = t.TempDir()
	defer func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
	}()

	for _, questDB := range []bool{false, true} {
		name := "postgres"
		if questDB {
			name = "questdb"
		}
		t.Run(name, func(t *testing.T) {
			IsQuestDB = questDB
			db := &sidAllocDBStub{hideMax: true, queryDelay: time.Millisecond, execDelay: time.Millisecond}
			allocator := NewSIDAllocator()

			const count = 16
			var wg sync.WaitGroup
			errs := make(chan error, count)
			for i := 0; i < count; i++ {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()
					state := NewSymbolStateWithAllocator(allocator)
					if err := BindExSymbolRecoveryDir(state, config.DataDir); err != nil {
						errs <- err
						return
					}
					_, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{{
						Exchange: "test", Market: "spot", Symbol: "PAIR-" + string(rune('A'+i)),
					}})
					if err != nil {
						errs <- err
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatalf("AddSymbols returned error: %v", err)
			}

			db.mu.Lock()
			defer db.mu.Unlock()
			seen := make(map[int32]bool, len(db.ids))
			for _, sid := range db.ids {
				if seen[sid] {
					t.Fatalf("duplicate SID allocated: %d (all=%v)", sid, db.ids)
				}
				seen[sid] = true
			}
			if len(db.ids) != count {
				t.Fatalf("inserted SID count = %d, want %d", len(db.ids), count)
			}
		})
	}
}

func TestEnsureSymbolsDefaultStateUsesLegacyRecoveryRoot(t *testing.T) {
	oldQuestDB, oldDataDir := IsQuestDB, config.DataDir
	IsQuestDB = true
	config.DataDir = t.TempDir()
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
	})

	defaultState := NewSymbolState()
	previous := swapDefaultSymbolState(defaultState)
	t.Cleanup(func() { swapDefaultSymbolState(previous) })

	dbCalls := 0
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		dbCalls++
		return visibilityRowStub{}
	}}
	query := newEnsureSymbolQueries(New(db), defaultState)

	if _, err := query.AddSymbols(context.Background(), nil); err != nil {
		t.Fatalf("default EnsureSymbols query requires explicit recovery root: %v", err)
	}
	if dbCalls != 0 {
		t.Fatalf("empty default EnsureSymbols query made %d database calls", dbCalls)
	}
}

func TestMaxSIDQueryScanErrorsStopAllocation(t *testing.T) {
	oldQuestDB := IsQuestDB
	defer func() { IsQuestDB = oldQuestDB }()

	for _, questDB := range []bool{false, true} {
		name := "postgres"
		if questDB {
			name = "questdb"
		}
		t.Run(name, func(t *testing.T) {
			IsQuestDB = questDB
			state := NewSymbolState()
			if err := BindExSymbolRecoveryDir(state, t.TempDir()); err != nil {
				t.Fatal(err)
			}
			db := &sidAllocDBStub{maxErr: errors.New("max scan failed")}
			_, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{{
				Exchange: "test", Market: "spot", Symbol: "BTC/USDT",
			}})
			if err == nil || !strings.Contains(err.Error(), "max scan failed") {
				t.Fatalf("AddSymbols error = %v, want max scan failure", err)
			}
			if state.MaxSID() != 0 || state.GetExSymbol2("test", "spot", "BTC/USDT") != nil {
				t.Fatalf("failed max query assigned state: max=%d symbols=%v", state.MaxSID(), state.GetExSymbols("", ""))
			}
			db.mu.Lock()
			inserted := len(db.ids)
			db.mu.Unlock()
			if inserted != 0 {
				t.Fatalf("failed max query inserted %d rows", inserted)
			}
		})
	}
}

func TestAddSymbolsRejectsForeignIdentityBeforeSideEffects(t *testing.T) {
	oldQuestDB := IsQuestDB
	t.Cleanup(func() { IsQuestDB = oldQuestDB })

	for _, questDB := range []bool{false, true} {
		name := "postgres"
		if questDB {
			name = "questdb"
		}
		t.Run(name, func(t *testing.T) {
			IsQuestDB = questDB
			dataDir := t.TempDir()
			state := NewSymbolStateWithIdentity("binance", "spot")
			allocator := state.sidAllocator()
			if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
				t.Fatal(err)
			}
			dbCalls := 0
			db := &visibilityDBStub{
				exec: func(string, ...interface{}) (pgconn.CommandTag, error) {
					dbCalls++
					return pgconn.CommandTag{}, nil
				},
				queryRow: func(string, ...interface{}) pgx.Row {
					dbCalls++
					return visibilityRowStub{scan: func(...interface{}) error { return nil }}
				},
			}

			n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{
				{Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
				{Exchange: "okx", Market: "spot", Symbol: "ETH/USDT"},
			})
			if n != 0 || err == nil || !strings.Contains(err.Error(), "does not match symbol state identity") {
				t.Fatalf("AddSymbols = (%d, %v), want foreign identity error", n, err)
			}
			if dbCalls != 0 {
				t.Fatalf("foreign identity allowed %d database calls", dbCalls)
			}
			if state.MaxSID() != 0 || allocator.max.Load() != 0 || state.SymbolCount() != 0 {
				t.Fatalf("foreign identity changed state: max=%d allocator=%d symbols=%v",
					state.MaxSID(), allocator.max.Load(), state.GetExSymbols("", ""))
			}
			markers, globErr := filepath.Glob(filepath.Join(dataDir, "recovery", "exsymbol-*.pending.json"))
			if globErr != nil || len(markers) != 0 {
				t.Fatalf("foreign identity created recovery marker: markers=%v err=%v", markers, globErr)
			}
		})
	}
}

func TestAddSymbolsPgCachesStoredConflictIdentity(t *testing.T) {
	oldQuestDB := IsQuestDB
	IsQuestDB = false
	defer func() { IsQuestDB = oldQuestDB }()

	stored := ExSymbol{
		ID: 7, Exchange: "test", ExgReal: "stored", Market: "spot", Symbol: "BTC/USDT",
		Combined: true, ListMs: 11, DelistMs: 22, AggRules: `{"price":"last"}`,
	}
	db := &sidAllocDBStub{
		maxID:      7,
		items:      map[int32]ExSymbol{7: stored},
		identities: map[string]int32{exSymbolKey(stored.Exchange, stored.Market, stored.Symbol): 7},
	}
	explicit := NewSymbolState()
	defaultState := NewSymbolState()
	previous := swapDefaultSymbolState(defaultState)
	defer swapDefaultSymbolState(previous)

	if _, err := New(db).WithSymbolState(explicit).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: "test", ExgReal: "requested", Market: "spot", Symbol: "BTC/USDT",
	}}); err != nil {
		t.Fatal(err)
	}
	got := explicit.GetExSymbol2("test", "spot", "BTC/USDT")
	if got == nil || *got != stored {
		t.Fatalf("explicit state cached %+v, want stored row %+v", got, stored)
	}
	if explicit.GetSymbolByID(8) != nil {
		t.Fatal("conflict published invented SID 8")
	}
	if defaultState.SymbolCount() != 0 {
		t.Fatalf("default state was polluted: %v", defaultState.GetExSymbols("", ""))
	}
}

func TestAddSymbolsRejectsReservationCanonicalMetadataConflict(t *testing.T) {
	oldQuestDB := IsQuestDB
	IsQuestDB = false
	t.Cleanup(func() { IsQuestDB = oldQuestDB })

	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "test", "spot")
	old := &ExSymbol{ID: 7, Exchange: "test", Market: "spot", Symbol: "BTC/USDT", ListMs: 11}
	state.CacheExSymbol(old)
	if got := allocator.reserveSID(exSymbolKey(old.Exchange, old.Market, old.Symbol), old.ID); got != old.ID {
		t.Fatalf("initial reservation = %d, want %d", got, old.ID)
	}
	db := &sidAllocDBStub{}

	n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: old.Exchange, Market: old.Market, Symbol: old.Symbol, ListMs: 22,
	}})
	if n != 0 || err == nil || !strings.Contains(err.Error(), "canonical metadata") {
		t.Fatalf("AddSymbols = (%d, %v), want canonical metadata conflict", n, err)
	}
	if got := state.GetSymbolByID(old.ID); got == nil || got.ListMs != old.ListMs {
		t.Fatalf("canonical metadata was overwritten: %+v", got)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.ids) != 0 {
		t.Fatalf("metadata conflict inserted %d rows", len(db.ids))
	}
}

func TestSharedAllocatorResetDoesNotReuseSID(t *testing.T) {
	allocator := NewSIDAllocator()
	first := NewSymbolStateWithAllocator(allocator)
	second := NewSymbolStateWithAllocator(allocator)
	if got := first.NextSID(); got != 1 {
		t.Fatalf("first SID = %d, want 1", got)
	}
	first.Reset()
	second.Reset()
	if got := second.NextSID(); got != 2 {
		t.Fatalf("SID after shared reset = %d, want 2", got)
	}
	if got := first.NextSID(); got != 3 {
		t.Fatalf("SID after peer allocation = %d, want 3", got)
	}
}

func TestSetExSymbolsFencesSharedAllocator(t *testing.T) {
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocator(allocator)
	if err := state.SetExSymbols([]*ExSymbol{{
		ID: 41, Exchange: "test", Market: "spot", Symbol: "FROZEN/USDT",
	}}); err != nil {
		t.Fatal(err)
	}
	if got := state.NextSID(); got != 42 {
		t.Fatalf("SID after frozen catalog = %d, want 42", got)
	}
	state.Reset()
	if got := state.NextSID(); got != 43 {
		t.Fatalf("SID after frozen catalog reset = %d, want 43", got)
	}
}

func TestSetExSymbolsKeepsLogicalSIDAcrossReplacement(t *testing.T) {
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocator(allocator)
	original := &ExSymbol{ID: 11, Exchange: "test", Market: "spot", Symbol: "OLD/USDT"}
	if err := state.SetExSymbols([]*ExSymbol{original}); err != nil {
		t.Fatal(err)
	}

	updated := &ExSymbol{ID: original.ID, Exchange: original.Exchange, Market: original.Market,
		Symbol: original.Symbol, ListMs: 123_000}
	if err := state.SetExSymbols([]*ExSymbol{updated}); err != nil {
		t.Fatalf("same logical symbol replacement returned error: %v", err)
	}
	if got := state.GetExSymbol2(original.Exchange, original.Market, original.Symbol); got == nil || got.ID != original.ID || got.ListMs != updated.ListMs {
		t.Fatalf("same logical symbol replacement = %+v, want sid %d with updated metadata", got, original.ID)
	}

	foreign := &ExSymbol{ID: original.ID, Exchange: original.Exchange, Market: original.Market, Symbol: "NEW/USDT"}
	if err := state.SetExSymbols([]*ExSymbol{foreign}); err == nil || !strings.Contains(err.Error(), "already reserved for logical symbol") {
		t.Fatalf("cross-logical SID replacement error = %v, want reservation conflict", err)
	}
	if got := state.GetSymbolByID(original.ID); got == nil || got.Symbol != original.Symbol || got.ListMs != updated.ListMs {
		t.Fatalf("failed cross-logical replacement changed catalog: %+v", got)
	}
	if got := state.GetExSymbol2(foreign.Exchange, foreign.Market, foreign.Symbol); got != nil {
		t.Fatalf("failed cross-logical replacement published foreign symbol: %+v", got)
	}
}

func TestSetExSymbolsFencesEveryPhysicalSID(t *testing.T) {
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocator(allocator)
	items := []*ExSymbol{
		{ID: 11, Exchange: "test", Market: "spot", Symbol: "PAIR/USDT", ExgReal: "venue"},
		{ID: 12, Exchange: "test", Market: "spot", Symbol: "PAIR/USDT"},
	}
	if err := state.SetExSymbols(items); err != nil {
		t.Fatal(err)
	}
	other := NewSymbolStateWithAllocator(allocator)
	if err := other.CacheExSymbolChecked(&ExSymbol{
		ID: 11, Exchange: "test", Market: "spot", Symbol: "OTHER/USDT",
	}); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatalf("physical SID 11 was not fenced: %v", err)
	}
	if err := other.CacheExSymbolChecked(&ExSymbol{
		ID: 12, Exchange: "test", Market: "spot", Symbol: "OTHER/USDT",
	}); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatalf("physical SID 12 was not fenced: %v", err)
	}
}

func TestSharedAllocatorFollowsCachedAndObservedSIDs(t *testing.T) {
	allocator := NewSIDAllocator()
	catalog := NewSymbolStateWithAllocator(allocator)
	sibling := NewSymbolStateWithAllocator(allocator)

	catalog.CacheExSymbol(&ExSymbol{
		ID: 41, Exchange: "test", Market: "spot", Symbol: "CACHED/USDT",
	})
	if got := sibling.NextSID(); got != 42 {
		t.Fatalf("SID after cached catalog = %d, want 42", got)
	}

	catalog.ObserveSID(60)
	if got := sibling.NextSID(); got != 61 {
		t.Fatalf("SID after observed catalog = %d, want 61", got)
	}

	catalog.SetMaxSID(80)
	if got := sibling.NextSID(); got != 81 {
		t.Fatalf("SID after explicitly set catalog max = %d, want 81", got)
	}
}

func TestSharedAllocatorReservesLogicalIdentity(t *testing.T) {
	allocator := NewSIDAllocator()
	const count = 12
	ids := make(chan int32, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state := NewSymbolStateWithAllocator(allocator)
			unlock := allocator.lockEnsure()
			defer unlock()
			target := &ExSymbol{Exchange: "test", Market: "spot", Symbol: "BTC/USDT"}
			resolved, err := resolveEnsuredSymbolLocked(allocator, state, target)
			if err != nil {
				t.Errorf("initial symbol resolution failed: %v", err)
				return
			}
			if resolved == nil {
				// The test already holds ensureMu so resolution, allocation, and
				// publication remain one atomic identity operation.
				target.ID = nextSymbolSID(allocator, state)
				if err := state.cacheExSymbolChecked(target); err != nil {
					t.Errorf("cache resolved symbol failed: %v", err)
					return
				}
				allocator.reserveSID(exSymbolKey(target.Exchange, target.Market, target.Symbol), target.ID)
			}
			if _, err := resolveEnsuredSymbolLocked(allocator, state, target); err != nil {
				t.Errorf("final symbol resolution failed: %v", err)
				return
			}
			ids <- target.ID
		}()
	}
	wg.Wait()
	close(ids)
	for id := range ids {
		if id != 1 {
			t.Fatalf("reserved identity SID = %d, want 1", id)
		}
	}
}

func TestSharedAllocatorNextSIDIsAtomicAcrossStates(t *testing.T) {
	allocator := NewSIDAllocator()
	const workers = 32
	ids := make(chan int32, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids <- NewSymbolStateWithAllocator(allocator).NextSID()
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[int32]bool, workers)
	for id := range ids {
		if id <= 0 || seen[id] {
			t.Fatalf("shared allocator returned duplicate/invalid SID %d (seen=%v)", id, seen)
		}
		seen[id] = true
	}
	if len(seen) != workers || allocator.max.Load() != workers {
		t.Fatalf("shared allocator SIDs = %v, max=%d; want %d unique SIDs", seen, allocator.max.Load(), workers)
	}
}

func TestAddSymbolsReusesSharedReservationBeforeHiddenWalRow(t *testing.T) {
	oldQuest, oldDataDir := IsQuestDB, config.DataDir
	IsQuestDB = true
	installQuestWaitScript(t, 1)
	dataDir := t.TempDir()
	config.DataDir = dataDir
	t.Cleanup(func() {
		IsQuestDB = oldQuest
		config.DataDir = oldDataDir
	})

	allocator := NewSIDAllocator()
	first := NewSymbolStateWithAllocator(allocator)
	second := NewSymbolStateWithAllocator(allocator)
	if err := BindExSymbolRecoveryDir(first, dataDir); err != nil {
		t.Fatal(err)
	}
	if err := BindExSymbolRecoveryDir(second, dataDir); err != nil {
		t.Fatal(err)
	}
	db := &sidAllocDBStub{hideMax: true}
	arg := []AddSymbolsParams{{Exchange: "test", Market: "spot", Symbol: "BTC/USDT"}}
	if n, err := New(db).WithSymbolState(first).AddSymbols(context.Background(), arg); err != nil || n != 1 {
		t.Fatalf("first AddSymbols = (%d, %v)", n, err)
	}
	if n, err := New(db).WithSymbolState(second).AddSymbols(context.Background(), arg); err != nil || n != 1 {
		t.Fatalf("second AddSymbols = (%d, %v)", n, err)
	}
	db.mu.Lock()
	ids := append([]int32(nil), db.ids...)
	db.mu.Unlock()
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("hidden WAL identity was inserted twice: ids=%v", ids)
	}
	if got := second.GetExSymbol2("test", "spot", "BTC/USDT"); got == nil || got.ID != 1 {
		t.Fatalf("second state did not reuse reservation: %+v", got)
	}
}

func TestConcurrentAddSymbolsSameIdentityUsesOneSharedSID(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	db := &sidAllocDBStub{hideMax: true, queryDelay: time.Millisecond, execDelay: time.Millisecond}
	arg := []AddSymbolsParams{{Exchange: "test", Market: "spot", Symbol: "BTC/USDT"}}

	const workers = 16
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state := NewSymbolStateWithAllocator(allocator)
			if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
				errs <- err
				return
			}
			<-start
			if n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), arg); err != nil || n != 1 {
				errs <- fmt.Errorf("AddSymbols = (%d, %v)", n, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	db.mu.Lock()
	ids := append([]int32(nil), db.ids...)
	db.mu.Unlock()
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("concurrent identity was assigned more than once: ids=%v", ids)
	}
	if got := allocator.reservationSID(exSymbolKey("test", "spot", "BTC/USDT")); got != 1 {
		t.Fatalf("shared identity reservation = %d, want 1", got)
	}
}

func TestAddSymbolsSameIdentityAcrossRecoveryRootsReusesSharedSID(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	reservationDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), reservationDir)
	first := NewSymbolStateWithAllocatorAndIdentity(allocator, "test", "spot")
	second := NewSymbolStateWithAllocatorAndIdentity(allocator, "test", "spot")
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	if err := BindExSymbolRecoveryDir(first, firstDir); err != nil {
		t.Fatal(err)
	}
	if err := BindExSymbolRecoveryDir(second, secondDir); err != nil {
		t.Fatal(err)
	}

	db := &sidAllocDBStub{hideMax: true}
	arg := []AddSymbolsParams{{Exchange: "test", Market: "spot", Symbol: "BTC/USDT"}}
	if n, err := New(db).WithSymbolState(first).AddSymbols(context.Background(), arg); err != nil || n != 1 {
		t.Fatalf("first root AddSymbols = (%d, %v), want one symbol", n, err)
	}
	if n, err := New(db).WithSymbolState(second).AddSymbols(context.Background(), arg); err != nil || n != 1 {
		t.Fatalf("second root AddSymbols = (%d, %v), want one reused symbol", n, err)
	}

	db.mu.Lock()
	ids := append([]int32(nil), db.ids...)
	db.mu.Unlock()
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("same logical symbol crossed roots with duplicate SID insert: ids=%v", ids)
	}
	if got := second.GetExSymbol2("test", "spot", "BTC/USDT"); got == nil || got.ID != 1 {
		t.Fatalf("second root did not reuse shared SID: %+v", got)
	}
}

func TestExchangeDependentEntriesRejectNil(t *testing.T) {
	checks := []struct {
		name string
		call func() *errs.Error
	}{
		{name: "GetExSymbol", call: func() *errs.Error { _, err := GetExSymbol(nil, "BTC/USDT"); return err }},
		{name: "SymbolState.GetExSymbol", call: func() *errs.Error { _, err := NewSymbolState().GetExSymbol(nil, "BTC/USDT"); return err }},
		{name: "InitListDatesWithExchange", call: func() *errs.Error { return InitListDatesWithExchange(NewSymbolState(), nil) }},
		{name: "GetExSHoles exchange", call: func() *errs.Error { _, err := GetExSHoles(nil, &ExSymbol{}, 0, 1, true); return err }},
		{name: "GetExSHoles symbol", call: func() *errs.Error {
			_, err := GetExSHoles(&listDateExchange{info: &banexg.ExgInfo{}}, nil, 0, 1, true)
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			err := check.call()
			if err == nil || err.Code != core.ErrBadConfig {
				t.Fatalf("error = %v, want ErrBadConfig", err)
			}
		})
	}
}

func TestEnsureListDatesKeepsCallerInputInSyncWithState(t *testing.T) {
	oldQuestDB := IsQuestDB
	oldBacktest := core.BackTestMode
	oldNoDownload := config.Data.BTNoKlineDownload
	oldNetDisable := core.NetDisable
	IsQuestDB = false
	core.BackTestMode = false
	config.Data.BTNoKlineDownload = false
	core.NetDisable = false
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		core.BackTestMode = oldBacktest
		config.Data.BTNoKlineDownload = oldNoDownload
		core.NetDisable = oldNetDisable
	})

	state := NewSymbolState()
	defaultState := NewSymbolState()
	previous := swapDefaultSymbolState(defaultState)
	t.Cleanup(func() { swapDefaultSymbolState(previous) })

	input := &ExSymbol{ID: 11, Exchange: "test", Market: banexg.MarketSpot, Symbol: "TEST/USDT"}
	state.CacheExSymbol(input)
	defaultState.CacheExSymbol(&ExSymbol{ID: 12, Exchange: "test", Market: banexg.MarketSpot, Symbol: "DEFAULT/USDT"})
	exchange := &listDateExchange{
		info: &banexg.ExgInfo{ID: input.Exchange, MarketType: banexg.MarketSpot},
	}
	query := New(&setAggRulesDBStub{item: input}).WithSymbolState(state)
	inputs := map[int32]*ExSymbol{input.ID: input}
	if err := query.EnsureListDates(exchange, inputs, nil); err != nil {
		t.Fatal(err)
	}
	if input.ListMs != 0 {
		t.Fatalf("caller input was mutated: %+v", input)
	}
	if got := state.GetSymbolByID(input.ID); got == nil || got.ListMs != 123_000 {
		t.Fatalf("state ListMs = %+v, want 123000", got)
	}
	if got := inputs[input.ID]; got == nil || got.ListMs != 123_000 || got == input {
		t.Fatalf("query input map was not refreshed with a new snapshot: %+v", got)
	}
	if got := defaultState.GetSymbolByID(input.ID); got != nil {
		t.Fatalf("default state was updated by explicit query: %+v", got)
	}
	if got := defaultState.GetSymbolByID(12); got == nil || got.ListMs != 0 {
		t.Fatalf("default state changed unexpectedly: %+v", got)
	}
}

func TestEnsureListDatesSeparatesDefaultAndExplicitStates(t *testing.T) {
	oldQuestDB := IsQuestDB
	oldBacktest := core.BackTestMode
	oldNoDownload := config.Data.BTNoKlineDownload
	oldNetDisable := core.NetDisable
	IsQuestDB = false
	core.BackTestMode = false
	config.Data.BTNoKlineDownload = false
	core.NetDisable = false
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		core.BackTestMode = oldBacktest
		config.Data.BTNoKlineDownload = oldNoDownload
		core.NetDisable = oldNetDisable
	})

	const sid = int32(11)
	defaultState := NewSymbolState()
	previous := swapDefaultSymbolState(defaultState)
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
	explicitState := NewSymbolState()
	explicitInput := &ExSymbol{ID: sid, Exchange: "test", Market: banexg.MarketSpot, Symbol: "EXPLICIT/USDT"}
	defaultInput := &ExSymbol{ID: sid, Exchange: "test", Market: banexg.MarketSpot, Symbol: "DEFAULT/USDT"}
	explicitState.CacheExSymbol(explicitInput)
	defaultState.CacheExSymbol(defaultInput)
	explicitBefore := explicitState.GetSymbolByID(sid)
	defaultBefore := defaultState.GetSymbolByID(sid)
	explicitMap := map[int32]*ExSymbol{sid: explicitInput}
	defaultMap := map[int32]*ExSymbol{sid: defaultInput}
	exchange := &listDateExchange{
		info: &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot},
	}
	sess := New(&setAggRulesDBStub{item: explicitInput})

	if err := EnsureListDatesWithState(sess, explicitState, exchange, explicitMap, nil); err != nil {
		t.Fatal(err)
	}
	if err := EnsureListDates(sess, exchange, defaultMap, nil); err != nil {
		t.Fatal(err)
	}
	if got := exchange.calls.Load(); got != 2 {
		t.Fatalf("FetchOHLCV calls = %d, want one per state", got)
	}
	if explicitInput.ListMs != 0 || defaultInput.ListMs != 0 {
		t.Fatalf("caller inputs were mutated: explicit=%+v default=%+v", explicitInput, defaultInput)
	}
	explicitAfter := explicitState.GetSymbolByID(sid)
	defaultAfter := defaultState.GetSymbolByID(sid)
	if explicitAfter == nil || explicitAfter.ListMs != 123_000 || explicitAfter == explicitBefore {
		t.Fatalf("explicit state snapshot = %+v, before=%p", explicitAfter, explicitBefore)
	}
	if defaultAfter == nil || defaultAfter.ListMs != 123_000 || defaultAfter == defaultBefore {
		t.Fatalf("default state snapshot = %+v, before=%p", defaultAfter, defaultBefore)
	}
	if explicitBefore.ListMs != 0 || defaultBefore.ListMs != 0 {
		t.Fatalf("published snapshots were mutated: explicit=%+v default=%+v", explicitBefore, defaultBefore)
	}
	if explicitMap[sid] != explicitAfter || defaultMap[sid] != defaultAfter {
		t.Fatalf("caller maps do not use current snapshots: explicit=%p/%p default=%p/%p",
			explicitMap[sid], explicitAfter, defaultMap[sid], defaultAfter)
	}
}

func TestEnsureListDatesDoesNotUpdateReplacementCatalog(t *testing.T) {
	oldQuestDB := IsQuestDB
	oldBacktest := core.BackTestMode
	oldNoDownload := config.Data.BTNoKlineDownload
	oldNetDisable := core.NetDisable
	IsQuestDB = false
	core.BackTestMode = false
	config.Data.BTNoKlineDownload = false
	core.NetDisable = false
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		core.BackTestMode = oldBacktest
		config.Data.BTNoKlineDownload = oldNoDownload
		core.NetDisable = oldNetDisable
	})

	state := NewSymbolState()
	oldItem := &ExSymbol{ID: 11, Exchange: "test", Market: banexg.MarketSpot, Symbol: "OLD/USDT"}
	state.CacheExSymbol(oldItem)
	started := make(chan struct{})
	release := make(chan struct{})
	exchange := &listDateExchange{
		info:    &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot},
		started: started,
		release: release,
	}
	db := &setAggRulesDBStub{item: oldItem}
	done := make(chan *errs.Error, 1)
	go func() {
		done <- EnsureListDatesWithState(New(db), state, exchange, map[int32]*ExSymbol{oldItem.ID: oldItem}, nil)
	}()
	<-started
	newItem := &ExSymbol{ID: oldItem.ID + 1, Exchange: "test", Market: banexg.MarketSpot, Symbol: "NEW/USDT"}
	if err := state.SetExSymbols([]*ExSymbol{newItem}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := state.GetSymbolByID(newItem.ID); got == nil || got.Symbol != newItem.Symbol || got.ListMs != 0 {
		t.Fatalf("replacement catalog was overwritten: %+v", got)
	}
	if got := db.execs.Load(); got != 0 {
		t.Fatalf("stale listing update executed %d database writes", got)
	}
}

type listDateExchange struct {
	banexg.BanExchange
	info    *banexg.ExgInfo
	calls   atomic.Int32
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (e *listDateExchange) Info() *banexg.ExgInfo { return e.info }

func (e *listDateExchange) HasApi(string, string) bool { return true }

func (e *listDateExchange) FetchOHLCV(string, string, int64, int, map[string]interface{}) ([]*banexg.Kline, *errs.Error) {
	e.calls.Add(1)
	if e.started != nil {
		e.once.Do(func() { close(e.started) })
	}
	if e.release != nil {
		<-e.release
	}
	return []*banexg.Kline{{Time: 123_000}}, nil
}

type setAggRulesDBStub struct {
	item  *ExSymbol
	execs atomic.Int32
}

func (s *setAggRulesDBStub) Exec(_ context.Context, _ string, _ ...interface{}) (pgconn.CommandTag, error) {
	s.execs.Add(1)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (s *setAggRulesDBStub) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("unexpected Query call")
}

func (s *setAggRulesDBStub) QueryRow(_ context.Context, sql string, _ ...interface{}) pgx.Row {
	if strings.Contains(sql, "LATEST BY sid") {
		item := *s.item
		return symbolStateRowStub{scan: func(dest ...interface{}) error {
			*dest[0].(*int32) = item.ID
			*dest[1].(*string) = item.Exchange
			*dest[2].(*string) = item.ExgReal
			*dest[3].(*string) = item.Market
			*dest[4].(*string) = item.Symbol
			*dest[5].(*bool) = item.Combined
			*dest[6].(*int64) = item.ListMs
			*dest[7].(*int64) = item.DelistMs
			*dest[8].(*string) = item.AggRules
			return nil
		}}
	}
	if strings.Contains(sql, "SELECT max(ts)") {
		return symbolStateRowStub{scan: func(dest ...interface{}) error {
			maxTS := time.Now().UTC().Add(time.Second)
			*dest[0].(**time.Time) = &maxTS
			return nil
		}}
	}
	panic("unexpected QueryRow call: " + sql)
}

func (s *setAggRulesDBStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unexpected CopyFrom call")
}

type symbolStateRowStub struct {
	scan func(dest ...interface{}) error
}

func (r symbolStateRowStub) Scan(dest ...interface{}) error {
	return r.scan(dest...)
}

type sidAllocDBStub struct {
	mu         sync.Mutex
	maxID      int32
	ids        []int32
	items      map[int32]ExSymbol
	identities map[string]int32
	hideMax    bool
	maxErr     error
	queryDelay time.Duration
	execDelay  time.Duration
}

func (s *sidAllocDBStub) Exec(_ context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	if !strings.Contains(sql, "INSERT INTO exsymbol") {
		return pgconn.CommandTag{}, errors.New("unexpected Exec: " + sql)
	}
	if s.execDelay > 0 {
		time.Sleep(s.execDelay)
	}
	sid := args[0].(int32)
	item := ExSymbol{ID: sid}
	if len(args) == 9 {
		item.Exchange = args[1].(string)
		item.ExgReal = args[2].(string)
		item.Market = args[3].(string)
		item.Symbol = args[4].(string)
		item.Combined = args[5].(bool)
		item.ListMs = args[6].(int64)
		item.DelistMs = args[7].(int64)
		item.AggRules = args[8].(string)
	} else {
		item.Exchange = args[2].(string)
		item.ExgReal = args[3].(string)
		item.Market = args[4].(string)
		item.Symbol = args[5].(string)
		item.Combined = args[6].(bool)
		item.ListMs = args[7].(int64)
		item.DelistMs = args[8].(int64)
		item.AggRules = args[9].(string)
	}
	s.mu.Lock()
	if s.items == nil {
		s.items = make(map[int32]ExSymbol)
	}
	s.ids = append(s.ids, sid)
	s.items[sid] = item
	if sid > s.maxID {
		s.maxID = sid
	}
	s.mu.Unlock()
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (s *sidAllocDBStub) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("unexpected Query call")
}

func (s *sidAllocDBStub) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	if s.queryDelay > 0 {
		time.Sleep(s.queryDelay)
	}
	if strings.Contains(sql, "SELECT max(id) FROM exsymbol") || strings.Contains(sql, "SELECT max(sid) FROM exsymbol_q") {
		if s.maxErr != nil {
			return sidAllocRowStub{err: s.maxErr}
		}
		s.mu.Lock()
		maxID := s.maxID
		s.mu.Unlock()
		if s.hideMax {
			return sidAllocRowStub{maxID: nil}
		}
		return sidAllocRowStub{maxID: &maxID}
	}
	if strings.Contains(sql, "INSERT INTO exsymbol (") {
		sid := args[0].(int32)
		item := ExSymbol{
			ID: sid, Exchange: args[1].(string), ExgReal: args[2].(string), Market: args[3].(string),
			Symbol: args[4].(string), Combined: args[5].(bool), ListMs: args[6].(int64),
			DelistMs: args[7].(int64), AggRules: args[8].(string),
		}
		key := exSymbolKey(item.Exchange, item.Market, item.Symbol)
		s.mu.Lock()
		if s.items == nil {
			s.items = make(map[int32]ExSymbol)
		}
		if s.identities == nil {
			s.identities = make(map[string]int32)
		}
		if storedID, ok := s.identities[key]; ok {
			item = s.items[storedID]
		} else {
			s.ids = append(s.ids, sid)
			s.items[sid] = item
			s.identities[key] = sid
			if sid > s.maxID {
				s.maxID = sid
			}
		}
		s.mu.Unlock()
		return sidAllocRowStub{item: &item}
	}
	if strings.Contains(sql, "LATEST BY sid") {
		sid := args[0].(int32)
		s.mu.Lock()
		item, ok := s.items[sid]
		s.mu.Unlock()
		if !ok {
			return sidAllocRowStub{err: pgx.ErrNoRows}
		}
		return sidAllocRowStub{item: &item}
	}
	panic("unexpected QueryRow call: " + sql)
}

func (s *sidAllocDBStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unexpected CopyFrom call")
}

type sidAllocRowStub struct {
	maxID *int32
	item  *ExSymbol
	err   error
}

func (r sidAllocRowStub) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	if r.item == nil {
		*dest[0].(**int32) = r.maxID
		return nil
	}
	item := r.item
	*dest[0].(*int32) = item.ID
	*dest[1].(*string) = item.Exchange
	*dest[2].(*string) = item.ExgReal
	*dest[3].(*string) = item.Market
	*dest[4].(*string) = item.Symbol
	*dest[5].(*bool) = item.Combined
	*dest[6].(*int64) = item.ListMs
	*dest[7].(*int64) = item.DelistMs
	*dest[8].(*string) = item.AggRules
	return nil
}
