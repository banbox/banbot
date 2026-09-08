package orm

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func assertExSymbolVisibilityTimeout(t *testing.T, n int64, err error) {
	t.Helper()
	var timeoutErr *errs.Error
	if n != 1 || err == nil || !errors.As(err, &timeoutErr) || timeoutErr.Code != core.ErrTimeout ||
		!strings.Contains(timeoutErr.Short(), "exsymbol rows not visible before timeout") {
		t.Fatalf("AddSymbols = (%d, %v), want one logical symbol and a visibility timeout", n, err)
	}
}

func TestAddSymbolsQuestDBWithoutPendingMarkerInsertsExactlyOnce(t *testing.T) {
	oldQuestDB, oldDataDir, oldDatabase, oldWait := IsQuestDB, config.DataDir, config.Database, questWaitForCondition
	IsQuestDB = true
	config.DataDir = t.TempDir()
	config.Database = &config.DatabaseConfig{Url: "postgresql://admin:quest@questdb:8812/banbot", DbType: "questdb"}
	questWaitForCondition = func(_ context.Context, _ time.Duration, _ time.Duration, check func() (bool, error)) (bool, error) {
		return check()
	}
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
		config.Database = oldDatabase
		questWaitForCondition = oldWait
	})

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	cleanupSharedSIDReservations(t, allocator)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "test", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	row := AddSymbolsParams{Exchange: "test", Market: "spot", Symbol: "BTC/USDT"}
	expected := exSymbolRecoveryRow{ID: 1, Exchange: row.Exchange, Market: row.Market, Symbol: row.Symbol}
	inserted := make([]int32, 0, 1)
	db := &visibilityDBStub{
		exec: func(sql string, args ...interface{}) (pgconn.CommandTag, error) {
			if !strings.Contains(sql, "INSERT INTO exsymbol_q") {
				t.Fatalf("unexpected Exec: %s", sql)
			}
			inserted = append(inserted, args[0].(int32))
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(sql string, _ ...interface{}) pgx.Row {
			switch {
			case strings.Contains(sql, "SELECT max(sid) FROM exsymbol_q"):
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					*dest[0].(**int32) = nil
					return nil
				}}
			case strings.Contains(sql, "LATEST BY sid"):
				return exSymbolRecoveryDBRow(expected, nil)
			default:
				t.Fatalf("unexpected QueryRow: %s", sql)
				return visibilityRowStub{scan: func(...interface{}) error { return nil }}
			}
		},
	}

	n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{row})
	if err != nil || n != 1 {
		t.Fatalf("initial AddSymbols = (%d, %v), want one inserted symbol", n, err)
	}
	if !reflect.DeepEqual(inserted, []int32{expected.ID}) {
		t.Fatalf("initial AddSymbols INSERTs = %v, want exactly one INSERT for SID %d", inserted, expected.ID)
	}
}

func TestAddSymbolsQuestDBInitialInsertThenRetryReusesSharedSIDReservationAcrossRoots(t *testing.T) {
	oldQuestDB, oldDataDir, oldDatabase, oldRootFn, oldWait := IsQuestDB, config.DataDir, config.Database, compactProcessLockRootFn, questWaitForCondition
	IsQuestDB = true
	config.DataDir = t.TempDir()
	config.Database = &config.DatabaseConfig{Url: "postgresql://admin:quest@questdb:8812/banbot", DbType: "questdb"}
	compactProcessLockRootFn = func() string { return t.TempDir() }
	questWaitForCondition = func(_ context.Context, _ time.Duration, _ time.Duration, check func() (bool, error)) (bool, error) {
		if _, err := check(); err != nil {
			return false, err
		}
		return false, nil
	}
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
		config.Database = oldDatabase
		compactProcessLockRootFn = oldRootFn
		questWaitForCondition = oldWait
	})

	namespace := "shared-ledger-" + t.Name()
	firstRoot, secondRoot := t.TempDir(), t.TempDir()
	firstAllocator := NewSIDAllocatorForStorage(namespace, firstRoot)
	secondAllocator := NewSIDAllocatorForStorage(namespace, secondRoot)
	cleanupSharedSIDReservations(t, firstAllocator)
	first := NewSymbolStateWithAllocatorAndIdentity(firstAllocator, "test", "spot")
	second := NewSymbolStateWithAllocatorAndIdentity(secondAllocator, "test", "spot")
	if err := BindExSymbolRecoveryDir(first, firstRoot); err != nil {
		t.Fatal(err)
	}
	if err := BindExSymbolRecoveryDir(second, secondRoot); err != nil {
		t.Fatal(err)
	}
	secondAllocator.observeSID(50)

	row := AddSymbolsParams{Exchange: "test", Market: "spot", Symbol: "BTC/USDT"}
	expected := exSymbolRecoveryRow{ID: 1, Exchange: row.Exchange, Market: row.Market, Symbol: row.Symbol}
	inserted := make([]int32, 0, 2)
	visible := false
	db := &visibilityDBStub{
		exec: func(sql string, args ...interface{}) (pgconn.CommandTag, error) {
			if !strings.Contains(sql, "INSERT INTO exsymbol_q") {
				t.Fatalf("unexpected Exec: %s", sql)
			}
			inserted = append(inserted, args[0].(int32))
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(sql string, _ ...interface{}) pgx.Row {
			switch {
			case strings.Contains(sql, "SELECT max(sid) FROM exsymbol_q"):
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					*dest[0].(**int32) = nil
					return nil
				}}
			case strings.Contains(sql, "LATEST BY sid"):
				if visible {
					return exSymbolRecoveryDBRow(expected, nil)
				}
				return visibilityRowStub{scan: func(...interface{}) error { return pgx.ErrNoRows }}
			default:
				t.Fatalf("unexpected QueryRow: %s", sql)
				return visibilityRowStub{scan: func(...interface{}) error { return nil }}
			}
		},
	}
	defer func() {
		_ = removeSharedSIDReservations(firstAllocator, []exSymbolRecoveryRow{expected})
	}()

	n, err := New(db).WithSymbolState(first).AddSymbols(context.Background(), []AddSymbolsParams{row})
	assertExSymbolVisibilityTimeout(t, n, err)
	if !reflect.DeepEqual(inserted, []int32{1}) {
		t.Fatalf("initial AddSymbols INSERTs = %v, want exactly one INSERT for SID 1", inserted)
	}
	n, err = New(db).WithSymbolState(second).AddSymbols(context.Background(), []AddSymbolsParams{row})
	assertExSymbolVisibilityTimeout(t, n, err)
	if len(inserted) != 1 || inserted[0] != 1 {
		t.Fatalf("cross-root retry INSERTs = %v, want no INSERT beyond initial SID 1", inserted)
	}
	if _, marker, err := readSharedSIDReservationMarker(firstAllocator); err != nil {
		t.Fatal(err)
	} else if len(marker.Rows) != 1 || marker.Rows[0].ID != 1 {
		t.Fatalf("shared pending ledger = %+v, want one unresolved SID 1", marker.Rows)
	}
	for name, root := range map[string]string{"first": firstRoot, "second": secondRoot} {
		markers, err := readPendingExSymbolMarkers(filepath.Join(root, "recovery"))
		if err != nil || len(markers) != 1 {
			t.Fatalf("%s recovery markers = %d/%v, want one unresolved marker", name, len(markers), err)
		}
	}

	visible = true
	for name, call := range map[string]*SymbolState{"second": second, "first": first} {
		n, err := New(db).WithSymbolState(call).AddSymbols(context.Background(), []AddSymbolsParams{row})
		if err != nil || n != 1 {
			t.Fatalf("%s visible retry = (%d, %v), want one reconciled symbol", name, n, err)
		}
		if got := call.GetExSymbol2(row.Exchange, row.Market, row.Symbol); got == nil || got.ID != expected.ID {
			t.Fatalf("%s visible retry cached %+v, want sid %d", name, got, expected.ID)
		}
	}
	if len(inserted) != 1 {
		t.Fatalf("visible retries inserted duplicate rows: %v", inserted)
	}
	key := exSymbolKey(row.Exchange, row.Market, row.Symbol)
	for name, allocator := range map[string]*SIDAllocator{"first": firstAllocator, "second": secondAllocator} {
		if pending, confirmed := allocator.pendingSID(key), allocator.reservedSID(key); pending != 0 || confirmed != expected.ID {
			t.Fatalf("%s allocator reservation = pending %d confirmed %d, want 0/%d", name, pending, confirmed, expected.ID)
		}
	}
	if path, marker, err := readSharedSIDReservationMarker(firstAllocator); err != nil {
		t.Fatal(err)
	} else if path != "" || len(marker.Rows) != 0 {
		t.Fatalf("visible retries retained shared marker: path=%q rows=%+v", path, marker.Rows)
	}
	for name, root := range map[string]string{"first": firstRoot, "second": secondRoot} {
		markers, err := readPendingExSymbolMarkers(filepath.Join(root, "recovery"))
		if err != nil || len(markers) != 0 {
			t.Fatalf("%s visible retry retained recovery markers: %d/%v", name, len(markers), err)
		}
	}
}

func TestAddSymbolsQuestDBDefaultAllocatorsFenceHiddenSIDsAcrossProcesses(t *testing.T) {
	oldQuestDB, oldDataDir, oldDatabase, oldWait := IsQuestDB, config.DataDir, config.Database, questWaitForCondition
	IsQuestDB = true
	config.DataDir = t.TempDir()
	config.Database = &config.DatabaseConfig{Url: "postgresql://admin:quest@questdb:8812/banbot", DbType: "questdb"}
	questWaitForCondition = func(_ context.Context, _ time.Duration, _ time.Duration, check func() (bool, error)) (bool, error) {
		if _, err := check(); err != nil {
			return false, err
		}
		return false, nil
	}
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
		config.Database = oldDatabase
		questWaitForCondition = oldWait
	})

	firstAllocator := NewSIDAllocator()
	secondAllocator := NewSIDAllocator()
	if firstAllocator.Namespace() == "" || firstAllocator.Namespace() != secondAllocator.Namespace() {
		t.Fatalf("default allocator namespaces = %q and %q", firstAllocator.Namespace(), secondAllocator.Namespace())
	}
	firstDir, secondDir := t.TempDir(), t.TempDir()
	firstAllocator = NewSIDAllocatorForStorage(firstAllocator.Namespace(), firstDir)
	secondAllocator = NewSIDAllocatorForStorage(secondAllocator.Namespace(), secondDir)
	cleanupSharedSIDReservations(t, firstAllocator)
	if root := sidReservationRootForAllocator(firstAllocator); root == "" || root != sidReservationRootForAllocator(secondAllocator) {
		t.Fatalf("reservation roots split same remote storage identity: %q and %q", root, sidReservationRootForAllocator(secondAllocator))
	}
	t.Cleanup(func() {
		_, marker, err := readSharedSIDReservationMarker(firstAllocator)
		if err != nil {
			t.Errorf("read shared SID reservation marker during cleanup: %v", err)
			return
		}
		if len(marker.Rows) > 0 {
			if err := removeSharedSIDReservations(firstAllocator, marker.Rows); err != nil {
				t.Errorf("remove shared SID reservation marker during cleanup: %v", err)
			}
		}
	})

	first := NewSymbolStateWithAllocatorAndIdentity(firstAllocator, "test", "spot")
	second := NewSymbolStateWithAllocatorAndIdentity(secondAllocator, "test", "spot")
	if err := BindExSymbolRecoveryDir(first, firstDir); err != nil {
		t.Fatal(err)
	}
	if err := BindExSymbolRecoveryDir(second, secondDir); err != nil {
		t.Fatal(err)
	}
	inserted := make([]int32, 0, 2)
	db := &visibilityDBStub{
		exec: func(sql string, args ...interface{}) (pgconn.CommandTag, error) {
			if !strings.Contains(sql, "INSERT INTO exsymbol_q") {
				t.Fatalf("unexpected Exec: %s", sql)
			}
			inserted = append(inserted, args[0].(int32))
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(sql string, _ ...interface{}) pgx.Row {
			if strings.Contains(sql, "SELECT max(sid) FROM exsymbol_q") {
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					*dest[0].(**int32) = nil
					return nil
				}}
			}
			return visibilityRowStub{scan: func(...interface{}) error { return pgx.ErrNoRows }}
		},
	}
	for i, call := range []struct {
		state  *SymbolState
		symbol string
	}{{first, "BTC/USDT"}, {second, "ETH/USDT"}} {
		n, err := New(db).WithSymbolState(call.state).AddSymbols(context.Background(), []AddSymbolsParams{{
			Exchange: "test", Market: "spot", Symbol: call.symbol,
		}})
		if n != 1 || err == nil {
			t.Fatalf("process %d AddSymbols = (%d, %v), want one inserted row and a visibility timeout", i+1, n, err)
		}
		assertExSymbolVisibilityTimeout(t, n, err)
	}

	if !reflect.DeepEqual(inserted, []int32{1, 2}) {
		t.Fatalf("hidden WAL allocations = %v, want [1 2]", inserted)
	}
	_, marker, err := readSharedSIDReservationMarker(secondAllocator)
	if err != nil {
		t.Fatal(err)
	}
	if len(marker.Rows) != 2 || marker.Rows[0].ID != 1 || marker.Rows[1].ID != 2 {
		t.Fatalf("shared reservation marker = %+v", marker.Rows)
	}
}

func TestAddSymbolsQuestDBSerializesDifferentDataDirsByStorageIdentity(t *testing.T) {
	oldQuestDB, oldDataDir, oldDatabase, oldRootFn := IsQuestDB, config.DataDir, config.Database, compactProcessLockRootFn
	IsQuestDB = true
	config.DataDir = t.TempDir()
	config.Database = &config.DatabaseConfig{Url: "postgresql://admin:quest@questdb:8812/banbot", DbType: "questdb"}
	lockRoot := t.TempDir()
	compactProcessLockRootFn = func() string { return lockRoot }
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
		config.Database = oldDatabase
		compactProcessLockRootFn = oldRootFn
	})

	firstDir, secondDir := t.TempDir(), t.TempDir()
	namespace := CanonicalDatabaseIdentityForType(config.Database.Url, firstDir, config.Database.DbType)
	firstAllocator := NewSIDAllocatorForStorage(namespace, firstDir)
	secondAllocator := NewSIDAllocatorForStorage(namespace, secondDir)
	cleanupSharedSIDReservations(t, firstAllocator)
	if got, want := sidReservationRootForAllocator(firstAllocator), sidReservationRootForAllocator(secondAllocator); got == "" || got != want {
		t.Fatalf("same remote storage identity has split reservation roots: %q and %q", got, want)
	}
	first := NewSymbolStateWithAllocatorAndIdentity(firstAllocator, "test", "spot")
	second := NewSymbolStateWithAllocatorAndIdentity(secondAllocator, "test", "spot")
	if err := BindExSymbolRecoveryDir(first, firstDir); err != nil {
		t.Fatal(err)
	}
	if err := BindExSymbolRecoveryDir(second, secondDir); err != nil {
		t.Fatal(err)
	}

	db := &sidAllocDBStub{queryDelay: time.Millisecond, execDelay: time.Millisecond}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, call := range []struct {
		state  *SymbolState
		symbol string
	}{{first, "BTC/USDT"}, {second, "ETH/USDT"}} {
		call := call
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := New(db).WithSymbolState(call.state).AddSymbols(context.Background(), []AddSymbolsParams{
				{Exchange: "test", Market: "spot", Symbol: call.symbol},
			})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.ids) != 2 {
		t.Fatalf("inserted SID count = %d, want 2: %v", len(db.ids), db.ids)
	}
	seen := make(map[int32]bool, len(db.ids))
	for _, sid := range db.ids {
		if seen[sid] {
			t.Fatalf("duplicate SID allocated across DataDirs: %d (%v)", sid, db.ids)
		}
		seen[sid] = true
	}
}

func TestAddSymbolsQuestDBReusesCanonicalIdentityAcrossAllocators(t *testing.T) {
	oldQuestDB, oldDataDir, oldRootFn := IsQuestDB, config.DataDir, compactProcessLockRootFn
	IsQuestDB = true
	config.DataDir = t.TempDir()
	lockRoot := t.TempDir()
	compactProcessLockRootFn = func() string { return lockRoot }
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
		compactProcessLockRootFn = oldRootFn
	})

	stored := ExSymbol{
		ID: 17, Exchange: "binance", ExgReal: "binance", Market: "spot", Symbol: "BTC/USDT",
		Combined: true, ListMs: 123, DelistMs: 456, AggRules: `{"price":"last"}`,
	}
	db := &sidAllocDBStub{
		maxID:      17,
		items:      map[int32]ExSymbol{stored.ID: stored},
		identities: map[string]int32{exSymbolKey(stored.Exchange, stored.Market, stored.Symbol): stored.ID},
	}
	arg := []AddSymbolsParams{{
		Exchange: stored.Exchange, ExgReal: "runtime-specific", Market: stored.Market, Symbol: stored.Symbol,
		ListMs: 999, DelistMs: 1000,
	}}

	states := []*SymbolState{
		NewSymbolStateWithAllocator(NewSIDAllocator()),
		NewSymbolStateWithAllocator(NewSIDAllocator()),
	}
	for i, state := range states {
		if err := BindExSymbolRecoveryDir(state, t.TempDir()); err != nil {
			t.Fatal(err)
		}
		n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), arg)
		if err != nil || n != 1 {
			t.Fatalf("runtime %d AddSymbols = (%d, %v), want one reused symbol", i, n, err)
		}
		got := state.GetExSymbol2(stored.Exchange, stored.Market, stored.Symbol)
		if got == nil || *got != stored {
			t.Fatalf("runtime %d cached %+v, want canonical row %+v", i, got, stored)
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.ids) != 0 {
		t.Fatalf("canonical identity was inserted again: sids=%v", db.ids)
	}
}

// These test doubles predate identity-keyed QueryRow calls. Keep their
// existing SID-oriented callbacks for the visibility/recovery tests while
// exposing the narrow canonical lookup used by AddSymbols.
func (*visibilityDBStub) lookupQuestCanonicalExSymbol(context.Context, string, string, string) (*ExSymbol, error) {
	return nil, nil
}

func (s *sidAllocDBStub) lookupQuestCanonicalExSymbol(_ context.Context, exchange, market, symbol string) (*ExSymbol, error) {
	key := exSymbolKey(exchange, market, symbol)
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.identities[key]; ok {
		if item, exists := s.items[id]; exists {
			copyItem := item
			return &copyItem, nil
		}
	}
	for _, item := range s.items {
		if exSymbolKey(item.Exchange, item.Market, item.Symbol) == key {
			copyItem := item
			return &copyItem, nil
		}
	}
	return nil, nil
}

func TestAddSymbolsQuestDBRegistryUsesStableWriteTimestamp(t *testing.T) {
	oldQuestDB, oldDataDir := IsQuestDB, config.DataDir
	IsQuestDB = true
	config.DataDir = t.TempDir()
	installQuestWaitScript(t, 1)
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
	})

	registryDB := newSIDRegistryFakeDB()
	registry := newSymbolSIDRegistryForDB(registryDB)
	namespace := "registry-test:" + t.Name()
	row := AddSymbolsParams{Exchange: "test", Market: "spot", Symbol: "BTC/USDT", ExgReal: "test"}
	insertedSIDs := make([]int32, 0, 2)
	insertedTS := make([]time.Time, 0, 2)
	db := &visibilityDBStub{
		exec: func(sql string, args ...interface{}) (pgconn.CommandTag, error) {
			if !strings.Contains(sql, "INSERT INTO exsymbol_q") {
				t.Fatalf("unexpected Exec: %s", sql)
			}
			insertedSIDs = append(insertedSIDs, args[0].(int32))
			insertedTS = append(insertedTS, args[1].(time.Time))
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			if strings.Contains(sql, "SELECT max(sid) FROM exsymbol_q") {
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					maxSID := int32(17)
					*dest[0].(**int32) = &maxSID
					return nil
				}}
			}
			if !strings.Contains(sql, "LATEST BY sid") {
				t.Fatalf("unexpected QueryRow: %s", sql)
			}
			id := args[0].(int32)
			return exSymbolRecoveryDBRow(exSymbolRecoveryRow{
				ID: id, Exchange: row.Exchange, ExgReal: row.ExgReal,
				Market: row.Market, Symbol: row.Symbol,
			}, nil)
		},
	}
	newState := func() *SymbolState {
		dataDir := t.TempDir()
		allocator := NewSIDAllocatorForStorageWithRegistry(namespace, dataDir, registry)
		cleanupSharedSIDReservations(t, allocator)
		state := NewSymbolStateWithAllocatorAndIdentity(allocator, row.Exchange, row.Market)
		if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
			t.Fatal(err)
		}
		return state
	}

	for i, state := range []*SymbolState{newState(), newState()} {
		n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{row})
		if err != nil || n != 1 {
			t.Fatalf("registry AddSymbols run %d = (%d, %v), want one symbol", i+1, n, err)
		}
	}
	if !reflect.DeepEqual(insertedSIDs, []int32{18, 18}) {
		t.Fatalf("registry physical SIDs = %v, want stable SID [18 18] above existing SID 17", insertedSIDs)
	}
	if len(insertedTS) != 2 || !insertedTS[0].Equal(insertedTS[1]) {
		t.Fatalf("registry write timestamps = %v, want one stable timestamp", insertedTS)
	}
}

func TestQueryQuestDBCanonicalSymbolUsesLogicalLatestRows(t *testing.T) {
	stored := ExSymbol{
		ID: 17, Exchange: "binance", ExgReal: "binance", Market: "spot", Symbol: "BTC/USDT",
		Combined: true, ListMs: 123, DelistMs: 456, AggRules: `{"price":"last"}`,
	}
	db := &canonicalExSymbolQueryDBStub{
		row: visibilityRowStub{scan: func(dest ...interface{}) error {
			*dest[0].(*int32) = stored.ID
			*dest[1].(*string) = stored.Exchange
			*dest[2].(*string) = stored.ExgReal
			*dest[3].(*string) = stored.Market
			*dest[4].(*string) = stored.Symbol
			*dest[5].(*bool) = stored.Combined
			*dest[6].(*int64) = stored.ListMs
			*dest[7].(*int64) = stored.DelistMs
			*dest[8].(*string) = stored.AggRules
			return nil
		}},
	}

	got, err := queryQuestDBCanonicalSymbol(context.Background(), New(db), AddSymbolsParams{
		Exchange: stored.Exchange, Market: stored.Market, Symbol: stored.Symbol,
	})
	if err != nil || got == nil || !reflect.DeepEqual(*got, stored) {
		t.Fatalf("canonical query = (%+v, %v), want %+v", got, err, stored)
	}
	for _, want := range []string{
		"FROM exsymbol_q", "LATEST BY sid", "exchange = $1", "market = $2", "symbol = $3",
		"coalesce(is_deleted, false) = false", "ORDER BY sid", "LIMIT 1",
	} {
		if !strings.Contains(db.sql, want) {
			t.Fatalf("canonical query missing %q: %s", want, db.sql)
		}
	}
	if !reflect.DeepEqual(db.args, []interface{}{stored.Exchange, stored.Market, stored.Symbol}) {
		t.Fatalf("canonical query args = %#v", db.args)
	}
}

type canonicalExSymbolQueryDBStub struct {
	row  pgx.Row
	sql  string
	args []interface{}
}

func (s *canonicalExSymbolQueryDBStub) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	panic("unexpected Exec call")
}

func (s *canonicalExSymbolQueryDBStub) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("unexpected Query call")
}

func (s *canonicalExSymbolQueryDBStub) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	s.sql = sql
	s.args = append([]interface{}(nil), args...)
	return s.row
}

func (s *canonicalExSymbolQueryDBStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unexpected CopyFrom call")
}
