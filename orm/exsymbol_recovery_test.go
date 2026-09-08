package orm

import (
	"context"
	"errors"
	"fmt"
	"os"
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

func cleanupSharedSIDReservations(t *testing.T, allocator *SIDAllocator) {
	t.Helper()
	cleanup := func() {
		_, marker, err := readSharedSIDReservationMarker(allocator)
		if err != nil {
			t.Errorf("read shared SID reservation marker during cleanup: %v", err)
			return
		}
		if len(marker.Rows) > 0 {
			if err := removeSharedSIDReservations(allocator, marker.Rows); err != nil {
				t.Errorf("remove shared SID reservation marker during cleanup: %v", err)
			}
		}
	}
	// Clear a marker left by an interrupted test run before the test publishes
	// its own rows, then repeat the cleanup after the test completes.
	cleanup()
	t.Cleanup(cleanup)
}

func TestExSymbolRecoveryRootOwnedBySymbolState(t *testing.T) {
	dataDir := t.TempDir()
	allocator := NewSIDAllocator()
	first := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	second := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")

	if err := BindExSymbolRecoveryDir(first, dataDir); err != nil {
		t.Fatal(err)
	}
	if err := BindExSymbolRecoveryDir(second, dataDir); err != nil {
		t.Fatalf("bind shared allocator to same root: %v", err)
	}
	want := filepath.Join(dataDir, "recovery")
	if got, err := exSymbolRecoveryRoot(second, false); err != nil || got != want {
		t.Fatalf("symbol state recovery root = %q, %v; want %q", got, err, want)
	}

	other := filepath.Join(t.TempDir(), "recovery")
	if err := BindExSymbolRecoveryDir(second, filepath.Dir(other)); err == nil ||
		!strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), other) {
		t.Fatalf("rebind shared allocator error = %v; want both roots", err)
	}

	second.recoveryMu.RLock()
	ownedRoot := second.recoveryRoot
	second.recoveryMu.RUnlock()
	if ownedRoot != want {
		t.Fatalf("state-owned recovery root = %q, want %q", ownedRoot, want)
	}
}

func TestBindExSymbolRecoveryDirUsesSharedStorageIdentityForReservations(t *testing.T) {
	oldDataDir, oldDatabase := config.DataDir, config.Database
	config.DataDir = t.TempDir()
	config.Database = &config.DatabaseConfig{Url: "postgresql://admin:quest@questdb:8812/banbot", DbType: "questdb"}
	t.Cleanup(func() {
		config.DataDir = oldDataDir
		config.Database = oldDatabase
	})

	runtimeDataDir, siblingDataDir := t.TempDir(), t.TempDir()
	namespace := defaultSIDAllocatorNamespace()
	allocator := NewSIDAllocatorForStorage(namespace, runtimeDataDir)
	sibling := NewSIDAllocatorForStorage(namespace, siblingDataDir)
	state := NewSymbolStateWithAllocator(allocator)
	if err := BindExSymbolRecoveryDir(state, runtimeDataDir); err != nil {
		t.Fatal(err)
	}
	if root := sidReservationRootForAllocator(allocator); root == "" || root != sidReservationRootForAllocator(sibling) {
		t.Fatalf("reservation roots split shared storage identity: %q and %q", root, sidReservationRootForAllocator(sibling))
	}
}

func TestQuestDBSIDAllocationRejectsMissingStorageIdentity(t *testing.T) {
	oldQuestDB, oldRootFn := IsQuestDB, compactProcessLockRootFn
	IsQuestDB = true
	compactProcessLockRootFn = func() string { return "" }
	t.Cleanup(func() {
		IsQuestDB = oldQuestDB
		compactProcessLockRootFn = oldRootFn
	})

	_, err := acquireSIDReservationLease(context.Background(), NewSIDAllocatorForNamespace(""))
	var configErr *errs.Error
	if !errors.As(err, &configErr) || configErr.Code != core.ErrBadConfig {
		t.Fatalf("missing storage identity error = %v, want BadConfig", err)
	}
}

func TestExSymbolRecoveryRootConcurrentBindAndRead(t *testing.T) {
	dataDir := t.TempDir()
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	want := filepath.Join(dataDir, "recovery")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 16 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			root, err := exSymbolRecoveryRoot(state, false)
			if err != nil {
				errs <- err
			} else if root != want {
				errs <- fmt.Errorf("recovery root = %q, want %q", root, want)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if root, err := exSymbolRecoveryRoot(state, false); err != nil || root != want {
		t.Fatalf("final recovery root = %q, %v; want %q", root, err, want)
	}
}

func TestSIDAllocatorNamespacesIsolateReservations(t *testing.T) {
	key := exSymbolKey("binance", "spot", "BTC/USDT")
	first := NewSIDAllocatorForNamespace("storage-a")
	second := NewSIDAllocatorForNamespace("storage-b")
	if got := first.reserveSID(key, 7); got != 7 {
		t.Fatalf("first namespace reservation = %d, want 7", got)
	}
	if got := second.reservedSID(key); got != 0 {
		t.Fatalf("reservation crossed namespace boundary: %d", got)
	}
	if got := second.reserveSID(key, 11); got != 11 {
		t.Fatalf("second namespace reservation = %d, want 11", got)
	}
}

func TestSIDAllocatorPendingReservationRequiresConfirmation(t *testing.T) {
	allocator := NewSIDAllocator()
	key := exSymbolKey("binance", "spot", "ETH/USDT")
	reservation := []sidReservation{{key: key, id: 9}}
	if err := allocator.reservePendingSIDBatch(reservation); err != nil {
		t.Fatal(err)
	}
	if got := allocator.reservedSID(key); got != 0 {
		t.Fatalf("pending reservation was visible as confirmed SID %d", got)
	}
	if got := allocator.pendingSID(key); got != 9 {
		t.Fatalf("pending reservation = %d, want 9", got)
	}
	if err := allocator.markSIDConfirmed(key, 9); err != nil {
		t.Fatal(err)
	}
	if got := allocator.pendingSID(key); got != 0 {
		t.Fatalf("confirmed reservation remained pending as SID %d", got)
	}
	if got := allocator.reservedSID(key); got != 9 {
		t.Fatalf("confirmed reservation = %d, want 9", got)
	}
}

func TestReconcilePendingExSymbolMarkerRejectsForeignNamespace(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	row := exSymbolRecoveryRow{ID: 23, Exchange: "binance", Market: "spot", Symbol: "SOL/USDT"}
	if _, err := writePendingExSymbolMarkerForNamespace(root, "storage-a", []exSymbolRecoveryRow{row}); err != nil {
		t.Fatal(err)
	}
	allocator := NewSIDAllocatorForNamespace("storage-b")
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		t.Fatal("foreign namespace marker reached database visibility query")
		return nil
	}}
	err := reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root)
	if err == nil || !strings.Contains(err.Error(), "storage namespace") {
		t.Fatalf("foreign namespace reconciliation error = %v", err)
	}
	if got := allocator.pendingSID(exSymbolKey(row.Exchange, row.Market, row.Symbol)); got != 0 {
		t.Fatalf("foreign marker reserved SID %d", got)
	}
}

func TestReconcilePendingExSymbolMarkerConfirmsCachesAndRemoves(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	cleanupSharedSIDReservations(t, allocator)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	row := exSymbolRecoveryRow{ID: 17, Exchange: "binance", ExgReal: "binance", Market: "spot", Symbol: "BTC/USDT", Combined: true, ListMs: 10, DelistMs: 20, AggRules: `{"price":"last"}`}
	path, err := writePendingExSymbolMarker(filepath.Join(dataDir, "recovery"), []exSymbolRecoveryRow{row})
	if err != nil {
		t.Fatal(err)
	}
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		return exSymbolRecoveryDBRow(row, nil)
	}}

	if _, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("confirmed marker was not removed: %v", err)
	}
	got := state.GetSymbolByID(row.ID)
	if got == nil || got.Symbol != row.Symbol || got.AggRules != row.AggRules {
		t.Fatalf("confirmed row was not cached: %+v", got)
	}
	target := &ExSymbol{Exchange: row.Exchange, Market: row.Market, Symbol: row.Symbol}
	resolved, resolveErr := resolveEnsuredSymbol(allocator, NewSymbolStateWithAllocator(allocator), target)
	if resolveErr != nil || resolved == nil || target.ID != row.ID {
		t.Fatalf("confirmed row was not reserved: %+v", target)
	}
}

func TestReconcileSharedSIDReservationForeignRowWaitsForVisibility(t *testing.T) {
	oldQuestDB := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuestDB })

	allocator := NewSIDAllocatorForNamespace("shared:" + t.Name())
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	row := exSymbolRecoveryRow{ID: 23, Exchange: "okx", Market: "linear", Symbol: "BTC/USDT"}
	path, err := publishSharedSIDReservations(allocator, []exSymbolRecoveryRow{row})
	if err != nil {
		t.Fatal(err)
	}

	missing := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		return visibilityRowStub{scan: func(...interface{}) error { return pgx.ErrNoRows }}
	}}
	if err := reconcileSharedSIDReservations(context.Background(), New(missing), state, allocator); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("unresolved shared reservation was removed: %v", err)
	}
	if got := allocator.pendingSID(exSymbolKey(row.Exchange, row.Market, row.Symbol)); got != row.ID {
		t.Fatalf("unresolved shared reservation SID = %d, want %d", got, row.ID)
	}

	visible := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		return exSymbolRecoveryDBRow(row, nil)
	}}
	if err := reconcileSharedSIDReservations(context.Background(), New(visible), state, allocator); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("confirmed shared reservation was not removed: %v", err)
	}
	if got := allocator.reservedSID(exSymbolKey(row.Exchange, row.Market, row.Symbol)); got != row.ID {
		t.Fatalf("confirmed foreign reservation SID = %d, want %d", got, row.ID)
	}
}

func TestReadSharedSIDReservationMarkerRejectsIncompleteEntryAfterPublishedMarker(t *testing.T) {
	root := t.TempDir()
	allocator := NewSIDAllocatorForNamespace("test:" + t.Name())
	allocator.identityMu.Lock()
	allocator.reservationRoot = root
	allocator.identityMu.Unlock()
	row := exSymbolRecoveryRow{ID: 23, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	marker := exSymbolRecoveryMarker{
		Version:   exSymbolRecoveryVersion,
		CreatedAt: time.Now().UTC(),
		Namespace: allocator.Namespace(),
		Rows:      []exSymbolRecoveryRow{row},
	}
	if _, err := persistExSymbolRecoveryMarker(root, filepath.Join(root, sidReservationMarkerName), marker); err != nil {
		t.Fatal(err)
	}
	incomplete := filepath.Join(root, "sid-reservation~incomplete")
	if err := os.WriteFile(incomplete, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err := readSharedSIDReservationMarker(allocator)
	if err == nil || !strings.Contains(err.Error(), "incomplete shared SID reservation marker") {
		t.Fatalf("shared marker read error = %v, want incomplete marker error", err)
	}
}

func TestPublishSharedSIDReservationsResyncsExistingMarkerFileAndDirectory(t *testing.T) {
	root := t.TempDir()
	allocator := NewSIDAllocatorForNamespace("test:" + t.Name())
	allocator.identityMu.Lock()
	allocator.reservationRoot = root
	allocator.identityMu.Unlock()
	row := exSymbolRecoveryRow{ID: 29, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	path, err := publishSharedSIDReservations(allocator, []exSymbolRecoveryRow{row})
	if err != nil {
		t.Fatal(err)
	}

	oldFileSync := syncExSymbolRecoveryFile
	oldDirSync := syncExSymbolRecoveryDir
	var events []string
	syncExSymbolRecoveryFile = func(*os.File) error {
		events = append(events, "file")
		return nil
	}
	syncExSymbolRecoveryDir = func(dir string) error {
		if dir != root {
			return fmt.Errorf("unexpected directory sync: %s", dir)
		}
		events = append(events, "directory")
		return nil
	}
	t.Cleanup(func() {
		syncExSymbolRecoveryFile = oldFileSync
		syncExSymbolRecoveryDir = oldDirSync
	})

	if got, err := publishSharedSIDReservations(allocator, []exSymbolRecoveryRow{row}); err != nil || got != path {
		t.Fatalf("republish existing marker = (%q, %v), want %q", got, err, path)
	}
	if !reflect.DeepEqual(events, []string{"file", "directory"}) {
		t.Fatalf("existing marker durability sync order = %v, want file then directory", events)
	}
}

func TestRemovePendingExSymbolMarkerSyncsParentDirectory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "exsymbol-1.pending.json")
	if err := os.WriteFile(path, []byte("marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldSync := syncExSymbolRecoveryDir
	var synced string
	syncExSymbolRecoveryDir = func(dir string) error {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return fmt.Errorf("marker still exists during directory sync: %v", err)
		}
		synced = dir
		return nil
	}
	t.Cleanup(func() { syncExSymbolRecoveryDir = oldSync })

	if err := removePendingExSymbolMarker(path); err != nil {
		t.Fatal(err)
	}
	if synced != root {
		t.Fatalf("synced directory = %q, want %q", synced, root)
	}
}

func TestRemovePendingExSymbolMarkerRestoresOnDirectorySyncFailure(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "exsymbol-1.pending.json")
	want := []byte("marker")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	oldSync := syncExSymbolRecoveryDir
	syncExSymbolRecoveryDir = func(string) error { return errors.New("directory sync failed") }
	t.Cleanup(func() { syncExSymbolRecoveryDir = oldSync })

	if err := removePendingExSymbolMarker(path); err == nil {
		t.Fatal("marker removal unexpectedly succeeded")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("recovery marker was not restored: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("restored marker = %q, want %q", got, want)
	}
}

func TestRemovePendingExSymbolMarkerSyncsFileBeforeDirectory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "exsymbol-1.pending.json")
	if err := os.WriteFile(path, []byte("marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldFileSync := syncExSymbolRecoveryFile
	oldDirSync := syncExSymbolRecoveryDir
	var events []string
	syncExSymbolRecoveryFile = func(*os.File) error {
		events = append(events, "file")
		return nil
	}
	syncExSymbolRecoveryDir = func(dir string) error {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return fmt.Errorf("marker still exists during directory sync: %v", err)
		}
		events = append(events, "directory")
		if dir != root {
			return fmt.Errorf("unexpected directory sync: %s", dir)
		}
		return nil
	}
	t.Cleanup(func() {
		syncExSymbolRecoveryFile = oldFileSync
		syncExSymbolRecoveryDir = oldDirSync
	})

	if err := removePendingExSymbolMarker(path); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"file", "directory"}) {
		t.Fatalf("removal durability sync order = %v, want file then directory", events)
	}
}

func TestRemovePendingExSymbolMarkerRowsPreservesUnresolvedRows(t *testing.T) {
	root := t.TempDir()
	rows := []exSymbolRecoveryRow{
		{ID: 11, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		{ID: 12, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"},
	}
	path, err := writePendingExSymbolMarker(root, rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := removePendingExSymbolMarkerRows(path, rows[1:]); err != nil {
		t.Fatal(err)
	}
	marker, err := readPendingExSymbolMarker(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(marker.Rows, rows[:1]) {
		t.Fatalf("remaining recovery rows = %+v, want %+v", marker.Rows, rows[:1])
	}
	if err := removePendingExSymbolMarkerRows(path, rows[:1]); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("fully resolved marker still exists: %v", err)
	}
}

func TestWritePendingExSymbolMarkerSyncsNewDirectoryParentsBeforePublish(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "nested", "recovery")
	oldSync := syncExSymbolRecoveryDir
	var synced []string
	syncExSymbolRecoveryDir = func(dir string) error {
		synced = append(synced, dir)
		return syncDirectory(dir)
	}
	t.Cleanup(func() { syncExSymbolRecoveryDir = oldSync })

	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{{
		ID: 1, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{base, filepath.Join(base, "nested"), root}
	if !reflect.DeepEqual(synced, want) {
		t.Fatalf("directory sync order = %v, want %v", synced, want)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("published marker missing: %v", err)
	}
}

func TestWritePendingExSymbolMarkerRetainsPublishedFileWhenDirectorySyncFails(t *testing.T) {
	root := t.TempDir()
	oldSync := syncExSymbolRecoveryDir
	var syncCalls int
	syncExSymbolRecoveryDir = func(dir string) error {
		syncCalls++
		if dir == root {
			return errors.New("published directory sync failed")
		}
		return syncDirectory(dir)
	}
	t.Cleanup(func() { syncExSymbolRecoveryDir = oldSync })

	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{{
		ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
	}})
	if err == nil || !strings.Contains(err.Error(), "published directory sync failed") {
		t.Fatalf("write error = %v, want published directory sync failure", err)
	}
	if syncCalls != 1 {
		t.Fatalf("published marker directory sync calls = %d, want 1", syncCalls)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("published marker was lost after directory sync failure: %v", err)
	}
	if _, err := readPendingExSymbolMarker(path); err != nil {
		t.Fatalf("published marker became unreadable after directory sync failure: %v", err)
	}
}

func TestWritePendingExSymbolMarkerSyncsFileBeforeDirectory(t *testing.T) {
	root := t.TempDir()
	oldFileSync := syncExSymbolRecoveryFile
	oldDirSync := syncExSymbolRecoveryDir
	var events []string
	syncExSymbolRecoveryFile = func(*os.File) error {
		events = append(events, "file")
		return nil
	}
	syncExSymbolRecoveryDir = func(dir string) error {
		if dir != root {
			return fmt.Errorf("unexpected directory sync: %s", dir)
		}
		events = append(events, "directory")
		return nil
	}
	t.Cleanup(func() {
		syncExSymbolRecoveryFile = oldFileSync
		syncExSymbolRecoveryDir = oldDirSync
	})

	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{{
		ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"file", "directory"}) {
		t.Fatalf("durability sync order = %v, want file then directory", events)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("published marker missing: %v", err)
	}
}

func TestWritePendingExSymbolMarkerDoesNotPublishWhenFileSyncFails(t *testing.T) {
	root := t.TempDir()
	wantErr := errors.New("file sync failed")
	oldFileSync := syncExSymbolRecoveryFile
	oldDirSync := syncExSymbolRecoveryDir
	directorySyncs := 0
	syncExSymbolRecoveryFile = func(*os.File) error { return wantErr }
	syncExSymbolRecoveryDir = func(string) error {
		directorySyncs++
		return nil
	}
	t.Cleanup(func() {
		syncExSymbolRecoveryFile = oldFileSync
		syncExSymbolRecoveryDir = oldDirSync
	})

	if _, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{{
		ID: 8, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT",
	}}); !errors.Is(err, wantErr) {
		t.Fatalf("write error = %v, want %v", err, wantErr)
	}
	if directorySyncs != 0 {
		t.Fatalf("directory was synced after file fsync failure: %d", directorySyncs)
	}
	markers, err := filepath.Glob(filepath.Join(root, exSymbolRecoveryMarkerPrefix+"*"+exSymbolRecoveryMarkerSuffix))
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 0 {
		t.Fatalf("file-sync failure published markers: %v", markers)
	}
}

func TestFindPendingExSymbolMarkerMergesAndSyncsCompatibleRows(t *testing.T) {
	root := t.TempDir()
	first := exSymbolRecoveryRow{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	second := exSymbolRecoveryRow{ID: 8, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{first})
	if err != nil {
		t.Fatal(err)
	}
	oldSync := syncExSymbolRecoveryDir
	var synced string
	syncExSymbolRecoveryDir = func(dir string) error {
		synced = dir
		return nil
	}
	t.Cleanup(func() { syncExSymbolRecoveryDir = oldSync })

	got, err := findPendingExSymbolMarker(root, "", []exSymbolRecoveryRow{first, second})
	if err != nil || got != path {
		t.Fatalf("find/update marker = (%q, %v), want %q", got, err, path)
	}
	if synced != root {
		t.Fatalf("updated marker synced directory = %q, want %q", synced, root)
	}
	marker, err := readPendingExSymbolMarker(path)
	if err != nil {
		t.Fatal(err)
	}
	if !sameExSymbolRecoveryRows(marker.Rows, []exSymbolRecoveryRow{first, second}) {
		t.Fatalf("merged marker rows = %+v", marker.Rows)
	}
}

func TestFindPendingExSymbolMarkerMergesMultipleOverlappingMarkers(t *testing.T) {
	root := t.TempDir()
	first := exSymbolRecoveryRow{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	second := exSymbolRecoveryRow{ID: 8, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	if _, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{first}); err != nil {
		t.Fatal(err)
	}
	if _, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{second}); err != nil {
		t.Fatal(err)
	}

	path, err := findPendingExSymbolMarker(root, "", []exSymbolRecoveryRow{first, second})
	if err != nil {
		t.Fatalf("merge multiple markers: %v", err)
	}
	if path == "" {
		t.Fatal("merge multiple markers returned no path")
	}
	markers, err := readPendingExSymbolMarkers(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 1 {
		t.Fatalf("merged marker count = %d, want one: %+v", len(markers), markers)
	}
	if markers[0].path != path {
		t.Fatalf("returned marker path = %q, remaining path = %q", path, markers[0].path)
	}
	if !sameExSymbolRecoveryRows(markers[0].marker.Rows, []exSymbolRecoveryRow{first, second}) {
		t.Fatalf("merged marker rows = %+v", markers[0].marker.Rows)
	}
}

func TestFindPendingExSymbolMarkerRetainsUpdatedStateWhenDirectorySyncFails(t *testing.T) {
	root := t.TempDir()
	first := exSymbolRecoveryRow{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	second := exSymbolRecoveryRow{ID: 8, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{first})
	if err != nil {
		t.Fatal(err)
	}
	oldSync := syncExSymbolRecoveryDir
	syncExSymbolRecoveryDir = func(string) error { return errors.New("directory sync failed") }
	t.Cleanup(func() { syncExSymbolRecoveryDir = oldSync })

	if _, err := findPendingExSymbolMarker(root, "", []exSymbolRecoveryRow{first, second}); err == nil {
		t.Fatal("marker update unexpectedly succeeded")
	}
	marker, err := readPendingExSymbolMarker(path)
	if err != nil {
		t.Fatalf("updated marker was not retained: %v", err)
	}
	if !sameExSymbolRecoveryRows(marker.Rows, []exSymbolRecoveryRow{first, second}) {
		t.Fatalf("retained marker rows = %+v", marker.Rows)
	}
}

func TestReconcilePendingExSymbolMarkerReservesUnresolvedIdentityBeforeQuery(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	installQuestWaitScript(t, 1)
	row := exSymbolRecoveryRow{ID: 23, Exchange: "binance", Market: "spot", Symbol: "SOL/USDT"}
	if _, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{row}); err != nil {
		t.Fatal(err)
	}
	queried := false
	db := &visibilityDBStub{
		exec: func(string, ...interface{}) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, nil
		},
		queryRow: func(_ string, args ...interface{}) pgx.Row {
			queried = true
			if got := allocator.pendingSID(exSymbolKey(row.Exchange, row.Market, row.Symbol)); got != row.ID {
				return visibilityRowStub{scan: func(...interface{}) error {
					return fmt.Errorf("SID reservation was not published before visibility query: %d", got)
				}}
			}
			if args[0].(int32) != row.ID {
				t.Fatalf("queried sid = %v, want %d", args[0], row.ID)
			}
			return exSymbolRecoveryDBRow(row, pgx.ErrNoRows)
		},
	}

	if err := reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root); err != nil {
		t.Fatal(err)
	}
	if !queried {
		t.Fatal("unresolved marker was not checked for visibility")
	}
	if got := allocator.pendingSID(exSymbolKey(row.Exchange, row.Market, row.Symbol)); got != row.ID {
		t.Fatalf("unresolved identity reservation = %d, want %d", got, row.ID)
	}
}

type addSymbolsRetryScenario struct {
	dataDir     string
	allocator   *SIDAllocator
	state       *SymbolState
	row         exSymbolRecoveryRow
	visible     bool
	inserts     int
	insertedSID int32
	db          *visibilityDBStub
}

func newAddSymbolsRetryScenario(t *testing.T) *addSymbolsRetryScenario {
	t.Helper()
	oldQuest := IsQuestDB
	IsQuestDB = true
	installQuestWaitScript(t, 1)
	t.Cleanup(func() { IsQuestDB = oldQuest })

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	cleanupSharedSIDReservations(t, allocator)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	row := exSymbolRecoveryRow{ID: 23, Exchange: "binance", Market: "spot", Symbol: "SOL/USDT"}
	if _, err := writePendingExSymbolMarkerForNamespace(filepath.Join(dataDir, "recovery"), allocator.Namespace(), []exSymbolRecoveryRow{row}); err != nil {
		t.Fatal(err)
	}
	scenario := &addSymbolsRetryScenario{
		dataDir:   dataDir,
		allocator: allocator,
		state:     state,
		row:       row,
	}
	scenario.db = &visibilityDBStub{
		exec: func(_ string, args ...interface{}) (pgconn.CommandTag, error) {
			scenario.inserts++
			scenario.insertedSID = args[0].(int32)
			return pgconn.CommandTag{}, nil
		},
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			if strings.Contains(sql, "SELECT max(sid)") {
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					*dest[0].(**int32) = nil
					return nil
				}}
			}
			if !strings.Contains(sql, "LATEST BY sid") {
				t.Fatalf("unexpected query during unresolved retry: %s", sql)
			}
			if got := allocator.reservationSID(exSymbolKey(row.Exchange, row.Market, row.Symbol)); got != row.ID {
				t.Fatalf("retry queried before reservation: %d sql=%q visible=%v", got, sql, scenario.visible)
			}
			if args[0].(int32) != row.ID {
				t.Fatalf("retry queried sid = %v, want %d", args[0], row.ID)
			}
			if !scenario.visible {
				return exSymbolRecoveryDBRow(row, pgx.ErrNoRows)
			}
			return exSymbolRecoveryDBRow(row, nil)
		},
	}
	return scenario
}

func (s *addSymbolsRetryScenario) add() (int64, error) {
	return New(s.db).WithSymbolState(s.state).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: s.row.Exchange, Market: s.row.Market, Symbol: s.row.Symbol,
	}})
}

func TestAddSymbolsRetryPermanentErrNoRowsReplaysMissingRowAndKeepsMarker(t *testing.T) {
	scenario := newAddSymbolsRetryScenario(t)

	n, err := scenario.add()
	var timeoutErr *errs.Error
	if n != 1 || err == nil || !errors.As(err, &timeoutErr) || timeoutErr.Code != core.ErrTimeout {
		t.Fatalf("AddSymbols unresolved retry = (%d, %v), want one retained reservation and a visibility timeout", n, err)
	}
	if scenario.inserts != 1 {
		t.Fatalf("unresolved retry inserted %d rows, want one recovery INSERT", scenario.inserts)
	}
	if got := scenario.state.GetExSymbol2(scenario.row.Exchange, scenario.row.Market, scenario.row.Symbol); got != nil {
		t.Fatalf("unresolved retry cached unconfirmed identity: %+v", got)
	}
	key := exSymbolKey(scenario.row.Exchange, scenario.row.Market, scenario.row.Symbol)
	if got := scenario.allocator.reservedSID(key); got != 0 {
		t.Fatalf("unresolved retry became confirmed reservation: %d", got)
	}
	if got := scenario.allocator.pendingSID(key); got != scenario.row.ID {
		t.Fatalf("unresolved retry lost pending SID: %d", got)
	}
	markers, globErr := filepath.Glob(filepath.Join(scenario.dataDir, "recovery", "exsymbol-*.pending.json"))
	if globErr != nil || len(markers) != 1 {
		t.Fatalf("unresolved retry recovery markers = %v, err=%v; want one retained marker", markers, globErr)
	}
	if _, marker, markerErr := readSharedSIDReservationMarker(scenario.allocator); markerErr != nil {
		t.Fatal(markerErr)
	} else if len(marker.Rows) != 1 || marker.Rows[0].ID != scenario.row.ID {
		t.Fatalf("unresolved retry shared marker = %+v, want retained sid %d", marker.Rows, scenario.row.ID)
	}
}

func TestAddSymbolsRetryReconcilesWhenMarkerBecomesVisible(t *testing.T) {
	scenario := newAddSymbolsRetryScenario(t)
	n, err := scenario.add()
	var timeoutErr *errs.Error
	if n != 1 || err == nil || !errors.As(err, &timeoutErr) || timeoutErr.Code != core.ErrTimeout {
		t.Fatalf("AddSymbols unresolved retry = (%d, %v), want retained pending marker and a visibility timeout", n, err)
	}
	if scenario.inserts != 1 {
		t.Fatalf("unresolved retry inserted %d rows, want one recovery INSERT", scenario.inserts)
	}

	scenario.visible = true
	n, err = scenario.add()
	if err != nil || n != 1 {
		t.Fatalf("AddSymbols visible retry = (%d, %v), want one reused symbol", n, err)
	}
	if scenario.inserts != 1 {
		t.Fatalf("visible retry inserted %d rows, want one recovery INSERT", scenario.inserts)
	}
	if got := scenario.state.GetExSymbol2(scenario.row.Exchange, scenario.row.Market, scenario.row.Symbol); got == nil || got.ID != scenario.row.ID {
		t.Fatalf("visible retry did not cache confirmed identity: %+v", got)
	}
	key := exSymbolKey(scenario.row.Exchange, scenario.row.Market, scenario.row.Symbol)
	if got := scenario.allocator.pendingSID(key); got != 0 {
		t.Fatalf("visible retry retained pending SID: %d", got)
	}
	if got := scenario.allocator.reservedSID(key); got != scenario.row.ID {
		t.Fatalf("visible retry did not confirm SID: %d", got)
	}
	markers, globErr := filepath.Glob(filepath.Join(scenario.dataDir, "recovery", "exsymbol-*.pending.json"))
	if globErr != nil || len(markers) != 0 {
		t.Fatalf("visible retry retained recovery marker: markers=%v err=%v", markers, globErr)
	}
	if path, marker, markerErr := readSharedSIDReservationMarker(scenario.allocator); markerErr != nil {
		t.Fatal(markerErr)
	} else if path != "" || len(marker.Rows) != 0 {
		t.Fatalf("visible retry retained shared marker: path=%q rows=%+v", path, marker.Rows)
	}
}

func TestAddSymbolsRecoversPartialBatchAndCleansMarkers(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	installQuestWaitScript(t, 1)
	t.Cleanup(func() { IsQuestDB = oldQuest })

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	cleanupSharedSIDReservations(t, allocator)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	arg := []AddSymbolsParams{
		{Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		{Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"},
	}
	rows := map[int32]exSymbolRecoveryRow{
		1: {ID: 1, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		2: {ID: 2, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"},
	}
	visible := make(map[int32]bool)
	writeTS := make(map[int32]time.Time)
	inserted := make([]int32, 0, 3)
	secondInsertErr := errors.New("second row insert failed")
	secondInsertFailed := false
	db := &visibilityDBStub{
		exec: func(sql string, args ...interface{}) (pgconn.CommandTag, error) {
			if !strings.Contains(sql, "INSERT INTO exsymbol_q") {
				t.Fatalf("unexpected Exec: %s", sql)
			}
			sid := args[0].(int32)
			ts := args[1].(time.Time)
			if previous, ok := writeTS[sid]; ok && !ts.Equal(previous) {
				t.Fatalf("SID %d recovery timestamp changed from %s to %s", sid, previous, ts)
			}
			writeTS[sid] = ts
			inserted = append(inserted, sid)
			if sid == 2 && !secondInsertFailed {
				secondInsertFailed = true
				return pgconn.CommandTag{}, secondInsertErr
			}
			visible[sid] = true
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			if strings.Contains(sql, "SELECT max(sid)") {
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					*dest[0].(**int32) = nil
					return nil
				}}
			}
			if !strings.Contains(sql, "LATEST BY sid") {
				t.Fatalf("unexpected QueryRow: %s", sql)
			}
			sid := args[0].(int32)
			row := rows[sid]
			if !visible[sid] {
				return exSymbolRecoveryDBRow(row, pgx.ErrNoRows)
			}
			return exSymbolRecoveryDBRow(row, nil)
		},
	}

	n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), arg)
	if n != 1 || !errors.Is(err, secondInsertErr) {
		t.Fatalf("partial AddSymbols = (%d, %v), want first row committed and second insert failure", n, err)
	}
	markers, err := readPendingExSymbolMarkers(filepath.Join(dataDir, "recovery"))
	if err != nil || len(markers) != 1 {
		t.Fatalf("partial recovery markers = %d/%v, want one marker", len(markers), err)
	}
	if markers[0].marker.Rows[0].Inserted == nil || !*markers[0].marker.Rows[0].Inserted ||
		markers[0].marker.Rows[1].Inserted == nil || *markers[0].marker.Rows[1].Inserted {
		t.Fatalf("partial marker insertion states = %+v, want [inserted, pending]", markers[0].marker.Rows)
	}

	n, err = New(db).WithSymbolState(state).AddSymbols(context.Background(), arg)
	if err != nil || n != int64(len(arg)) {
		t.Fatalf("recovery AddSymbols = (%d, %v), want both symbols", n, err)
	}
	if !reflect.DeepEqual(inserted, []int32{1, 2, 2}) {
		t.Fatalf("physical INSERT SIDs = %v, want initial [1 2] plus recovery [2]", inserted)
	}
	markers, err = readPendingExSymbolMarkers(filepath.Join(dataDir, "recovery"))
	if err != nil || len(markers) != 0 {
		t.Fatalf("recovery markers after replay = %d/%v, want none", len(markers), err)
	}
	if path, marker, err := readSharedSIDReservationMarker(allocator); err != nil {
		t.Fatal(err)
	} else if path != "" || len(marker.Rows) != 0 {
		t.Fatalf("shared recovery marker after replay = path %q rows %+v, want none", path, marker.Rows)
	}
	for _, row := range rows {
		if got := state.GetSymbolByID(row.ID); got == nil || got.Symbol != row.Symbol {
			t.Fatalf("recovered symbol %d was not cached: %+v", row.ID, got)
		}
	}
}

func TestReconcilePendingExSymbolMarkerRejectsSIDReservationConflict(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	reservedKey := exSymbolKey("binance", "spot", "BTC/USDT")
	if got := allocator.reserveSID(reservedKey, 23); got != 23 {
		t.Fatalf("initial reservation = %d, want 23", got)
	}
	conflict := exSymbolRecoveryRow{ID: 23, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{conflict})
	if err != nil {
		t.Fatal(err)
	}
	queries := 0
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		queries++
		return exSymbolRecoveryDBRow(conflict, nil)
	}}

	err = reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root)
	if err == nil || !strings.Contains(err.Error(), "sid 23") {
		t.Fatalf("reconcile error = %v, want SID conflict", err)
	}
	if queries != 0 {
		t.Fatalf("SID conflict made %d visibility queries", queries)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("conflicting marker was removed: %v", statErr)
	}
	if got := allocator.reservedSID(exSymbolKey(conflict.Exchange, conflict.Market, conflict.Symbol)); got != 0 {
		t.Fatalf("conflicting identity was reserved as sid %d", got)
	}
}

func TestReconcilePendingExSymbolMarkerRejectsCanonicalMetadataConflict(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	state := NewSymbolStateWithIdentity("binance", "spot")
	state.CacheExSymbol(&ExSymbol{ID: 23, Exchange: "binance", Market: "spot", Symbol: "SOL/USDT", ListMs: 10})
	conflict := exSymbolRecoveryRow{ID: 23, Exchange: "binance", Market: "spot", Symbol: "SOL/USDT", ListMs: 20}
	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{conflict})
	if err != nil {
		t.Fatal(err)
	}
	queries := 0
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		queries++
		return exSymbolRecoveryDBRow(conflict, nil)
	}}

	err = reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root)
	if err == nil || !strings.Contains(err.Error(), "canonical metadata") {
		t.Fatalf("reconcile error = %v, want canonical metadata conflict", err)
	}
	if queries != 0 {
		t.Fatalf("metadata conflict made %d visibility queries", queries)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("conflicting marker was removed: %v", statErr)
	}
	if got := state.GetSymbolByID(conflict.ID); got == nil || got.ListMs != 10 {
		t.Fatalf("canonical metadata was overwritten: %+v", got)
	}
}

func TestAddSymbolsReconcilesPendingMarkerBeforeNewBatch(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	cleanupSharedSIDReservations(t, allocator)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	previous := exSymbolRecoveryRow{ID: 17, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	if _, err := writePendingExSymbolMarker(filepath.Join(dataDir, "recovery"), []exSymbolRecoveryRow{previous}); err != nil {
		t.Fatal(err)
	}
	next := exSymbolRecoveryRow{ID: 18, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	inserts := 0
	db := &visibilityDBStub{
		exec: func(string, ...interface{}) (pgconn.CommandTag, error) {
			inserts++
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			if strings.Contains(sql, "SELECT max(sid)") {
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					maxSID := previous.ID
					*dest[0].(**int32) = &maxSID
					return nil
				}}
			}
			if args[0].(int32) == previous.ID {
				return exSymbolRecoveryDBRow(previous, nil)
			}
			return exSymbolRecoveryDBRow(next, nil)
		},
	}

	n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: next.Exchange, Market: next.Market, Symbol: next.Symbol,
	}})
	if err != nil || n != 1 {
		t.Fatalf("AddSymbols = (%d, %v), want one new row", n, err)
	}
	if inserts != 1 {
		t.Fatalf("new batch inserts = %d, want 1", inserts)
	}
	markers, err := filepath.Glob(filepath.Join(dataDir, "recovery", "exsymbol-*.pending.json"))
	if err != nil || len(markers) != 0 {
		t.Fatalf("confirmed old and new markers should be removed: markers=%v err=%v", markers, err)
	}
	for _, row := range []exSymbolRecoveryRow{previous, next} {
		if got := state.GetSymbolByID(row.ID); got == nil || got.Symbol != row.Symbol {
			t.Fatalf("row %d was not reconciled into state: %+v", row.ID, got)
		}
	}
}

func TestAddSymbolsReusesIdentityReservedByRecoveryBeforeInsert(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	row := exSymbolRecoveryRow{ID: 17, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	if _, err := writePendingExSymbolMarker(filepath.Join(dataDir, "recovery"), []exSymbolRecoveryRow{row}); err != nil {
		t.Fatal(err)
	}
	inserts := 0
	db := &visibilityDBStub{
		exec: func(string, ...interface{}) (pgconn.CommandTag, error) {
			inserts++
			return pgconn.CommandTag{}, nil
		},
		queryRow: func(sql string, _ ...interface{}) pgx.Row {
			if !strings.Contains(sql, "LATEST BY sid") {
				t.Fatalf("unexpected query after recovered reservation: %s", sql)
			}
			return exSymbolRecoveryDBRow(row, nil)
		},
	}

	n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: row.Exchange, Market: row.Market, Symbol: row.Symbol,
	}})
	if err != nil || n != 1 {
		t.Fatalf("AddSymbols = (%d, %v)", n, err)
	}
	if inserts != 0 {
		t.Fatalf("recovered identity was inserted again: %d", inserts)
	}
	if got := state.GetExSymbol2(row.Exchange, row.Market, row.Symbol); got == nil || got.ID != row.ID {
		t.Fatalf("recovered identity not reused: %+v", got)
	}
}

func TestReconcilePendingExSymbolMarkerRejectsMalformedBeforeChangingState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, filepath.Dir(root)); err != nil {
		t.Fatal(err)
	}
	visible := exSymbolRecoveryRow{ID: 18, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	unresolved := exSymbolRecoveryRow{ID: 19, Exchange: "binance", Market: "spot", Symbol: "SOL/USDT"}
	pending, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{visible, unresolved})
	if err != nil {
		t.Fatal(err)
	}
	malformed := filepath.Join(root, "exsymbol-malformed.pending.json")
	if err := os.WriteFile(malformed, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	db := &visibilityDBStub{queryRow: func(_ string, args ...interface{}) pgx.Row {
		if args[0].(int32) == visible.ID {
			return exSymbolRecoveryDBRow(visible, nil)
		}
		return exSymbolRecoveryDBRow(unresolved, pgx.ErrNoRows)
	}}

	err = reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root)
	if err == nil || !strings.Contains(err.Error(), "decode exsymbol recovery marker") {
		t.Fatalf("reconcile error = %v, want malformed marker error", err)
	}
	for _, path := range []string{pending, malformed} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("unresolved marker %s was removed: %v", path, err)
		}
	}
	if got := state.GetSymbolByID(visible.ID); got != nil {
		t.Fatalf("valid marker was partially reconciled before malformed marker rejection: %+v", got)
	}
	if got := state.GetSymbolByID(unresolved.ID); got != nil {
		t.Fatalf("unresolved row was cached: %+v", got)
	}
	if got := allocator.max.Load(); got != 0 {
		t.Fatalf("malformed reconciliation fenced SID %d, want 0", got)
	}
}

func TestReconcilePendingExSymbolMarkerRejectsIncompleteTemporaryFile(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	tempMarker := filepath.Join(root, "exsymbol-1234-5678")
	if err := os.WriteFile(tempMarker, []byte("{\"version\":1"), 0o600); err != nil {
		t.Fatal(err)
	}
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		t.Fatal("incomplete marker reached database visibility query")
		return nil
	}}

	err := reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root)
	if err == nil || !strings.Contains(err.Error(), "incomplete exsymbol recovery marker") {
		t.Fatalf("incomplete marker error = %v", err)
	}
	if allocator.max.Load() != 0 || state.MaxSID() != 0 {
		t.Fatalf("incomplete marker changed allocation state: allocator=%d state=%d", allocator.max.Load(), state.MaxSID())
	}
	if _, err := os.Stat(tempMarker); err != nil {
		t.Fatalf("incomplete marker was removed: %v", err)
	}
}

func TestAddSymbolsMalformedRecoveryMarkerBlocksAllocationAndInsert(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dataDir, "recovery", "exsymbol-malformed.pending.json")
	if err := os.MkdirAll(filepath.Dir(marker), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte("{"), 0o600); err != nil {
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

	n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: "binance", Market: "spot", Symbol: "ETH/USDT",
	}})
	if n != 0 || err == nil || !strings.Contains(err.Error(), "decode exsymbol recovery marker") {
		t.Fatalf("AddSymbols = (%d, %v), want malformed marker error", n, err)
	}
	if dbCalls != 0 {
		t.Fatalf("malformed marker allowed %d database calls", dbCalls)
	}
	if state.MaxSID() != 0 || allocator.max.Load() != 0 || state.SymbolCount() != 0 {
		t.Fatalf("malformed marker changed state: max=%d allocator=%d symbols=%v",
			state.MaxSID(), allocator.max.Load(), state.GetExSymbols("", ""))
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("malformed marker was not retained: %v", err)
	}
}

func TestReconcilePendingExSymbolMarkerFiltersForeignIdentityAndFencesSID(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	allocator := NewSIDAllocator()
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, filepath.Dir(root)); err != nil {
		t.Fatal(err)
	}
	foreign := exSymbolRecoveryRow{ID: 101, Exchange: "okx", Market: "linear", Symbol: "BTC/USDT"}
	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{foreign})
	if err != nil {
		t.Fatal(err)
	}
	queries := 0
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		queries++
		return exSymbolRecoveryDBRow(foreign, nil)
	}}

	if err := reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root); err != nil {
		t.Fatal(err)
	}
	if queries != 0 {
		t.Fatalf("foreign marker was queried by unrelated state: %d queries", queries)
	}
	if state.GetSymbolByID(foreign.ID) != nil {
		t.Fatal("foreign marker was cached in unrelated state")
	}
	if got := allocator.max.Load(); got != foreign.ID {
		t.Fatalf("foreign marker SID was not fenced: max=%d want=%d", got, foreign.ID)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("foreign marker was removed: %v", err)
	}

	owner := NewSymbolStateWithAllocatorAndIdentity(allocator, foreign.Exchange, foreign.Market)
	if err := BindExSymbolRecoveryDir(owner, filepath.Dir(root)); err != nil {
		t.Fatal(err)
	}
	if err := reconcilePendingExSymbolMarkers(context.Background(), New(&visibilityDBStub{
		queryRow: func(string, ...interface{}) pgx.Row {
			return exSymbolRecoveryDBRow(foreign, nil)
		},
	}), owner, root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("owner did not remove confirmed marker: %v", err)
	}
	if got := owner.GetSymbolByID(foreign.ID); got == nil || got.Symbol != foreign.Symbol {
		t.Fatalf("owner did not cache foreign marker: %+v", got)
	}
}

func TestReconcilePendingExSymbolMarkerPropagatesHardQueryFailure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	state := NewSymbolStateWithIdentity("binance", "spot")
	row := exSymbolRecoveryRow{ID: 19, Exchange: "binance", Market: "spot", Symbol: "SOL/USDT"}
	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{row})
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("scan failed")
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		return visibilityRowStub{scan: func(...interface{}) error { return want }}
	}}

	err = reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root)
	if !errors.Is(err, want) {
		t.Fatalf("reconcile error = %v, want %v", err, want)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("hard failure removed recovery marker: %v", err)
	}
}

func TestReconcilePendingExSymbolMarkerRejectsVisibleMetadataConflict(t *testing.T) {
	root := filepath.Join(t.TempDir(), "recovery")
	state := NewSymbolStateWithIdentity("binance", "spot")
	want := exSymbolRecoveryRow{ID: 19, Exchange: "binance", Market: "spot", Symbol: "SOL/USDT"}
	actual := want
	actual.Symbol = "ETH/USDT"
	path, err := writePendingExSymbolMarker(root, []exSymbolRecoveryRow{want})
	if err != nil {
		t.Fatal(err)
	}
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		return exSymbolRecoveryDBRow(actual, nil)
	}}

	err = reconcilePendingExSymbolMarkers(context.Background(), New(db), state, root)
	if err == nil || !strings.Contains(err.Error(), "metadata conflict") {
		t.Fatalf("reconcile error = %v, want metadata conflict", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("conflicting recovery marker was removed: %v", statErr)
	}
	if state.GetSymbolByID(actual.ID) != nil {
		t.Fatal("conflicting visible row was cached")
	}
}

func TestAddSymbolsHardVisibilityFailureKeepsRecoveryMarker(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	cleanupSharedSIDReservations(t, allocator)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	want := errors.New("connection lost")
	db := &visibilityDBStub{
		exec: func(string, ...interface{}) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(sql string, _ ...interface{}) pgx.Row {
			if strings.Contains(sql, "SELECT max(sid)") {
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					*dest[0].(**int32) = nil
					return nil
				}}
			}
			return visibilityRowStub{scan: func(...interface{}) error { return want }}
		},
	}

	n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: "binance", Market: "spot", Symbol: "ADA/USDT",
	}})
	if n != 1 || !errors.Is(err, want) {
		t.Fatalf("AddSymbols = (%d, %v), want inserted row and hard visibility error", n, err)
	}
	markers, globErr := filepath.Glob(filepath.Join(dataDir, "recovery", "exsymbol-*.pending.json"))
	if globErr != nil || len(markers) != 1 {
		t.Fatalf("hard failure must retain one recovery marker: markers=%v err=%v", markers, globErr)
	}
}

func TestAddSymbolsStopsBeforeInsertWhenRecoveryDirectorySyncFails(t *testing.T) {
	oldQuest := IsQuestDB
	oldSync := syncExSymbolRecoveryDir
	IsQuestDB = true
	syncExSymbolRecoveryDir = func(string) error { return errors.New("directory sync failed") }
	t.Cleanup(func() {
		IsQuestDB = oldQuest
		syncExSymbolRecoveryDir = oldSync
	})

	dataDir := t.TempDir()
	allocator := NewSIDAllocatorForStorage("test:"+t.Name(), dataDir)
	state := NewSymbolStateWithAllocatorAndIdentity(allocator, "binance", "spot")
	if err := BindExSymbolRecoveryDir(state, dataDir); err != nil {
		t.Fatal(err)
	}
	inserts := 0
	db := &visibilityDBStub{
		exec: func(string, ...interface{}) (pgconn.CommandTag, error) {
			inserts++
			return pgconn.CommandTag{}, nil
		},
		queryRow: func(string, ...interface{}) pgx.Row {
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				*dest[0].(**int32) = nil
				return nil
			}}
		},
	}

	n, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
	}})
	if n != 0 || err == nil || !strings.Contains(err.Error(), "directory sync failed") {
		t.Fatalf("AddSymbols = (%d, %v), want directory sync failure", n, err)
	}
	if inserts != 0 {
		t.Fatalf("database insert ran after directory sync failure: %d", inserts)
	}
	markers, globErr := filepath.Glob(filepath.Join(dataDir, "recovery", "exsymbol-*.pending.json"))
	if globErr != nil || len(markers) != 0 {
		t.Fatalf("marker must not be published before directory sync succeeds: markers=%v err=%v", markers, globErr)
	}
}

func TestExplicitSymbolStateRequiresRecoveryDirectoryBinding(t *testing.T) {
	oldQuest, oldDataDir := IsQuestDB, config.DataDir
	IsQuestDB = true
	config.DataDir = t.TempDir()
	t.Cleanup(func() {
		IsQuestDB = oldQuest
		config.DataDir = oldDataDir
	})
	marker := filepath.Join(config.DataDir, "recovery", "exsymbol-malformed.pending.json")
	if err := os.MkdirAll(filepath.Dir(marker), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	state := NewSymbolState()
	dbCalls := 0
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		dbCalls++
		return visibilityRowStub{}
	}}

	_, err := New(db).WithSymbolState(state).AddSymbols(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "no recovery directory") {
		t.Fatalf("unbound explicit state error = %v", err)
	}
	if dbCalls != 0 {
		t.Fatalf("unbound explicit state made %d database calls", dbCalls)
	}
}

func exSymbolRecoveryDBRow(row exSymbolRecoveryRow, err error) pgx.Row {
	return visibilityRowStub{scan: func(dest ...interface{}) error {
		if err != nil {
			return err
		}
		*dest[0].(*int32) = row.ID
		*dest[1].(*string) = row.Exchange
		*dest[2].(*string) = row.ExgReal
		*dest[3].(*string) = row.Market
		*dest[4].(*string) = row.Symbol
		*dest[5].(*bool) = row.Combined
		*dest[6].(*int64) = row.ListMs
		*dest[7].(*int64) = row.DelistMs
		*dest[8].(*string) = row.AggRules
		return nil
	}}
}
