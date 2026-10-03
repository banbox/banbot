package orm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestQuestMetadataBatchVersionsSurviveClockRollback(t *testing.T) {
	installMetadataVersionTestRoot(t)
	var versions []time.Time
	db := &visibilityDBStub{exec: func(_ string, args ...any) (pgconn.CommandTag, error) {
		for i := 0; i < len(args); i += 5 {
			versions = append(versions, args[i].(time.Time))
		}
		return pgconn.CommandTag{}, nil
	}}
	q := New(db)
	factors := []*AdjFactor{{Sid: 1, StartMs: 1}, {Sid: 1, StartMs: 2}, {Sid: 1, StartMs: 3}}
	future := time.Now().UTC().Add(time.Hour)
	if err := batchInsertAdjFactorsDeleted(context.Background(), q, factors, future); err != nil {
		t.Fatal(err)
	}
	if err := batchInsertAdjFactorsDeleted(context.Background(), New(db), factors, future.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(versions); i++ {
		if !versions[i].After(versions[i-1]) {
			t.Fatalf("version %d regressed: %s <= %s", i, versions[i], versions[i-1])
		}
	}
}

func TestQuestMetadataTombstoneDoesNotResurrectOldAdjFactor(t *testing.T) {
	db := &visibilityDBStub{query: func(sql string, _ ...any) (pgx.Rows, error) {
		// QuestDB's same-layer predicate is evaluated before latest selection.
		// A closed latest subquery must instead select the tombstone first.
		latest := strings.Index(sql, "LATEST BY sid, sub_id, start_ms")
		filter := strings.Index(sql, "coalesce(is_deleted, false)")
		if latest < 0 || filter < latest || !strings.Contains(sql[latest:filter], ")") {
			return newInterfaceRows([][]any{{int32(1), int32(2), int64(10), float64(3)}}), nil
		}
		return newInterfaceRows(nil), nil
	}}
	got, err := New(db).getAdjFactorsQuest(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("deleted latest factor resurrected: %+v", got)
	}
}

func TestQuestMetadataReleaseVersionFollowsOwnerVersion(t *testing.T) {
	installMetadataVersionTestRoot(t)
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })
	owner := normalizeQuestTimestamp(time.Now().Add(time.Hour))
	var released time.Time
	db := &visibilityDBStub{exec: func(_ string, args ...any) (pgconn.CommandTag, error) {
		released = args[2].(time.Time)
		return pgconn.CommandTag{}, nil
	}}
	if err := New(db).DelInsKline(context.Background(), 823412, "1m", owner); err != nil {
		t.Fatal(err)
	}
	if !released.After(owner) {
		t.Fatalf("release reuses/regresses owner version: %s <= %s", released, owner)
	}
}

func installMetadataVersionTestRoot(t *testing.T) {
	t.Helper()
	previous := compactProcessLockRootFn
	root := t.TempDir()
	compactProcessLockRootFn = func() string { return root }
	t.Cleanup(func() { compactProcessLockRootFn = previous })
}

// Extra queries establish the metadata allocator's initial WAL/version floor;
// legacy fixtures that focus on another operation start with an empty catalog.
func metadataTestQueryRow(sql string) (pgx.Row, bool) {
	if strings.HasPrefix(sql, "SELECT coalesce(cast(max(ts) as long), 0) FROM") {
		return scriptedRow{values: []any{int64(0)}}, true
	}
	if strings.HasPrefix(sql, "SELECT sequencerTxn, writerTxn, writerLagTxnCount, suspended") {
		return scriptedRow{values: []any{int64(0), int64(0), int64(0), false}}, true
	}
	return nil, false
}

func TestQuestCompactSelectionFiltersTombstonesAfterLatest(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "timeframe", Type: "SYMBOL", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "is_deleted", Type: "BOOLEAN"},
	}
	sql, err := buildQuestCompactRewriteSQL("ins_kline_q_compact", "ins_kline_q", compactTables["ins_kline_q"], columns)
	if err != nil {
		t.Fatal(err)
	}
	latest := strings.Index(sql, "LATEST BY")
	filter := strings.Index(sql, `coalesce("is_deleted", false)`)
	if latest < 0 || filter < latest || !strings.Contains(sql[latest:filter], ")") {
		t.Fatalf("compact would copy resurrected rows: %s", sql)
	}
}

func TestQuestMetadataMutableRangeFilterDoesNotResurrectOldSpan(t *testing.T) {
	db := &visibilityDBStub{query: func(sql string, _ ...any) (pgx.Rows, error) {
		latest := strings.Index(sql, "LATEST BY sid, tbl, timeframe, start_ms")
		filter := strings.Index(sql, "stop_ms > $4")
		if latest < 0 || filter < latest || !strings.Contains(sql[latest:filter], ")") {
			// The old span overlaps the requested window, but its latest
			// replacement ends before it. Filtering stop_ms first resurrects it.
			return newInterfaceRows([][]any{{int64(1), int64(100), true, time.Now().UTC()}}), nil
		}
		return newInterfaceRows(nil), nil
	}}
	got, err := New(db).loadSRangesSpansFromDB(context.Background(), 1, "x", "1m", 50, 100)
	if err != nil || len(got) != 0 {
		t.Fatalf("outdated mutable range resurrected: %+v, %v", got, err)
	}
}
func TestQuestMetadataVersionReservationsPersistAndSeparateNamespaces(t *testing.T) {
	root := t.TempDir()
	clock := time.Date(2030, 1, 1, 0, 0, 0, 999, time.UTC)
	first, err := reserveQuestMetadataVersions(context.Background(), root, "adj_factors_q", 4, clock, func() (time.Time, error) { return clock.Add(time.Hour), nil })
	if err != nil {
		t.Fatal(err)
	}
	// A new allocator call reopens the durable highwater, even with an older
	// clock and stale database view. Reserved versions need not be WAL-visible.
	second, err := reserveQuestMetadataVersions(context.Background(), root, "adj_factors_q", 2, clock.Add(-time.Hour), func() (time.Time, error) { return clock, nil })
	if err != nil {
		t.Fatal(err)
	}
	if !second.After(first.Add(3 * time.Microsecond)) {
		t.Fatalf("reopened reservation overlaps prior batch: %s vs %s", second, first)
	}
	other, err := reserveQuestMetadataVersions(context.Background(), t.TempDir(), "adj_factors_q", 1, clock, func() (time.Time, error) { return time.Time{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if !other.Before(first) || other.Nanosecond()%1000 != 0 {
		t.Fatalf("namespace was not isolated or timestamp not normalized: %s", other)
	}
}

func TestQuestMetadataConcurrentReservationsDoNotOverlap(t *testing.T) {
	root := t.TempDir()
	clock := time.Now().UTC()
	starts := make(chan time.Time, 12)
	errorsCh := make(chan error, 12)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start, err := reserveQuestMetadataVersions(context.Background(), root, "calendars_q", 5, clock, func() (time.Time, error) { return time.Time{}, nil })
			starts <- start
			errorsCh <- err
		}()
	}
	wg.Wait()
	close(starts)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[time.Time]bool)
	for start := range starts {
		for i := range 5 {
			version := start.Add(time.Duration(i) * time.Microsecond)
			if seen[version] {
				t.Fatalf("overlapping reserved version %s", version)
			}
			seen[version] = true
		}
	}
}

func TestQuestMetadataReservationFailurePreventsInsert(t *testing.T) {
	oldQuest, oldRoot := IsQuestDB, compactProcessLockRootFn
	IsQuestDB = true
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "metadata_calendars_q.version"), 0700); err != nil {
		t.Fatal(err)
	}
	compactProcessLockRootFn = func() string { return root }
	t.Cleanup(func() { IsQuestDB, compactProcessLockRootFn = oldQuest, oldRoot })
	writes := 0
	db := &visibilityDBStub{exec: func(_ string, _ ...any) (pgconn.CommandTag, error) {
		writes++
		return pgconn.CommandTag{}, nil
	}}
	_, err := New(db).AddCalendars(context.Background(), []AddCalendarsParams{{Name: "test", StartMs: 1, StopMs: 2}})
	if err == nil || writes != 0 {
		t.Fatalf("failed reservation still inserted: error=%v writes=%d", err, writes)
	}
}

func TestQuestMetadataReservationPropagatesSeedFailure(t *testing.T) {
	want := errors.New("metadata snapshot unavailable")
	_, err := reserveQuestMetadataVersions(context.Background(), t.TempDir(), "calendars_q", 1, time.Now(), func() (time.Time, error) { return time.Time{}, want })
	if !errors.Is(err, want) {
		t.Fatalf("seed error = %v", err)
	}
}

type metadataSeedDB struct {
	visibilityDBStub
	read func(string, ...any) pgx.Row
}

func (db *metadataSeedDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	return db.read(sql, args...)
}

func TestQuestMetadataInitialSeedWaitsForOldWALAndReadsMaxOnce(t *testing.T) {
	installMetadataVersionTestRoot(t)
	installCompactWaitScript(t, 3)
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	walReads, maxReads := 0, 0
	db := &metadataSeedDB{read: func(sql string, _ ...any) pgx.Row {
		if strings.Contains(sql, "wal_tables()") {
			walReads++
			writer := int64(4)
			if walReads > 1 {
				writer = 5
			}
			return scriptedRow{values: []any{int64(5), writer, int64(0), false}}
		}
		maxReads++
		if walReads != 2 {
			t.Errorf("max read before old WAL became visible: %d checks", walReads)
		}
		return scriptedRow{values: []any{future.UnixMicro()}}
	}}
	q := New(db)
	first, err := q.reserveMetadataVersions(context.Background(), "calendars_q", 2, time.Time{})
	if err != nil || !first.After(future) {
		t.Fatalf("initial reservation=%s err=%v, old visible version=%s", first, err, future)
	}
	second, err := New(db).reserveMetadataVersions(context.Background(), "calendars_q", 1, time.Time{})
	if err != nil || !second.After(first.Add(time.Microsecond)) || walReads != 2 || maxReads != 1 {
		t.Fatalf("existing highwater re-read stale snapshot: first=%s second=%s reads=%d/%d err=%v", first, second, walReads, maxReads, err)
	}
}

func TestQuestMetadataInitialSeedTimeoutPreventsInsert(t *testing.T) {
	installMetadataVersionTestRoot(t)
	installCompactWaitScript(t, 2)
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })
	writes := 0
	db := &metadataSeedDB{
		visibilityDBStub: visibilityDBStub{exec: func(string, ...any) (pgconn.CommandTag, error) {
			writes++
			return pgconn.CommandTag{}, nil
		}},
		read: func(sql string, _ ...any) pgx.Row {
			if !strings.Contains(sql, "wal_tables()") {
				t.Errorf("seed read incomplete WAL snapshot: %s", sql)
			}
			return scriptedRow{values: []any{int64(5), int64(4), int64(1), false}}
		},
	}
	_, err := New(db).AddCalendars(context.Background(), []AddCalendarsParams{{Name: "test", StartMs: 1, StopMs: 2}})
	if err == nil || !strings.Contains(err.Error(), "visibility timeout") || writes != 0 {
		t.Fatalf("WAL timeout result: err=%v writes=%d", err, writes)
	}
	if _, err := os.Stat(filepath.Join(compactProcessLockRootFn(), "metadata_calendars_q.version")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete initial WAL snapshot published highwater: %v", err)
	}
	db.read = func(sql string, _ ...any) pgx.Row {
		if strings.Contains(sql, "wal_tables()") {
			return scriptedRow{values: []any{int64(5), int64(5), int64(0), false}}
		}
		return scriptedRow{values: []any{time.Now().UnixMicro()}}
	}
	if _, err := New(db).AddCalendars(context.Background(), []AddCalendarsParams{{Name: "test", StartMs: 1, StopMs: 2}}); err != nil || writes != 1 {
		t.Fatalf("recovery after WAL catchup did not initialize safely: err=%v writes=%d", err, writes)
	}
}

func TestQuestMetadataInitialSeedRejectsSuspendedWAL(t *testing.T) {
	installMetadataVersionTestRoot(t)
	db := &metadataSeedDB{read: func(sql string, _ ...any) pgx.Row {
		if !strings.Contains(sql, "wal_tables()") {
			t.Errorf("read max from suspended metadata WAL: %s", sql)
		}
		return scriptedRow{values: []any{int64(5), int64(4), int64(1), true}}
	}}
	_, err := New(db).reserveMetadataVersions(context.Background(), "calendars_q", 1, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "suspended") {
		t.Fatalf("suspended metadata WAL accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(compactProcessLockRootFn(), "metadata_calendars_q.version")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("suspended snapshot published highwater: %v", err)
	}
}

func TestQuestMetadataCorruptHighwaterAndMissingIdentityFailClosed(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "metadata_calendars_q.version"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, metadataRoot := range []string{root, ""} {
		_, err := reserveQuestMetadataVersions(context.Background(), metadataRoot, "calendars_q", 1, time.Now(), nil)
		if err == nil {
			t.Fatalf("unsafe metadata reservation for root %q succeeded", metadataRoot)
		}
	}
}

func TestQuestMetadataDirectoryFlushFailureKeepsReservedHighwater(t *testing.T) {
	root := t.TempDir()
	previous := syncExSymbolRecoveryDir
	want := errors.New("directory flush failed")
	syncExSymbolRecoveryDir = func(string) error { return want }
	t.Cleanup(func() { syncExSymbolRecoveryDir = previous })
	_, err := reserveQuestMetadataVersions(context.Background(), root, "calendars_q", 3, time.Now(), nil)
	if !errors.Is(err, want) {
		t.Fatalf("flush failure not propagated: %v", err)
	}
	payload, err := os.ReadFile(filepath.Join(root, "metadata_calendars_q.version"))
	if err != nil {
		t.Fatal("published highwater removed on failure:", err)
	}
	highwater, err := strconv.ParseInt(strings.TrimSpace(string(payload)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	syncExSymbolRecoveryDir = previous
	start, err := reserveQuestMetadataVersions(context.Background(), root, "calendars_q", 1, time.UnixMicro(highwater-10), nil)
	if err != nil || start.UnixMicro() <= highwater {
		t.Fatalf("retry reused failed reservation: %s <= %d, %v", start, highwater, err)
	}
}
