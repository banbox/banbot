package orm

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type visibilityDBStub struct {
	exec     func(sql string, args ...interface{}) (pgconn.CommandTag, error)
	query    func(sql string, args ...interface{}) (pgx.Rows, error)
	queryRow func(sql string, args ...interface{}) pgx.Row
}

func (s *visibilityDBStub) Exec(_ context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	if s.exec == nil {
		panic("unexpected Exec call")
	}
	return s.exec(sql, args...)
}

func (s *visibilityDBStub) Query(_ context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	if s.query == nil {
		panic("unexpected Query call")
	}
	return s.query(sql, args...)
}

func (s *visibilityDBStub) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	return s.queryRow(sql, args...)
}

func (s *visibilityDBStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unexpected CopyFrom call")
}

type visibilityRowStub struct {
	scan func(dest ...interface{}) error
}

func (r visibilityRowStub) Scan(dest ...interface{}) error {
	return r.scan(dest...)
}

type questVisibilityExchangeStub struct {
	banexg.BanExchange
	info *banexg.ExgInfo
}

func (e *questVisibilityExchangeStub) Info() *banexg.ExgInfo {
	return e.info
}

func installQuestVisibilitySymbol(t *testing.T, exs *ExSymbol) {
	t.Helper()
	state := NewSymbolState()
	if exs != nil {
		state.CacheExSymbol(exs)
	}
	previous := swapDefaultSymbolState(state)
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
}

func installQuestWaitScript(t *testing.T, checks int) {
	t.Helper()
	old := questWaitForCondition
	questWaitForCondition = func(_ context.Context, _ time.Duration, _ time.Duration, check func() (bool, error)) (bool, error) {
		for i := 0; i < checks; i++ {
			ok, err := check()
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	t.Cleanup(func() { questWaitForCondition = old })
}

type rewriteExecStub struct {
	exec func(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func (s *rewriteExecStub) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return s.exec(ctx, sql, args...)
}

func TestDropQuestRewriteTableUsesRecoveryContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var usable bool
	var gotSQL string
	db := &rewriteExecStub{exec: func(ctx context.Context, sql string, _ ...interface{}) (pgconn.CommandTag, error) {
		usable = ctx.Err() == nil
		gotSQL = sql
		return pgconn.CommandTag{}, nil
	}}
	if err := dropQuestRewriteTable(ctx, db, "tmp_table"); err != nil {
		t.Fatal(err)
	}
	if !usable {
		t.Fatal("cleanup used the canceled rewrite context")
	}
	if gotSQL != `DROP TABLE IF EXISTS "tmp_table"` {
		t.Fatalf("cleanup SQL = %q", gotSQL)
	}
}

func TestDropQuestRewriteTablePropagatesCleanupError(t *testing.T) {
	wantErr := errors.New("drop failed")
	db := &rewriteExecStub{exec: func(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, wantErr
	}}
	err := dropQuestRewriteTable(context.Background(), db, "tmp_table")
	if !errors.Is(err, wantErr) {
		t.Fatalf("cleanup error = %v, want %v", err, wantErr)
	}
}

func TestQueryQuestTableColumnsUsesFieldNamesWhenQuestDBReordersMetadata(t *testing.T) {
	rows := &interfaceRows{
		rows: [][]any{{
			"DOUBLE",   // type
			"metric",   // column
			false,      // indexed
			int64(256), // index_block_capacity
			false,      // symbol_cached
			int64(0),   // symbol_capacity
			true,       // designated
			true,       // upsert_key
			"",         // index_type
			"",         // index_include
		}},
		idx:          -1,
		valuesErrAt:  -1,
		descriptions: questTableColumnDescriptions("type", "column", "indexed", "index_block_capacity", "symbol_cached", "symbol_capacity", "designated", "upsert_key", "index_type", "index_include"),
	}
	db := &visibilityDBStub{query: func(string, ...interface{}) (pgx.Rows, error) { return rows, nil }}

	columns, err := queryQuestTableColumnsDB(context.Background(), db, "series_q")
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 1 {
		t.Fatalf("columns = %#v, want one column", columns)
	}
	got := columns[0]
	if got.Name != "metric" || got.Type != "DOUBLE" || !got.Designated || !got.UpsertKey || got.IndexBlockCapacity != 256 {
		t.Fatalf("reordered metadata decoded incorrectly: %+v", got)
	}
}

func questTableColumnDescriptions(names ...string) []pgconn.FieldDescription {
	descriptions := make([]pgconn.FieldDescription, len(names))
	for i, name := range names {
		descriptions[i].Name = name
	}
	return descriptions
}

func questTestFingerprint(columns []questTableColumn, rows [][]any) [sha256.Size]byte {
	h := sha256.New()
	writeQuestFingerprintHeader(h, columns)
	for _, values := range rows {
		h.Write([]byte{0x52})
		writeQuestFingerprintUint(h, uint64(len(values)))
		for _, value := range values {
			writeQuestFingerprintBytes(h, appendQuestFingerprintValue(nil, value))
		}
	}
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], h.Sum(nil))
	return fingerprint
}

func TestAddSymbolsQuestVisibilityTimeoutKeepsPendingIdentity(t *testing.T) {
	oldQuest := IsQuestDB
	oldDataDir := config.DataDir
	IsQuestDB = true
	installQuestWaitScript(t, 1)
	config.DataDir = t.TempDir()
	allocator := NewSIDAllocator()
	cleanupSharedSIDReservations(t, allocator)
	state := NewSymbolStateWithAllocator(allocator)
	state.SetMaxSID(41)
	oldState := swapDefaultSymbolState(state)
	defer func() {
		IsQuestDB = oldQuest
		config.DataDir = oldDataDir
		swapDefaultSymbolState(oldState)
	}()

	var insertArgs []interface{}
	db := &visibilityDBStub{
		exec: func(_ string, args ...interface{}) (pgconn.CommandTag, error) {
			insertArgs = append([]interface{}{}, args...)
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			switch {
			case strings.Contains(sql, "SELECT max(sid) FROM exsymbol_q"):
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					var maxVal *int32
					*dest[0].(**int32) = maxVal
					return nil
				}}
			case strings.Contains(sql, "LATEST BY sid"):
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					return pgx.ErrNoRows
				}}
			default:
				t.Fatalf("unexpected sql: %s", sql)
				return visibilityRowStub{scan: func(dest ...interface{}) error { return nil }}
			}
		},
	}

	n, err := New(db).AddSymbols(context.Background(), []AddSymbolsParams{{
		Exchange: "binance",
		ExgReal:  "binance",
		Market:   "spot",
		Symbol:   "BTC/USDT",
		Combined: true,
		ListMs:   123,
		DelistMs: 456,
	}})
	var timeoutErr *errs.Error
	if err == nil || !errors.As(err, &timeoutErr) || timeoutErr.Code != core.ErrTimeout ||
		!strings.Contains(timeoutErr.Short(), "exsymbol rows not visible before timeout") {
		t.Fatalf("AddSymbols timeout = %v, want retryable visibility timeout", err)
	}
	if n != 1 {
		t.Fatalf("expected one inserted row, got %d", n)
	}
	if len(insertArgs) != 10 {
		t.Fatalf("expected full metadata insert args, got %d", len(insertArgs))
	}

	if got := GetExSymbol2("binance", "spot", "BTC/USDT"); got != nil {
		t.Fatalf("unconfirmed WAL row was cached: %+v", got)
	}
	key := exSymbolKey("binance", "spot", "BTC/USDT")
	if got := allocator.reservedSID(key); got != 0 {
		t.Fatalf("unconfirmed WAL row became confirmed reservation: %d", got)
	}
	if got := allocator.pendingSID(key); got != 42 {
		t.Fatalf("pending WAL reservation = %d, want 42", got)
	}
	sibling := NewSymbolStateWithAllocator(allocator)
	target := &ExSymbol{Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	if resolved, resolveErr := resolveEnsuredSymbol(allocator, sibling, target); resolveErr != nil || resolved != nil {
		t.Fatalf("shared allocator exposed unconfirmed identity: resolved=%+v target=%+v", resolved, target)
	}
	markers, err := filepath.Glob(filepath.Join(config.DataDir, "recovery", "exsymbol-*.pending.json"))
	if err != nil {
		t.Fatalf("find recovery markers: %v", err)
	}
	if len(markers) != 1 {
		t.Fatalf("visibility timeout must retain one recovery marker, got %v", markers)
	}
}

func TestWaitForQuestKlineCoverageVisibleBypassesCache(t *testing.T) {
	installQuestWaitScript(t, 3)

	const (
		sid   = int32(777)
		start = int64(100)
		stop  = int64(200)
	)
	srangesCacheUpdate(sid, "kline_15m", "15m", []srangeSpan{{StartMs: start, StopMs: stop, HasData: true}})
	defer srangesCacheDel(sid, "kline_15m", "15m")

	queries := 0
	db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
		queries++
		if queries < 3 {
			return &staleCoveredRows{idx: 0}, nil
		}
		return &staleCoveredRows{idx: -1, startMS: start, stopMS: stop}, nil
	}}
	if err := waitForQuestKlineCoverageVisible(context.Background(), New(db), sid, "15m", start, stop); err != nil {
		t.Fatal(err)
	}
	if queries != 3 {
		t.Fatalf("cache must not satisfy DB visibility wait: queries=%d", queries)
	}
}

func TestWaitForQuestKlineCoverageVisibleTimeoutIsRetryable(t *testing.T) {
	installQuestWaitScript(t, 1)

	db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
		return &staleCoveredRows{idx: 0}, nil
	}}
	err := waitForQuestKlineCoverageVisible(context.Background(), New(db), 780, "1m", 100, 200)
	if err == nil || err.Code != core.ErrTimeout || !strings.Contains(err.Short(), "coverage not visible before timeout") {
		t.Fatalf("expected retryable coverage timeout, got %v", err)
	}
}

func TestWaitForQuestSeriesCoverageVisiblePollsPersistedKnownEmptyCoverage(t *testing.T) {
	installQuestWaitScript(t, 3)

	const (
		sid   = int32(778)
		start = int64(100)
		stop  = int64(200)
	)
	info := NewSeriesInfo("macro", "1h", []SeriesField{{Name: "value", Type: "float"}})
	srangesCacheUpdate(sid, info.Binding.Table, info.TimeFrame, []srangeSpan{{StartMs: start, StopMs: stop, HasData: true}})
	defer srangesCacheDel(sid, info.Binding.Table, info.TimeFrame)

	queries := 0
	db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
		queries++
		if queries < 3 {
			return &staleCoveredRows{idx: 0}, nil
		}
		return &staleCoveredRows{idx: -1, startMS: start, stopMS: stop, knownEmpty: true}, nil
	}}
	if err := waitForQuestSeriesCoverageVisible(context.Background(), New(db), info, sid, start, stop); err != nil {
		t.Fatal(err)
	}
	if queries != 3 {
		t.Fatalf("cache must not satisfy series coverage visibility wait: queries=%d", queries)
	}
}

func TestWaitForQuestSeriesCoverageVisibleTimesOut(t *testing.T) {
	installQuestWaitScript(t, 1)

	queries := 0
	db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
		queries++
		return &staleCoveredRows{idx: 0}, nil
	}}
	info := NewSeriesInfo("macro", "1h", []SeriesField{{Name: "value", Type: "float"}})
	err := waitForQuestSeriesCoverageVisible(context.Background(), New(db), info, 779, 100, 200)
	if err == nil || !strings.Contains(err.Short(), "coverage not visible before timeout") {
		t.Fatalf("expected series coverage timeout, got %v", err)
	}
	if queries != 1 {
		t.Fatalf("expected one deterministic visibility check before timeout, queries=%d", queries)
	}
}

func TestWaitForQuestExsymbolVisiblePollsUntilRowVisible(t *testing.T) {
	installQuestWaitScript(t, 3)

	calls := 0
	db := &visibilityDBStub{
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			calls++
			if calls < 3 {
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					return pgx.ErrNoRows
				}}
			}
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				*dest[0].(*int32) = 7
				*dest[1].(*string) = "binance"
				*dest[2].(*string) = "binance"
				*dest[3].(*string) = "spot"
				*dest[4].(*string) = "BTC/USDT"
				*dest[5].(*bool) = false
				*dest[6].(*int64) = 0
				*dest[7].(*int64) = 0
				return nil
			}}
		},
	}

	item, err := waitForQuestExsymbolVisible(context.Background(), New(db), 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if item == nil || item.ID != 7 || item.Symbol != "BTC/USDT" {
		t.Fatalf("unexpected item: %+v", item)
	}
	if calls < 3 {
		t.Fatalf("expected polling, calls=%d", calls)
	}
}

func TestQuestExsymbolVisibilityPropagatesHardQueryFailure(t *testing.T) {
	checks := []struct {
		name string
		call func(*Queries) error
	}{
		{name: "batch", call: func(q *Queries) error {
			_, err := questExsymbolsVisible(context.Background(), q, []exSymbolRecoveryRow{{
				ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
			}})
			return err
		}},
		{name: "single", call: func(q *Queries) error {
			_, _, err := pollQuestExsymbolVisible(context.Background(), q, 7)
			return err
		}},
	}
	failures := []struct {
		name string
		err  error
	}{
		{name: "connection", err: errors.New("connection lost")},
		{name: "syntax", err: &pgconn.PgError{Code: "42601", Message: "syntax error"}},
		{name: "permission", err: &pgconn.PgError{Code: "42501", Message: "permission denied"}},
	}
	for _, check := range checks {
		for _, failure := range failures {
			t.Run(check.name+"/"+failure.name, func(t *testing.T) {
				calls := 0
				db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
					calls++
					return visibilityRowStub{scan: func(...interface{}) error { return failure.err }}
				}}
				if err := check.call(New(db)); !errors.Is(err, failure.err) {
					t.Fatalf("visibility error = %v, want hard error", err)
				}
				if calls != 1 {
					t.Fatalf("hard query failure was retried %d times", calls)
				}
			})
		}
	}
}

func TestQuestExsymbolsVisibleWaitsForExpectedRow(t *testing.T) {
	installQuestWaitScript(t, 3)

	expected := exSymbolRecoveryRow{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	calls := 0
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		calls++
		row := expected
		if calls < 3 {
			row.Symbol = "ETH/USDT"
		}
		return exSymbolRecoveryDBRow(row, nil)
	}}

	visible, err := questExsymbolsVisible(context.Background(), New(db), []exSymbolRecoveryRow{expected})
	if err != nil || !visible {
		t.Fatalf("visibility = (%v, %v), want confirmed expected row", visible, err)
	}
	if calls != 3 {
		t.Fatalf("mismatched row should remain unconfirmed, calls=%d", calls)
	}
}

func TestWaitForQuestConditionHandlesNilAndCanceledContext(t *testing.T) {
	if ok, err := waitForQuestCondition(nil, 0, time.Millisecond, func() (bool, error) { return false, nil }); ok || err != nil {
		t.Fatalf("nil context wait = (%v, %v), want timeout", ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := waitForQuestCondition(ctx, time.Second, time.Millisecond, func() (bool, error) { return false, nil }); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context wait = (%v, %v), want context.Canceled", ok, err)
	}
}

func TestSeriesVisibilityWaitsOnlyForNoRows(t *testing.T) {
	installQuestWaitScript(t, 4)

	info := NewSeriesInfo("macro", "1m", []SeriesField{{Name: "value", Type: "float"}})
	t.Run("no rows then visible", func(t *testing.T) {
		calls := 0
		db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
			calls++
			switch calls {
			case 1, 3:
				return visibilityRowStub{scan: func(...interface{}) error { return pgx.ErrNoRows }}
			case 2:
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					*dest[0].(*int64) = 1
					*dest[1].(*int64) = 1
					return nil
				}}
			default:
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					maxTime := int64(100)
					*dest[0].(**int64) = &maxTime
					return nil
				}}
			}
		}}
		if err := waitForQuestSeriesVisible(context.Background(), New(db), info, 7, 100); err != nil {
			t.Fatalf("wait after no rows: %v", err)
		}
		if calls != 4 {
			t.Fatalf("visibility checks = %d, want 4", calls)
		}
	})

	for _, phase := range []string{"wal", "row"} {
		t.Run("hard error/"+phase, func(t *testing.T) {
			wantErr := errors.New("connection lost")
			calls := 0
			db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
				calls++
				if phase == "row" && calls == 1 {
					return visibilityRowStub{scan: func(dest ...interface{}) error {
						*dest[0].(*int64) = 1
						*dest[1].(*int64) = 1
						return nil
					}}
				}
				return visibilityRowStub{scan: func(...interface{}) error { return wantErr }}
			}}
			if err := waitForQuestSeriesVisible(context.Background(), New(db), info, 7, 100); !errors.Is(err, wantErr) {
				t.Fatalf("hard visibility error = %v, want %v", err, wantErr)
			}
			wantCalls := 1
			if phase == "row" {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("hard error retried: calls=%d want=%d", calls, wantCalls)
			}
		})
	}
}

func TestKLineSeriesVisibilityWaitsOnlyForNoRowsAndPreservesNull(t *testing.T) {
	installQuestWaitScript(t, 2)

	info := NewKLineSeriesInfo("custom", "1m", []SeriesField{{Name: "metric", Type: "float"}})
	row := &DataRecord{Sid: 7, TimeMS: 100, Values: nil}
	calls := 0
	db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
		calls++
		if calls == 1 {
			return visibilityRowStub{scan: func(...interface{}) error { return pgx.ErrNoRows }}
		}
		return visibilityRowStub{scan: func(dest ...interface{}) error {
			*dest[0].(*int32) = 7
			*dest[1].(*int64) = 100
			*dest[2].(*sql.NullFloat64) = sql.NullFloat64{}
			return nil
		}}
	}}
	if err := waitForQuestKLineSeriesVisible(context.Background(), New(db), info, row); err != nil {
		t.Fatalf("nullable visibility after no rows: %v", err)
	}
	if calls != 2 {
		t.Fatalf("visibility checks = %d, want 2", calls)
	}

	wantErr := errors.New("scan failed")
	calls = 0
	db.queryRow = func(string, ...interface{}) pgx.Row {
		calls++
		return visibilityRowStub{scan: func(...interface{}) error { return wantErr }}
	}
	if err := waitForQuestKLineSeriesVisible(context.Background(), New(db), info, row); !errors.Is(err, wantErr) {
		t.Fatalf("hard visibility error = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Fatalf("hard error retried: calls=%d", calls)
	}
}

func TestWaitForQuestKlineWindowVisiblePolls(t *testing.T) {
	installQuestWaitScript(t, 3)

	calls := 0
	db := &visibilityDBStub{
		query: func(sql string, args ...interface{}) (pgx.Rows, error) {
			if !strings.Contains(sql, "SELECT cast(ts as long)/1000") {
				t.Fatalf("unexpected sql: %s", sql)
			}
			calls++
			if calls < 3 {
				return nil, pgx.ErrNoRows
			}
			return newInterfaceRows([][]any{{int64(0)}}), nil
		},
	}

	visible, err := waitForQuestKlineWindowVisible(context.Background(), New(db), 1, "1m", 0, 60_000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !visible {
		t.Fatal("expected visible rows")
	}
	if calls < 3 {
		t.Fatalf("expected polling, calls=%d", calls)
	}
}

func TestWaitForQuestKlineWindowVisibleTimesOut(t *testing.T) {
	installQuestWaitScript(t, 1)

	calls := 0
	db := &visibilityDBStub{query: func(string, ...interface{}) (pgx.Rows, error) {
		calls++
		return newInterfaceRows(nil), nil
	}}
	visible, err := waitForQuestKlineWindowVisible(context.Background(), New(db), 1, "1m", 0, 60_000)
	if visible || err == nil || err.Code != core.ErrTimeout || !strings.Contains(err.Short(), "window not visible before timeout") {
		t.Fatalf("window visibility = (%v, %v), want timeout failure", visible, err)
	}
	if calls != 1 {
		t.Fatalf("expected one deterministic visibility check before timeout, calls=%d", calls)
	}
}

func TestWaitForQuestKlineVisibilityPropagatesHardQueryFailure(t *testing.T) {
	wantErr := errors.New("query connection lost")
	for _, check := range []struct {
		name string
		call func(*Queries) error
	}{
		{name: "window", call: func(q *Queries) error {
			_, err := waitForQuestKlineWindowVisible(context.Background(), q, 1, "1m", 0, 60_000)
			return err
		}},
		{name: "range", call: func(q *Queries) error {
			_, _, err := waitForQuestKlineRangeVisible(context.Background(), q, 1, "1m", 0, 60_000)
			return err
		}},
	} {
		t.Run(check.name, func(t *testing.T) {
			calls := 0
			db := &visibilityDBStub{query: func(string, ...interface{}) (pgx.Rows, error) {
				calls++
				return nil, wantErr
			}}
			if err := check.call(New(db)); !errors.Is(err, wantErr) {
				t.Fatalf("visibility error = %v, want %v", err, wantErr)
			}
			if calls != 1 {
				t.Fatalf("hard query failure was retried %d times", calls)
			}
		})
	}
}

func TestWaitForQuestKlineTimestampVisiblePolls(t *testing.T) {
	installQuestWaitScript(t, 3)

	calls := 0
	db := &visibilityDBStub{
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			if !strings.Contains(sql, "ts = cast($2 as timestamp)") {
				t.Fatalf("unexpected sql: %s", sql)
			}
			calls++
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				*dest[0].(*bool) = calls >= 3
				return nil
			}}
		},
	}

	if err := waitForQuestKlineTimestampVisible(context.Background(), New(db), 1, "1m", 60_000); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls < 3 {
		t.Fatalf("expected polling, calls=%d", calls)
	}
}

func TestWaitForQuestKlineTimestampVisibleTimesOut(t *testing.T) {
	installQuestWaitScript(t, 1)

	db := &visibilityDBStub{
		queryRow: func(string, ...interface{}) pgx.Row {
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				*dest[0].(*bool) = false
				return nil
			}}
		},
	}

	if err := waitForQuestKlineTimestampVisible(context.Background(), New(db), 1, "1m", 60_000); err == nil {
		t.Fatal("expected visibility timeout error")
	}
}

func TestWaitForQuestExsymbolTimestampVisibleNormalizesToMicroseconds(t *testing.T) {
	installQuestWaitScript(t, 1)

	want := time.Date(2026, time.May, 27, 0, 56, 2, 336_789_123, time.UTC)
	db := &visibilityDBStub{
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				maxTS := normalizeQuestTimestamp(want)
				*dest[0].(**time.Time) = &maxTS
				return nil
			}}
		},
	}

	if err := waitForQuestExsymbolTimestampVisible(context.Background(), New(db), 1337, want); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForQuestCalendarTimestampVisibleNormalizesToMicroseconds(t *testing.T) {
	installQuestWaitScript(t, 1)

	want := time.Date(2026, time.May, 27, 17, 4, 23, 129_654_321, time.UTC)
	db := &visibilityDBStub{
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				maxTS := normalizeQuestTimestamp(want)
				*dest[0].(**time.Time) = &maxTS
				return nil
			}}
		},
	}

	if err := waitForQuestCalendarTimestampVisible(context.Background(), New(db), "spot", want); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForQuestKlineRangeVisibleTimeoutKeepsPending(t *testing.T) {
	installQuestWaitScript(t, 1)

	db := &visibilityDBStub{
		query: func(string, ...interface{}) (pgx.Rows, error) {
			return newInterfaceRows(nil), nil
		},
	}

	start, end, err := waitForQuestKlineRangeVisible(context.Background(), New(db), 1, "1m", 100, 200)
	if err == nil || err.Code != core.ErrTimeout || !strings.Contains(err.Short(), "range not visible before timeout") {
		t.Fatalf("expected retryable range timeout, got %v", err)
	}
	if start != 0 || end != 0 {
		t.Fatalf("expected invisible range, got start=%d end=%d", start, end)
	}
}

func TestWaitForQuestKlineRangeVisibleRequiresPendingEdges(t *testing.T) {
	installQuestWaitScript(t, 2)
	installQuestVisibilitySymbol(t, &ExSymbol{ID: 1, Exchange: "fake", Market: "spot", Symbol: "FAKE/USDT"})
	oldGetWith := questGetWith
	questGetWith = func(string, string, string) (banexg.BanExchange, *errs.Error) {
		return &questVisibilityExchangeStub{info: &banexg.ExgInfo{FullDay: true, NoHoliday: true}}, nil
	}
	t.Cleanup(func() { questGetWith = oldGetWith })

	calls := 0
	db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
		calls++
		if calls == 1 {
			return newInterfaceRows([][]any{{int64(120_000)}}), nil
		}
		return newInterfaceRows([][]any{{int64(60_000)}, {int64(120_000)}}), nil
	}}

	start, end, err := waitForQuestKlineRangeVisible(context.Background(), New(db), 1, "1m", 60_000, 180_000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if start != 60_000 || end != 180_000 {
		t.Fatalf("visible range = (%d, %d), want pending range", start, end)
	}
	if calls != 2 {
		t.Fatalf("expected retry after broad historical range, calls=%d", calls)
	}
}

func TestQuestKlineWindowVisibleRejectsInteriorGap(t *testing.T) {
	installQuestWaitScript(t, 1)
	installQuestVisibilitySymbol(t, &ExSymbol{ID: 1, Exchange: "fake", Market: "spot", Symbol: "FAKE/USDT"})
	oldGetWith := questGetWith
	questGetWith = func(string, string, string) (banexg.BanExchange, *errs.Error) {
		return &questVisibilityExchangeStub{info: &banexg.ExgInfo{FullDay: true, NoHoliday: true}}, nil
	}
	t.Cleanup(func() { questGetWith = oldGetWith })

	db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
		return newInterfaceRows([][]any{{int64(60_000)}, {int64(180_000)}}), nil
	}}
	visible, err := waitForQuestKlineWindowVisible(context.Background(), New(db), 1, "1m", 60_000, 240_000)
	if visible || err == nil || err.Code != core.ErrTimeout {
		t.Fatalf("interior gap visibility = (%v, %v), want timeout", visible, err)
	}
}

func TestQuestKlineWindowVisiblePropagatesExchangeConstructionFailure(t *testing.T) {
	installQuestWaitScript(t, 3)
	installQuestVisibilitySymbol(t, &ExSymbol{ID: 1, Exchange: "fake", Market: "spot", Symbol: "FAKE/USDT"})

	wantErr := errs.New(core.ErrBadConfig, errors.New("exchange construction failed"))
	oldGetWith := questGetWith
	getWithCalls := 0
	questGetWith = func(string, string, string) (banexg.BanExchange, *errs.Error) {
		getWithCalls++
		return nil, wantErr
	}
	t.Cleanup(func() { questGetWith = oldGetWith })

	queryCalls := 0
	db := &visibilityDBStub{query: func(string, ...interface{}) (pgx.Rows, error) {
		queryCalls++
		return newInterfaceRows([][]any{{int64(60_000)}, {int64(180_000)}}), nil
	}}
	visible, err := waitForQuestKlineWindowVisible(context.Background(), New(db), 1, "1m", 60_000, 240_000)
	if visible || !errors.Is(err, wantErr) {
		t.Fatalf("exchange construction failure = (%v, %v), want immediate %v", visible, err, wantErr)
	}
	if err.Code == core.ErrTimeout {
		t.Fatalf("exchange construction failure was converted to timeout: %v", err)
	}
	if getWithCalls != 1 || queryCalls != 1 {
		t.Fatalf("exchange construction failure retried: getWith=%d query=%d", getWithCalls, queryCalls)
	}
}

func TestQuestKlineWindowVisiblePropagatesExchangeHolesFailure(t *testing.T) {
	installQuestWaitScript(t, 3)
	installQuestVisibilitySymbol(t, &ExSymbol{ID: 2, Exchange: "fake", Market: "spot", Symbol: "FAKE/USDT"})

	exchange := &questVisibilityExchangeStub{info: &banexg.ExgInfo{Min1mHole: 0}}
	wantErr := errs.New(core.ErrBadConfig, errors.New("session metadata failed"))
	oldGetWith := questGetWith
	oldGetExSHoles := questGetExSHoles
	getWithCalls, holesCalls := 0, 0
	questGetWith = func(string, string, string) (banexg.BanExchange, *errs.Error) {
		getWithCalls++
		return exchange, nil
	}
	questGetExSHoles = func(got banexg.BanExchange, _ *ExSymbol, _ int64, _ int64, _ bool) ([][2]int64, *errs.Error) {
		holesCalls++
		if got != exchange {
			return nil, errs.NewMsg(core.ErrRunTime, "unexpected fake exchange")
		}
		return nil, wantErr
	}
	t.Cleanup(func() {
		questGetWith = oldGetWith
		questGetExSHoles = oldGetExSHoles
	})

	queryCalls := 0
	db := &visibilityDBStub{query: func(string, ...interface{}) (pgx.Rows, error) {
		queryCalls++
		return newInterfaceRows([][]any{{int64(60_000)}, {int64(180_000)}}), nil
	}}
	visible, err := waitForQuestKlineWindowVisible(context.Background(), New(db), 2, "1m", 60_000, 240_000)
	if visible || !errors.Is(err, wantErr) {
		t.Fatalf("session metadata failure = (%v, %v), want immediate %v", visible, err, wantErr)
	}
	if err.Code == core.ErrTimeout {
		t.Fatalf("session metadata failure was converted to timeout: %v", err)
	}
	if getWithCalls != 1 || holesCalls != 1 || queryCalls != 1 {
		t.Fatalf("session metadata failure retried: getWith=%d holes=%d query=%d", getWithCalls, holesCalls, queryCalls)
	}
}

func TestQuestKlineWindowVisibleMissingSymbolFailsClosed(t *testing.T) {
	installQuestWaitScript(t, 1)
	installQuestVisibilitySymbol(t, nil)

	oldGetWith := questGetWith
	questGetWith = func(string, string, string) (banexg.BanExchange, *errs.Error) {
		t.Fatal("missing symbol attempted exchange construction")
		return nil, nil
	}
	t.Cleanup(func() { questGetWith = oldGetWith })

	db := &visibilityDBStub{query: func(string, ...interface{}) (pgx.Rows, error) {
		return newInterfaceRows([][]any{{int64(60_000)}, {int64(180_000)}}), nil
	}}
	visible, err := waitForQuestKlineWindowVisible(context.Background(), New(db), 3, "1m", 60_000, 240_000)
	if visible || err == nil || err.Code != core.ErrTimeout {
		t.Fatalf("missing symbol visibility = (%v, %v), want fail-closed timeout", visible, err)
	}
}

func TestQuestKlineWindowVisibleUsesExplicitSymbolState(t *testing.T) {
	sid := int32(4)
	legacy := NewSymbolState()
	legacy.CacheExSymbol(&ExSymbol{ID: sid, Exchange: "legacy", Market: "spot", Symbol: "LEGACY/USDT"})
	previous := swapDefaultSymbolState(legacy)
	t.Cleanup(func() { swapDefaultSymbolState(previous) })

	explicit := NewSymbolState()
	explicit.CacheExSymbol(&ExSymbol{ID: sid, Exchange: "explicit", Market: "spot", Symbol: "EXPLICIT/USDT"})
	exchange := &questVisibilityExchangeStub{info: &banexg.ExgInfo{Min1mHole: 0}}
	oldGetWith := questGetWith
	oldGetExSHoles := questGetExSHoles
	var got *ExSymbol
	questGetWith = func(name, market, _ string) (banexg.BanExchange, *errs.Error) {
		if name != "explicit" || market != "spot" {
			return nil, errs.NewMsg(core.ErrBadConfig, "legacy symbol was used: %s/%s", name, market)
		}
		return exchange, nil
	}
	questGetExSHoles = func(_ banexg.BanExchange, exs *ExSymbol, _ int64, _ int64, _ bool) ([][2]int64, *errs.Error) {
		got = exs
		return [][2]int64{{120_000, 180_000}}, nil
	}
	t.Cleanup(func() {
		questGetWith = oldGetWith
		questGetExSHoles = oldGetExSHoles
	})

	db := &visibilityDBStub{query: func(string, ...interface{}) (pgx.Rows, error) {
		return newInterfaceRows([][]any{{int64(60_000)}, {int64(180_000)}}), nil
	}}
	visible, err := questKlineWindowVisibleWithSymbolState(context.Background(), New(db), explicit, sid, "1m", 60_000, 240_000)
	if err != nil || !visible {
		t.Fatalf("explicit symbol visibility = (%v, %v), want visible", visible, err)
	}
	if got == nil || got.Exchange != "explicit" || got.Symbol != "EXPLICIT/USDT" {
		t.Fatalf("resolved symbol = %+v, want explicit runtime symbol", got)
	}
}

func TestUpdatePendingInsContinuesAfterQuestVisibilityTimeout(t *testing.T) {
	oldQuest := IsQuestDB
	oldDataDir := config.DataDir
	IsQuestDB = true
	config.DataDir = t.TempDir()
	installQuestWaitScript(t, 1)
	t.Cleanup(func() {
		IsQuestDB = oldQuest
		config.DataDir = oldDataDir
	})

	queryRows := 0
	db := &visibilityDBStub{
		query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
			if strings.Contains(sql, "FROM ins_kline_q") {
				queryRows++
				return newInterfaceRows([][]any{{int32(1), "1d", time.UnixMilli(100), int64(100), int64(200)}}), nil
			}
			if strings.Contains(sql, "SELECT cast(ts as long)/1000") {
				return newInterfaceRows(nil), nil
			}
			t.Fatalf("unexpected pending visibility query: %s", sql)
			return nil, nil
		},
	}

	if err := New(db).UpdatePendingIns(); err != nil {
		t.Fatalf("UpdatePendingIns returned timeout as fatal error: %v", err)
	}
	if queryRows != 1 {
		t.Fatalf("pending insert query count = %d, want 1", queryRows)
	}
}

func TestVerifyQuestRewriteSnapshotPollsUntilExpected(t *testing.T) {
	installQuestWaitScript(t, 3)

	calls := 0
	db := &visibilityDBStub{
		query: func(sql string, args ...interface{}) (pgx.Rows, error) {
			calls++
			if calls < 3 {
				return newRewriteSnapshotRows(map[int32]int64{1: 2}), nil
			}
			return newRewriteSnapshotRows(map[int32]int64{1: 2, 2: 3}), nil
		},
	}

	if err := verifyQuestRewriteSnapshot(context.Background(), New(db), "tmp_tbl", map[int32]int64{1: 2, 2: 3}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls < 3 {
		t.Fatalf("expected polling, calls=%d", calls)
	}
}

func TestVerifyQuestRewriteSnapshotRejectsSameTotalWrongDistribution(t *testing.T) {
	installQuestWaitScript(t, 1)

	db := &visibilityDBStub{
		query: func(string, ...interface{}) (pgx.Rows, error) {
			return newRewriteSnapshotRows(map[int32]int64{1: 3, 2: 2}), nil
		},
	}

	err := verifyQuestRewriteSnapshot(context.Background(), New(db), "tmp_tbl", map[int32]int64{1: 2, 2: 3})
	if err == nil {
		t.Fatal("expected snapshot distribution mismatch")
	}
}

func TestVerifyQuestRewriteTableSnapshotPropagatesHardQueryFailure(t *testing.T) {
	wantErr := errors.New("table metadata unavailable")
	calls := 0
	db := &visibilityDBStub{
		query: func(string, ...interface{}) (pgx.Rows, error) {
			calls++
			return nil, wantErr
		},
	}
	expected := &questRewriteTableSnapshot{
		Columns: []questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}},
		Counts:  map[int32]int64{},
	}

	err := verifyQuestRewriteTableSnapshot(context.Background(), New(db), "tmp", "sid", "", expected)
	if !errors.Is(err, wantErr) {
		t.Fatalf("snapshot verification error = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Fatalf("hard metadata failure was retried %d times", calls)
	}
}

func TestReplaceVerifiedQuestTableRejectsChangedSourceBeforeRename(t *testing.T) {
	installQuestWaitScript(t, 1)
	execSQL := make([]string, 0, 1)
	db := &visibilityDBStub{
		exec: func(sql string, _ ...interface{}) (pgconn.CommandTag, error) {
			execSQL = append(execSQL, sql)
			return pgconn.CommandTag{}, nil
		},
		query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
			switch {
			case strings.Contains(sql, "table_columns"):
				return newInterfaceRows([][]any{{"sid", "INT", false, true}, {"metric", "DOUBLE", false, false}}), nil
			case strings.Contains(sql, "count(*)"):
				return newInterfaceRows([][]any{{int32(7), int64(1)}}), nil
			default:
				return newInterfaceRows([][]any{{int32(7), float64(1.25)}}), nil
			}
		},
	}
	expected := &questRewriteTableSnapshot{
		Columns: []questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}, {Name: "metric", Type: "DOUBLE"}},
		Counts:  map[int32]int64{},
		Fingerprint: questTestFingerprint([]questTableColumn{
			{Name: "sid", Type: "INT", UpsertKey: true},
			{Name: "metric", Type: "DOUBLE"},
		}, nil),
	}
	err := replaceVerifiedQuestTable(context.Background(), New(db), "source", "tmp", "backup", "sid", "sid = 7", expected)
	if err == nil || !strings.Contains(err.Error(), "before rename") {
		t.Fatalf("changed source error = %v, want pre-rename validation failure", err)
	}
	if len(execSQL) != 1 || execSQL[0] != `DROP TABLE IF EXISTS "tmp"` {
		t.Fatalf("changed source did not only clean up the temporary table: %v", execSQL)
	}
}

func TestCaptureQuestRewriteTableSnapshotPreservesSchemaTypesAndNulls(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "metric", Type: "DOUBLE"},
		{Name: "note", Type: "STRING"},
	}
	rows := [][]any{
		{int32(7), time.UnixMilli(123).UTC(), float64(1.25), nil},
		{int32(7), time.UnixMilli(124).UTC(), nil, "second"},
	}
	var fingerprintSQL string
	db := &visibilityDBStub{
		query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
			switch {
			case strings.Contains(sql, "table_columns"):
				return newInterfaceRows([][]any{{"sid", "INT", false, true}, {"ts", "TIMESTAMP", true, true}, {"metric", "DOUBLE", false, false}, {"note", "STRING", false, false}}), nil
			case strings.Contains(sql, "count(*)"):
				return newInterfaceRows([][]any{{int32(7), int64(2)}}), nil
			default:
				fingerprintSQL = sql
				return newInterfaceRows(rows), nil
			}
		},
	}

	snapshot, err := captureQuestRewriteTableSnapshot(context.Background(), New(db), "series_q", "sid", "sid = 7")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RowCount != int64(len(rows)) {
		t.Fatalf("row count = %d, want %d", snapshot.RowCount, len(rows))
	}
	if snapshot.Fingerprint != questTestFingerprint(columns, rows) {
		t.Fatal("fingerprint did not include all values")
	}
	if got := snapshot.Columns[2]; got.Name != "metric" || got.Type != "DOUBLE" {
		t.Fatalf("custom column schema lost: %+v", snapshot.Columns)
	}
	for _, column := range []string{`"metric"`, `"note"`} {
		if !strings.Contains(fingerprintSQL, column) {
			t.Fatalf("fingerprint query omitted %s: %s", column, fingerprintSQL)
		}
	}
	if !strings.Contains(fingerprintSQL, `ORDER BY "sid", "ts", "metric", "note"`) || strings.Contains(fingerprintSQL, "LIMIT") {
		t.Fatalf("fingerprint query is not a stable full scan: %s", fingerprintSQL)
	}

	changed := *snapshot
	changed.Fingerprint = questTestFingerprint(columns, [][]any{
		{int32(7), time.UnixMilli(123).UTC(), float64(1.25), int64(0)},
		rows[1],
	})
	if equalQuestRewriteTableSnapshot(&changed, snapshot) {
		t.Fatal("SQL NULL and numeric zero compared equal")
	}
	changed.Fingerprint = questTestFingerprint(columns, [][]any{
		{int32(7), time.UnixMilli(123).UTC(), float32(1.25), nil},
		rows[1],
	})
	if equalQuestRewriteTableSnapshot(&changed, snapshot) {
		t.Fatal("different runtime field types compared equal")
	}
}

func TestBuildQuestRewriteSQLPlacesPhysicalCastInsideSelect(t *testing.T) {
	columns := []questTableColumn{
		{Name: "instrument", Type: "SYMBOL", Indexed: true, IndexBlockCapacity: 256, SymbolCached: true, SymbolCapacity: 128, UpsertKey: true, columnPropertiesKnown: true},
		{Name: "event_time", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "metric", Type: "DOUBLE"},
	}

	got, err := buildQuestRewriteSQLChecked("series_compact", "series", "instrument = 'BTC'", "month", "event_time", columns)
	if err != nil {
		t.Fatal(err)
	}
	wantCast := `CAST("instrument" AS SYMBOL CAPACITY 128 CACHE INDEX CAPACITY 256) AS "instrument"`
	castAt := strings.Index(got, wantCast)
	fromAt := strings.Index(got, `FROM "series"`)
	if castAt < 0 || fromAt < 0 || castAt > fromAt {
		t.Fatalf("physical cast is not in the SELECT projection: %q", got)
	}
	if strings.Contains(got[fromAt:], "CAST(") || strings.Contains(got, "),\nCAST(") {
		t.Fatalf("physical cast was appended after the CTAS projection: %q", got)
	}
	if !strings.Contains(got, `) TIMESTAMP("event_time") PARTITION BY MONTH WAL`) {
		t.Fatalf("CTAS schema clause is not after the closing projection: %q", got)
	}
}

func TestReplaceVerifiedQuestTableDropsBackupOnlyAfterValidation(t *testing.T) {
	columns := []questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}, {Name: "metric", Type: "DOUBLE"}}
	execSQL := make([]string, 0, 3)
	db := &visibilityDBStub{
		exec: func(sql string, _ ...interface{}) (pgconn.CommandTag, error) {
			execSQL = append(execSQL, sql)
			return pgconn.CommandTag{}, nil
		},
		query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
			if strings.Contains(sql, "table_columns") {
				return newInterfaceRows([][]any{{"sid", "INT", false, true}, {"metric", "DOUBLE", false, false}}), nil
			}
			return newInterfaceRows(nil), nil
		},
	}
	expected := &questRewriteTableSnapshot{
		Columns:     columns,
		Counts:      map[int32]int64{},
		Fingerprint: questTestFingerprint(columns, nil),
	}
	if err := replaceVerifiedQuestTable(context.Background(), New(db), "source", "tmp", "backup", "sid", "", expected); err != nil {
		t.Fatal(err)
	}
	if len(execSQL) != 3 || !strings.HasPrefix(execSQL[0], `RENAME TABLE "source"`) || !strings.HasPrefix(execSQL[1], `RENAME TABLE "tmp"`) || execSQL[2] != `DROP TABLE "backup"` {
		t.Fatalf("replacement order = %v", execSQL)
	}
}

func TestReplaceVerifiedQuestTableKeepsSourcePredicateAfterActivation(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "metric", Type: "DOUBLE"},
	}
	filteredRows := [][]any{{int32(7), float64(1.25)}}
	unfilteredRows := [][]any{{int32(7), float64(1.25)}, {int32(8), float64(9.5)}}
	execSQL := make([]string, 0, 3)
	db := &visibilityDBStub{
		exec: func(sql string, _ ...interface{}) (pgconn.CommandTag, error) {
			execSQL = append(execSQL, sql)
			return pgconn.CommandTag{}, nil
		},
		query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
			switch {
			case strings.Contains(sql, "table_columns"):
				return newInterfaceRows([][]any{
					{"sid", "INT", false, true},
					{"metric", "DOUBLE", false, false},
				}), nil
			case strings.Contains(sql, "count(*)"):
				if strings.Contains(sql, "WHERE sid = 7") {
					return newInterfaceRows([][]any{{int32(7), int64(1)}}), nil
				}
				return newInterfaceRows([][]any{{int32(7), int64(1)}, {int32(8), int64(1)}}), nil
			default:
				if strings.Contains(sql, "WHERE sid = 7") {
					return newInterfaceRows(filteredRows), nil
				}
				return newInterfaceRows(unfilteredRows), nil
			}
		},
	}
	expected := &questRewriteTableSnapshot{
		Columns:     columns,
		Counts:      map[int32]int64{7: 1},
		RowCount:    1,
		Fingerprint: questTestFingerprint(columns, filteredRows),
	}
	if err := replaceVerifiedQuestTable(context.Background(), New(db), "source", "tmp", "backup", "sid", "sid = 7", expected); err != nil {
		t.Fatalf("replacement with filtered source = %v", err)
	}
	if len(execSQL) != 3 || execSQL[2] != `DROP TABLE "backup"` {
		t.Fatalf("replacement order = %v", execSQL)
	}
}

func TestReplaceVerifiedQuestTableRestoresBackupWhenFinalValidationFails(t *testing.T) {
	installQuestWaitScript(t, 1)

	execSQL := make([]string, 0, 4)
	db := &visibilityDBStub{
		exec: func(sql string, _ ...interface{}) (pgconn.CommandTag, error) {
			execSQL = append(execSQL, sql)
			return pgconn.CommandTag{}, nil
		},
		query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
			if strings.Contains(sql, "table_columns") {
				metricType := "DOUBLE"
				if len(execSQL) >= 2 {
					metricType = "FLOAT"
				}
				return newInterfaceRows([][]any{{"sid", "INT", false, true}, {"metric", metricType, false, false}}), nil
			}
			return newInterfaceRows(nil), nil
		},
	}
	expected := &questRewriteTableSnapshot{
		Columns: []questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}, {Name: "metric", Type: "DOUBLE"}},
		Counts:  map[int32]int64{},
		Fingerprint: questTestFingerprint([]questTableColumn{
			{Name: "sid", Type: "INT", UpsertKey: true},
			{Name: "metric", Type: "DOUBLE"},
		}, nil),
	}
	err := replaceVerifiedQuestTable(context.Background(), New(db), "source", "tmp", "backup", "sid", "", expected)
	if err == nil || !strings.Contains(err.Error(), "source restored") {
		t.Fatalf("replacement error = %v, want restored source", err)
	}
	if len(execSQL) != 5 || strings.Contains(strings.Join(execSQL, ";"), `DROP TABLE "backup"`) {
		t.Fatalf("failed replacement deleted backup: %v", execSQL)
	}
	if execSQL[2] != `RENAME TABLE "source" TO "tmp"` || execSQL[3] != `RENAME TABLE "backup" TO "source"` {
		t.Fatalf("restore order = %v", execSQL)
	}
	if execSQL[4] != `DROP TABLE IF EXISTS "tmp"` {
		t.Fatalf("temporary table cleanup = %q", execSQL[4])
	}
}

func TestReplaceVerifiedQuestTableRejectsCorruptionAfterAnyRow(t *testing.T) {
	installQuestWaitScript(t, 1)
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "metric", Type: "DOUBLE"},
		{Name: "note", Type: "STRING"},
	}
	for _, test := range []struct {
		name      string
		rowCount  int
		corruptAt int
	}{
		{name: "second row", rowCount: 4, corruptAt: 1},
		{name: "sixty-fifth row", rowCount: 65, corruptAt: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := make([][]any, test.rowCount)
			counts := make(map[int32]int64)
			for i := range rows {
				sid := int32(i%2 + 1)
				metric := any(float64(i) + 0.25)
				if i%5 == 0 {
					metric = nil
				}
				note := any("row")
				if i%3 == 0 {
					note = nil
				}
				rows[i] = []any{sid, time.UnixMilli(int64(100 + i)).UTC(), metric, note}
				counts[sid]++
			}
			if test.rowCount > 2 {
				rows[2] = append([]any(nil), rows[1]...)
				counts = make(map[int32]int64)
				for _, row := range rows {
					counts[row[0].(int32)]++
				}
			}
			actualRows := make([][]any, len(rows))
			for i, row := range rows {
				actualRows[i] = append([]any(nil), row...)
			}
			actualRows[test.corruptAt][2] = float64(999999)
			var valuesCalls int
			var execSQL []string
			var db *visibilityDBStub
			db = &visibilityDBStub{
				exec: func(sql string, _ ...interface{}) (pgconn.CommandTag, error) {
					execSQL = append(execSQL, sql)
					return pgconn.CommandTag{}, nil
				},
				query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
					switch {
					case strings.Contains(sql, "table_columns"):
						return newInterfaceRows([][]any{{"sid", "INT", false, true}, {"ts", "TIMESTAMP", true, true}, {"metric", "DOUBLE", false, false}, {"note", "STRING", false, false}}), nil
					case strings.Contains(sql, "count(*)"):
						countRows := make([][]any, 0, len(counts))
						for sid, count := range counts {
							countRows = append(countRows, []any{sid, count})
						}
						return newInterfaceRows(countRows), nil
					default:
						streamRows := rows
						if len(execSQL) >= 2 {
							streamRows = actualRows
						}
						stream := newInterfaceRows(streamRows)
						stream.valuesCalls = &valuesCalls
						return stream, nil
					}
				},
			}
			expected := &questRewriteTableSnapshot{
				Columns:     columns,
				Counts:      counts,
				RowCount:    int64(len(rows)),
				Fingerprint: questTestFingerprint(columns, rows),
			}

			err := replaceVerifiedQuestTable(context.Background(), New(db), "source", "tmp", "backup", "sid", "", expected)
			if err == nil || !strings.Contains(err.Error(), "source restored") {
				t.Fatalf("corrupt row replacement error = %v, want source restoration", err)
			}
			if valuesCalls != len(rows)*2 {
				t.Fatalf("source and activated fingerprints consumed %d rows, want %d", valuesCalls, len(rows)*2)
			}
			if len(execSQL) != 5 || strings.Contains(strings.Join(execSQL, ";"), `DROP TABLE "backup"`) {
				t.Fatalf("corrupt row was allowed to drop the backup: %v", execSQL)
			}
			if execSQL[2] != `RENAME TABLE "source" TO "tmp"` || execSQL[3] != `RENAME TABLE "backup" TO "source"` {
				t.Fatalf("corrupt row did not preserve recovery order: %v", execSQL)
			}
			if execSQL[4] != `DROP TABLE IF EXISTS "tmp"` {
				t.Fatalf("temporary table cleanup = %q", execSQL[4])
			}
		})
	}
}

func TestCaptureQuestRewriteFingerprintPropagatesRowsError(t *testing.T) {
	wantErr := errors.New("stream read failed")
	db := &visibilityDBStub{
		query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
			if strings.Contains(sql, "table_columns") {
				return newInterfaceRows([][]any{{"sid", "INT", false, false}}), nil
			}
			if strings.Contains(sql, "count(*)") {
				return newInterfaceRows([][]any{{int32(1), int64(1)}}), nil
			}
			return &interfaceRows{rows: [][]any{{int32(1)}}, idx: -1, valuesErrAt: 0, valuesErr: wantErr}, nil
		},
	}
	_, err := captureQuestRewriteTableSnapshot(context.Background(), New(db), "source", "sid", "")
	if !errors.Is(err, wantErr) {
		t.Fatalf("fingerprint rows error = %v, want %v", err, wantErr)
	}

	db.query = func(sql string, _ ...interface{}) (pgx.Rows, error) {
		if strings.Contains(sql, "table_columns") {
			return newInterfaceRows([][]any{{"sid", "INT", false, false}}), nil
		}
		if strings.Contains(sql, "count(*)") {
			return newInterfaceRows([][]any{{int32(1), int64(1)}}), nil
		}
		return &interfaceRows{rows: [][]any{{int32(1)}}, idx: -1, err: wantErr, valuesErrAt: -1}, nil
	}
	_, err = captureQuestRewriteTableSnapshot(context.Background(), New(db), "source", "sid", "")
	if !errors.Is(err, wantErr) {
		t.Fatalf("fingerprint rows.Err = %v, want %v", err, wantErr)
	}
}

type rewriteSnapshotRows struct {
	items [][2]int64
	idx   int
}

type interfaceRows struct {
	rows         [][]any
	idx          int
	err          error
	valuesErrAt  int
	valuesErr    error
	valuesCalls  *int
	descriptions []pgconn.FieldDescription
}

func newInterfaceRows(rows [][]any) *interfaceRows {
	return &interfaceRows{rows: rows, idx: -1, valuesErrAt: -1}
}
func (r *interfaceRows) Close()     {}
func (r *interfaceRows) Err() error { return r.err }
func (r *interfaceRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}
func (r *interfaceRows) FieldDescriptions() []pgconn.FieldDescription { return r.descriptions }
func (r *interfaceRows) RawValues() [][]byte                          { return nil }
func (r *interfaceRows) Conn() *pgx.Conn                              { return nil }
func (r *interfaceRows) Values() ([]any, error) {
	if r.valuesCalls != nil {
		*r.valuesCalls++
	}
	if r.idx == r.valuesErrAt {
		return nil, r.valuesErr
	}
	return r.rows[r.idx], nil
}
func (r *interfaceRows) Next() bool {
	r.idx++
	return r.idx < len(r.rows)
}
func (r *interfaceRows) Scan(dest ...any) error {
	for i, value := range r.rows[r.idx] {
		out := reflect.ValueOf(dest[i]).Elem()
		if value == nil {
			out.SetZero()
		} else {
			out.Set(reflect.ValueOf(value))
		}
	}
	return nil
}

func newRewriteSnapshotRows(counts map[int32]int64) *rewriteSnapshotRows {
	items := make([][2]int64, 0, len(counts))
	for sid, count := range counts {
		items = append(items, [2]int64{int64(sid), count})
	}
	return &rewriteSnapshotRows{items: items, idx: -1}
}

func (r *rewriteSnapshotRows) Close()                                       {}
func (r *rewriteSnapshotRows) Err() error                                   { return nil }
func (r *rewriteSnapshotRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *rewriteSnapshotRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *rewriteSnapshotRows) RawValues() [][]byte                          { return nil }
func (r *rewriteSnapshotRows) Conn() *pgx.Conn                              { return nil }
func (r *rewriteSnapshotRows) Values() ([]any, error) {
	item := r.items[r.idx]
	return []any{int32(item[0]), item[1]}, nil
}
func (r *rewriteSnapshotRows) Next() bool {
	r.idx++
	return r.idx < len(r.items)
}
func (r *rewriteSnapshotRows) Scan(dest ...any) error {
	item := r.items[r.idx]
	*dest[0].(*int32) = int32(item[0])
	*dest[1].(*int64) = item[1]
	return nil
}
