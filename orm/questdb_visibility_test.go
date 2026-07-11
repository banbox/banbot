package orm

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

func TestAddSymbolsQuestVisibilityTimeoutKeepsCachedIdentity(t *testing.T) {
	oldQuest := IsQuestDB
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	oldMaxSID := maxSid
	oldKey := keySymbolMap
	oldID := idSymbolMap
	oldMarket := marketMap
	IsQuestDB = true
	questReadAfterWriteTimeout = 5 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	maxSid = 41
	keySymbolMap = map[string]*ExSymbol{}
	idSymbolMap = map[int32]*ExSymbol{}
	marketMap = map[string]int{}
	defer func() {
		IsQuestDB = oldQuest
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
		maxSid = oldMaxSID
		keySymbolMap = oldKey
		idSymbolMap = oldID
		marketMap = oldMarket
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
					return errors.New("not visible yet")
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
	if err != nil {
		t.Fatalf("AddSymbols returned error: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected one inserted row, got %d", n)
	}
	if len(insertArgs) != 10 {
		t.Fatalf("expected full metadata insert args, got %d", len(insertArgs))
	}

	got := GetExSymbol2("binance", "spot", "BTC/USDT")
	if got == nil {
		t.Fatal("expected cached symbol after insert timeout")
	}
	if got.ID != 42 || !got.Combined || got.ListMs != 123 || got.DelistMs != 456 {
		t.Fatalf("cached symbol lost canonical metadata: %+v", got)
	}
}

func TestWaitForQuestKlineCoverageVisibleBypassesCache(t *testing.T) {
	oldGrace := klineInsertQuestVisibilityGrace
	oldPoll := questReadAfterWritePollInterval
	klineInsertQuestVisibilityGrace = 20 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		klineInsertQuestVisibilityGrace = oldGrace
		questReadAfterWritePollInterval = oldPoll
	}()

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

func TestWaitForQuestExsymbolVisiblePollsUntilRowVisible(t *testing.T) {
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 20 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

	calls := 0
	db := &visibilityDBStub{
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			calls++
			if calls < 3 {
				return visibilityRowStub{scan: func(dest ...interface{}) error {
					return errors.New("not visible yet")
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

func TestWaitForQuestKlineWindowVisiblePolls(t *testing.T) {
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 20 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

	calls := 0
	db := &visibilityDBStub{
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			if !strings.Contains(sql, "count(*) > 0") {
				t.Fatalf("unexpected sql: %s", sql)
			}
			calls++
			visible := calls >= 3
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				*dest[0].(*bool) = visible
				return nil
			}}
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

func TestWaitForQuestKlineTimestampVisiblePolls(t *testing.T) {
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 20 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

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
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 5 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

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
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 20 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

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
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 20 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

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
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 5 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

	db := &visibilityDBStub{
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				var minTime, maxTime *int64
				*dest[0].(**int64) = minTime
				*dest[1].(**int64) = maxTime
				return nil
			}}
		},
	}

	start, end, err := waitForQuestKlineRangeVisible(context.Background(), New(db), 1, "1m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if start != 0 || end != 0 {
		t.Fatalf("expected invisible range, got start=%d end=%d", start, end)
	}
}

func TestVerifyQuestRewriteSnapshotPollsUntilExpected(t *testing.T) {
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 20 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

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
	oldTimeout := questReadAfterWriteTimeout
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWriteTimeout = 5 * time.Millisecond
	questReadAfterWritePollInterval = time.Millisecond
	defer func() {
		questReadAfterWriteTimeout = oldTimeout
		questReadAfterWritePollInterval = oldPoll
	}()

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

type rewriteSnapshotRows struct {
	items [][2]int64
	idx   int
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
