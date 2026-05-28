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
	queryRow func(sql string, args ...interface{}) pgx.Row
}

func (s *visibilityDBStub) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	panic("unexpected Exec call")
}

func (s *visibilityDBStub) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("unexpected Query call")
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

func TestVerifyQuestRewriteCountMismatch(t *testing.T) {
	db := &visibilityDBStub{
		queryRow: func(sql string, args ...interface{}) pgx.Row {
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				*dest[0].(*int64) = 3
				return nil
			}}
		},
	}

	err := verifyQuestRewriteCount(context.Background(), New(db), "tmp_tbl", 4)
	if err == nil {
		t.Fatal("expected mismatch error")
	}
}
