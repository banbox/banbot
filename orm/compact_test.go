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

type compactDBStub struct {
	counts   []int64
	queryErr error
	queryIdx int
}

func (s *compactDBStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("unexpected Exec call")
}

func (s *compactDBStub) QueryRow(context.Context, string, ...any) pgx.Row {
	if s.queryErr != nil {
		return compactRowStub{err: s.queryErr}
	}
	if len(s.counts) == 0 {
		return compactRowStub{count: 0}
	}
	idx := s.queryIdx
	if idx >= len(s.counts) {
		idx = len(s.counts) - 1
	}
	s.queryIdx++
	return compactRowStub{count: s.counts[idx]}
}

type compactRowStub struct {
	count int64
	err   error
}

func (r compactRowStub) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return errors.New("expected one scan destination")
	}
	ptr, ok := dest[0].(*int64)
	if !ok {
		return errors.New("expected *int64 scan destination")
	}
	*ptr = r.count
	return nil
}

func TestWaitCompactVisibleCountWaitsForWalVisibility(t *testing.T) {
	oldTimeout := compactVerifyTimeout
	oldPoll := compactVerifyPollInterval
	compactVerifyTimeout = 20 * time.Millisecond
	compactVerifyPollInterval = time.Millisecond
	defer func() {
		compactVerifyTimeout = oldTimeout
		compactVerifyPollInterval = oldPoll
	}()

	db := &compactDBStub{counts: []int64{0, 0, 5}}
	got, err := waitCompactVisibleCount(context.Background(), db, "ins_kline_q_new", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 5 {
		t.Fatalf("count mismatch: got %d want 5", got)
	}
	if db.queryIdx < 3 {
		t.Fatalf("expected polling, query count=%d", db.queryIdx)
	}
}

func TestWaitCompactVisibleCountAllowsLegitimateEmptySnapshot(t *testing.T) {
	db := &compactDBStub{counts: []int64{0}}
	got, err := waitCompactVisibleCount(context.Background(), db, "sranges_q_new", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("count mismatch: got %d want 0", got)
	}
}

func TestWaitCompactVisibleCountFailsOnPermanentMismatch(t *testing.T) {
	oldTimeout := compactVerifyTimeout
	oldPoll := compactVerifyPollInterval
	compactVerifyTimeout = 5 * time.Millisecond
	compactVerifyPollInterval = time.Millisecond
	defer func() {
		compactVerifyTimeout = oldTimeout
		compactVerifyPollInterval = oldPoll
	}()

	db := &compactDBStub{counts: []int64{1, 1, 1}}
	_, err := waitCompactVisibleCount(context.Background(), db, "sranges_q_new", 2)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestCompactTempTableNameIsUniqueAndNotLegacyNewName(t *testing.T) {
	const table = "sranges_q"
	first := compactTempTableName(table)
	second := compactTempTableName(table)

	if first == table+"_new" || second == table+"_new" {
		t.Fatalf("compact temp table must not use legacy fixed _new name: %q %q", first, second)
	}
	if first == second {
		t.Fatalf("compact temp table names must be unique: %q", first)
	}
	if !strings.HasPrefix(first, table+"_compact_") || !strings.HasPrefix(second, table+"_compact_") {
		t.Fatalf("compact temp table name prefix mismatch: %q %q", first, second)
	}
}
