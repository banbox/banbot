package orm

import (
	"context"
	"strings"
	"testing"

	"github.com/banbox/banexg/errs"
)

type stubStoreRepo struct {
	ensureCalls   int
	insertCalls   int
	queryCalls    int
	deleteCalls   int
	coverageCalls int
	inserted      []*DataRecord
	queryRows     []*DataRecord
}

func (s *stubStoreRepo) EnsureSeriesTable(ctx context.Context, info *SeriesInfo) *errs.Error {
	s.ensureCalls++
	return nil
}

func (s *stubStoreRepo) InsertSeriesBatch(ctx context.Context, info *SeriesInfo, rows []*DataRecord) *errs.Error {
	s.insertCalls++
	s.inserted = append([]*DataRecord(nil), rows...)
	return nil
}

func (s *stubStoreRepo) QuerySeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*DataRecord, *errs.Error) {
	s.queryCalls++
	return append([]*DataRecord(nil), s.queryRows...), nil
}

func (s *stubStoreRepo) DeleteSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	s.deleteCalls++
	return nil
}

func (s *stubStoreRepo) UpdateSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	s.coverageCalls++
	return nil
}

func (s *stubStoreRepo) UpdateSeriesCoverage(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, rows []*DataRecord) *errs.Error {
	s.coverageCalls++
	return nil
}

func (s *stubStoreRepo) GetSeriesRange(ctx context.Context, info *SeriesInfo, sid int32) (int64, int64, *errs.Error) {
	return 0, 0, nil
}

func testSeriesInfo(name string) *SeriesInfo {
	return NewSeriesInfo(name, "1m", []SeriesField{{Name: "value", Type: "float"}})
}

func TestNewSeriesInfoUsesCanonicalTimeframeTable(t *testing.T) {
	info := NewSeriesInfo("funding_rate", "5m", []SeriesField{{Name: "rate", Type: "float"}})
	if info.Binding.Table != "funding_rate_5m" {
		t.Fatalf("expected canonical timeframe table name, got %q", info.Binding.Table)
	}
	if info.Binding.TimeColumn != "ts" || info.Binding.EndColumn != "end_ms" || info.Binding.SIDColumn != "sid" {
		t.Fatalf("unexpected default binding: %+v", info.Binding)
	}
}

func TestSeriesStoreWriteBatchNormalizesAndUpdatesCoverage(t *testing.T) {
	repo := &stubStoreRepo{}
	store := NewSeriesStore(repo)
	info := testSeriesInfo("funding_rate")
	target := &ExSymbol{ID: 9, Exchange: "custom", Market: "funding", Symbol: "BTCUSDT"}
	rows := []*DataRecord{
		{TimeMS: 60_000, EndMS: 120_000, Values: map[string]any{"value": 0.2}},
		{TimeMS: 0, EndMS: 60_000, Values: map[string]any{"value": 0.1}},
	}

	if err := store.WriteBatch(context.Background(), info, target, rows); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if repo.insertCalls != 1 || repo.coverageCalls != 1 {
		t.Fatalf("expected one insert and coverage update, got insert=%d coverage=%d", repo.insertCalls, repo.coverageCalls)
	}
	if len(repo.inserted) != 2 {
		t.Fatalf("expected 2 inserted rows, got %+v", repo.inserted)
	}
	if repo.inserted[0].Sid != target.ID || repo.inserted[1].Sid != target.ID {
		t.Fatalf("expected target sid to be filled, got %+v", repo.inserted)
	}
	if repo.inserted[0].TimeMS != 0 || repo.inserted[1].TimeMS != 60_000 {
		t.Fatalf("expected rows sorted by time, got %+v", repo.inserted)
	}
}

func TestSeriesStoreReadConvertsRecordsToEvents(t *testing.T) {
	repo := &stubStoreRepo{queryRows: []*DataRecord{{
		Sid:    7,
		TimeMS: 10,
		EndMS:  20,
		Closed: true,
		Values: map[string]any{"value": 1.2},
	}}}
	store := NewSeriesStore(repo)
	info := testSeriesInfo("position")
	target := &ExSymbol{ID: 7, Exchange: "custom", Market: "portfolio", Symbol: "net_position"}

	got, err := store.Read(context.Background(), info, target, 0, 100, 10)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %+v", got)
	}
	if got[0].Source != info.Name || got[0].Sid != target.ID || got[0].ExSymbol != target {
		t.Fatalf("unexpected event identity: %+v", got[0])
	}
}

func TestSeriesStoreDeleteUsesRepositoryContract(t *testing.T) {
	repo := &stubStoreRepo{}
	store := NewSeriesStore(repo)
	info := testSeriesInfo("custom_metric")
	target := &ExSymbol{ID: 3, Exchange: "custom", Market: "metric", Symbol: "latency"}

	if err := store.Delete(context.Background(), info, target, 10, 20); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if repo.deleteCalls != 1 {
		t.Fatalf("expected repository delete call, got %d", repo.deleteCalls)
	}
}

func TestSeriesStoreUpdateCoverageUsesRepositoryContract(t *testing.T) {
	repo := &stubStoreRepo{}
	store := NewSeriesStore(repo)
	info := testSeriesInfo("custom_metric")
	target := &ExSymbol{ID: 3, Exchange: "custom", Market: "metric", Symbol: "latency"}

	if err := store.UpdateCoverage(context.Background(), info, target, 10, 20, nil); err != nil {
		t.Fatalf("UpdateCoverage failed: %v", err)
	}
	if repo.coverageCalls != 1 {
		t.Fatalf("expected repository coverage call, got %d", repo.coverageCalls)
	}
}

func TestNormalizeDataRecordsRejectsSidMismatch(t *testing.T) {
	_, err := NormalizeDataRecords(1, []*DataRecord{{Sid: 2, TimeMS: 0, EndMS: 1}})
	if err == nil || !strings.Contains(err.Short(), "does not match") {
		t.Fatalf("expected sid mismatch error, got %v", err)
	}
}

func TestQuestSeriesRewritePredicateKeepsOtherSidsAndCoveredRows(t *testing.T) {
	binding := SeriesBinding{Table: "funding_rate_1h", TimeColumn: "ts", EndColumn: "end_ms", SIDColumn: "sid"}
	got := questSeriesRewritePredicate(binding, 7, []MSRange{
		{Start: 1_000, Stop: 2_000},
		{Start: 3_000, Stop: 4_000},
	})
	for _, want := range []string{
		`"sid" <> 7`,
		`"sid" = 7`,
		`cast("ts" as long) >= 1000000`,
		`cast("ts" as long) < 2000000`,
		`cast("ts" as long) >= 3000000`,
		`cast("ts" as long) < 4000000`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("predicate %q missing %q", got, want)
		}
	}
	if got := questSeriesRewritePredicate(binding, 7, nil); got != `"sid" <> 7` {
		t.Fatalf("expected empty coverage to keep only other sids, got %q", got)
	}
}
