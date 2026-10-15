package data

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

type pagedTestSource struct {
	*stubSeriesSource
	page    func(context.Context, int64, int64, int) ([]*orm.DataRecord, error)
	cursors []int64
}

func (s *pagedTestSource) FetchHistoryPage(ctx context.Context, _ *orm.Subscription, start, end int64, limit int) ([]*orm.DataRecord, error) {
	s.cursors = append(s.cursors, start)
	return s.page(ctx, start, end, limit)
}

func (s *pagedTestSource) WarmupStart(_ context.Context, _ *orm.Subscription, anchor int64) (int64, error) {
	return anchor, nil
}

type pageCaptureRepo struct {
	*stubSeriesRepo
	pages [][]*orm.DataRecord
}

func (r *pageCaptureRepo) InsertSeriesBatch(ctx context.Context, info *orm.SeriesInfo, rows []*orm.DataRecord) *errs.Error {
	r.pages = append(r.pages, rows)
	return r.stubSeriesRepo.InsertSeriesBatch(ctx, info, rows)
}

func TestHistoryPagesStreamBootstrapTypedValues(t *testing.T) {
	for _, tf := range []string{"event", "1d"} {
		t.Run(tf, func(t *testing.T) {
			base := newStubRegistrySource("paged_history")
			base.info.TimeFrame = tf
			values := map[string]any{"integer": int64(9007199254740993), "nullable": nil, "nested": map[string]any{"flag": true, "bytes": []byte{1, 2}}}
			rows := []*orm.DataRecord{{TimeMS: 10, EndMS: 11, Values: values}, {TimeMS: 20, EndMS: 21, Values: map[string]any{"integer": int64(2)}}, {TimeMS: 30, EndMS: 31, Values: map[string]any{"integer": int64(3)}}}
			source := &pagedTestSource{stubSeriesSource: base}
			source.page = func(_ context.Context, start, end int64, limit int) ([]*orm.DataRecord, error) {
				for _, row := range rows {
					if row.TimeMS >= start && row.TimeMS < end {
						return []*orm.DataRecord{row}, nil
					}
				}
				return nil, nil
			}
			catalog := NewDataSourceCatalog()
			if err := catalog.RegisterDataSource(source); err != nil {
				t.Fatal(err)
			}
			repo := &pageCaptureRepo{stubSeriesRepo: &stubSeriesRepo{}}
			sub := Subscription{Source: base.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}, TimeFrame: tf}
			plan, err := catalog.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{{Subscription: sub, Consumer: "test", Required: true}}, SubscriptionPlanOptions{AnchorMS: 10, EndMS: 40, PageRows: 1, PageBytes: 10000})
			if err != nil {
				t.Fatal(err)
			}
			if err := plan.Bootstrap(context.Background(), repo); err != nil {
				t.Fatal(err)
			}
			if source.fetchCount != 0 || repo.insertCalls != 3 || !reflect.DeepEqual(source.cursors, []int64{10, 11, 21, 31}) {
				t.Fatalf("whole-range fetch or wrong pages: fetch=%d writes=%d cursors=%v", source.fetchCount, repo.insertCalls, source.cursors)
			}
			if !reflect.DeepEqual(repo.pages[0][0].Values, values) || repo.pages[0][0].Sid != 7 {
				t.Fatal("typed/null values or SID normalization lost")
			}
			if _, ok := repo.pages[1][0].Values["nullable"]; ok {
				t.Fatal("missing field became NULL")
			}
		})
	}
}

func TestHistoryPagesRejectWholeInvalidPageBeforeConsumer(t *testing.T) {
	base := newStubRegistrySource("invalid_history")
	sub := &Subscription{Source: base.info.Name, TimeFrame: "1d", ExSymbol: &orm.ExSymbol{ID: 7}}
	valid := &orm.DataRecord{Sid: 7, TimeMS: 10, EndMS: 11, Values: map[string]any{"value": "small"}}
	for name, rows := range map[string][]*orm.DataRecord{
		"wide":     {{Sid: 7, TimeMS: 10, EndMS: 11, Values: map[string]any{"value": strings.Repeat("x", 10000)}}},
		"too many": {valid, valid, valid}, "duplicate": {valid, valid},
		"out of range": {{TimeMS: 40}}, "nil": {nil}, "foreign": {{Sid: 8, TimeMS: 10}},
	} {
		t.Run(name, func(t *testing.T) {
			source := &pagedTestSource{stubSeriesSource: base, page: func(context.Context, int64, int64, int) ([]*orm.DataRecord, error) { return rows, nil }}
			consumed := false
			err := ReadSourceHistory(orm.WithSeriesReadByteLimit(context.Background(), 1000), source, sub, 10, 40, 2, func([]*orm.DataRecord) error { consumed = true; return nil })
			if err == nil || consumed || len(source.cursors) != 1 {
				t.Fatalf("invalid page consumed or retried: err=%v consumed=%v", err, consumed)
			}
		})
	}
}

func TestHistoryPagesCancellationAndMinimumTimestamp(t *testing.T) {
	base := newStubRegistrySource("cancel_history")
	sub := &Subscription{Source: base.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}}
	source := &pagedTestSource{stubSeriesSource: base, page: func(_ context.Context, start, end int64, limit int) ([]*orm.DataRecord, error) {
		return []*orm.DataRecord{{TimeMS: start}}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	consumed := 0
	err := ReadSourceHistory(ctx, source, sub, math.MinInt64, 10, 1, func([]*orm.DataRecord) error { consumed++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || consumed != 1 || len(source.cursors) != 1 {
		t.Fatalf("cancellation or minimum timestamp: %v consumed=%d", err, consumed)
	}
	ctx, cancel = context.WithCancel(context.Background())
	source.page = func(context.Context, int64, int64, int) ([]*orm.DataRecord, error) {
		cancel()
		return []*orm.DataRecord{{TimeMS: 1}}, nil
	}
	err = ReadSourceHistory(ctx, source, sub, 1, 10, 1, func([]*orm.DataRecord) error { t.Fatal("consumed canceled fetch"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestHistoryByteBudgetLegacyRejectsOnlyUncachedFetch(t *testing.T) {
	base := newStubRegistrySource("legacy_history")
	base.rows = []*orm.DataRecord{{TimeMS: 10, EndMS: 11, Values: map[string]any{"value": int64(1)}}}
	sub := &Subscription{Source: base.info.Name, TimeFrame: "1d", ExSymbol: &orm.ExSymbol{ID: 7}}
	repo := &stubSeriesRepo{}
	ctx := orm.WithSeriesReadByteLimit(context.Background(), 1000)
	if err := ensureSeriesRangeWithRepo(nil, ctx, repo, base, sub, 10, 40); err == nil || base.fetchCount != 0 || repo.insertCalls != 0 {
		t.Fatalf("unpaged budget silently fetched: %v", err)
	}
	repo.missing = []orm.MSRange{}
	if err := ensureSeriesRangeWithRepo(nil, ctx, repo, base, sub, 10, 40); err != nil || base.fetchCount != 0 {
		t.Fatalf("cached legacy source rejected: %v", err)
	}
	repo.missing = nil
	if err := ensureSeriesRangeWithRepo(nil, context.Background(), repo, base, sub, 10, 40); err != nil || base.fetchCount != 1 || repo.insertCalls != 1 {
		t.Fatalf("disabled budget changed legacy fetch: %v", err)
	}
}

func TestHistSeriesFeederByteBudgetReturnsReadError(t *testing.T) {
	info := orm.NewSeriesInfo("budget_feeder", "event", []orm.SeriesField{{Name: "value", Type: "string"}})
	repo := &stubSeriesRepo{queryRows: []*orm.DataRecord{{Sid: 7, TimeMS: 10, EndMS: 11, Values: map[string]any{"value": strings.Repeat("x", 10000)}}}}
	called := false
	feeder, err := NewHistSeriesFeeder(repo, info, &Subscription{ExSymbol: &orm.ExSymbol{ID: 7}}, func(*orm.DataSeries) { called = true }, 1)
	if err != nil {
		t.Fatal(err)
	}
	feeder.BatchBytes = 1000
	feeder.SetEndMS(40)
	feeder.SetSeek(10)
	if feeder.getNextMS() == math.MaxInt64 || feeder.RunBatch(feeder.GetBatch()) == nil || called || len(feeder.rows) != 0 {
		t.Fatal("oversized page became silent EOF or callback")
	}
	feeder.BatchBytes = 0
	feeder.SetSeek(10)
	if err := feeder.RunBatch(feeder.GetBatch()); err != nil || !called {
		t.Fatal("reset/disabled budget did not restore legacy behavior", err)
	}
}

func TestSubscriptionBudgetReportUsesCompiledUnion(t *testing.T) {
	plan := &SubscriptionPlan{options: SubscriptionPlanOptions{PageRows: 3, PrefetchRows: 8, PageBytes: 1000}, streams: []PlannedStream{{Subscription: Subscription{WarmupNum: 2}}, {Subscription: Subscription{WarmupNum: 4}}}}
	report := plan.BudgetReport()
	if report.StreamCount != 2 || report.ActivePageRowsEstimate != 6 || report.DeclaredWarmupRows != 6 || report.ActivePageBytesEstimate != 2000 || len(report.Exclusions) == 0 {
		t.Fatalf("incorrect estimate: %+v", report)
	}
}
