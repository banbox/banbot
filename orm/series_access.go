package orm

import (
	"context"

	"github.com/banbox/banexg/errs"
)

// BoundSeriesStore reuses a series definition and target across operations.
// It retains the supplied pointers; configure them before use and do not mutate
// them concurrently. Values maps still follow SeriesStore's ownership rules.
type BoundSeriesStore struct {
	store  *SeriesStore
	info   *SeriesInfo
	target *ExSymbol
}

// Bind selects a series and target without performing database I/O. Validation
// remains in each underlying operation. For columns attached to existing K-line
// rows, use KLineSeriesStore instead: those writes update existing rows only.
func (s *SeriesStore) Bind(info *SeriesInfo, target *ExSymbol) *BoundSeriesStore {
	return &BoundSeriesStore{store: s, info: info, target: target}
}

func (s *BoundSeriesStore) Ensure(ctx context.Context) *errs.Error {
	return s.store.Ensure(ctx, s.info)
}

func (s *BoundSeriesStore) Write(ctx context.Context, row *DataRecord) *errs.Error {
	return s.store.Write(ctx, s.info, s.target, row)
}

func (s *BoundSeriesStore) WriteBatch(ctx context.Context, rows []*DataRecord) *errs.Error {
	return s.store.WriteBatch(ctx, s.info, s.target, rows)
}

func (s *BoundSeriesStore) WriteSeries(ctx context.Context, row *DataSeries) *errs.Error {
	return s.store.WriteSeries(ctx, s.info, s.target, row)
}

func (s *BoundSeriesStore) WriteSeriesBatch(ctx context.Context, rows []*DataSeries) *errs.Error {
	return s.store.WriteSeriesBatch(ctx, s.info, s.target, rows)
}

func (s *BoundSeriesStore) Read(ctx context.Context, startMS, endMS int64, limit int) ([]*DataSeries, *errs.Error) {
	return s.store.Read(ctx, s.info, s.target, startMS, endMS, limit)
}

func (s *BoundSeriesStore) Missing(ctx context.Context, startMS, endMS int64) ([]MSRange, *errs.Error) {
	return s.store.Missing(ctx, s.info, s.target, startMS, endMS)
}

func (s *BoundSeriesStore) FillMissing(ctx context.Context, startMS, endMS int64, fetch SeriesFetchFunc) *errs.Error {
	return s.store.FillMissing(ctx, s.info, s.target, startMS, endMS, fetch)
}

func (s *BoundSeriesStore) Coverage(ctx context.Context) (int64, int64, *errs.Error) {
	return s.store.Coverage(ctx, s.info, s.target)
}

func (s *BoundSeriesStore) UpdateCoverage(ctx context.Context, startMS, endMS int64, rows []*DataRecord) *errs.Error {
	return s.store.UpdateCoverage(ctx, s.info, s.target, startMS, endMS, rows)
}

func (s *BoundSeriesStore) Delete(ctx context.Context, startMS, endMS int64) *errs.Error {
	return s.store.Delete(ctx, s.info, s.target, startMS, endMS)
}
