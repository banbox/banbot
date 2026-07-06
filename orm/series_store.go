package orm

import (
	"context"
	"sort"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

type SeriesStore struct {
	repo SeriesRepo
}

func NewSeriesStore(repo SeriesRepo) *SeriesStore {
	if repo == nil {
		repo = DefaultSeriesRepo()
	}
	return &SeriesStore{repo: repo}
}

func DefaultSeriesStore() *SeriesStore {
	return NewSeriesStore(nil)
}

func (s *SeriesStore) Ensure(ctx context.Context, info *SeriesInfo) *errs.Error {
	return s.repoOrDefault().EnsureSeriesTable(ctx, info)
}

func (s *SeriesStore) Write(ctx context.Context, info *SeriesInfo, target *ExSymbol, row *DataRecord) *errs.Error {
	if row == nil {
		return errs.NewMsg(core.ErrBadConfig, "series row is nil")
	}
	return s.WriteBatch(ctx, info, target, []*DataRecord{row})
}

func (s *SeriesStore) WriteBatch(ctx context.Context, info *SeriesInfo, target *ExSymbol, rows []*DataRecord) *errs.Error {
	if len(rows) == 0 {
		return nil
	}
	if target == nil || target.ID <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "series target exsymbol is required")
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	if IsQuestDB {
		binding := normalizedSeriesBinding(info.Binding)
		tblLock := cptState.getTableLock(binding.Table)
		tblLock.RLock()
		defer tblLock.RUnlock()
		ctx = withSeriesTableReadLockSkipped(ctx)
	}
	items, err := NormalizeDataRecords(target.ID, rows)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	repo := s.repoOrDefault()
	if err := repo.InsertSeriesBatch(ctx, info, items); err != nil {
		return err
	}
	startMS := items[0].TimeMS
	endMS := items[len(items)-1].EndMS
	return repo.UpdateSeriesCoverage(ctx, info, target.ID, startMS, endMS, items)
}

func (s *SeriesStore) Read(ctx context.Context, info *SeriesInfo, target *ExSymbol, startMS, endMS int64, limit int) ([]*DataSeries, *errs.Error) {
	if target == nil || target.ID <= 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "series target exsymbol is required")
	}
	if err := validateSeriesInfo(info); err != nil {
		return nil, err
	}
	rows, err := s.repoOrDefault().QuerySeriesRange(ctx, info, target.ID, startMS, endMS, limit)
	if err != nil {
		return nil, err
	}
	return RecordsToSeries(info, target, rows), nil
}

func (s *SeriesStore) Delete(ctx context.Context, info *SeriesInfo, target *ExSymbol, startMS, endMS int64) *errs.Error {
	if target == nil || target.ID <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "series target exsymbol is required")
	}
	return s.repoOrDefault().DeleteSeriesRange(ctx, info, target.ID, startMS, endMS)
}

func (s *SeriesStore) UpdateCoverage(ctx context.Context, info *SeriesInfo, target *ExSymbol, startMS, endMS int64, rows []*DataRecord) *errs.Error {
	if target == nil || target.ID <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "series target exsymbol is required")
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	if IsQuestDB {
		binding := normalizedSeriesBinding(info.Binding)
		tblLock := cptState.getTableLock(binding.Table)
		tblLock.RLock()
		defer tblLock.RUnlock()
		ctx = withSeriesTableReadLockSkipped(ctx)
	}
	return s.repoOrDefault().UpdateSeriesCoverage(ctx, info, target.ID, startMS, endMS, rows)
}

func (s *SeriesStore) Coverage(ctx context.Context, info *SeriesInfo, target *ExSymbol) (int64, int64, *errs.Error) {
	if target == nil || target.ID <= 0 {
		return 0, 0, errs.NewMsg(core.ErrBadConfig, "series target exsymbol is required")
	}
	return s.repoOrDefault().GetSeriesRange(ctx, info, target.ID)
}

func (s *SeriesStore) repoOrDefault() SeriesRepo {
	if s != nil && s.repo != nil {
		return s.repo
	}
	return DefaultSeriesRepo()
}

func NormalizeDataRecords(sid int32, rows []*DataRecord) ([]*DataRecord, *errs.Error) {
	if sid <= 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "series target sid is required")
	}
	items := make([]*DataRecord, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		cp := *row
		if cp.Sid == 0 {
			cp.Sid = sid
		}
		if cp.Sid != sid {
			return nil, errs.NewMsg(core.ErrBadConfig, "series row sid %d does not match target sid %d", cp.Sid, sid)
		}
		if cp.EndMS <= cp.TimeMS {
			return nil, errs.NewMsg(core.ErrBadConfig, "series row end_ms must be greater than time_ms")
		}
		items = append(items, &cp)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].TimeMS == items[j].TimeMS {
			return items[i].EndMS < items[j].EndMS
		}
		return items[i].TimeMS < items[j].TimeMS
	})
	return items, nil
}

func RecordToSeries(info *SeriesInfo, target *ExSymbol, row *DataRecord) *DataSeries {
	if info == nil || target == nil || row == nil {
		return nil
	}
	return &DataSeries{
		Source:    NormalizeSeriesSource(info.Name),
		Sid:       row.Sid,
		TimeMS:    row.TimeMS,
		EndMS:     row.EndMS,
		TimeFrame: info.TimeFrame,
		Closed:    row.Closed,
		Values:    row.Values,
		ExSymbol:  target,
	}
}

func RecordsToSeries(info *SeriesInfo, target *ExSymbol, rows []*DataRecord) []*DataSeries {
	if len(rows) == 0 {
		return nil
	}
	items := make([]*DataSeries, 0, len(rows))
	for _, row := range rows {
		evt := RecordToSeries(info, target, row)
		if evt != nil {
			items = append(items, evt)
		}
	}
	return items
}

func SeriesToRecord(evt *DataSeries) *DataRecord {
	if evt == nil {
		return nil
	}
	return &DataRecord{
		Sid:    evt.Sid,
		TimeMS: evt.TimeMS,
		EndMS:  evt.EndMS,
		Closed: evt.Closed,
		Values: evt.Values,
	}
}
