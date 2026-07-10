package data

import (
	"context"
	"math"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type HistSeriesFeeder struct {
	info      *orm.SeriesInfo
	target    *orm.ExSymbol
	callback  FnDataSeries
	store     *orm.SeriesStore
	warmEndMS int64
	endMS     int64
	offsetMS  int64
	rowIdx    int
	nextMS    int64
	rows      []*orm.DataSeries
	loadErr   *errs.Error
}

type seriesLoadErrorBatch struct {
	err *errs.Error
}

func (b *seriesLoadErrorBatch) TimeMS() int64 { return 0 }

func NewHistSeriesFeeder(repo orm.SeriesRepo, info *orm.SeriesInfo, sub *strat.DataSub, callback FnDataSeries, warmEndMS int64) (*HistSeriesFeeder, error) {
	if info == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "series info is required")
	}
	if sub == nil || sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "data sub exsymbol is required")
	}
	projected, err := projectSeriesInfo(info, sub.Fields)
	if err != nil {
		return nil, err
	}
	return &HistSeriesFeeder{
		info: projected, target: sub.ExSymbol, callback: callback,
		store: orm.NewSeriesStore(repo), warmEndMS: warmEndMS,
	}, nil
}

func projectSeriesInfo(info *orm.SeriesInfo, fields []string) (*orm.SeriesInfo, error) {
	cp := *info
	cp.Binding = info.Binding
	if len(fields) == 0 {
		cp.Binding.Fields = append([]orm.SeriesField(nil), info.Binding.Fields...)
		return &cp, nil
	}
	available := make(map[string]orm.SeriesField, len(info.Binding.Fields))
	for _, field := range info.Binding.Fields {
		available[field.Name] = field
	}
	cp.Binding.Fields = make([]orm.SeriesField, 0, len(fields))
	for _, name := range orm.NormalizeSeriesFields(info.Name, fields) {
		field, ok := available[name]
		if !ok {
			return nil, errs.NewMsg(core.ErrBadConfig, "unsupported field %q", name)
		}
		cp.Binding.Fields = append(cp.Binding.Fields, field)
	}
	return &cp, nil
}

func (f *HistSeriesFeeder) getNextMS() int64 { return f.nextMS }

func (f *HistSeriesFeeder) getSymbol() string {
	return strat.DataSubKey(f.info.Name, f.target.ID, f.info.TimeFrame)
}

func (f *HistSeriesFeeder) SetEndMS(ms int64) { f.endMS = ms }

func (f *HistSeriesFeeder) SetSeek(since int64) {
	f.offsetMS = since
	f.rowIdx = -1
	f.nextMS = 0
	f.rows = nil
	f.loadErr = nil
	f.CallNext()
}

func (f *HistSeriesFeeder) GetBatch() Batch {
	if f.loadErr != nil {
		return &seriesLoadErrorBatch{err: f.loadErr}
	}
	if f.rowIdx < 0 || f.rowIdx >= len(f.rows) {
		return nil
	}
	return SeriesBatch{DataSeries: f.rows[f.rowIdx]}
}

func (f *HistSeriesFeeder) RunBatch(batch Batch) *errs.Error {
	if failed, ok := batch.(*seriesLoadErrorBatch); ok {
		return failed.err
	}
	item, ok := batch.(SeriesBatch)
	if !ok || item.DataSeries == nil {
		return nil
	}
	evt := item.DataSeries
	evt.IsWarmUp = evt.EndMS <= f.warmEndMS
	btime.CurTimeMS = evt.EndMS
	if f.callback != nil {
		f.callback(evt)
	}
	return nil
}

func (f *HistSeriesFeeder) CallNext() {
	if f.loadErr != nil {
		return
	}
	if f.rowIdx+1 < len(f.rows) {
		f.rowIdx++
		f.nextMS = f.rows[f.rowIdx].EndMS
		return
	}
	if f.endMS > 0 && f.offsetMS >= f.endMS {
		f.rowIdx = -1
		f.nextMS = math.MaxInt64
		return
	}
	endMS := f.endMS
	if endMS <= 0 {
		endMS = math.MaxInt64
	}
	rows, err := f.store.Read(context.Background(), f.info, f.target, f.offsetMS, endMS, 20_000)
	if err != nil {
		f.loadErr = err
		f.rowIdx = -1
		f.nextMS = f.offsetMS
		return
	}
	if len(rows) == 0 {
		f.rowIdx = -1
		f.nextMS = math.MaxInt64
		return
	}
	f.rows = rows
	f.rowIdx = 0
	f.nextMS = rows[0].EndMS
	f.offsetMS = rows[len(rows)-1].TimeMS + 1
}

func (f *HistSeriesFeeder) Type() string { return f.info.Name }

func (f *HistSeriesFeeder) sameProjection(other *HistSeriesFeeder) bool {
	if f == nil || other == nil || f.target.ID != other.target.ID || f.info.Name != other.info.Name || f.info.TimeFrame != other.info.TimeFrame {
		return false
	}
	a, b := f.info.Binding.Fields, other.info.Binding.Fields
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var _ IHistFeeder = (*HistSeriesFeeder)(nil)
