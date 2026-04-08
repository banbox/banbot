package data

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type DataSink interface {
	Emit(sub *strat.DataSub, rows []*orm.DataRecord) error
}

type DataSource interface {
	Info() *orm.SeriesInfo
	FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)
	SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink DataSink) error
}

var (
	dataSourcesMu sync.RWMutex
	dataSources   = make(map[string]DataSource)
)

func RegisterDataSource(src DataSource) error {
	if src == nil {
		return fmt.Errorf("data source is nil")
	}
	info := src.Info()
	if err := orm.ValidateSeriesInfo(info); err != nil {
		return err
	}
	if info.Name == "" {
		return fmt.Errorf("data source name is required")
	}
	dataSourcesMu.Lock()
	defer dataSourcesMu.Unlock()
	if _, ok := dataSources[info.Name]; ok {
		return fmt.Errorf("data source %q already registered", info.Name)
	}
	dataSources[info.Name] = src
	return nil
}

func GetDataSource(name string) DataSource {
	dataSourcesMu.RLock()
	defer dataSourcesMu.RUnlock()
	return dataSources[name]
}

func ListDataSources() []string {
	dataSourcesMu.RLock()
	defer dataSourcesMu.RUnlock()
	items := make([]string, 0, len(dataSources))
	for name := range dataSources {
		items = append(items, name)
	}
	sort.Strings(items)
	return items
}

func EnsureSeriesRange(ctx context.Context, src DataSource, sub *strat.DataSub, startMS, endMS int64) *errs.Error {
	return EnsureSeriesRangeWithRepo(ctx, orm.DefaultSeriesRepo(), src, sub, startMS, endMS)
}

func EnsureSeriesRangeWithRepo(ctx context.Context, repo orm.SeriesRepo, src DataSource, sub *strat.DataSub, startMS, endMS int64) *errs.Error {
	if startMS >= endMS {
		return nil
	}
	if src == nil {
		return errs.NewMsg(core.ErrBadConfig, "data source is required")
	}
	if repo == nil {
		return errs.NewMsg(core.ErrBadConfig, "series repository is required")
	}
	if sub == nil || sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "data sub exsymbol is required")
	}
	info := src.Info()
	if err := orm.ValidateSeriesInfo(info); err != nil {
		return err
	}
	tf := sub.TimeFrame
	if tf == "" {
		tf = info.TimeFrame
	}
	if tf != info.TimeFrame {
		return errs.NewMsg(core.ErrBadConfig, "sub timeframe %s does not match source timeframe %s", tf, info.TimeFrame)
	}
	if sub.Source != "" && sub.Source != info.Name {
		return errs.NewMsg(core.ErrBadConfig, "sub source %s does not match data source %s", sub.Source, info.Name)
	}
	missing, err := orm.MissingSeriesRanges(ctx, info, sub.ExSymbol.ID, startMS, endMS)
	if err != nil || len(missing) == 0 {
		return err
	}
	if err := repo.EnsureSeriesTable(ctx, info); err != nil {
		return err
	}
	for _, gap := range missing {
		rows, err_ := src.FetchHistory(ctx, sub, gap.Start, gap.Stop)
		if err_ != nil {
			return errs.New(core.ErrRunTime, err_)
		}
		rows, err = normalizeSeriesRows(sub.ExSymbol.ID, rows)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			if err = repo.InsertSeriesBatch(ctx, info, rows); err != nil {
				return err
			}
		}
		if err = orm.UpdateSeriesCoverage(ctx, info, sub.ExSymbol.ID, gap.Start, gap.Stop, rows); err != nil {
			return err
		}
	}
	return nil
}

func normalizeSeriesRows(sid int32, rows []*orm.DataRecord) ([]*orm.DataRecord, *errs.Error) {
	if len(rows) == 0 {
		return nil, nil
	}
	items := make([]*orm.DataRecord, 0, len(rows))
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
