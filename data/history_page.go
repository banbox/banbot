package data

import (
	"context"
	"fmt"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

// PagedHistorySource returns strictly increasing timestamps in [start,end),
// at most limit rows. An empty page ends the range; source-owned transport
// buffers are outside the decoded page budget and must be bounded by adapters.
type PagedHistorySource interface {
	FetchHistoryPage(context.Context, *orm.Subscription, int64, int64, int) ([]*orm.DataRecord, error)
}

type historyPageRowsKey struct{}

// ReadSourceHistory validates each page before handing it to storage/warmup.
// With a byte limit, non-paged adapters fail before their whole-range Fetch.
// Without a byte limit, their existing full-range behavior remains available.
func ReadSourceHistory(ctx context.Context, source DataSource, sub *orm.Subscription, start, end int64, pageRows int, consume func([]*orm.DataRecord) error) error {
	if ctx == nil || source == nil || sub == nil || sub.ExSymbol == nil || consume == nil || pageRows <= 0 {
		return fmt.Errorf("history pages require context, source, subscription, consumer and positive page rows")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if start >= end {
		return nil
	}
	paged, ok := source.(PagedHistorySource)
	if !ok {
		if orm.SeriesReadByteLimit(ctx) > 0 {
			return fmt.Errorf("source %s requires paged history for an enabled page_bytes budget", sub.Source)
		}
		rows, err := source.FetchHistory(ctx, sub, start, end)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := consume(rows); err != nil {
			return err
		}
		return ctx.Err()
	}
	for cursor := start; cursor < end; {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := paged.FetchHistoryPage(ctx, sub, cursor, end, pageRows)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if err := orm.CheckDataRecordPage(ctx, rows, sub.ExSymbol.ID, cursor, end, pageRows); err != nil {
			return fmt.Errorf("source %s: %w", sub.Source, err)
		}
		previous := rows[len(rows)-1].TimeMS
		if err := consume(rows); err != nil {
			return err
		}
		cursor = previous + 1 // Last timestamp < end, so advancing cannot overflow.
	}
	return ctx.Err()
}

func ensurePagedSeriesRange(catalog *DataSourceCatalog, ctx context.Context, store *orm.SeriesStore, source DataSource, sub *orm.Subscription, start, end int64) (result *errs.Error) {
	if ctx == nil {
		return errs.NewMsg(core.ErrBadConfig, "history context is required")
	}
	rowsWritten := 0
	defer func() {
		if catalog == nil {
			return
		}
		status := DataSourceOpStatus{State: "ok", AtMS: time.Now().UnixMilli(), Sid: sub.ExSymbol.ID, TimeFrame: source.Info().TimeFrame, StartMS: start, EndMS: end, Rows: rowsWritten}
		if result != nil {
			status.State, status.Error = "error", result.Short()
		}
		catalog.markDataSourceBackfill(source.Info().Name, status, status.Error)
	}()
	gaps := []orm.MSRange{{Start: start, Stop: end}}
	if source.Info().TimeFrame != "event" {
		var err *errs.Error
		gaps, err = store.Missing(ctx, source.Info(), sub.ExSymbol, start, end)
		if err != nil {
			return err
		}
	}
	pageRows, _ := ctx.Value(historyPageRowsKey{}).(int)
	if pageRows <= 0 {
		pageRows = 20000
	}
	for _, gap := range gaps {
		gapRows := 0
		err := ReadSourceHistory(ctx, source, sub, gap.Start, gap.Stop, pageRows, func(rows []*orm.DataRecord) error {
			if len(rows) == 0 {
				return nil
			}
			if err := store.WriteBatch(ctx, source.Info(), sub.ExSymbol, rows); err != nil {
				return err
			}
			rowsWritten += len(rows)
			gapRows += len(rows)
			return nil
		})
		if err != nil {
			return errs.New(core.ErrDbReadFail, err)
		}
		if gapRows == 0 && source.Info().TimeFrame != "event" {
			if err := store.UpdateCoverage(ctx, source.Info(), sub.ExSymbol, gap.Start, gap.Stop, nil); err != nil {
				return err
			}
		}
	}
	return nil
}
