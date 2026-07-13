package orm

import (
	"context"
	"fmt"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

var (
	questReadAfterWriteTimeout      = 500 * time.Millisecond
	questReadAfterWritePollInterval = 50 * time.Millisecond
)

func normalizeQuestTimestamp(ts time.Time) time.Time {
	return ts.UTC().Truncate(time.Microsecond)
}

func waitForQuestCondition(ctx context.Context, timeout, interval time.Duration, check func() (bool, error)) (bool, error) {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := check()
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(interval):
		}
	}
}

func questExsymbolBySID(ctx context.Context, q *Queries, sid int32) (*ExSymbol, error) {
	row := q.db.QueryRow(ctx, `SELECT sid, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, coalesce(agg_rules, '')
FROM exsymbol_q
LATEST BY sid
WHERE sid = $1 AND coalesce(is_deleted, false) = false`, sid)
	var item ExSymbol
	if err := row.Scan(&item.ID, &item.Exchange, &item.ExgReal, &item.Market, &item.Symbol, &item.Combined, &item.ListMs, &item.DelistMs, &item.AggRules); err != nil {
		return nil, err
	}
	return &item, nil
}

func questExsymbolVisible(ctx context.Context, q *Queries, sid int32) (bool, error) {
	_, visible, err := pollQuestExsymbolVisible(ctx, q, sid)
	return visible, err
}

func pollQuestExsymbolVisible(ctx context.Context, q *Queries, sid int32) (*ExSymbol, bool, error) {
	var item *ExSymbol
	ok, err := waitForQuestCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		got, err := questExsymbolBySID(ctx, q, sid)
		if err != nil {
			return false, nil
		}
		item = got
		return true, nil
	})
	if err != nil {
		return nil, false, err
	}
	return item, ok, nil
}

func waitForQuestExsymbolVisible(ctx context.Context, q *Queries, sid int32) (*ExSymbol, error) {
	item, ok, err := pollQuestExsymbolVisible(ctx, q, sid)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errs.NewMsg(core.ErrDbReadFail, "questdb row not visible before timeout: table=exsymbol_q sid=%d", sid)
	}
	return item, nil
}

func waitForQuestExsymbolTimestampVisible(ctx context.Context, q *Queries, sid int32, want time.Time) error {
	want = normalizeQuestTimestamp(want)
	ok, err := waitForQuestCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var maxTS *time.Time
		if err := q.db.QueryRow(ctx, `SELECT max(ts) FROM exsymbol_q WHERE sid = $1`, sid).Scan(&maxTS); err != nil {
			return false, err
		}
		return maxTS != nil && !maxTS.Before(want), nil
	})
	if err != nil {
		return err
	}
	if !ok {
		return errs.NewMsg(core.ErrDbReadFail, "questdb row version not visible before timeout: table=exsymbol_q sid=%d", sid)
	}
	return nil
}

func waitForQuestCalendarTimestampVisible(ctx context.Context, q *Queries, market string, want time.Time) error {
	want = normalizeQuestTimestamp(want)
	ok, err := waitForQuestCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var maxTS *time.Time
		if err := q.db.QueryRow(ctx, `SELECT max(ts) FROM calendars_q WHERE market = $1`, market).Scan(&maxTS); err != nil {
			return false, err
		}
		return maxTS != nil && !maxTS.Before(want), nil
	})
	if err != nil {
		return err
	}
	if !ok {
		return errs.NewMsg(core.ErrDbReadFail, "questdb row version not visible before timeout: table=calendars_q market=%s", market)
	}
	return nil
}

func questKlineWindowVisible(ctx context.Context, q *Queries, sid int32, timeframe string, startMS, endMS int64) (bool, error) {
	tblName := "kline_" + timeframe
	sqlText := fmt.Sprintf(`SELECT count(*) > 0 FROM %s
WHERE sid = $1 AND ts >= cast($2 as timestamp) AND ts < cast($3 as timestamp)`, tblName)
	var ok bool
	err := q.db.QueryRow(ctx, sqlText, sid, startMS*1000, endMS*1000).Scan(&ok)
	return ok, err
}

func waitForQuestKlineWindowVisible(ctx context.Context, q *Queries, sid int32, timeframe string, startMS, endMS int64) (bool, *errs.Error) {
	visible := false
	_, err := waitForQuestCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		ok, err := questKlineWindowVisible(ctx, q, sid, timeframe, startMS, endMS)
		if err != nil {
			return false, err
		}
		visible = ok
		return ok, nil
	})
	if err != nil {
		return false, NewDbErr(core.ErrDbReadFail, err)
	}
	return visible, nil
}

func questKlineCoverageVisible(ctx context.Context, q *Queries, sid int32, timeframe string, startMS, endMS int64) (bool, error) {
	spans, err := q.loadSRangesSpansFromDB(ctx, sid, "kline_"+timeframe, timeframe, startMS, endMS)
	if err != nil {
		return false, err
	}
	covered := make([]MSRange, 0, len(spans))
	for _, span := range spans {
		if span.HasData && span.StopMs > span.StartMs {
			covered = append(covered, MSRange{Start: span.StartMs, Stop: span.StopMs})
		}
	}
	missing := subtractMSRanges(MSRange{Start: startMS, Stop: endMS}, mergeMSRanges(covered))
	return len(missing) == 0, nil
}

// pollQuestKlineCoverageVisible distinguishes a normal WAL visibility timeout from
// a database read failure. Callers that already have a durable pending insert marker
// can defer the former to recovery without treating it as a failed write.
func pollQuestKlineCoverageVisible(ctx context.Context, q *Queries, sid int32, timeframe string, startMS, endMS int64) (bool, *errs.Error) {
	ok, err := waitForQuestCondition(ctx, klineInsertQuestVisibilityGrace, questReadAfterWritePollInterval, func() (bool, error) {
		return questKlineCoverageVisible(ctx, q, sid, timeframe, startMS, endMS)
	})
	if err != nil {
		return false, NewDbErr(core.ErrDbReadFail, err)
	}
	return ok, nil
}

func waitForQuestKlineCoverageVisible(ctx context.Context, q *Queries, sid int32, timeframe string, startMS, endMS int64) *errs.Error {
	ok, err := pollQuestKlineCoverageVisible(ctx, q, sid, timeframe, startMS, endMS)
	if err != nil {
		return err
	}
	if !ok {
		return errs.NewMsg(core.ErrDbReadFail,
			"questdb kline coverage not visible before timeout: sid=%d timeframe=%s start=%d end=%d",
			sid, timeframe, startMS, endMS)
	}
	return nil
}

// pollQuestKlineTimestampVisible distinguishes a normal WAL visibility timeout
// from a database read failure. A successful INSERT is durable even while QuestDB
// has not made the row queryable yet.
func pollQuestKlineTimestampVisible(ctx context.Context, q *Queries, sid int32, timeframe string, timeMS int64) (bool, *errs.Error) {
	tblName := "kline_" + timeframe
	sqlText := fmt.Sprintf(`SELECT count(*) > 0 FROM %s
	WHERE sid = $1 AND ts = cast($2 as timestamp)`, tblName)
	ok, err := waitForQuestCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var visible bool
		if err := q.db.QueryRow(ctx, sqlText, sid, timeMS*1000).Scan(&visible); err != nil {
			return false, err
		}
		return visible, nil
	})
	if err != nil {
		return false, NewDbErr(core.ErrDbReadFail, err)
	}
	return ok, nil
}

func waitForQuestKlineTimestampVisible(ctx context.Context, q *Queries, sid int32, timeframe string, timeMS int64) *errs.Error {
	ok, err := pollQuestKlineTimestampVisible(ctx, q, sid, timeframe, timeMS)
	if err != nil {
		return err
	}
	if !ok {
		return errs.NewMsg(core.ErrDbReadFail,
			"questdb kline row not visible before timeout: sid=%d timeframe=%s time=%d", sid, timeframe, timeMS)
	}
	return nil
}

func queryQuestKlineTimeRange(ctx context.Context, q *Queries, sid int32, timeframe string) (int64, int64, error) {
	tblName := "kline_" + timeframe
	sqlText := fmt.Sprintf("select min(cast(ts as long)/1000), max(cast(ts as long)/1000) from %s where sid=$1", tblName)
	row := q.db.QueryRow(ctx, sqlText, sid)
	var minTime, maxTime *int64
	if err := row.Scan(&minTime, &maxTime); err != nil {
		return 0, 0, err
	}
	if minTime == nil || maxTime == nil {
		return 0, 0, nil
	}
	return *minTime, *maxTime, nil
}

func waitForQuestKlineRangeVisible(ctx context.Context, q *Queries, sid int32, timeframe string) (int64, int64, *errs.Error) {
	var start, end int64
	_, err := waitForQuestCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var err error
		start, end, err = queryQuestKlineTimeRange(ctx, q, sid, timeframe)
		if err != nil {
			return false, err
		}
		return start > 0 && end > 0, nil
	})
	if err != nil {
		return 0, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	return start, end, nil
}

func verifyQuestRewriteSnapshot(ctx context.Context, q *Queries, tableName string, expected map[int32]int64) *errs.Error {
	var actual map[int32]int64
	ok, err := waitForQuestCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		rows, err := q.db.Query(ctx, fmt.Sprintf("SELECT sid, count(*) FROM %s GROUP BY sid ORDER BY sid", tableName))
		if err != nil {
			return false, err
		}
		defer rows.Close()
		actual = make(map[int32]int64)
		for rows.Next() {
			var sid int32
			var count int64
			if err := rows.Scan(&sid, &count); err != nil {
				return false, err
			}
			actual[sid] = count
		}
		if err := rows.Err(); err != nil {
			return false, err
		}
		return equalQuestRewriteSnapshot(actual, expected), nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if !ok {
		return errs.NewMsg(core.ErrDbReadFail,
			"rewrite snapshot mismatch before timeout: table=%s got=%v want=%v", tableName, actual, expected)
	}
	return nil
}

func equalQuestRewriteSnapshot(actual, expected map[int32]int64) bool {
	if len(actual) != len(expected) {
		return false
	}
	for sid, count := range expected {
		if actual[sid] != count {
			return false
		}
	}
	return true
}
