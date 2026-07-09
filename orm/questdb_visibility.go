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

func verifyQuestRewriteCount(ctx context.Context, q *Queries, tableName string, expected int64) *errs.Error {
	var count int64
	if err := q.db.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&count); err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if count != expected {
		return errs.NewMsg(core.ErrDbReadFail, "rewrite snapshot row count mismatch: table=%s got=%d want=%d", tableName, count, expected)
	}
	return nil
}
