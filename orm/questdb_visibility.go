package orm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type questTableColumn struct {
	Name                  string
	Type                  string
	Indexed               bool
	IndexBlockCapacity    int64
	SymbolCached          bool
	SymbolCapacity        int64
	Designated            bool
	UpsertKey             bool
	IndexType             string
	IndexInclude          string
	columnPropertiesKnown bool
}

type questRewriteTableSnapshot struct {
	Columns     []questTableColumn
	Counts      map[int32]int64
	RowCount    int64
	Fingerprint [sha256.Size]byte
}

type questRewriteDB interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type questRewriteExecDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type questCompactRewriteSnapshot struct {
	Columns     []questTableColumn
	RowCount    int64
	Fingerprint [sha256.Size]byte
}

const questRewriteSwapIntentVersion = 1

type questRewriteSwapIntent struct {
	Version         int
	Kind            string
	Source          string
	Temp            string
	Backup          string
	SIDColumn       string
	SourcePredicate string
	TableSnapshot   *questRewriteTableSnapshot
	CompactMeta     *TableCompactMeta
	CompactSnapshot *questCompactRewriteSnapshot
}

type questRewriteIntentStore interface {
	Load(string) (*questRewriteSwapIntent, error)
	Save(*questRewriteSwapIntent) error
	Remove(string) error
}

type fileQuestRewriteIntentStore struct{ root string }

var questRewriteIntentStoreFn = func() questRewriteIntentStore {
	return &fileQuestRewriteIntentStore{root: compactProcessLockRootFn()}
}

func (s *fileQuestRewriteIntentStore) path(table string) (string, error) {
	if s.root == "" {
		return "", errors.New("QuestDB rewrite intent storage identity is empty")
	}
	name := strings.NewReplacer("/", "_", "\\", "_").Replace(table)
	if name == "" || name == "." || name == ".." {
		return "", fmt.Errorf("invalid QuestDB rewrite source table %q", table)
	}
	return filepath.Join(s.root, name+".swap.json"), nil
}

func (s *fileQuestRewriteIntentStore) Load(table string) (*questRewriteSwapIntent, error) {
	if s.root == "" {
		return nil, nil
	}
	path, err := s.path(table)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read QuestDB rewrite intent: %w", err)
	}
	var intent questRewriteSwapIntent
	if err := json.Unmarshal(data, &intent); err != nil {
		return nil, fmt.Errorf("decode QuestDB rewrite intent %s: %w", path, err)
	}
	if intent.Version != questRewriteSwapIntentVersion || intent.Source != table || intent.Temp == "" || intent.Backup == "" {
		return nil, fmt.Errorf("invalid QuestDB rewrite intent for %s", table)
	}
	return &intent, nil
}

func (s *fileQuestRewriteIntentStore) Save(intent *questRewriteSwapIntent) error {
	path, err := s.path(intent.Source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return fmt.Errorf("create QuestDB rewrite intent directory: %w", err)
	}
	data, err := json.Marshal(intent)
	if err != nil {
		return fmt.Errorf("encode QuestDB rewrite intent: %w", err)
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create QuestDB rewrite intent: %w", err)
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("persist QuestDB rewrite intent: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("activate QuestDB rewrite intent: %w", err)
	}
	return syncQuestRewriteIntentDir(s.root)
}

func (s *fileQuestRewriteIntentStore) Remove(table string) error {
	path, err := s.path(table)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove QuestDB rewrite intent: %w", err)
	}
	return syncQuestRewriteIntentDir(s.root)
}

func syncQuestRewriteIntentDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open QuestDB rewrite intent directory: %w", err)
	}
	err = errors.Join(dir.Sync(), dir.Close())
	if err != nil {
		return fmt.Errorf("sync QuestDB rewrite intent directory: %w", err)
	}
	return nil
}

var (
	questReadAfterWriteTimeout      = 3 * time.Second
	questReadAfterWritePollInterval = 50 * time.Millisecond
	questWaitForCondition           = waitForQuestCondition
	questGetWith                    = exg.GetWith
	questGetExSHoles                = GetExSHoles
	questRewriteRecoveryTimeout     = 5 * time.Second
)

func normalizeQuestTimestamp(ts time.Time) time.Time {
	return ts.UTC().Truncate(time.Microsecond)
}

func waitForQuestCondition(ctx context.Context, timeout, interval time.Duration, check func() (bool, error)) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
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

func questRewriteRecoveryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), questRewriteRecoveryTimeout)
}

func dropQuestRewriteTable(ctx context.Context, db questRewriteExecDB, table string) error {
	recoveryCtx, cancelRecovery := questRewriteRecoveryContext(ctx)
	defer cancelRecovery()
	if _, err := db.Exec(recoveryCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(table))); err != nil {
		return fmt.Errorf("cleanup rewrite table %s: %w", table, err)
	}
	return nil
}

func cleanupQuestRewriteFailure(ctx context.Context, db questRewriteExecDB, table string, cause error) error {
	if cleanupErr := dropQuestRewriteTable(ctx, db, table); cleanupErr != nil {
		var typedErr *errs.Error
		if errors.As(cause, &typedErr) {
			return fmt.Errorf("%s; %v", typedErr.Short(), cleanupErr)
		}
		return fmt.Errorf("%w; %v", cause, cleanupErr)
	}
	return cause
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

func questExsymbolsVisible(ctx context.Context, q *Queries, expected []exSymbolRecoveryRow) (bool, error) {
	if len(expected) == 0 {
		return true, nil
	}
	return questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		for _, row := range expected {
			item, err := questExsymbolBySID(ctx, q, row.ID)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return false, nil
				}
				return false, err
			}
			if !row.matches(item) {
				return false, nil
			}
		}
		return true, nil
	})
}

func pollQuestExsymbolVisible(ctx context.Context, q *Queries, sid int32) (*ExSymbol, bool, error) {
	var item *ExSymbol
	ok, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		got, err := questExsymbolBySID(ctx, q, sid)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
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
	ok, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var maxTS *time.Time
		if err := q.db.QueryRow(ctx, `SELECT max(ts) FROM exsymbol_q WHERE sid = $1`, sid).Scan(&maxTS); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
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
	ok, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var maxTS *time.Time
		if err := q.db.QueryRow(ctx, `SELECT max(ts) FROM calendars_q WHERE market = $1`, market).Scan(&maxTS); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
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
	return questKlineWindowVisibleWithExSymbol(ctx, q, sid, GetSymbolByID(sid), timeframe, startMS, endMS)
}

// questKlineWindowVisibleWithSymbolState keeps explicit runtime catalogs
// isolated from the legacy SID-only wrapper above.
func questKlineWindowVisibleWithSymbolState(ctx context.Context, q *Queries, state *SymbolState, sid int32, timeframe string, startMS, endMS int64) (bool, error) {
	if state == nil {
		return questKlineWindowVisible(ctx, q, sid, timeframe, startMS, endMS)
	}
	return questKlineWindowVisibleWithExSymbol(ctx, q, sid, state.GetSymbolByID(sid), timeframe, startMS, endMS)
}

func questKlineWindowVisibleWithExSymbol(ctx context.Context, q *Queries, sid int32, exs *ExSymbol, timeframe string, startMS, endMS int64) (bool, error) {
	tblName := "kline_" + timeframe
	tfMSecs := int64(utils2.TFToSecs(timeframe) * 1000)
	if startMS >= endMS || tfMSecs <= 0 {
		return false, nil
	}
	// A min/max query can mistake an interior WAL hole for a complete range.
	// Stream timestamps instead: recovery is cold-path work, and this preserves
	// the physical-row evidence without loading OHLCV values into memory.
	sqlText := fmt.Sprintf(`SELECT cast(ts as long)/1000 FROM %s
WHERE sid = $1 AND ts >= cast($2 as timestamp) AND ts < cast($3 as timestamp)
ORDER BY ts`, tblName)
	rows, err := q.db.Query(ctx, sqlText, sid, startMS*1000, endMS*1000)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var (
		firstTime int64
		lastTime  int64
		found     bool
		holes     []MSRange
	)
	for rows.Next() {
		var current int64
		if err := rows.Scan(&current); err != nil {
			return false, err
		}
		if !found {
			firstTime = current
			found = true
		} else if current > lastTime+tfMSecs {
			holes = append(holes, MSRange{Start: lastTime + tfMSecs, Stop: current})
		}
		lastTime = current
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if firstTime > startMS {
		holes = append(holes, MSRange{Start: startMS, Stop: firstTime})
	}
	if lastTime+tfMSecs < endMS {
		holes = append(holes, MSRange{Start: lastTime + tfMSecs, Stop: endMS})
	}
	if len(holes) == 0 {
		return true, nil
	}
	return questKlineWindowHolesAllowedWithExSymbol(exs, timeframe, startMS, endMS, tfMSecs, holes)
}

func questKlineWindowHolesAllowed(sid int32, timeframe string, startMS, endMS, tfMSecs int64, holes []MSRange) (bool, error) {
	return questKlineWindowHolesAllowedWithExSymbol(GetSymbolByID(sid), timeframe, startMS, endMS, tfMSecs, holes)
}

func questKlineWindowHolesAllowedWithExSymbol(exs *ExSymbol, timeframe string, startMS, endMS, tfMSecs int64, holes []MSRange) (bool, error) {
	// A missing catalog entry cannot prove that a gap is an exchange pause, so
	// fail closed and retain the pending marker for a later recovery pass.
	if exs == nil {
		return false, nil
	}
	exchange, err := questGetWith(exs.Exchange, exs.Market, "")
	if err != nil {
		return false, fmt.Errorf("construct exchange for QuestDB kline hole check: %w", err)
	}
	allowed, err := questGetExSHoles(exchange, exs, startMS, endMS, true)
	if err != nil {
		return false, fmt.Errorf("load exchange session holes for QuestDB kline hole check: %w", err)
	}
	allowedRanges := make([]MSRange, 0, len(allowed))
	for _, item := range allowed {
		allowedRanges = append(allowedRanges, MSRange{Start: item[0], Stop: item[1]})
	}
	remaining := make([]MSRange, 0, len(holes))
	for _, hole := range holes {
		remaining = append(remaining, subtractMSRanges(hole, allowedRanges)...)
	}
	if tfMSecs == 60_000 {
		minHole := exchange.Info().Min1mHole
		filtered := remaining[:0]
		for _, hole := range remaining {
			if int((hole.Stop-hole.Start)/tfMSecs) > minHole {
				filtered = append(filtered, hole)
			}
		}
		remaining = filtered
	}
	return len(remaining) == 0, nil
}

func waitForQuestKlineWindowVisible(ctx context.Context, q *Queries, sid int32, timeframe string, startMS, endMS int64) (bool, *errs.Error) {
	visible, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		ok, err := questKlineWindowVisible(ctx, q, sid, timeframe, startMS, endMS)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		return ok, nil
	})
	if err != nil {
		return false, NewDbErr(core.ErrDbReadFail, err)
	}
	if !visible {
		return false, errs.NewMsg(core.ErrTimeout,
			"questdb kline window not visible before timeout: sid=%d timeframe=%s start=%d end=%d",
			sid, timeframe, startMS, endMS)
	}
	return true, nil
}

func questKlineCoverageVisible(ctx context.Context, q *Queries, sid int32, timeframe string, startMS, endMS int64) (bool, error) {
	unlock := LockCompactTableRead("sranges_q")
	defer unlock()
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

func waitForQuestKlineCoverageVisible(ctx context.Context, q *Queries, sid int32, timeframe string, startMS, endMS int64) *errs.Error {
	ok, err := questWaitForCondition(ctx, klineInsertQuestVisibilityGrace, questReadAfterWritePollInterval, func() (bool, error) {
		visible, err := questKlineCoverageVisible(ctx, q, sid, timeframe, startMS, endMS)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		return visible, nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if !ok {
		return errs.NewMsg(core.ErrTimeout,
			"questdb kline coverage not visible before timeout: sid=%d timeframe=%s start=%d end=%d",
			sid, timeframe, startMS, endMS)
	}
	return nil
}

// WaitForSeriesCoverageVisible waits until QuestDB's persisted sranges snapshot,
// rather than the in-process cache, answers the complete requested interval.
func WaitForSeriesCoverageVisible(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	if !IsQuestDB || startMS >= endMS {
		return nil
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return waitForQuestSeriesCoverageVisible(ctx, q, info, sid, startMS, endMS)
}

func waitForQuestSeriesCoverageVisible(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	ok, err := questWaitForCondition(ctx, klineInsertQuestVisibilityGrace, questReadAfterWritePollInterval, func() (bool, error) {
		unlock := LockCompactTableRead("sranges_q")
		defer unlock()
		spans, err := q.loadSRangesSpansFromDB(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		answered := make([]MSRange, 0, len(spans))
		for _, span := range spans {
			answered = append(answered, MSRange{Start: span.StartMs, Stop: span.StopMs})
		}
		return len(subtractMSRanges(MSRange{Start: startMS, Stop: endMS}, mergeMSRanges(answered))) == 0, nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if !ok {
		return errs.NewMsg(core.ErrTimeout,
			"questdb series coverage not visible before timeout: table=%s sid=%d timeframe=%s start=%d end=%d",
			binding.Table, sid, info.TimeFrame, startMS, endMS)
	}
	return nil
}

func waitForQuestKlineTimestampVisible(ctx context.Context, q *Queries, sid int32, timeframe string, timeMS int64) *errs.Error {
	tblName := "kline_" + timeframe
	sqlText := fmt.Sprintf(`SELECT count(*) > 0 FROM %s
	WHERE sid = $1 AND ts = cast($2 as timestamp)`, tblName)
	ok, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var visible bool
		if err := q.db.QueryRow(ctx, sqlText, sid, timeMS*1000).Scan(&visible); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		return visible, nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if !ok {
		return errs.NewMsg(core.ErrDbReadFail,
			"questdb kline row not visible before timeout: sid=%d timeframe=%s time=%d", sid, timeframe, timeMS)
	}
	return nil
}

func waitForQuestKlineRangeVisible(ctx context.Context, q *Queries, sid int32, timeframe string, wantStart, wantEnd int64) (int64, int64, *errs.Error) {
	visible, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		return questKlineWindowVisible(ctx, q, sid, timeframe, wantStart, wantEnd)
	})
	if err != nil {
		return 0, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	if !visible {
		return 0, 0, errs.NewMsg(core.ErrTimeout,
			"questdb kline range not visible before timeout: sid=%d timeframe=%s start=%d end=%d",
			sid, timeframe, wantStart, wantEnd)
	}
	return wantStart, wantEnd, nil
}

func verifyQuestRewriteSnapshot(ctx context.Context, q *Queries, tableName string, expected map[int32]int64) *errs.Error {
	var actual map[int32]int64
	ok, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		rows, err := q.db.Query(ctx, fmt.Sprintf("SELECT sid, count(*) FROM %s GROUP BY sid ORDER BY sid", tableName))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
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

func captureQuestRewriteTableSnapshot(ctx context.Context, q *Queries, tableName, sidColumn, predicate string) (*questRewriteTableSnapshot, error) {
	columns, err := queryQuestTableColumns(ctx, q, tableName)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("questdb table %s has no columns", tableName)
	}
	counts, err := queryQuestRewriteCounts(ctx, q, tableName, sidColumn, predicate)
	if err != nil {
		return nil, err
	}
	rowCount, fingerprint, err := streamQuestRewriteFingerprint(ctx, q.db, tableName, columns, predicate)
	if err != nil {
		return nil, err
	}
	var countedRows int64
	for _, count := range counts {
		countedRows += count
	}
	if rowCount != countedRows {
		return nil, fmt.Errorf("questdb table %s row count changed while fingerprinting: got=%d want=%d", tableName, rowCount, countedRows)
	}
	return &questRewriteTableSnapshot{
		Columns:     columns,
		Counts:      counts,
		RowCount:    rowCount,
		Fingerprint: fingerprint,
	}, nil
}

func queryQuestTableColumns(ctx context.Context, q *Queries, tableName string) ([]questTableColumn, error) {
	return queryQuestTableColumnsDB(ctx, q.db, tableName)
}

func queryQuestTableColumnsDB(ctx context.Context, db questRewriteDB, tableName string) ([]questTableColumn, error) {
	tableLiteral := strings.ReplaceAll(tableName, "'", "''")
	// QuestDB exposes different table_columns() schemas across releases. Query
	// the function as a whole so a server that has not added newer metadata
	// columns does not reject the projection before we can inspect its shape.
	rows, err := db.Query(ctx, fmt.Sprintf("SELECT * FROM table_columns('%s')", tableLiteral))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	descriptions := rows.FieldDescriptions()
	if len(descriptions) > 0 {
		return queryQuestTableColumnsByName(rows, descriptions)
	}
	return queryQuestTableColumnsPositionally(rows)
}

func queryQuestTableColumnsByName(rows pgx.Rows, descriptions []pgconn.FieldDescription) ([]questTableColumn, error) {
	indexes := make(map[string]int, len(descriptions))
	for index, description := range descriptions {
		indexes[normalizeQuestColumnField(string(description.Name))] = index
	}
	if _, ok := indexes["column"]; !ok {
		return nil, fmt.Errorf("questdb table_columns result has no column field")
	}
	if _, ok := indexes["type"]; !ok {
		return nil, fmt.Errorf("questdb table_columns result has no type field")
	}
	var columns []questTableColumn
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read QuestDB table_columns row: %w", err)
		}
		if len(values) != len(descriptions) {
			return nil, fmt.Errorf("questdb table_columns field count changed: got=%d want=%d", len(values), len(descriptions))
		}
		column, err := decodeQuestTableColumn(values, indexes)
		if err != nil {
			return nil, err
		}
		column.IndexType = strings.TrimSpace(column.IndexType)
		column.IndexInclude = strings.TrimSpace(column.IndexInclude)
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func queryQuestTableColumnsPositionally(rows pgx.Rows) ([]questTableColumn, error) {
	const (
		legacyFieldCount         = 4
		propertyFieldCount       = 8
		questDBCurrentFieldCount = 9
		currentFieldCount        = 10
		questDBFieldCount        = 11
	)
	fieldCount := 0
	var columns []questTableColumn
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read QuestDB table_columns row: %w", err)
		}
		if fieldCount == 0 {
			fieldCount = len(values)
			if fieldCount != legacyFieldCount && fieldCount != propertyFieldCount && fieldCount != questDBCurrentFieldCount && fieldCount != currentFieldCount && fieldCount != questDBFieldCount {
				return nil, fmt.Errorf("questdb table_columns returned %d fields, expected %d, %d, %d, %d, or %d", fieldCount, legacyFieldCount, propertyFieldCount, questDBCurrentFieldCount, currentFieldCount, questDBFieldCount)
			}
		}
		if len(values) != fieldCount {
			return nil, fmt.Errorf("questdb table_columns field count changed: got=%d want=%d", len(values), fieldCount)
		}
		column, err := decodeQuestTableColumnPositionally(values, fieldCount)
		if err != nil {
			return nil, err
		}
		column.IndexType = strings.TrimSpace(column.IndexType)
		column.IndexInclude = strings.TrimSpace(column.IndexInclude)
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func normalizeQuestColumnField(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "")
	return name
}

func decodeQuestTableColumn(values []any, indexes map[string]int) (questTableColumn, error) {
	var column questTableColumn
	var err error
	column.Name, err = questColumnString(values, indexes, "column", true)
	if err != nil {
		return column, err
	}
	column.Type, err = questColumnString(values, indexes, "type", true)
	if err != nil {
		return column, err
	}
	column.Indexed, err = questColumnBool(values, indexes, "indexed")
	if err != nil {
		return column, err
	}
	column.IndexBlockCapacity, err = questColumnInt64(values, indexes, "indexblockcapacity")
	if err != nil {
		return column, err
	}
	column.SymbolCached, err = questColumnBool(values, indexes, "symbolcached")
	if err != nil {
		return column, err
	}
	column.SymbolCapacity, err = questColumnInt64(values, indexes, "symbolcapacity")
	if err != nil {
		return column, err
	}
	column.Designated, err = questColumnBool(values, indexes, "designated")
	if err != nil {
		return column, err
	}
	column.UpsertKey, err = questColumnBool(values, indexes, "upsertkey")
	if err != nil {
		return column, err
	}
	column.IndexType, err = questColumnString(values, indexes, "indextype", false)
	if err != nil {
		return column, err
	}
	column.IndexInclude, err = questColumnString(values, indexes, "indexinclude", false)
	if err != nil {
		return column, err
	}
	column.columnPropertiesKnown = hasQuestColumnField(indexes, "indexed") || hasQuestColumnField(indexes, "symbolcached")
	return column, nil
}

func decodeQuestTableColumnPositionally(values []any, fieldCount int) (questTableColumn, error) {
	var column questTableColumn
	read := func(index int, target any) error {
		return assignQuestColumnValue(target, values[index])
	}
	var err error
	if err = read(0, &column.Name); err != nil {
		return column, fmt.Errorf("decode QuestDB table_columns name: %w", err)
	}
	if err = read(1, &column.Type); err != nil {
		return column, fmt.Errorf("decode QuestDB table_columns type: %w", err)
	}
	switch fieldCount {
	case 4:
		err = read(2, &column.Designated)
		if err == nil {
			err = read(3, &column.UpsertKey)
		}
	case 8:
		column.columnPropertiesKnown = true
		err = read(2, &column.Indexed)
		if err == nil {
			err = read(3, &column.IndexBlockCapacity)
		}
		if err == nil {
			err = read(4, &column.SymbolCached)
		}
		if err == nil {
			err = read(5, &column.SymbolCapacity)
		}
		if err == nil {
			err = read(6, &column.Designated)
		}
		if err == nil {
			err = read(7, &column.UpsertKey)
		}
	case 9:
		column.columnPropertiesKnown = true
		err = read(2, &column.Indexed)
		if err == nil {
			err = read(3, &column.IndexBlockCapacity)
		}
		if err == nil {
			err = read(4, &column.SymbolCached)
		}
		if err == nil {
			err = read(5, &column.SymbolCapacity)
		}
		if err == nil {
			err = read(7, &column.Designated)
		}
		if err == nil {
			err = read(8, &column.UpsertKey)
		}
	case 10:
		column.columnPropertiesKnown = true
		err = read(2, &column.Indexed)
		if err == nil {
			err = read(3, &column.IndexBlockCapacity)
		}
		if err == nil {
			err = read(4, &column.SymbolCached)
		}
		if err == nil {
			err = read(5, &column.SymbolCapacity)
		}
		if err == nil {
			err = read(6, &column.Designated)
		}
		if err == nil {
			err = read(7, &column.UpsertKey)
		}
		if err == nil {
			err = read(8, &column.IndexType)
		}
		if err == nil {
			err = read(9, &column.IndexInclude)
		}
	case 11:
		column.columnPropertiesKnown = true
		err = read(2, &column.Indexed)
		if err == nil {
			err = read(3, &column.IndexBlockCapacity)
		}
		if err == nil {
			err = read(4, &column.SymbolCached)
		}
		if err == nil {
			err = read(5, &column.SymbolCapacity)
		}
		if err == nil {
			err = read(7, &column.Designated)
		}
		if err == nil {
			err = read(8, &column.UpsertKey)
		}
		if err == nil {
			err = read(9, &column.IndexType)
		}
		if err == nil {
			err = read(10, &column.IndexInclude)
		}
	default:
		return column, fmt.Errorf("questdb table_columns returned unsupported field count %d", fieldCount)
	}
	if err != nil {
		return column, fmt.Errorf("decode QuestDB table_columns row: %w", err)
	}
	return column, nil
}

func hasQuestColumnField(indexes map[string]int, name string) bool {
	_, ok := indexes[name]
	return ok
}

func questColumnString(values []any, indexes map[string]int, name string, required bool) (string, error) {
	index, ok := indexes[name]
	if !ok {
		if required {
			return "", fmt.Errorf("questdb table_columns result has no %s field", name)
		}
		return "", nil
	}
	return questValueString(values[index])
}

func questColumnBool(values []any, indexes map[string]int, name string) (bool, error) {
	index, ok := indexes[name]
	if !ok || values[index] == nil {
		return false, nil
	}
	return questValueBool(values[index])
}

func questColumnInt64(values []any, indexes map[string]int, name string) (int64, error) {
	index, ok := indexes[name]
	if !ok || values[index] == nil {
		return 0, nil
	}
	return questValueInt64(values[index])
}

func questValueString(value any) (string, error) {
	switch value := value.(type) {
	case nil:
		return "", nil
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return fmt.Sprint(value), nil
	}
}

func questValueBool(value any) (bool, error) {
	if typed, ok := value.(bool); ok {
		return typed, nil
	}
	parsed, err := strconv.ParseBool(fmt.Sprint(value))
	if err != nil {
		return false, fmt.Errorf("parse bool %v: %w", value, err)
	}
	return parsed, nil
}

func questValueInt64(value any) (int64, error) {
	if typed, ok := value.(int64); ok {
		return typed, nil
	}
	if typed, ok := value.(int32); ok {
		return int64(typed), nil
	}
	parsed, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse integer %v: %w", value, err)
	}
	return parsed, nil
}

func assignQuestColumnValue(target any, value any) error {
	switch output := target.(type) {
	case *string:
		var err error
		*output, err = questValueString(value)
		return err
	case *bool:
		if value == nil {
			return nil
		}
		var err error
		*output, err = questValueBool(value)
		return err
	case *int64:
		if value == nil {
			return nil
		}
		var err error
		*output, err = questValueInt64(value)
		return err
	default:
		return fmt.Errorf("unsupported QuestDB table_columns target %T", target)
	}
}

func streamQuestRewriteFingerprint(ctx context.Context, db questRewriteDB, tableName string, columns []questTableColumn, predicate string) (int64, [sha256.Size]byte, error) {
	fromClause := "FROM " + quoteIdent(tableName)
	if predicate != "" {
		fromClause += " WHERE " + predicate
	}
	return streamQuestRewriteFingerprintFromClause(ctx, db, tableName, columns, fromClause)
}

func streamQuestRewriteFingerprintFromClause(ctx context.Context, db questRewriteDB, tableName string, columns []questTableColumn, fromClause string) (int64, [sha256.Size]byte, error) {
	var fingerprint [sha256.Size]byte
	if len(columns) == 0 {
		return 0, fingerprint, fmt.Errorf("questdb table %s has no columns", tableName)
	}
	columnNames := questRewriteColumnNames(columns)
	if !strings.HasPrefix(strings.TrimSpace(fromClause), "FROM ") {
		return 0, fingerprint, fmt.Errorf("questdb table %s fingerprint source clause must start with FROM", tableName)
	}
	sqlText := fmt.Sprintf("SELECT %s %s ORDER BY %s", strings.Join(columnNames, ", "), fromClause, strings.Join(columnNames, ", "))
	rows, err := db.Query(ctx, sqlText)
	if err != nil {
		return 0, fingerprint, err
	}
	defer rows.Close()

	h := sha256.New()
	writeQuestFingerprintHeader(h, columns)
	var rowCount int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fingerprint, err
		}
		if len(values) != len(columns) {
			return 0, fingerprint, fmt.Errorf("questdb table %s fingerprint column count mismatch: got=%d want=%d", tableName, len(values), len(columns))
		}
		h.Write([]byte{0x52})
		writeQuestFingerprintUint(h, uint64(len(values)))
		for _, value := range values {
			encoded := appendQuestFingerprintValue(nil, value)
			writeQuestFingerprintBytes(h, encoded)
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return 0, fingerprint, err
	}
	copy(fingerprint[:], h.Sum(nil))
	return rowCount, fingerprint, nil
}

func writeQuestFingerprintHeader(h hash.Hash, columns []questTableColumn) {
	h.Write([]byte("banbot-questdb-row-fingerprint-v1"))
	writeQuestFingerprintUint(h, uint64(len(columns)))
	for _, column := range columns {
		writeQuestFingerprintString(h, column.Name)
		writeQuestFingerprintString(h, column.Type)
		if column.Indexed {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		writeQuestFingerprintUint(h, uint64(column.IndexBlockCapacity))
		if column.SymbolCached {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		writeQuestFingerprintUint(h, uint64(column.SymbolCapacity))
		if column.Designated {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		if column.UpsertKey {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		writeQuestFingerprintString(h, column.IndexType)
		writeQuestFingerprintString(h, column.IndexInclude)
	}
}

func writeQuestFingerprintUint(h hash.Hash, value uint64) {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	_, _ = h.Write(encoded[:n])
}

func writeQuestFingerprintString(h hash.Hash, value string) {
	writeQuestFingerprintBytes(h, []byte(value))
}

func writeQuestFingerprintBytes(h hash.Hash, value []byte) {
	writeQuestFingerprintUint(h, uint64(len(value)))
	_, _ = h.Write(value)
}

func appendQuestFingerprintValue(dst []byte, value any) []byte {
	if value == nil {
		return append(dst, 0)
	}
	return appendQuestFingerprintReflectValue(dst, reflect.ValueOf(value))
}

func appendQuestFingerprintReflectValue(dst []byte, value reflect.Value) []byte {
	if !value.IsValid() {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	dst = appendQuestFingerprintStringBytes(dst, questFingerprintTypeName(value.Type()))
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return append(dst, 0)
		}
		dst = append(dst, 1)
		return appendQuestFingerprintReflectValue(dst, value.Elem())
	case reflect.Pointer:
		if value.IsNil() {
			return append(dst, 0)
		}
		dst = append(dst, 1)
		return appendQuestFingerprintReflectValue(dst, value.Elem())
	case reflect.Bool:
		if value.Bool() {
			return append(dst, 1)
		}
		return append(dst, 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return appendQuestFingerprintUintBytes(dst, uint64(value.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return appendQuestFingerprintUintBytes(dst, value.Uint())
	case reflect.Float32:
		return appendQuestFingerprintUintBytes(dst, uint64(math.Float32bits(float32(value.Float()))))
	case reflect.Float64:
		return appendQuestFingerprintUintBytes(dst, math.Float64bits(value.Float()))
	case reflect.Complex64:
		complexValue := complex64(value.Complex())
		dst = appendQuestFingerprintUintBytes(dst, uint64(math.Float32bits(real(complexValue))))
		return appendQuestFingerprintUintBytes(dst, uint64(math.Float32bits(imag(complexValue))))
	case reflect.Complex128:
		complexValue := value.Complex()
		dst = appendQuestFingerprintUintBytes(dst, math.Float64bits(real(complexValue)))
		return appendQuestFingerprintUintBytes(dst, math.Float64bits(imag(complexValue)))
	case reflect.String:
		return appendQuestFingerprintStringBytes(dst, value.String())
	case reflect.Slice:
		if value.IsNil() {
			return append(dst, 0)
		}
		dst = append(dst, 1)
		fallthrough
	case reflect.Array:
		dst = appendQuestFingerprintUintBytes(dst, uint64(value.Len()))
		for i := 0; i < value.Len(); i++ {
			dst = appendQuestFingerprintReflectValue(dst, value.Index(i))
		}
		return dst
	case reflect.Struct:
		if value.Type() == reflect.TypeOf(time.Time{}) && value.CanInterface() {
			if encoded, err := value.Interface().(time.Time).MarshalBinary(); err == nil {
				dst = append(dst, 'T')
				return appendQuestFingerprintStringBytes(dst, string(encoded))
			}
		}
		dst = appendQuestFingerprintUintBytes(dst, uint64(value.NumField()))
		for i := 0; i < value.NumField(); i++ {
			dst = appendQuestFingerprintStringBytes(dst, value.Type().Field(i).Name)
			dst = appendQuestFingerprintReflectValue(dst, value.Field(i))
		}
		return dst
	case reflect.Map:
		if value.IsNil() {
			return append(dst, 0)
		}
		dst = append(dst, 1)
		entries := make([][]byte, 0, value.Len())
		iter := value.MapRange()
		for iter.Next() {
			entry := appendQuestFingerprintReflectValue(nil, iter.Key())
			entry = appendQuestFingerprintReflectValue(entry, iter.Value())
			entries = append(entries, entry)
		}
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i], entries[j]) < 0 })
		dst = appendQuestFingerprintUintBytes(dst, uint64(len(entries)))
		for _, entry := range entries {
			dst = appendQuestFingerprintStringBytes(dst, string(entry))
		}
		return dst
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return appendQuestFingerprintUintBytes(dst, uint64(value.Pointer()))
	default:
		if value.CanInterface() {
			return appendQuestFingerprintStringBytes(dst, fmt.Sprintf("%#v", value.Interface()))
		}
		return dst
	}
}

func questFingerprintTypeName(typ reflect.Type) string {
	if typ.PkgPath() == "" {
		return typ.String()
	}
	return typ.PkgPath() + "." + typ.String()
}

func appendQuestFingerprintUintBytes(dst []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(dst, encoded[:]...)
}

func appendQuestFingerprintStringBytes(dst []byte, value string) []byte {
	dst = appendQuestFingerprintUintBytes(dst, uint64(len(value)))
	return append(dst, value...)
}

func captureQuestCompactRewriteSnapshot(ctx context.Context, db questRewriteDB, table string, meta *TableCompactMeta) (*questCompactRewriteSnapshot, error) {
	if meta == nil {
		return nil, fmt.Errorf("compact metadata is nil for table %s", table)
	}
	if meta.LatestByKeys == "" {
		return nil, fmt.Errorf("compact latest-by keys are empty for table %s", table)
	}
	columns, err := queryQuestTableColumnsDB(ctx, db, table)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("questdb table %s has no columns", table)
	}
	if !questRewriteHasColumn(columns, "is_deleted") {
		return nil, fmt.Errorf("questdb compact table %s has no is_deleted column", table)
	}
	latestBy, err := quoteQuestRewriteIdentifierList(meta.LatestByKeys, columns)
	if err != nil {
		return nil, fmt.Errorf("compact latest-by keys for %s: %w", table, err)
	}
	livePredicate := fmt.Sprintf("coalesce(%s, false) = false", quoteIdent("is_deleted"))
	fromLatest := fmt.Sprintf("FROM %s LATEST BY %s WHERE %s", quoteIdent(table), latestBy, livePredicate)
	rowCount, fingerprint, err := streamQuestRewriteFingerprintFromClause(ctx, db, table, columns, fromLatest)
	if err != nil {
		return nil, err
	}
	return &questCompactRewriteSnapshot{Columns: columns, RowCount: rowCount, Fingerprint: fingerprint}, nil
}

func questRewriteHasColumn(columns []questTableColumn, name string) bool {
	for _, column := range columns {
		if column.Name == name {
			return true
		}
	}
	return false
}

func questRewriteColumnType(columns []questTableColumn, name string) (string, bool) {
	for _, column := range columns {
		if column.Name == name {
			return strings.TrimSpace(column.Type), true
		}
	}
	return "", false
}

func questRewriteSchema(columns []questTableColumn) (string, []string, error) {
	if len(columns) == 0 {
		return "", nil, errors.New("questdb rewrite schema is empty")
	}
	seen := make(map[string]struct{}, len(columns))
	var designated string
	upsertKeys := make([]string, 0, len(columns))
	for _, column := range columns {
		if strings.TrimSpace(column.Name) == "" {
			return "", nil, errors.New("questdb rewrite schema contains an empty column name")
		}
		if _, ok := seen[column.Name]; ok {
			return "", nil, fmt.Errorf("questdb rewrite schema contains duplicate column %q", column.Name)
		}
		seen[column.Name] = struct{}{}
		if strings.TrimSpace(column.Type) == "" {
			return "", nil, fmt.Errorf("questdb rewrite column %q has no type", column.Name)
		}
		if column.Designated {
			if designated != "" {
				return "", nil, fmt.Errorf("questdb rewrite schema has multiple designated columns: %q and %q", designated, column.Name)
			}
			designated = column.Name
		}
		if column.UpsertKey {
			upsertKeys = append(upsertKeys, quoteIdent(column.Name))
		}
	}
	if designated == "" {
		return "", nil, errors.New("questdb rewrite schema has no designated timestamp column")
	}
	return designated, upsertKeys, nil
}

func equalQuestTableColumns(actual, expected []questTableColumn) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		left, right := actual[i], expected[i]
		if left.Name != right.Name ||
			left.Type != right.Type ||
			left.Indexed != right.Indexed ||
			left.IndexBlockCapacity != right.IndexBlockCapacity ||
			left.SymbolCached != right.SymbolCached ||
			left.SymbolCapacity != right.SymbolCapacity ||
			left.Designated != right.Designated ||
			left.UpsertKey != right.UpsertKey ||
			left.IndexType != right.IndexType ||
			left.IndexInclude != right.IndexInclude {
			return false
		}
	}
	return true
}

func questRewriteColumnPropertiesKnown(column questTableColumn) bool {
	return column.columnPropertiesKnown ||
		column.Indexed ||
		column.IndexBlockCapacity != 0 ||
		column.SymbolCached ||
		column.SymbolCapacity != 0 ||
		strings.TrimSpace(column.IndexType) != "" ||
		strings.TrimSpace(column.IndexInclude) != ""
}

func questRewriteColumnIndex(column questTableColumn, columns []questTableColumn) (string, error) {
	indexType := strings.ToUpper(strings.Join(strings.Fields(column.IndexType), " "))
	hasIndexMetadata := column.Indexed || indexType != "" || strings.TrimSpace(column.IndexInclude) != "" || column.IndexBlockCapacity != 0
	if !column.Indexed {
		if hasIndexMetadata {
			return "", fmt.Errorf("questdb column %q has index properties but is not indexed", column.Name)
		}
		return "", nil
	}
	if column.IndexBlockCapacity < 0 {
		return "", fmt.Errorf("questdb column %q has invalid index block capacity %d", column.Name, column.IndexBlockCapacity)
	}
	switch indexType {
	case "", "BITMAP":
		indexType = "BITMAP"
	case "POSTING", "POSTING DELTA", "POSTING EF":
	default:
		return "", fmt.Errorf("questdb column %q has unsupported index type %q", column.Name, column.IndexType)
	}
	if indexType != "BITMAP" && column.IndexBlockCapacity != 0 {
		return "", fmt.Errorf("questdb posting index on column %q cannot specify index block capacity %d", column.Name, column.IndexBlockCapacity)
	}
	indexSQL := "INDEX"
	if indexType != "BITMAP" {
		indexSQL += " TYPE " + indexType
	} else if column.IndexBlockCapacity > 0 {
		indexSQL += fmt.Sprintf(" CAPACITY %d", column.IndexBlockCapacity)
	}
	if include := strings.TrimSpace(column.IndexInclude); include != "" {
		if indexType == "BITMAP" {
			return "", fmt.Errorf("bitmap index on column %q cannot specify included columns", column.Name)
		}
		quoted, err := quoteQuestRewriteIdentifierList(include, columns)
		if err != nil {
			return "", fmt.Errorf("index include list for column %q: %w", column.Name, err)
		}
		indexSQL += " INCLUDE (" + quoted + ")"
	}
	return indexSQL, nil
}

func questRewriteColumnCast(column questTableColumn, columns []questTableColumn) (string, error) {
	typeName := strings.ToUpper(strings.TrimSpace(column.Type))
	if typeName != "SYMBOL" {
		if column.Indexed || column.IndexBlockCapacity != 0 || column.SymbolCapacity != 0 || column.SymbolCached || strings.TrimSpace(column.IndexType) != "" || strings.TrimSpace(column.IndexInclude) != "" {
			return "", fmt.Errorf("questdb non-SYMBOL column %q has SYMBOL or index properties", column.Name)
		}
		return "", nil
	}
	if !questRewriteColumnPropertiesKnown(column) {
		return "", nil
	}
	if column.SymbolCapacity < 0 {
		return "", fmt.Errorf("questdb SYMBOL column %q has invalid symbol capacity %d", column.Name, column.SymbolCapacity)
	}
	typeDef := "SYMBOL"
	if questRewriteColumnPropertiesKnown(column) {
		if column.SymbolCapacity > 0 {
			typeDef += fmt.Sprintf(" CAPACITY %d", column.SymbolCapacity)
		}
		if column.SymbolCached {
			typeDef += " CACHE"
		} else {
			typeDef += " NOCACHE"
		}
	}
	indexSQL, err := questRewriteColumnIndex(column, columns)
	if err != nil {
		return "", err
	}
	if indexSQL != "" {
		typeDef += " " + indexSQL
	}
	return fmt.Sprintf("CAST(%s AS %s)", quoteIdent(column.Name), typeDef), nil
}

func questRewriteColumnSelect(columns []questTableColumn) (string, error) {
	selectCols := make([]string, 0, len(columns))
	for _, column := range columns {
		cast, err := questRewriteColumnCast(column, columns)
		if err != nil {
			return "", err
		}
		if cast == "" {
			selectCols = append(selectCols, quoteIdent(column.Name))
		} else {
			selectCols = append(selectCols, cast+" AS "+quoteIdent(column.Name))
		}
	}
	return strings.Join(selectCols, ", "), nil
}

func quoteQuestRewriteIdentifierList(value string, columns []questTableColumn) (string, error) {
	parts := strings.Split(value, ",")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return "", fmt.Errorf("questdb rewrite identifier list %q contains an empty item", value)
		}
		if !questRewriteHasColumn(columns, name) {
			return "", fmt.Errorf("questdb rewrite identifier %q is absent from the schema snapshot", name)
		}
		quoted = append(quoted, quoteIdent(name))
	}
	return strings.Join(quoted, ", "), nil
}

func validateQuestRewritePartitionBy(value string) (string, error) {
	partition := strings.ToUpper(strings.TrimSpace(value))
	switch partition {
	case "NONE", "HOUR", "DAY", "WEEK", "MONTH", "YEAR":
		return partition, nil
	default:
		return "", fmt.Errorf("unsupported QuestDB partition unit %q", value)
	}
}

func equalQuestCompactRewriteSnapshot(actual, expected *questCompactRewriteSnapshot) bool {
	if actual == nil || expected == nil {
		return actual == expected
	}
	return actual.RowCount == expected.RowCount &&
		equalQuestTableColumns(actual.Columns, expected.Columns) &&
		actual.Fingerprint == expected.Fingerprint
}

func verifyQuestCompactRewriteSnapshot(ctx context.Context, db questRewriteDB, table string, meta *TableCompactMeta, expected *questCompactRewriteSnapshot) error {
	var actual *questCompactRewriteSnapshot
	ok, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var err error
		actual, err = captureQuestCompactRewriteSnapshot(ctx, db, table, meta)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		return equalQuestCompactRewriteSnapshot(actual, expected), nil
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("compact rewrite snapshot mismatch before timeout: table=%s got=%v want=%v", table, actual, expected)
	}
	return nil
}

func buildQuestCompactRewriteSQL(tmpTable, sourceTable string, meta *TableCompactMeta, columns []questTableColumn) (string, error) {
	if meta == nil {
		return "", fmt.Errorf("compact metadata is nil for table %s", sourceTable)
	}
	if !questRewriteHasColumn(columns, "is_deleted") {
		return "", fmt.Errorf("questdb compact table %s has no is_deleted column", sourceTable)
	}
	if deletedType, _ := questRewriteColumnType(columns, "is_deleted"); !strings.EqualFold(deletedType, "BOOLEAN") {
		return "", fmt.Errorf("questdb compact table %s has non-boolean is_deleted column type %q", sourceTable, deletedType)
	}
	designated, upsertKeys, err := questRewriteSchema(columns)
	if err != nil {
		return "", fmt.Errorf("compact schema for %s: %w", sourceTable, err)
	}
	latestBy, err := quoteQuestRewriteIdentifierList(meta.LatestByKeys, columns)
	if err != nil {
		return "", fmt.Errorf("compact latest-by keys for %s: %w", sourceTable, err)
	}
	partitionBy, err := validateQuestRewritePartitionBy(meta.PartitionBy)
	if err != nil {
		return "", err
	}
	selectCols, err := questRewriteColumnSelect(columns)
	if err != nil {
		return "", fmt.Errorf("compact schema properties for %s: %w", sourceTable, err)
	}
	sqlText := fmt.Sprintf(`CREATE TABLE %s AS (
  SELECT %s
  FROM %s
  LATEST BY %s
  WHERE coalesce(%s, false) = false

)`,
		quoteIdent(tmpTable),
		selectCols,
		quoteIdent(sourceTable),
		latestBy,
		quoteIdent("is_deleted"),
	)
	sqlText += fmt.Sprintf(" TIMESTAMP(%s) PARTITION BY %s WAL", quoteIdent(designated), partitionBy)
	if len(upsertKeys) > 0 {
		sqlText += fmt.Sprintf("\nDEDUP UPSERT KEYS(%s)", strings.Join(upsertKeys, ", "))
	}
	return sqlText, nil
}

func queryQuestRewriteCounts(ctx context.Context, q *Queries, tableName, sidColumn, predicate string) (map[int32]int64, error) {
	sqlText := fmt.Sprintf("SELECT %s, count(*) FROM %s", quoteIdent(sidColumn), quoteIdent(tableName))
	if predicate != "" {
		sqlText += " WHERE " + predicate
	}
	sqlText += fmt.Sprintf(" GROUP BY %s ORDER BY %s", quoteIdent(sidColumn), quoteIdent(sidColumn))
	rows, err := q.db.Query(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[int32]int64)
	for rows.Next() {
		var sid int32
		var count int64
		if err := rows.Scan(&sid, &count); err != nil {
			return nil, err
		}
		counts[sid] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return counts, nil
}

func questRewriteColumnNames(columns []questTableColumn) []string {
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = quoteIdent(column.Name)
	}
	return names
}

func buildQuestRewriteSQLChecked(tmpTable, sourceTable, predicate, partitionBy, timeColumn string, columns []questTableColumn) (string, error) {
	designated, upsertKeys, err := questRewriteSchema(columns)
	if err != nil {
		return "", err
	}
	if timeColumn != "" && strings.TrimSpace(timeColumn) != designated {
		return "", fmt.Errorf("rewrite time column %q does not match designated snapshot column %q", timeColumn, designated)
	}
	partitionBy, err = validateQuestRewritePartitionBy(partitionBy)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(predicate) == "" {
		return "", errors.New("questdb rewrite predicate is empty")
	}
	selectCols, err := questRewriteColumnSelect(columns)
	if err != nil {
		return "", fmt.Errorf("questdb rewrite schema properties: %w", err)
	}
	sqlText := fmt.Sprintf(`CREATE TABLE %s AS (
  SELECT %s
  FROM %s
  WHERE %s
)`,
		quoteIdent(tmpTable),
		selectCols,
		quoteIdent(sourceTable),
		predicate,
	)
	sqlText += fmt.Sprintf(" TIMESTAMP(%s) PARTITION BY %s WAL", quoteIdent(designated), partitionBy)
	if len(upsertKeys) > 0 {
		sqlText += fmt.Sprintf("\nDEDUP UPSERT KEYS(%s)", strings.Join(upsertKeys, ", "))
	}
	return sqlText, nil
}

func verifyQuestRewriteTableSnapshot(ctx context.Context, q *Queries, tableName, sidColumn, predicate string, expected *questRewriteTableSnapshot) *errs.Error {
	var actual *questRewriteTableSnapshot
	ok, err := questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		var err error
		actual, err = captureQuestRewriteTableSnapshot(ctx, q, tableName, sidColumn, predicate)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		return equalQuestRewriteTableSnapshot(actual, expected), nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if !ok {
		return errs.NewMsg(core.ErrDbReadFail, "rewrite table snapshot mismatch before timeout: table=%s got=%v want=%v", tableName, actual, expected)
	}
	return nil
}

func equalQuestRewriteTableSnapshot(actual, expected *questRewriteTableSnapshot) bool {
	if actual == nil || expected == nil {
		return actual == expected
	}
	return actual.RowCount == expected.RowCount &&
		equalQuestTableColumns(actual.Columns, expected.Columns) &&
		reflect.DeepEqual(actual.Counts, expected.Counts) &&
		actual.Fingerprint == expected.Fingerprint
}

type questRewriteSwapDB interface {
	questRewriteDB
	questRewriteExecDB
}

type questRewriteDBTX struct{ questRewriteSwapDB }

func (questRewriteDBTX) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom is unavailable during QuestDB rewrite recovery")
}

func questRewriteTableExists(ctx context.Context, db questRewriteDB, table string) (bool, error) {
	var count int64
	if err := db.QueryRow(ctx, `SELECT count(*) FROM tables() WHERE table_name = $1`, table).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func verifyQuestRewriteIntentTable(ctx context.Context, db questRewriteSwapDB, intent *questRewriteSwapIntent, table string, sourceView bool) error {
	switch intent.Kind {
	case "table":
		if intent.TableSnapshot == nil || intent.SIDColumn == "" {
			return errors.New("QuestDB table rewrite intent has no snapshot")
		}
		predicate := ""
		if sourceView {
			predicate = intent.SourcePredicate
		}
		if err := verifyQuestRewriteTableSnapshot(ctx, New(questRewriteDBTX{db}), table, intent.SIDColumn, predicate, intent.TableSnapshot); err != nil {
			return errors.New(err.Short())
		}
		return nil
	case "compact":
		if intent.CompactMeta == nil || intent.CompactSnapshot == nil {
			return errors.New("QuestDB compact rewrite intent has no snapshot")
		}
		return verifyQuestCompactRewriteSnapshot(ctx, db, table, intent.CompactMeta, intent.CompactSnapshot)
	default:
		return fmt.Errorf("unknown QuestDB rewrite intent kind %q", intent.Kind)
	}
}

func renameQuestRewriteTable(ctx context.Context, db questRewriteExecDB, from, to string) error {
	_, err := db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(from), quoteIdent(to)))
	return err
}

func reconcileQuestRewriteSwap(ctx context.Context, db questRewriteSwapDB, table string) error {
	store := questRewriteIntentStoreFn()
	intent, err := store.Load(table)
	if err != nil || intent == nil {
		return err
	}
	recoveryCtx, cancel := questRewriteRecoveryContext(ctx)
	defer cancel()

	sourceExists, err := questRewriteTableExists(recoveryCtx, db, intent.Source)
	if err != nil {
		return fmt.Errorf("inspect rewrite source %s: %w", intent.Source, err)
	}
	tempExists, err := questRewriteTableExists(recoveryCtx, db, intent.Temp)
	if err != nil {
		return fmt.Errorf("inspect rewrite temp %s: %w", intent.Temp, err)
	}
	backupExists, err := questRewriteTableExists(recoveryCtx, db, intent.Backup)
	if err != nil {
		return fmt.Errorf("inspect rewrite backup %s: %w", intent.Backup, err)
	}

	verify := func(name string, sourceView bool) error {
		return verifyQuestRewriteIntentTable(recoveryCtx, db, intent, name, sourceView)
	}
	drop := func(name string) error { return dropQuestRewriteTable(recoveryCtx, db, name) }
	finish := func() error { return store.Remove(intent.Source) }

	switch {
	case sourceExists && tempExists && !backupExists:
		if err := verify(intent.Source, true); err != nil {
			return fmt.Errorf("reconcile rewrite source before temporary cleanup: %w", err)
		}
		if err := drop(intent.Temp); err != nil {
			return err
		}
		return finish()
	case !sourceExists && tempExists && backupExists:
		if err := verify(intent.Temp, false); err == nil {
			if err := renameQuestRewriteTable(recoveryCtx, db, intent.Temp, intent.Source); err != nil {
				return fmt.Errorf("resume rewrite activation: %w", err)
			}
			if err := verify(intent.Source, false); err != nil {
				return fmt.Errorf("verify resumed rewrite activation: %w", err)
			}
			if err := drop(intent.Backup); err != nil {
				return err
			}
			return finish()
		}
		if err := verify(intent.Backup, true); err != nil {
			return fmt.Errorf("neither rewrite temp nor backup is verified: %w", err)
		}
		if err := renameQuestRewriteTable(recoveryCtx, db, intent.Backup, intent.Source); err != nil {
			return fmt.Errorf("restore rewrite backup: %w", err)
		}
		if err := verify(intent.Source, true); err != nil {
			return fmt.Errorf("verify restored rewrite source: %w", err)
		}
		if err := drop(intent.Temp); err != nil {
			return err
		}
		return finish()
	case sourceExists && !tempExists && backupExists:
		if err := verify(intent.Source, false); err == nil {
			if err := drop(intent.Backup); err != nil {
				return err
			}
			return finish()
		}
		if err := verify(intent.Backup, true); err != nil {
			return fmt.Errorf("activated rewrite and backup are both unverified: %w", err)
		}
		if err := renameQuestRewriteTable(recoveryCtx, db, intent.Source, intent.Temp); err != nil {
			return fmt.Errorf("preserve invalid rewrite source: %w", err)
		}
		if err := renameQuestRewriteTable(recoveryCtx, db, intent.Backup, intent.Source); err != nil {
			return fmt.Errorf("restore rewrite backup: %w", err)
		}
		if err := verify(intent.Source, true); err != nil {
			return fmt.Errorf("verify restored rewrite source: %w", err)
		}
		if err := drop(intent.Temp); err != nil {
			return err
		}
		return finish()
	case !sourceExists && !tempExists && backupExists:
		if err := verify(intent.Backup, true); err != nil {
			return fmt.Errorf("verify lone rewrite backup: %w", err)
		}
		if err := renameQuestRewriteTable(recoveryCtx, db, intent.Backup, intent.Source); err != nil {
			return fmt.Errorf("restore lone rewrite backup: %w", err)
		}
		if err := verify(intent.Source, true); err != nil {
			return fmt.Errorf("verify restored lone rewrite source: %w", err)
		}
		return finish()
	case !sourceExists && tempExists && !backupExists:
		if err := verify(intent.Temp, false); err != nil {
			return fmt.Errorf("verify lone rewrite temp: %w", err)
		}
		if err := renameQuestRewriteTable(recoveryCtx, db, intent.Temp, intent.Source); err != nil {
			return fmt.Errorf("activate lone rewrite temp: %w", err)
		}
		if err := verify(intent.Source, false); err != nil {
			return fmt.Errorf("verify activated lone rewrite temp: %w", err)
		}
		return finish()
	case sourceExists && !tempExists && !backupExists:
		if targetErr := verify(intent.Source, false); targetErr != nil {
			if sourceErr := verify(intent.Source, true); sourceErr != nil {
				return fmt.Errorf("lone rewrite source is unverified: target=%v source=%v", targetErr, sourceErr)
			}
		}
		return finish()
	default:
		return fmt.Errorf("ambiguous QuestDB rewrite state for %s: source=%t temp=%t backup=%t", intent.Source, sourceExists, tempExists, backupExists)
	}
}

func saveQuestRewriteSwapIntent(intent *questRewriteSwapIntent) error {
	intent.Version = questRewriteSwapIntentVersion
	if err := questRewriteIntentStoreFn().Save(intent); err != nil {
		return fmt.Errorf("persist QuestDB rewrite swap intent: %w", err)
	}
	return nil
}

func clearQuestRewriteSwapIntent(table string) error {
	return questRewriteIntentStoreFn().Remove(table)
}

func replaceVerifiedQuestTable(ctx context.Context, q *Queries, tableName, tmpTable, backupTable, sidColumn, sourcePredicate string, expected *questRewriteTableSnapshot) *errs.Error {
	if err := reconcileQuestRewriteSwap(ctx, q.db, tableName); err != nil {
		return NewDbErr(core.ErrDbExecFail, fmt.Errorf("reconcile interrupted rewrite: %w", err))
	}
	if sourceErr := verifyQuestRewriteTableSnapshot(ctx, q, tableName, sidColumn, sourcePredicate, expected); sourceErr != nil {
		cause := fmt.Errorf("verify rewrite source snapshot before rename: %s", sourceErr.Short())
		return NewDbErr(core.ErrDbReadFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTable, cause))
	}
	intent := &questRewriteSwapIntent{
		Kind: "table", Source: tableName, Temp: tmpTable, Backup: backupTable,
		SIDColumn: sidColumn, SourcePredicate: sourcePredicate, TableSnapshot: expected,
	}
	if err := saveQuestRewriteSwapIntent(intent); err != nil {
		return NewDbErr(core.ErrDbExecFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTable, err))
	}
	if _, err := q.db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(tableName), quoteIdent(backupTable))); err != nil {
		cause := fmt.Errorf("rename source to backup: %w", err)
		cause = cleanupQuestRewriteFailure(ctx, q.db, tmpTable, cause)
		if clearErr := clearQuestRewriteSwapIntent(tableName); clearErr != nil {
			cause = fmt.Errorf("%w; clear swap intent: %v", cause, clearErr)
		}
		return NewDbErr(core.ErrDbExecFail, cause)
	}
	if _, err := q.db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(tmpTable), quoteIdent(tableName))); err != nil {
		recoveryCtx, cancelRecovery := questRewriteRecoveryContext(ctx)
		_, restoreErr := q.db.Exec(recoveryCtx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(backupTable), quoteIdent(tableName)))
		cancelRecovery()
		if restoreErr != nil {
			return NewDbErr(core.ErrDbExecFail, fmt.Errorf("activate rewritten table: %w; restore backup failed: %w; temporary=%s backup=%s", err, restoreErr, tmpTable, backupTable))
		}
		if cleanupErr := dropQuestRewriteTable(ctx, q.db, tmpTable); cleanupErr != nil {
			return NewDbErr(core.ErrDbExecFail, fmt.Errorf("activate rewritten table: %w; source restored; temporary cleanup failed: %v; temporary=%s", err, cleanupErr, tmpTable))
		}
		if clearErr := clearQuestRewriteSwapIntent(tableName); clearErr != nil {
			return NewDbErr(core.ErrDbExecFail, fmt.Errorf("activate rewritten table: %w; source restored; clear swap intent: %v", err, clearErr))
		}
		return NewDbErr(core.ErrDbExecFail, fmt.Errorf("activate rewritten table: %w; source restored; temporary=%s", err, tmpTable))
	}
	if verifyErr := verifyQuestRewriteTableSnapshot(ctx, q, tableName, sidColumn, sourcePredicate, expected); verifyErr != nil {
		recoveryCtx, cancelRecovery := questRewriteRecoveryContext(ctx)
		_, moveErr := q.db.Exec(recoveryCtx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(tableName), quoteIdent(tmpTable)))
		_, restoreErr := q.db.Exec(recoveryCtx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(backupTable), quoteIdent(tableName)))
		cancelRecovery()
		if moveErr != nil || restoreErr != nil {
			return errs.NewMsg(core.ErrDbExecFail, "verify activated rewritten table: %v; restore failed: move=%v restore=%v; temporary=%s backup=%s", verifyErr, moveErr, restoreErr, tmpTable, backupTable)
		}
		if cleanupErr := dropQuestRewriteTable(ctx, q.db, tmpTable); cleanupErr != nil {
			return NewDbErr(core.ErrDbExecFail, fmt.Errorf("verify activated rewritten table: %v; source restored; temporary cleanup failed: %v; temporary=%s", verifyErr, cleanupErr, tmpTable))
		}
		if clearErr := clearQuestRewriteSwapIntent(tableName); clearErr != nil {
			return NewDbErr(core.ErrDbExecFail, fmt.Errorf("verify activated rewritten table: %v; source restored; clear swap intent: %v", verifyErr, clearErr))
		}
		return errs.NewMsg(core.ErrDbReadFail, "verify activated rewritten table: %v; source restored; temporary=%s", verifyErr, tmpTable)
	}
	cleanupCtx, cancelCleanup := questRewriteRecoveryContext(ctx)
	_, cleanupErr := q.db.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE %s", quoteIdent(backupTable)))
	cancelCleanup()
	if cleanupErr != nil {
		return NewDbErr(core.ErrDbExecFail, fmt.Errorf("drop verified rewrite backup %s: %w", backupTable, cleanupErr))
	}
	if err := clearQuestRewriteSwapIntent(tableName); err != nil {
		return NewDbErr(core.ErrDbExecFail, fmt.Errorf("clear completed rewrite swap intent: %w", err))
	}
	return nil
}
