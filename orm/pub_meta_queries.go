package orm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// insKlineLocks is an in-process lock map for active kline insert jobs.
// Key format: "<sid>/<timeframe>"  Value: ts assigned during AddInsKline.
// This avoids false "locked" detection caused by QuestDB WAL async commit lag:
// DelInsKline writes is_deleted=true to QuestDB but the WAL row may not be
// visible to LATEST BY queries immediately, making a subsequent GetInsKline
// see the old lock row and incorrectly block the next insert.
var (
	insKlineLocks   = make(map[string]time.Time)
	insKlineLocksmu sync.Mutex
)

func insKlineLockKey(sid int32, timeframe string) string {
	return fmt.Sprintf("%d/%s", sid, timeframe)
}

func releaseKlineInsertOwnership(sid int32, timeframe string, ts time.Time) error {
	return (&Queries{}).releaseInsertOwnership(sid, timeframe, ts)
}

func (q *Queries) insertLockKey(sid int32, timeframe string) string {
	key := insKlineLockKey(sid, timeframe)
	if q.storage != nil {
		key = q.storage.Identity() + "\x00" + key
	}
	return key
}

func (q *Queries) releaseInsertOwnership(sid int32, timeframe string, ts time.Time) error {
	err := releaseKlineInsertFileLock(q.insertLockRoot(), sid, timeframe, ts)
	key := q.insertLockKey(sid, timeframe)
	insKlineLocksmu.Lock()
	delete(insKlineLocks, key)
	insKlineLocksmu.Unlock()
	return err
}

type AddAdjFactorsParams struct {
	Sid     int32   `json:"sid"`
	SubID   int32   `json:"sub_id"`
	StartMs int64   `json:"start_ms"`
	Factor  float64 `json:"factor"`
}

type AddCalendarsParams struct {
	Name    string `json:"name"`
	StartMs int64  `json:"start_ms"`
	StopMs  int64  `json:"stop_ms"`
}

type AddInsKlineParams struct {
	Sid       int32  `json:"sid"`
	Timeframe string `json:"timeframe"`
	StartMs   int64  `json:"start_ms"`
	StopMs    int64  `json:"stop_ms"`
}

func (q *Queries) AddCalendars(ctx context.Context, arg []AddCalendarsParams) (int64, error) {
	if len(arg) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		return q.addCalendarsPg(ctx, arg)
	}
	unlock := q.LockCompactTableRead("calendars_q")
	defer unlock()
	now := time.Now().UTC()
	const cols = 4
	args := make([]any, 0, len(arg)*cols)
	for i, c := range arg {
		args = append(args, now.Add(time.Duration(i)*time.Microsecond), c.Name, c.StartMs, c.StopMs)
	}
	sql := "INSERT INTO calendars_q (ts,market,start_ms,stop_ms,is_deleted) VALUES " + buildBatchValues(len(arg), cols, ",false")
	if _, err := q.db.Exec(ctx, sql, args...); err != nil {
		return 0, err
	}
	return int64(len(arg)), nil
}

func (q *Queries) AddAdjFactors(ctx context.Context, arg []AddAdjFactorsParams) (int64, error) {
	if len(arg) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		return q.addAdjFactorsPg(ctx, arg)
	}
	unlock := q.LockCompactTableRead("adj_factors_q")
	defer unlock()
	now := time.Now().UTC()
	const cols = 5
	args := make([]any, 0, len(arg)*cols)
	for i, f := range arg {
		args = append(args, now.Add(time.Duration(i)*time.Microsecond), f.Sid, f.SubID, f.StartMs, f.Factor)
	}
	sql := "INSERT INTO adj_factors_q (ts,sid,sub_id,start_ms,factor,is_deleted) VALUES " + buildBatchValues(len(arg), cols, ",false")
	if _, err := q.db.Exec(ctx, sql, args...); err != nil {
		return 0, err
	}
	return int64(len(arg)), nil
}

func (q *Queries) GetAdjFactors(ctx context.Context, sid int32) ([]*AdjFactor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		return q.getAdjFactorsPg(ctx, sid)
	}
	unlock := q.LockCompactTableRead("adj_factors_q")
	defer unlock()
	return q.getAdjFactorsQuest(ctx, sid)
}

func (q *Queries) getAdjFactorsQuest(ctx context.Context, sid int32) ([]*AdjFactor, error) {
	rows, err := q.db.Query(ctx, `SELECT sid, sub_id, start_ms, factor
FROM adj_factors_q
LATEST BY sid, sub_id, start_ms
WHERE sid = $1 AND coalesce(is_deleted, false) = false
ORDER BY start_ms`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AdjFactor
	for rows.Next() {
		var i AdjFactor
		if err := rows.Scan(&i.Sid, &i.SubID, &i.StartMs, &i.Factor); err != nil {
			return nil, err
		}
		out = append(out, &i)
	}
	return out, rows.Err()
}

func (q *Queries) DelAdjFactors(ctx context.Context, sid int32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		return q.delAdjFactorsPg(ctx, sid)
	}
	unlock := q.LockCompactTableRead("adj_factors_q")
	defer unlock()
	factors, err := q.getAdjFactorsQuest(ctx, sid)
	if err != nil {
		return err
	}
	if len(factors) == 0 {
		return nil
	}
	now := time.Now().UTC()
	if err = batchInsertAdjFactorsDeleted(ctx, q, factors, now); err != nil {
		return err
	}
	q.MarkTableForCompact("adj_factors_q", len(factors))
	return nil
}

// batchInsertAdjFactorsDeleted marks a list of adj_factor rows as deleted in one multi-row INSERT.
func batchInsertAdjFactorsDeleted(ctx context.Context, q *Queries, factors []*AdjFactor, now time.Time) error {
	const cols = 5 // ts, sid, sub_id, start_ms, factor
	args := make([]any, 0, len(factors)*cols)
	for i, f := range factors {
		args = append(args, now.Add(time.Duration(i)*time.Microsecond), f.Sid, f.SubID, f.StartMs, f.Factor)
	}
	sql := "INSERT INTO adj_factors_q (ts,sid,sub_id,start_ms,factor,is_deleted) VALUES " + buildBatchValues(len(factors), cols, ",true")
	_, err := q.db.Exec(ctx, sql, args...)
	return err
}

func (q *Queries) GetInsKline(ctx context.Context, sid int32, timeframe string) (*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		return q.getInsKlinePg(ctx, sid, timeframe)
	}
	unlock := q.LockCompactTableRead("ins_kline_q")
	defer unlock()
	load := func() (*InsKline, error) {
		row := q.db.QueryRow(ctx, `SELECT sid, timeframe, ts, start_ms, stop_ms
FROM ins_kline_q
LATEST BY sid, timeframe
WHERE sid = $1 AND timeframe = $2 AND coalesce(is_deleted, false) = false`, sid, timeframe)
		var i InsKline
		if err := row.Scan(&i.Sid, &i.Timeframe, &i.Ts, &i.StartMs, &i.StopMs); err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		return &i, nil
	}
	item, err := load()
	if err != nil {
		repaired, repairErr := tryRepairQuestDBMissingPartition(ctx, q.db, err, "GetInsKline")
		if repairErr != nil {
			return nil, repairErr
		}
		if repaired {
			return load()
		}
	}
	return item, err
}

func (q *Queries) GetAllInsKlines(ctx context.Context) ([]*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		return q.getAllInsKlinesPg(ctx)
	}
	unlock := q.LockCompactTableRead("ins_kline_q")
	defer unlock()
	load := func() ([]*InsKline, error) {
		rows, err := q.db.Query(ctx, `SELECT sid, timeframe, ts, start_ms, stop_ms
FROM ins_kline_q
LATEST BY sid, timeframe
WHERE coalesce(is_deleted, false) = false`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []*InsKline
		for rows.Next() {
			var i InsKline
			if err := rows.Scan(&i.Sid, &i.Timeframe, &i.Ts, &i.StartMs, &i.StopMs); err != nil {
				return nil, err
			}
			out = append(out, &i)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	}
	items, err := load()
	if err != nil {
		repaired, repairErr := tryRepairQuestDBMissingPartition(ctx, q.db, err, "GetAllInsKlines")
		if repairErr != nil {
			return nil, repairErr
		}
		if repaired {
			return load()
		}
	}
	return items, err
}

func (q *Queries) DelInsKline(ctx context.Context, sid int32, timeframe string, ts time.Time) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		err := q.delInsKlinePg(ctx, sid, timeframe)
		releaseErr := q.releaseInsertOwnership(sid, timeframe, ts)
		if err != nil {
			return err
		}
		return releaseErr
	}
	unlock := q.LockCompactTableRead("ins_kline_q")
	defer unlock()
	write := func() error {
		_, err := q.db.Exec(ctx, `INSERT INTO ins_kline_q (sid, timeframe, ts, start_ms, stop_ms, is_deleted)
	VALUES ($1, $2, $3, 0, 0, true)`, sid, timeframe, ts)
		return err
	}
	err := write()
	if err != nil {
		repaired, repairErr := tryRepairQuestDBMissingPartition(ctx, q.db, err, "DelInsKline")
		if repairErr != nil {
			return repairErr
		}
		if repaired {
			err = write()
		}
	}
	// Keep the table access guard through ownership release so a compact cannot
	// replace the lease table in the middle of this logical operation.
	releaseErr := q.releaseInsertOwnership(sid, timeframe, ts)
	if err != nil {
		return err
	}
	q.MarkTableForCompact("ins_kline_q", 1)
	return releaseErr
}

func (q *Queries) AddInsKline(ctx context.Context, arg AddInsKlineParams) (time.Time, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := q.insertLockKey(arg.Sid, arg.Timeframe)
	insKlineLocksmu.Lock()
	if _, locked := insKlineLocks[key]; locked {
		insKlineLocksmu.Unlock()
		return time.Time{}, nil
	}
	ts := time.Now().UTC()
	if q.isQuestDB() {
		ts = normalizeQuestTimestamp(ts)
	}
	insKlineLocks[key] = ts
	insKlineLocksmu.Unlock()
	claimed, err := acquireKlineInsertFileLock(q.insertLockRoot(), arg.Sid, arg.Timeframe, ts)
	if err != nil || !claimed {
		insKlineLocksmu.Lock()
		delete(insKlineLocks, key)
		insKlineLocksmu.Unlock()
		return time.Time{}, err
	}
	if !q.isQuestDB() {
		claimed, err = q.tryAddInsKlinePg(ctx, arg)
		if err != nil || !claimed {
			_ = q.releaseInsertOwnership(arg.Sid, arg.Timeframe, ts)
			return time.Time{}, err
		}
		return ts, nil
	}
	unlock := q.LockCompactTableRead("ins_kline_q")
	defer unlock()

	write := func() error {
		_, err := q.db.Exec(ctx, `INSERT INTO ins_kline_q (sid, timeframe, ts, start_ms, stop_ms, is_deleted)
VALUES ($1, $2, $3, $4, $5, false)`, arg.Sid, arg.Timeframe, ts, arg.StartMs, arg.StopMs)
		return err
	}
	err = write()
	if err != nil {
		repaired, repairErr := tryRepairQuestDBMissingPartition(ctx, q.db, err, "AddInsKline")
		if repairErr != nil {
			err = repairErr
		} else if repaired {
			err = write()
		}
	}
	if err != nil {
		_ = q.releaseInsertOwnership(arg.Sid, arg.Timeframe, ts)
		return time.Time{}, err
	}
	return ts, nil
}

type AddSymbolsParams struct {
	Exchange string `json:"exchange"`
	ExgReal  string `json:"exg_real"`
	Market   string `json:"market"`
	Symbol   string `json:"symbol"`
	Combined bool   `json:"combined"`
	ListMs   int64  `json:"list_ms"`
	DelistMs int64  `json:"delist_ms"`
	AggRules string `json:"agg_rules"`
}

type SetListMSParams struct {
	ID       int32 `json:"id"`
	ListMs   int64 `json:"list_ms"`
	DelistMs int64 `json:"delist_ms"`
}

type SetAggRulesParams struct {
	ID       int32  `json:"id"`
	AggRules string `json:"agg_rules"`
}

func (q *Queries) ListExchanges(ctx context.Context) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		return q.listExchangesPg(ctx)
	}
	unlock := q.LockCompactTableRead("exsymbol_q")
	defer unlock()
	rows, err := q.db.Query(ctx, `SELECT DISTINCT exchange
FROM exsymbol_q
LATEST BY sid
WHERE coalesce(is_deleted, false) = false
ORDER BY exchange`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (q *Queries) ListSymbols(ctx context.Context, exchange string) ([]*ExSymbol, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		return q.listSymbolsPg(ctx, exchange)
	}
	unlock := q.LockCompactTableRead("exsymbol_q")
	defer unlock()
	rows, err := q.db.Query(ctx, `SELECT sid, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, coalesce(agg_rules, '')
FROM exsymbol_q
LATEST BY sid
WHERE exchange = $1 AND coalesce(is_deleted, false) = false
ORDER BY sid`, exchange)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ExSymbol
	for rows.Next() {
		var i ExSymbol
		if err := rows.Scan(&i.ID, &i.Exchange, &i.ExgReal, &i.Market, &i.Symbol, &i.Combined, &i.ListMs, &i.DelistMs, &i.AggRules); err != nil {
			return nil, err
		}
		out = append(out, &i)
	}
	return out, rows.Err()
}

func (q *Queries) AddSymbols(ctx context.Context, arg []AddSymbolsParams) (int64, error) {
	state, err := q.requireSymbolState()
	if err != nil {
		return 0, err
	}
	return q.addSymbols(ctx, state, q.usesLegacySymbolCatalog(), arg)
}

func (q *SymbolQueries) AddSymbols(ctx context.Context, arg []AddSymbolsParams) (int64, error) {
	if q == nil {
		return 0, errs.NewMsg(core.ErrBadConfig, "symbol query is required")
	}
	state := q.symbolState()
	if state == nil {
		return 0, fmt.Errorf("explicit storage requires an explicit symbol state")
	}
	return q.Queries.addSymbols(ctx, state, q.symbols == nil, arg)
}

func (q *Queries) addSymbols(ctx context.Context, state *SymbolState, legacy bool, arg []AddSymbolsParams) (int64, error) {
	state = symbolStateOrDefault(state)
	allocator := state.sidAllocator()
	unlockEnsure := allocator.lockEnsure()
	defer unlockEnsure()
	return q.addSymbolsLocked(ctx, state, legacy, arg)
}

func (q *Queries) addSymbolsLocked(ctx context.Context, state *SymbolState, legacy bool, arg []AddSymbolsParams) (result int64, retErr error) {
	allocator := state.sidAllocator()
	if state.identitySet {
		for i, item := range arg {
			if !state.acceptsIdentity(item.Exchange, item.Market) {
				return 0, fmt.Errorf("add symbol %d identity %s:%s does not match symbol state identity %s:%s",
					i, item.Exchange, item.Market, state.identityExchange, state.identityMarket)
			}
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if q.isQuestDB() {
		registry, registryErr := allocator.configuredSIDRegistry()
		if registryErr != nil {
			return 0, registryErr
		}
		if registry != nil {
			return q.addSymbolsQuestDBWithRegistry(ctx, state, legacy, arg, registry)
		}
	}
	if len(arg) == 0 && !q.isQuestDB() {
		return 0, nil
	}
	var err error
	var recoveryRoot string
	if q.isQuestDB() {
		recoveryRoot, err = exSymbolRecoveryRoot(state, legacy)
		if err != nil {
			return 0, err
		}
	}
	if q.isQuestDB() {
		releaseSIDLease, leaseErr := acquireLocalSIDReservationLease(ctx, allocator)
		if leaseErr != nil {
			return 0, leaseErr
		}
		defer func() {
			if releaseErr := releaseSIDLease(); releaseErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("release exchange symbol SID lease: %w", releaseErr))
			}
		}()
	}
	if !q.isQuestDB() {
		if err := state.reserveCanonicalSIDs(allocator); err != nil {
			return 0, err
		}
		newArg, pendingRows, err := reuseReservedAddSymbols(state, allocator, arg)
		if err != nil {
			return 0, err
		}
		if len(pendingRows) > 0 {
			return 0, fmt.Errorf("pending exchange symbol SID reservations require QuestDB visibility")
		}
		if len(newArg) == 0 {
			return int64(len(arg)), nil
		}
		result, err := q.addSymbolsPg(ctx, state, newArg)
		if err != nil {
			return result, err
		}
		return int64(len(arg)), nil
	}
	unlock, lockErr := lockCompactTableReadExclusiveProcessAtRoot(ctx, "exsymbol_q", compactProcessLockRootForAllocator(allocator))
	if lockErr != nil {
		return 0, lockErr
	}
	defer func() {
		if releaseErr := unlock(); releaseErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("release exsymbol table lease: %w", releaseErr))
		}
	}()
	if err := reconcileSharedSIDReservations(ctx, q, state, allocator); err != nil {
		return 0, err
	}
	for _, root := range recoveryRootsForAllocator(allocator, recoveryRoot) {
		if err := reconcilePendingExSymbolMarkersLocked(ctx, q, state, root); err != nil {
			return 0, err
		}
	}
	if err := state.reserveCanonicalSIDs(allocator); err != nil {
		return 0, err
	}
	newArg, pendingRows, err := reuseReservedAddSymbols(state, allocator, arg)
	if err != nil {
		return 0, err
	}
	if len(pendingRows) > 0 {
		sharedMarkerPath, markerPath, markerErr := ensurePendingExSymbolMarkers(
			allocator, recoveryRoot, pendingRows)
		if markerErr != nil {
			return 0, markerErr
		}
		visible, visibleErr := questExsymbolsVisible(ctx, q, pendingRows)
		if visibleErr != nil {
			return int64(len(arg)), visibleErr
		}
		if !visible {
			ids := make([]int32, len(pendingRows))
			for i, row := range pendingRows {
				ids[i] = row.ID
			}
			log.Warn("questdb pending exsymbol rows still not visible after timeout; retain recovery marker",
				zap.Int32s("sids", ids), zap.String("marker", markerPath), zap.String("shared_marker", sharedMarkerPath))
			return int64(len(arg)), errs.NewMsg(core.ErrTimeout,
				"questdb exsymbol rows not visible before timeout: sids=%v", ids)
		}
		if err := reconcileSharedSIDReservations(ctx, q, state, allocator); err != nil {
			return int64(len(arg)), err
		}
		for _, root := range recoveryRootsForAllocator(allocator, recoveryRoot) {
			if err := reconcilePendingExSymbolMarkersLocked(ctx, q, state, root); err != nil {
				return int64(len(arg)), err
			}
		}
		for _, row := range pendingRows {
			key := exSymbolKey(row.Exchange, row.Market, row.Symbol)
			if allocator.pendingSID(key) != 0 {
				return int64(len(arg)), fmt.Errorf("visible exsymbol SID %d remained pending for logical symbol %s", row.ID, key)
			}
		}
	}
	newArg, err = reuseQuestDBCanonicalSymbols(ctx, q, state, newArg)
	if err != nil {
		return 0, err
	}
	if len(newArg) == 0 {
		return int64(len(arg)), nil
	}
	dbMax, err := queryMaxSidFromQDB(ctx, q.db)
	if err != nil {
		return 0, err
	}
	prepareSymbolSIDAllocation(allocator, state, dbMax)
	now := time.Now().UTC()
	ids := make([]int32, len(newArg))
	for i := range newArg {
		key := exSymbolKey(newArg[i].Exchange, newArg[i].Market, newArg[i].Symbol)
		if pendingID := allocator.pendingSID(key); pendingID > 0 {
			ids[i] = pendingID
		} else {
			ids[i] = nextSymbolSID(allocator, state)
		}
	}
	pendingRows = pendingExSymbolRows(newArg, ids)
	for i := range pendingRows {
		pendingRows[i].WriteTS = now.Add(time.Duration(i) * time.Microsecond)
	}
	sharedMarkerPath, markerPath, err := ensurePendingExSymbolMarkers(allocator, recoveryRoot, pendingRows)
	if err != nil {
		return 0, err
	}
	for i, s := range newArg {
		sid := ids[i]
		ts := pendingRows[i].WriteTS
		_, err := q.db.Exec(ctx, `INSERT INTO exsymbol_q (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules, is_deleted)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)`, sid, ts, s.Exchange, s.ExgReal, s.Market, s.Symbol, s.Combined, s.ListMs, s.DelistMs, s.AggRules)
		if err != nil {
			return int64(i), err
		}
		if err := markPendingExSymbolRowInserted(markerPath, pendingRows[i]); err != nil {
			return int64(i + 1), err
		}
		if err := markPendingExSymbolRowInserted(sharedMarkerPath, pendingRows[i]); err != nil {
			return int64(i + 1), err
		}
	}
	visible, err := questExsymbolsVisible(ctx, q, pendingRows)
	if err != nil {
		return int64(len(newArg)), err
	}
	if !visible {
		log.Warn("questdb exsymbol rows still not visible after timeout; retain recovery marker",
			zap.Int32s("sids", ids), zap.String("marker", markerPath), zap.String("shared_marker", sharedMarkerPath))
		return int64(len(arg)), errs.NewMsg(core.ErrTimeout,
			"questdb exsymbol rows not visible before timeout: sids=%v", ids)
	}
	if err := cacheConfirmedQuestExSymbols(ctx, q, state, allocator, pendingRows); err != nil {
		return int64(len(arg)), err
	}
	if err := removePendingExSymbolMarkerRows(markerPath, pendingRows); err != nil {
		// Keep the marker when cleanup fails. A later recovery pass can safely
		// re-check the rows and remove it.
		return int64(len(arg)), err
	}
	if err := removeSharedSIDReservations(allocator, pendingRows); err != nil {
		// The QuestDB row is confirmed, but retain the shared ledger when its
		// durable cleanup fails so another writer can reconcile it safely.
		return int64(len(arg)), err
	}
	return int64(len(arg)), nil
}

// addSymbolsQuestDBWithRegistry keeps QuestDB as the physical catalog while a
// shared PostgreSQL registry owns logical identity. The registry operation is
// atomic; the QuestDB write remains a recoverable second phase because WAL has
// asynchronous visibility and no cross-database transaction exists.
func (q *Queries) addSymbolsQuestDBWithRegistry(ctx context.Context, state *SymbolState, legacy bool,
	arg []AddSymbolsParams, registry *SymbolSIDRegistry) (result int64, retErr error) {
	recoveryRoot, err := exSymbolRecoveryRoot(state, legacy)
	if err != nil {
		return 0, err
	}
	allocator := state.sidAllocator()
	unlock, err := lockCompactTableReadExclusiveProcessAtRoot(ctx, "exsymbol_q", compactProcessLockRootForAllocator(allocator))
	if err != nil {
		return 0, err
	}
	defer func() {
		if releaseErr := unlock(); releaseErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("release exsymbol table lease: %w", releaseErr))
		}
	}()
	if err := reconcileSharedSIDReservations(ctx, q, state, allocator); err != nil {
		return 0, err
	}
	for _, root := range recoveryRootsForAllocator(allocator, recoveryRoot) {
		if err := reconcilePendingExSymbolMarkersLocked(ctx, q, state, root); err != nil {
			return 0, err
		}
	}
	if err := state.reserveCanonicalSIDs(allocator); err != nil {
		return 0, err
	}
	newArg, pendingRows, err := reuseReservedAddSymbols(state, allocator, arg)
	if err != nil {
		return 0, err
	}
	if len(pendingRows) > 0 {
		if result, retErr = waitForPendingQuestSymbols(ctx, q, state, allocator, recoveryRoot, arg, pendingRows); retErr != nil {
			return result, retErr
		}
	}

	// Import pre-existing physical rows before allocating any new SID. This
	// makes first deployment against an existing exsymbol_q catalog preserve
	// every already published physical identity.
	missing := make([]AddSymbolsParams, 0, len(newArg))
	for _, requested := range newArg {
		item, lookupErr := queryQuestDBCanonicalSymbol(ctx, q, requested)
		if lookupErr != nil {
			return 0, lookupErr
		}
		if item == nil {
			missing = append(missing, requested)
			continue
		}
		if err := state.validateReservedSymbol(item); err != nil {
			return 0, err
		}
		if _, err := registry.Adopt(ctx, item); err != nil {
			return 0, err
		}
		if err := state.cacheExSymbolChecked(item); err != nil {
			return 0, err
		}
	}
	if len(missing) == 0 {
		return int64(len(arg)), nil
	}
	physicalMax, err := queryMaxSidFromQDB(ctx, q.db)
	if err != nil {
		return 0, err
	}
	if err := registry.EnsureSIDFloor(ctx, physicalMax); err != nil {
		return 0, err
	}
	reservations, err := registry.Reserve(ctx, missing)
	if err != nil {
		return 0, err
	}
	if len(reservations) != len(missing) {
		return 0, fmt.Errorf("SID registry returned %d rows for %d symbols", len(reservations), len(missing))
	}
	now := time.Now().UTC()
	rows := make([]exSymbolRecoveryRow, len(reservations))
	for i, reservation := range reservations {
		item := &reservation.ExSymbol
		if err := state.validateReservedSymbol(item); err != nil {
			return 0, err
		}
		writeTS := reservation.WriteTS
		if writeTS.IsZero() {
			// Test doubles may omit the timestamp. Production registries always
			// return the database-owned stable value.
			writeTS = now.Add(time.Duration(i) * time.Microsecond)
		}
		rows[i] = exSymbolRecoveryRow{
			ID: item.ID, Exchange: item.Exchange, ExgReal: item.ExgReal, Market: item.Market,
			Symbol: item.Symbol, Combined: item.Combined, ListMs: item.ListMs,
			DelistMs: item.DelistMs, AggRules: item.AggRules, WriteTS: writeTS,
		}
	}
	sharedMarkerPath, markerPath, err := ensurePendingExSymbolMarkers(allocator, recoveryRoot, rows)
	if err != nil {
		return 0, err
	}
	for i, reservation := range reservations {
		item := reservation.ExSymbol
		if _, err := q.db.Exec(ctx, `INSERT INTO exsymbol_q
	  (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)`,
			item.ID, rows[i].WriteTS, item.Exchange, item.ExgReal, item.Market, item.Symbol,
			item.Combined, item.ListMs, item.DelistMs, item.AggRules); err != nil {
			return int64(i), err
		}
		if err := markPendingExSymbolRowInserted(markerPath, rows[i]); err != nil {
			return int64(i + 1), err
		}
		if err := markPendingExSymbolRowInserted(sharedMarkerPath, rows[i]); err != nil {
			return int64(i + 1), err
		}
	}
	visible, err := questExsymbolsVisible(ctx, q, rows)
	if err != nil {
		return int64(len(arg)), err
	}
	if !visible {
		ids := make([]int32, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
		log.Warn("questdb registry exsymbol rows still not visible after timeout; retain recovery marker",
			zap.Int32s("sids", ids), zap.String("marker", markerPath), zap.String("shared_marker", sharedMarkerPath))
		return int64(len(arg)), errs.NewMsg(core.ErrTimeout,
			"questdb registry exsymbol rows not visible before timeout: sids=%v", ids)
	}
	if err := cacheConfirmedQuestExSymbols(ctx, q, state, allocator, rows); err != nil {
		return int64(len(arg)), err
	}
	if err := removePendingExSymbolMarkerRows(markerPath, rows); err != nil {
		return int64(len(arg)), err
	}
	if err := removeSharedSIDReservations(allocator, rows); err != nil {
		return int64(len(arg)), err
	}
	return int64(len(arg)), nil
}

func waitForPendingQuestSymbols(ctx context.Context, q *Queries, state *SymbolState, allocator *SIDAllocator,
	recoveryRoot string, arg []AddSymbolsParams, pendingRows []exSymbolRecoveryRow) (int64, error) {
	sharedMarkerPath, markerPath, err := ensurePendingExSymbolMarkers(allocator, recoveryRoot, pendingRows)
	if err != nil {
		return 0, err
	}
	visible, err := questExsymbolsVisible(ctx, q, pendingRows)
	if err != nil {
		return int64(len(arg)), err
	}
	if !visible {
		ids := make([]int32, 0, len(pendingRows))
		for _, row := range pendingRows {
			ids = append(ids, row.ID)
		}
		log.Warn("questdb registry pending exsymbol rows still not visible after timeout; retain recovery marker",
			zap.Int32s("sids", ids), zap.String("marker", markerPath), zap.String("shared_marker", sharedMarkerPath))
		return int64(len(arg)), errs.NewMsg(core.ErrTimeout,
			"questdb registry pending exsymbol rows not visible before timeout: sids=%v", ids)
	}
	if err := cacheConfirmedQuestExSymbols(ctx, q, state, allocator, pendingRows); err != nil {
		return int64(len(arg)), err
	}
	if err := removePendingExSymbolMarkerRows(markerPath, pendingRows); err != nil {
		return int64(len(arg)), err
	}
	if err := removeSharedSIDReservations(allocator, pendingRows); err != nil {
		return int64(len(arg)), err
	}
	return int64(len(arg)), nil
}

func reuseQuestDBCanonicalSymbols(ctx context.Context, q *Queries, state *SymbolState, arg []AddSymbolsParams) ([]AddSymbolsParams, error) {
	remaining := make([]AddSymbolsParams, 0, len(arg))
	for _, requested := range arg {
		key := exSymbolKey(requested.Exchange, requested.Market, requested.Symbol)
		item, err := queryQuestDBCanonicalSymbol(ctx, q, requested)
		if err != nil {
			return nil, fmt.Errorf("lookup exsymbol %s: %w", key, err)
		}
		if item == nil {
			remaining = append(remaining, requested)
			continue
		}
		if exSymbolKey(item.Exchange, item.Market, item.Symbol) != key {
			remaining = append(remaining, requested)
			continue
		}
		if err := state.cacheExSymbolChecked(item); err != nil {
			return nil, fmt.Errorf("cache canonical exsymbol %s sid %d: %w", key, item.ID, err)
		}
	}
	return remaining, nil
}

type questCanonicalExSymbolReader interface {
	lookupQuestCanonicalExSymbol(context.Context, string, string, string) (*ExSymbol, error)
}

func queryQuestDBCanonicalSymbol(ctx context.Context, q *Queries, requested AddSymbolsParams) (*ExSymbol, error) {
	if reader, ok := q.db.(questCanonicalExSymbolReader); ok {
		return reader.lookupQuestCanonicalExSymbol(ctx, requested.Exchange, requested.Market, requested.Symbol)
	}
	row := q.db.QueryRow(ctx, `SELECT sid, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, coalesce(agg_rules, '')
FROM exsymbol_q
LATEST BY sid
WHERE exchange = $1 AND market = $2 AND symbol = $3 AND coalesce(is_deleted, false) = false
ORDER BY sid
LIMIT 1`,
		requested.Exchange, requested.Market, requested.Symbol)
	var item ExSymbol
	if err := row.Scan(&item.ID, &item.Exchange, &item.ExgReal, &item.Market, &item.Symbol,
		&item.Combined, &item.ListMs, &item.DelistMs, &item.AggRules); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &item, nil
}

func cacheConfirmedQuestExSymbols(ctx context.Context, q *Queries, state *SymbolState, allocator *SIDAllocator, rows []exSymbolRecoveryRow) error {
	for _, expected := range rows {
		item, err := questExsymbolBySID(ctx, q, expected.ID)
		if err != nil {
			return fmt.Errorf("confirm exsymbol sid %d after visibility wait: %w", expected.ID, err)
		}
		if !expected.matches(item) {
			return fmt.Errorf("confirm exsymbol sid %d metadata mismatch after visibility wait", expected.ID)
		}
		if err := state.validateReservedSymbol(item); err != nil {
			return fmt.Errorf("cache confirmed exsymbol sid %d: %w", expected.ID, err)
		}
		if err := allocator.markSIDConfirmed(exSymbolKey(item.Exchange, item.Market, item.Symbol), item.ID); err != nil {
			return fmt.Errorf("confirm exsymbol sid %d reservation: %w", expected.ID, err)
		}
		if err := state.cacheExSymbolChecked(item); err != nil {
			return fmt.Errorf("cache confirmed exsymbol sid %d: %w", expected.ID, err)
		}
	}
	return nil
}

func ensurePendingExSymbolMarkers(allocator *SIDAllocator, recoveryRoot string, rows []exSymbolRecoveryRow) (string, string, error) {
	if len(rows) == 0 {
		return "", "", nil
	}
	rows = append([]exSymbolRecoveryRow(nil), rows...)
	now := time.Now().UTC()
	for i := range rows {
		if rows[i].WriteTS.IsZero() {
			rows[i].WriteTS = now.Add(time.Duration(i) * time.Microsecond)
		}
		if rows[i].Inserted == nil {
			inserted := false
			rows[i].Inserted = &inserted
		}
	}
	sharedMarkerPath, err := publishSharedSIDReservations(allocator, rows)
	if err != nil {
		return "", "", err
	}
	markerPath, err := findPendingExSymbolMarkerAcrossRoots(
		recoveryRootsForAllocator(allocator, recoveryRoot), allocator.Namespace(), rows)
	if err != nil {
		return "", "", err
	}
	if markerPath == "" {
		markerPath, err = writePendingExSymbolMarkerForNamespace(recoveryRoot, allocator.Namespace(), rows)
		if err != nil {
			return "", "", err
		}
	}
	reservations := make([]sidReservation, 0, len(rows))
	for _, row := range rows {
		reservations = append(reservations, sidReservation{
			key: exSymbolKey(row.Exchange, row.Market, row.Symbol),
			id:  row.ID,
		})
	}
	if err := allocator.reservePendingSIDBatch(reservations); err != nil {
		return "", "", fmt.Errorf("reserve pending exsymbol SIDs: %w", err)
	}
	return sharedMarkerPath, markerPath, nil
}

func reuseReservedAddSymbols(state *SymbolState, allocator *SIDAllocator, arg []AddSymbolsParams) ([]AddSymbolsParams, []exSymbolRecoveryRow, error) {
	newArg := make([]AddSymbolsParams, 0, len(arg))
	pendingRows := make([]exSymbolRecoveryRow, 0, len(arg))
	seen := make(map[string]AddSymbolsParams, len(arg))
	for _, item := range arg {
		key := exSymbolKey(item.Exchange, item.Market, item.Symbol)
		if previous, ok := seen[key]; ok {
			if !sameAddSymbolsParams(previous, item) {
				return nil, nil, fmt.Errorf("duplicate add symbol identity %s has conflicting metadata", key)
			}
			continue
		}
		seen[key] = item
		// A pending QuestDB write already owns this logical symbol. Reuse its
		// SID before allocating another one, even before WAL visibility catches
		// up and promotes the reservation to confirmed.
		if sid := allocator.reservationSID(key); sid > 0 {
			if allocator.reservedSID(key) == sid {
				requested := makeExSymbolFromAdd(sid, item)
				if current := state.GetExSymbol2(item.Exchange, item.Market, item.Symbol); current != nil {
					if !sameExSymbolSnapshot(current, requested) {
						return nil, nil, fmt.Errorf("add symbol identity %s reservation sid %d conflicts with canonical metadata", key, sid)
					}
					continue
				}
				if err := state.validateReservedSymbol(requested); err != nil {
					return nil, nil, fmt.Errorf("add symbol identity %s reservation sid %d conflicts with state: %w", key, sid, err)
				}
				if err := state.cacheExSymbolChecked(requested); err != nil {
					return nil, nil, fmt.Errorf("cache reserved exsymbol %s sid %d: %w", key, sid, err)
				}
				continue
			}
			requested := makeExSymbolFromAdd(sid, item)
			if current := state.GetExSymbol2(item.Exchange, item.Market, item.Symbol); current != nil {
				if !sameExSymbolSnapshot(current, requested) {
					return nil, nil, fmt.Errorf("add symbol identity %s pending sid %d conflicts with canonical metadata", key, sid)
				}
			}
			if err := state.validateReservedSymbol(requested); err != nil {
				return nil, nil, fmt.Errorf("add symbol identity %s pending sid %d conflicts with state: %w", key, sid, err)
			}
			pendingRows = append(pendingRows, pendingExSymbolRows([]AddSymbolsParams{item}, []int32{sid})[0])
			continue
		}
		if current := state.GetExSymbol2(item.Exchange, item.Market, item.Symbol); current != nil {
			requested := makeExSymbolFromAdd(current.ID, item)
			if !sameExSymbolSnapshot(current, requested) {
				return nil, nil, fmt.Errorf("add symbol identity %s conflicts with canonical metadata", key)
			}
			continue
		}
		newArg = append(newArg, item)
	}
	return newArg, pendingRows, nil
}

func sameAddSymbolsParams(a, b AddSymbolsParams) bool {
	return a.Exchange == b.Exchange && a.ExgReal == b.ExgReal && a.Market == b.Market && a.Symbol == b.Symbol &&
		a.Combined == b.Combined && a.ListMs == b.ListMs && a.DelistMs == b.DelistMs && a.AggRules == b.AggRules
}

func queryMaxSidFromQDB(ctx context.Context, db DBTX) (int32, error) {
	var maxVal *int32
	row := db.QueryRow(ctx, `SELECT max(sid) FROM exsymbol_q`)
	if err := row.Scan(&maxVal); err != nil {
		return 0, err
	}
	if maxVal == nil {
		return 0, nil
	}
	return *maxVal, nil
}

func (q *Queries) SetListMS(ctx context.Context, arg SetListMSParams) error {
	state, err := q.requireSymbolState()
	if err != nil {
		return err
	}
	return q.setListMS(ctx, state, arg, nil)
}

func (q *SymbolQueries) SetListMS(ctx context.Context, arg SetListMSParams) error {
	if q == nil {
		return errs.NewMsg(core.ErrBadConfig, "symbol query is required")
	}
	state := q.symbolState()
	if state == nil {
		return fmt.Errorf("explicit storage requires an explicit symbol state")
	}
	return q.Queries.setListMS(ctx, state, arg, nil)
}

func (q *Queries) setListMS(ctx context.Context, state *SymbolState, arg SetListMSParams, base *ExSymbol) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		err := q.setListMSPg(ctx, arg)
		if err == nil {
			symbolStateOrDefault(state).updateListMS(arg.ID, arg.ListMs, arg.DelistMs, base)
		}
		return err
	}
	unlock := q.LockCompactTableRead("exsymbol_q")
	defer unlock()
	item, err := waitForQuestExsymbolVisible(ctx, q, arg.ID)
	if err != nil || item == nil {
		return fmt.Errorf("SetListMS: sid %d not found: %w", arg.ID, err)
	}
	ts := time.Now().UTC()
	_, err = q.db.Exec(ctx, `INSERT INTO exsymbol_q (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)`,
		item.ID, ts, item.Exchange, item.ExgReal, item.Market, item.Symbol, item.Combined, arg.ListMs, arg.DelistMs, item.AggRules)
	if err == nil {
		symbolStateOrDefault(state).updateListMS(item.ID, arg.ListMs, arg.DelistMs, item)
		if err = waitForQuestExsymbolTimestampVisible(ctx, q, item.ID, ts); err != nil {
			return err
		}
		q.MarkTableForCompact("exsymbol_q", 1)
	}
	return err
}

func (q *Queries) SetAggRules(ctx context.Context, arg SetAggRulesParams) error {
	state, err := q.requireSymbolState()
	if err != nil {
		return err
	}
	return q.setAggRules(ctx, state, arg)
}

func (q *SymbolQueries) SetAggRules(ctx context.Context, arg SetAggRulesParams) error {
	if q == nil {
		return errs.NewMsg(core.ErrBadConfig, "symbol query is required")
	}
	state := q.symbolState()
	if state == nil {
		return fmt.Errorf("explicit storage requires an explicit symbol state")
	}
	return q.Queries.setAggRules(ctx, state, arg)
}

func (q *Queries) setAggRules(ctx context.Context, state *SymbolState, arg SetAggRulesParams) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !q.isQuestDB() {
		err := q.setAggRulesPg(ctx, arg)
		if err == nil {
			symbolStateOrDefault(state).updateAggRules(arg.ID, arg.AggRules, nil)
		}
		return err
	}
	unlock := q.LockCompactTableRead("exsymbol_q")
	defer unlock()
	item, err := waitForQuestExsymbolVisible(ctx, q, arg.ID)
	if err != nil || item == nil {
		return fmt.Errorf("SetAggRules: sid %d not found: %w", arg.ID, err)
	}
	ts := time.Now().UTC()
	_, err = q.db.Exec(ctx, `INSERT INTO exsymbol_q (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)`,
		item.ID, ts, item.Exchange, item.ExgReal, item.Market, item.Symbol, item.Combined, item.ListMs, item.DelistMs, arg.AggRules)
	if err == nil {
		symbolStateOrDefault(state).updateAggRules(item.ID, arg.AggRules, item)
		if err = waitForQuestExsymbolTimestampVisible(ctx, q, item.ID, ts); err != nil {
			return err
		}
		q.MarkTableForCompact("exsymbol_q", 1)
	}
	return err
}
