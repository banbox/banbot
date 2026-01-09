package ormo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

type WalletSnapshotItem struct {
	Coin          string             `json:"coin"`
	Available     float64            `json:"available"`
	Pendings      map[string]float64 `json:"pendings,omitempty"`
	Frozens       map[string]float64 `json:"frozens,omitempty"`
	UnrealizedPOL float64            `json:"unrealized_pol"`
	UsedUPol      float64            `json:"used_upol"`
	Withdraw      float64            `json:"withdraw"`
}

type WalletSnapshotSummary struct {
	BaseCurrency       string  `json:"base_currency"`
	TotalLegal         float64 `json:"total_legal"`
	AvailableLegal     float64 `json:"available_legal"`
	UnrealizedPOLLegal float64 `json:"unrealized_pol_legal"`
	WithdrawLegal      float64 `json:"withdraw_legal"`
}

var (
	walletSnapshotOnce sync.Once
	walletSnapshotErr  *errs.Error
)

const (
	walletSnapshotCompactHourMS   = int64(60 * 60 * 1000)
	walletSnapshotDeleteBatchSize = 200
)

func ensureWalletSnapshotTables(db *orm.TrackedDB) *errs.Error {
	walletSnapshotOnce.Do(func() {
		ctx := context.Background()
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS wallet_snapshot (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				task_id INTEGER NOT NULL,
				account TEXT NOT NULL,
				time_ms INTEGER NOT NULL,
				created_at INTEGER NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS idx_ws_task_time ON wallet_snapshot (task_id, time_ms)`,
			`CREATE INDEX IF NOT EXISTS idx_ws_account_time ON wallet_snapshot (account, time_ms)`,
			`CREATE TABLE IF NOT EXISTS wallet_snapshot_compact (
				task_id INTEGER NOT NULL,
				account TEXT NOT NULL,
				compacted_until_ms INTEGER NOT NULL,
				updated_at INTEGER NOT NULL,
				PRIMARY KEY (task_id, account)
			)`,
			`CREATE INDEX IF NOT EXISTS idx_wsc_task_account ON wallet_snapshot_compact (task_id, account)`,
			`CREATE TABLE IF NOT EXISTS wallet_snapshot_item (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				snapshot_id INTEGER NOT NULL,
				coin TEXT NOT NULL,
				available REAL NOT NULL,
				pendings TEXT NOT NULL,
				frozens TEXT NOT NULL,
				unrealized_pol REAL NOT NULL,
				used_upol REAL NOT NULL,
				withdraw REAL NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS idx_wsi_snapshot ON wallet_snapshot_item (snapshot_id)`,
			`CREATE INDEX IF NOT EXISTS idx_wsi_coin ON wallet_snapshot_item (coin)`,
			`CREATE TABLE IF NOT EXISTS wallet_snapshot_summary (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				snapshot_id INTEGER NOT NULL,
				base_currency TEXT NOT NULL,
				total_legal REAL NOT NULL,
				available_legal REAL NOT NULL,
				unrealized_pol_legal REAL NOT NULL,
				withdraw_legal REAL NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS idx_wss_snapshot ON wallet_snapshot_summary (snapshot_id)`,
		}
		for _, stmt := range stmts {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				walletSnapshotErr = errs.New(core.ErrDbExecFail, err)
				return
			}
		}
	})
	return walletSnapshotErr
}

func SaveWalletSnapshot(taskID int64, account string, timeMS int64, items []*WalletSnapshotItem, summary *WalletSnapshotSummary) *errs.Error {
	if len(items) == 0 {
		return nil
	}
	_, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := ensureWalletSnapshotTables(conn); err != nil {
		return err
	}
	ctx := context.Background()
	tx, err_ := conn.BeginTx(ctx, nil)
	if err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	res, err_ := tx.ExecContext(ctx, `insert into wallet_snapshot (task_id, account, time_ms, created_at) values (?, ?, ?, ?)`,
		taskID, account, timeMS, btime.UTCStamp())
	if err_ != nil {
		tx.Rollback()
		return errs.New(core.ErrDbExecFail, err_)
	}
	snapshotID, err_ := res.LastInsertId()
	if err_ != nil {
		tx.Rollback()
		return errs.New(core.ErrDbExecFail, err_)
	}
	stmt, err_ := tx.PrepareContext(ctx, `insert into wallet_snapshot_item
		(snapshot_id, coin, available, pendings, frozens, unrealized_pol, used_upol, withdraw)
		values (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err_ != nil {
		tx.Rollback()
		return errs.New(core.ErrDbExecFail, err_)
	}
	for _, item := range items {
		pendings := "{}"
		if len(item.Pendings) > 0 {
			raw, err := json.Marshal(item.Pendings)
			if err != nil {
				stmt.Close()
				tx.Rollback()
				return errs.New(core.ErrMarshalFail, err)
			}
			pendings = string(raw)
		}
		frozens := "{}"
		if len(item.Frozens) > 0 {
			raw, err := json.Marshal(item.Frozens)
			if err != nil {
				stmt.Close()
				tx.Rollback()
				return errs.New(core.ErrMarshalFail, err)
			}
			frozens = string(raw)
		}
		if _, err := stmt.ExecContext(ctx, snapshotID, item.Coin, item.Available, pendings, frozens, item.UnrealizedPOL, item.UsedUPol, item.Withdraw); err != nil {
			stmt.Close()
			tx.Rollback()
			return errs.New(core.ErrDbExecFail, err)
		}
	}
	stmt.Close()
	if summary != nil {
		if _, err := tx.ExecContext(ctx, `insert into wallet_snapshot_summary
			(snapshot_id, base_currency, total_legal, available_legal, unrealized_pol_legal, withdraw_legal)
			values (?, ?, ?, ?, ?, ?)`,
			snapshotID, summary.BaseCurrency, summary.TotalLegal, summary.AvailableLegal, summary.UnrealizedPOLLegal, summary.WithdrawLegal); err != nil {
			tx.Rollback()
			return errs.New(core.ErrDbExecFail, err)
		}
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return errs.New(core.ErrDbExecFail, err)
	}
	return nil
}

func LoadLatestWalletSnapshot(taskID int64, account string) ([]*WalletSnapshotItem, *WalletSnapshotSummary, *errs.Error) {
	_, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	if err := ensureWalletSnapshotTables(conn); err != nil {
		return nil, nil, err
	}
	ctx := context.Background()
	var snapshotID int64
	err_ := conn.QueryRowContext(ctx, `select id from wallet_snapshot
		where task_id=? and account=?
		order by time_ms desc, id desc
		limit 1`, taskID, account).Scan(&snapshotID)
	if err_ != nil {
		if err_ == sql.ErrNoRows {
			return nil, nil, nil
		}
		return nil, nil, errs.New(core.ErrDbReadFail, err_)
	}
	rows, err_ := conn.QueryContext(ctx, `select coin, available, pendings, frozens, unrealized_pol, used_upol, withdraw
		from wallet_snapshot_item where snapshot_id=?`, snapshotID)
	if err_ != nil {
		return nil, nil, errs.New(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	items := make([]*WalletSnapshotItem, 0)
	for rows.Next() {
		var coin string
		var available, unrealizedPOL, usedUPol, withdraw float64
		var pendingsRaw, frozensRaw string
		if err := rows.Scan(&coin, &available, &pendingsRaw, &frozensRaw, &unrealizedPOL, &usedUPol, &withdraw); err != nil {
			return nil, nil, errs.New(core.ErrDbReadFail, err)
		}
		item := &WalletSnapshotItem{
			Coin:          coin,
			Available:     available,
			Pendings:      map[string]float64{},
			Frozens:       map[string]float64{},
			UnrealizedPOL: unrealizedPOL,
			UsedUPol:      usedUPol,
			Withdraw:      withdraw,
		}
		if pendingsRaw != "" {
			_ = json.Unmarshal([]byte(pendingsRaw), &item.Pendings)
		}
		if frozensRaw != "" {
			_ = json.Unmarshal([]byte(frozensRaw), &item.Frozens)
		}
		items = append(items, item)
	}
	var summary *WalletSnapshotSummary
	row := conn.QueryRowContext(ctx, `select base_currency, total_legal, available_legal, unrealized_pol_legal, withdraw_legal
		from wallet_snapshot_summary where snapshot_id=? limit 1`, snapshotID)
	var baseCurrency string
	var totalLegal, availableLegal, unrealizedPOLLegal, withdrawLegal float64
	if err := row.Scan(&baseCurrency, &totalLegal, &availableLegal, &unrealizedPOLLegal, &withdrawLegal); err == nil {
		summary = &WalletSnapshotSummary{
			BaseCurrency:       baseCurrency,
			TotalLegal:         totalLegal,
			AvailableLegal:     availableLegal,
			UnrealizedPOLLegal: unrealizedPOLLegal,
			WithdrawLegal:      withdrawLegal,
		}
	}
	return items, summary, nil
}

func LoadWalletSnapshotCompactUntil(taskID int64, account string) (int64, *errs.Error) {
	_, conn, err := Conn(orm.DbTrades, false)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if err := ensureWalletSnapshotTables(conn); err != nil {
		return 0, err
	}
	ctx := context.Background()
	var compactedUntilMS int64
	err_ := conn.QueryRowContext(ctx, `select compacted_until_ms from wallet_snapshot_compact
		where task_id=? and account=?`, taskID, account).Scan(&compactedUntilMS)
	if err_ != nil {
		if err_ == sql.ErrNoRows {
			return 0, nil
		}
		return 0, errs.New(core.ErrDbReadFail, err_)
	}
	return compactedUntilMS, nil
}

func SaveWalletSnapshotCompactUntil(taskID int64, account string, compactedUntilMS int64) *errs.Error {
	_, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := ensureWalletSnapshotTables(conn); err != nil {
		return err
	}
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, `insert into wallet_snapshot_compact
		(task_id, account, compacted_until_ms, updated_at)
		values (?, ?, ?, ?)
		on conflict(task_id, account) do update set
			compacted_until_ms=excluded.compacted_until_ms,
			updated_at=excluded.updated_at`,
		taskID, account, compactedUntilMS, btime.UTCStamp()); err != nil {
		return errs.New(core.ErrDbExecFail, err)
	}
	return nil
}

func FindEarliestWalletSnapshotTime(taskID int64, account string, beforeMS int64) (int64, *errs.Error) {
	_, conn, err := Conn(orm.DbTrades, false)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if err := ensureWalletSnapshotTables(conn); err != nil {
		return 0, err
	}
	ctx := context.Background()
	var timeMS int64
	err_ := conn.QueryRowContext(ctx, `select time_ms from wallet_snapshot
		where task_id=? and account=? and time_ms<?
		order by time_ms asc, id asc
		limit 1`, taskID, account, beforeMS).Scan(&timeMS)
	if err_ != nil {
		if err_ == sql.ErrNoRows {
			return 0, nil
		}
		return 0, errs.New(core.ErrDbReadFail, err_)
	}
	return timeMS, nil
}

func CompactWalletSnapshotsByHour(taskID int64, account string, startMS int64, endMS int64) (int, *errs.Error) {
	if endMS <= startMS {
		return 0, nil
	}
	_, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if err := ensureWalletSnapshotTables(conn); err != nil {
		return 0, err
	}
	ctx := context.Background()
	rows, err_ := conn.QueryContext(ctx, `select id, time_ms from wallet_snapshot
		where task_id=? and account=? and time_ms>=? and time_ms<?
		order by time_ms asc, id asc`, taskID, account, startMS, endMS)
	if err_ != nil {
		return 0, errs.New(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	var deleteIDs []int64
	curBoundary := int64(-1)
	var keepID int64
	var keepDiff int64
	for rows.Next() {
		var snapshotID int64
		var timeMS int64
		if err := rows.Scan(&snapshotID, &timeMS); err != nil {
			return 0, errs.New(core.ErrDbReadFail, err)
		}
		boundary := roundToNearestHour(timeMS)
		diff := absInt64(timeMS - boundary)
		if curBoundary == -1 || boundary != curBoundary {
			curBoundary = boundary
			keepID = snapshotID
			keepDiff = diff
			continue
		}
		if diff < keepDiff {
			deleteIDs = append(deleteIDs, keepID)
			keepID = snapshotID
			keepDiff = diff
		} else {
			deleteIDs = append(deleteIDs, snapshotID)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, errs.New(core.ErrDbReadFail, err)
	}
	if len(deleteIDs) == 0 {
		return 0, nil
	}
	tx, err_ := conn.BeginTx(ctx, nil)
	if err_ != nil {
		return 0, errs.New(core.ErrDbExecFail, err_)
	}
	for i := 0; i < len(deleteIDs); i += walletSnapshotDeleteBatchSize {
		end := i + walletSnapshotDeleteBatchSize
		if end > len(deleteIDs) {
			end = len(deleteIDs)
		}
		chunk := deleteIDs[i:end]
		if err := deleteWalletSnapshotsByIDs(ctx, tx, chunk); err != nil {
			tx.Rollback()
			return 0, errs.New(core.ErrDbExecFail, err)
		}
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return 0, errs.New(core.ErrDbExecFail, err)
	}
	return len(deleteIDs), nil
}

func roundToNearestHour(timeMS int64) int64 {
	return ((timeMS + walletSnapshotCompactHourMS/2) / walletSnapshotCompactHourMS) * walletSnapshotCompactHourMS
}

func absInt64(val int64) int64 {
	if val < 0 {
		return -val
	}
	return val
}

func deleteWalletSnapshotsByIDs(ctx context.Context, tx *sql.Tx, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := buildPlaceholders(len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`delete from wallet_snapshot_item where snapshot_id in (%s)`, placeholders), args...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`delete from wallet_snapshot_summary where snapshot_id in (%s)`, placeholders), args...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`delete from wallet_snapshot where id in (%s)`, placeholders), args...); err != nil {
		return err
	}
	return nil
}

func buildPlaceholders(num int) string {
	if num <= 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i < num; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('?')
	}
	return b.String()
}
