package orm

import (
	"context"
	"fmt"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type compressionPolicy struct {
	table         string
	compressAfter int64
}

var compressionPolicies = []compressionPolicy{
	{table: "kline_1m", compressAfter: 60 * 24 * 60 * 60 * 1000},
	{table: "kline_5m", compressAfter: 60 * 24 * 60 * 60 * 1000},
	{table: "kline_15m", compressAfter: 90 * 24 * 60 * 60 * 1000},
	{table: "kline_1h", compressAfter: 180 * 24 * 60 * 60 * 1000},
	{table: "kline_1d", compressAfter: 3 * 365 * 24 * 60 * 60 * 1000},
}

type compressionState struct {
	exists         bool
	enabled        bool
	settingsOK     bool
	integerNowFunc string
	policyAfter    int64
}

// EnsureTimescaleCompression keeps banbot's K-line compression settings in sync.
// It is a no-op for QuestDB and PostgreSQL databases without TimescaleDB.
func EnsureTimescaleCompression(ctx context.Context) *errs.Error {
	if IsQuestDB {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if pool == nil {
		return errs.NewMsg(core.ErrDbConnFail, "database is not initialized")
	}
	if err := ensureTimescaleCompression(ctx, pool); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func ensureTimescaleCompression(ctx context.Context, db *pgxpool.Pool) error {
	var hasTimescale bool
	if err := db.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
	)`).Scan(&hasTimescale); err != nil {
		return err
	}
	if !hasTimescale {
		return nil
	}

	if _, err := db.Exec(ctx, `CREATE OR REPLACE FUNCTION public.banbot_now_ms()
		RETURNS BIGINT
		LANGUAGE SQL STABLE
		AS 'SELECT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000)::BIGINT'`); err != nil {
		return err
	}

	updated := make([]string, 0, len(compressionPolicies))
	for _, policy := range compressionPolicies {
		state, err := loadCompressionState(ctx, db, policy.table)
		if err != nil {
			return err
		}
		if !state.exists {
			continue
		}

		changed := false
		if !state.enabled || !state.settingsOK {
			query := fmt.Sprintf(`ALTER TABLE public.%s SET (
				timescaledb.compress,
				timescaledb.compress_orderby = 'time DESC',
				timescaledb.compress_segmentby = 'sid'
			)`, policy.table)
			if _, err = db.Exec(ctx, query); err != nil {
				return err
			}
			changed = true
		}

		if !isBanbotNowFunc(state.integerNowFunc) {
			if _, err = db.Exec(ctx, `SELECT set_integer_now_func(
				$1::regclass,
				'public.banbot_now_ms'::regproc,
				replace_if_exists => TRUE
			)`, "public."+policy.table); err != nil {
				return err
			}
			changed = true
		}

		if state.policyAfter != policy.compressAfter {
			if err = replaceCompressionPolicy(ctx, db, policy); err != nil {
				return err
			}
			changed = true
		}
		if changed {
			updated = append(updated, policy.table)
		}
	}

	if len(updated) > 0 {
		log.Info("updated timescaledb compression settings", zap.Strings("tables", updated))
	}
	return nil
}

func loadCompressionState(ctx context.Context, db *pgxpool.Pool, table string) (compressionState, error) {
	var state compressionState
	err := db.QueryRow(ctx, `SELECT
		EXISTS (
			SELECT 1 FROM timescaledb_information.hypertables
			WHERE hypertable_schema = 'public' AND hypertable_name = $1
		),
		COALESCE((
			SELECT compression_enabled FROM timescaledb_information.hypertables
			WHERE hypertable_schema = 'public' AND hypertable_name = $1
		), FALSE),
		(
			SELECT COUNT(*) = 2
				AND COUNT(*) FILTER (WHERE attname = 'sid' AND segmentby_column_index = 1) = 1
				AND COUNT(*) FILTER (WHERE attname = 'time' AND orderby_column_index = 1 AND orderby_asc = FALSE) = 1
			FROM timescaledb_information.compression_settings
			WHERE hypertable_schema = 'public' AND hypertable_name = $1
		),
		COALESCE((
			SELECT integer_now_func FROM timescaledb_information.dimensions
			WHERE hypertable_schema = 'public' AND hypertable_name = $1 AND column_name = 'time'
		), ''),
		COALESCE((
			SELECT (config->>'compress_after')::BIGINT FROM timescaledb_information.jobs
			WHERE hypertable_schema = 'public' AND hypertable_name = $1
				AND proc_name IN ('policy_compression', 'policy_columnstore')
			LIMIT 1
		), -1)
	`, table).Scan(
		&state.exists,
		&state.enabled,
		&state.settingsOK,
		&state.integerNowFunc,
		&state.policyAfter,
	)
	return state, err
}

func replaceCompressionPolicy(ctx context.Context, db *pgxpool.Pool, policy compressionPolicy) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	table := "public." + policy.table
	if _, err = tx.Exec(ctx, `SELECT remove_compression_policy($1::regclass, if_exists => TRUE)`, table); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT add_compression_policy($1::regclass, $2::BIGINT, if_not_exists => TRUE)`, table, policy.compressAfter); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func isBanbotNowFunc(name string) bool {
	name = strings.ReplaceAll(name, `"`, "")
	return name == "banbot_now_ms" || name == "public.banbot_now_ms"
}
