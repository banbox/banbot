package orm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// applyQuestDBMigrations executes individual non-transactional steps so a
// successful ADD COLUMN followed by a failed version marker can be retried.
// Only a duplicate-column error from an ADD COLUMN statement is idempotent;
// other failures stop the migration before its version can be recorded.
func applyQuestDBMigrations(ctx context.Context, db questExecer, ddl string, current int64) (int64, error) {
	for _, migration := range strings.Split(ddl, "-- version") {
		lines := strings.SplitN(strings.TrimSpace(migration), "\n", 2)
		if len(lines) != 2 {
			continue
		}
		version, err := strconv.ParseInt(strings.TrimSpace(lines[0]), 10, 64)
		if err != nil || version <= current {
			continue
		}
		for _, statement := range splitQuestDBStatements(lines[1]) {
			_, err := db.Exec(ctx, statement, pgx.QueryExecModeSimpleProtocol)
			if err != nil {
				upper := strings.ToUpper(statement)
				if !strings.HasPrefix(upper, "ALTER TABLE ") || !strings.Contains(upper, " ADD COLUMN ") || !isQuestDuplicateColumnErr(err) {
					return current, fmt.Errorf("questdb migration %d: %w", version, err)
				}
			}
		}
		if _, err := db.Exec(ctx, `insert into schema_migrations (version, applied_ts) values ($1,$2)`, version, time.Now().UTC()); err != nil {
			return current, err
		}
		current = version
	}
	return current, nil
}
