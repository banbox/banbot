package orm

import (
	"context"
	"database/sql"
	"fmt"
)

type PubQueries struct{}

var defaultPubQueries = &PubQueries{}

func PubQ() *PubQueries {
	return defaultPubQueries
}

func getMaxID(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, table, col string) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sqlText := fmt.Sprintf("select max(%s) from %s", col, table)
	row := queryer.QueryRowContext(ctx, sqlText)
	var v sql.NullInt64
	if err := row.Scan(&v); err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return v.Int64, nil
}
