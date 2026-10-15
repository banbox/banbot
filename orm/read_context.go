package orm

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// WithReadContext binds legacy read helpers that internally use Background to
// an explicit reader lifetime. It preserves storage, symbol, exchange and
// kline policies; Query/QueryRow use the reader's actual cancellation scope.
func (q *Queries) WithReadContext(ctx context.Context) *Queries {
	if q == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	copyQuery := *q
	copyQuery.db = &readerContextDB{DBTX: q.db, ctx: ctx}
	return &copyQuery
}

type readerContextDB struct {
	DBTX
	ctx context.Context
}

func (q *Queries) seriesReadContext() context.Context {
	if reader, ok := q.db.(*readerContextDB); ok {
		return reader.ctx
	}
	return context.Background()
}

func (d *readerContextDB) Query(_ context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return d.DBTX.Query(d.ctx, sql, args...)
}
func (d *readerContextDB) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	return d.DBTX.QueryRow(d.ctx, sql, args...)
}
