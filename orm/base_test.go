package orm

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type beginDelegatingDB struct{}

func (beginDelegatingDB) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (beginDelegatingDB) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	return nil, nil
}

func (beginDelegatingDB) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	return nil
}

func (beginDelegatingDB) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (beginDelegatingDB) Begin(context.Context) (pgx.Tx, error) {
	return nil, context.Canceled
}

func TestSubQueriesBeginDelegatesToPoolConnection(t *testing.T) {
	_, err := (&SubQueries{db: beginDelegatingDB{}}).Begin(context.Background())
	if err != context.Canceled {
		t.Fatalf("Begin error = %v, want delegated error %v", err, context.Canceled)
	}
}

func TestNormalizeDatabaseURLLegacyBracketedIPv4(t *testing.T) {
	got := normalizeDatabaseURL("postgresql://user:pass@[127.0.0.1]:5432/ban")
	if got != "postgresql://user:pass@127.0.0.1:5432/ban" {
		t.Fatalf("normalized URL = %q", got)
	}
}
