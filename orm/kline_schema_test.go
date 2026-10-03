package orm

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type klineSchemaDB struct {
	DBTX
	rows func() [][]any
}

func (d *klineSchemaDB) Query(ctx context.Context, _ string, _ ...interface{}) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return newInterfaceRows(d.rows()), nil
}

func TestKlineProjectionSchemaPreservesPhysicalTypesAndNullability(t *testing.T) {
	for _, quest := range []bool{false, true} {
		kind, nullable := "bigint", false
		db := &klineSchemaDB{rows: func() [][]any {
			if quest {
				return [][]any{{"sid", "INT", false, false}, {"ts", "TIMESTAMP", false, true}, {"integer", kind, false, false}, {"extra", "STRING", false, false}}
			}
			return [][]any{{"sid", "integer", false}, {"time", "bigint", false}, {"integer", kind, nullable}, {"extra", "text", true}}
		}}
		if quest {
			kind = "LONG"
		}
		q := NewWithStorage(db, &Storage{questDB: quest})
		first, err := q.ReadKlineProjectionSchema(context.Background(), "1m", []string{"integer"})
		if err != nil {
			t.Fatal(err)
		}
		if first.Table != "kline_1m" || len(first.Fields) != 3 || first.Fields[0].Name != "integer" || first.Fields[0].Nullability == "" {
			t.Fatalf("physical projected schema lost: %+v", first)
		}
		kind, nullable = "DOUBLE", true
		second, err := q.ReadKlineProjectionSchema(context.Background(), "1m", []string{"integer"})
		if err != nil || reflect.DeepEqual(first, second) {
			t.Fatalf("physical type change was invisible: %+v err=%v", second, err)
		}
		if !quest {
			if first.Fields[0].Nullability != "not-null" || second.Fields[0].Nullability != "nullable" {
				t.Fatal("PostgreSQL NOT NULL semantics were omitted")
			}
		}
		if _, err := q.ReadKlineProjectionSchema(context.Background(), "1m", []string{"missing"}); err == nil {
			t.Fatal("missing projected column admitted")
		}
	}
}

func TestKlineProjectionSchemaCancellationStopsActualMetadataSQL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := &blockedReaderDB{entered: make(chan struct{}), closed: make(chan struct{})}
	q := NewWithStorage(db, &Storage{})
	done := make(chan error, 1)
	go func() {
		_, err := q.ReadKlineProjectionSchema(ctx, "1m", []string{"integer"})
		done <- err
	}()
	select {
	case <-db.entered:
	case <-time.After(time.Second):
		t.Fatal("metadata SQL not entered")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("metadata cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("metadata SQL outlived cancellation")
	}
}
