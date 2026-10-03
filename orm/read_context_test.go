package orm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/jackc/pgx/v5"
)

type blockedReaderDB struct {
	DBTX
	entered, closed chan struct{}
	got             context.Context
}

func (d *blockedReaderDB) Query(ctx context.Context, _ string, _ ...interface{}) (pgx.Rows, error) {
	d.got = ctx
	close(d.entered)
	defer close(d.closed)
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestKlineReadContextCancelsBlockedSQLAndPreservesOwners(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := &blockedReaderDB{entered: make(chan struct{}), closed: make(chan struct{})}
	storage := &Storage{}
	symbols := NewSymbolStateWithIdentity("fixture", "linear")
	original := NewWithStorage(db, storage).WithSeriesSymbolState(symbols).WithKlineRuntimeOptions(KlineRuntimeOptions{Storage: storage, ClockValid: true, NowMS: 1704067380000, Backtest: true, NoDownload: true})
	original = original.WithExchange(&marketLoadArgsExchange{info: &banexg.ExgInfo{ID: "fixture", MarketType: "linear"}})
	query := original.WithReadContext(ctx)
	if query.storage != storage || query.symbols != symbols || query.options != original.options || original.db != db {
		t.Fatal("reader wrapper changed runtime identity or original query")
	}
	done := make(chan error, 1)
	go func() {
		_, err := query.QuerySeriesFields(&ExSymbol{ID: 1, Symbol: "A", Exchange: "fixture", Market: "linear"}, "1m", []string{"close"}, 1704067200000, 1704067380000, 2, false)
		done <- err
	}()
	select {
	case <-db.entered:
	case err := <-done:
		t.Fatalf("query rejected before SQL: %v", err)
	case <-time.After(time.Second):
		t.Fatal("SQL read not entered")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled SQL succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Kline query outlived canceled reader")
	}
	select {
	case <-db.closed:
	case <-time.After(time.Second):
		t.Fatal("SQL reader did not exit")
	}
	if !errors.Is(db.got.Err(), context.Canceled) {
		t.Fatal("SQL used an unrelated context")
	}
}
