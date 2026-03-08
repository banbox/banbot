package orm

import (
	"context"
	"fmt"
	"testing"
)

func TestDb(t *testing.T) {
	t.Skip("integration test")
	ctx := context.Background()

	pq, conn, err2 := Conn(ctx)
	if err2 != nil {
		fmt.Printf("Conn fail: %s", err2)
		return
	}
	defer conn.Release()
	symbols, err := pq.ListSymbols(ctx, "binance")
	if err != nil {
		fmt.Printf("list goods fail: %s", err)
		return
	}
	fmt.Printf("loaded %d goods", len(symbols))
}
