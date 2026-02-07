package orm

import (
	"context"
	"fmt"
	"testing"
)

func TestDb(t *testing.T) {
	t.Skip("integration test")
	ctx := context.Background()

	pq := PubQ()
	symbols, err := pq.ListSymbols(ctx, "binance")
	if err != nil {
		fmt.Printf("list goods fail: %s", err)
		return
	}
	fmt.Printf("loaded %d goods", len(symbols))
}
