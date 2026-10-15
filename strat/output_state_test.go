package strat

import (
	"sync"
	"testing"
)

func TestTradeStratOutputSnapshotAndDrain(t *testing.T) {
	stgy := &TradeStrat{}
	stgy.WriteOutput("one", false)
	stgy.WriteOutput("two", false)
	if got := stgy.SnapshotOutputs(false); len(got) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(got))
	}
	if got := stgy.DrainOutputs(); len(got) != 2 {
		t.Fatalf("drain len = %d, want 2", len(got))
	}
	if got := stgy.DrainOutputs(); len(got) != 0 {
		t.Fatalf("second drain len = %d, want 0", len(got))
	}
}

func TestTradeStratOutputDrainConcurrentWithWrites(t *testing.T) {
	stgy := &TradeStrat{}
	const writers = 4
	const linesPerWriter = 250
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < linesPerWriter; i++ {
				stgy.WriteOutput("line", false)
			}
		}()
	}
	drained := make(chan int, 1)
	go func() {
		count := 0
		for {
			rows := stgy.DrainOutputs()
			count += len(rows)
			if count >= writers*linesPerWriter {
				drained <- count
				return
			}
		}
	}()
	wg.Wait()
	count := <-drained
	if count != writers*linesPerWriter {
		t.Fatalf("drained %d lines, want %d", count, writers*linesPerWriter)
	}
}
