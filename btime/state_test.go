package btime

import (
	"sort"
	"sync"
	"testing"
)

func TestClockStateAdvanceMSIsAtomic(t *testing.T) {
	clock := NewClockState(true, nil)
	clock.SetTimeMS(100)
	const calls = 64
	values := make(chan int64, calls)
	var wg sync.WaitGroup
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			values <- clock.AdvanceMS(1)
		}()
	}
	wg.Wait()
	close(values)

	got := make([]int64, 0, calls)
	for value := range values {
		got = append(got, value)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	for i, value := range got {
		want := int64(101 + i)
		if value != want {
			t.Fatalf("advance result[%d] = %d, want %d; values=%v", i, value, want, got)
		}
	}
	if got := clock.TimeMS(); got != 100+calls {
		t.Fatalf("clock time = %d, want %d", got, 100+calls)
	}
}
