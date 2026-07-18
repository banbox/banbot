package utils

import (
	"sync"
	"testing"

	"github.com/banbox/banexg/errs"
)

func TestParallelRunReturnsLowestStartedErrorIndex(t *testing.T) {
	const taskNum = 8
	started := make(chan int, taskNum)
	release := make(chan struct{})
	want := make([]*errs.Error, taskNum)
	for i := range want {
		want[i] = errs.NewMsg(1000+i, "task %d", i)
	}
	result := make(chan *errs.Error, 1)
	go func() {
		result <- ParallelRun(want, taskNum, func(i int, item *errs.Error) *errs.Error {
			started <- i
			<-release
			return item
		})
	}()
	for range taskNum {
		<-started
	}
	close(release)
	if got := <-result; got != want[0] {
		t.Fatalf("returned error = %v, want lowest input index error %v", got, want[0])
	}
}

func TestParallelRunProcessesEachItemWithinLimit(t *testing.T) {
	const (
		taskNum = 9
		limit   = 3
	)
	var lock sync.Mutex
	active := 0
	maxActive := 0
	seen := make([]int, taskNum)
	release := make(chan struct{})
	started := make(chan struct{}, taskNum)
	result := make(chan *errs.Error, 1)
	go func() {
		result <- ParallelRun(make([]struct{}, taskNum), limit, func(i int, _ struct{}) *errs.Error {
			lock.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			lock.Unlock()
			started <- struct{}{}
			<-release
			lock.Lock()
			seen[i]++
			active--
			lock.Unlock()
			return nil
		})
	}()
	for range limit {
		<-started
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if maxActive > limit {
		t.Fatalf("maximum concurrency = %d, limit %d", maxActive, limit)
	}
	for i, count := range seen {
		if count != 1 {
			t.Fatalf("item %d handled %d times", i, count)
		}
	}
}
