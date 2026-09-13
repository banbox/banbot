package biz

import (
	"sync"
	"testing"

	"github.com/banbox/banbot/strat"
)

func TestReserveLiveEnter(t *testing.T) {
	const (
		timeframe = "1m"
		barStart  = int64(1789232880000)
	)
	makeReq := func(strategy, tag string, short bool) *strat.EnterReq {
		return &strat.EnterReq{StratName: strategy, Tag: tag, Short: short}
	}

	mgr := &OrderMgr{}
	first, ok := mgr.reserveLiveEnter("BNB/USDT:USDT", timeframe, makeReq("s1", "init", false), barStart+1)
	if !ok {
		t.Fatal("first live entry must be accepted")
	}
	if _, ok := mgr.reserveLiveEnter("BNB/USDT:USDT", timeframe, makeReq("s1", "init", false), barStart+2000); ok {
		t.Fatal("same strategy, direction, tag, pair, and bar must be rejected")
	}

	cases := []struct {
		name string
		req  *strat.EnterReq
	}{
		{"different direction", makeReq("s1", "init", true)},
		{"different strategy", makeReq("s2", "init", false)},
		{"different tag", makeReq("s1", "martin1", false)},
	}
	for _, tc := range cases {
		if _, ok := mgr.reserveLiveEnter("BNB/USDT:USDT", timeframe, tc.req, barStart+3000); !ok {
			t.Fatalf("%s must not be deduplicated", tc.name)
		}
	}

	if _, ok := mgr.reserveLiveEnter("BNB/USDT:USDT", timeframe, makeReq("s1", "init", false), barStart+60000); !ok {
		t.Fatal("same request in the next timeframe must be accepted")
	}

	mgr.releaseLiveEnter(first)
	if _, ok := mgr.reserveLiveEnter("BNB/USDT:USDT", timeframe, makeReq("s1", "init", false), barStart+60000); ok {
		t.Fatal("releasing an expired reservation must not remove the next timeframe reservation")
	}
}

func TestReleaseLiveEnter(t *testing.T) {
	const nowMS = int64(1789232880001)
	mgr := &OrderMgr{}
	req := &strat.EnterReq{StratName: "s1", Tag: "init"}
	reservation, ok := mgr.reserveLiveEnter("BNB/USDT:USDT", "1m", req, nowMS)
	if !ok {
		t.Fatal("first live entry must be accepted")
	}
	mgr.releaseLiveEnter(reservation)
	if _, ok := mgr.reserveLiveEnter("BNB/USDT:USDT", "1m", req, nowMS+1); !ok {
		t.Fatal("a failed save must release its reservation")
	}
}
func TestReserveLiveEnterConcurrent(t *testing.T) {
	const (
		nowMS   = int64(1789232880001)
		workers = 32
	)
	mgr := &OrderMgr{}
	req := &strat.EnterReq{StratName: "s1", Tag: "init"}
	start := make(chan struct{})
	var wg sync.WaitGroup
	accepted := make(chan struct{}, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, ok := mgr.reserveLiveEnter("BNB/USDT:USDT", "1m", req, nowMS); ok {
				accepted <- struct{}{}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(accepted)
	count := 0
	for range accepted {
		count++
	}
	if count != 1 {
		t.Fatalf("concurrent duplicate entries accepted: got %d, want 1", count)
	}
}
