package runner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/banbox/banbot/factor"
)

func TestLiveComputationGroupReleasesJoinedGenerationsAndKeepsOtherBorrowers(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.Snapshot.Universe = factor.Universe{Version: "lifecycle", Static: true, Investable: []int32{1, 2}, Reference: []int32{1, 2}, Tradable: []int32{1, 2}}
	c.ComputationGroup = NewComputationGroup()
	c.ComputationContext = ComputationContext{DataNamespace: "fixture", ClockDomain: "clock", SamplingIdentity: "closed-hour"}
	const grid int64 = 3600000
	newLive := func() *Live {
		t.Helper()
		live, err := NewLive(c, &liveSink{}, func() int64 { return grid + 1 }, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { live.Stop(); _ = live.Join(context.Background()) })
		return live
	}
	first, second := newLive(), newLive()
	if !first.SharesComputation(second) {
		t.Fatal("compatible borrowers did not share")
	}
	first.Stop()
	if err := first.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	third := newLive()
	if !second.SharesComputation(third) {
		t.Fatal("joining one borrower released another's computation")
	}
	second.Stop()
	if err := second.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	third.Stop()
	if err := third.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.ComputationGroup.sessions) != 0 {
		t.Fatalf("joined generation still retained %d sessions", len(c.ComputationGroup.sessions))
	}
	for generation := 0; generation < 20; generation++ {
		c.Snapshot.Universe.Version = fmt.Sprintf("generation-%d", generation)
		live := newLive()
		live.Stop()
		if err := live.Join(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(c.ComputationGroup.sessions) != 0 {
			t.Fatalf("generation %d retained stale computation", generation)
		}
	}
}

func TestLiveComputationBorrowRemainsOwnedUntilActualJoin(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.ComputationGroup = NewComputationGroup()
	c.ComputationContext = ComputationContext{DataNamespace: "fixture", ClockDomain: "clock", SamplingIdentity: "closed-hour"}
	live, err := NewLive(c, &liveSink{}, func() int64 { return 3600001 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Join must preserve the borrow while tracked callback work is still owned,
	// even when Stop has canceled intake and a caller's Join deadline expires.
	live.work.Add(1)
	live.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err = live.Join(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Join did not wait for actual work: %v", err)
	}
	c.ComputationGroup.mu.Lock()
	retained := len(c.ComputationGroup.sessions)
	c.ComputationGroup.mu.Unlock()
	if retained != 1 {
		t.Fatal("Stop released a computation still used by work")
	}
	live.work.Done()
	if err = live.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.ComputationGroup.sessions) != 0 {
		t.Fatal("completed Join retained computation")
	}
}
