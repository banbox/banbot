package runner

import (
	"context"
	"errors"
	"reflect"
	"sync"
)

// replayTimeline orders same-account historical consumers by timestamp and
// declaration order, so one strategy cannot move the shared venue into a
// future quote/funding time while another is still accepting an older target.
type replayTimeline struct {
	mu              sync.Mutex
	consumers, turn int
	at              int64
	changed         chan struct{}
	allSourceEvents bool
}

func (t *replayTimeline) enter(ctx context.Context, index int, at int64) error {
	for {
		t.mu.Lock()
		if t.turn == index {
			if index == 0 {
				t.at = at
			} else if t.at != at {
				t.mu.Unlock()
				return errors.New("runner: shared account replay timelines differ")
			}
			t.mu.Unlock()
			return ctx.Err()
		}
		changed := t.changed
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (t *replayTimeline) leave() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.turn = (t.turn + 1) % t.consumers
	close(t.changed)
	t.changed = make(chan struct{})
}
func sameReplayTimeline(a, b Config) bool {
	inputA, inputB := "", ""
	if a.HistoricalInput != nil {
		inputA = a.HistoricalInput.Identity()
	}
	if b.HistoricalInput != nil {
		inputB = b.HistoricalInput.Identity()
	}
	return inputA == inputB && reflect.DeepEqual(a.Chunks, b.Chunks) && a.DecisionInterval == b.DecisionInterval && a.DecisionDelayMS == b.DecisionDelayMS && a.Prices.Source == b.Prices.Source && a.Prices.TimeFrame == b.Prices.TimeFrame && a.FundingSource == b.FundingSource && (a.Snapshot.ReplayTime != 0) == (b.Snapshot.ReplayTime != 0)
}
