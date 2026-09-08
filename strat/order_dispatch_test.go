package strat

import (
	"testing"

	"github.com/banbox/banbot/orm/ormo"
)

func TestFireOdChangeWithStateIsolatesCallbacks(t *testing.T) {
	oldSubs := accOdSubs
	lockOdSub.Lock()
	accOdSubs = make(map[string][]FnOdChange)
	lockOdSub.Unlock()
	t.Cleanup(func() {
		lockOdSub.Lock()
		accOdSubs = oldSubs
		lockOdSub.Unlock()
	})

	var legacyCalls, firstCalls, firstWildcardCalls, secondCalls int
	AddOdSub("runtime", func(string, *ormo.InOutOrder, int) { legacyCalls++ })
	first := NewState()
	first.AddOdSub("runtime", func(string, *ormo.InOutOrder, int) { firstCalls++ })
	first.AddOdSub("*", func(string, *ormo.InOutOrder, int) { firstWildcardCalls++ })
	second := NewState()
	second.AddOdSub("runtime", func(string, *ormo.InOutOrder, int) { secondCalls++ })
	order := &ormo.InOutOrder{Enter: &ormo.ExOrder{}}

	FireOdChangeWithState(first, "runtime", order, OdChgEnter)
	if firstCalls != 1 || firstWildcardCalls != 1 || secondCalls != 0 || legacyCalls != 0 {
		t.Fatalf("first runtime dispatch leaked: first=%d wildcard=%d second=%d legacy=%d",
			firstCalls, firstWildcardCalls, secondCalls, legacyCalls)
	}

	FireOdChangeWithState(second, "runtime", order, OdChgEnter)
	if firstCalls != 1 || firstWildcardCalls != 1 || secondCalls != 1 || legacyCalls != 0 {
		t.Fatalf("second runtime dispatch leaked: first=%d wildcard=%d second=%d legacy=%d",
			firstCalls, firstWildcardCalls, secondCalls, legacyCalls)
	}

	FireOdChangeWithState(nil, "runtime", order, OdChgEnter)
	if legacyCalls != 1 {
		t.Fatalf("nil state did not preserve legacy dispatch: legacy=%d", legacyCalls)
	}
}
