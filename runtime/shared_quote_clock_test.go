package runtime

import (
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"testing"
)

func TestSharedLegacyEntryAndExplicitExitUseQuoteCompletionClock(t *testing.T) {
	f := newSharedTriggerFixture(t)
	calls := 0
	f.bridge.Quote = func(_ string, now int64) (execution.VisibleQuote, error) {
		calls++
		completion := max(now, f.rt.Clock.TimeMS()) + 1
		f.rt.Clock.SetTimeMS(completion)
		return execution.VisibleQuote{Bid: f.price, Ask: f.price, AtMS: completion, ReceivedMS: completion, ValidUntilMS: completion + 1000, Bar: f.bar}, nil
	}
	od := f.entry(t, &strat.EnterReq{Tag: "delayed-quote", Amount: 1})
	if f.steps(t) != 10 {
		t.Fatal("entry failed after quote IO")
	}
	if _, err := f.manager.ExitOpenOrders("BTC", &strat.ExitReq{Tag: "explicit", StratName: "legacy", OrderID: od.ID}); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 0 || calls < 4 {
		t.Fatal("explicit exit did not use advancing quotes", calls)
	}
}
