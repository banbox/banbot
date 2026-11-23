package runtime

import (
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

// newHubMinimumFixture builds the standard remote-robot Hub fixture and then
// gives the robot-side instrument a venue minimum. The shared fixture's
// instrument has no MinSteps/MinNotional, and the bridge's Instruments map is
// shared by reference with the robot manager, so setting it here makes the
// robot's own size pre-check see the minimum. The hub-side instrument keeps no
// minimum, so any entry that slips past the robot pre-check would fill.
func newHubMinimumFixture(t *testing.T, minSteps int64) *hubTSFixture {
	t.Helper()
	f := newHubTSFixture(t, nil)
	instrument := f.owner.bridge.Instruments["BTC"]
	instrument.MinSteps = minSteps
	f.owner.bridge.Instruments["BTC"] = instrument
	if f.bridge.Bridge.Instruments["BTC"].MinSteps != minSteps {
		t.Fatal("fixture bridge does not share the instrument map; minimum not applied")
	}
	return f
}

// An entry below the instrument minimum must be dropped quietly by the robot:
// (nil, nil), nothing sent to the venue, no sticky error, and later valid
// entries keep working. If the pre-check were missing, the Hub would fill it
// (the hub-side fixture instrument has no minimum), and if it surfaced as an
// error it would stop the robot via LastError.
func TestHubRobotDropsEntryBelowVenueMinimumCleanly(t *testing.T) {
	f := newHubMinimumFixture(t, 10) // 10 steps = 1 unit at QuantityStep 0.1
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	fills := f.owner.adapter.Metrics().Fills

	od, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "tiny", Tag: "tiny", Amount: 0.5})
	if err != nil || od != nil {
		t.Fatalf("below-minimum entry must be (nil, nil), got %v, %v", od, err)
	}
	if got := f.owner.adapter.Metrics().Fills; got != fills {
		t.Fatalf("below-minimum entry reached the venue: fills %d -> %d", fills, got)
	}
	if e := f.manager.LastError(); e != nil {
		t.Fatalf("below-minimum drop left a sticky error: %v", e)
	}

	good, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "ok", Tag: "ok", Amount: 1})
	if err != nil || good == nil {
		t.Fatalf("normal entry after a drop failed: %v, %v", good, err)
	}
	if good.Enter.Filled != 1 || f.owner.adapter.Metrics().Fills != fills+1 {
		t.Fatalf("normal entry not filled: %+v fills=%d", good.Enter, f.owner.adapter.Metrics().Fills)
	}
	if e := f.manager.LastError(); e != nil {
		t.Fatal(e)
	}
}
