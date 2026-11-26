package biz

import (
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestHubEntryEndCauseMapping(t *testing.T) {
	m := hubProjectionTestManager(t)
	m.journal.Commands = map[string]*hubFrozenCommand{"a/exit/c1": {Exit: &execution.TimeSeriesExitCommand{ID: "c1", ExitTag: "my_cancel"}}}
	cases := []struct {
		name     string
		ev       execution.StrategyAcceptedEvent
		tag, why string
		cancel   bool
	}{
		{"owner OCO loser", execution.StrategyAcceptedEvent{Kind: execution.EntryIntent, CommandID: "oco-oco_filled_sibling-9", Rejected: "oco_filled_sibling"}, ExitTagOCOCancel, "oco_filled_sibling", false},
		{"owner isolation", execution.StrategyAcceptedEvent{Kind: execution.EntryIntent, CommandID: "isolated-risk_limit-9", Rejected: "risk_limit"}, core.ExitTagForceExit, "risk_limit", false},
		{"robot OCO cancel_entry", execution.StrategyAcceptedEvent{Kind: execution.EntryIntent, CommandID: "ts/s/3" + hubFollowupOCO}, ExitTagOCOCancel, "", false},
		{"strategy exit request", execution.StrategyAcceptedEvent{Kind: execution.ExitIntent, CommandID: "c1"}, "my_cancel", "", true},
		{"strategy exit without a journaled command", execution.StrategyAcceptedEvent{Kind: execution.ExitIntent, CommandID: "other"}, core.ExitTagForceExit, "", true},
	}
	for _, c := range cases {
		lot := &hubProjectedLot{}
		m.captureEntryEndLocked(lot, c.ev)
		if lot.EndTag != c.tag || lot.EndReason != c.why || lot.EndCancel != c.cancel {
			t.Errorf("%s: got tag %q reason %q cancel %v", c.name, lot.EndTag, lot.EndReason, lot.EndCancel)
		}
	}
	// Edits and ladder follow-ups never end an entry; a lot with fills is not an unfilled end.
	lot := &hubProjectedLot{}
	m.captureEntryEndLocked(lot, execution.StrategyAcceptedEvent{Kind: execution.ExitIntent, CommandID: "x" + hubFollowupEdit})
	m.captureEntryEndLocked(lot, execution.StrategyAcceptedEvent{Kind: execution.EntryIntent, CommandID: "x" + hubFollowupLadder})
	filled := &hubProjectedLot{Entered: 1}
	m.captureEntryEndLocked(filled, execution.StrategyAcceptedEvent{Kind: execution.EntryIntent, CommandID: "oco-x-1", Rejected: "x"})
	if lot.EndTag != "" || filled.EndTag != "" {
		t.Fatal("edit/ladder/filled lots must not capture an end", lot.EndTag, filled.EndTag)
	}
}

func TestHubEntryEndMarkedOnceAndFallbackTags(t *testing.T) {
	m := hubProjectionTestManager(t)
	lot := hubViewTestLot(t, m, nil)
	lot.Entered, lot.EntryCost = 0, decimal.Zero
	k := hubLotKey("a", "lot")
	// Definition gone, StopBars set: expiry; reported once.
	if !m.markEntryEndLocked(k, lot, 0) {
		t.Fatal("first mark")
	}
	if lot.EndTag != core.ExitTagForceExit || !lot.EndNotified {
		t.Fatal("fallback without StopBars", lot.EndTag)
	}
	if m.markEntryEndLocked(k, lot, 0) {
		t.Fatal("an ended lot must be reported once")
	}
	// A pending Expired+Canceled definition is expiry.
	lot2 := hubViewTestLot(t, m, nil)
	lot2.Entered = 0
	p := execution.PendingDefinition{}
	p.Order.Strategy, p.Order.Lot, p.Order.Canceled = "a", "lot", true
	p.Order.Entry.State = execution.Expired
	m.pending[k] = p
	if !m.markEntryEndLocked(k, lot2, 0) || lot2.EndTag != core.ExitTagEntExp || lot2.EndReason == "" {
		t.Fatal("expired definition", lot2.EndTag, lot2.EndReason)
	}
	// An active, uncancelled entry or a held position is not an end.
	lot3 := hubViewTestLot(t, m, nil)
	lot3.Entered = 0
	p.Order.Canceled, p.Order.Entry.State = false, execution.PendingCondition
	m.pending[k] = p
	if m.markEntryEndLocked(k, lot3, 0) || m.markEntryEndLocked(k, lot3, 5) {
		t.Fatal("live entry reported as ended")
	}
	// The ended view is a labelled Delete order.
	delete(m.pending, k)
	lot2.EndTag, lot2.EndReason, lot2.Terminal = core.ExitTagEntExp, "reach StopEnterBars", true
	od, err := m.viewLocked(lot2, true)
	if err != nil {
		t.Fatal(err)
	}
	if od.Status != ormo.InOutStatusDelete || od.ExitTag != core.ExitTagEntExp || od.Exit == nil || od.Info[ormo.KeyStatusMsg] != "reach StopEnterBars" {
		t.Fatal("ended view", od.Status, od.ExitTag, od.Exit)
	}
}

func TestHubDropOrderCarriesRequestIdentity(t *testing.T) {
	m := hubProjectionTestManager(t)
	req := &strat.EnterReq{Tag: "sig", Short: true, Amount: 2, Limit: 99, LegalCost: 10, Infos: map[string]string{"k": "v"}}
	od := m.dropOrderLocked("ts", "BTC", "1m", 3, req, 55, 1000, 100, "risk_limit", "msg")
	if od.ID != 55 || od.Symbol != "BTC" || od.Timeframe != "1m" || od.Strategy != "ts" || !od.Short || od.EnterTag != "sig" || od.Status != ormo.InOutStatusDelete ||
		od.ExitTag != core.ExitTagForceExit || od.Enter.Amount != 2 || od.Enter.Price != 99 || od.Info["reject_code"] != "risk_limit" || od.Info["k"] != "v" || od.Exit == nil {
		t.Fatalf("dropped order identity: %+v", od.IOrder)
	}
	if hubDropTag(hubDropSize) != core.ExitTagFatalErr || hubDropTag("venue_minimum") != core.ExitTagFatalErr || hubDropTag("limit_off_tick") != core.ExitTagForceExit {
		t.Fatal("drop tag mapping")
	}
}
