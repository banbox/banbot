package biz

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func reviewMgr(t *testing.T) *HubOrderMgr {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := &HubOrderMgr{ctx: ctx, journal: hubOrderJournal{Lots: map[string]*hubProjectedLot{}, Plans: map[string]*hubLotPlan{}}}
	m.config.StatePath = filepath.Join(t.TempDir(), "robot.json")
	m.config.Client = execution.NewHubClient(filepath.Join(t.TempDir(), "absent.sock"))
	m.config.Bridge.Instruments = map[string]execution.Instrument{"BTC": {QuantityStep: decimal.NewFromInt(1), ContractSize: decimal.NewFromInt(1)}}
	return m
}

func reviewLot() *hubProjectedLot {
	lot := &hubProjectedLot{Entered: 10, EntryCost: decimal.NewFromInt(1000)}
	lot.Record.Metadata.Symbol = "BTC"
	return lot
}

func reviewPending(stop int64) execution.PendingDefinition {
	var p execution.PendingDefinition
	p.Order.ProtectionLevels = map[string]decimal.Decimal{}
	p.Order.ProtectionDone = map[string]bool{}
	if stop > 0 {
		p.Specs = []execution.PositionProtection{{Kind: execution.StopLossProtection, Level: decimal.NewFromInt(stop)}}
		p.Order.ProtectionLevels[string(execution.StopLossProtection)] = decimal.NewFromInt(stop)
	}
	return p
}

func TestHubLadderTinyRungMergedIntoNextRung(t *testing.T) {
	m := reviewMgr(t)
	l, err := newHubLadder(&strat.EnterReq{TakeProfits: []strat.TakeProfitTarget{{Price: 105, ExitRate: 0.3, MoveSLToBreakeven: true}, {Price: 110, ExitRate: 0.4}, {Price: 120, ExitRate: 0.3}}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	p := reviewPending(0)
	p.Specs = []execution.PositionProtection{l.tpSpec()}
	// 3 steps held: 0.3 -> floor(0.9)=0 steps, 0.4/1 -> floor(1.2)=1 step, so only rung 0 is skipped.
	got, need := m.ladderStep(*l, reviewLot(), p, 3)
	if !need || got.Index != 1 || got.Skipped != 1 || got.Remaining != 1 {
		t.Fatalf("rung not skipped: %+v need=%v", got, need)
	}
	if r := got.Rungs[1].ExitRate; r < 0.699 || r > 0.701 || !got.Rungs[1].MoveSLToBreakeven {
		t.Fatalf("share not merged: %+v", got.Rungs[1])
	}
	if l.Index != 0 || l.Rungs[1].ExitRate != 0.4 {
		t.Fatal("journaled ladder was mutated by a dry-run step")
	}
	spec := got.tpSpec()
	if !spec.Level.Equal(decimal.NewFromInt(110)) || spec.Rate.Sub(decimal.RequireFromString("0.7")).Abs().GreaterThan(decimal.RequireFromString("0.000001")) {
		t.Fatalf("installed spec %+v", spec)
	}
	// Merged rung 1 executes (0.7 of 3 -> 2 steps; 1 left): the last rung closes the rest, total intended 1.0.
	got.advance()
	if last := got.tpSpec(); !last.Rate.Equal(decimal.NewFromInt(1)) || got.Remaining < 0.299 || got.Remaining > 0.301 {
		t.Fatalf("last rung %+v remaining %v", last, got.Remaining)
	}
	// A last rung is never skipped, however small the position.
	one := *l
	one.Index, one.Remaining = 2, 0.3
	one.skipTinyRungs(1)
	if one.Index != 2 || one.Skipped != 0 {
		t.Fatal("last rung skipped")
	}
	// Large positions are untouched.
	if big, _ := m.ladderStep(*l, reviewLot(), p, 100); big.Index != 0 || big.Skipped != 0 {
		t.Fatalf("rung skipped on a large position: %+v", big)
	}
}

func TestHubLadderBreakevenReconcilesWithOwnerStop(t *testing.T) {
	m := reviewMgr(t)
	for _, short := range []bool{false, true} {
		l, _ := newHubLadder(&strat.EnterReq{Short: short, TakeProfits: []strat.TakeProfitTarget{{Price: 90, ExitRate: 0.5}, {Price: 110, ExitRate: 0.5}}}, 100)
		if short {
			l.Rungs = []strat.TakeProfitTarget{{Price: 95, ExitRate: 0.5}, {Price: 90, ExitRate: 0.5}}
		} else {
			l.Rungs = []strat.TakeProfitTarget{{Price: 105, ExitRate: 0.5}, {Price: 110, ExitRate: 0.5}}
		}
		l.Resolved, l.Index, l.Remaining, l.BEPending = true, 1, 0.5, true
		loose, tight := int64(90), int64(100)
		if short {
			loose = 110
		}
		p := reviewPending(loose)
		p.Specs = append(p.Specs, l.tpSpec())
		if _, need := m.ladderStep(*l, reviewLot(), p, 5); !need {
			t.Fatalf("short=%v: loose owner stop must keep the break-even pending", short)
		}
		p = reviewPending(tight)
		p.Specs = append(p.Specs, l.tpSpec())
		got, need := m.ladderStep(*l, reviewLot(), p, 5)
		if need || got.BEPending {
			t.Fatalf("short=%v: tight owner stop must settle the flag (no resend loop): %+v need=%v", short, got, need)
		}
		// Already tighter than break-even also settles it.
		p = reviewPending(tight + map[bool]int64{false: 2, true: -2}[short])
		p.Specs = append(p.Specs, l.tpSpec())
		if got, need = m.ladderStep(*l, reviewLot(), p, 5); need || got.BEPending {
			t.Fatalf("short=%v: tighter stop must settle the flag", short)
		}
	}
	// With the stop merely absent the flag stays and the tightening adds it.
	if !ladderBEBlocked(hubLadder{BEPending: true}, decimal.Zero) || ladderBEBlocked(hubLadder{BEPending: true}, decimal.NewFromInt(1)) || ladderBEBlocked(hubLadder{}, decimal.Zero) {
		t.Fatal("ladderBEBlocked")
	}
}

func TestHubPruneDropsCancelledOCOLoser(t *testing.T) {
	m := reviewMgr(t)
	m.journal.Lots["a"] = &hubProjectedLot{}
	m.journal.Lots["b"] = &hubProjectedLot{}
	m.journal.Lots["c"] = &hubProjectedLot{Entered: 1}
	m.journal.Lots["d"] = &hubProjectedLot{}
	m.journal.Plans["a"] = &hubLotPlan{OCOGroup: "g", OCOCancelled: true}
	m.journal.Plans["b"] = &hubLotPlan{OCOGroup: "g"}                                   // not yet cancelled
	m.journal.Plans["c"] = &hubLotPlan{OCOGroup: "g", OCOCancelled: true}               // has fills
	m.journal.Plans["d"] = &hubLotPlan{OCOGroup: "g", OCOCancelled: true, Cmd: "x/oco"} // in flight
	if err := m.pruneLocked(); err != nil {
		t.Fatal(err)
	}
	if m.journal.Plans["a"] != nil || m.journal.Plans["b"] == nil || m.journal.Plans["c"] == nil || m.journal.Plans["d"] == nil {
		t.Fatalf("plans after prune: %v", m.journal.Plans)
	}
}

// An owner (or transport) refusal never stops the robot: the frozen command is
// retried with the same body, abandoned after hubFollowupAttempts, and a later
// reconciliation builds a fresh one once the back-off elapsed.
func TestHubFollowupRetryAbandonBackoff(t *testing.T) {
	m := reviewMgr(t)
	cmd := &execution.PendingCommand{ID: "ts/s/1/lad", Action: execution.PendingSetProtections}
	plan := &hubLotPlan{Cmd: cmd.ID, Ladder: &hubLadder{BEPending: true}}
	f := &hubFollowup{Pending: cmd, Kind: hubFollowupLadder, BE: true}
	m.journal.Plans["k"] = plan
	m.journal.Followups = map[string]*hubFollowup{cmd.ID: f}
	now := time.Now()
	for i := 1; i < hubFollowupAttempts; i++ {
		if err := m.sendFollowupLocked("k", plan, f, now); err != nil {
			t.Fatal(err)
		}
		if f.Attempts != i || plan.Cmd != cmd.ID || m.journal.Followups[cmd.ID] == nil || !m.backoff["k"].until.After(now) {
			t.Fatalf("attempt %d: %+v", i, f)
		}
	}
	// Inside the back-off window a retry does nothing.
	pd := execution.PendingDefinition{}
	if err := m.retryFollowupLocked("k", plan, pd, now); err != nil || f.Attempts != hubFollowupAttempts-1 {
		t.Fatal("retry ignored the back-off", f.Attempts, err)
	}
	if err := m.sendFollowupLocked("k", plan, f, now); err != nil {
		t.Fatal(err)
	}
	if plan.Cmd != "" || m.journal.Followups[cmd.ID] != nil {
		t.Fatal("follow-up not abandoned")
	}
	if !plan.Ladder.BEPending {
		t.Fatal("abandoning must not settle the break-even flag")
	}
	b := m.backoff["k"]
	if b == nil || b.fails < 1 || !b.until.After(now) || b.until.After(now.Add(hubFollowupMaxBackoff)) {
		t.Fatalf("back-off after abandon: %+v", b)
	}
	// The back-off grows but is capped.
	for i := 0; i < 20; i++ {
		m.failFollowupLocked("k")
	}
	if got := time.Until(m.backoff["k"].until); got > hubFollowupMaxBackoff {
		t.Fatal("back-off not capped", got)
	}
	if m.LastError() != nil {
		t.Fatal(m.LastError())
	}
}
