package runtime

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// startHubBar is startHub with an adjustable quote Bar. Production hub quotes
// carry Bar 0; the migration may be started with the legacy bar clock still
// visible (bar > 0) and the clock dropped afterwards.
func (s *legacyScenario) startHubBar(t *testing.T, price int64, bar *atomic.Int64) (*legacyHubRun, error) {
	t.Helper()
	f := s.f
	r := &legacyHubRun{t: t, price: decimal.NewFromInt(price)}
	r.process = NewProcess()
	var err error
	if r.borrow, err = r.process.BorrowAccount(f.key, *f.opts); err != nil {
		t.Fatal(err)
	}
	if r.hub, err = execution.NewEmbeddedHub(context.Background(), r.borrow.Service(), 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	bridge := *f.bridge
	bridge.Quote = func(string, int64) (execution.VisibleQuote, error) {
		r.mu.Lock()
		p := r.price
		r.mu.Unlock()
		now := time.Now().UnixMilli()
		return execution.VisibleQuote{Bid: p, Ask: p, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 5000, Bar: bar.Load()}, nil
	}
	r.bridge = &biz.HubOrderBridgeConfig{Client: r.hub.Port(), Account: f.key, Bridge: bridge, WriterID: "ts-local-test", StatePath: s.statePath, LocalAccount: true}
	r.rt, err = r.process.NewRuntime(Options{Mode: core.RunModeLive, Env: core.RunEnvProd, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedMarketData: true, HubOrderBridge: r.bridge})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	r.rt.Clock.SetTimeMS(now)
	if err = r.rt.SharedExecution().Reconcile("startup", now); err != nil {
		t.Fatal(err)
	}
	r.manager, err = biz.NewHubOrderMgr(r.rt.BizDeps(), r.bridge, nil)
	if err != nil {
		r.manager = nil
		r.stop()
		return nil, err
	}
	t.Cleanup(r.stop)
	return r, nil
}

func ownerBook(t *testing.T, r *legacyHubRun) (defs []execution.PendingDefinition) {
	t.Helper()
	err := r.borrow.WithState(func(a *execution.SharedAccount) error {
		var err error
		defs, err = a.Store().LoadPendingBook(context.Background())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return defs
}

func ownerOCOReason(t *testing.T, r *legacyHubRun, lot execution.VirtualLotID) string {
	t.Helper()
	var reason string
	_ = r.borrow.WithState(func(a *execution.SharedAccount) error {
		body, err := a.Store().StrategyCheckpoint(context.Background(), "ts", "pending-oco:"+string(lot))
		if err != nil {
			return nil
		}
		var rec struct{ Reason string }
		if err := json.Unmarshal(body, &rec); err != nil {
			t.Fatal(err)
		}
		reason = rec.Reason
		return nil
	})
	return reason
}

func waitOwner(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// lotsByOrder maps legacy order IDs to lots through the owner's metadata.
func lotsByOrder(t *testing.T, r *legacyHubRun, key execution.AccountKey) map[int64]string {
	t.Helper()
	page, err := r.hub.Port().TimeSeriesProjection(context.Background(), key, execution.TimeSeriesProjectionRequest{Name: "probe-lots", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	out := map[int64]string{}
	for _, rec := range page.Records {
		out[rec.Metadata.OrderID] = string(rec.Lot)
	}
	return out
}

func TestHubLegacyCommandRetries(t *testing.T) {
	s := newLegacyScenario(t)
	f := s.f
	// A partial legacy exit with its own command ID.
	exitReq := func(rate float64) *strat.ExitReq {
		return &strat.ExitReq{StratName: "legacy", CommandID: "x-half", Tag: "half", ExitRate: rate, Dirt: core.OdDirtBoth, OrderID: s.plain}
	}
	if _, e := f.manager.ExitOpenOrders("BTC", exitReq(0.5)); e != nil {
		t.Fatal(e)
	}
	f.observe(t, 100, 4)
	f.process.Close()
	r, err := s.startHub(t, 100)
	if err != nil {
		t.Fatal(err)
	}
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	fills := f.adapter.Metrics().Fills
	before := r.orders(t)
	// Entry retries return the original order even when the payload differs
	// (no hash comparison), and send nothing.
	for _, retry := range []*strat.EnterReq{
		{CommandID: "cmd-plain", Tag: "plain", Amount: 0.5, StopLoss: 70},
		{CommandID: "cmd-plain", Tag: "other", Amount: 3},
	} {
		if od, e := r.manager.EnterOrder(symbol, "ws", retry); e != nil || od == nil || od.ID != s.plain {
			t.Fatalf("entry retry %+v: %+v %v", retry, od, e)
		}
	}
	// An exit retry returns the original result orders, again without a hash
	// comparison and without sending.
	for _, rate := range []float64{0.5, 1} {
		ods, e := r.manager.ExitOpenOrders("BTC", exitReq(rate))
		if e != nil || len(ods) != 1 || ods[0].ID != s.plain {
			t.Fatalf("exit retry rate %v: %v %v", rate, summarize(map[int64]*ormo.InOutOrder{0: nil}), e)
		}
	}
	if got := r.orders(t); len(got) != len(before) || f.adapter.Metrics().Fills != fills {
		t.Fatalf("retries changed the account: %v fills %d->%d", summarize(got), fills, f.adapter.Metrics().Fills)
	}
	if r.manager.LastError() != nil {
		t.Fatal(r.manager.LastError())
	}

	// A real hub command keeps the hash check: same payload replays, a
	// different payload is refused.
	req := &strat.EnterReq{CommandID: "hub-1", Tag: "hub", Amount: 0.5, OrderType: core.OrderTypeLimit, Limit: 60}
	first, e := r.manager.EnterOrder(symbol, "ws", req)
	if e != nil || first == nil {
		t.Fatalf("hub entry: %v %v", first, e)
	}
	if again, e := r.manager.EnterOrder(symbol, "ws", req); e != nil || again == nil || again.ID != first.ID {
		t.Fatalf("hub replay: %+v %v", again, e)
	}
	changed := *req
	changed.Amount = 0.7
	if od, e := r.manager.EnterOrder(symbol, "ws", &changed); e == nil || od != nil || !strings.Contains(e.Error(), "reused") {
		t.Fatalf("a reused hub command ID with a different payload must be refused: %+v %v", od, e)
	}
	r.stop()
	r.manager = nil

	// An imported command is never resent: forcing it unfinished in the
	// journal must not make resumeCommands send (it has nothing to send).
	journal := readJournal(t, s.statePath)
	var commands map[string]map[string]any
	if err := json.Unmarshal(journal["Commands"], &commands); err != nil {
		t.Fatal(err)
	}
	imported := 0
	for _, c := range commands {
		if c["Imported"] == true {
			c["Done"] = false
			imported++
		}
	}
	if imported == 0 {
		t.Fatal("journal has no imported command")
	}
	journal["Commands"], _ = json.Marshal(commands)
	body, _ := json.Marshal(journal)
	if err := os.WriteFile(s.statePath, body, 0600); err != nil {
		t.Fatal(err)
	}
	r2, err := s.startHub(t, 100)
	if err != nil {
		t.Fatalf("restart with unfinished imported commands: %v", err)
	}
	if od, e := r2.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "cmd-plain", Amount: 9}); e != nil || od == nil || od.ID != s.plain {
		t.Fatalf("imported retry after restart: %+v %v", od, e)
	}
	if f.adapter.Metrics().Fills != fills || r2.manager.LastError() != nil {
		t.Fatalf("restart resent an imported command: fills %d->%d, %v", fills, f.adapter.Metrics().Fills, r2.manager.LastError())
	}
}

// Migrated OCO members are enforced by the owner, without the robot.
func TestHubLegacyMigratedOCOOwnerEnforcement(t *testing.T) {
	for _, tc := range []struct {
		name   string
		price  int64
		reason string
	}{{"simultaneous", 80, execution.OCOSimultaneousReason}, {"filled sibling", 85, execution.OCOFilledSiblingReason}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &legacyScenario{f: newSharedTriggerFixture(t), statePath: t.TempDir() + "/robot/ts-local.json"}
			f := s.f
			a := f.entry(t, &strat.EnterReq{CommandID: "a", Tag: "a", Amount: 0.5, OrderType: core.OrderTypeLimit, Limit: 85, OCOGroup: "g"}).ID
			b := f.entry(t, &strat.EnterReq{CommandID: "b", Tag: "b", Amount: 0.5, OrderType: core.OrderTypeLimit, Limit: 80, OCOGroup: "g"}).ID
			f.process.Close()
			r, err := s.startHub(t, 100)
			if err != nil {
				t.Fatal(err)
			}
			// Only the owner acts from here: the robot loop is stopped.
			r.manager.CleanUp()
			lots := lotsByOrder(t, r, s.f.key)
			var groups = map[int64]execution.PendingOrder{}
			for _, d := range ownerBook(t, r) {
				for id, lot := range lots {
					if lot == string(d.Order.Lot) {
						groups[id] = d.Order
					}
				}
			}
			if groups[a].OCOGroup != "g" || groups[b].OCOGroup != "g" || groups[a].OCOSeq != a || groups[b].OCOSeq != b {
				t.Fatalf("owner OCO group not bound: %+v", groups)
			}
			r.setPrice(tc.price)
			waitOwner(t, "owner OCO cancellation", func() bool { return ownerOCOReason(t, r, execution.VirtualLotID(lots[b])) != "" })
			if got := ownerOCOReason(t, r, execution.VirtualLotID(lots[b])); got != tc.reason {
				t.Fatalf("reason %q want %q", got, tc.reason)
			}
			if ownerOCOReason(t, r, execution.VirtualLotID(lots[a])) != "" {
				t.Fatal("the winner was cancelled")
			}
			time.Sleep(200 * time.Millisecond)
			r.setPrice(80)
			time.Sleep(300 * time.Millisecond)
			for _, d := range ownerBook(t, r) {
				if string(d.Order.Lot) == lots[b] && (!d.Order.Canceled || d.Order.Entry.FilledSteps != 0) {
					t.Fatalf("cancelled sibling is alive: %+v", d.Order)
				}
			}
		})
	}
}

// A resting entry created on a quote bar clock (CreatedBar > 0, StopBars N)
// is rebound to the hub's feed clock with N-k bars left.
func TestHubLegacyMigratedEntryKeepsStopBars(t *testing.T) {
	setup := func(t *testing.T, createdBars, extra int64) (*legacyScenario, int64) {
		s := &legacyScenario{f: newSharedTriggerFixture(t), statePath: t.TempDir() + "/robot/ts-local.json"}
		id := s.f.entry(t, &strat.EnterReq{CommandID: "rest", Tag: "rest", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 5}).ID
		s.f.observe(t, 100, 2)
		s.f.observe(t, 100, 3) // k = 2 bars consumed, 3 left
		s.f.process.Close()
		return s, id
	}
	closeBar := func(r *legacyHubRun, n int64) {
		ms := time.Now().UnixMilli() + n*60000
		if e := r.manager.UpdateByDataSeries(nil, &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: "ws", TimeMS: ms - 1, EndMS: ms, Closed: true, Values: map[string]any{"close": 100.0}}); e != nil {
			t.Fatal(e)
		}
	}
	live := func(r *legacyHubRun, id int64) bool {
		od := r.orders(t)[id]
		return od != nil && od.Status != ormo.InOutStatusDelete
	}
	t.Run("expires after exactly N-k closed bars", func(t *testing.T) {
		s, id := setup(t, 1, 0)
		var bar atomic.Int64
		bar.Store(3)
		r, err := s.startHubBar(t, 100, &bar)
		if err != nil {
			t.Fatal(err)
		}
		bar.Store(0) // production hub quotes carry no bar
		defs := ownerBook(t, r)
		if len(defs) != 1 || defs[0].Order.Entry.Conditions.StopBars != 0 || defs[0].Order.Entry.Conditions.CreatedBar != 0 || defs[0].Order.EntryFeedExpiry == nil {
			t.Fatalf("entry clock not rebound: %+v", defs)
		}
		for n := int64(1); n <= 2; n++ {
			closeBar(r, n)
			time.Sleep(150 * time.Millisecond)
			if !live(r, id) {
				t.Fatalf("expired after only %d of the 3 remaining bars", n)
			}
		}
		if r.manager.LastError() != nil {
			t.Fatal(r.manager.LastError())
		}
		// Restart in between (journal kept) and a journal-less re-import must not
		// restart the lifetime.
		r.stop()
		if err = os.Remove(s.statePath); err != nil {
			t.Fatal(err)
		}
		bar.Store(7) // a different visible clock must not matter any more
		r, err = s.startHubBar(t, 100, &bar)
		if err != nil {
			t.Fatalf("re-import: %v", err)
		}
		bar.Store(0)
		if !live(r, id) {
			t.Fatal("entry lost on re-import")
		}
		closeBar(r, 3)
		waitOwner(t, "expiry on the third bar", func() bool { return !live(r, id) })
	})
	t.Run("fills on the hub path with quote bar 0", func(t *testing.T) {
		s, id := setup(t, 1, 0)
		var bar atomic.Int64
		bar.Store(3)
		r, err := s.startHubBar(t, 100, &bar)
		if err != nil {
			t.Fatal(err)
		}
		bar.Store(0)
		r.setPrice(90)
		r.waitFor(t, "fill", func(o map[int64]*ormo.InOutOrder) bool { return o[id] != nil && o[id].Enter.Filled == 1 })
	})
	t.Run("refused when the consumed bars are unknowable", func(t *testing.T) {
		s, _ := setup(t, 1, 0)
		var bar atomic.Int64 // Bar 0 from the start
		r, err := s.startHubBar(t, 100, &bar)
		if err == nil {
			r.stop()
			t.Fatal("expected the migration to be refused")
		}
		if !strings.Contains(err.Error(), "no bar clock is visible") || !strings.Contains(err.Error(), "legacy order") {
			t.Fatalf("unclear error: %v", err)
		}
		if _, statErr := os.Stat(s.statePath); !os.IsNotExist(statErr) {
			t.Fatalf("a journal was written despite the refusal: %v", statErr)
		}
	})
}

func TestHubLegacyBindRejectsConflictingOCO(t *testing.T) {
	s := &legacyScenario{f: newSharedTriggerFixture(t), statePath: t.TempDir() + "/robot/ts-local.json"}
	f := s.f
	a := f.entry(t, &strat.EnterReq{CommandID: "a", Tag: "a", Amount: 0.5, OrderType: core.OrderTypeLimit, Limit: 85, OCOGroup: "g"}).ID
	f.process.Close()
	r, err := s.startHub(t, 100)
	if err != nil {
		t.Fatal(err)
	}
	lot := execution.VirtualLotID(lotsByOrder(t, r, f.key)[a])
	page, err := r.hub.Port().TimeSeriesProjection(context.Background(), f.key, execution.TimeSeriesProjectionRequest{Name: "probe-bind", Limit: 10})
	if err != nil || len(page.Records) != 1 {
		t.Fatalf("%v %v", page, err)
	}
	md := page.Records[0].Metadata
	bind := func(group string, seq int64) error {
		return r.rt.SharedExecution().BindTimeSeriesLegacy(context.Background(), execution.TimeSeriesLegacyBinding{Strategy: "ts", Lot: lot, Metadata: md, OCOGroup: group, OCOSeq: seq})
	}
	if err := bind("g", a); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	for _, c := range []struct {
		g string
		s int64
	}{{"h", a}, {"g", a + 5}, {" g", a}, {"g", 0}, {"", 3}} {
		if err := bind(c.g, c.s); err == nil {
			t.Fatalf("binding %+v accepted", c)
		}
	}
}
