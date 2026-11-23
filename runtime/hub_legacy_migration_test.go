package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
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

// legacyScenario is an embedded account whose SharedOrderMgr created: a held
// long with a stop-loss and a two-rung ladder, a plain held long, a resting
// limit entry and an OCO pair of resting limit entries.
type legacyScenario struct {
	f                                  *sharedTriggerFixture
	ladder, plain, resting, ocoA, ocoB int64
	statePath                          string
	before                             []byte
}

func newLegacyScenario(t *testing.T) *legacyScenario {
	t.Helper()
	f := newSharedTriggerFixture(t)
	s := &legacyScenario{f: f, statePath: filepath.Join(t.TempDir(), "robot", "ts-local.json")}
	s.ladder = f.entry(t, &strat.EnterReq{CommandID: "cmd-ladder", Tag: "ladder", Amount: 1, StopLoss: 70, TakeProfits: []strat.TakeProfitTarget{{Price: 110, ExitRate: 0.5, Tag: "tp1"}, {Price: 120, ExitRate: 0.5, Tag: "tp2"}}}).ID
	s.plain = f.entry(t, &strat.EnterReq{CommandID: "cmd-plain", Tag: "plain", Amount: 0.5, StopLoss: 70}).ID
	s.resting = f.entry(t, &strat.EnterReq{CommandID: "cmd-rest", Tag: "rest", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90}).ID
	s.ocoA = f.entry(t, &strat.EnterReq{CommandID: "cmd-oco-a", Tag: "oco-a", Amount: 0.5, OrderType: core.OrderTypeLimit, Limit: 85, OCOGroup: "g"}).ID
	s.ocoB = f.entry(t, &strat.EnterReq{CommandID: "cmd-oco-b", Tag: "oco-b", Amount: 0.5, OrderType: core.OrderTypeLimit, Limit: 80, OCOGroup: "g"}).ID
	f.observe(t, 100, 2)
	f.observe(t, 100, 3)
	s.before = legacyCheckpointBytes(t, f.rt.SharedExecution())
	if len(s.before) == 0 {
		t.Fatal("legacy checkpoint is empty")
	}
	return s
}

func legacyCheckpointBytes(t *testing.T, b *biz.SharedAccountBorrow) []byte {
	t.Helper()
	var out bytes.Buffer
	ctx := context.Background()
	err := b.WithState(func(s *execution.SharedAccount) error {
		root, err := s.Store().StrategyCheckpoint(ctx, "__legacy_bridge", "legacy-ts")
		if err != nil {
			return err
		}
		out.Write(root)
		after := ""
		for {
			page, err := s.Store().StrategyCheckpointsAfter(ctx, "__legacy_bridge", "legacy-ts/", after, 100)
			if err != nil {
				return err
			}
			for _, item := range page {
				out.WriteString(item.Name)
				out.Write(item.Payload)
			}
			if len(page) < 100 {
				return nil
			}
			after = page[len(page)-1].Name
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

type legacyHubRun struct {
	t       *testing.T
	process *Process
	rt      *Runtime
	hub     *execution.EmbeddedHub
	borrow  *biz.SharedAccountBorrow
	bridge  *biz.HubOrderBridgeConfig
	manager *biz.HubOrderMgr
	mu      sync.Mutex
	price   decimal.Decimal
	once    sync.Once
}

func (r *legacyHubRun) setPrice(p int64) {
	r.mu.Lock()
	r.price = decimal.NewFromInt(p)
	r.mu.Unlock()
}

func (r *legacyHubRun) stop() {
	r.once.Do(func() {
		if r.manager != nil {
			r.manager.CleanUp()
		}
		if err := r.hub.Close(); err != nil {
			r.t.Error(err)
		}
		r.borrow.Release()
		r.process.Close()
	})
}

// startHub restarts the scenario's account with the Hub order manager. A construction
// error of the manager is returned (and nothing is left running).
func (s *legacyScenario) startHub(t *testing.T, price int64) (*legacyHubRun, error) {
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
		return execution.VisibleQuote{Bid: p, Ask: p, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 5000, Bar: f.bar}, nil
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

func (r *legacyHubRun) orders(t *testing.T) map[int64]*ormo.InOutOrder {
	t.Helper()
	if err := r.manager.UpdateByDataSeries(nil, nil); err != nil {
		t.Fatal(err)
	}
	orders, lock := r.rt.Orders.GetOpenODs("default")
	lock.Lock()
	defer lock.Unlock()
	out := map[int64]*ormo.InOutOrder{}
	for id, od := range orders {
		out[id] = od.Clone()
	}
	return out
}

func (r *legacyHubRun) waitFor(t *testing.T, what string, ok func(map[int64]*ormo.InOutOrder) bool) map[int64]*ormo.InOutOrder {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		orders := r.orders(t)
		if ok(orders) {
			return orders
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %v", what, summarize(orders))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func summarize(orders map[int64]*ormo.InOutOrder) []string {
	var out []string
	for id, od := range orders {
		exit := 0.0
		if od.Exit != nil {
			exit = od.Exit.Filled
		}
		out = append(out, od.EnterTag+":"+decimal.NewFromInt(id).String()+":"+decimal.NewFromInt(int64(od.Status)).String()+":"+decimal.NewFromFloat(od.Enter.Filled).String()+"/"+decimal.NewFromFloat(exit).String())
	}
	return out
}

func triggerPrice(od *ormo.InOutOrder, key string) float64 {
	if ts, ok := od.Info[key].(*ormo.TriggerState); ok && ts != nil && ts.ExitTrigger != nil {
		return ts.Price
	}
	return 0
}

func readJournal(t *testing.T, path string) map[string]json.RawMessage {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]json.RawMessage
	if err = json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestHubLegacyMigrationEndToEnd(t *testing.T) {
	s := newLegacyScenario(t)
	f := s.f
	f.process.Close()
	r, err := s.startHub(t, 100)
	if err != nil {
		t.Fatal(err)
	}
	ids := []int64{s.ladder, s.plain, s.resting, s.ocoA, s.ocoB}
	orders := r.orders(t)
	for _, id := range ids {
		od := orders[id]
		if od == nil {
			t.Fatalf("legacy order %d is not rendered by the hub facade: %v", id, summarize(orders))
		}
		if od.Strategy != "legacy" || od.Symbol != "BTC" || od.Timeframe != "ws" {
			t.Fatalf("identity lost: %+v", od.IOrder)
		}
	}
	if len(orders) != len(ids) {
		t.Fatalf("unexpected extra orders: %v", summarize(orders))
	}
	for id, tag := range map[int64]string{s.ladder: "ladder", s.plain: "plain", s.resting: "rest", s.ocoA: "oco-a", s.ocoB: "oco-b"} {
		if orders[id].EnterTag != tag {
			t.Fatalf("tag of %d: %q want %q", id, orders[id].EnterTag, tag)
		}
	}
	if od := orders[s.ladder]; od.Enter.Filled != 1 || triggerPrice(od, ormo.OdInfoStopLoss) != 70 || triggerPrice(od, ormo.OdInfoTakeProfit) != 110 {
		t.Fatalf("ladder order lost fill or protections: filled=%v sl=%v tp=%v", od.Enter.Filled, triggerPrice(od, ormo.OdInfoStopLoss), triggerPrice(od, ormo.OdInfoTakeProfit))
	}
	if od := orders[s.plain]; od.Enter.Filled != 0.5 || triggerPrice(od, ormo.OdInfoStopLoss) != 70 {
		t.Fatalf("plain order: filled=%v sl=%v", od.Enter.Filled, triggerPrice(od, ormo.OdInfoStopLoss))
	}
	if orders[s.resting].Enter.Filled != 0 || orders[s.ocoA].Enter.Filled != 0 || orders[s.ocoB].Enter.Filled != 0 {
		t.Fatal("resting entries must still be unfilled")
	}
	journal := readJournal(t, s.statePath)
	if _, ok := journal["Migrated"]; !ok {
		t.Fatal("journal does not record the migration")
	}
	if after := legacyCheckpointBytes(t, r.rt.SharedExecution()); !bytes.Equal(after, s.before) {
		t.Fatal("legacy checkpoint changed")
	}

	// The owner extension is idempotent and refuses changed identities.
	account := r.rt.SharedExecution()
	snap, err := account.Snapshot(context.Background())
	if err != nil || len(snap.Lots) == 0 {
		t.Fatalf("snapshot: %v %v", snap, err)
	}
	var lot execution.VirtualLot
	for _, l := range snap.Lots {
		if l.SignedSteps == 10 {
			lot = l
		}
	}
	projection, err := r.hub.Port().TimeSeriesProjection(context.Background(), f.key, execution.TimeSeriesProjectionRequest{Name: "probe", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	var md execution.TimeSeriesMetadata
	for _, rec := range projection.Records {
		if rec.Lot == lot.ID {
			md = rec.Metadata
		}
	}
	if md.OrderID != s.ladder {
		t.Fatalf("owner metadata of the held lot: %+v", md)
	}
	bind := func(strategy execution.StrategyID, lot execution.VirtualLotID, m execution.TimeSeriesMetadata) error {
		return account.BindTimeSeriesLegacy(context.Background(), execution.TimeSeriesLegacyBinding{Strategy: strategy, Lot: lot, Metadata: m})
	}
	if err = bind("ts", lot.ID, md); err != nil {
		t.Fatalf("identical retry must be a no-op: %v", err)
	}
	changed := md
	changed.OrderID++
	if err = bind("ts", lot.ID, changed); err == nil {
		t.Fatal("a changed identity was accepted")
	}
	if err = bind("ts", "no-such-lot", md); err == nil {
		t.Fatal("an unknown lot was bound")
	}
	flipped := md
	flipped.Short = true
	if err = bind("ts", lot.ID, flipped); err == nil {
		t.Fatal("a flipped direction was bound")
	}

	// Pre-migration command IDs replay the original order: no duplicate order,
	// nothing sent.
	fills := f.adapter.Metrics().Fills
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	retry := &strat.EnterReq{CommandID: "cmd-plain", Tag: "plain", Amount: 0.5, StopLoss: 70}
	if od, e := r.manager.EnterOrder(symbol, "ws", retry); e != nil || od == nil || od.ID != s.plain {
		t.Fatalf("legacy CommandID retry must return the original order %d, got %+v %v", s.plain, od, e)
	}
	if got := r.orders(t); len(got) != len(ids) || f.adapter.Metrics().Fills != fills {
		t.Fatalf("retry of a pre-migration command created an order: %v", summarize(got))
	}
	if r.manager.LastError() != nil {
		t.Fatal(r.manager.LastError())
	}

	// The strategy can close a migrated held order.
	closed, e := r.manager.ExitOrder(orders[s.plain], &strat.ExitReq{CommandID: "close-plain", Tag: "done"})
	if e != nil || closed == nil || closed.Status != ormo.InOutStatusFullExit || closed.Exit == nil || closed.Exit.Filled != 0.5 {
		t.Fatalf("exit of migrated order: %+v %v", closed, e)
	}

	// Ladder: rung one executes, rung two is installed and closes the rest.
	r.setPrice(110)
	// Rung two is installed by a follow-up command one poll after rung one's
	// fill is observed, so wait for both instead of checking right after the fill.
	r.waitFor(t, "ladder rung one and rung two installed", func(o map[int64]*ormo.InOutOrder) bool {
		od := o[s.ladder]
		return od != nil && od.Exit != nil && od.Exit.Filled == 0.5 && triggerPrice(od, ormo.OdInfoTakeProfit) == 120
	})
	r.setPrice(120)
	r.waitFor(t, "ladder rung two", func(o map[int64]*ormo.InOutOrder) bool {
		od := o[s.ladder]
		return od == nil || od.Status == ormo.InOutStatusFullExit
	})

	// The resting entry still fills, then the OCO winner cancels its sibling.
	r.setPrice(90)
	r.waitFor(t, "resting entry fill", func(o map[int64]*ormo.InOutOrder) bool {
		return o[s.resting] != nil && o[s.resting].Enter.Filled == 1
	})
	r.setPrice(85)
	r.waitFor(t, "OCO winner and cancelled loser", func(o map[int64]*ormo.InOutOrder) bool {
		a, b := o[s.ocoA], o[s.ocoB]
		return a != nil && a.Enter.Filled == 0.5 && (b == nil || b.Status == ormo.InOutStatusDelete)
	})
	r.setPrice(80)
	time.Sleep(300 * time.Millisecond)
	if od := r.orders(t)[s.ocoB]; od != nil && od.Enter.Filled != 0 {
		t.Fatal("the cancelled OCO sibling filled")
	}

	// A second restart neither re-migrates nor duplicates anything.
	r.stop()
	journalBefore, err := os.ReadFile(s.statePath)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.startHub(t, 85)
	if err != nil {
		t.Fatal(err)
	}
	again := r2.orders(t)
	if _, ok := again[s.resting]; !ok {
		t.Fatalf("migrated state lost on restart: %v", summarize(again))
	}
	var migrated struct{ Migrated struct{ AtMS int64 } }
	_ = json.Unmarshal(journalBefore, &migrated)
	var migratedAfter struct{ Migrated struct{ AtMS int64 } }
	body, _ := os.ReadFile(s.statePath)
	_ = json.Unmarshal(body, &migratedAfter)
	if migrated.Migrated.AtMS == 0 || migrated.Migrated.AtMS != migratedAfter.Migrated.AtMS {
		t.Fatal("restart re-migrated")
	}
}

func TestHubLegacyMigrationRefusesUnmappableOrder(t *testing.T) {
	s := newLegacyScenario(t)
	f := s.f
	key := "ts/" + decimal.NewFromInt(s.ladder).String()
	err := f.rt.SharedExecution().WithState(func(a *execution.SharedAccount) error {
		name := "legacy-ts/order:id:" + key
		body, err := a.Store().StrategyCheckpoint(context.Background(), "__legacy_bridge", name)
		if err != nil {
			return err
		}
		var order map[string]any
		if err = json.Unmarshal(body, &order); err != nil {
			return err
		}
		order["TPLadderIndex"] = 7 // beyond the two rungs
		body, _ = json.Marshal(order)
		return a.Store().SaveProjectionMetadata(context.Background(), execution.StrategyCheckpoint{Strategy: "__legacy_bridge", Name: name, Payload: body})
	})
	if err != nil {
		t.Fatal(err)
	}
	before := legacyCheckpointBytes(t, f.rt.SharedExecution())
	f.process.Close()
	r, err := s.startHub(t, 100)
	if err == nil {
		r.stop()
		t.Fatal("an unmappable legacy order must make startup fail")
	}
	if want := "ladder progress out of range"; !bytes.Contains([]byte(err.Error()), []byte(want)) || !bytes.Contains([]byte(err.Error()), []byte("legacy order")) {
		t.Fatalf("unclear error: %v", err)
	}
	if _, statErr := os.Stat(s.statePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a journal was written despite the refusal: %v", statErr)
	}
	// Nothing was bound either: every lot is still without owner metadata.
	p := NewProcess()
	defer p.Close()
	borrow, err := p.BorrowAccount(f.key, *f.opts)
	if err != nil {
		t.Fatal(err)
	}
	defer borrow.Release()
	if after := legacyCheckpointBytes(t, borrow); len(after) == 0 || bytes.Equal(after, before) == false {
		t.Fatal("legacy checkpoint changed by the refused import")
	}
	err = borrow.WithState(func(a *execution.SharedAccount) error {
		defs, err := a.Store().LoadPendingBook(context.Background())
		if err != nil {
			return err
		}
		for _, d := range defs {
			if _, err := a.Store().StrategyCheckpoint(context.Background(), d.Order.Strategy, execution.TimeSeriesMetadataName(d.Order.Lot)); err == nil {
				t.Errorf("lot %s was bound despite the refusal", d.Order.Lot)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHubLegacyMigrationCrashConvergesAndRollback(t *testing.T) {
	s := newLegacyScenario(t)
	f := s.f
	f.process.Close()
	r, err := s.startHub(t, 100)
	if err != nil {
		t.Fatal(err)
	}
	first := r.orders(t)
	r.stop()
	// A crash after the owner binds but before the journal write leaves no
	// journal: the next start repeats the import and converges.
	if err = os.Remove(s.statePath); err != nil {
		t.Fatal(err)
	}
	r2, err := s.startHub(t, 100)
	if err != nil {
		t.Fatalf("re-import after a crash must converge: %v", err)
	}
	second := r2.orders(t)
	if len(first) != 5 || len(second) != len(first) {
		t.Fatalf("orders after re-import: %v vs %v", summarize(first), summarize(second))
	}
	for id, od := range first {
		if second[id] == nil || second[id].EnterTag != od.EnterTag || second[id].Enter.Filled != od.Enter.Filled {
			t.Fatalf("order %d differs after re-import", id)
		}
	}
	r2.stop()

	// Rollback: the shared facade still works from its untouched checkpoint.
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	rt.Clock.SetTimeMS(200)
	if err = rt.SharedExecution().Reconcile("rollback", 200); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	if _, ok := biz.GetOdMgrWithState(rt.Trading, "default").(*biz.SharedOrderMgr); !ok {
		t.Fatalf("rollback manager is %T", biz.GetOdMgrWithState(rt.Trading, "default"))
	}
	if after := legacyCheckpointBytes(t, rt.SharedExecution()); !bytes.Equal(after, s.before) {
		t.Fatal("legacy checkpoint changed across migration and rollback")
	}
	orders, lock := rt.Orders.GetOpenODs("default")
	lock.Lock()
	defer lock.Unlock()
	for _, id := range []int64{s.ladder, s.plain, s.resting, s.ocoA, s.ocoB} {
		if orders[id] == nil {
			t.Fatalf("rollback lost legacy order %d", id)
		}
	}
}
