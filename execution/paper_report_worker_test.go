package execution

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// These tests run the real private report worker against the durable paper
// venue, the same composition a dry_run Hub account uses.

type paperWorkerRig struct {
	t        *testing.T
	root     string
	registry *AccountRegistry
	service  *SharedAccount
	b        *SharedAccountBorrow
	venue    *PaperAdapter
	errs     <-chan error
	seq      int64
}

func fastReportWorker(t *testing.T) {
	t.Helper()
	tick, interval := privateReportTick, privateReconcileInterval
	privateReportTick, privateReconcileInterval = 10*time.Millisecond, 0
	t.Cleanup(func() { privateReportTick, privateReconcileInterval = tick, interval })
}

// openPaperWorkerRig opens (or reopens) the account at root. A fresh root is
// funded; an existing one is recovered from its ledger and venue journal.
func openPaperWorkerRig(t *testing.T, root string, adapter func(*PaperAdapter) ExecutionAdapter) *paperWorkerRig {
	t.Helper()
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	venue, err := NewDurablePaperAdapter(intentPrice("1000"), intentPrice("0.001"), intentPrice("0.001"), PaperBidAsk)
	if err != nil {
		t.Fatal(err)
	}
	var wrapped ExecutionAdapter = venue
	if adapter != nil {
		wrapped = adapter(venue)
	}
	service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: filepath.Join(root, "ledger.db"), SenderLeaseDir: filepath.Join(root, "leases"), Adapter: wrapped, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	b := service.Borrow()
	r := &paperWorkerRig{t: t, root: root, registry: registry, service: service, b: b, venue: venue}
	t.Cleanup(r.close)
	if snapshot, err := b.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	} else if snapshot.Checkpoint == 0 {
		fundStrategies(t, service.store)
	}
	if err := b.RecoverPersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Reconcile("startup", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := b.RegisterRiskPolicy("v1", domainRisk()); err != nil {
		t.Fatal(err)
	}
	if err := b.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 3600_000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *paperWorkerRig) close() {
	r.b.Release()
	r.service.Close()
	r.registry.Close()
}

func (r *paperWorkerRig) start() {
	r.t.Helper()
	if err := r.b.StartReports(); err != nil {
		r.t.Fatal(err)
	}
	var err error
	if r.errs, err = r.b.ReportErrors(); err != nil {
		r.t.Fatal(err)
	}
}

// send persists and transmits one order through the owner executor while
// holding the same service lock the worker and market stream take.
func (r *paperWorkerRig) send(id string, side OrderSide, kind IntentKind, strategy string, steps int64, limit string, planTTL time.Duration) {
	r.t.Helper()
	err := r.b.WithState(func(s *SharedAccount) error {
		now := time.Now().UnixMilli()
		r.seq++
		plan := Plan{ID: "plan-" + id, Sequence: r.seq + 100, DecisionMS: now - 20, ExpiresMS: now + planTTL.Milliseconds()}
		intent := EligibleIntent{ID: VirtualIntentID(id + "-" + strategy), Account: s.store.key, Strategy: StrategyID(strategy), Lot: VirtualLotID("lot-" + strategy), Instrument: "BTC", Kind: kind, Side: side, QuantitySteps: steps, State: Eligible, Conditions: IntentConditions{CreatedBar: 5}}
		plan.Intents = []EligibleIntent{intent}
		order := OrderIntent{ID: id, PlanID: plan.ID, Instrument: ledgerInstrument(), Side: side, Steps: steps,
			Observation: ExecutionObservation{Price: intentPrice("100"), Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now - 20, ValidUntilMS: now + 3600_000, Bar: 5},
			Allocations: []FillAllocation{{ID: strategy, IntentID: intent.ID, Strategy: intent.Strategy, Lot: intent.Lot, Side: side, Kind: kind, Steps: steps}}}
		if limit != "" {
			order.Limit = intentPrice(limit)
		}
		ctx := context.Background()
		if err := s.store.SavePlan(ctx, plan); err != nil {
			return err
		}
		if err := s.store.PrepareOrder(ctx, order, now); err != nil {
			return err
		}
		return s.executorFor(ctx).Send(id, now)
	})
	if err != nil {
		r.t.Fatal(err)
	}
}

func (r *paperWorkerRig) order(id string) StoredOrder {
	r.t.Helper()
	order, err := r.service.store.Order(context.Background(), id)
	if err != nil {
		r.t.Fatal(err)
	}
	return order
}

func (r *paperWorkerRig) frozen() bool {
	r.t.Helper()
	snapshot, err := r.b.Snapshot(context.Background())
	if err != nil {
		r.t.Fatal(err)
	}
	return snapshot.RiskFrozen
}

func (r *paperWorkerRig) alive() bool {
	select {
	case <-r.service.reportDone:
		return false
	default:
		return true
	}
}

func (r *paperWorkerRig) ready() bool {
	r.service.mu.Lock()
	defer r.service.mu.Unlock()
	return r.service.ready
}

func (r *paperWorkerRig) waitFor(what string, limit time.Duration, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s (frozen=%v alive=%v ready=%v)", what, r.frozen(), r.alive(), r.ready())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *paperWorkerRig) assertHealthy(what string) {
	r.t.Helper()
	if r.frozen() || !r.alive() {
		r.t.Fatalf("%s: account frozen=%v worker alive=%v", what, r.frozen(), r.alive())
	}
	select {
	case err := <-r.errs:
		r.t.Fatalf("%s: unexpected report error %v", what, err)
	default:
	}
}

// pendingOrders is the paper venue's resting-order count.
func (r *paperWorkerRig) pendingOrders() int {
	r.venue.mu.Lock()
	defer r.venue.mu.Unlock()
	return len(r.venue.pending)
}

// fullReconciles counts worker snapshots through the venue wrapper.
type countingPaper struct {
	*PaperAdapter
	snapshots chan struct{}
	err       error
}

func (a *countingPaper) Snapshot(ctx context.Context) (VenueSnapshot, error) {
	if a.err != nil {
		return VenueSnapshot{}, a.err
	}
	select {
	case a.snapshots <- struct{}{}:
	default:
	}
	return a.PaperAdapter.Snapshot(ctx)
}

func TestPaperReportWorkerCancelsExpiredWorkingOrder(t *testing.T) {
	fastReportWorker(t)
	r := openPaperWorkerRig(t, t.TempDir(), nil)
	r.send("rest", Buy, EntryIntent, "a", 10, "90", 400*time.Millisecond)
	if r.pendingOrders() != 1 || r.order("rest").State != OrderAcknowledged {
		t.Fatal("limit order did not rest on the paper venue")
	}
	r.start()
	// The resting order must reconcile cleanly while it is live.
	r.waitFor("ready with a resting order", 3*time.Second, r.ready)
	r.assertHealthy("resting order before expiry")
	r.waitFor("worker to cancel the expired order", 5*time.Second, func() bool { return r.order("rest").State == OrderCanceled })
	if r.pendingOrders() != 0 {
		t.Fatal("expired order still rests on the venue")
	}
	r.waitFor("ready after cancel", 3*time.Second, r.ready)
	r.assertHealthy("after expiry cancel")
}

func TestPaperReportWorkerFreezesOnPersistentFailure(t *testing.T) {
	fastReportWorker(t)
	var counting *countingPaper
	r := openPaperWorkerRig(t, t.TempDir(), func(p *PaperAdapter) ExecutionAdapter {
		counting = &countingPaper{PaperAdapter: p, snapshots: make(chan struct{}, 1)}
		return counting
	})
	r.start()
	r.waitFor("ready", 3*time.Second, r.ready)
	r.assertHealthy("baseline")
	// A non-transient venue fault: the worker must persist a freeze like live.
	counting.err = errors.New("paper venue corrupted")
	r.waitFor("freeze", 5*time.Second, r.frozen)
	select {
	case err := <-r.errs:
		if err == nil {
			t.Fatal("nil report error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("freeze was not reported")
	}
	if r.ready() {
		t.Fatal("frozen account still ready")
	}
	_, err := r.b.ApplyPendingCommand(context.Background(), pendingCreate("after-freeze", "a"), time.Now().UnixMilli())
	if err == nil {
		t.Fatal("frozen account accepted a new entry")
	}
}

func TestPaperReportWorkerNormalOperationNoMismatch(t *testing.T) {
	fastReportWorker(t)
	// The bare PaperAdapter, as in the Hub: AdvancePaperQuote only drives it directly.
	r := openPaperWorkerRig(t, t.TempDir(), nil)
	r.start()
	r.waitFor("ready", 3*time.Second, r.ready)
	// Each worker tick is 10ms with a full reconcile; wait for n of them.
	snapshots := func(n int) { time.Sleep(time.Duration(n) * 30 * time.Millisecond) }
	// A market fill, a partial close and a resting order, all while the worker ticks.
	r.send("buy", Buy, EntryIntent, "a", 10, "", time.Hour)
	snapshots(3)
	r.assertHealthy("after market fill")
	r.send("trim", Sell, ExitIntent, "a", 4, "", time.Hour)
	snapshots(3)
	r.assertHealthy("after partial close")
	r.send("rest", Buy, EntryIntent, "b", 10, "90", time.Hour)
	if r.pendingOrders() != 1 {
		t.Fatal("limit order did not rest")
	}
	snapshots(3)
	r.assertHealthy("with a resting order")
	// Quote path and worker run concurrently: a non-touching quote, then one
	// that fills the resting order. The fill is applied exactly once.
	time.Sleep(10 * time.Millisecond)
	for i, ask := range []string{"95", "95", "89"} {
		now := time.Now().UnixMilli()
		quote := VisibleQuote{Bid: intentPrice("88"), Ask: intentPrice(ask), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 3600_000}
		if err := r.b.WithState(func(s *SharedAccount) error { return s.AdvancePaperQuote(context.Background(), "BTC", quote, now) }); err != nil {
			t.Fatalf("quote %d: %v", i, err)
		}
		snapshots(2)
		time.Sleep(3 * time.Millisecond)
	}
	r.waitFor("resting order filled", 3*time.Second, func() bool { return r.order("rest").State == OrderFilled })
	snapshots(4)
	r.assertHealthy("after quote fill")
	if r.pendingOrders() != 0 {
		t.Fatal("filled order still rests")
	}
	if fills := r.order("rest").FilledSteps; fills != 10 {
		t.Fatalf("fill applied %d steps, want exactly 10", fills)
	}
	positions := map[string]int64{}
	r.venue.mu.Lock()
	for id, p := range r.venue.positions {
		positions[id] = p.steps
	}
	r.venue.mu.Unlock()
	if positions["BTC"] != 16 {
		t.Fatalf("venue position %v, want 16 (10-4+10)", positions)
	}
	r.waitFor("ready", 3*time.Second, r.ready)
}

func TestPaperReportWorkerStopsCleanly(t *testing.T) {
	fastReportWorker(t)
	r := openPaperWorkerRig(t, t.TempDir(), nil)
	r.start()
	r.waitFor("ready", 3*time.Second, r.ready)
	done := make(chan error, 1)
	go func() { done <- r.service.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown reported %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("account close did not join the paper report worker")
	}
	if r.alive() {
		t.Fatal("worker still running after close")
	}
	if r.service.reportShutdownErr != nil {
		t.Fatal(r.service.reportShutdownErr)
	}
}

func TestPaperReportWorkerStreamHonoursContext(t *testing.T) {
	p, err := NewPaperAdapter(intentPrice("1000"), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := p.Reports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream:
		t.Fatal("paper stream emitted or closed before cancel")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case _, ok := <-stream:
		if ok {
			t.Fatal("paper stream emitted")
		}
	case <-time.After(time.Second):
		t.Fatal("paper stream did not close on cancel")
	}
	if _, err := p.Reports(ctx); err == nil {
		t.Fatal("stream opened on a cancelled context")
	}
}

func TestPaperReportWorkerRestartReconcilesCleanly(t *testing.T) {
	fastReportWorker(t)
	root := t.TempDir()
	first := openPaperWorkerRig(t, root, nil)
	first.start()
	first.waitFor("ready", 3*time.Second, first.ready)
	first.send("buy", Buy, EntryIntent, "a", 10, "", time.Hour)
	first.send("trim", Sell, ExitIntent, "a", 3, "", time.Hour)
	first.send("rest", Buy, EntryIntent, "b", 10, "90", time.Hour)
	first.waitFor("ready", 3*time.Second, first.ready)
	first.assertHealthy("before restart")
	first.b.Release()
	if err := first.service.Close(); err != nil {
		t.Fatal(err)
	}
	first.registry.Close()

	second := openPaperWorkerRig(t, root, nil)
	if second.pendingOrders() != 1 {
		t.Fatal("journal replay lost the resting order")
	}
	second.start()
	second.waitFor("ready after restart", 3*time.Second, second.ready)
	time.Sleep(100 * time.Millisecond)
	second.assertHealthy("after restart")
	// ready dips between each tick's refresh and reconcile, exactly as in live.
	second.waitFor("restarted account reconciled", 3*time.Second, second.ready)
	if second.order("rest").State != OrderAcknowledged {
		t.Fatal("resting order state lost across restart")
	}
}
