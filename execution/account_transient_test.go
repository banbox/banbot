package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

func TestTransientAccountReadErrorTable(t *testing.T) {
	codes := map[string]int{
		"NetFail": errs.CodeNetFail, "Timeout": errs.CodeTimeout, "SystemBusy": errs.CodeSystemBusy,
		"ServerError": errs.CodeServerError, "RateLimit": errs.CodeRateLimit,
		"TemporarilyBanned": errs.CodeTemporarilyBanned, "ExecutionUnknown": errs.CodeExecutionUnknown,
	}
	for name, code := range codes {
		base := error(errs.NewMsg(code, "venue read failed"))
		for wrap, err := range map[string]error{
			"plain": base, "join": errors.Join(errors.New("ctx"), base), "wrapf": fmt.Errorf("stage: %w", base),
			"nested": fmt.Errorf("outer: %w", errors.Join(fmt.Errorf("inner: %w", base), errors.New("x"))),
		} {
			if !transientAccountReadError(err) {
				t.Errorf("%s/%s must be transient", name, wrap)
			}
		}
	}
	if !transientAccountReadError(context.DeadlineExceeded) || !transientAccountReadError(fmt.Errorf("w: %w", context.DeadlineExceeded)) {
		t.Error("deadline exceeded must be transient")
	}
	if transientAccountReadError(context.Canceled) || transientAccountReadError(fmt.Errorf("w: %w", context.Canceled)) {
		t.Error("cancel must not be transient")
	}
	if transientAccountReadError(nil) {
		t.Error("nil must not be transient")
	}
	if transientAccountReadError(errors.New("plain")) || transientAccountReadError(errs.NewMsg(errs.CodeNoMarketForPair, "other code")) {
		t.Error("non-transient errors misclassified")
	}
	// A typed-nil *errs.Error is never produced as a non-nil error by the code
	// under test; the guarded helper converts it to a true nil interface.
	guarded := func(e *errs.Error) error {
		if e == nil {
			return nil
		}
		return e
	}
	var typedNil *errs.Error
	if transientAccountReadError(guarded(typedNil)) {
		t.Error("guarded typed nil must not be transient")
	}
}

// reportFake wraps the paper venue with a controllable private report stream,
// snapshot and funding recovery.
type reportFake struct {
	*PaperAdapter
	reports chan BanexgStreamReport
	borrow  *SharedAccountBorrow

	mu           sync.Mutex
	snapErr      error
	recoverErr   error
	recoverCalls int
	snapCalls    int
	onRecover    func(context.Context, *SharedAccountBorrow) error
}

func (a *reportFake) Reports(ctx context.Context) (<-chan BanexgStreamReport, error) {
	go func() { <-ctx.Done(); close(a.reports) }()
	return a.reports, nil
}
func (a *reportFake) Snapshot(ctx context.Context) (VenueSnapshot, error) {
	a.mu.Lock()
	a.snapCalls++
	err := a.snapErr
	a.mu.Unlock()
	if err != nil {
		return VenueSnapshot{}, err
	}
	return a.PaperAdapter.Snapshot(ctx)
}
func (a *reportFake) RecoverCash(ctx context.Context, b *SharedAccountBorrow) error {
	a.mu.Lock()
	a.recoverCalls++
	err, hook := a.recoverErr, a.onRecover
	a.mu.Unlock()
	if err != nil {
		return err
	}
	if hook != nil {
		return hook(ctx, b)
	}
	return nil
}
func (a *reportFake) set(f func()) { a.mu.Lock(); f(); a.mu.Unlock() }
func (a *reportFake) counts() (recover, snap int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.recoverCalls, a.snapCalls
}

type reportRig struct {
	t         *testing.T
	b         *SharedAccountBorrow
	fake      *reportFake
	committed chan struct{}
	errs      <-chan error
}

func newReportRig(t *testing.T) *reportRig {
	t.Helper()
	b, venue := strategyService(t, true)
	fake := &reportFake{PaperAdapter: venue, reports: make(chan BanexgStreamReport, 8), borrow: b}
	b.service.opts.Adapter = fake
	rig := &reportRig{t: t, b: b, fake: fake, committed: make(chan struct{}, 64)}
	unsubscribe, err := b.SubscribeCommitted(func() {
		select {
		case rig.committed <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unsubscribe)
	if err := b.StartReports(); err != nil {
		t.Fatal(err)
	}
	if rig.errs, err = b.ReportErrors(); err != nil {
		t.Fatal(err)
	}
	return rig
}

func (r *reportRig) drain() {
	for {
		select {
		case <-r.committed:
		case <-time.After(150 * time.Millisecond):
			return
		}
	}
}

// wake drives the same reconcile("private-account") closure as a periodic tick.
func (r *reportRig) wake() {
	r.t.Helper()
	r.drain()
	r.fake.reports <- BanexgStreamReport{AccountChanged: true}
	select {
	case <-r.committed:
	case <-r.b.service.reportDone:
		return
	case <-time.After(5 * time.Second):
		r.t.Fatal("worker did not process the wake-up")
	}
	r.drain()
}

func (r *reportRig) ready() bool {
	r.b.service.mu.Lock()
	defer r.b.service.mu.Unlock()
	return r.b.service.ready
}
func (r *reportRig) alive() bool {
	select {
	case <-r.b.service.reportDone:
		return false
	default:
		return true
	}
}
func (r *reportRig) frozen() bool {
	s, err := r.b.Snapshot(context.Background())
	if err != nil {
		r.t.Fatal(err)
	}
	return s.RiskFrozen
}
func (r *reportRig) assertHealthy(what string) {
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

var seq int

func (r *reportRig) createCommand() PendingCommand {
	seq++
	c := pendingCreate(fmt.Sprintf("create-%d", seq), "a")
	c.Lot = VirtualLotID(fmt.Sprintf("lot-a-%d", seq))
	return c
}

func (r *reportRig) wantNotReady() {
	r.t.Helper()
	_, err := r.b.ApplyPendingCommand(context.Background(), r.createCommand(), 12)
	if code, ok := RejectionCode(err); !ok || code != RejectAccountNotReady {
		r.t.Fatalf("want typed account_not_ready, got %v", err)
	}
	cancel := PendingCommand{ID: fmt.Sprintf("cancel-%d", seq), Strategy: "a", Lot: "lot-a", Action: PendingCancelEntry}
	_, err = r.b.ApplyPendingCommand(context.Background(), cancel, 12)
	if err == nil {
		r.t.Fatal("non-create command accepted while unready")
	}
	if code, ok := RejectionCode(err); ok {
		r.t.Fatalf("non-create command must keep the hard error, got typed %s", code)
	}
}
func (r *reportRig) wantAccepted() {
	r.t.Helper()
	if _, err := r.b.ApplyPendingCommand(context.Background(), r.createCommand(), 12); err != nil {
		r.t.Fatalf("create refused after recovery: %v", err)
	}
}

func TestReportWorkerTransientFundingFailureDoesNotFreeze(t *testing.T) {
	rig := newReportRig(t)
	rig.fake.set(func() { rig.fake.recoverErr = error(errs.NewMsg(errs.CodeRateLimit, "income rate limited")) })
	rig.wake()
	if n, _ := rig.fake.counts(); n != 1 {
		t.Fatalf("first reconcile must fetch funding once, got %d", n)
	}
	rig.assertHealthy("after rate-limited funding fetch")
	if !rig.ready() {
		t.Fatal("reconcile without settlement must still make the account ready")
	}
	rig.wake() // backoff: the scheduled fetch is not retried at once
	if n, _ := rig.fake.counts(); n != 1 {
		t.Fatalf("funding fetched during retry backoff: %d", n)
	}
	rig.fake.set(func() { rig.fake.recoverErr = nil })
	rig.wake()
	rig.assertHealthy("after funding recovered")
	rig.wantAccepted()
}

func TestReportWorkerTransientSnapshotFailureSkipsTick(t *testing.T) {
	for name, cause := range map[string]error{
		"net-fail": error(errs.NewMsg(errs.CodeNetFail, "network down")),
		"deadline": fmt.Errorf("snapshot: %w", context.DeadlineExceeded),
	} {
		t.Run(name, func(t *testing.T) {
			rig := newReportRig(t)
			rig.wake()
			rig.assertHealthy("baseline")
			rig.fake.set(func() { rig.fake.snapErr = cause })
			rig.wake()
			rig.assertHealthy("after transient snapshot failure")
			if rig.ready() {
				t.Fatal("account must be unready while the snapshot fails")
			}
			rig.wantNotReady()
			rig.wake()
			rig.assertHealthy("persisting outage")
			rig.fake.set(func() { rig.fake.snapErr = nil })
			rig.wake()
			rig.assertHealthy("after recovery")
			if !rig.ready() {
				t.Fatal("account not ready after the venue recovered")
			}
			rig.wantAccepted()
		})
	}
}

func TestReportWorkerNonTransientFundingFailureFreezesAndStops(t *testing.T) {
	rig := newReportRig(t)
	rig.fake.set(func() { rig.fake.recoverErr = errors.New("funding payload corrupt") })
	rig.wake()
	select {
	case <-rig.b.service.reportDone:
	case <-time.After(5 * time.Second):
		t.Fatal("worker survived a non-transient funding failure")
	}
	if !rig.frozen() || rig.ready() {
		t.Fatal("non-transient funding failure did not fail closed")
	}
	select {
	case err := <-rig.errs:
		if err == nil {
			t.Fatal("nil report error")
		}
	case <-time.After(time.Second):
		t.Fatal("failure not reported")
	}
}

func TestReportWorkerPersistentCashMismatchFreezesAndStops(t *testing.T) {
	rig := newReportRig(t)
	rig.wake()
	rig.assertHealthy("baseline")
	before, _ := rig.fake.counts()
	if err := rig.fake.ApplyCash(decimal.NewFromInt(7)); err != nil { // external change, no funding record
		t.Fatal(err)
	}
	rig.wake()
	select {
	case <-rig.b.service.reportDone:
	case <-time.After(5 * time.Second):
		t.Fatal("worker survived a genuine cash mismatch")
	}
	if after, _ := rig.fake.counts(); after != before+1 {
		t.Fatalf("mismatch must fetch funding exactly once on demand: %d -> %d", before, after)
	}
	if !rig.frozen() || rig.ready() {
		t.Fatal("genuine cash mismatch did not fail closed")
	}
}

func TestReportWorkerSnapshotFailureNonTransientFreezes(t *testing.T) {
	rig := newReportRig(t)
	rig.fake.set(func() { rig.fake.snapErr = errors.New("balance payload corrupt") })
	rig.wake()
	select {
	case <-rig.b.service.reportDone:
	case <-time.After(5 * time.Second):
		t.Fatal("worker survived a non-transient snapshot failure")
	}
	if !rig.frozen() {
		t.Fatal("non-transient snapshot failure did not freeze")
	}
}

func TestReportWorkerFundingFetchedOnDemandAfterCashChange(t *testing.T) {
	rig := newReportRig(t)
	var pendingFunding decimal.Decimal // settlement the venue has published but the ledger lacks
	rig.fake.set(func() {
		rig.fake.onRecover = func(ctx context.Context, b *SharedAccountBorrow) error {
			if pendingFunding.IsZero() {
				return nil
			}
			_, err := b.ApplyFunding(FundingSettlement{ID: "funding/1", Instrument: ledgerInstrument(), Mark: intentPrice("100"), Rate: intentPrice("0.0001"), AccountAmount: pendingFunding, AtMS: time.Now().UnixMilli()})
			return err
		}
	})
	rig.wake()
	if n, _ := rig.fake.counts(); n != 1 {
		t.Fatalf("first reconcile fetches funding once, got %d", n)
	}
	for i := 0; i < 3; i++ { // no cash change: no more fetches within the interval
		rig.wake()
	}
	if n, _ := rig.fake.counts(); n != 1 {
		t.Fatalf("funding refetched without cash change: %d", n)
	}
	rig.assertHealthy("baseline")
	rig.fake.set(func() { pendingFunding = intentPrice("5") })
	if err := rig.fake.ApplyCash(intentPrice("5")); err != nil { // venue pays the settlement
		t.Fatal(err)
	}
	rig.wake()
	if n, _ := rig.fake.counts(); n != 2 {
		t.Fatalf("cash mismatch must trigger exactly one on-demand fetch, got %d", n)
	}
	rig.assertHealthy("after on-demand funding")
	if !rig.ready() {
		t.Fatal("account not ready after funding explained the mismatch")
	}
	snapshot, err := rig.b.Snapshot(context.Background())
	if err != nil || !snapshot.AccountSettledCash.Equal(intentPrice("1005")) {
		t.Fatal("funding not committed to the ledger", snapshot.AccountSettledCash, err)
	}
	rig.wantAccepted()
}

// Real 5s ticks: the first periodic tick reconciles (one venue snapshot) and
// fetches funding; later ticks within privateReconcileInterval take no venue
// snapshot and do not refetch funding, and a transient failure never freezes.
func TestReportWorkerPeriodicTicksFetchFundingAtMostPerInterval(t *testing.T) {
	rig := newReportRig(t)
	rig.fake.set(func() { rig.fake.recoverErr = error(errs.NewMsg(errs.CodeRateLimit, "income rate limited")) })
	deadline := time.Now().Add(9 * time.Second)
	for {
		if _, snap := rig.fake.counts(); snap >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic tick did not run")
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(6 * time.Second) // at least one more 5s tick inside the interval
	if _, snap := rig.fake.counts(); snap != 1 {
		t.Fatalf("ticks inside privateReconcileInterval took %d venue snapshots, want 1", snap)
	}
	if n, _ := rig.fake.counts(); n != 1 {
		t.Fatalf("periodic ticks fetched funding %d times", n)
	}
	rig.assertHealthy("after periodic ticks")
	if !rig.ready() {
		t.Fatal("periodic reconcile did not ready the account")
	}
}

func TestReportWorkerOrderReportCommitsRecoveredFillDespiteTransientReconcileFailure(t *testing.T) {
	rig := newReportRig(t)
	rig.wake()
	now := int64(10) // the fixture's account quote is valid until 1000
	update := strategyRequest("a", "fill", now, StrategyTargetsFull, ExecutableTarget{Lot: "lot-fill", SignedSteps: 3})
	input := &update.Requests[0]
	input.Quote = VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}
	constraint := contributor(rig.b.service.store, "a", "lot-fill", Buy, 3, "99", "0")
	constraint.Conditions.PostOnly = true
	constraint.Conditions.ExpiresAtMS = 900
	input.IntentConstraints = []EligibleIntent{constraint}
	if err := rig.b.RebalanceStrategy(update, now); err != nil {
		t.Fatal(err)
	}
	// The venue fills the resting order; the stream then reports its identity.
	if _, err := rig.fake.AdvanceQuote(context.Background(), "BTC", VisibleQuote{Bid: intentPrice("98"), Ask: intentPrice("98"), AtMS: now + 1, ReceivedMS: now + 1, ValidUntilMS: 1000, Bar: now + 1}, now+1); err != nil {
		t.Fatal(err)
	}
	snapshot, err := rig.b.Snapshot(context.Background())
	if err != nil || len(snapshot.Orders) == 0 {
		t.Fatal("no order to report", snapshot.Orders, err)
	}
	orderID := snapshot.Orders[0].Intent.ID
	for _, lot := range snapshot.Lots {
		if lot.ID == "lot-fill" && lot.SignedSteps != 0 {
			t.Fatal("fixture fill already committed", lot)
		}
	}
	rig.drain()
	rig.fake.set(func() { rig.fake.snapErr = error(errs.NewMsg(errs.CodeNetFail, "network down")) })
	rig.fake.reports <- BanexgStreamReport{OrderID: orderID}
	select {
	case <-rig.committed:
	case <-rig.b.service.reportDone:
		t.Fatal("worker stopped on a transient reconcile failure")
	case <-time.After(5 * time.Second):
		t.Fatal("committed listeners not notified for the recovered fill")
	}
	rig.drain()
	rig.assertHealthy("order report with transient reconcile failure")
	after, err := rig.b.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var filled int64
	for _, lot := range after.Lots {
		if lot.ID == "lot-fill" {
			filled = lot.SignedSteps
		}
	}
	if filled != 3 {
		t.Fatal("recovered fill was not committed to the ledger", filled, after.Lots)
	}
	if rig.ready() {
		t.Fatal("account must stay unready until reconciliation succeeds")
	}
	rig.fake.set(func() { rig.fake.snapErr = nil })
	rig.wake()
	rig.assertHealthy("after recovery")
	if !rig.ready() {
		t.Fatal("not ready after recovery")
	}
}
