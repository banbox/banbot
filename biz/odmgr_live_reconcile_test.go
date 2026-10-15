package biz

import (
	"fmt"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// reconcileExchange is the smallest adapter that provides both contracts the
// submit reconciliation needs: stable per-purpose client order ids, and a lookup
// that tells an uncertain submission apart from a confirmed absence.
type reconcileExchange struct {
	*issue138Exchange
	createCalls int
	fetchCalls  int
	createIDs   []string
	fetchIDs    []string
	createErr   *errs.Error
	fetchRes    *banexg.Order
	fetchErr    *errs.Error
}

func (e *reconcileExchange) GetLeverage(string, float64, string) (float64, float64) {
	return 1, 10
}

func (e *reconcileExchange) CreateOrder(symbol, odType, side string, amount, price float64,
	params map[string]interface{}) (*banexg.Order, *errs.Error) {
	e.createCalls++
	clientID, _ := params[banexg.ParamClientOrderId].(string)
	e.createIDs = append(e.createIDs, clientID)
	if e.createErr != nil {
		return nil, e.createErr
	}
	return &banexg.Order{ID: "created-1", Symbol: symbol, Side: side,
		ClientOrderID: clientID, Status: banexg.OdStatusOpen, Timestamp: 200}, nil
}

func (e *reconcileExchange) BuildPurposeClientOrderID(namespace string, orderID int64,
	clientID string, purpose banexg.ClientOrderPurpose) string {
	marker := "z"
	if purpose == banexg.ClientOrderPurposeExit {
		marker = "y"
	}
	return fmt.Sprintf("%s_%d_%s_%s", namespace, orderID, marker, clientID)
}

func (e *reconcileExchange) IndeterminateSubmit(err *errs.Error) bool {
	return err != nil && err.Code == errs.CodeTimeout
}

func (e *reconcileExchange) FindSubmittedOrder(symbol, clientOrderID string,
	params map[string]interface{}) (*banexg.Order, *errs.Error) {
	e.fetchCalls++
	e.fetchIDs = append(e.fetchIDs, clientOrderID)
	if e.fetchErr != nil {
		return nil, e.fetchErr
	}
	if e.fetchRes == nil {
		return nil, nil
	}
	res := *e.fetchRes
	res.Symbol = symbol
	res.ClientOrderID = clientOrderID
	return &res, nil
}

func newReconcileOrder(id int64) *ormo.InOutOrder {
	od := issue138Order("")
	od.ID = id
	od.Enter.OrderType = banexg.OdTypeMarket
	return od
}

func TestSubmitClientOrderIDSeparatesEnterFromExit(t *testing.T) {
	exchange := &reconcileExchange{issue138Exchange: &issue138Exchange{}}
	withIssue138Exchange(t, exchange)
	oldName := config.Name
	config.Name = "unittest"
	t.Cleanup(func() { config.Name = oldName })
	mgr := newLiveOrderMgr("reconcile-id", func(*ormo.InOutOrder, bool) {})
	od := newReconcileOrder(1900)

	enter, err := mgr.buildSubmitClientOrderID(od, true)
	if err != nil {
		t.Fatalf("build enter id: %v", err)
	}
	again, err := mgr.buildSubmitClientOrderID(od, true)
	if err != nil {
		t.Fatalf("rebuild enter id: %v", err)
	}
	exit, err := mgr.buildSubmitClientOrderID(od, false)
	if err != nil {
		t.Fatalf("build exit id: %v", err)
	}
	if enter != again {
		t.Fatalf("enter id changed between builds: %q != %q", enter, again)
	}
	if enter == exit {
		t.Fatalf("entry and exit share client id %q", enter)
	}
	if mgr.getClientOrderID(enter) != od.ID || mgr.getClientOrderID(exit) != od.ID {
		t.Fatalf("purpose ids are not parseable back to the local order: %q %q", enter, exit)
	}
}

func TestSubmitClientOrderIDFallsBackWithoutPurposeCapability(t *testing.T) {
	withIssue138Exchange(t, &issue138Exchange{})
	mgr := newLiveOrderMgr("reconcile-fallback", func(*ormo.InOutOrder, bool) {})
	od := newReconcileOrder(1901)
	first, err := mgr.buildSubmitClientOrderID(od, true)
	if err != nil {
		t.Fatalf("build fallback id: %v", err)
	}
	second, err := mgr.buildSubmitClientOrderID(od, true)
	if err != nil {
		t.Fatalf("rebuild fallback id: %v", err)
	}
	if first == second {
		t.Fatalf("adapter without purpose capability must keep randomizing: %q", first)
	}
	if mgr.submitReconciler() != nil {
		t.Fatal("adapter without reconcile capability must not reconcile submissions")
	}
}

func TestUncertainEntrySubmitIsAdoptedFromExchange(t *testing.T) {
	exchange := &reconcileExchange{
		issue138Exchange: &issue138Exchange{},
		createErr:        errs.NewMsg(errs.CodeTimeout, "response lost"),
		fetchRes:         &banexg.Order{ID: "entry-1", Status: banexg.OdStatusOpen, Timestamp: 300},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("reconcile-adopt", func(*ormo.InOutOrder, bool) {})
	od := newReconcileOrder(1902)

	if err := mgr.submitExgOrder(od, true); err != nil {
		t.Fatalf("uncertain submit should be resolved: %v", err)
	}
	if od.Enter.OrderID != "entry-1" {
		t.Fatalf("accepted entry not adopted: %q", od.Enter.OrderID)
	}
	if submitUnconfirmedAction(od) != "" {
		t.Fatal("resolved submission must clear the unconfirmed marker")
	}
	if exchange.createCalls != 1 || exchange.fetchCalls != 1 {
		t.Fatalf("submit/lookup calls = %d/%d, want 1/1", exchange.createCalls, exchange.fetchCalls)
	}
	if exchange.createIDs[0] != exchange.fetchIDs[0] {
		t.Fatalf("lookup used a different id than the submission: %q vs %q", exchange.createIDs[0], exchange.fetchIDs[0])
	}
}

func TestUnconfirmedEntryIsResubmittedOnlyAfterConfirmedAbsence(t *testing.T) {
	exchange := &reconcileExchange{issue138Exchange: &issue138Exchange{}}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("reconcile-absent", func(*ormo.InOutOrder, bool) {})
	od := newReconcileOrder(1903)
	od.SetInfo(ormo.OdInfoSubmitUnconfirmed, ormo.OdActionEnter)

	if err := mgr.submitExgOrder(od, true); err != nil {
		t.Fatalf("resubmit after confirmed absence: %v", err)
	}
	if exchange.fetchCalls != 1 || exchange.createCalls != 1 {
		t.Fatalf("lookup/submit calls = %d/%d, want 1/1", exchange.fetchCalls, exchange.createCalls)
	}
	if exchange.createIDs[0] != exchange.fetchIDs[0] {
		t.Fatalf("retry must reuse the idempotency key: %q vs %q", exchange.createIDs[0], exchange.fetchIDs[0])
	}

	// An existing exchange order is adopted instead of submitting a second one.
	exchange.fetchRes = &banexg.Order{ID: "entry-2", Status: banexg.OdStatusOpen, Timestamp: 300}
	pending := newReconcileOrder(1904)
	pending.SetInfo(ormo.OdInfoSubmitUnconfirmed, ormo.OdActionEnter)
	if err := mgr.submitExgOrder(pending, true); err != nil {
		t.Fatalf("adopt pending submission: %v", err)
	}
	if exchange.createCalls != 1 {
		t.Fatalf("second entry submitted while the first is on the exchange: %d creates", exchange.createCalls)
	}
	if pending.Enter.OrderID != "entry-2" {
		t.Fatalf("pending entry not adopted: %q", pending.Enter.OrderID)
	}

	// A failed lookup is not a confirmed absence, so nothing is submitted.
	exchange.fetchErr = errs.NewMsg(errs.CodeNetFail, "lookup failed")
	blocked := newReconcileOrder(1905)
	blocked.SetInfo(ormo.OdInfoSubmitUnconfirmed, ormo.OdActionEnter)
	if err := mgr.submitExgOrder(blocked, true); err == nil {
		t.Fatal("expected lookup failure to block the resubmit")
	}
	if exchange.createCalls != 1 {
		t.Fatalf("resubmitted after a failed lookup: %d creates", exchange.createCalls)
	}
}

func TestSubmitReconcileRetriesAreBounded(t *testing.T) {
	exchange := &reconcileExchange{
		issue138Exchange: &issue138Exchange{},
		createErr:        errs.NewMsg(errs.CodeTimeout, "response lost"),
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("reconcile-bound", func(*ormo.InOutOrder, bool) {})
	od := newReconcileOrder(1906)

	if err := mgr.submitExgOrder(od, true); err == nil {
		t.Fatal("an unresolvable uncertain submit must keep reporting the failure")
	}
	if submitUnconfirmedAction(od) != ormo.OdActionEnter {
		t.Fatalf("uncertain submit was not marked for reconciliation: %q",
			od.GetInfoString(ormo.OdInfoSubmitUnconfirmed))
	}

	lastErr := errs.NewMsg(errs.CodeTimeout, "response lost")
	for attempt := 1; attempt < submitReconcileMaxTries; attempt++ {
		mgr.scheduleSubmitReconcile(od, ormo.OdActionEnter, lastErr)
		if submitUnconfirmedAction(od) != ormo.OdActionEnter {
			t.Fatalf("reconciliation stopped after %d attempts", attempt)
		}
	}
	if tries := od.GetInfoInt64(ormo.OdInfoSubmitReconcileTries); tries != submitReconcileMaxTries-1 {
		t.Fatalf("reconcile tries = %d, want %d", tries, submitReconcileMaxTries-1)
	}
	mgr.scheduleSubmitReconcile(od, ormo.OdActionEnter, lastErr)
	if submitUnconfirmedAction(od) != "" {
		t.Fatal("exhausted reconciliation must clear the marker")
	}
	if od.ExitTag != core.ExitTagFatalErr {
		t.Fatalf("exhausted entry reconciliation should take the submit failure path: tag=%q", od.ExitTag)
	}
}

func TestConclusiveRejectionEndsReconciliation(t *testing.T) {
	exchange := &reconcileExchange{
		issue138Exchange: &issue138Exchange{},
		createErr:        errs.NewMsg(errs.CodeOrderRejected, "rejected"),
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("reconcile-rejected", func(*ormo.InOutOrder, bool) {})
	od := newReconcileOrder(1909)
	od.SetInfo(ormo.OdInfoSubmitUnconfirmed, ormo.OdActionEnter)

	if err := mgr.submitExgOrder(od, true); err == nil {
		t.Fatal("expected the rejection to be reported")
	}
	if submitUnconfirmedAction(od) != "" {
		t.Fatal("a conclusive answer must end reconciliation")
	}
	if exchange.createCalls != 1 || exchange.fetchCalls != 1 {
		t.Fatalf("lookup/submit calls = %d/%d, want 1/1", exchange.fetchCalls, exchange.createCalls)
	}
}

func TestSubmitUnconfirmedActionStopsOnceSubmitted(t *testing.T) {
	od := newReconcileOrder(1907)
	if submitUnconfirmedAction(od) != "" {
		t.Fatal("an order without a marker has nothing to reconcile")
	}
	od.SetInfo(ormo.OdInfoSubmitUnconfirmed, ormo.OdActionEnter)
	if submitUnconfirmedAction(od) != ormo.OdActionEnter {
		t.Fatal("unconfirmed entry must stay reconcilable")
	}
	od.Enter.OrderID = "settled-1"
	if submitUnconfirmedAction(od) != "" {
		t.Fatal("a submission recorded on the exchange must not be re-queued")
	}
}

func TestMatchesSubmittedOrderRejectsForeignOrders(t *testing.T) {
	od := newReconcileOrder(1908)
	clientID := "bot_1908_z_"
	base := &banexg.Order{ID: "o-1", Symbol: od.Symbol, Side: od.Enter.Side,
		ClientOrderID: clientID, PositionSide: banexg.PosSideLong}
	if !matchesSubmittedOrder(od, od.Enter, clientID, base) {
		t.Fatal("matching entry order rejected")
	}
	otherSide := *base
	otherSide.Side = banexg.OdSideSell
	if matchesSubmittedOrder(od, od.Enter, clientID, &otherSide) {
		t.Fatal("order of the opposite side accepted")
	}
	short := *base
	short.PositionSide = banexg.PosSideShort
	od.Short = true
	if matchesSubmittedOrder(od, od.Enter, clientID, base) {
		t.Fatal("long order accepted for a short entry")
	}
	if !matchesSubmittedOrder(od, od.Enter, clientID, &short) {
		t.Fatal("matching short order rejected")
	}
	unrelated := *base
	unrelated.Symbol = "OTHER/USDT:USDT"
	if matchesSubmittedOrder(od, od.Enter, clientID, &unrelated) {
		t.Fatal("order of another symbol accepted")
	}
}
